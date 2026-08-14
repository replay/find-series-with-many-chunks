package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <wal-dir|tenant-tsdb-dir>\n", os.Args[0])
		os.Exit(1)
	}

	walDir, wblDir := walDirs(os.Args[1])

	stats := map[chunks.HeadSeriesRef]int64{}
	var winMin, winMax int64
	var haveWindow bool

	addSample := func(ref chunks.HeadSeriesRef, ts int64) {
		stats[ref]++
		if !haveWindow {
			winMin, winMax = ts, ts
			haveWindow = true
			return
		}
		if ts < winMin {
			winMin = ts
		}
		if ts > winMax {
			winMax = ts
		}
	}

	if err := walkWAL(walDir, func(r *wlog.Reader) error {
		return forEachSample(r, addSample)
	}); err != nil {
		panic(err)
	}
	if wblDir != "" {
		if err := walkWAL(wblDir, func(r *wlog.Reader) error {
			return forEachSample(r, addSample)
		}); err != nil && !errors.Is(err, fs.ErrNotExist) {
			panic(err)
		}
	}

	type hit struct {
		ref  chunks.HeadSeriesRef
		lset labels.Labels
		n    int64
	}
	top := make([]hit, 0, 32)
	for ref, n := range stats {
		top = append(top, hit{ref: ref, n: n})
		sort.Slice(top, func(i, j int) bool { return top[i].n > top[j].n })
		if len(top) > 20 {
			top = top[:20]
		}
	}

	wanted := map[chunks.HeadSeriesRef]int{}
	for i, h := range top {
		wanted[h.ref] = i
	}
	resolve := func(r *wlog.Reader) error {
		return forEachSeries(r, func(ref chunks.HeadSeriesRef, lset labels.Labels) {
			i, ok := wanted[ref]
			if !ok {
				return
			}
			top[i].lset = lset.Copy()
		})
	}
	if err := walkWAL(walDir, resolve); err != nil {
		panic(err)
	}
	if wblDir != "" {
		if err := walkWAL(wblDir, resolve); err != nil && !errors.Is(err, fs.ErrNotExist) {
			panic(err)
		}
	}

	for _, h := range top {
		lset := h.lset.String()
		if lset == "" {
			lset = fmt.Sprintf("<unknown ref=%d>", h.ref)
		}
		fmt.Printf("%8d  %10.1f/min  %s\n", h.n, dpm(h.n, winMin, winMax), lset)
	}
}

func walDirs(path string) (wal, wbl string) {
	if st, err := os.Stat(filepath.Join(path, "wal")); err == nil && st.IsDir() {
		wal = filepath.Join(path, "wal")
		if st, err := os.Stat(filepath.Join(path, "wbl")); err == nil && st.IsDir() {
			wbl = filepath.Join(path, "wbl")
		}
		return wal, wbl
	}
	wal = path
	if st, err := os.Stat(filepath.Join(filepath.Dir(path), "wbl")); err == nil && st.IsDir() {
		wbl = filepath.Join(filepath.Dir(path), "wbl")
	}
	return wal, wbl
}

func walkWAL(dir string, fn func(*wlog.Reader) error) error {
	cpDir, startFrom, err := wlog.LastCheckpoint(dir)
	if err != nil && !errors.Is(err, record.ErrNotFound) {
		return err
	}
	if err == nil {
		sr, err := wlog.NewSegmentsReader(cpDir)
		if err != nil {
			return err
		}
		err = fn(wlog.NewReader(sr))
		sr.Close()
		if err != nil {
			return err
		}
		startFrom++
	}

	first, last, err := wlog.Segments(dir)
	if err != nil {
		return err
	}
	if last < 0 {
		return nil
	}
	if startFrom < first {
		startFrom = first
	}
	for i := startFrom; i <= last; i++ {
		s, err := wlog.OpenReadSegment(wlog.SegmentName(dir, i))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				break
			}
			return err
		}
		sr := wlog.NewSegmentBufReader(s)
		err = fn(wlog.NewReader(sr))
		sr.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func forEachSample(r *wlog.Reader, fn func(chunks.HeadSeriesRef, int64)) error {
	dec := record.NewDecoder(nil, slog.New(slog.DiscardHandler))
	var (
		samples    []record.RefSample
		hists      []record.RefHistogramSample
		floatHists []record.RefFloatHistogramSample
	)
	for r.Next() {
		rec := r.Record()
		var err error
		switch dec.Type(rec) {
		case record.Samples, record.SamplesV2:
			samples, err = dec.Samples(rec, samples[:0])
			if err != nil {
				return err
			}
			for _, s := range samples {
				fn(s.Ref, s.T)
			}
		case record.HistogramSamples, record.CustomBucketsHistogramSamples, record.HistogramSamplesV2:
			hists, err = dec.HistogramSamples(rec, hists[:0])
			if err != nil {
				return err
			}
			for i, s := range hists {
				fn(s.Ref, s.T)
				hists[i].H = nil
			}
		case record.FloatHistogramSamples, record.CustomBucketsFloatHistogramSamples, record.FloatHistogramSamplesV2:
			floatHists, err = dec.FloatHistogramSamples(rec, floatHists[:0])
			if err != nil {
				return err
			}
			for i, s := range floatHists {
				fn(s.Ref, s.T)
				floatHists[i].FH = nil
			}
		}
	}
	return ignoreTorn(r.Err())
}

func forEachSeries(r *wlog.Reader, fn func(chunks.HeadSeriesRef, labels.Labels)) error {
	dec := record.NewDecoder(nil, slog.New(slog.DiscardHandler))
	var series []record.RefSeries
	for r.Next() {
		rec := r.Record()
		if dec.Type(rec) != record.Series {
			continue
		}
		var err error
		series, err = dec.Series(rec, series[:0])
		if err != nil {
			return err
		}
		for _, s := range series {
			fn(s.Ref, s.Labels)
		}
	}
	return ignoreTorn(r.Err())
}

func ignoreTorn(err error) error {
	if err != nil && strings.Contains(err.Error(), "last record is torn") {
		return nil
	}
	return err
}

func dpm(samples, minT, maxT int64) float64 {
	if samples == 0 {
		return 0
	}
	dur := maxT - minT
	if dur <= 0 {
		dur = 1
	}
	return float64(samples) / (float64(dur) / float64(time.Minute/time.Millisecond))
}
