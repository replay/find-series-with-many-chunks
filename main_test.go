package main

import (
	"bytes"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func TestTopSeriesByChunkCount(t *testing.T) {
	blockDir := t.TempDir()
	writeTestIndex(t, filepath.Join(blockDir, "index"), []testSeries{
		{labels: labels.FromStrings("__name__", "cpu_usage_total", "instance", "a"), chunkCount: 2},
		{labels: labels.FromStrings("__name__", "http_requests_total", "instance", "b"), chunkCount: 5},
		{labels: labels.FromStrings("__name__", "memory_usage_bytes", "instance", "c"), chunkCount: 3},
	})

	results, err := topSeriesByChunkCount(blockDir, 2)
	if err != nil {
		t.Fatalf("topSeriesByChunkCount returned error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if got, want := results[0].Labels.String(), `{__name__="http_requests_total",instance="b"}`; got != want {
		t.Fatalf("unexpected first series: got %s want %s", got, want)
	}
	if got, want := results[0].ChunkCount, 5; got != want {
		t.Fatalf("unexpected first chunk count: got %d want %d", got, want)
	}
	if got, want := results[1].Labels.String(), `{__name__="memory_usage_bytes",instance="c"}`; got != want {
		t.Fatalf("unexpected second series: got %s want %s", got, want)
	}
	if got, want := results[1].ChunkCount, 3; got != want {
		t.Fatalf("unexpected second chunk count: got %d want %d", got, want)
	}
}

func TestRunPrintsTableForIndexFile(t *testing.T) {
	blockDir := t.TempDir()
	indexPath := filepath.Join(blockDir, "index")
	writeTestIndex(t, indexPath, []testSeries{
		{labels: labels.FromStrings("__name__", "alpha"), chunkCount: 1},
		{labels: labels.FromStrings("__name__", "beta"), chunkCount: 4},
	})

	var stdout, stderr bytes.Buffer
	exitCode := run(&stdout, &stderr, []string{"-n", "1", indexPath})
	if exitCode != 0 {
		t.Fatalf("run returned exit code %d with stderr %q", exitCode, stderr.String())
	}

	output := stdout.String()
	if !strings.Contains(output, "chunk_count") {
		t.Fatalf("expected header in output, got %q", output)
	}
	if !strings.Contains(output, `{__name__="beta"}`) {
		t.Fatalf("expected highest chunk-count series in output, got %q", output)
	}
	if strings.Contains(output, `{__name__="alpha"}`) {
		t.Fatalf("expected limit to exclude lower ranked series, got %q", output)
	}
}

type testSeries struct {
	labels     labels.Labels
	chunkCount int
}

func writeTestIndex(t *testing.T, indexPath string, series []testSeries) {
	t.Helper()

	writer, err := index.NewWriter(indexPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	symbols := make(map[string]struct{})
	postings := index.NewMemPostings()
	valuesByName := make(map[string]map[string]struct{})

	for _, s := range series {
		for _, label := range s.labels {
			symbols[label.Name] = struct{}{}
			symbols[label.Value] = struct{}{}
			if valuesByName[label.Name] == nil {
				valuesByName[label.Name] = make(map[string]struct{})
			}
			valuesByName[label.Name][label.Value] = struct{}{}
		}
	}

	if err := writer.AddSymbols(symbols); err != nil {
		t.Fatalf("AddSymbols: %v", err)
	}

	for i, s := range series {
		metas := make([]chunks.Meta, 0, s.chunkCount)
		for chunkIndex := 0; chunkIndex < s.chunkCount; chunkIndex++ {
			metas = append(metas, chunks.Meta{
				MinTime: int64(chunkIndex * 1000),
				MaxTime: int64((chunkIndex + 1) * 1000),
				Ref:     uint64((i+1)*1000 + chunkIndex),
			})
		}

		ref := uint64(i + 1)
		if err := writer.AddSeries(ref, s.labels, metas...); err != nil {
			t.Fatalf("AddSeries: %v", err)
		}
		postings.Add(ref, s.labels)
	}

	for name, values := range valuesByName {
		orderedValues := make([]string, 0, len(values))
		for value := range values {
			orderedValues = append(orderedValues, value)
		}
		sort.Strings(orderedValues)

		if err := writer.WriteLabelIndex([]string{name}, orderedValues); err != nil {
			t.Fatalf("WriteLabelIndex: %v", err)
		}
	}

	allRefs := make([]uint64, 0, len(series))
	for i := range series {
		allRefs = append(allRefs, uint64(i+1))
	}
	if err := writer.WritePostings("", "", index.NewListPostings(allRefs)); err != nil {
		t.Fatalf("WritePostings all: %v", err)
	}

	for _, label := range postings.SortedKeys() {
		if err := writer.WritePostings(label.Name, label.Value, postings.Get(label.Name, label.Value)); err != nil {
			t.Fatalf("WritePostings(%s=%s): %v", label.Name, label.Value, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
