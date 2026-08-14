package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

type seriesChunkCount struct {
	Ref        uint64
	Labels     labels.Labels
	ChunkCount int
}

func main() {
	os.Exit(run(os.Stdout, os.Stderr, os.Args[1:]))
}

func run(stdout, stderr io.Writer, args []string) int {
	flags := flag.NewFlagSet("find-series-with-many-chunks", flag.ContinueOnError)
	flags.SetOutput(stderr)

	limit := flags.Int("n", 10, "number of series to print")

	if err := flags.Parse(args); err != nil {
		return 1
	}
	if *limit <= 0 {
		fmt.Fprintln(stderr, "-n must be greater than zero")
		return 1
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: find-series-with-many-chunks [-n count] <block-dir-or-index-path>")
		return 1
	}

	results, err := topSeriesByChunkCount(flags.Arg(0), *limit)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if len(results) == 0 {
		fmt.Fprintln(stdout, "no series found")
		return 0
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "chunk_count\tseries_ref\tlabels")
	for _, result := range results {
		fmt.Fprintf(tw, "%d\t%d\t%s\n", result.ChunkCount, result.Ref, result.Labels.String())
	}
	if err := tw.Flush(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func topSeriesByChunkCount(path string, limit int) ([]seriesChunkCount, error) {
	indexPath, err := resolveIndexPath(path)
	if err != nil {
		return nil, err
	}

	reader, err := index.NewFileReader(indexPath)
	if err != nil {
		return nil, fmt.Errorf("open index %q: %w", indexPath, err)
	}
	defer reader.Close()

	postings, err := reader.Postings("", "")
	if err != nil {
		return nil, fmt.Errorf("read postings from %q: %w", indexPath, err)
	}
	postings = reader.SortedPostings(postings)

	results := make([]seriesChunkCount, 0)
	for postings.Next() {
		var (
			lset labels.Labels
			chks []chunks.Meta
		)

		ref := postings.At()
		if err := reader.Series(ref, &lset, &chks); err != nil {
			return nil, fmt.Errorf("read series %d from %q: %w", ref, indexPath, err)
		}

		results = append(results, seriesChunkCount{
			Ref:        ref,
			Labels:     append(labels.Labels(nil), lset...),
			ChunkCount: len(chks),
		})
	}
	if err := postings.Err(); err != nil {
		return nil, fmt.Errorf("iterate postings from %q: %w", indexPath, err)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].ChunkCount != results[j].ChunkCount {
			return results[i].ChunkCount > results[j].ChunkCount
		}
		if results[i].Labels.String() != results[j].Labels.String() {
			return results[i].Labels.String() < results[j].Labels.String()
		}
		return results[i].Ref < results[j].Ref
	})

	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit], nil
}

func resolveIndexPath(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat %q: %w", path, err)
	}
	if !info.IsDir() {
		return path, nil
	}

	indexPath := filepath.Join(path, "index")
	if _, err := os.Stat(indexPath); err != nil {
		return "", fmt.Errorf("stat %q: %w", indexPath, err)
	}
	return indexPath, nil
}
