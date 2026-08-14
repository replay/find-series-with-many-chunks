package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	r, err := index.NewFileReader(os.Args[1], index.DecodePostingsRaw)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	n, v := index.AllPostingsKey()
	p, err := r.Postings(context.Background(), n, v)
	if err != nil {
		panic(err)
	}

	type hit struct {
		lset labels.Labels
		n    int
	}
	top := make([]hit, 0, 32)
	var b labels.ScratchBuilder
	var chks []chunks.Meta

	for p.Next() {
		if err := r.Series(p.At(), &b, &chks); err != nil {
			panic(err)
		}
		h := hit{lset: b.Labels(), n: len(chks)}
		top = append(top, h)
		sort.Slice(top, func(i, j int) bool { return top[i].n > top[j].n })
		if len(top) > 20 {
			top = top[:20]
		}
	}

	for _, h := range top {
		fmt.Printf("%8d  %s\n", h.n, h.lset)
	}
}
