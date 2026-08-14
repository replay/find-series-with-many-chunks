# find-series-with-many-chunks

One-off tools for finding series that create too many TSDB chunks.

## analyze-index

Ranks series in a persisted block by chunk count. Pass the block's `index` file:

```
go run ./cmd/analyze-index /path/to/block/index
```

## analyze-wal

Ranks series in an ingester WAL by sample count (and DPM over the WAL window).
Safe to run against a live ingester (read-only; a torn last record is ignored).
Pass a tenant TSDB dir or the `wal/` directory itself:

```
go run ./cmd/analyze-wal /data/tsdb/<tenant>
go run ./cmd/analyze-wal /data/tsdb/<tenant>/wal
```

Output columns: samples, DPM, labels. Also reads `wbl/` when present.
