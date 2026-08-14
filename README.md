# find-series-with-many-chunks

Small CLI tool that reads a Prometheus block index and prints the series with
the highest chunk counts.

## Usage

```bash
go run . /path/to/prometheus/block
go run . -n 25 /path/to/prometheus/block/index
```