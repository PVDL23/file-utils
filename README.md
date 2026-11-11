# file-utils

Streaming file utilities for data engineering: download, upload, and convert between CSV, TSV, JSONL, XML, Avro, Arrow IPC, and Parquet formats. Works with local paths or S3-compatible storage (tested with Scaleway).

## Installation

```bash
uv sync
```

## Usage

### Python API

```python
import file_utils

# Convert CSV to Parquet
file_utils.convert("data.csv", "data.parquet")

# Convert with options
file_utils.convert(
    "s3://bucket/data.csv",
    "data.parquet",
    compression="zstd",
    chunk_rows=65536,
)

# Use convenience wrappers
file_utils.csv_to_parquet("data.csv", "data.parquet")
file_utils.parquet_to_jsonl("data.parquet", "data.jsonl")

# Convert XML to Parquet
file_utils.convert("data.xml", "data.parquet", record_tag="record")
file_utils.xml_to_parquet("data.xml", "data.parquet", record_tag="person")

# Convert Parquet to XML
file_utils.parquet_to_xml("data.parquet", "data.xml", record_tag="row", root_tag="data")

# S3 operations
from file_utils import s3_client, upload, download

client = s3_client(endpoint_url="https://s3.fr-par.scw.cloud")
upload("local.csv", "s3://bucket/data.csv")
download("s3://bucket/data.csv", "local.csv")

# Uniform IO
from file_utils import open_any, exists, file_size

with open_any("s3://bucket/data.parquet", "rb") as f:
    # work with file
    pass

print(exists("s3://bucket/data.parquet"))
print(file_size("s3://bucket/data.parquet"))
```

### CLI

```bash
# Convert between formats
file-utils convert data.csv data.parquet
file-utils convert data.jsonl data.parquet --compression zstd

# Upload to S3
file-utils upload data.parquet s3://bucket/path/data.parquet

# Download from S3
file-utils download s3://bucket/path/data.parquet data.parquet

# List S3 objects
file-utils ls s3://bucket/path/

# Show file info
file-utils info data.parquet
```

## Scaleway Configuration

```bash
export AWS_ACCESS_KEY_ID=<scw-access-key>
export AWS_SECRET_ACCESS_KEY=<scw-secret-key>
export S3_ENDPOINT=https://s3.fr-par.scw.cloud  # or nl-ams for Amsterdam
export AWS_DEFAULT_REGION=fr-par              # or nl-ams
```

## Features

- **Streaming**: All conversions stream through Arrow record batches with configurable batch sizes
- **Memory efficient**: Peak memory is one batch (default 65,536 rows), never the whole file
- **Format support**: CSV, TSV, JSONL, XML, Avro, Arrow IPC, Parquet
- **Storage**: Local filesystem or S3-compatible (Scaleway, AWS, etc.)
- **Schema mapping**: Automatic Avro ↔ Arrow schema conversion
- **Compression**: Parquet with ZSTD (default), Snappy, GZIP, or uncompressed

## Supported Conversions

All format pairs are supported:
- CSV ↔ Parquet, JSONL, XML, Avro, Arrow IPC
- TSV ↔ Parquet, JSONL, XML, Avro, Arrow IPC
- JSONL ↔ Parquet, CSV, XML, Avro, Arrow IPC
- XML ↔ Parquet, CSV, JSONL, Avro, Arrow IPC
- Avro ↔ Parquet, CSV, JSONL, XML, Arrow IPC
- Arrow IPC ↔ Parquet, CSV, JSONL, XML, Avro
- Parquet ↔ CSV, TSV, JSONL, XML, Avro, Arrow IPC

## Tests

```bash
uv run pytest
```

All tests run against local temporary files (no S3 required).
