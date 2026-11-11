# Quick Start

Get started with file-utils in just a few minutes!

## Basic Usage

### Install the package

```bash
uv add /path/to/file-utils
# or
pip install /path/to/file-utils
```

### Import the package

```python
import file_utils
```

## Opening Files

The `open_any` function provides a unified way to open files from any storage backend:

```python
from file_utils import open_any

# Open a local file
with open_any("/path/to/file.csv", "rb") as f:
    data = f.read()

# Open an S3 file
with open_any("s3://my-bucket/data/file.csv", "rb") as f:
    data = f.read()

# Open from HTTP/HTTPS
with open_any("https://example.com/data/file.csv", "rb") as f:
    data = f.read()
```

## Downloading Files

Use the `download` function to download files from any source to a local destination:

```python
from file_utils import download

# Download from HTTP/HTTPS
download("https://example.com/data/file.csv", "/local/path/file.csv")

# Download from S3
download("s3://my-bucket/data/file.csv", "/local/path/file.csv")

# Copy local file
download("/source/path/file.csv", "/dest/path/file.csv")
```

## File Operations

### Check if a file exists

```python
from file_utils import exists

if exists("s3://my-bucket/data/file.csv"):
    print("File exists!")
```

### Get file size

```python
from file_utils import file_size

size = file_size("s3://my-bucket/data/file.parquet")
print(f"File size: {size} bytes")
```

## S3 Operations

### Upload a file to S3

```python
from file_utils import upload

upload("/local/path/file.csv", "s3://my-bucket/data/file.csv")
```

### Download from S3 (legacy function)

```python
from file_utils import s3_download

s3_download("s3://my-bucket/data/file.csv", "/local/path/file.csv")
```

### List keys in S3

```python
from file_utils import list_keys

for key in list_keys("s3://my-bucket/data/"):
    print(key)
```

### Delete an S3 object

```python
from file_utils import delete

delete("s3://my-bucket/data/old-file.csv")
```

## Format Conversion

Convert between different file formats:

```python
from file_utils import csv_to_parquet, jsonl_to_parquet, xml_to_csv

# Convert CSV to Parquet
csv_to_parquet("/path/to/input.csv", "/path/to/output.parquet")

# Convert JSONL to Parquet
jsonl_to_parquet("/path/to/input.jsonl", "/path/to/output.parquet")

# Convert XML to CSV
xml_to_csv("/path/to/input.xml", "/path/to/output.csv")
```

## Detect File Format

```python
from file_utils import detect_format

format = detect_format("/path/to/file.csv")
print(f"Format: {format}")  # Output: csv
```

## Working with S3 Client

```python
from file_utils import s3_client

# Create a client with custom endpoint (e.g., MinIO)
client = s3_client(
    endpoint_url="https://minio.example.com:9000",
    region="us-east-1"
)

# Use the client with upload/download
upload("/local/file.csv", "s3://my-bucket/file.csv", client=client)
```

## Streaming Large Files

Process large files in chunks to avoid memory issues:

```python
from file_utils import open_any

# Read a large file in chunks
chunk_size = 1024 * 1024  # 1MB chunks

with open_any("s3://my-bucket/large-file.parquet", "rb") as f:
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        # Process chunk
        process_chunk(chunk)
```

## Next Steps

- [File Formats](file-formats.md) - Detailed information about each supported format
- [API Reference](api-reference.md) - Complete API documentation
- [Examples](examples.md) - Practical examples for common use cases
