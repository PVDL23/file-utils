# API Reference

Complete API documentation for file-utils package.

## Modules

- [`file_utils`](https://github.com/yourusername/file-utils/blob/master/src/file_utils/__init__.py) - Main package module
- [`file_utils.io`](https://github.com/yourusername/file-utils/blob/master/src/file_utils/io.py) - Input/Output operations
- [`file_utils.stream`](https://github.com/yourusername/file-utils/blob/master/src/file_utils/stream.py) - S3 streaming operations
- [`file_utils.convert`](https://github.com/yourusername/file-utils/blob/master/src/file_utils/convert.py) - Format conversion

---

## IO Module

### Functions

#### `is_s3(path)`

Check if a path is an S3 URL.

**Parameters:**
- `path` (str | Path): Path to check

**Returns:**
- `bool`: True if the path is an S3 URL (s3://, s3a://, s3n://)

**Example:**
```python
from file_utils import is_s3

is_s3("s3://bucket/key")  # True
is_s3("/local/path")  # False
```

---

#### `split_s3(path)`

Split an S3 URL into bucket and key.

**Parameters:**
- `path` (str | Path): S3 URL

**Returns:**
- `tuple[str, str]`: (bucket, key)

**Raises:**
- `ValueError`: If not an S3 URL or bucket is empty

**Example:**
```python
from file_utils import split_s3

bucket, key = split_s3("s3://my-bucket/data/file.csv")
# bucket = "my-bucket", key = "data/file.csv"
```

---

#### `open_any(path, mode='rb', **kwargs)`

Open a file from any storage backend (local, S3, HTTP/HTTPS).

**Parameters:**
- `path` (str | Path): File path or URL
- `mode` (str): File mode ('rb', 'wb', 'rt', 'wt', etc.)
- `**kwargs`: Additional arguments passed to fsspec.open

**Returns:**
- File-like object

**Example:**
```python
from file_utils import open_any

# Open local file
with open_any("/path/to/file.csv", "rb") as f:
    data = f.read()

# Open S3 file
with open_any("s3://bucket/key", "rb") as f:
    data = f.read()

# Open HTTP file
with open_any("https://example.com/file.csv", "rb") as f:
    data = f.read()
```

---

#### `exists(path)`

Check if a path exists.

**Parameters:**
- `path` (str | Path): File path or URL

**Returns:**
- `bool`: True if the path exists

**Example:**
```python
from file_utils import exists

exists("/local/file.csv")  # True/False
exists("s3://bucket/key")  # True/False
exists("https://example.com/file.csv")  # True/False
```

---

#### `file_size(path)`

Get the size of a file in bytes.

**Parameters:**
- `path` (str | Path): File path or URL

**Returns:**
- `int`: Size in bytes

**Example:**
```python
from file_utils import file_size

size = file_size("s3://bucket/large-file.parquet")
print(f"File size: {size} bytes")
```

---

#### `download(src, dst, **kwargs)`

Download a file from any source to a local destination.

**Parameters:**
- `src` (str | Path): Source path or URL (local, s3://, http://, https://)
- `dst` (str | Path): Destination local path
- `**kwargs`: Additional arguments passed to underlying download functions

**Returns:**
- `str | Path`: The destination path

**Example:**
```python
from file_utils import download

# Download from HTTPS
download("https://example.com/file.csv", "/local/file.csv")

# Download from S3
download("s3://bucket/key", "/local/key")

# Copy local file
download("/source/file.csv", "/dest/file.csv")
```

---

## Stream Module

### Functions

#### `s3_client(endpoint_url=None, region=None, **kwargs)`

Create an S3 client with configurable endpoint and region.

**Parameters:**
- `endpoint_url` (str | None): S3 endpoint URL
- `region` (str | None): AWS region
- `**kwargs`: Additional arguments passed to boto3.client

**Returns:**
- `boto3.client`: S3 client

**Example:**
```python
from file_utils import s3_client

# Create client with default configuration
client = s3_client()

# Create client with custom endpoint (e.g., MinIO)
client = s3_client(
    endpoint_url="https://minio.example.com:9000",
    region="us-east-1"
)
```

---

#### `upload(src, s3_url, client=None, *, threshold_mb=64, chunk_mb=64, max_concurrency=8, extra_args=None)`

Upload a file to S3 with multipart support.

**Parameters:**
- `src` (str): Source path (local or s3://)
- `s3_url` (str): Destination S3 URL
- `client` (boto3.client | None): Optional S3 client
- `threshold_mb` (int): Minimum file size in MB for multipart upload (default: 64)
- `chunk_mb` (int): Chunk size in MB for multipart upload (default: 64)
- `max_concurrency` (int): Maximum number of concurrent uploads (default: 8)
- `extra_args` (dict | None): Extra arguments passed to upload_fileobj

**Returns:**
- `str`: The S3 URL

**Raises:**
- `ValueError`: If s3_url is not a valid S3 URL

**Example:**
```python
from file_utils import upload

# Upload local file to S3
upload("/local/file.csv", "s3://bucket/data/file.csv")

# Upload with custom settings
upload(
    "/local/large-file.parquet",
    "s3://bucket/data/large-file.parquet",
    threshold_mb=128,
    chunk_mb=32,
    max_concurrency=4
)
```

---

#### `s3_download(s3_url, dst, client=None, *, threshold_mb=64, chunk_mb=64, max_concurrency=8)`

Download a file from S3 (legacy function, use `download()` for general use).

**Parameters:**
- `s3_url` (str): Source S3 URL
- `dst` (str): Destination path
- `client` (boto3.client | None): Optional S3 client
- `threshold_mb` (int): Minimum file size in MB for multipart download (default: 64)
- `chunk_mb` (int): Chunk size in MB for multipart download (default: 64)
- `max_concurrency` (int): Maximum number of concurrent downloads (default: 8)

**Returns:**
- `str`: The destination path

**Raises:**
- `ValueError`: If s3_url is not a valid S3 URL

**Example:**
```python
from file_utils import s3_download

s3_download("s3://bucket/data/file.csv", "/local/file.csv")
```

---

#### `list_keys(s3_url, client=None)`

List keys under a prefix in S3.

**Parameters:**
- `s3_url` (str): S3 URL with optional prefix
- `client` (boto3.client | None): Optional S3 client

**Yields:**
- `str`: Key names

**Raises:**
- `ValueError`: If s3_url is not a valid S3 URL

**Example:**
```python
from file_utils import list_keys

# List all keys in a bucket
for key in list_keys("s3://bucket/"):
    print(key)

# List keys with a prefix
for key in list_keys("s3://bucket/data/"):
    print(key)
```

---

#### `object_size(s3_url, client=None)`

Get the size of an S3 object in bytes.

**Parameters:**
- `s3_url` (str): S3 URL of the object
- `client` (boto3.client | None): Optional S3 client

**Returns:**
- `int`: Size in bytes

**Raises:**
- `ValueError`: If s3_url is not a valid S3 URL

**Example:**
```python
from file_utils import object_size

size = object_size("s3://bucket/data/file.parquet")
print(f"Object size: {size} bytes")
```

---

#### `delete(s3_url, client=None)`

Delete an S3 object.

**Parameters:**
- `s3_url` (str): S3 URL of the object to delete
- `client` (boto3.client | None): Optional S3 client

**Raises:**
- `ValueError`: If s3_url is not a valid S3 URL

**Example:**
```python
from file_utils import delete

# Delete an S3 object
delete("s3://bucket/data/old-file.csv")
```

---

## Convert Module

### Functions

#### `detect_format(path)`

Detect format from file extension.

**Parameters:**
- `path` (str | Path): File path or URL

**Returns:**
- `str`: Format name ('csv', 'jsonl', 'xml', 'avro', 'parquet', 'arrow')

**Raises:**
- `ValueError`: If extension is unknown

**Example:**
```python
from file_utils import detect_format

format = detect_format("data.csv")
# Returns: "csv"
```

---

### Conversion Functions

All conversion functions follow the same pattern:

#### `csv_to_parquet(src, dst, **kwargs)`
#### `tsv_to_parquet(src, dst, **kwargs)`
#### `jsonl_to_parquet(src, dst, **kwargs)`
#### `xml_to_parquet(src, dst, **kwargs)`
#### `avro_to_parquet(src, dst, **kwargs)`
#### `arrow_to_parquet(src, dst, **kwargs)`

Convert source format to Parquet.

**Parameters:**
- `src` (str | Path): Source file path or URL
- `dst` (str | Path): Destination file path
- `**kwargs`: Format-specific options

**Example:**
```python
from file_utils import csv_to_parquet

csv_to_parquet("input.csv", "output.parquet")
```

---

#### `parquet_to_csv(src, dst, **kwargs)`
#### `parquet_to_tsv(src, dst, **kwargs)`
#### `parquet_to_jsonl(src, dst, **kwargs)`
#### `parquet_to_xml(src, dst, **kwargs)`
#### `parquet_to_avro(src, dst, **kwargs)`
#### `parquet_to_arrow(src, dst, **kwargs)`

Convert Parquet to target format.

**Parameters:**
- `src` (str | Path): Source Parquet file path or URL
- `dst` (str | Path): Destination file path
- `**kwargs`: Format-specific options

**Example:**
```python
from file_utils import parquet_to_csv

parquet_to_csv("input.parquet", "output.csv")
```

---

#### `xml_to_csv(src, dst, **kwargs)`
#### `csv_to_xml(src, dst, **kwargs)`
#### `xml_to_jsonl(src, dst, **kwargs)`
#### `jsonl_to_xml(src, dst, **kwargs)`

Convert between XML and other text formats.

**Parameters:**
- `src` (str | Path): Source file path or URL
- `dst` (str | Path): Destination file path
- `**kwargs`: Format-specific options

**Example:**
```python
from file_utils import xml_to_csv

xml_to_csv("input.xml", "output.csv")
```

---

#### `convert(src, dst, **kwargs)`

Generic conversion function that automatically detects source and destination formats.

**Parameters:**
- `src` (str | Path): Source file path or URL
- `dst` (str | Path): Destination file path
- `**kwargs`: Format-specific options

**Example:**
```python
from file_utils import convert

# Convert CSV to Parquet
convert("input.csv", "output.parquet")

# Convert JSONL to Arrow
convert("input.jsonl", "output.arrow")
```

---

## Exported Functions

The main `file_utils` package exports all functions from the submodules:

```python
from file_utils import (
    # IO functions
    is_s3,
    split_s3,
    open_any,
    exists,
    file_size,
    download,
    # Stream functions
    s3_client,
    upload,
    s3_download,
    list_keys,
    object_size,
    delete,
    # Convert functions
    detect_format,
    convert,
    csv_to_parquet,
    tsv_to_parquet,
    jsonl_to_parquet,
    avro_to_parquet,
    arrow_to_parquet,
    parquet_to_csv,
    parquet_to_tsv,
    parquet_to_jsonl,
    parquet_to_avro,
    parquet_to_arrow,
    xml_to_parquet,
    parquet_to_xml,
    xml_to_csv,
    csv_to_xml,
    xml_to_jsonl,
    jsonl_to_xml,
)
```
