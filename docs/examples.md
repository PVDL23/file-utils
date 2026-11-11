# Examples

Practical examples for common use cases with file-utils.

---

## Table of Contents

- [Basic File Operations](#basic-file-operations)
- [S3 Operations](#s3-operations)
- [HTTP/HTTPS Downloads](#httphttps-downloads)
- [Format Conversion](#format-conversion)
- [Streaming Large Files](#streaming-large-files)
- [Working with Different Formats](#working-with-different-formats)
- [Batch Processing](#batch-processing)
- [Error Handling](#error-handling)

---

## Basic File Operations

### Check if a file exists

```python
from file_utils import exists

# Local file
if exists("/path/to/file.csv"):
    print("File exists!")

# S3 file
if exists("s3://my-bucket/data/file.csv"):
    print("S3 file exists!")

# HTTP URL
if exists("https://example.com/data/file.csv"):
    print("URL is accessible!")
```

### Get file size

```python
from file_utils import file_size

# Get size of local file
size = file_size("/path/to/file.csv")
print(f"File size: {size} bytes")

# Get size of S3 file
size = file_size("s3://my-bucket/data/file.parquet")
print(f"S3 file size: {size / 1024 / 1024:.2f} MB")

# Get size of HTTP file
size = file_size("https://example.com/data/large-file.csv")
print(f"HTTP file size: {size} bytes")
```

### Open and read a file

```python
from file_utils import open_any

# Read entire file
with open_any("/path/to/file.csv", "rb") as f:
    content = f.read()

# Read line by line
with open_any("data.csv", "rt") as f:
    for line in f:
        print(line.strip())
```

---

## S3 Operations

### Configure S3 client

```python
import os
from file_utils import s3_client

# Option 1: Use environment variables
os.environ["AWS_ACCESS_KEY_ID"] = "your-access-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your-secret-key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

client = s3_client()

# Option 2: Custom endpoint (e.g., MinIO)
client = s3_client(
    endpoint_url="https://minio.example.com:9000",
    region="us-east-1"
)

# Option 3: Pass client to operations
from file_utils import upload

upload("/local/file.csv", "s3://bucket/file.csv", client=client)
```

### Upload files to S3

```python
from file_utils import upload

# Simple upload
upload("/local/data.csv", "s3://my-bucket/data/data.csv")

# Upload with multipart for large files
upload(
    "/local/large-file.parquet",
    "s3://my-bucket/data/large-file.parquet",
    threshold_mb=100,  # Use multipart for files > 100MB
    chunk_mb=50,       # 50MB chunks
    max_concurrency=4  # 4 concurrent uploads
)

# Upload from S3 to S3 (copy)
upload("s3://source-bucket/file.csv", "s3://dest-bucket/file.csv")
```

### Download from S3

```python
from file_utils import download, s3_download

# Using the general download function
download("s3://my-bucket/data/file.csv", "/local/file.csv")

# Using the legacy s3_download function
s3_download("s3://my-bucket/data/file.csv", "/local/file.csv")

# Download with multipart for large files
s3_download(
    "s3://my-bucket/data/large-file.parquet",
    "/local/large-file.parquet",
    threshold_mb=100,
    chunk_mb=50,
    max_concurrency=4
)
```

### List S3 objects

```python
from file_utils import list_keys

# List all objects in a bucket
for key in list_keys("s3://my-bucket/"):
    print(key)

# List objects with a prefix
for key in list_keys("s3://my-bucket/data/2024/"):
    print(key)

# Count objects
count = sum(1 for _ in list_keys("s3://my-bucket/data/"))
print(f"Total objects: {count}")

# Get total size of all objects
from file_utils import object_size

total_size = sum(
    object_size(f"s3://my-bucket/{key}")
    for key in list_keys("s3://my-bucket/data/")
)
print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
```

### Delete S3 objects

```python
from file_utils import delete, list_keys

# Delete a single object
delete("s3://my-bucket/data/old-file.csv")

# Delete multiple objects
for key in list_keys("s3://my-bucket/data/old/"):
    delete(f"s3://my-bucket/{key}")
```

---

## HTTP/HTTPS Downloads

### Download a file from HTTP/HTTPS

```python
from file_utils import download

# Download a file
download("https://example.com/data/file.csv", "/local/file.csv")

# Download to current directory
download("https://example.com/data/file.jsonl", "file.jsonl")

# Download with custom filename
download(
    "https://example.com/data/very-long-url-file.csv",
    "/local/short-name.csv"
)
```

### Download and process directly

```python
from file_utils import download
import tempfile
import os

# Download to a temporary file
with tempfile.TemporaryDirectory() as tmpdir:
    temp_file = os.path.join(tmpdir, "temp.csv")
    download("https://example.com/data/file.csv", temp_file)
    
    # Process the file
    with open(temp_file, "r") as f:
        for line in f:
            process_line(line)
```

### Download multiple files

```python
from file_utils import download

urls = [
    "https://example.com/data/file1.csv",
    "https://example.com/data/file2.csv",
    "https://example.com/data/file3.csv",
]

for i, url in enumerate(urls):
    download(url, f"/local/file{i+1}.csv")
```

---

## Format Conversion

### Convert CSV to Parquet

```python
from file_utils import csv_to_parquet

# Simple conversion
csv_to_parquet("/path/to/input.csv", "/path/to/output.parquet")

# Convert with compression
csv_to_parquet(
    "/path/to/input.csv",
    "/path/to/output.parquet",
    compression="zstd"  # or "snappy", "gzip", "brotli", "lz4"
)
```

### Convert JSONL to Parquet

```python
from file_utils import jsonl_to_parquet

jsonl_to_parquet("/path/to/input.jsonl", "/path/to/output.parquet")
```

### Convert XML to CSV

```python
from file_utils import xml_to_csv

xml_to_csv("/path/to/input.xml", "/path/to/output.csv")
```

### Convert Parquet to JSONL

```python
from file_utils import parquet_to_jsonl

parquet_to_jsonl("/path/to/input.parquet", "/path/to/output.jsonl")
```

### Batch conversion

```python
from file_utils import csv_to_parquet, detect_format
import os

input_dir = "/path/to/input"
output_dir = "/path/to/output"

for filename in os.listdir(input_dir):
    input_path = os.path.join(input_dir, filename)
    output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.parquet")
    
    fmt = detect_format(input_path)
    
    if fmt == "csv":
        csv_to_parquet(input_path, output_path)
    elif fmt == "jsonl":
        from file_utils import jsonl_to_parquet
        jsonl_to_parquet(input_path, output_path)
```

### Convert from S3 to local

```python
from file_utils import s3_download, csv_to_parquet
import tempfile
import os

with tempfile.TemporaryDirectory() as tmpdir:
    # Download CSV from S3
    csv_path = os.path.join(tmpdir, "data.csv")
    s3_download("s3://my-bucket/data.csv", csv_path)
    
    # Convert to Parquet
    parquet_path = "/local/output.parquet"
    csv_to_parquet(csv_path, parquet_path)
```

---

## Streaming Large Files

### Read a large CSV in chunks

```python
from file_utils import open_any

chunk_size = 1024 * 1024  # 1MB chunks

with open_any("s3://my-bucket/large-file.csv", "rb") as f:
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        process_chunk(chunk)
```

### Read JSONL line by line

```python
from file_utils import open_any
import json

with open_any("s3://my-bucket/large-file.jsonl", "rt") as f:
    for line in f:
        record = json.loads(line)
        process_record(record)
```

### Stream and convert

```python
from file_utils import open_any, csv_to_parquet
import tempfile
import os

# For very large files, use temporary files
def stream_and_convert(s3_path, output_path):
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download to temp
        temp_csv = os.path.join(tmpdir, "temp.csv")
        download(s3_path, temp_csv)
        
        # Convert
        csv_to_parquet(temp_csv, output_path)
        
        # Cleanup happens automatically

stream_and_convert("s3://my-bucket/huge.csv", "/local/huge.parquet")
```

---

## Working with Different Formats

### CSV operations

```python
from file_utils import open_any, csv_to_parquet, parquet_to_csv

# Read CSV
with open_any("data.csv", "rt") as f:
    header = f.readline()
    print(f"Header: {header}")

# Convert to Parquet
csv_to_parquet("data.csv", "data.parquet")

# Convert back to CSV
parquet_to_csv("data.parquet", "data-new.csv")
```

### JSONL operations

```python
from file_utils import jsonl_to_parquet, parquet_to_jsonl, open_any
import json

# Read JSONL
with open_any("data.jsonl", "rt") as f:
    for line in f:
        data = json.loads(line)
        print(data)

# Convert to Parquet
jsonl_to_parquet("data.jsonl", "data.parquet")

# Convert back to JSONL
parquet_to_jsonl("data.parquet", "data-new.jsonl")
```

### XML operations

```python
from file_utils import xml_to_csv, xml_to_parquet, xml_to_jsonl

# Convert XML to CSV
xml_to_csv("data.xml", "data.csv")

# Convert XML to Parquet
xml_to_parquet("data.xml", "data.parquet")

# Convert XML to JSONL
xml_to_jsonl("data.xml", "data.jsonl")
```

### Parquet operations

```python
from file_utils import (
    csv_to_parquet,
    jsonl_to_parquet,
    xml_to_parquet,
    avro_to_parquet,
    arrow_to_parquet,
    parquet_to_csv,
    parquet_to_jsonl,
    parquet_to_xml,
    parquet_to_avro,
    parquet_to_arrow
)

# Convert various formats to Parquet
csv_to_parquet("data.csv", "data.parquet")
jsonl_to_parquet("data.jsonl", "data.parquet")
xml_to_parquet("data.xml", "data.parquet")

# Convert Parquet to various formats
parquet_to_csv("data.parquet", "data.csv")
parquet_to_jsonl("data.parquet", "data.jsonl")
parquet_to_xml("data.parquet", "data.xml")
```

### Detect format automatically

```python
from file_utils import detect_format, convert

# Detect format
fmt = detect_format("data.csv")
print(f"Format: {fmt}")

# Auto-convert based on file extensions
convert("data.csv", "data.parquet")
convert("data.jsonl", "data.arrow")
```

---

## Batch Processing

### Process all CSV files in a directory

```python
from file_utils import csv_to_parquet
import os

input_dir = "/path/to/csv-files"
output_dir = "/path/to/parquet-files"

for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.parquet")
        csv_to_parquet(input_path, output_path)
```

### Process all files in S3 bucket

```python
from file_utils import list_keys, download, csv_to_parquet, detect_format
import os

bucket = "my-bucket"
prefix = "data/2024/01/"
output_dir = "/local/output"

for key in list_keys(f"s3://{bucket}/{prefix}"):
    # Download file
    local_path = os.path.join(output_dir, os.path.basename(key))
    download(f"s3://{bucket}/{key}", local_path)
    
    # Convert based on format
    fmt = detect_format(local_path)
    
    if fmt == "csv":
        output_path = os.path.join(output_dir, f"{os.path.splitext(key)[0]}.parquet")
        csv_to_parquet(local_path, output_path)
```

---

## Error Handling

### Handle missing files

```python
from file_utils import exists, open_any, download

# Check before opening
if exists("s3://my-bucket/data.csv"):
    with open_any("s3://my-bucket/data.csv", "rt") as f:
        data = f.read()
else:
    print("File does not exist!")

# Or use try-except
try:
    with open_any("s3://my-bucket/data.csv", "rt") as f:
        data = f.read()
except FileNotFoundError:
    print("File not found!")
```

### Handle S3 errors

```python
from file_utils import s3_client, list_keys
import botocore.exceptions

try:
    for key in list_keys("s3://my-bucket/data/"):
        print(key)
except botocore.exceptions.ClientError as e:
    print(f"S3 error: {e}")
except ValueError as e:
    print(f"Invalid S3 URL: {e}")
```

### Handle conversion errors

```python
from file_utils import csv_to_parquet
import pyarrow.lib

try:
    csv_to_parquet("bad-data.csv", "output.parquet")
except pyarrow.lib.ArrowInvalid as e:
    print(f"Invalid CSV data: {e}")
except Exception as e:
    print(f"Conversion failed: {e}")
```

### Retry logic for downloads

```python
from file_utils import download
import time

max_retries = 3
retry_delay = 5  # seconds

for attempt in range(max_retries):
    try:
        download("https://example.com/data/file.csv", "/local/file.csv")
        break
    except Exception as e:
        if attempt == max_retries - 1:
            raise
        print(f"Attempt {attempt + 1} failed: {e}")
        time.sleep(retry_delay)
```

---

## Complete Workflow Examples

### Example 1: ETL Pipeline

```python
from file_utils import (
    download,
    list_keys,
    csv_to_parquet,
    jsonl_to_parquet,
    detect_format
)
import os

def etl_from_s3_to_parquet(bucket, prefix, output_dir):
    """Download files from S3, convert to Parquet, and save locally."""
    
    for key in list_keys(f"s3://{bucket}/{prefix}"):
        # Download file
        local_file = os.path.join(output_dir, os.path.basename(key))
        download(f"s3://{bucket}/{key}", local_file)
        
        # Convert based on format
        fmt = detect_format(local_file)
        output_file = os.path.join(
            output_dir,
            f"{os.path.splitext(os.path.basename(key))[0]}.parquet"
        )
        
        if fmt == "csv":
            csv_to_parquet(local_file, output_file)
        elif fmt == "jsonl":
            jsonl_to_parquet(local_file, output_file)
        
        # Cleanup
        os.remove(local_file)

# Run ETL
etl_from_s3_to_parquet("my-bucket", "raw-data/", "/local/processed")
```

### Example 2: Data Lake Ingestion

```python
from file_utils import (
    list_keys,
    download,
    xml_to_parquet,
    csv_to_parquet,
    jsonl_to_parquet
)
import os
from datetime import datetime

def ingest_data_lake(source_dir, target_bucket):
    """Ingest files from local directory to S3 data lake in Parquet format."""
    
    date = datetime.now().strftime("%Y/%m/%d")
    
    for filename in os.listdir(source_dir):
        input_path = os.path.join(source_dir, filename)
        
        # Create output path
        output_filename = f"{os.path.splitext(filename)[0]}.parquet"
        output_path = f"s3://{target_bucket}/data/{date}/{output_filename}"
        
        # Convert based on extension
        if filename.endswith(".xml"):
            xml_to_parquet(input_path, output_path)
        elif filename.endswith(".csv"):
            csv_to_parquet(input_path, output_path)
        elif filename.endswith(".jsonl"):
            jsonl_to_parquet(input_path, output_path)

# Run ingestion
ingest_data_lake("/local/raw-data", "my-data-lake")
```

### Example 3: Web Scraping and Storage

```python
from file_utils import download, jsonl_to_parquet
import tempfile
import os

def scrape_and_store(urls, output_dir):
    """Download files from URLs and store as Parquet."""
    
    for url in urls:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download file
            filename = os.path.basename(url.split("?")[0])
            local_path = os.path.join(tmpdir, filename)
            download(url, local_path)
            
            # Convert to Parquet
            output_path = os.path.join(
                output_dir,
                f"{os.path.splitext(filename)[0]}.parquet"
            )
            
            if filename.endswith(".jsonl"):
                jsonl_to_parquet(local_path, output_path)
            elif filename.endswith(".csv"):
                from file_utils import csv_to_parquet
                csv_to_parquet(local_path, output_path)

# Run scraping
urls = [
    "https://example.com/data/file1.jsonl",
    "https://example.com/data/file2.csv",
]
scrape_and_store(urls, "/local/scraped-data")
```
