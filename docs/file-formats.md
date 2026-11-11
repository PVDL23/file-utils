# Supported File Formats

file-utils supports the following file formats with streaming read/write and conversion capabilities:

## CSV (Comma-Separated Values)

### Overview
CSV is a simple, widely-used format for tabular data. Each line represents a row, and columns are separated by commas (or other delimiters).

### Supported Features
- ✅ Reading and writing
- ✅ Custom delimiters (including TSV with tab delimiter)
- ✅ Conversion to/from other formats
- ✅ Chunked reading for large files

### Usage Examples

```python
from file_utils import open_any, csv_to_parquet

# Read CSV
with open_any("data.csv", "rt") as f:
    for line in f:
        print(line)

# Convert CSV to Parquet
csv_to_parquet("input.csv", "output.parquet")

# Read CSV in chunks
with open_any("large.csv", "rb") as f:
    while chunk := f.read(1024 * 1024):  # 1MB chunks
        process(chunk)
```

### Notes
- First row is treated as header by default
- Supports custom delimiters (comma, tab, semicolon, etc.)
- Automatic type inference

---

## TSV (Tab-Separated Values)

### Overview
TSV is similar to CSV but uses tabs as delimiters. Often used in data exchange where comma characters might appear in the data.

### Supported Features
- ✅ Reading and writing (handled as CSV with tab delimiter)
- ✅ Conversion to/from other formats
- ✅ Chunked reading for large files

### Usage Examples

```python
from file_utils import tsv_to_parquet, parquet_to_csv

# Convert TSV to Parquet
tsv_to_parquet("input.tsv", "output.parquet")

# Convert Parquet to TSV (with tab delimiter)
parquet_to_csv("input.parquet", "output.tsv", delimiter="\t")
```

---

## JSONL (JSON Lines)

### Overview
JSONL is a format where each line is a valid JSON object. Also known as NDJSON (Newline-Delimited JSON). Ideal for streaming and incremental processing.

### Supported Features
- ✅ Reading and writing
- ✅ Conversion to/from other formats
- ✅ Streaming line-by-line
- ✅ Handling of large files

### Usage Examples

```python
from file_utils import open_any, jsonl_to_parquet, parquet_to_jsonl

# Read JSONL line by line
with open_any("data.jsonl", "rt") as f:
    for line in f:
        record = json.loads(line)
        process(record)

# Convert JSONL to Parquet
jsonl_to_parquet("input.jsonl", "output.parquet")

# Convert Parquet to JSONL
parquet_to_jsonl("input.parquet", "output.jsonl")
```

### File Extensions
- `.jsonl` (preferred)
- `.ndjson`
- `.json` (treated as JSONL in this context)

---

## XML

### Overview
XML (eXtensible Markup Language) is a markup language that defines a set of rules for encoding documents in a format that is both human-readable and machine-readable.

### Supported Features
- ✅ Reading and writing
- ✅ Conversion to/from CSV, JSONL, Parquet
- ✅ Streaming XML parsing
- ✅ Handling of nested structures

### Usage Examples

```python
from file_utils import open_any, xml_to_csv, xml_to_parquet, csv_to_xml

# Read XML
with open_any("data.xml", "rb") as f:
    content = f.read()

# Convert XML to CSV
xml_to_csv("input.xml", "output.csv")

# Convert XML to Parquet
xml_to_parquet("input.xml", "output.parquet")

# Convert CSV to XML
csv_to_xml("input.csv", "output.xml")
```

### Notes
- Uses secure XML parsing (defusedxml) to prevent XML attacks
- Handles nested XML structures by flattening or creating hierarchical data
- Supports XML namespaces

---

## Avro

### Overview
Avro is a binary serialization format developed within Apache's Hadoop project. It uses JSON for defining data types and protocols, and serializes data in a compact binary format.

### Supported Features
- ✅ Reading and writing
- ✅ Conversion to/from other formats
- ✅ Schema preservation
- ✅ Efficient binary format

### Usage Examples

```python
from file_utils import open_any, avro_to_parquet, parquet_to_avro

# Read Avro file
with open_any("data.avro", "rb") as f:
    content = f.read()

# Convert Avro to Parquet
avro_to_parquet("input.avro", "output.parquet")

# Convert Parquet to Avro
parquet_to_avro("input.parquet", "output.avro")
```

### Notes
- Uses fastavro library for efficient Avro operations
- Preserves schema information during conversion
- Binary format (not human-readable)

---

## Arrow IPC (Inter-Process Communication)

### Overview
Arrow IPC is a memory format for representing Arrow data in a language-agnostic way. It enables efficient data exchange between processes and systems without serialization overhead.

### Supported Features
- ✅ Reading and writing
- ✅ Conversion to/from other formats
- ✅ Zero-copy operations when possible
- ✅ Efficient in-memory format

### Usage Examples

```python
from file_utils import open_any, arrow_to_parquet, parquet_to_arrow

# Read Arrow IPC file
with open_any("data.arrow", "rb") as f:
    content = f.read()

# Convert Arrow to Parquet
arrow_to_parquet("input.arrow", "output.parquet")

# Convert Parquet to Arrow
parquet_to_arrow("input.parquet", "output.arrow")
```

### File Extensions
- `.arrow`
- `.ipc`
- `.feather` (Apache Arrow's Feather format)

---

## Parquet

### Overview
Parquet is a columnar storage file format optimized for use with big data processing frameworks. It provides efficient columnar storage with compression and encoding schemes.

### Supported Features
- ✅ Reading and writing
- ✅ Conversion to/from all other formats
- ✅ Columnar storage
- ✅ Compression support (zstd, snappy, gzip)
- ✅ Predicate pushdown
- ✅ Efficient for analytics

### Usage Examples

```python
from file_utils import (
    open_any,
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

# Read Parquet file
with open_any("data.parquet", "rb") as f:
    # Use pyarrow to read
    import pyarrow.parquet as pq
    table = pq.read_table(f)

# Convert CSV to Parquet
csv_to_parquet("input.csv", "output.parquet")

# Convert JSONL to Parquet
jsonl_to_parquet("input.jsonl", "output.parquet")

# Convert Parquet to CSV
parquet_to_csv("input.parquet", "output.csv")

# Convert Parquet to JSONL
parquet_to_jsonl("input.parquet", "output.jsonl")
```

### File Extensions
- `.parquet`
- `.pq` (short form)

### Compression Options
- `zstd` (default) - Good compression with fast read/write
- `snappy` - Very fast compression/decompression
- `gzip` - Higher compression ratio, slower
- `brotli` - High compression ratio
- `lz4` - Fast compression

---

## Format Comparison

| Feature | CSV | TSV | JSONL | XML | Avro | Arrow IPC | Parquet |
|---------|-----|-----|-------|-----|------|-----------|---------|
| Human Readable | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Binary Format | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| Schema Support | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Compression | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| Columnar | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |
| Row-based | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Streaming | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Type Preservation | ⚠️ | ⚠️ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Nesting Support | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Best For | Simple data | Simple data | Incremental | Hierarchical | Hadoop | In-memory | Analytics |

---

## Format Detection

The `detect_format` function automatically identifies the file format based on the file extension:

```python
from file_utils import detect_format

formats = [
    "data.csv",
    "data.tsv", 
    "data.jsonl",
    "data.ndjson",
    "data.json",
    "data.xml",
    "data.avro",
    "data.arrow",
    "data.ipc",
    "data.feather",
    "data.parquet",
    "data.pq"
]

for path in formats:
    fmt = detect_format(path)
    print(f"{path} -> {fmt}")
```

Output:
```
data.csv -> csv
data.tsv -> csv
data.jsonl -> jsonl
data.ndjson -> jsonl
data.json -> jsonl
data.xml -> xml
data.avro -> avro
data.arrow -> arrow
data.ipc -> arrow
data.feather -> arrow
data.parquet -> parquet
data.pq -> parquet
```

---

## Conversion Matrix

You can convert between any of the supported formats:

| From \ To | CSV | TSV | JSONL | XML | Avro | Arrow | Parquet |
|-----------|-----|-----|-------|-----|------|-------|---------|
| **CSV** | - | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **TSV** | ✅ | - | ✅ | ✅ | ✅ | ✅ | ✅ |
| **JSONL** | ✅ | ✅ | - | ✅ | ✅ | ✅ | ✅ |
| **XML** | ✅ | ✅ | ✅ | - | ✅ | ✅ | ✅ |
| **Avro** | ✅ | ✅ | ✅ | ✅ | - | ✅ | ✅ |
| **Arrow** | ✅ | ✅ | ✅ | ✅ | ✅ | - | ✅ |
| **Parquet** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | - |
