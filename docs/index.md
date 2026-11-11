# file-utils Documentation

A Python package for streaming file operations with support for CSV, TSV, JSONL, XML, Avro, Arrow IPC, and Parquet formats. Includes uniform local, S3, and HTTP/HTTPS file access.

## Quick Navigation

- [Installation](installation.md)
- [Quick Start](quickstart.md)
- [File Formats](file-formats.md)
- [API Reference](api-reference.md)
- [Examples](examples.md)

## Features

- **Streaming file operations** - Process large files without loading them entirely into memory
- **Multiple storage backends** - Local filesystem, S3, HTTP/HTTPS
- **Format conversion** - Convert between CSV, TSV, JSONL, XML, Avro, Arrow IPC, and Parquet
- **Efficient I/O** - Use fsspec for unified file access across different storage systems
- **Chunked processing** - Read and write files in chunks for memory efficiency

## Supported Formats

| Format | Read | Write | Conversion |
|--------|------|-------|------------|
| CSV | ✅ | ✅ | ✅ |
| TSV | ✅ | ✅ | ✅ |
| JSONL | ✅ | ✅ | ✅ |
| XML | ✅ | ✅ | ✅ |
| Avro | ✅ | ✅ | ✅ |
| Arrow IPC | ✅ | ✅ | ✅ |
| Parquet | ✅ | ✅ | ✅ |

## Supported Storage

- **Local filesystem** - Standard file paths
- **S3** - Amazon S3 and compatible services (MinIO, etc.)
- **HTTP/HTTPS** - Download files from web URLs
