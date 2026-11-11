# Agent Instructions for file-utils Package

## Overview

This repository contains the `file-utils` Python package - a streaming file conversion utility for data engineering workflows.

## Package Structure

```
file-utils/
├── pyproject.toml          # Project configuration, dependencies
├── README.md               # Documentation
├── AGENTS.md              # This file - agent instructions
├── src/file_utils/
│   ├── __init__.py         # Package exports
│   ├── py.typed            # Type hints marker
│   ├── io.py               # Uniform local/S3 file access
│   ├── convert.py          # Streaming format conversion (core)
│   ├── stream.py           # boto3 S3 operations
│   └── cli.py              # CLI entry point
└── tests/
    ├── __init__.py
    ├── test_io.py          # Tests for io module
    ├── test_convert.py     # Tests for convert module
    └── test_stream.py      # Tests for stream module
```

## Supported Formats

- **CSV** / **TSV** - Tabular text formats
- **JSONL** - Newline-delimited JSON
- **XML** - Extensible Markup Language
- **Avro** - Binary format with schema
- **Arrow IPC** / **Feather** - Columnar memory format
- **Parquet** - Columnar storage format

## Development Workflow

### Setup

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Import in Python
import file_utils
file_utils.convert("input.csv", "output.parquet")
```

### Testing

All tests are in the `tests/` directory and run with pytest:

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_convert.py
```

Tests use only local temporary files (no S3 required).

### Adding New Features

1. **New format support**:
   - Add dependency to pyproject.toml
   - Update `detect_format()` in convert.py
   - Implement Reader class (inherits BaseReader)
   - Implement Sink class (inherits BaseSink)
   - Update get_reader() and get_sink() factories
   - Add convenience wrappers
   - Add tests

2. **New functionality**:
   - Add to appropriate module
   - Export in __init__.py
   - Add tests
   - Update README.md

### Code Style

- Use type hints with `from __future__ import annotations`
- Follow existing patterns in the codebase
- Keep functions focused and well-documented
- Handle errors gracefully with descriptive messages

## Git Workflow

```bash
# Make changes
# Run tests to verify
uv run pytest

# Commit changes
git add .
git commit -m "Add feature X"

# Push to remote
git push
```

## CLI Usage

```bash
# Convert files
file-utils convert input.csv output.parquet
file-utils convert input.xml output.parquet --record-tag person

# Show file info
file-utils info data.parquet

# S3 operations
file-utils upload local.csv s3://bucket/data.csv
file-utils download s3://bucket/data.csv local.csv
file-utils ls s3://bucket/
```

## Python API Usage

```python
from file_utils import convert, open_any, exists

# Convert between formats
convert("data.csv", "data.parquet")
convert("data.xml", "data.parquet", record_tag="person")

# Uniform IO
with open_any("data.parquet", "rb") as f:
    # work with file
    pass

# Check existence
if exists("data.parquet"):
    # do something
    pass

# Convenience wrappers
from file_utils import csv_to_parquet, parquet_to_jsonl, xml_to_parquet
csv_to_parquet("data.csv", "data.parquet")
```

## Notes

- All conversions stream through Arrow record batches for memory efficiency
- Peak memory usage is one batch (default 65,536 rows)
- Works with local paths and S3 URLs (`s3://bucket/key`)
- For S3, configure with `S3_ENDPOINT` and AWS credentials
