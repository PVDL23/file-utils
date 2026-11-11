# Installation

## Prerequisites

- Python 3.8 or higher
- pip or uv (recommended)

## Install with uv (Recommended)

```bash
# Create a new project
uv init

# Add file-utils as a dependency
uv add git+https://github.com/yourusername/file-utils.git

# Or if you have the package locally
uv add /path/to/file-utils
```

## Install with pip

```bash
# From a local directory
pip install /path/to/file-utils

# Or clone and install
git clone https://github.com/yourusername/file-utils.git
cd file-utils
pip install .
```

## Development Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/file-utils.git
cd file-utils

# Create virtual environment with uv
uv sync

# Or with pip
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

## Dependencies

The package requires the following dependencies:

- **pyarrow** - For Parquet, Arrow IPC, CSV, and TSV operations
- **fastavro** - For Avro format support
- **boto3** - For S3 operations
- **fsspec** - For unified file system access (local, S3, HTTP/HTTPS)
- **defusedxml** - For secure XML parsing

All dependencies are automatically installed when you install file-utils.

## Running Tests

The package uses pytest for testing. With uv:

```bash
# Run all tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_io.py

# Run with coverage
uv run pytest --cov=src/file_utils --cov-report=term-missing
```

With pip:

```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_io.py

# Run with coverage
pytest --cov=src/file_utils --cov-report=term-missing
```

## Configuration

### S3 Configuration

For S3 operations, configure your AWS credentials:

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=your_region

# Optional: Custom S3 endpoint (e.g., for MinIO)
export S3_ENDPOINT=https://your-minio-server:9000
```

Or use the `s3_client` function with explicit parameters:

```python
from file_utils import s3_client

client = s3_client(
    endpoint_url="https://your-minio-server:9000",
    region="us-east-1"
)
```
