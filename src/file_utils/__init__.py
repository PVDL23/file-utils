"""file-utils: Streaming file utilities for CSV, TSV, JSONL, XML, Avro, Arrow IPC, and Parquet."""

from __future__ import annotations

__version__ = "0.1.0"

from file_utils.io import (
    is_s3,
    split_s3,
    open_any,
    exists,
    file_size,
)
from file_utils.convert import (
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
from file_utils.stream import (
    s3_client,
    upload,
    download,
    list_keys,
    object_size,
    delete,
)

__all__ = [
    "__version__",
    # io
    "is_s3",
    "split_s3",
    "open_any",
    "exists",
    "file_size",
    # convert
    "detect_format",
    "convert",
    "csv_to_parquet",
    "tsv_to_parquet",
    "jsonl_to_parquet",
    "avro_to_parquet",
    "arrow_to_parquet",
    "parquet_to_csv",
    "parquet_to_tsv",
    "parquet_to_jsonl",
    "parquet_to_avro",
    "parquet_to_arrow",
    "xml_to_parquet",
    "parquet_to_xml",
    "xml_to_csv",
    "csv_to_xml",
    "xml_to_jsonl",
    "jsonl_to_xml",
    # stream
    "s3_client",
    "upload",
    "download",
    "list_keys",
    "object_size",
    "delete",
]
