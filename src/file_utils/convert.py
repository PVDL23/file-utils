"""Streaming format conversion between CSV, TSV, JSONL, XML, Avro, Arrow IPC, and Parquet."""

from __future__ import annotations

import io
import json
import itertools
from pathlib import Path
from typing import Iterator, Any
from datetime import date, datetime, time
from decimal import Decimal
from xml.etree import ElementTree as ET

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import fastavro

from file_utils.io import open_any


# Default settings
DEFAULT_CHUNK_ROWS = 65_536
DEFAULT_ROW_GROUP_ROWS = 1_000_000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_AVRO_CODEC = "deflate"


def detect_format(path: str | Path) -> str:
    """
    Detect format from file extension.
    
    Args:
        path: File path or URL
        
    Returns:
        Format name: 'csv', 'jsonl', 'xml', 'avro', 'parquet', or 'arrow'
        
    Raises:
        ValueError: If extension is unknown
    """
    path_str = str(path)
    suffixes = Path(path_str).suffixes
    
    # Check for compound extensions
    if len(suffixes) >= 2:
        double_suffix = (suffixes[-2] + suffixes[-1]).lower()
        if double_suffix == ".ndjson":
            return "jsonl"
    
    suffix = suffixes[-1].lower() if suffixes else ""
    
    if suffix in (".jsonl",):
        return "jsonl"
    elif suffix in (".json",):
        # Check if it's actually ndjson
        stem_suffix = Path(path_str).stem.lower()
        if stem_suffix.endswith(".ndjson") or (len(suffixes) >= 2 and suffixes[-2].lower() == ".nd"):
            return "jsonl"
        return "jsonl"  # Also treat plain .json as jsonl for this use case
    elif suffix in (".ndjson",):
        return "jsonl"
    elif suffix in (".xml",):
        return "xml"
    elif suffix in (".csv",):
        return "csv"
    elif suffix in (".tsv",):
        return "csv"  # TSV is handled as CSV with tab delimiter
    elif suffix in (".avro",):
        return "avro"
    elif suffix in (".parquet", ".pq"):
        return "parquet"
    elif suffix in (".arrow", ".ipc", ".feather"):
        return "arrow"
    else:
        raise ValueError(f"Unknown file extension: {suffix}")


# ============================================================================
# Avro <-> Arrow Schema Mapping
# ============================================================================

def _arrow_type_to_avro_type(arrow_type: pa.DataType) -> dict | str:
    """Convert Arrow type to Avro type (dict for complex types, str for primitives)."""
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    elif pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return "int"
    elif pa.types.is_int64(arrow_type) or pa.types.is_uint8(arrow_type) or \
         pa.types.is_uint16(arrow_type) or pa.types.is_uint32(arrow_type) or pa.types.is_uint64(arrow_type):
        return "long"
    elif pa.types.is_float32(arrow_type):
        return "float"
    elif pa.types.is_float64(arrow_type):
        return "double"
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "bytes"
    elif pa.types.is_fixed_size_binary(arrow_type):
        return "bytes"
    elif pa.types.is_date32(arrow_type):
        return {"type": "int", "logicalType": "date"}
    elif pa.types.is_time32(arrow_type):
        return {"type": "int", "logicalType": "time-millis"}
    elif pa.types.is_time64(arrow_type):
        return {"type": "long", "logicalType": "time-micros"}
    elif pa.types.is_timestamp(arrow_type):
        if arrow_type.unit in ("s", "ms"):
            return {"type": "long", "logicalType": "timestamp-millis"}
        elif arrow_type.unit in ("us", "ns"):
            # ns is lossy, convert to micros
            return {"type": "long", "logicalType": "timestamp-micros"}
    elif pa.types.is_decimal(arrow_type):
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": arrow_type.precision,
            "scale": arrow_type.scale,
        }
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        items = _arrow_type_to_avro_type(arrow_type.value_type)
        return {"type": "array", "items": items}
    elif pa.types.is_fixed_size_list(arrow_type):
        items = _arrow_type_to_avro_type(arrow_type.value_type)
        return {"type": "array", "items": items}
    elif pa.types.is_struct(arrow_type):
        fields = []
        for field in arrow_type:
            field_type = _arrow_type_to_avro_type(field.type)
            fields.append({"name": field.name, "type": field_type})
        return {"type": "record", "name": f"{arrow_type}_struct", "fields": fields}
    elif pa.types.is_map(arrow_type):
        values = _arrow_type_to_avro_type(arrow_type.value_type)
        return {"type": "map", "values": values}
    elif pa.types.is_null(arrow_type):
        return "null"
    else:
        raise TypeError(f"Unsupported Arrow type for Avro conversion: {arrow_type}")


def _arrow_field_to_avro_field(arrow_field: pa.Field) -> dict:
    """Convert Arrow Field to Avro field schema."""
    avro_type = _arrow_type_to_avro_type(arrow_field.type)
    
    if arrow_field.nullable:
        # Create union with null
        avro_type = ["null", avro_type]
    
    result = {"name": arrow_field.name, "type": avro_type}
    
    if arrow_field.nullable:
        result["default"] = None
    
    return result


def arrow_schema_to_avro_schema(arrow_schema: pa.Schema) -> dict:
    """Convert Arrow Schema to Avro schema dict."""
    fields = []
    for field in arrow_schema:
        fields.append(_arrow_field_to_avro_field(field))
    
    return {
        "type": "record",
        "name": "root",
        "fields": fields,
    }


def _avro_type_to_arrow_type(avro_type: dict | str, logical_types: dict | None = None) -> pa.DataType:
    """Convert Avro type to Arrow type."""
    if isinstance(avro_type, str):
        type_str = avro_type
        if type_str == "boolean":
            return pa.bool_()
        elif type_str == "int":
            return pa.int32()
        elif type_str == "long":
            return pa.int64()
        elif type_str == "float":
            return pa.float32()
        elif type_str == "double":
            return pa.float64()
        elif type_str == "string":
            return pa.string()
        elif type_str == "bytes":
            return pa.binary()
        elif type_str == "null":
            return pa.null()
        else:
            raise TypeError(f"Unsupported primitive Avro type: {type_str}")
    
    # Dict type
    type_name = avro_type.get("type")
    
    if type_name == "array":
        items = avro_type.get("items")
        if items is None:
            raise TypeError("Array type missing 'items' field")
        value_type = _avro_type_to_arrow_type(items, logical_types)
        return pa.list_(value_type)
    
    elif type_name == "map":
        values = avro_type.get("values")
        if values is None:
            raise TypeError("Map type missing 'values' field")
        value_type = _avro_type_to_arrow_type(values, logical_types)
        return pa.map_(value_type)
    
    elif type_name == "record":
        fields = avro_type.get("fields", [])
        arrow_fields = []
        for field in fields:
            field_type = field.get("type")
            if field_type is None:
                raise TypeError("Record field missing 'type'")
            arrow_type = _avro_type_to_arrow_type(field_type, logical_types)
            # Handle nullable fields (union with null)
            is_nullable = False
            if isinstance(field_type, list):
                # Check if it's a union with null
                types_in_union = [t if isinstance(t, str) else t.get("type") for t in field_type]
                if "null" in types_in_union:
                    is_nullable = True
                    # Get the non-null type
                    for t in field_type:
                        if t != "null":
                            arrow_type = _avro_type_to_arrow_type(t, logical_types)
                            break
            
            arrow_field = pa.field(field["name"], arrow_type, nullable=is_nullable)
            arrow_fields.append(arrow_field)
        return pa.struct(arrow_fields)
    
    elif type_name == "fixed":
        size = avro_type.get("size", 0)
        return pa.fixed_size_binary(size)
    
    elif type_name == "int" and avro_type.get("logicalType") == "date":
        return pa.date32()
    
    elif type_name == "int" and avro_type.get("logicalType") == "time-millis":
        return pa.time32("ms")
    
    elif type_name == "long" and avro_type.get("logicalType") == "time-micros":
        return pa.time64("us")
    
    elif type_name == "long" and avro_type.get("logicalType") == "timestamp-millis":
        return pa.timestamp("ms")
    
    elif type_name == "long" and avro_type.get("logicalType") == "timestamp-micros":
        return pa.timestamp("us")
    
    elif type_name == "bytes" and avro_type.get("logicalType") == "decimal":
        precision = avro_type.get("precision", 256)
        scale = avro_type.get("scale", 0)
        return pa.decimal128(precision, scale)
    
    elif type_name == "string" and avro_type.get("logicalType") == "uuid":
        return pa.string()
    
    else:
        raise TypeError(f"Unsupported Avro type: {avro_type}")


def avro_schema_to_arrow_schema(avro_schema: dict) -> pa.Schema:
    """Convert Avro schema dict to Arrow Schema."""
    # Handle named types at the top level
    if avro_schema.get("type") == "record":
        fields = []
        for field_dict in avro_schema.get("fields", []):
            field_name = field_dict.get("name")
            field_type = field_dict.get("type")
            nullable = field_dict.get("nullable", False)
            
            # Check if it's a union type
            is_nullable = False
            if isinstance(field_type, list):
                types_in_union = [t if isinstance(t, str) else t.get("type") for t in field_type]
                if "null" in types_in_union:
                    is_nullable = True
                    # Get the non-null type
                    for t in field_type:
                        if t != "null":
                            arrow_type = _avro_type_to_arrow_type(t)
                            fields.append(pa.field(field_name, arrow_type, nullable=True))
                            break
                    continue
            
            if is_nullable or nullable:
                arrow_type = _avro_type_to_arrow_type(field_type)
                fields.append(pa.field(field_name, arrow_type, nullable=True))
            else:
                arrow_type = _avro_type_to_arrow_type(field_type)
                fields.append(pa.field(field_name, arrow_type))
        
        return pa.schema(fields)
    else:
        raise ValueError(f"Top-level Avro schema must be a record, got: {avro_schema.get('type')}")


# ============================================================================
# JSON serialization helpers
# ============================================================================

def _json_default(o: Any) -> Any:
    """JSON encoder default for datetime, date, time, and Decimal."""
    if isinstance(o, datetime):
        return o.isoformat()
    elif isinstance(o, date):
        return o.isoformat()
    elif isinstance(o, time):
        return o.isoformat()
    elif isinstance(o, Decimal):
        return str(o)
    elif isinstance(o, bytes):
        return o.hex()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


# ============================================================================
# Reader Classes
# ============================================================================

class BaseReader:
    """Base class for all readers."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
        self.path = path
        self.chunk_rows = chunk_rows
        self._closed = False
    
    @property
    def schema(self) -> pa.Schema:
        raise NotImplementedError
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        raise NotImplementedError
    
    def close(self) -> None:
        self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CSVReader(BaseReader):
    """Reader for CSV and TSV files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS, delimiter: str | None = None):
        super().__init__(path, chunk_rows)
        self.delimiter = delimiter
        self._reader = None
        self._schema = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._open()
        return self._schema
    
    def _open(self):
        if self._reader is not None:
            return
        
        # Determine delimiter
        path_str = str(self.path)
        if self.delimiter is None:
            if Path(path_str).suffix.lower() == ".tsv":
                self.delimiter = "\t"
            else:
                self.delimiter = ","
        
        file_obj = open_any(self.path, "rb")
        parse_options = csv.ParseOptions(delimiter=self.delimiter)
        read_options = csv.ReadOptions()
        
        self._reader = csv.open_csv(
            file_obj,
            parse_options=parse_options,
            read_options=read_options,
        )
        self._schema = self._reader.schema
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open()
        for batch in self._reader:
            yield batch
    
    def close(self) -> None:
        if self._reader is not None:
            self._reader.close()
            self._reader = None
        self._closed = True


class JSONLReader(BaseReader):
    """Reader for JSONL (newline-delimited JSON) files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS, schema: pa.Schema | None = None):
        super().__init__(path, chunk_rows)
        self._user_schema = schema
        self._file = None
        self._schema = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._open_and_infer()
        return self._schema
    
    def _open_and_infer(self):
        if self._file is not None:
            return
        
        self._file = open_any(self.path, "rt", encoding="utf-8")
        
        if self._user_schema is not None:
            self._schema = self._user_schema
            return
        
        # Read first chunk to infer schema
        chunk = []
        for _ in range(self.chunk_rows):
            line = self._file.readline()
            if not line:
                break
            line = line.strip()
            if line:
                chunk.append(json.loads(line))
        
        if not chunk:
            raise ValueError("Empty JSONL file, cannot infer schema")
        
        # Infer schema from first chunk
        self._schema = pa.schema([
            pa.field(k, _infer_json_type(v)) for k, v in chunk[0].items()
        ])
        
        # Rewind - we need to put the chunk back
        # Since we can't rewind a stream easily, we'll buffer the chunk
        self._buffered_chunk = chunk
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open_and_infer()
        
        # Yield buffered chunk first
        if hasattr(self, '_buffered_chunk') and self._buffered_chunk:
            batch = pa.RecordBatch.from_pylist(self._buffered_chunk, schema=self._schema)
            yield batch
            delattr(self, '_buffered_chunk')
        
        # Continue reading
        chunk = []
        for line in self._file:
            line = line.strip()
            if line:
                chunk.append(json.loads(line))
                if len(chunk) >= self.chunk_rows:
                    batch = pa.RecordBatch.from_pylist(chunk, schema=self._schema)
                    yield batch
                    chunk = []
        
        if chunk:
            batch = pa.RecordBatch.from_pylist(chunk, schema=self._schema)
            yield batch
    
    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


def _infer_json_type(value: Any) -> pa.DataType:
    """Infer Arrow type from JSON value."""
    if value is None:
        return pa.null()
    elif isinstance(value, bool):
        return pa.bool_()
    elif isinstance(value, int):
        return pa.int64()
    elif isinstance(value, float):
        return pa.float64()
    elif isinstance(value, str):
        return pa.string()
    elif isinstance(value, list):
        if value:
            element_type = _infer_json_type(value[0])
        else:
            element_type = pa.null()
        return pa.list_(element_type)
    elif isinstance(value, dict):
        if value:
            fields = [pa.field(k, _infer_json_type(v)) for k, v in value.items()]
            return pa.struct(fields)
        else:
            return pa.struct([])
    else:
        return pa.string()


class XMLReader(BaseReader):
    """Reader for XML files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS, 
                 record_tag: str | None = None):
        super().__init__(path, chunk_rows)
        self.record_tag = record_tag  # Optional: specify which tag contains records
        self._file = None
        self._root = None
        self._schema = None
        self._iterator = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._open()
        return self._schema
    
    def _open(self):
        if self._file is not None:
            return
        
        self._file = open_any(self.path, "rb")
        
        # Parse XML to get root element
        tree = ET.parse(self._file)
        self._root = tree.getroot()
        
        # Find first record to infer schema
        records = self._get_records()
        
        if not records:
            raise ValueError("No records found in XML file")
        
        # Convert first record to dict and infer schema
        first_record = self._xml_to_dict(next(records))
        
        # Infer schema from first record
        self._schema = pa.schema([
            pa.field(k, self._infer_arrow_type(v)) for k, v in first_record.items()
        ])
        
        # Re-create iterator for batches()
        self._file.seek(0)
        self._iterator = None
    
    def _get_records(self):
        """Iterate over record elements in XML."""
        # If record_tag is specified, find all elements with that tag
        if self.record_tag:
            for elem in self._root.findall(self.record_tag):
                yield elem
        else:
            # Default: all direct children of root are records
            for elem in self._root:
                yield elem
    
    def _xml_to_dict(self, element: ET.Element) -> dict:
        """Convert an XML element to a dict."""
        result = {}
        
        # Add attributes
        for key, value in element.attrib.items():
            result[key] = value
        
        # Add child elements
        for child in element:
            child_data = self._xml_to_dict(child)
            
            # Handle multiple children with same tag
            if child.tag in result:
                # Convert to list if not already
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                # Single child: store the dict directly
                if child_data:
                    result[child.tag] = child_data
                else:
                    # Leaf element: store text content
                    result[child.tag] = child.text or ""
        
        # If element has text but no children or attributes
        if not element.attrib and len(element) == 0:
            text = element.text
            if text:
                # If we already have this tag from above, don't override
                if not result:
                    return text
        
        return result
    
    def _infer_arrow_type(self, value: Any) -> pa.DataType:
        """Infer Arrow type from a value."""
        if value is None:
            return pa.null()
        elif isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, str):
            return pa.string()
        elif isinstance(value, list):
            if value:
                element_type = self._infer_arrow_type(value[0])
            else:
                element_type = pa.null()
            return pa.list_(element_type)
        elif isinstance(value, dict):
            if value:
                fields = [pa.field(k, self._infer_arrow_type(v)) for k, v in value.items()]
                return pa.struct(fields)
            else:
                return pa.struct([])
        else:
            return pa.string()
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open()
        
        if self._iterator is None:
            self._file.seek(0)
            tree = ET.parse(self._file)
            self._root = tree.getroot()
            self._iterator = self._get_records()
        
        chunk = []
        for elem in self._iterator:
            record = self._xml_to_dict(elem)
            chunk.append(record)
            if len(chunk) >= self.chunk_rows:
                batch = pa.RecordBatch.from_pylist(chunk, schema=self._schema)
                yield batch
                chunk = []
        
        if chunk:
            batch = pa.RecordBatch.from_pylist(chunk, schema=self._schema)
            yield batch
    
    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class AvroReader(BaseReader):
    """Reader for Avro files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
        super().__init__(path, chunk_rows)
        self._file = None
        self._avro_reader = None
        self._arrow_schema = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._arrow_schema is None:
            self._open()
        return self._arrow_schema
    
    def _open(self):
        if self._avro_reader is not None:
            return
        
        self._file = open_any(self.path, "rb")
        self._avro_reader = fastavro.reader(self._file)
        
        # Get writer schema
        writer_schema = self._avro_reader.writer_schema
        if writer_schema is None:
            # Try reader schema
            writer_schema = self._avro_reader.reader_schema
        
        if writer_schema is None:
            raise ValueError("Cannot determine Avro schema from file")
        
        self._arrow_schema = avro_schema_to_arrow_schema(writer_schema)
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open()
        
        chunk = []
        for record in self._avro_reader:
            chunk.append(record)
            if len(chunk) >= self.chunk_rows:
                batch = pa.RecordBatch.from_pylist(chunk, schema=self._arrow_schema)
                yield batch
                chunk = []
        
        if chunk:
            batch = pa.RecordBatch.from_pylist(chunk, schema=self._arrow_schema)
            yield batch
    
    def close(self) -> None:
        if self._avro_reader is not None:
            self._avro_reader = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class ParquetReader(BaseReader):
    """Reader for Parquet files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
        super().__init__(path, chunk_rows)
        self._file = None
        self._reader = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._reader is None:
            self._open()
        return self._reader.schema_arrow
    
    def _open(self):
        if self._reader is not None:
            return
        
        self._file = open_any(self.path, "rb")
        self._reader = pq.ParquetFile(self._file)
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open()
        for batch in self._reader.iter_batches(batch_size=self.chunk_rows):
            yield batch
    
    def close(self) -> None:
        if self._reader is not None:
            self._reader = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class ArrowReader(BaseReader):
    """Reader for Arrow IPC files."""
    
    def __init__(self, path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
        super().__init__(path, chunk_rows)
        self._file = None
        self._reader = None
    
    @property
    def schema(self) -> pa.Schema:
        if self._reader is None:
            self._open()
        return self._reader.schema
    
    def _open(self):
        if self._reader is not None:
            return
        
        self._file = open_any(self.path, "rb")
        try:
            self._reader = ipc.open_file(self._file)
        except (ValueError, pa.ArrowIOError):
            # Try streaming format
            self._file.seek(0)
            self._reader = ipc.open_stream(self._file)
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        self._open()
        for i in range(self._reader.num_record_batches):
            yield self._reader.get_batch(i)
    
    def close(self) -> None:
        if self._reader is not None:
            self._reader = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


# ============================================================================
# Sink Classes
# ============================================================================

class BaseSink:
    """Base class for all sinks."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, **kwargs):
        self.path = path
        self.schema = schema
        self.kwargs = kwargs
        self._closed = False
        self._first_batch = True
    
    def write(self, batch: pa.RecordBatch) -> None:
        raise NotImplementedError
    
    def close(self) -> None:
        self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ParquetSink(BaseSink):
    """Sink for Parquet files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, 
                 compression: str = DEFAULT_COMPRESSION, 
                 row_group_rows: int = DEFAULT_ROW_GROUP_ROWS, **kwargs):
        super().__init__(path, schema, **kwargs)
        self.compression = compression
        self.row_group_rows = row_group_rows
        self._writer = None
        self._file = None
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._writer is None:
            self._file = open_any(self.path, "wb")
            self._writer = pq.ParquetWriter(
                self._file,
                self.schema,
                compression=self.compression,
            )
        
        self._writer.write_batch(batch, row_group_size=self.row_group_rows)
    
    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class ArrowSink(BaseSink):
    """Sink for Arrow IPC files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, **kwargs):
        super().__init__(path, schema, **kwargs)
        self._writer = None
        self._file = None
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._writer is None:
            self._file = open_any(self.path, "wb")
            self._writer = ipc.new_file(self._file, self.schema)
        
        self._writer.write_batch(batch)
    
    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class CSVSink(BaseSink):
    """Sink for CSV and TSV files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, 
                 delimiter: str | None = None, **kwargs):
        super().__init__(path, schema, **kwargs)
        # Determine delimiter
        path_str = str(path)
        if delimiter is None:
            if Path(path_str).suffix.lower() == ".tsv":
                self.delimiter = "\t"
            else:
                self.delimiter = ","
        else:
            self.delimiter = delimiter
        self._file = None
        self._first_batch = True
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._file is None:
            self._file = open_any(self.path, "wb")
        
        # Convert batch to table
        table = pa.Table.from_batches([batch])
        
        # Write CSV
        buf = io.BytesIO()
        write_options = csv.WriteOptions(
            include_header=self._first_batch,
            delimiter=self.delimiter,
            quoting_style="none",
            quoting_header="none",
        )
        csv.write_csv(table, buf, write_options=write_options)
        
        self._file.write(buf.getvalue())
        self._first_batch = False
    
    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class JSONLSink(BaseSink):
    """Sink for JSONL files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, **kwargs):
        super().__init__(path, schema, **kwargs)
        self._file = None
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._file is None:
            self._file = open_any(self.path, "wt", encoding="utf-8")
        
        # Convert batch to list of dicts
        import pyarrow.compute as pc
        
        # Use pandas-like to_pylist which handles nested data
        rows = batch.to_pylist()
        
        for row in rows:
            line = json.dumps(row, default=_json_default) + "\n"
            self._file.write(line)
    
    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class AvroSink(BaseSink):
    """Sink for Avro files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, 
                 codec: str = DEFAULT_AVRO_CODEC, **kwargs):
        super().__init__(path, schema, **kwargs)
        self.codec = codec
        self._writer = None
        self._file = None
        self._avro_schema = None
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._writer is None:
            self._file = open_any(self.path, "wb")
            # Convert Arrow schema to Avro schema
            self._avro_schema = arrow_schema_to_avro_schema(self.schema)
            
            # Build codec
            if self.codec == "deflate":
                codec_obj = "deflate"
            elif self.codec == "null":
                codec_obj = None
            elif self.codec == "snappy":
                codec_obj = "snappy"
            elif self.codec == "zstandard":
                codec_obj = "zstandard"
            else:
                raise ValueError(f"Unsupported Avro codec: {self.codec}")
            
            self._writer = fastavro.write.Writer(
                self._file,
                self._avro_schema,
                codec=codec_obj,
            )
        
        # Convert batch to list of dicts
        rows = batch.to_pylist()
        
        # Write all rows
        # Note: fastavro.writer.Writer expects an iterator, but we write batch by batch
        # We need to use the write_records method or write them one by one
        for row in rows:
            self._writer.write(row)
    
    def close(self) -> None:
        if self._writer is not None:
            # Flush and close the writer
            self._writer.flush()
            self._writer = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


class XMLSink(BaseSink):
    """Sink for XML files."""
    
    def __init__(self, path: str | Path, schema: pa.Schema, 
                 record_tag: str = "record", root_tag: str = "root", **kwargs):
        super().__init__(path, schema, **kwargs)
        self.record_tag = record_tag
        self.root_tag = root_tag
        self._file = None
        self._writer = None
        self._first_batch = True
    
    def _dict_to_xml(self, data: dict, parent: ET.Element | None = None) -> ET.Element:
        """Convert a dict to an XML element."""
        if parent is None:
            elem = ET.Element(self.record_tag)
        else:
            elem = ET.SubElement(parent, self.record_tag)
        
        for key, value in data.items():
            if isinstance(value, dict):
                # Nested dict: create child element
                child = ET.SubElement(elem, key)
                self._dict_to_xml(value, child)
            elif isinstance(value, list):
                # List: create multiple child elements
                for item in value:
                    if isinstance(item, dict):
                        child = ET.SubElement(elem, key)
                        self._dict_to_xml(item, child)
                    else:
                        child = ET.SubElement(elem, key)
                        child.text = str(item) if item is not None else ""
            else:
                # Simple value: create child element with text
                child = ET.SubElement(elem, key)
                child.text = str(value) if value is not None else ""
        
        return elem
    
    def write(self, batch: pa.RecordBatch) -> None:
        if self._file is None:
            self._file = open_any(self.path, "wb")
            # Write XML header
            self._file.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            # Create root element
            self._writer = ET.Element(self.root_tag)
            self._first_batch = False
        
        # Convert batch to list of dicts
        rows = batch.to_pylist()
        
        for row in rows:
            self._dict_to_xml(row, self._writer)
    
    def close(self) -> None:
        if self._writer is not None:
            # Write the XML tree to file
            tree = ET.ElementTree(self._writer)
            tree.write(self._file, encoding="utf-8", xml_declaration=False)
            self._writer = None
        if self._file is not None:
            self._file.close()
            self._file = None
        self._closed = True


# ============================================================================
# Reader/Sink Factory
# ============================================================================

def get_reader(path: str | Path, chunk_rows: int = DEFAULT_CHUNK_ROWS, 
               delimiter: str | None = None, json_schema: pa.Schema | None = None, 
               record_tag: str | None = None) -> BaseReader:
    """Get a reader for the given path based on format detection."""
    fmt = detect_format(path)
    
    if fmt == "csv":
        return CSVReader(path, chunk_rows=chunk_rows, delimiter=delimiter)
    elif fmt == "jsonl":
        return JSONLReader(path, chunk_rows=chunk_rows, schema=json_schema)
    elif fmt == "xml":
        return XMLReader(path, chunk_rows=chunk_rows, record_tag=record_tag)
    elif fmt == "avro":
        return AvroReader(path, chunk_rows=chunk_rows)
    elif fmt == "parquet":
        return ParquetReader(path, chunk_rows=chunk_rows)
    elif fmt == "arrow":
        return ArrowReader(path, chunk_rows=chunk_rows)
    else:
        raise ValueError(f"Unknown format: {fmt}")


def get_sink(path: str | Path, schema: pa.Schema, 
             compression: str = DEFAULT_COMPRESSION,
             avro_codec: str = DEFAULT_AVRO_CODEC,
             row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
             delimiter: str | None = None, 
             record_tag: str = "record", root_tag: str = "root") -> BaseSink:
    """Get a sink for the given path based on format detection."""
    fmt = detect_format(path)
    
    if fmt == "csv":
        return CSVSink(path, schema, delimiter=delimiter, compression=compression)
    elif fmt == "jsonl":
        return JSONLSink(path, schema)
    elif fmt == "xml":
        return XMLSink(path, schema, record_tag=record_tag, root_tag=root_tag)
    elif fmt == "avro":
        return AvroSink(path, schema, codec=avro_codec)
    elif fmt == "parquet":
        return ParquetSink(path, schema, compression=compression, row_group_rows=row_group_rows)
    elif fmt == "arrow":
        return ArrowSink(path, schema)
    else:
        raise ValueError(f"Unknown format: {fmt}")


# ============================================================================
# Main convert function
# ============================================================================

def convert(src: str | Path, dst: str | Path, *,
            compression: str = DEFAULT_COMPRESSION,
            avro_codec: str = DEFAULT_AVRO_CODEC,
            chunk_rows: int = DEFAULT_CHUNK_ROWS,
            row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
            delimiter: str | None = None,
            json_schema: pa.Schema | None = None,
            record_tag: str | None = None,
            root_tag: str = "root") -> str | Path:
    """
    Convert between file formats with streaming.
    
    Args:
        src: Source path (local or s3://)
        dst: Destination path (local or s3://)
        compression: Compression for Parquet (default: zstd)
        avro_codec: Codec for Avro (default: deflate)
        chunk_rows: Number of rows per batch (default: 65536)
        row_group_rows: Number of rows per row group for Parquet (default: 1000000)
        delimiter: Delimiter for CSV/TSV (auto-detected from extension if None)
        json_schema: Optional schema for JSONL reading
        record_tag: XML tag name for records (for XML reading/writing, default: None for reading, "record" for writing)
        root_tag: XML root tag name (for XML writing, default: "root")
        
    Returns:
        The destination path
        
    Raises:
        ValueError: If format is unknown or other errors
    """
    reader = None
    sink = None
    
    try:
        # Get reader and detect source format
        src_fmt = detect_format(src)
        reader = get_reader(src, chunk_rows=chunk_rows, delimiter=delimiter, json_schema=json_schema, record_tag=record_tag)
        
        # Get the schema from the first batch
        # For some readers, we can get schema without reading
        schema = reader.schema
        
        # Special case: JSONL without user schema needs to read first batch
        # But we already handle that in JSONLReader
        
        # Get sink
        sink = get_sink(
            dst, schema,
            compression=compression,
            avro_codec=avro_codec,
            row_group_rows=row_group_rows,
            delimiter=delimiter,
            record_tag=record_tag or "record",
            root_tag=root_tag,
        )
        
        # Stream batches
        first_batch = True
        for batch in reader.batches():
            if first_batch:
                # For JSONL, we need to ensure schema is set
                # The reader should have already inferred it
                first_batch = False
            sink.write(batch)
        
        # Handle empty source
        # If reader already knows schema (parquet/arrow/avro/csv), write empty output
        # For JSONL, we need at least one row to infer schema
        
    finally:
        if reader is not None:
            reader.close()
        if sink is not None:
            sink.close()
    
    return dst


# ============================================================================
# Convenience wrappers
# ============================================================================

def csv_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert CSV to Parquet."""
    return convert(src, dst, **kwargs)


def tsv_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert TSV to Parquet."""
    return convert(src, dst, **kwargs)


def jsonl_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert JSONL to Parquet."""
    return convert(src, dst, **kwargs)


def avro_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Avro to Parquet."""
    return convert(src, dst, **kwargs)


def arrow_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Arrow IPC to Parquet."""
    return convert(src, dst, **kwargs)


def parquet_to_csv(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to CSV."""
    return convert(src, dst, **kwargs)


def parquet_to_tsv(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to TSV."""
    return convert(src, dst, **kwargs)


def parquet_to_jsonl(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to JSONL."""
    return convert(src, dst, **kwargs)


def parquet_to_avro(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to Avro."""
    return convert(src, dst, **kwargs)


def parquet_to_arrow(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to Arrow IPC."""
    return convert(src, dst, **kwargs)


def xml_to_parquet(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert XML to Parquet."""
    return convert(src, dst, **kwargs)


def parquet_to_xml(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert Parquet to XML."""
    return convert(src, dst, **kwargs)


def xml_to_csv(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert XML to CSV."""
    return convert(src, dst, **kwargs)


def csv_to_xml(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert CSV to XML."""
    return convert(src, dst, **kwargs)


def xml_to_jsonl(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert XML to JSONL."""
    return convert(src, dst, **kwargs)


def jsonl_to_xml(src: str | Path, dst: str | Path, **kwargs) -> str | Path:
    """Convert JSONL to XML."""
    return convert(src, dst, **kwargs)
