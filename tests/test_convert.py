"""Tests for file_utils.convert module."""

import json
import pytest
from datetime import datetime, date, time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import fastavro

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
    arrow_schema_to_avro_schema,
    avro_schema_to_arrow_schema,
)


class TestDetectFormat:
    """Tests for detect_format function."""
    
    def test_csv(self):
        assert detect_format("data.csv") == "csv"
    
    def test_tsv(self):
        assert detect_format("data.tsv") == "csv"  # TSV is CSV with tab delimiter
    
    def test_jsonl(self):
        assert detect_format("data.jsonl") == "jsonl"
        assert detect_format("data.ndjson") == "jsonl"
    
    def test_json(self):
        assert detect_format("data.json") == "jsonl"
    
    def test_xml(self):
        assert detect_format("data.xml") == "xml"
    
    def test_avro(self):
        assert detect_format("data.avro") == "avro"
    
    def test_parquet(self):
        assert detect_format("data.parquet") == "parquet"
        assert detect_format("data.pq") == "parquet"
    
    def test_arrow(self):
        assert detect_format("data.arrow") == "arrow"
        assert detect_format("data.ipc") == "arrow"
        assert detect_format("data.feather") == "arrow"
    
    def test_unknown_extension(self):
        with pytest.raises(ValueError, match="Unknown file extension"):
            detect_format("data.xlsx")


class TestCSVToParquet:
    """Tests for CSV to Parquet conversion."""
    
    def test_csv_to_parquet_basic(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
        
        parquet_file = tmp_path / "test.parquet"
        convert(str(csv_file), str(parquet_file))
        
        assert parquet_file.exists()
        
        # Verify content
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["Alice", "Bob"]
    
    def test_csv_to_parquet_zstd_compression(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n")
        
        parquet_file = tmp_path / "test.parquet"
        convert(str(csv_file), str(parquet_file), compression="zstd")
        
        # Verify compression
        pf = pq.ParquetFile(str(parquet_file))
        compression = pf.metadata.row_group(0).column(0).compression
        assert compression == "ZSTD"


class TestJSONLToParquet:
    """Tests for JSONL to Parquet conversion."""
    
    def test_jsonl_to_parquet_nested(self, tmp_path):
        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text(
            '{"id": 1, "tags": ["a", "b"], "meta": {"x": 1}}\n'
            '{"id": 2, "tags": ["c"], "meta": {"x": 2}}\n'
        )
        
        parquet_file = tmp_path / "test.parquet"
        convert(str(jsonl_file), str(parquet_file))
        
        # Verify content
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2
        
        # Check nested data
        import pyarrow.compute as pc
        ids = table.column("id").to_pylist()
        assert ids == [1, 2]


class TestAvroToParquet:
    """Tests for Avro to Parquet conversion."""
    
    def test_avro_to_parquet_schema_types(self, tmp_path):
        avro_file = tmp_path / "test.avro"
        
        # Create Avro schema with various types
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "string_field", "type": "string"},
                {"name": "long_field", "type": "long"},
                {"name": "double_field", "type": "double"},
                {"name": "boolean_field", "type": "boolean"},
                {"name": "timestamp_field", "type": "long", "logicalType": "timestamp-micros"},
                {"name": "array_field", "type": {"type": "array", "items": "string"}},
                {"name": "nullable_field", "type": ["null", "string"], "default": None},
            ],
        }
        
        records = [
            {
                "string_field": "hello",
                "long_field": 42,
                "double_field": 3.14,
                "boolean_field": True,
                "timestamp_field": 1700000000000,  # micros
                "array_field": ["a", "b"],
                "nullable_field": "value",
            },
            {
                "string_field": "world",
                "long_field": 99,
                "double_field": 2.71,
                "boolean_field": False,
                "timestamp_field": 1700000001000,
                "array_field": ["c"],
                "nullable_field": None,
            },
        ]
        
        # Write Avro file
        with open(avro_file, "wb") as f:
            fastavro.writer(f, avro_schema, records)
        
        # Convert to Parquet
        parquet_file = tmp_path / "test.parquet"
        convert(str(avro_file), str(parquet_file))
        
        assert parquet_file.exists()
        
        # Verify content
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2


class TestArrowToParquet:
    """Tests for Arrow IPC to Parquet conversion."""
    
    def test_arrow_to_parquet_roundtrip(self, tmp_path):
        arrow_file = tmp_path / "test.arrow"
        
        # Create Arrow file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64()),
        ])
        batch = pa.record_batch([
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([1.5, 2.5, 3.5], type=pa.float64()),
        ], schema=schema)
        
        with pa.OSFile(str(arrow_file), "wb") as f:
            with pa.ipc.new_file(f, schema) as writer:
                writer.write_batch(batch)
        
        # Convert to Parquet
        parquet_file = tmp_path / "test.parquet"
        convert(str(arrow_file), str(parquet_file))
        
        assert parquet_file.exists()
        
        # Read back and verify
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 3


class TestParquetToCSV:
    """Tests for Parquet to CSV conversion."""
    
    def test_parquet_to_csv(self, tmp_path):
        # Create Parquet file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }, schema=schema)
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to CSV
        csv_file = tmp_path / "test.csv"
        convert(str(parquet_file), str(csv_file))
        
        assert csv_file.exists()
        content = csv_file.read_text()
        assert "id,name" in content
        assert "1,Alice" in content


class TestParquetToTSV:
    """Tests for Parquet to TSV conversion."""
    
    def test_parquet_to_tsv(self, tmp_path):
        # Create Parquet file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        table = pa.table({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }, schema=schema)
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to TSV
        tsv_file = tmp_path / "test.tsv"
        convert(str(parquet_file), str(tsv_file))
        
        assert tsv_file.exists()
        content = tsv_file.read_text()
        # TSV uses tab delimiter
        assert "id\tname" in content


class TestParquetToJSONL:
    """Tests for Parquet to JSONL conversion."""
    
    def test_parquet_to_jsonl(self, tmp_path):
        # Create Parquet file with nested data
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("tags", pa.list_(pa.string())),
        ])
        table = pa.table({
            "id": [1, 2],
            "tags": [["a", "b"], ["c"]],
        }, schema=schema)
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to JSONL
        jsonl_file = tmp_path / "test.jsonl"
        convert(str(parquet_file), str(jsonl_file))
        
        assert jsonl_file.exists()
        lines = jsonl_file.read_text().strip().split("\n")
        assert len(lines) == 2
        
        record1 = json.loads(lines[0])
        assert record1["id"] == 1
        assert record1["tags"] == ["a", "b"]


class TestParquetToAvro:
    """Tests for Parquet to Avro conversion."""
    
    def test_parquet_to_avro(self, tmp_path):
        # Create Parquet file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        table = pa.table({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }, schema=schema)
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to Avro
        avro_file = tmp_path / "test.avro"
        convert(str(parquet_file), str(avro_file))
        
        assert avro_file.exists()
        
        # Verify Avro content
        with open(avro_file, "rb") as f:
            reader = fastavro.reader(f)
            records = list(reader)
            assert len(records) == 2
            assert records[0]["id"] == 1
            assert records[0]["name"] == "Alice"


class TestParquetToArrow:
    """Tests for Parquet to Arrow IPC conversion."""
    
    def test_parquet_to_arrow(self, tmp_path):
        # Create Parquet file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64()),
        ])
        table = pa.table({
            "id": [1, 2, 3],
            "value": [1.5, 2.5, 3.5],
        }, schema=schema)
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to Arrow
        arrow_file = tmp_path / "test.arrow"
        convert(str(parquet_file), str(arrow_file))
        
        assert arrow_file.exists()
        
        # Verify Arrow content
        with pa.memory_map(str(arrow_file), "r") as source:
            reader = pa.ipc.open_file(source)
            assert reader.num_record_batches == 1
            batch = reader.get_batch(0)
            assert batch.num_rows == 3


class TestRoundTrip:
    """Tests for round-trip conversions."""
    
    def test_csv_parquet_csv_roundtrip(self, tmp_path):
        csv_file = tmp_path / "original.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n3,300\n")
        
        parquet_file = tmp_path / "temp.parquet"
        csv_file2 = tmp_path / "result.csv"
        
        convert(str(csv_file), str(parquet_file))
        convert(str(parquet_file), str(csv_file2))
        
        # Verify round-trip
        original = csv_file.read_text()
        result = csv_file2.read_text()
        
        # Both should have same data (header may differ slightly)
        assert "1,100" in result
        assert "2,200" in result
        assert "3,300" in result
    
    def test_jsonl_parquet_jsonl_roundtrip(self, tmp_path):
        jsonl_file = tmp_path / "original.jsonl"
        jsonl_file.write_text(
            '{"id": 1, "data": {"x": 1}}\n'
            '{"id": 2, "data": {"x": 2}}\n'
        )
        
        parquet_file = tmp_path / "temp.parquet"
        jsonl_file2 = tmp_path / "result.jsonl"
        
        convert(str(jsonl_file), str(parquet_file))
        convert(str(parquet_file), str(jsonl_file2))
        
        # Verify round-trip
        original_records = [
            json.loads(line) for line in jsonl_file.read_text().strip().split("\n")
        ]
        result_records = [
            json.loads(line) for line in jsonl_file2.read_text().strip().split("\n")
        ]
        
        assert len(result_records) == len(original_records)
        for orig, res in zip(original_records, result_records):
            assert orig["id"] == res["id"]
            assert orig["data"]["x"] == res["data"]["x"]
    
    def test_avro_parquet_avro_roundtrip(self, tmp_path):
        # Create original Avro file
        avro_schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
            ],
        }
        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        
        avro_file = tmp_path / "original.avro"
        with open(avro_file, "wb") as f:
            fastavro.writer(f, avro_schema, records)
        
        parquet_file = tmp_path / "temp.parquet"
        avro_file2 = tmp_path / "result.avro"
        
        convert(str(avro_file), str(parquet_file))
        convert(str(parquet_file), str(avro_file2))
        
        assert avro_file2.exists()
        
        # Verify content
        with open(avro_file2, "rb") as f:
            reader = fastavro.reader(f)
            result_records = list(reader)
            assert len(result_records) == 2
    
    def test_arrow_parquet_arrow_roundtrip(self, tmp_path):
        # Create original Arrow file
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64()),
        ])
        batch = pa.record_batch([
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array([1.5, 2.5, 3.5], type=pa.float64()),
        ], schema=schema)
        
        arrow_file = tmp_path / "original.arrow"
        with pa.OSFile(str(arrow_file), "wb") as f:
            with pa.ipc.new_file(f, schema) as writer:
                writer.write_batch(batch)
        
        parquet_file = tmp_path / "temp.parquet"
        arrow_file2 = tmp_path / "result.arrow"
        
        convert(str(arrow_file), str(parquet_file))
        convert(str(parquet_file), str(arrow_file2))
        
        assert arrow_file2.exists()
        
        # Verify content
        with pa.memory_map(str(arrow_file2), "r") as source:
            reader = pa.ipc.open_file(source)
            result_batch = reader.get_batch(0)
            assert result_batch.num_rows == 3


class TestConvertDispatch:
    """Tests for convert function dispatch."""
    
    def test_tsv_delimiter_both_directions(self, tmp_path):
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text("id\tname\n1\tAlice\n2\tBob\n")
        
        parquet_file = tmp_path / "test.parquet"
        tsv_file2 = tmp_path / "result.tsv"
        
        convert(str(tsv_file), str(parquet_file))
        convert(str(parquet_file), str(tsv_file2))
        
        result = tsv_file2.read_text()
        # Should use tab delimiter
        assert "\t" in result
    
    def test_unknown_extension_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown file extension"):
            convert("data.xlsx", "data.parquet")


class TestSchemaMapping:
    """Tests for Avro <-> Arrow schema mapping."""
    
    def test_arrow_schema_to_avro_simple(self):
        arrow_schema = pa.schema([
            pa.field("string_field", pa.string(), nullable=False),
            pa.field("int_field", pa.int32(), nullable=False),
            pa.field("long_field", pa.int64(), nullable=False),
            pa.field("float_field", pa.float32(), nullable=False),
            pa.field("double_field", pa.float64(), nullable=False),
            pa.field("bool_field", pa.bool_(), nullable=False),
            pa.field("nullable_field", pa.string(), nullable=True),
        ])
        
        avro_schema = arrow_schema_to_avro_schema(arrow_schema)
        
        assert avro_schema["type"] == "record"
        fields = avro_schema["fields"]
        
        # Check types
        assert fields[0]["type"] == "string"
        assert fields[1]["type"] == "int"
        assert fields[2]["type"] == "long"
        assert fields[3]["type"] == "float"
        assert fields[4]["type"] == "double"
        assert fields[5]["type"] == "boolean"
        # Nullable field should be union
        assert isinstance(fields[6]["type"], list)
        assert "null" in fields[6]["type"]
    
    def test_arrow_schema_to_avro_date_time(self):
        arrow_schema = pa.schema([
            pa.field("date_field", pa.date32(), nullable=False),
            pa.field("time_field", pa.time64("us"), nullable=False),
        ])
        
        avro_schema = arrow_schema_to_avro_schema(arrow_schema)
        fields = avro_schema["fields"]
        
        assert fields[0]["type"]["logicalType"] == "date"
        assert fields[1]["type"]["logicalType"] == "time-micros"
    
    def test_avro_schema_to_arrow_simple(self):
        avro_schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "string_field", "type": "string"},
                {"name": "int_field", "type": "int"},
                {"name": "long_field", "type": "long"},
                {"name": "float_field", "type": "float"},
                {"name": "double_field", "type": "double"},
                {"name": "bool_field", "type": "boolean"},
            ],
        }
        
        arrow_schema = avro_schema_to_arrow_schema(avro_schema)
        
        assert len(arrow_schema) == 6
        assert pa.types.is_string(arrow_schema[0].type)
        assert pa.types.is_int32(arrow_schema[1].type)
        assert pa.types.is_int64(arrow_schema[2].type)
        assert pa.types.is_float32(arrow_schema[3].type)
        assert pa.types.is_float64(arrow_schema[4].type)
        assert pa.types.is_boolean(arrow_schema[5].type)
    
    def test_avro_schema_to_arrow_with_nullable(self):
        avro_schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "nullable_field", "type": ["null", "string"], "default": None},
            ],
        }
        
        arrow_schema = avro_schema_to_arrow_schema(avro_schema)
        
        assert len(arrow_schema) == 1
        assert arrow_schema[0].nullable is True
        assert pa.types.is_string(arrow_schema[0].type)


class TestConvenienceWrappers:
    """Tests for convenience wrapper functions."""
    
    def test_csv_to_parquet_wrapper(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,value\n1,100\n")
        
        parquet_file = tmp_path / "test.parquet"
        csv_to_parquet(str(csv_file), str(parquet_file))
        
        assert parquet_file.exists()
    
    def test_tsv_to_parquet_wrapper(self, tmp_path):
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text("id\tvalue\n1\t100\n")
        
        parquet_file = tmp_path / "test.parquet"
        tsv_to_parquet(str(tsv_file), str(parquet_file))
        
        assert parquet_file.exists()
    
    def test_jsonl_to_parquet_wrapper(self, tmp_path):
        jsonl_file = tmp_path / "test.jsonl"
        jsonl_file.write_text('{"id": 1}\n')
        
        parquet_file = tmp_path / "test.parquet"
        jsonl_to_parquet(str(jsonl_file), str(parquet_file))
        
        assert parquet_file.exists()
    
    def test_avro_to_parquet_wrapper(self, tmp_path):
        avro_file = tmp_path / "test.avro"
        avro_schema = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "long"}]}
        with open(avro_file, "wb") as f:
            fastavro.writer(f, avro_schema, [{"id": 1}])
        
        parquet_file = tmp_path / "test.parquet"
        avro_to_parquet(str(avro_file), str(parquet_file))
        
        assert parquet_file.exists()
    
    def test_arrow_to_parquet_wrapper(self, tmp_path):
        arrow_file = tmp_path / "test.arrow"
        schema = pa.schema([pa.field("id", pa.int64())])
        batch = pa.record_batch([pa.array([1], type=pa.int64())], schema=schema)
        with pa.OSFile(str(arrow_file), "wb") as f:
            with pa.ipc.new_file(f, schema) as writer:
                writer.write_batch(batch)
        
        parquet_file = tmp_path / "test.parquet"
        arrow_to_parquet(str(arrow_file), str(parquet_file))
        
        assert parquet_file.exists()
    
    def test_parquet_to_csv_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        csv_file = tmp_path / "test.csv"
        parquet_to_csv(str(parquet_file), str(csv_file))
        
        assert csv_file.exists()
    
    def test_parquet_to_tsv_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        tsv_file = tmp_path / "test.tsv"
        parquet_to_tsv(str(parquet_file), str(tsv_file))
        
        assert tsv_file.exists()
    
    def test_parquet_to_jsonl_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        jsonl_file = tmp_path / "test.jsonl"
        parquet_to_jsonl(str(parquet_file), str(jsonl_file))
        
        assert jsonl_file.exists()
    
    def test_parquet_to_avro_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        avro_file = tmp_path / "test.avro"
        parquet_to_avro(str(parquet_file), str(avro_file))
        
        assert avro_file.exists()
    
    def test_parquet_to_arrow_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        arrow_file = tmp_path / "test.arrow"
        parquet_to_arrow(str(parquet_file), str(arrow_file))
        
        assert arrow_file.exists()


class TestXMLToParquet:
    """Tests for XML to Parquet conversion."""
    
    def test_xml_to_parquet_basic(self, tmp_path):
        xml_file = tmp_path / "test.xml"
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <record>
        <id>1</id>
        <name>Alice</name>
        <age>30</age>
    </record>
    <record>
        <id>2</id>
        <name>Bob</name>
        <age>25</age>
    </record>
</root>"""
        xml_file.write_text(xml_content)
        
        parquet_file = tmp_path / "test.parquet"
        convert(str(xml_file), str(parquet_file), record_tag="record")
        
        assert parquet_file.exists()
        
        # Verify content
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == ["1", "2"]
        assert table.column("name").to_pylist() == ["Alice", "Bob"]
    
    def test_xml_to_parquet_with_attributes(self, tmp_path):
        xml_file = tmp_path / "test.xml"
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <person id="1" status="active">
        <name>Alice</name>
    </person>
    <person id="2" status="inactive">
        <name>Bob</name>
    </person>
</root>"""
        xml_file.write_text(xml_content)
        
        parquet_file = tmp_path / "test.parquet"
        convert(str(xml_file), str(parquet_file), record_tag="person")
        
        assert parquet_file.exists()
        
        # Verify content
        table = pq.read_table(str(parquet_file))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == ["1", "2"]
        assert table.column("status").to_pylist() == ["active", "inactive"]


class TestParquetToXML:
    """Tests for Parquet to XML conversion."""
    
    def test_parquet_to_xml_basic(self, tmp_path):
        # Create Parquet file
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_file))
        
        # Convert to XML
        xml_file = tmp_path / "test.xml"
        convert(str(parquet_file), str(xml_file), record_tag="person", root_tag="people")
        
        assert xml_file.exists()
        
        # Verify XML structure
        content = xml_file.read_text()
        assert "<people>" in content
        assert "<person>" in content
        assert "<id>1</id>" in content
        assert "<name>Alice</name>" in content


class TestXMLRoundTrip:
    """Tests for XML round-trip conversions."""
    
    def test_xml_parquet_xml_roundtrip(self, tmp_path):
        xml_file = tmp_path / "original.xml"
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <record>
        <id>1</id>
        <value>100</value>
    </record>
    <record>
        <id>2</id>
        <value>200</value>
    </record>
</root>"""
        xml_file.write_text(xml_content)
        
        parquet_file = tmp_path / "temp.parquet"
        xml_file2 = tmp_path / "result.xml"
        
        convert(str(xml_file), str(parquet_file), record_tag="record")
        convert(str(parquet_file), str(xml_file2), record_tag="record", root_tag="root")
        
        assert xml_file2.exists()
        
        # Verify round-trip
        content = xml_file2.read_text()
        assert "<root>" in content
        assert "<record>" in content


class TestXMLConvenienceWrappers:
    """Tests for XML convenience wrapper functions."""
    
    def test_xml_to_parquet_wrapper(self, tmp_path):
        xml_file = tmp_path / "test.xml"
        xml_file.write_text("<root><record><id>1</id></record></root>")
        
        parquet_file = tmp_path / "test.parquet"
        xml_to_parquet(str(xml_file), str(parquet_file), record_tag="record")
        
        assert parquet_file.exists()
    
    def test_parquet_to_xml_wrapper(self, tmp_path):
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"id": [1, 2]})
        pq.write_table(table, str(parquet_file))
        
        xml_file = tmp_path / "test.xml"
        parquet_to_xml(str(parquet_file), str(xml_file))
        
        assert xml_file.exists()
        content = xml_file.read_text()
        assert "<root>" in content
        assert "<record>" in content
