################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

"""Tests for VARIANT data type support in pypaimon.

VARIANT is stored in Parquet as a struct with two non-nullable BINARY fields::

    required group <col> {
        required binary value;    // encoded variant payload
        required binary metadata; // key-dictionary for object field names
    }

PyArrow reads this group transparently as ``pa.struct``; no special reader is
needed.  These tests verify the schema-mapping round-trip and the Parquet
read/write cycle.
"""

import io
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.schema.data_types import (
    AtomicType,
    DataField,
    DataTypeParser,
    PyarrowFieldParser,
    RowType,
    is_variant_struct,
)
from pypaimon.table.row.generic_row import GenericRowDeserializer, GenericRowSerializer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _variant_arrow_type() -> pa.StructType:
    """The canonical Arrow representation of a VARIANT column."""
    return pa.struct([
        pa.field('value', pa.binary(), nullable=False),
        pa.field('metadata', pa.binary(), nullable=False),
    ])


def _make_variant_bytes(json_str: str) -> bytes:
    """Produce a minimal Paimon-compatible VARIANT value payload.

    This is not a full Variant binary-spec encoder; it encodes the JSON string
    as a UTF-8 string primitive (type byte 0x15 = string) so that the bytes
    are structurally valid and round-trip as the same raw bytes.

    Encoding layout (Paimon/Parquet Variant spec v1):
        - header byte: 0x15  (primitive, type=string)
        - 4-byte little-endian length
        - UTF-8 string bytes
    """
    import struct
    payload = json_str.encode('utf-8')
    return struct.pack('<B', 0x15) + struct.pack('<I', len(payload)) + payload


def _make_metadata() -> bytes:
    """Minimal Parquet Variant metadata: version=1, zero dictionary entries."""
    return b'\x01\x00'


# ---------------------------------------------------------------------------
# 1. Schema parsing
# ---------------------------------------------------------------------------

class TestVariantSchemaParsing(unittest.TestCase):

    def test_parse_variant_keyword(self):
        """DataTypeParser accepts the VARIANT keyword."""
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT')
        self.assertIsInstance(dt, AtomicType)
        self.assertEqual(dt.type, 'VARIANT')
        self.assertTrue(dt.nullable)

    def test_parse_variant_not_null(self):
        """DataTypeParser accepts VARIANT NOT NULL."""
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT NOT NULL')
        self.assertIsInstance(dt, AtomicType)
        self.assertFalse(dt.nullable)

    def test_variant_to_dict_roundtrip(self):
        """AtomicType('VARIANT') survives a to_dict / from_dict round-trip."""
        dt = AtomicType('VARIANT')
        serialised = dt.to_dict()
        restored = DataTypeParser.parse_data_type(serialised)
        self.assertEqual(dt, restored)

    def test_variant_str(self):
        """str() representation is 'VARIANT'."""
        self.assertEqual(str(AtomicType('VARIANT')), 'VARIANT')
        self.assertEqual(str(AtomicType('VARIANT', nullable=False)), 'VARIANT NOT NULL')


# ---------------------------------------------------------------------------
# 2. Arrow type mapping — Paimon → Arrow
# ---------------------------------------------------------------------------

class TestVariantFromPaimonType(unittest.TestCase):

    def test_from_paimon_type_returns_struct(self):
        """VARIANT maps to a two-field BINARY struct."""
        arrow_type = PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))
        self.assertTrue(pa.types.is_struct(arrow_type))
        self.assertEqual(arrow_type.num_fields, 2)

    def test_struct_field_names(self):
        arrow_type = PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))
        self.assertEqual(arrow_type.field(0).name, 'value')
        self.assertEqual(arrow_type.field(1).name, 'metadata')

    def test_struct_field_types(self):
        arrow_type = PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))
        self.assertTrue(pa.types.is_binary(arrow_type.field(0).type))
        self.assertTrue(pa.types.is_binary(arrow_type.field(1).type))

    def test_struct_fields_not_nullable(self):
        arrow_type = PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))
        self.assertFalse(arrow_type.field(0).nullable)
        self.assertFalse(arrow_type.field(1).nullable)

    def test_from_paimon_field(self):
        """from_paimon_field wraps the type in a pa.Field with correct nullability."""
        df = DataField(id=0, name='payload', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(df)
        self.assertEqual(pa_field.name, 'payload')
        self.assertTrue(pa.types.is_struct(pa_field.type))
        # The outer field is nullable (VARIANT default is nullable)
        self.assertTrue(pa_field.nullable)

    def test_from_paimon_schema(self):
        """from_paimon_schema produces correct Arrow schema for a mixed table."""
        fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
        ]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        self.assertEqual(schema.field('payload').type, _variant_arrow_type())


# ---------------------------------------------------------------------------
# 3. Arrow type mapping — Arrow → Paimon  (is_variant_struct + to_paimon_type)
# ---------------------------------------------------------------------------

class TestVariantToPaimonType(unittest.TestCase):

    def testis_variant_struct_positive(self):
        """is_variant_struct recognises the canonical VARIANT struct."""
        self.assertTrue(is_variant_struct(_variant_arrow_type()))

    def testis_variant_struct_wrong_names(self):
        """A struct with wrong field names is NOT recognised as VARIANT."""
        st = pa.struct([
            pa.field('val', pa.binary(), nullable=False),
            pa.field('meta', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def testis_variant_struct_nullable_fields(self):
        """A struct with nullable fields is NOT recognised as VARIANT."""
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=True),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def testis_variant_struct_wrong_types(self):
        """A struct with non-binary field types is NOT recognised as VARIANT."""
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def testis_variant_struct_extra_fields(self):
        """A struct with more than 2 fields (shredded variant) is NOT auto-recognised."""
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
            pa.field('typed_value', pa.int64(), nullable=True),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_to_paimon_type_variant(self):
        """to_paimon_type converts the canonical VARIANT struct back to VARIANT."""
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=True)
        self.assertIsInstance(result, AtomicType)
        self.assertEqual(result.type, 'VARIANT')
        self.assertTrue(result.nullable)

    def test_to_paimon_type_variant_not_null(self):
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=False)
        self.assertFalse(result.nullable)

    def test_ordinary_struct_not_confused_with_variant(self):
        """A normal ROW struct with non-VARIANT fields maps to RowType, not VARIANT."""
        st = pa.struct([
            pa.field('a', pa.int32()),
            pa.field('b', pa.string()),
        ])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)

    def test_struct_same_names_but_different_types_is_rowtype(self):
        """A struct named value/metadata but with non-binary types maps to RowType."""
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.string(), nullable=False),
        ])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)


# ---------------------------------------------------------------------------
# 4. Full schema round-trip
# ---------------------------------------------------------------------------

class TestVariantSchemaRoundTrip(unittest.TestCase):

    def test_paimon_to_arrow_to_paimon(self):
        """VARIANT field survives a full Paimon → Arrow → Paimon round-trip."""
        original = DataField(id=0, name='v', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(original)
        restored_type = PyarrowFieldParser.to_paimon_type(pa_field.type, pa_field.nullable)
        self.assertIsInstance(restored_type, AtomicType)
        self.assertEqual(restored_type.type, 'VARIANT')

    def test_mixed_schema_round_trip(self):
        """A table schema with VARIANT alongside other types round-trips correctly."""
        original_fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
            DataField(id=2, name='ts', type=AtomicType('TIMESTAMP(6)')),
        ]
        pa_schema = PyarrowFieldParser.from_paimon_schema(original_fields)
        restored_fields = PyarrowFieldParser.to_paimon_schema(pa_schema)

        self.assertEqual(restored_fields[0].name, 'id')
        self.assertEqual(restored_fields[1].name, 'payload')
        self.assertIsInstance(restored_fields[1].type, AtomicType)
        self.assertEqual(restored_fields[1].type.type, 'VARIANT')
        self.assertEqual(restored_fields[2].name, 'ts')


# ---------------------------------------------------------------------------
# 5. Parquet read/write cycle
# ---------------------------------------------------------------------------

class TestVariantParquetCycle(unittest.TestCase):
    """Verify that VARIANT columns survive a Parquet write → read cycle.

    PyArrow writes the struct-of-binary as a Parquet GROUP, which matches the
    layout produced by Paimon Java.  On read, PyArrow reconstructs the struct
    transparently — no custom reader is required.
    """

    def _make_table(self) -> pa.Table:
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('payload', _variant_arrow_type()),
        ])
        value1 = _make_variant_bytes('{"key": "hello"}')
        value2 = _make_variant_bytes('42')
        meta = _make_metadata()
        payload_col = pa.array(
            [{'value': value1, 'metadata': meta},
             {'value': value2, 'metadata': meta}],
            type=_variant_arrow_type(),
        )
        return pa.table(
            {'id': pa.array([1, 2], type=pa.int64()), 'payload': payload_col},
            schema=schema,
        )

    def test_write_and_read_parquet(self):
        """VARIANT struct column survives Parquet write → read."""
        original = self._make_table()
        buf = io.BytesIO()
        pq.write_table(original, buf)
        buf.seek(0)
        restored = pq.read_table(buf)

        self.assertEqual(restored.schema.field('payload').type, _variant_arrow_type())
        self.assertEqual(restored.num_rows, 2)

    def test_variant_values_preserved(self):
        """The raw value and metadata bytes are preserved across Parquet round-trip."""
        original = self._make_table()
        buf = io.BytesIO()
        pq.write_table(original, buf)
        buf.seek(0)
        restored = pq.read_table(buf)

        payload_col = restored.column('payload')
        row0 = payload_col[0].as_py()
        self.assertIn('value', row0)
        self.assertIn('metadata', row0)
        self.assertEqual(row0['value'], _make_variant_bytes('{"key": "hello"}'))
        self.assertEqual(row0['metadata'], _make_metadata())

    def test_null_variant_row(self):
        """A NULL VARIANT value is handled correctly."""
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('payload', _variant_arrow_type()),
        ])
        payload_col = pa.array(
            [None, {'value': _make_variant_bytes('true'), 'metadata': _make_metadata()}],
            type=_variant_arrow_type(),
        )
        table = pa.table({'id': [1, 2], 'payload': payload_col}, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        restored = pq.read_table(buf)
        self.assertIsNone(restored.column('payload')[0].as_py())
        self.assertIsNotNone(restored.column('payload')[1].as_py())

    def test_write_to_file(self):
        """VARIANT table can be written to and read from a real file path."""
        original = self._make_table()
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            path = f.name
        pq.write_table(original, path)
        restored = pq.read_table(path)
        self.assertEqual(restored.num_rows, 2)
        import os
        os.unlink(path)


# ---------------------------------------------------------------------------
# 6. BinaryRow serializer / deserializer  (safety-net paths)
# ---------------------------------------------------------------------------

class TestVariantBinaryRow(unittest.TestCase):
    """The BinaryRow path for VARIANT is a safety net; VARIANT is never a key.

    We verify that the code does not silently corrupt data or raise unexpected
    errors.  The deserializer returns {'value': bytes, 'metadata': bytes};
    the serializer encodes the value payload as a variable-length binary field.
    """

    def _make_field(self) -> DataField:
        return DataField(id=0, name='v', type=AtomicType('VARIANT'))

    def test_serialize_variant_dict(self):
        """Serializing a VARIANT dict does not raise."""
        from pypaimon.table.row.generic_row import GenericRow
        field = self._make_field()
        value = {'value': b'\x15\x05hello', 'metadata': b'\x01\x00'}
        row = GenericRow([value], [field])
        serialized = GenericRowSerializer.to_bytes(row)
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), 0)

    def test_serialize_null_variant(self):
        """A NULL VARIANT value serializes to the null-bit representation."""
        from pypaimon.table.row.generic_row import GenericRow
        field = self._make_field()
        row = GenericRow([None], [field])
        serialized = GenericRowSerializer.to_bytes(row)
        self.assertIsInstance(serialized, bytes)

    def test_deserialize_produces_dict(self):
        """Deserializing a serialized VARIANT row returns a dict with 'value' key."""
        from pypaimon.table.row.generic_row import GenericRow
        field = self._make_field()
        value = {'value': b'\x15\x05hello', 'metadata': b'\x01\x00'}
        row = GenericRow([value], [field])
        serialized = GenericRowSerializer.to_bytes(row)
        restored = GenericRowDeserializer.from_bytes(serialized, [field])
        result = restored.values[0]
        self.assertIsInstance(result, dict)
        self.assertIn('value', result)

    def test_serialize_bytes_fallback(self):
        """Serializing raw bytes (not a dict) as VARIANT does not raise."""
        from pypaimon.table.row.generic_row import GenericRow
        field = self._make_field()
        row = GenericRow([b'\x15\x05hello'], [field])
        serialized = GenericRowSerializer.to_bytes(row)
        self.assertIsInstance(serialized, bytes)


if __name__ == '__main__':
    unittest.main()
