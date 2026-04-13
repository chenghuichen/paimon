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
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""Tests for VARIANT type support in pypaimon.

Covers two layers:
  1. Type-system layer  – schema parsing, Paimon↔Arrow type mapping, Parquet I/O.
  2. Encoding layer     – GenericVariant binary encoding/decoding, to_json, variant_get.
"""

import decimal
import io
import json
import struct
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.data.generic_variant import (
    GenericVariant,
    Type,
    _PRIMITIVE,
    _SHORT_STR,
    _TRUE,
    _INT1,
)
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
    """Produce a minimal VARIANT value payload encoding a UTF-8 string primitive."""
    payload = json_str.encode('utf-8')
    return struct.pack('<B', 0x15) + struct.pack('<I', len(payload)) + payload


def _make_metadata() -> bytes:
    """Minimal Parquet Variant metadata: version=1, zero dictionary entries."""
    return b'\x01\x00'


def _roundtrip(json_str):
    """Build a GenericVariant from JSON, decode back to JSON, parse with stdlib json."""
    v = GenericVariant.from_json(json_str)
    return json.loads(v.to_json())


# ===========================================================================
# 1. Schema parsing
# ===========================================================================

class TestVariantSchemaParsing(unittest.TestCase):

    def test_parse_variant_keyword(self):
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT')
        self.assertIsInstance(dt, AtomicType)
        self.assertEqual(dt.type, 'VARIANT')
        self.assertTrue(dt.nullable)

    def test_parse_variant_not_null(self):
        dt = DataTypeParser.parse_atomic_type_sql_string('VARIANT NOT NULL')
        self.assertIsInstance(dt, AtomicType)
        self.assertFalse(dt.nullable)

    def test_variant_to_dict_roundtrip(self):
        dt = AtomicType('VARIANT')
        restored = DataTypeParser.parse_data_type(dt.to_dict())
        self.assertEqual(dt, restored)

    def test_variant_str(self):
        self.assertEqual(str(AtomicType('VARIANT')), 'VARIANT')
        self.assertEqual(str(AtomicType('VARIANT', nullable=False)), 'VARIANT NOT NULL')


# ===========================================================================
# 2. Arrow type mapping – Paimon → Arrow
# ===========================================================================

class TestVariantFromPaimonType(unittest.TestCase):

    def _arrow_type(self):
        return PyarrowFieldParser.from_paimon_type(AtomicType('VARIANT'))

    def test_returns_struct(self):
        self.assertTrue(pa.types.is_struct(self._arrow_type()))
        self.assertEqual(self._arrow_type().num_fields, 2)

    def test_field_names(self):
        t = self._arrow_type()
        self.assertEqual(t.field(0).name, 'value')
        self.assertEqual(t.field(1).name, 'metadata')

    def test_field_types_are_binary(self):
        t = self._arrow_type()
        self.assertTrue(pa.types.is_binary(t.field(0).type))
        self.assertTrue(pa.types.is_binary(t.field(1).type))

    def test_fields_not_nullable(self):
        t = self._arrow_type()
        self.assertFalse(t.field(0).nullable)
        self.assertFalse(t.field(1).nullable)

    def test_from_paimon_field(self):
        df = DataField(id=0, name='payload', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(df)
        self.assertEqual(pa_field.name, 'payload')
        self.assertTrue(pa.types.is_struct(pa_field.type))
        self.assertTrue(pa_field.nullable)

    def test_from_paimon_schema(self):
        fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
        ]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        self.assertEqual(schema.field('payload').type, _variant_arrow_type())


# ===========================================================================
# 3. Arrow type mapping – Arrow → Paimon  (is_variant_struct + to_paimon_type)
# ===========================================================================

class TestVariantToPaimonType(unittest.TestCase):

    def test_is_variant_struct_positive(self):
        self.assertTrue(is_variant_struct(_variant_arrow_type()))

    def test_is_variant_struct_wrong_names(self):
        st = pa.struct([
            pa.field('val', pa.binary(), nullable=False),
            pa.field('meta', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_nullable_fields(self):
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=True),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_wrong_types(self):
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_is_variant_struct_extra_fields(self):
        st = pa.struct([
            pa.field('value', pa.binary(), nullable=False),
            pa.field('metadata', pa.binary(), nullable=False),
            pa.field('typed_value', pa.int64(), nullable=True),
        ])
        self.assertFalse(is_variant_struct(st))

    def test_to_paimon_type_variant(self):
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=True)
        self.assertIsInstance(result, AtomicType)
        self.assertEqual(result.type, 'VARIANT')
        self.assertTrue(result.nullable)

    def test_to_paimon_type_variant_not_null(self):
        result = PyarrowFieldParser.to_paimon_type(_variant_arrow_type(), nullable=False)
        self.assertFalse(result.nullable)

    def test_ordinary_struct_maps_to_row_type(self):
        st = pa.struct([pa.field('a', pa.int32()), pa.field('b', pa.string())])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)

    def test_struct_same_names_different_types_is_row_type(self):
        st = pa.struct([
            pa.field('value', pa.string(), nullable=False),
            pa.field('metadata', pa.string(), nullable=False),
        ])
        result = PyarrowFieldParser.to_paimon_type(st, nullable=True)
        self.assertIsInstance(result, RowType)


# ===========================================================================
# 4. Full schema round-trip
# ===========================================================================

class TestVariantSchemaRoundTrip(unittest.TestCase):

    def test_paimon_to_arrow_to_paimon(self):
        original = DataField(id=0, name='v', type=AtomicType('VARIANT'))
        pa_field = PyarrowFieldParser.from_paimon_field(original)
        restored = PyarrowFieldParser.to_paimon_type(pa_field.type, pa_field.nullable)
        self.assertIsInstance(restored, AtomicType)
        self.assertEqual(restored.type, 'VARIANT')

    def test_mixed_schema_round_trip(self):
        fields = [
            DataField(id=0, name='id', type=AtomicType('BIGINT')),
            DataField(id=1, name='payload', type=AtomicType('VARIANT')),
            DataField(id=2, name='ts', type=AtomicType('TIMESTAMP(6)')),
        ]
        pa_schema = PyarrowFieldParser.from_paimon_schema(fields)
        restored = PyarrowFieldParser.to_paimon_schema(pa_schema)

        self.assertEqual(restored[1].name, 'payload')
        self.assertIsInstance(restored[1].type, AtomicType)
        self.assertEqual(restored[1].type.type, 'VARIANT')
        self.assertEqual(restored[2].name, 'ts')


# ===========================================================================
# 5. Parquet read/write cycle
# ===========================================================================

class TestVariantParquetCycle(unittest.TestCase):

    def _make_table(self) -> pa.Table:
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('payload', _variant_arrow_type()),
        ])
        meta = _make_metadata()
        payload_col = pa.array(
            [{'value': _make_variant_bytes('{"key":"hello"}'), 'metadata': meta},
             {'value': _make_variant_bytes('42'), 'metadata': meta}],
            type=_variant_arrow_type(),
        )
        return pa.table({'id': [1, 2], 'payload': payload_col}, schema=schema)

    def test_write_and_read_parquet(self):
        original = self._make_table()
        buf = io.BytesIO()
        pq.write_table(original, buf)
        buf.seek(0)
        restored = pq.read_table(buf)
        self.assertEqual(restored.schema.field('payload').type, _variant_arrow_type())
        self.assertEqual(restored.num_rows, 2)

    def test_variant_values_preserved(self):
        original = self._make_table()
        buf = io.BytesIO()
        pq.write_table(original, buf)
        buf.seek(0)
        row0 = pq.read_table(buf).column('payload')[0].as_py()
        self.assertEqual(row0['value'], _make_variant_bytes('{"key":"hello"}'))
        self.assertEqual(row0['metadata'], _make_metadata())

    def test_null_variant_row(self):
        schema = pa.schema([pa.field('id', pa.int64()), pa.field('payload', _variant_arrow_type())])
        payload_col = pa.array(
            [None, {'value': _make_variant_bytes('true'), 'metadata': _make_metadata()}],
            type=_variant_arrow_type(),
        )
        buf = io.BytesIO()
        pq.write_table(pa.table({'id': [1, 2], 'payload': payload_col}, schema=schema), buf)
        buf.seek(0)
        restored = pq.read_table(buf)
        self.assertIsNone(restored.column('payload')[0].as_py())
        self.assertIsNotNone(restored.column('payload')[1].as_py())

    def test_write_to_file(self):
        original = self._make_table()
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            path = f.name
        pq.write_table(original, path)
        restored = pq.read_table(path)
        self.assertEqual(restored.num_rows, 2)
        import os
        os.unlink(path)


# ===========================================================================
# 6. GenericVariant – primitive types
# ===========================================================================

class TestPrimitives(unittest.TestCase):

    def test_null(self):
        v = GenericVariant.from_json('null')
        self.assertEqual(v.get_type(), Type.NULL)
        self.assertIsNone(v.to_python())
        self.assertEqual(v.to_json(), 'null')

    def test_true(self):
        v = GenericVariant.from_json('true')
        self.assertEqual(v.get_type(), Type.BOOLEAN)
        self.assertTrue(v.get_boolean())
        self.assertEqual(v.to_json(), 'true')

    def test_false(self):
        v = GenericVariant.from_json('false')
        self.assertFalse(v.get_boolean())
        self.assertEqual(v.to_json(), 'false')

    def test_int_small(self):
        v = GenericVariant.from_json('42')
        self.assertEqual(v.get_type(), Type.LONG)
        self.assertEqual(v.get_long(), 42)

    def test_int_negative(self):
        self.assertEqual(GenericVariant.from_json('-100').get_long(), -100)

    def test_int_int2_boundary(self):
        self.assertEqual(GenericVariant.from_json('1000').get_long(), 1000)

    def test_int_int4_boundary(self):
        self.assertEqual(GenericVariant.from_json('100000').get_long(), 100000)

    def test_int_int8(self):
        large = 2 ** 33
        self.assertEqual(GenericVariant.from_json(str(large)).get_long(), large)

    def test_float_double(self):
        self.assertAlmostEqual(float(GenericVariant.from_json('1.5').to_python()), 1.5)

    def test_float_scientific(self):
        v = GenericVariant.from_json('1.5e10')
        self.assertEqual(v.get_type(), Type.DOUBLE)
        self.assertAlmostEqual(v.get_double(), 1.5e10)

    def test_string_short(self):
        v = GenericVariant.from_json('"hello"')
        self.assertEqual(v.get_type(), Type.STRING)
        self.assertEqual(v.get_string(), 'hello')

    def test_string_long(self):
        long_str = 'x' * 100
        v = GenericVariant.from_json(json.dumps(long_str))
        self.assertEqual(v.get_string(), long_str)

    def test_string_unicode(self):
        self.assertEqual(GenericVariant.from_json('"北京"').get_string(), '北京')

    def test_decimal_precision(self):
        v = GenericVariant.from_json('100.99')
        self.assertEqual(v.get_type(), Type.DECIMAL)
        self.assertAlmostEqual(float(v.get_decimal()), 100.99)


# ===========================================================================
# 7. GenericVariant – objects
# ===========================================================================

class TestObject(unittest.TestCase):

    def _obj(self):
        return GenericVariant.from_json('{"age":30,"city":"Beijing","active":true}')

    def test_type(self):
        self.assertEqual(self._obj().get_type(), Type.OBJECT)

    def test_object_size(self):
        self.assertEqual(self._obj().object_size(), 3)

    def test_get_field_by_key(self):
        v = self._obj()
        self.assertEqual(v.get_field_by_key('age').get_long(), 30)
        self.assertEqual(v.get_field_by_key('city').get_string(), 'Beijing')
        self.assertTrue(v.get_field_by_key('active').get_boolean())

    def test_get_field_missing(self):
        self.assertIsNone(self._obj().get_field_by_key('missing'))

    def test_fields_sorted_alphabetically(self):
        v = GenericVariant.from_json('{"z":1,"a":2,"m":3}')
        keys = [v.get_field_at_index(i)[0] for i in range(v.object_size())]
        self.assertEqual(keys, sorted(keys))

    def test_to_python(self):
        result = self._obj().to_python()
        self.assertEqual(result, {'age': 30, 'city': 'Beijing', 'active': True})

    def test_to_json_roundtrip(self):
        self.assertEqual(
            _roundtrip('{"age":30,"city":"Beijing","active":true}'),
            {'age': 30, 'city': 'Beijing', 'active': True}
        )

    def test_nested_object(self):
        v = GenericVariant.from_json('{"user":{"name":"Alice","age":25}}')
        user = v.get_field_by_key('user')
        self.assertEqual(user.get_field_by_key('name').get_string(), 'Alice')
        self.assertEqual(user.get_field_by_key('age').get_long(), 25)

    def test_empty_object(self):
        v = GenericVariant.from_json('{}')
        self.assertEqual(v.get_type(), Type.OBJECT)
        self.assertEqual(v.object_size(), 0)
        self.assertEqual(v.to_python(), {})

    def test_large_object_binary_search(self):
        """Objects with >32 fields use binary search; verify correctness."""
        obj = {f'key{i:03d}': i for i in range(50)}
        v = GenericVariant.from_json(json.dumps(obj))
        self.assertEqual(v.get_field_by_key('key000').get_long(), 0)
        self.assertEqual(v.get_field_by_key('key049').get_long(), 49)


# ===========================================================================
# 8. GenericVariant – arrays
# ===========================================================================

class TestArray(unittest.TestCase):

    def _arr(self):
        return GenericVariant.from_json('[1,2,3]')

    def test_type(self):
        self.assertEqual(self._arr().get_type(), Type.ARRAY)

    def test_array_size(self):
        self.assertEqual(self._arr().array_size(), 3)

    def test_get_element_at_index(self):
        v = self._arr()
        self.assertEqual(v.get_element_at_index(0).get_long(), 1)
        self.assertEqual(v.get_element_at_index(2).get_long(), 3)

    def test_out_of_bounds(self):
        self.assertIsNone(self._arr().get_element_at_index(99))

    def test_to_python(self):
        self.assertEqual(self._arr().to_python(), [1, 2, 3])

    def test_mixed_array(self):
        self.assertEqual(
            GenericVariant.from_json('[1,"two",null,true]').to_python(),
            [1, 'two', None, True]
        )

    def test_nested_array(self):
        v = GenericVariant.from_json('[[1,2],[3,4]]')
        self.assertEqual(v.get_element_at_index(0).to_python(), [1, 2])
        self.assertEqual(v.get_element_at_index(1).to_python(), [3, 4])

    def test_empty_array(self):
        v = GenericVariant.from_json('[]')
        self.assertEqual(v.get_type(), Type.ARRAY)
        self.assertEqual(v.array_size(), 0)
        self.assertEqual(v.to_python(), [])


# ===========================================================================
# 9. GenericVariant – variant_get (JSONPath extraction + cast)
# ===========================================================================

class TestVariantGet(unittest.TestCase):

    def setUp(self):
        self.v = GenericVariant.from_json(
            '{"name":"Alice","age":30,"score":9.5,"active":true,'
            '"address":{"city":"Beijing","zip":"100000"},'
            '"tags":["python","data"],"balance":1234.56}'
        )

    def test_get_string(self):
        self.assertEqual(self.v.variant_get('$.name', 'string'), 'Alice')

    def test_get_int(self):
        self.assertEqual(self.v.variant_get('$.age', 'int'), 30)

    def test_get_long(self):
        self.assertEqual(self.v.variant_get('$.age', 'long'), 30)

    def test_get_double(self):
        self.assertAlmostEqual(self.v.variant_get('$.score', 'double'), 9.5, places=5)

    def test_get_boolean(self):
        self.assertTrue(self.v.variant_get('$.active', 'boolean'))

    def test_nested_field(self):
        self.assertEqual(self.v.variant_get('$.address.city', 'string'), 'Beijing')

    def test_array_index(self):
        self.assertEqual(self.v.variant_get('$.tags[0]', 'string'), 'python')
        self.assertEqual(self.v.variant_get('$.tags[1]', 'string'), 'data')

    def test_missing_path_returns_none(self):
        self.assertIsNone(self.v.variant_get('$.nonexistent'))

    def test_type_mismatch_returns_none(self):
        self.assertIsNone(self.v.variant_get('$.tags', 'int'))

    def test_no_cast_returns_python_value(self):
        self.assertEqual(self.v.variant_get('$.age'), 30)

    def test_root_dollar_only(self):
        self.assertEqual(GenericVariant.from_json('42').variant_get('$', 'int'), 42)

    def test_bracket_key_syntax(self):
        self.assertEqual(self.v.variant_get("$['name']", 'string'), 'Alice')

    def test_decimal_cast(self):
        result = self.v.variant_get('$.balance', 'decimal')
        self.assertIsInstance(result, decimal.Decimal)
        self.assertAlmostEqual(float(result), 1234.56, places=2)

    def test_string_cast_on_int(self):
        self.assertEqual(self.v.variant_get('$.age', 'string'), '30')


# ===========================================================================
# 10. GenericVariant – constructors
# ===========================================================================

class TestConstructors(unittest.TestCase):

    def test_from_dict_roundtrip(self):
        original = GenericVariant.from_json('{"x":1,"y":2}')
        restored = GenericVariant.from_dict({'value': original.value(), 'metadata': original.metadata()})
        self.assertEqual(restored.to_json(), original.to_json())

    def test_from_dict_array(self):
        original = GenericVariant.from_json('[1,2,3]')
        restored = GenericVariant.from_dict({'value': original.value(), 'metadata': original.metadata()})
        self.assertEqual(restored.get_type(), Type.ARRAY)
        self.assertEqual(restored.to_python(), [1, 2, 3])

    def test_from_python_dict(self):
        v = GenericVariant.from_python({'a': 1, 'b': 'hello'})
        self.assertEqual(v.variant_get('$.a', 'int'), 1)
        self.assertEqual(v.variant_get('$.b', 'string'), 'hello')

    def test_from_python_list(self):
        self.assertEqual(GenericVariant.from_python([10, 20, 30]).to_python(), [10, 20, 30])

    def test_from_python_none(self):
        self.assertIsNone(GenericVariant.from_python(None).to_python())

    def test_from_python_bytes(self):
        v = GenericVariant.from_python(b'\x01\x02\x03')
        self.assertEqual(v.get_type(), Type.BINARY)
        self.assertEqual(v.get_binary(), b'\x01\x02\x03')


# ===========================================================================
# 11. GenericVariant – to_arrow_array
# ===========================================================================

class TestToArrowArray(unittest.TestCase):

    def test_basic(self):
        gv1 = GenericVariant.from_json('{"a":1}')
        gv2 = GenericVariant.from_json('[1,2]')
        arr = GenericVariant.to_arrow_array([gv1, gv2])
        self.assertIsInstance(arr, pa.StructArray)
        self.assertEqual(len(arr), 2)
        restored = GenericVariant.from_dict(arr[0].as_py())
        self.assertEqual(restored.variant_get('$.a', 'int'), 1)

    def test_with_nulls(self):
        arr = GenericVariant.to_arrow_array([GenericVariant.from_json('42'), None])
        self.assertEqual(len(arr), 2)
        self.assertIsNone(arr[1].as_py())

    def test_empty(self):
        self.assertEqual(len(GenericVariant.to_arrow_array([])), 0)


# ===========================================================================
# 12. Java byte-level encoding compatibility
# ===========================================================================

class TestJavaCompatibility(unittest.TestCase):
    """Verify byte-level compatibility with Paimon Java's GenericVariant encoding."""

    def test_null_encoding(self):
        self.assertEqual(GenericVariant.from_json('null').value(), bytes([0x00]))

    def test_true_encoding(self):
        v = GenericVariant.from_json('true')
        self.assertEqual(v.value()[0], (_TRUE << 2) | _PRIMITIVE)
        self.assertTrue(v.get_boolean())

    def test_int1_encoding(self):
        v = GenericVariant.from_json('1')
        self.assertEqual(v.value()[0], (_INT1 << 2) | _PRIMITIVE)
        self.assertEqual(v.value()[1], 1)

    def test_string_short_encoding(self):
        v = GenericVariant.from_json('"hi"')
        self.assertEqual(v.value()[0], (2 << 2) | _SHORT_STR)
        self.assertEqual(v.value()[1:3], b'hi')

    def test_object_field_order(self):
        v = GenericVariant.from_json('{"z":1,"a":2}')
        key0, child0 = v.get_field_at_index(0)
        self.assertEqual(key0, 'a')
        self.assertEqual(child0.get_long(), 2)


# ===========================================================================
# 13. Complex roundtrip
# ===========================================================================

class TestComplexRoundtrip(unittest.TestCase):

    def _check(self, json_str):
        self.assertEqual(_roundtrip(json_str), json.loads(json_str))

    def test_nested_object_array(self):
        self._check('{"users":[{"name":"Alice","age":30},{"name":"Bob","age":25}]}')

    def test_deep_nesting(self):
        self._check('{"a":{"b":{"c":{"d":42}}}}')

    def test_array_of_objects(self):
        self._check('[{"x":1},{"x":2},{"x":3}]')

    def test_all_primitive_types(self):
        self._check('{"n":null,"b":true,"i":42,"s":"hello","f":1.5}')


# ===========================================================================
# 14. BinaryRow – VARIANT is unsupported (raises, not silently corrupts)
# ===========================================================================

class TestVariantBinaryRow(unittest.TestCase):
    """VARIANT is not a valid key/partition type; BinaryRow does not support it.

    Both the serializer and deserializer must raise rather than silently
    return corrupt data.
    """

    def _field(self):
        return DataField(id=0, name='v', type=AtomicType('VARIANT'))

    def test_deserialize_raises(self):
        """Deserializing a VARIANT field from BinaryRow raises ValueError."""
        from pypaimon.table.row.generic_row import GenericRow
        # Build a BinaryRow that looks like it has a BINARY field at position 0.
        # The exact bytes don't matter; the type dispatch must fail before reading.
        field = self._field()
        # Serialize a plain binary value via a different type to get a valid BinaryRow layout,
        # then attempt to deserialize it as VARIANT.
        binary_field = DataField(id=0, name='v', type=AtomicType('BYTES'))
        row = GenericRow([b'\x00'], [binary_field])
        serialized = GenericRowSerializer.to_bytes(row)
        with self.assertRaises(ValueError) as ctx:
            GenericRowDeserializer.from_bytes(serialized, [field])
        self.assertIn('VARIANT', str(ctx.exception))

    def test_serialize_raises(self):
        """Serializing a VARIANT field via BinaryRow raises TypeError."""
        from pypaimon.table.row.generic_row import GenericRow
        field = self._field()
        row = GenericRow([{'value': b'\x00', 'metadata': b'\x01\x00'}], [field])
        with self.assertRaises(TypeError) as ctx:
            GenericRowSerializer.to_bytes(row)
        self.assertIn('VARIANT', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
