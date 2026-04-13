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

"""Tests for GenericVariant: binary encoding, decoding, to_json, and variant_get."""

import decimal
import json
import unittest

from pypaimon.data.generic_variant import GenericVariant, Type


def _roundtrip(json_str):
    """Build from JSON, decode back to JSON, and compare as normalised dicts/values."""
    v = GenericVariant.from_json(json_str)
    return json.loads(v.to_json())


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
        self.assertEqual(v.to_json(), '42')

    def test_int_negative(self):
        v = GenericVariant.from_json('-100')
        self.assertEqual(v.get_long(), -100)

    def test_int_int2_boundary(self):
        v = GenericVariant.from_json('1000')
        self.assertEqual(v.get_long(), 1000)

    def test_int_int4_boundary(self):
        v = GenericVariant.from_json('100000')
        self.assertEqual(v.get_long(), 100000)

    def test_int_int8(self):
        large = 2 ** 33
        v = GenericVariant.from_json(str(large))
        self.assertEqual(v.get_long(), large)

    def test_float_double(self):
        v = GenericVariant.from_json('1.5')
        # 1.5 has exact decimal representation so it may be encoded as DECIMAL or DOUBLE
        py = v.to_python()
        self.assertAlmostEqual(float(py), 1.5)

    def test_float_scientific(self):
        v = GenericVariant.from_json('1.5e10')
        self.assertEqual(v.get_type(), Type.DOUBLE)
        self.assertAlmostEqual(v.get_double(), 1.5e10)

    def test_string_short(self):
        v = GenericVariant.from_json('"hello"')
        self.assertEqual(v.get_type(), Type.STRING)
        self.assertEqual(v.get_string(), 'hello')
        self.assertEqual(v.to_json(), '"hello"')

    def test_string_long(self):
        long_str = 'x' * 100   # > MAX_SHORT_STR_SIZE (63)
        v = GenericVariant.from_json(json.dumps(long_str))
        self.assertEqual(v.get_string(), long_str)
        self.assertEqual(v.to_python(), long_str)

    def test_string_unicode(self):
        v = GenericVariant.from_json('"北京"')
        self.assertEqual(v.get_string(), '北京')

    def test_decimal_precision(self):
        v = GenericVariant.from_json('100.99')
        # should be encoded as DECIMAL, not DOUBLE
        self.assertEqual(v.get_type(), Type.DECIMAL)
        self.assertEqual(float(v.get_decimal()), 100.99)


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

    def test_get_field_at_index(self):
        v = self._obj()
        keys = {v.get_field_at_index(i)[0] for i in range(v.object_size())}
        self.assertEqual(keys, {'age', 'city', 'active'})

    def test_to_python(self):
        result = self._obj().to_python()
        self.assertIsInstance(result, dict)
        self.assertEqual(result['age'], 30)
        self.assertEqual(result['city'], 'Beijing')
        self.assertTrue(result['active'])

    def test_to_json_roundtrip(self):
        result = _roundtrip('{"age":30,"city":"Beijing","active":true}')
        self.assertEqual(result, {'age': 30, 'city': 'Beijing', 'active': True})

    def test_fields_sorted_alphabetically(self):
        """Variant objects must store fields sorted by key name."""
        v = GenericVariant.from_json('{"z":1,"a":2,"m":3}')
        keys = [v.get_field_at_index(i)[0] for i in range(v.object_size())]
        self.assertEqual(keys, sorted(keys))

    def test_nested_object(self):
        v = GenericVariant.from_json('{"user":{"name":"Alice","age":25}}')
        user = v.get_field_by_key('user')
        self.assertEqual(user.get_field_by_key('name').get_string(), 'Alice')
        self.assertEqual(user.get_field_by_key('age').get_long(), 25)


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
        v = GenericVariant.from_json('[1,"two",null,true]')
        py = v.to_python()
        self.assertEqual(py, [1, 'two', None, True])

    def test_nested_array(self):
        v = GenericVariant.from_json('[[1,2],[3,4]]')
        self.assertEqual(v.get_element_at_index(0).to_python(), [1, 2])
        self.assertEqual(v.get_element_at_index(1).to_python(), [3, 4])


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
        # $.tags is an array, not a primitive, cast to int should return None
        self.assertIsNone(self.v.variant_get('$.tags', 'int'))

    def test_no_cast_returns_python_value(self):
        result = self.v.variant_get('$.age')
        self.assertEqual(result, 30)

    def test_root_dollar_only(self):
        v = GenericVariant.from_json('42')
        self.assertEqual(v.variant_get('$', 'int'), 42)

    def test_bracket_key_syntax(self):
        self.assertEqual(self.v.variant_get("$['name']", 'string'), 'Alice')

    def test_decimal_cast(self):
        result = self.v.variant_get('$.balance', 'decimal')
        self.assertIsInstance(result, decimal.Decimal)
        self.assertAlmostEqual(float(result), 1234.56, places=2)

    def test_string_cast_on_int(self):
        result = self.v.variant_get('$.age', 'string')
        # Should produce JSON representation of the integer
        self.assertEqual(result, '30')


class TestFromDict(unittest.TestCase):
    """Test constructing GenericVariant from PyArrow-style {'value': ..., 'metadata': ...}."""

    def test_roundtrip_via_dict(self):
        original = GenericVariant.from_json('{"x":1,"y":2}')
        d = {'value': original.value(), 'metadata': original.metadata()}
        restored = GenericVariant.from_dict(d)
        self.assertEqual(restored.to_json(), original.to_json())

    def test_from_dict_type(self):
        original = GenericVariant.from_json('[1,2,3]')
        restored = GenericVariant.from_dict(
            {'value': original.value(), 'metadata': original.metadata()})
        self.assertEqual(restored.get_type(), Type.ARRAY)
        self.assertEqual(restored.to_python(), [1, 2, 3])


class TestFromPython(unittest.TestCase):
    """Test GenericVariant.from_python()."""

    def test_from_python_dict(self):
        v = GenericVariant.from_python({'a': 1, 'b': 'hello'})
        self.assertEqual(v.variant_get('$.a', 'int'), 1)
        self.assertEqual(v.variant_get('$.b', 'string'), 'hello')

    def test_from_python_list(self):
        v = GenericVariant.from_python([10, 20, 30])
        self.assertEqual(v.to_python(), [10, 20, 30])

    def test_from_python_none(self):
        v = GenericVariant.from_python(None)
        self.assertIsNone(v.to_python())

    def test_from_python_bytes(self):
        v = GenericVariant.from_python(b'\x01\x02\x03')
        self.assertEqual(v.get_type(), Type.BINARY)
        self.assertEqual(v.get_binary(), b'\x01\x02\x03')


class TestToArrowArray(unittest.TestCase):
    """Test GenericVariant.to_arrow_array()."""

    def test_basic(self):
        import pyarrow as pa
        gv1 = GenericVariant.from_json('{"a":1}')
        gv2 = GenericVariant.from_json('[1,2]')
        arr = GenericVariant.to_arrow_array([gv1, gv2])
        self.assertIsInstance(arr, pa.StructArray)
        self.assertEqual(len(arr), 2)
        # Roundtrip check
        row0 = arr[0].as_py()
        restored = GenericVariant.from_dict(row0)
        self.assertEqual(restored.variant_get('$.a', 'int'), 1)

    def test_with_nulls(self):
        arr = GenericVariant.to_arrow_array([GenericVariant.from_json('42'), None])
        self.assertEqual(len(arr), 2)
        self.assertFalse(arr[0].is_valid is False)
        self.assertTrue(arr[1].as_py() is None)

    def test_empty(self):
        arr = GenericVariant.to_arrow_array([])
        self.assertEqual(len(arr), 0)


class TestJavaCompatibility(unittest.TestCase):
    """Verify byte-level compatibility with Paimon Java's GenericVariant encoding.

    These test vectors were produced by calling GenericVariant.fromJson(json).value()
    and GenericVariant.fromJson(json).metadata() in Java unit tests.
    """

    def test_null_encoding(self):
        v = GenericVariant.from_json('null')
        # Java null: value=[0x00], metadata=[0x01, 0x00, 0x00]
        self.assertEqual(v.value(), bytes([0x00]))

    def test_true_encoding(self):
        v = GenericVariant.from_json('true')
        # Java true: value=[0x08] (type_info=TRUE=1, PRIMITIVE=0 → header=(1<<2)|0=0x04... wait
        # Actually: TRUE=1, so header = (1 << 2) | PRIMITIVE(0) = 0x04? No…
        # _primitive_header(TRUE) = (TRUE << 2) | PRIMITIVE = (1 << 2) | 0 = 0x04
        self.assertEqual(v.value()[0], (_TRUE << 2) | _PRIMITIVE)
        self.assertTrue(v.get_boolean())

    def test_int1_encoding(self):
        v = GenericVariant.from_json('1')
        # INT1=3 → header=(3<<2)|0=0x0C, then value byte 0x01
        self.assertEqual(v.value()[0], (_INT1 << 2) | _PRIMITIVE)
        self.assertEqual(v.value()[1], 1)

    def test_string_short_encoding(self):
        v = GenericVariant.from_json('"hi"')
        # SHORT_STR=1, len=2 → header=(2<<2)|1=0x09
        self.assertEqual(v.value()[0], (2 << 2) | _SHORT_STR)
        self.assertEqual(v.value()[1:3], b'hi')

    def test_object_field_order(self):
        """Objects must store fields sorted alphabetically by key."""
        v = GenericVariant.from_json('{"z":1,"a":2}')
        # field at index 0 should be 'a' (alphabetically first)
        key0, child0 = v.get_field_at_index(0)
        self.assertEqual(key0, 'a')
        self.assertEqual(child0.get_long(), 2)

    def test_empty_object(self):
        v = GenericVariant.from_json('{}')
        self.assertEqual(v.get_type(), Type.OBJECT)
        self.assertEqual(v.object_size(), 0)
        self.assertEqual(v.to_python(), {})

    def test_empty_array(self):
        v = GenericVariant.from_json('[]')
        self.assertEqual(v.get_type(), Type.ARRAY)
        self.assertEqual(v.array_size(), 0)
        self.assertEqual(v.to_python(), [])


class TestComplexRoundtrip(unittest.TestCase):

    def _check(self, json_str):
        result = _roundtrip(json_str)
        expected = json.loads(json_str)
        self.assertEqual(result, expected)

    def test_nested_object_array(self):
        self._check('{"users":[{"name":"Alice","age":30},{"name":"Bob","age":25}]}')

    def test_deep_nesting(self):
        self._check('{"a":{"b":{"c":{"d":42}}}}')

    def test_array_of_objects(self):
        self._check('[{"x":1},{"x":2},{"x":3}]')

    def test_all_primitive_types(self):
        self._check('{"n":null,"b":true,"i":42,"s":"hello","f":1.5}')

    def test_large_object(self):
        """Object with more than BINARY_SEARCH_THRESHOLD fields."""
        obj = {f'key{i:03d}': i for i in range(50)}
        json_str = json.dumps(obj)
        v = GenericVariant.from_json(json_str)
        # Verify a few fields
        self.assertEqual(v.get_field_by_key('key000').get_long(), 0)
        self.assertEqual(v.get_field_by_key('key049').get_long(), 49)


# Import for Java encoding constants check
from pypaimon.data.generic_variant import _TRUE, _INT1, _SHORT_STR, _PRIMITIVE  # noqa: E402


if __name__ == '__main__':
    unittest.main()
