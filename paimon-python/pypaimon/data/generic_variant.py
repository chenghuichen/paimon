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

"""Python implementation of Paimon GenericVariant.

Mirrors the binary encoding defined by Paimon Java's GenericVariant /
GenericVariantUtil / GenericVariantBuilder, which itself is based on the
Parquet Variant spec (https://github.com/apache/parquet-format/blob/main/VariantEncoding.md).

Primary entry points:
    GenericVariant.from_json(json_str)  – build from a JSON string
    GenericVariant(value, metadata)     – wrap raw bytes from a Parquet/Paimon VARIANT column
    v.to_json()                         – decode back to a JSON string
    v.variant_get('$.field', 'int')     – JSONPath extraction with optional cast
    v.to_python()                       – decode to native Python objects
"""

import base64
import datetime
import decimal as _decimal
import enum
import json as _json
import re
import struct
import uuid as _uuid

# ---------------------------------------------------------------------------
# Constants (matching GenericVariantUtil.java)
# ---------------------------------------------------------------------------

_PRIMITIVE = 0
_SHORT_STR = 1
_OBJECT = 2
_ARRAY = 3

_NULL = 0
_TRUE = 1
_FALSE = 2
_INT1 = 3
_INT2 = 4
_INT4 = 5
_INT8 = 6
_DOUBLE = 7
_DECIMAL4 = 8
_DECIMAL8 = 9
_DECIMAL16 = 10
_DATE = 11
_TIMESTAMP = 12
_TIMESTAMP_NTZ = 13
_FLOAT = 14
_BINARY = 15
_LONG_STR = 16
_UUID = 20

_VERSION = 1
_VERSION_MASK = 0x0F
_BINARY_SEARCH_THRESHOLD = 32
_SIZE_LIMIT = 128 * 1024 * 1024
_MAX_SHORT_STR_SIZE = 0x3F   # 63
_U8_MAX = 255
_U16_MAX = 65535
_U24_MAX = 16777215
_U32_SIZE = 4
_MAX_DECIMAL4_PRECISION = 9
_MAX_DECIMAL8_PRECISION = 18
_MAX_DECIMAL16_PRECISION = 38

# Epoch for date/timestamp conversions
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DT_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_DT_NTZ = datetime.datetime(1970, 1, 1)


class Type(enum.Enum):
    """High-level variant value types (many-to-one from wire types)."""
    OBJECT = 'OBJECT'
    ARRAY = 'ARRAY'
    NULL = 'NULL'
    BOOLEAN = 'BOOLEAN'
    LONG = 'LONG'
    STRING = 'STRING'
    DOUBLE = 'DOUBLE'
    DECIMAL = 'DECIMAL'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'
    TIMESTAMP_NTZ = 'TIMESTAMP_NTZ'
    FLOAT = 'FLOAT'
    BINARY = 'BINARY'
    UUID = 'UUID'


# Populate module-level lookup tables now that Type is defined.
_PRIMITIVE_TYPE_MAP = {
    _NULL: Type.NULL,
    _TRUE: Type.BOOLEAN, _FALSE: Type.BOOLEAN,
    _INT1: Type.LONG, _INT2: Type.LONG, _INT4: Type.LONG, _INT8: Type.LONG,
    _DOUBLE: Type.DOUBLE,
    _DECIMAL4: Type.DECIMAL, _DECIMAL8: Type.DECIMAL, _DECIMAL16: Type.DECIMAL,
    _DATE: Type.DATE,
    _TIMESTAMP: Type.TIMESTAMP,
    _TIMESTAMP_NTZ: Type.TIMESTAMP_NTZ,
    _FLOAT: Type.FLOAT,
    _BINARY: Type.BINARY,
    _LONG_STR: Type.STRING,
    _UUID: Type.UUID,
}
_PRIMITIVE_FIXED_SIZES = {
    _NULL: 1, _TRUE: 1, _FALSE: 1,
    _INT1: 2, _INT2: 3, _INT4: 5, _INT8: 9,
    _DOUBLE: 9, _FLOAT: 5, _DATE: 5,
    _TIMESTAMP: 9, _TIMESTAMP_NTZ: 9,
    _DECIMAL4: 6, _DECIMAL8: 10, _DECIMAL16: 18,
    _UUID: 17,
}
_LONG_FAMILY_SIZES = {
    _INT1: 1, _INT2: 2, _INT4: 4, _INT8: 8,
    _DATE: 4, _TIMESTAMP: 8, _TIMESTAMP_NTZ: 8,
}


# ---------------------------------------------------------------------------
# Low-level binary utilities
# ---------------------------------------------------------------------------

def _read_unsigned(data, pos, n):
    """Read a little-endian unsigned integer of n bytes."""
    return int.from_bytes(data[pos:pos + n], 'little', signed=False)


def _read_signed(data, pos, n):
    """Read a little-endian signed integer of n bytes."""
    return int.from_bytes(data[pos:pos + n], 'little', signed=True)


def _write_le(buf, pos, value, n):
    """Write value as n-byte little-endian into bytearray buf at pos."""
    buf[pos:pos + n] = value.to_bytes(n, 'little')


def _get_int_size(value):
    """Return the minimum number of bytes (1-4) needed for an unsigned int."""
    if value <= _U8_MAX:
        return 1
    if value <= _U16_MAX:
        return 2
    if value <= _U24_MAX:
        return 3
    return 4


def _primitive_header(type_id):
    return (type_id << 2) | _PRIMITIVE


def _short_str_header(size):
    return (size << 2) | _SHORT_STR


def _object_header(large_size, id_size, offset_size):
    return (
        ((1 if large_size else 0) << 6)
        | ((id_size - 1) << 4)
        | ((offset_size - 1) << 2)
        | _OBJECT
    )


def _array_header(large_size, offset_size):
    return (
        ((1 if large_size else 0) << 4)
        | ((offset_size - 1) << 2)
        | _ARRAY
    )


def _get_type(value, pos):
    b = value[pos]
    basic_type = b & 0x3
    type_info = (b >> 2) & 0x3F
    if basic_type == _SHORT_STR:
        return Type.STRING
    if basic_type == _OBJECT:
        return Type.OBJECT
    if basic_type == _ARRAY:
        return Type.ARRAY
    # PRIMITIVE
    t = _PRIMITIVE_TYPE_MAP.get(type_info)
    if t is None:
        raise ValueError(f'Unknown primitive variant type id: {type_info}')
    return t


def _value_size(value, pos):
    """Return the byte size of the variant value starting at pos."""
    b = value[pos]
    basic_type = b & 0x3
    type_info = (b >> 2) & 0x3F
    if basic_type == _SHORT_STR:
        return 1 + type_info
    if basic_type == _OBJECT:
        return _handle_object(
            value, pos,
            lambda size, id_size, offset_size, id_start, offset_start, data_start: (
                data_start - pos + _read_unsigned(
                    value, offset_start + size * offset_size, offset_size)
            )
        )
    if basic_type == _ARRAY:
        return _handle_array(
            value, pos,
            lambda size, offset_size, offset_start, data_start: (
                data_start - pos + _read_unsigned(
                    value, offset_start + size * offset_size, offset_size)
            )
        )
    # PRIMITIVE
    size = _PRIMITIVE_FIXED_SIZES.get(type_info)
    if size is not None:
        return size
    if type_info in (_BINARY, _LONG_STR):
        return 1 + _U32_SIZE + _read_unsigned(value, pos + 1, _U32_SIZE)
    raise ValueError(f'Unknown primitive type id: {type_info}')


def _handle_object(value, pos, handler):
    b = value[pos]
    type_info = (b >> 2) & 0x3F
    large_size = bool((type_info >> 4) & 0x1)
    size_bytes = _U32_SIZE if large_size else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    id_size = ((type_info >> 2) & 0x3) + 1
    offset_size = (type_info & 0x3) + 1
    id_start = pos + 1 + size_bytes
    offset_start = id_start + size * id_size
    data_start = offset_start + (size + 1) * offset_size
    return handler(size, id_size, offset_size, id_start, offset_start, data_start)


def _handle_array(value, pos, handler):
    b = value[pos]
    type_info = (b >> 2) & 0x3F
    large_size = bool((type_info >> 2) & 0x1)
    size_bytes = _U32_SIZE if large_size else 1
    size = _read_unsigned(value, pos + 1, size_bytes)
    offset_size = (type_info & 0x3) + 1
    offset_start = pos + 1 + size_bytes
    data_start = offset_start + (size + 1) * offset_size
    return handler(size, offset_size, offset_start, data_start)


def _get_metadata_key(metadata, key_id):
    offset_size = ((metadata[0] >> 6) & 0x3) + 1
    dict_size = _read_unsigned(metadata, 1, offset_size)
    if key_id >= dict_size:
        raise ValueError('MALFORMED_VARIANT: key id out of range')
    string_start = 1 + (dict_size + 2) * offset_size
    offset = _read_unsigned(metadata, 1 + (key_id + 1) * offset_size, offset_size)
    next_offset = _read_unsigned(metadata, 1 + (key_id + 2) * offset_size, offset_size)
    return metadata[string_start + offset:string_start + next_offset].decode('utf-8')


# ---------------------------------------------------------------------------
# Path parsing (VariantPathSegment equivalent)
# ---------------------------------------------------------------------------

_PATH_INDEX = re.compile(r'\[(\d+)\]')
_PATH_KEY = re.compile(r'\.([^\.\[\'\"]+)|\[\'([^\']+)\'\]|\["([^"]+)"\]')


def _parse_path(path):
    """Parse a JSONPath string like '$.a[0].b' into a list of str/int segments."""
    if not path or path[0] != '$':
        raise ValueError(f'Invalid variant path (must start with $): {path!r}')
    segments = []
    remaining = path[1:]
    while remaining:
        m = _PATH_INDEX.match(remaining)
        if m:
            segments.append(int(m.group(1)))
            remaining = remaining[m.end():]
            continue
        m = _PATH_KEY.match(remaining)
        if m:
            key = m.group(1) or m.group(2) or m.group(3)
            segments.append(key)
            remaining = remaining[m.end():]
            continue
        raise ValueError(f'Invalid variant path segment in {path!r} near {remaining!r}')
    return segments


# ---------------------------------------------------------------------------
# Cast helpers
# ---------------------------------------------------------------------------

def _cast(v, cast_type):
    """Cast a GenericVariant to a Python type specified by cast_type string.

    Supported cast_type values (case-insensitive):
        boolean, int / tinyint / smallint / bigint / long,
        float / double, string / varchar / char,
        date, timestamp, timestamp_ntz, decimal, binary
    """
    ct = cast_type.lower()
    vtype = v.get_type()

    if vtype == Type.NULL:
        return None

    if ct == 'boolean':
        if vtype == Type.BOOLEAN:
            return v.get_boolean()
        if vtype == Type.STRING:
            return v.get_string().lower() == 'true'
        if vtype == Type.LONG:
            return v.get_long() != 0
        return None

    if ct in ('int', 'tinyint', 'smallint', 'bigint', 'long'):
        if vtype == Type.LONG:
            return v.get_long()
        if vtype == Type.DOUBLE:
            return int(v.get_double())
        if vtype == Type.FLOAT:
            return int(v.get_float())
        if vtype == Type.DECIMAL:
            return int(v.get_decimal())
        if vtype == Type.BOOLEAN:
            return 1 if v.get_boolean() else 0
        if vtype == Type.STRING:
            return int(v.get_string())
        return None

    if ct in ('float', 'double'):
        if vtype == Type.DOUBLE:
            return v.get_double()
        if vtype == Type.FLOAT:
            return float(v.get_float())
        if vtype == Type.LONG:
            return float(v.get_long())
        if vtype == Type.DECIMAL:
            return float(v.get_decimal())
        if vtype == Type.STRING:
            return float(v.get_string())
        return None

    if ct in ('string', 'varchar', 'char'):
        if vtype == Type.STRING:
            return v.get_string()
        return v.to_json()

    if ct == 'date':
        if vtype == Type.DATE:
            return _EPOCH_DATE + datetime.timedelta(days=int(v.get_long()))
        if vtype == Type.STRING:
            return datetime.date.fromisoformat(v.get_string())
        return None

    if ct == 'timestamp':
        if vtype == Type.TIMESTAMP:
            micros = v.get_long()
            return _EPOCH_DT_UTC + datetime.timedelta(microseconds=micros)
        return None

    if ct == 'timestamp_ntz':
        if vtype == Type.TIMESTAMP_NTZ:
            micros = v.get_long()
            return _EPOCH_DT_NTZ + datetime.timedelta(microseconds=micros)
        return None

    if ct == 'decimal':
        if vtype == Type.DECIMAL:
            return v.get_decimal()
        if vtype == Type.LONG:
            return _decimal.Decimal(v.get_long())
        if vtype == Type.DOUBLE:
            return _decimal.Decimal(str(v.get_double()))
        if vtype == Type.STRING:
            return _decimal.Decimal(v.get_string())
        return None

    if ct == 'binary':
        if vtype == Type.BINARY:
            return v.get_binary()
        return None

    raise ValueError(f'Unsupported cast_type: {cast_type!r}')


# ---------------------------------------------------------------------------
# GenericVariantBuilder
# ---------------------------------------------------------------------------

class _GenericVariantBuilder:
    """Builds a GenericVariant from Python values or JSON strings.

    Mirrors GenericVariantBuilder.java.
    """

    def __init__(self):
        self._buf = bytearray(128)
        self._pos = 0
        self._dict = {}    # key str -> id int
        self._keys = []    # id -> key bytes

    # -- dict management --

    def _get_or_add_key(self, key):
        if key not in self._dict:
            kid = len(self._keys)
            self._dict[key] = kid
            self._keys.append(key.encode('utf-8'))
        return self._dict[key]

    # -- buffer management --

    def _ensure(self, n):
        needed = self._pos + n
        if needed > len(self._buf):
            new_cap = max(needed, len(self._buf) * 2)
            new_buf = bytearray(new_cap)
            new_buf[:self._pos] = self._buf[:self._pos]
            self._buf = new_buf

    def _write_byte(self, b):
        self._ensure(1)
        self._buf[self._pos] = b & 0xFF
        self._pos += 1

    def _write_le(self, value, n):
        self._ensure(n)
        _write_le(self._buf, self._pos, value, n)
        self._pos += n

    # -- primitives --

    def append_null(self):
        self._write_byte(_primitive_header(_NULL))

    def append_boolean(self, b):
        self._write_byte(_primitive_header(_TRUE if b else _FALSE))

    def append_long(self, n):
        if -(1 << 7) <= n < (1 << 7):
            self._write_byte(_primitive_header(_INT1))
            self._write_le(n & 0xFF, 1)
        elif -(1 << 15) <= n < (1 << 15):
            self._write_byte(_primitive_header(_INT2))
            self._write_le(n & 0xFFFF, 2)
        elif -(1 << 31) <= n < (1 << 31):
            self._write_byte(_primitive_header(_INT4))
            self._write_le(n & 0xFFFFFFFF, 4)
        else:
            self._write_byte(_primitive_header(_INT8))
            self._write_le(n & 0xFFFFFFFFFFFFFFFF, 8)

    def append_double(self, d):
        self._write_byte(_primitive_header(_DOUBLE))
        self._ensure(8)
        struct.pack_into('<d', self._buf, self._pos, d)
        self._pos += 8

    def append_float(self, f):
        self._write_byte(_primitive_header(_FLOAT))
        self._ensure(4)
        struct.pack_into('<f', self._buf, self._pos, f)
        self._pos += 4

    def append_decimal(self, d):
        d = d.normalize()
        # Compute unscaled integer and scale
        sign, digits, exponent = d.as_tuple()
        if exponent > 0:
            # e.g. Decimal('1E+2') — the mantissa alone does not represent the true value.
            # Callers should use append_double() for such values; _try_decimal_or_double
            # handles this automatically when encoding from build_python().
            raise ValueError(
                f'append_decimal requires a non-positive exponent (got {d!r}); '
                'use append_double() for Decimal values with positive exponents'
            )
        unscaled = int(''.join(str(x) for x in digits))
        if sign:
            unscaled = -unscaled
        scale = -exponent if exponent < 0 else 0
        precision = len(digits)

        if scale <= _MAX_DECIMAL4_PRECISION and precision <= _MAX_DECIMAL4_PRECISION:
            self._write_byte(_primitive_header(_DECIMAL4))
            self._write_byte(scale)
            self._write_le(unscaled & 0xFFFFFFFF, 4)
        elif scale <= _MAX_DECIMAL8_PRECISION and precision <= _MAX_DECIMAL8_PRECISION:
            self._write_byte(_primitive_header(_DECIMAL8))
            self._write_byte(scale)
            self._write_le(unscaled & 0xFFFFFFFFFFFFFFFF, 8)
        else:
            self._write_byte(_primitive_header(_DECIMAL16))
            self._write_byte(scale)
            # 16-byte little-endian two's complement
            self._ensure(16)
            raw = unscaled.to_bytes(16, 'little', signed=True)
            self._buf[self._pos:self._pos + 16] = raw
            self._pos += 16

    def append_string(self, s):
        text = s.encode('utf-8')
        if len(text) <= _MAX_SHORT_STR_SIZE:
            self._write_byte(_short_str_header(len(text)))
        else:
            self._write_byte(_primitive_header(_LONG_STR))
            self._write_le(len(text), _U32_SIZE)
        self._ensure(len(text))
        self._buf[self._pos:self._pos + len(text)] = text
        self._pos += len(text)

    def append_binary(self, b):
        self._write_byte(_primitive_header(_BINARY))
        self._write_le(len(b), _U32_SIZE)
        self._ensure(len(b))
        self._buf[self._pos:self._pos + len(b)] = b
        self._pos += len(b)

    def append_date(self, days_since_epoch):
        self._write_byte(_primitive_header(_DATE))
        self._write_le(days_since_epoch & 0xFFFFFFFF, 4)

    def append_timestamp(self, micros_since_epoch):
        self._write_byte(_primitive_header(_TIMESTAMP))
        self._write_le(micros_since_epoch & 0xFFFFFFFFFFFFFFFF, 8)

    def append_timestamp_ntz(self, micros_since_epoch):
        self._write_byte(_primitive_header(_TIMESTAMP_NTZ))
        self._write_le(micros_since_epoch & 0xFFFFFFFFFFFFFFFF, 8)

    # -- composite --

    def _finish_writing_object(self, start, fields):
        """fields: list of (key_str, id_int, offset_int). Modified in-place (sorted)."""
        fields.sort(key=lambda f: f[0])
        for i in range(1, len(fields)):
            if fields[i][0] == fields[i - 1][0]:
                raise ValueError('Duplicate key in variant object')

        size = len(fields)
        data_size = self._pos - start
        large_size = size > _U8_MAX
        size_bytes = _U32_SIZE if large_size else 1
        max_id = max((f[1] for f in fields), default=0)
        id_size = _get_int_size(max_id)
        offset_size = _get_int_size(data_size)
        header_size = 1 + size_bytes + size * id_size + (size + 1) * offset_size

        self._ensure(header_size)
        # Shift field data right to make room for header.
        dst = start + header_size
        src = start
        self._buf[dst:dst + data_size] = self._buf[src:src + data_size]
        self._pos += header_size

        self._buf[start] = _object_header(large_size, id_size, offset_size)
        _write_le(self._buf, start + 1, size, size_bytes)
        id_start = start + 1 + size_bytes
        offset_start = id_start + size * id_size
        for i, (_, fid, offset) in enumerate(fields):
            _write_le(self._buf, id_start + i * id_size, fid, id_size)
            _write_le(self._buf, offset_start + i * offset_size, offset, offset_size)
        _write_le(self._buf, offset_start + size * offset_size, data_size, offset_size)

    def _finish_writing_array(self, start, offsets):
        size = len(offsets)
        data_size = self._pos - start
        large_size = size > _U8_MAX
        size_bytes = _U32_SIZE if large_size else 1
        offset_size = _get_int_size(data_size)
        header_size = 1 + size_bytes + (size + 1) * offset_size

        self._ensure(header_size)
        dst = start + header_size
        self._buf[dst:dst + data_size] = self._buf[start:start + data_size]
        self._pos += header_size

        self._buf[start] = _array_header(large_size, offset_size)
        _write_le(self._buf, start + 1, size, size_bytes)
        offset_start = start + 1 + size_bytes
        for i, off in enumerate(offsets):
            _write_le(self._buf, offset_start + i * offset_size, off, offset_size)
        _write_le(self._buf, offset_start + size * offset_size, data_size, offset_size)

    # -- build from Python value --

    def build_python(self, obj):
        """Recursively encode a Python value into the variant binary buffer."""
        if obj is None:
            self.append_null()
        elif isinstance(obj, bool):   # must be before int check
            self.append_boolean(obj)
        elif isinstance(obj, int):
            self.append_long(obj)
        elif isinstance(obj, float):
            self.append_double(obj)
        elif isinstance(obj, _decimal.Decimal):
            self._try_decimal_or_double(obj)
        elif isinstance(obj, str):
            self.append_string(obj)
        elif isinstance(obj, dict):
            fields = []
            start = self._pos
            for key, val in obj.items():
                fid = self._get_or_add_key(key)
                offset = self._pos - start
                fields.append((key, fid, offset))
                self.build_python(val)
            self._finish_writing_object(start, fields)
        elif isinstance(obj, (list, tuple)):
            elem_offsets = []
            start = self._pos
            for val in obj:
                elem_offsets.append(self._pos - start)
                self.build_python(val)
            self._finish_writing_array(start, elem_offsets)
        elif isinstance(obj, bytes):
            self.append_binary(obj)
        else:
            raise TypeError(f'Unsupported Python type for variant encoding: {type(obj).__name__}')

    def _try_decimal_or_double(self, d):
        """Encode as DECIMAL if precision/scale fit, otherwise as DOUBLE."""
        try:
            sign, digits, exponent = d.as_tuple()
            # Positive exponent means scientific notation (e.g. 1.5e10) → use DOUBLE
            if exponent > 0:
                self.append_double(float(d))
                return
            scale = -exponent if exponent < 0 else 0
            precision = len(digits)
            if scale <= _MAX_DECIMAL16_PRECISION and precision <= _MAX_DECIMAL16_PRECISION:
                self.append_decimal(d)
                return
        except (ArithmeticError, ValueError):
            pass
        self.append_double(float(d))

    # -- result --

    def result(self):
        """Build metadata and return the completed GenericVariant."""
        n_keys = len(self._keys)
        total_str_size = sum(len(k) for k in self._keys)
        max_size = max(total_str_size, n_keys, 0)
        offset_size = _get_int_size(max_size) if max_size > 0 else 1

        # metadata layout:
        #   [0]            : version byte | ((offset_size-1) << 6)
        #   [1..offset_size] : dictSize (n_keys)
        #   [(offset_size+1)..(offset_size+1+(n_keys+1)*offset_size-1)] : offsets
        #   remaining      : UTF-8 key strings
        offset_start = 1 + offset_size
        string_start = offset_start + (n_keys + 1) * offset_size
        metadata_size = string_start + total_str_size

        metadata = bytearray(metadata_size)
        metadata[0] = _VERSION | ((offset_size - 1) << 6)
        _write_le(metadata, 1, n_keys, offset_size)

        current_offset = 0
        for i, key_bytes in enumerate(self._keys):
            _write_le(metadata, offset_start + i * offset_size, current_offset, offset_size)
            klen = len(key_bytes)
            metadata[string_start + current_offset:string_start + current_offset + klen] = key_bytes
            current_offset += klen
        _write_le(metadata, offset_start + n_keys * offset_size, current_offset, offset_size)

        return GenericVariant(bytes(self._buf[:self._pos]), bytes(metadata))


# ---------------------------------------------------------------------------
# GenericVariant
# ---------------------------------------------------------------------------

class GenericVariant:
    """Python representation of a Paimon/Parquet VARIANT value.

    A VARIANT value is stored as two byte arrays:
        value    – encoded payload (Parquet Variant binary spec)
        metadata – key dictionary for object field names

    Typical usage::

        # Construct from a JSON string
        v = GenericVariant.from_json('{"age": 30, "city": "Beijing"}')
        print(v.to_json())                          # '{"age":30,"city":"Beijing"}'
        print(v.variant_get('$.age', 'int'))        # 30
        print(v.variant_get('$.city', 'string'))    # 'Beijing'

        # Construct from raw bytes (e.g. what to_arrow() returns for a VARIANT column)
        row = result.column('payload')[0].as_py()   # {'value': bytes, 'metadata': bytes}
        v = GenericVariant.from_dict(row)
        print(v.to_python())                        # {'age': 30, 'city': 'Beijing'}
    """

    __slots__ = ('_value', '_metadata', '_pos')

    def __init__(self, value: bytes, metadata: bytes, _pos: int = 0):
        self._value = bytes(value)
        self._metadata = bytes(metadata)
        self._pos = _pos
        if len(metadata) < 1 or (metadata[0] & _VERSION_MASK) != _VERSION:
            raise ValueError('MALFORMED_VARIANT: invalid metadata version')

    # -- constructors --

    @classmethod
    def from_json(cls, json_str: str) -> 'GenericVariant':
        """Parse a JSON string and encode it as a VARIANT binary."""
        # parse_float=_decimal.Decimal preserves decimal precision as in Java's tryParseDecimal
        obj = _json.loads(json_str, parse_float=_decimal.Decimal)
        builder = _GenericVariantBuilder()
        builder.build_python(obj)
        return builder.result()

    @classmethod
    def from_python(cls, obj) -> 'GenericVariant':
        """Encode a Python object (dict / list / str / int / float / bool / None) as VARIANT."""
        builder = _GenericVariantBuilder()
        builder.build_python(obj)
        return builder.result()

    @classmethod
    def from_dict(cls, d: dict) -> 'GenericVariant':
        """Wrap raw bytes from a PyArrow VARIANT struct: {'value': bytes, 'metadata': bytes}."""
        return cls(bytes(d['value']), bytes(d['metadata']))

    @classmethod
    def to_arrow_array(cls, variants):
        """Convert a list of GenericVariant (or None) to a PyArrow StructArray.

        The returned array has the canonical VARIANT layout::

            struct<value: binary NOT NULL, metadata: binary NOT NULL>

        Example::

            gv1 = GenericVariant.from_json('{"age":30}')
            gv2 = GenericVariant.from_json('[1,2,3]')
            col = GenericVariant.to_arrow_array([gv1, gv2])
            table = pa.table({'id': [1, 2], 'payload': col})
        """
        import pyarrow as _pa

        values = []
        metadatas = []
        mask = []
        for v in variants:
            if v is None:
                values.append(b'')
                metadatas.append(b'')
                mask.append(True)
            else:
                values.append(v.value())
                metadatas.append(v.metadata())
                mask.append(False)

        variant_type = _pa.struct([
            _pa.field('value', _pa.binary(), nullable=False),
            _pa.field('metadata', _pa.binary(), nullable=False),
        ])
        return _pa.StructArray.from_arrays(
            [_pa.array(values, type=_pa.binary()),
             _pa.array(metadatas, type=_pa.binary())],
            fields=[variant_type[0], variant_type[1]],
            mask=_pa.array(mask, type=_pa.bool_()),
        )

    # -- raw bytes --

    def value(self) -> bytes:
        """Return the value payload bytes (sliced to the exact variant extent)."""
        if self._pos == 0:
            return self._value
        size = _value_size(self._value, self._pos)
        return self._value[self._pos:self._pos + size]

    def metadata(self) -> bytes:
        """Return the metadata (key-dictionary) bytes."""
        return self._metadata

    # -- type introspection --

    def get_type(self) -> Type:
        return _get_type(self._value, self._pos)

    # -- primitive getters --

    def get_boolean(self) -> bool:
        b = self._value[self._pos]
        type_info = (b >> 2) & 0x3F
        if (b & 0x3) != _PRIMITIVE or type_info not in (_TRUE, _FALSE):
            raise TypeError('Expected BOOLEAN variant')
        return type_info == _TRUE

    def get_long(self) -> int:
        b = self._value[self._pos]
        type_info = (b >> 2) & 0x3F
        if (b & 0x3) != _PRIMITIVE:
            raise TypeError('Expected integer/date/timestamp variant')
        n = _LONG_FAMILY_SIZES.get(type_info)
        if n is None:
            raise TypeError(f'Expected LONG-family variant, got type_info={type_info}')
        return _read_signed(self._value, self._pos + 1, n)

    def get_double(self) -> float:
        b = self._value[self._pos]
        if (b & 0x3) != _PRIMITIVE or (b >> 2) & 0x3F != _DOUBLE:
            raise TypeError('Expected DOUBLE variant')
        return struct.unpack_from('<d', self._value, self._pos + 1)[0]

    def get_float(self) -> float:
        b = self._value[self._pos]
        if (b & 0x3) != _PRIMITIVE or (b >> 2) & 0x3F != _FLOAT:
            raise TypeError('Expected FLOAT variant')
        return struct.unpack_from('<f', self._value, self._pos + 1)[0]

    def get_decimal(self) -> _decimal.Decimal:
        b = self._value[self._pos]
        type_info = (b >> 2) & 0x3F
        if (b & 0x3) != _PRIMITIVE or type_info not in (_DECIMAL4, _DECIMAL8, _DECIMAL16):
            raise TypeError('Expected DECIMAL variant')
        scale = self._value[self._pos + 1] & 0xFF
        if type_info == _DECIMAL4:
            unscaled = _read_signed(self._value, self._pos + 2, 4)
        elif type_info == _DECIMAL8:
            unscaled = _read_signed(self._value, self._pos + 2, 8)
        else:
            raw = bytes(self._value[self._pos + 2:self._pos + 18])
            unscaled = int.from_bytes(raw, 'little', signed=True)
        return _decimal.Decimal(unscaled) / (_decimal.Decimal(10) ** scale)

    def get_string(self) -> str:
        b = self._value[self._pos]
        basic_type = b & 0x3
        type_info = (b >> 2) & 0x3F
        if basic_type == _SHORT_STR:
            start = self._pos + 1
            return self._value[start:start + type_info].decode('utf-8')
        if basic_type == _PRIMITIVE and type_info == _LONG_STR:
            length = _read_unsigned(self._value, self._pos + 1, _U32_SIZE)
            start = self._pos + 1 + _U32_SIZE
            return self._value[start:start + length].decode('utf-8')
        raise TypeError('Expected STRING variant')

    def get_binary(self) -> bytes:
        b = self._value[self._pos]
        if (b & 0x3) != _PRIMITIVE or (b >> 2) & 0x3F != _BINARY:
            raise TypeError('Expected BINARY variant')
        length = _read_unsigned(self._value, self._pos + 1, _U32_SIZE)
        start = self._pos + 1 + _U32_SIZE
        return bytes(self._value[start:start + length])

    def get_uuid(self) -> _uuid.UUID:
        b = self._value[self._pos]
        if (b & 0x3) != _PRIMITIVE or (b >> 2) & 0x3F != _UUID:
            raise TypeError('Expected UUID variant')
        raw = bytes(self._value[self._pos + 1:self._pos + 17])
        return _uuid.UUID(bytes=raw)

    # -- object navigation --

    def object_size(self) -> int:
        """Number of fields in an OBJECT variant."""
        return _handle_object(
            self._value, self._pos,
            lambda size, *_: size,
        )

    def get_field_by_key(self, key: str):
        """Return the field GenericVariant for the given key, or None if not found."""
        metadata = self._metadata
        # Pre-parse the metadata header once for the entire lookup.
        meta_offset_size = ((metadata[0] >> 6) & 0x3) + 1
        meta_dict_size = _read_unsigned(metadata, 1, meta_offset_size)
        string_start = 1 + (meta_dict_size + 2) * meta_offset_size

        def _get_key(key_id):
            off = _read_unsigned(metadata, 1 + (key_id + 1) * meta_offset_size, meta_offset_size)
            nxt = _read_unsigned(metadata, 1 + (key_id + 2) * meta_offset_size, meta_offset_size)
            return metadata[string_start + off:string_start + nxt].decode('utf-8')

        def _lookup(size, id_size, offset_size, id_start, offset_start, data_start):
            # Linear scan for small objects, binary search for large ones.
            if size < _BINARY_SEARCH_THRESHOLD:
                for i in range(size):
                    fid = _read_unsigned(self._value, id_start + id_size * i, id_size)
                    if key == _get_key(fid):
                        offset = _read_unsigned(
                            self._value, offset_start + offset_size * i, offset_size)
                        return GenericVariant(self._value, self._metadata,
                                              data_start + offset)
            else:
                lo, hi = 0, size - 1
                while lo <= hi:
                    mid = (lo + hi) >> 1
                    fid = _read_unsigned(self._value, id_start + id_size * mid, id_size)
                    cmp = _get_key(fid)
                    if cmp < key:
                        lo = mid + 1
                    elif cmp > key:
                        hi = mid - 1
                    else:
                        offset = _read_unsigned(
                            self._value, offset_start + offset_size * mid, offset_size)
                        return GenericVariant(self._value, self._metadata,
                                              data_start + offset)
            return None

        return _handle_object(self._value, self._pos, _lookup)

    def get_field_at_index(self, index: int):
        """Return (key, GenericVariant) for the field at position index, or None."""
        def _get(size, id_size, offset_size, id_start, offset_start, data_start):
            if index < 0 or index >= size:
                return None
            fid = _read_unsigned(self._value, id_start + id_size * index, id_size)
            key = _get_metadata_key(self._metadata, fid)
            offset = _read_unsigned(
                self._value, offset_start + offset_size * index, offset_size)
            child = GenericVariant(self._value, self._metadata, data_start + offset)
            return (key, child)

        return _handle_object(self._value, self._pos, _get)

    # -- array navigation --

    def array_size(self) -> int:
        """Number of elements in an ARRAY variant."""
        return _handle_array(self._value, self._pos, lambda size, *_: size)

    def get_element_at_index(self, index: int):
        """Return the element GenericVariant at position index, or None."""
        def _get(size, offset_size, offset_start, data_start):
            if index < 0 or index >= size:
                return None
            offset = _read_unsigned(
                self._value, offset_start + offset_size * index, offset_size)
            return GenericVariant(self._value, self._metadata, data_start + offset)

        return _handle_array(self._value, self._pos, _get)

    # -- high-level API --

    def to_json(self) -> str:
        """Decode the variant to a JSON string."""
        parts = []
        self._to_json_impl(self._value, self._metadata, self._pos, parts)
        return ''.join(parts)

    def _to_json_impl(self, value, metadata, pos, parts):
        vtype = _get_type(value, pos)
        if vtype == Type.OBJECT:
            def _render(size, id_size, offset_size, id_start, offset_start, data_start):
                parts.append('{')
                for i in range(size):
                    fid = _read_unsigned(value, id_start + id_size * i, id_size)
                    key = _get_metadata_key(metadata, fid)
                    offset = _read_unsigned(
                        value, offset_start + offset_size * i, offset_size)
                    if i != 0:
                        parts.append(',')
                    parts.append(_json.dumps(key))
                    parts.append(':')
                    self._to_json_impl(value, metadata, data_start + offset, parts)
                parts.append('}')
            _handle_object(value, pos, _render)
        elif vtype == Type.ARRAY:
            def _render_arr(size, offset_size, offset_start, data_start):
                parts.append('[')
                for i in range(size):
                    offset = _read_unsigned(
                        value, offset_start + offset_size * i, offset_size)
                    if i != 0:
                        parts.append(',')
                    self._to_json_impl(value, metadata, data_start + offset, parts)
                parts.append(']')
            _handle_array(value, pos, _render_arr)
        else:
            sub = GenericVariant(value, metadata, pos)
            if vtype == Type.NULL:
                parts.append('null')
            elif vtype == Type.BOOLEAN:
                parts.append('true' if sub.get_boolean() else 'false')
            elif vtype == Type.LONG:
                parts.append(str(sub.get_long()))
            elif vtype == Type.STRING:
                parts.append(_json.dumps(sub.get_string()))
            elif vtype == Type.DOUBLE:
                d = sub.get_double()
                parts.append(_json.dumps(d) if d == d and d not in (float('inf'), float('-inf'))
                             else _json.dumps(str(d)))
            elif vtype == Type.FLOAT:
                f = sub.get_float()
                parts.append(_json.dumps(float(f)) if f == f and f not in (float('inf'), float('-inf'))
                             else _json.dumps(str(f)))
            elif vtype == Type.DECIMAL:
                parts.append(str(sub.get_decimal().normalize()))
            elif vtype == Type.DATE:
                days = int(sub.get_long())
                parts.append(_json.dumps(str(_EPOCH_DATE + datetime.timedelta(days=days))))
            elif vtype == Type.TIMESTAMP:
                micros = sub.get_long()
                dt = _EPOCH_DT_UTC + datetime.timedelta(microseconds=micros)
                parts.append(_json.dumps(dt.strftime('%Y-%m-%d %H:%M:%S.%f+00:00')))
            elif vtype == Type.TIMESTAMP_NTZ:
                micros = sub.get_long()
                dt = _EPOCH_DT_NTZ + datetime.timedelta(microseconds=micros)
                parts.append(_json.dumps(dt.strftime('%Y-%m-%d %H:%M:%S.%f')))
            elif vtype == Type.BINARY:
                parts.append(_json.dumps(base64.b64encode(sub.get_binary()).decode('ascii')))
            elif vtype == Type.UUID:
                parts.append(_json.dumps(str(sub.get_uuid())))

    def to_python(self):
        """Decode the variant to native Python objects.

        Object  → dict
        Array   → list
        Boolean → bool
        Integer → int
        Double/Float → float
        Decimal → decimal.Decimal
        String  → str
        Date    → datetime.date
        Timestamp → datetime.datetime (UTC-aware)
        Timestamp_NTZ → datetime.datetime (naive)
        Binary  → bytes
        UUID    → str
        Null    → None
        """
        vtype = self.get_type()
        if vtype == Type.NULL:
            return None
        if vtype == Type.BOOLEAN:
            return self.get_boolean()
        if vtype == Type.LONG:
            return self.get_long()
        if vtype == Type.DOUBLE:
            return self.get_double()
        if vtype == Type.FLOAT:
            return float(self.get_float())
        if vtype == Type.DECIMAL:
            return self.get_decimal()
        if vtype == Type.STRING:
            return self.get_string()
        if vtype == Type.DATE:
            return _EPOCH_DATE + datetime.timedelta(days=int(self.get_long()))
        if vtype == Type.TIMESTAMP:
            return _EPOCH_DT_UTC + datetime.timedelta(microseconds=self.get_long())
        if vtype == Type.TIMESTAMP_NTZ:
            return _EPOCH_DT_NTZ + datetime.timedelta(microseconds=self.get_long())
        if vtype == Type.BINARY:
            return self.get_binary()
        if vtype == Type.UUID:
            return str(self.get_uuid())
        if vtype == Type.OBJECT:
            def _build_dict(size, id_size, offset_size, id_start, offset_start, data_start):
                result = {}
                for i in range(size):
                    fid = _read_unsigned(self._value, id_start + id_size * i, id_size)
                    key = _get_metadata_key(self._metadata, fid)
                    offset = _read_unsigned(self._value, offset_start + offset_size * i, offset_size)
                    child = GenericVariant(self._value, self._metadata, data_start + offset)
                    result[key] = child.to_python()
                return result
            return _handle_object(self._value, self._pos, _build_dict)
        if vtype == Type.ARRAY:
            def _build_list(size, offset_size, offset_start, data_start):
                result = []
                for i in range(size):
                    offset = _read_unsigned(self._value, offset_start + offset_size * i, offset_size)
                    result.append(GenericVariant(self._value, self._metadata, data_start + offset).to_python())
                return result
            return _handle_array(self._value, self._pos, _build_list)
        return None

    def variant_get(self, path: str, cast_type: str = None):
        """JSONPath extraction with optional type cast.

        Args:
            path:      JSONPath expression, e.g. '$.age', '$[0].name', '$.tags[1]'
            cast_type: Target type name (case-insensitive), e.g. 'int', 'string',
                       'double', 'boolean', 'date', 'timestamp', 'decimal', 'binary'.
                       If None, returns the native Python value via to_python().

        Returns:
            The extracted value cast to the requested type, or None if the path
            does not exist or the cast is not applicable.
        """
        v = self
        for segment in _parse_path(path):
            if isinstance(segment, str):
                if v.get_type() != Type.OBJECT:
                    return None
                v = v.get_field_by_key(segment)
                if v is None:
                    return None
            else:  # int index
                if v.get_type() != Type.ARRAY:
                    return None
                v = v.get_element_at_index(segment)
                if v is None:
                    return None

        if cast_type is None:
            return v.to_python()
        return _cast(v, cast_type)

    # -- dunder --

    def __repr__(self) -> str:
        return f'GenericVariant({self.to_json()!r})'

    def __str__(self) -> str:
        return self.to_json()

    def __eq__(self, other) -> bool:
        if not isinstance(other, GenericVariant):
            return NotImplemented
        return self.value() == other.value() and self._metadata == other._metadata

    def __hash__(self):
        return hash((self.value(), self._metadata))
