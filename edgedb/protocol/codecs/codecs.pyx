#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import uuid


include "./base.pyx"
include "./scalar.pyx"
include "./tuple.pyx"
include "./namedtuple.pyx"
include "./object.pyx"
include "./array.pyx"
include "./set.pyx"


cdef class CodecsRegistry:

    def __init__(self):
        self.codecs = {}

    cdef BaseCodec _build_codec(self, FastReadBuffer spec, list codecs_list):
        cdef:
            const char *t = spec.read(1)
            bytes tid = spec.read(16)[:16]
            uint16_t els
            uint16_t i
            uint16_t str_len
            uint16_t pos
            BaseCodec res
            BaseCodec sub_codec


        res = self.codecs.get(tid)
        if res is not None:
            if t[0] == 0:
                # set
                spec.read(2)

            elif t[0] == 1:
                # shape
                els = <uint16_t>hton.unpack_int16(spec.read(2))
                for i in range(els):
                    spec.read(1)
                    str_len = <uint16_t>hton.unpack_int16(spec.read(2))
                    spec.read(str_len + 2)

            elif t[0] == 2:
                # base scalar
                pass

            elif t[0] == 3:
                # scalar
                spec.read(2)

            elif t[0] == 4:
                # tuple
                els = <uint16_t>hton.unpack_int16(spec.read(2))
                for i in range(els):
                    spec.read(2)

            elif t[0] == 5:
                # named tuple
                els = <uint16_t>hton.unpack_int16(spec.read(2))
                for i in range(els):
                    str_len = <uint16_t>hton.unpack_int16(spec.read(2))
                    spec.read(str_len + 2)

            elif t[0] == 6:
                # array
                spec.read(2)

            else:
                raise NotImplementedError

            return res

        if t[0] == 0:
            # set
            pos = <uint16_t>hton.unpack_int16(spec.read(2))
            sub_codec = <BaseCodec>codecs_list[pos]
            return SetCodec.new(tid, sub_codec)

        elif t[0] == 1:
            # shape
            els = <uint16_t>hton.unpack_int16(spec.read(2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            flags = cpython.PyTuple_New(els)
            for i in range(els):
                flag = <uint8_t>spec.read(1)[0]

                str_len = <uint16_t>hton.unpack_int16(spec.read(2))
                name = PyUnicode_FromStringAndSize(
                    spec.read(str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(spec.read(2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

                cpython.Py_INCREF(flag)
                cpython.PyTuple_SetItem(flags, i, flag)

            return ObjectCodec.new(tid, names, flags, codecs)

        elif t[0] == 2:
            # base scalar
            return <BaseCodec>BASE_SCALAR_CODECS[tid]

        elif t[0] == 3:
            # scalar
            pos = <uint16_t>hton.unpack_int16(spec.read(2))
            return <BaseCodec>codecs_list[pos]

        elif t[0] == 4:
            # tuple
            els = <uint16_t>hton.unpack_int16(spec.read(2))
            codecs = cpython.PyTuple_New(els)
            for i in range(els):
                pos = <uint16_t>hton.unpack_int16(spec.read(2))

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            return TupleCodec.new(tid, codecs)

        elif t[0] == 5:
            # named tuple
            els = <uint16_t>hton.unpack_int16(spec.read(2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = <uint16_t>hton.unpack_int16(spec.read(2))
                name = PyUnicode_FromStringAndSize(
                    spec.read(str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(spec.read(2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            return NamedTupleCodec.new(tid, names, codecs)

        elif t[0] == 6:
            # array
            pos = <uint16_t>hton.unpack_int16(spec.read(2))
            sub_codec = <BaseCodec>codecs_list[pos]
            return ArrayCodec.new(tid, sub_codec)

        else:
            raise NotImplementedError

    cdef BaseCodec build_codec(self, bytes spec):
        cdef:
            FastReadBuffer buf
            BaseCodec res
            list codecs_list

        buf = FastReadBuffer.new()
        buf.buf = cpython.PyBytes_AsString(spec)
        buf.len = cpython.Py_SIZE(spec)

        codecs_list = []
        while buf.len:
            res = self._build_codec(buf, codecs_list)
            codecs_list.append(res)
            self.codecs[res.tid] = res

        return res


cdef dict BASE_SCALAR_CODECS = {}


cdef register_base_scalar_codec(
        str name, str id, encode_func encoder, decode_func decoder):

    cdef:
        bytes tid
        BaseCodec codec

    tid = uuid.UUID(id).bytes
    if tid in BASE_SCALAR_CODECS:
        raise RuntimeError(f'base scalar codec for {id} is already registered')

    codec = ScalarCodec.new(tid, name, encoder, decoder)
    BASE_SCALAR_CODECS[tid] = codec


cdef register_base_scalar_codecs():
    register_base_scalar_codec(
        'std::uuid',
        '00000000-0000-0000-0000-000000000001',
        pgbase_uuid_encode,
        pgbase_uuid_decode)

    register_base_scalar_codec(
        'std::str',
        '00000000-0000-0000-0000-000000000002',
        pgbase_text_encode,
        pgbase_text_decode)

    register_base_scalar_codec(
        'std::bytes',
        '00000000-0000-0000-0000-000000000003',
        pgbase_bytea_encode,
        pgbase_bytea_decode)

    register_base_scalar_codec(
        'std::int16',
        '00000000-0000-0000-0000-000000000004',
        pgbase_int2_encode,
        pgbase_int2_decode)

    register_base_scalar_codec(
        'std::int32',
        '00000000-0000-0000-0000-000000000005',
        pgbase_int4_encode,
        pgbase_int4_decode)

    register_base_scalar_codec(
        'std::int64',
        '00000000-0000-0000-0000-000000000006',
        pgbase_int8_encode,
        pgbase_int8_decode)

    register_base_scalar_codec(
        'std::float32',
        '00000000-0000-0000-0000-000000000007',
        pgbase_float4_encode,
        pgbase_float4_decode)

    register_base_scalar_codec(
        'std::float64',
        '00000000-0000-0000-0000-000000000008',
        pgbase_float8_encode,
        pgbase_float8_decode)

    register_base_scalar_codec(
        'std::decimal',
        '00000000-0000-0000-0000-000000000009',
        pgbase_numeric_encode_binary,
        pgbase_numeric_decode_binary)

    register_base_scalar_codec(
        'std::bool',
        '00000000-0000-0000-0000-00000000000A',
        pgbase_bool_encode,
        pgbase_bool_decode)

    register_base_scalar_codec(
        'std::datetime',
        '00000000-0000-0000-0000-00000000000B',
        pgbase_timestamptz_encode,
        pgbase_timestamptz_decode)

    register_base_scalar_codec(
        'std::date',
        '00000000-0000-0000-0000-00000000000C',
        pgbase_date_encode,
        pgbase_date_decode)

    register_base_scalar_codec(
        'std::time',
        '00000000-0000-0000-0000-00000000000D',
        pgbase_time_encode,
        pgbase_time_decode)

    register_base_scalar_codec(
        'std::timedelta',
        '00000000-0000-0000-0000-00000000000E',
        pgbase_interval_encode,
        pgbase_interval_decode)

    register_base_scalar_codec(
        'std::json',
        '00000000-0000-0000-0000-00000000000F',
        pgbase_jsonb_encode,
        pgbase_jsonb_decode)


register_base_scalar_codecs()
