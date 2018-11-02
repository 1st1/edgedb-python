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


DEF CTYPE_SET = 0
DEF CTYPE_SHAPE = 1
DEF CTYPE_BASE_SCALAR = 2
DEF CTYPE_SCALAR = 3
DEF CTYPE_TUPLE = 4
DEF CTYPE_NAMEDTUPLE = 5
DEF CTYPE_ARRAY = 6

DEF _CODECS_BUILD_CACHE_SIZE = 200



cdef class CodecsRegistry:

    def __init__(self, *, cache_size=1000):
        self.codecs_build_cache = LRUMapping(maxsize=_CODECS_BUILD_CACHE_SIZE)
        self.codecs = LRUMapping(maxsize=cache_size)

    cdef BaseCodec _build_codec(self, FRBuffer *spec, list codecs_list):
        cdef:
            char t = frb_read(spec, 1)[0]
            bytes tid = frb_read(spec, 16)[:16]
            uint16_t els
            uint16_t i
            uint16_t str_len
            uint16_t pos
            BaseCodec res
            BaseCodec sub_codec


        res = self.codecs.get(tid, None)
        if res is None:
            res = self.codecs_build_cache.get(tid, None)
        if res is not None:
            # We have a codec for this "tid"; advance the buffer
            # so that we can process the next codec.

            if t == CTYPE_SET:
                frb_read(spec, 2)

            elif t == CTYPE_SHAPE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 1)
                    str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_BASE_SCALAR:
                pass

            elif t == CTYPE_SCALAR:
                frb_read(spec, 2)

            elif t == CTYPE_TUPLE:
                # tuple
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 2)

            elif t == CTYPE_NAMEDTUPLE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_ARRAY:
                frb_read(spec, 2)

            else:
                raise NotImplementedError

            return res

        if t == CTYPE_SET:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = SetCodec.new(tid, sub_codec)

        elif t == CTYPE_SHAPE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            flags = cpython.PyTuple_New(els)
            for i in range(els):
                flag = <uint8_t>frb_read(spec, 1)[0]

                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

                cpython.Py_INCREF(flag)
                cpython.PyTuple_SetItem(flags, i, flag)

            res = ObjectCodec.new(tid, names, flags, codecs)

        elif t == CTYPE_BASE_SCALAR:
            res = <BaseCodec>BASE_SCALAR_CODECS[tid]

        elif t == CTYPE_SCALAR:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            res = <BaseCodec>codecs_list[pos]

        elif t == CTYPE_TUPLE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            for i in range(els):
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = TupleCodec.new(tid, codecs)

        elif t == CTYPE_NAMEDTUPLE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = NamedTupleCodec.new(tid, names, codecs)

        elif t == CTYPE_ARRAY:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = ArrayCodec.new(tid, sub_codec)

        else:
            raise NotImplementedError

        self.codecs_build_cache[tid] = res
        return res

    cdef has_codec(self, bytes type_id):
        return type_id in self.codecs

    cdef BaseCodec get_codec(self, bytes type_id):
        codec = self.codecs[type_id]
        return <BaseCodec>codec

    cdef BaseCodec build_codec(self, bytes spec):
        cdef:
            FRBuffer buf
            BaseCodec res
            list codecs_list

        frb_init(
            &buf,
            cpython.PyBytes_AsString(spec),
            cpython.Py_SIZE(spec))

        codecs_list = []
        while frb_get_len(&buf):
            res = self._build_codec(&buf, codecs_list)
            codecs_list.append(res)
            self.codecs[res.tid] = res

        return res


cdef dict BASE_SCALAR_CODECS = {}


cdef register_base_scalar_codec(
        str name, str id,
        pgproto.encode_func encoder,
        pgproto.decode_func decoder):

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
        '00000000-0000-0000-0000-000000000003',
        pgproto.uuid_encode,
        pgproto.uuid_decode)

    register_base_scalar_codec(
        'std::str',
        '00000000-0000-0000-0000-000000000004',
        pgproto.text_encode,
        pgproto.text_decode)

    register_base_scalar_codec(
        'std::bytes',
        '00000000-0000-0000-0000-000000000005',
        pgproto.bytea_encode,
        pgproto.bytea_decode)

    register_base_scalar_codec(
        'std::int16',
        '00000000-0000-0000-0000-000000000006',
        pgproto.int2_encode,
        pgproto.int2_decode)

    register_base_scalar_codec(
        'std::int32',
        '00000000-0000-0000-0000-000000000007',
        pgproto.int4_encode,
        pgproto.int4_decode)

    register_base_scalar_codec(
        'std::int64',
        '00000000-0000-0000-0000-000000000008',
        pgproto.int8_encode,
        pgproto.int8_decode)

    register_base_scalar_codec(
        'std::float32',
        '00000000-0000-0000-0000-000000000009',
        pgproto.float4_encode,
        pgproto.float4_decode)

    register_base_scalar_codec(
        'std::float64',
        '00000000-0000-0000-0000-00000000000A',
        pgproto.float8_encode,
        pgproto.float8_decode)

    register_base_scalar_codec(
        'std::decimal',
        '00000000-0000-0000-0000-00000000000B',
        pgproto.numeric_encode_binary,
        pgproto.numeric_decode_binary)

    register_base_scalar_codec(
        'std::bool',
        '00000000-0000-0000-0000-00000000000C',
        pgproto.bool_encode,
        pgproto.bool_decode)

    register_base_scalar_codec(
        'std::datetime',
        '00000000-0000-0000-0000-00000000000D',
        pgproto.timestamptz_encode,
        pgproto.timestamptz_decode)

    register_base_scalar_codec(
        'std::date',
        '00000000-0000-0000-0000-00000000000E',
        pgproto.date_encode,
        pgproto.date_decode)

    register_base_scalar_codec(
        'std::time',
        '00000000-0000-0000-0000-00000000000F',
        pgproto.time_encode,
        pgproto.time_decode)

    register_base_scalar_codec(
        'std::timedelta',
        '00000000-0000-0000-0000-000000000010',
        pgproto.interval_encode,
        pgproto.interval_decode)

    register_base_scalar_codec(
        'std::json',
        '00000000-0000-0000-0000-000000000011',
        pgproto.jsonb_encode,
        pgproto.jsonb_decode)


register_base_scalar_codecs()
