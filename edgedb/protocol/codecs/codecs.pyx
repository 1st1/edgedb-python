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


import codecs
import uuid


@cython.final
cdef class Codec:

    def __cinit__(self):
        self.c_encoder = NULL
        self.c_decoder = NULL
        self.encoder = NULL
        self.decoder = NULL

    def __init__(self, bytes tid, str name, CodecType type):
        self.tid = tid
        self.name = name
        self.type = type

    cdef encode_namedtuple(self, CodecContext ctx, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode_namedtuple(self, CodecContext ctx, FastReadBuffer buf):
        cdef:
            tuple result
            ssize_t elem_count
            ssize_t i
            int32_t elem_len
            uint32_t elem_typ
            Codec elem_codec
            FastReadBuffer elem_buf = FastReadBuffer.new()

        elem_count = <ssize_t><uint32_t>hton.unpack_int32(buf.read(4))

        if elem_count != len(self.fields_codecs):
            raise RuntimeError(
                f'cannot decode namedtuple: expected {len(self.fields_codecs)}'
                f'elements, got {elem_count}')

        result = cpython.PyTuple_New(elem_count)

        for i in range(elem_count):
            elem_typ = <uint32_t>hton.unpack_int32(buf.read(4))
            elem_len = hton.unpack_int32(buf.read(4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <Codec>self.fields_codecs[i]
                elem = elem_codec.decode(elem_buf.slice_from(buf, elem_len))

            cpython.Py_INCREF(elem)
            cpython.PyTuple_SET_ITEM(result, i, elem)

        return result

    cdef encode_scalar(self, CodecContext ctx, WriteBuffer buf, object obj):
        self.c_encoder(ctx, buf, obj)

    cdef decode_scalar(self, CodecContext ctx, FastReadBuffer buf):
        return self.c_decoder(ctx, buf)

    cdef encode(self, WriteBuffer buf, object obj):
        self.encoder(self, DEFAULT_CODEC_CONTEXT, buf, obj)

    cdef decode(self, FastReadBuffer buf):
        return self.decoder(self, DEFAULT_CODEC_CONTEXT, buf)

    @staticmethod
    cdef Codec new_base_scalar_codec(
            bytes tid, str name,
            encode_func encoder, decode_func decoder):

        cdef Codec codec

        codec = Codec(tid, name, CODEC_C_SCALAR)
        codec.c_encoder = encoder
        codec.c_decoder = decoder
        codec.encoder = <codec_encode_func>&Codec.encode_scalar
        codec.decoder = <codec_decode_func>&Codec.decode_scalar
        return codec

    @staticmethod
    cdef Codec new_named_tuple_codec(
            bytes tid, list fields_names, list fields_codecs):

        cdef Codec codec

        codec = Codec(tid, 'namedtuple', CODEC_NAMEDTUPLE)
        codec.fields_names = fields_names
        codec.fields_codecs = fields_codecs
        codec.encoder = <codec_encode_func>&Codec.encode_namedtuple
        codec.decoder = <codec_decode_func>&Codec.decode_namedtuple
        return codec


cdef class CodecsRegistry:

    def __init__(self):
        self.codecs = {}

    cdef Codec _build_decoder(self, FastReadBuffer spec, list codecs_list):
        cdef:
            const char *t = spec.read(1)
            bytes tid = spec.read(16)[:16]
            uint16_t els
            uint16_t i
            uint16_t str_len
            uint16_t pos
            Codec res

        res = self.codecs.get(tid)
        if res is not None:
            return res

        if t[0] == 0:
            # set
            raise NotImplementedError

        elif t[0] == 1:
            # shape
            raise NotImplementedError

        elif t[0] == 2:
            # base scalar
            return <Codec>BASE_SCALAR_CODECS[tid]

        elif t[0] == 3:
            # scalar
            raise NotImplementedError

        elif t[0] == 4:
            # tuple
            raise NotImplementedError

        elif t[0] == 5:
            # named tuple
            els = <uint16_t>hton.unpack_int16(spec.read(2))
            names = []
            codecs = []
            for i in range(els):
                str_len = <uint16_t>hton.unpack_int16(spec.read(2))
                name = PyUnicode_FromStringAndSize(
                    spec.read(str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(spec.read(2))

                names.append(name)
                codecs.append(codecs_list[pos])

            return Codec.new_named_tuple_codec(tid, names, codecs)

        elif t[0] == 6:
            # array
            raise NotImplementedError

        raise NotImplementedError

    cdef Codec build_decoder(self, bytes spec):
        print('>>>>', spec, '<<<<')
        cdef:
            FastReadBuffer buf
            Codec res
            list codecs_list

        buf = FastReadBuffer.new()
        buf.buf = cpython.PyBytes_AsString(spec)
        buf.len = cpython.Py_SIZE(spec)

        codecs_list = []
        while buf.len:
            res = self._build_decoder(buf, codecs_list)
            codecs_list.append(res)
            self.codecs[res.tid] = res

        return res


cdef dict BASE_SCALAR_CODECS = {}


@cython.final
cdef class EdegDBCodecContext(CodecContext):

    def __cinit__(self):
        self._codec = codecs.lookup('utf-8')

    cpdef get_text_codec(self):
        return self._codec

    cdef is_encoding_utf8(self):
        return True


cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()


cdef register_base_scalar_codec(
        str name, str id, encode_func encoder, decode_func decoder):

    cdef:
        bytes tid
        Codec codec

    tid = uuid.UUID(id).bytes
    if tid in BASE_SCALAR_CODECS:
        raise RuntimeError(f'base scalar codec for {id} is already registered')

    codec = Codec.new_base_scalar_codec(tid, name, encoder, decoder)
    BASE_SCALAR_CODECS[tid] = codec


cdef register_base_scalar_codecs():
    register_base_scalar_codec(
        'std::uuid',
        '00000000-0000-0000-0000-000000000002',
        pgbase_uuid_encode,
        pgbase_uuid_decode)

    register_base_scalar_codec(
        'std::str',
        '00000000-0000-0000-0000-000000000003',
        pgbase_text_encode,
        pgbase_text_decode)

    register_base_scalar_codec(
        'std::bytes',
        '00000000-0000-0000-0000-000000000004',
        pgbase_bytea_encode,
        pgbase_bytea_decode)

    register_base_scalar_codec(
        'std::int16',
        '00000000-0000-0000-0000-000000000005',
        pgbase_int2_encode,
        pgbase_int2_decode)

    register_base_scalar_codec(
        'std::int32',
        '00000000-0000-0000-0000-000000000006',
        pgbase_int4_encode,
        pgbase_int4_decode)

    register_base_scalar_codec(
        'std::int64',
        '00000000-0000-0000-0000-000000000007',
        pgbase_int8_encode,
        pgbase_int8_decode)

    register_base_scalar_codec(
        'std::float32',
        '00000000-0000-0000-0000-000000000008',
        pgbase_float4_encode,
        pgbase_float4_decode)

    register_base_scalar_codec(
        'std::float64',
        '00000000-0000-0000-0000-000000000009',
        pgbase_float8_encode,
        pgbase_float8_decode)

    register_base_scalar_codec(
        'std::decimal',
        '00000000-0000-0000-0000-00000000000A',
        pgbase_numeric_encode_binary,
        pgbase_numeric_decode_binary)

    register_base_scalar_codec(
        'std::bool',
        '00000000-0000-0000-0000-00000000000B',
        pgbase_bool_encode,
        pgbase_bool_decode)

    register_base_scalar_codec(
        'std::datetime',
        '00000000-0000-0000-0000-00000000000C',
        pgbase_timestamptz_encode,
        pgbase_timestamptz_decode)

    register_base_scalar_codec(
        'std::date',
        '00000000-0000-0000-0000-00000000000D',
        pgbase_date_encode,
        pgbase_date_decode)

    register_base_scalar_codec(
        'std::time',
        '00000000-0000-0000-0000-00000000000E',
        pgbase_time_encode,
        pgbase_time_decode)

    register_base_scalar_codec(
        'std::timedelta',
        '00000000-0000-0000-0000-00000000000F',
        pgbase_interval_encode,
        pgbase_interval_decode)

    register_base_scalar_codec(
        'std::json',
        '00000000-0000-0000-0000-000000000010',
        pgbase_jsonb_encode,
        pgbase_jsonb_decode)


register_base_scalar_codecs()
