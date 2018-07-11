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


cdef enum CodecType:

    CODEC_UNDEFINED    = 0
    CODEC_C_SCALAR     = 1
    CODEC_TUPLE        = 2
    CODEC_NAMEDTUPLE   = 3


ctypedef object (*codec_encode_func)(Codec codec,
                                     CodecContext ctx,
                                     WriteBuffer buf,
                                     object obj)

ctypedef object (*codec_decode_func)(Codec codec,
                                     CodecContext ctx,
                                     FastReadBuffer buf)


cdef class Codec:

    cdef:
        bytes           tid
        str             name
        CodecType       type

        tuple           fields_names
        list            fields_codecs
        object          desc

        encode_func     c_encoder
        decode_func     c_decoder

        codec_encode_func encoder
        codec_decode_func decoder

    cdef encode_namedtuple(self, CodecContext ctx, WriteBuffer buf, object obj)
    cdef decode_namedtuple(self, CodecContext ctx, FastReadBuffer buf)

    cdef encode_scalar(self, CodecContext ctx, WriteBuffer buf, object obj)
    cdef decode_scalar(self, CodecContext ctx, FastReadBuffer buf)

    cdef inline encode(self, WriteBuffer buf, object obj)
    cdef inline decode(self, FastReadBuffer buf)

    @staticmethod
    cdef Codec new_base_scalar_codec(
        bytes tid, str name,
        encode_func encoder, decode_func decoder)

    @staticmethod
    cdef Codec new_named_tuple_codec(
        bytes tid, tuple fields_names, list fields_codecs)


cdef class CodecsRegistry:

    cdef:
        dict codecs

    cdef Codec _build_decoder(self, FastReadBuffer spec, list codecs_list)
    cdef Codec build_decoder(self, bytes spec)


cdef class EdegDBCodecContext(CodecContext):
    cdef:
        object _codec
