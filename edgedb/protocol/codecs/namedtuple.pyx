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


@cython.final
cdef class NamedTupleCodec(BaseCodec):

    def __cinit__(self):
        self.descriptor = None
        self.fields_codecs = ()

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, FastReadBuffer buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            BaseCodec elem_codec
            FastReadBuffer elem_buf = FastReadBuffer.new()

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(buf.read(4))

        if elem_count != len(self.fields_codecs):
            raise RuntimeError(
                f'cannot decode NamedTuple: expected {len(self.fields_codecs)} '
                f'elements, got {elem_count}')

        result = datatypes.EdgeNamedTuple_New(self.descriptor)

        for i in range(elem_count):
            buf.read(4)  # ignore element type oid
            elem_len = hton.unpack_int32(buf.read(4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <BaseCodec>self.fields_codecs[i]
                elem = elem_codec.decode(elem_buf.slice_from(buf, elem_len))

            datatypes.EdgeNamedTuple_SetItem(result, i, elem)

        return result

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple fields_names, tuple fields_codecs):
        cdef:
            NamedTupleCodec codec

        codec = NamedTupleCodec.__new__(NamedTupleCodec)

        codec.tid = tid
        codec.name = 'NamedTuple'
        codec.descriptor = datatypes.EdgeRecordDesc_New(
            fields_names, <object>NULL)
        codec.fields_codecs = fields_codecs

        return codec
