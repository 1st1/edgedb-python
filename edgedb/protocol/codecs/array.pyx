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


cdef class BaseArrayCodec(BaseCodec):

    def __cinit__(self):
        self.sub_codec = None

    cdef _new_collection(self, Py_ssize_t size):
        raise NotImplementedError

    cdef _set_collection_item(self, object collection, Py_ssize_t i,
                              object element):
        raise NotImplementedError

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, FastReadBuffer buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            FastReadBuffer elem_buf = FastReadBuffer.new()
            int32_t ndims = hton.unpack_int32(buf.read(4))

        buf.read(8)  # ignore flags & element oid

        if ndims > 1:
            raise RuntimeError('only 1-dimensional arrays are supported')

        if ndims == 0:
            return self._new_collection(0)

        assert ndims == 1

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(buf.read(4))
        buf.read(4)  # Ignore the lower bound information

        result = self._new_collection(elem_count)
        for i in range(elem_count):
            elem_len = hton.unpack_int32(buf.read(4))
            if elem_len == -1:
                elem = None
            else:
                elem_buf.slice_from(buf, elem_len)
                elem = self.sub_codec.decode(elem_buf)

            self._set_collection_item(result, i, elem)

        return result

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}\n{self.sub_codec.dump(level + 1)}'


@cython.final
cdef class ArrayCodec(BaseArrayCodec):

    cdef _new_collection(self, Py_ssize_t size):
        return datatypes.EdgeArray_New(size)

    cdef _set_collection_item(self, object collection, Py_ssize_t i,
                              object element):
        datatypes.EdgeArray_SetItem(collection, i, element)

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            ArrayCodec codec

        codec = ArrayCodec.__new__(ArrayCodec)

        codec.tid = tid
        codec.name = 'Array'
        codec.sub_codec = sub_codec

        return codec
