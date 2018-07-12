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
cdef class SetCodec(BaseArrayCodec):

    cdef _new_collection(self, Py_ssize_t size):
        return datatypes.EdgeSet_New(size)

    cdef _set_collection_item(self, object collection, Py_ssize_t i,
                              object element):
        datatypes.EdgeSet_SetItem(collection, i, element)

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            SetCodec codec

        codec = SetCodec.__new__(SetCodec)

        codec.tid = tid
        codec.name = 'Set'
        codec.sub_codec = sub_codec

        return codec
