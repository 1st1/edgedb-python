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


cdef class BaseCodec:

    def __init__(self):
        raise RuntimeError(
            'codecs are not supposed to be instantiated directly')

    def __cinit__(self):
        self.tid = None
        self.name = None

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, FastReadBuffer buf):
        raise NotImplementedError


@cython.final
cdef class EdegDBCodecContext(CodecContext):

    def __cinit__(self):
        self._codec = codecs.lookup('utf-8')

    cpdef get_text_codec(self):
        return self._codec

    cdef is_encoding_utf8(self):
        return True


cdef EdegDBCodecContext DEFAULT_CODEC_CONTEXT = EdegDBCodecContext()
