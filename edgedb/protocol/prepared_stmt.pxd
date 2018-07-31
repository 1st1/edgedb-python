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


cdef class PreparedStatementState:
    cdef:
        readonly str name
        readonly str query
        readonly bint closed
        readonly bytes args_desc
        readonly bytes row_desc

        FastReadBuffer buffer

        CodecsRegistry _c
        BaseCodec _dec
        BaseCodec _enc

    cdef _set_args_desc(self, bytes data)
    cdef _set_row_desc(self, bytes data)

    cdef _encode_args(self, args, kwargs)

    cdef _decode_row(self, const char* cbuf, ssize_t buf_len)
