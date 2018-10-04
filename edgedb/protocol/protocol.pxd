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


cimport cython
cimport cpython

from libc.stdint cimport int16_t, int32_t, uint16_t, \
                         uint32_t, int64_t, uint64_t

from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
    FRBuffer,
)

from edgedb.pgproto cimport pgproto
from edgedb.pgproto.debug cimport PG_DEBUG


include "./codecs/codecs.pxd"

include "./coreproto.pxd"
include "./prepared_stmt.pxd"


cdef class BaseProtocol(CoreProtocol):

    cdef:
        object loop
        object address
        object cancel_sent_waiter
        object cancel_waiter
        object waiter
        bint return_extra
        object create_future
        object timeout_handle
        object timeout_callback
        object completed_callback
        object connection
        bint is_reading

        PreparedStatementState statement

        str last_query

        bint writing_paused
        bint closing

        readonly uint64_t queries_count

    cdef _get_timeout_impl(self, timeout)
    cdef _check_state(self)
    cdef _new_waiter(self, timeout)
    cdef _coreproto_error(self)

    cdef _on_result__connect(self, object waiter)
    cdef _on_result__prepare(self, object waiter)
    cdef _on_result__bind_execute(self, object waiter)
    cdef _on_result__simple_query(self, object waiter)

    cdef _handle_waiter_on_connection_lost(self, cause)

    cdef _dispatch_result(self)

    cdef inline resume_reading(self)
    cdef inline pause_reading(self)
