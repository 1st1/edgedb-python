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
include "./prepared_stmt.pxd"


ctypedef object (*decode_row_method)(object, const char*, ssize_t)


cdef enum TransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status


cdef class Protocol:

    cdef:
        ReadBuffer buffer
        object transport

        bint connected
        object connected_fut
        object conref

        object loop
        object msg_waiter

        readonly object addr
        readonly object con_params

        object backend_secret

        TransactionStatus xact_status

    cdef CodecsRegistry get_codecs_registry(self)
    cdef get_connection(self)
    cdef write(self, WriteBuffer buf)
    cpdef abort(self)
    cdef handle_error_message(self)

    cdef parse_data_messages(self, PreparedStatementState stmt, result)
    cdef parse_error_message(self)
    cdef parse_sync_message(self)

    cdef fallthrough(self)
