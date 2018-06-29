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



cdef enum ConnectionStatus:
    CONNECTION_OK = 1
    CONNECTION_BAD = 2
    CONNECTION_STARTED = 3           # Waiting for connection to be made.


cdef enum ProtocolState:
    PROTOCOL_IDLE = 0

    PROTOCOL_FAILED = 1
    PROTOCOL_ERROR_CONSUME = 2
    PROTOCOL_CANCELLED = 3

    PROTOCOL_AUTH = 10
    PROTOCOL_PREPARE = 20
    PROTOCOL_BIND_EXECUTE = 21


cdef enum AuthenticationMessage:
    AUTH_SUCCESSFUL = 0


cdef enum ResultType:
    RESULT_OK = 1
    RESULT_FAILED = 2


cdef enum TransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status


ctypedef object (*decode_row_method)(object, const char*, ssize_t)


cdef class CoreProtocol:
    cdef:
        ReadBuffer buffer
        bint _skip_discard
        bint _discard_data

        # executemany support data
        object _execute_iter
        str _execute_portal_name
        str _execute_stmt_name

        ConnectionStatus con_status
        ProtocolState state
        TransactionStatus xact_status

        str encoding

        object transport

        # Instance of _ConnectionParameters
        object con_params

        readonly int32_t backend_secret

        ## Result
        ResultType result_type
        object result
        bytes result_data
        bytes result_status_msg

    cdef _process__auth(self, char mtype)
    cdef _process__prepare(self, char mtype)
    cdef _process__bind_execute(self, char mtype)

    cdef _parse_data_msgs(self)
    cdef _parse_msg_authentication(self)
    cdef _parse_msg_parameter_status(self)
    cdef _parse_msg_notification(self)
    cdef _parse_msg_backend_key_data(self)
    cdef _parse_msg_ready_for_query(self)
    cdef _parse_msg_error_response(self, is_error)
    cdef _parse_msg_command_complete(self)

    cdef _write(self, buf)
    cdef inline _write_sync_message(self)

    cdef _read_server_messages(self)

    cdef _push_result(self)
    cdef _reset_result(self)
    cdef _set_state(self, ProtocolState new_state)

    cdef _ensure_connected(self)

    cdef _prepare(self, str stmt_name, str query)
    cdef _bind_execute(self, PreparedStatementState statement, bytes bind_args)

    cdef _connect(self)
    cdef _terminate(self)

    cdef _on_result(self)
    cdef _on_notification(self, pid, channel, payload)
    cdef _on_notice(self, parsed)
    cdef _set_server_parameter(self, name, val)
    cdef _on_connection_lost(self, exc)

    cdef _decode_row(self, const char* buf, ssize_t buf_len)
