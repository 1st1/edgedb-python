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


import socket

from hashlib import md5 as hashlib_md5  # for MD5 authentication


cdef class CoreProtocol:

    def __init__(self, con_params):
        # type of `con_params` is `_ConnectionParameters`
        self.buffer = ReadBuffer()
        self.con_params = con_params
        self.transport = None
        self.con_status = CONNECTION_BAD
        self.state = PROTOCOL_IDLE
        self.xact_status = PQTRANS_IDLE
        self.encoding = 'utf-8'

        self._skip_discard = False

        # executemany support data
        self._execute_iter = None
        self._execute_portal_name = None
        self._execute_stmt_name = None

        self._reset_result()

    cdef _write(self, buf):
        self.transport.write(memoryview(buf))

    cdef inline _write_sync_message(self):
        self.transport.write(SYNC_MESSAGE)

    cdef _read_server_messages(self):
        cdef:
            char mtype
            ProtocolState state

        while self.buffer.has_message() == 1:
            mtype = self.buffer.get_message_type()
            state = self.state

            try:
                if mtype == b'S':
                    # ParameterStatus
                    self._parse_msg_parameter_status()
                    continue
                elif mtype == b'A':
                    # NotificationResponse
                    self._parse_msg_notification()
                    continue
                elif mtype == b'N':
                    # 'N' - NoticeResponse
                    self._on_notice(self._parse_msg_error_response(False))
                    continue

                if state == PROTOCOL_AUTH:
                    self._process__auth(mtype)

                elif state == PROTOCOL_PREPARE:
                    self._process__prepare(mtype)

                elif state == PROTOCOL_BIND_EXECUTE:
                    self._process__bind_execute(mtype)

                elif state == PROTOCOL_CANCELLED:
                    # discard all messages until the sync message
                    if mtype == b'E':
                        self._parse_msg_error_response(True)
                    elif mtype == b'Z':
                        self._parse_msg_ready_for_query()
                        self._push_result()
                    else:
                        self.buffer.consume_message()

                elif state == PROTOCOL_ERROR_CONSUME:
                    # Error in protocol (on asyncpg side);
                    # discard all messages until sync message

                    if mtype == b'Z':
                        # Sync point, self to push the result
                        if self.result_type != RESULT_FAILED:
                            self.result_type = RESULT_FAILED
                            self.result = RuntimeError(
                                'unknown error in protocol implementation')

                        self._push_result()

                    else:
                        self.buffer.consume_message()

                else:
                    raise RuntimeError(
                        'protocol is in an unknown state {}'.format(state))

            except Exception as ex:
                self.result_type = RESULT_FAILED
                self.result = ex

                if mtype == b'Z':
                    self._push_result()
                else:
                    self.state = PROTOCOL_ERROR_CONSUME

            finally:
                if self._skip_discard:
                    self._skip_discard = False
                else:
                    self.buffer.discard_message()

    cdef _process__auth(self, char mtype):
        if mtype == b'Y':
            self.buffer.discard_message()

        elif mtype == b'R':
            # Authentication...
            self._parse_msg_authentication()
            if self.result_type != RESULT_OK:
                self.con_status = CONNECTION_BAD
                self._push_result()
                self.transport.close()

        elif mtype == b'K':
            # BackendKeyData
            self._parse_msg_backend_key_data()

        elif mtype == b'E':
            # ErrorResponse
            self.con_status = CONNECTION_BAD
            self._parse_msg_error_response(True)
            self._push_result()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self.con_status = CONNECTION_OK
            self._push_result()

    cdef _process__prepare(self, char mtype):
        if mtype == b'1':
            # ParseComplete
            # TODO recv in/out ids
            self.buffer.consume_message()

        elif mtype == b'T':
            # Query description

            desc_len = self.buffer.read_int16()
            data_desc = self.buffer.read(desc_len)
            desc_len = self.buffer.read_int16()
            param_desc = self.buffer.read(desc_len)

            self.result_row_desc = (<Memory>data_desc).as_bytes()
            self.result_param_desc = (<Memory>param_desc).as_bytes()

        elif mtype == b'n':
            # NoData -- Query doesn't return data
            self.buffer.consume_message()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

    cdef _process__bind_execute(self, char mtype):
        if mtype == b'D':
            self._parse_data_msgs()

    cdef _parse_data_msgs(self):
        cdef:
            ReadBuffer buf = self.buffer
            list rows
            decode_row_method decoder = <decode_row_method>self._decode_row

            const char* cbuf
            ssize_t cbuf_len
            object row
            Memory mem

        if PG_DEBUG:
            if buf.get_message_type() != b'D':
                raise RuntimeError(
                    '_parse_data_msgs: first message is not "D"')

        if self._discard_data:
            while True:
                buf.consume_message()
                if not buf.has_message() or buf.get_message_type() != b'D':
                    self._skip_discard = True
                    return

        if PG_DEBUG:
            if type(self.result) is not list:
                raise RuntimeError(
                    '_parse_data_msgs: result is not a list, but {!r}'.
                    format(self.result))

        rows = self.result
        while True:
            cbuf = buf.try_consume_message(&cbuf_len)
            if cbuf != NULL:
                row = decoder(self, cbuf, cbuf_len)
            else:
                mem = buf.consume_message()
                row = decoder(self, mem.buf, mem.length)

            cpython.PyList_Append(rows, row)

            if not buf.has_message() or buf.get_message_type() != b'D':
                self._skip_discard = True
                return

    cdef _parse_msg_command_complete(self):
        cdef:
            const char* cbuf
            ssize_t cbuf_len

        cbuf = self.buffer.try_consume_message(&cbuf_len)
        if cbuf != NULL and cbuf_len > 0:
            msg = cpython.PyBytes_FromStringAndSize(cbuf, cbuf_len - 1)
        else:
            msg = self.buffer.read_cstr()
        self.result_status_msg = msg

    cdef _parse_msg_backend_key_data(self):
        self.backend_secret = self.buffer.read_int32()

    cdef _parse_msg_parameter_status(self):
        name = self.buffer.read_cstr()
        name = name.decode(self.encoding)

        val = self.buffer.read_cstr()
        val = val.decode(self.encoding)

        self._set_server_parameter(name, val)

    cdef _parse_msg_notification(self):
        pid = self.buffer.read_int32()
        channel = self.buffer.read_cstr().decode(self.encoding)
        payload = self.buffer.read_cstr().decode(self.encoding)
        self._on_notification(pid, channel, payload)

    cdef _parse_msg_authentication(self):
        cdef:
            int32_t status
            bytes md5_salt

        status = self.buffer.read_int32()

        if status == AUTH_SUCCESSFUL:
            # AuthenticationOk
            self.result_type = RESULT_OK
        else:
            self.result_type = RESULT_FAILED
            self.result = RuntimeError(
                'unsupported authentication method requested by the '
                'server: {}'.format(status))

        self.buffer.consume_message()

    cdef _parse_msg_ready_for_query(self):
        cdef char status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

    cdef _parse_msg_error_response(self, is_error):
        cdef:
            char code
            bytes message
            dict parsed = {}

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_cstr()

            parsed[chr(code)] = message.decode()

        if is_error:
            self.result_type = RESULT_FAILED
            self.result = parsed
        else:
            return parsed

    cdef _push_result(self):
        try:
            self._on_result()
        finally:
            self._set_state(PROTOCOL_IDLE)
            self._reset_result()

    cdef _reset_result(self):
        self.result_type = RESULT_OK
        self.result = None
        self.result_data = b''
        self.result_status_msg = None
        self._discard_data = False

    cdef _set_state(self, ProtocolState new_state):
        if new_state == PROTOCOL_IDLE:
            if self.state == PROTOCOL_FAILED:
                raise RuntimeError(
                    'cannot switch to "idle" state; '
                    'protocol is in the "failed" state')
            elif self.state == PROTOCOL_IDLE:
                raise RuntimeError(
                    'protocol is already in the "idle" state')
            else:
                self.state = new_state

        elif new_state == PROTOCOL_FAILED:
            self.state = PROTOCOL_FAILED

        elif new_state == PROTOCOL_CANCELLED:
            self.state = PROTOCOL_CANCELLED

        else:
            if self.state == PROTOCOL_IDLE:
                self.state = new_state

            elif self.state == PROTOCOL_FAILED:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'protocol is in the "failed" state'.format(new_state))
            else:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'another operation ({}) is in progress'.format(
                        new_state, self.state))

    cdef _ensure_connected(self):
        if self.con_status != CONNECTION_OK:
            raise RuntimeError('not connected')

    # API for subclasses

    cdef _connect(self):
        cdef:
            WriteBuffer ver_buf
            WriteBuffer msg_buf
            WriteBuffer buf

        if self.con_status != CONNECTION_BAD:
            raise RuntimeError('already connected')

        self._set_state(PROTOCOL_AUTH)
        self.con_status = CONNECTION_STARTED

        # protocol version
        ver_buf = WriteBuffer()
        ver_buf.write_int16(1)
        ver_buf.write_int16(0)

        msg_buf = WriteBuffer.new_message(b'0')
        msg_buf.write_utf8(self.con_params.user)
        msg_buf.write_utf8(self.con_params.password)
        msg_buf.write_utf8(self.con_params.database)
        msg_buf.end_message()

        buf = WriteBuffer()
        buf.write_buffer(ver_buf)
        buf.write_buffer(msg_buf)
        self._write(buf)

    cdef _prepare(self, str stmt_name, str query):
        cdef:
            WriteBuffer packet
            WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_PREPARE)

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'P')
        buf.write_utf8(stmt_name)
        buf.write_utf8(query)
        buf.end_message()
        packet.write_buffer(buf)

        buf = WriteBuffer.new_message(b'D')
        buf.write_byte(b'S')
        buf.write_utf8(stmt_name)
        buf.end_message()
        packet.write_buffer(buf)

        packet.write_bytes(SYNC_MESSAGE)

        self._write(packet)

    cdef _bind_execute(self, PreparedStatementState statement,
                       bytes bind_args):
        cdef:
            WriteBuffer packet
            WriteBuffer buf

        self._ensure_connected()
        self._set_state(PROTOCOL_BIND_EXECUTE)
        self.result = []

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'E')
        buf.write_utf8(statement.name)
        buf.write_bytes(bind_args)
        packet.write_buffer(buf.end_message())

        packet.write_bytes(SYNC_MESSAGE)
        self._write(packet)
        print('send BIND EXECUTE')

    cdef _terminate(self):
        cdef WriteBuffer buf
        self._ensure_connected()
        buf = WriteBuffer.new_message(b'X')
        buf.end_message()
        self._write(buf)

    cdef _set_server_parameter(self, name, val):
        pass

    cdef _on_result(self):
        pass

    cdef _on_notice(self, parsed):
        pass

    cdef _on_notification(self, pid, channel, payload):
        pass

    cdef _on_connection_lost(self, exc):
        pass

    cdef _decode_row(self, const char* buf, ssize_t buf_len):
        pass

    # asyncio callbacks:

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

        sock = transport.get_extra_info('socket')
        if (sock is not None and
              (not hasattr(socket, 'AF_UNIX')
               or sock.family != socket.AF_UNIX)):
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_NODELAY, 1)

        try:
            self._connect()
        except Exception as ex:
            transport.abort()
            self.con_status = CONNECTION_BAD
            self._set_state(PROTOCOL_FAILED)
            self._on_error(ex)

    def connection_lost(self, exc):
        self.con_status = CONNECTION_BAD
        self._set_state(PROTOCOL_FAILED)
        self._on_connection_lost(exc)


cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
