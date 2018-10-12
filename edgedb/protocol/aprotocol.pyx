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

import asyncio
import collections
import time
import weakref

from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_slice_from,
    frb_check,
    frb_set_len,
    frb_get_len,
)

from edgedb.pgproto cimport pgproto
from edgedb.pgproto cimport hton

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from . cimport datatypes
from . cimport cpythonx

include "./consts.pxi"
include "./codecs/codecs.pyx"
include "./prepared_stmt.pyx"


cdef class Protocol:

    def __init__(self, addr, con_params, loop):
        self.buffer = ReadBuffer()

        self.loop = loop
        self.transport = None
        self.connected_fut = loop.create_future()

        self.addr = addr
        self.con_params = con_params

        self.msg_waiter = None
        self.connected = False
        self.backend_secret = None

        self.xact_status = PQTRANS_UNKNOWN
        self.conref = None

    ## APIs for edgedb.Connection

    def set_connection(self, connection):
        self.conref = weakref.ref(connection)

    async def prepare(self, stmt_name, str query):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype
            CodecsRegistry reg
            BaseCodec in_type = None
            BaseCodec out_type = None
            int16_t type_size
            bytes in_type_id
            bytes out_type_id

        if not self.connected:
            raise RuntimeError('not connected')
        if stmt_name:
            raise RuntimeError('named statements are not yet supported')

        buf = WriteBuffer.new_message(b'P')
        buf.write_utf8(stmt_name)
        buf.write_utf8(query)
        buf.end_message()
        buf.write_bytes(FLUSH_MESSAGE)
        self.write(buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'1':
                    in_type_id = self.buffer.read_bytes(16)
                    out_type_id = self.buffer.read_bytes(16)
                    break

                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        reg = self.get_codecs_registry()
        if reg.has_codec(in_type_id):
            in_type = reg.get_codec(in_type_id)
        if reg.has_codec(out_type_id):
            out_type = reg.get_codec(out_type_id)

        if in_type is None or out_type is None:
            n = 0
            if in_type is None:
                n += 1
            if out_type is None:
                n += 1

            buf = WriteBuffer.new_message(b'D')
            buf.write_byte(b'T')
            buf.write_utf8(stmt_name)
            buf.write_int16(n)

            if in_type is None:
                buf.write_bytes(in_type_id)
            if out_type is None:
                buf.write_bytes(out_type_id)
            buf.end_message()
            buf.write_bytes(FLUSH_MESSAGE)
            self.write(buf)

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'T':

                        if in_type is None:
                            type_size = self.buffer.read_int16()
                            in_type_data = self.buffer.read_bytes(type_size)
                            in_type = reg.build_codec(in_type_data)

                        if out_type is None:
                            type_size = self.buffer.read_int16()
                            out_type_data = self.buffer.read_bytes(type_size)
                            out_type = reg.build_codec(out_type_data)

                        break

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()

        return PreparedStatementState(stmt_name, query, in_type, out_type)

    async def execute(self, PreparedStatementState stmt, args, kwargs):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'E')
        buf.write_utf8(stmt.name)
        stmt._encode_args(buf, args, kwargs)
        packet.write_buffer(buf.end_message())

        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = datatypes.EdgeSet_New(0)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    self.parse_data_messages(stmt, result)

                elif mtype == b'C':  # CommandComplete
                    self.buffer.discard_message()

                elif mtype == b'Z':
                    self.parse_sync_message()
                    return result

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        return result

    async def connect(self):
        cdef:
            WriteBuffer ver_buf
            WriteBuffer msg_buf
            WriteBuffer buf
            char mtype
            int32_t status

        if self.connected_fut is not None:
            await self.connected_fut
        if self.connected:
            raise RuntimeError('already connected')
        if self.transport is None:
            raise RuntimeError('no transport object in connect()')

        # protocol version
        ver_buf = WriteBuffer()
        ver_buf.write_int16(1)
        ver_buf.write_int16(0)

        msg_buf = WriteBuffer.new_message(b'0')
        msg_buf.write_utf8(self.con_params.user or '')
        msg_buf.write_utf8(self.con_params.password or '')
        msg_buf.write_utf8(self.con_params.database or '')
        msg_buf.end_message()

        buf = WriteBuffer()
        buf.write_buffer(ver_buf)
        buf.write_buffer(msg_buf)
        self.write(buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'Y':
                self.buffer.discard_message()

            elif mtype == b'R':
                # Authentication...
                status = self.buffer.read_int32()
                if status != 0:
                    self.abort()
                    raise RuntimeError(
                        f'unsupported authentication method requested by the '
                        f'server: {status}')

            elif mtype == b'K':
                # BackendKeyData
                self.backend_secret = self.buffer.read_int32()

            elif mtype == b'E':
                # ErrorResponse
                self.handle_error_message()

            elif mtype == b'Z':
                # ReadyForQuery
                self.parse_sync_message()
                if self.xact_status == PQTRANS_IDLE:
                    self.connected = True
                    return
                else:
                    raise RuntimeError('non-idle status after connect')

            else:
                self.fallthrough()

            self.buffer.finish_message()

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        # TODO:
        # * handle Notice and ServerStatus messages here

        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

    cdef parse_data_messages(self, PreparedStatementState stmt, result):
        cdef:
            ReadBuffer buf = self.buffer
            object rows

            decode_row_method decoder = <decode_row_method>stmt._decode_row
            pgproto.try_consume_message_method try_consume_message = \
                <pgproto.try_consume_message_method>buf.try_consume_message
            pgproto.take_message_type_method take_message_type = \
                <pgproto.take_message_type_method>buf.take_message_type

            const char* cbuf
            ssize_t cbuf_len
            object row

        if PG_DEBUG:
            if buf.get_message_type() != b'D':
                raise RuntimeError('first message is not "D"')

        # if self._discard_data:
        #     while take_message_type(buf, b'D'):
        #         buf.discard_message()
        #     return

        if PG_DEBUG:
            if not datatypes.EdgeSet_Check(result):
                raise RuntimeError(
                    f'result is not an edgedb.Set, but {result!r}')

        while take_message_type(buf, b'D'):
            cbuf = try_consume_message(buf, &cbuf_len)
            if cbuf != NULL:
                row = decoder(stmt, cbuf, cbuf_len)
            else:
                mem = buf.consume_message()
                row = decoder(
                    stmt,
                    cpython.PyBytes_AS_STRING(mem),
                    cpython.PyBytes_GET_SIZE(mem))

            datatypes.EdgeSet_AppendItem(result, row)

    cdef parse_error_message(self):
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

        return parsed

    cdef parse_sync_message(self):
        cdef char status

        assert self.buffer.get_message_type() == b'Z'

        status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

        self.buffer.finish_message()

    cdef handle_error_message(self):
        assert self.buffer.get_message_type() == b'E'
        exc_details = self.parse_error_message()
        raise RuntimeError(str(exc_details))

    ## Private APIs and implementation details

    cdef get_connection(self):
        return self.conref()

    cdef CodecsRegistry get_codecs_registry(self):
        con = self.get_connection()
        if con is None:
            raise RuntimeError('cannot get codec registry, no connection')
        reg = con._codecs_registry
        if reg is None:
            raise RuntimeError('unset codecs registry')
        return <CodecsRegistry>reg

    cpdef abort(self):
        pass

    cdef write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        self.msg_waiter = self.loop.create_future()
        await self.msg_waiter

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None:
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self.msg_waiter is not None and self.buffer.take_message():
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass


## etc

cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())


## Other exports

_RecordDescriptor = datatypes.EdgeRecordDesc_InitType()
Tuple = datatypes.EdgeTuple_InitType()
NamedTuple = datatypes.EdgeNamedTuple_InitType()
Object = datatypes.EdgeObject_InitType()
Set = datatypes.EdgeSet_InitType()
Array = datatypes.EdgeArray_InitType()


_EDGE_POINTER_IS_IMPLICIT = datatypes.EDGE_POINTER_IS_IMPLICIT
_EDGE_POINTER_IS_LINKPROP = datatypes.EDGE_POINTER_IS_LINKPROP


def _create_object_factory(tuple pointers, frozenset linkprops):
    flags = ()
    for name in pointers:
        if name in linkprops:
            flags += (datatypes.EDGE_POINTER_IS_LINKPROP,)
        else:
            flags += (0,)

    desc = datatypes.EdgeRecordDesc_New(pointers, flags)
    size = len(pointers)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = datatypes.EdgeObject_New(desc)
        for i in range(size):
            datatypes.EdgeObject_SetItem(o, i, items[i])

        return o

    return factory
