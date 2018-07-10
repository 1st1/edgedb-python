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


# cython: language_level=3


import asyncio
import collections
import time

from . cimport datatypes

include "./pgbase/pgbase.pyx"
include "./codecs/codecs.pyx"

include "coreproto.pyx"
include "prepared_stmt.pyx"


NO_TIMEOUT = object()


cdef class BaseProtocol(CoreProtocol):
    def __init__(self, addr, connected_fut, con_params, loop):
        # type of `con_params` is `_ConnectionParameters`
        CoreProtocol.__init__(self, con_params)

        self.loop = loop
        self.waiter = connected_fut
        self.cancel_waiter = None
        self.cancel_sent_waiter = None

        self.address = addr

        self.return_extra = False

        self.last_query = None

        self.statement = None

        self.closing = False
        self.is_reading = True

        self.timeout_handle = None
        self.timeout_callback = self._on_timeout
        self.completed_callback = self._on_waiter_completed

        self.queries_count = 0

        try:
            self.create_future = loop.create_future
        except AttributeError:
            self.create_future = self._create_future_fallback

    def set_connection(self, connection):
        self.connection = connection

    def is_in_transaction(self):
        # PQTRANS_INTRANS = idle, within transaction block
        # PQTRANS_INERROR = idle, within failed transaction
        return self.xact_status in (PQTRANS_INTRANS, PQTRANS_INERROR)

    cdef inline resume_reading(self):
        if not self.is_reading:
            self.is_reading = True
            self.transport.resume_reading()

    cdef inline pause_reading(self):
        if self.is_reading:
            self.is_reading = False
            self.transport.pause_reading()

    def is_closed(self):
        return self.closing

    def is_connected(self):
        return not self.closing and self.con_status == CONNECTION_OK

    def abort(self):
        if self.closing:
            return
        self.closing = True
        self._handle_waiter_on_connection_lost(None)
        self._terminate()
        self.transport.abort()

    @cython.iterable_coroutine
    async def prepare(self, stmt_name, query):
        waiter = self._new_waiter(None)
        self.statement = PreparedStatementState(stmt_name, query)
        self._prepare(stmt_name, query)  # network op
        return await waiter

    cdef _on_result__prepare(self, object waiter):
        if PG_DEBUG:
            if self.statement is None:
                raise RuntimeError(
                    '_on_result__prepare: statement is None')

        if self.result_param_desc is not None:
            self.statement._set_args_desc(self.result_param_desc)
        if self.result_row_desc is not None:
            self.statement._set_row_desc(self.result_row_desc)
        waiter.set_result(self.statement)

    @cython.iterable_coroutine
    async def bind_execute(self, statement, args):
        assert len(args) == 0

        cdef WriteBuffer buf
        buf = WriteBuffer.new()
        buf.write_int32(0x00010001)  # all arguments are encoded in binary
        buf.write_int16(0)  # number of arguments
        buf.write_int32(0x00010001)  # all columns should be in binary
        bind_args = bytes(buf)

        waiter = self._new_waiter(None)
        self.statement = statement
        self._bind_execute(statement, bind_args)
        return await waiter

    @cython.iterable_coroutine
    async def close(self, timeout):
        if self.closing:
            return

        self.closing = True

        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None

        if self.cancel_waiter is not None:
            await self.cancel_waiter

        if self.waiter is not None:
            # If there is a query running, cancel it
            self._request_cancel()
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None
            if self.cancel_waiter is not None:
                await self.cancel_waiter

        assert self.waiter is None

        timeout = self._get_timeout_impl(timeout)

        # Ask the server to terminate the connection and wait for it
        # to drop.
        self.waiter = self._new_waiter(timeout)
        self._terminate()
        try:
            await self.waiter
        except ConnectionResetError:
            # There appears to be a difference in behaviour of asyncio
            # in Windows, where, instead of calling protocol.connection_lost()
            # a ConnectionResetError will be thrown into the task.
            pass
        finally:
            self.waiter = None
        self.transport.abort()

    def _request_cancel(self):
        self.cancel_waiter = self.create_future()
        self.cancel_sent_waiter = self.create_future()
        self.connection._cancel_current_command(self.cancel_sent_waiter)
        self._set_state(PROTOCOL_CANCELLED)

    def _on_timeout(self, fut):
        if self.waiter is not fut or fut.done() or \
                self.cancel_waiter is not None or \
                self.timeout_handle is None:
            return
        self._request_cancel()
        self.waiter.set_exception(asyncio.TimeoutError())

    def _on_waiter_completed(self, fut):
        if fut is not self.waiter or self.cancel_waiter is not None:
            return
        if fut.cancelled():
            if self.timeout_handle:
                self.timeout_handle.cancel()
                self.timeout_handle = None
            self._request_cancel()

    def _create_future_fallback(self):
        return asyncio.Future(loop=self.loop)

    cdef _handle_waiter_on_connection_lost(self, cause):
        if self.waiter is not None and not self.waiter.done():
            exc = RuntimeError(
                'connection was closed in the middle of '
                'operation')
            if cause is not None:
                exc.__cause__ = cause
            self.waiter.set_exception(exc)
        self.waiter = None

    cdef _set_server_parameter(self, name, val):
        self.settings.add_setting(name, val)

    def _get_timeout(self, timeout):
        if timeout is not None:
            try:
                if type(timeout) is bool:
                    raise ValueError
                timeout = float(timeout)
            except ValueError:
                raise ValueError(
                    'invalid timeout value: expected non-negative float '
                    '(got {!r})'.format(timeout)) from None

        return self._get_timeout_impl(timeout)

    cdef inline _get_timeout_impl(self, timeout):
        if timeout is None:
            timeout = self.connection._config.command_timeout
        elif timeout is NO_TIMEOUT:
            timeout = None
        else:
            timeout = float(timeout)

        if timeout is not None and timeout <= 0:
            raise asyncio.TimeoutError()
        return timeout

    cdef _check_state(self):
        if self.cancel_waiter is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is cancelling')
        if self.closing:
            raise RuntimeError(
                'cannot perform operation: connection is closed')
        if self.waiter is not None or self.timeout_handle is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is in progress')

    def _is_cancelling(self):
        return (
            self.cancel_waiter is not None or
            self.cancel_sent_waiter is not None
        )

    @cython.iterable_coroutine
    async def _wait_for_cancellation(self):
        if self.cancel_sent_waiter is not None:
            await self.cancel_sent_waiter
            self.cancel_sent_waiter = None
        if self.cancel_waiter is not None:
            await self.cancel_waiter

    cdef _coreproto_error(self):
        try:
            if self.waiter is not None:
                if not self.waiter.done():
                    raise RuntimeError(
                        'waiter is not done while handling critical '
                        'protocol error')
                self.waiter = None
        finally:
            self.abort()

    cdef _new_waiter(self, timeout):
        if self.waiter is not None:
            raise RuntimeError(
                'cannot perform operation: another operation is in progress')
        self.waiter = self.create_future()
        if timeout is not None:
            self.timeout_handle = self.connection._loop.call_later(
                timeout, self.timeout_callback, self.waiter)
        self.waiter.add_done_callback(self.completed_callback)
        return self.waiter

    cdef _on_result__connect(self, object waiter):
        waiter.set_result(True)

    cdef _dispatch_result(self):
        waiter = self.waiter
        self.waiter = None

        if PG_DEBUG:
            if waiter is None:
                raise RuntimeError('_on_result: waiter is None')

        if waiter.cancelled():
            return

        if waiter.done():
            raise RuntimeError('_on_result: waiter is done')

        if self.result_type == RESULT_FAILED:
            if isinstance(self.result, dict):
                exc = RuntimeError(
                    self.result, query=self.last_query)
            else:
                exc = self.result
            waiter.set_exception(exc)
            return

        try:
            if self.state == PROTOCOL_AUTH:
                self._on_result__connect(waiter)

            elif self.state == PROTOCOL_PREPARE:
                self._on_result__prepare(waiter)

            else:
                raise RuntimeError(
                    'got result for unknown protocol state {}'.
                    format(self.state))

        except Exception as exc:
            waiter.set_exception(exc)

    cdef _decode_row(self, const char* buf, ssize_t buf_len):
        if PG_DEBUG:
            if self.statement is None:
                raise RuntimeError(
                    '_decode_row: statement is None')

        return self.statement._decode_row(buf, buf_len)

    cdef _on_result(self):
        if self.timeout_handle is not None:
            self.timeout_handle.cancel()
            self.timeout_handle = None

        if self.cancel_waiter is not None:
            # We have received the result of a cancelled command.
            if not self.cancel_waiter.done():
                # The cancellation future might have been cancelled
                # by the cancellation of the entire task running the query.
                self.cancel_waiter.set_result(None)
            self.cancel_waiter = None
            if self.waiter is not None and self.waiter.done():
                self.waiter = None
            if self.waiter is None:
                return

        try:
            self._dispatch_result()
        finally:
            self.statement = None
            self.last_query = None
            self.return_extra = False

    cdef _on_notice(self, parsed):
        self.connection._process_log_message(parsed, self.last_query)

    cdef _on_notification(self, pid, channel, payload):
        self.connection._process_notification(pid, channel, payload)

    cdef _on_connection_lost(self, exc):
        if self.closing:
            # The connection was lost because
            # Protocol.close() was called
            if self.waiter is not None and not self.waiter.done():
                if exc is None:
                    self.waiter.set_result(None)
                else:
                    self.waiter.set_exception(exc)
            self.waiter = None
        else:
            # The connection was lost because it was
            # terminated or due to another error;
            # Throw an error in any awaiting waiter.
            self.closing = True
            self._handle_waiter_on_connection_lost(exc)


class Protocol(BaseProtocol, asyncio.Protocol):
    pass


RecordDescriptor = datatypes.EdgeRecordDesc_InitType()
Tuple = datatypes.EdgeTuple_InitType()
NamedTuple = datatypes.EdgeNamedTuple_InitType()
Object = datatypes.EdgeObject_InitType()
Set = datatypes.EdgeSet_InitType()


def create_object_factory(tuple pointers, frozenset linkprops):
    desc = datatypes.EdgeRecordDesc_New(pointers, linkprops)
    size = len(pointers)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = datatypes.EdgeObject_New(desc)
        for i in range(size):
            datatypes.EdgeObject_SetItem(o, i, items[i])

        return o

    return factory
