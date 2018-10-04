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
cdef class PreparedStatementState:

    def __init__(self, name, query):
        self.name = name
        self.query = query
        self.closed = False
        self.args_desc = None
        self.row_desc = None

        self._c = CodecsRegistry()
        self._dec = None
        self._enc = None

    cdef _set_args_desc(self, bytes data):
        self.args_desc = data
        self._enc = self._c.build_codec(data)

    cdef _set_row_desc(self, bytes data):
        self.row_desc = data
        self._dec = self._c.build_codec(data)

    cdef _encode_args(self, args, kwargs):
        if args and kwargs:
            raise RuntimeError(
                'either positional or named arguments are supported; '
                'not both')

        cdef WriteBuffer buf
        buf = WriteBuffer.new()

        if kwargs:
            if not isinstance(self._enc, NamedTupleCodec):
                raise RuntimeError(
                    'expected positional arguments, got named arguments')

            (<NamedTupleCodec>self._enc).encode_kwargs(buf, kwargs)

        else:
            if not isinstance(self._enc, TupleCodec):
                raise RuntimeError(
                    'expected named arguments, got positional arguments')
            self._enc.encode(buf, args)

        bind_args = bytes(buf)
        return bind_args

    cdef _decode_row(self, const char* cbuf, ssize_t buf_len):
        cdef:
            FRBuffer _rbuf
            FRBuffer *rbuf = &_rbuf

        if PG_DEBUG:
            frb_init(rbuf, cbuf, buf_len)

            flen = hton.unpack_int16(frb_read(rbuf, 2))
            if flen != 1:
                raise RuntimeError(
                    f'invalid number of columns: expected 1 got {flen}')

            buflen = hton.unpack_int32(frb_read(rbuf, 4))
            if frb_get_len(rbuf) != buflen:
                raise RuntimeError('invalid buffer length')
        else:
            # EdgeDB returns rows with one column; Postgres' rows
            # are encoded as follows:
            #   2 bytes - int16 - number of coluns
            #   4 bytes - int32 - every column is prefixed with its length
            # so we want to skip first 6 bytes:
            frb_init(rbuf, cbuf + 6, buf_len - 6)

        return self._dec.decode(rbuf)
