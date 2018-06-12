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

    cdef _set_args_desc(self, bytes data):
        self.args_desc = data

    cdef _set_row_desc(self, bytes data):
        self.row_desc = data
