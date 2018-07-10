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


cimport cpython


cdef extern from "datatypes/datatypes.h":

    object EdgeRecordDesc_InitType()
    object EdgeRecordDesc_New(object, object)

    object EdgeTuple_InitType()

    object EdgeNamedTuple_InitType()

    object EdgeObject_InitType()
    object EdgeObject_New(object);
    int EdgeObject_SetItem(object, Py_ssize_t, object) except -1;

    object EdgeSet_InitType()

    object EdgeArray_InitType()
