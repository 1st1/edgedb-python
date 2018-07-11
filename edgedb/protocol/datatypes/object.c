/*
* This source file is part of the EdgeDB open source project.
*
* Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


#include "datatypes.h"
#include "freelist.h"
#include "internal.h"


static int init_type_called = 0;
static Py_hash_t base_hash = -1;


EDGE_SETUP_FREELIST(
    EDGE_OBJECT,
    EdgeObject,
    EDGE_OBJECT_FREELIST_MAXSAVE,
    EDGE_OBJECT_FREELIST_SIZE)


#define EdgeObject_GET_ITEM(op, i) \
    (((EdgeObject *)(op))->ob_item[i])
#define EdgeObject_SET_ITEM(op, i, v) \
    (((EdgeObject *)(op))->ob_item[i] = v)


PyObject *
EdgeObject_New(PyObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = EdgeRecordDesc_GetSize(desc);

    EdgeObject *o = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_OBJECT, EdgeObject,
                           &EdgeObject_Type, o, size);
    assert(o != NULL);
    assert(Py_SIZE(o) == size);
    assert(EdgeObject_Check(o));

    Py_INCREF(desc);
    o->desc = desc;

    o->cached_hash = -1;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


int
EdgeObject_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeObject_Check(o));
    EdgeObject *o = (EdgeObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeObject_SET_ITEM(o, i, el);
    return 0;
}


static void
object_dealloc(EdgeObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->desc);
    o->cached_hash = -1;
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_OBJECT, EdgeObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static int
object_traverse(EdgeObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->desc);

    Py_ssize_t i;
    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


static Py_hash_t
object_hash(EdgeObject *o)
{
    if (o->cached_hash == -1) {
        o->cached_hash = _EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
}


static PyObject *
object_getattr(EdgeObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(
        (PyObject *)o->desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_LINKPROP:
        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_AttributeError, name);
            return NULL;

        case L_ATTR: {
            PyObject *val = EdgeObject_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        default:
            abort();
    }
}


static PyObject *
object_repr(EdgeObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "Object{", 7) < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderItems(&writer,
                                 (PyObject *)o, o->desc,
                                 o->ob_item, Py_SIZE(o), 1) < 0)
    {
        goto error;
    }

    if (_PyUnicodeWriter_WriteChar(&writer, '}') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}


PyTypeObject EdgeObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Object",
    .tp_basicsize = sizeof(EdgeObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)object_dealloc,
    .tp_hash = (hashfunc)object_hash,
    .tp_getattro = (getattrofunc)object_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)object_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)object_repr,
};


PyObject *
EdgeObject_InitType(void)
{
    if (PyType_Ready(&EdgeObject_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.Object");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeObject_Type;
}
