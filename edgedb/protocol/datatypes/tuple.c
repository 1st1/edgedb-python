#include "datatypes.h"
#include "freelist.h"
#include "internal.h"


EDGE_SETUP_FREELIST(
    EDGE_TUPLE,
    EdgeTupleObject,
    EDGE_TUPLE_FREELIST_MAXSAVE,
    EDGE_TUPLE_FREELIST_SIZE)


#define EdgeTuple_GET_ITEM(op, i) \
    (((EdgeTupleObject *)(op))->ob_item[i])
#define EdgeTuple_SET_ITEM(op, i, v) \
    (((EdgeTupleObject *)(op))->ob_item[i] = v)


EdgeTupleObject *
EdgeTuple_New(Py_ssize_t size)
{
    EdgeTupleObject *obj = NULL;

    EDGE_NEW_WITH_FREELIST(EDGE_TUPLE, EdgeTupleObject,
                           &EdgeTuple_Type, obj, size)
    assert(obj != NULL);
    assert(EdgeTuple_Check(obj));
    assert(Py_SIZE(obj) == size);

    PyObject_GC_Track(obj);
    return obj;
}


int
EdgeTuple_SetItem(EdgeTupleObject *o, Py_ssize_t i, PyObject *el)
{
    assert(EdgeTuple_Check(o));
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeTuple_SET_ITEM(o, i, el);
    return 0;
}


static void
tuple_dealloc(EdgeTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_TUPLE, EdgeTupleObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static Py_hash_t
tuple_hash(EdgeTupleObject *o)
{
    return _EdgeGeneric_Hash(o->ob_item, Py_SIZE(o));
}


static int
tuple_traverse(EdgeTupleObject *o, visitproc visit, void *arg)
{
    Py_ssize_t i;
    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


static PyObject *
tuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyObject *iterable = NULL;
    EdgeTupleObject *o;

    if (type != &EdgeTuple_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!_Edge_NoKeywords("edgedb.Tuple", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.Tuple", 0, 1, &iterable))
    {
        return NULL;
    }

    if (iterable == NULL) {
        o = EdgeTuple_New(0);
        return (PyObject *)o;
    }

    PyObject *tup = PySequence_Tuple(iterable);
    if (tup == NULL) {
        return NULL;
    }

    o = EdgeTuple_New(Py_SIZE(tup));
    if (o == NULL) {
        Py_DECREF(tup);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(tup); i++) {
        PyObject *el = PyTuple_GET_ITEM(tup, i);
        Py_INCREF(el);
        EdgeTuple_SET_ITEM(o, i, el);
    }
    Py_DECREF(tup);
    return (PyObject *)o;
}


static Py_ssize_t
tuple_length(EdgeTupleObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
tuple_getitem(EdgeTupleObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return NULL;
    }
    PyObject *el = EdgeTuple_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static PyObject *
tuple_repr(EdgeTupleObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteChar(&writer, '(') < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderValues(&writer,
                                  (PyObject *)o, o->ob_item, Py_SIZE(o)) < 0)
    {
        goto error;
    }

    if (_PyUnicodeWriter_WriteChar(&writer, ')') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}


static PySequenceMethods tuple_as_sequence = {
    .sq_length = (lenfunc)tuple_length,
    .sq_item = (ssizeargfunc)tuple_getitem,
};


PyTypeObject EdgeTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Tuple",
    .tp_basicsize = sizeof(EdgeTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)tuple_dealloc,
    .tp_as_sequence = &tuple_as_sequence,
    .tp_hash = (hashfunc)tuple_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)tuple_traverse,
    .tp_new = tuple_new,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)tuple_repr,
};


PyObject *
EdgeTuple_InitType(void)
{
    if (PyType_Ready(&EdgeTuple_Type) < 0) {
        return NULL;
    }

    return (PyObject *)&EdgeTuple_Type;
}
