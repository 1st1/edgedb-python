#include "datatypes.h"


#define TUPLE_SIZE(o) Py_SIZE(o)


static Py_hash_t
tuple_hash(EdgeBaseObject *v)
{
    return EdgeBaseObject_Hash(v, TUPLE_SIZE(v));
}


static PyObject *
tuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyObject *iterable = NULL;
    EdgeBaseObject *o;

    if (type != &EdgeTuple_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!Edge_NoKeywords("edgedb.tuple", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.tuple", 0, 1, &iterable))
    {
        return NULL;
    }

    if (iterable == NULL) {
        o = EdgeBaseObject_New(&EdgeTuple_Type, 0);
        return (PyObject *)o;
    }

    PyObject *tup = PySequence_Tuple(iterable);
    if (tup == NULL) {
        return NULL;
    }

    o = EdgeBaseObject_New(&EdgeTuple_Type, Py_SIZE(tup));
    if (o == NULL) {
        Py_DECREF(tup);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(tup); i++) {
        PyObject *el = PyTuple_GET_ITEM(tup, i);
        Py_INCREF(el);
        EdgeBase_SET_ITEM(o, i, el);
    }
    Py_DECREF(tup);
    return (PyObject *)o;
}


static Py_ssize_t
tuple_length(EdgeBaseObject *o)
{
    return TUPLE_SIZE(o);
}


static PyObject *
tuple_getitem(EdgeBaseObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= TUPLE_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return NULL;
    }
    Py_INCREF(o->ob_item[i]);
    return o->ob_item[i];
}


static PySequenceMethods tuple_as_sequence = {
    .sq_length = (lenfunc)tuple_length,
    .sq_item = (ssizeargfunc)tuple_getitem,
};


PyTypeObject EdgeTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Tuple",
    .tp_basicsize = sizeof(EdgeBaseObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)EdgeBaseObject_Dealloc,
    .tp_as_sequence = &tuple_as_sequence,
    .tp_hash = (hashfunc)tuple_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)EdgeBaseObject_Traverse,
    .tp_new = tuple_new,
    .tp_free = PyObject_GC_Del
};


PyObject *
EdgeTuple_InitType(void)
{
    if (PyType_Ready(&EdgeTuple_Type) < 0) {
        return NULL;
    }

    return (PyObject *)&EdgeTuple_Type;
}
