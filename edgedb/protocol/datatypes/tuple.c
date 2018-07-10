#include "datatypes.h"
#include "freelist.h"


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

    PyObject_GC_Track(obj);
    return obj;
}


static void
tuple_dealloc(EdgeTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_TUPLE, EdgeTupleObject, o);
}


static Py_hash_t
tuple_hash(EdgeTupleObject *o)
{
    return EdgeTupleLike_Hash(o->ob_item, Py_SIZE(o));
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

    if (!Edge_NoKeywords("edgedb.tuple", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.tuple", 0, 1, &iterable))
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
    .tp_basicsize = sizeof(EdgeTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)tuple_dealloc,
    .tp_as_sequence = &tuple_as_sequence,
    .tp_hash = (hashfunc)tuple_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)tuple_traverse,
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


Py_hash_t
EdgeTupleLike_Hash(PyObject **els, Py_ssize_t len)
{
    Py_uhash_t x;  /* Unsigned for defined overflow behavior. */
    PyObject **p = els;
    Py_hash_t y;
    Py_uhash_t mult;

    mult = _PyHASH_MULTIPLIER;
    x = 0x345678UL;
    while (--len >= 0) {
        y = PyObject_Hash(*p++);
        if (y == -1) {
            return -1;
        }
        x = (x ^ (Py_uhash_t)y) * mult;
        /* the cast might truncate len; that doesn't change hash stability */
        mult += (Py_uhash_t)(82520UL + (size_t)len + (size_t)len);
    }
    x += 97531UL;
    if (x == (Py_uhash_t)-1) {
        x = (Py_uhash_t)-2;
    }
    return (Py_hash_t)x;
}
