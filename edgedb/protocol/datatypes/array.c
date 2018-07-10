#include "datatypes.h"
#include "freelist.h"


static int init_type_called = 0;
static Py_hash_t base_hash = -1;


EDGE_SETUP_FREELIST(
    EDGE_ARRAY,
    EdgeArrayObject,
    EDGE_ARRAY_FREELIST_MAXSAVE,
    EDGE_ARRAY_FREELIST_SIZE)


#define EdgeArray_GET_ITEM(op, i) \
    (((EdgeArrayObject *)(op))->ob_item[i])
#define EdgeArray_SET_ITEM(op, i, v) \
    (((EdgeArrayObject *)(op))->ob_item[i] = v)


EdgeArrayObject *
EdgeArray_New(Py_ssize_t size)
{
    EdgeArrayObject *obj = NULL;

    EDGE_NEW_WITH_FREELIST(EDGE_ARRAY, EdgeArrayObject,
                           &EdgeArray_Type, obj, size)
    assert(obj != NULL);
    assert(EdgeArray_Check(obj));
    assert(Py_SIZE(obj) == size);

    obj->cached_hash = -1;

    PyObject_GC_Track(obj);
    return obj;
}


int
EdgeArray_SetItem(EdgeArrayObject *o, Py_ssize_t i, PyObject *el)
{
    assert(EdgeArray_Check(o));
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeArray_SET_ITEM(o, i, el);
    return 0;
}


static void
array_dealloc(EdgeArrayObject *o)
{
    o->cached_hash = -1;
    PyObject_GC_UnTrack(o);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_ARRAY, EdgeArrayObject, o);
}


static Py_hash_t
array_hash(EdgeArrayObject *o)
{
     if (o->cached_hash == -1) {
        o->cached_hash = EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
}


static int
array_traverse(EdgeArrayObject *o, visitproc visit, void *arg)
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
array_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyObject *iterable = NULL;
    EdgeArrayObject *o;

    if (type != &EdgeArray_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!Edge_NoKeywords("edgedb.Array", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.Array", 0, 1, &iterable))
    {
        return NULL;
    }

    if (iterable == NULL) {
        o = EdgeArray_New(0);
        return (PyObject *)o;
    }

    PyObject *tup = PySequence_Tuple(iterable);
    if (tup == NULL) {
        return NULL;
    }

    o = EdgeArray_New(Py_SIZE(tup));
    if (o == NULL) {
        Py_DECREF(tup);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(tup); i++) {
        PyObject *el = PyTuple_GET_ITEM(tup, i);
        Py_INCREF(el);
        EdgeArray_SET_ITEM(o, i, el);
    }
    Py_DECREF(tup);
    return (PyObject *)o;
}


static Py_ssize_t
array_length(EdgeArrayObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
array_getitem(EdgeArrayObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "array index out of range");
        return NULL;
    }
    PyObject *el = EdgeArray_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static PySequenceMethods array_as_sequence = {
    .sq_length = (lenfunc)array_length,
    .sq_item = (ssizeargfunc)array_getitem,
};


PyTypeObject EdgeArray_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Array",
    .tp_basicsize = sizeof(EdgeArrayObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)array_dealloc,
    .tp_as_sequence = &array_as_sequence,
    .tp_hash = (hashfunc)array_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)array_traverse,
    .tp_new = array_new,
    .tp_free = PyObject_GC_Del
};


PyObject *
EdgeArray_InitType(void)
{
    if (PyType_Ready(&EdgeArray_Type) < 0) {
        return NULL;
    }

    base_hash = EdgeGeneric_HashString("edgedb.Array");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeArray_Type;
}
