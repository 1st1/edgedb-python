#include "datatypes.h"


#define TUPLE_SIZE(o) (Py_SIZE(o) - 1)
#define NAMEDTUPLE_CHECK(o) (Py_TYPE(o) == &EdgeNamedTuple_Type)
#define DESC_CHECK(d) (Py_TYPE(d) == &EdgeRecordDesc_Type)

#define GET_DESC(o) (assert(NAMEDTUPLE_CHECK(o)),                   \
                     (EdgeRecordDescObject *)EdgeBase_GET_ITEM(     \
                            o, Py_SIZE(o) - 1))

#define SET_DESC(o, d) do {                                         \
        assert(NAMEDTUPLE_CHECK(o));                                \
        assert(d != NULL);                                          \
        assert(DESC_CHECK(d));                                      \
        Py_CLEAR(EdgeBase_GET_ITEM(o, Py_SIZE(o) - 1));             \
        Py_INCREF(d);                                               \
        EdgeBase_SET_ITEM(o, Py_SIZE(o) - 1, (PyObject*)d);         \
    } while(0);


EdgeBaseObject *
EdgeNamedTuple_New(EdgeRecordDescObject *desc)
{
    if (desc == NULL || !DESC_CHECK(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = desc->size;
    EdgeBaseObject *nt = EdgeBaseObject_New(&EdgeNamedTuple_Type, size + 1);
    if (nt == NULL) {
        return NULL;
    }

    SET_DESC(nt, desc);
    return nt;
}


static Py_hash_t
namedtuple_hash(EdgeBaseObject *v)
{
    return EdgeBaseObject_Hash(v, TUPLE_SIZE(v));
}


static PyObject *
namedtuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    EdgeBaseObject *o = NULL;
    PyObject *keys_tup = NULL;
    PyObject *kwargs_iter = NULL;
    EdgeRecordDescObject *desc = NULL;

    if (type != &EdgeNamedTuple_Type) {
        PyErr_BadInternalCall();
        goto fail;
    }

    if (args != NULL && PyTuple_GET_SIZE(args) > 0) {
        PyErr_BadInternalCall();
        goto fail;
    }

    if (kwargs == NULL ||
            !PyDict_CheckExact(kwargs) ||
            PyDict_Size(kwargs) == 0)
    {
        PyErr_SetString(
            PyExc_ValueError,
            "edgedb.NamedTuple requires at least one field/value");
        goto fail;
    }

    Py_ssize_t size = PyDict_Size(kwargs);
    assert(size);

    keys_tup = PyTuple_New(size);
    if (keys_tup == NULL) {
        goto fail;
    }

    kwargs_iter = PyObject_GetIter(kwargs);
    if (kwargs_iter == NULL) {
        goto fail;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyIter_Next(kwargs_iter);
        if (key == NULL) {
            if (PyErr_Occurred()) {
                goto fail;
            }
            else {
                PyErr_BadInternalCall();
                goto fail;
            }
        }

        Py_INCREF(key);
        PyTuple_SET_ITEM(keys_tup, i, key);
    }
    Py_CLEAR(kwargs_iter);

    desc = EdgeRecordDesc_New(keys_tup, NULL);
    if (desc == NULL) {
        goto fail;
    }

    o = EdgeNamedTuple_New(desc);
    if (o == NULL) {
        goto fail;
    }
    Py_CLEAR(desc);

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyTuple_GET_ITEM(keys_tup, i);  /* borrowed */
        PyObject *val = PyDict_GetItem(kwargs, key);  /* borrowed */
        if (val == NULL) {
            if (PyErr_Occurred()) {
                goto fail;
            }
            else {
                PyErr_BadInternalCall();
                goto fail;
            }
        }
        Py_INCREF(val);
        EdgeBase_SET_ITEM(o, i, val);
    }
    Py_CLEAR(keys_tup);

    return (PyObject *)o;

fail:
    Py_CLEAR(kwargs_iter);
    Py_CLEAR(keys_tup);
    Py_CLEAR(desc);
    Py_CLEAR(o);
    return NULL;
}


static Py_ssize_t
namedtuple_length(EdgeBaseObject *o)
{
    return TUPLE_SIZE(o);
}


static PyObject *
namedtuple_getitem(EdgeBaseObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= TUPLE_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return NULL;
    }
    Py_INCREF(o->ob_item[i]);
    return o->ob_item[i];
}


static PySequenceMethods namedtuple_as_sequence = {
    .sq_length = (lenfunc)namedtuple_length,
    .sq_item = (ssizeargfunc)namedtuple_getitem,
};


PyTypeObject EdgeNamedTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.NamedTuple",
    .tp_basicsize = sizeof(EdgeBaseObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)EdgeBaseObject_Dealloc,
    .tp_as_sequence = &namedtuple_as_sequence,
    .tp_hash = (hashfunc)namedtuple_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)EdgeBaseObject_Traverse,
    .tp_new = namedtuple_new,
    .tp_free = PyObject_GC_Del
};


PyObject *
EdgeNamedTuple_InitType(void)
{
    if (PyType_Ready(&EdgeNamedTuple_Type) < 0) {
        return NULL;
    }

    return (PyObject *)&EdgeNamedTuple_Type;
}
