#include "datatypes.h"
#include "freelist.h"


EDGE_SETUP_FREELIST(
    EDGE_NAMED_TUPLE,
    EdgeNamedTupleObject,
    EDGE_NAMEDTUPLE_FREELIST_MAXSAVE,
    EDGE_NAMEDTUPLE_FREELIST_SIZE)


#define EdgeNamedTuple_GET_ITEM(op, i) \
    (((EdgeNamedTupleObject *)(op))->ob_item[i])
#define EdgeNamedTuple_SET_ITEM(op, i, v) \
    (((EdgeNamedTupleObject *)(op))->ob_item[i] = v)


EdgeNamedTupleObject *
EdgeNamedTuple_New(EdgeRecordDescObject *desc)
{
    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = desc->size;

    EdgeNamedTupleObject *nt = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_NAMED_TUPLE, EdgeNamedTupleObject,
                           &EdgeNamedTuple_Type, nt, size);
    assert(nt != NULL);

    Py_INCREF(desc);
    nt->desc = desc;

    PyObject_GC_Track(nt);
    return nt;
}


static void
namedtuple_dealloc(EdgeNamedTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->desc);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_NAMED_TUPLE, EdgeNamedTupleObject, o);
}


static Py_hash_t
namedtuple_hash(EdgeNamedTupleObject *v)
{
    return EdgeTupleLike_Hash(v->ob_item, Py_SIZE(v));
}


static int
namedtuple_traverse(EdgeNamedTupleObject *o, visitproc visit, void *arg)
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


static PyObject *
namedtuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    EdgeNamedTupleObject *o = NULL;
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
        EdgeNamedTuple_SET_ITEM(o, i, val);
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


static PyObject *
namedtuple_getattr(EdgeNamedTupleObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(o->desc, name, &pos);
    if (ret == L_ERROR) {
        return NULL;
    }
    else if (ret == L_NOT_FOUND) {
        PyErr_SetObject(PyExc_AttributeError, name);
        return NULL;
    }
    else if (ret == L_LINKPROP) {
        /* shouldn't be possible for namedtuples */
        PyErr_BadInternalCall();
        return NULL;
    }
    else if (ret == L_ATTR) {
        PyObject *val = EdgeNamedTuple_GET_ITEM(o, pos);
        Py_INCREF(val);
        return val;
    }
    else {
        abort();
    }
}


static Py_ssize_t
namedtuple_length(EdgeNamedTupleObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
namedtuple_getitem(EdgeNamedTupleObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "namedtuple index out of range");
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
    .tp_basicsize = sizeof(EdgeNamedTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)namedtuple_dealloc,
    .tp_as_sequence = &namedtuple_as_sequence,
    .tp_hash = (hashfunc)namedtuple_hash,
    .tp_getattro = (getattrofunc)namedtuple_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)namedtuple_traverse,
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
