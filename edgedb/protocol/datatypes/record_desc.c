#include "datatypes.h"


static void
record_desc_dealloc(EdgeRecordDescObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->index);
    Py_CLEAR(o->keys);
    PyObject_GC_Del(o);
}


static int
record_desc_traverse(EdgeRecordDescObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->index);
    Py_VISIT(o->keys);
    return 0;
}

static PyObject *
record_desc_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    if (args == NULL ||
            PyTuple_Size(args) < 1 ||
            PyTuple_Size(args) > 2 ||
            (kwds != NULL && PyDict_Size(kwds)))
    {
        PyErr_SetString(
            PyExc_TypeError,
            "RecordDescriptor accepts one to two positional arguments");
        return NULL;
    }

    return (PyObject *)EdgeRecordDesc_New(
        PyTuple_GET_ITEM(args, 0),
        PyTuple_Size(args) == 2 ? PyTuple_GET_ITEM(args, 1) : NULL);
}


static PyObject *
record_desc_is_linkprop(EdgeRecordDescObject *o, PyObject *arg)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(o, arg, &pos);
    if (ret == L_ERROR) {
        return NULL;
    }
    else if (ret == L_NOT_FOUND) {
        PyErr_SetObject(PyExc_LookupError, arg);
        return NULL;
    }
    else if (ret == L_LINKPROP) {
        Py_RETURN_TRUE;
    }
    else if (ret == L_ATTR) {
        Py_RETURN_FALSE;
    }
    else {
        abort();
    }
}


static PyObject *
record_desc_get_pos(EdgeRecordDescObject *o, PyObject *arg) {
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(o, arg, &pos);
    if (ret == L_ERROR) {
        return NULL;
    }
    else if (ret == L_NOT_FOUND) {
        PyErr_SetObject(PyExc_LookupError, arg);
        return NULL;
    }
    else if (ret == L_LINKPROP) {
        PyLong_FromLong((long)pos);
    }
    else if (ret == L_ATTR) {
        PyLong_FromLong((long)pos);
    }
    else {
        abort();
    }
    return PyLong_FromLong((long)pos);
}


static PyMethodDef record_desc_methods[] = {
    {"is_linkprop", (PyCFunction)record_desc_is_linkprop, METH_O, NULL},
    {"get_pos", (PyCFunction)record_desc_get_pos, METH_O, NULL},
    {NULL, NULL}
};


PyTypeObject EdgeRecordDesc_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "edgedb.RecordDescriptor",
    .tp_basicsize = sizeof(EdgeRecordDescObject),
    .tp_dealloc = (destructor)record_desc_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_desc_traverse,
    .tp_iter = PyObject_SelfIter,
    .tp_new = record_desc_tp_new,
    .tp_methods = record_desc_methods,
};


EdgeRecordDescObject *
EdgeRecordDesc_New(PyObject *keys, PyObject *link_props_keys)
{
    EdgeRecordDescObject *o;

    if (!keys || !PyTuple_CheckExact(keys)) {
        PyErr_SetString(
            PyExc_TypeError,
            "RecordDescriptor requires a tuple as its first argument");
        return NULL;
    }

    if (Py_SIZE(keys) > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_TypeError,
            "EdgeDB does not supports tuples with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    if (link_props_keys != NULL && !PyFrozenSet_CheckExact(link_props_keys)) {
        PyErr_SetString(
            PyExc_TypeError,
            "RecordDescriptor requires a frozenset as its second argument");
        return NULL;
    }

    PyObject *index = PyDict_New();
    if (index == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(keys); i++) {
        PyObject *key = PyTuple_GET_ITEM(keys, i);  /* borrowed */
        Py_ssize_t key_index = i;

        if (link_props_keys != NULL) {
            int ret = PySet_Contains(link_props_keys, key);
            if (ret < 0) {
                Py_DECREF(index);
                return NULL;
            }
            if (ret > 0) {
                key_index |= EDGE_RECORD_LINK_PROP_BIT;
            }
        }

        PyObject *num = PyLong_FromLong(key_index);
        if (num == NULL) {
            Py_DECREF(index);
            return NULL;
        }

        if (PyDict_SetItem(index, key, num)) {
            Py_DECREF(index);
            Py_DECREF(num);
            return NULL;
        }

        Py_DECREF(num);
    }

    o = PyObject_GC_New(EdgeRecordDescObject, &EdgeRecordDesc_Type);
    if (o == NULL) {
        Py_DECREF(index);
        return NULL;
    }

    Py_INCREF(index);
    o->index = index;

    Py_INCREF(keys);
    o->keys = keys;

    o->size = Py_SIZE(keys);

    PyObject_GC_Track(o);
    return o;
}


edge_attr_lookup_t
EdgeRecordDesc_Lookup(EdgeRecordDescObject *d, PyObject *key, Py_ssize_t *pos)
{
    PyObject *res = PyDict_GetItem(d->index, key);  /* borrowed */
    if (res == NULL) {
        if (PyErr_Occurred()) {
            return L_ERROR;
        }
        else {
            return L_NOT_FOUND;
        }
    }

    assert(PyLong_CheckExact(res));
    long res_long = PyLong_AsLong(res);
    if (res_long < 0) {
        assert(PyErr_Occurred());
        return L_ERROR;
    }

    if (res_long & EDGE_RECORD_LINK_PROP_BIT) {
        *pos = res_long & EDGE_MAX_TUPLE_SIZE;
        return L_LINKPROP;
    }
    else {
        *pos = res_long;
        return L_ATTR;
    }
}


PyObject *
EdgeRecordDesc_InitType(void)
{
    if (PyType_Ready(&EdgeRecordDesc_Type) < 0) {
        return NULL;
    }

    return (PyObject *)&EdgeRecordDesc_Type;
}
