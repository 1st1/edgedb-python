#include "datatypes.h"
#include "internal.h"


#define POINTER_PROP_IS_LINKPROP    (1 << 0)

static int init_type_called = 0;


static void
record_desc_dealloc(EdgeRecordDescObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->index);
    Py_CLEAR(o->keys);
    PyMem_RawFree(o->posbits);
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

    return EdgeRecordDesc_New(
        PyTuple_GET_ITEM(args, 0),
        PyTuple_Size(args) == 2 ? PyTuple_GET_ITEM(args, 1) : NULL);
}


static PyObject *
record_desc_is_linkprop(EdgeRecordDescObject *o, PyObject *arg)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINKPROP:
            Py_RETURN_TRUE;

        case L_ATTR:
            Py_RETURN_FALSE;

        default:
            abort();
    }
}


static PyObject *
record_desc_get_pos(EdgeRecordDescObject *o, PyObject *arg) {
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINKPROP:
        case L_ATTR:
            return PyLong_FromLong((long)pos);

        default:
            abort();
    }
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
    .tp_new = record_desc_tp_new,
    .tp_methods = record_desc_methods,
};


PyObject *
EdgeRecordDesc_New(PyObject *keys, PyObject *link_props_keys)
{
    EdgeRecordDescObject *o;

    assert(init_type_called);

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

    Py_ssize_t size = Py_SIZE(keys);
    uint8_t *bits = (uint8_t *)PyMem_RawCalloc((size_t)size, sizeof(uint8_t));
    if (bits == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyTuple_GET_ITEM(keys, i);  /* borrowed */
        if (!PyUnicode_CheckExact(key)) {
            PyErr_SetString(
                PyExc_ValueError,
                "RecordDescriptor received a non-str key");
            goto fail;
        }

        Py_ssize_t key_index = i;

        if (link_props_keys != NULL) {
            int ret = PySet_Contains(link_props_keys, key);
            if (ret < 0) {
                Py_DECREF(index);
                goto fail;
            }
            if (ret > 0) {
                bits[i] |= POINTER_PROP_IS_LINKPROP;
            }
        }

        PyObject *num = PyLong_FromLong(key_index);
        if (num == NULL) {
            Py_DECREF(index);
            goto fail;
        }

        if (PyDict_SetItem(index, key, num)) {
            Py_DECREF(index);
            Py_DECREF(num);
            goto fail;
        }

        Py_DECREF(num);
    }

    o = PyObject_GC_New(EdgeRecordDescObject, &EdgeRecordDesc_Type);
    if (o == NULL) {
        Py_DECREF(index);
        goto fail;
    }

    o->posbits = bits;

    o->index = index;

    Py_INCREF(keys);
    o->keys = keys;

    o->size = size;

    PyObject_GC_Track(o);
    return (PyObject *)o;

fail:
    PyMem_RawFree(bits);
    return NULL;
}


edge_attr_lookup_t
EdgeRecordDesc_Lookup(PyObject *ob, PyObject *key, Py_ssize_t *pos)
{
    assert(EdgeRecordDesc_Check(ob));
    EdgeRecordDescObject *d = (EdgeRecordDescObject *)ob;

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
    assert(res_long < d->size);
    *pos = res_long;

    if (d->posbits[res_long] & POINTER_PROP_IS_LINKPROP) {
        return L_LINKPROP;
    }
    else {
        return L_ATTR;
    }
}


PyObject *
EdgeRecordDesc_PointerName(PyObject *ob, Py_ssize_t pos)
{
    assert(EdgeRecordDesc_Check(ob));
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    PyObject * key = PyTuple_GetItem(o->keys, pos);
    if (key == NULL) {
        return NULL;
    }
    Py_INCREF(key);
    return key;
}


int
EdgeRecordDesc_PointerIsLinkProp(PyObject *ob, Py_ssize_t pos)
{
    assert(EdgeRecordDesc_Check(ob));
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    if (pos < 0 || pos >= o->size) {
        PyErr_SetNone(PyExc_IndexError);
        return -1;
    }
    return o->posbits[pos] & POINTER_PROP_IS_LINKPROP;
}

Py_ssize_t
EdgeRecordDesc_GetSize(PyObject *ob)
{
    assert(EdgeRecordDesc_Check(ob));
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    return o->size;
}


PyObject *
EdgeRecordDesc_InitType(void)
{
    if (PyType_Ready(&EdgeRecordDesc_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeRecordDesc_Type;
}
