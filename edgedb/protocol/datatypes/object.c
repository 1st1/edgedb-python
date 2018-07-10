#include "datatypes.h"
#include "freelist.h"


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


EdgeObject *
EdgeObject_New(EdgeRecordDescObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = desc->size;

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
    return o;
}


int
EdgeObject_SetItem(EdgeObject *o, Py_ssize_t i, PyObject *el)
{
    assert(EdgeObject_Check(o));
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
    EDGE_DEALLOC_WITH_FREELIST(EDGE_OBJECT, EdgeObject, o);
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
        o->cached_hash = EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
}


static PyObject *
object_getattr(EdgeObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(o->desc, name, &pos);
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
    .tp_free = PyObject_GC_Del
};


PyObject *
EdgeObject_InitType(void)
{
    if (PyType_Ready(&EdgeObject_Type) < 0) {
        return NULL;
    }

    base_hash = EdgeGeneric_HashString("edgedb.Object");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeObject_Type;
}
