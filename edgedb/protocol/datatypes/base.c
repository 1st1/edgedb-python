#include "datatypes.h"


static EdgeBaseObject *free_list[EDGE_BASE_OBJECT_MAXSAVESIZE];
static int numfree[EDGE_BASE_OBJECT_MAXSAVESIZE];


EdgeBaseObject *
EdgeBaseObject_New(PyTypeObject *type, Py_ssize_t size)
{
    EdgeBaseObject *o;
    Py_ssize_t i;

    if (size < 0) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (size < EDGE_BASE_OBJECT_MAXSAVESIZE && (o = free_list[size]) != NULL) {
        free_list[size] = (EdgeBaseObject *) o->ob_item[0];
        numfree[size]--;
        Py_TYPE(o) = type;
        _Py_NewReference((PyObject *)o);
    }
    else {
        /* Check for overflow */
        if ((size_t)size > ((size_t)PY_SSIZE_T_MAX - sizeof(EdgeBaseObject) -
                    sizeof(PyObject *)) / sizeof(PyObject *))
        {
            PyErr_NoMemory();
            return NULL;
        }

        o = PyObject_GC_NewVar(EdgeBaseObject, type, size);
        if (o == NULL) {
            return NULL;
        }
    }

    for (i = 0; i < size; i++) {
        o->ob_item[i] = NULL;
    }

    PyObject_GC_Track(o);
    return o;
}


void
EdgeBaseObject_Dealloc(EdgeBaseObject *o)
{
    Py_ssize_t i;
    Py_ssize_t len = Py_SIZE(o);

    PyObject_GC_UnTrack(o);

    Py_TRASHCAN_SAFE_BEGIN(o)
    if (len > 0) {
        i = len;
        while (--i >= 0) {
            Py_CLEAR(o->ob_item[i]);
        }

        if (len < EDGE_BASE_OBJECT_MAXSAVESIZE &&
            numfree[len] < EDGE_BASE_OBJECT_MAXFREELIST)
        {
            o->ob_item[0] = (PyObject *) free_list[len];
            numfree[len]++;
            free_list[len] = o;
            Py_TYPE(o) = NULL;
            goto done; /* return */
        }
    }
    Py_TYPE(o)->tp_free((PyObject *)o);
done:
    Py_TRASHCAN_SAFE_END(o)
}


int
EdgeBaseObject_Traverse(EdgeBaseObject *o, visitproc visit, void *arg)
{
    Py_ssize_t i;
    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


Py_hash_t
EdgeBaseObject_Hash(EdgeBaseObject *o, Py_ssize_t len)
{
    Py_uhash_t x;  /* Unsigned for defined overflow behavior. */
    Py_hash_t y;
    PyObject **p;
    Py_uhash_t mult;

    assert(len >= 0);
    assert(len <= Py_SIZE(o));

    mult = _PyHASH_MULTIPLIER;
    x = 0x345678UL;
    p = o->ob_item;
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
