#include "datatypes.h"


Py_hash_t
_EdgeGeneric_Hash(PyObject **els, Py_ssize_t len)
{
    /* Python's tuple hash algorithm.  Hashes of edgedb.Tuple and
       edgedb.NamedTuple must be equal to hashes of Python't tuples
       with the same elements */

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


Py_hash_t
_EdgeGeneric_HashString(const char *s)
{
    PyObject *o = PyUnicode_FromString(s);
    if (o == NULL) {
        return -1;
    }

    Py_hash_t res = PyObject_Hash(o);
    Py_DECREF(o);
    return res;
}


Py_hash_t
_EdgeGeneric_HashWithBase(Py_hash_t base_hash, PyObject **els, Py_ssize_t len)
{
    /* Roughly equivalent to calling `hash((base_hash, *els))` in Python */

    assert(base_hash != -1);

    Py_hash_t els_hash = _EdgeGeneric_Hash(els, len);
    if (els_hash == -1) {
        return -1;
    }

    Py_uhash_t x = 0x345678UL;
    Py_uhash_t mult = _PyHASH_MULTIPLIER;

    x = (x ^ (Py_uhash_t)base_hash) * mult;
    mult += (Py_uhash_t)(82520UL + (size_t)4);
    x = (x ^ (Py_uhash_t)els_hash) * mult;
    x += 97531UL;

    Py_hash_t res = (Py_hash_t)x;
    if (res == -1) {
        res = -2;
    }
    return res;
}
