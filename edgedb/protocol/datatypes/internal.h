#ifndef EDGE_INTERNAL_H
#define EDGE_INTERNAL_H


#include "datatypes.h"


#define _Edge_IsContainer(o)                                \
    (EdgeTuple_Check(o) ||                                  \
     EdgeNamedTuple_Check(o) ||                             \
     EdgeObject_Check(o) ||                                 \
     EdgeSet_Check(o) ||                                    \
     EdgeArray_Check(o))


int _Edge_NoKeywords(const char *, PyObject *);

Py_hash_t _EdgeGeneric_Hash(PyObject **, Py_ssize_t);
Py_hash_t _EdgeGeneric_HashWithBase(Py_hash_t, PyObject **, Py_ssize_t);
Py_hash_t _EdgeGeneric_HashString(const char *);


int _EdgeGeneric_RenderValues(
    _PyUnicodeWriter *, PyObject *, PyObject **, Py_ssize_t);

int _EdgeGeneric_RenderItems(_PyUnicodeWriter *,
                             PyObject *, EdgeRecordDescObject *,
                             PyObject **, Py_ssize_t, int);


#endif
