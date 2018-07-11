#ifndef EDGE_DATATYPES_H
#define EDGE_DATATYPES_H


#include <stdint.h>
#include "Python.h"


#define EDGE_MAX_TUPLE_SIZE (0x4000 - 1)


/* === edgedb.RecordDesc ==================================== */

extern PyTypeObject EdgeRecordDesc_Type;

#define EdgeRecordDesc_Check(d) (Py_TYPE(d) == &EdgeRecordDesc_Type)

typedef struct {
    PyObject_HEAD
    PyObject *index;
    PyObject *keys;
    uint8_t *posbits;
    Py_ssize_t size;
} EdgeRecordDescObject;

typedef enum {L_ERROR, L_NOT_FOUND, L_LINKPROP, L_ATTR} edge_attr_lookup_t;

PyObject * EdgeRecordDesc_InitType(void);
PyObject * EdgeRecordDesc_New(PyObject *, PyObject *);
PyObject * EdgeRecordDesc_PointerName(PyObject *, Py_ssize_t);
int EdgeRecordDesc_PointerIsLinkProp(PyObject *, Py_ssize_t);
Py_ssize_t EdgeRecordDesc_GetSize(PyObject *);
edge_attr_lookup_t EdgeRecordDesc_Lookup(PyObject *, PyObject *, Py_ssize_t *);


/* === edgedb.Tuple ========================================= */

#define EDGE_TUPLE_FREELIST_SIZE 500
#define EDGE_TUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeTuple_Type;

#define EdgeTuple_Check(d) (Py_TYPE(d) == &EdgeTuple_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *ob_item[1];
} EdgeTupleObject;

PyObject * EdgeTuple_InitType(void);
PyObject * EdgeTuple_New(Py_ssize_t size);
int EdgeTuple_SetItem(PyObject *, Py_ssize_t, PyObject *);



/* === edgedb.NamedTuple ==================================== */

#define EDGE_NAMEDTUPLE_FREELIST_SIZE 500
#define EDGE_NAMEDTUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeNamedTuple_Type;

#define EdgeNamedTuple_Check(d) (Py_TYPE(d) == &EdgeNamedTuple_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *desc;
    PyObject *ob_item[1];
} EdgeNamedTupleObject;

PyObject * EdgeNamedTuple_InitType(void);
PyObject * EdgeNamedTuple_New(PyObject *);
int EdgeNamedTuple_SetItem(PyObject *, Py_ssize_t, PyObject *);


/* === edgedb.Object ======================================== */

#define EDGE_OBJECT_FREELIST_SIZE 2000
#define EDGE_OBJECT_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeObject_Type;

#define EdgeObject_Check(d) (Py_TYPE(d) == &EdgeObject_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *desc;
    Py_hash_t cached_hash;
    PyObject *ob_item[1];
} EdgeObject;

PyObject * EdgeObject_InitType(void);
PyObject * EdgeObject_New(PyObject *);
int EdgeObject_SetItem(PyObject *, Py_ssize_t, PyObject *);


/* === edgedb.Set =========================================== */

extern PyTypeObject EdgeSet_Type;

#define EdgeSet_Check(d) (Py_TYPE(d) == &EdgeSet_Type)

typedef struct {
    PyObject_HEAD
    Py_hash_t cached_hash;
    PyObject *els;
} EdgeSetObject;

PyObject * EdgeSet_InitType(void);
PyObject * EdgeSet_New(Py_ssize_t);
int EdgeSet_SetItem(PyObject *, Py_ssize_t, PyObject *);


/* === edgedb.Array ========================================= */

#define EDGE_ARRAY_FREELIST_SIZE 500
#define EDGE_ARRAY_FREELIST_MAXSAVE 10

extern PyTypeObject EdgeArray_Type;

#define EdgeArray_Check(d) (Py_TYPE(d) == &EdgeArray_Type)

typedef struct {
    PyObject_VAR_HEAD
    Py_hash_t cached_hash;
    PyObject *ob_item[1];
} EdgeArrayObject;

PyObject * EdgeArray_InitType(void);
PyObject * EdgeArray_New(Py_ssize_t size);
int EdgeArray_SetItem(PyObject *, Py_ssize_t, PyObject *);


#endif
