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
EdgeRecordDescObject * EdgeRecordDesc_New(PyObject *, PyObject *);
PyObject * EdgeRecordDesc_PointerName(EdgeRecordDescObject *, Py_ssize_t);
int EdgeRecordDesc_PointerIsLinkProp(EdgeRecordDescObject *, Py_ssize_t);
edge_attr_lookup_t EdgeRecordDesc_Lookup(
    EdgeRecordDescObject *, PyObject *, Py_ssize_t *);


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
EdgeTupleObject * EdgeTuple_New(Py_ssize_t size);
int EdgeTuple_SetItem(EdgeTupleObject *, Py_ssize_t, PyObject *);



/* === edgedb.NamedTuple ==================================== */

#define EDGE_NAMEDTUPLE_FREELIST_SIZE 500
#define EDGE_NAMEDTUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeNamedTuple_Type;

#define EdgeNamedTuple_Check(d) (Py_TYPE(d) == &EdgeNamedTuple_Type)

typedef struct {
    PyObject_VAR_HEAD
    EdgeRecordDescObject *desc;
    PyObject *ob_item[1];
} EdgeNamedTupleObject;

PyObject * EdgeNamedTuple_InitType(void);
EdgeNamedTupleObject * EdgeNamedTuple_New(EdgeRecordDescObject *);
int EdgeNamedTuple_SetItem(EdgeNamedTupleObject *, Py_ssize_t, PyObject *);


/* === edgedb.Object ======================================== */

#define EDGE_OBJECT_FREELIST_SIZE 2000
#define EDGE_OBJECT_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeObject_Type;

#define EdgeObject_Check(d) (Py_TYPE(d) == &EdgeObject_Type)

typedef struct {
    PyObject_VAR_HEAD
    EdgeRecordDescObject *desc;
    Py_hash_t cached_hash;
    PyObject *ob_item[1];
} EdgeObject;

PyObject * EdgeObject_InitType(void);
EdgeObject * EdgeObject_New(EdgeRecordDescObject *);
int EdgeObject_SetItem(EdgeObject *, Py_ssize_t, PyObject *);


/* === edgedb.Set =========================================== */

extern PyTypeObject EdgeSet_Type;

#define EdgeSet_Check(d) (Py_TYPE(d) == &EdgeSet_Type)

typedef struct {
    PyObject_HEAD
    Py_hash_t cached_hash;
    PyObject *els;
} EdgeSetObject;

PyObject * EdgeSet_InitType(void);
EdgeSetObject * EdgeSet_New(Py_ssize_t);
int EdgeSet_SetItem(EdgeSetObject *, Py_ssize_t, PyObject *);


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
EdgeArrayObject * EdgeArray_New(Py_ssize_t size);
int EdgeArray_SetItem(EdgeArrayObject *, Py_ssize_t, PyObject *);


#endif
