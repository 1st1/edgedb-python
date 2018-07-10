#ifndef EDGE_DATATYPES_H
#define EDGE_DATATYPES_H

#include "Python.h"


#define EDGE_MAX_TUPLE_SIZE (0x4000 - 1)
#define EDGE_RECORD_LINK_PROP_BIT  (1 << 15)


/* === edgedb.RecordDesc ==================================== */

extern PyTypeObject EdgeRecordDesc_Type;

#define EdgeRecordDesc_Check(d) (Py_TYPE(d) == &EdgeRecordDesc_Type)

typedef struct {
    PyObject_HEAD
    PyObject *index;
    PyObject *keys;
    Py_ssize_t size;
} EdgeRecordDescObject;

typedef enum {L_ERROR, L_NOT_FOUND, L_LINKPROP, L_ATTR} edge_attr_lookup_t;

PyObject * EdgeRecordDesc_InitType(void);
EdgeRecordDescObject * EdgeRecordDesc_New(PyObject *, PyObject *);
edge_attr_lookup_t EdgeRecordDesc_Lookup(
    EdgeRecordDescObject *, PyObject *, Py_ssize_t *);


/* === edgedb.Tuple ========================================= */

#define EDGE_TUPLE_FREELIST_SIZE 2000
#define EDGE_TUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeTuple_Type;

typedef struct {
    PyObject_VAR_HEAD
    PyObject *ob_item[1];
} EdgeTupleObject;

PyObject * EdgeTuple_InitType(void);
EdgeTupleObject * EdgeTuple_New(Py_ssize_t size);
Py_hash_t EdgeTupleLike_Hash(PyObject **, Py_ssize_t);


/* === edgedb.NamedTuple ==================================== */

#define EDGE_NAMEDTUPLE_FREELIST_SIZE 2000
#define EDGE_NAMEDTUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeNamedTuple_Type;

typedef struct {
    PyObject_VAR_HEAD
    EdgeRecordDescObject *desc;
    PyObject *ob_item[1];
} EdgeNamedTupleObject;

PyObject * EdgeNamedTuple_InitType(void);
EdgeNamedTupleObject * EdgeNamedTuple_New(EdgeRecordDescObject *);


/* helpers */
int Edge_NoKeywords(const char *, PyObject *);

#endif
