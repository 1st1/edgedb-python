#ifndef EDGE_DATATYPES_H
#define EDGE_DATATYPES_H

#include "Python.h"


#define EDGE_MAX_TUPLE_SIZE (0x4000 - 1)
#define EDGE_RECORD_LINK_PROP_BIT  (1 << 15)

/* Maximum number of records of each size to save */
#define EDGE_BASE_OBJECT_MAXFREELIST 2000

/* Largest record to save on free list */
#define EDGE_BASE_OBJECT_MAXSAVESIZE 20


typedef struct {
    PyObject_HEAD
    PyObject *index;
    PyObject *keys;
    Py_ssize_t size;
} EdgeRecordDescObject;


typedef struct {
    PyObject_VAR_HEAD
    PyObject *ob_item[1];
} EdgeBaseObject;


#define EdgeBase_GET_ITEM(op, i) (((EdgeBaseObject *)(op))->ob_item[i])
#define EdgeBase_SET_ITEM(op, i, v) (((EdgeBaseObject *)(op))->ob_item[i] = v)


extern PyTypeObject EdgeRecordDesc_Type;
extern PyTypeObject EdgeTuple_Type;
extern PyTypeObject EdgeNamedTuple_Type;


PyObject * EdgeRecordDesc_InitType(void);
EdgeRecordDescObject * EdgeRecordDesc_New(PyObject *, PyObject *);
int EdgeRecordDesc_Lookup(EdgeRecordDescObject *, PyObject *, Py_ssize_t *);

EdgeBaseObject * EdgeBaseObject_New(PyTypeObject *, Py_ssize_t);
void EdgeBaseObject_Dealloc(EdgeBaseObject *);
int EdgeBaseObject_Traverse(EdgeBaseObject *, visitproc, void *);
Py_hash_t EdgeBaseObject_Hash(EdgeBaseObject *, Py_ssize_t);


/* edgedb.Tuple */
PyObject * EdgeTuple_InitType(void);


/* edgedb.NamedTuple */
PyObject * EdgeNamedTuple_InitType(void);
EdgeBaseObject * EdgeNamedTuple_New(EdgeRecordDescObject *);


/* helpers */
int Edge_NoKeywords(const char *, PyObject *);

#endif
