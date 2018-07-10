#include "internal.h"


static PyObject *
render_object(PyObject *obj)
{
    if (Py_EnterRecursiveCall(" while getting the repr of an EdgeDB object")) {
        return NULL;
    }
    PyObject *val = PyObject_Repr(obj);
    Py_LeaveRecursiveCall();
    return val;
}


int
_EdgeGeneric_RenderValues(_PyUnicodeWriter *writer,
                          PyObject *host, PyObject **items, Py_ssize_t len)
{
    int res = Py_ReprEnter(host);
    if (res != 0) {
        if (res > 0) {
            if (_PyUnicodeWriter_WriteASCIIString(writer, "...", 3) < 0) {
                return -1;
            }
            goto done;
        }
        else {
            return -1;
        }
    }

    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject *item_repr = render_object(items[i]);
        if (item_repr == NULL) {
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(writer, item_repr) < 0) {
            Py_DECREF(item_repr);
            goto error;
        }
        Py_DECREF(item_repr);

        if (i < len - 1) {
            if (_PyUnicodeWriter_WriteASCIIString(writer, ", ", 2) < 0) {
                goto error;
            }
        }
    }

done:
    Py_ReprLeave((PyObject *)host);
    return 0;

error:
    Py_ReprLeave((PyObject *)host);
    return -1;
}


int
_EdgeGeneric_RenderItems(_PyUnicodeWriter *writer,
                         PyObject *host, EdgeRecordDescObject *desc,
                         PyObject **items, Py_ssize_t len,
                         int include_link_props)
{
    assert(desc->size == len);

    PyObject *item_repr = NULL;
    PyObject *item_name = NULL;

    int res = Py_ReprEnter(host);
    if (res != 0) {
        if (res > 0) {
            if (_PyUnicodeWriter_WriteASCIIString(writer, "...", 3) < 0) {
                return -1;
            }
            goto done;
        }
        else {
            return -1;
        }
    }

    for (Py_ssize_t i = 0; i < len; i++) {
        int is_linkprop = EdgeRecordDesc_PointerIsLinkProp(desc, i);
        if (is_linkprop < 0) {
            goto error;
        }

        if (is_linkprop) {
            if (include_link_props) {
                if (_PyUnicodeWriter_WriteChar(writer, '@') < 0) {
                    goto error;
                }
            }
            else {
                continue;
            }
        }

        item_repr = render_object(items[i]);
        if (item_repr == NULL) {
            goto error;
        }

        item_name = EdgeRecordDesc_PointerName(desc, i);
        if (item_name == NULL) {
            goto error;
        }
        assert(PyUnicode_CheckExact(item_name));

        if (_PyUnicodeWriter_WriteStr(writer, item_name) < 0) {
            goto error;
        }
        Py_CLEAR(item_name);

        if (_PyUnicodeWriter_WriteASCIIString(writer, " := ", 4) < 0) {
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(writer, item_repr) < 0) {
            goto error;
        }
        Py_CLEAR(item_repr);

        if (i < len - 1) {
            if (_PyUnicodeWriter_WriteASCIIString(writer, ", ", 2) < 0) {
                goto error;
            }
        }
    }

done:
    Py_ReprLeave((PyObject *)host);
    return 0;

error:
    Py_CLEAR(item_repr);
    Py_CLEAR(item_name);
    Py_ReprLeave((PyObject *)host);
    return -1;
}
