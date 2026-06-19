#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

static int contains(const uint8_t *b, Py_ssize_t n, const char *needle, Py_ssize_t m) {
    if (m <= 0 || n < m) return 0;
    for (Py_ssize_t i = 0; i <= n - m; i++) {
        if (memcmp(b + i, needle, (size_t)m) == 0) return 1;
    }
    return 0;
}

static int is_digit(uint8_t c) { return c >= '0' && c <= '9'; }

static int parse_signed_2(const uint8_t *b, Py_ssize_t i, Py_ssize_t end, int *value, Py_ssize_t *next) {
    int neg = 0;
    if (i < end && b[i] == 'M') { neg = 1; i++; }
    if (i + 1 >= end || !is_digit(b[i]) || !is_digit(b[i + 1])) return 0;
    int v = (b[i] - '0') * 10 + (b[i + 1] - '0');
    *value = neg ? -v : v;
    *next = i + 2;
    return 1;
}

static int extract_temp(const uint8_t *raw, Py_ssize_t n, const uint8_t *icao, Py_ssize_t icao_n, Py_ssize_t station_pos, int *temp_c) {
    Py_ssize_t end = station_pos + 220;
    if (end > n) end = n;
    Py_ssize_t i = station_pos + icao_n;
    while (i < end) {
        while (i < end && raw[i] <= 32) i++;
        Py_ssize_t tok_start = i;
        while (i < end && raw[i] > 32) i++;
        Py_ssize_t tok_end = i;
        if (tok_end <= tok_start) continue;
        Py_ssize_t slash = -1;
        for (Py_ssize_t j = tok_start; j < tok_end; j++) {
            if (raw[j] == '/') { slash = j; break; }
        }
        if (slash <= tok_start) continue;
        int v = 0; Py_ssize_t next = 0;
        if (!parse_signed_2(raw, tok_start, slash, &v, &next)) continue;
        if (next == slash) { *temp_c = v; return 1; }
    }
    return 0;
}

static PyObject *parse_any(PyObject *self, PyObject *args) {
    Py_buffer buf;
    PyObject *stations;
    if (!PyArg_ParseTuple(args, "y*O!:parse_any", &buf, &PyTuple_Type, &stations)) return NULL;
    const uint8_t *b = (const uint8_t *)buf.buf;
    Py_ssize_t n = buf.len;

    int has_saus = contains(b, n, "SAUS", 4);
    int has_spus = contains(b, n, "SPUS", 4);
    int has_metar = contains(b, n, "METAR", 5);
    int has_speci = contains(b, n, "SPECI", 5);
    if (!(has_saus || has_spus || has_metar || has_speci)) {
        PyBuffer_Release(&buf);
        Py_RETURN_NONE;
    }

    Py_ssize_t count = PyTuple_GET_SIZE(stations);
    Py_ssize_t best_pos = -1;
    PyObject *best_station = NULL;
    const uint8_t *best_icao = NULL;
    Py_ssize_t best_icao_n = 0;

    for (Py_ssize_t s = 0; s < count; s++) {
        PyObject *item = PyTuple_GET_ITEM(stations, s);
        Py_buffer ibuf;
        if (PyObject_GetBuffer(item, &ibuf, PyBUF_SIMPLE) != 0) { PyErr_Clear(); continue; }
        const uint8_t *icao = (const uint8_t *)ibuf.buf;
        Py_ssize_t m = ibuf.len;
        if (m > 0 && n >= m + 2) {
            for (Py_ssize_t i = 0; i <= n - m; i++) {
                uint8_t before = (i == 0) ? ' ' : b[i - 1];
                uint8_t after = (i + m >= n) ? ' ' : b[i + m];
                if (before <= 32 && after <= 32 && memcmp(b + i, icao, (size_t)m) == 0) {
                    if (best_pos < 0 || i < best_pos) {
                        best_pos = i;
                        best_station = item;
                        best_icao = icao;
                        best_icao_n = m;
                    }
                    break;
                }
            }
        }
        PyBuffer_Release(&ibuf);
    }

    if (best_pos < 0 || best_station == NULL) {
        PyBuffer_Release(&buf);
        Py_RETURN_NONE;
    }

    int temp_c = 0;
    if (!extract_temp(b, n, best_icao, best_icao_n, best_pos, &temp_c)) {
        PyBuffer_Release(&buf);
        Py_RETURN_NONE;
    }
    const char *kind = (has_spus || has_speci) ? "SPUS/SPECI" : "SAUS/METAR";
    Py_ssize_t raw_start = best_pos > 64 ? best_pos - 64 : 0;
    Py_ssize_t raw_end = best_pos + 220 < n ? best_pos + 220 : n;
    PyObject *ret = Py_BuildValue("Ninns", PyBytes_FromStringAndSize((const char *)best_icao, best_icao_n), temp_c, raw_start, raw_end, kind);
    PyBuffer_Release(&buf);
    return ret;
}

static PyMethodDef Methods[] = {
    {"parse_any", parse_any, METH_VARARGS, "Parse first target NWWS METAR/SPECI hit. Returns (icao_bytes, temp_c, raw_start, raw_end, kind) or None."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = { PyModuleDef_HEAD_INIT, "_nwws_fast", NULL, -1, Methods };
PyMODINIT_FUNC PyInit__nwws_fast(void) { return PyModule_Create(&moduledef); }
