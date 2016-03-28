#include "/usr/local/anaconda/include/python2.7/Python.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include "/global/dsm/dsm.h"

#define TRUE (1)
#define FALSE (0)

#define dprintf if (debugMessagesOn) printf

int dSMOpen         = FALSE; /* TRUE iff dsm is open */
int debugMessagesOn = FALSE; /* Print debugging text messages */

#define DSM_BYTE      (1)
#define DSM_SHORT     (2)
#define DSM_LONG      (3)
#define DSM_FLOAT     (4)
#define DSM_DOUBLE    (5)
#define DSM_STRING    (6)
#define DSM_STRUCTURE (7)

#define DECODE_ERROR (100)

static PyObject *dSMNoShare;
static PyObject *dSMNoResource;
static PyObject *dSMNoSuchName;
static PyObject *dSMVersionMismatch;
static PyObject *dSMRPCFailure;
static PyObject *dSMInternalError;
static PyObject *dSMIllegalName;
static PyObject *dSMNotImplemented;
static PyObject *dSMRangeError;
static PyObject *dSMDecodeError;
static PyObject *dSMWrongType;
static PyObject *dSMNothingMonitored;
static PyObject *dSMCatchAll;

void raiseDSMError(int status, char *message)
{
  dprintf("%s\n", message);
  switch (status) {
  case DSM_RPC_ERROR:
    PyErr_SetString(dSMRPCFailure, "DSM error: RPC Failure");
    break;
  case DSM_TARGET_INVALID:
    PyErr_SetString(dSMNoShare, "DSM error: no shared space for target computer");
    break;
  case DSM_NAME_INVALID:
    PyErr_SetString(dSMNoSuchName, "DSM error: allocation name doesn't exist");
    break;
  case DSM_ALLOC_VERS:
    PyErr_SetString(dSMVersionMismatch, "DSM error: allocation version mismatch");
    break;
  case DSM_INTERNAL_ERROR:
    PyErr_SetString(dSMInternalError, "DSM Internal Error - Contact Maintainer (good luck with that)");
    break;
  case DSM_NO_RESOURCE:
    PyErr_SetString(dSMNoResource, "DSM error: Can't open shared memory");
    break;
  default:
    PyErr_SetString(dSMCatchAll, "DSM error: catchall error");
    dsm_error_message(status, "Unhandled DSM error");
  }
}

int decodeObject(char *name, int *type, int *nDim, int **dimensions)
{
  int CSaw, i, j;
  char *ptr;

  /* dprintf("Decoding type %c\n", name[strlen(name)-1]); */
  *nDim = 0;
  *dimensions = NULL;
  switch (name[strlen(name)-1]) {
  case 'B':
    *type = DSM_BYTE; break;
  case 'S':
    *type = DSM_SHORT; break;
  case 'L':
    *type = DSM_LONG; break;
  case 'F':
    *type = DSM_FLOAT; break;
  case 'D':
    *type = DSM_DOUBLE; break;
  case 'X':
    *type = DSM_STRUCTURE;
    return DSM_SUCCESS;
  case '0':
  case '1':
  case '2':
  case '3':
  case '4':
  case '5':
  case '6':
  case '7':
  case '8':
  case '9':
    CSaw = 0;
    i = strlen(name)-2;
    while ((!CSaw) && (i > 0)) {
      if (name[i] == 'C')
	CSaw = i;
      else
	i--;
    }
    if ((i == 0) || CSaw == 0) {
      PyErr_SetString(dSMIllegalName, "DSM error: Illegal Name");
      return DSM_ERROR;
    }
    for (j = i+1; j < strlen(name); j++)
      if (!((name[j] >= '0') && (name[j] <= '9'))) {
	PyErr_SetString(dSMIllegalName, "DSM error: Illegal Name");
	return DSM_ERROR;
      }
    *nDim = 1;
    *dimensions = PyMem_Malloc(sizeof(int));
    if (*dimensions == NULL) {
      fprintf(stderr, "PyMem_Malloc failure for dimensions of \"%s\"\n", name);
      PyErr_NoMemory();
      return DSM_ERROR;
    }
    *dimensions[0] = atoi(&name[i+1]);
    *type = DSM_STRING;
  break;
  default:
    PyErr_SetString(dSMIllegalName, "DSM error: Illegal Name");
    return DSM_ERROR;
  }
  ptr = name;
  do {
    ptr = strstr(ptr, "_V");
    if (ptr != NULL) {
      if (strlen(ptr) > 2) {
	if (isdigit(ptr[2])) {
	  char *endPtr, temp[10];
	  int size;
	  
	  endPtr = strstr(ptr+2, "_");
	  strncpy(temp, ptr+2, endPtr-ptr-2);
	  temp[endPtr-ptr-2] = (char)0;
	  size = atoi(temp);
	  *nDim += 1;
	  *dimensions = PyMem_Realloc(*dimensions, (*nDim)*sizeof(int));
	  if (*dimensions == NULL) {
	    fprintf(stderr, "PyMem_Realloc failure(%d) for dimensions of \"%s\"\n", *nDim, name);
	    PyErr_NoMemory();
	    return DSM_ERROR;
	  }
	  ((int *)*dimensions)[(*nDim)-1] = size;
	}
      }
      ptr += 1;
    }
  } while (ptr != NULL);
  /* dprintf("Returning type %d for \"%s\", nDim = %d\n", *type, name, *nDim); */
  if (FALSE && debugMessagesOn && (*nDim > 0))
    for (i = 0; i < *nDim; i++)
      printf("Dimension %d: %d\n", i, ((int *)*dimensions)[i]);
  return DSM_SUCCESS;
}

int open_dsm(void)
{
  int status = DSM_SUCCESS;

  if (!dSMOpen) {
    status = dsm_open();
    if (status == DSM_SUCCESS) {
      dSMOpen = TRUE;
      dprintf("DSM successfully opened\n");
    } else
      raiseDSMError(status, "dsm_open()");
  }
  return status;
}

static PyObject *pydsm_open(PyObject *self, PyObject *args)
{
  int status;

  if (!PyArg_ParseTuple(args, "i", &debugMessagesOn))
    return NULL;
  dprintf("Turning on debugging messages\n");
  status = open_dsm();
  if (status != DSM_SUCCESS)
    return NULL;
  else
    Py_RETURN_NONE;
}

PyObject *buildTuples(PyObject **tuples, int dim, int nDim, int *dimensions, int type, int baseSize, char *arrayBase)
{
  static int counter;
  static int ptr;
  int i, localCounter;
  PyObject *tObj;

  if (dim == 0)
    counter = ptr = 0;
  else
    counter++;
  localCounter = counter;
  tuples[localCounter] = PyTuple_New(dimensions[dim]);
  if (dim != nDim-1)
    for (i = 0; i < dimensions[dim]; i++)
      PyTuple_SetItem(tuples[localCounter], (Py_ssize_t)i,  buildTuples(tuples, dim+1, nDim, dimensions, type, baseSize, arrayBase));
  else
    for (i = 0; i < dimensions[nDim-1]; i++) {
      switch (type) {
      case DSM_BYTE:
	tObj = PyInt_FromLong((long)arrayBase[ptr]); break;
      case DSM_SHORT:
	tObj = PyInt_FromLong((long)((short *)arrayBase)[ptr]); break;
      case DSM_LONG:
	tObj = PyInt_FromLong((long)((int *)arrayBase)[ptr]); break;
      case DSM_FLOAT:
	tObj = PyFloat_FromDouble((double)((float *)arrayBase)[ptr]); break;
      case DSM_DOUBLE:
	tObj = PyFloat_FromDouble(((double *)arrayBase)[ptr]); break;
      case DSM_STRING:
	tObj = PyString_FromString((const char *)&arrayBase[ptr*baseSize]); break;
      default:
	tObj = PyFloat_FromDouble(((double *)arrayBase)[ptr]);
      }
      PyTuple_SetItem(tuples[localCounter], (Py_ssize_t)i, tObj);
      ptr++;
    }
  return tuples[localCounter];
}

void close_dsm(void)
{
  if (dSMOpen) {
    dsm_close();
    dSMOpen = FALSE;
  }
}

static PyObject *pydsm_close(PyObject *self)
{
  int status;

  if (dSMOpen) {
    dprintf("Closing dsm\n");
    close_dsm();
  } else
    status = DSM_SUCCESS;
  return Py_BuildValue("i", status);
}

static PyObject *pydsm_clear_monitor(PyObject *self)
{
  int status;

  dprintf("Clearing all monitored variables\n");
  status = open_dsm();
  if (status == DSM_SUCCESS) {
    status = dsm_clear_monitor();
    if (status != DSM_SUCCESS) {
      raiseDSMError(status, "pydsm_clear_monitor: dsm_clear_monitor");
      return NULL;
    }
  } else {
    raiseDSMError(status, "pydsm_clear_monitor: dsm_open");
    return NULL;
  }
  return Py_BuildValue("i", status);
}

void fixNames(char *partner, char *name)
{
  int i;

  /* Convert computer name (partner) to lower case, and variable name (name) to uppercase */
  for (i = 0; i < strlen(name); i++)
    name[i] = toupper(name[i]);
  if (name != NULL)
    for (i = 0; i < strlen(partner); i++)
      partner[i] = tolower(partner[i]);
}

static int monitorMaxSize = 0; /* This variable holds the size of the largest variable monitored */

static PyObject *pydsm_monitor(PyObject *self, PyObject *args)
{
  int status, type, nDim, fullSize;
  int elementSize = 0;
  int *dimensions = NULL;
  char *partner, *name;
  
  status = open_dsm();
  if (status == DSM_SUCCESS) {
    if (!PyArg_ParseTuple(args, "ss", &partner, &name))
      return NULL;
    fixNames(partner, name);
    dprintf("pydsm_monitor: request for \"%s\" on \"%s\"\n", name, partner);
    if (toupper(name[strlen(name)-1]) == 'X') {
      PyErr_SetString(dSMNotImplemented, "DSM error: Monitoring structures not yet implemented in the pydsm module");
      return NULL;
    }
    if ((status = decodeObject(name, &type, &nDim, &dimensions)) != DSM_SUCCESS) {
      fprintf(stderr, "Error %d returned by decodeObject (%s)\n", status, name);
      if (dimensions != NULL)
	PyMem_Free(dimensions);
      return NULL;
    } else {
      int i;

      switch (type) {
      case DSM_BYTE:
	elementSize = 1; break;
      case DSM_SHORT:
	elementSize = 2; break;
      case DSM_LONG:
      case DSM_FLOAT:
	elementSize = 4; break;
      case DSM_DOUBLE:
	elementSize = 8; break;
      case DSM_STRING:
	elementSize = dimensions[nDim-1];
	nDim--; break;
      default:
	/* This code should not be reached, because decodeObject will trap error */
	fprintf(stderr, "pydsm.monitor: Unknown type (%d)\n", type);
	return NULL;
      }
      fullSize = elementSize;
      for (i = 0; i < nDim; i++)
	fullSize *= dimensions[i];
      dprintf("Element size %d, full size = %d\n", elementSize, fullSize);
      if (fullSize > monitorMaxSize) {
	dprintf("Changing monitorMaxSize from %d to %d\n", monitorMaxSize, fullSize);
	monitorMaxSize = fullSize;
      }
      PyMem_Free(dimensions);
      status = dsm_monitor(partner, name);
      if (status != DSM_SUCCESS) {
	raiseDSMError(status, "dsm_monitor()");
	return NULL;
      }
    }
  }
  Py_RETURN_NONE;
}

static PyObject *pydsm_no_monitor(PyObject *self, PyObject *args)
{
  int status, type, nDim;
  int *dimensions = NULL;
  char *partner, *name;
  
  status = open_dsm();
  if (status == DSM_SUCCESS) {
    if (!PyArg_ParseTuple(args, "ss", &partner, &name))
      return NULL;
    fixNames(partner, name);
    dprintf("pydsm_no_monitor: request for \"%s\" on \"%s\"\n", name, partner);
    if (toupper(name[strlen(name)-1]) == 'X') {
      PyErr_SetString(dSMNotImplemented, "DSM error: Monitoring structures not yet implemented in the pydsm module");
      return NULL;
    }
    if ((status = decodeObject(name, &type, &nDim, &dimensions)) != DSM_SUCCESS) { /* Just doing this for error checking in the name */
      fprintf(stderr, "Error %d returned by decodeObject (%s)\n", status, name);
      if (dimensions != NULL)
	PyMem_Free(dimensions);
      return NULL;
    } else {
      PyMem_Free(dimensions);
      status = dsm_no_monitor(partner, name);
      if (status != DSM_SUCCESS) {
	raiseDSMError(status, "dsm_no_monitor()");
	return NULL;
      }
    }
  }
  Py_RETURN_NONE;
}

PyObject *makePyObject(char *partner, dsm_structure *structure, char *name, char *buf, time_t theTime, int rM)
{
  int status, type, nDim;
  int *dimensions = NULL;
  time_t timestamp;
  static PyObject **containerTupleBase = NULL;
  PyObject *readTime, *makePyObjectTuple;

  timestamp = theTime; /* Overwritten later, if dsm_read is actually called  */
  if ((status = decodeObject(name, &type, &nDim, &dimensions)) != DSM_SUCCESS) {
    fprintf(stderr, "Error %d returned by decodeObject (%s)\n", status, name);
    if (dimensions != NULL)
      PyMem_Free(dimensions);
    return NULL;
  } else {
    int size = 0;

    /* OK we've successfully decoded the name. */
    if (nDim == 0) {
      int retInt;
      double retDouble;
      char *value = NULL;
      PyObject *retIntObject, *retDoubleObject;

      if (dimensions != NULL) {
	fprintf(stderr, "pydsm logic error: dimesion array has space, but nDim == 0\n");
	exit(-1);
      }
      /* It's a single variable */
      switch (type) {
      case DSM_BYTE:
	size = sizeof(char);   break;
      case DSM_SHORT:
	size = sizeof(short);  break;
      case DSM_LONG:
	size = sizeof(int);    break;
      case DSM_FLOAT:
	size = sizeof(float);  break;
      case DSM_DOUBLE:
	size = sizeof(double); break;
      default:
	fprintf(stderr, "Could not handle type %d\n", type);
	return NULL;
      }
      value = (char *)PyMem_Malloc(size);
      if (value == NULL) {
	fprintf(stderr, "value PyMem_Malloc");
	PyErr_NoMemory();
	return NULL;
      }
      if (buf != NULL) {
	bcopy(buf, value, size);
	status = DSM_SUCCESS;
      } else if (structure != NULL)
	status = dsm_structure_get_element(structure, name, &value[0]);
      else
	status = dsm_read(partner, name, &value[0], &timestamp);
      if (status != DSM_SUCCESS) {
	PyMem_Free(value);	
	raiseDSMError(status, "read or get_element");
	return NULL;
      }
      dprintf("dsm_read completed successfully, with a timestamp of %d\n", (int)timestamp);
      makePyObjectTuple = PyTuple_New(2);
      readTime = PyInt_FromLong((long)timestamp);
      PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)1, readTime);
      switch (type) {
      case DSM_BYTE:
	retInt = value[0];
	PyMem_Free(value);
	dprintf("Got the byte 0x%x\n", retInt);
	retIntObject = PyInt_FromLong((long)retInt);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retIntObject);
	return makePyObjectTuple;
      case DSM_SHORT:
	retInt = ((short *)value)[0];
	PyMem_Free(value);
	dprintf("Got the short %d\n", retInt);
	retIntObject = PyInt_FromLong((long)retInt);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retIntObject);
	return makePyObjectTuple;
      case DSM_LONG:
	retInt = ((int *)value)[0];
	PyMem_Free(value);
	dprintf("Got the long %d\n", retInt);
	retIntObject = PyInt_FromLong((long)retInt);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retIntObject);
	return makePyObjectTuple;
      case DSM_FLOAT:
	retDouble = ((float *)value)[0];
	PyMem_Free(value);
	dprintf("Got the float %f\n", retDouble);
	retDoubleObject = PyFloat_FromDouble(retDouble);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retDoubleObject);
	return makePyObjectTuple;
      case DSM_DOUBLE:
	retDouble = ((double *)value)[0];
	PyMem_Free(value);
	dprintf("Got the double %f\n", retDouble);
	retDoubleObject = PyFloat_FromDouble(retDouble);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retDoubleObject);
	return makePyObjectTuple;
      }
    } else {
      /* Oh crap, it's an array */
      if ((type == DSM_STRING) && (nDim == 1)) {
	char *value;
	PyObject *retStringObject;
	
	/* OK, this is the easiest case: a simple string of length dimensions[0] */
	value = PyMem_Malloc(dimensions[0]*sizeof(char));
	PyMem_Free(dimensions);
	if (value == NULL) {
	  fprintf(stderr,"PyMem_Malloc for string type");
	  PyErr_NoMemory();
	  return NULL;
	}
	if (buf != NULL) {
	  bcopy(buf, value, dimensions[0]*sizeof(char));
	} else if (structure != NULL)
	  status = dsm_structure_get_element(structure, name, &value[0]);
	else
	  status = dsm_read(partner, name, &value[0], &timestamp);
	if (status != DSM_SUCCESS) {
	  PyMem_Free(value);	
	  if (dimensions != NULL)
	    PyMem_Free(dimensions);
	  raiseDSMError(status, "string DSM read or get_element");
	  return NULL;
	}
	dprintf("dsm_read of string (%s) completed successfully, with a timestamp of %d\n", value, (int)timestamp);
	makePyObjectTuple = PyTuple_New(2);
	readTime = PyInt_FromLong((long)timestamp);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)1, readTime);
	retStringObject = PyString_FromString(value);
	PyMem_Free(value);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, retStringObject);
	return makePyObjectTuple;
      } else {
	int i, arraySize, baseSize;
	int nContainerTuples;
	int stringSize = 0;
	int j, offset;
	char *arrayBase = NULL;
	PyObject *myTuple;
	
	/* Here we handle nontrivial arrays - arrays other than simple character strings */
	if (type == DSM_STRING) {
	  stringSize = dimensions[0];
	  for (i = 0; i < nDim-1; i++)
	    dimensions[i] = dimensions[i+1];
	  nDim -= 1;
	}
	dprintf("Nontrivial array of %d dimensions\n", nDim);
	arraySize = 1;
	for (i = 0; i < nDim; i++)
	  arraySize *= dimensions[i];
	dprintf("Full array size: %d\n", arraySize);
	switch (type) {
	case (DSM_BYTE):
	  baseSize = 1; break;
	case DSM_SHORT:
	  baseSize = 2; break;
	case DSM_LONG:
	case DSM_FLOAT:
	  baseSize = 4; break;
	case DSM_STRING:
	  baseSize = stringSize; break;
	default:
	  baseSize = 8;
	}
	arrayBase = PyMem_Malloc(arraySize * baseSize);
	if (arrayBase == NULL) {
	  fprintf(stderr, "PyMem_Malloc of arrayBase");
	  PyErr_NoMemory();
	  return NULL;
	}
	if (buf != NULL)
	  bcopy(buf, &arrayBase[0], arraySize * baseSize);
	else if (structure != NULL)
	  status = dsm_structure_get_element(structure, name, &arrayBase[0]);
	else
	  status = dsm_read(partner, name, &arrayBase[0], &timestamp);
	if (status != DSM_SUCCESS) {
	  if (dimensions != NULL)
	    PyMem_Free(dimensions);
	  raiseDSMError(status, "array DSM read or get_element");
	  return NULL;
	}
	dprintf("dsm_read of array completed successfully, with a timestamp of %d\n", (int)timestamp);
	makePyObjectTuple = PyTuple_New(2);
	readTime = PyInt_FromLong((long)timestamp);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)1, readTime);

	/* More than one dimension, so we need container tuples to contain the data tuples */
	nContainerTuples = 0;
	for (i = 0; i < nDim-1; i++) {
	  offset = 1;
	  for (j = 0; j < nDim - i - 1; j++)
	    offset *= dimensions[j];
	  nContainerTuples += offset;
	}
	nContainerTuples++;
	dprintf("I will need %d container tuples\n", nContainerTuples);
	containerTupleBase = (PyObject **)PyMem_Malloc(nContainerTuples*sizeof(PyObject *));
	if (containerTupleBase == NULL) {
	  fprintf(stderr, "PyMem_Malloc of container tuples");
	  PyErr_NoMemory();
	  return NULL;
	}
	myTuple = buildTuples(containerTupleBase, 0, nDim, dimensions, type, baseSize, arrayBase);
	PyMem_Free(dimensions);
	PyMem_Free(arrayBase);
	PyMem_Free(containerTupleBase);
	PyTuple_SetItem(makePyObjectTuple, (Py_ssize_t)0, myTuple);
	return makePyObjectTuple;
      }
    }
  }
  return NULL;
}

PyObject *handleStructure(char *partner, char *name)
{
  static int firstCall = TRUE;
  int status, i;
  static int nhosts;
  char *member = NULL;
  dsm_structure structure;
  static struct dsm_allocation_list *alp;
  PyObject *item, *readTime;
  PyObject *handleStructureDict;
  time_t timestamp;

  dprintf("in handleStructure(%s, %s)\n", partner, name);
  if (firstCall) {
    dprintf("Reading DSM allocation list\n");
    dsm_get_allocation_list(&nhosts, &alp);
    firstCall = FALSE;
  }

  status = dsm_structure_init(&structure, name);
  if (status != DSM_SUCCESS) {
    raiseDSMError(status, "init of structure");
    return NULL;
  }
  status = dsm_read(partner, name, &structure, &timestamp);
  if (status != DSM_SUCCESS) {
    raiseDSMError(status, "Read of structure");
    return NULL;
  }
  handleStructureDict = PyDict_New();
  dprintf("nhosts = %d\n", nhosts);
  for (i = 0; i < nhosts; i++) {
    if (!strcmp(partner, alp[i].host_name)) {
      int nEntries, j;

      nEntries = alp[i].n_entries;
      dprintf("Found my partner, with %d entries\n", nEntries);
      for (j = 0; j < nEntries; j++) {
	if (strstr(alp[i].alloc_list[j], name) == alp[i].alloc_list[j]) {
	  member = &alp[i].alloc_list[j][strlen(name)+1];
	  dprintf("Found member \"%s\"\n", member);
	  if ((item = makePyObject(partner, &structure, member, NULL, (time_t)0, FALSE)) == NULL)
	    return NULL;
	  else {
	    readTime = PyInt_FromLong((long)timestamp);
	    PyTuple_SetItem(item, (Py_ssize_t)1, readTime);
	    PyDict_SetItemString(handleStructureDict, member, item);
	    Py_DECREF(item);
	  }
	}
      }
      break;
    }
  }
  dsm_structure_destroy(&structure);
  return handleStructureDict;
}

static PyObject *pydsm_read(PyObject *self, PyObject *args)
{
  int status;
  static char *name = NULL;
  static char *partner = NULL;
  PyObject *readTuple = NULL;
  PyObject *retObject = NULL;

  status = open_dsm();
  if (status == DSM_SUCCESS) {
    if (!PyArg_ParseTuple(args, "ss", &partner, &name))
      return NULL;
    fixNames(partner, name);
    /* printf("pydsm_read: read request for \"%s\" on \"%s\"\n", name, partner); */
    /* printf("\"%s\"\n", name); */
    if (toupper(name[strlen(name)-1]) == 'X')
      readTuple = handleStructure(partner, name);
    else {
      readTuple = makePyObject(partner, NULL, name, NULL, (time_t)0, FALSE);
    }
    if (readTuple == NULL)
      return NULL;
    else {
      retObject = Py_BuildValue("O", readTuple);
      Py_DECREF(readTuple);
      return retObject;
    }
  } else
    return NULL;
}

static PyObject *pydsm_read_wait(PyObject *self)
{
  PyObject *readWaitTuple = NULL;
  PyObject *returnTuple;
  int status;

  if (monitorMaxSize <= 0) {
    PyErr_SetString(dSMNothingMonitored, "DSM error: read_wait called with nothing monitored.");
    return NULL;
  }
  status = open_dsm();
  if (status == DSM_SUCCESS) {
    char partner[DSM_NAME_LENGTH], allocName[DSM_NAME_LENGTH], *buf;

    buf = PyMem_Malloc(monitorMaxSize);
    if (buf == NULL) {
      fprintf(stderr, "PyMem_Malloc failure for read_wait buffer\n");
      PyErr_NoMemory();
      return NULL;
    }
    dprintf("Calling dsm_read_wait\n");
    status = dsm_read_wait(partner, allocName, buf);
    dprintf("Returned from read_wait - host = \"%s\", alloc = \"%s\"\n", partner, allocName);
    if (status == DSM_SUCCESS) {
      PyObject *partnerObj, *allocNameObj;
      time_t theTime;

      readWaitTuple = PyTuple_New(3);
      partnerObj = PyString_FromString(partner);
      PyTuple_SetItem(readWaitTuple, (Py_ssize_t)0, partnerObj);
      allocNameObj = PyString_FromString(allocName);
      PyTuple_SetItem(readWaitTuple, (Py_ssize_t)1, allocNameObj);
      theTime = time(NULL);
      PyTuple_SetItem(readWaitTuple, (Py_ssize_t)2, makePyObject(partner, NULL, allocName, buf, theTime, FALSE));
      PyMem_Free(buf);
      returnTuple = Py_BuildValue("O", readWaitTuple);
      Py_DECREF(readWaitTuple);
      return returnTuple;
    } else {
      PyMem_Free(buf);
      return NULL;
    }
  } else
    return NULL;
  Py_RETURN_NONE; /* This statement can't be reached.   It's here to prevent a compiler warning */
}

int getElement(PyObject *data, int nDim, int *indices, int type, char *buffer, int size)
{
  char tByte, *tString;
  short tShort;
  long tLong = 0;
  int i,  error;
  float tFloat;
  double tDouble;
  PyObject *item;

  item = data;
  error = FALSE;
  for (i = 0; i < nDim-1; i++) {
    item = PySequence_GetItem(item, indices[i]);
    Py_XDECREF(item);
    if (item == NULL) {
      fprintf(stderr, "Error on element %d, index %d\n", i, indices[i]);
      error = TRUE;
      break;
    }
  }
  if (error)
    return DSM_ERROR;
  item = PySequence_GetItem(item, indices[nDim-1]);
  switch (type) {
  case DSM_BYTE:
    tLong = PyLong_AsLong(item);
    dprintf("Got a value of %d decoded\n", (int)tLong);
    if ((SCHAR_MIN > tLong) || (tLong > SCHAR_MAX)) {
      fprintf(stderr, "%ld is out-of-range for a signed byte integer", tLong);
      PyErr_SetString(dSMRangeError, "DSM error: Value to be written is out of range");
      return DSM_ERROR;
    }
    tByte = (char)tLong;
    *buffer = tByte;
    break;
  case DSM_SHORT:
    tLong = PyLong_AsLong(item);
    dprintf("Got a value of %d decoded\n", (int)tLong);
    if ((SHRT_MIN > tLong) || (tLong > SHRT_MAX)) {
      fprintf(stderr, "%ld is out-of-range for a signed short integer", tLong);
      PyErr_SetString(dSMRangeError, "DSM error: Value to be written is out of range");
      return DSM_ERROR;
    }
    tShort = (short)tLong;
    *((short *)buffer) = tShort;
    break;
  case DSM_LONG:
    tLong = PyLong_AsLong(item);
    dprintf("Got a value of %d decoded\n", (int)tLong);
    *((int *)buffer) = tLong;
    break;
  case DSM_FLOAT:
    tFloat = (float)PyFloat_AsDouble(item);
    dprintf("Data decoded to %f\n", tFloat);
    *((float *)buffer) = tFloat;
    break;
  case DSM_DOUBLE:
    tDouble = PyFloat_AsDouble(item);
    dprintf("Data decoded to %f\n", tDouble);
    *((double *)buffer) = tDouble;
    break;
  case DSM_STRING:
    tString = PyString_AsString(item);
    dprintf("Got back a string of \"%s\"\n", tString);
    if (strlen(tString) > (size-1)) {
      PyErr_SetString(dSMRangeError, "DSM error: Sring passes to pydsm.write() is too large for target variable");
      return DSM_ERROR;
    } else
      strncpy(buffer, tString, size);
    break;
  default:
    PyErr_SetString(dSMNotImplemented, "DSM error: Unrecognized scalar type");
    return DSM_ERROR;
  }
  if (item != data)
    Py_XDECREF(item);
  return DSM_SUCCESS;
}

int buildArray(PyObject *data, int nDim, int *dimensions, int type, char **bigArray)
{
  int i, nElements, el, size, status;
  int *indices = NULL;

  switch (type) {
  case DSM_BYTE:
    size = sizeof(char); break;
  case DSM_SHORT:
    size = sizeof(short); break;
  case DSM_LONG:
    size = sizeof(int); break;
  case DSM_FLOAT:
    size = sizeof(float); break;
  case DSM_DOUBLE:
    size = sizeof(double); break;
  case DSM_STRING:
    size = dimensions[0];
    for (i = 0; i < nDim-1; i++)
      dimensions[i] = dimensions[i+1];
    nDim--;
    dprintf("String dimension %d\n", size);
    break;
  default:
    fprintf(stderr, "buildArray: Unrecognized type (%d)\n", type);
    return DSM_ERROR;
  }
  indices = PyMem_Malloc(nDim*sizeof(int));
  if (indices == NULL) {
    fprintf(stderr, "PyMem_Malloc of indices");
    PyErr_NoMemory();
    return DSM_ERROR;
  }
  nElements = 1;
  for (i = 0; i < nDim; i++) {
    indices[i] = 0;
    nElements *= dimensions[i];
  }
  *bigArray = PyMem_Malloc(nElements * size);
  if (*bigArray == NULL) {
    fprintf(stderr, "PyMem_Malloc of bigArray");
    PyErr_NoMemory();
    return DSM_ERROR;
  }
  for (el = 0; el < nElements; el++) {
    dprintf("%d:  ", el);
    for (i = 0; i < nDim; i++)
      dprintf("%d, ", indices[i]);
    dprintf(": ");
    status = getElement(data, nDim, indices, type, &((*bigArray)[el*size]), size);
    if (status != DSM_SUCCESS) {
      PyMem_Free(indices);
      return DSM_ERROR;
    }
    indices[nDim-1]++;
    for (i = nDim-1; i >= 0; i--)
      if (indices[i] == dimensions[i]) {
	indices[i] = 0;
	if (i > 0)
	  indices[i-1]++;
      }
  }
  PyMem_Free(indices);
  return DSM_SUCCESS;
}

int writeObject(char *partner, char *name, PyObject *data, int notify, dsm_structure *structure)
{
  char tByte;
  short tShort;
  int status = DSM_SUCCESS;
  int type, nDim;
  int *dimensions = NULL;
  long tLong;
  float tFloat;
  double tDouble;

  if ((status = decodeObject(name, &type, &nDim, &dimensions)) != DSM_SUCCESS) {
    fprintf(stderr, "Error %d returned by decodeObject *%s)\n", status, name);
    if (dimensions != NULL)
      PyMem_Free(dimensions);
    return DSM_ERROR;
  }
  if (nDim == 0) {
    /* Easiest case: just a single value */

    if (dimensions != NULL)
      PyMem_Free(dimensions);
    dprintf("Handling a simple scalar (%s)\n", name);
    switch (type) {
    case DSM_BYTE:
      tLong = PyLong_AsLong(data);
      dprintf("Got a value of %d decoded\n", (int)tLong);
      if ((SCHAR_MIN > tLong) || (tLong > SCHAR_MAX)) {
	fprintf(stderr, "%ld is out-of-range for a signed byte integer (%s)", tLong, name);
	PyErr_SetString(dSMRangeError, "DSM error: Value to be written is out of range");
	return DSM_ERROR;
      }
      tByte = (char)tLong;
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, &tByte);
	else
	  status = dsm_write(partner, name, &tByte);
      } else
	status = dsm_structure_set_element(structure, name, &tByte);
      break;
    case DSM_SHORT:
      tLong = PyLong_AsLong(data);
      dprintf("Got a value of %d decoded\n", (int)tLong);
      if ((SHRT_MIN > tLong) || (tLong > SHRT_MAX)) {
	fprintf(stderr, "%ld is out-of-range for a signed short integer (%s)", tLong, name);
	PyErr_SetString(dSMRangeError, "DSM error: Value to be written is out of range");
	return DSM_ERROR;
      }
      tShort = (short)tLong;
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, &tShort);
	else
	  status = dsm_write(partner, name, &tShort);
      } else
	status = dsm_structure_set_element(structure, name, &tShort);
      break;
    case DSM_LONG:
      tLong = PyLong_AsLong(data);
      dprintf("Got a value of %d decoded\n", (int)tLong);
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, &tLong);
	else
	  status = dsm_write(partner, name, &tLong);
      } else
	status = dsm_structure_set_element(structure, name, &tLong);
      break;
    case DSM_FLOAT:
      tFloat = (float)PyFloat_AsDouble(data);
      dprintf("Data decoded to %f\n", tFloat);
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, &tFloat);
	else
	  status = dsm_write(partner, name, &tFloat);
      } else
	status = dsm_structure_set_element(structure, name, &tFloat);
      break;
    case DSM_DOUBLE:
      tDouble = PyFloat_AsDouble(data);
      dprintf("Data decoded to %f\n", tDouble);
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, &tDouble);
	else
	  status = dsm_write(partner, name, &tDouble);
      } else
	status = dsm_structure_set_element(structure, name, &tDouble);
      break;
    default:
      PyErr_SetString(dSMNotImplemented, "DSM error: Unrecognized scalar type");
      return DSM_ERROR;
    }
  } else if ((nDim == 1) && (type == DSM_STRING)) {
    char *string;

    /* Second easiest case - a single string */
    if (dimensions != NULL)
      PyMem_Free(dimensions);
    string = PyString_AsString(data);
    if (string == NULL)
      return DSM_ERROR;
    dprintf("Writing a simple string \"%s\"\n", string);
    if (structure == NULL) {
      if (notify)
	status = dsm_write_notify(partner, name, string);
      else
	status = dsm_write(partner, name, string);
    } else
      status = dsm_structure_set_element(structure, name, string);
  } else {
    char *writeArray;

    dprintf("Handling an array of dimension %d\n", nDim);
    buildArray(data, nDim, dimensions, type, &writeArray);
    if (dimensions != NULL)
      PyMem_Free(dimensions);
    if (PyErr_Occurred()) {
	PyErr_SetString(dSMDecodeError, "DSM error: Could not decode all elements in tuple/list passed to pydsm.write().   This probably indicates a dimensionality problem or data type error.");
      PyMem_Free(writeArray);
      return DECODE_ERROR;
    } else {
      if (structure == NULL) {
	if (notify)
	  status = dsm_write_notify(partner, name, writeArray);
	else
	  status = dsm_write(partner, name, writeArray);
      } else
	status = dsm_structure_set_element(structure, name, writeArray);
      PyMem_Free(writeArray);
    }
  }
  return status;
}

static PyObject *pydsm_write(PyObject *self, PyObject *args, PyObject *keyWords)
{
  char *partner, *name;
  int status;
  int notify = FALSE;
  time_t timestamp;
  static char *keyWordList[] = {"partner", "name", "data", "notify", NULL};
  PyObject *notifyObject = NULL;
  PyObject *data;

  status = open_dsm();
  if (status == DSM_SUCCESS) {
    if (!PyArg_ParseTupleAndKeywords(args, keyWords, "ssO|O", keyWordList, &partner, &name, &data, &notifyObject))
      return NULL;
    fixNames(partner, name);
    if (notifyObject != NULL)
      notify = PyObject_IsTrue(notifyObject);
    dprintf("pydsm_write: write request for \"%s\" on \"%s\" notify = %d\n", name, partner, notify);
    if (strlen(name) < 2) {
      return NULL;
    } else if (name[strlen(name)-1] == 'X') {
      int i, nKeys;
      char *key;
      dsm_structure structure;
      PyObject *keys, *item;

      dprintf("Oh crap, it's a structure\n");
      if (!PyDict_Check(data)) {
	PyErr_SetString(dSMWrongType, "DSM error: Wrong type of data object passed to pydsm.write - must be a dictionary.");
	return NULL;
      }
      status = dsm_structure_init(&structure, name);
      if (status != DSM_SUCCESS) {
	raiseDSMError(status, "init of structure");
	return NULL;
      }
      status = dsm_read(partner, name, &structure, &timestamp);
      if (status != DSM_SUCCESS) {
	raiseDSMError(status, "pydsm.write() Read of structure");
	return NULL;
      }
      keys = PyDict_Keys(data);
      if (keys == NULL)
	return NULL;
      nKeys = PyList_Size(keys);
      dprintf("There are %d keys\n", nKeys);
      for (i = 0; i < nKeys; i++) {
	item = PyList_GetItem(keys, i);
	key = PyString_AsString(item);
	dprintf("Processing key %d: \"%s\"\n", i, key);
	item = PyDict_GetItemString(data, key);
	if (item == NULL)
	  return NULL; /* Should be impossible */
	status = writeObject(partner, key, item, notify, &structure);
	if (status != DSM_SUCCESS) {
	  if (status != DECODE_ERROR)
	    raiseDSMError(status, "pydsm_write");
	  return NULL;
	}	
      }
      Py_XDECREF(keys);
      if (notify)
	status = dsm_write(partner, name, &structure);
      else
	status = dsm_write_notify(partner, name, &structure);
      if (status != DSM_SUCCESS) {
	raiseDSMError(status, "pydsm.write() Write of structure");
	return NULL;
      }
      dsm_structure_destroy(&structure);
    } else {
      status = writeObject(partner, name, data, notify, NULL);
      if (status != DSM_SUCCESS) {
	if (status != DECODE_ERROR)
	  raiseDSMError(status, "pydsm_write");
	return NULL;
      }
    }
  } else
    return NULL;
  Py_RETURN_NONE;
}

static PyMethodDef pydsmMethods[] = {
  {"clear_monitor", (PyCFunction)pydsm_clear_monitor, METH_NOARGS,                  "Clear the monitor list"},
  {"close",         (PyCFunction)pydsm_close,         METH_NOARGS,                  "Close DSM, release resources"},
  {"monitor",                    pydsm_monitor,       METH_VARARGS,                 "Add a variable to the monitor list"},
  {"no_monitor",                 pydsm_no_monitor,    METH_VARARGS,                 "Remove a variable from the monitor list"},
  {"open",                       pydsm_open,          METH_VARARGS,                 "Initialize DSM"},
  {"read",                       pydsm_read,          METH_VARARGS,                 "Read a DSM variable"},
  {"read_wait",     (PyCFunction)pydsm_read_wait,     METH_NOARGS,                  "Wait for and read a monitored DSM variable"},
  {"write",         (PyCFunction)pydsm_write,         METH_VARARGS | METH_KEYWORDS, "Write a DSM variable"},
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initpydsm(void)
{
  PyObject *m;
  
  m = Py_InitModule3("pydsm", pydsmMethods, "Python API for the SMA DSM system");
  if (m == NULL)
    return;
  dSMNoShare = PyErr_NewException("pydsm.DSM_NoShare", NULL, NULL);
  Py_INCREF(dSMNoShare);
  PyModule_AddObject(m, "DSM_NoShare", dSMNoShare);
  dSMNoResource = PyErr_NewException("pydsm.DSM_NoResource", NULL, NULL);
  Py_INCREF(dSMNoResource);
  PyModule_AddObject(m, "DSM_NoResource", dSMNoResource);
  dSMNoSuchName = PyErr_NewException("pydsm.DSM_NoSuchName", NULL, NULL);
  Py_INCREF(dSMNoSuchName);
  PyModule_AddObject(m, "DSM_NoSuchName", dSMNoSuchName);
  dSMVersionMismatch = PyErr_NewException("pydsm.DSM_VersionMismatch", NULL, NULL);
  Py_INCREF(dSMVersionMismatch);
  PyModule_AddObject(m, "DSM_VersionMismatch", dSMVersionMismatch);
  dSMCatchAll = PyErr_NewException("pydsm.DSM_Catchall", NULL, NULL);
  Py_INCREF(dSMCatchAll);
  PyModule_AddObject(m, "DSM_Catchall", dSMCatchAll);
  dSMIllegalName = PyErr_NewException("pydsm.DSM_IllegalName", NULL, NULL);
  Py_INCREF(dSMIllegalName);
  PyModule_AddObject(m, "DSM_IllegalName", dSMIllegalName);
  dSMNotImplemented = PyErr_NewException("pydsm.DSM_NotImplemented", NULL, NULL);
  Py_INCREF(dSMNotImplemented);
  PyModule_AddObject(m, "DSM_NotImplemented", dSMNotImplemented);
  dSMRangeError = PyErr_NewException("pydsm.DSM_RangeError", NULL, NULL);
  Py_INCREF(dSMRangeError);
  PyModule_AddObject(m, "DSM_RangeError", dSMRangeError);
  dSMDecodeError = PyErr_NewException("pydsm.DSM_DecodeError", NULL, NULL);
  Py_INCREF(dSMDecodeError);
  PyModule_AddObject(m, "DSM_DecodeError", dSMDecodeError);
  dSMWrongType = PyErr_NewException("pydsm.DSM_WrongType", NULL, NULL);
  Py_INCREF(dSMWrongType);
  PyModule_AddObject(m, "DSM_WrongType", dSMWrongType);
  dSMNothingMonitored = PyErr_NewException("pydsm.DSM_NothingMonitored", NULL, NULL);
  Py_INCREF(dSMNothingMonitored);
  PyModule_AddObject(m, "DSM_NothingMonitored", dSMNothingMonitored);
  dSMRPCFailure = PyErr_NewException("pydsm.DSM_RPCFailure", NULL, NULL);
  Py_INCREF(dSMRPCFailure);
  PyModule_AddObject(m, "DSM_RPCFailure", dSMRPCFailure);
  dSMInternalError = PyErr_NewException("pydsm.DSM_InternalError", NULL, NULL);
  Py_INCREF(dSMInternalError);
  PyModule_AddObject(m, "DSM_InternalError", dSMInternalError);
}
