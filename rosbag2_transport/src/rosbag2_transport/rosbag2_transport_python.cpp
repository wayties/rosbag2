// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Python.h>
#include <chrono>
#include <string>
#include <vector>

#include "rosbag2_transport/rosbag2_transport.hpp"
#include "rosbag2_transport/record_options.hpp"
#include "rosbag2_transport/storage_options.hpp"
#include "rmw/rmw.h"

static PyObject *
rosbag2_transport_record(PyObject * Py_UNUSED(self), PyObject * args, PyObject * kwargs)
{
  rosbag2_transport::StorageOptions storage_options{};
  rosbag2_transport::RecordOptions record_options{};

  static const char * kwlist[] = {
    "uri",
    "storage_id",
    "serialization_format",
    "node_prefix",
    "all",
    "no_discovery",
    "polling_interval",
    "topics",
    nullptr};

  char * uri = nullptr;
  char * storage_id = nullptr;
  char * serilization_format = nullptr;
  char * node_prefix = nullptr;
  bool all = false;
  bool no_discovery = false;
  uint64_t polling_interval_ms = 100;
  PyObject * topics = nullptr;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ssss|bbKO", const_cast<char **>(kwlist),
    &uri,
    &storage_id,
    &serilization_format,
    &node_prefix,
    &all,
    &no_discovery,
    &polling_interval_ms,
    &topics))
  {
    return nullptr;
  }

  storage_options.uri = std::string(uri);
  storage_options.storage_id = std::string(storage_id);
  record_options.all = all;
  record_options.is_discovery_disabled = no_discovery;
  record_options.topic_polling_interval = std::chrono::milliseconds(polling_interval_ms);
  record_options.node_prefix = std::string(node_prefix);

  if (topics) {
    PyObject * topic_iterator = PyObject_GetIter(topics);
    if (topic_iterator != nullptr) {
      PyObject * topic;
      while ((topic = PyIter_Next(topic_iterator))) {
        record_options.topics.emplace_back(PyUnicode_AsUTF8(topic));

        Py_DECREF(topic);
      }
      Py_DECREF(topic_iterator);
    }
  }
  record_options.rmw_serialization_format = std::string(serilization_format).empty() ?
    rmw_get_serialization_format() :
    serilization_format;

  rosbag2_transport::Rosbag2Transport transport;
  transport.init();
  transport.record(storage_options, record_options);
  transport.shutdown();

  Py_RETURN_NONE;
}

static PyObject *
rosbag2_transport_play(PyObject * Py_UNUSED(self), PyObject * args, PyObject * kwargs)
{
  rosbag2_transport::PlayOptions play_options{};
  rosbag2_transport::StorageOptions storage_options{};

  static const char * kwlist[] = {
    "uri",
    "storage_id",
    "node_prefix",
    "read_ahead_queue_size",
    nullptr
  };

  char * uri;
  char * storage_id;
  char * node_prefix;
  size_t read_ahead_queue_size;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sss|k", const_cast<char **>(kwlist),
    &uri,
    &storage_id,
    &node_prefix,
    &read_ahead_queue_size))
  {
    return nullptr;
  }

  storage_options.uri = std::string(uri);
  storage_options.storage_id = std::string(storage_id);

  play_options.node_prefix = std::string(node_prefix);
  play_options.read_ahead_queue_size = read_ahead_queue_size;

  rosbag2_transport::Rosbag2Transport transport;
  transport.init();
  transport.play(storage_options, play_options);
  transport.shutdown();

  Py_RETURN_NONE;
}

static PyObject *
rosbag2_transport_info(PyObject * Py_UNUSED(self), PyObject * args, PyObject * kwargs)
{
  static const char * kwlist[] = {"uri", "storage_id", nullptr};

  char * char_uri;
  char * char_storage_id;
  if (!PyArg_ParseTupleAndKeywords(
      args, kwargs, "ss", const_cast<char **>(kwlist), &char_uri, &char_storage_id))
  {
    return nullptr;
  }

  std::string uri = std::string(char_uri);
  std::string storage_id = std::string(char_storage_id);

  rosbag2_transport::Rosbag2Transport transport;
  transport.print_bag_info(uri, storage_id);

  Py_RETURN_NONE;
}

static std::shared_ptr<rosbag2_transport::Rosbag2Transport> transport_ = nullptr;
static std::shared_ptr<rosbag2_introspection_message_t> introspection_message_ = nullptr;

static void *
get_capsule_pointer(PyObject * pymetaclass, const char * attr)
{
  PyObject * pyattr = PyObject_GetAttrString(pymetaclass, attr);
  if (!pyattr) {
    return nullptr;
  }
  void * ptr = PyCapsule_GetPointer(pyattr, nullptr);
  Py_DECREF(pyattr);
  return ptr;
}

static PyObject *
convert_to_py(void * message, PyObject * pyclass)
{
  PyObject * pymetaclass = PyObject_GetAttrString(pyclass, "__class__");
  if (!pymetaclass) {
    return nullptr;
  }
  PyObject * (*convert)(void *) = (PyObject * (*)(void*))get_capsule_pointer(
      pymetaclass, "_CONVERT_TO_PY");
  Py_DECREF(pymetaclass);
  if (!convert) {
    return nullptr;
  }
  return convert(message);
}

static PyObject *
rosbag2_transport_open(PyObject * Py_UNUSED(self), PyObject * args)
{
  rosbag2_transport::StorageOptions storage_options{};

  char * char_uri;
  char * char_storage_id;
  if (!PyArg_ParseTuple(args, "ss", &char_uri, &char_storage_id)) {
    return nullptr;
  }

  storage_options.uri = std::string(char_uri);
  storage_options.storage_id = std::string(char_storage_id);

  if (transport_ != nullptr) {
    transport_.reset();
    transport_ = nullptr;
  }

  transport_ = std::make_shared<rosbag2_transport::Rosbag2Transport>();
  if (transport_->open(storage_options)) {
    Py_RETURN_TRUE;
  }

  transport_.reset();
  transport_ = nullptr;

  Py_RETURN_FALSE;
}

static PyObject *
rosbag2_transport_next(PyObject * Py_UNUSED(self), PyObject * Py_UNUSED(noargs))
{
  if (transport_ == nullptr) {
    return nullptr;
  }

  introspection_message_ = transport_->next();
  if (introspection_message_) {
    return Py_BuildValue("s", introspection_message_->topic_name);
  }

  Py_RETURN_NONE;
}

static PyObject *
rosbag2_transport_convert(PyObject * Py_UNUSED(self), PyObject * args)
{
  PyObject * pymsg_type;

  if (!PyArg_ParseTuple(args, "O", &pymsg_type)) {
    return nullptr;
  }

  if (transport_ == nullptr) {
    return nullptr;
  }

  if (introspection_message_) {
    PyObject * pymsg = convert_to_py(introspection_message_->message, pymsg_type);
    if (!pymsg) {
      return nullptr;
    }
    PyObject * val = Py_BuildValue("dO", introspection_message_->time_stamp * 1e-9, pymsg);
    Py_DECREF(pymsg);
    return val;
  }

  return nullptr;
}

static PyObject *
rosbag2_transport_close(PyObject * Py_UNUSED(self), PyObject * Py_UNUSED(noargs))
{
  if (transport_ == nullptr) {
    return nullptr;
  }

  if (introspection_message_) {
    introspection_message_.reset();
    introspection_message_ = nullptr;
  }

  transport_.reset();
  transport_ = nullptr;

  Py_RETURN_TRUE;
}

/// Define the public methods of this module
#if __GNUC__ >= 8
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wcast-function-type"
#endif
static PyMethodDef rosbag2_transport_methods[] = {
  {
    "record", reinterpret_cast<PyCFunction>(rosbag2_transport_record), METH_VARARGS | METH_KEYWORDS,
    "Record to bag"
  },
  {
    "play", reinterpret_cast<PyCFunction>(rosbag2_transport_play), METH_VARARGS | METH_KEYWORDS,
    "Play bag"
  },
  {
    "info", reinterpret_cast<PyCFunction>(rosbag2_transport_info), METH_VARARGS | METH_KEYWORDS,
    "Print bag info"
  },
  {
    "open", reinterpret_cast<PyCFunction>(rosbag2_transport_open), METH_VARARGS,
    "Open bag"
  },
  {
    "next", reinterpret_cast<PyCFunction>(rosbag2_transport_next), METH_NOARGS,
    "Read next message in opened bag"
  },
  {
    "convert", reinterpret_cast<PyCFunction>(rosbag2_transport_convert), METH_VARARGS,
    "Convert current message to python object"
  },
  {
    "close", reinterpret_cast<PyCFunction>(rosbag2_transport_close), METH_NOARGS,
    "Close opened bag"
  },
  {nullptr, nullptr, 0, nullptr}  /* sentinel */
};
#if __GNUC__ >= 8
# pragma GCC diagnostic pop
#endif

PyDoc_STRVAR(rosbag2_transport__doc__,
  "Python module for rosbag2 transport");

/// Define the Python module
static struct PyModuleDef _rosbag2_transport_module = {
  PyModuleDef_HEAD_INIT,
  "_rosbag2_transport",
  rosbag2_transport__doc__,
  -1,   /* -1 means that the module keeps state in global variables */
  rosbag2_transport_methods,
  nullptr,
  nullptr,
  nullptr,
  nullptr
};

/// Init function of this module
PyMODINIT_FUNC PyInit__rosbag2_transport_py(void)
{
  return PyModule_Create(&_rosbag2_transport_module);
}
