#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#  @generated
#

from __future__ import absolute_import
import six
from thrift.util.Recursive import fix_spec
from thrift.Thrift import *
from thrift.protocol.TProtocol import TProtocolException


import nebula2.common.ttypes


import pprint
import warnings
from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from thrift.protocol import THeaderProtocol
fastproto = None
if not '__pypy__' in sys.builtin_module_names:
  try:
    from thrift.protocol import fastproto
  except:
    pass
all_structs = []
UTF8STRINGS = bool(0) or sys.version_info.major >= 3

__all__ = ['UTF8STRINGS', 'ErrorCode', 'ExecutionResponse', 'AuthResponse']

class ErrorCode:
  SUCCEEDED = 0
  E_DISCONNECTED = -1
  E_FAIL_TO_CONNECT = -2
  E_RPC_FAILURE = -3
  E_BAD_USERNAME_PASSWORD = -4
  E_SESSION_INVALID = -5
  E_SESSION_TIMEOUT = -6
  E_SYNTAX_ERROR = -7
  E_EXECUTION_ERROR = -8
  E_STATEMENT_EMTPY = -9

  _VALUES_TO_NAMES = {
    0: "SUCCEEDED",
    -1: "E_DISCONNECTED",
    -2: "E_FAIL_TO_CONNECT",
    -3: "E_RPC_FAILURE",
    -4: "E_BAD_USERNAME_PASSWORD",
    -5: "E_SESSION_INVALID",
    -6: "E_SESSION_TIMEOUT",
    -7: "E_SYNTAX_ERROR",
    -8: "E_EXECUTION_ERROR",
    -9: "E_STATEMENT_EMTPY",
  }

  _NAMES_TO_VALUES = {
    "SUCCEEDED": 0,
    "E_DISCONNECTED": -1,
    "E_FAIL_TO_CONNECT": -2,
    "E_RPC_FAILURE": -3,
    "E_BAD_USERNAME_PASSWORD": -4,
    "E_SESSION_INVALID": -5,
    "E_SESSION_TIMEOUT": -6,
    "E_SYNTAX_ERROR": -7,
    "E_EXECUTION_ERROR": -8,
    "E_STATEMENT_EMTPY": -9,
  }

class ExecutionResponse:
  """
  Attributes:
   - error_code
   - latency_in_us
   - data
   - space_name
   - error_msg
  """

  thrift_spec = None
  thrift_field_annotations = None
  thrift_struct_annotations = None
  __init__ = None
  @staticmethod
  def isUnion():
    return False

  def read(self, iprot):
    if (isinstance(iprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(iprot, THeaderProtocol.THeaderProtocolAccelerate) and iprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastproto is not None:
      fastproto.decode(self, iprot.trans, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0)
      self.checkRequired()
      return
    if (isinstance(iprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(iprot, THeaderProtocol.THeaderProtocolAccelerate) and iprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastproto is not None:
      fastproto.decode(self, iprot.trans, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2)
      self.checkRequired()
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.error_code = iprot.readI32()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.latency_in_us = iprot.readI32()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRUCT:
          self.data = nebula2.common.ttypes.DataSet()
          self.data.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.space_name = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.error_msg = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    if self.error_code == None:
      raise TProtocolException(TProtocolException.MISSING_REQUIRED_FIELD, "Required field 'error_code' was not found in serialized data! Struct: ExecutionResponse")

    if self.latency_in_us == None:
      raise TProtocolException(TProtocolException.MISSING_REQUIRED_FIELD, "Required field 'latency_in_us' was not found in serialized data! Struct: ExecutionResponse")

    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('ExecutionResponse')
    if self.error_code != None:
      oprot.writeFieldBegin('error_code', TType.I32, 1)
      oprot.writeI32(self.error_code)
      oprot.writeFieldEnd()
    if self.latency_in_us != None:
      oprot.writeFieldBegin('latency_in_us', TType.I32, 2)
      oprot.writeI32(self.latency_in_us)
      oprot.writeFieldEnd()
    if self.data != None:
      oprot.writeFieldBegin('data', TType.STRUCT, 3)
      self.data.write(oprot)
      oprot.writeFieldEnd()
    if self.space_name != None:
      oprot.writeFieldBegin('space_name', TType.STRING, 4)
      oprot.writeString(self.space_name)
      oprot.writeFieldEnd()
    if self.error_msg != None:
      oprot.writeFieldBegin('error_msg', TType.STRING, 5)
      oprot.writeString(self.error_msg)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.error_code is not None:
      value = pprint.pformat(self.error_code, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    error_code=%s' % (value))
    if self.latency_in_us is not None:
      value = pprint.pformat(self.latency_in_us, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    latency_in_us=%s' % (value))
    if self.data is not None:
      value = pprint.pformat(self.data, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    data=%s' % (value))
    if self.space_name is not None:
      value = pprint.pformat(self.space_name, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    space_name=%s' % (value))
    if self.error_msg is not None:
      value = pprint.pformat(self.error_msg, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    error_msg=%s' % (value))
    return "%s(%s)" % (self.__class__.__name__, "\n" + ",\n".join(L) if L else '')

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False

    return self.__dict__ == other.__dict__ 

  def __ne__(self, other):
    return not (self == other)

  # Override the __hash__ function for Python3 - t10434117
  if not six.PY2:
    __hash__ = object.__hash__

class AuthResponse:
  """
  Attributes:
   - error_code
   - error_msg
   - session_id
  """

  thrift_spec = None
  thrift_field_annotations = None
  thrift_struct_annotations = None
  __init__ = None
  @staticmethod
  def isUnion():
    return False

  def read(self, iprot):
    if (isinstance(iprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(iprot, THeaderProtocol.THeaderProtocolAccelerate) and iprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastproto is not None:
      fastproto.decode(self, iprot.trans, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0)
      self.checkRequired()
      return
    if (isinstance(iprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(iprot, THeaderProtocol.THeaderProtocolAccelerate) and iprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastproto is not None:
      fastproto.decode(self, iprot.trans, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2)
      self.checkRequired()
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.error_code = iprot.readI32()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.error_msg = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.session_id = iprot.readI64()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    if self.error_code == None:
      raise TProtocolException(TProtocolException.MISSING_REQUIRED_FIELD, "Required field 'error_code' was not found in serialized data! Struct: AuthResponse")

    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('AuthResponse')
    if self.error_code != None:
      oprot.writeFieldBegin('error_code', TType.I32, 1)
      oprot.writeI32(self.error_code)
      oprot.writeFieldEnd()
    if self.error_msg != None:
      oprot.writeFieldBegin('error_msg', TType.STRING, 2)
      oprot.writeString(self.error_msg)
      oprot.writeFieldEnd()
    if self.session_id != None:
      oprot.writeFieldBegin('session_id', TType.I64, 3)
      oprot.writeI64(self.session_id)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.error_code is not None:
      value = pprint.pformat(self.error_code, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    error_code=%s' % (value))
    if self.error_msg is not None:
      value = pprint.pformat(self.error_msg, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    error_msg=%s' % (value))
    if self.session_id is not None:
      value = pprint.pformat(self.session_id, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    session_id=%s' % (value))
    return "%s(%s)" % (self.__class__.__name__, "\n" + ",\n".join(L) if L else '')

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False

    return self.__dict__ == other.__dict__ 

  def __ne__(self, other):
    return not (self == other)

  # Override the __hash__ function for Python3 - t10434117
  if not six.PY2:
    __hash__ = object.__hash__

all_structs.append(ExecutionResponse)
ExecutionResponse.thrift_spec = (
  None, # 0
  (1, TType.I32, 'error_code', ErrorCode, None, 0, ), # 1
  (2, TType.I32, 'latency_in_us', None, None, 0, ), # 2
  (3, TType.STRUCT, 'data', [nebula2.common.ttypes.DataSet, nebula2.common.ttypes.DataSet.thrift_spec, False], None, 1, ), # 3
  (4, TType.STRING, 'space_name', False, None, 1, ), # 4
  (5, TType.STRING, 'error_msg', False, None, 1, ), # 5
)

ExecutionResponse.thrift_struct_annotations = {
}
ExecutionResponse.thrift_field_annotations = {
}

def ExecutionResponse__init__(self, error_code=None, latency_in_us=None, data=None, space_name=None, error_msg=None,):
  self.error_code = error_code
  self.latency_in_us = latency_in_us
  self.data = data
  self.space_name = space_name
  self.error_msg = error_msg

ExecutionResponse.__init__ = ExecutionResponse__init__

def ExecutionResponse__setstate__(self, state):
  state.setdefault('error_code', None)
  state.setdefault('latency_in_us', None)
  state.setdefault('data', None)
  state.setdefault('space_name', None)
  state.setdefault('error_msg', None)
  self.__dict__ = state

ExecutionResponse.__getstate__ = lambda self: self.__dict__.copy()
ExecutionResponse.__setstate__ = ExecutionResponse__setstate__

all_structs.append(AuthResponse)
AuthResponse.thrift_spec = (
  None, # 0
  (1, TType.I32, 'error_code', ErrorCode, None, 0, ), # 1
  (2, TType.STRING, 'error_msg', False, None, 1, ), # 2
  (3, TType.I64, 'session_id', None, None, 1, ), # 3
)

AuthResponse.thrift_struct_annotations = {
}
AuthResponse.thrift_field_annotations = {
}

def AuthResponse__init__(self, error_code=None, error_msg=None, session_id=None,):
  self.error_code = error_code
  self.error_msg = error_msg
  self.session_id = session_id

AuthResponse.__init__ = AuthResponse__init__

def AuthResponse__setstate__(self, state):
  state.setdefault('error_code', None)
  state.setdefault('error_msg', None)
  state.setdefault('session_id', None)
  self.__dict__ = state

AuthResponse.__getstate__ = lambda self: self.__dict__.copy()
AuthResponse.__setstate__ = AuthResponse__setstate__

fix_spec(all_structs)
del all_structs
