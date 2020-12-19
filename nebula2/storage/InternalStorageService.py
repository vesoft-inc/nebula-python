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


from .ttypes import *
import nebula2.common.ttypes
import nebula2.meta.ttypes

from thrift.Thrift import TProcessor
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

from thrift.util.Decorators import *

class Iface:
  def getValue(self, req=None):
    """
    Parameters:
     - req
    """
    pass

  def forwardTransaction(self, req=None):
    """
    Parameters:
     - req
    """
    pass


class ContextIface:
  def getValue(self, handler_ctx, req=None):
    """
    Parameters:
     - req
    """
    pass

  def forwardTransaction(self, handler_ctx, req=None):
    """
    Parameters:
     - req
    """
    pass


# HELPER FUNCTIONS AND STRUCTURES

class getValue_args:
  """
  Attributes:
   - req
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
        if ftype == TType.STRUCT:
          self.req = GetValueRequest()
          self.req.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('getValue_args')
    if self.req != None:
      oprot.writeFieldBegin('req', TType.STRUCT, 1)
      self.req.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.req is not None:
      value = pprint.pformat(self.req, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    req=%s' % (value))
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

all_structs.append(getValue_args)
getValue_args.thrift_spec = (
  None, # 0
  (1, TType.STRUCT, 'req', [GetValueRequest, GetValueRequest.thrift_spec, False], None, 2, ), # 1
)

getValue_args.thrift_struct_annotations = {
}
getValue_args.thrift_field_annotations = {
}

def getValue_args__init__(self, req=None,):
  self.req = req

getValue_args.__init__ = getValue_args__init__

def getValue_args__setstate__(self, state):
  state.setdefault('req', None)
  self.__dict__ = state

getValue_args.__getstate__ = lambda self: self.__dict__.copy()
getValue_args.__setstate__ = getValue_args__setstate__

class getValue_result:
  """
  Attributes:
   - success
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
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = GetValueResponse()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('getValue_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.success is not None:
      value = pprint.pformat(self.success, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    success=%s' % (value))
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

all_structs.append(getValue_result)
getValue_result.thrift_spec = (
  (0, TType.STRUCT, 'success', [GetValueResponse, GetValueResponse.thrift_spec, False], None, 2, ), # 0
)

getValue_result.thrift_struct_annotations = {
}
getValue_result.thrift_field_annotations = {
}

def getValue_result__init__(self, success=None,):
  self.success = success

getValue_result.__init__ = getValue_result__init__

def getValue_result__setstate__(self, state):
  state.setdefault('success', None)
  self.__dict__ = state

getValue_result.__getstate__ = lambda self: self.__dict__.copy()
getValue_result.__setstate__ = getValue_result__setstate__

class forwardTransaction_args:
  """
  Attributes:
   - req
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
        if ftype == TType.STRUCT:
          self.req = InternalTxnRequest()
          self.req.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('forwardTransaction_args')
    if self.req != None:
      oprot.writeFieldBegin('req', TType.STRUCT, 1)
      self.req.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.req is not None:
      value = pprint.pformat(self.req, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    req=%s' % (value))
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

all_structs.append(forwardTransaction_args)
forwardTransaction_args.thrift_spec = (
  None, # 0
  (1, TType.STRUCT, 'req', [InternalTxnRequest, InternalTxnRequest.thrift_spec, False], None, 2, ), # 1
)

forwardTransaction_args.thrift_struct_annotations = {
}
forwardTransaction_args.thrift_field_annotations = {
}

def forwardTransaction_args__init__(self, req=None,):
  self.req = req

forwardTransaction_args.__init__ = forwardTransaction_args__init__

def forwardTransaction_args__setstate__(self, state):
  state.setdefault('req', None)
  self.__dict__ = state

forwardTransaction_args.__getstate__ = lambda self: self.__dict__.copy()
forwardTransaction_args.__setstate__ = forwardTransaction_args__setstate__

class forwardTransaction_result:
  """
  Attributes:
   - success
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
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = ExecResponse()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()
    self.checkRequired()

  def checkRequired(self):
    return

  def write(self, oprot):
    if (isinstance(oprot, TBinaryProtocol.TBinaryProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_BINARY_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=0))
      return
    if (isinstance(oprot, TCompactProtocol.TCompactProtocolAccelerated) or (isinstance(oprot, THeaderProtocol.THeaderProtocolAccelerate) and oprot.get_protocol_id() == THeaderProtocol.THeaderProtocol.T_COMPACT_PROTOCOL)) and self.thrift_spec is not None and fastproto is not None:
      oprot.trans.write(fastproto.encode(self, [self.__class__, self.thrift_spec, False], utf8strings=UTF8STRINGS, protoid=2))
      return
    oprot.writeStructBegin('forwardTransaction_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def __repr__(self):
    L = []
    padding = ' ' * 4
    if self.success is not None:
      value = pprint.pformat(self.success, indent=0)
      value = padding.join(value.splitlines(True))
      L.append('    success=%s' % (value))
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

all_structs.append(forwardTransaction_result)
forwardTransaction_result.thrift_spec = (
  (0, TType.STRUCT, 'success', [ExecResponse, ExecResponse.thrift_spec, False], None, 2, ), # 0
)

forwardTransaction_result.thrift_struct_annotations = {
}
forwardTransaction_result.thrift_field_annotations = {
}

def forwardTransaction_result__init__(self, success=None,):
  self.success = success

forwardTransaction_result.__init__ = forwardTransaction_result__init__

def forwardTransaction_result__setstate__(self, state):
  state.setdefault('success', None)
  self.__dict__ = state

forwardTransaction_result.__getstate__ = lambda self: self.__dict__.copy()
forwardTransaction_result.__setstate__ = forwardTransaction_result__setstate__

class Client(Iface):
  def __enter__(self):
    return self

  def __exit__(self, type, value, tb):
    self._iprot.trans.close()
    if self._iprot is not self._oprot:
      self._oprot.trans.close()

  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot != None:
      self._oprot = oprot
    self._seqid = 0

  def getValue(self, req=None):
    """
    Parameters:
     - req
    """
    self.send_getValue(req)
    return self.recv_getValue()

  def send_getValue(self, req=None):
    self._oprot.writeMessageBegin('getValue', TMessageType.CALL, self._seqid)
    args = getValue_args()
    args.req = req
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_getValue(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = getValue_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "getValue failed: unknown result");

  def forwardTransaction(self, req=None):
    """
    Parameters:
     - req
    """
    self.send_forwardTransaction(req)
    return self.recv_forwardTransaction()

  def send_forwardTransaction(self, req=None):
    self._oprot.writeMessageBegin('forwardTransaction', TMessageType.CALL, self._seqid)
    args = forwardTransaction_args()
    args.req = req
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_forwardTransaction(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = forwardTransaction_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    raise TApplicationException(TApplicationException.MISSING_RESULT, "forwardTransaction failed: unknown result");


class Processor(Iface, TProcessor):
  _onewayMethods = ()

  def __init__(self, handler):
    TProcessor.__init__(self)
    self._handler = handler
    self._processMap = {}
    self._priorityMap = {}
    self._processMap["getValue"] = Processor.process_getValue
    self._priorityMap["getValue"] = TPriority.NORMAL
    self._processMap["forwardTransaction"] = Processor.process_forwardTransaction
    self._priorityMap["forwardTransaction"] = TPriority.NORMAL

  def onewayMethods(self):
    l = []
    l.extend(Processor._onewayMethods)
    return tuple(l)

  @process_main()
  def process(self,): pass

  @process_method(getValue_args, oneway=False)
  def process_getValue(self, args, handler_ctx):
    result = getValue_result()
    try:
      result.success = self._handler.getValue(args.req)
    except:
      ex = sys.exc_info()[1]
      self._event_handler.handlerError(handler_ctx, 'getValue', ex)
      result = Thrift.TApplicationException(message=str(ex))
    return result

  @process_method(forwardTransaction_args, oneway=False)
  def process_forwardTransaction(self, args, handler_ctx):
    result = forwardTransaction_result()
    try:
      result.success = self._handler.forwardTransaction(args.req)
    except:
      ex = sys.exc_info()[1]
      self._event_handler.handlerError(handler_ctx, 'forwardTransaction', ex)
      result = Thrift.TApplicationException(message=str(ex))
    return result

Iface._processor_type = Processor

class ContextProcessor(ContextIface, TProcessor):
  _onewayMethods = ()

  def __init__(self, handler):
    TProcessor.__init__(self)
    self._handler = handler
    self._processMap = {}
    self._priorityMap = {}
    self._processMap["getValue"] = ContextProcessor.process_getValue
    self._priorityMap["getValue"] = TPriority.NORMAL
    self._processMap["forwardTransaction"] = ContextProcessor.process_forwardTransaction
    self._priorityMap["forwardTransaction"] = TPriority.NORMAL

  def onewayMethods(self):
    l = []
    l.extend(ContextProcessor._onewayMethods)
    return tuple(l)

  @process_main()
  def process(self,): pass

  @process_method(getValue_args, oneway=False)
  def process_getValue(self, args, handler_ctx):
    result = getValue_result()
    try:
      result.success = self._handler.getValue(handler_ctx, args.req)
    except:
      ex = sys.exc_info()[1]
      self._event_handler.handlerError(handler_ctx, 'getValue', ex)
      result = Thrift.TApplicationException(message=str(ex))
    return result

  @process_method(forwardTransaction_args, oneway=False)
  def process_forwardTransaction(self, args, handler_ctx):
    result = forwardTransaction_result()
    try:
      result.success = self._handler.forwardTransaction(handler_ctx, args.req)
    except:
      ex = sys.exc_info()[1]
      self._event_handler.handlerError(handler_ctx, 'forwardTransaction', ex)
      result = Thrift.TApplicationException(message=str(ex))
    return result

ContextIface._processor_type = ContextProcessor

fix_spec(all_structs)
del all_structs

