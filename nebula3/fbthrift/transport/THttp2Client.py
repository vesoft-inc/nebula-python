# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#
from nebula3.fbthrift.transport.TTransport import *
import httpx

default_timeout = 60

class THttp2Client(TTransportBase):
  def __init__(self, url, 
               timeout=None, 
               verify=None, 
               certfile=None, 
               keyfile=None, 
               password=None,
               http_headers=None,
               ):
    self.__wbuf = StringIO()
    self.__rbuf = StringIO()
    self.__http = None
    if timeout is not None and timeout > 0 :
      self.timeout = timeout
    if timeout is None:
      self.timeout = default_timeout

    self.url = url
    if verify is None:
      self.verify = False
    else:
      self.verify = verify
    if certfile is not None :
      self.cert = (certfile, keyfile, password)
    else: 
      self.cert = None
    self.response = None
    self.http_headers = http_headers
  
  def isOpen(self):
    return self.__http is not None and self.__http.is_closed is False

  def open(self):
    if self.cert is None:
      self.__http = httpx.Client(http1=False,http2=True, verify=False, timeout=self.timeout)
    else:
      self.__http = httpx.Client(http1=False,http2=True, verify=self.verify, cert=self.cert, timeout=self.timeout)

  def close(self):
    self.__http.close()
    self.__http = None

  def read(self, sz):
    return self.__rbuf.read(sz)


  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    if self.isOpen():
      self.close()
    self.open()

      # Pull data out of buffer
    data = self.__wbuf.getvalue()
    self.__wbuf = StringIO()

    # HTTP2 request
    header = {
      'Content-Type': 'application/x-thrift',
      'Content-Length': str(len(data)),
      'User-Agent': 'Python/THttpClient',
    }
    if self.http_headers is not None and isinstance(self.http_headers, dict):
      header.update(self.http_headers)
    try:
      self.response= self.__http.post(self.url, headers=header, data=data)
    except Exception as e:
      raise TTransportException(TTransportException.UNKNOWN, str(e))
    # Get reply to flush the request
    self.code = self.response.status_code
    self.headers = self.response.headers
    self.__rbuf = StringIO()
    self.__rbuf.write(self.response.read())
    self.__rbuf.seek(0)
