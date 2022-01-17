# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from nebula3.fbthrift.transport.TSocket import TSocket
from nebula3.fbthrift.transport.TTransport import TTransportException
import socket


class TSocketOverHttpTunnel(TSocket):
    def __init__(self, host, port, proxy_host, proxy_port):
        TSocket.__init__(self, proxy_host, proxy_port)
        try:
            # Use IP address since sometimes proxy_host cannot resolve
            # external hostnames using unbound
            info = socket.getaddrinfo(
                host,
                None,
                socket.AF_INET | socket.AF_INET6,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP)
            self.remote_host = info[0][4][0]
        except socket.error as e:
            raise TTransportException(TTransportException.NOT_OPEN, str(e))
        self.remote_port = port

    def open(self):
        TSocket.open(self)
        self.write("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n" % (
            self.remote_host, self.remote_port,
            self.remote_host, self.remote_port))
        res = self.read(4096)
        try:
            status = res.split()[1]
            if status != '200':
                self.close()
                raise TTransportException(TTransportException.NOT_OPEN,
                        "Error response from proxy server: %s" % res)
        except IndexError:
            self.close()
            raise TTransportException(TTransportException.NOT_OPEN,
                    "Error response from proxy server: %s" % res)
