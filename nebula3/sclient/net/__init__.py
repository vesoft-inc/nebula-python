#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import socket

from nebula3.Exception import InValidHostname
from nebula3.storage import GraphStorageService
from nebula3.storage.ttypes import ScanVertexRequest, ScanEdgeRequest
from nebula3.fbthrift.transport import TSocket, TTransport
from nebula3.fbthrift.protocol import TBinaryProtocol


class GraphStorageConnection(object):
    def __init__(self, address, timeout, meta_cache):
        self._address = address
        self._timeout = timeout
        self._meta_cache = meta_cache
        self._connection = None
        self._ip = ''
        try:
            self._ip = socket.gethostbyname(address.host)
            if not isinstance(address.port, int):
                raise RuntimeError('Wrong port type: {}'.format(type(address.port)))
        except Exception:
            raise InValidHostname(str(address.host))

    def open(self):
        try:
            self.close()
            s = TSocket.TSocket(self._address.host, self._address.port)
            if self._timeout > 0:
                s.setTimeout(self._timeout)
            transport = TTransport.TBufferedTransport(s)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self._connection = GraphStorageService.Client(protocol)
        except Exception:
            raise

    def scan_vertex(self, req):
        return self._connection.scanVertex(req)

    def scan_edge(self, req):
        return self._connection.scanEdge(req)

    def storage_addr(self):
        return self._address

    def update_leader_info(self, space_id, part_id, address):
        self._meta_cache.update_storage_leader(space_id, part_id, address)

    def close(self):
        try:
            if self._connection is not None:
                self._connection._iprot.trans.close()
        except Exception:
            raise

    def __del__(self):
        self.close()
