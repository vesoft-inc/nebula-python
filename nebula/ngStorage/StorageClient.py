# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import socket
import struct
import random
import logging

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from storage.StorageService import Client
from storage.ttypes import EntryId
from storage.ttypes import PropDef
from storage.ttypes import PropOwner
from storage.ttypes import ScanEdgeRequest
from storage.ttypes import ScanVertexRequest
from storage.ttypes import ErrorCode

class Iterator:
  def __init__(self, it=None):
    self._it = iter(it)
    self._hasnext = None

  def __iter__(self):
      return self

  def next(self):
    if self._hasnext:
      result = self._thenext
    else:
      result = next(self._it)
    self._hasnext = None
    return result

  def has_next(self):
    if self._hasnext is None:
      try:
          self._thenext = next(self._it)
      except StopIteration:
          self._hasnext = False
      else:
          self._hasnext = True
    return self._hasnext


class ScanEdgeResponseIter:
    def __init__(self, client_dad, space, leader, scan_edge_request, client):
        self._client_dad = client_dad
        self._space = space
        self._leader = leader
        self._scan_edge_request = scan_edge_request
        self._client = client
        self._cursor = None
        self._have_next = True

    def has_next(self):
        return self._have_next

    def next(self):
        self._scan_edge_request.cursor = self._cursor
        scan_edge_response = self._client.scanEdge(self._scan_edge_request)
        if scan_edge_response is None:
            raise Exception('scan_edge_response is None')
        self._cursor = scan_edge_response.next_cursor
        self._have_next = scan_edge_response.has_next

        if not self._client_dad.is_successfully(scan_edge_response):
            self._leader, self._client = self._client_dad.handle_result_codes(scan_edge_response.result.failed_codes, self._space)
            self._have_next = False
            return None
        else:
            return scan_edge_response

        return None


class ScanVertexResponseIter:
    def __init__(self, client_dad, space, leader, scan_vertex_request, client):
        self._client_dad = client_dad
        self._space = space
        self._leader = leader
        self._scan_vertex_request = scan_vertex_request
        self._client = client
        self._cursor = None
        self._have_next = True

    def has_next(self):
        return self._have_next

    def next(self):
        self._scan_vertex_request.cursor = self._cursor
        scan_vertex_response = self._client.scanVertex(self._scan_vertex_request)
        if scan_vertex_response is None:
            raise Exception('scan_vertex_reponse is None')
        self._cursor = scan_vertex_response.next_cursor
        self._have_next = scan_vertex_response.has_next

        if not self._client_dad.is_successfully(scan_vertex_response):
            logging.info('scan_vertex_response is not successfully, failed_codes: ', scan_vertex_response.result.failed_codes)
            self._leader, self._client = self._client_dad.handle_result_codes(scan_vertex_response.result.failed_codes, self._space)
            self._have_next = False
            return None
        else:
            return scan_vertex_response

        return None


class ScanSpaceEdgeResponseIter:
    def __init__(self, client_dad, space, part_ids_iter, return_cols, all_cols, limit, start_time, end_time):
        self._client_dad = client_dad
        self._scan_edge_response_iter = None
        self._space = space
        self._part_ids_iter = part_ids_iter
        self._return_cols = return_cols
        self._all_cols = all_cols
        self._limit = limit
        self._start_time = start_time
        self._end_time = end_time

    def has_next(self):
        return self._part_ids_iter.has_next() or self._scan_edge_response_iter.has_next()

    def next(self):
        if self._scan_edge_response_iter is None or not self._scan_edge_response_iter.has_next():
            part = self._part_ids_iter.next()
            if part is None:
                return None
            leader = self._client_dad.get_leader(self._space, part)
            if leader is None:
                raise Exception('part %s not found in space %s' % (part, self._space))
            space_id = self._client_dad._meta_client.get_space_id_from_cache(self._space)
            if space_id == -1:
                raise Exception('space %s not found' % self._space)
            colums = self._client_dad.get_edge_return_cols(self._space, self._return_cols)
            scan_edge_request = ScanEdgeRequest(space_id, part, None, colums, self._all_cols, self._limit, self._start_time, self._end_time)
            self._scan_edge_response_iter = self._client_dad.do_scan_edge(self._space, leader, scan_edge_request)
            if self._scan_edge_response_iter is None:
                raise Exception('scan_edge_response_iter is None')

        return self._scan_edge_response_iter.next()


class ScanSpaceVertexResponseIter:
    def __init__(self, client_dad, space, part_ids_iter, return_cols, all_cols, limit, start_time, end_time):
        self._client_dad = client_dad
        self._scan_vertex_response_iter = None
        self._space = space
        self._part_ids_iter = part_ids_iter
        self._return_cols = return_cols
        self._all_cols = all_cols
        self._limit = limit
        self._start_time = start_time
        self._end_time = end_time

    def has_next(self):
        return self._part_ids_iter.has_next() or self._scan_vertex_response_iter.has_next()

    def next(self):
        if self._scan_vertex_response_iter is None or not self._scan_vertex_response_iter.has_next():
            part = self._part_ids_iter.next()
            if part is None:
                return None
            leader = self._client_dad.get_leader(self._space, part)
            if leader is None:
                raise Exception('part %s not found in space %s' % (part, self._space))
            space_id = self._client_dad._meta_client.get_space_id_from_cache(self._space)
            if space_id == -1:
                raise Exception('space %s not found' % self._space)
            colums = self._client_dad.get_vertex_return_cols(self._space, self._return_cols)
            scan_vertex_request = ScanVertexRequest(space_id, part, None, colums, self._all_cols, self._limit, self._start_time, self._end_time)
            self._scan_vertex_response_iter = self._client_dad.do_scan_vertex(self._space, leader, scan_vertex_request)
            if self._scan_vertex_response_iter is None:
                raise Exception('scan_vertex_response_iter is None')

        return self._scan_vertex_response_iter.next()


class StorageClient:
    def __init__(self, meta_client, timeout=1000,
            connection_retry=3):
        """Initializer
        Arguments:
            - meta_clent: an initialized MetaClient
            - timeout: maximum connection timeout in millisecond
            - connection_retry: maximum number of connection retries
        Returns: empty
        """
        self._meta_client = meta_client
        self._clients = {}
        self._leaders = {}
        self._timeout = timeout
        self._connection_retry = connection_retry

    def connect(self, address):
        """ connect to storage server
        Arguments:
            - address: the address of the storage servers
        Returns:
            - client: a storage client object
        """
        retry = self._connection_retry
        if address not in self._clients.keys():
            while retry > 0:
                client = self.do_connect(address)
                if client != None:
                    self._clients[address] = client
                    return client
                retry -= 1
            return None
        else:
            return self._clients[address]

    def do_connects(self, addresses):
        for address in addresses:
            client = self.do_connect(address)
            self._clients[address] = client

    def do_connect(self, address):
        host = address[0]
        port = address[1]
        try:
            transport = TSocket.TSocket(host, port)
            transport.setTimeout(self._timeout)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            return Client(protocol)
        except Exception as x:
            logging.exception(x)
            return None

    def scan_edge(self, space, return_cols, all_cols, limit, start_time, end_time):
        """ scan edges of a space
        Arguments:
            - space: name of the space to scan
            - return_cols: the edge's attribute columns to be returned
            - all_cols: whether to return all attribute columns.
            - limit: maximum number of data returned
            - start_time: start time of the edge data to return
            - end_time: end time of the edge data to return
        Returns:
            - iter: an iterator that can traverse all scanned edge data
        """
        part_ids = self._meta_client.get_parts_alloc_from_cache()[space].keys()
        part_ids_iter = Iterator(part_ids)
        return ScanSpaceEdgeResponseIter(self, space, part_ids_iter, return_cols, all_cols, limit, start_time, end_time)

    def scan_part_edge(self, space, part, return_cols, all_cols, limit, start_time, end_time):
        """ scan edges of a partition
        Arguments:
            - space: name of the space to scan
            - return_cols: the edge's attribute columns to be returned
            - all_cols: whether to return all attribute columns.
            - limit: maximum number of data returned
            - start_time: start time of the edge data to return
            - end_time: end time of the edge data to return
        Returns:
            - iter: an iterator that can traverse all scanned edge data
        """
        space_id = self._meta_client.get_space_id_from_cache(space)
        columns = self.get_edge_return_cols(space, return_cols)
        scan_edge_request = ScanEdgeRequest(space_id, part, None, columns, all_cols, limit, start_time, end_time)
        leader = self.get_leader(space, part)
        if leader is None:
            raise Exception('part %s not found in space %s' % (part, space))
        return self.do_scan_edge(space, leader, scan_edge_request)

    def scan_vertex(self, space, return_cols, all_cols, limit, start_time, end_time):
        """ scan vertexes of a space
        Arguments:
            - space: name of the space to scan
            - return_cols: the tag's attribute columns to be returned
            - all_cols: whether to return all attribute columns.
            - limit: maximum number of data returned
            - start_time: start time of the vertex data to return
            - end_time: end time of the vertex data to return
        Returns:
            - iter: an iterator that can traverse all scanned vertex data
        """
        part_ids = self._meta_client.get_parts_alloc_from_cache()[space].keys()
        part_ids_iter = Iterator(part_ids)
        return ScanSpaceVertexResponseIter(self, space, part_ids_iter, return_cols, all_cols, limit, start_time, end_time)

    def scan_part_vertex(self, space, part, return_cols, all_cols, limit, start_time, end_time):
        """ scan vertexes of a partition
        Arguments:
            - space: name of the space to scan
            - return_cols: the tag's attribute columns to be returned
            - all_cols: whether to return all attribute columns
              When all_cols is True, return all attribute columns,
              and when all_cols is False, just return attribute columns which specified in return_cols
            - limit: maximum number of data returned
            - start_time: start time of the vertex data to return
            - end_time: end time of the vertex data to return
        Returns:
            - iter: an iterator that can traverse all scanned vertex data
        """
        space_id = self._meta_client.get_space_id_from_cache(space)
        columns = self.get_vertex_return_cols(space, return_cols)
        scan_vertex_request = ScanVertexRequest(space_id, part, None, columns, all_cols, limit, start_time, end_time)
        leader = self.get_leader(space, part)
        if leader is None:
            raise Exception('part %s not found in space %s' % (part, space))
        return self.do_scan_vertex(space, leader, scan_vertex_request)

    def do_scan_edge(self, space, leader, scan_edge_request):
        client = self.connect(leader)
        if client is None:
            logging.fatal('cannot connect to leader:', leader)
            return None

        return ScanEdgeResponseIter(self, space, leader, scan_edge_request, client)

    def do_scan_vertex(self, space, leader, scan_vertex_request):
        client = self.connect(leader)
        if client is None:
            logging.fatal('cannot connect to leader:', leader)
            return None

        return ScanVertexResponseIter(self, space, leader, scan_vertex_request, client)

    def get_edge_return_cols(self, space, return_cols):
        columns = {}
        for edge_name, prop_names in return_cols.items():
            edge_item = self._meta_client.get_edge_item_from_cache(space, edge_name)
            if edge_item is None:
                raise Exception('edge %s not found in space %s' % (edge_name, space))
            edge_type = edge_item.edge_type
            entry_id = EntryId(edge_type=edge_type)
            prop_defs = []
            for prop_name in prop_names:
                prop_def = PropDef(PropOwner.EDGE, entry_id, prop_name)
                prop_defs.append(prop_def)
            columns[edge_type] = prop_defs
        return columns

    def get_vertex_return_cols(self, space, return_cols):
        columns = {}
        for tag_name, prop_names in return_cols.items():
            tag_item = self._meta_client.get_tag_item_from_cache(space, tag_name)
            if tag_item is None:
                raise Exception('tag %s not found in space %s' % (tag_name, space))
            tag_id = tag_item.tag_id
            entry_id = EntryId(tag_id=tag_id)
            prop_defs = []
            for prop_name in prop_names:
                prop_def = PropDef(PropOwner.SOURCE, entry_id, prop_name)
                prop_defs.append(prop_def)
            columns[tag_id] = prop_defs
        return columns

    def get_leader(self, space_name, part):
        return self._meta_client.get_space_part_leader_from_cache(space_name, part)

    def update_leader(self, space_name, part_id, leader):
        self._meta_client.update_space_part_leader(space_name, part_id, leader)

    def handle_result_codes(self, failed_codes, space):
        for result_code in failed_codes:
            if result_code.code == ErrorCode.E_LEADER_CHANGED:
                logging.info('ErrorCode.E_LEADER_CHANGED, leader changed to :', result_code.leader)
                host_addr = result_code.leader
                if host_addr is not None and host_addr.ip != 0 and host_addr.port != 0:
                    host = socket.inet_ntoa(struct.pack('I',socket.htonl(host_addr.ip & 0xffffffff)))
                    port = host_addr.port
                    new_leader = (host, port)
                    self.update_leader(space, result_code.part_id, new_leader)
                    if new_leader in self._clients.keys():
                        new_client = self._clients[new_leader]
                    else:
                        new_client = None
                    return new_leader, new_client

        return None, None

    def is_successfully(self, response):
        return len(response.result.failed_codes) == 0
