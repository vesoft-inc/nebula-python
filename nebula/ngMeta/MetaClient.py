# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import random
import socket
import struct
import six
import threading
import logging
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from meta.MetaService import Client
from meta.ttypes import EdgeItem
from meta.ttypes import ErrorCode
from meta.ttypes import GetEdgeReq
from meta.ttypes import GetEdgeResp
from meta.ttypes import GetPartsAllocReq
from meta.ttypes import GetPartsAllocResp
from meta.ttypes import GetTagReq
from meta.ttypes import GetTagResp
from meta.ttypes import ListHostsReq
from meta.ttypes import ListHostsResp
from meta.ttypes import ListEdgesReq
from meta.ttypes import ListEdgesResp
from meta.ttypes import ListSpacesReq
from meta.ttypes import ListSpacesResp
from meta.ttypes import ListTagsReq
from meta.ttypes import ListTagsResp
from meta.ttypes import TagItem

if six.PY3:
    Timer = threading.Timer
else:
    Timer = threading._Timer

class RepeatTimer(Timer):
    def __init__(self, interval, function):
        Timer.__init__(self, interval, function)
        self.daemon = True # set the RepeatTimer thread as a daemon thread, so it can end when main thread ends

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class MetaClient:
    def __init__(self, addresses, timeout=1000,
                connection_retry=3):
        """Initializer
        Arguments:
            - addresses: meta server addresses
            - timeout: maximum connection timeout in millisecond
            - connection_retry: maximum number of connection retries
        Returns: empty
        """
        self._addresses = addresses
        self._timeout = timeout
        self._connection_retry = connection_retry
        self._space_name_map = {} # map<space_name, space_id>
        self._space_part_location = {} # map<space_name, map<part_id, list<address>>>
        self._space_part_leader = {} # map<space_name, map<part_id, leader'saddress>>
        self._space_tag_items = {} # map<space_name, map<tag_name, tag_item>>
        self._space_edge_items = {} # map<space_name, map<edge_name, edge_item>>
        self._tag_name_map = {} # map<space_name, map<tag_id, tag_item.tag_name>>
        self._edge_name_map = {} # map<space_name, map<edge_name, edge_item>>
        self._client = None

    def connect(self):
        """connect to meta servers
        Arguments: emtpy
        Returns:
            - error_code: the code indicates whether the connection is successful
        """
        retry = self._connection_retry
        while retry > 0:
            code = self.do_connect(self._addresses)
            if code == 0:
                return ErrorCode.SUCCEEDED
            retry -= 1
        return ErrorCode.E_FAIL_TO_CONNECT

    def do_connect(self, addresses):
        address = addresses[random.randint(0, len(addresses)-1)]
        host = address[0]
        port = address[1]
        try:
            transport = TSocket.TSocket(host, port)
            transport.setTimeout(self._timeout)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self._client = Client(protocol)
            self.update_schemas()
            RepeatTimer(2, self.update_schemas).start() # call updatSchemas() every 2 seconds
            return 0
        except Exception as x:
            logging.exception(x)
            return -1

    def update_schemas(self):
        for space_id_name in self.list_spaces():
            space_name = space_id_name.name # class IdName
            self._space_name_map[space_name] = space_id_name.id.get_space_id()
            self._space_part_location[space_name] = self.get_parts_alloc(space_name)
            self._space_part_leader[space_name] = {}
            # Loading tag schema's cache
            tags = {}
            tags_name = {}
            for tag_item in self.get_tags(space_name):
                tags[tag_item.tag_name] = tag_item
                tags_name[tag_item.tag_id] = tag_item.tag_name

            self._space_tag_items[space_name] = tags
            self._tag_name_map[space_name] = tags_name

            # Loading edge schema's cache
            edges = {}
            edges_name = {}
            for edge_item in self.get_edges(space_name):
                edges[edge_item.edge_name] = edge_item
                edges_name[edge_item.edge_type] = edge_item.edge_name
            self._space_edge_items[space_name] = edges
            self._edge_name_map[space_name] = edges_name

        # Update leader of partions
        self.set_space_part_leader()

    def get_tags_name(self, space_name):
        if space_name in self._space_tag_items.keys():
            return self._space_tag_items[space_name].keys()

        return None

    def get_edges_name(self, space_name):
        if space_name in self._space_edge_items.keys():
            return self._space_edge_items[space_name].keys()

        return None

    def get_space_id_from_cache(self, space_name):
        """get space id of the space
        Arguments:
            - space_name: name of the space
        Returns:
            - space_id: id of the space
        """
        if space_name not in self._space_name_map.keys():
            return -1
        else:
            return self._space_name_map[space_name]

    def get_space_part_leader_from_cache(self, space_name, part_id):
        """get leader of the partion
        Arguments:
            - space_name: name of the space
            - part_id: id of the partition
        Returns:
            - leader address of the partition
        """
        if space_name not in self._space_part_leader.keys():
            return None
        if part_id not in self._space_part_leader[space_name].keys():
            return None
        return self._space_part_leader[space_name][part_id]

    def update_space_part_leader(self, space_name, part_id, leader):
        self._space_part_leader[space_name][part_id] = leader

    def set_space_part_leader(self):
        list_hosts_req = ListHostsReq()
        list_hosts_resp = self._client.listHosts(list_hosts_req)
        if list_hosts_resp.code != ErrorCode.SUCCEEDED:
            logging.error('set_space_part_leader error, eror code: ', list_hosts_resp.code)
            return None

        for host_item in list_hosts_resp.hosts:
            host = socket.inet_ntoa(struct.pack('I',socket.htonl(host_item.hostAddr.ip & 0xffffffff)))
            port = host_item.hostAddr.port
            leader = (host, port)
            for space, part_ids in host_item.leader_parts.items():
                for part_id in part_ids:
                    self._space_part_leader[space][part_id] = leader

    def list_spaces(self):
        """list all of the spaces
        Arguments: empty
        Returns:
            - spaces: IdName of all spaces
            IdName's attributes:
                - id
                - name
        """
        list_spaces_req = ListSpacesReq()
        list_spaces_resp = self._client.listSpaces(list_spaces_req)
        if list_spaces_resp.code == ErrorCode.SUCCEEDED:
            return list_spaces_resp.spaces # IdName
        else:
            logging.error('list spaces error, error code: ', list_spaces_resp.code)
            return None

    def get_part_alloc_from_cache(self, space_name, part_id):
        """get addresses of the partition
        Arguments:
            - space_name: name of the space
            - part_id: id of the partition
        Returns:
            - addresses: addresses of the partition
        """
        if space_name in self._space_part_location.keys():
            parts_alloc = self._space_part_location[space_name]
            if part_id in parts_alloc.keys():
                return parts_alloc[part]

        return None

    def get_parts_alloc(self, space_name):
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        get_parts_alloc_req = GetPartsAllocReq(space_id)
        get_parts_alloc_resp = self._client.getPartsAlloc(get_parts_alloc_req)

        if get_parts_alloc_resp.code == ErrorCode.SUCCEEDED:
            address_map = {}
            for part_id, host_addrs in get_parts_alloc_resp.parts.items():
                addresses = []
                for host_addr in host_addrs:
                    host = socket.inet_ntoa(struct.pack('I',socket.htonl(host_addr.ip & 0xffffffff)))
                    port = host_addr.port
                    addresses.append((host, port))
                address_map[part_id] = addresses

            return address_map
        else:
            logging.error("get parts alloc error, error code: ", getParts_alloc_resp.code)
            return None

    def get_parts_alloc_from_cache(self):
        """ get addresses of partitions of spaces
        Arguments: empty
        Returns:
            - space_part_location: map<space_name, map<part_id, list<address>>>
        """
        return self._space_part_location

    def get_tag_item_from_cache(self, space_name, tag_name):
        """ get TagItem of the tag
        Arguments:
            - space_name: name of the space
            - tag_name: name of the tag
        Returns:
            - TagItem
            TagItem's attributes:
                - tag_id
                - tag_name
                - version
                - schema
        """
        if space_name in self._space_tag_items.keys() and tag_name in self._space_tag_items[space_name].keys():
            return self._space_tag_items[space_name][tag_name]

        return None

    def get_tag_name_from_cache(self, space_name, tag_id):
        """ get tag_name of the tag
        Arguments:
            - space_name: name of the space
            - tag_id: id of the tag
        Returns:
            - tag_name: name of the tag
        """
        if space_name in self._tag_name_map.keys():
            tag_names = self._tag_name_map[space_name]
            if tag_id in tag_names.keys():
                return tag_names[tag_id]

        return None

    def get_tags(self, space_name):
        """ get TagItems of the space
        Arguments:
            - space_name: name of the space
        Returns:
            - tags: TagItems
            TagItem's attributes:
                - tag_id
                - tag_name
                - version
                - schema
        """
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        list_tags_req = ListTagsReq(space_id)
        list_tags_resp = self._client.listTags(list_tags_req)

        if list_tags_resp.code == ErrorCode.SUCCEEDED:
            return list_tags_resp.tags
        else:
            logging.error('get tags error, error code: ', list_tags_resp.code)
            return None

    def get_tag(self, space_name, tag_name, version=0):
        """ get tag schema of the given version
        Arguments:
            - space_name: name of the space
            - tag_name: name of the tag
            - version: version of the tag schema
        Returns:
            - Schema: tag schema of the given version
            Schema's attributes:
                - columns
                - schema_prop
        """
        space_id = self.get_space_id_from_cache(space_name)
        get_tag_req = GetTagReq(space_id, tag_name, version)
        get_tag_resp = self._client.getTag(get_tag_req)

        if get_tag_resp.code == ErrorCode.SUCCEEDED:
            return get_tag_resp.schema
        else:
            return None

    def get_tag_schema(self, space_name, tag_name, version=0):
        """ get tag schema columns of the given version
        Arguments:
            - space_name: name of the space
            - tag_name: name of the tag
            - version: version of the tag schema
        Returns:
            - result: columns of the tag schema
        """
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        get_tag_req = GetTagReq(space_id, tag_name, version)
        get_tag_resp = self._client.getTag(get_tag_req)
        result = {}
        for column_def in get_tag_resp.schema.columns:
            result[column_def.name] = column_def.type.type
        return result

    def get_edge_item_from_cache(self, space_name, edge_name):
        """ get EdgeItem of the edge
        Arguments:
            - space_name: name of the space
            - edge_name: name of the edge
        Returns:
            - EdgeItem
            EdgeItem's attributes:
                - edge_type
                - edge_name
                - version
                - schema
        """
        if space_name not in self._space_edge_items.keys():
            edges = {}
            for edge_item in self.getEdges(space_name):
                edges[edge_item.edge_name] = edge_item
            self._space_edge_items[space_name] = edges

        edge_items = self._space_edge_items[space_name]
        if edge_name in edge_items.keys():
            return edge_items[edge_name]
        else:
            return None

    def get_edge_name_from_cache(self, space_name, edge_type):
        """ get edge name of the edge
        Arguments:
            - space_name: name of the space
            - edge_type: edge type of the edge
        Returns:
            - edge_name: name of the edge
        """
        if space_name in self._edge_name_map.keys():
            edge_names = self._edge_name_map[space_name]
            if edge_type in edge_names.keys():
                return edge_names[edge_type]

        return None

    def get_edges(self, space_name):
        """ get EdgeItems of the space
        Arguments:
            - space_name: name of the space
        Returns:
            - edges: EdgeItems
            EdgeItem's attributes:
                - edge_type
                - edge_name
                - version
                - schema
        """
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        list_edges_req = ListEdgesReq(space_id)
        list_edges_resp =self._client.listEdges(list_edges_req)
        if list_edges_resp.code == ErrorCode.SUCCEEDED:
            return list_edges_resp.edges
        else:
            logging.error('get tags error, error code: ', list_edges_resp.code)
            return None

    def get_edge(self, space_name, edge_name, version=0):
        """ get edge schema of the given version
        Arguments:
            - space_name: name of the space
            - edge_name: name of the edge
            - version: version of the edge schema
        Returns:
            - schema of the edge
            Schema's attributes:
                - columns
                - schema_prop
        """
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        get_edge_req = GetEdgeReq(space_id, edge_name, version)
        get_edge_resp = self._client.getEdge(get_edge_req)
        if get_edge_resp.code == ErrorCode.SUCCEEDED:
            return get_edge_resp.Schema
        else:
            logging.error('get edge error, error code: ', get_edge_resp.code)
            return None

    def get_edge_schema(self, space_name, edge_name, version=0):
        """ get edge schema columns of the given version
        Arguments:
            - space_name: name of the space
            - edge_name: name of the edge
            - version: version of the edge schema
        Returns:
            - result: columns of the edge schema
        """
        space_id = self.get_space_id_from_cache(space_name)
        if space_id == -1:
            return None
        get_edge_req = GetEdgeReq(space_id, edge_name, version)
        get_edge_resp = self._client.getEdge(get_edge_req)
        result = {}
        for column_def in get_edge_resp.schema.columns:
            result[column_def.name] = column_def.type.type
        return result
