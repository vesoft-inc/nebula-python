#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import logging
import socket
import threading

from _thread import RLock
from nebula2.Exception import InValidHostname
from nebula2.common.ttypes import HostAddr
from nebula2.meta.ttypes import (
    ListTagsReq,
    ListEdgesReq,
    ListSpacesReq,
    GetPartsAllocReq,
    ErrorCode,
    ListHostsReq, HostRole)

from nebula2.meta import (
    ttypes,
    MetaService
)

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol


class MetaClient(object):
    def __init__(self, addresses, timeout):
        if len(addresses) == 0:
            raise RuntimeError("")
        self._addresses = []
        self._timeout = timeout

        for address in addresses:
            try:
                ip = socket.gethostbyname(address[0])
            except Exception:
                raise InValidHostname(str(address[0]))
            self._addresses.append((ip, address[1]))
        self._leader = self._addresses[0]
        self._connection = None

    def open(self):
        try:
            self.close()
            s = TSocket.TSocket(self._leader[0], self._leader[1])
            if self._timeout > 0:
                s.setTimeout(self._timeout)
            transport = TTransport.TBufferedTransport(s)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self._connection = MetaService.Client(protocol)
        except Exception:
            raise

    def list_tags(self, space_id):
        if self._connection is None:
            raise RuntimeError('The connection is no open')
        req = ListTagsReq()
        req.space_id = space_id
        resp = self._connection.listTags(req)
        if resp.code != ErrorCode.SUCCEEDED:
            self.update_leader()
            raise RuntimeError("List tags from space id:{} failed, error code: {}"
                               .format(space_id, ErrorCode._VALUES_TO_NAMES(resp.code)))
        return resp.tags

    def list_edges(self, space_id):
        if self._connection is None:
            raise RuntimeError('The connection is no open')
        req = ListEdgesReq()
        req.space_id = space_id
        resp = self._connection.listEdges(req)
        if resp.code != ErrorCode.SUCCEEDED:
            self.update_leader()
            raise RuntimeError("List edges from space id:{} failed, error code: {}"
                               .format(space_id, ErrorCode._VALUES_TO_NAMES(resp.code)))
        return resp.edges

    def list_spaces(self):
        if self._connection is None:
            raise RuntimeError('The connection is no open')
        req = ListSpacesReq()
        resp = self._connection.listSpaces(req)
        if resp.code != ErrorCode.SUCCEEDED:
            self.update_leader()
            raise RuntimeError("List spaces failed, error code: {}"
                               .format(ErrorCode._VALUES_TO_NAMES(resp.code)))
        return resp.spaces

    def list_hosts(self):
        if self._connection is None:
            raise RuntimeError('The connection is no open')
        req = ListHostsReq()
        # req.role = HostRole.STORAGE
        resp = self._connection.listHosts(req)
        if resp.code != ErrorCode.SUCCEEDED:
            self.update_leader()
            raise RuntimeError("List spaces failed, error code: {}"
                               .format(ErrorCode._VALUES_TO_NAMES(resp.code)))
        return resp.hosts

    def get_parts_alloc(self, space_id):
        if self._connection is None:
            raise RuntimeError('The connection is no open')
        req = GetPartsAllocReq()
        req.space_id = space_id
        resp = self._connection.getPartsAlloc(req)
        if resp.code != ErrorCode.SUCCEEDED:
            self.update_leader()
            raise RuntimeError("List parts from space id:{} failed, error code: {}"
                               .format(space_id, ErrorCode._VALUES_TO_NAMES(resp.code)))
        return resp.parts

    def close(self):
        try:
            if self._connection is not None:
                self._connection._iprot.trans.close()
        except Exception:
            raise

    def update_leader(self, resp):
        if resp.code == ErrorCode.E_LEADER_CHANGED:
            try:
                if resp.leader is not None:
                    self._leader = (resp.leader.host, resp.leader.port)
                    self.open()
            except Exception as e:
                logging.error(e)

    def __del__(self):
        self.close()


class MetaCache(object):
    class SpaceCache(object):
        space_id = 0
        space_name = ''
        tag_items = {}
        edge_items = {}

    def __init__(self, meta_addrs, timeout=2000, decode_type='utf-8'):
        self._decode_type = decode_type
        self._lock = RLock()
        self._meta_client = MetaClient(meta_addrs, timeout)
        self._meta_client.open()
        self._space_caches = {}
        self._storage_addrs = []
        self._storage_leader = {}

        # load meta data
        self._load_all()

    def _load_all(self):
        try:
            spaces = self._meta_client.list_spaces()
            space_caches = {}
            for space in spaces:
                space_id = space.id.get_space_id()
                space_cache = MetaCache.SpaceCache()
                space_cache.space_id = space_id
                space_cache.space_name = space.name.decode('utf-8')
                tags = self._meta_client.list_tags(space_id)
                edges = self._meta_client.list_edges(space_id)
                for tag in tags:
                    space_cache.tag_items[tag.tag_name.decode(self._decode_type)] = tag
                for edge in edges:
                    space_cache.edge_items[edge.edge_name.decode(self._decode_type)] = edge
                space_caches[space.name.decode(self._decode_type)] = space_cache

            hosts = self._meta_client.list_hosts()
            storage_addrs = []
            storage_part_leaders = {}
            for host_item in hosts:
                storage_addrs.append(host_item.hostAddr)
                leader_parts = host_item.leader_parts
                for space_name in leader_parts.keys():
                    decode_space_name = space_name.decode(self._decode_type)
                    if decode_space_name not in storage_part_leaders.keys():
                        storage_part_leaders[decode_space_name] = {}
                    for part in leader_parts[space_name]:
                        storage_part_leaders[decode_space_name][part] = host_item.hostAddr

            with self._lock:
                self._storage_addrs = storage_addrs
                self._space_caches = space_caches
                self._storage_leader = storage_part_leaders
        except Exception as x:
            logging.error('Update meta data failed: {}'.format(x))
            import traceback
            logging.error(traceback.format_exc())

    def get_all_storage_addrs(self):
        """

        :return: list[HostAddr]
        """
        return self._storage_addrs

    def get_tag_id(self, space_name, tag_name):
        '''
        get_tag_id
        :param space_name:
        :param tag_name:
        :return:
        '''
        with self._lock:
            tag_items = self._get_tag_item(space_name, tag_name)
            return tag_items.tag_id

    def get_edge_type(self, space_name, edge_name):
        '''
        get_edge_type
        :param space_name:
        :param edge_name:
        :return:
        '''
        with self._lock:
            if space_name not in self._space_caches.keys():
                raise RuntimeError("Space name:{} is not found".format(space_name))
            space_cache = self._space_caches[space_name]
            if edge_name not in space_cache.edge_items.keys():
                raise RuntimeError("Edge name:{} is not found".format(edge_name))
            edge_item = self._get_edge_item(space_name, edge_name)
            return edge_item.edge_type

    def get_space_id(self, space_name):
        '''
        get_space_id
        :param space_name:
        :return:
        '''
        with self._lock:
            if space_name not in self._space_caches.keys():
                raise RuntimeError("{} is not found".format(space_name))
            return self._space_caches[space_name].space_id

    def get_tag_schema(self, space_name, tag_name):
        '''
        get_tag_schema
        :param space_name:
        :param tag_name:
        :return:
        '''
        with self._lock:
            tag_item = self._get_tag_item(space_name, tag_name)
            return tag_item.schema

    def get_edge_schema(self, space_name, edge_name):
        '''
        get_edge_schema
        :param space_name:
        :param edge_name:
        :return:
        '''
        with self._lock:
            edge_item = self._get_edge_item(space_name, edge_name)
            return edge_item.schema

    def get_part_leader(self, space_name, part_id):
        part_leaders = self.get_part_leaders(space_name)
        if part_id not in part_leaders.keys():
            raise RuntimeError("Part id:{} is not found".format(part_id))
        return part_leaders[part_id]

    def get_part_leaders(self, space_name):
        with self._lock:
            if space_name not in self._storage_leader.keys():
                raise RuntimeError("Space name:{} is not found".format(space_name))
            return self._storage_leader[space_name]

    def _get_tag_item(self, space_name, tag_name):
        if space_name not in self._space_caches.keys():
            raise RuntimeError("Space name:{} is not found".format(space_name))
        space_cache = self._space_caches[space_name]
        if tag_name not in space_cache.tag_items.keys():
            raise RuntimeError("Tag name:{} is not found".format(tag_name))
        return space_cache.tag_items[tag_name]

    def _get_edge_item(self, space_name, edge_name):
        if space_name not in self._space_caches.keys():
            raise RuntimeError("Space name:{} is not found".format(space_name))
        space_cache = self._space_caches[space_name]
        if edge_name not in space_cache.edge_items.keys():
            raise RuntimeError("Edge name:{} is not found".format(edge_name))
        return space_cache.edge_items[edge_name]

    def update_storage_leader(self, space_name, part_id, address):
        '''
        if the storage leader change, storage client need to call this function
        :param space_name:
        :param part_id:
        :param address: HostAddr
        :return:
        '''
        with self._lock:
            if space_name not in self._storage_leader.keys():
                logging.error("Space name:{} is not found".format(space_name))
                return
            if part_id not in self._storage_leader[space_name].keys():
                logging.error("part_id:{} is not found".format(space_name))
                return
            if isinstance(address, HostAddr):
                self._storage_leader[space_name][part_id] = address


