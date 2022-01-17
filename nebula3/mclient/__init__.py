#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import socket

from _thread import RLock
from nebula3.Exception import (
    InValidHostname,
    PartNotFoundException,
    SpaceNotFoundException,
    TagNotFoundException,
    EdgeNotFoundException,
)
from nebula3.common.ttypes import HostAddr, ErrorCode
from nebula3.meta.ttypes import (
    HostStatus,
    ListTagsReq,
    ListEdgesReq,
    ListSpacesReq,
    GetPartsAllocReq,
    ListHostsReq,
    HostRole,
)
from nebula3.meta import ttypes, MetaService

from nebula3.fbthrift.transport import TSocket, TTransport
from nebula3.fbthrift.protocol import TBinaryProtocol
from nebula3.logger import logger


class MetaClient(object):
    def __init__(self, addresses, timeout):
        if len(addresses) == 0:
            raise RuntimeError('Input empty addresses')
        self._timeout = timeout
        self._connection = None
        self._retry_count = 3
        self._addresses = addresses
        for address in addresses:
            try:
                socket.gethostbyname(address[0])
            except Exception:
                raise InValidHostname(str(address[0]))
        self._leader = self._addresses[0]
        self._lock = RLock()

    def open(self):
        """open the connection to connect meta service

        :eturn: void
        """
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
        """get all version tags

        :param space_id: the specified space id
        :eturn: list<TagItem>
        """
        with self._lock:
            if self._connection is None:
                raise RuntimeError('The connection is no open')
            req = ListTagsReq()
            req.space_id = space_id
            count = 0
            while count < self._retry_count:
                resp = self._connection.listTags(req)
                if resp.code != ErrorCode.SUCCEEDED:
                    if resp.code == ErrorCode.E_LEADER_CHANGED:
                        self.update_leader(resp.leader)
                        count = count + 1
                        continue
                    raise RuntimeError(
                        "List tags from space id:{} failed, error code: {}".format(
                            space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                        )
                    )
                return resp.tags
            raise RuntimeError(
                "List tags from space id:{} failed, error code: {}".format(
                    space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                )
            )

    def list_edges(self, space_id):
        """get all version edge

        :param space_id: the specified space id
        :return: list<EdgeItem>
        """
        with self._lock:
            if self._connection is None:
                raise RuntimeError('The connection is no open')
            req = ListEdgesReq()
            req.space_id = space_id
            count = 0
            while count < self._retry_count:
                resp = self._connection.listEdges(req)
                if resp.code != ErrorCode.SUCCEEDED:
                    if resp.code == ErrorCode.E_LEADER_CHANGED:
                        self.update_leader(resp.leader)
                        count = count + 1
                        continue
                    raise RuntimeError(
                        "List edges from space id:{} failed, error code: {}".format(
                            space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                        )
                    )
                return resp.edges
            raise RuntimeError(
                "List edges from space id:{} failed, error code: {}".format(
                    space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                )
            )

    def list_spaces(self):
        """get all spaces info

        :eturn: list<IdName>
        """
        with self._lock:
            if self._connection is None:
                raise RuntimeError('The connection is no open')
            req = ListSpacesReq()
            count = 0
            while count < self._retry_count:
                resp = self._connection.listSpaces(req)
                if resp.code != ErrorCode.SUCCEEDED:
                    if resp.code == ErrorCode.E_LEADER_CHANGED:
                        self.update_leader(resp.leader)
                        count = count + 1
                        continue
                    raise RuntimeError(
                        "List spaces failed, error code: {}".format(
                            ErrorCode._VALUES_TO_NAMES.get(resp.code)
                        )
                    )
                return resp.spaces
            raise RuntimeError(
                "List spaces failed, error code: {}".format(
                    ErrorCode._VALUES_TO_NAMES.get(resp.code)
                )
            )

    def list_hosts(self):
        """get all online hosts info

        :eturn: list<HostItem>
        """
        with self._lock:
            if self._connection is None:
                raise RuntimeError('The connection is no open')
            req = ListHostsReq()
            req.role = HostRole.STORAGE
            count = 0
            while count < self._retry_count:
                resp = self._connection.listHosts(req)
                if resp.code != ErrorCode.SUCCEEDED:
                    if resp.code == ErrorCode.E_LEADER_CHANGED:
                        self.update_leader(resp.leader)
                        count = count + 1
                        continue
                    raise RuntimeError(
                        "List spaces failed, error code: {}".format(
                            ErrorCode._VALUES_TO_NAMES.get(resp.code)
                        )
                    )
                valid_hosts = []
                for host in resp.hosts:
                    if host.status == HostStatus.ONLINE:
                        valid_hosts.append(host)
                return valid_hosts
            raise RuntimeError(
                "List spaces failed, error code: {}".format(
                    ErrorCode._VALUES_TO_NAMES.get(resp.code)
                )
            )

    def get_parts_alloc(self, space_id):
        """get all parts info of the specified space id

        :param space_id:
        :eturn: map<PartitionID, list<HostAddr>>
        """
        with self._lock:
            if self._connection is None:
                raise RuntimeError('The connection is no open')
            req = GetPartsAllocReq()
            req.space_id = space_id
            count = 0
            while count < self._retry_count:
                resp = self._connection.getPartsAlloc(req)
                if resp.code != ErrorCode.SUCCEEDED:
                    if resp.code == ErrorCode.E_LEADER_CHANGED:
                        self.update_leader(resp.leader)
                        count = count + 1
                        continue
                    raise RuntimeError(
                        "List parts from space id:{} failed, error code: {}".format(
                            space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                        )
                    )
                return resp.parts
            raise RuntimeError(
                "List parts from space id:{} failed, error code: {}".format(
                    space_id, ErrorCode._VALUES_TO_NAMES.get(resp.code)
                )
            )

    def close(self):
        """close the connection

        :eturn: void
        """
        try:
            if self._connection is not None:
                self._connection._iprot.trans.close()
        except Exception:
            raise

    def update_leader(self, leader):
        """update the leader meta info when happen leader change

        :param leader: the address of meta leader
        :eturn:
        """
        try:
            self._leader = (leader.host, leader.port)
            self.open()
        except Exception as e:
            logger.error(e)

    def __del__(self):
        self.close()


class MetaCache(object):
    class SpaceCache:
        def __init__(self):
            self.space_id = 0
            self.space_name = ''
            self.tag_items = {}
            self.edge_items = {}
            self.parts_alloc = {}

        def __repr__(self):
            return 'space_id: {}, space_name: {}, tag_items: {}, edge_items: {}, parts_alloc: {}'.format(
                self.space_id,
                self.space_name,
                self.tag_items,
                self.edge_items,
                self.parts_alloc,
            )

    def __init__(self, meta_addrs, timeout=2000, load_period=10, decode_type='utf-8'):
        self._decode_type = decode_type
        self._load_period = load_period
        self._lock = RLock()
        self._space_caches = {}
        self._space_id_names = {}
        self._storage_addrs = []
        self._storage_leader = {}
        self._close = False
        self._meta_client = MetaClient(meta_addrs, timeout)
        self._meta_client.open()

        # load meta data
        self._load_all()

    def close(self):
        """close the metaClient

        :eturn: void
        """
        if self._close:
            return
        self._close = True
        if self._meta_client is not None:
            self._meta_client.close()

    def __del__(self):
        self.close()

    def _load_all(self):
        """load all space info and schema info from meta services

        :eturn: void
        """
        try:
            spaces = self._meta_client.list_spaces()
            space_caches = {}
            space_id_names = {}
            for space in spaces:
                space_id = space.id.get_space_id()
                space_cache = MetaCache.SpaceCache()
                space_cache.space_id = space_id
                space_cache.space_name = space.name.decode('utf-8')
                space_id_names[space_id] = space_cache.space_name
                tags = self._meta_client.list_tags(space_id)
                edges = self._meta_client.list_edges(space_id)
                parts_alloc = self._meta_client.get_parts_alloc(space_id)
                for tag in tags:
                    tag_name = tag.tag_name.decode(self._decode_type)
                    if tag_name not in space_cache.tag_items.keys():
                        space_cache.tag_items[tag_name] = tag
                    else:
                        if space_cache.tag_items[tag_name].version < tag.version:
                            space_cache.tag_items[tag_name] = tag
                for edge in edges:
                    edge_name = edge.edge_name.decode(self._decode_type)
                    if edge_name not in space_cache.edge_items.keys():
                        space_cache.edge_items[edge_name] = edge
                    else:
                        if space_cache.edge_items[edge_name].version < edge.version:
                            space_cache.edge_items[edge_name] = edge
                    space_cache.edge_items[
                        edge.edge_name.decode(self._decode_type)
                    ] = edge
                space_cache.parts_alloc = parts_alloc
                space_caches[space.name.decode(self._decode_type)] = space_cache

            hosts = self._meta_client.list_hosts()
            storage_addrs = []
            for host_item in hosts:
                storage_addrs.append(host_item.hostAddr)

            with self._lock:
                self._storage_addrs = storage_addrs
                self._space_caches = space_caches
                self._space_id_names = space_id_names
                for space_name in self._space_caches.keys():
                    if space_name in self._storage_leader.keys():
                        continue
                    parts_alloc = self._space_caches[space_name].parts_alloc
                    self._storage_leader[space_name] = {}
                    for part_id in parts_alloc:
                        self._storage_leader[space_name][part_id] = parts_alloc[
                            part_id
                        ][0]
        except Exception as x:
            logger.error('Update meta data failed: {}'.format(x))
            import traceback

            logger.error(traceback.format_exc())

    def get_all_storage_addrs(self):
        """get all storage address

        :return: list[HostAddr]
        """
        return self._storage_addrs

    def get_tag_id(self, space_name, tag_name):
        """get tag id

        :param space_name:
        :param tag_name:
        :return: tag_id
        """
        with self._lock:
            tag_item = self._get_tag_item(space_name, tag_name)
            return tag_item.tag_id

    def get_edge_type(self, space_name, edge_name):
        """get edge type

        :param space_name:
        :param edge_name:
        :return: edge_type
        """
        with self._lock:
            edge_item = self._get_edge_item(space_name, edge_name)
            return edge_item.edge_type

    def get_space_id(self, space_name):
        """get space id

        :param space_name:
        :return: space_id
        """
        with self._lock:
            if space_name not in self._space_caches.keys():
                self._load_all()
                if space_name not in self._space_caches.keys():
                    raise SpaceNotFoundException(space_name)
            return self._space_caches[space_name].space_id

    def get_tag_schema(self, space_name, tag_name):
        """get tag schema

        :param space_name:
        :param tag_name:
        :return: schema
        """
        tag_item = self._get_tag_item(space_name, tag_name)
        return tag_item.schema

    def get_edge_schema(self, space_name, edge_name):
        """get edge schema

        :param space_name:
        :param edge_name:
        :return: schema
        """
        edge_item = self._get_edge_item(space_name, edge_name)
        return edge_item.schema

    def get_part_leader(self, space_name, part_id):
        """

        :param space_name:
        :param part_id:
        :return: storage ip port: HostAddr
        """
        part_leaders = self.get_part_leaders(space_name)
        if part_id not in part_leaders.keys():
            raise PartNotFoundException(part_id)
        return part_leaders[part_id]

    def get_part_leaders(self, space_name):
        """get all part leader info of the space

        :param space_name: space name
        :eturn: map<PartitionID, HostAddr>
        """
        with self._lock:
            if space_name not in self._storage_leader.keys():
                self._load_all()
                if space_name not in self._storage_leader.keys():
                    raise SpaceNotFoundException(space_name)
            return self._storage_leader[space_name]

    def get_part_alloc(self, space_name):
        """get all part info of the space

        :param space_name: space name
        :eturn: map<PartitionID, list<HostAddr>>
        """
        with self._lock:
            if space_name not in self._space_caches.keys():
                self._load_all()
                if space_name not in self._space_caches.keys():
                    raise SpaceNotFoundException(space_name)
            return self._space_caches[space_name].parts_alloc

    def _get_tag_item(self, space_name, tag_name):
        with self._lock:
            if space_name not in self._space_caches.keys():
                self._load_all()
                if space_name not in self._space_caches.keys():
                    raise SpaceNotFoundException(space_name)
            space_info = self._space_caches[space_name]
            if tag_name not in space_info.tag_items.keys():
                self._load_all()
                if tag_name not in space_info.tag_items.keys():
                    raise TagNotFoundException(tag_name)
            return space_info.tag_items[tag_name]

    def _get_edge_item(self, space_name, edge_name):
        with self._lock:
            if space_name not in self._space_caches.keys():
                self._load_all()
                if space_name not in self._space_caches.keys():
                    raise SpaceNotFoundException(space_name)
            space_info = self._space_caches[space_name]
            if edge_name not in space_info.edge_items.keys():
                self._load_all()
                if edge_name not in space_info.edge_items.keys():
                    raise EdgeNotFoundException(edge_name)
            return space_info.edge_items[edge_name]

    def update_storage_leader(self, space_id, part_id, address: HostAddr):
        """if the storage leader change, storage client need to call this function

        :param space_id:
        :param part_id:
        :param address: HostAddr, if the address is None, it means the leader can't connect,
        choose the peer as leader
        :return: coid
        """
        with self._lock:
            if space_id not in self._space_id_names.keys():
                logger.error("Space name:{} is not found".format(space_id))
                return
            space_name = self._space_id_names.get(space_id)
            if part_id not in self._storage_leader[space_name].keys():
                logger.error("part_id:{} is not found".format(space_name))
                return
            if address is not None:
                self._storage_leader[space_name][part_id] = address
                return
            part_addresses = self._space_caches[space_name].parts_alloc.get(part_id)
            for part_addr in part_addresses:
                if part_addr == address:
                    continue
                self._storage_leader[space_name][part_id] = part_addr
