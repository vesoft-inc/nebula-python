#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
The client to scan vertex and edge from storage,
the return data is from thr graph database
"""
import logging
import sys

from nebula2.sclient.ScanResult import ScanResult
from nebula2.sclient.net import GraphStorageConnection
from nebula2.storage.ttypes import (
    ScanEdgeRequest,
    ScanVertexRequest,
    VertexProp,
    EdgeProp
)


class GraphStorageClient(object):
    DEFAULT_START_TIME = 0
    DEFAULT_END_TIME = sys.maxsize
    DEFAULT_LIMIT = 1000

    def __init__(self, meta_cache, time_out=60000):
        self._meta_cache = meta_cache
        self._time_out = time_out
        self._connections = []
        self._create_connection()

    def get_conns(self):
        return self._connections

    def close(self):
        try:
            for conn in self._connections:
                conn.close()
        except Exception as e:
            logging.error('Close connection failed: {}'.format(e))
            raise

    def _create_connection(self):
        storage_addrs = self._meta_cache.get_all_storage_addrs()
        try:
            for addr in storage_addrs:
                conn = GraphStorageConnection(addr, self._time_out, self._meta_cache)
                conn.open()
                self._connections.append(conn)
        except Exception as e:
            logging.error('Create storage connection failed: {}'.format(e))
            raise

    def get_space_addrs(self, space_name):
        return self.meta_cache.get_space_addrs(space_name)

    def scan_vertex(self,
                    space_name,
                    tag_name,
                    prop_names=[],
                    no_columns=False,
                    limit=DEFAULT_LIMIT,
                    start_time=DEFAULT_START_TIME,
                    end_time=DEFAULT_END_TIME,
                    where=None,
                    only_latest_version=False,
                    enable_read_from_follower=True,
                    partial_success=False):
        """
        scan_vertex
        :param prop_names:
        :param tag_name:
        :param space_name: the space name
        :param no_columns: only return the vertex id
        :param limit: the max vertex number from one storaged
        :param start_time: the min version of vertex
        :param end_time: the max version of vertex
        :param where: now is unsupported
        :param only_latest_version: when storage enable multi versions and only_latest_version is true,
        only return latest version.
        when storage disable multi versions, just use the default value.
        :param enable_read_from_follower: if set to false, forbid follower read
        :param partial_success: if set true, when partial success, it will continue until finish
        :return: ScanResult
        """
        part_leaders = self._meta_cache.get_part_leaders(space_name)
        return self._scan_vertex(space_name,
                                 part_leaders,
                                 tag_name,
                                 prop_names,
                                 no_columns,
                                 limit,
                                 start_time,
                                 end_time,
                                 where,
                                 only_latest_version,
                                 enable_read_from_follower,
                                 partial_success)

    def scan_vertex_with_part(self,
                              space_name,
                              part,
                              tag_name,
                              prop_names=[],
                              no_columns=False,
                              limit=DEFAULT_LIMIT,
                              start_time=DEFAULT_START_TIME,
                              end_time=DEFAULT_END_TIME,
                              where=None,
                              only_latest_version=False,
                              enable_read_from_follower=True,
                              partial_success=False):
        """
        scan_vertex_with_part
        :param prop_names:
        :param tag_name:
        :type part: part id
        :param space_name: the space name
        :param no_columns: only return the vertex id
        :param limit: the max vertex number from one storaged
        :param start_time: the min version of vertex
        :param end_time: the max version of vertex
        :param where: now is unsupported
        :param only_latest_version: when storage enable multi versions and only_latest_version is true,
        only return latest version.
        when storage disable multi versions, just use the default value.
        :param enable_read_from_follower: if set to false, forbid follower read
        :param partial_success: if set true, when partial success, it will continue until finish
        :return: ScanResult
        """

        part_leaders = {part: self._meta_cache.get_part_leader(space_name, part)}
        return self._scan_vertex(space_name,
                                 part_leaders,
                                 tag_name,
                                 prop_names,
                                 no_columns,
                                 limit,
                                 start_time,
                                 end_time,
                                 where,
                                 only_latest_version,
                                 enable_read_from_follower,
                                 partial_success)

    def _scan_vertex(self,
                     space_name,
                     part_leaders,
                     tag_name,
                     prop_names,
                     no_columns,
                     limit,
                     start_time,
                     end_time,
                     where,
                     only_latest_version,
                     enable_read_from_follower,
                     partial_success=False):
        space_id = self._meta_cache.get_space_id(space_name)
        tag_id = self._meta_cache.get_tag_id(space_name, tag_name)
        vertex_prop: VertexProp = VertexProp()
        vertex_prop.tag = tag_id
        vertex_prop.props = []
        for prop_name in prop_names:
            vertex_prop.props.append(prop_name.encode('utf-8'))

        # When storage return column names, here need delete
        if len(prop_names) == 0:
            schema = self._meta_cache.get_tag_schema(space_name, tag_name)
            for col in schema.columns:
                vertex_prop.props.append(col.name)

        req = ScanVertexRequest()
        req.space_id = space_id
        req.part_id = 0
        req.return_columns = vertex_prop
        req.no_columns = no_columns
        req.limit = limit
        req.start_time = start_time
        req.end_time = end_time
        req.filter = where
        req.only_latest_version = only_latest_version
        req.enable_read_from_follower = enable_read_from_follower
        return ScanResult(self,
                          req=req,
                          part_addrs=part_leaders,
                          schema_name=tag_name,
                          is_vertex=True,
                          partial_success=partial_success)

    def scan_edge(self,
                  space_name,
                  edge_name,
                  prop_names=[],
                  no_columns=False,
                  limit=DEFAULT_LIMIT,
                  start_time=DEFAULT_START_TIME,
                  end_time=DEFAULT_END_TIME,
                  where=None,
                  only_latest_version=False,
                  enable_read_from_follower=True,
                  partial_success=False):
        """

        scan_edge
        :param prop_names:
        :param edge_name:
        :param space_name: the space name
        :param no_columns: only return the edge key
        :param limit: the max vertex number from one storaged
        :param start_time: the min version of vertex
        :param end_time: the max version of vertex
        :param where: now is unsupported
        :param only_latest_version: when storage enable multi versions and only_latest_version is true,
        only return latest version.
        when storage disable multi versions, just use the default value.
        :param enable_read_from_follower: if set to false, forbid follower read
        :param partial_success: if set true, when partial success, it will continue until finish
        :return: ScanResult
        """
        part_leaders = self._meta_cache.get_part_leaders(space_name)
        return self._scan_edge(space_name,
                               part_leaders,
                               edge_name,
                               prop_names,
                               no_columns,
                               limit,
                               start_time,
                               end_time,
                               where,
                               only_latest_version,
                               enable_read_from_follower,
                               partial_success)

    def scan_edge_with_part(self,
                            space_name,
                            part,
                            tag_name,
                            prop_names=[],
                            no_columns=False,
                            limit=DEFAULT_LIMIT,
                            start_time=DEFAULT_START_TIME,
                            end_time=DEFAULT_END_TIME,
                            where=None,
                            only_latest_version=False,
                            enable_read_from_follower=True,
                            partial_success=False):
        """
        :param space_name: the space name
        :param part: the partition num of the given space
        :type prop_names: object
        :param tag_name:
        :param no_columns: only return the edge key
        :param limit: the max vertex number from one storaged
        :param start_time: the min version of edge
        :param end_time: the max version of edge
        :param where: now is unsupported
        :param only_latest_version: when storage enable multi versions and only_latest_version is true,
        only return latest version.
        when storage disable multi versions, just use the default value.
        :param enable_read_from_follower: if set to false, forbid follower read
        :param partial_success: if set true, when partial success, it will continue until finish
        :return: ScanResult
        """
        part_leaders = {part: self._meta_cache.get_part_leader(space_name, part)}
        return self._scan_edge(space_name,
                               part_leaders,
                               tag_name,
                               prop_names,
                               no_columns,
                               limit,
                               start_time,
                               end_time,
                               where,
                               only_latest_version,
                               enable_read_from_follower,
                               partial_success)

    def _scan_edge(self,
                   space_name,
                   part_leaders,
                   edge_name,
                   prop_names,
                   no_columns,
                   limit,
                   start_time,
                   end_time,
                   where,
                   only_latest_version,
                   enable_read_from_follower,
                   partial_success):
        space_id = self._meta_cache.get_space_id(space_name)
        edge_type = self._meta_cache.get_edge_type(space_name, edge_name)
        edge_prop = EdgeProp()
        edge_prop.type = edge_type
        edge_prop.props = []

        for prop_name in prop_names:
            edge_prop.props.append(prop_name.encode('utf-8'))

        # When storage return column names, here need delete
        if len(prop_names) == 0:
            schema = self._meta_cache.get_edge_schema(space_name, edge_name)
            for col in schema.columns:
                edge_prop.props.append(col.name)

        req = ScanEdgeRequest()
        req.space_id = space_id
        req.part_id = 0
        req.return_columns = edge_prop
        req.no_columns = no_columns
        req.limit = limit
        req.start_time = start_time
        req.end_time = end_time
        req.filter = where
        req.only_latest_version = only_latest_version
        req.enable_read_from_follower = enable_read_from_follower
        return ScanResult(self,
                          req=req,
                          part_addrs=part_leaders,
                          schema_name=edge_name,
                          is_vertex=False,
                          partial_success=partial_success)
