#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import concurrent

from nebula3.sclient import PartManager, do_scan_job, PartInfo

from nebula3.sclient.BaseResult import BaseResult, VertexData, EdgeData
from nebula3.logger import logger


class VertexResult(BaseResult):
    def __init__(self, data_sets, decode_type='utf-8'):
        super().__init__(data_sets=data_sets, decode_type=decode_type, is_vertex=True)

    def as_nodes(self):
        """convert the vertexes to relationships

        :return: list<Node>
        """
        nodes = []
        for data_set in self._data_sets:
            for row in data_set.rows:
                vertex_data = VertexData(row, data_set.column_names, self._decode_type)
                nodes.append(vertex_data.as_node())
        return nodes


class EdgeResult(BaseResult):
    def __init__(self, data_sets: list, decode_type='utf-8'):
        super().__init__(data_sets=data_sets, decode_type=decode_type, is_vertex=False)

    def as_relationships(self):
        """convert the edges to relationships

        :return: list<Relationship>
        """
        relationships = []
        for data_set in self._data_sets:
            for row in data_set.rows:
                edge_data = EdgeData(row, data_set.column_names, self._decode_type)
                relationships.append(edge_data.as_relationship())
        return relationships


class ScanResult(object):
    """the scan result"""

    def __init__(
        self,
        graph_storage_client,
        req,
        part_addrs,
        partial_success=False,
        is_vertex=True,
        decode_type='utf-8',
    ):
        self._is_vertex = is_vertex
        self._decode_type = decode_type
        self._data_sets = []
        self._graph_storage_client = graph_storage_client
        self._partial_success = partial_success
        self._req = req
        part_infos = {}
        for part_id in part_addrs.keys():
            part_infos[part_id] = PartInfo(part_id, part_addrs[part_id])
        self._parts_manager = PartManager(part_infos)

    def has_next(self):
        """whether if has data, the first call is always return True

        :return: True of False
        """
        return self._parts_manager.has_next()

    def next(self):
        """get scan data result

        :return: VertexResult or EdgeResult
        """
        conns = self._graph_storage_client.get_conns()
        num = len(conns)
        if num == 0:
            raise RuntimeError('There is no storage connection')
        logger.debug('Graph storage client num: {}'.format(num))
        exceptions = []
        result = []
        with concurrent.futures.ThreadPoolExecutor(num) as executor:
            do_scan = []
            for i, conn in enumerate(conns):
                future = executor.submit(
                    do_scan_job,
                    conns[i],
                    self._parts_manager,
                    self._req,
                    self._is_vertex,
                    self._partial_success,
                )
                do_scan.append(future)

            for future in concurrent.futures.as_completed(do_scan):
                if future.exception() is not None:
                    logger.error(future.exception())
                    exceptions.append(future.exception())
                else:
                    ret, data_sets = future.result()
                    if ret is not None:
                        logger.error('Scan failed: {}'.format(ret))
                        exceptions.append(RuntimeError('Scan failed: {}'.format(ret)))
                        continue
                    if len(data_sets) != 0:
                        result.extend(data_sets)
            self._parts_manager.reset_jobs()
        if len(exceptions) == 0:
            if len(result) == 0:
                logger.warning('Get empty result')
                return None
            else:
                if self._is_vertex:
                    return VertexResult(result, self._decode_type)
                else:
                    return EdgeResult(result, self._decode_type)
        else:
            raise exceptions[0]
