# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import logging

from meta.MetaService import Client
from storage.ttypes import ScanEdgeResponse

from nebula.ngData.data import Row
from nebula.ngData.data import RowReader
from nebula.ngData.data import Result

class ScanEdgeProcessor:
    def __init__(self, meta_client):
        """Initializer
        Arguments:
            - meta_client: an initialized MetaClient
        Returns: emtpy
        """
        self._meta_client = meta_client

    def process(self, space_name, scan_edge_response):
        """ process the ScanEdgeResponse
        Arguments:
            - space_name: name of the space
            - scan_edge_response: response of storage server
        Returns:
            - result: a dataset of edges and its property values
        """
        row_readers = {}
        rows = {}
        edge_type_name_map = {}

        if scan_edge_response.edge_schema is not None:
            for edge_type, schema in scan_edge_response.edge_schema.items():
                edge_name = self._meta_client.get_edge_name_from_cache(space_name, edge_type)
                edge_item = self._meta_client.get_edge_item_from_cache(space_name, edge_name)
                schema_version = edge_item.version
                row_readers[edge_type] = RowReader(schema, schema_version)
                rows[edge_name] = []
                edge_type_name_map[edge_type] = edge_name
        else:
            logging.info('scan_edge_response.edge_schema is None')

        if scan_edge_response.edge_data is not None:
            for scan_edge in scan_edge_response.edge_data:
                edge_type = scan_edge.type
                if edge_type not in row_readers.keys():
                    continue

                row_reader = row_readers[edge_type]
                edge_name = edge_type_name_map[edge_type]
                default_properties = row_reader.edge_key(scan_edge.src, edge_name, scan_edge.dst)
                properties = row_reader.decode_value(scan_edge.value)
                rows[edge_name].append(Row(default_properties, properties))
        else:
            logging.info('scan_edge_response.edge_data is None')

        return Result(rows)
