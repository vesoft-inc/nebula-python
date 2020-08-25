# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from meta.MetaService import Client
from storage.ttypes import ScanVertexResponse

from nebula.ngData.data import Row
from nebula.ngData.data import RowReader
from nebula.ngData.data import Result

class ScanVertexProcessor:
    def __init__(self, meta_client):
        self._meta_client = meta_client

    def process(self, space_name, scan_vertex_response):
        if scan_vertex_response is None:
            print('process: scan_vertex_response is None')
            return None
        row_readers = {}
        rows = {}
        tag_id_name_map = {}
        if scan_vertex_response.vertex_schema is not None:
            for tag_id, schema in scan_vertex_response.vertex_schema.items():
                tag_name = self._meta_client.get_tag_name_from_cache(space_name, tag_id)
                tag_item = self._meta_client.get_tag_item_from_cache(space_name, tag_name)
                schema_version = tag_item.version
                row_readers[tag_id] = RowReader(schema, schema_version)
                rows[tag_name] = []
                tag_id_name_map[tag_id] = tag_name
        else:
            print('scan_vertex_response.vertex_schema is None')

        if scan_vertex_response.vertex_data is not None:
            for scan_tag in scan_vertex_response.vertex_data:
                tag_id = scan_tag.tagId
                if tag_id not in row_readers.keys():
                    continue

                row_reader = row_readers[tag_id]
                default_properties = row_reader.vertex_key(scan_tag.vertexId, scan_tag.tagId)
                properties = row_reader.decode_value(scan_tag.value)
                tag_name = tag_id_name_map[tag_id]
                rows[tag_name].append(Row(default_properties, properties))
        else:
            print('scan_vertex_response.vertex_data is None')

        return Result(rows)
