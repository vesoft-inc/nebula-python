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
    def __init__(self, metaClient):
        self.metaClient = metaClient

    def process(self, spaceName, scanVertexResponse):
        if scanVertexResponse is None:
            print('process: scanVertexResponse is None')
            return None
        rowReaders = {}
        rows = {}
        tagIdNameMap = {}
        if scanVertexResponse.vertex_schema is not None:
            for tagId, schema in scanVertexResponse.vertex_schema.items():
                tagName = self.metaClient.getTagNameFromCache(spaceName, tagId)
                tagItem = self.metaClient.getTagItemFromCache(spaceName, tagName)
                schemaVersion = tagItem.version
                rowReaders[tagId] = RowReader(schema, schemaVersion)
                rows[tagName] = []
                tagIdNameMap[tagId] = tagName
        else:
            print('scanVertexResponse.vertex_schema is None')

        if scanVertexResponse.vertex_data is not None:
            for scanTag in scanVertexResponse.vertex_data:
                tagId = scanTag.tagId
                if tagId not in rowReaders.keys():
                    continue

                rowReader = rowReaders[tagId]
                defaultProperties = rowReader.vertexKey(scanTag.vertexId, scanTag.tagId)
                properties = rowReader.decodeValue(scanTag.value)
                tagName = tagIdNameMap[tagId]
                rows[tagName].append(Row(defaultProperties, properties))
        else:
            print('scanVertexResponse.vertex_data is None')

        return Result(rows)
