from meta.MetaService import Client
from storage.ttypes import ScanEdgeResponse

from nebula.ngData.data import Row
from nebula.ngData.data import RowReader
from nebula.ngData.data import Result

class ScanEdgeProcessor:
    def __init__(self, metaClient):
        self.metaClient = metaClient

    def process(self, spaceName, scanEdgeResponse):
        rowReaders = {}
        rows = {}
        edgeTypeNameMap = {}

        if scanEdgeResponse.edge_schema is not None:
            for edgeType, schema in scanEdgeResponse.edge_schema.items():
                edgeName = self.metaClient.getEdgeNameFromCache(spaceName, edgeType)
                edgeItem = self.metaClient.getEdgeItemFromCache(spaceName, edgeName)
                schemaVersion = edgeItem.version
                rowReaders[edgeType] = RowReader(schema, schemaVersion)
                rows[edgeName] = [] ###
                edgeTypeNameMap[edgeType] = edgeName
        else:
            print('scanEdgeResponse.edge_schema is None')

        if scanEdgeResponse.edge_data is not None:
            for scanEdge in scanEdgeResponse.edge_data:
                edgeType = scanEdge.type
                if edgeType not in rowReaders.keys():
                    continue

                rowReader = rowReaders[edgeType]
                defaultProperties = rowReader.edgeKey(scanEdge.src, scanEdge.type, scanEdge.dst)
                properties = rowReader.decodeValue(scanEdge.value)
                edgeName = edgeTypeNameMap[edgeType]
                rows[edgeName].append(Row(defaultProperties, properties))
        else:
            print('scanEdgeResponse.edge_data is None')

        return Result(rows)
