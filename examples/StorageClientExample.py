# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula StorageClient example.
"""

import sys, getopt
from meta.ttypes import ErrorCode

sys.path.insert(0, '../')

from nebula.ngMeta.MetaClient import MetaClient
from nebula.ngStorage.StorageClient import StorageClient
from nebula.ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor
from nebula.ngStorage.ngProcessor.ScanVertexProcessor import ScanVertexProcessor


def scanEdge(space, returnCols, allCols):
    scanEdgeResponseIter = storageClient.scanEdge(space, returnCols, allCols, 100, 0, sys.maxsize)
    scanEdgeResponse = scanEdgeResponseIter.next()
    if scanEdgeResponse is None:
        print('scanEdgeResponse is None')
        return
    processEdge(space, scanEdgeResponse)
    while scanEdgeResponseIter.hasNext():
        scanEdgeResponse = scanEdgeResponseIter.next()
        if scanEdgeResponse is None:
            print("Error occurs while scaning edge")
            break
        processEdge(space, scanEdgeResponse)

def scanVertex(space, returnCols, allCols):
    scanVertexResponseIter = storageClient.scanVertex(space, returnCols, allCols, 100, 0, sys.maxsize)
    scanVertexResponse = scanVertexResponseIter.next()
    if scanVertexResponse is None:
        print('scanVertexResponse is None')
        return
    processVertex(space, scanVertexResponse)
    while scanVertexResponseIter.hasNext():
        scanVertexResponse = scanVertexResponseIter.next()
        if scanVertexResponse is None:
            print("Error occurs while scaning vertex")
            break
        processVertex(space, scanVertexResponse)

def processEdge(space, scanEdgeResponse):
    result = scanEdgeProcessor.process(space, scanEdgeResponse)
    # Get the corresponding rows by edgeName
    for edgeName, edgeRows in result.rows.items():
        for row in edgeRows:

            srcId = row.defaultProperties[0].getValue()
            dstId = row.defaultProperties[2].getValue()
            props = {}
            for prop in row.properties:
                propName = prop.getName()
                propValue = prop.getValue()
                props[propName] = propValue
            print(props)

def processVertex(space, scanVertexResponse):
    result = scanVertexProcessor.process(space, scanVertexResponse)
    if result is None:
        return None
    for tagName, tagRows in result.rows.items():
        for row in tagRows:
            vid = row.defaultProperties[0].getValue()
            props = {}
            for prop in row.properties:
                propName = prop.getName()
                propValue = prop.getValue()
                props[propName] = propValue
            print(props)

def getReturnCols(space):
    tagItems = metaClient.getTags(space)
    vertexReturnCols = {}
    if tagItems is None:
        print('tags not found in space ', space)
    else:
        for tagItem in tagItems:
            tagName = tagItem.tag_name
            vertexReturnCols[tagName] = metaClient.getTagSchema(space, tagName).keys()
    edgeItems = metaClient.getEdges(space)
    edgeReturnCols = {}
    if edgeItems is None:
        print('edges not found in space ', space)
    else:
        for edgeItem in edgeItems:
            edgeName = edgeItem.edge_name
            edgeReturnCols[edgeName] = metaClient.getEdgeSchema(space, edgeName).keys()

    return vertexReturnCols, edgeReturnCols


if __name__ == '__main__':
    metaClient = MetaClient([(sys.argv[1], sys.argv[2])])
    code =  metaClient.connect()
    if code == ErrorCode.E_FAIL_TO_CONNECT:
        raise Exception('connect to %s:%d failed' % (sys.argv[1], sys.argv[2]))
    storageClient = StorageClient(metaClient)
    scanEdgeProcessor = ScanEdgeProcessor(metaClient)
    scanVertexProcessor = ScanVertexProcessor(metaClient)

    spaceToRead = sys.argv[3]
    vertexReturnCols, edgeReturnCols = getReturnCols(spaceToRead)
    allCols = True

    if spaceToRead not in metaClient.getPartsAllocFromCache().keys():
        raise Exception('spaceToRead %s is not found in nebula' % spaceToRead)
    else:
        scanVertex(spaceToRead, vertexReturnCols, allCols)
        scanEdge(spaceToRead, edgeReturnCols, allCols)
