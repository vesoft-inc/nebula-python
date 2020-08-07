# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import socket
import struct
import random

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from storage.StorageService import Client
from storage.ttypes import EntryId
from storage.ttypes import PropDef
from storage.ttypes import PropOwner
from storage.ttypes import ScanEdgeRequest
from storage.ttypes import ScanVertexRequest
from storage.ttypes import ErrorCode

class Iterator:
  def __init__(self, it=None):
    self.it = iter(it)
    self._hasnext = None

  def __iter__(self): 
      return self

  def next(self):
    if self._hasnext:
      result = self._thenext
    else:
      result = next(self.it)
    self._hasnext = None
    return result

  def hasNext(self):
    if self._hasnext is None:
      try: 
          self._thenext = next(self.it)
      except StopIteration: 
          self._hasnext = False
      else: 
          self._hasnext = True
    return self._hasnext


class ScanEdgeResponseIter:
    def __init__(self, clientDad, space, leader, scanEdgeRequest, client):
        self.clientDad = clientDad
        self.space = space
        self.leader = leader
        self.scanEdgeRequest = scanEdgeRequest
        self.client = client
        self.cursor = None
        self.haveNext = True

    def hasNext(self):
        return self.haveNext

    def next(self):
        self.scanEdgeRequest.cursor = self.cursor
        scanEdgeResponse = self.client.scanEdge(self.scanEdgeRequest)
        assert(scanEdgeResponse is not None), 'scanEdgeReponse is none'
        self.cursor = scanEdgeResponse.next_cursor
        self.haveNext = scanEdgeResponse.has_next

        if not self.clientDad.isSuccessfully(scanEdgeResponse):
            print('scanEdgeResponse is not successfully, failed_codes: ', scanEdgeResponse.result.failed_codes)
            self.leader, self.client = self.clientDad.handleResultCodes(scanEdgeResponse.result.failed_codes, self.space)
            self.haveNext = False
            return None
        else:
            return scanEdgeResponse
        
        return None


class ScanVertexResponseIter:
    def __init__(self, clientDad, space, leader, scanVertexRequest, client):
        self.clientDad = clientDad
        self.space = space
        self.leader = leader
        self.scanVertexRequest = scanVertexRequest
        self.client = client
        self.cursor = None
        self.haveNext = True

    def hasNext(self):
        return self.haveNext

    def next(self):
        self.scanVertexRequest.cursor = self.cursor
        scanVertexResponse = self.client.scanVertex(self.scanVertexRequest)
        assert(scanVertexResponse is not None), 'scanVertexReponse is none'
        self.cursor = scanVertexResponse.next_cursor
        self.haveNext = scanVertexResponse.has_next

        if not self.clientDad.isSuccessfully(scanVertexResponse):
            print('scanVertexResponse is not successfully, failed_codes: ', scanVertexResponse.result.failed_codes)
            self.leader, self.client = self.clientDad.handleResultCodes(scanVertexResponse.result.failed_codes, self.space)
            self.haveNext = False
            return None
        else:
            return scanVertexResponse

        return None


class ScanSpaceEdgeResponseIter:
    def __init__(self, clientDad, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        self.clientDad = clientDad
        self.scanEdgeResponseIter = None
        self.space = space
        self.partIdsIter = partIdsIter
        self.returnCols = returnCols
        self.allCols = allCols
        self.limit = limit
        self.startTime = startTime
        self.endTime = endTime

    def hasNext(self):
        return self.partIdsIter.hasNext() or self.scanEdgeResponseIter.hasNext()

    def next(self):
        if self.scanEdgeResponseIter is None or not self.scanEdgeResponseIter.hasNext():
            part = self.partIdsIter.next()
            if part is None:
                return None
            leader = self.clientDad.getLeader(self.space, part)
            if leader is None:
                raise Exception('part %s not found in space %s' % (part, self.space))
            spaceId = self.clientDad.metaClient.getSpaceIdFromCache(self.space)
            if spaceId == -1:
                raise Exception('space %s not found' % self.space)
            colums = self.clientDad.getEdgeReturnCols(self.space, self.returnCols)
            scanEdgeRequest = ScanEdgeRequest(spaceId, part, None, colums, self.allCols, self.limit, self.startTime, self.endTime)
            self.scanEdgeResponseIter = self.clientDad.doScanEdge(self.space, leader, scanEdgeRequest)
            assert(self.scanEdgeResponseIter is not None), 'scanEdgeResponseIter is None'
        
        return self.scanEdgeResponseIter.next()


class ScanSpaceVertexResponseIter:
    def __init__(self, clientDad, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        self.clientDad = clientDad
        self.scanVertexResponseIter = None
        self.space = space
        self.partIdsIter = partIdsIter
        self.returnCols = returnCols
        self.allCols = allCols
        self.limit = limit
        self.startTime = startTime
        self.endTime = endTime

    def hasNext(self):
        return self.partIdsIter.hasNext() or self.scanVertexResponseIter.hasNext()

    def next(self):
        if self.scanVertexResponseIter is None or not self.scanVertexResponseIter.hasNext():
            part = self.partIdsIter.next()
            if part is None:
                return None
            print('part: ', part)
            leader = self.clientDad.getLeader(self.space, part)
            if leader is None:
                raise Exception('part %s not found in space %s' % (part, self.space))
            spaceId = self.clientDad.metaClient.getSpaceIdFromCache(self.space)
            if spaceId == -1:
                raise Exception('space %s not found' % self.space)
            colums = self.clientDad.getVertexReturnCols(self.space, self.returnCols)
            scanVertexRequest = ScanVertexRequest(spaceId, part, None, colums, self.allCols, self.limit, self.startTime, self.endTime)
            self.scanVertexResponseIter = self.clientDad.doScanVertex(self.space, leader, scanVertexRequest)
            assert(self.scanVertexResponseIter is not None), 'scanVertexResponseIter is None'

        return self.scanVertexResponseIter.next()


class StorageClient:
    def __init__(self, metaClient):
        self.metaClient = metaClient
        self.clients = {}
        self.leaders = {}
        self.timeout = 1000

    def connect(self, address):
        if address not in self.clients.keys():
            client = self.doConnect(address)
            self.clients[address] = client
            return client
        else:
            return self.clients[address]

    def disConnect(self, address):
        self.clients.remove(address)

    def doConnects(self, addresses):
        for address in addresses:
            client = self.doConnect(address)
            self.clients[address] = client

    def doConnect(self, address):
        host = address[0]
        port = address[1]
        #print('StorageClient is connect to: tTransport ip: %s, port: %s' % (host, port))
        tTransport = TSocket.TSocket(host, port)
        tTransport.setTimeout(self.timeout)
        tTransport = TTransport.TBufferedTransport(tTransport)
        tProtocol = TBinaryProtocol.TBinaryProtocol(tTransport)
        tTransport.open()
        return Client(tProtocol)

    def scanEdge(self, space, returnCols, allCols, limit, startTime, endTime):
        partIds = self.metaClient.getPartsAllocFromCache()[space].keys()
        partIdsIter = Iterator(partIds)
        return ScanSpaceEdgeResponseIter(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime)
   
    def scanPartEdge(self, space, part, returnCols, allCols, limit, startTime, endTime):
        spaceId = self.metaClient.getSpaceIdFromCache(space)
        columns = self.getEdgeReturnCols(space, returnCols)
        scanEdgeRequest = ScanEdgeRequest(spaceId, part, None, columns, allCols, limit, startTime, endTime)
        leader = self.getLeader(space, part)
        if leader is None:
            raise Exception('part %s not found in space %s' % (part, space))
        return self.doScanEdge(space, leader, scanEdgeRequest)

    def scanVertex(self, space, returnCols, allCols, limit, startTime, endTime):
        partIds = self.metaClient.getPartsAllocFromCache()[space].keys()
        partIdsIter = Iterator(partIds)
        return ScanSpaceVertexResponseIter(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime)
    
    def scanPartVertex(self, space, part, returnCols, allCols, limit, startTime, endTime):
        spaceId = self.metaClient.getSpaceIdFromCache(space)
        columns = self.getVertexReturnCols(space, returnCols)
        scanVertexRequest = ScanVertexRequest(spaceId, part, None, columns, allCols, limit, startTime, endTime)
        leader = self.getLeader(space, part)
        if leader is None:
            raise Exception('part %s not found in space %s' % (part, space))
        return self.doScanVertex(space, leader, scanVertexRequest)

    def doScanEdge(self, space, leader, scanEdgeRequest):
        client = self.connect(leader)
        if client is None:
            print('cannot connect to leader:', leader)
            self.disConnect(leader)
            return None

        return ScanEdgeResponseIter(self, space, leader, scanEdgeRequest, client)
    
    def doScanVertex(self, space, leader, scanVertexRequest):
        client = self.connect(leader)
        if client is None:
            print('cannot connect to leader:', leader)
            self.disConnect(leader)
            return None

        return ScanVertexResponseIter(self, space, leader, scanVertexRequest, client)

    def getEdgeReturnCols(self, space, returnCols):
        columns = {}
        for edgeName, propNames in returnCols.items():
            edgeItem = self.metaClient.getEdgeItemFromCache(space, edgeName)
            if edgeItem is None:
                raise Exception('edge %s not found in space %s' % (edgeName, space))
            edgeType = edgeItem.edge_type
            entryId = EntryId(edge_type=edgeType)
            propDefs = []
            for propName in propNames:
                propDef = PropDef(PropOwner.EDGE, entryId, propName)
                propDefs.append(propDef)
            columns[edgeType] = propDefs
        return columns
    
    def getVertexReturnCols(self, space, returnCols):
        columns = {}
        for tagName, propNames in returnCols.items():
            tagItem = self.metaClient.getTagItemFromCache(space, tagName)
            if tagItem is None:
                raise Exception('tag %s not found in space %s' % (tagName, space))
            tagId = tagItem.tag_id
            entryId = EntryId(tag_id=tagId)
            propDefs = []
            for propName in propNames:
                propDef = PropDef(PropOwner.SOURCE, entryId, propName)
                propDefs.append(propDef)
            columns[tagId] = propDefs
        return columns

    def getLeader(self, spaceName, part):
        return self.metaClient.getSpacePartLeaderFromCache(spaceName, part)
    
    def updateLeader(self, spaceName, partId, leader):
        self.metaClient.updateSpacePartLeader(spaceName, partId, leader)

    def handleResultCodes(self, failedCodes, space):
        for resultCode in failedCodes:
            if resultCode.code == ErrorCode.E_LEADER_CHANGED:
                print('ErrorCode.E_LEADER_CHANGED, leader changed to :', resultCode.leader)
                hostAddr = resultCode.leader
                if hostAddr is not None and hostAddr.ip != 0 and hostAddr.port != 0:
                    host = socket.inet_ntoa(struct.pack('I',socket.htonl(hostAddr.ip & 0xffffffff)))
                    port = hostAddr.port
                    newLeader = (host, port)
                    self.updateLeader(space, resultCode.part_id, newLeader)
                    if newLeader in self.clients.keys():
                        newClient = self.clients[newLeader]
                    else:
                        newClient = None
                    return newLeader, newClient
        
        return None, None

    def isSuccessfully(self, response):
        return len(response.result.failed_codes) == 0
