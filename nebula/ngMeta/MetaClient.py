# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import random
import socket
import struct
import six
import threading
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from meta.MetaService import Client
from meta.ttypes import EdgeItem
from meta.ttypes import ErrorCode
from meta.ttypes import GetEdgeReq
from meta.ttypes import GetEdgeResp
from meta.ttypes import GetPartsAllocReq
from meta.ttypes import GetPartsAllocResp
from meta.ttypes import GetTagReq
from meta.ttypes import GetTagResp
from meta.ttypes import ListHostsReq
from meta.ttypes import ListHostsResp
from meta.ttypes import ListEdgesReq
from meta.ttypes import ListEdgesResp
from meta.ttypes import ListSpacesReq
from meta.ttypes import ListSpacesResp
from meta.ttypes import ListTagsReq
from meta.ttypes import ListTagsResp
from meta.ttypes import TagItem

if six.PY3:
    Timer = threading.Timer
else:
    Timer = threading._Timer

class RepeatTimer(Timer):
    def __init__(self, interval, function):
        Timer.__init__(self, interval, function)
        self.daemon = True # set the RepeatTimer thread as a daemon thread, so it can end when main thread ends
    
    def run(self):
        while not self.finished.wait(self.interval):
            #print('daemon: ', self.daemon)
            self.function(*self.args, **self.kwargs)

class MetaClient:
    def __init__(self, addresses, timeout=1000,
                connectionRetry=3, executionRetry=3):
        self.addresses = addresses
        self.timeout = timeout
        self.connectionRetry = connectionRetry
        self.executionRetry = executionRetry
        self.spaceNameMap = {} # map<spaceName, spaceId>
        self.spacePartLocation = {} # map<spaceName, map<partId, list<address>>>
        self.spacePartLeader = {} # map<spaceName, map<partId, leader'saddress>>
        self.spaceTagItems = {} # map<spaceName, map<TagItem.tag_name, TagItem>>
        self.spaceEdgeItems = {} # map<spaceName, map<edgeItem.edge_name, edgeItem>>
        self.tagNameMap = {} # map<spaceName, map<TagItem.tag_id, TagItem.tag_name>>
        self.edgeNameMap = {} # map<spaceName, map<edgeItem.edge_name, edgeItem>>
        self.client = None
   
    def connect(self):
        while self.connectionRetry > 0:
            code = self.doConnect(self.addresses)
            if code == 0:
                return ErrorCode.SUCCEEDED
            self.connectionRetry -= 1
        return ErrorCode.E_FAIL_TO_CONNECT

    def doConnect(self, addresses):
        address = addresses[random.randint(0, len(addresses)-1)]
        #print('metaClient is connecting to: ', address[0], address[1])
        host = address[0]
        port = address[1]
        tTransport = TSocket.TSocket(host, port)
        tTransport.setTimeout(self.timeout)
        tTransport = TTransport.TBufferedTransport(tTransport)
        tProtocol = TBinaryProtocol.TBinaryProtocol(tTransport)
        tTransport.open()
        self.client = Client(tProtocol)
        self.updateSchemas()
        RepeatTimer(2, self.updateSchemas).start() # call updatSchemas() every 2 seconds
        
        return 0

    def updateSchemas(self):
        print('threading active_count: ', threading.active_count())
        for spaceIdName in self.listSpaces():
            spaceName = spaceIdName.name # class IdName
            self.spaceNameMap[spaceName] = spaceIdName.id.get_space_id()
            #print(spaceName, spaceIdName.id.get_space_id())
            self.spacePartLocation[spaceName] = self.getPartsAlloc(spaceName)
            self.spacePartLeader[spaceName] = {}
            # Loading tag schema's cache
            tags = {}
            tagsName = {}
            for tagItem in self.getTags(spaceName):
                tags[tagItem.tag_name] = tagItem
                tagsName[tagItem.tag_id] = tagItem.tag_name

            self.spaceTagItems[spaceName] = tags
            self.tagNameMap[spaceName] = tagsName

            # Loading edge schema's cache
            edges = {}
            edgesName = {}
            for edgeItem in self.getEdges(spaceName):
                edges[edgeItem.edge_name] = edgeItem
                edgesName[edgeItem.edge_type] = edgeItem.edge_name
            self.spaceEdgeItems[spaceName] = edges
            self.edgeNameMap[spaceName] = edgesName
        self.setSpacePartLeader()
        return 0

    def getSpaceIdFromCache(self, spaceName):
        if spaceName not in self.spaceNameMap.keys():
            return -1
        else:
            return self.spaceNameMap[spaceName]

    def getSpacePartLeaderFromCache(self, spaceName, partId):
        if spaceName not in self.spacePartLeader.keys():
            return None
        if partId not in self.spacePartLeader[spaceName].keys():
            return None
        return self.spacePartLeader[spaceName][partId]
    
    def updateSpacePartLeader(self, spaceName, partId, leader):
        self.spacePartLeader[spaceName][partId] = leader

    def setSpacePartLeader(self):
        listHostsReq = ListHostsReq()
        listHostsResp = self.client.listHosts(listHostsReq)
        if listHostsResp.code != ErrorCode.SUCCEEDED:
            print('setSpacePartLeader error, eror code: ', listHostsResp.code)
            return None

        for hostItem in listHostsResp.hosts:
            host = socket.inet_ntoa(struct.pack('I',socket.htonl(hostItem.hostAddr.ip & 0xffffffff))) 
            port = hostItem.hostAddr.port
            leader = (host, port)
            for space, partIds in hostItem.leader_parts.items():
                for partId in partIds:
                    self.spacePartLeader[space][partId] = leader

    def listSpaces(self):
        listSpacesReq = ListSpacesReq()
        listSpacesResp = self.client.listSpaces(listSpacesReq)
        if listSpacesResp.code == ErrorCode.SUCCEEDED:
            return listSpacesResp.spaces########## spaceNameID--> IdName
        else:
            print('list spaces error, error code: ', listSpacesResp.code)
            return None

    def getPartFromCache(self, spaceName, part):
        if spaceName not in self.spacePartLocation.keys():
            self.spacePartLocation[spaceName] = self.getPartsAlloc(spaceName)
        partsAlloc = self.spacePartLocation[spaceName]
        if partsAlloc is None or part not in partsAlloc.keys():
            return None
        return partsAlloc[part]

    def getPartsAlloc(self, spaceName):
        spaceId = self.getSpaceIdFromCache(spaceName)
        if spaceId == -1:
            return None
        getPartsAllocReq = GetPartsAllocReq(spaceId)
        getPartsAllocResp = self.client.getPartsAlloc(getPartsAllocReq)

        if getPartsAllocResp.code == ErrorCode.SUCCEEDED:
            addressMap = {}
            for partId, hostAddrs in getPartsAllocResp.parts.items():
                addresses = []
                for hostAddr in hostAddrs:
                    host = socket.inet_ntoa(struct.pack('I',socket.htonl(hostAddr.ip & 0xffffffff)))
                    port = hostAddr.port
                    addresses.append((host, port))
                addressMap[partId] = addresses

            return addressMap
        else:
            print("get parts alloc error, error code: ", getPartsAllocResp.code)
            return None

    def getPartsAllocFromCache(self):
        return self.spacePartLocation

    def getPartAllocFromCache(self, spaceName, part):
        if spaceName in self.spacePartLocation.keys():
            partsAlloc = self.spacePartLocation[spaceName]
            if part in partsAlloc.keys():
                return partsAlloc[part]
        
        return None
    
    def getTagItemFromCache(self, spaceName, tagName):
        if spaceName in self.spaceTagItems.keys() and tagName in self.spaceTagItems[spaceName].keys():
            return self.spaceTagItems[spaceName][tagName]
        
        return None

    def getTagNameFromCache(self, spaceName, tagId):
        if spaceName in self.tagNameMap.keys():
            tagNames = self.tagNameMap[spaceName]
            if tagId in tagNames.keys():
                return tagNames[tagId]

        return None

    def getTags(self, spaceName):
        spaceId = self.getSpaceIdFromCache(spaceName)
        if spaceId == -1:
            return None
        listTagsReq = ListTagsReq(spaceId)
        listTagsResp = self.client.listTags(listTagsReq)
        
        if listTagsResp.code == ErrorCode.SUCCEEDED:
            return listTagsResp.tags
        else:
            print('get tags error, error code: ', listTagsResp.code)
            return None

    def getTag(self, spaceName, tagName, version=-1):
        spaceId = self.getSpaceIdFromCache(spaceName)
        getTagReq = GetTagReq(spaceId, tagName, version)
        getTagResp = self.client.getTag(getTagReq)

        if getTagResp.code == ErrorCode.SUCCEEDED:
            return getTagResp.schema
        else:
            return None

    def getTagSchema(self, spaceName, tagName, version=-1):
        spaceId = self.getSpaceIdFromCache(spaceName)
        print('spaceId: ', spaceId)
        if spaceId == -1:
            return None
        getTagReq = GetTagReq(spaceId, tagName, version)
        getTagResp = self.client.getTag(getTagReq)
        result = {}
        for columnDef in getTagResp.schema.columns:
            result[columnDef.name] = columnDef.type.type
        print('result: ', result)
        return result

    def getEdgeItemFromCache(self, spaceName, edgeName):
        if spaceName not in self.spaceEdgeItems.keys():
            edges = {}
            for edgeItem in self.getEdges(spaceName):
                edges[edgeItem.edge_name] = edgeItem
            self.spaceEdgeItems[spaceName] = edges

        edgeItems = self.spaceEdgeItems[spaceName]
        if edgeName in edgeItems.keys():
            return edgeItems[edgeName]
        else:
            return None
    
    def getEdgeNameFromCache(self, spaceName, edgeType):
        if spaceName in self.edgeNameMap.keys():
            edgeNames = self.edgeNameMap[spaceName]
            if edgeType in edgeNames.keys():
                return edgeNames[edgeType]

        return None

    def getEdges(self, spaceName):
        spaceId = self.getSpaceIdFromCache(spaceName)
        if spaceId == -1:
            return None
        listEdgesReq = ListEdgesReq(spaceId)
        listEdgesResp =self.client.listEdges(listEdgesReq)
        if listEdgesResp.code == ErrorCode.SUCCEEDED:
            return listEdgesResp.edges
        else:
            print('get tags error, error code: ', listEdgesResp.code)
            return None

    def getEdge(self, spaceName, edgeName, version=-1):
        spaceId = self.getSpaceIdFromCache(spaceName)
        if spaceId == -1:
            return None
        getEdgeReq = GetEdgeReq(spaceId, edgeName, version)
        getEdgeResp = self.client.getEdge(getEdgeReq)
        if getEdgeResp.code == ErrorCode.SUCCEEDED:
            return getEdgeResp.Schema
        else:
            print('get edge error, error code: ', getEdgeResp.code)
            return None

    def getEdgeSchema(self, spaceName, edgeName, version=-1):
        spaceId = self.getSpaceIdFromCache(spaceName)
        if spaceId == -1:
            return None
        getEdgeReq = GetEdgeReq(spaceId, edgeName, version)
        getEdgeResp = self.client.getEdge(getEdgeReq)
        result = {}
        for columnDef in getEdgeResp.schema.columns:
            result[columnDef.name] = columnDef.type.type
        print('edge sapcId: ', spaceId)
        print(result)
        return result
