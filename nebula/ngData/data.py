# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from enum import Enum
import struct
import six

class PropertyDef:
    PropertyType = Enum('PropertyType', ('UNKNOWN', 'BOOL', 'INT', 'VID', 'FLOAT', 'DOUBLE', \
        'STRING', 'VERTEX_ID', 'TAG_ID', 'SRC_ID', 'EDGE_TYPE', 'EDGE_RANK', 'DST_ID'))

    def __init__(self, propertyType, name):
        self.propertyType = propertyType
        self.name = name


class Property:
    def __init__(self, propertyType, name, value):
        self.propertyDef = PropertyDef(propertyType, name)
        self.value = value
    
    def getType(self):
        return self.propertyDef.type
    
    def getName(self):
        return self.propertyDef.name
    
    def getValue(self):
        return self.value 

class Row:
    def __init__(self, defaultProperties, properties):
        self.defaultProperties = defaultProperties
        self.properties = properties


class RowReader:
    def __init__(self, schema, schemaVersion=0):
        self.schemaVersion = schemaVersion
        self.defs = []
        self.fieldNum = 0
        self.propertyNameIndex = {}
        self.propertyTypes = []
        self.offset = 0

        idx = 0
        for columnDef in schema.columns:
            propertyType = PropertyDef.PropertyType(columnDef.type.type+1) # ColumnDef is in common/ttypes.py
            columnName = columnDef.name
            self.defs.append((columnName, propertyType))
            self.propertyNameIndex[columnName] = idx
            idx += 1
        self.fieldNum = len(self.defs)

    def decodeValue(self, value, schemaVersion=None):
        if schemaVersion is None:
            schemaVersion = self.schemaVersion
        self.offset = 0
        # need to check if value is valid
        if six.PY2:
            blkOffBytesNum = ord(value[0]) & 0x07 + 1
            verBytesNum = ord(value[0]) >> 5
        else:
            blkOffBytesNum = value[0] & 0x07 + 1
            verBytesNum = value[0] >> 5
        self.offset += 1
        ver = 0
        if verBytesNum > 0:
            for i in range(verBytesNum):
                if six.PY2:
                    ver |= ord(value[self.offset]) << 8
                else:
                    ver |= value[self.offset] << 8
                self.offSet += 1
        # print('blkOffBytesNum: ', blkOffBytesNum, ' verBytesNum: ', verBytesNum, ' ver: ', ver, ' schemaVersion: ', schemaVersion)
        if ver != schemaVersion:
            raise Exception('parsed version %d is not equal to version %d provided', ver, schemaVersion)
        self.offset += blkOffBytesNum * (self.fieldNum // 16)
        properties = []
        for i in range(len(self.defs)):
            field = self.defs[i][0]
            propertyType = self.defs[i][1]
            if propertyType == PropertyDef.PropertyType.BOOL:
                properties.append(self.getBoolProperty(field, value))
            elif propertyType == PropertyDef.PropertyType.INT:
                properties.append(self.getIntProperty(field, value))
            elif propertyType == PropertyDef.PropertyType.FLOAT: #unused now
                properties.append(self.getFloatProperty(field, value))
            elif propertyType == PropertyDef.PropertyType.DOUBLE:
                properties.append(self.getDoubleProperty(field, value))
            elif propertyType == PropertyDef.PropertyType.STRING:
                properties.append(self.getStringProperty(field, value))
            else:
                raise Exception('Invalid propertyType in schema: ', propertyType)

        return properties

    def edgeKey(self, srcId, edgeType, dstId):
        properties = []
        properties.append(Property(PropertyDef.PropertyType.SRC_ID, "_srcId", srcId))
        properties.append(Property(PropertyDef.PropertyType.EDGE_TYPE, "_edgeType", edgeType))
        properties.append(Property(PropertyDef.PropertyType.DST_ID, "_dstId", dstId))
        return properties

    def vertexKey(self, vertexId, tagId):
        properties = []
        properties.append(Property(PropertyDef.PropertyType.VERTEX_ID, "_vertexId", vertexId))
        properties.append(Property(PropertyDef.PropertyType.TAG_ID, "_tagId", tagId))
        return properties

    def getProperty(self, row, name):
        if name not in propertyNameIndex.keys():
            return None
        return row.properties[propertyNameIndex[name]]

    def getPropertyByIndex(self, row, index):
        if index < 0 or index >= len(row.getProperties()):
            return None
        return row.properties[index]

    def getBoolProperty(self, name, value):
        if six.PY2:
            val = ord(value[self.offset]) != 0x00
        else:
            val = value[self.offset] != 0x00
        self.offset += 1
        return Property(PropertyDef.PropertyType.BOOL, name, val)

    def getIntProperty(self, name, value):
        val = self.readCompressedInt(value)
        return Property(PropertyDef.PropertyType.INT, name, val)  #### 字节流解析出data

    def getFloatProperty(self, name, value):
        val = struct.unpack_from('<f', value, self.offset)[0]
        self.offset += 4
        return Property(PropertyDef.PropertyType.FLOAT, name, val)

    def getDoubleProperty(self, name, value):
        val = struct.unpack_from('<d', value, self.offset)[0]
        self.offset += 8
        return Property(PropertyDef.PropertyType.DOUBLE, name, val)

    def getStringProperty(self, name, value):
        strLen = self.readCompressedInt(value)
        #val = value[self.offset:self.offset+strLen].decode(encoding='utf-8')
        if six.PY2:
            val = str(value[self.offset:self.offset+strLen])
        else:
            val = str(value[self.offset:self.offset+strLen], encoding='utf-8')
        self.offset += strLen
        return Property(PropertyDef.PropertyType.STRING, name, val)
    
    def readCompressedInt(self, value):
        shift = 0
        val = 0
        curOff = self.offset
        byteV = 0
        while curOff < len(value):
            byteV = struct.unpack_from('b', value, curOff)[0]
            #print('curByte: ', value[curOff], 'byteV: ', byteV)
            if byteV >= 0:
                break
            val |= (byteV & 0x7f) << shift
            curOff += 1
            shift += 7
        if curOff == len(value):
            return None
        val |= byteV << shift
        curOff += 1
        #print('readCompressedInt: ', value[self.offset:curOff], 'val is: ', val)
        self.offset = curOff
        return val


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.size = 0
        for entry in rows:
            self.size += len(rows[entry])
