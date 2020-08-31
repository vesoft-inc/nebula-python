# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from common.ttypes import SupportedType
import struct
import six
from datetime import datetime

class PropertyDef:
    def __init__(self, property_type, name):
        self._property_type = property_type
        self._name = name


class Property:
    def __init__(self, property_type, name, value):
        self._propertyDef = PropertyDef(property_type, name)
        self._value = value

    def get_type(self):
        return self._propertyDef._property_type

    def get_name(self):
        return self._propertyDef._name

    def get_value(self):
        return self._value

class Row:
    def __init__(self, default_properties, properties):
        self._default_properties = default_properties
        self._properties = properties


class RowReader:
    def __init__(self, schema, schema_version=0):
        """Initalizer
        Arguments:
            - schema: schema of tag or edge
            - schema_version: version of schema
        Returns: emtpy
        """
        self._schema_version = schema_version
        self._defs = []
        self._field_num = 0
        self._property_name_index = {}
        self._property_types = []
        self._offset = 0

        idx = 0
        for column_def in schema.columns:
            property_type = column_def.type.type
            column_name = column_def.name
            self._defs.append((column_name, property_type))
            self._property_name_index[column_name] = idx
            idx += 1
        self._field_num = len(self._defs)

    def decode_value(self, value, schema_version=None):
        """ decode data
        Arguments:
            - value: data value scanned from the storage server
            - schema_version: version of schema
        Returns: decoded property values
        """
        if schema_version is None:
            schema_version = self._schema_version
        self._offset = 0
        # need to check if value is valid
        if six.PY2:
            blk_off_bytes_num = ord(value[0]) & 0x07 + 1
            ver_bytes_num = ord(value[0]) >> 5
        else:
            blk_off_bytes_num = value[0] & 0x07 + 1
            ver_bytes_num = value[0] >> 5
        self._offset += 1
        ver = 0
        if ver_bytes_num > 0:
            for i in range(ver_bytes_num):
                if six.PY2:
                    ver |= ord(value[self._offset]) << 8
                else:
                    ver |= value[self._offset] << 8
                self._offSet += 1
        if ver != schema_version:
            raise Exception('parsed version %d is not equal to version %d provided', ver, schema_version)
        self._offset += blk_off_bytes_num * (self._field_num // 16)
        properties = []
        for i in range(len(self._defs)):
            field = self._defs[i][0]
            property_type = self._defs[i][1]
            if property_type == SupportedType.BOOL:
                properties.append(self.get_bool_property(field, value))
            elif property_type == SupportedType.INT:
                properties.append(self.get_int_property(field, value))
            elif property_type == SupportedType.FLOAT: #unused now
                properties.append(self.get_float_property(field, value))
            elif property_type == SupportedType.DOUBLE:
                properties.append(self.get_double_property(field, value))
            elif property_type == SupportedType.STRING:
                properties.append(self.get_string_property(field, value))
            elif property_type == SupportedType.TIMESTAMP:
                properties.append(self.get_timestamp_property(field, value))
            else:
                raise Exception('Unsupported propertyType in schema: ', property_type)

        return properties

    def edge_key(self, srcId, edgeName, dstId):
        properties = []
        properties.append(Property(SupportedType.VID, "_src", srcId))
        properties.append(Property(SupportedType.STRING, "_edge", edgeName))
        properties.append(Property(SupportedType.VID, "_dst", dstId))
        return properties

    def vertex_key(self, vertexId, tagName):
        properties = []
        properties.append(Property(SupportedType.VID, "_vid", vertexId))
        properties.append(Property(SupportedType.STRING, "_tag", tagName))
        return properties

    def get_property(self, row, name):
        if name not in property_name_index.keys():
            return None
        return row.properties[property_name_index[name]]

    def get_property_by_index(self, row, index):
        if index < 0 or index >= len(row.get_properties()):
            return None
        return row.properties[index]

    def get_bool_property(self, name, value):
        if six.PY2:
            val = ord(value[self._offset]) != 0x00
        else:
            val = value[self._offset] != 0x00
        self._offset += 1
        return Property(SupportedType.BOOL, name, val)

    def get_int_property(self, name, value):
        val = self.read_compressed_int(value)
        return Property(SupportedType.INT, name, val)  #### 字节流解析出data

    def get_timestamp_property(self, name, value):
        val = self.read_compressed_int(value)
        return Property(SupportedType.TIMESTAMP, name, val)

    def get_float_property(self, name, value):
        val = struct.unpack_from('<f', value, self._offset)[0]
        self._offset += 4
        return Property(SupportedType.FLOAT, name, val)

    def get_double_property(self, name, value):
        val = struct.unpack_from('<d', value, self._offset)[0]
        self._offset += 8
        return Property(SupportedType.DOUBLE, name, val)

    def get_string_property(self, name, value):
        strLen = self.read_compressed_int(value)
        if six.PY2:
            val = str(value[self._offset:self._offset+strLen])
        else:
            val = str(value[self._offset:self._offset+strLen], encoding='utf-8')
        self._offset += strLen
        return Property(SupportedType.STRING, name, val)

    def read_compressed_int(self, value):
        shift = 0
        val = 0
        curOff = self._offset
        byteV = 0
        while curOff < len(value):
            byteV = struct.unpack_from('b', value, curOff)[0]
            if byteV >= 0:
                break
            val |= (byteV & 0x7f) << shift
            curOff += 1
            shift += 7
        if curOff == len(value):
            return None
        val |= byteV << shift
        curOff += 1
        self._offset = curOff
        return val


class Result:
    def __init__(self, rows):
        self._rows = rows
        self._size = 0
        for entry in rows:
            self._size += len(rows[entry])
