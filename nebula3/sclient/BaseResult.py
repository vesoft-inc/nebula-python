#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from nebula3.common import ttypes
from nebula3.common.ttypes import Vertex, Tag, Edge
from nebula3.data.DataObject import DataSetWrapper, Node, ValueWrapper, Relationship


class VertexData(object):
    # TODO just ignore '_vid' column
    PROP_START_INDEX_with_vid = 2
    PROP_START_INDEX = 1

    def __init__(self, row, col_names, decode_type='utf-8'):
        """
        the format is
        '''
        |tag_name._vid|tag_name.prop1|tag_name.prop2|
        '''
        """
        if len(row.values) != len(col_names):
            raise RuntimeError(
                'Input row size is not equal with the col name size, {} != {}'.format(
                    row.values, col_names
                )
            )
        self._row = row
        self._decode_type = decode_type
        self._col_names = []
        self._tag_name = ''
        for col_name in col_names:
            names = col_name.split(b'.')
            # TODO, just keep some behevior with before
            if len(names) == 1 and names[0] == b'_vid':
                continue
            if len(names) != 2:
                raise RuntimeError('Input wrong col name format of tag')
            self._col_names.append(names[1])
            self._tag_name = names[0]

    def get_id(self):
        """get vertex id, if the space vid_type is int, you can use get_id().as_int(),
        if the space vid_type is fixed_string, you can use get_id().as_string()

        :return: ValueWrapper
        """
        if len(self._row.values) < 1:
            raise RuntimeError(
                'The row value is bad format, '
                'get vertex id failed: len is {}'.format(len(self._row.values))
            )
        return ValueWrapper(self._row.values[0], self._decode_type)

    def as_node(self):
        """convert the vertex data to structure Node

        :return: Node
        """
        if len(self._row.values) < self.PROP_START_INDEX:
            raise RuntimeError(
                'The row value is bad format, '
                'as node failed: len is {}'.format(len(self._row.values))
            )

        vertex = Vertex()
        vertex.tags = []
        vertex.vid = self._row.values[0]
        tag = Tag()
        tag.name = self._tag_name
        tag.props = {}
        index = self.PROP_START_INDEX
        while index < len(self._col_names):
            tag.props[self._col_names[index]] = self._row.values[index]
            index = index + 1
        vertex.tags.append(tag)

        return Node(vertex).set_decode_type(self._decode_type)

    def get_prop_values(self):
        """get all prop values from the vertex data

        :return: list<ValueWrapper>
        """
        index = self.PROP_START_INDEX_with_vid
        prop_values = []
        while index < len(self._row.values):
            prop_values.append(
                ValueWrapper(self._row.values[index], decode_type=self._decode_type)
            )
            index = index + 1
        return prop_values

    def __repr__(self):
        return str(self.as_node())


class EdgeData(object):
    PROP_START_INDEX = 4

    def __init__(self, row, col_names, decode_type='utf-8'):
        """
        the format is
        '''
        |edge_name._src|edge_name._type|edge_name._rank|edge_name._dst|edge_name.prop1|edge_name.prop2|
        '''
        """
        if len(row.values) != len(col_names):
            raise RuntimeError(
                'Input row size is not equal '
                'with the col name size, {} != {}'.format(
                    len(row.values), len(col_names)
                )
            )
        self._row = row
        self._decode_type = decode_type
        self._col_names = []
        self._edge_name = ''
        for col_name in col_names:
            names = col_name.split(b'.')
            if len(names) != 2:
                raise RuntimeError('Input wrong col name format of edge')
            self._col_names.append(names[1])
            self._edge_name = names[0]

    def get_src_id(self):
        """get src id, if the space vid_type is int, you can use get_src_id().as_int(),
        if the space vid_type is fixed_string, you can use get_src_id().as_string()

        :return: ValueWrapper
        """
        if len(self._row.values) < 1:
            raise RuntimeError(
                'The row value is bad format, '
                'get edge src id failed: len is {}'.format(len(self._row.values))
            )
        return ValueWrapper(self._row.values[0], self._decode_type)

    def get_edge_name(self):
        """get edge name

        :return: edge name
        """
        return self._edge_name.decode(self._decode_type)

    def get_ranking(self):
        """get edge ranking

        :return: ranking
        """
        if len(self._row.values) < 3:
            raise RuntimeError(
                'The row value is bad format, '
                'get edge ranking failed: len is {}'.format(len(self._row.values))
            )
        assert self._row.values[2].getType() == ttypes.Value.IVAL
        return self._row.values[2].get_iVal()

    def get_dst_id(self):
        """get dst id, if the space vid_type is int, you can use get_dst_id().as_int(),
        if the space vid_type is fixed_string, you can use get_dst_id().as_string()

        :return: ValueWrapper
        """
        if len(self._row.values) < 4:
            raise RuntimeError(
                'The row value is bad format, '
                'get edge dst id failed: len is {}'.format(len(self._row.values))
            )
        return ValueWrapper(self._row.values[3], self._decode_type)

    def as_relationship(self):
        """convert the edge data to structure Relationship

        :return: Relationship
        """
        if len(self._row.values) < self.PROP_START_INDEX:
            raise RuntimeError(
                'The row value is bad format, '
                'as relationship failed: len is {}'.format(len(self._row.values))
            )
        edge = Edge()
        edge.src = self._row.values[0]
        edge.type = self._row.values[1].get_iVal()
        edge.name = self._edge_name
        edge.ranking = self._row.values[2].get_iVal()
        edge.dst = self._row.values[3]
        edge.props = {}
        index = self.PROP_START_INDEX
        while index < len(self._col_names):
            edge.props[self._col_names[index]] = self._row.values[index]
            index = index + 1

        return Relationship(edge).set_decode_type(self._decode_type)

    def get_prop_values(self):
        """get all prop values from the edge data

        :return: list<ValueWrapper>
        """
        index = self.PROP_START_INDEX
        prop_values = []
        while index < len(self._row.values):
            prop_values.append(
                ValueWrapper(self._row.values[index], decode_type=self._decode_type)
            )
            index = index + 1
        return prop_values

    def __repr__(self):
        return str(self.as_relationship())


class BaseResult(object):
    def __init__(self, data_sets: list, decode_type='utf-8', is_vertex=True):
        assert data_sets is not None
        self.is_vertex = is_vertex
        self._data_sets = data_sets
        self._decode_type = decode_type
        self._pos = -1
        self._data_set_pos = 0
        self._table_pos = -1
        self._size = 0
        for data_set in self._data_sets:
            self._size += len(data_set.rows)

    def get_data_set(self):
        """get DataSet, it's the origin values, the string type is binary

        :return: DataSet
        """
        result = None
        for data_set in self._data_sets:
            if result is None:
                result = data_set
                continue
            if len(data_set.column_names) != len(result.column_names):
                raise RuntimeError('Multi DataSets are different col size')
            result.rows.extend(data_set.rows)

        return result

    def get_data_set_wrapper(self):
        """get DataSetWrapper, it's the wrapper for DataSet value, the string type is str

        :return: DataSetWrapper
        """
        result = None
        for data_set in self._data_sets:
            if result is None:
                result = data_set
                continue
            if len(data_set.column_names) != len(result.column_names):
                raise RuntimeError('Multi DataSets are different col size')
            result.rows.extend(data_set.rows)

        if result is None:
            return None
        return DataSetWrapper(result, self._decode_type)

    def __repr__(self):
        return str(self._data_sets)

    def __iter__(self):
        self._pos = -1
        return self

    def __next__(self):
        """The VertexData or EdgeData iterator

        :return: VertexData or EdgeData Iterator
        """
        if len(self._data_sets) == 0 or self._pos >= self._size - 1:
            raise StopIteration
        self._pos += 1
        self._table_pos += 1
        if self._table_pos >= len(self._data_sets[self._data_set_pos].rows):
            self._table_pos = 0
            self._data_set_pos += 1
        col_names = self._data_sets[self._data_set_pos].column_names
        row = self._data_sets[self._data_set_pos].rows[self._table_pos]
        if self.is_vertex:
            return VertexData(row, col_names, self._decode_type)
        else:
            return EdgeData(row, col_names, self._decode_type)
