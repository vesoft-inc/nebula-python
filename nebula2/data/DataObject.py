#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from nebula2.common import ttypes
from nebula2.Exception import (
    InvalidValueTypeException,
    InvalidKeyException,
    OutOfRangeException
)
from nebula2.common.ttypes import NullType


class Record(object):
    def __init__(self, values, names):
        assert len(names) == len(values),\
            'len(names): {} != len(values): {}, names: {}, values: {}'.format(
                len(names), len(values), str(names), str(values))
        self._record = list()
        self._names = names

        for val in values:
            self._record.append(ValueWrapper(val))

    def __iter__(self):
        return iter(self._record)

    def size(self):
        return len(self._names)

    def get_value(self, index):
        """
        get value by index
        :return: Value
        """
        if index >= len(self._names):
            raise OutOfRangeException()
        return self._record[index]

    def get_value_by_key(self, key):
        """
        get value by key
        :return: Value
        """
        try:
            return self._record[self._names.index(key)]
        except Exception:
            raise InvalidKeyException(key)

    def keys(self):
        """
        keys()
        :return: the col name of the recod
        """
        return self._names

    def values(self):
        return self._record

    def __repr__(self):
        return "{}".format('\n'.join([str(val_wrap) for val_wrap in self._record]))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DataSetWrapper(object):
    def __init__(self, data_set, decode_type='utf-8'):
        assert data_set is not None
        self._decode_type = decode_type
        self._data_set = data_set
        self._column_names = []
        self._key_indexes = {}
        self._pos = -1
        for index, name in enumerate(self._data_set.column_names):
            d_name = name.decode(self._decode_type)
            self._column_names.append(d_name)
            self._key_indexes[d_name] = index

    def get_row_size(self):
        return len(self._data_set.rows)

    def get_col_names(self):
        return self._column_names

    def get_rows(self):
        return self._data_set.rows

    def get_row_types(self):
        """
        Get row types
        :param empty
        :return: list<int>
          ttypes.Value.__EMPTY__ = 0
          ttypes.Value.NVAL = 1
          ttypes.Value.BVAL = 2
          ttypes.Value.IVAL = 3
          ttypes.Value.FVAL = 4
          ttypes.Value.SVAL = 5
          ttypes.Value.DVAL = 6
          ttypes.Value.TVAL = 7
          ttypes.Value.DTVAL = 8
          ttypes.Value.VVAL = 9
          ttypes.Value.EVAL = 10
          ttypes.Value.PVAL = 11
          ttypes.Value.LVAL = 12
          ttypes.Value.MVAL = 13
          ttypes.Value.UVAL = 14
          ttypes.Value.GVAL = 15
        """
        if len(self._data_set.rows) == 0:
            return []
        return [(value.getType()) for value in self._data_set.rows[0].values]

    def row_values(self, row_index):
        """
        Get row values
        :param index: the Record index
        :return: list<ValueWrapper>
        """
        if row_index >= len(self._data_set.rows):
            raise OutOfRangeException()
        return [(ValueWrapper(value)) for value in self._data_set.rows[row_index].values]

    def column_values(self, key):
        """
        get column values
        :param key: the col name
        :return: list<ValueWrapper>
        """
        if key not in self._column_names:
            raise InvalidKeyException(key)

        return [(ValueWrapper(row.values[self._key_indexes[key]])) for row in self._data_set.rows]

    def __iter__(self):
        self._pos = -1
        return self

    def __next__(self):
        """
        The record iterator
        :return: record
        """
        if len(self._data_set.rows) == 0 or self._pos >= len(self._data_set.rows) - 1:
            raise StopIteration
        self._pos = self._pos + 1
        return Record(self._data_set.rows[self._pos].values, self._column_names)

    def __repr__(self):
        data_str = []
        for i in range(self.get_row_size()):
            data_str.append(str(self.row_values(i)))
        value_str = ','.join(data_str)
        return 'keys: {}, values: {}'.format(self._column_names, value_str)


class Null(object):
    __NULL__ = NullType.__NULL__
    NaN = NullType.NaN
    BAD_DATA = NullType.BAD_DATA
    BAD_TYPE = NullType.BAD_TYPE
    ERR_OVERFLOW = NullType.ERR_OVERFLOW
    UNKNOWN_PROP = NullType.UNKNOWN_PROP
    DIV_BY_ZERO = NullType.DIV_BY_ZERO
    OUT_OF_RANGE = NullType.OUT_OF_RANGE

    def __init__(self, type):
        self._type = type

    def __repr__(self):
        return NullType._VALUES_TO_NAMES[self._type]

    def __eq__(self, other):
        return self._type == other._type


class ValueWrapper(object):
    def __init__(self, value, decode_type='utf-8'):
        self._value = value
        self._decode_type = decode_type

    def get_value(self):
        return self._value

    def is_null(self):
        return self._value.getType() == ttypes.Value.NVAL

    def is_empty(self):
        return self._value.getType() == ttypes.Value.__EMPTY__

    def is_bool(self):
        return self._value.getType() == ttypes.Value.BVAL

    def is_int(self):
        return self._value.getType() == ttypes.Value.IVAL

    def is_double(self):
        return self._value.getType() == ttypes.Value.FVAL

    def is_string(self):
        return self._value.getType() == ttypes.Value.SVAL

    def is_list(self):
        return self._value.getType() == ttypes.Value.LVAL

    def is_set(self):
        return self._value.getType() == ttypes.Value.UVAL

    def is_map(self):
        return self._value.getType() == ttypes.Value.MVAL

    def is_time(self):
        return self._value.getType() == ttypes.Value.TVAL

    def is_date(self):
        return self._value.getType() == ttypes.Value.DVAL

    def is_datetime(self):
        return self._value.getType() == ttypes.Value.DTVAL

    def is_vertex(self):
        return self._value.getType() == ttypes.Value.VVAL

    def is_edge(self):
        return self._value.getType() == ttypes.Value.EVAL

    def is_path(self):
        return self._value.getType() == ttypes.Value.PVAL

    def as_null(self):
        """
        :return: Null
        """
        if self._value.getType() == ttypes.Value.NVAL:
            return Null(self._value.get_nVal())
        raise InvalidValueTypeException("expect NULL type, but is " + self._get_type_name())

    def as_bool(self):
        """
        :return Boolean:
        """
        if self._value.getType() == ttypes.Value.BVAL:
            return self._value.get_bVal()
        raise InvalidValueTypeException("expect bool type, but is " + self._get_type_name())

    def as_int(self):
        """
        :return int:
        """
        if self._value.getType() == ttypes.Value.IVAL:
            return self._value.get_iVal()
        raise InvalidValueTypeException("expect bool type, but is " + self._get_type_name())

    def as_double(self):
        """
        :return double:
        """
        if self._value.getType() == ttypes.Value.FVAL:
            return self._value.get_fVal()
        raise InvalidValueTypeException("expect int type, but is " + self._get_type_name())

    def as_string(self):
        """
        :return string:
        """
        if self._value.getType() == ttypes.Value.SVAL:
            return self._value.get_sVal().decode(self._decode_type)
        raise InvalidValueTypeException("expect string type, but is " + self._get_type_name())

    def as_time(self):
        """
        :return: TimeWrapper
        """
        if self._value.getType() == ttypes.Value.TVAL:
            return TimeWrapper(self._value.get_tVal())
        raise InvalidValueTypeException("expect time type, but is " + self._get_type_name())

    def as_date(self):
        """
        :return: DateWrapper
        """
        if self._value.getType() == ttypes.Value.DVAL:
            return DateWrapper(self._value.get_dVal())
        raise InvalidValueTypeException("expect date type, but is " + self._get_type_name())

    def as_datetime(self):
        """
        :return: DateTimeWrapper
        """
        if self._value.getType() == ttypes.Value.DTVAL:
            return DateTimeWrapper(self._value.get_dtVal())
        raise InvalidValueTypeException("expect datetime type, but is " + self._get_type_name())

    def as_list(self):
        """
        :return: list<ValueWrapper>
        """
        if self._value.getType() == ttypes.Value.LVAL:
            result = []
            for val in self._value.get_lVal().values:
                result.append(ValueWrapper(val))
            return result
        raise InvalidValueTypeException("expect list type, but is " + self._get_type_name())

    def as_set(self):
        """
        :return: set<ValueWrapper>
        """
        if self._value.getType() == ttypes.Value.UVAL:
            result = set()
            for val in self._value.get_uVal().values:
                result.add(ValueWrapper(val))
            return result
        raise InvalidValueTypeException("expect set type, but is " + self._get_type_name())

    def as_map(self):
        """
        :return: map<string, ValueWrapper>
        """
        if self._value.getType() == ttypes.Value.MVAL:
            result = {}
            kvs = self._value.get_mVal().kvs
            for key in kvs.keys():
                result[key.decode(self._decode_type)] = ValueWrapper(kvs[key])
            return result
        raise InvalidValueTypeException("expect map type, but is " + self._get_type_name())

    def as_node(self):
        """
        :return: Node
        """
        if self._value.getType() == ttypes.Value.VVAL:
            return Node(self._value.get_vVal())
        raise InvalidValueTypeException("expect vertex type, but is " + self._get_type_name())

    def as_relationship(self):
        """
        :return: Relationship
        """
        if self._value.getType() == ttypes.Value.EVAL:
            return Relationship(self._value.get_eVal())
        raise InvalidValueTypeException("expect edge type, but is " + self._get_type_name())

    def as_path(self):
        """
        :return: PathWrapper
        """
        if self._value.getType() == ttypes.Value.PVAL:
            return PathWrapper(self._value.get_pVal())
        raise InvalidValueTypeException("expect path type, but is " + self._get_type_name())

    def _get_type_name(self):
        if self.is_empty():
            return "empty"
        if self.is_null():
            return "null"
        if self.is_bool():
            return "bool"
        if self.is_int():
            return "int"
        if self.is_double():
            return "double"
        if self.is_string():
            return "string"
        if self.is_list():
            return "list"
        if self.is_set():
            return "set"
        if self.is_map():
            return "map"
        if self.is_time():
            return "time"
        if self.is_date():
            return "date"
        if self.is_datetime():
            return "datetime"
        if self.is_vertex():
            return "vertex"
        if self.is_edge():
            return "edge"
        if self.is_path():
            return "path"
        return "unknown"

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        if self.get_value().getType() != o.get_value().getType():
            return False
        if self.is_empty():
            return o.is_empty()
        elif self.is_null():
            return self.as_null() == o.as_null()
        elif self.is_bool():
            return self.as_bool() == o.as_bool()
        elif self.is_int():
            return self.as_int() == o.as_int()
        elif self.is_double():
            return self.as_double() == o.as_double()
        elif self.is_string():
            return self.as_string() == o.as_string()
        elif self.is_list():
            return self.as_list() == o.as_list()
        elif self.is_set():
            return self.as_set() == o.as_set()
        elif self.is_map():
            return self.as_map() == o.as_map()
        elif self.is_vertex():
            return self.as_node() == o.as_node()
        elif self.is_edge():
            return self.as_relationship() == o.as_relationship()
        elif self.is_path():
            return self.as_path() == o.as_path()
        elif self.is_time():
            return self.as_time() == self.as_time()
        elif self.is_date():
            return self.as_date() == self.as_date()
        elif self.is_datetime():
            return self.as_datetime() == self.as_datetime()
        else:
            raise RuntimeError('Unsupported type:{} to compare'.format(self._get_type_name()))
        return False

    def __repr__(self):
        if self.is_empty():
            return '__EMPTY__'
        elif self.is_null():
            return str(self.as_null())
        elif self.is_bool():
            return 'True' if self.as_bool() else 'False'
        elif self.is_int():
            return str(self.as_int())
        elif self.is_double():
            return str(self.as_double())
        elif self.is_string():
            return '\"{}\"'.format(self.as_string())
        elif self.is_list():
            return str(self.as_list())
        elif self.is_set():
            return str(self.as_set())
        elif self.is_map():
            return str(self.as_map())
        elif self.is_vertex():
            return str(self.as_node())
        elif self.is_edge():
            return str(self.as_relationship())
        elif self.is_path():
            return str(self.as_path())
        elif self.is_time():
            return str(self.as_time())
        elif self.is_date():
            return str(self.as_date())
        elif self.is_datetime():
            return str(self.as_datetime())
        else:
            raise RuntimeError('Unsupported type:{} to compare'.format(self._get_type_name()))
        return False

    def __hash__(self):
        return self._value.__hash__()


class TimeWrapper(object):
    def __init__(self, time, time_zone='+08:00'):
        self._time = time
        self._time_zone = time_zone

    def get_hour(self):
        return self._time.hour

    def get_minute(self):
        return self._time.minute

    def get_sec(self):
        return self._time.sec

    def get_microsec(self):
        return self._time.microsec

    def get_time(self):
        return self._time

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._time.hour == other.get_hour() and \
               self._time.minute == other.get_minute() and \
               self._time.sec == other.get_sec() and \
               self._time.microsec == self.get_microsec()

    def __repr__(self):
        return "%02d:%02d:%02d.%06d" % (self._time.hour,
                                        self._time.minute,
                                        self._time.sec,
                                        self._time.microsec)


class DateWrapper(object):
    def __init__(self, date):
        self._date = date

    def get_year(self):
        return self._date.year

    def get_month(self):
        return self._date.month

    def get_day(self):
        return self._date.day

    def get_date(self):
        return self._date

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._date.year == other.get_year() and \
               self._date.month == other.get_month() and \
               self._date.day == other.get_day()

    def __repr__(self):
        return "%d-%02d-%02d" % (self._date.year, self._date.month, self._date.day)


class DateTimeWrapper(object):
    def __init__(self, date_time, time_zone='+08:00'):
        self._date_time = date_time
        self._time_zone = time_zone

    def get_year(self):
        return self._date_time.year

    def get_month(self):
        return self._date_time.month

    def get_day(self):
        return self._date_time.day

    def get_hour(self):
        return self._date_time.hour

    def get_minute(self):
        return self._date_time.minute

    def get_sec(self):
        return self._date_time.sec

    def get_microsec(self):
        return self._date_time.microsec

    def get_datetime(self):
        return self._date_time

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._date_time.year == other.get_year() and \
               self._date_time.month == other.get_month() and \
               self._date_time.day == other.get_day() and \
               self._date_time.hour == other.get_hour() and \
               self._date_time.minute == other.get_minute() and \
               self._date_time.sec == other.get_sec() and \
               self._date_time.microsec == other.get_microsec()

    def __repr__(self):
        return "%d-%02d-%02dT%02d:%02d:%02d.%06d" % (self._date_time.year,
                                                     self._date_time.month,
                                                     self._date_time.day,
                                                     self._date_time.hour,
                                                     self._date_time.minute,
                                                     self._date_time.sec,
                                                     self._date_time.microsec)


class GenValue(object):
    @classmethod
    def gen_vertex(cls, vid, tags):
        vertex = ttypes.Vertex()
        vertex.vid = vid
        vertex.tags = tags
        return vertex

    @classmethod
    def gen_edge(cls, src_id, dst_id, type, edge_name, ranking, props):
        edge = ttypes.Edge()
        edge.src = src_id
        edge.dst = dst_id
        edge.type = type
        edge.name = edge_name
        edge.ranking = ranking
        edge.props = props
        return edge

    @classmethod
    def gen_segment(cls, start_node, end_node, relationship):
        segment = Segment()
        segment.start_node = start_node
        segment.end_node = end_node
        segment.relationship = relationship
        return segment


class Node(object):
    def __init__(self, vertex, decode_type='utf-8'):
        self._value = vertex
        self._tag_indexes = dict()
        self._decode_type = decode_type
        for index, tag in enumerate(self._value.tags, start=0):
            self._tag_indexes[tag.name.decode(self._decode_type)] = index

    def get_id(self):
        """
        get vertex id
        :return: ValueWrapper
        """
        return ValueWrapper(self._value.vid, self._decode_type)

    def tags(self):
        return list(self._tag_indexes.keys())

    def has_tag(self, tag):
        return True if tag in self._tag_indexes.keys() else False

    def propertys(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)

        props = self._value.tags[self._tag_indexes[tag]].props
        result_props = {}
        for key in props.keys():
            result_props[key.decode(self._decode_type)] = ValueWrapper(props[key])
        return result_props

    def prop_names(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        return [(key.decode(self._decode_type)) for key in self._value.tags[index].props.keys()]

    def prop_values(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        return [(ValueWrapper(value)) for value in self._value.tags[index].props.values()]

    def __repr__(self):
        tag_str_list = list()
        for tag in self._tag_indexes.keys():
            prop_strs = ['%s: %s' % (key, str(val)) for key, val in self.propertys(tag).items()]
            tag_str_list.append(':%s{%s}' % (tag, ', '.join(prop_strs)))
        return '({} {})'.format(str(self.get_id()), ' '.join(tag_str_list))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.get_id() == other.get_id()

    def __ne__(self, other):
        return not (self == other)


class Relationship(object):
    def __init__(self, edge, decode_type='utf-8'):
        self._decode_type = decode_type
        self._value = edge

    def start_vertex_id(self):
        """
        get start vid
        :return: ValueWrapper
        """
        if self._value.type > 0:
            return ValueWrapper(self._value.src, self._decode_type)
        else:
            return ValueWrapper(self._value.dst, self._decode_type)

    def end_vertex_id(self):
        """
        get end vid
        :return: ValueWrapper
        """
        if self._value.type > 0:
            return ValueWrapper(self._value.dst, self._decode_type)
        else:
            return ValueWrapper(self._value.src, self._decode_type)

    def edge_name(self):
        return self._value.name.decode(self._decode_type)

    def ranking(self):
        return self._value.ranking

    def propertys(self):
        props = {}
        for key in self._value.props.keys():
            props[key.decode(self._decode_type)] = ValueWrapper(self._value.props[key])
        return props

    def keys(self):
        return [(key.decode(self._decode_type)) for key in self._value.props.keys()]

    def values(self):
        return [(ValueWrapper(value)) for value in self._value.props.values]

    def __repr__(self):
        prop_strs = ['%s: %s' % (key, str(val)) for key, val in self.propertys().items()]
        return "(%s)-[:%s@%d{%s}]->(%s)" % (str(self.start_vertex_id()),
                                            self.edge_name(),
                                            self.ranking(),
                                            ', '.join(prop_strs),
                                            str(self.end_vertex_id()))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.start_vertex_id() == other.start_vertex_id() \
               and self.end_vertex_id() == other.end_vertex_id() \
               and self.edge_name() == other.edge_name() \
               and self.ranking() == self.ranking()

    def __ne__(self, other):
        return not (self == other)


class Segment:
    start_node = None
    end_node = None
    relationship = None

    def __repr__(self):
        return "{}-[:{}@{}{}]->{}".format(self.start_node,
                                          self.relationship.edge_name(),
                                          self.relationship.ranking(),
                                          self.relationship.propertys(),
                                          self.end_node)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.start_node == other.start_node \
               and self.end_node == other.end_node \
               and self.relationship == other.relationship


class PathWrapper(object):
    def __init__(self, path, decode_type='utf-8'):
        self._decode_type = decode_type
        self._nodes = list()
        self._segments = list()
        self._relationships = list()

        self._path = path
        self._nodes.append(Node(path.src))

        vids = []
        vids.append(path.src.vid)
        for step in self._path.steps:
            type = step.type
            if step.type > 0:
                start_node = self._nodes[-1]
                end_node = Node(step.dst)
                src_id = vids[-1]
                dst_id = step.dst.vid
            else:
                type = -type
                end_node = self._nodes[-1]
                start_node = Node(step.dst)
                dst_id = vids[-1]
                src_id = step.dst.vid
            vids.append(step.dst.vid)
            relationship = Relationship(GenValue.gen_edge(src_id,
                                                          dst_id,
                                                          type,
                                                          step.name,
                                                          step.ranking,
                                                          step.props))

            self._relationships.append(relationship)
            segment = GenValue.gen_segment(start_node, end_node, relationship)
            if segment.start_node == self._nodes[-1]:
                self._nodes.append(segment.end_node)
            elif segment.end_node == self._nodes[-1]:
                self._nodes.append(segment.start_node)
            else:
                raise Exception("Relationship [{}] does not connect to the last node".
                                format(relationship))

            self._segments.append(segment)

    def __iter__(self):
        return iter(self._segments)

    def start_node(self):
        if len(self._nodes) == 0:
            return None
        return self._nodes[0]

    def length(self):
        return len(self._segments)

    def contain_node(self, node):
        return True if node in self._nodes else False

    def contain_relationship(self, relationship):
        return True if relationship in self._relationships else False

    def nodes(self):
        return self._nodes

    def relationships(self):
        return self._relationships

    def segments(self):
        return self._segments

    def __repr__(self):
        edge_strs = []
        for step in self._path.steps:
            relationship = Relationship(GenValue.gen_edge(step.dst.vid,
                                                          step.dst.vid,
                                                          type,
                                                          step.name,
                                                          step.ranking,
                                                          step.props))
            edge_str = ''
            prop_strs = ['%s: %s' % (key, str(val)) for key, val in relationship.propertys().items()]
            if step.type > 0:
                edge_str = '-[:%s@%d{%s}]->%s' % (relationship.edge_name(),
                                                  relationship.ranking(),
                                                  ', '.join(prop_strs),
                                                  Node(step.dst))
            else:
                edge_str = "<-[:%s@%d{%s}]-%s" % (relationship.edge_name(),
                                                  relationship.ranking(),
                                                  ', '.join(prop_strs),
                                                  Node(step.dst))

            edge_strs.append(edge_str)
        return '{}{}'.format(Node(self._path.src), ''.join(edge_strs))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._segments == other.segments()

    def __ne__(self, other):
        return not (self == other)
