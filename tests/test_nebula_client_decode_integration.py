# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not need this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Integration tests for NebulaClient decode functionality
Based on Java NebulaClientDecodeTest - requires actual NebulaGraph connection

To run these tests:
1. Start NebulaGraph server (default: 127.0.0.1:9669)
2. Set environment variables:
   - NEBULA_HOSTS: NebulaGraph addresses (default: "127.0.0.1:9669")
   - NEBULA_USER: Username (default: "root")
   - NEBULA_PASSWORD: Password (default: "nebula")
3. Run: python -m pytest tests/test_nebula_client_decode_integration.py -v
"""

import os
import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from nebulagraph_python import NebulaClient
from nebulagraph_python.py_data_types import (
    GeoShape,
    Geography,
    NDuration,
    NLineString,
    NPoint,
    NPolygon,
    NVector,
)
from nebulagraph_python.decoder.data_types import ColumnType
from nebulagraph_python.value_wrapper import ValueWrapper


class TestNebulaClientDecodeIntegration(unittest.TestCase):
    """Integration tests for NebulaClient decode functionality"""

    @classmethod
    def setUpClass(cls):
        """Set up test environment - create graph type and insert test data"""
        cls.hosts = os.getenv("NEBULA_HOSTS", "127.0.0.1:9669")
        cls.user = os.getenv("NEBULA_USER", "root")
        cls.password = os.getenv("NEBULA_PASSWORD", "nebula")

        cls.client = None
        try:
            cls.client = NebulaClient(
                addresses=cls.hosts,
                user_name=cls.user,
                password=cls.password,
            )

            # Clean up existing graphs
            cls.client.execute("DROP GRAPH IF EXISTS decode")
            cls.client.execute("DROP GRAPH TYPE IF EXISTS decode_type")

            # Create graph type
            create_graph_type = """
                CREATE GRAPH TYPE IF NOT EXISTS decode_type AS {
                    node player(
                        LABEL player{
                            id INT32 PRIMARY KEY, 
                            age INT32, 
                            name STRING, 
                            p_bool BOOL, 
                            p_float FLOAT32, 
                            p_double FLOAT64, 
                            p_date DATE, 
                            p_datetime LOCAL DATETIME, 
                            p_time LOCAL TIME,
                            p_zonedTime ZONED TIME, 
                            p_ZonedDT ZONED DATETIME,
                            p_list LIST<STRING>
                        }
                    ),
                    node person(LABEL person{id INT32 PRIMARY KEY, high INT32}),
                    edge friend(person)-[LABEL friend{degree INT32}]->(person)
                }
            """
            cls.client.execute(create_graph_type)

            # Create graph
            cls.client.execute("CREATE GRAPH IF NOT EXISTS decode TYPED decode_type")

            # Insert test data
            insert_node1 = """
                USE decode INSERT OR IGNORE 
                (@player{
                    id:1,
                    age:10,
                    name:"Tom",
                    p_bool:true, 
                    p_float:1.0, 
                    p_double:2.0,
                    p_date:DATE("2024-01-01"), 
                    p_datetime:LOCAL_DATETIME("2024-01-01T12:01:10"),
                    p_time:LOCAL_TIME("10:12:13"),
                    p_zonedTime:ZONED_TIME("10:12:12+0800"),
                    p_ZonedDT:ZONED_DATETIME("2024-12-12T10:00:00+0800"),
                    p_list:["a","b"]
                })
            """
            cls.client.execute(insert_node1)

            insert_node2 = 'USE decode INSERT OR IGNORE (@person{id:2,high:20})'
            cls.client.execute(insert_node2)

            insert_edge = """
                TABLE t{src,dst,degree}= (2,2,11) 
                USE decode FOR r IN t 
                MATCH(v1:person) WHERE v1.id=r.src 
                MATCH(v2:person) WHERE v2.id=r.dst 
                INSERT OR IGNORE(v1)-[@friend{degree:r.degree}]->(v2)
            """
            cls.client.execute(insert_edge)

            # Set timezone
            cls.client.execute('SESSION SET TIME ZONE "Asia/Shanghai"')

        except Exception as e:
            if cls.client:
                cls.client.close()
            raise unittest.SkipTest(f"Failed to set up test environment: {e}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        if cls.client:
            cls.client.close()
            cls.client = None

    def test_const_vector_geo_point_result(self):
        """Test: RETURN ST_GeogFromText("POINT(24.7 36.842)") AS p"""
        result = self.client.execute('RETURN ST_GeogFromText("POINT(24.7 36.842)") AS p')

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row["p"].cast()

        self.assertIsInstance(value, Geography)
        self.assertIsInstance(value, NPoint)
        self.assertEqual(value.get_shape(), GeoShape.GEO_SHAPE_POINT)
        self.assertEqual(value.get_lng(), 24.7)
        self.assertEqual(value.get_lat(), 36.842)
        self.assertEqual(str(value), "POINT(24.7 36.842)")

    def test_const_vector_geo_linestring_result(self):
        """Test: RETURN ST_GeogFromText("LINESTRING(-122.416667 37.783333, -122.383333 37.766667)") AS p"""
        result = self.client.execute(
            'RETURN ST_GeogFromText("LINESTRING(-122.416667 37.783333, -122.383333 37.766667)") AS p'
        )

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row["p"].cast()

        self.assertIsInstance(value, Geography)
        self.assertIsInstance(value, NLineString)
        self.assertEqual(value.get_shape(), GeoShape.GEO_SHAPE_LINESTRING)
        self.assertEqual(len(value.get_points()), 2)
        self.assertEqual(str(value), "LINESTRING(-122.416667 37.783333, -122.383333 37.766667)")

    def test_const_vector_geo_polygon_result(self):
        """Test: RETURN ST_GeogFromText("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2))") AS p"""
        result = self.client.execute(
            'RETURN ST_GeogFromText("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2))") AS p'
        )

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row["p"].cast()

        self.assertIsInstance(value, Geography)
        self.assertIsInstance(value, NPolygon)
        self.assertEqual(value.get_shape(), GeoShape.GEO_SHAPE_POLYGON)
        self.assertEqual(value.get_loop_num(), 2)

    def test_const_vector_int_result(self):
        """Test: for i in range(1,100) return 1 as c"""
        result = self.client.execute("for i in range(1,100) return 1 as c")

        self.assertEqual(result.size, 100)
        values = [row["c"].cast() for row in result]

        for value in values:
            self.assertEqual(value, 1)

    def test_const_vector_string_result(self):
        """Test: return "abc" as v"""
        result = self.client.execute('return "abc" as v')

        self.assertEqual(result.size, 1)
        row = result.one()
        self.assertEqual(row["v"].cast(), "abc")

        # Test Chinese characters
        result = self.client.execute('return "中文"')
        row = result.one()
        self.assertEqual(row[0].cast(), "中文")

    def test_const_vector_bool_result(self):
        """Test: return true as t, false as f"""
        result = self.client.execute("return true as t, false as f")

        self.assertEqual(result.size, 1)
        row = result.one()
        self.assertTrue(row["t"].cast())
        self.assertFalse(row["f"].cast())

    def test_const_vector_date_result(self):

            """Test: return date("2024-01-01")"""

            result = self.client.execute('return date("2024-01-01")')



            self.assertEqual(result.size, 1)

            row = result.one()

            value = row[0].cast()

            self.assertEqual(value, date(2024, 1, 1))

    def test_const_vector_duration_result(self):
        """Test: return duration "P1Y" """
        result = self.client.execute('return duration "P1Y"')

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row[0].cast()

        self.assertIsInstance(value, NDuration)
        self.assertEqual(str(value), "P1Y")

        # Test time-based duration
        result = self.client.execute('return duration "PT1H2M3S"')
        row = result.one()
        value = row[0].cast()

        self.assertIsInstance(value, NDuration)
        self.assertEqual(str(value), "PT1H2M3S")

    def test_const_vector_list_result(self):
        """Test: return LIST[0,1,2,3,4]"""
        result = self.client.execute("return LIST[0,1,2,3,4]")

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row[0].cast()

        self.assertEqual(len(value), 5)

    def test_const_vector_record_result(self):
        """Test: LET r={a:1, b:true, c:"str literal"} return r as r1"""
        result = self.client.execute('LET r={a:1, b:true, c:"str literal"} return r as r1')

        self.assertEqual(result.size, 1)
        row = result.one()
        record = row["r1"].cast()

        self.assertEqual(record.get_value("a").cast(), 1)
        self.assertTrue(record.get_value("b").cast())
        self.assertEqual(record.get_value("c").cast(), "str literal")

    def test_const_vector_vector_result(self):
        """Test: LET r=VECTOR<3,FLOAT32>([1.0,2.0,3.0]) return r as r1"""
        result = self.client.execute('LET r=VECTOR<3,FLOAT32>([1.0,2.0,3.0]) return r as r1')

        self.assertEqual(result.size, 1)
        row = result.one()
        vector = row["r1"].cast()

        self.assertIsInstance(vector, NVector)
        self.assertEqual(vector.get_dimension(), 3)
        self.assertEqual(vector.get_values(), [1.0, 2.0, 3.0])

    def test_decode_node_result(self):
        """Test: use decode match(v) return v"""
        result = self.client.execute("use decode match(v) return v")

        self.assertEqual(result.size, 2)

        for row in result:
            node = row["v"].cast()
            node_type = node.get_type()

            if node_type == "player":
                props = node.get_properties()
                self.assertEqual(props["id"].cast(), 1)
                self.assertEqual(props["age"].cast(), 10)
                self.assertEqual(props["name"].cast(), "Tom")
                self.assertTrue(props["p_bool"].cast())
                self.assertEqual(props["p_float"].cast(), 1.0)
                self.assertEqual(props["p_double"].cast(), 2.0)
                self.assertEqual(props["p_date"].cast(), date(2024, 1, 1))
                self.assertEqual(props["p_datetime"].cast(), datetime(2024, 1, 1, 12, 1, 10))
                self.assertEqual(props["p_time"].cast(), time(10, 12, 13))
                self.assertEqual(len(props["p_list"].cast()), 2)
            elif node_type == "person":
                props = node.get_properties()
                self.assertEqual(props["id"].cast(), 2)
                self.assertEqual(props["high"].cast(), 20)

    def test_decode_edge_result(self):
        """Test: use decode match(v)-[e @friend]->(v1) return e"""
        result = self.client.execute("use decode match(v)-[e @friend]->(v1) return e")

        self.assertEqual(result.size, 1)
        row = result.one()
        edge = row["e"].cast()

        self.assertEqual(edge.get_type(), "friend")
        props = edge.get_properties()
        self.assertEqual(props["degree"].cast(), 11)

    def test_decode_path_result(self):
        """Test: use decode match p=(v)-[e @friend]->(v1) return p"""
        result = self.client.execute("use decode match p=(v)-[e @friend]->(v1) return p")

        self.assertEqual(result.size, 1)
        row = result.one()
        path = row["p"].cast()

        # Path should have 3 elements: node, edge, node
        self.assertEqual(len(path.get_values()), 3)

    def test_decode_string_result(self):
        """Test: let a="abcdefghij" for i in range(1,100) return a || cast(i as STRING) as c"""
        result = self.client.execute(
            'let a="abcdefghij" for i in range(1,100) return a || cast(i as STRING) as c'
        )

        self.assertEqual(result.size, 100)

        for i, row in enumerate(result, start=1):
            expected = f"abcdefghij{i}"
            self.assertEqual(row["c"].cast(), expected)

    def test_decode_date_result(self):
        """Test: let a="2024-01-01" for i in range(1,10) return date(a)"""
        result = self.client.execute('let a="2024-01-01" for i in range(1,10) return date(a)')

        self.assertEqual(result.size, 10)

        for row in result:
            self.assertEqual(row[0].cast(), date(2024, 1, 1))

    def test_decode_duration_result(self):
        """Test: return duration "P1Y" """
        result = self.client.execute('return duration "P1Y"')

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row[0].cast()

        self.assertIsInstance(value, NDuration)
        self.assertEqual(str(value), "P1Y")

    def test_decode_list_result(self):
        """Test: LET l=[0,1,2,3,4] RETURN l[0:2] AS a"""
        result = self.client.execute("LET l=[0,1,2,3,4] RETURN l[0:2] AS a")

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row["a"].cast()

        self.assertEqual(value[0].cast(), 0)
        self.assertEqual(value[1].cast(), 1)

    def test_decode_record_result(self):
        """Test: LET a1=1,b1=true,c1="str" return{a:a1, b:b1, c:c1} as r"""
        result = self.client.execute('LET a1=1,b1=true,c1="str" return{a:a1, b:b1, c:c1} as r')

        self.assertEqual(result.size, 1)
        row = result.one()
        record = row["r"].cast()

        self.assertEqual(record.get_value("a").cast(), 1)
        self.assertTrue(record.get_value("b").cast())
        self.assertEqual(record.get_value("c").cast(), "str")

    def test_decode_set_result(self):
        """Test: LET a=0,b=1,c=2 RETURN SET{a,b,c}"""
        result = self.client.execute("LET a=0,b=1,c=2 RETURN SET{a,b,c}")

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row[0]

        # Cast to set
        result_set = value.cast()
        self.assertIsInstance(result_set, set)
        self.assertEqual(len(result_set), 3)

        # Check that the set contains the expected values
        # Create expected ValueWrapper objects for comparison
        expected_values = {
            ValueWrapper(0, ColumnType.INT32),
            ValueWrapper(1, ColumnType.INT32),
            ValueWrapper(2, ColumnType.INT32),
        }

        # Check if all expected values are in the result set
        for expected in expected_values:
            self.assertTrue(expected in result_set, f"Expected {expected} to be in set")

    def test_decode_map_result(self):
        """Test: LET a1=1,b1=2,c1=3 return Map{'a':a1, 'b':b1, 'c':c1} as r"""
        result = self.client.execute("LET a1=1,b1=2,c1=3 return Map{'a':a1, 'b':b1, 'c':c1} as r")

        self.assertEqual(result.size, 1)
        row = result.one()
        value = row["r"]

        # Cast to dict
        result_map = value.cast()
        self.assertIsInstance(result_map, dict)
        self.assertEqual(len(result_map), 3)

        # Create expected keys
        key_a = ValueWrapper("a", ColumnType.STRING)
        key_b = ValueWrapper("b", ColumnType.STRING)
        key_c = ValueWrapper("c", ColumnType.STRING)

        # Check values
        self.assertEqual(result_map[key_a].cast(), 1)
        self.assertEqual(result_map[key_b].cast(), 2)
        self.assertEqual(result_map[key_c].cast(), 3)


if __name__ == "__main__":
    unittest.main()
