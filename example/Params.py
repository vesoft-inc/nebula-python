import time
from typing import Any, Dict, List

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from nebula3.common import ttypes
from nebula3.data.ResultSet import ResultSet

# define a config
config = Config()
connection_pool = ConnectionPool()
connection_pool.init([("127.0.0.1", 9669)], config)

# get session from the connection pool
client = connection_pool.get_session("root", "nebula")
client.execute("CREATE SPACE IF NOT EXISTS test(vid_type=FIXED_STRING(30));")


time.sleep(
    6
)  # two cycles of heartbeat, by default of a NebulaGraph cluster, we will need to sleep 20s

client.execute(
    "USE test;"
    "CREATE TAG IF NOT EXISTS person(name string, age int);"
    "CREATE EDGE IF NOT EXISTS like (likeness double);"
)

# prepare NebulaGraph Byte typed parameters

bval = ttypes.Value()
bval.set_bVal(True)
ival = ttypes.Value()
ival.set_iVal(3)
sval = ttypes.Value()
sval.set_sVal("Bob")

params = {"p1": ival, "p2": bval, "p3": sval}


# we could pass NebulaGraph Raw byte params like params, they will be evaluated in server side:
resp = client.execute_parameter(
    "RETURN abs($p1)+3 AS col1, (toBoolean($p2) AND false) AS col2, toLower($p3)+1 AS col3",
    params,
)

# It may be not dev friendly to prepare i.e. a list of string typed params, actually NebulaGrap python client supports to pass premitive typed parms, too.

params_premitive = {
    "p1": 3,
    "p2": True,
    "p3": "Bob",
    "p4": ["Bob", "Lily"],
}

resp = client.execute_py(
    "RETURN abs($p1)+3 AS col1, (toBoolean($p2) and false) AS col2, toLower($p3)+1 AS col3",
    params_premitive,
)
resp = client.execute_py(
    "MATCH (v) WHERE id(v) in $p4 RETURN id(v) AS vertex_id",
    params_premitive,
)
