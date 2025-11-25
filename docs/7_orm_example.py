from datetime import date
from typing import Any, Dict

import rich
from pydantic import BaseModel, Field
from typing_extensions import Optional

from nebulagraph_python.client import NebulaClient
from nebulagraph_python.client._session import SessionConfig
from nebulagraph_python.orm import (
    AsJSON,
    AsNVector,
    GraphType,
    bind_edge,
    bind_node,
    edge_model,
    get_graph_type,
    node_model,
    upsert_edge_only_gql,
    upsert_gql,
)
from nebulagraph_python.py_data_types import NVector

# Create client
client = NebulaClient(
    hosts=["127.0.0.1:9669"],
    username="root",
    password="NebulaGraph01",
    session_config=SessionConfig(
        graph="movie",
    ),
)


# Option 1: Bind to a graph type

graph_type = get_graph_type(client)


class Movie(bind_node(graph_type, "Movie")):
    id: int
    name: str


class Actor(bind_node(graph_type, "Actor")):
    id: int
    name: str
    birth_date: Optional[date] = Field(alias="birthDate")


class Act(bind_edge(graph_type, "Act")):
    pass


for row in client.execute_py(
    "MATCH (u@Actor)-[e@Act]->(m@Movie) LIMIT 1 RETURN u, e, m"
).as_primitive_by_row():
    actor = Actor.from_primitive(row["u"])
    movie = Movie.from_primitive(row["m"])
    act = Act.from_primitive(row["e"])
    rich.print(actor.name, movie.name, act)

q = upsert_gql(Actor(id=1111, name="John Doe", birthDate=date(1990, 1, 1)))
print(q)
q = upsert_gql(
    Actor(id=1112, name="Jane Doe", birthDate=date(1990, 1, 1)),
    Act(),
    Movie(id=1112, name="Unpromised Land"),
)
print(q)
q = upsert_edge_only_gql(288290978140258405, Act(), 289271897131057153)
print(q)
q = upsert_edge_only_gql({"id": 1112}, Act(), {"id": 1112})
print(q)

# Option 2: Define a graph type


class Node1(node_model("Node1", primary_key=["id"])):
    class Metadata(BaseModel):
        some: int
        other: date

    id: int
    name: str
    metadata: AsJSON[Metadata]


class Node2(node_model("Node2", primary_key=["id"])):
    id: int
    name: str
    random_dict: AsJSON[Dict[str, Any]]


class Edge1(
    edge_model(
        "Node1",
        "Edge1",
        "Node2",
        labels=["Hello", "World"],
        multiedge_keys=["a", "b"],
    )
):
    a: int
    b: Optional[str]
    c: float = 1.0
    d: Optional[bool] = Field(alias="d_alias")
    vec: AsNVector[3] | None = Field(alias="vec_alias")


graph_type = GraphType.from_models(
    "define_type_test_type",
    [Node1, Node2, Edge1],
)

print(graph_type.to_gql())

if input("Execute the DDL? (y/N)") == "y":
    client.execute_py(graph_type.to_gql())
    client.execute_py("CREATE GRAPH IF NOT EXISTS define_type_test define_type_test_type")


q = upsert_gql(
    Node1(id=1, name="foo", metadata=Node1.Metadata(some=1, other=date(1970, 1, 1))),
    Edge1(a=1, b="bar", c=2.0, d_alias=True, vec_alias=NVector(values=[1, 2, 3])),
    Node2(
        id=2,
        name="bar",
        random_dict={"a": 1, "b": {"c": 2}},
    ),
)

print(q)

if input("Execute the UPSERT? (y/N)") == "y":
    client.execute_py("USE define_type_test " + q)

res = client.execute_py(
    "USE define_type_test MATCH (u@Node1)-[e@Edge1]->(v@Node2) RETURN u, e, v"
).one()
n1 = Node1.from_primitive(res["u"].cast_primitive())
e1 = Edge1.from_primitive(res["e"].cast_primitive())
n2 = Node2.from_primitive(res["v"].cast_primitive())

rich.print(n1, e1, n2)
