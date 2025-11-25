## ORM Guide

This guide shows how to use the lightweight ORM helpers to:
- bind Python models to an existing NebulaGraph schema
- define models and generate a graph type DDL
- serialize/deserialize records
- generate UPSERT GQL for nodes and edges

### Setup

```python
from datetime import date
from typing import Any, Dict
from pydantic import BaseModel, Field
from typing_extensions import Optional

from nebulagraph_python.client import NebulaClient, SessionConfig
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

client = NebulaClient(
    hosts=["127.0.0.1:9669"],
    username="root",
    password="NebulaGraph01",
    session_config=SessionConfig(graph="movie"),
)
```

### Option 1: Bind models to an existing graph type

Use `get_graph_type(client)` to fetch the current graph type, then bind nodes/edges with `bind_node`/`bind_edge`.

```python
graph_type = get_graph_type(client)

class Movie(bind_node(graph_type, "Movie")):
    id: int
    name: str

class Actor(bind_node(graph_type, "Actor")):
    id: int
    name: str
    # When a schema field name differs, use Field(alias=...)
    birth_date: Optional[date] = Field(alias="birthDate")

class Act(bind_edge(graph_type, "Act")):
    pass
```

Deserialize query results into your models:

```python
for row in client.execute_py(
    "MATCH (u@Actor)-[e@Act]->(m@Movie) LIMIT 1 RETURN u, e, m"
).as_primitive_by_row():
    actor = Actor.from_primitive(row["u"])  # node record -> model
    movie = Movie.from_primitive(row["m"])
    act = Act.from_primitive(row["e"])      # edge record -> model
```

Generate UPSERT GQL for nodes/edges:

```python
# Single node upsert
q1 = upsert_gql(Actor(id=1111, name="John Doe", birthDate=date(1990, 1, 1)))

# Triplet upsert (src node, edge, dst node)
q2 = upsert_gql(
    Actor(id=1112, name="Jane Doe", birthDate=date(1990, 1, 1)),
    Act(),
    Movie(id=1112, name="Unpromised Land"),
)

# Edge-only upsert with internal vertex ids
q3 = upsert_edge_only_gql(288290978140258405, Act(), 289271897131057153)

# Edge-only upsert with primary key objects
q4 = upsert_edge_only_gql({"id": 1112}, Act(), {"id": 1112})
```

Execute the generated GQL as needed:

```python
client.execute_py(q1)
client.execute_py(q2)
```

### Option 2: Define models and generate a graph type

Use `node_model` and `edge_model` to describe your schema in Python, then synthesize a `GraphType` and its DDL.

```python
class Node1(node_model("Node1", primary_key=["id"])):
    class Metadata(BaseModel):
        some: int
        other: date

    id: int
    name: str
    metadata: AsJSON[Metadata]  # JSON field backed by schema

class Node2(node_model("Node2", primary_key=["id"])):
    id: int
    name: str
    random_dict: AsJSON[Dict[str, Any]]

class Edge1(
    edge_model(
        "Node1",        # src label
        "Edge1",        # edge label
        "Node2",        # dst label
        labels=["Hello", "World"],
        multiedge_keys=["a", "b"],
    )
):
    a: int
    b: Optional[str]
    c: float = 1.0
    d: Optional[bool] = Field(alias="d_alias")
    vec: AsNVector[3] | None = Field(alias="vec_alias")
```

Create a graph type and view the DDL:

```python
graph_type = GraphType.from_models(
    "define_type_test_type",
    [Node1, Node2, Edge1],
)
print(graph_type.to_gql())
```

Example DDL usage:

```python
client.execute_py(graph_type.to_gql())
client.execute_py("CREATE GRAPH IF NOT EXISTS define_type_test define_type_test_type")
```

### UPSERT examples with defined models

```python
q = upsert_gql(
    Node1(id=1, name="foo", metadata=Node1.Metadata(some=1, other=date(1970, 1, 1))),
    Edge1(a=1, b="bar", c=2.0, d_alias=True, vec_alias=NVector(values=[1, 2, 3])),
    Node2(id=2, name="bar", random_dict={"a": 1, "b": {"c": 2}}),
)
print(q)

# Execute within the target graph
client.execute_py("USE define_type_test " + q)
```

Read back and deserialize:

```python
res = client.execute_py(
    "USE define_type_test MATCH (u@Node1)-[e@Edge1]->(v@Node2) RETURN u, e, v"
).one()

n1 = Node1.from_primitive(res["u"].cast_primitive())
e1 = Edge1.from_primitive(res["e"].cast_primitive())
n2 = Node2.from_primitive(res["v"].cast_primitive())
```

### Key concepts and helpers

- **bind_node / bind_edge**: Attach models to labels from an existing graph type.
- **node_model / edge_model**: Declare new node/edge models in Python; supports `primary_key` and edge `multiedge_keys`.
- **GraphType**: Build from models and emit DDL via `to_gql()`.
- **AsJSON[T]**: Strongly typed JSON column mapped to `T` (Pydantic `BaseModel` or structural type).
- **AsNVector[N]**: Fixed-length numeric vector field; use `NVector(values=[...])` at runtime.
- **Field(alias=...)**: Map Python attribute names to differing schema field names.
- **from_primitive(...)**: Convert Nebula primitive node/edge values to Python models.
- **upsert_gql(...) / upsert_edge_only_gql(...)**: Generate UPSERT statements from model instances.

### Notes

- Primary keys must match your schema. For edge-only upserts you can use internal vertex ids or primary key objects.
- Optional fields map naturally; use `Field(alias=...)` when schema names differ.
- Use `USE <graph>` to target the correct graph before executing generated GQL.
