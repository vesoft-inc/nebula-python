## Scan all nodes and edges

This guide shows how to iterate through every node and edge type in a graph and stream all records using `scan_nodes` and `scan_edges`.

### Prerequisites
- A running NebulaGraph service and an accessible graph space (e.g., `movie`).
- Valid credentials and network access to the graph service.

### Example: scan everything in a space

```python
from nebulagraph_python.client import NebulaClient
from nebulagraph_python.tools import scan_edges, scan_nodes
from nebulagraph_python.tools.get_graph_type import get_graph_type
from nebulagraph_python.tools.session_conf import get_graph_type_name


# Create client
client = NebulaClient(
    addresses="127.0.0.1:9669",
    user_name="root",
    password="nebula",
    connect_timeout_ms=3000,
    request_timeout_ms=30000,
)

# Discover schema metadata for the target graph
graph_type_name = get_graph_type_name(client, graph_name="movie")
graph_type = get_graph_type(client, graph_type_name=graph_type_name)

# Scan all nodes (by type)
for node_type_name, node_type in graph_type.nodes.items():
    for count, node in enumerate(
        scan_nodes(
            client,
            graph_name="movie",
            type_name=node_type_name,
            properties_list=list(node_type.properties.keys()),
            batch_size=10,  # adjust as needed
        )
    ):
        print("node", node_type_name, count, node)

# Scan all edges (by type)
for edge_type_name, edge_type in graph_type.edges.items():
    for count, (src, edge, dst) in enumerate(
        scan_edges(
            client,
            graph_name="movie",
            type_name=edge_type_name,
            properties_list=list(edge_type.properties.keys()),
        )
    ):
        print("edge", edge_type_name, count, src, edge, dst)

# Close the client
client.close()
```

### Notes
- **`properties_list`**: provide the properties you want to fetch. Using `list(node_type.properties.keys())` pulls all properties for that type.
- **`batch_size`**: controls page size when scanning nodes; tune it based on data volume and memory.
- The edge scan yields tuples `(src, edge, dst)` describing the full relation.
- Remember to close the client when done to release resources.
