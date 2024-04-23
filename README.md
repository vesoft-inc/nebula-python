# NebulaGraph Python Client

[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)
[![pypi-version](https://img.shields.io/pypi/v/nebula3-python)](https://pypi.org/project/nebula3-python/)
[![python-version](https://img.shields.io/badge/python-3.6.2+%20|%203.7%20|%203.8%20|%203.9%20|%203.10%20|%203.11%20|%203.12-blue)](https://www.python.org/)

## Getting Started

**Note**: Ensure you are using the correct version, refer to the [Capability Matrix](#Compatibility-Matrix) for how the Python client version corresponds to the NebulaGraph Database version.

### Accessing NebulaGraph

- For **first-time** trying out Python client, go through [Quick Example: Connecting to GraphD Using Graph Client](#Quick-Example:-Connecting-to-GraphD-Using-Graph-Client).

- If your Graph Application is a **Web Service** dedicated to one Graph Space, go with Singleton of **Session Pool**, check [Using the Session Pool: A Guide](#Using-the-Session-Pool:-A-Guide).

- If you're building Graph Analysis Tools(Scan instead of Query), you may want to use the **Storage Client** to scan vertices and edges, see [Quick Example: Using Storage Client to Scan Vertices and Edges](#Quick-Example:-Using-Storage-Client-to-Scan-Vertices-and-Edges).

### Handling Query Results

- On how to form a query result into a **Pandas DataFrame**, see [Example: Fetching Query Results into a Pandas DataFrame](#Example:-Fetching-Query-Results-into-a-Pandas-DataFrame).

- On how to render/visualize the query result, see [Example: Extracting Edge and Vertex Lists from Query Results](#Example:-Extracting-Edge-and-Vertex-Lists-from-Query-Results), it demonstrates how to extract lists of edges and vertices from any query result by utilizing the `ResultSet.dict_for_vis()` method.

### Jupyter Notebook Integration

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/wey-gu/jupyter_nebulagraph/blob/main/docs/get_started.ipynb)


If you are about to access NebulaGraph within Jupyter Notebook, you may want to use the [NebulaGraph Jupyter Extension](https://pypi.org/project/jupyter-nebulagraph/), which provides a more interactive way to access NebulaGraph. See also this on Google Colab: [NebulaGraph on Google Colab](https://colab.research.google.com/github/wey-gu/jupyter_nebulagraph/blob/main/docs/get_started.ipynb).

## Obtaining nebula3-python

### Method 1: Installation via pip

```python
# for v3.x
pip install nebula3-python==$version
# for v2.x
pip install nebula2-python==$version
```

### Method 2: Installation via source

<details>
<summary>Click to expand</summary>

- Clone from GitHub

```bash
git clone https://github.com/vesoft-inc/nebula-python.git
cd nebula-python
```

- Install from source

> For python version >= 3.7.0

```bash
pip install .
```

> For python version >= 3.6.2, < 3.7.0

```bash
python3 setup.py install
```

</details>

## Quick Example: Connecting to GraphD Using Graph Client

```python
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# define a config
config = Config()
config.max_connection_pool_size = 10
# init connection pool
connection_pool = ConnectionPool()
# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

# option 1 control the connection release yourself
# get session from the pool
session = connection_pool.get_session('root', 'nebula')

# select space
session.execute('USE basketballplayer')

# show tags
result = session.execute('SHOW TAGS')
print(result)

# release session
session.release()

# option 2 with session_context, session will be released automatically
with connection_pool.session_context('root', 'nebula') as session:
    session.execute('USE basketballplayer')
    result = session.execute('SHOW TAGS')
    print(result)

# close the pool
connection_pool.close()
```

## Using the Session Pool: A Guide

The session pool is a collection of sessions that are managed by the pool. It is designed to improve the efficiency of session management and to reduce the overhead of session creation and destruction.

Session Pool comes with the following assumptions:

1. A space must already exist in the database prior to the initialization of the session pool.
2. Each session pool is associated with a single user and a single space to ensure consistent access control for the user. For instance, a user may possess different access permissions across various spaces. To execute queries in multiple spaces, consider utilizing several session pools.
3. Whenever `sessionPool.execute()` is invoked, the session executes the query within the space specified in the session pool configuration.
4. It is imperative to avoid executing commands through the session pool that would alter passwords or remove users.

For more details, see [SessionPoolExample.py](example/SessionPoolExample.py).

## Example: Extracting Edge and Vertex Lists from Query Results

For graph visualization purposes, the following code snippet demonstrates how to effortlessly extract lists of edges and vertices from any query result by utilizing the `ResultSet.dict_for_vis()` method.

```python
result = session.execute(
    'GET SUBGRAPH WITH PROP 2 STEPS FROM "player101" YIELD VERTICES AS nodes, EDGES AS relationships;')

data_for_vis = result.dict_for_vis()
```

Then, we could pass the `data_for_vis` to a front-end visualization library such as `vis.js`, `d3.js` or Apache ECharts. There is an example of Apache ECharts in [exapmple/apache_echarts.html](example/apache_echarts.html).

The dict/JSON structure with `dict_for_vis()` is as follows:

<details>
  <summary>Click to expand</summary>

```json
{
    'nodes': [
        {
            'id': 'player100',
            'labels': ['player'],
            'props': {
                'name': 'Tim Duncan',
                'age': '42',
                'id': 'player100'
            }
        },
        {
            'id': 'player101',
            'labels': ['player'],
            'props': {
                'age': '36',
                'name': 'Tony Parker',
                'id': 'player101'
            }
        }
    ],
    'edges': [
        {
            'src': 'player100',
            'dst': 'player101',
            'name': 'follow',
            'props': {
                'degree': '95'
            }
        }
    ],
    'nodes_dict': {
        'player100': {
            'id': 'player100',
            'labels': ['player'],
            'props': {
                'name': 'Tim Duncan',
                'age': '42',
                'id': 'player100'
            }
        },
        'player101': {
            'id': 'player101',
            'labels': ['player'],
            'props': {
                'age': '36',
                'name': 'Tony Parker',
                'id': 'player101'
            }
        }
    },
    'edges_dict': {
        ('player100', 'player101', 0, 'follow'): {
            'src': 'player100',
            'dst': 'player101',
            'name': 'follow',
            'props': {
                'degree': '95'
            }
        }
    },
    'nodes_count': 2,
    'edges_count': 1
}
```

</details>

## Example: Fetching Query Results into a Pandas DataFrame

> For `nebula3-python>=3.6.0`:

Assuming you have pandas installed, you can use the following code to fetch query results into a pandas DataFrame:

```bash
pip3 install pandas
```

```python
result = session.execute('<your query>')
df = result.as_data_frame()
```

<details>
  <summary>For `nebula3-python<3.6.0`:</summary>

```python
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import pandas as pd
from typing import Dict
from nebula3.data.ResultSet import ResultSet

def result_to_df(result: ResultSet) -> pd.DataFrame:
    """
    build list for each column, and transform to dataframe
    """
    assert result.is_succeeded()
    columns = result.keys()
    d: Dict[str, list] = {}
    for col_num in range(result.col_size()):
        col_name = columns[col_num]
        col_list = result.column_values(col_name)
        d[col_name] = [x.cast() for x in col_list]
    return pd.DataFrame(d)

# define a config
config = Config()

# init connection pool
connection_pool = ConnectionPool()

# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

# option 2 with session_context, session will be released automatically
with connection_pool.session_context('root', 'nebula') as session:
    session.execute('USE <your graph space>')
    result = session.execute('<your query>')
    df = result_to_df(result)
    print(df)

# close the pool
connection_pool.close()

```

</details>

## Quick Example: Using Storage Client to Scan Vertices and Edges

Storage Client enables you to scan vertices and edges from the storage service instead of the graph service w/ nGQL/Cypher. This is useful when you need to scan a large amount of data.

<details>
  <summary>Click to expand</summary>

You should make sure the scan client can connect to the address of storage which see from `SHOW HOSTS`

```python
from nebula3.mclient import MetaCache, HostAddr
from nebula3.sclient.GraphStorageClient import GraphStorageClient

# the metad servers's address
meta_cache = MetaCache([('172.28.1.1', 9559),
                        ('172.28.1.2', 9559),
                        ('172.28.1.3', 9559)],
                       50000)

# option 1 metad usually discover the storage address automatically
graph_storage_client = GraphStorageClient(meta_cache)

# option 2 manually specify the storage address
storage_addrs = [HostAddr(host='172.28.1.4', port=9779),
                 HostAddr(host='172.28.1.5', port=9779),
                 HostAddr(host='172.28.1.6', port=9779)]
graph_storage_client = GraphStorageClient(meta_cache, storage_addrs)

resp = graph_storage_client.scan_vertex(
        space_name='ScanSpace',
        tag_name='person')
while resp.has_next():
    result = resp.next()
    for vertex_data in result:
        print(vertex_data)

resp = graph_storage_client.scan_edge(
    space_name='ScanSpace',
    edge_name='friend')
while resp.has_next():
    result = resp.next()
    for edge_data in result:
        print(edge_data)
```

</details>

See [ScanVertexEdgeExample.py](example/ScanVertexEdgeExample.py) for more details.

## Compatibility Matrix

| Nebula-Python Version | Compatible NebulaGraph Versions | Notes                                                      |
| --------------------- | ------------------------------- | ---------------------------------------------------------- |
| 3.5.1                 | 3.x                             | Highly recommended. Latest release for NebulaGraph 3.x series. |
| master                | master                          | Includes recent changes. Not yet released.                 |
| 3.0.0 ~ 3.5.0         | 3.x                             | Compatible with any released version within the NebulaGraph 3.x series. |
| 2.6.0                 | 2.6.0, 2.6.1                    |                                                            |
| 2.5.0                 | 2.5.0                           |                                                            |
| 2.0.0                 | 2.0.0, 2.0.1                    |                                                            |
| 1.0                   | 1.x                             |                                                            |

## Directory Structure Overview

```text
.
└──nebula-python
    │
    ├── nebula3                               // client source code
    │   ├── fbthrift                          // the RPC code generated from thrift protocol
    │   ├── common
    │   ├── data
    │   ├── graph
    │   ├── meta
    │   ├── net                               // the net code for graph client
    │   ├── storage                           // the storage client code
    │   ├── Config.py                         // the pool config
    │   └── Exception.py                      // the exceptions
    │
    ├── examples
    │   ├── FormatResp.py                     // the format response example
    │   ├── SessionPoolExample.py             // the session pool example
    │   ├── GraphClientMultiThreadExample.py  // the multi thread example
    │   ├── GraphClientSimpleExample.py       // the simple example
    │   └── ScanVertexEdgeExample.py          // the scan vertex and edge example(storage client)
    │
    ├── tests                                 // the test code
    │
    ├── setup.py                              // used to install or package
    │
    └── README.md                             // the introduction of nebula3-python

```


## Contribute to Nebula-Python

<details>
<summary>Click to expand</summary>

To contribute, start by [forking](https://github.com/vesoft-inc/nebula-python/fork) the repository. Next, clone your forked repository to your local machine. Remember to substitute `{username}` with your actual GitHub username in the URL below:

```bash
git clone https://github.com/{username}/nebula-python.git
cd nebula-python
```
For package management, we utilize [PDM](https://github.com/pdm-project/pdm). Please begin by installing it:

```bash
pipx install pdm
```

Visit the [PDM documentation](https://pdm-project.org) for alternative installation methods.

Install the package and all dev dependencies:

```bash
pdm install
```

Make sure the Nebula server in running, then run the tests with pytest:

```bash
pdm test
```

Using the default formatter with [black](https://github.com/psf/black).

Please run `pdm fmt` to format python code before submitting.

See [How to contribute](https://github.com/vesoft-inc/nebula-community/blob/master/Contributors/how-to-contribute.md) for the general process of contributing to Nebula projects.

</details>

