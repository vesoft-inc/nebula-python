# nebula-python

This directory holds the Python API for Nebula Graph. It is used to connect with Nebula Graph 2.0.

## Before you start

Before you start, please read this section to choose the right branch for you. In branch v1.0, the API works only for Nebula Graph 1.0. In the master branch, the API works only for Nebula Graph 2.0.

## The directory structure

```text
|--nebula-python
    |
    |-- nebula2                       // client code
    |   |-- common
    |   |-- data
    |   |-- graph
    |   |-- meta
    |   |-- net                       // the net code for graph client
    |   |-- storage
    |   |-- Config.py                 // the pool config
    |   |__ Exception.py              // the define exception
    |
    |-- thrift                        // the thrift lib code
    |
    |-- examples
    |   |-- MultiThreadExample.py     // the multi thread example
    |   |__ SimpleExample.py          // the simple example
    |
    |-- tests                         // the test code
    |
    |-- setup.py                      // used to install or package
    |
    |__ README.md                     // the introduction of nebula2-python

```

## How to get nebula2-python

### Option one: clone from GitHub

- Clone from GitHub

```bash
git clone https://github.com/vesoft-inc/nebula-python.git
cd nebula-python
```

- Install

```python
sudo python3 setup.py install
```

When your environment cannot access `pypi`, you need to install the following packages manually.

- django-import-export
- future
- six
- httplib2
- futures   # python2.x is needed

### Option two: using pip

```python
pip install nebula2-python
```

## Quick example

```python
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

# define a config
config = Config()
config.max_connection_pool_size = 10
# init connection pool
connection_pool = ConnectionPool()
# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 3699)], config)

# get session from the pool
session = connection_pool.get_session('root', 'nebula')

# show hosts
result = session.execute('SHOW HOSTS')
print(result)

# release session
session.release()

# close the pool
connection_pool.close()
```

## How to choose nebula-python

| Nebula2-Python Version | NebulaGraph Version |
|---|---|
| 2.0.0.post1  | 2.0.0beta |

