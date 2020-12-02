# nebula-python

This directory provides Nebula client API in Python. It used to connect NebulaGraph2.0.


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
    |__ README.md                     // the introduce of nebula2-python

```

## How to get nebula2-python

### Option One: cloning from GitHub

- Cloning

```bash
git clone https://github.com/vesoft-inc/nebula-python.git
cd nebula-python
```

- Install

```python
sudo python3 setup.py install
```

When your environment cannot access `pypi`, you need to manually install the following packages.

- django-import-export
- future
- six
- httplib2
- futures   # python2.x is needed

### Option Two: using pip

```python
pip install nebula2-python
```

## Quick Example
  
```python
from nebula2.net import ConnectionPool
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

# release session
session.release()

# close the pool
connection_pool.close()
```


## How to choose nebula-python

| Nebula2-Python Version | NebulaGraph Version |
|---|---|
| 2.0.0.post1  | 2.0.0beta |

