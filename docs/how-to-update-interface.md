# How to update the gen file when the thrift file had change in repo `https://github.com/vesoft-inc/nebula`
## generate graph sync interface

```
/opt/vesoft/third-party/bin/thrift1 --strict --allow-neg-enum-vals --gen "py" -o . graph.thrift
```

## generate graph async interface

Only use the **GraphService.py**, and modify the file name to **AsyncGraphService.py**, and copy it into `graph directory`

```
/opt/vesoft/third-party/bin/thrift1 --strict --allow-neg-enum-vals --gen "py:asyncio" -o . graph.thrift
```
