# Note on code gen

When the proto files are updated, you need to run the following commands to generate the python code.

```bash
pdm sync
pdm run proto_gen
```

## Resolve common.proto conflict with other projects(e.g. pymilvus)

> This needs to be done every time we update the proto files.

1. Rename common.proto to nebula_common.proto
2. Rename common.proto in vector.proto and graph.proto to nebula_common.proto

Which is implemented in `proto/scripts/update_proto.py`.