## Update RPC interface

NebulaGraph 3 uses thrift to define the RPC interface. The interface files are in the `src/interface` directory of the NebulaGraph repository. The interface files are used to generate the RPC interface code in different languages.
How to update generated files when the thrift file changes in the repository `https://github.com/vesoft-inc/nebula`:

- Download the thrift binary from OSS, which was built on Fedora30:

```bash
wget https://oss-cdn.nebula-graph.com.cn/fbthrift_bin/thrift1
```

- Utilize the `thrift1` binary along with the thrift files located at `https://github.com/vesoft-inc/nebula/tree/master/src/interface` to generate the interface files.

```bash
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . common.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . graph.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . meta.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . storage.thrift
```