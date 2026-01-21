#!/bin/bash
# run example program, using local source code and dependencies

export PYTHONPATH="${PYTHONPATH}:$(pwd)/src:$(pwd)/deps"

python3 example/NebulaPoolExample.py
