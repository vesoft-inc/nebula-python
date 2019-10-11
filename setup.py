#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import sys
from setuptools import setup

from nebula import __version__

import urllib
thrift_file = 'https://raw.githubusercontent.com/vesoft-inc/nebula/master/src/interface/graph.thrift'
urllib.urlretrieve(thrift_file, "nebula/graph.thrift")

setup(
    name = 'nebula-client',
    version = __version__,
    description = 'Python client for Nebula Graph',
    url = 'github.com/vesoft-inc/nebula',
    author = 'darion.wang',
    author_email = 'darion.wang@vesoft-inc.com',
    packages=['nebula'],
    package_dir={'nebula': 'nebula'},
    package_data={'nebula': ['graph.thrift']},
)
