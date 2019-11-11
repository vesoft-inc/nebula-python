#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import sys
from setuptools import setup

setup(
    name = 'nebula-client',
    version = "rc2",
    description = 'Python client for Nebula Graph',
    url = 'https://github.com/vesoft-inc/nebula-python',
    author = 'darion.wang',
    author_email = 'darion.wang@vesoft-inc.com',
    install_requires = ['gevent', 'future', 'futures', 'six', 'gevent']
    packages=['nebula'],
    package_dir={'nebula': 'nebula'},
)
