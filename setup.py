#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from setuptools import setup, find_packages

setup(
    name='nebula-python',
    version="1.0.0-rc4",
    description='Python client for Nebula Graph',
    url='https://github.com/vesoft-inc/nebula-python',
    author='darion.wang',
    author_email='darion.wang@vesoft-inc.com',
    install_requires=['httplib2',
                      'future',
                      'six',
                      'futures; python_version == "2.7"'],
    packages=find_packages(),
    platforms=["2.7, 3.5, 3.7"],
    package_dir={'nebula': 'nebula',
                 'thirft': 'thirft',
                 'common': 'common',
                 'graph': 'graph'},
)
