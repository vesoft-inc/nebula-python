#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from setuptools import setup, find_packages

setup(
    name='nebula2-python',
    version="0.0.1-4",
    license = "Apache 2.0 + Common Clause 1.0",
    description='Python client for Nebula Graph',
    url='https://github.com/vesoft-inc/nebula-python',
    author='darion.wang',
    author_email='darion.wang@vesoft-inc.com',
    install_requires=['httplib2',
                      'future; python_version == "2.7"',
                      'six',
                      'futures; python_version == "2.7"'],
    packages=find_packages(),
    platforms=["3.5, 3.7"],
    package_dir={'nebula2': 'nebula2',
                 'thirft': 'thirft'},
)
