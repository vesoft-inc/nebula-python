#!/usr/bin/env python

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from setuptools import setup, find_packages

setup(
    name='nebula2-python',
    version='2.0.0rc1',
    license="Apache 2.0 + Common Clause 1.0",
    author='vesoft-inc',
    author_email='info@vesoft.com',
    long_description='Python client for Nebula Graph V2.0',
    url='https://github.com/vesoft-inc/nebula-python',
    install_requires=['httplib2',
                      'future',
                      'six',
                      'futures; python_version == "2.7"'],
    packages=find_packages(),
    platforms=["2.7, 3.5, 3.7"],
    package_dir={'nebula2': 'nebula2',
                 'thirft': 'thirft'},
)
