#!/usr/bin/env python

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from setuptools import setup, find_packages

setup(
    name='nebula2-python',
    version='2.5.0',
    license="Apache 2.0 + Common Clause 1.0",
    author='vesoft-inc',
    author_email='info@vesoft.com',
    long_description='Python client for Nebula Graph V2.0',
    url='https://github.com/vesoft-inc/nebula-python',
    install_requires=['httplib2',
                      'future',
                      'six',
                      'pytz'],
    packages=find_packages(),
    platforms=["3.5, 3.7"],
    package_dir={'nebula2': 'nebula2'},
)
