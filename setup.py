#!/usr/bin/env python

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from setuptools import setup, find_packages
from pathlib import Path

base_dir = Path(__file__).parent
long_description = (base_dir / 'README.md').read_text()


setup(
    name='nebula2-python',
    version='2.6.1',
    license='Apache 2.0',
    author='vesoft-inc',
    author_email='info@vesoft.com',
    description='Python client for Nebula Graph V2.6',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/vesoft-inc/nebula-python',
    install_requires=['httplib2',
                      'future',
                      'six',
                      'pytz'],
    packages=find_packages(),
    platforms=['3.5, 3.7'],
    package_dir={'nebula2': 'nebula2'},
)
