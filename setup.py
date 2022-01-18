#!/usr/bin/env python

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from setuptools import setup, find_packages
from pathlib import Path

base_dir = Path(__file__).parent
long_description = (base_dir / 'README.md').read_text()


setup(
    name='nebula3-python',
    version='3.0.0',
    license='Apache 2.0',
    author='vesoft-inc',
    author_email='info@vesoft.com',
    description='Python client for Nebula Graph V3.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/vesoft-inc/nebula-python',
    install_requires=['httplib2', 'future', 'six', 'pytz'],
    packages=find_packages(),
    platforms=['3.6, 3.7'],
    package_dir={'nebula3': 'nebula3'},
)
