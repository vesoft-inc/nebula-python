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
    version='3.5.1',
    license='Apache 2.0',
    author='vesoft-inc',
    author_email='info@vesoft.com',
    description='Python client for NebulaGraph V3.4',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/vesoft-inc/nebula-python',
    install_requires=[
        'httplib2 >= 0.20.0',
        'future >= 0.18.0',
        'six >= 1.16.0',
        'pytz >= 2021.1',
        'httpx[http2] >= 0.22.0',
    ],
    packages=find_packages(),
    platforms=['3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12'],
    package_dir={'nebula3': 'nebula3'},
)
