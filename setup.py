#!/usr/bin/env python

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import sys
from setuptools import setup, find_packages
from pathlib import Path

base_dir = Path(__file__).parent
long_description = (base_dir / "README.md").read_text()

requirements = [
    "httplib2 >= 0.20.0",
    "future >= 0.18.0",
    "six >= 1.16.0",
    "pytz >= 2021.1",
    "httpx[http2] >= 0.22.0",
]

if sys.version_info < (3, 7):
    # httpcore-->anyio-->contextvars when it's < 3.7
    # while setuptools doesn't handle the dependency well
    requirements.append("contextvars==2.4")

setup(
    name="nebula3-python",
    version="3.8.2",
    license="Apache 2.0",
    author="vesoft-inc",
    author_email="info@vesoft.com",
    description="Python client for NebulaGraph v3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vesoft-inc/nebula-python",
    install_requires=requirements,
    packages=find_packages(),
    platforms=["3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12"],
    package_dir={"nebula3": "nebula3"},
)
