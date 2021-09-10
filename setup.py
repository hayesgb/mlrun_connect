#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import versioneer

setup(
    name="mlrun_connect",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Utilities for connection mlrun to cloud services",
    url="https://github.com/hayesgb/mlrun_connect/",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
    ],
    maintainer="Greg Hayes",
    license="Apache 2.0",
    keywords=["mlrun", "iguazio", "nuclio", "azure"],
    packages=["mlrun_connect"],
    python_requires=">3.6",
    long_description_content_type="text/markdown",
    long_description=open("README.md").read() if exists("README.md") else "",
    install_requires=[
        "azure-identity",
        "azure-servicebus>=7.3.2",
        "mlrun",
        "pandas",
        "v3io_frames",
    ],
    tests_require=["pytest"],
    zip_safe=False,
)   