#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import os
import sys

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = "\n" + f.read()

about = {}

with open(os.path.join(here, "esque", "__version__.py")) as f:
    exec(f.read(), about)

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist bdist_wheel upload")
    sys.exit()

required = [
    "pip>=9.0.1",
    "setuptools>=36.2.1",
    "virtualenv",
    "confluent-kafka",
    "click>=7.0",
    "pykafka",
    "pendulum",
]

setup(
    name="esque",
    version=about["__version__"],
    description="A usable kafka tool.",
    keywords="kafka commandline apache",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="real.digital",
    url="https://github.com/real-digital/esque",
    author_email="opensource@real-digital.de",
    packages=find_packages(exclude=["tests", "tests.*"]),
    entry_points={"console_scripts": ["esque=esque.cli.commands:esque"]},
    package_data={"config": ["sample_config.cfg"]},
    python_requires=">=3.6",
    setup_requires=[],
    install_requires=required,
    extras_require={"test": ["pytest", "pytest-mock"], "dev": ["black", "flake8"]},
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
