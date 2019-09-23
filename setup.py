#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import os
import sys
from subprocess import call

from setuptools import find_packages, setup
from setuptools.command.install import install

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
    "pyyaml",
    "requests",
    "fastavro>=0.22.3",
    "avro-python3==1.8.2",
]

if sys.version_info < (3, 7, 0):
    required.append("dataclasses")


class InstallWithPostCommand(install):
    """Post-installation for installation mode."""

    def run(self):
        install.run(self)
        print("installing auto completion")
        call(["./scripts/auto_completion.sh"])


setup(
    name="esque",
    version=about["__version__"],
    description="esque - an operational kafka tool.",
    keywords="kafka commandline apache",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="real.digital",
    url="https://github.com/real-digital/esque",
    author_email="opensource@real-digital.de",
    packages=find_packages(exclude=["tests", "tests.*"]),
    entry_points={"console_scripts": ["esque=esque.cli.commands:esque"]},
    python_requires=">=3.6",
    setup_requires=[],
    install_requires=required,
    extras_require={
        "test": ["pytest", "pytest-mock", "pytest-cov"],
        "dev": ["black", "flake8", "beautifulsoup4", "requests"],
    },
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
    cmdclass={"install": InstallWithPostCommand},
)
