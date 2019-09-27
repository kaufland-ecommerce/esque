#!/bin/bash
export PYBUILD_DISABLE=test/python3
python3 setup.py sdist --formats=gztar
# the built archive is in dist, by default
archive_name=`ls dist | head -1`
py2dsc --with-python2=False --with-python3=True --dist-dir=./deb_final/ dist/${archive_name}
# build the actual package
dpkg-buildpackage -rfakeroot -uc -us
