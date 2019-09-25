#!/bin/bash

# this script prepares all the files needed for dpkg-deb
NAME=$1
PROJECT_HOMEPAGE=$2
PROJECT_MAINTAINER=$3
PROJECT_DESCRIPTION=$4
DEB_STAGING_PATH=$5
SOURCE_CODE_PATH=$6
VERSION="1.0"   # default, if there is no __version__.py for some reason
if [ -f ../esque/__version__.py ]
then
  VERSION=`cat ../esque/__version__.py | cut -f2 -d= | sed 's/ //g' | sed 's/"//g'`
fi
# just in case
rm -rf $1
# create a new staging environment
FULL_PACKAGE_ROOT=${DEB_STAGING_PATH}/${NAME}-${VERSION}/DEBIAN
mkdir --parent ${FULL_PACKAGE_ROOT}/src
cp ${SOURCE_CODE_PATH}/installation/deb/* ${FULL_PACKAGE_ROOT}/
sed -i 's/__LIBRARY__/'${NAME}'/g' ${FULL_PACKAGE_ROOT}/control
sed -i 's/__MAINTAINER__/'${PROJECT_MAINTAINER}'/g' ${FULL_PACKAGE_ROOT}/control
sed -i 's/__VERSION__/'${VERSION}'/g' ${FULL_PACKAGE_ROOT}/control
sed -i 's/__HOMEPAGE__/'${PROJECT_HOMEPAGE}'/g' ${FULL_PACKAGE_ROOT}/control
sed -i 's/__DESCRIPTION__/'${PROJECT_DESCRIPTION}'/g' ${FULL_PACKAGE_ROOT}/control
cp ${SOURCE_CODE_PATH}/* ${FULL_PACKAGE_ROOT}/src/
cp -r ${SOURCE_CODE_PATH}/scripts ${FULL_PACKAGE_ROOT}/src/
cp -r ${SOURCE_CODE_PATH}/esque ${FULL_PACKAGE_ROOT}/src/
cp -r ${SOURCE_CODE_PATH}/tests ${FULL_PACKAGE_ROOT}/src/
# build the package
cd ${DEB_STAGING_PATH}
dpkg-deb --build ${NAME}-${VERSION}
