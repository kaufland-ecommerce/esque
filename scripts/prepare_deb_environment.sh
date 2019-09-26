#!/bin/bash

# this script prepares all the files needed for dpkg-deb
NAME=$1
PROJECT_HOMEPAGE=$2
PROJECT_MAINTAINER=$3
PROJECT_DESCRIPTION=$4
DEB_STAGING_PATH=$5
SOURCE_CODE_PATH=$6
VERSION="1.0"   # default, if there is no __version__.py for some reason

# create a new staging environment
rm -rf ${DEB_STAGING_PATH}
CONTROL_DIRECTORY=${DEB_STAGING_PATH}/DEBIAN
mkdir --parent ${CONTROL_DIRECTORY}
if [ -f ./esque/__version__.py ]
then
  VERSION=`cat ./esque/__version__.py | cut -f2 -d= | sed 's/ //g' | sed 's/"//g'`
fi
for ctrlfile in `ls ${SOURCE_CODE_PATH}/installation/deb/`
do
  if [ "x${ctrlfile}" != "instenv" -a "x${ctrlfile}" != "control" ]
  then
    cat ${SOURCE_CODE_PATH}/installation/deb/instenv >> /tmp/tmpctrl
    cat ${SOURCE_CODE_PATH}/installation/deb/${ctrlfile} >> /tmp/tmpctrl
    mv /tmp/tmpctrl ${CONTROL_DIRECTORY}
  fi
done
mv ${SOURCE_CODE_PATH}/installation/deb/control ${CONTROL_DIRECTORY}/
rm -rf ${SOURCE_CODE_PATH}/installation/deb
sed -i 's,__LIBRARY__,'"${NAME}"',g' ${CONTROL_DIRECTORY}/control
sed -i 's,__MAINTAINER__,'"${PROJECT_MAINTAINER}"',g' ${CONTROL_DIRECTORY}/control
sed -i 's,__VERSION__,'"${VERSION}"',g' ${CONTROL_DIRECTORY}/control
sed -i 's,__HOMEPAGE__,'"${PROJECT_HOMEPAGE}"',g' ${CONTROL_DIRECTORY}/control
sed -i 's,__DESCRIPTION__,'"${PROJECT_DESCRIPTION}"',g' ${CONTROL_DIRECTORY}/control
cp -r ${SOURCE_CODE_PATH}/* ${DEB_STAGING_PATH}/
exit 0
