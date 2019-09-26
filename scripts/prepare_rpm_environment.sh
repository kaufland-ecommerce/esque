#!/bin/bash

# this script prepares all the files needed for dpkg-deb
NAME=$1
PROJECT_HOMEPAGE=$2
PROJECT_MAINTAINER=$3
PROJECT_DESCRIPTION=$4
RPM_STAGING_PATH=$5
SOURCE_CODE_PATH=$6
VERSION="1.0"   # default, if there is no __version__.py for some reason

# create a new staging environment
rm -rf ${RPM_STAGING_PATH}
mkdir --parent ${RPM_STAGING_PATH}
if [ -f ./esque/__version__.py ]
then
  VERSION=`cat ./esque/__version__.py | cut -f2 -d= | sed 's/ //g' | sed 's/"//g'`
fi
cp -r ${SOURCE_CODE_PATH}/* ${RPM_STAGING_PATH}/
echo "%pre" >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/instenv >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/preinst >> ${RPM_STAGING_PATH}/.spec
echo "" >> ${RPM_STAGING_PATH}/.spec
echo "%post" >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/instenv >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/postinst >> ${RPM_STAGING_PATH}/.spec
echo "" >> ${RPM_STAGING_PATH}/.spec
echo "%preun" >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/instenv >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/prerm >> ${RPM_STAGING_PATH}/.spec
echo "" >> ${RPM_STAGING_PATH}/.spec
echo "%postun" >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/instenv >> ${RPM_STAGING_PATH}/.spec
cat ${SOURCE_CODE_PATH}/installation/rpm/postrm >> ${RPM_STAGING_PATH}/.spec
exit 0
