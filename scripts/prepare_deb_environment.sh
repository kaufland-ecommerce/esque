#!/bin/bash

# this script prepares all the files needed for dpkg-deb
configuration_path=$1
NAME="esque"
VERSION="1.0"   # default, if there is no __version__.py for some reason
if [ -f ../esque/__version__.py ]
then
  VERSION=`cat ../esque/__version__.py | cut -f2 -d= | sed 's/ //g' | sed 's/"//g'`
fi
# just in case
rm -rf $1
# create a new staging environment
mkdir --parent $1/$NAME_$VERSION-1/DEBIAN