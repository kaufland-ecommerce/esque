#!/bin/bash
export PYBUILD_DISABLE=test/python3
python3 setup.py sdist --formats=gztar
# the built archive is in dist, by default
archive_name=`ls dist | head -1`
py2dsc --with-python2=False --with-python3=True --dist-dir=./deb_final/ dist/${archive_name}
# build the actual package
deb_folder_name=`echo ${archive_name} | sed 's/.tar.gz//g'`
# copy pre/post inst/rem scripts to debian folder
cat ./installation/deb/instenv >> ./deb_final/${deb_folder_name}/debian/preinst
cat ./installation/deb/preinst >> ./deb_final/${deb_folder_name}/debian/preinst
cat ./installation/deb/instenv >> ./deb_final/${deb_folder_name}/debian/postinst
cat ./installation/deb/postinst >> ./deb_final/${deb_folder_name}/debian/postinst
cat ./installation/deb/instenv >> ./deb_final/${deb_folder_name}/debian/prerm
cat ./installation/deb/prerm >> ./deb_final/${deb_folder_name}/debian/prerm
cat ./installation/deb/instenv >> ./deb_final/${deb_folder_name}/debian/postrm
cat ./installation/deb/postrm >> ./deb_final/${deb_folder_name}/debian/postrm
cd deb_final/${deb_folder_name}
dpkg-buildpackage -rfakeroot -uc -us
# the resulting deb package is one directory up, in deb_final
cd ..
deb_package=`ls *.deb | head -1`
