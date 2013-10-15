#! /bin/sh
#

version=$1
OS=x86_64

cd /home/nightly/rpm/ > /dev/null 2>&1
cd RPMS/$OS/ > /dev/null 2>&1
test -f infinidb-plafform-$version*.rpm || echo "no rpm"
test -f  infinidb-plafform-$version*.rpm || exit -1
rpm -qi -p  infinidb-plafform-$version*.rpm >  infinidb-plafform-$OS-rpm-info.txt
echo " " >>  infinidb-plafform-$OS-rpm-info.txt
echo "MD5SUM" >>  infinidb-plafform-$OS-rpm-info.txt
md5sum  infinidb-plafform-$version*.rpm >>  infinidb-plafform-$OS-rpm-info.txt
echo " " >>  infinidb-plafform-$OS-rpm-info.txt
#
test -f infinidb-storage-engine-*.rpm || echo "no rpm"
test -f  infinidb-storage-engine-*.rpm || exit -1
rpm -qi -p  infinidb-storage-engine-*.rpm >  infinidb-storage-engine-$OS-rpm-info.txt
echo " " >>  infinidb-storage-engine-$OS-rpm-info.txt
echo "MD5SUM" >>  infinidb-storage-engine-$OS-rpm-info.txt
md5sum  infinidb-storage-engine-*.rpm >>  infinidb-storage-engine-$OS-rpm-info.txt
echo " " >>  infinidb-storage-engine-$OS-rpm-info.txt
#
test -f infinidb-mysql-*.rpm || echo "no rpm"
test -f  infinidb-mysql-*.rpm || exit -1
rpm -qi -p  infinidb-mysql-*.rpm >  infinidb-mysql-$OS-rpm-info.txt
echo " " >>  infinidb-mysql-$OS-rpm-info.txt
echo "MD5SUM" >>  infinidb-mysql-$OS-rpm-info.txt
md5sum  infinidb-mysql-*.rpm >>  infinidb-mysql-$OS-rpm-info.txt
echo " " >>  infinidb-mysql-$OS-rpm-info.txt
#
