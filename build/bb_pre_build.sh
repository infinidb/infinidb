#!/bin/bash

prefix="$1"

if [ -z "$prefix" ]; then
	echo "usage: $0 destdir" 1>&2
	exit 1
fi

rm -rf infinidb mysql infinidb-ent

git clone http://srvengcm1.calpont.com/repos/infinidb.git
cd infinidb
git checkout develop
cd ..

git clone https://github.com/infinidb/mysql.git
cd mysql
git checkout develop
cd ..

git clone http://srvengcm1.calpont.com/repos/infinidb-ent.git
cd infinidb-ent
git checkout develop
cd ..

cd infinidb
cp -r utils/autoconf/* .

mkdir -p export/{include,lib,etc,share,bin,sbin,post}

autoreconf

./configure --prefix=$prefix

grep -Eqs 'CentOS release 6' /etc/redhat-release
rc1=$?
lsb_release -a 2>/dev/null | grep -Eqs squeeze
rc2=$?
if [ $rc1 -eq 0 -o $rc2 -eq 0 ]; then
	grep -Eqs 'VERSION=1.5.22' libtool
	if [ $? -eq 0 ]; then
		cp -f /usr/bin/libtool .
	fi
fi

