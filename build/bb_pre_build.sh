#!/bin/bash

prefix="$1"

if [ -z "$prefix" ]; then
	echo "usage: $0 destdir" 1>&2
	exit 1
fi

rm -rf infinidb mysql infinidb-ent

git clone http://srvgip1.calpont.com/repos/infinidb.git
cd infinidb
git checkout develop
cd ..

git clone https://github.com/infinidb/mysql.git
cd mysql
git checkout develop
cd ..

git clone http://srvgip1.calpont.com/repos/infinidb-ent.git
cd infinidb-ent
git checkout develop
cd ..

cd infinidb

mkdir -p export/{include,lib,etc,share,bin,sbin,post}

cp -r utils/autoconf/* .
autoreconf
libtoolize --force --install

./configure --prefix=$prefix

