#!/bin/bash

mysqldir=mysql-5.1.39

prefix=/usr/local
withdebug=
for arg in "$@"; do
	if [ $(expr -- "$arg" : '--prefix=') -eq 9 ]; then
		prefix="$(echo $arg | awk -F= '{print $2}')"
	elif [ $(expr -- "$arg" : '--with-debug') -eq 12 ]; then
		withdebug=--with-debug
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

if [ ! -d $mysqldir ]; then
	echo "Didn't find required MySQL install directory: $mysqldir" 1>&2
	exit 1
fi

cd $mysqldir

if [ ! -d ../dbcon/mysql ]; then
	echo "Didn't find required MySQL patch source directory: ../dbcon/mysql" 1>&2
	exit 1
fi

for file in configure.in; do
	cp ../dbcon/mysql/$file .
done

pushd ../dbcon/mysql >/dev/null
filelist=$(ls -1 *.cc *.h *.yy | sed -e '/^ha_/d' -e '/^idb_/d')
popd >/dev/null
for file in $filelist; do
	cp ../dbcon/mysql/$file sql
done

autoreconf --force --install
./configure --prefix="$prefix/Calpont/mysql" $withdebug --without-libedit --with-readline --with-client-ldflags="-Wl,-rpath -Wl,/usr/local/Calpont/mysql/lib/mysql"

make || exit 1
make install

cp ../dbcon/mysql/my.cnf $prefix/Calpont/mysql

for dir in \
docs \
man \
mysql-test \
sql-bench
do
	rm -rf $prefix/Calpont/mysql/$dir
done
rm -rf $prefix/Calpont/mysql/share/man

