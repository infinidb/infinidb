#!/bin/bash
#

prefix=/usr/local
installdir=$prefix/Calpont
rpmmode=install
pwprompt=" "

for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/Calpont
	elif [ `expr -- "$arg" : '--rpmmode='` -eq 10 ]; then
		rpmmode="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
		password="`echo $arg | awk -F= '{print $2}'`"
		pwprompt="--password=$password"
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

df=$installdir/mysql/my.cnf

# N.B.: mysqld is not running when this script is called by post-mysql-install!

