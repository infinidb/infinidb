#!/bin/bash
#
# $Id$
#

prefix=/usr/local
rpmmode=install
pwprompt=" "
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--rpmmode='` -eq 10 ]; then
		rpmmode="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
		password="`echo $arg | awk -F= '{print $2}'`"
		pwprompt="--password=$password"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

df=$prefix/Calpont/mysql/my.cnf

$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --force --user=root $pwprompt mysql 2>/dev/null <<EOD
INSTALL PLUGIN infinidb SONAME 'libcalmysql.so';
CREATE FUNCTION calgetstats RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calsettrace RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calsetparms RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calflushcache RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calgettrace RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calgetversion RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calonlinealter RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calviewtablelock RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calcleartablelock RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION callastinsertid RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE DATABASE IF NOT EXISTS infinidb_vtable; 
EOD

$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$prefix/Calpont/mysql/syscatalog_mysql.sql
