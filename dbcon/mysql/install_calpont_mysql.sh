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
CREATE DATABASE IF NOT EXISTS querystats;
CREATE TABLE IF NOT EXISTS infinidb_querystats.querystats
(
  queryID bigint NOT NULL AUTO_INCREMENT,
  sessionID bigint DEFAULT NULL,
  host varchar(50),
  user varchar(50),
  priority char(20),
  queryType char(25),
  query varchar(8000),
  startTime timestamp NOT NULL,
  endTime timestamp NOT NULL,
  rows bigint,
  errno int,
  phyIO bigint,
  cacheIO bigint,
  blocksTouched bigint,
  CPBlocksSkipped bigint,
  msgInUM bigint,
  msgOutUm bigint,
  maxMemPct int,
  blocksChanged bigint,
  numTempFiles bigint,
  tempFileSpace bigint,
  PRIMARY KEY (queryID)
);

CREATE TABLE IF NOT EXISTS infinidb_querystats.user_priority
(
  host varchar(50),
  user varchar(50),
  priority char(20)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE IF NOT EXISTS infinidb_querystats.priority
(
  priority char(20) primary key,
  priority_level int
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

insert ignore into infinidb_querystats.priority values ('High', 100),('Medium', 66), ('Low', 33);
EOD

$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$prefix/Calpont/mysql/syscatalog_mysql.sql
$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$prefix/Calpont/mysql/calsetuserpriority.sql
$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$prefix/Calpont/mysql/calremoveuserpriority.sql
$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$prefix/Calpont/mysql/calshowprocesslist.sql
