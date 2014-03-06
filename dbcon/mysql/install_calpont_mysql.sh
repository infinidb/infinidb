#!/bin/bash
#
# $Id$
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

$installdir/mysql/bin/mysql --defaults-file=$df --force --user=root $pwprompt mysql 2>/tmp/mysql_install.log <<EOD
INSTALL PLUGIN infinidb SONAME 'libcalmysql.so';
-- these are deprecated names
DELETE FROM mysql.func WHERE name='caldisablepartition';
DELETE FROM mysql.func WHERE name='caldroppartition';
DELETE FROM mysql.func WHERE name='calenablepartition';
DELETE FROM mysql.func WHERE name='caldisablepartitions';
DELETE FROM mysql.func WHERE name='calenablepartitions';
DELETE FROM mysql.func WHERE name='caldroppartitions';
DELETE FROM mysql.func WHERE name='calshowpartitions';
DELETE FROM mysql.func WHERE name='caldroppartitionsbyvalue';
DELETE FROM mysql.func WHERE name='caldisablepartitionsbyvalue';
DELETE FROM mysql.func WHERE name='calenablepartitionsbyvalue';
DELETE FROM mysql.func WHERE name='calshowpartitionsbyvalue';
-- these functions moved libs
DROP FUNCTION caldisablepartitions;
DROP FUNCTION calenablepartitions;
DROP FUNCTION caldroppartitions;
DROP FUNCTION calshowpartitions;
DROP FUNCTION caldroppartitionsbyvalue;
DROP FUNCTION caldisablepartitionsbyvalue;
DROP FUNCTION calenablepartitionsbyvalue;
DROP FUNCTION calshowpartitionsbyvalue;
CREATE FUNCTION calgetstats RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calsettrace RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calsetparms RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calflushcache RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calgettrace RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calgetversion RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calonlinealter RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION calviewtablelock RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calcleartablelock RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION caldisablepartitions RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calenablepartitions RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION caldroppartitions RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calshowpartitions RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION callastinsertid RETURNS INTEGER SONAME 'libcalmysql.so';
CREATE FUNCTION caldroppartitionsbyvalue RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION caldisablepartitionsbyvalue RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calenablepartitionsbyvalue RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION calshowpartitionsbyvalue RETURNS STRING SONAME 'libcalmysql.so';
CREATE FUNCTION idbpm RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbdbroot RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbsegment RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbsegmentdir RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbextentrelativerid RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbblockid RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbextentid RETURNS INTEGER soname 'libcalmysql.so';
CREATE FUNCTION idbextentmin RETURNS STRING soname 'libcalmysql.so';
CREATE FUNCTION idbextentmax RETURNS STRING soname 'libcalmysql.so';
CREATE FUNCTION idbpartition RETURNS STRING soname 'libcalmysql.so';
CREATE FUNCTION idblocalpm RETURNS INTEGER soname 'libcalmysql.so';

CREATE DATABASE IF NOT EXISTS infinidb_vtable;
CREATE DATABASE IF NOT EXISTS infinidb_querystats;
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

$installdir/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$installdir/mysql/syscatalog_mysql.sql
$installdir/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$installdir/mysql/calsetuserpriority.sql
$installdir/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$installdir/mysql/calremoveuserpriority.sql
$installdir/mysql/bin/mysql --defaults-file=$df --user=root $pwprompt mysql 2>/dev/null <$installdir/mysql/calshowprocesslist.sql

sed -i 's/infinidb_compression_type=1/infinidb_compression_type=2/' $installdir/mysql/my.cnf >/dev/null 2>&1

