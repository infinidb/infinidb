INSTALL PLUGIN infinidb SONAME 'libcalmysql.dll';
CREATE FUNCTION calgetstats RETURNS STRING SONAME 'libcalmysql.dll';
CREATE FUNCTION calsettrace RETURNS INTEGER SONAME 'libcalmysql.dll';
CREATE FUNCTION calsetparms RETURNS STRING SONAME 'libcalmysql.dll';
CREATE FUNCTION calflushcache RETURNS INTEGER SONAME 'libcalmysql.dll';
CREATE FUNCTION calgettrace RETURNS STRING SONAME 'libcalmysql.dll';
CREATE FUNCTION calgetversion RETURNS STRING SONAME 'libcalmysql.dll';
CREATE FUNCTION calonlinealter RETURNS INTEGER SONAME 'libcalmysql.dll';
CREATE FUNCTION calviewtablelock RETURNS STRING SONAME 'libcalmysql.dll';
CREATE FUNCTION calcleartablelock RETURNS STRING SONAME 'libcalmysql.dll';

create database if not exists calpontsys;
use calpontsys;

-- SYSTABLE
create table if not exists systable (
	tablename varchar(64),
	`schema` varchar(64),
	objectid int,
	createdate date,
	lastupdate date,
	init int,
	next int,
	numofrows int,
	avgrowlen int,
	numofblocks int,
	autoincrement int
	) engine=infinidb comment='SCHEMA SYNC ONLY';

-- SYSCOLUMN
create table if not exists syscolumn (
	`schema` varchar(64),
	tablename varchar(64),
	columnname varchar(64),
	objectid int,
	dictobjectid int,
	listobjectid int,
	treeobjectid int,
	datatype int,
	columnlength int,
	columnposition int,
	lastupdate date,
	defaultvalue varchar(8),
	nullable int,
	scale int,
	prec int,
	autoincrement char(1),
	distcount int,
	nullcount int,
	minvalue varchar(64),
	maxvalue varchar(64),
	compressiontype int,
	nextvalue bigint
	) engine=infinidb comment='SCHEMA SYNC ONLY';

