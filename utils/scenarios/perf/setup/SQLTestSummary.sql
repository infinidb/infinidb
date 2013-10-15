create table SQLTestSummary (
TestID			int,
TestRunID		char(14),	
TestRunDesc		varchar(255),
ExecServer		varchar(15),
StackName		varchar(15),
numDM			tinyint,
numUM			tinyint,
numPM			tinyint,
CalpontDB		varchar(15),
ScriptFileName	varchar(255),
NumIterations		tinyint,
NumSessions		tinyint,
NumSQLStmts		tinyint,
DataVolume		char(1),
IOType			char(1),
NumStmts		int,
NumStmtsProcessed	int,	
RunCompleted		char(1)
)engine=infinidb;


create table SQLTestTime (
TestRunID		char(14),
IterNum		tinyint,
SessNum		tinyint,
SQLSeqNum		tinyint,
StartTime		datetime,
EndTime		datetime
)engine=infinidb;



create table BulkTestSummary (
TestID			int,
TestRunID		char(14),
TestRunDesc		varchar(255),
ExecServer		varchar(15),
StackName		varchar(15),
numDM			tinyint,
numUM			tinyint,
numPM			tinyint,
CalpontDB		varchar(15),
ScriptFileName	varchar(255),
NumTables		tinyint,
NumTablesLoaded	tinyint,
RunCompleted		char(1)
)engine=infinidb;


create table BulkTestStats (
TestRunID		char(14),
TableName		varchar(25),
SourceFile		varchar(25),
LoadTime		int,
RowsProcessed		bigint,
RowsInserted		bigint
)engine=infinidb;







