create table TestSummary (
TestID			int,
TestRunID		bigint,
TestRunDesc		varchar(255),
ExecServer		varchar(15),
StackName		varchar(15),
numDM			tinyint,
numUM			tinyint,
numPM			tinyint
CalpontDB		varchar(15),
Software		varchar(20),
GroupID		int,
ScriptFileName	varchar(255),
NumIterations		int,
NumSessions		int,
IOType			char(1),
NumStmts		int,
NumStmtsProcessed	int,
RunCompleted		char(1)
);

