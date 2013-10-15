Create table BulkSummary (
TestID	int,
TestRunID bigint,
TestRunDesc varchar(255),
ExecServer varchar(15),
StackName varchar(15),
numDM tinyint,
numUM tinyint,
numPM tinyint,
CalpontDB varchar(15),
ScriptFileName varchar(255),
NumTables tinyint,
NumTablesLoaded tinyint,
RunCompleted char(1),
RowCntsMatched char(1),
StartTime datetime,
EndTime datetime
);