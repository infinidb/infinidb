Create table BulkStats (
TestRunID bigint,
TableName varchar(25),
SourceFile varchar(25),
LoadTime int,
RowCntProcessed bigint,
RowCntInserted bigint,
RowCntDB bigint
);