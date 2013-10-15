create table TestStats (
TestRunID	bigint,
IterNum	int,
SessNum	int,
SQLSeqNum	int,
SQLIdxNum	int,
MaxMemPct	int,
NumTempFiles	int,
TempFileSpace	int,
PhyIO		int,
CacheIO	int,
BlocksTouched	int,
CasPartBlks	int,
MsgBytesIn	int,
MsgBytesOut	int,
QuerySetupTime decimal(18,6)
);
