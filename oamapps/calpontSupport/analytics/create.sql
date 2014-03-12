/*
Table for output from track.sh.

Contains a row per process / snapshot.

*/

drop table if exists processLog;

create table processLog (
snapshotid int, 
dtm datetime,         /* repeated for a snapshot */
processPid int,
processUserName varchar(7),
processPriority int,
processNice int,
moduleSwapUsedK int,
moduleCachedUsedK int,
processVirt int,
processRes  int,
processShr int,
processS   char(8),
processCPUPct decimal(4,1),
processMemPct decimal(4,1),
processTime varchar(20),
processCommand varchar(20))engine=infinidb;
