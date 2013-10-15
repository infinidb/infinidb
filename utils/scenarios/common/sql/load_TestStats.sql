/*****************************************************************************
*  Script Name:    load_TestQuery.sql
*  Date Created:   2008.08.25
*  Author:         Joseph Williams
*  Purpose:        Script to load the performance test result from a CSV file.
******************************************************************************/
LOAD DATA INFILE '/usr/local/Calpont/data/bulk/data/import/TestStats.tbl' 
INTO TABLE TestStats
FIELDS 
TERMINATED BY '|' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(IterNum,SessNum,SQLSeqNum,SQLIdxNum,MaxMemPct,NumTempFiles,TempFieSpace,PhyIO,CacheIO,BlocksTouched,CasPartBlks,MsgBytesIn,MsgBytesOut,QuerySetupTime);
