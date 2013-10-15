/*****************************************************************************
*  Script Name:    load_TestSumamry.sql
*  Date Created:   2008.08.25
*  Author:         Joseph Williams
*  Purpose:        Script to load the performance test result from a CSV file.
******************************************************************************/
LOAD DATA INFILE '/usr/local/Calpont/data/bulk/data/import/TestSummary.tbl' 
INTO TABLE TestSummary
FIELDS 
TERMINATED BY '|' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(TestID,TestRunID,TestRunDesc,ExecServer,StackName,numDM,numUM,numPM,CalpontDB,ScriptFileName,NumIterations,NumSessions,DataVolume,IOType,NumStmts,NumStmtsProcessed,RunCompleted);
 