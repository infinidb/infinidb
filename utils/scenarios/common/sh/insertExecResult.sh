#!/bin/bash
#
# $1 = testRunID
#
#=========================================================================================
# MySQL load SQL test results
#=========================================================================================
function getTestInfo {
#
   testID=`cat $dirName/testInfo.txt | grep testID |awk -F"=" '{print $2}'`
   loadedFlag=`cat $dirName/testInfo.txt | grep loadedFlag |awk -F"=" '{print $2}'`
   if [ -z $loadedFlag ]; then
      loadedFlag=N
   fi
#
   rm -f insertSummaryScript.sql
   rm -f insertTimeScript.sql
   rm -f insertStatsScript.sql
}

#=========================================================================================
# insert SQL test results
#=========================================================================================
function insertSQLTestResult {
#
# insert test summary table
#
   summaryFileName="insertSummaryScript.sql"
   cat $dirName/testResultSummary.txt |
   while read summaryLine; do
      testID=`echo $summaryLine|awk -F"|" '{print $1}'`
      testRunID=`echo $summaryLine|awk -F"|" '{print $2}'`
      testRunDesc=`echo $summaryLine|awk -F"|" '{print $3}'`
      execServer=`echo $summaryLine|awk -F"|" '{print $4}'`
      stackName=`echo $summaryLine|awk -F"|" '{print $5}'`
      numDM=`echo $summaryLine|awk -F"|" '{print $6}'`
      numUM=`echo $summaryLine|awk -F"|" '{print $7}'`
      numPM=`echo $summaryLine|awk -F"|" '{print $8}'`
      calpontDB=`echo $summaryLine|awk -F"|" '{print $9}'`
      swRelease=`echo $summaryLine|awk -F"|" '{print $10}'`
      grpTestNum=`echo $summaryLine|awk -F"|" '{print $11}'`
      scriptFileName=`echo $summaryLine|awk -F"|" '{print $12}'`
      numIterations=`echo $summaryLine|awk -F"|" '{print $13}'`
      numSessions=`echo $summaryLine|awk -F"|" '{print $14}'`
      IOType=`echo $summaryLine|awk -F"|" '{print $15}'`
      numStmts=`echo $summaryLine|awk -F"|" '{print $16}'`
      numStmtsProcessed=`echo $summaryLine|awk -F"|" '{print $17}'`
      numCompleted=`echo $summaryLine|awk -F"|" '{print $18}'`
#
      vals="$testID,$testRunID,\"$testRunDesc\",\"$execServer\",\"$stackName\",$numDM,$numUM,$numPM,\"$calpontDB\",\"$swRelease\",$grpTestNum,\"$scriptFileName\",$numIterations,$numSessions,\"$IOType\",$numStmts,$numStmtsProcessed,\"$numCompleted\""
      stmt="insert into testSummary values ($vals);"
      echo $stmt >> $summaryFileName
   done

#
# insert test time table
#
   timeFileName="insertTimeScript.sql"
   cat $dirName/testResultTime.txt |
   while read timeLine; do
      testRunID=`echo $timeLine|awk -F"|" '{print $1}'`
      iterNum=`echo $timeLine|awk -F"|" '{print $2}'`
      sessNum=`echo $timeLine|awk -F"|" '{print $3}'`
      SQLSeqNum=`echo $timeLine|awk -F"|" '{print $4}'`
      SQLIdxNum=`echo $timeLine|awk -F"|" '{print $5}'`
      startTime=`echo $timeLine|awk -F"|" '{print $6}'`
      endTime=`echo $timeLine|awk -F"|" '{print $7}'`
#
      vals="$testRunID,$iterNum,$sessNum,$SQLSeqNum,$SQLIdxNum,\"$startTime\",\"$endTime\""
      vals=`echo $vals |sed 's/""/NULL/g'`
      stmt="insert into testTime values ($vals);"
      echo $stmt >> $timeFileName
   done
#
# insert test stats table
#
   statsFileName="insertStatsScript.sql"
   cat $dirName/testResultStats.txt |
   while read statsLine; do
      vals=`echo $statsLine |sed 's/|/,/g'`
      stmt="insert into testStats values ($vals);"
      stmt=`echo $stmt |sed 's/,,/,NULL,/g'|sed 's/,,/,NULL,/g'|sed 's/,)/)/g'`
      echo $stmt >> $statsFileName
   done
#
   mysql lqrefd01 -hws_tkerr_tx -uroot -pqalpont! <insertSummaryScript.sql
   mysql lqrefd01 -hws_tkerr_tx -uroot -pqalpont! <insertTimeScript.sql
   mysql lqrefd01 -hws_tkerr_tx -uroot -pqalpont! <insertStatsScript.sql
}
#=========================================================================================
# cpimport bulk test results
#=========================================================================================
function insertBulkTestResult {
#
# insert test summary table
#
   summaryFileName="insertSummaryScript.sql"
   cat $dirName/testResultSummary.txt |
   while read summaryLine; do
      testID=`echo $summaryLine|awk -F"|" '{print $1}'`
      testRunID=`echo $summaryLine|awk -F"|" '{print $2}'`
      testRunDesc=`echo $summaryLine|awk -F"|" '{print $3}'`
      execServer=`echo $summaryLine|awk -F"|" '{print $4}'`
      stackName=`echo $summaryLine|awk -F"|" '{print $5}'`
      numDM=`echo $summaryLine|awk -F"|" '{print $6}'`
      numUM=`echo $summaryLine|awk -F"|" '{print $7}'`
      numPM=`echo $summaryLine|awk -F"|" '{print $8}'`
      calpontDB=`echo $summaryLine|awk -F"|" '{print $9}'`
      scriptFileName=`echo $summaryLine|awk -F"|" '{print $10}'`
      numTables=`echo $summaryLine|awk -F"|" '{print $11}'`
      numTablesLoaded=`echo $summaryLine|awk -F"|" '{print $12}'`
      runCompleted=`echo $summaryLine|awk -F"|" '{print $13}'`
      rowCntMatched=`echo $summaryLine|awk -F"|" '{print $14}'`
      startTime=`echo $summaryLine|awk -F"|" '{print $15}'`
      endTime=`echo $summaryLine|awk -F"|" '{print $16}'`
#
      vals="$testID,$testRunID,\"$testRunDesc\",\"$execServer\",\"$stackName\",$numDM,$numUM,$numPM,\"$calpontDB\",\"$scriptFileName\",$numTables,$numTablesLoaded,\"$runCompleted\",\"$rowCntMatched\",\"$startTime\",\"$endTime\""
      stmt="insert into bulkSummary values ($vals);"
      stmt=`echo $stmt |sed 's/,,/,NULL,/g'|sed 's/,,/,NULL,/g'|sed 's/,)/)/g'`
      echo $stmt >> $summaryFileName
   done
#
# insert test stats table
#
   statsFileName="insertStatsScript.sql"
   cat $dirName/testResultStats.txt |
   while read statsLine; do
      testRunID=`echo $statsLine|awk -F"|" '{print $1}'`
      tableName=`echo $statsLine|awk -F"|" '{print $2}'`
      sourceFile=`echo $statsLine|awk -F"|" '{print $3}'`
      loadTime=`echo $statsLine|awk -F"|" '{print $4}'`
      rowCntProcessed=`echo $statsLine|awk -F"|" '{print $5}'`
      rowCntInserted=`echo $statsLine|awk -F"|" '{print $6}'`
      rowCntDB=`echo $statsLine|awk -F"|" '{print $7}'`
#
      vals="$testRunID,\"$tableName\",\"$sourceFile\",$loadTime,$rowCntProcessed,$rowCntInserted,$rowCntDB"
      stmt="insert into bulkStats values ($vals);"
      stmt=`echo $stmt |sed 's/,,/,NULL,/g'|sed 's/,,/,NULL,/g'|sed 's/,)/)/g'`
      echo $stmt >> $statsFileName
   done
#
   mysql lqrefd01 -hws_tkerr_tx -uroot -pqalpont! <insertSummaryScript.sql
   mysql lqrefd01 -hws_tkerr_tx -uroot -pqalpont! <insertStatsScript.sql
#
}
#=========================================================================================
# Main
#=========================================================================================
#
   if [ $# -ne 1 ]; then
      echo Syntax: bulkExtcResult.sh testRunID
      echo Exiting.....
      exit 1
   fi
#
# Verified existance of testRunID
#
   testRunID=$1
   host=`hostname -s`
   dirName=/root/genii/testResult/$testRunID
#
   if [ ! -d $dirName ]; then
      echo TestRunID $testRunID does not exist on this server \($host\).
      echo Please make sure the test was executed on this server.
      echo Exit.....
      exit 1
   fi
#
   getTestInfo
loadedFlag=N
   if [ $loadedFlag = "Y" ]; then
      echo "Test result for $testRunID has been previously loaded."
      echo "If you need to load it again, please reset the loaded flag"
      echo "in the testInfo.txt file in the test result directory.
      echo "exiting....
      exit 1
   fi
#
   case "$testID" in
      1)
         insertSQLTestResult
         ;;
      2)
         insertBulkTestResult
         ;;
   esac
   echo loadedFlag=Y >> $dirName/testInfo.txt
#
   exit 0
