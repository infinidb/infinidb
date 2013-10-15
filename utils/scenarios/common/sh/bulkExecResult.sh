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
}

function copySQLResultFiles {
#
   rm -f /usr/local/Calpont/data/bulk/data/import/TestSummary.*
   rm -f /usr/local/Calpont/data/bulk/data/import/TestTime.*
   rm -f /usr/local/Calpont/data/bulk/data/import/TestStats.*
#
   cp -f testResultSummary.txt /usr/local/Calpont/data/bulk/data/import/TestSummary.tbl
   cp -f testResultTime.txt /usr/local/Calpont/data/bulk/data/import/TestTime.tbl
   cp -f testResultStats.txt /usr/local/Calpont/data/bulk/data/import/TestStats.tbl
}


function CalLoadSQLTestResult {
#
   /usr/local/Calpont/bin/colxml perfstats -t TestSummary -j 1001
   /usr/local/Calpont/bin/colxml perfstats -t TestTime -j 1002
   /usr/local/Calpont/bin/colxml perfstats -t TestStats -j 1003
#   
   /usr/local/Calpont/bin/cpimport -j 1001
   /usr/local/Calpont/bin/cpimport -j 1002
   /usr/local/Calpont/bin/cpimport -j 1003
}
#
function MySQLLoadSQLTestResult {
#
   copySQLResultFiles
   mysql lqrefd01 -hws_tkerr_tx.calpont.com -uroot -pqalpont! </root/genii/utils/scenarios/common/sql/load_TestSummary.sql
   mysql lqrefd01 -hws_tkerr_tx.calpont.com -uroot -pqalpont! </root/genii/utils/scenarios/common/sql/load_TestTime.sql
   mysql lqrefd01 -hws_tkerr_tx.calpont.com -uroot -pqalpont! </root/genii/utils/scenarios/common/sql/load_TestStats.sql
}


#=========================================================================================
# cpimport SQL test results
#=========================================================================================
function CalLoadSQLTestResult {
#
   rm -f /usr/local/Calpont/data/bulk/data/import/TestSummary.*
   rm -f /usr/local/Calpont/data/bulk/data/import/TestTime.*
   rm -f /usr/local/Calpont/data/bulk/data/import/TestStats.*
#
   /usr/local/Calpont/bin/colxml perfstats -t TestSummary -j 1001
   /usr/local/Calpont/bin/colxml perfstats -t TestTime -j 1002
   /usr/local/Calpont/bin/colxml perfstats -t TestStats -j 1003
#   
   cp -f testResultSummary.txt /usr/local/Calpont/data/bulk/data/import/TestSummary.tbl
   cp -f testResultTime.txt /usr/local/Calpont/data/bulk/data/import/TestTime.tbl
   cp -f testResultStats.txt /usr/local/Calpont/data/bulk/data/import/TestStats.tbl
#
   /usr/local/Calpont/bin/cpimport -j 1001
   /usr/local/Calpont/bin/cpimport -j 1002
   /usr/local/Calpont/bin/cpimport -j 1003
}
#=========================================================================================
# cpimport bulk test results
#=========================================================================================
function CalLoadBulkTestResult {
#
   rm -f /usr/local/Calpont/data/bulk/data/import/BulkSummary.*
   rm -f /usr/local/Calpont/data/bulk/data/import/BulkTime.*
#
   /usr/local/Calpont/bin/colxml perfstats -t BulkSummary -j 1001
   /usr/local/Calpont/bin/colxml perfstats -t BulkStats -j 1002
#   
   cp -f testResultSummary.txt /usr/local/Calpont/data/bulk/data/import/BulkSummary.tbl
   cp -f testResultTime.txt /usr/local/Calpont/data/bulk/data/import/BulkStats.tbl
#
   /usr/local/Calpont/bin/cpimport -j 1001
   /usr/local/Calpont/bin/cpimport -j 1002
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
   cd $dirName
#
   getTestInfo
   if [ $loadFlag = "Y" ]; then
      echo "Test result for $testRunID has been previously loaded."
      echo "If you need to load it again, please reset the loaded flag"
      echo "in the testInfo.txt file in the test result directory.
      echo "exiting....
      exit 1
   fi
#
   case "$testID" in
      1)
         CalLoadSQLTestResult
         ;;
      2)
         CalLoadBulkTestResult
         ;;
   esac
