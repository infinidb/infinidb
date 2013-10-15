#!/bin/bash
#=========================================================================================
#
#$1 = testDB
#$2 = sourceFileName
#
   if [ $# -lt 2 ]; then
      echo Syntax dwSubmitCpimport.sh dbName sourceFileName
      echo Exiting.....
      exit 1
   fi
#
   testDB=$1
   sourceFileName=$2
#
   testID=2
#---------------------------------------------------------------------------
# Create a cpimport script, which will be executed by a PM remotely.
#---------------------------------------------------------------------------
   bulkScriptName="bulkScript.sh"
#
   echo \#!/bin/bash > $bulkScriptName
   echo \# >> $bulkScriptName
#
   echo rm -f /usr/local/Calpont/data/bulk/log/Jobxml_9999.log >> $bulkScriptName
   echo rm -f /usr/local/Calpont/data/bulk/log/Job_9999.log >> $bulkScriptName
   echo /usr/local/Calpont/bin/colxml $testDB -t lineitem -l $sourceFileName -j 9999 >> $bulkScriptName
   echo /usr/local/Calpont/bin/cpimport -j 9999 >> $bulkScriptName
   echo cp /usr/local/Calpont/data/bulk/job/Job_9999.xml . >> $bulkScriptName
   echo cp /usr/local/Calpont/data/bulk/log/Jobxml_9999.log . >> $bulkScriptName
   echo cp /usr/local/Calpont/data/bulk/log/Job_9999.log . >> $bulkScriptName
   chmod 777 $bulkScriptName
#
#append current directory path to to script file name
   scriptFileName=`pwd`\/$bulkScriptName
#
   autopilotExecDir=`pwd`
   export autopilotExecDir
#
   echo testID=$testID >testInfo.txt
   echo testDB=$testDB >>testInfo.txt
   echo testType=NA >>testInfo.txt
   echo scriptName=$scriptFileName >>testInfo.txt
   echo sessions=1 >>testInfo.txt
   echo iterations=1 >>testInfo.txt
   /root/genii/utils/scenarios/common/sh/testExecEngine.sh > testExec.log
   testRunID=`cat testInfo.txt |grep testResultDir |awk -F"=" '{print $2}'`
   /root/genii/utils/scenarios/common/sh/collExecResult.sh $testRunID >collExecResult.log
   /root/genii/utils/scenarios/common/sh/insertExecResult.sh $testRunID >bulkExecResult.log
