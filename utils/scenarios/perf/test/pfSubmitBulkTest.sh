#!/bin/bash
#=========================================================================================
#
   if [ $# -lt 2 ]; then
      echo Syntax pfSumbitBulkTest.sh dbName PM1ServerName
      echo Exiting.....
      exit 1
   fi
#
   testID=2
#
   testDB=$1
   PM1=$2
   curDir=`pwd`
#---------------------------------------------------------------------------
# Create a cpimport script, which will be executed by a PM remotely.
#---------------------------------------------------------------------------
   cpimportScriptName="cpimportScript.sh"
#
   echo \#\!/bin/bash > $cpimportScriptName
   echo \# >> $cpimportScriptName
   echo cd /usr/local/Calpont/data/bulk/log >> $cpimportScriptName
   echo rm -f Jobxml_9999.log >> $cpimportScriptName
   echo rm -f Job_9999.log >> $cpimportScriptName
   echo rm -f fileStats.txt >> $cpimportScriptName
   echo rm -f finished.txt >> $cpimportScriptName
   echo "ls -alh /usr/local/Calpont/data/bulk/data/import/*.tbl > fileStats.txt" >> $cpimportScriptName
#   echo "wc -l /usr/local/Calpont/data/bulk/data/import/*.tbl >> fileStats.txt" >> $cpimportScriptName
#
   echo /usr/local/Calpont/bin/colxml $testDB -r 2 -j 9999 >> $cpimportScriptName
   echo sleep 5 >> $cpimportScriptName
   echo sync >> $cpimportScriptName
#
   echo \# /usr/local/Calpont/bin/cpimport -j 9999 -i >> $cpimportScriptName
   echo sleep 5 >> $cpimportScriptName
   echo touch finished.txt >> $cpimportScriptName
   echo sync >> $cpimportScriptName
   chmod 777 $cpimportScriptName
#---------------------------------------------------------------------------
# Create a bulktest script, which will be submitted to the execution engine.
#---------------------------------------------------------------------------
   bulkScriptName="bulkScript.sh"
#
   echo \#/bin/bash > $bulkScriptName
   echo \# >> $bulkScriptName
#
   echo "/root/genii/utils/scenarios/common/sh/remote_command.sh $PM1 qalpont! \"/mnt/parentOAM$curDir/$cpimportScriptName\"" >> $bulkScriptName
#
   echo sleep 5 >> $bulkScriptName
   echo "while [ ! -f /mnt/pm1/usr/local/Calpont/data/bulk/log/finished.txt ]; do" >> $bulkScriptName
   echo    sleep 5 >> $bulkScriptName
   echo echo waiting...... >> $bulkScriptName
   echo done >> $bulkScriptName
#
   echo cp /mnt/pm1/usr/local/Calpont/data/bulk/job/Job_9999.xml . >> $bulkScriptName
   echo cp /mnt/pm1/usr/local/Calpont/data/bulk/log/Jobxml_9999.log . >> $bulkScriptName
   echo cp /mnt/pm1/usr/local/Calpont/data/bulk/log/Job_9999.log . >> $bulkScriptName
   echo cp /mnt/pm1/usr/local/Calpont/data/bulk/log/fileStats.txt . >> $bulkScriptName
#   
   chmod 777 $bulkScriptName
#
#append current directory path to to script file name
   scriptFileName=`pwd`\/$bulkScriptName
#
   echo testID=$testID >testInfo.txt
   echo testDB=$testDB >>testInfo.txt
   echo scriptName=$scriptFileName >>testInfo.txt
   echo sessions=1 >>testInfo.txt
   echo iterations=1 >>testInfo.txt
#
   autopilotExecDir=`pwd`
   export autopilotExecDir
#
   /root/genii/utils/scenarios/common/sh/testExecEngine.sh > testExec.log
   testRunID=`cat testInfo.txt |grep testResultDir |awk -F"=" '{print $2}'`
   /root/genii/utils/scenarios/common/sh/collExecResult.sh $testRunID >collExecResult.log
#   /root/genii/utils/scenarios/common/sh/insertExecResult.sh $testRunID >bulkExecResult.log


