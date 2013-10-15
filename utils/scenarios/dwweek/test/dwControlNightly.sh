#!/bin/bash
#
#$1 = testDB
#$2 = start hour
#
   if [ $# -ne 2 ]; then
      echo Syntax: dwControlNightly.sh testDB startHour
      echo Exiting.....
      exit 1
   fi
#
   testDB=$1
   startHour=$2
#
   keepGoing=1
   nightlyDone=0
   while [ $keepGoing -eq 1 ]; do
	vTime=$(date "+%H:%M:%S %x")
	vHour=${vTime:0:2}
   	if [ $vHour -eq $startHour ]; then
          if [ $nightlyDone -eq 0 ]; then
          	dirName=nightly
          	mkdir -p $dirName
          	cd $dirName
#
# group to run nightly stats, pre-DML
	   	/root/genii/utils/scenarios/perf/test/pfSubmitGroupTest.sh 204 $testDB 1 1 S 0 M
              rm -rf *
# group to delete and update lineitem rows
	   	/root/genii/utils/scenarios/perf/test/pfSubmitGroupTest.sh 203 $testDB 1 1 S 0 M
              rm -rf *
# group to run nightly stats, post-DML
	   	/root/genii/utils/scenarios/perf/test/pfSubmitGroupTest.sh 204 $testDB 1 1 S 0 M
              rm -rf *
#
# backup database
# 		/root/genii/utils/scenarios/dwweek/test/dwbackup.sh
	   	cd ..
          	nightlyDone=1
	   fi
       else
          nightlyDone=0
       fi
       sleep 60
       if [ -f /root/genii/utils/scenarios/dwweek/data/continue.txt ]; then
          keepGoing=`cat /root/genii/utils/scenarios/dwweek/data/continue.txt`
       fi
   done

 
