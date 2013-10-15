#!/bin/bash
#
#$1 = testDB
#$2 = start hour
#
   if [ $# -ne 2 ]; then
      echo Syntax: dwControlReport.sh testDB startHour
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
# group to run nightly, lengthy reports
	   	/root/genii/utils/scenarios/perf/test/pfSubmitGroupTest.sh 205 $testDB 1 1 S 0 M
              rm -rf *
	   	cd ..
          	nightlyDone=1
	   fi
       else
          nightlyDone=0
       fi
       sleep 60
   done

 
