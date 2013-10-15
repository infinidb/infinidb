#!/bin/bash
#
#$1 = dwweek
#$2 = start hour
#$3 = stop hour
#$4 = interval in minute
#
   if [ $# -ne 4 ]; then
      echo Syntax: dwControlBulk.sh testDB startHour stopHour intervalInMinutes
      echo Exiting.....
      exit 1
   fi
#
   testDB=$1
   startHour=$2
   stopHour=$(($3 - 1))
   interval=$4
#
   jobNum=0
#
   keepGoing=1
   while [ $keepGoing -eq 1 ]; do
	vTime=$(date "+%H:%M:%S %x")
	vHour=${vTime:0:2}
   	if [ $vHour -ge $startHour ] && [ $vHour -le $stopHour ]; then
	   vMin=${vTime:3:2}
          vHour=`expr $vHour + 0`
          vMin=`expr $vMin + 0`
	   minutes=$((($vHour + 1) * 60 + $vMin - ($startHour + 1) * 60))
          remainder=`expr $minutes % $interval`
          if [ $remainder -eq 0 ]; then
             ((jobNum++))
             if [ $jobNum -gt 68 ]; then
                jobNum=1
             fi
			 if [ $jobNum -lt 10 ]; then
				dirName=${vTime:15:4}${vTime:9:2}${vTime:12:2}_0$jobNum
				mkdir $dirName
				cd $dirName
				/root/genii/utils/scenarios/dwweek/test/dwSubmitCpimport.sh $testDB lineitem_0$jobNum.tbl
			 else
				dirName=${vTime:15:4}${vTime:9:2}${vTime:12:2}_$jobNum
				mkdir $dirName
				cd $dirName
				/root/genii/utils/scenarios/dwweek/test/dwSubmitCpimport.sh $testDB lineitem_$jobNum.tbl
			 fi
             cd ..
             timeToSleep=1
          else
             timeToSleep=1
          fi
       else
          timeToSleep=5
       fi
   	sleep $timeToSleep
       if [ -f /root/genii/utils/scenarios/dwweek/data/continue.txt ]; then
          keepGoing=`cat /root/genii/utils/scenarios/dwweek/data/continue.txt`
       fi
   done


 
