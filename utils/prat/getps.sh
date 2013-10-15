#!/bin/sh
#
#/*******************************************************************************
#*  Script Name:    getps.sh
#*  Date Created:   2009.03.09
#*  Author:         Calpont Corp.
#*  Purpose:        Retrieve the appropriate ps files on all servers in the stack
#*
#*  Parameters:     date - day of month in question (dd)
#*                  starttime - start of ps period (hh:mm)
#*                  endtime - end of ps period (hh:mm)
#*                  
#******************************************************************************/

CTOOLS=/usr/local/Calpont/tools

date=$1
starttime=$2
endtime=$3
hostdir=$4
localhost=$(hostname -s)
modulename=`cat /usr/local/Calpont/local/module`
if [ -f /tmp/.prat/.hostlist2.txt ]; then
   sc=`wc -l < /tmp/.prat/.hostlist2.txt`
else
   sc=0
fi
rc=1
#
readserverlist ()
{ cat /tmp/hostlist.txt |
    while read moduletype hostname hostdir; do
    if [ $hostname = $localhost ]; then
       echo Extracting system activity data from local host $localhost
       getpslocal $date $starttime $endtime $hostdir
    elif [ $rc -lt $sc ]; then 
        rc=$sc
       cat /tmp/.prat/.hostlist2.txt |
       while read servername srvpwd hostdir; do
       if [ $servername != $localhost ]; then
          echo Extracting system activity data from remote host $servername
          getpsremote $servername $srvpwd $date $starttime $endtime $hostdir
       fi
       done         
    fi
    done
}
#
getpsremote ()
{ # Send the command to the remote module(s) to extract ps data
  /usr/local/Calpont/bin/remote_command.sh $servername $srvpwd "$CTOOLS/getps.sh $date $starttime $endtime $hostdir"
}
#
getpslocal ()
{ mkdir -p $CTOOLS/data/$hostdir/ps
  # create the beginning and ending time search variables
  st=`echo $starttime | awk -F":" '{ printf "%.4d\n", $1$2 }'`
  sh=`echo $starttime | awk -F":" '{ print $1 }'`
  sm=`echo $starttime | awk -F":" '{ print $2 }'`
  et=`echo $endtime | awk -F":" '{ printf "%.4d\n", $1$2 }'`
  eh=`echo $endtime | awk -F":" '{ print $1 }'`
  em=`echo $endtime | awk -F":" '{ print $2 }'`
  start="Start $sh:$sm"
  end="End $eh:$em"
  foundstart="no"
  foundend="no"
  #
  # Find the proper daily log to search
  curdate=`date +%d`
  logdate=`expr $curdate - $date`
  if [ $date == $curdate ]; then
     logfile="pslog"
  elif [ $logdate -gt 7 ]; then
       echo "ps daily logs are available for one week only"
       exit
  else
       logfile="pslog.$logdate"
  fi
  # 
  #-----------------------------------------------------------------------------
  # Search through the file looking for start and end time matches 
  #-----------------------------------------------------------------------------
  #
  # Look for beginning time
  k=$st
  while [ $k -ge $st ] && [ $k -le $et ] && [ $foundstart == "no" ]; do
       if [ $sm -ge 60 ]; then
          k=`expr $k + 39`
          sm=`expr $sm - 61`
       elif [ $k -ge $st ] && [ $k -le $et ]; then
          grep -q "Start $sh:$sm" /var/log/prat/ps/$logfile
          if [ "$?" -eq "0" ] && [ $foundstart == "no" ]; then
             start="Start $sh:$sm"
             foundstart="yes"
          fi 
       fi
       if [ $foundstart == "no" ]; then
          k=`expr $k + 0`
          k=$((k + 1))
          ((sm++))
       fi
  done
  #
  # Look for ending time
  while [ $k -ge $st ] && [ $k -le $et ] && [ $foundend == "no" ]; do     
       if [ $em -ge 60 ]; then        
          k=`expr $k + 39`
          em=`expr $em - 61`
       elif [ $k -ge $st ] && [ $k -le $et ]; then
          grep -q "End $eh:$em" /var/log/prat/ps/$logfile
          if [ "$?" -eq "0" ] && [ $foundend == "no" ]; then
             end="End $eh:$em"
             foundend="yes"
          fi
       fi
       if [ $foundend == "no" ]; then
          k=`expr $k + 0`
          k=$((k + 1))
          ((em++))
       fi
  done
  #
  #  create the awk command and write it to a temporary  run file 
  cmd="/$start/,/$end/ {print \$0} "
  echo $cmd >> /tmp/cmd.$$
  # 
  # execute the command 
  awk -f /tmp/cmd.$$ /var/log/prat/ps/$logfile > $CTOOLS/data/$hostdir/ps/pslog
  #
  rm -rf /tmp/cmd.$$
}
#
if [ $modulename = "dm1" ]; then
   # Read through the host list and process each module in the stack
   readserverlist
else
   getpslocal $date $starttime $endtime $hostdir
fi
#
exit 0
