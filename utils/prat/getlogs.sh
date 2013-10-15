#!/bin/sh
#
#/*******************************************************************************
#*  Script Name:    getlogs.sh
#*  Date Created:   2009.03.09
#*  Author:         Calpont Corp.
#*  Purpose:        Copy Calpont log files on all server in stack
#*
#*  Parameters:     Date  - day of month in question
#*		    hostdir - directory name for this run
#******************************************************************************/

CTOOLS=/usr/local/Calpont/tools

date=$1
hostdir=$2
localhost=$(hostname -s)
modulename=`cat /usr/local/Calpont/local/module`
currentdate=`date +%d`
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
       echo Copying Calpont logs from local host $localhost
       getlogslocal $date hostdir
    elif [ $rc -lt $sc ]; then 
        rc=$sc
       cat /tmp/.prat/.hostlist2.txt |
       while read servername srvpwd hostdir; do
       if [ $servername != $localhost ]; then
          echo Copying Calpont logs from remote host $servername
          getlogsremote $servername $srvpwd $date $hostdir
       fi
       done         
    fi
    done
}
#
getlogsremote ()
{ # Send the command to the remote module(s) to copy the Calpont logs
  /usr/local/Calpont/bin/remote_command.sh $servername $srvpwd "$CTOOLS/getlogs.sh $date $hostdir"
}
#
getlogslocal ()
{ mkdir -p $CTOOLS/data/$hostdir/logs
  if [ $date = $currentdate ]; then
     cp -r /var/log/Calpont/* /usr/local/Calpont/tools/data/$hostdir/logs
  else
     find /var/log/Calpont -type f -name "*$date"  -exec sh -c 'exec cp -f "$@" '$CTOOLS/data/$hostdir/logs'' find-copy {} +
  fi
}
#
if [ $modulename = "dm1" ]; then
   # Read through the host list and process each module in the stack
   readserverlist
else
   getlogslocal $date $hostdir
fi
#
exit 0
