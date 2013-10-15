#!/bin/sh

#/*******************************************************************************
#*  Script Name:    getsar.sh
#*  Date Created:   2009.03.09
#*  Author:         Calpont Corp.
#*  Purpose:        Build a sar command based on user input and create the data file on all servers in the stack
#*  Parameters:     date - day of month in question (dd)
#*                  starttime - start of sar period (hh:mm)
#*                  endtime - end of sar period (hh:mm)
#*                  hostdir - directory name for this run
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

readserverlist ()
{ cat /tmp/hostlist.txt |
    while read moduletype hostname hostdir; do
    if [ $hostname = $localhost ]; then
       echo Extracting sar data from local host $localhost
       getsarlocal $date $starttime $endtime $hostdir
    elif [ $rc -lt $sc ]; then 
	rc=$sc
       cat /tmp/.prat/.hostlist2.txt |
       while read servername srvpwd hostdir; do
       if [ $servername != $localhost ]; then
          echo Extracting sar data from remote host $servername
          getsarremote $servername $srvpwd $date $starttime $endtime $hostdir
       fi
       done	    
    fi
    done
}
#
getsarremote ()
{ # Send the sar extraction statments to the remote module
  /usr/local/Calpont/bin/remote_command.sh $servername $srvpwd "$CTOOLS/getsar.sh $date $starttime $endtime $hostdir"
}
#
getsarlocal ()
{ mkdir -p $CTOOLS/data/$hostdir/sar
  # Create sar statements and extract data to text files
  echo "LC_ALL=C sar -P ALL -s $starttime:00 -e $endtime:00 -f /var/log/sa/sa$date > $CTOOLS/data/$hostdir/sar/cpu_$localhost.txt" >> /tmp/sarcpu.sh
  chmod 755 /tmp/sarcpu.sh
  /tmp/sarcpu.sh
  echo "LC_ALL=C sar -r -s $starttime:00 -e $endtime:00 -f /var/log/sa/sa$date > $CTOOLS/data/$hostdir/sar/mem_$localhost.txt" >> /tmp/sarmem.sh
  chmod 755 /tmp/sarmem.sh
  /tmp/sarmem.sh
  echo "LC_ALL=C sar -n DEV -s $starttime:00 -e $endtime:00 -f /var/log/sa/sa$date > $CTOOLS/data/$hostdir/sar/net_$localhost.txt" >> /tmp/sarnet.sh
  chmod 755 /tmp/sarnet.sh
  /tmp/sarnet.sh
  rm -rf /tmp/sar*.sh
}
#
if [ $modulename = "dm1" ]; then
   # Read through the host list and process each module in the stack
   readserverlist
else
   getsarlocal $date $starttime $endtime $hostdir
fi
#
exit 0
