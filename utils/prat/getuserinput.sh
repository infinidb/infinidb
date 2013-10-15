#!/bin/sh
#
#/*******************************************************************************
#*  Script Name:    getuserinput.sh
#*  Date Created:   2009.05.20
#*  Author:         Calpont Corp.
#*  Purpose:        Collect data on all hosts in a stack
#*  Parameter:      None
#******************************************************************************/

CTOOLS=/usr/local/Calpont/tools

getuserinput ()
{ echo -n "Enter the two digit day of the month for the desired timeframe  > "
  read date
  echo -n "Enter the starting hour and minute (hh:mm) for the desired timeframe  > "
  read starttime
  echo -n "Enter the ending hour and minute (hh:mm) for the desired timeframe  > "
  read endtime
  echo $date $starttime $endtime > /tmp/pratinput.txt
}

getpwds ()
{ # Read through the host list to ask for passwords
  for hostname in `cat /tmp/serverlist.txt`; do
    echo -n -e "\nEnter the password for hostname = $hostname > "
    read -s password
    hostdir=`grep "$hostname" /tmp/hostlist.txt | awk -F" " '{print $3}'`
    echo $hostname $password $hostdir >> /tmp/.prat/.hostlist2.txt
  done
}

# clean up previous data files if necessary
if  [ ! -f /tmp/.prat/.hostlist2.txt ]
then
    cd /tmp/.prat
    touch .hostlist2.txt
else
    rm -rf /tmp/.prat/.hostlist2.txt
fi

if  [ -f /tmp/pratinput.txt ] 
then
    rm -rf /tmp/pratinput.txt
fi

getuserinput
getpwds

# End of script
