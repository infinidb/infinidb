#!/bin/sh
#
#/*******************************************************************************
#*  Script Name:    prat.sh
#*  Date Created:   2009.03.09
#*  Author:         Calpont Corp.
#*  Purpose:        Main PRAT script
#*  Parameter:      None
#******************************************************************************/

CTOOLS=/usr/local/Calpont/tools

# Retrieve the names of servers in this Calpont stack
$CTOOLS/getmodules.sh

# Get user input for day of month, start & end times for analysis period
$CTOOLS/getuserinput.sh

# Extract input data for use in subsequent steps
date=`cat /tmp/pratinput.txt | awk -F" " '{ print $1 }'`
starttime=`cat /tmp/pratinput.txt | awk -F" " '{ print $2 }'`
endtime=`cat /tmp/pratinput.txt | awk -F" " '{ print $3 }'`

# call sar script to get sar data from each host in the stack
echo -e "\n\nGathering sar data"
$CTOOLS/getsar.sh $date $starttime $endtime

# call script to get Calpont log data from each host in the stack
echo -e "\n\nGathering Calpont log data"
$CTOOLS/getlogs.sh $date

# call script to get Calpont log data from each host in the stack
echo -e "\n\nGathering system process activity data"
$CTOOLS/getps.sh $date $starttime $endtime

# gather data up to DM (host that called this script)
echo -e "\n\nGathering data to this host"
$CTOOLS/copy2here.sh

# clean up previous data files
rm -rf /tmp/.prat/.hostlist2.txt
rm -rf /tmp/hostlist.txt
rm -rf /tmp/serverlist.txt
rm -rf /tmp/pratinput.txt

echo "PRAT utility data collection complete!"

# End of script
