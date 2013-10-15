#!/bin/sh
#
#/***************************************************************************
#*  Script Name:    getmodules.sh
#*  Date Created:   2009.03.09
#*  Author:         Calpont Corp.
#*  Purpose:        Retrieve the host names of the servers in this stack
#*
#*  Parameters:     None
#*
#***************************************************************************/
#
moduletype=`cat /usr/local/Calpont/local/module`
hostdir=`date +%Y%m%d%H%M%S`
#
# Module check
if [ $moduletype != "dm1" ]; then
   echo
   echo
   echo "The PRAT utility can only be run from Director Module #1" 1>&2
   echo
   echo
   exit 1
fi
#
# clean up previous data files if necessary
if  [ -f /tmp/hostlist.txt ] 
then
    rm -rf /tmp/hostlist.txt
fi

if  [ -f /tmp/serverlist.txt ]
then
    rm -rf /tmp/serverlist.txt
fi

# issue Calpont console command and send the output to a file
/usr/local/Calpont/bin/calpontConsole getsystemnetworkconfig ACK_YES |
egrep -w 'Director|User|Performance' | 
awk -F" " '{print $1"\t" $2"\t" $3"\t" $4"\t" $6}' > /tmp/modulelist.txt
#
# Add the timestamped directory name to each line
cat /tmp/modulelist.txt |
while read modulename moduletype module modulenumber host  ; do
    echo $modulename $host "$host"_prat_"$hostdir" >> /tmp/hostlist.txt
    echo $host >> /tmp/serverlist.txt
done
#
rm -rf /tmp/modulelist.txt
#
# End of Script
