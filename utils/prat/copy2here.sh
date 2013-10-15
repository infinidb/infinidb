#!/bin/sh
#
#/*******************************************************************************
#*  Script Name:    copy2here.sh
#*  Date Created:   2009.03.10
#*  Author:         Calpont Corp.
#*  Purpose:        copy the data files to calling host for archiving
#*
#*  Parameters:     None
#*                  
#******************************************************************************/

CTOOLS=/usr/local/Calpont/tools

localhost=$(hostname -s)
#
if [ -f /tmp/.prat/.hostlist2.txt ]; then
   sc=`wc -l < /tmp/.prat/.hostlist2.txt`
else
   sc=0
fi
rc=1

cat /tmp/hostlist.txt |
while read moduletype hostname hostdir; do
    if [ $hostname = $localhost ]; then
	cp /tmp/hostlist.txt $CTOOLS/data/$hostdir
	cp /tmp/pratinput.txt $CTOOLS/data/$hostdir
        echo Creating tar file on local host $localhost
        tarfile=$hostdir.tar
        cd $CTOOLS/data
        tar -cf $tarfile $hostdir
    elif [ $rc -lt $sc ]; then 
        rc=$sc
       cat /tmp/.prat/.hostlist2.txt |
       while read servername srvpwd hostdir; do
       if [ $servername != $localhost ]; then
          echo Collecting files on remote host $servername
	  echo "  and copying them to this server"
          tarfile=$hostdir.tar
          /usr/local/Calpont/bin/remote_command.sh $servername $srvpwd "$CTOOLS/tarfiles.sh $hostdir $tarfile" 
	  cd $CTOOLS/data
          /usr/local/Calpont/bin/remote_scp_get.sh $servername $srvpwd $CTOOLS/data/$tarfile 
       fi
       done
    fi
done
#
# End of script
