#!/bin/bash
#
# $Id: transactionLogArchiver.sh 1375 2009-04-30 13:53:06Z rdempsey $

DATE=`date +'%s'`  
CDATA=/var/log/Calpont/data

if [ ! -d $CDATA/archive ]; then
	echo "Installation error: $CDATA/archive is not a directory." 1>&2
	exit 1
fi

if [ -f $CDATA/data_mods.log ]; then
	# Don't bother rotating an empty log
	if [ ! -s $CDATA/data_mods.log ]; then
		exit 0
	fi
	cp $CDATA/data_mods.log $CDATA/archive >/dev/null 2>&1
	if [ $? -ne 0 ]; then
		echo "Could not copy $CDATA/data_mods.log to $CDATA/archive" 1>&2
		exit 1
	fi
else
	# Is this a reportable/fatal error?
	echo "No such file: $CDATA/data_mods.log" 1>&2
fi

rm -f $CDATA/data_mods.log >/dev/null 2>&1
touch $CDATA/data_mods.log >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Could not create $CDATA/data_mods.log" 1>&2
	exit 1
fi

chmod 666 $CDATA/data_mods.log >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Could not set the perms on $CDATA/data_mods.log" 1>&2
	exit 1
fi

pkill -HUP syslog >/dev/null 2>&1

mv $CDATA/archive/data_mods.log $CDATA/archive/data_mods.log.$DATE >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Could not move $CDATA/archive/data_mods.log to $CDATA/archive/data_mods.log.$DATE" 1>&2
	exit 1
fi

