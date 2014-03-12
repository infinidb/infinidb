#!/bin/bash
#
# $Id: hardwareReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        MODULE=$1
else
        MODULE="pm1"
fi

if [ $2 ] ; then
        INSTALLDIR=$2
else
        INSTALLDIR="/usr/local/Calpont"
fi

if [ $USER = "root" ]; then
	SUDO=" "
else
	SUDO="sudo"
fi

$SUDO rm -f /tmp/${MODULE}_configReport.txt

{
echo " "
echo "******************** Configuration/Status Report for ${MODULE} ********************"
echo " "

if test -f /sbin/chkconfig ; then
	echo "-- chkconfig configuration --"
	echo " "
	echo "################# /sbin/chkconfig --list | grep infinidb #################"
	echo " "
	$SUDO /sbin/chkconfig --list | grep infinidb 2>/dev/null
	echo "################# /sbin/chkconfig --list | grep mysql-Calpont #################"
	echo " "
	$SUDO /sbin/chkconfig --list | grep mysql-Calpont 2>/dev/null
fi

echo " "
echo "-- fstab Configuration --"
echo " "
echo "################# cat /etc/fstab #################"
echo " "
$SUDO cat /etc/fstab 2>/dev/null

echo " "
echo "-- Server Processes --"
echo " "
echo "################# ps axu #################"
echo " "
$SUDO ps axu

echo " "
echo "-- Server Processes with resource usage --"
echo " "
echo "################# top -b -n 1 #################"
echo " "
$SUDO top -b -n 1

} > /tmp/${MODULE}_configReport.txt

exit 0