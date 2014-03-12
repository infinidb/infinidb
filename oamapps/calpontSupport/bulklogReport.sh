#! /bin/sh
#
# $Id: logReport.sh 421 2007-04-05 15:46:55Z dhill $
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

$SUDO rm -f /tmp/${MODULE}_bulklogReport.txt

{

if test -d $INSTALLDIR/data/bulk ; then
	echo " "
	echo "-- Check for Errors in Bulk Logs --"
	echo " "
	echo "################# egrep '(ERR|CRIT)' $INSTALLDIR/data/bulk/log/*.err #################"
	echo " "
	egrep '(ERR|CRIT)' $INSTALLDIR/data/bulk/log/*.err 2>/dev/null
fi

} > /tmp/${MODULE}_bulklogReport.txt

exit 0