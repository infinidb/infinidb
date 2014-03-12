#! /bin/sh
#
# $Id: resourceReport.sh 421 2007-04-05 15:46:55Z dhill $
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

$SUDO rm -f /tmp/${MODULE}_resourceReport.txt

{
echo " "
echo "******************** Resource Usage Report for ${MODULE} ********************"
echo " "

echo " "
echo "-- Shared Memory --"
echo " "
echo "################# ipcs -l #################"
echo " "
$SUDO ipcs -l

echo "################# $INSTALLDIR/bin/clearShm -n #################"
echo " "
$INSTALLDIR/bin/clearShm -n

echo " "
echo "-- Disk Usage --"
echo " "
echo "################# df -k #################"
echo " "
$SUDO df -k

echo " "
echo "-- Disk BRM Data files --"
echo " "
ls -l $INSTALLDIR/data1/systemFiles/dbrm 2> /dev/null
ls -l $INSTALLDIR/dbrm 2> /dev/null

echo "################# cat $INSTALLDIR/data1/systemFiles/dbrm/BRM_saves_current #################"
echo " "
cat $INSTALLDIR/data1/systemFiles/dbrm/BRM_saves_current 2> /dev/null

echo " "
echo "-- View Table Locks --"
echo " "
echo "################# cat bin/viewtablelock #################"
echo " "
$INSTALLDIR/bin/viewtablelock 2> /dev/null

echo " "
echo "-- BRM Extent Map  --"
echo " "
echo "################# bin/editem -i #################"
echo " "
$INSTALLDIR/bin/editem -i 2>/dev/null

} > /tmp/${MODULE}_resourceReport.txt

exit 0