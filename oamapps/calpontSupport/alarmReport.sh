#! /bin/sh
#
# $Id: logReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        SERVER=$1
else
        SERVER="localhost"
fi

if [ $2 ] ; then
        DATE=$2
else
        DATE=" "
fi

rm -f /tmp/logReport.log

{
echo " "
echo "******************** Alarm Report for $SERVER ********************"
echo " "

echo "-- Today's Alarms --"
echo " "
cat /var/log/Calpont/alarm.log 2>/dev/null

if test -f /var/log/Calpont/archive/alarm.log-$DATE ; then
	echo "-- Archived Alarms --"
	echo " "
	cat /var/log/Calpont/archive/alarm.log-$DATE 2>/dev/null
fi

} > /tmp/logReport.log

exit 0