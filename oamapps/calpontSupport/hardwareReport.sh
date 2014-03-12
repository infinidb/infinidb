#! /bin/sh
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

$SUDO rm -f /tmp/${MODULE}_hardwareReport.txt

{
echo " "
echo "******************** Hardware Report for ${MODULE} ********************"
echo " "

echo "-- Server OS Version --"
echo " "
echo "################# cat /proc/version #################"
echo " "
$SUDO cat /proc/version 2>/dev/null
echo " "
echo "################# uname -a #################"
echo " "
uname -a
echo " "
echo "################# cat /etc/issue #################"
echo " "
cat /etc/issue 2>/dev/null
echo " "
echo "run os_check.sh"
echo " "
echo "################# /bin/os_check.sh #################"
echo " "
$INSTALLDIR/bin/os_check.sh 2>/dev/null

echo " "
echo "-- Server Uptime --"
echo " "
echo "################# uptime #################"
echo " "
uptime

echo " "
echo "-- Server cpu-info --"
echo " "
echo "################# cat /proc/cpuinfo #################"
echo " "
$SUDO cat /proc/cpuinfo 2>/dev/null

echo " "
echo "-- Server memory-info --"
echo " "
echo "################# cat /proc/meminfo #################"
echo " "
$SUDO cat /proc/meminfo 2>/dev/null

echo " "
echo "-- Server mounts --"
echo " "
echo "################# cat /proc/mounts #################"
echo " "
$SUDO cat /proc/mounts 2>/dev/null

echo " "
echo "-- Server Disk Scheduler for Calpont Mounts --"
echo " "
for scsi_dev in `mount | awk '/mnt\/tmp/ {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do
        echo '/dev/'$scsi_dev ' scheduler setup is'
        cat /sys/block/$scsi_dev/queue/scheduler 2>/dev/null
done
for scsi_dev in `mount | awk '/Calpont\/data/ {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do
        if [ $scsi_dev != "local" ] ; then
                echo '/dev/'$scsi_dev ' scheduler setup is'
                cat /sys/block/$scsi_dev/queue/scheduler 2>/dev/null
        fi
done

echo " "
echo "-- Server Ethernet Configuration --"
echo " "
echo "################# ifconfig -a #################"
echo " "
ifconfig -a 2>/dev/null

} > /tmp/${MODULE}_hardwareReport.txt

exit 0