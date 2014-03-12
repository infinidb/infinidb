#! /bin/sh
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

if [ $USER = "hdfs" ]; then
	SUDO=" "
else
	SUDO="sudo -u hdfs"
fi

sudo rm -f /tmp/hdfsReport.txt

{
echo 
echo "****************************** HDFS REPORT ********************************"
echo 
echo "-- Hadoop version --"
echo 
echo "################# hadoop version  #################"
echo
$SUDO hadoop version

echo 
echo "-- Data File Plugin --"
echo 
echo "######### $INSTALLDIR/bin/getConfig SystemConfig DataFilePlugin ##########"
echo
sudo $INSTALLDIR/bin/getConfig SystemConfig DataFilePlugin

echo
echo
echo "-- Hadoop Configuration File --"
echo
echo "################ core-site.xml ################"
echo
cat $HADOOP_CONF_DIR/core-site.xml

echo
echo "################ hdfs-site.xml ################"
echo
cat $HADOOP_CONF_DIR/hdfs-site.xml

echo
echo "-- Hadoop Health Check --"
echo 
echo "################# hdfs dfsadmin -report #################"
echo 
$SUDO hadoop dfsadmin -report 2>/dev/null

echo 
echo "-- HDFS check --"
echo 
echo "################# hdfs fsck $INSTALLDIR #################"
echo 
$SUDO hadoop fsck $INSTALLDIR 2>/dev/null
} > /tmp/hadoopReport.txt

exit 0
