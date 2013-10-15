#! /bin/sh
# IDBCloudGetIP.sh
# Get IP Address from a Cloud environment
#
# 1. Amazon EC2

prefix=/usr/local

#get instance name from called
instanceName="$1"

ec2=`$prefix/Calpont/bin/getConfig Installation EC2_HOME`

if [ $ec2 == "unassigned" ]; then
	echo "stopped"
	exit 1
fi

java=`$prefix/Calpont/bin/getConfig Installation JAVA_HOME`
path=`$prefix/Calpont/bin/getConfig Installation EC2_PATH`

export PATH=$path
export EC2_HOME=$ec2
export JAVA_HOME=$java

# get x509 Certification and Private Key
x509Cert=`$prefix/Calpont/bin/getConfig Installation AmazonX509Certificate`
x509PriKey=`$prefix/Calpont/bin/getConfig Installation AmazonX509PrivateKey`

#get instance info
ec2-describe-instances -C $x509Cert -K $x509PriKey $instanceName > /tmp/instanceInfo 2> /dev/null

#check if running
cat /tmp/instanceInfo | grep running > /tmp/instanceStatus
if [ `cat /tmp/instanceStatus | wc -c` -eq 0 ]; then
   	echo "stopped"
	exit 1
fi

#get priviate IP Address
IpAddr=`cat /tmp/instanceInfo | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $15}'`

echo $IpAddr
exit 0
