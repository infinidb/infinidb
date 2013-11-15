#! /bin/sh
# IDBInstanceCmds.sh
# get-local-instance-ID, get-zone, getPrivateIP from a Cloud environment
#
# 1. Amazon EC2

prefix=/usr/local

#check command
if [ "$1" = "" ]; then
	echo "Enter Command Name: {launchInstance|getInstance|getZone|getPrivateIP|getType|getKey|getAMI|getType|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup}
}"
	exit 1
fi

if [ "$1" = "getPrivateIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "launchInstance" ]; then
	if [ "$2" = "" ]; then
		IPaddress="unassigned"
	else
		IPaddress="$2"
	fi
	if [ "$3" = "" ]; then
		instanceType="unassigned"
	else
		instanceType="$3"
	fi
	if [ "$4" = "" ]; then
		group="unassigned"
	else
		group="$4"
	fi
fi

if [ "$1" = "terminateInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "stopInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "startInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "assignElasticIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	else
		instanceName="$2"
	fi
	if [ "$3" = "" ]; then
		echo "Enter Elastic IP Address"
		exit 1
	else
		IPAddress="$3"
	fi
fi

if [ "$1" = "deassignElasticIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Elastic IP Address"
		exit 1
	else
		IPAddress="$2"
	fi
fi


test -f /usr/local/Calpont/post/functions && . /usr/local/Calpont/post/functions

ec2=`$prefix/Calpont/bin/getConfig Installation EC2_HOME`

if [ $ec2 == "unassigned" ]; then
	if [ "$1" = "getPrivateIP" ]; then
		echo "stopped"
		exit 1
	else
       echo "unknown"
        exit 1
	fi
fi

java=`$prefix/Calpont/bin/getConfig Installation JAVA_HOME`
path=`$prefix/Calpont/bin/getConfig Installation EC2_PATH`

export PATH=$path
export EC2_HOME=$ec2
export JAVA_HOME=$java

# get x509 Certification and Private Key and region
x509Cert=`$prefix/Calpont/bin/getConfig Installation AmazonX509Certificate`
x509PriKey=`$prefix/Calpont/bin/getConfig Installation AmazonX509PrivateKey`
Region=`$prefix/Calpont/bin/getConfig Installation AmazonRegion`
subnet=`$prefix/Calpont/bin/getConfig Installation AmazonSubNetID`

if test ! -f $x509Cert ; then
	echo "FAILED: missing x509Cert : $x509Cert"
	exit 1
fi

if test ! -f $x509PriKey ; then
	echo "FAILED: missing x509PriKey : $x509PriKey"
	exit 1
fi

#default instance to null
instance=""

getInstance() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	# first get local IP Address
	localIP=`ifconfig eth0 | grep "inet addr:" | awk '{print substr($2,6,20)}'`

	#get local Instance ID
	instance=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
	echo $instance
	return
}

getInstancePrivate() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	# first get local IP Address
	localIP=`ifconfig eth0 | grep "inet addr:" | awk '{print substr($2,6,20)}'`

	#get local Instance ID
	instance=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
	return
}


getZone() {
	#get from Calpont.xml if it's there, if not, get from instance then store
	zone=`$prefix/Calpont/bin/getConfig Installation AmazonZone`

	if [ "$zone" = "unassigned" ]; then
		#get local Instance ID
		getInstancePrivate >/dev/null 2>&1
		#get zone
		if [ "$subnet" == "unassigned" ]; then
			zone=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $11}'`
		else
			zone=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $10}'`
		fi
		$prefix/Calpont/bin/setConfig Installation AmazonZone $zone
	fi

	echo $zone
	return
}

getPrivateIP() {
	#get instance info
	ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region $instanceName > /tmp/instanceInfo_$instanceName 2>&1
	
	#check if running or terminated
	cat /tmp/instanceInfo_$instanceName | grep running > /tmp/instanceStatus_$instanceName
	if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -eq 0 ]; then
		# not running
		cat /tmp/instanceInfo_$instanceName | grep terminated > /tmp/instanceStatus_$instanceName
		if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
			echo "terminated"
			exit 1
		else
			cat /tmp/instanceInfo_$instanceName | grep shutting-down > /tmp/instanceStatus_$instanceName
			if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
				echo "terminated"
				exit 1
			else
				echo "stopped"
				exit 1
			fi
		fi
	fi
	
	#running, get priviate IP Address
	if [ "$subnet" == "unassigned" ]; then
		IpAddr=`cat /tmp/instanceInfo_$instanceName | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $15}'`
	else
		IpAddr=`cat /tmp/instanceInfo_$instanceName | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $13}'`
	fi

	echo $IpAddr
	exit 0
}

getType() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Type
	if [ "$subnet" == "unassigned" ]; then
		instanceType=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $9}'`
	else
		instanceType=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $8}'`
	fi

	echo $instanceType
	return
}

getKey() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Key
	if [ "$subnet" == "unassigned" ]; then
		key=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
	else
		key=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $6}'`
	fi

	echo $key
	return
}

getAMI() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get AMI
	ami=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $3}'`

	echo $ami
	return
}

getGroup() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get group 
	if [ "$subnet" == "unassigned" ]; then
		group=`ec2-describe-instances -C $x509Cert -K $x509PriKey $instance --region $Region |  grep -m 1 RESERVATION | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $4}'`
	else
		group=`ec2-describe-instances -C $x509Cert -K $x509PriKey $instance --region $Region |  grep -m 1 GROUP | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
	fi

	echo $group
	return
}

getProfile() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Type
	if [ "$subnet" == "unassigned" ]; then
		instanceProfile=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $23}'`
	else
		instanceProfile=`ec2-describe-instances -C $x509Cert -K $x509PriKey --region $Region | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $21}'`
	fi

	echo $instanceProfile
	return
}

launchInstance() {
	#get publickey
	getKey >/dev/null 2>&1
	if [ "$group" = "unassigned" ]; then
		#get group
		getGroup >/dev/null 2>&1
	fi
	#get AMI
	getAMI >/dev/null 2>&1
	#get Zone
	getZone >/dev/null 2>&1
	if [ "$instanceType" = "unassigned" ]; then
		#get type
		getType >/dev/null 2>&1
	fi
	#get AMI Profile
	getProfile >/dev/null 2>&1

	if [ "$subnet" == "unassigned" ]; then
		#NOT VPC
		if [ "$instanceProfile" = "" ] || [ "$instanceProfile" = "default" ]; then
			newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone --region $Region $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		else
			newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		fi
	else	# VPC
		if [ "$instanceProfile" = "" ] || [ "$instanceProfile" = "default" ]; then
			if [ "$group" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone  --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone  --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -t $instanceType -z $zone --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -t $instanceType -z $zone --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			fi
		else
			if [ "$group" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -C $x509Cert -K $x509PriKey -k $key -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			fi
		fi
	fi
	echo $newInstance
	return
}

terminateInstance() {
	#terminate Instance
	ec2-terminate-instances -C $x509Cert -K $x509PriKey --region $Region $instanceName > /tmp/termInstanceInfo_$instanceName 2>&1
	return
}

stopInstance() {
	#terminate Instance
	ec2-stop-instances -C $x509Cert -K $x509PriKey --region $Region $instanceName > /tmp/stopInstanceInfo_$instanceName 2>&1
	return
}

startInstance() {
	#terminate Instance
	ec2-start-instances -C $x509Cert -K $x509PriKey --region $Region $instanceName > /tmp/startInstanceInfo_$instanceName 2>&1

	cat /tmp/startInstanceInfo_$instanceName | grep INSTANCE > /tmp/startInstanceStatus_$instanceName
	if [ `cat /tmp/startInstanceStatus_$instanceName | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/startInstanceInfo_$instanceName"
		exit 1
	fi
	echo "Success"
	exit 0
}

assignElasticIP() {
	#terminate Instance
	ec2-associate-address -C $x509Cert -K $x509PriKey -i $instanceName $IPAddress > /tmp/assignElasticIPInfo_$IPAddress 2>&1

	cat /tmp/assignElasticIPInfo_$IPAddress | grep ADDRESS > /tmp/assignElasticIPStatus_$IPAddress
	if [ `cat /tmp/assignElasticIPStatus_$IPAddress | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/assignElasticIPStatus_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

deassignElasticIP() {
	#terminate Instance
	ec2-disassociate-address -C $x509Cert -K $x509PriKey $IPAddress > /tmp/deassignElasticIPInfo_$IPAddress 2>&1

	cat /tmp/deassignElasticIPInfo_$IPAddress | grep ADDRESS > /tmp/deassignElasticIPStatus_$IPAddress
	if [ `cat /tmp/deassignElasticIPStatus_$IPAddress | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/deassignElasticIPStatus_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

case "$1" in
  getInstance)
  	getInstance
	;;
  getZone)
  	getZone
	;;
  getPrivateIP)
  	getPrivateIP
	;;
  getType)
  	getType
	;;
  getKey)
  	getKey
	;;
  getAMI)
  	getAMI
	;;
  getType)
  	getType
	;;
  launchInstance)
  	launchInstance
	;;
  terminateInstance)
  	terminateInstance
	;;
  stopInstance)
  	stopInstance
	;;
  startInstance)
  	startInstance
	;;
  assignElasticIP)
  	assignElasticIP
	;;
  deassignElasticIP)
  	deassignElasticIP
	;;
  getProfile)
  	getProfile
	;;
  getGroup)
  	getGroup
	;;
  *)
	echo $"Usage: $0 {launchInstance|getInstance|getZone|getPrivateIP|getType|getKey|getAMI|getType|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup}"
	exit 1
esac

exit $?
