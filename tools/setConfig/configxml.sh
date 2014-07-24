#!/bin/bash
#
# configxml	set/get an entry in Calpont.xml file
#
#

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	test -f /etc/default/infinidb && . /etc/default/infinidb
fi

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	INFINIDB_INSTALL_DIR=/usr/local/Calpont
fi

export INFINIDB_INSTALL_DIR=$INFINIDB_INSTALL_DIR

InstallDir=$INFINIDB_INSTALL_DIR

if [ $InstallDir != "/usr/local/Calpont" ]; then
	export PATH=$InstallDir/bin:$InstallDir/mysql/bin:/bin:/usr/bin
	export LD_LIBRARY_PATH=$InstallDir/lib:$InstallDir/mysql/lib/mysql
fi

case "$1" in
 setconfig)

	if  test ! $4 ; then 
		echo $"Usage: $0 setconfig section variable set-value"
		exit 1
	fi

	oldvalue=$($InstallDir/bin/getConfig $2 $3)

	if [ -z $oldvalue ]; then 
		echo "$2 / $3 not found in Calpont.xml"	
		exit 1
	fi

	echo "Old value of $2 / $3 is $oldvalue"

	calxml=$InstallDir/etc/Calpont.xml

	seconds=$(date +%s)
	cp $calxml $calxml.$seconds
	echo
	echo "$calxml backed up to $calxml.$seconds"
	echo

	oldvalue=$($InstallDir/bin/getConfig $2 $3)

	echo "Old value of $2 / $3 is $oldvalue"

	$InstallDir/bin/setConfig $2 $3 $4

	newvalue=$($InstallDir/bin/getConfig $2 $3)

	echo "$2 / $3 now set to $newvalue"
	;;
 
 getconfig)
	if  test ! $3 ; then 
		echo $"Usage: $0 getconfig section variable"
		exit 1
	fi

	value=$($InstallDir/bin/getConfig $2 $3)

	if [ -z $value ]; then 
		echo "$2 / $3 not found in Calpont.xml"	
		exit 1
	fi

	echo "Current value of $2 / $3 is $value"	
	;;
 
 *)
	echo $"Usage: $0 {setconfig|getconfig} section variable set-value"
	exit 1

esac
# vim:ts=4 sw=4:

