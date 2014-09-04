#!/bin/bash
#
# configxml	set/get an entry in Calpont.xml file
#
#
case "$1" in
 setconfig)

	if  test ! $4 ; then 
	echo $"Usage: $0 setconfig section variable set-value"
	exit 1
	fi

	calbin=/usr/local/Calpont/bin

	oldvalue=`$calbin/getConfig $2 $3`

	if [ -z $oldvalue ]; then 
	echo "$2 / $3 not found in Calpont.xml"	
	exit 1
	fi

	echo "Old value of $2 / $3 is $oldvalue"

	calext=/usr/local/Calpont/etc/Calpont.xml

	seconds=`date +%s`
	cp $calext $calext.$seconds
	echo
	echo $"$calext backed up to $calext.$seconds"
	echo

	calbin=/usr/local/Calpont/bin

	oldvalue=`$calbin/getConfig $2 $3`

	echo "Old value of $2 / $3 is $oldvalue"

	$calbin/setConfig $2 $3 $4

	newvalue=`$calbin/getConfig $2 $3`

	echo "$2 / $3 now set to $newvalue"
	;;
 
 getconfig)
	if  test ! $3 ; then 
	echo $"Usage: $0 getconfig section variable"
	exit 1
	fi

	calbin=/usr/local/Calpont/bin

	value=`$calbin/getConfig $2 $3`

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
