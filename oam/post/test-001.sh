#!/bin/bash
#
# $Id: test-001.sh 2176 2010-11-02 19:14:53Z dhill $

test -f /usr/local/Calpont/post/functions && . /usr/local/Calpont/post/functions

scrname=`basename $0`
tname="check-syscat-oids"

mt=`module_type`

#These tests only for PM
if [ "$mt" != "pm" ]; then
	exit 0
fi

#check for dbrm and data1, don't run if missing both
if firstboot; then
	if [ -d /usr/local/Calpont/data1/000.dir ]; then
		cplogger -c 50 $scrname "$tname" "missing dbrm data with existing 000.dir"
		exit 1
	else
		exit 0
	fi
else
	#check for oidbitmap file
	if oidbitmapfile; then
		cplogger -c 50 $scrname "$tname" "missing oidbitmapfile with existing current file"
		exit 1
	fi
fi

#check for both current file and OIDBITMAP file

#Make sure all syscat OIDs are present (N.B. only works for shared-everything)

cplogger -i 48 $scrname "$tname"

catoids=
catoids="$catoids 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010"
catoids="$catoids 2001 2004"
catoids="$catoids 1021 1022 1023 1024 1025 1026 1027 1028 1029 1030 1031 1032 1033 1034 1035 1036 1037 1038 1039 1040"
catoids="$catoids 2061 2064 2067 2070 2073 2076"

for oid in $catoids; do
	if [ ! -s `oid2file $oid` ]; then
		cplogger -c 50 $scrname "$tname" "could not find file for OID $oid"
		exit 1
	fi
done

cplogger -i 52 $scrname "$tname"

exit 0

