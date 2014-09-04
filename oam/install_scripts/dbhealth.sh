#!/bin/bash
#
# $Id$
#
# generic InfiniDB Health Test script.
#
# Notes: This script can be run at varies times t check the system DB health state
#

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	INFINIDB_INSTALL_DIR=/usr/local/Calpont
fi

declare -x INFINIDB_INSTALL_DIR=$INFINIDB_INSTALL_DIR

pwprompt=" "
for arg in "$@"; do
        if [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
                installdir="`echo $arg | awk -F= '{print $2}'`"
        elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
                password="`echo $arg | awk -F= '{print $2}'`"
                pwprompt="--password=$password"
        fi
done

test -f $INFINIDB_INSTALL_DIR/post/functions && . $INFINIDB_INSTALL_DIR/post/functions

$INFINIDB_INSTALL_DIR/mysql/bin/mysql \
                --defaults-file=$INFINIDB_INSTALL_DIR/mysql/my.cnf \
                --user=root $pwprompt \
                --execute="source $INFINIDB_INSTALL_DIR/bin/dbhealth.sql;" > /tmp/dbhealthTest.log 2>&1

grep OK /tmp/dbhealthTest.log > /tmp/error.check
if [ `cat /tmp/error.check | wc -c` -eq 0 ]; then
     echo "System Health Test Failed, check /tmp/dbhealthTest.log" 
     rm -f /tmp/error.check                
     exit 1;        
fi        

echo "OK"
echo "System Health Test Passed"

rm -f /tmp/error.check

exit 0
