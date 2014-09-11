#!/bin/bash
#
# $Id$
#
# generic InfiniDB Health Test script.
#
# Notes: This script can be run at varies times t check the system DB health state
#

prefix=/usr/local
pwprompt=" "
for arg in "$@"; do
        if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
                prefix="`echo $arg | awk -F= '{print $2}'`"
        elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
                password="`echo $arg | awk -F= '{print $2}'`"
                pwprompt="--password=$password"
        fi
done

test -f $prefix/Calpont/post/functions && . $prefix/Calpont/post/functions

$prefix/Calpont/mysql/bin/mysql \
                --defaults-file=$prefix/Calpont/mysql/my.cnf \
                --user=root $pwprompt \
                --execute='source /usr/local/Calpont/bin/dbhealth.sql;' > /tmp/dbhealthTest.log 2>&1

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