#!/bin/bash
#
# This script does the following:
#   1) Executes supplied SQL script on reference database and captures output to file
#   2) Executes supplied SQL script on test database and captures output to file
#   3) diff both output files and captures different to file
#
#$1 = Test database name
#$2 = Ref  server name
#$3 = Ref  database name
#$4 = Ref  user name
#$5 = Ref  user password
#$6 = SQL script to execute
#
   logFileName=`basename $6`
#
#  Execute script on reference database
#
   if [ $2 != "NA" ]; then
      mysql $3 -h$2 -u$4 -p$5 <$6 > $logFileName.ref.log
   fi
#
#  Execute script on test database
#
   /usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -f -u root $1 <$6 > $logFileName.test.log 2>&1
   diff $logFileName.ref.log $logFileName.test.log > $logFileName.diff.log
