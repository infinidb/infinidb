#!/bin/bash

#This is the procedure for running bulkload using cpimport program
#Usage of this program :
#The necessary input parameter is the schema name
#For example: bulkload.sh TPCH

#A table name and a Job ID can be entered by user when it is prompted or they can be skipped by hitting enter key
#When the table name is skipped, ALL of the columns and index in ALL of the tables in the schema will be loaded 

#When table name is entered, All of the columns and indexes in the entered table will be loaded
#Job ID will determine the names of the two xml files. For example, job id 100 will generate Job_100.xml for columns and Job_101 for index xml file. Job id for index xml file is the entered job id +1
#if the job id is skipped, the default job ids are 299 and 300 for column and index files
#There are two xml files will be generated which reside in bulkroot directory under subdirectory job
#For example, the job directory may look like /usr/local/Calpont/test/bulk/job 

# Set up a default search path.
PATH="$HOME/genii/export/bin:.:/sbin:/usr/sbin:/bin:/usr/bin:/usr/X11R6/bin"
export PATH
echo "CALPONT_CONFIG_FILE=" $CALPONT_CONFIG_FILE
awk '/BulkRoot/ { sub(/<BulkRoot>/,"",$0); sub(/<\/BulkRoot>/,"",$0); sub(/" "/,"",$0);print $0 > "tmp.txt"}' $CALPONT_CONFIG_FILE 
sed -e 's/ *//g' tmp.txt > out.txt

BulkRoot=$(cat out.txt)
echo "BulkRoot=" $BulkRoot

awk '/DBRoot1/ { sub(/<DBRoot1>/,"",$0); sub(/<\/DBRoot1>/,"",$0); sub(/" "/,"",$0);print $0 > "tmp.txt"}' $CALPONT_CONFIG_FILE 
sed -e 's/ *//g' tmp.txt > out.txt
DBRoot=$(cat out.txt)
echo "DBRoot=" $DBRoot
rm -rf out.txt tmp.txt

killall -q -u $USER PrimProc
killall -q -u $USER ExeMgr
killall -q -u $USER DDLProc
killall -q -u $USER DMLProc
