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

echo "This is Script name  " $0		

USERNAME=`grep "^${USER}:" /etc/passwd | cut -d: -f5`
JOBID=""
TABLENAME=""
Schema =""
echo "Your Name is :" $USERNAME
echo "Your User Id  is :" $USER
Schema=$1
if [ -n "$Schema" ]; then
 echo "Schema is   " $Schema
else
 Schema="TPCH"
if [ -n "$Schema" ]; then
  echo "Schema is   " $Schema
else
  echo "Error using the script, a schema is needed! "
  echo "usage as follows: "
  echo "                bulkload.sh SCHEMA_NAME(your schema name)"
  echo "Try again! Goodbye!"
  exit
 fi
fi

  
MAXERROR=10
FORMAT=CSV
DESC="table columns definition"
NAME="column definitions for tables in $Schema"

SUFFIX=.tbl
if [ -n "$JOBID" ]; then 
      echo "INPUT JOB ID is " $JOBID
else
 JOBID=299
 echo "DEFAULT COLUMN JOB ID is " $JOBID
fi

let "JOBID2 = JOBID+1"
echo "DEFAULT INDEX JOB ID is " $JOBID2

#generate column xml file
if [ -n "$TABLENAME" ]; then
 colxml $Schema -t $TABLENAME  -j $JOBID   -d "|"   -s "$DESC" -e $MAXERROR  -n "$NAME"   -u $USER
 command="colxml $Schema -t $TABLENAME -j $JOBID   -d \"|\" -f $FORMAT   -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"   -u $USER "
 echo $command
else
 colxml $Schema  -j $JOBID   -d "|"   -s "$DESC" -e $MAXERROR  -n "$NAME"   -u $USER
 command="colxml $Schema  -j $JOBID   -d \"|\" -f $FORMAT   -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"   -u $USER "
 echo $command
fi


#generate index xml file
DESC="table index definition"
NAME="index definitions for tables in $Schema"

if [ -n "$TABLENAME" ]; then
 indxml $Schema -t $TABLENAME  -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER 
 command="indxml $Schema -t $TABLENAME -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER "
 echo $command
else
 indxml $Schema -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER
 command="indxml $Schema -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER "
 echo $command
fi

#bulk load column files
cpimport  -j  $JOBID
command="cpimport -j $JOBID"
echo $command
#bulk load index files
cpimport -c -i -j $JOBID2
command="cpimport -i -c -j $JOBID2"
echo $command
