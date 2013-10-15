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

#echo "This is Script name  " $0		
PROG_NAME=$(basename $0)

USERNAME=`grep "^${USER}:" /etc/passwd | cut -d: -f5`
JOBID=""
TABLENAME=""
Schema=""
DELIMITER="|"
MAXERROR=10
FORMAT=CSV
DESC="table columns definition"
NAME="table columns definition"


while getopts 't:j:e:s:d:p:n:hu' OPTION
do
 case ${OPTION} in
        s) Schema=${OPTARG};;
        t) TABLENAME=${OPTARG};;
        j) JOBID=${OPTARG};;
        e) MAXERROR=${OPTARG};;
        p) DESC=${OPTARG};;
        d) DELIMITER=${OPTARG};;
        n) NAME=${OPTARG};;
        h) echo "Usage: ${PROG_NAME} -s schema -j jobid [-t TableName  -e max_error_row -p description -d delimiter -n name ]"
           exit 2;;
        u) echo "Usage: ${PROG_NAME} -s schema -j jobid [-t TableName -e max_error_row -p description -d delimiter -n name  ]"
           exit 2;;
      \?)  echo  "Usage: ${PROG_NAME} -s schema -j jobid [-t TableName -e max_error_row -p description -d delimiter -n name ]"
           exit 2;;
 esac
done

if [ -n "$Schema" ]; then
 echo "Schema is   " $Schema
else
 echo "Error using the script, a schema is needed! "
 echo "usage as follows: "
 echo "Usage: ${PROG_NAME} -s schema  -j jobid [-t TableName  -p description -d delimiter -e max_error_rows -n name ]"
 echo "PLEASE ONLY INPUT SCHEMA NAME:"
  read Schema 
 if [ -n "$Schema" ]; then
  echo "Schema is   " $Schema
 else
  echo "Error using the script, a schema is needed! "
  echo "Usage: ${PROG_NAME} -s schema -j jobid [-t TableName  -p description -d delimiter -e max_error_rows -n name  ]"
  echo "Try again! Goodbye!"
  exit 2;
 fi
fi
NAME="column definitions for tables in $Schema"

if [ -n "$JOBID" ]; then 
      echo "INPUT JOB ID is " $JOBID
else
  echo "Error using the script, a jobid is needed! "
  echo "PLEASE INPUT jobid:"
   read JOBID
  if [ -n "$JOBID" ]; then
   echo "JOBID is   " $JOBID
  else
   echo "Error using the script, a jobid is needed! "
   echo "Usage: ${PROG_NAME} -s schema -j jobid [-t TableName  -s description -d delimiter -e max_error_rows -n name ]"
   echo "Try again! Goodbye!"
   exit 2;
  fi
fi
################################################################################

if [ -n "$TABLENAME" ]; then
  ./bulkloadp.sh -e $MAXERROR -s $Schema -t "$TABLENAME" -j $JOBID -p "$DESC" -d "$DELIMITER" -n "$NAME" -u $USER
  
else
  ./bulkloadp.sh -e $MAXERROR -s $Schema -j $JOBID -d "$DELIMITER" -p "$DESC" -n "$NAME" -u $USER
fi
