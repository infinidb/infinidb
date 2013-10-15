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
dbrm start
PrimProc > primproc.log &
dbbuilder 5 
cp *.xml ~/genii/writeengine/test/bulk/job
