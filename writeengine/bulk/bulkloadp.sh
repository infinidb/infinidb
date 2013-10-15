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
PROG_NAME=$(basename $0)
SUFFIX=.tbl
TABLENAME=""
while getopts 't:j:e:s:d:p:n:u:h' OPTION
do
 case ${OPTION} in
        s) Schema=${OPTARG};;
        t) TABLENAME=${OPTARG};;           
        j) JOBID=${OPTARG};;
        e) MAXERROR=${OPTARG};;
        p) DESC=${OPTARG};;
        d) DELIMITER=${OPTARG};;
        n) NAME=${OPTARG};;
        u) USER=${OPTARG};;
        h) echo "Options: ${PROG_NAME} -s schema -j jobid [-t TableName  -e max_error_row -p description -d delimiter -n name -u user]"
           exit 2;;
      \?)  echo  "Options: ${PROG_NAME} -s schema -j jobid [-t TableName -e max_error_row -s description -d delimiter -n name -u user]"
           exit 2;;
 esac
done

#generate column xml file
echo "MAXERROR in $PROG_NAME =" $MAXERROR
echo "JOBID in $PROG_NAME =" $JOBID
echo "Schema is " $Schema
echo "DESC is " $DESC
echo "DELIMITER =" $DELIMITER
echo "TABLENAME is " $TABLENAME
echo "NAME is " $NAME

if [ -n "$TABLENAME" ]; then
 ./colxml $Schema -t $TABLENAME  -j $JOBID   -d $DELIMITER   -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER
if [ "$?" <> "0" ]; then
        echo "Error in colxml !" 1>&2
        exit 1
fi
command="colxml $Schema -t $TABLENAME -j $JOBID   -d $DELIMITER   -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"   -u \"$USER\" "
 echo $command
else
 ./colxml $Schema  -j $JOBID   -d $DELIMITER   -s "$DESC" -e $MAXERROR  -n "$NAME" -u $USER
if [ "$?" <> "0" ]; then
        echo "Error in colxml !" 1>&2
        exit 1
fi
 command="colxml $Schema  -j $JOBID   -d "$DELIMITER"   -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"   -u \"$USER\" "
 echo $command
fi

#generate index xml file
DESC="table index definition"
NAME="index definitions for tables in $Schema"
let "JOBID2 = JOBID+1"
echo "DEFAULT INDEX JOB ID is " $JOBID2
if [ -n "$TABLENAME" ]; then
 ./indxml $Schema -t $TABLENAME  -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER 
if [ "$?" <> "0" ]; then
        echo "Error in indxml !" 1>&2
        exit 1
fi

 command="indxml $Schema -t $TABLENAME -j $JOBID2 -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"  -u \"$USER\" "
 echo $command

else
 ./indxml $Schema -j $JOBID2 -s "$DESC" -e $MAXERROR  -n "$NAME"  -u $USER
if [ "$?" <> "0" ]; then
        echo "Error in colxml !" 1>&2
        exit 1
fi

 command="indxml $Schema -j $JOBID2 -s \"$DESC\" -e $MAXERROR  -n \"$NAME\"  -u \"$USER\" "
 echo $command
fi
#get bulkroot
if [ -n "$CALPONT_CONFIG_FILE" ]; then
  echo "CALPONT_CONFIG_FILE=" $CALPONT_CONFIG_FILE
elif [ -z "$CALPONT_CONFIG_FILE"]; then
  CALPONT_CONFIG_FILE="/usr/local/Calpont/etc/Calpont.xml"
  echo "CALPONT_CONFIG_FILE=" $CALPONT_CONFIG_FILE
else
  CALPONT_CONFIG_FILE="/usr/local/Calpont/etc/Calpont.xml"
  echo "CALPONT_CONFIG_FILE=" $CALPONT_CONFIG_FILE
fi

awk '/BulkRoot/ { sub(/<BulkRoot>/,"",$0); sub(/<\/BulkRoot>/,"",$0); sub(/" "/,"",$0);print $0 > "tmp.txt"}' $CALPONT_CONFIG_FILE 
sed -e 's/ *//g' tmp.txt > out.txt

BulkRoot=$(cat out.txt)
echo "BulkRoot=" $BulkRoot
rm -rf out.txt tmp.txt

#bulk load column files
./cpimport  -j  $JOBID
command="cpimport -j $JOBID"
echo $command
#bulk load parallel index files
#./splitidx -j $JOBID2
#IDX_SHELL_SCRIPT="$BulkRoot/process/Job_$JOBID2.sh"
#chmod +x $IDX_SHELL_SCRIPT
#echo " run parallel loading $IDX_SHELL_SCRIPT"
#$IDX_SHELL_SCRIPT



