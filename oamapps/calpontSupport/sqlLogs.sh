#!/bin/bash

#
# Script that does analysis on SQL statements from an InfiniDB debug log.
#
DB=idb_idb_sqllogs
TABLE=statements

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	INFINIDB_INSTALL_DIR=/usr/local/Calpont
fi

declare -x INFINIDB_INSTALL_DIR=$INFINIDB_INSTALL_DIR

if [ $INFINIDB_INSTALL_DIR != "/usr/local/Calpont" ]; then
	export PATH=$INFINIDB_INSTALL_DIR/bin:$INFINIDB_INSTALL_DIR/mysql/bin:/bin:/usr/bin
	export LD_LIBRARY_PATH=$INFINIDB_INSTALL_DIR/lib:$INFINIDB_INSTALL_DIR/mysql/lib/mysql
fi


if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="$INFINIDB_INSTALL_DIR/mysql/bin/mysql --defaults-file=$INFINIDB_INSTALL_DIR/mysql/my.cnf -u root"
        export MYSQLCMD
fi

main()
{
	if [ "$option" == "usage" ]; then
		usage
		exit
	elif [ "$option" == "create" ]; then
		if [ -f $logFile ]; then
			create
		else
			echo ""
			usage
			echo ""
			echo "*** $logFile not found. Pleae specify a valid debug log file.***"
			echo ""
			exit
		fi
	elif [ "$option" == "list" ]; then
		list
	elif [ "$option" == "listAll" ]; then
		listAll
	elif [ "$option" == "active" ]; then
		listActive
	elif [ "$option" == "activeAll" ]; then
		listActiveAll
	else
		echo ""
		usage
		echo "*** $option is not a valid option. ***"
		echo ""
		exit	
	fi
	
}

usage ()
{
        echo "
        This script can be used to analyze select statements from an InfiniDB debug log.
        
        Usage:
        
        ./sqlLogs.sh create [debug log file name - default:debug.log]
                  Creates an $DB.$TABLE table with the select statements from the debug log.  Must be run before other options can be used.
        
        ./sqlLogs.sh list
                  Lists the SQL statements and run times showing the following:
                  id                 - statement id in sequential order based on the start time
                  starttime          - start time of the statement
                  endtime            - end time of the statement
                  runtime            - total run time of the statement
                  sessionid          - the MySQL session id for the statement
                  sessionstatementid - the sequence of the statement within the session
        
        ./sqlLogs.sh listAll
          Same as list with the addition of the SQL statement.
        
        ./sqlLogs.sh active timestamp
                  Example:  ./sqlLogs.sh active 00:35:50.291408
                  Lists the id, starttime, endtime, runtime, timeActive, sessionid, and sessionstatementid for the sql statements
                  that were active at the given timestamp.
        
        ./sqlLogs.sh activeAll timestamp
                  Same as listActive with the addition of the SQL statement.
      	"

}

create ()
{
	echo ""
	echo "Step 1 of 4.  Building import file with Start SQL log entries."
	grep "Start SQL" $logFile | grep -v syscolumn | grep -v systable | awk -F '|' '{print NR "|" substr($1,8,6) substr($1,length($1)-9,9) "|" $2 "|" $5}' |
	sort -t '|' -n -k 3 -k 1 |
	awk -F '|' '
	{
	if(NR == 1)
	{
	        prevSession=$3;
	        val=1;
	}
	else if(prevSession == $3)
	        val++;
	else
	{
	        val=1;
	        prevSession=$3;
	}
	print $0 "|" val "|"
	}' | sort -t '|' -n -k 1 > /tmp/idbtmp.tbl

	echo "Step 2 of 4.  Populating $DB.start table with Start SQL log entries."
	sql="
	create database if not exists $DB;
	use $DB;
	drop table if exists start;
	CREATE TABLE start (
	  id int,
	  time char(20),
	  sessionid int,
	  statement varchar(8000),
	  sessionStatementId int
	) ENGINE=MyISAM ;
	create index start_idx on start (sessionid, sessionStatementId);
	load data infile '/tmp/idbtmp.tbl' into table start fields terminated by '|';
	"
	$MYSQLCMD -e "$sql"

	echo "Step 3 of 4.  Building import file with End SQL log entries."
	grep "End SQL" $logFile | grep -v "2147483" | awk -F '|' '{print NR "|" substr($1,8,6) substr($1,length($1)-9,9) "|" $2}' |
	sort -t '|' -n -k 3 -k 1 |
	awk -F '|' '
	{
	if(NR == 1)
	{
	        prevSession=$3;
	        val=1;
	}
	else if(prevSession == $3)
	        val++;
	else
	{
	        val=1;
	        prevSession=$3;
	}
	print $0 "|" val "|"
	}' | sort -t '|' -n -k 1 > /tmp/idbtmp.tbl
	 
	echo "Step 4 of 4.  Populating $DB.stop table with End SQL log entries."
	sql="
	drop table if exists stop;
	CREATE TABLE stop (
	  id int,
	  time char(20),
	  sessionid int,
	  sessionStatementId int
	) ENGINE=MyISAM ;
	create index stop_idx on stop (sessionid, sessionStatementId);
	load data infile '/tmp/idbtmp.tbl' into table stop fields terminated by '|';
	"
	$MYSQLCMD $DB -e "$sql;"

	echo "Step 5 of 5.  Populating $DB.$TABLE table."
	sql="
	drop table if exists $TABLE;
	create table $TABLE as 
	(select 
	a.id id, a.time starttime, b.time endtime, substr(timediff(b.time, a.time), 1, 30) runTime, a.sessionid sessionId, a.sessionstatementid sessionStatementId, a.statement statement 
	from start a left join stop b 
	on a.sessionid = b.sessionid and a.sessionstatementid = b.sessionstatementid);
	"
	$MYSQLCMD $DB -e "$sql"

	echo "All done."
	echo ""
}

list() {
	sql="select id, starttime, endtime, runtime, sessionid, sessionstatementid from statements;" 
	$MYSQLCMD $DB -vvv -e "$sql"
}

listAll() 
{
	sql="select id, starttime, endtime, runtime, sessionid, sessionstatementid, trim(statement) statement from statements;" 
	$MYSQLCMD $DB -vvv -e "$sql"
}

listActive()
{
	dtm=$parm2
	sql="
		select id, starttime, endtime, runtime, timediff('$dtm', starttime) timeActive, sessionid, sessionstatementid from statements 
		where starttime <= '$dtm' and (endtime is null or endtime > '$dtm');
		" 
	$MYSQLCMD $DB -vvv -e "$sql"
}

listActiveAll()
{
	dtm=$parm2
	sql="
		select id, starttime, endtime, runtime, timediff('$dtm', starttime) timeActive, sessionid, sessionstatementid, trim(statement) statement from statements 
		where starttime <= '$dtm' and (endtime is null or endtime > '$dtm');
		" 
	$MYSQLCMD $DB -vvv -e "$sql"
}

if [ $# -lt 1 ]; then
	usage
	exit
else
	option=$1
	parm2=$2
	logFile=debug.log
	if [ $# -ge 2 ]; then
		logFile=$2
	fi
fi
	
main
