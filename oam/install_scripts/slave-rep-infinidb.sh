#!/bin/bash
#
# $Id$
#
# generic InfiniDB Slave Replication script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep ERROR /tmp/slave-rep-status.log > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: /tmp/slave-rep-status.log"
		rm -f /tmp/error.check
		exit 1;
	fi
	rm -f /tmp/error.check
}

prefix=/usr/local
installdir=$prefix/Calpont
pwprompt=
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/Calpont
	elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
		password="`echo $arg | awk -F= '{print $2}'`"
		pwprompt="--password=$password"
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--masteripaddr='` -eq 15 ]; then
		masteripaddr="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--masterlogfile='` -eq 16 ]; then
		masterlogfile="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--masterlogpos='` -eq 15 ]; then
		masterlogpos="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--port='` -eq 7 ]; then
		port="`echo $arg | awk -F= '{print $2}'`"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

repUser="idbrep"
password="Calpont1"

>/tmp/slave-rep-status.log

#
# Run stop slave command
#
echo "Run stop slave command" >>/tmp/slave-rep-status.log
cat >/tmp/idb_slave-rep.sql <<EOD
stop slave;
EOD

cat /tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log

checkForError

#
# Run Change Master Command
#
echo "Run Change Master Command" >>/tmp/slave-rep-status.log
cat >/tmp/idb_slave-rep.sql <<EOD
CHANGE MASTER TO
    	MASTER_HOST='$masteripaddr',
    	MASTER_USER='$repUser',
    	MASTER_PASSWORD='$password',
    	MASTER_PORT=$port,
   	MASTER_LOG_FILE='$masterlogfile',
    	MASTER_LOG_POS=$masterlogpos;

EOD

cat /tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log

checkForError

#
# Run start slave command
#
echo "Run start slave command" >>/tmp/slave-rep-status.log
cat >/tmp/idb_slave-rep.sql <<EOD
start slave;
EOD

cat /tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log

checkForError

#
# Run SHOW SLAVE STATUS
#
echo "Run SHOW MASTER STATUS to node log" >>/tmp/slave-rep-status.log
cat >/tmp/idb_slave-rep.sql <<EOD
SHOW SLAVE STATUS
EOD

cat /tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_slave-rep.sql >>/tmp/slave-rep-status.log

checkForError

#alls good, 'OK' for success
echo "OK"

