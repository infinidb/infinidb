#!/bin/bash
#
# $Id$
#
# generic InfiniDB upgrade script.
#
# Notes: This script gets run every time InfiniDB starts up so it needs to be:
#           1) able to only do stuff once if needed and nothing if not needed
#           2) reasonably fast when there's nothing to do

# check log for error
checkForError() {
	grep ERROR /tmp/upgrade-status.log.$$ > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: /tmp/upgrade-status.log.$$"
		rm -f /tmp/error.check
		exit 1;
	fi
	rm -f /tmp/error.check
}

if [ "x$1" != xdoupgrade ]; then
	echo "Don't run this script by hand! You probably want to use install-infinidb.sh." 1>&2
	exit 1
fi

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
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

mt=`module_type`
mid=`module_id`

# for CE version
if [ -z "$mt" ]; then
	mt=pm
fi
if [ -z "$mid" ]; then
	mid=1
fi

has_um=`$installdir/bin/getConfig SystemModuleConfig ModuleCount2`
if [ -z "$has_um" ]; then
	has_um=0
fi

>/tmp/upgrade-status.log.$$
echo "mt = $mt" >>/tmp/upgrade-status.log.$$
echo "mid = $mid" >>/tmp/upgrade-status.log.$$
echo "has_um = $has_um" >>/tmp/upgrade-status.log.$$

#This upgrade only for UM or PM with no UM
if [ $has_um -eq 0 -o "x$mt" = xum ]; then
	#---------------------------------------------------------------------------
	# See if compressiontype column is in SYSCOLUMN
	#---------------------------------------------------------------------------
	echo "checking calpontsys for compressiontype..." >>/tmp/upgrade-status.log.$$
	$installdir/mysql/bin/mysql \
		--defaults-file=$installdir/mysql/my.cnf \
		--user=root $pwprompt \
		--execute='describe syscolumn;' \
		calpontsys | grep compressiontype >>/tmp/upgrade-status.log.$$ 2>&1

	#
	# Add compressiontype column to SYSCOLUMN if applicable
	#
	if [ $? -ne 0 ]; then
		echo "calpontsys needs upgrade for compressiontype" >>/tmp/upgrade-status.log.$$
		echo "added compressiontype column" >>/tmp/upgrade-status.log.$$
		cat >/tmp/idb_upgrade.sql <<EOD
alter table syscolumn add compressiontype int comment 'schema sync only';
EOD
		cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1

		checkForError

		if [ $mid -eq 1 ]; then
			echo "update compressiontype to backend" >>/tmp/upgrade-status.log.$$
			cat >/tmp/idb_upgrade.sql <<EOD
select calonlinealter('alter table syscolumn add (compressiontype int)') as xxx;
update syscolumn set compressiontype=0 where compressiontype is null;
EOD
			cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
			$installdir/mysql/bin/mysql \
				--defaults-file=$installdir/mysql/my.cnf \
				--user=root $pwprompt \
				calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1
	
			checkForError
		fi

		#
		# Verify that compressiontype was successfully added to SYSCOLUMN
		#
		rm -f /tmp/idb_upgrade.sql
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe syscolumn;' \
			calpontsys | grep compressiontype >/tmp/upgrade-status-1.log 2>&1
		rc=$?
		cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
		if [ $rc -ne 0 ]; then
			echo "FAILED adding compressiontype to SYSCOLUMN!"
			exit 1
		fi
		cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
		rm -f /tmp/upgrade-status-1.log
		if [ -z "$cnt" ]; then
			cnt=0
		fi
		if [ $cnt -ne 1 ]; then
			echo "FAILED adding compressiontype to SYSCOLUMN!"
			exit 1
		fi

		#
		# Verify that compressiontype (OID 1041) was successfully added to BRM
		#
		if [ $has_um -eq 0 ]; then
			$installdir/bin/editem -o1041 1>/tmp/upgrade-status-1.log 2>/dev/null
			rc=$?
			cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
			if [ $rc -ne 0 ]; then
				echo "FAILED adding compressiontype to BRM!"
				exit 1
			fi
			cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
			rm -f /tmp/upgrade-status-1.log
			if [ -z "$cnt" ]; then
				cnt=0
			fi
			if [ $cnt -lt 2 ]; then
				echo "FAILED adding compressiontype to BRM!"
				exit 1
			fi
		fi
	fi

	#---------------------------------------------------------------------------
	# See if autoincrement column is in SYSTABLE
	#---------------------------------------------------------------------------
	echo "checking calpontsys for autoincrement..." >>/tmp/upgrade-status.log.$$
	$installdir/mysql/bin/mysql \
		--defaults-file=$installdir/mysql/my.cnf \
		--user=root $pwprompt \
		--execute='describe systable;' \
		calpontsys | grep autoincrement >>/tmp/upgrade-status.log.$$ 2>&1

	#
	# Add autoincrement column to SYSTABLE if applicable
	#
	if [ $? -ne 0 ]; then
		echo "calpontsys needs upgrade for autoincrement" >>/tmp/upgrade-status.log.$$
		echo "add autoincrement columns" >>/tmp/upgrade-status.log.$$
		cat >/tmp/idb_upgrade.sql <<EOD
alter table systable add autoincrement int comment 'schema sync only';
EOD
		cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1

		checkForError

		if [ $mid -eq 1 ]; then
			echo "update autoincrement to backend" >>/tmp/upgrade-status.log.$$
			cat >/tmp/idb_upgrade.sql <<EOD
select calonlinealter('alter table systable add (autoincrement int)') as xxx;
update systable set autoincrement=0 where autoincrement is null;
EOD
			cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
			$installdir/mysql/bin/mysql \
				--defaults-file=$installdir/mysql/my.cnf \
				--user=root $pwprompt \
				calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1
	
			checkForError
		fi

		#
		#Verify that autoincrement was successfully added to SYSTABLE
		#
		rm -f /tmp/idb_upgrade.sql
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe systable;' \
			calpontsys | grep autoincrement >/tmp/upgrade-status-1.log 2>&1
		rc=$?
		cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
		if [ $rc -ne 0 ]; then
			echo "FAILED adding autoincrement to SYSTABLE!"
			exit 1
		fi
		cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
		rm -f /tmp/upgrade-status-1.log
		if [ -z "$cnt" ]; then
			cnt=0
		fi
		if [ $cnt -ne 1 ]; then
			echo "FAILED adding autoincrement to SYSTABLE!"
			exit 1
		fi

		#
		# Verify that autoincrement (OID 1011) was successfully added to BRM
		#
		if [ $has_um -eq 0 ]; then
			$installdir/bin/editem -o1011 1>/tmp/upgrade-status-1.log 2>/dev/null
			rc=$?
			cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
			if [ $rc -ne 0 ]; then
				echo "FAILED adding autoincrement to BRM!"
				exit 1
			fi
			cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
			rm -f /tmp/upgrade-status-1.log
			if [ "x$cnt" = x ]; then
				cnt=0
			fi
			if [ $cnt -lt 2 ]; then
				echo "FAILED adding autoincrement to BRM!"
				exit 1
			fi
		fi
	fi

	#---------------------------------------------------------------------------
	# See if nextvalue column is in SYSCOLUMN
	#---------------------------------------------------------------------------
	echo "checking calpontsys for nextvalue..." >>/tmp/upgrade-status.log.$$
	$installdir/mysql/bin/mysql \
		--defaults-file=$installdir/mysql/my.cnf \
		--user=root $pwprompt \
		--execute='describe syscolumn;' \
		calpontsys | grep nextvalue >>/tmp/upgrade-status.log.$$ 2>&1

	#
	# Add nextvalue column to SYSCOLUMN if applicable.
	# Also set old autoincrement column in SYSCOLUMN to 'n'.
	#
	if [ $? -ne 0 ]; then
		echo "calpontsys needs upgrade for nextvalue" >>/tmp/upgrade-status.log.$$
		echo "add nextvalue columns" >>/tmp/upgrade-status.log.$$
		cat >/tmp/idb_upgrade.sql <<EOD
alter table syscolumn add nextvalue bigint comment 'schema sync only';
EOD
		cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1

		checkForError

		if [ $mid -eq 1 ]; then
			echo "update nextvalue to backend" >>/tmp/upgrade-status.log.$$
			cat >/tmp/idb_upgrade.sql <<EOD
select calonlinealter('alter table syscolumn add (nextvalue bigint)') as xxx;
update syscolumn set nextvalue=1 where nextvalue is null;
update syscolumn set autoincrement='n' where autoincrement is null;
EOD
			cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
			$installdir/mysql/bin/mysql \
				--defaults-file=$installdir/mysql/my.cnf \
				--user=root $pwprompt \
				calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1
	
			checkForError
		fi

		#
		# Verify that nextvalue was successfully added to SYSCOLUMN
		#
		rm -f /tmp/idb_upgrade.sql
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe syscolumn;' \
			calpontsys | grep nextvalue >/tmp/upgrade-status-1.log 2>&1
		rc=$?
		cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
		if [ $rc -ne 0 ]; then
			echo "FAILED adding nextvalue to SYSCOLUMN!"
			exit 1
		fi
		cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
		rm -f /tmp/upgrade-status-1.log
		if [ -z "$cnt" ]; then
			cnt=0
		fi
		if [ $cnt -ne 1 ]; then
			echo "FAILED adding nextvalue to SYSCOLUMN!"
			exit 1
		fi

		#
		# Verify that nextvalue (OID 1042) was successfully added to BRM
		#
		if [ $has_um -eq 0 ]; then
			$installdir/bin/editem -o1042 1>/tmp/upgrade-status-1.log 2>/dev/null
			rc=$?
			cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
			if [ $rc -ne 0 ]; then
				echo "FAILED adding nextvalue to BRM!"
				exit 1
			fi
			cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
			rm -f /tmp/upgrade-status-1.log
			if [ "x$cnt" = x ]; then
				cnt=0
			fi
			if [ $cnt -lt 2 ]; then
				echo "FAILED adding nextvalue to BRM!"
				exit 1
			fi
		fi
	fi

	#---------------------------------------------------------------------------
	# See if systable schema and tablename columns are varchar(128).
	#---------------------------------------------------------------------------
	recreate=0
	echo "checking calpontsys.systable schema and tablename for varchar(128)..." >>/tmp/upgrade-status.log.$$
	colCount=` \
	$installdir/mysql/bin/mysql \
		--defaults-file=$installdir/mysql/my.cnf \
		--user=root $pwprompt \
		--execute='describe systable;' \
		calpontsys | egrep "schema|tablename" | grep "varchar(128)" | wc -l`
	if [ $colCount -ne 2 ]; then
		recreate=1
		echo "calpontsys needs upgrade to expand systable schema and tablename" >>/tmp/upgrade-status.log.$$
	fi
	
	#---------------------------------------------------------------------------
	# See if syscolumn schema, tablename, and columname columns are varchar(128).
	#---------------------------------------------------------------------------
	if [ $recreate -eq 0 ]; then
		echo "checking calpontsys.syscolumn schema, tablename, columnname for varchar(128)..." >>/tmp/upgrade-status.log.$$
		colCount=` \
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe syscolumn;' \
			calpontsys | egrep "schema|tablename|columnname" | grep "varchar(128)" | wc -l`
		if [ $colCount -ne 3 ]; then
			recreate=1
			echo "calpontsys needs upgrade to expand syscolumn schema, tablename, and columnname" >>/tmp/upgrade-status.log.$$
		fi
	fi

	#---------------------------------------------------------------------------
	# See if defaultvalue column in SYSCOLUMN is varchar(64)
	#---------------------------------------------------------------------------
	if [ $recreate -eq 0 ]; then
		echo "checking calpontsys for defaultvalue varchar(64)..." >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe syscolumn;' \
			calpontsys | grep defaultvalue | grep 'varchar(64)' >>/tmp/upgrade-status.log.$$ 2>&1
		if [ $? -ne 0 ]; then
			recreate=1
			echo "calpontsys needs upgrade to change defaultvalue" >>/tmp/upgrade-status.log.$$
		fi
	fi

	#
	# Change defaultvalue column to varchar(64) if applicable
	#
	if [ $recreate -ne 0 ]; then
		cat >/tmp/idb_upgrade.sql <<EOD
drop table if exists systable restrict;
drop table if exists syscolumn restrict;
EOD
		cat /tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			calpontsys </tmp/idb_upgrade.sql >>/tmp/upgrade-status.log.$$ 2>&1

		checkForError

		echo "create systable and syscolumn with schema sync only" >>/tmp/upgrade-status.log.$$
		cat $installdir/mysql/syscatalog_mysql.sql >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			calpontsys <$installdir/mysql/syscatalog_mysql.sql >>/tmp/upgrade-status.log.$$ 2>&1

		checkForError

		#
		# Verify column widths:
		# varchar(64) for syscolumn.defaultvalue
		# varchar(128) for systable (schema, tablename) and syscolumn (schema, tablename, and columnname).
		#
		rm -f /tmp/idb_upgrade.sql
		echo "verify column widths" >>/tmp/upgrade-status.log.$$
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe syscolumn;' \
			calpontsys | grep defaultvalue | grep 'varchar(64)' >/tmp/upgrade-status-1.log 2>&1
		$installdir/mysql/bin/mysql \
			--defaults-file=$installdir/mysql/my.cnf \
			--user=root $pwprompt \
			--execute='describe systable; describe syscolumn;' \
			calpontsys | egrep "schema|tablename|columnname" | grep 'varchar(128)' >>/tmp/upgrade-status-1.log 2>&1
		cat /tmp/upgrade-status-1.log >>/tmp/upgrade-status.log.$$
		cnt=`wc -l /tmp/upgrade-status-1.log | awk '{print $1}'`
		rm -f /tmp/upgrade-status-1.log
		if [ -z "$cnt" ]; then
			cnt=0
		fi
		if [ $cnt -ne 6 ]; then
			echo "FAILED width of schema, tablename, columnname, defaultvalue verification!"
			exit 1
		fi
	fi
fi

echo "OK"
