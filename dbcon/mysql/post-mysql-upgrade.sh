#!/bin/bash
#

prefix=/usr/local
installdir=$prefix/Calpont
rpmmode=install
pwprompt=" "

for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/Calpont
	elif [ `expr -- "$arg" : '--rpmmode='` -eq 10 ]; then
		rpmmode="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
		password="`echo $arg | awk -F= '{print $2}'`"
		pwprompt="--password=$password"
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

df=$installdir/mysql/my.cnf
lf=/tmp/mysql_post-up.log

> $lf

$installdir/mysql/bin/mysql_upgrade --defaults-file=$df >>$lf 2>&1

$installdir/mysql/bin/mysql --defaults-file=$df --force --user=root $pwprompt mysql >>$lf 2>&1 <<EOD
alter table user add column (password_expired enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N');
alter table user add column Create_tablespace_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' after Trigger_priv;
EOD

#this needs to be done twice, apparently...
$installdir/mysql/bin/mysql_upgrade --defaults-file=$df >>$lf 2>&1

