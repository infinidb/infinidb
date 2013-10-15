#!/bin/bash

prefix=/usr/local
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

client="${prefix}/Calpont/mysql/bin/mysql --defaults-file=${prefix}/Calpont/mysql/my.cnf --user=root"

echo "
        create database calpont;
        use calpont;
        create table caltest (col1 int, col2 int) engine=infinidb;
        show create table caltest;
" > /tmp/minitest.$$
${client} < /tmp/minitest.$$ > /tmp/minitest.out.$$ 2>&1
if [ $? -ne 0 ]; then
	echo "test failed!" 1>&2
	exit 1
fi

egrep -qsi 'engine=infinidb' /tmp/minitest.out.$$
if [ $? -ne 0 ]; then
	cat /tmp/minitest.out.$$
	echo "test failed!" 1>&2
	exit 1
fi

echo "
        use calpont;
        set autocommit=0;
        insert into caltest values (1, 2);
        insert into caltest values (3, 4);
        commit;
        select * from caltest;
" > /tmp/minitest.$$

${client} < /tmp/minitest.$$
rc=$?

rm -f /tmp/*.$$

exit $rc

