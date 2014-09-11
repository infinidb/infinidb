#!/bin/bash

prefix=/usr/local
builddir=
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--builddir='` -eq 11 ]; then
		builddir="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

if [ -z "$builddir" ]; then
	echo "I really need a builddir to continue!" 1>&2
	exit 1
fi

if [ ! -d ${builddir}/Calpont ]; then
	echo "I did't find a Calpont dir in ${builddir}!" 1>&2
	exit 1
fi

# stop any current procs
if [ -x ${prefix}/Calpont/bin/infinidb ]; then
	${prefix}/Calpont/bin/infinidb stop
fi

# really stop current procs
sleep 5
for proc in DMLProc DDLProc ExeMgr PrimProc controllernode workernode; do
	pkill -9 $proc
	sleep 1
done
if [ -x ${prefix}/Calpont/mysql/mysql-Calpont ]; then
	${prefix}/Calpont/mysql/mysql-Calpont stop
fi

# cleanup

#    remove shm segs
if [ -x ${prefix}/Calpont/bin/clearShm ]; then
	${prefix}/Calpont/bin/clearShm stop
fi
#    remove Calpont dir
/usr/local/bin/rootStuff
rm -rf ${prefix}/Calpont 2>/dev/null
/usr/local/bin/rootStuff

#    (we'll leave the logging stuff in place for now)

# install the binaries
tar -C ${builddir} -cf - Calpont | tar -C ${prefix} -xf -
if [ $? -ne 0 ]; then
	echo "There was a problem installing the binaries!" 1>&2
	exit 1
fi
#chown -R root.root ${prefix}/Calpont
find ${prefix}/Calpont -type d | xargs chmod +rx
find ${prefix}/Calpont -type f | xargs chmod +r

#fix the port numbers
sed -i -e 's/port.*=.*3306/port=14406/' ${prefix}/Calpont/mysql/my.cnf

# configure the s/w
if [ ! -x ${prefix}/Calpont/bin/install-infinidb.sh ]; then
	echo "There was a problem configuring the s/w!" 1>&2
	exit 1
fi
${prefix}/Calpont/bin/install-infinidb.sh 2>/dev/null

# really install MySQL
${prefix}/Calpont/mysql/bin/mysql_install_db --defaults-file=${prefix}/Calpont/mysql/my.cnf >/dev/null

#fix pp blocks
sed -i -e 's?<NumBlocksPct>.*</NumBlocksPct>?<NumBlocksPct>46</NumBlocksPct>?' ${prefix}/Calpont/etc/Calpont.xml

# start the system
export SLOWSTART=1
${prefix}/Calpont/bin/infinidb start

#really start MySQL
${prefix}/Calpont/mysql/mysql-Calpont start
sleep 10
${prefix}/Calpont/mysql/install_calpont_mysql.sh
${prefix}/Calpont/bin/upgrade-infinidb.sh doupgrade

# perform the tests
if [ ! -x ${builddir}/mini-tests.sh ]; then
	echo "There was a problem trying to start testing the s/w!" 1>&2
	exit 1
fi
${builddir}/mini-tests.sh --prefix=${prefix}
if [ $? -ne 0 ]; then
	echo "There were problems running the tests!" 1>&2
	exit 1
fi

# stop the system
${prefix}/Calpont/bin/infinidb stop
${prefix}/Calpont/mysql/mysql-Calpont stop

exit 0

