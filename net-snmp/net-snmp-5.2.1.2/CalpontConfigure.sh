#! /bin/sh
#
# $Id#
#
# configures and installs net-snmp modules that we need

prefix=/usr/local/Calpont
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

if [ ! -f Makefile ]; then
	./configure --prefix="$prefix" --with-mib-modules=disman/event-mib --with-cc=gcc \
		--with-logfile=/var/log/snmpd.log \
		--with-sys-location=Unknown \
		--with-sys-contact=root@localhost.localdomain \
		--with-default-snmp-version=3 \
		--with-persistent-directory=/var/net-snmp \
		--without-openssl \
		--with-ldflags="-Wl,-rpath -Wl,$prefix/lib"
fi

if [ ! -f snmplib/.libs/libnetsnmp.so.5.2.1 ]; then
	make
fi

if [ ! -f "$prefix/lib/libnetsnmp.so.5.2.1" ]; then
	make install
fi

