#!/bin/bash
#
# $Id: startupTests.sh 2937 2012-05-30 18:17:09Z rdempsey $
#
# startupTests - perform sanity testing on system DB at system startup time
#				 called by Process-Monitor

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	test -f /etc/default/infinidb && . /etc/default/infinidb
fi

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	INFINIDB_INSTALL_DIR=/usr/local/Calpont
fi

export INFINIDB_INSTALL_DIR=$INFINIDB_INSTALL_DIR

test -f $INFINIDB_INSTALL_DIR/post/functions && . $INFINIDB_INSTALL_DIR/post/functions

for testScript in $INFINIDB_INSTALL_DIR/post/*.sh; do
	if [ -x $testScript ]; then
		eval $testScript
		rc=$?
		if [ $rc -ne 0 ]; then
			cplogger -c 51 $testScript
			echo "FAILED, check Critical log for additional info"
			exit $rc
		fi
	fi
done
echo "OK"

cplogger -i 54

exit 0

