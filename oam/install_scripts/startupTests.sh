#! /bin/sh
#
# $Id: startupTests.sh 1141 2009-01-16 20:59:06Z dhill $
#
# startupTests - perform sanity testing on system DB at system startup time
#				 called by Process-Monitor

test -f /usr/local/Calpont/post/functions && . /usr/local/Calpont/post/functions

for testScript in /usr/local/Calpont/post/*.sh; do
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

