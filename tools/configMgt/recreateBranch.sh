#!/usr/bin/expect
#
# $Id: autoRecreateBranch.sh 421 2007-04-05 15:46:55Z dhill $
#
set BRANCH [lindex $argv 0]
log_user 1
spawn -noecho /bin/bash
if { $BRANCH == "-h" } {
	send_user "\n"
	send_user "'autoRecreateBranch.sh' deletes and recreates source from Branch on a \n"
	send_user "development branch\n"
	send_user "Usage: autoRecreateBranch.sh 'branch'\n"
	send_user "			branch - SVN branch name\n"
	exit
}

send_user  "\n##### Subsystem: delete  build \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/build/branches/$BRANCH
send_user  "\n##### Subsystem: delete  dbcon \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/dbcon/branches/$BRANCH
send_user  "\n##### Subsystem: delete  ddlproc \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/ddlproc/branches/$BRANCH
send_user  "\n##### Subsystem: delete  dmlproc \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/dmlproc/branches/$BRANCH
send_user  "\n##### Subsystem: delete  dmlib \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/dmlib/branches/$BRANCH
send_user  "\n##### Subsystem: delete  exemgr \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/exemgr/branches/$BRANCH
send_user  "\n##### Subsystem: delete  oam \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/oam/branches/$BRANCH
send_user  "\n##### Subsystem: delete  oamapps \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/oamapps/branches/$BRANCH
send_user  "\n##### Subsystem: delete  primitives \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/primitives/branches/$BRANCH
send_user  "\n##### Subsystem: delete  procmgr \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/procmgr/branches/$BRANCH
send_user  "\n##### Subsystem: delete  procmon \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/procmon/branches/$BRANCH
send_user  "\n##### Subsystem: delete  snmpd \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/snmpd/branches/$BRANCH
send_user  "\n##### Subsystem: delete  tools \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/tools/branches/$BRANCH
send_user  "\n##### Subsystem: delete  utils \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/utils/branches/$BRANCH
send_user  "\n##### Subsystem: delete  versioning \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/versioning/branches/$BRANCH
send_user  "\n##### Subsystem: delete  writeengine \n"
exec svn delete -m "deleting branch $BRANCH" http://srvengcm1.calpont.com/svn/genii/writeengine/branches/$BRANCH
send_user  "\n##### Make branch $BRANCH \n"
exec ~/genii/build/makeBranch.pl $BRANCH

send_user "Successfully recreated branch $BRANCH\n"
exit