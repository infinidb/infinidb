#!/usr/bin/expect
#
# $Id: autoReleaseNotes.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server
# Argument 1 - release
# Argument 2 - date since last release (for BUG generation) in "2008-06-02 00:00:00" format
# Argument 3 - debug flag 
set timeout 1800
set SERVER srvengcm1
set USERNAME root
set PASSWORD Calpont1
set RELEASE [lindex $argv 0]
set DATE [lindex $argv 1]
set DEBUG [lindex $argv 2]
set COMMAND "'/home/bugzilla/resolve_bug_report $RELEASE $DATE'"

#set SHARED "//cal6500/shared"
set SHARED "//calweb/shared"

log_user $DEBUG
spawn -noecho /bin/bash
if { $RELEASE == "-h" } {
	send_user "\n"
	send_user "'autoReleaseNotes.sh' generates bug reports for release notes\n"
	send_user "\n"
	send_user "Usage: autoReleaseNotes.sh 'release' 'date'\n"
	send_user "			release - Calpont release number (optional)\n"
	send_user "			date - date since last build ('2008-06-02 00:00:00' format)\n"
	exit
}
# 
# send command
#
send "ssh $USERNAME@$SERVER $COMMAND\n"
expect {
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" } abort
							}
						}
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit -1 }
	-re "word: " { send "$PASSWORD\n" } abort
}
expect {
	-re "Generate Resolved Bug Report Successfully Completed" { send_user "Successfully completed BUG Report and placed on //cal6500/shared/Iterations/\n" } abort
}

exec echo -e "\n##### Subsystem: build \n" > svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/build/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: dbcon \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/dbcon/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: dmlib \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/dmlib/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: exemgr \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/exemgr/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: oam \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/oam/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: oamapps \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/oamapps/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: primitives \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/primitives/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: procmgr \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/procmgr/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: procmon \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/procmon/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: snmpd \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/snmpd/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: tools \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/tools/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: utils \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/utils/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: versioning \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/versioning/trunk >> svn_release_notes.txt
exec echo -e "\n##### Subsystem: writeengine \n" >> svn_release_notes.txt
exec svn log -r "{$DATE}:HEAD" http://srvengcm1.calpont.com/svn/genii/writeengine/trunk >> svn_release_notes.txt

exec smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c "cd Iterations;cd $RELEASE;put svn_release_notes.txt"

send_user "Successfully completed SVN Reports and placed on //cal6500/shared/Iterations/\n"
exit