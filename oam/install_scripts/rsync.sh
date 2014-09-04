#!/usr/bin/expect
#
# $Id: rsync.sh 2915 2012-05-23 16:01:34Z dhill $
#
# Remote Install RPM and custom OS files from postConfigure script
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Root Password of remote server
# Argument 3 - Debug flag 1 for on, 0 for off
set USERNAME "root@"
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set DEBUG 0
set DEBUG [lindex $argv 2]
log_user $DEBUG
spawn -noecho /bin/bash
set prefix /usr/local
set installdir $prefix/Calpont

if { $PASSWORD == "ssh" } {
	set USERNAME ""
	set PASSWORD ""
}

set COMMAND "rsync -vuopg -e ssh --delete --exclude=*err --exclude=*pid -r $installdir/mysql/db $USERNAME$SERVER:/usr/local/Calpont/mysql/"

#
# run command
#
set timeout 600
send "$COMMAND\n"
expect {
	-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit -1}
	-re "service not known"    { send_user "           FAILED: Invalid Host\n" ; exit -1}
	-re "ssh: connect to host" { send_user "           FAILED: Invalid Host\n" ; exit -1 }
	-re "authenticity" { send "yes\n" 
						 expect {
						 	-re "word: " { send "$PASSWORD\n" } abort
							-re "passphrase" { send "$PASSWORD\n" } abort
						 }
						}
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase" { send "$PASSWORD\n" } abort
}
expect {
	-re "# " { exit 0 }
	-re "Permission denied" { send_user "           FAILED: Invalid password\n" ; exit 1 }
	-re "(y or n)"  { send "y\n" 
					  expect -re "# " { exit 0 }
					}
}

exit 0