#!/usr/bin/expect
#
# $Id: remote_commend.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server root password
# Argument 3 - Command
set timeout 30
set USERNAME $env(USER)"@"
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set FILE [lindex $argv 2]
set DEBUG [lindex $argv 3]
log_user $DEBUG
spawn -noecho /bin/bash

if { $PASSWORD == "ssh" } {
	set PASSWORD ""
}

# 
# send command
#
expect -re "# "
send "scp $FILE $USERNAME$SERVER:$FILE\n"
expect {
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" } abort
							-re "passphrase" { send "$PASSWORD\n" } abort
							}
						}
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	-re "Connection refused" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "Connection timed out" { send_user "FAILED: Connection timed out\n" ; exit 1 }
	-re "lost connection" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase" { send "$PASSWORD\n" } abort
	-re "WARNING:" { send "rm -f /root/.ssh/known_hosts" ; exit 1 }
}
expect {
	-re "100%" 						{ send_user "DONE\n" } abort
	-re "scp:"  					{ send_user "FAILED\n" ; exit 1 }
	-re "Permission denied"         { send_user "FAILED: Invalid password\n" ; exit 1 }
	-re "No such file or directory" { send_user "FAILED: Invalid file\n" ; exit 1 }
	-re "Connection refused" { send_user "FAILED: Connection refused\n" ; exit 1 }
	-re "closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
}
#sleep to make sure it's finished
sleep 5
exit 0

