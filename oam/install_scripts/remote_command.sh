#!/usr/bin/expect
#
# $Id: remote_command.sh 3496 2012-12-17 22:51:51Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server password
# Argument 3 - Command
# Argument 4 - debug flag
# Argument 5 - Remote user name (optional)
set timeout 30
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set COMMAND [lindex $argv 2]
set DEBUG [lindex $argv 3]
set USERNAME $env(USER)
set UNM [lindex $argv 4]
if { $UNM != "" } {
	set USERNAME "$UNM"
}
log_user $DEBUG
spawn -noecho /bin/bash
expect -re "\[#$] "

if { $PASSWORD == "ssh" } {
	set PASSWORD ""
}

# 
# send command
#
send "ssh $USERNAME@$SERVER $COMMAND\n"
expect {
	-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1}
	-re "service not known"    { send_user "           FAILED: Invalid Host\n" ; exit 1}
	-re "ssh: connect to host" { send_user "           FAILED: Invalid Host\n" ; exit 1 }
	-re "Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	-re "closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	-re "authenticity" { send "yes\n" 
						 expect {
						 	-re "word: " { send "$PASSWORD\n" } abort
							-re "passphrase" { send "$PASSWORD\n" } abort
						 }
						}
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase" { send "$PASSWORD\n" } abort
	-re "\[#$] " { exit 0 }
}
expect {
	-re "\[#$] " { exit 0 }
	-re "Permission denied" { send_user "           FAILED: Invalid password\n" ; exit 1 }
	-re "(y or n)"  { send "y\n" 
					  expect -re "\[#$] " { exit 0 }
					}
}
exit 0

