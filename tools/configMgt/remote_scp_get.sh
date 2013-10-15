#!/usr/bin/expect
#
# $Id: remote_commend.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server root password
# Argument 3 - Command
set timeout 30
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set FILE [lindex $argv 2]
set USERNAME [lindex $argv 3]
set DEBUG [lindex $argv 4]
log_user $DEBUG
spawn -noecho /bin/bash
# 
# send command
#
expect -re {[$#] }
send "scp $USERNAME@$SERVER:$FILE .\n"
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
	-re "100%" 						{ send_user "DONE\n" } abort
	-re "scp"  						{ send_user "FAILED\n" ; exit -1 }
	-re "Permission denied"         { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
}
exit

