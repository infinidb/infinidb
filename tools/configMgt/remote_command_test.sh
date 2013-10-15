#!/usr/bin/expect
#
# $Id: remote_commend.sh 421 2007-04-05 15:46:55Z dhill $
#
# Remote command execution script to another server 
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server root password
# Argument 3 - Command
# Argument 4 - Good Response
# Argument 5 - Bad Response
# Argument 6 - timeout
# Argument 7 - Debug flag
set timeout 30
set USERNAME root
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set COMMAND [lindex $argv 2]
set GOOD_RESPONSE [lindex $argv 3]
set BAD_RESPONSE [lindex $argv 4]
set timeout [lindex $argv 5]
set DEBUG [lindex $argv 6]
log_user $DEBUG
spawn -noecho /bin/bash
expect -re "# "
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
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit }
	-re "Permission denied" { send_user "FAILED: Invalid Password\n" ; exit }
	-re "word: " { send "$PASSWORD\n" } abort
}
expect {
#	-re $GOOD_RESPONSE exit
	-re $GOOD_RESPONSE { send_user " " ; exit }
	-re $BAD_RESPONSE { send_user "FAILED\n" ; exit }
	timeout { send_user "TIMEOUT\n" ; exit }
}
send_user "UNKNOWN RESPONSE\n"
exit

