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
set stty_init {cols 512 -opost};
set timeout 30
set SERVER [lindex $argv 0]
set USERNAME [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set COMMAND [lindex $argv 3]
set GOOD_RESPONSE [lindex $argv 4]
set BAD_RESPONSE [lindex $argv 5]
set timeout [lindex $argv 6]
set DEBUG [lindex $argv 7]
log_user $DEBUG
spawn -noecho /bin/bash
expect -re {[$#] }
# 
# send command
#
send "ssh $USERNAME@$SERVER $COMMAND\n"
expect {
	-re "authenticity" { send "yes\n" 
						expect {
							timeout { send_user "TIMEOUT\n" ; exit 2 }
							-re "word: " { send "$PASSWORD\n" } abort
							}
						}
	timeout { send_user "TIMEOUT\n" ; exit 2 }
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	-re "Permission denied" { send_user "FAILED: Invalid Password\n" ; exit 1 }
	-re "word: " { send "$PASSWORD\n" } abort
	-re $GOOD_RESPONSE { send_user " " ; exit 0 }
	-re $BAD_RESPONSE { send_user "FAILED\n" ; exit 1 }
}
expect {
#	-re $GOOD_RESPONSE exit
	timeout { send_user "FAILED-TIMEOUT\n" ; exit 1 }
	-re $GOOD_RESPONSE { send_user " " ; exit 0 }
	-re $BAD_RESPONSE { send_user "FAILED\n" ; exit 1 }
	-re "No such file" { send_user "FAILED\n" ; exit 1 }
}
exit 1

