#!/usr/bin/expect
#
# $Id: remote_command.sh 3495 2012-12-17 22:51:40Z dhill $
#
# Remote command execution script to another server
# Argument 1 - Remote Server Host Name or IP address
# Argument 2 - Remote Server password
# Argument 3 - Command
# Argument 4 - debug flag
# Argument 5 - Remote user name (optional)
# Argument 6 - Force a tty to be allocated (optional)
set stty_init {cols 512 -opost};
set timeout 30
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set COMMAND [lindex $argv 2]
set DEBUG [lindex $argv 3]

if {[info exists env(USER)]} {
    set USERNAME $env(USER)
} else {
    set USERNAME "root"
}

set UNM [lindex $argv 4]
if { $UNM != "" && $UNM != "-" } {
	set USERNAME "$UNM"
}
set TTY ""
set TTYOPT [lindex $argv 5]
if { $TTYOPT != "" } {
	set TTY "-t"
}
log_user $DEBUG
spawn -noecho /bin/bash
expect -re {[$#] }

if { $PASSWORD == "ssh" } {
	set PASSWORD ""
}

# 
# send command
#
send "ssh $TTY $USERNAME@$SERVER $COMMAND\n"
expect {
	"Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1}
	"service not known"    { send_user "           FAILED: Invalid Host\n" ; exit 1}
	"ssh: connect to host" { send_user "           FAILED: Invalid Host\n" ; exit 1 }
	"Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	"Connection closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	"authenticity" { send "yes\n" 
						 expect {
						 	"word: " { send "$PASSWORD\n" }
							"passphrase" { send "$PASSWORD\n" }
						 }
						}
	"word: " { send "$PASSWORD\n" }
	"passphrase" { send "$PASSWORD\n" }
	-re {[$#] } { exit 0 }
}
expect {
	-re {[$#] } { exit 0 }
	"Permission denied" { send_user "           FAILED: Invalid password\n" ; exit 1 }
	"(y or n)"  { send "y\n" 
					  expect -re {[$#] } { exit 0 }
					}
}
exit 0

