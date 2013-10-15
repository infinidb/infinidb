#!/usr/bin/expect
#
# $Id$
#
# Install custom OS files on External Module
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - OAM Parent Host Name
# Argument 4 - Root Password of remote server
# Argument 5 - Debug flag 1 for on, 0 for off
set timeout 10
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set OAMHOSTNAME [lindex $argv 2]
set PASSWORD [lindex $argv 3]
set DEBUG [lindex $argv 4]
log_user $DEBUG
spawn -noecho /bin/bash
#
# Validate Calpont RPM is installed
#
send_user "\n"
send_user "Check Calpont RPM installed                     "
expect -re "# "
send "ssh $USERNAME@$SERVER 'rpm -iq calpont'\n"
expect {
	-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit }
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit }
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" }
						}
						}
	-re "word: " { send "$PASSWORD\n" }
}
expect {
	-re "tools " 			  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "package calpont"     { send_user "ERROR: Calpont RPM not installed on External Module\n" ; exit -1 }
}
#
# Create mount directories
#
send_user "\n"
send_user "Create etc mount directory                      "
expect -re "# "
send "ssh $USERNAME@$SERVER 'mkdir /mnt/$OAMHOSTNAME\_etc'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "mkdir: cannot"       { send_user "DONE: already installed\n" 
								send_user "\nInstallation Successfully Completed on '$MODULE'\n"; exit 0 }
}
send_user "\n"
send_user "Create OAM mount directory                      "
send "ssh $USERNAME@$SERVER 'mkdir /mnt/$OAMHOSTNAME\_OAM'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "mkdir: cannot"       { send_user "DONE: already installed\n" 
								send_user "\nInstallation Successfully Completed on '$MODULE'\n"; exit 0 }
}
#
# copy over calpont OS files
#
send_user "\n"
send_user "Copy Mount File to Module                       "
send "scp /tmp/etc/$MODULE/*mount  $USERNAME@$SERVER:/mnt/.\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "100%" 				{ send_user "DONE" }
	-re "scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	-re "Permission denied"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "ERROR: Invalid file\n" ; exit -1 }
}
#
send_user "\n"
send_user "Copy Custom inittab to Module                   "
send "scp /tmp/etc/$MODULE/inittab.calpont  $USERNAME@$SERVER:/tmp/.\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "100%" 				{ send_user "DONE" }
	-re "scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	-re "Permission denied"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "ERROR: Invalid file\n" ; exit -1 }
}
#
send_user "\n"
send_user "Copy syslogd.conf to Module                     "
send "scp /etc/syslog.conf  $USERNAME@$SERVER:/etc/.\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "100%" 				{ send_user "DONE" }
	-re "scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	-re "Permission denied"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "ERROR: Invalid file\n" ; exit -1 }
}
#
# chmod of mount file
#
send_user "\n"
send_user "Run chmod on Mount File                         "
send "ssh $USERNAME@$SERVER 'chmod 777 /mnt/$OAMHOSTNAME\_*mount'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
}
#
# Update External Module inittab
#
send_user "\n"
send_user "Update inittab                                  "
send "ssh $USERNAME@$SERVER 'cat /tmp/inittab.calpont >> /etc/inittab'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
}
#
# Restart External Module syslogd
#
send_user "\n"
send_user "Restart syslogd                                 "
send "ssh $USERNAME@$SERVER 'service syslog restart'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
}
#
# startup ProcMon
#
send_user "\n"
send_user "Startup ProcMon                                 "
send "ssh $USERNAME@$SERVER 'kill -HUP 1'\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
expect {
	-re "# " 				  { send_user "DONE" }
	-re "Permission denied"   { send_user "ERROR: Invalid password\n" ; exit -1 }
}
send_user "\n"
#
send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

