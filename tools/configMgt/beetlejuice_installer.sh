#!/usr/bin/expect
#
# $Id: beetlejuice_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Beetlejuice Installer
# Argument 0 - Server IP address
# Argument 1 - Root Password
# Argument 2 - Debug flag 1 for on, 0 for off

set timeout 30
set USERNAME root
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set PACKAGE [lindex $argv 2]
set RELEASE [lindex $argv 3]
set DEBUG [lindex $argv 4]
log_user $DEBUG
spawn -noecho /bin/bash
# 
# get the package
#
send_user "Get Calpont Package                           "
send "rm -f $PACKAGE\n"
#expect -re "#"
send "smbclient //cal6500/shared -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $PACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "FAILED: $PACKAGE not found in //cal6500/shared/Iterations/$RELEASE/\n" ; exit -1 }
	-re "getting" 				 { send_user "DONE" } abort
}
send_user "\n"
# 
# send the DM package
#
expect -re "#"
send_user "Copy Calpont Package                          "
send "ssh $USERNAME@$SERVER 'rm -f /root/calpont*.rpm'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
expect -re "#"
send "scp $PACKAGE $USERNAME@$SERVER:/root/.\n"
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
	-re "100%" 						{ send_user "DONE" } abort
	-re "scp"  						{ send_user "FAILED\n" ; 
				 			send_user "\n*** Installation Failed\n" ; 
							exit -1 }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
}
send "rm -f $PACKAGE\n"
# 
# backup custom os files
#
send_user "\n"
expect -re "#"
send_user "Backup Custom OS Files                        "
send "ssh $USERNAME@$SERVER 'rm -f /etc/*.calpont;cp /etc/inittab /etc/inittab.calpont;cp /etc/syslog.conf /etc/syslog.conf.calpont'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "#"                  { send_user "DONE" } abort
	-re "cp"       			  { send_user "FAILED" ; exit -1 }
}
send_user "\n"
# 
# unmount disk
#
expect -re "#"
send_user "Unmount disk                                  "
send "ssh $USERNAME@$SERVER 'umount -a'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "#"                  { send_user "DONE" } abort
}
send_user "\n"
# 
# erase package
#
expect -re "#"
send_user "Erase Old Calpont-oracle Package              "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps calpont-oracle'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "#"                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
expect -re "#"
send_user "Erase Old Calpont-Mysql Package               "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps calpont-mysql'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "#"                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
expect -re "#"
send_user "Erase Old Calpont Package                     "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps calpont'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "#"                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
expect -re "#"
#
# install package
#
expect -re "#"
set timeout 120
send_user "Install New Calpont Package                   "
send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$PACKAGE'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "completed" 		  { send_user "DONE" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; 
								send_user "\n*** Installation Failed\n" ; 
									exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
# 
# Restore custom os files
#
set timeout 30
expect -re "#"
send_user "Restore Custom OS Files                       "
send "ssh $USERNAME@$SERVER 'mv -f /etc/inittab.calpont /etc/inittab;mv -f /etc/syslog.conf.calpont /etc/syslog.conf'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "#"                  { send_user "DONE" } abort
	-re "mv: cannot"       	  { send_user "FAILED" ; exit -1 }
}
send_user "\n"
# 
# mount disk
#
expect -re "#"
send_user "Mount disk                                    "
send "ssh $USERNAME@$SERVER 'mount -a'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "#"                  { send_user "DONE" } abort
}
send_user "\n"
# 
# restart syslog
#
expect -re "#"
send_user "Restart syslog service                        "
send "ssh $USERNAME@$SERVER 'service syslog restart'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "Starting kernel logger"  { send_user "DONE" } abort
	-re "service "  			   { send_user "WARNING: service not available" } abort
}
send_user "\n"
# 
# startup ProcMons
#
expect -re "#"
send_user "Startup ProcMon's                             "
send "ssh $USERNAME@$SERVER 'kill -HUP 1'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "#"                  { send_user "DONE" } abort
}
send_user "\n"
#
exit

