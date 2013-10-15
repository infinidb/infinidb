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
set SYSTEMRPM [lindex $argv 2]
set CALPONTRPMNAME [lindex $argv 3]
set CONNECTORRPM1NAME [lindex $argv 4]
set CONNECTORRPM2NAME [lindex $argv 5]
set RELEASE [lindex $argv 6]
set DEBUG [lindex $argv 7]

set CALPONTRPM $CALPONTRPMNAME"-1"$SYSTEMRPM
set CONNECTORRPM1 $CONNECTORRPM1NAME"-1"$SYSTEMRPM
set CONNECTORRPM2 $CONNECTORRPM2NAME"-1"$SYSTEMRPM
#set SHARED "//cal6500/shared"
set SHARED "//calweb/shared"

log_user $DEBUG

spawn -noecho /bin/bash
# 
# get the package
#
send_user "Get Calpont Packages                          "
send "rm -f $SYSTEMRPM\n"
#expect -re "#"
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $SYSTEMRPM'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "FAILED: $SYSTEMRPM not found in $SHARED/Iterations/$RELEASE/\n" ; exit -1 }
	-re "getting" 				 { send_user "DONE" } abort
}
send_user "\n"
# 
# send the DM Package
#
expect -re "#"
send_user "Copy Calpont Packages                         "
send "ssh $USERNAME@$SERVER 'rm -f /root/$SYSTEMRPM'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
expect -re "#"
send "scp $SYSTEMRPM $USERNAME@$SERVER:/root/.\n"
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
	-re "No such file or directory" { send_user "FAILED: Invalid Package\n" ; exit -1 }
}
send "rm -f $SYSTEMRPM\n"
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
# erase Package
#
expect -re "#"
send_user "Erase Old $CONNECTORRPM1NAME Package              "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps $CONNECTORRPM1NAME'\n"
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
send_user "Erase Old $CONNECTORRPM2NAME Package               "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps $CONNECTORRPM2NAME'\n"
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
send_user "Erase Old $CALPONTRPMNAME Package                     "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps $CALPONTRPMNAME'\n"
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
#
# install Package
#
expect -re "#"
set timeout 120
send_user "Install New $CALPONTRPMNAME Package                   "
send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$CALPONTRPM'\n"
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
expect -re "#"
send_user "Install New $CONNECTORRPM1NAME Package            "
send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$CONNECTORRPM1'\n"
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
expect -re "#"
send_user "Install New $CONNECTORRPM2NAME Package             "
send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$CONNECTORRPM2'\n"
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

