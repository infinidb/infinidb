#!/usr/bin/expect
#
# $Id: patch_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Install Software Patch on Module
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Patch Directory Location
# Argument 5 - Install Directory Location 
# Argument 6 - Software File being installed
# Argument 7 - Debug flag 1 for on, 0 for off
set timeout 20
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set PATCHLOCATION [lindex $argv 3]
set INSTALLLOCATION [lindex $argv 4]
set FILE [lindex $argv 5]
set DEBUG [lindex $argv 6]
set USERNAME "root"
set UNM [lindex $argv 7]
if { $UNM != "" } {
	set USERNAME $UNM
}
log_user $DEBUG
spawn -noecho /bin/bash
# 
# mv file being install
#
send_user "Backup Current File on Module"
expect -re "# "
send "ssh $USERNAME@$SERVER 'mv $INSTALLLOCATION$FILE $INSTALLLOCATION$FILE'.patchSave''\n"
# accept the remote host fingerprint (assuming never logged in before)
expect -re "service not known" { send_user "               FAILED: Invalid Host\n" ; exit }
expect -re "authenticity" { send "yes\n" }
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "# "                 { send_user "                 DONE" }
	-re "Permission denied"  { send_user "                 FAILED: Invalid password\n" } exit; 
	-re "mv"                 { send_user "                 FAILED: copy filed\n" ; exit}
}
send_user "\n"
# 
# send Patch File
#
send_user "Copy New Calpont Software File to Module"
expect -re "# "
send "scp $PATCHLOCATION$FILE  $USERNAME@$SERVER:$INSTALLLOCATION$FILE\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	-re "100%" 				        { send_user "      DONE" }
	-re "scp"  				        { send_user "      FAILED\n" ; 
				 			send_user "\n*** Installation Failed\n" ; 
							exit }
	-re "Permission denied"         { send_user "      FAILED: Invalid password\n" ; exit }
	-re "No such file or directory" { send_user "      FAILED: Invalid package\n" ; exit }
}
send_user "\n"

send_user "\n!!!Patch Installation Successfully Completed!!!\n"
exit

