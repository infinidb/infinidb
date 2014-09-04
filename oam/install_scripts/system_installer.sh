#!/usr/bin/expect
#
# $Id: system_installer.sh 2804 2012-03-22 12:57:42Z pleblanc $
#
# Install RPM and custom OS files on system
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Package name being installed
# Argument 5 - Install Type, "initial" or "upgrade"
# Argument 6 - Debug flag 1 for on, 0 for off
set timeout 30
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set RPMPACKAGE [lindex $argv 3]
set INSTALLTYPE [lindex $argv 4]
set DEBUG [lindex $argv 5]
log_user $DEBUG
spawn -noecho /bin/bash
#
if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
	# 
	# erase package
	#
	send_user "Erase Calpont Package on Module                 "
	expect -re "# "
	send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches calpont'\n"
	expect {
		-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit }
		-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit }
		-re "authenticity" { send "yes\n" 
							expect {
								-re "word: " { send "$PASSWORD\n" } abort
							}
		}
		-re "word: " { send "$PASSWORD\n" } abort
	}
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re "# "                  { send_user "DONE" } abort
		-re "uninstall completed" { send_user "DONE" } abort
		-re "ERROR dependencies" { send_user "ERROR: ERROR dependencies\n" ; exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		-re "error: package"       { send_user "INFO: Package not installed" } abort
		-re "not installed"       { send_user "INFO: Package not installed" } abort
	}
	send_user "\n"
}
if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the package
#
expect -re "# "
send_user "Copy New Calpont Package to Module              "
send "ssh $USERNAME@$SERVER 'rm -f /root/calpont*.rpm'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "# " { } abort
}
send "scp $RPMPACKAGE  $USERNAME@$SERVER:$RPMPACKAGE\n"
expect -re "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	-re "100%" 				{ send_user "DONE" } abort
	-re "scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	-re "Permission denied, please try again"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "ERROR: Invalid package\n" ; exit -1 }
}
send_user "\n"
#
set timeout 60
if { $INSTALLTYPE == "initial"} {
	#
	# install package
	#
	send_user "Install Calpont Package on Module               "
	send "ssh $USERNAME@$SERVER ' rpm -ivh $RPMPACKAGE'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "ERROR dependencies" { send_user "ERROR: ERROR dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
} else {
	#
	# upgrade package
	#
	send_user "Upgrade Calpont Package on Module               "
	send "ssh $USERNAME@$SERVER ' rpm -Uvh --noscripts $RPMPACKAGE'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "already installed"   { send_user "INFO: Already Installed\n" ; exit -1 }
		-re "ERROR dependencies" { send_user "ERROR: ERROR dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
}
expect -re "# "
send_user "\n"
set timeout 30
#
if { $INSTALLTYPE == "initial"} {
	#
	# copy over calpont OS files
	#
	send_user "Copy Calpont OS files to Module                 "
	send "scp /usr/local/Calpont/local/etc/$MODULE/*  $USERNAME@$SERVER:/usr/local/Calpont/local/.\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	#
	send_user "\n"
	set timeout 120
	#
	# Start module installer to setup Customer OS files
	#
	send_user "Run Module Installer                            "
	send "ssh $USERNAME@$SERVER '/usr/local/Calpont/bin/module_installer.sh'\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "!!!Module" 				  			{ send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		-re "FAILED"   								{ send_user "ERROR: missing OS file\n" ; exit -1 }
	}
	send_user "\n"
}
#
send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

