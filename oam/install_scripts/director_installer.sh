#!/usr/bin/expect
#
# $Id: system_installer.sh 1066 2008-11-13 21:44:44Z dhill $
#
# Install RPM and custom OS files on system
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Package name being installed
# Argument 5 - Install Type, "initial" or "upgrade"
# Argument 6 - Debug flag 1 for on, 0 for off
set timeout 10
set USERNAME root
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set CALPONTRPM [lindex $argv 3]
set CALPONTMYSQLRPM [lindex $argv 4]
set CALPONTMYSQLDRPM [lindex $argv 5]
set INSTALLTYPE [lindex $argv 6]
set DEBUG [lindex $argv 7]
log_user $DEBUG
spawn -noecho /bin/bash
#
if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
	# 
	# unmount disk
	#
	send_user "Unmount disk                                    "
	expect -re "# "
	send "ssh $USERNAME@$SERVER 'umount /usr/local/Calpont/data*'\n"
	expect {
		-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit }
		-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit }
		-re "authenticity" { send "yes\n" 
							expect {
								-re "word: " { send "$PASSWORD\n" } abort
							}
		}
		-re "word: " { send "$PASSWORD\n" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	expect {
		-re "# "                  { send_user "DONE" } abort
	}
	send_user "\n"
	# 
	# erase Calpont-MySql package
	#
	send_user "Erase Calpont-Mysql Package on Module           "
	expect -re "# "
	send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches calpont-mysql'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re "# "                  { send_user "DONE" } abort
		-re "uninstall completed" { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		-re "not installed"       { send_user "INFO: Package not installed" } abort
	}
	send_user "\n"
	# 
	# erase Calpont-MySqld package
	#
	send_user "Erase Calpont-Mysqld Package on Module          "
	expect -re "# "
	send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches calpont-mysqld'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re "# "                  { send_user "DONE" } abort
		-re "uninstall completed" { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		-re "not installed"       { send_user "INFO: Package not installed" } abort
	}
	send_user "\n"
	# 
	# erase Calpont package
	#
	send_user "Erase Calpont Package on Module                 "
	expect -re "# "
	send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches calpont'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re "# "                  { send_user "DONE" } abort
		-re "uninstall completed" { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		-re "not installed"       { send_user "INFO: Package not installed" } abort
	}
	send_user "\n"
}
if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the Calpont package
#
expect -re "# "
set timeout 20
send_user "Copy New Calpont Package to Module              "
send "ssh $USERNAME@$SERVER 'rm -f /root/calpont*.rpm'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "# " { } abort
}
send "scp $CALPONTRPM  $USERNAME@$SERVER:$CALPONTRPM\n"
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
# send the Calpont-mysql package
#
send_user "Copy New Calpont-MySql Package to Module        "
send "scp $CALPONTMYSQLRPM  $USERNAME@$SERVER:$CALPONTMYSQLRPM\n"
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
# send the Calpont-mysqld package
#
send_user "Copy New Calpont-MySqld Package to Module       "
send "scp $CALPONTMYSQLDRPM  $USERNAME@$SERVER:$CALPONTMYSQLDRPM\n"
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
#
send_user "\n"
expect -re "# "
set timeout 60
if { $INSTALLTYPE == "initial"} {
	#
	# install package
	#
	send_user "Install Calpont Package on Module               "
	send "ssh $USERNAME@$SERVER ' rpm -ivh $CALPONTRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# install package
	#
	send_user "Install Calpont-MySqld Package on Module        "
	send "ssh $USERNAME@$SERVER ' rpm -ivh $CALPONTMYSQLDRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# install package
	#
	send_user "Install Calpont-MySql Package on Module         "
	send "ssh $USERNAME@$SERVER ' rpm -ivh $CALPONTMYSQLRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	set timeout 10
	#
	# install package
	#
	send_user "Running Calpont-MySql Setup Scripts on Module   "
	send "ssh $USERNAME@$SERVER '/usr/local/Calpont/bin/post-mysql-install'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Shutting down MySQL." { send_user "DONE" } abort
		timeout { send_user "DONE" } abort
		-re "ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# install package
	#
	send_user "Running Calpont-MySqld Setup Scripts on Module  "
	send "ssh $USERNAME@$SERVER '/usr/local/Calpont/bin/post-mysqld-install'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Shutting down MySQL." { send_user "DONE" } abort
		timeout { send_user "DONE" } abort
		-re "ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
} else {
	#
	# upgrade package
	#
	expect -re "# "
	send_user "Upgrade Calpont Package on Module               "
	send "ssh $USERNAME@$SERVER ' rpm -Uvh --noscripts $CALPONTRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "already installed"   { send_user "INFO: Already Installed\n" ; exit -1 }
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# upgrade package
	#
	send_user "Upgrade Calpont-MySqld Package on Module        "
	send "ssh $USERNAME@$SERVER ' rpm -Uvh --noscripts $CALPONTMYSQLDRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "already installed"   { send_user "INFO: Already Installed\n" ; exit -1 }
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# upgrade package
	#
	send_user "Upgrade Calpont-MySql Package on Module         "
	send "ssh $USERNAME@$SERVER ' rpm -Uvh --noscripts $CALPONTMYSQLRPM'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "completed" 		  { send_user "DONE" } abort
		-re "already installed"   { send_user "INFO: Already Installed\n" ; exit -1 }
		-re "error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re "# "
	set timeout 10
	#
	# install package
	#
	send_user "Running Calpont-MySql Setup Scripts on Module   "
	send "ssh $USERNAME@$SERVER '/usr/local/Calpont/bin/post-mysql-install'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Shutting down MySQL." { send_user "DONE" } abort
		timeout { send_user "DONE" } abort
		-re "ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
	send_user "\n"
	expect -re "# "
	#
	# install package
	#
	send_user "Running Calpont-MySqld Setup Scripts on Module  "
	send "ssh $USERNAME@$SERVER '/usr/local/Calpont/bin/post-mysqld-install'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Shutting down MySQL." { send_user "DONE" } abort
		timeout { send_user "DONE" } abort
		-re "ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
}
send_user "\n"
expect -re "# "
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
	send_user "\n"
	#
	# copy over calpont config file
	#
	send_user "Copy Calpont Config file to Module              "
	send "scp /usr/local/Calpont/etc/*  $USERNAME@$SERVER:/usr/local/Calpont/etc/.\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	#
	# copy over custom OS tmp files
	#
	send_user "Copy Custom OS files to Module                  "
	send "scp -r /usr/local/Calpont/local/etc  $USERNAME@$SERVER:/usr/local/Calpont/local/.\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
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

