#!/usr/bin/expect
#
# $Id$
#
# Install RPM and custom OS files on system
# Argument 1 - Remote Module Name
# Argument 2 - Remote Server Host Name or IP address
# Argument 3 - Root Password of remote server
# Argument 4 - Package name being installed
# Argument 5 - Install Type, "initial" or "upgrade"
# Argument 6 - Debug flag 1 for on, 0 for off
set timeout 10
set MODULE [lindex $argv 0]
set SERVER [lindex $argv 1]
set PASSWORD [lindex $argv 2]
set CALPONTRPM1 [lindex $argv 3]
set CALPONTRPM2 [lindex $argv 4]
set CALPONTRPM3 [lindex $argv 5]
set CALPONTMYSQLRPM [lindex $argv 6]
set CALPONTMYSQLDRPM [lindex $argv 7]
set INSTALLTYPE [lindex $argv 8]
set DEBUG [lindex $argv 9]
set INSTALLDIR "/usr/local/Calpont"
set IDIR [lindex $argv 10]
if { $IDIR != "" } {
	set INSTALLDIR $IDIR
}
set USERNAME "root"
set UNM [lindex $argv 11]
if { $UNM != "" } {
	set USERNAME $UNM
}

log_user $DEBUG
spawn -noecho /bin/bash
#
if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
	# 
	# erase InfiniDB MySQL storage engine package
	#
	send_user "Erase InfiniDB MySQL Storage Engine Package on Module           "
	expect -re {[$#] }
	send "ssh $USERNAME@$SERVER 'rpm -e --nodeps --allmatches calpont-mysql >/dev/null 2>&1; rpm -e --nodeps --allmatches infinidb-storage-engine'\n"
	expect {
		"Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit }
		"service not known" { send_user "FAILED: Invalid Host\n" ; exit }
		"authenticity" { send "yes\n" 
							expect {
								"word: " { send "$PASSWORD\n" }
							}
		}
		"word: " { send "$PASSWORD\n" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re {[$#] }                  { send_user "DONE" }
		"uninstall completed" { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		"not installed"       { send_user "INFO: Package not installed" }
	}
	send_user "\n"
	# 
	# erase InfiniDB MySQL package
	#
	send_user "Erase InfiniDB MySQL Package on Module          "
	expect -re {[$#] }
	send "ssh $USERNAME@$SERVER 'rpm -e --nodeps --allmatches calpont-mysqld >/dev/null 2>&1; rpm -e --nodeps --allmatches infinidb-mysql'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re {[$#] }                  { send_user "DONE" }
		"uninstall completed" { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		"not installed"       { send_user "INFO: Package not installed" }
	}
	send_user "\n"
	# 
	# erase InfiniDB packages
	#
	send_user "Erase InfiniDB Packages on Module                 "
	expect -re {[$#] }
	send "ssh $USERNAME@$SERVER 'rpm -e --nodeps --allmatches calpont >/dev/null 2>&1; rpm -e --nodeps --allmatches infinidb-enterprise >/dev/null 2>&1; rpm -e --nodeps --allmatches infinidb-libs infinidb-platform'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	expect {
		-re {[$#] }                  { send_user "DONE" }
		"uninstall completed" { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit -1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		"not installed"       { send_user "INFO: Package not installed" }
	}
	send_user "\n"
}
if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the InfiniDB package
#
expect -re {[$#] }
set timeout 20
send_user "Copy New InfiniDB Package to Module              "
send "ssh $USERNAME@$SERVER 'rm -f /root/infinidb-*.rpm'\n"
expect "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re {[$#] } { }
}
send "scp $CALPONTRPM1 $CALPONTRPM2 $CALPONTRPM3 $USERNAME@$SERVER:.\n"
expect "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	"100%" 				{ send_user "DONE" }
	"scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	"Permission denied, please try again"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	"No such file or directory" { send_user "ERROR: Invalid package\n" ; exit -1 }
}
send_user "\n"
# 
# send the InfiniDB MySQL sotrage engine package
#
send_user "Copy New InfiniDB MySQL Storage Engine Package to Module        "
send "scp $CALPONTMYSQLRPM  $USERNAME@$SERVER:$CALPONTMYSQLRPM\n"
expect "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	"100%" 				{ send_user "DONE" }
	"scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	"Permission denied, please try again"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	"No such file or directory" { send_user "ERROR: Invalid package\n" ; exit -1 }
}
send_user "\n"
# 
# send the InfiniDB MySQL package
#
send_user "Copy New InfiniDB MySQL Package to Module       "
send "scp $CALPONTMYSQLDRPM  $USERNAME@$SERVER:$CALPONTMYSQLDRPM\n"
expect "word: "
# send the password
send "$PASSWORD\n"
# check return
expect {
	"100%" 				{ send_user "DONE" }
	"scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit -1 }
	"Permission denied, please try again"         { send_user "ERROR: Invalid password\n" ; exit -1 }
	"No such file or directory" { send_user "ERROR: Invalid package\n" ; exit -1 }
}
#
send_user "\n"
expect -re {[$#] }
set timeout 60
if { $INSTALLTYPE == "initial"} {
	#
	# install package
	#
	send_user "Install InfiniDB Packages on Module               "
	send "ssh $USERNAME@$SERVER 'rpm -ivh $CALPONTRPM1 $CALPONTRPM2 $CALPONTRPM3 $CALPONTMYSQLDRPM $CALPONTMYSQLRPM'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"completed" 		  { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re {[$#] }
	set timeout 10
	#
	# install package
	#
	send_user "Running InfiniDB MySQL Setup Scripts on Module   "
	send "ssh $USERNAME@$SERVER '$INSTALLDIR/bin/post-mysql-install'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		"Shutting down MySQL." { send_user "DONE" }
		timeout { send_user "DONE" }
		"ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
	send_user "\n"
	expect -re {[$#] }
	#
	# install package
	#
	send_user "Running InfiniDB MySQL Setup Scripts on Module  "
	send "ssh $USERNAME@$SERVER '$INSTALLDIR/bin/post-mysqld-install'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		"Shutting down MySQL." { send_user "DONE" }
		timeout { send_user "DONE" }
		"ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
} else {
	#
	# upgrade package
	#
	expect -re {[$#] }
	send_user "Upgrade InfiniDB Packages on Module               "
	send "ssh $USERNAME@$SERVER ' rpm -Uvh --noscripts $CALPONTRPM1 $CALPONTRPM2 $CALPONTRPM3 $CALPONTMYSQLDRPM $CALPONTMYSQLRPM'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"completed" 		  { send_user "DONE" }
		"already installed"   { send_user "INFO: Already Installed\n" ; exit -1 }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit -1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	expect -re {[$#] }
	set timeout 10
	#
	# install package
	#
	send_user "Running InfiniDB MySQL Setup Scripts on Module   "
	send "ssh $USERNAME@$SERVER '$INSTALLDIR/bin/post-mysql-install'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		"Shutting down MySQL." { send_user "DONE" }
		timeout { send_user "DONE" }
		"ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
	send_user "\n"
	expect -re {[$#] }
	#
	# install package
	#
	send_user "Running InfiniDB MySQL Setup Scripts on Module  "
	send "ssh $USERNAME@$SERVER '$INSTALLDIR/bin/post-mysqld-install'\n"
	expect "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		"Shutting down MySQL." { send_user "DONE" }
		timeout { send_user "DONE" }
		"ERROR" { send_user "ERROR: Daemon failed to run";
		exit -1 }
	}
}
send_user "\n"
expect -re {[$#] }
set timeout 30
#
if { $INSTALLTYPE == "initial"} {
	#
	# copy over InfiniDB OS files
	#
	send_user "Copy InfiniDB OS files to Module                 "
	send "scp $INSTALLDIR/local/etc/$MODULE/*  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	expect "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	#
	# copy over InfiniDB config file
	#
	send_user "Copy InfiniDB Config file to Module              "
	send "scp $INSTALLDIR/etc/*  $USERNAME@$SERVER:$INSTALLDIR/etc/.\n"
	expect "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	#
	# copy over custom OS tmp files
	#
	send_user "Copy Custom OS files to Module                  "
	send "scp -r $INSTALLDIR/local/etc  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	expect "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re {[$#] } 		  		  { send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
	}
	send_user "\n"
	#
	# Start module installer to setup Customer OS files
	#
	send_user "Run Module Installer                            "
	send "ssh $USERNAME@$SERVER '$INSTALLDIR/bin/module_installer.sh'\n"
	expect "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		"!!!Module" 				  			{ send_user "DONE" }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit -1 }
		"FAILED"   								{ send_user "ERROR: missing OS file\n" ; exit -1 }
	}
	send_user "\n"
}
#
send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

