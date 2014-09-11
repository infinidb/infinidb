#!/usr/bin/expect
#
# $Id: user_installer.sh 1066 2008-11-13 21:44:44Z dhill $
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
set CALPONTPKG [lindex $argv 3]
set MODULETYPE [lindex $argv 4]
set INSTALLTYPE [lindex $argv 5]
set PKGTYPE [lindex $argv 6]
set SERVERTYPE [lindex $argv 7]
set DEBUG [lindex $argv 8]

set BASH "/bin/bash "
if { $DEBUG == "1" } {
	set BASH "/bin/bash -x "
}

log_user $DEBUG
spawn -noecho /bin/bash

#

if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
	# 
	# unmount disk
	#
	send_user "Unmount External dbroot disk                    "
	expect -re "# "
	send " \n"
	send date\n
	send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/syslogSetup.sh uninstall;umount /usr/local/Calpont/data*'\n"
	expect {
		-re "Host key verification failed" { send_user "ERROR: Host key verification failed\n" ; exit 1}
		-re "service not known" { send_user "ERROR: Invalid Host\n" ; exit 1}
		-re "authenticity" { send "yes\n" 
							expect {
								-re "word: " { send "$PASSWORD\n" } abort
							}
		}
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	}
	expect {
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
		-re "# "                  { send_user "DONE" } abort
	}
	send_user "\n"
	#
	# remove Calpont files
	#
	send_user "Uninstall Calpont Package                       "
	expect -re "# "
	send " \n"
	send date\n
	send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/pre-uninstall; rm -f /etc/init.d/infinidb; rm -f /etc/init.d/mysql-Calpont;rm -f /usr/local/Calpont/releasenum'\n"
	expect {
		-re "Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1}
		-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit 1}
		-re "authenticity" { send "yes\n" 
							expect {
								-re "word: " { send "$PASSWORD\n" } abort
							}
		}
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	}
	expect {
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
		-re "# "                  { send_user "DONE" } abort
	}
	send_user "\n"
}
if { $INSTALLTYPE == "uninstall" } { 
	exit 0 
}

# 
# send the Calpont package
#
expect -re "# "
send_user "Copy New Calpont Package to Module              "
send " \n"
send date\n
send "scp $CALPONTPKG  $USERNAME@$SERVER:$CALPONTPKG\n"
set timeout 10
expect {
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase: " { send "$PASSWORD\n" } abort
}
set timeout 120
expect {
	-re "100%" 				{ send_user "DONE" } abort
	-re "scp"  				{ send_user "ERROR\n" ; 
				 			send_user "\n*** Installation ERROR\n" ; 
							exit 1 }
	-re "Permission denied, please try again"         { send_user "ERROR: Invalid password\n" ; exit 1 }
	-re "No such file or directory" { send_user "ERROR: Invalid package\n" ; exit 1 }
	-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
}
send_user "\n"
#
expect -re "# "
#
# install package
#
send_user "Install Calpont Package on Module               "
send " \n"
send date\n
send "ssh $USERNAME@$SERVER 'cd /usr/local/;rm -f *.bin.tar.gz;cp $CALPONTPKG .;tar --exclude db -zxvf *.bin.tar.gz;cat Calpont/releasenum'\n"
set timeout 10
expect {
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase: " { send "$PASSWORD\n" } abort
}
set timeout 120
expect {
	-re "release=" 		  	{ send_user "DONE" } abort
	-re "No such file" 		  { send_user "ERROR: Binary Install Failed, binary/releasenum not found\n" ; exit 1 }
	-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
}
#sleep to give time for cat Calpont/releasenum to complete
sleep 5

send_user "\n"
send_user "Run post-install script                         "
send " \n"
send date\n
send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/post-install'\n"
set timeout 10
expect {
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase: " { send "$PASSWORD\n" } abort
}
set timeout 30
# check return
expect {
	-re "InfiniDB syslog logging not working" { send_user "ERROR: InfiniDB System logging not setup\n" ; exit 1 }
	-re "# " 		  		  { send_user "DONE" } abort
	-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
}
send_user "\n"
expect -re "# "
#
if { $INSTALLTYPE == "initial"} {
	#
	# copy over calpont config file
	#
	send_user "Copy Calpont Config file to Module              "
	send " \n"
	send date\n
	send "scp /usr/local/Calpont/etc/*  $USERNAME@$SERVER:/usr/local/Calpont/etc/.\n"
	set timeout 10
	expect {
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
	}
	set timeout 60
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	}
	send_user "\n"
	#
	# copy over custom OS tmp files
	#
	send_user "Copy Custom OS files to Module                  "
	send " \n"
	send date\n
	send "scp -r /usr/local/Calpont/local/etc  $USERNAME@$SERVER:/usr/local/Calpont/local/.\n"
	set timeout 10
	expect {
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
	}
	set timeout 60
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	}
	send_user "\n"
	#
	# copy over calpont OS files
	#
	send_user "Copy Calpont OS files to Module                 "
	send " \n"
	send date\n
	send "scp /usr/local/Calpont/local/etc/$MODULE/*  $USERNAME@$SERVER:/usr/local/Calpont/local/.\n"
	set timeout 10
	expect {
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	}
	set timeout 60
	expect {
		-re "# " 		  		  { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	}
	send_user "\n"
	#
	# Start module installer to setup Customer OS files
	#
	send_user "Run Module Installer                            "
	send " \n"
	send date\n
	send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/module_installer.sh $PKGTYPE'\n"
	set timeout 10
	expect {
		-re "word: " { send "$PASSWORD\n" } abort
		-re "passphrase: " { send "$PASSWORD\n" } abort
	}
	set timeout 60
	expect {
		-re "!!!Module" 				  			{ send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		-re "FAILED"   								{ send_user "ERROR: missing module file\n" ; exit 1 }
		-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
	}
	send_user "\n"
	expect -re "# "
	if { $MODULETYPE == "um" || $SERVERTYPE == "2" } { 
		#
		# run mysql setup scripts
		#
		send_user "Run MySQL Setup Scripts on Module               "
		send " \n"
		send date\n
		send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/post-mysqld-install'\n"
		set timeout 10
		expect {
			-re "word: " { send "$PASSWORD\n" } abort
			-re "passphrase: " { send "$PASSWORD\n" } abort
		}
		set timeout 60
		expect {
			-re "ERROR" { send_user "ERROR: Daemon failed to run";
			exit 1 }
			-re "FAILED" { send_user "ERROR: Daemon failed to run";
			exit 1 }
			-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
		}
		#
		send " \n"
		send date\n
		send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/post-mysql-install'\n"
		set timeout 10
		expect {
			-re "word: " { send "$PASSWORD\n" } abort
			-re "passphrase: " { send "$PASSWORD\n" } abort
		}
		set timeout 60
		expect {
			-re "Shutting down mysql." { send_user "DONE" } abort
			-re "# " 	{ send_user "DONE" } abort
			timeout { send_user "DONE" } abort
			-re "ERROR" { send_user "ERROR: Daemon failed to run";
			exit 1 }
			-re "FAILED" { send_user "ERROR: Daemon failed to run";
			exit 1 }
			-re "Read-only file system" { send_user "ERROR: local disk - Read-only file system\n" ; exit 1}
		}
		send_user "\n"
	}
}

#
# check InfiniDB syslog functionality
#
set timeout 10
expect -re "# "

send_user "Check InfiniDB system logging functionality     "
send " \n"
send date\n
send "ssh $USERNAME@$SERVER '$BASH /usr/local/Calpont/bin/syslogSetup.sh check'\n"
set timeout 10
expect {
	-re "word: " { send "$PASSWORD\n" } abort
	-re "passphrase: " { send "$PASSWORD\n" } abort
}
set timeout 30
expect {
	-re "Logging working" { send_user "DONE" } abort
	timeout { send_user "DONE" } abort
	-re "not working" { send_user "ERROR: InfiniDB system logging functionality not working";
	exit 1 }
}
send_user "\n"

#
send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0

