#!/usr/bin/expect
#
# $Id: user_installer.sh 1066 20081113 21:44:44Z dhill $
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
set CALPONTRPM1 [lindex $argv 3]
set CALPONTRPM2 [lindex $argv 4]
set CALPONTRPM3 [lindex $argv 5]
set CALPONTMYSQLRPM [lindex $argv 6]
set CALPONTMYSQLDRPM [lindex $argv 7]
set INSTALLTYPE [lindex $argv 8]
set PKGTYPE [lindex $argv 9]
set NODEPS [lindex $argv 10]
set MYSQLPW [lindex $argv 11]
set MYSQLPORT [lindex $argv 12]
set DEBUG [lindex $argv 13]
set INSTALLDIR "/usr/local/Calpont"
set IDIR [lindex $argv 14]
if { $IDIR != "" } {
	set INSTALLDIR $IDIR
}
set USERNAME "root"
set UNM [lindex $argv 15]
if { $UNM != "" } {
	set USERNAME $UNM
}

if { $MYSQLPW == "none" } {
	set MYSQLPW " "
} 

set BASH "/bin/bash "
if { $DEBUG == "1" } {
	set BASH "/bin/bash -x "
}

log_user $DEBUG
spawn -noecho /bin/bash
#
if { $PKGTYPE == "rpm" } {
	set PKGERASE "rpm -e --nodeps --allmatches "
	set PKGINSTALL "rpm -ivh $NODEPS "
	set PKGUPGRADE "rpm -Uvh --noscripts "
} else {
	if { $PKGTYPE == "deb" } {
		set PKGERASE "dpkg -P "
		set PKGINSTALL "dpkg -i --force-confnew"
		set PKGUPGRADE "dpkg -i --force-confnew"
	} else {
		send_user "Invalid Package Type of $PKGTYPE"
		exit 1
	}
}

# check and see if remote server has ssh keys setup, set PASSWORD if so
send_user " "
send "ssh $USERNAME@$SERVER 'time'\n"
set timeout 60
expect {
	"Host key verification failed" { send_user "FAILED: Host key verification failed\n" ; exit 1 }
	"service not known" { send_user "FAILED: Invalid Host\n" ; exit 1 }
	"authenticity" { send "yes\n" 
						expect {
							"word: " { send "$PASSWORD\n" }
							"passphrase" { send "$PASSWORD\n" }
						}
	}
	"sys" { set PASSWORD "ssh" }
	"word: " { send "$PASSWORD\n" }
	"passphrase" { send "$PASSWORD\n" }
	"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
	"Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
	"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	"No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
	timeout { send_user "ERROR: Timeout to host\n" ; exit 1 }
}
set timeout 30
expect {
	-re {[$#] }        {  }
	"sys" {  }
}
send_user "\n"
#BUG 5749 - SAS: didn't work on their system until I added the sleep 60
sleep 60

if { $INSTALLTYPE == "initial" || $INSTALLTYPE == "uninstall" } {
	# 
	# erase InfiniDB packages
	#
	send_user "Erase InfiniDB Packages on Module           "
	send "ssh $USERNAME@$SERVER '$PKGERASE calpont >/dev/null 2>&1; $PKGERASE calpont-mysql >/dev/null 2>&1; $PKGERASE calpont-mysqld >/dev/null 2>&1; $PKGERASE infinidb-enterprise >/dev/null 2>&1; $PKGERASE infinidb-libs infinidb-platform;$PKGERASE infinidb-storage-engine infinidb-mysql;$PKGERASE dummy'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 120
	expect {
		"package dummy" { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; exit 1 }
		"Permission denied, please try again"   { send_user "ERROR: Invalid password\n" ; exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
}

if { $INSTALLTYPE == "uninstall" } { exit 0 }

# 
# send the InfinIDB package
#
set timeout 30
#expect -re {[$#] }
send_user "Copy new InfiniDB Packages to Module              "
send "ssh $USERNAME@$SERVER 'rm -f /root/infinidb-*.$PKGTYPE'\n"
if { $PASSWORD != "ssh" } {
	set timeout 30
	expect {
		"word: " { send "$PASSWORD\n" }
		"passphrase" { send "$PASSWORD\n" }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
}
expect {
        -re {[$#] } { }
        "Connection refused"   { send_user "ERROR: Connection refused\n" ; exit 1 }
        "closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
        "No route to host"   { send_user "ERROR: No route to host\n" ; exit 1 }
}
set timeout 30
expect {
	-re {[$#] } { }
}

send "scp $CALPONTRPM1 $CALPONTRPM2 $CALPONTMYSQLRPM $CALPONTMYSQLDRPM $USERNAME@$SERVER:.;$PKGERASE dummy\n"
if { $PASSWORD != "ssh" } {
	set timeout 30
	expect {
		"word: " { send "$PASSWORD\n" }
		"passphrase" { send "$PASSWORD\n" }
	}
}
set timeout 120
expect {
	"package dummy" 	{ send_user "DONE" }
	"directory"  		{ send_user "ERROR\n" ; 
				 	send_user "\n*** Installation ERROR\n" ; 
					exit 1 }
	"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
}
send_user "\n"

#sleep to make sure it's finished
sleep 5
if { $CALPONTRPM3 != "dummy.rpm" } {
	expect -re {[$#] }
	send_user "Copy new InfiniDB Enterprise Packages to Module   "
	send "scp $CALPONTRPM3 $USERNAME@$SERVER:.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 120
	expect {
		"100%" 			{ send_user "DONE" }
		"directory"  		{ send_user "ERROR\n" ; 
						send_user "\n*** Installation ERROR\n" ; 
						exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
}

#sleep to make sure it's finished
sleep 5
#
set timeout 30
expect -re {[$#] }
if { $INSTALLTYPE == "initial"} {
	#
	# install package
	#
	send_user "Install InfinIDB Packages on Module               "
	send "ssh $USERNAME@$SERVER '$PKGINSTALL $CALPONTRPM1 $CALPONTRPM2 $CALPONTMYSQLDRPM $CALPONTMYSQLRPM;$PKGERASE dummy'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 180
	expect {
		"package dummy" 	  { send_user "DONE" }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
							send_user "\n*** Installation ERROR\n" ; 
							exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
		"needs"	   { send_user "ERROR: disk space issue\n" ; exit 1 }
		"conflicts"	   { send_user "ERROR: File Conflict issue\n" ; exit 1 }
	}
	send_user "\n"

	if { $CALPONTRPM3 != "dummy.rpm" } {
        expect -re {[$#] }
		send_user "Install InfinIDB Enterprise Packages on Module    "
		send "ssh $USERNAME@$SERVER '$PKGINSTALL $CALPONTRPM3'\n"
		if { $PASSWORD != "ssh" } {
			set timeout 30
			expect {
				"word: " { send "$PASSWORD\n" }
				"passphrase" { send "$PASSWORD\n" }
			}
		}
		set timeout 60
		expect {
			"completed" 		  { send_user "DONE" }
			"Setting up" 		  { send_user "DONE" }
			"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
								exit 1 }
			"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
		}
		send_user "\n"
	}

	set timeout 30
	#expect -re {[$#] }
} else {
	#
	# upgrade package
	#
	#expect -re {[$#] }
	send_user "Upgrade InfinIDB Packages on Module               "
	send "ssh $USERNAME@$SERVER '$PKGUPGRADE $CALPONTRPM1 $CALPONTRPM2 $CALPONTRPM3 $CALPONTMYSQLDRPM $CALPONTMYSQLRPM;$PKGERASE dummy'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 120
	expect {
		"package dummy" 		  { send_user "DONE" }
		"already installed"   { send_user "INFO: Already Installed\n" ; exit 1 }
		"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
									send_user "\n*** Installation ERROR\n" ; 
										exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"

	if { $CALPONTRPM3 != "dummy.rpm" } {
		expect -re {[$#] }
		send_user "Upgrade InfinIDB Enterprise Packages on Module    "
		send "ssh $USERNAME@$SERVER '$PKGUPGRADE $CALPONTRPM3'\n"
		if { $PASSWORD != "ssh" } {
			set timeout 30
			expect {
				"word: " { send "$PASSWORD\n" }
				"passphrase" { send "$PASSWORD\n" }
			}
		}
		set timeout 30
		expect {
			"completed" 		  { send_user "DONE" }
			"Setting up" 		  { send_user "DONE" }
			"already installed"   { send_user "INFO: Already Installed\n" ; exit 1 }
			"error: Failed dependencies" { send_user "ERROR: Failed dependencies\n" ; 
										send_user "\n*** Installation ERROR\n" ; 
											exit 1 }
			"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
		}
		send_user "\n"
	}

	set timeout 30
	expect -re {[$#] }
}
#sleep to make sure it's finished
sleep 5
#
if { $INSTALLTYPE == "initial"} {
	#
	# copy over InfiniDB config file
	#
	send_user "Copy InfiniDB Config file to Module              "
	send "scp $INSTALLDIR/etc/*  $USERNAME@$SERVER:$INSTALLDIR/etc/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		"directory"  		{ send_user "ERROR\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
								exit 1 }
		-re {[$#] } 		  		  { send_user "DONE" }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# copy over custom OS tmp files
	#
	send_user "Copy Custom OS files to Module                  "
	send "scp -r $INSTALLDIR/local/etc  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		"directory"  		{ send_user "ERROR\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
								exit 1 }
		-re {[$#] } 		  		  { send_user "DONE" }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# copy over InfiniDB OS files
	#
	send_user "Copy InfiniDB OS files to Module                 "
	send "scp $INSTALLDIR/local/etc/$MODULE/*  $USERNAME@$SERVER:$INSTALLDIR/local/.\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		"directory"  		{ send_user "ERROR\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
								exit 1 }
		-re {[$#] } 		  		  { send_user "DONE" }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	#sleep to make sure it's finished
	sleep 5
	#
	# Start module installer to setup Custom OS files
	#
	send_user "Run Module Installer                            "
	send "ssh $USERNAME@$SERVER '$BASH $INSTALLDIR/bin/module_installer.sh --module=um --port=$MYSQLPORT'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 30
	expect {
		"!!!Module" 				  			{ send_user "DONE" }
		"FAILED"   								{ send_user "ERROR: missing OS file\n" ; exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
		"No such file"   { send_user "ERROR: File Not Found\n" ; exit 1 }
	}
	send_user "\n"
	set timeout 30
	expect -re {[$#] }
	#
	# run mysql setup scripts
	#
	send_user "Run MySQL Setup Scripts on Module               "
	send "ssh $USERNAME@$SERVER '$BASH $INSTALLDIR/bin/post-mysqld-install'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 20
	expect {
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	set timeout 30
	expect -re {[$#] }
	#
	send "ssh $USERNAME@$SERVER '$BASH $INSTALLDIR/bin/post-mysql-install $MYSQLPW'\n"
	if { $PASSWORD != "ssh" } {
		set timeout 30
		expect {
			"word: " { send "$PASSWORD\n" }
			"passphrase" { send "$PASSWORD\n" }
		}
	}
	set timeout 60
	expect {
		"Shutting down mysql." { send_user "DONE" }
		-re {[$#] } 	{ send_user "DONE" }
		timeout { send_user "DONE" }
		"ERROR" { send_user "ERROR: Daemon failed to run";
		exit 1 }
		"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
	}
	send_user "\n"
	set timeout 30
	#expect -re {[$#] }
}

#
# check InfiniDB syslog functionality
#

send_user "Check InfiniDB system logging functionality     "
send " \n"
send date\n
send "ssh $USERNAME@$SERVER '$BASH $INSTALLDIR/bin/syslogSetup.sh check'\n"
if { $PASSWORD != "ssh" } {
	set timeout 30
	expect {
		"word: " { send "$PASSWORD\n" }
		"passphrase" { send "$PASSWORD\n" }
	}
}
set timeout 30
expect {
	"Logging working" { send_user "DONE" }
	timeout { send_user "DONE" }
	"not working" { send_user "WARNING: InfiniDB system logging functionality not working" }
	"closed"   { send_user "ERROR: Connection closed\n" ; exit 1 }
}
send_user "\n"

send_user "\nInstallation Successfully Completed on '$MODULE'\n"
exit 0
# vim:ts=4 sw=4:

