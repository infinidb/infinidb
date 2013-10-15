#!/usr/bin/expect
#
# $Id: parent_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Parent OAM Installer, copy RPM's and custom OS files from postConfigure script
# Argument 0 - Parent OAM IP address
# Argument 1 - Root Password of Parent OAM Module
# Argument 2 - Calpont Config File
# Argument 3 - Debug flag 1 for on, 0 for off

set timeout 40
set USERNAME root
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set PACKAGE [lindex $argv 2]
set RELEASE [lindex $argv 3]
set CONFIGFILE [lindex $argv 4]
set DEBUG [lindex $argv 5]
set CALPONTPACKAGE infinidb-platform-$PACKAGE
set CALPONTPACKAGE0 infinidb-0$PACKAGE
set CALPONTPACKAGE1 infinidb-1$PACKAGE
set ORACLEPACKAGE infinidb-oracle$PACKAGE
set MYSQLPACKAGE infinidb-storage-engine-$PACKAGE
set MYSQLDPACKAGE infinidb-mysql-$PACKAGE

set SHARED "//calweb/shared"

log_user $DEBUG
spawn -noecho /bin/bash
send "rm -f $PACKAGE,$CALPONTPACKAGE0,$CALPONTPACKAGE1,$ORACLEPACKAGE,$MYSQLPACKAGE,$MYSQLDPACKAGE\n"
# 
# delete and erase all old packages from Director Module
#
send "ssh $USERNAME@$SERVER 'rm -f /root/calpont*.rpm'\n"
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
	-re "#" 						{ } abort
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
}
#
# erase calpont-oracle package
#
expect -re "# "
send_user "Erase Old Calpont-Oracle Connector Package    "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches calpont-oracle'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "# "                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
# 
# erase infinidb-mysql package
#
expect -re "# "
send_user "Erase Old Calpont-Mysqld Connector Package    "
send "ssh $USERNAME@$SERVER 'pkill -9 mysqld'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "# "                  { } abort
}
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches infinidb-mysql'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "# "                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
# 
# erase infinidb-storage-engine package
#
expect -re "# "
send_user "Erase Old Calpont-Mysql Connector Package     "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches infinidb-storage-engine'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "# "                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
send "rm -f $PACKAGE\n"
# 
# erase calpont package
#
expect -re "# "
send_user "Erase Old Calpont Packages                     "
send "ssh $USERNAME@$SERVER ' rpm -e --nodeps --allmatches infinidb-libs infinidb-platform infinidb-enterprise'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" { send_user "DONE" } abort
	-re "# "                  { send_user "DONE" } abort
	-re "not installed"       { send_user "WARNING: Package not installed" } abort
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
# 
# get the calpont package
#
expect -re "# "
send_user "Get Calpont Package                           "
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $CALPONTPACKAGE0'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { 
		send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $CALPONTPACKAGE1'\n"
		expect {
			-re "NT_STATUS_NO_SUCH_FILE" { send_user "FAILED: $CALPONTPACKAGE not found\n" ; exit -1 }
			-re "getting" 				 { send_user "DONE" } abort
		}
	}
	-re "getting" 				 { send_user "DONE" } abort
}
send_user "\n"
# 
# send the calpont package
#
send_user "Copy Calpont Package                          "
send "scp $CALPONTPACKAGE $USERNAME@$SERVER:/root/.\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
expect {
	-re "100%" 						{ send_user "DONE" } abort
	-re "scp"  						{ send_user "FAILED\n" ; 
				 			send_user "\n*** Installation Failed\n" ; 
							exit -1 }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
}
send_user "\n"
send "rm -f $PACKAGE\n"
#
# install calpont package
#
expect -re "# "
set timeout 120
send_user "Install New Calpont Package                   "
send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$CALPONTPACKAGE'\n"
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
set timeout 40
expect -re "# "
send "rm -f $PACKAGE\n"
#
if { $CONFIGFILE != "NULL"} {
	#
	# copy over Calpont.xml file
	#
	send_user "Copy Calpont Configuration File               "
	send "scp $CONFIGFILE $USERNAME@$SERVER:/usr/local/Calpont/etc/Calpont.xml\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{ send_user "DONE" } abort
		-re "scp"  						{ send_user "FAILED\n" ; 
								send_user "\n*** Installation Failed\n" ; 
								exit -1 }
		-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
		-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
	}
} else {
	#
	# rename previous installed config file
	#
	send_user "Copy RPM-saved Calpont Configuration File     "
	send "ssh $USERNAME@$SERVER 'cd /usr/local/Calpont/etc/;mv -f Calpont.xml Calpont.xml.install;cp -v Calpont.xml.rpmsave Calpont.xml'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Calpont.xml"         { send_user "DONE" } abort
		-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
	}
}
send_user "\n"
# 
# get the calpont-oracle package
#
set timeout 40
expect -re "# "
send_user "Get Calpont-Oracle Connector Package          "
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $ORACLEPACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "WARNING: $ORACLEPACKAGE not found, skipping\n" } abort
	-re "getting"	{ send_user "DONE\n"
						# 
						# send the calpont-oracle package
						#
						expect -re "# "
						send_user "Copy Calpont-Oracle Connector Package         "
						send "scp $ORACLEPACKAGE $USERNAME@$SERVER:/root/.\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "100%" 						{ send_user "DONE" } abort
							-re "scp"  						{ send_user "FAILED\n" ; 
													send_user "\n*** Installation Failed\n" ; 
													exit -1 }
							-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
							-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
						}
						#
						# install calpont-oracle package
						#
						send_user "\n"
						expect -re "# "
						set timeout 120
						send_user "Install Calpont-Oracle Connector Package      "
						send "ssh $USERNAME@$SERVER ' rpm -ivh /root/$ORACLEPACKAGE'\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "completed" 		  { send_user "DONE" } abort
							-re "Failed dependencies" { send_user "FAILED: Failed dependencies" ; exit -1 }
							-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
						}
						send_user "\n"
					}
}
set timeout 40
expect -re "# "
# 
# get the calpont-mysql package
#
send_user "Get Calpont-Mysql Connector Package           "
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $MYSQLPACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "WARNING: $MYSQLPACKAGE not found, skipping\n" } abort
	-re "getting"	{ send_user "DONE\n"
						# 
						# send the calpont-mysql package
						#
						expect -re "# "
						send_user "Copy Calpont-Mysql Connector Package          "
						send "scp $MYSQLPACKAGE $USERNAME@$SERVER:/root/.\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "100%" 						{ send_user "DONE" } abort
							-re "scp"  						{ send_user "FAILED\n" ; 
													send_user "\n*** Installation Failed\n" ; 
													exit -1 }
							-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
							-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
						}
						#
						# install calpont-mysql package
						#
						send_user "\n"
						expect -re "# "
						set timeout 120
						send_user "Install Calpont-Mysql Connector Package       "
						send "ssh $USERNAME@$SERVER ' rpm -ivh $MYSQLPACKAGE'\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "completed" 		  { send_user "DONE" } abort
							-re "Failed dependencies" { send_user "FAILED: Failed dependencies" ; exit -1 }
							-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
						}
						send_user "\n"
					}
}
expect -re "# "
# 
# get the infinidb-mysql package
#
send_user "Get Calpont-Mysqld Package                    "
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $MYSQLDPACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "WARNING: $MYSQLDPACKAGE not found, skipping\n" } abort
	-re "getting"	{ send_user "DONE\n"
						# 
						# send the infinidb-mysql package
						#
						expect -re "# "
						send_user "Copy Calpont-Mysqld Package                   "
						send "scp $MYSQLDPACKAGE $USERNAME@$SERVER:.\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "100%" 						{ send_user "DONE" } abort
							-re "scp"  						{ send_user "FAILED\n" ; 
													send_user "\n*** Installation Failed\n" ; 
													exit -1 }
							-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
							-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
						}
						#
						# install infinidb-mysql-mysqld package
						#
						send_user "\n"
						expect -re "# "
						set timeout 120
						send_user "Install Calpont-Mysqld Package                "
						send "ssh $USERNAME@$SERVER ' rpm -ivh $MYSQLDPACKAGE'\n"
						expect -re "word: "
						# password for ssh
						send "$PASSWORD\n"
						# check return
						expect {
							-re "completed" 		  { send_user "DONE" } abort
							-re "Failed dependencies" { send_user "FAILED: Failed dependencies" ; exit -1 }
							-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
						}
						send_user "\n"
					}
}
#
exit

