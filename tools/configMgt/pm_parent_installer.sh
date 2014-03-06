#!/usr/bin/expect
#
# $Id: parent_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Parent OAM Installer, copy RPM's and custom OS files from postConfigure script
# Argument 0 - Parent OAM IP address
# Argument 1 - Root Password of Parent OAM Module
# Argument 2 - Calpont Config File
# Argument 3 - Debug flag 1 for on, 0 for off

set USERNAME root
set SERVER [lindex $argv 0]
set PASSWORD [lindex $argv 1]
set PACKAGE [lindex $argv 2]
set RELEASE [lindex $argv 3]
set CONFIGFILE [lindex $argv 4]
set PREFIX [lindex $argv 5]
set USERNAME [lindex $argv 6]
set DEBUG [lindex $argv 7]

set CALPONTPACKAGE1 infinidb-libs-$PREFIX$PACKAGE
set CALPONTPACKAGE2 infinidb-platform-$PREFIX$PACKAGE
set CALPONTPACKAGE3 infinidb-enterprise-$PREFIX$PACKAGE
set MYSQLPACKAGE infinidb-storage-engine-$PACKAGE
set MYSQLDPACKAGE infinidb-mysql-$PACKAGE
set SHARED "//calweb/shared"
set INSTALLDIR "/usr/local"

log_user $DEBUG
spawn -noecho /bin/bash
send "rm -f $PACKAGE\n"
# 
# delete and erase all old packages from Parent OAM Module
#
set timeout 30
send "ssh $USERNAME@$SERVER 'rm -f /root/calpont-*.rpm /root/infinidb*.rpm'\n"
expect {
	-re "authenticity" { send "yes\n" 
						expect {
							-re "word: " { send "$PASSWORD\n" }
							}
						}
	-re "service not known" { send_user "FAILED: Invalid Host\n" ; exit -1 }
	-re "word: " { send "$PASSWORD\n" }
}
expect {
	-re {[$#] } 						{ }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
}
# 
# get the InfiniDB package
#
expect -re {[$#] }
send_user "Get InfiniDB Packages                         "
send "smbclient $SHARED -Wcalpont -Uoamuser%Calpont1 -c 'cd Iterations/$RELEASE/;prompt OFF;mget $PACKAGE'\n"
expect {
	-re "NT_STATUS_NO_SUCH_FILE" { send_user "FAILED: $PACKAGE not found\n" ; exit -1 }
	-re "getting" 				 { send_user "DONE" }
}
send_user "\n"
# 
# send the InfiniDB package
#
set timeout 120
send_user "Copy InfiniDB Packages                        "
send "scp -q $PACKAGE $USERNAME@$SERVER:/root/.\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
expect {
	-re {[$#] } 						{ send_user "DONE" }
	-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
	-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
}
send_user "\n"
#send "rm -f $PACKAGE\n"
# 
# erase InfiniDB package
#
set timeout 60
expect -re {[$#] }
send_user "Erase Old InfiniDB Package                    "
send "ssh $USERNAME@$SERVER 'rpm -e --nodeps \$(rpm -qa | grep '^calpont') >/dev/null 2>&1; rpm -e --nodeps \$(rpm -qa | grep '^infinidb-')'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "uninstall completed" {  }
	-re {[$#] }               {  }
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
sleep 10
send "ssh $USERNAME@$SERVER 'rm -f $INSTALLDIR/Calpont/releasenum >/dev/null 2>&1; test -x $INSTALLDIR/Calpont/bin/pre-uninstall && $INSTALLDIR/Calpont/bin/pre-uninstall'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re {[$#] }                  { send_user "DONE" }
	-re "uninstall completed" { send_user "DONE" }
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
}
send_user "\n"
sleep 5
#
# install InfiniDB package
#
set timeout 160
send_user "Install New InfiniDB Packages                 "
send "ssh $USERNAME@$SERVER ' rpm -ivh --nodeps $CALPONTPACKAGE1 $CALPONTPACKAGE2 $CALPONTPACKAGE3'\n"
expect -re "word: "
# password for ssh
send "$PASSWORD\n"
# check return
expect {
	-re "completed" 		  { send_user "DONE" }
	-re "Failed dependencies" { send_user "FAILED: Failed dependencies\n" ; 
								send_user "\n*** Installation Failed\n" ; 
									exit -1 }
	-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
	-re "File not found"   { send_user "FAILED: File not found\n" ; exit -1 }
}
send_user "\n"
set timeout 120
expect -re {[$#] }
send "rm -f $PACKAGE\n"
#
if { $CONFIGFILE != "NULL"} {
	send "scp $CONFIGFILE $USERNAME@$SERVER:/usr/local/Calpont/etc/Calpont.xml\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{  }
		-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
		-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
		-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
	}
	#
	# copy over Calpont.xml file
	#
	send_user "Copy InfiniDB Configuration File              "
	send "scp $CONFIGFILE $USERNAME@$SERVER:/usr/local/Calpont/etc/Calpont.xml.rpmsave\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{ send_user "DONE" }
		-re "scp"  						{ send_user "FAILED: SCP failure\n" ; exit -1 }
		-re "Permission denied, please try again"         { send_user "FAILED: Invalid password\n" ; exit -1 }
		-re "No such file or directory" { send_user "FAILED: Invalid package\n" ; exit -1 }
	}
	#do a dummy scp command
	send "scp $CONFIGFILE $USERNAME@$SERVER:/tmp/Calpont.xml\n"
	expect -re "word: "
	# send the password
	send "$PASSWORD\n"
	expect {
		-re "100%" 				  		{ send_user " " }
	}
} else {
	#
	# rename previous installed config file
	#
	send_user "Copy RPM-saved InfiniDB Configuration File     "
	send "ssh $USERNAME@$SERVER 'cd /usr/local/Calpont/etc/;mv -f Calpont.xml Calpont.xml.install;cp -v Calpont.xml.rpmsave Calpont.xml'\n"
	expect -re "word: "
	# password for ssh
	send "$PASSWORD\n"
	# check return
	expect {
		-re "Calpont.xml"         { send_user "DONE" }
		-re "Permission denied, please try again"   { send_user "FAILED: Invalid password\n" ; exit -1 }
	}
}
send_user "\n"
expect -re {[$#] }
#
exit

