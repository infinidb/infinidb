#!/usr/bin/expect
#
# $Id$
#
# Uninstall Package from system

set INFINIDB_INSTALL_DIR "/usr/local/Calpont"
set env(INFINIDB_INSTALL_DIR) $INFINIDB_INSTALL_DIR

set USERNAME $env(USER)
set PASSWORD " "
set DEBUG 0

set INFINIDBRPM1 "infinidb-libs"
set INFINIDBRPM2 "infinidb-platform"
set INFINIDBRPM3 "infinidb-enterprise"
set CONNECTORRPM1 "infinidb-mysql"
set CONNECTORRPM2 "infinidb-storage-engine"

spawn -noecho /bin/bash

for {set i 0} {$i<[llength $argv]} {incr i} {
	set arg($i) [lindex $argv $i]
}

set i 0
while true {
	if { $i == [llength $argv] } { break }
	if { $arg($i) == "-h" } {
		send_user "\n"
		send_user "'calpontUninstall.sh' performs a system uninstall of the Calpont InfiniDB Packages.\n"
		send_user "It will perform a shutdown of the InfiniDB software and the \n"
		send_user "remove the Packages from all configured servers of the InfiniDB System.\n"
		send_user "\n"
		send_user "Usage: calpontUninstall.sh -p 'password' -d\n"
		send_user "		password    - root password of the remote servers being un-installed'\n"
		send_user "		-d		- Debug flag, output verbose information\n"
		exit
	} elseif { $arg($i) == "-p" } {
		incr i
		set PASSWORD $arg($i)
	} elseif { $arg($i) == "-d" } {
		set DEBUG 1
	} elseif { $arg($i) == "-i" } {
		incr i
		set INSTALLDIR $arg($i)
	} elseif { $arg($i) == "-u" } {
		incr i
		set USERNAME $arg($i)
	}
	incr i
}

log_user $DEBUG

set timeout 2
set INSTALL 2
send "$INFINIDB_INSTALL_DIR/bin/getConfig DBRM_Controller NumWorkers\n"
expect {
        1                         { set INSTALL 1 }
}

set PACKAGE "rpm"
send "$INFINIDB_INSTALL_DIR/bin/getConfig Installation EEPackageType\n"
expect {
        rpm                         { set PACKAGE rpm }
        deb                         { set PACKAGE deb }
        binary                         { set PACKAGE binary }
}

set timeout 60
log_user $DEBUG
if { $INSTALL == "2" && $PASSWORD == " "} {puts "please enter the remote server root password, enter ./calpontUninstall.sh -h for additional info"; exit -1}

send_user "\nPerforming InfiniDB System Uninstall\n\n"

# 
# shutdownSystem
#
send_user "Shutdown InfiniDB System                         "
expect -re {[$#] }
send "$INFINIDB_INSTALL_DIR/bin/calpontConsole shutdownsystem y\n"
expect {
	"shutdownSystem "       { send_user "DONE" }
}
send_user "\n"


if { $INSTALL == "2"} {
	set timeout 600
	#
	# Run installer
	#
	send_user "Run System Uninstaller                           "
	send "$INFINIDB_INSTALL_DIR/bin/installer $INFINIDBRPM1 $INFINIDBRPM2 $INFINIDBRPM3 $CONNECTORRPM1 $CONNECTORRPM2 uninstall $PASSWORD n --nodeps dummymysqlpw $DEBUG\n"
	expect {
		"uninstall request successful" 			{ send_user "DONE" }
		"ERROR"   								{ send_user "FAILED" ; exit -1 }
	}
	send_user "\n"
}

if { $PACKAGE == "binary" } {
	send "$INFINIDB_INSTALL_DIR/bin/pre-uninstall\n"
	expect {
		-re {[$#] }                  {  }
	}

	send_user "\n"

	send_user "\nCalpont Package System Uninstall Completed\n\n"

	exit 0
}

send_user "\nCalpont Package System Uninstall Completed\n\n"

exit 0
