#!/usr/bin/expect
#
# $Id: calpontUninstall.sh 995 2008-09-13 01:57:47Z dhill $
#
# Uninstall Package from system
# Argument 1 - Root Password of remote server

set USERNAME root
set PASSWORD " "
set DEBUG 0

set CALPONTRPM "calpont"
set CONNECTORRPM1 "calpont-mysql"
set CONNECTORRPM2 "calpont-mysqld"

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
	}
	if { $arg($i) == "-p" } {
		incr i
		set PASSWORD $arg($i)
	} else {
		if { $arg($i) == "-d" } {
			set DEBUG 1
		}
	}
	incr i
}

log_user $DEBUG

set timeout 2
set INSTALL 2
send "/usr/local/Calpont/bin/getConfig DBRM_Controller NumWorkers\n"
expect {
        -re 1                         { set INSTALL 1 } abort
}

set PACKAGE "rpm"
send "/usr/local/Calpont/bin/getConfig Installation EEPackageType\n"
expect {
        -re rpm                         { set PACKAGE rpm } abort
        -re deb                         { set PACKAGE deb } abort
        -re binary                         { set PACKAGE binary } abort
}

set timeout 60
log_user $DEBUG
if { $INSTALL == "2" && $PASSWORD == " "} {puts "please enter the remote server root password, enter ./calpontUninstall.sh -h for additional info"; exit -1}

send_user "\nPerforming InfiniDB System Uninstall\n\n"

# 
# shutdownSystem
#
send_user "Shutdown InfiniDB System                         "
expect -re "# "
send "/usr/local/Calpont/bin/calpontConsole shutdownsystem y\n"
expect {
	-re "shutdownSystem "       { send_user "DONE" } abort
}
send_user "\n"

if { $INSTALL == "2"} {
	set timeout 600
	#
	# Run installer
	#
	send_user "Run System Uninstaller                           "
	send "/usr/local/Calpont/bin/installer $CALPONTRPM $CONNECTORRPM1 $CONNECTORRPM2 uninstall $PASSWORD dummyxmwd --nodeps dummymysqlpw $DEBUG\n"
	expect {
		-re "uninstall request successful" 			{ send_user "DONE" } abort
		-re "error"   								{ send_user "FAILED" ; exit -1 }
	}
	send_user "\n"
}

if { $PACKAGE == "binary" } {
	send "/usr/local/Calpont/bin/pre-uninstall\n"
	expect {
		-re "# "                  {  } abort
	}

	send_user "\n"

	send_user "\nCalpont Package System Uninstall Completed\n\n"

	exit 0
}

send_user "\nCalpont Package System Uninstall Completed\n\n"

exit 0
