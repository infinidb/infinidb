#!/usr/bin/expect
#
# $Id: system_installer.sh 995 2008-09-13 01:57:47Z dhill $
#
# Install RPM on system
# Argument 1 - Package name being installed
# Argument 2 - Root Password of remote server
# Argument 3 - Root Password of External Mode

set timeout 10
set USERNAME root
set RPMPACKAGE " "
set PASSWORD " "
set CONFIGFILE " "
set DEBUG 0

spawn -noecho /bin/bash

for {set i 0} {$i<[llength $argv]} {incr i} {
	set arg($i) [lindex $argv $i]
}

set i 0
while true {
	if { $i == [llength $argv] } { break }
	if { $arg($i) == "-h" } {
		send_user "\n"
		send_user "'postInstaller.sh' performs a system install of a Calpont RPM\n"
		send_user "on a system with Calpont already installed or on a new system\n"
		send_user "when the -c option is used.\n"
		send_user "\n"
		send_user "Usage: postInstaller.sh -r 'calpont-rpm' -p 'password' -c 'config-file' -d\n"
		send_user "		calpont-rpm - Calpont RPM with directory locatation, i.e. /root/calpont.x.x.x.x\n"
		send_user "		password    - root password on the servers being installed'\n"
		send_user "		config-file - Optional: Calpont.xml config file with directory location, i.e. /root/Calpont.xml\n"
		send_user "		-d 			- Debug flag\n"
		exit
	}
	if { $arg($i) == "-r" } {
		incr i
		set RPMPACKAGE $arg($i)
	} else {
		if { $arg($i) == "-p" } {
			incr i
			set PASSWORD $arg($i)
		} else {
			if { $arg($i) == "-c" } {
				incr i
				set CONFIGFILE $arg($i)
			} else {
				if { $arg($i) == "-d" } {
					set DEBUG 1
				}
			}
		}
	}
	incr i
}

log_user $DEBUG

if { $RPMPACKAGE == " " || $PASSWORD == " "} {puts "please enter both RPM and password, enter ./postInstaller.sh -h for additional info"; exit -1}

if { $CONFIGFILE == " " } {
	set CONFIGFILE /usr/local/Calpont/etc/Calpont.xml.rpmsave
}
if { [catch { open $CONFIGFILE "r"} handle ] } {
	puts "Calpont Config file not found: $CONFIGFILE"; exit -1
}


send_user "\nPerforming Calpont RPM System Install\n\n"

# 
# stopSystem
#
send_user "Stop Calpont System                             "
expect -re "# "
send "/usr/local/Calpont/bin/calpontConsole stopSystem INSTALL y\n"
expect {
	-re "# "                  	{ send_user "DONE" } abort
	-re "**** stopSystem Failed" { send_user "INFO: System not running" } abort
}
send_user "\n"
# 
# unmount disk
#
send_user "Unmount disk                                    "
expect -re "# "
send "umount /usr/local/Calpont/data*\n"
expect {
	-re "# "                  { send_user "DONE" } abort
}
send_user "\n"
# 
# erase package
#
send_user "Erase Calpont Package on Module                 "
expect -re "# "
send "rpm -e --nodeps calpont\n"
expect {
	-re "# "                  { send_user "DONE" } abort
	-re "uninstall completed" { send_user "DONE" } abort
	-re "ERROR dependencies" { send_user "ERROR: ERROR dependencies\n" ; exit -1 }
	-re "not installed"       { send_user "INFO: Package not installed" } abort
}
send_user "\n"

set timeout 60
#
# install package
#
send_user "Install Calpont Package on Module               "
send "rpm -ivh $RPMPACKAGE\n"
expect {
	-re "completed" 		  { send_user "DONE" } abort
	-re "ERROR dependencies" { send_user "ERROR: ERROR dependencies\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
									exit -1 }
	-re "error" 			  { send_user "ERROR\n" ; 
								send_user "\n*** Installation ERROR\n" ; 
								exit -1 }
}
expect -re "# "
log_user 0
exec mv -f /usr/local/Calpont/etc/Calpont.xml /usr/local/Calpont/etc/Calpont.xml.new  > /dev/null 2>&1
exec mv -f $CONFIGFILE  /usr/local/Calpont/etc/Calpont.xml  > /dev/null 2>&1

send_user "\n"
set timeout 380
#
# Run installer
#
send_user "Run System Installer                            "
send "/usr/local/Calpont/bin/installer $RPMPACKAGE initial $PASSWORD n 0\n"
expect {
	-re "reboot request successful" 			{ } abort
	-re "error"   								{ send_user "FAILED" ; exit -1 }
}

send_user "\nCalpont RPM System Install Completed, System now rebooting\n\n"

exit 0
