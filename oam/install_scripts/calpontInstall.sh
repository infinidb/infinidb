#!/usr/bin/expect
#
# $Id: system_installer.sh 995 2008-09-13 01:57:47Z dhill $
#
# Install Package on system
# Argument 1 - Package Version being installed (1.0.0-100)
# Argument 2 - DBMS connector type
# Argument 3 - Root Password of remote server
# Argument 4 - Root Password of External Mode

set timeout 30
set USERNAME root
set RPMVERSION " "
set PASSWORD " "
set XMPASSWORD dummy
set MYSQLPASSWORD dummymysqlpw
set PACKAGE " "
set CONFIGFILE " "
set DEBUG 0
set NODEPS "-h"

spawn -noecho /bin/bash

for {set i 0} {$i<[llength $argv]} {incr i} {
	set arg($i) [lindex $argv $i]
}

set i 0
while true {
	if { $i == [llength $argv] } { break }
	if { $arg($i) == "-h" } {
		send_user "\n"
		send_user "'calpontInstaller.sh' performs a system install of the Calpont InfiniDB Packages\n"
		send_user "from the /root/ directory. These Packages would have already been installed\n"
		send_user "on the local Module.\n"
		send_user "Usage: calpontInstaller.sh -v 'infinidb-version' -p 'password' -t 'package-type' -c 'config-file'-m 'mysql-password' -d\n"
		send_user "		infinidb-version - InfiniDB Version, i.e. 1.0.0-1\n"
		send_user "		password    	- root password on the servers being installed'\n"
		send_user "		package-type 	- Package Type being installed (rpm, deb, or binary)\n"
		send_user "		config-file 	- Optional: Calpont.xml config file with directory location, i.e. /root/Calpont.xml\n"
		send_user "			 	 	Default version is /usr/local/Calpont/etc/Calpont.xml.rpmsave\n"
		send_user "		mysql-password    - MySQL password on the servers being installed'\n"		
		send_user "		-d		- Debug flag, output verbose information\n"
		exit 0
	}
	if { $arg($i) == "-v" } {
		incr i
		set RPMVERSION $arg($i)
	} else {
		if { $arg($i) == "-p" } {
			incr i
			set PASSWORD $arg($i)
		} else {
			if { $arg($i) == "-t" } {
				incr i
				set PACKAGE $arg($i)
			} else {
				if { $arg($i) == "-c" } {
					incr i
					set CONFIGFILE $arg($i)
				} else {
					if { $arg($i) == "-d" } {
						set DEBUG 1
					} else {
						if { $arg($i) == "-f" } {
							set NODEPS "--nodeps"
						} else {
							if { $arg($i) == "-m" } {
								incr i
								set MYSQLPASSWORD $arg($i)
							}
						}
					}
				}
			}
		}
	}
	incr i
}

log_user $DEBUG

set timeout 2
send "/usr/local/Calpont/bin/infinidb status\n"
expect {
        -re "is running"	{ puts "InfiniDB is running, can't run calpontInstall.sh while InfiniDB is running. Exiting..\n";
						exit 1
					}
}

if { $CONFIGFILE == " " } {
	set CONFIGFILE /usr/local/Calpont/etc/Calpont.xml.rpmsave
}

if { [catch { open $CONFIGFILE "r"} handle ] } {
	puts "Calpont Config file not found: $CONFIGFILE"; exit 1
}

exec rm -f /usr/local/Calpont/etc/Calpont.xml.new  > /dev/null 2>&1
exec mv -f /usr/local/Calpont/etc/Calpont.xml /usr/local/Calpont/etc/Calpont.xml.new  > /dev/null 2>&1
exec /bin/cp -f $CONFIGFILE  /usr/local/Calpont/etc/Calpont.xml  > /dev/null 2>&1

set timeout 2
set INSTALL 2
send "/usr/local/Calpont/bin/getConfig DBRM_Controller NumWorkers\n"
expect {
        -re 1                         { set INSTALL 1
										set PASSWORD "dummy"
										set RPMVERSION "rpm" } abort
}


if { $INSTALL == "2" && $PASSWORD == " "} {puts "please enter the remote server root password, enter ./calpontInstaller.sh -h for additional info"; exit 1}

if { $INSTALL == "2" && $RPMVERSION == " " } {puts "please enter Package version, enter ./calpontInstaller.sh -h for additional info"; exit 1}

send_user "\n"

if { $INSTALL == "2" } {
	if { $PACKAGE == "rpm" } {
		set CALPONTPACKAGE /root/calpont-$RPMVERSION*.rpm
		set CONNECTORPACKAGE1 /root/calpont-mysql-$RPMVERSION*.rpm
		set CONNECTORPACKAGE2 /root/calpont-mysqld-$RPMVERSION*.rpm
		send_user "Installing InfiniDB Packages: $CALPONTPACKAGE, $CONNECTORPACKAGE1, $CONNECTORPACKAGE2\n\n"
		set EEPKG "rpm"
		} else {
		if { $PACKAGE == "deb" } {
			set CALPONTPACKAGE /root/calpont_$RPMVERSION*.deb
			set CONNECTORPACKAGE1 /root/calpont-mysql_$RPMVERSION*.deb
			set CONNECTORPACKAGE2 /root/calpont-mysqld_$RPMVERSION*.deb
			send_user "Installing InfiniDB Packages: $CALPONTPACKAGE, $CONNECTORPACKAGE1, $CONNECTORPACKAGE2\n\n"
			set EEPKG "deb"
		} else {
			if { $PACKAGE == "binary" } {
			set CALPONTPACKAGE /root/calpont-infinidb-ent-$RPMVERSION*bin.tar.gz
			set CONNECTORPACKAGE1 "nopkg"
			set CONNECTORPACKAGE2 "nopkg"
			send_user "Installing InfiniDB Package: $CALPONTPACKAGE\n\n"
			set EEPKG "binary"
			} else {
				puts "please enter Valid Package Type, enter ./calpontInstaller.sh -h for additional info"; exit 1
			}
		}
	}
} else {
	set CALPONTPACKAGE "dummy.rpm"
	set CONNECTORPACKAGE1 "dummy.rpm"
	set CONNECTORPACKAGE2 "dummy.rpm"
	set EEPKG "rpm"
}

send_user "Performing InfiniDB System Install, please wait...\n"

send "/usr/local/Calpont/bin/setConfig Installation EEPackageType $EEPKG\n" 
expect {
	-re "# "                  {  } abort
}

send_user "\n"
set timeout 600
#
# Run installer
#
send "/usr/local/Calpont/bin/installer $CALPONTPACKAGE $CONNECTORPACKAGE1 $CONNECTORPACKAGE2 initial $PASSWORD $XMPASSWORD $NODEPS  $MYSQLPASSWORD $DEBUG\n"
expect {
	-re "InfiniDB Install Successfully Completed" 	{ } abort
	-re "ERROR"   { send_user "FAILED: error returned from installer, execute with debug mode on to determine error\n" ; exit 1 }
	-re "Enter MySQL password"   { send_user "FAILED: a MySQL password is set\n" ; exit 1 }	
	timeout	{ send_user "FAILED: Timeout while running installer, execute with debug mode on to determine error\n" ; exit 1 }
}

send_user "\nCalpont Package System Install Completed\n\n"

exit 0
