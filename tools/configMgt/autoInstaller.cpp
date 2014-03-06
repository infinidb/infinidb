/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/******************************************************************************************
* $Id: autoInstaller.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by configure:
*		Calpont/etc/Calpont.xml
*
*		
******************************************************************************************/
/**
 * @file
 */

#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <glob.h>
#include <string>

#include <readline/readline.h>
#include <readline/history.h>

#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace oam;
using namespace config;

int main(int argc, char *argv[])
{
    Oam oam;
 	string systemName;
	string systemPackage;
	string systemUser = "root";
	string calpontPackage;
	string mysqlRPM;
	string mysqldRPM;
	string installParentModuleHostName;
	string installParentModuleIPAddr;
	string debug_flag = "0";
	string uninstall_debug = " ";
	string password;
	string XMpassword = "dummypw";
	string configFile = "NULL";
	string release = "Latest";
//	string SHARED = "//calweb/shared";
	string SHARED = "//srvfrisco2/public";
	string installDir = "/usr/local";
	string MySQLpassword = "dummymysqlpw";
	string installPackageType = "";

    char* pcommand = 0;
	string prompt;
	bool noprompt = false;
	string CE = "0";

	int forceVer = -1;

	Config* sysConfig = Config::makeConfig("./systems/CalpontSystems.xml");

	for( int i = 1; i < argc; i++ )
	{
		if( string("-h") == argv[i] ) {
			cout << endl;
			cout << "'autoInstaller' installs the RPMs located in" << endl;
			cout << "///srvfrisco2/public/Iterations on the specified" << endl;
			cout << "system. It will either install the latest rpm located in" << endl;
			cout << "in the /Latest directory or the rpm associated with the" << endl;
			cout << "release-number entered." << endl;
			cout << endl;
			cout << "The list of systems are located in '" << sysConfig->configFile() << "'" << endl;
			cout << endl;
			cout << "The 'Calpont.xml' used in the install will default to system" << endl;
			cout << "configuration of the 'Calpont.xml' located on the system being" << endl;
			cout << "or can be passed as an argument into 'quickInstaller'." << endl;
			cout << endl;
   			cout << "Usage: autoInstaller -s system [-h][-ce][-r release][-c configFile][-n][-d][-p package-type][-m mysql-password]" << endl;
			cout << "			-s system-name" << endl;
			cout << "			-ce community-edition install" << endl;
			cout << "			-r release-number (optional, default to 'Latest')" << endl;
			cout << "			-c InfiniDB Config File located in system-name directory (optional, default to system configuration)" << endl;
			cout << "			-n No Prompt (Used for automated Installs)" << endl;
			cout << "			-m System MySQL Password, if set" << endl;
			cout << "			-d Debug Flag" << endl;
			cout << "			-p Install Package Type (rpm or binary), defaults to " << sysConfig->configFile() << " setting" << endl;
			cout << "			-3 Force a version 3 install, defaults to autodetect" << endl;
			exit(0);
		}
      	else if( string("-s") == argv[i] ) {
			i++;
			if ( argc == i ) {
				cout << "ERROR: missing system argument" << endl;
				exit(1);
			}
			systemName = argv[i];
		}
		else if( string("-r") == argv[i] ) {
			i++;
			if ( argc == i ) {
				cout << "ERROR: missing release argument" << endl;
				exit(1);
			}
			release = argv[i];
		}
		else if( string("-c") == argv[i] ) {
			i++;
			if ( argc == i ) {
				cout << "ERROR: missing config file argument" << endl;
				exit(1);
			}
			configFile = argv[i];
			string systemConfig = "systems/" + systemName + "/" + configFile;
			ifstream oldFile (systemConfig.c_str());
			if (!oldFile) {
				cout << "ERROR: Config file '" + systemConfig + "' not found" << endl;
				exit(1);
			}
		}
		else if( string("-ce") == argv[i] ) {
			CE = "1";
		}
		else if( string("-n") == argv[i] ) {
			noprompt = true;
		}
		else if( string("-d") == argv[i] ) {
			debug_flag = "1";
			uninstall_debug = "-d";
		}
		else if( string("-m") == argv[i] ) {
			i++;
			if ( argc == i ) {
				cout << "ERROR: missing release argument" << endl;
				exit(1);
			}
			MySQLpassword = argv[i];
		}
		else if( string("-p") == argv[i] ) {
			i++;
			if ( argc == i ) {
				cout << "ERROR: package type argument" << endl;
				exit(1);
			}
			installPackageType = argv[i];
		}
		else if( string("-3") == argv[i] ) {
			forceVer = 3;
		}
		else {
			cout << "ERROR: Unknown option: " << argv[i] << endl;
			exit(1);
		}
	}

	if (systemName.empty() ) {
		cout << endl;
		cout << "Usage: autoInstaller -s system [-h][-ce][-r release][-c configFile][-n][-d]" << endl;
		cout << "			-s system-name" << endl;
		cout << "			-ce community-edition install" << endl;
		cout << "			-r release (optional, default to 'Latest') " << endl;
		cout << "			-c InfiniDB Config File (optional, default to system copy) " << endl;
		cout << "			-n no prompt Flag" << endl;
		cout << "			-d debug Flag" << endl;
		cout << "			-3 Force a version 3 install, defaults to autodetect" << endl;
		exit(1);
	}

	// get Parent OAM Module based on system name
	int systemCount;
	try {
		systemCount = strtol(sysConfig->getConfig("NetworkConfig", "SystemCount").c_str(), 0, 0);
		if ( systemCount == 0 ) {
			cout << "ERROR: SystemCount in " << sysConfig->configFile() << " equal to 0, exiting" << endl;
			cerr << "ERROR: SystemCount in " << sysConfig->configFile() << " equal to 0, exiting" << endl;
			exit(1);
		}
	}
	catch(...)
	{
		cout << "ERROR: Problem getting SystemCount from " << sysConfig->configFile() << ", exiting" << endl;
		cerr << "ERROR: Problem getting SystemCount from " << sysConfig->configFile() << ", exiting" << endl;
		exit(1);
	}

	bool found = false;
	for ( int i = 1 ; i <= systemCount ; i++)
	{
		string SystemName = "SystemName" + oam.itoa(i);
		string oamParentModule = "OAMParentModule" + oam.itoa(i);
		string SystemPackage = "SystemPackage" + oam.itoa(i);
		string SystemPassword = "SystemPassword" + oam.itoa(i);
		string SystemUser = "SystemUser" + oam.itoa(i);

		string tempSystem;
		try {
			tempSystem = sysConfig->getConfig("NetworkConfig", SystemName );
		}
		catch(...)
		{
			cout << "ERROR: Problem getting SystemName from " << sysConfig->configFile() << ", exiting" << endl;
			cerr << "ERROR: Problem getting SystemName from " << sysConfig->configFile() << ", exiting" << endl;
			exit(1);
		}

		if ( tempSystem == systemName ) {
			try {
				installParentModuleHostName = sysConfig->getConfig("NetworkConfig", oamParentModule );
				password = sysConfig->getConfig("NetworkConfig", SystemPassword );
				systemPackage = sysConfig->getConfig("NetworkConfig", SystemPackage );
				systemUser = sysConfig->getConfig("NetworkConfig", SystemUser );
				found = true;
				break;
			}
			catch(...)
			{
				cout << "ERROR: Problem getting SystemName from " << sysConfig->configFile() << ", exiting" << endl;
				cerr << "ERROR: Problem getting SystemName from " << sysConfig->configFile() << ", exiting" << endl;
				exit(1);
			}
		}
	}

	if ( !found ) {
		cout << "ERROR: System Name '" + systemName + "' not in " << sysConfig->configFile() << ", exiting" << endl;
		cerr << "ERROR: System Name '" + systemName + "' not in " << sysConfig->configFile() << ", exiting" << endl;
		exit(1);
	}

	//check if package type option is used
	if ( !installPackageType.empty() )
	{
		if ( installPackageType == "rpm" )
			systemPackage = "*.x86_64.rpm";
		else if ( installPackageType == "binary" )
			systemPackage = "*x86_64.bin.tar.gz";
		else
		{
			cout << "Invalid InfiniDB Package Type Arugument entered, use 'rpm' or 'binary' exiting" << endl;
			cerr << "Invalid InfiniDB Package Type Arugument entered, use 'rpm' or 'binary' exiting" << endl;
			exit(1);
		}
	}

	cout << endl << "*** Make sure nobody is sitting in a mounted directory, like data1 ***" << endl;

	if ( !noprompt ) {
		while(true)
		{
			cout << endl;

			if ( systemPackage == "*.x86_64.rpm" )
				prompt = "Are you sure you want to install the '" + release + "' InfiniDB RPM on '" + systemName + "' ? (y,n,exit) > ";
			else
				prompt = "Are you sure you want to install the '" + release + "/packages' InfiniDB Binary Package on '" + systemName + "' ? (y,n,exit) > ";

			pcommand = readline(prompt.c_str());
			if (!pcommand)
				exit(0);

			string confirm = pcommand;
			free(pcommand);
			pcommand = 0;
			if ( confirm == "exit" )
				exit(0);
			if ( confirm == "y" )
				break;
			else
			{
				if ( confirm == "n" ) {
					prompt = "Enter system (" + systemName + ") > ";
					pcommand = readline(prompt.c_str());
					if (pcommand)
					{
						systemName = pcommand;
						free(pcommand);
						pcommand = 0;
					}
					else {
						prompt = "Enter release (" + release + ") > ";
						pcommand = readline(prompt.c_str());
						if (pcommand)
						{
							release = pcommand;
							free(pcommand);
							pcommand = 0;
						}
					}	
				}
			}
		}
	}

	//get if root or no-root user install
	if ( systemUser != "root" )
		installDir = "/home/" + systemUser;

	//remove all calpont packages from local /root/ directory, can interfere with install
	if (geteuid() == 0)
		system("rm -f /root/calpont-*rpm /root/infinidb-*rpm");

	string systemDir = "systems/" + systemName + "/";

	string cmd = "mkdir " + systemDir + " > /dev/null 2>&1";
	system(cmd.c_str());

	//delete any packages and xml files
	cmd = "rm -f " + systemDir + "* > /dev/null 2>&1";
	system(cmd.c_str());

	int idbver = -1;
	string currentPrefix;
	string calpontPackagename;
	string mysqlRPMname;

	//TODO: replace all the goofy file I/O with glob(3) or better yet just parse the output of samba 'dir' cmd
	//  and avoid all the wasted network I/O
	if ( systemPackage == "*.x86_64.rpm" )
	{
		//get rpm
		string smbCmd = "cd " + systemDir + ";smbclient " + SHARED + " -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd Iterations/" + release + "/;prompt OFF;mget " + systemPackage + "' > /dev/null 2>&1";
		system(smbCmd.c_str());

		if (forceVer > 0)
		{
			idbver = forceVer;
		}
		else
		{
			//try to guess the release, v4+ takes precedence
			string sentinel = systemDir + "infinidb-libs-*.rpm";
			glob_t gt;
			memset(&gt, 0, sizeof(gt));
			idbver = 3;
			if (glob(sentinel.c_str(), 0, 0, &gt) == 0)
				idbver = 4;
			globfree(&gt);
		}
		//v4 really means 4+, it'll get fixed later...
		currentPrefix = oam.itoa(idbver);
		if (idbver >= 4)
		{
			calpontPackagename = "infinidb";
			mysqlRPMname = calpontPackagename + "-storage-engine";
		}
		else
		{
			if (CE == "1")
			{
				cout << endl << "FAILED: Community-Edition install valid for 4.x builds and above" << endl;
				cerr << endl << "FAILED: Community-Edition install valid for 4.x builds and above" << endl;
				exit(1);
			}

			calpontPackagename = "calpont";
			mysqlRPMname = calpontPackagename + "-mysql";
		}

		//check if package is there
		cmd = "ls " + systemDir + mysqlRPMname + "* > /tmp/package.txt 2>&1";

		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			{
				cout << endl << "FAILED: InfiniDB Package(s) not found in " << release << " , exiting" << endl;
				cerr << endl << "FAILED: InfiniDB Package(s) not found in " << release << " , exiting" << endl;
				exit(1);
			}
		}
	
		ifstream file ("/tmp/package.txt");
	
		char line[400];
		string buf;
	
		while (file.getline(line, 400))
		{
			buf = line;
	
			string::size_type pos;
			if (idbver >= 4)
			{
				pos = buf.find("infinidb-storage-engine-",0);
				if (pos != string::npos) {
					currentPrefix = buf.substr(pos+24,1);
					break;
				}
			}
			else
			{
				pos = buf.find("calpont-mysql-",0);
				if (pos != string::npos) {
					currentPrefix = buf.substr(pos+14,1);
					break;
				}
			}
		}
		file.close();

		calpontPackage = calpontPackagename + "-" + currentPrefix + systemPackage;

		cout << endl << "Using the InfiniDB Packages '" + systemPackage + "' from //srvfrisco2/public/Iterations/" + release + "/" << endl;
	}
	else	//binary package
	{
		//this will pickup more than we want...
		string smbCmd = "cd " + systemDir + ";smbclient " + SHARED + " -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd Iterations/" + release + "/packages;prompt OFF;mget *.x86_64.bin.tar.gz' > /dev/null 2>&1";
		system(smbCmd.c_str());

		//TODO: consolodate this with above test...
		if (forceVer > 0)
		{
			idbver = forceVer;
		}
		else
		{
			//try to guess the release, v4+ takes precedence
			string sentinel = systemDir + "infinidb-ent-*.tar.gz";
			glob_t gt;
			memset(&gt, 0, sizeof(gt));
			idbver = 3;
			if (glob(sentinel.c_str(), 0, 0, &gt) == 0)
				idbver = 4;
			globfree(&gt);
		}
		//v4 really means 4+, it'll get fixed later...
		currentPrefix = oam.itoa(idbver);
		if (idbver >= 4)
		{
			calpontPackagename = "infinidb";
			mysqlRPMname = calpontPackagename + "-storage-engine";
			calpontPackage = "infinidb-ent-" + systemPackage;
		}
		else
		{
			calpontPackagename = "calpont";
			mysqlRPMname = calpontPackagename + "-mysql";
			calpontPackage = calpontPackagename + "-infinidb-ent-" + systemPackage;
		}

		//check if package is there
		cmd = "ls " + systemDir + calpontPackage + " > /tmp/package.txt 2>&1";
	
		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			{
				cout << endl << "FAILED: InfiniDB binary package not found in " << release << ", exiting" << endl;
				cerr << endl << "FAILED: InfiniDB binary package not found in " << release << ", exiting" << endl;
				exit(1);
			}
		}
	
		ifstream file ("/tmp/package.txt");
	
		char line[400];
		string buf;
	
		while (file.getline(line, 400))
		{
			buf = line;
	
			string::size_type pos;
			if (idbver >= 4)
			{
				pos = buf.find("infinidb-ent-",0);
				if (pos != string::npos) {
					currentPrefix = buf.substr(pos+13,1);
					break;
				}
			}
			else
			{
				pos = buf.find("calpont-infinidb-ent-",0);
				if (pos != string::npos) {
					currentPrefix = buf.substr(pos+21,1);
					break;
				}
			}
		}
		file.close();

		cout << endl << "Using the InfiniDB Package '" + systemPackage + "' from //srvfrisco2/public/Iterations/" + release + "/packages" << endl;

	}

	mysqlRPM = mysqlRPMname + "-" + systemPackage;
	if (idbver >= 4)
		mysqldRPM = calpontPackagename + "-mysql-" + systemPackage;
	else
		mysqldRPM = calpontPackagename + "-mysqld-" + systemPackage;

	//TODO: we go to all the effort of downloading the packages above only to delete them here...
	cmd = "rm -f " + systemDir + systemPackage + " > /dev/null 2>&1";
	system(cmd.c_str());

	//get Network IP Address
	installParentModuleIPAddr = oam.getIPAddress( installParentModuleHostName);
	if ( installParentModuleIPAddr.empty() || installParentModuleIPAddr == "127.0.0.1" ) {
		cout << "Invalid Host Name (No DNS IP found), exiting..." << endl;
		cerr << "Invalid Host Name (No DNS IP found), exiting..." << endl;
		exit(1);
	}

	//check to see if system is up
	string cmdLine = "ping ";
	string cmdOption = " -w 5 >> /dev/null";

	cmd = cmdLine + installParentModuleIPAddr + cmdOption;
	int rtnCode = system(cmd.c_str());
	if ( rtnCode != 0 ) {
		cout << "  FAILED: failed ping test command installParentModuleIPAddr: " << installParentModuleIPAddr << ", exiting" << endl;
		cerr << "  FAILED: failed ping test command installParentModuleIPAddr: " << installParentModuleIPAddr << ", exiting" << endl;
		exit(1);
	}

	cout << endl;

#if 0
cout << "idbver = " << idbver << endl;
cout << "calpontPackagename = " << calpontPackagename << endl;
cout << "mysqlRPMname = " << mysqlRPMname << endl;
cout << "calpontPackage = " << calpontPackage << endl;
cout << "systemPackage = " << systemPackage << endl;
exit(0);
#endif

	//get release and system Calpont.xml and generate a new Calpont.xml file for installation
	if ( configFile == "NULL" ) {
		cout << "Get Release Calpont.xml                       " << flush;
		
		// get release Calpont.xml
		string cmd = "cd " + systemDir + ";smbclient " + SHARED + " -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd Iterations/" + release + "/;prompt OFF;mget Calpont.xml' > /dev/null 2>&1";
		rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			cout << "FAILED: no Release Calpont.xml found for release '" + release + "', exiting" << endl;
			cerr << "FAILED: no Release Calpont.xml found for release '" + release + "', exiting" << endl;
			exit(1);
		}
		else 
		{
			cmd = "cd " + systemDir + ";mv -f Calpont.xml Calpont.xml.new > /dev/null 2>&1";
			rtnCode = system(cmd.c_str());
			if (rtnCode != 0) {
				cout << "FAILED: no Release Calpont.xml found for release '" + release + "', exiting" << endl;
				cerr << "FAILED: no Release Calpont.xml found for release '" + release + "', exiting" << endl;
				exit(1);
			}
			else
			{
				cout << "DONE" << endl;

				//get system Calpont.xml
				cout << "Get System Calpont.xml                        " << flush;
				for ( int retry = 0 ; retry < 5 ; retry++ )
				{
					cmd = "./remote_scp_get.sh " + installParentModuleIPAddr + " " + password + " " + installDir + "/Calpont/etc/Calpont.xml " + systemUser + " " + debug_flag;
					rtnCode = system(cmd.c_str());
					sleep(2);
					if (rtnCode == 0) {
						cmd = "mv Calpont.xml " + systemDir + "/.";
						rtnCode = system(cmd.c_str());
						if ( rtnCode == 0 ) {
							//Calpont.xml found
	
							//try to parse it
							Config* sysConfigOld;

						    ofstream file("/dev/null");

						    //save cout stream buffer
						    streambuf* strm_buffer = cerr.rdbuf();

							try {
							    // redirect cout to /dev/null
								cerr.rdbuf(file.rdbuf());

								sysConfigOld = Config::makeConfig( systemDir + "/Calpont.xml");

							   // restore cout stream buffer
								cerr.rdbuf (strm_buffer);

								//update release Calpont.xml with System Configuration info from system Calpont.xml
								cout << "Run Calpont.xml autoConfigure                 " << flush;
								cmd = "cd " + systemDir + ";../../autoConfigure";
								rtnCode = system(cmd.c_str());
								if (rtnCode != 0) {
									cout << "  FAILED, try Calpont.xml.rpmsave" << endl;
									goto RPMSAVE;
								}
								else {
									cout << "DONE" << endl;
									configFile = "Calpont.xml.new";
									goto CONFIGDONE;
								}
							}
							catch(...)
							{
//								cout << "  FAILED to parse, try re-reading again" << endl;
							   // restore cout stream buffer
								cerr.rdbuf (strm_buffer);
								continue;
							}
						}
					}
				}

RPMSAVE:
				//try Calpont.xml.rpmsave
				cout << "Get System Calpont.xml.rpmsave                " << flush;
				cmd = "./remote_scp_get.sh " + installParentModuleIPAddr + " " + password + " " + installDir + "/Calpont/etc/Calpont.xml.rpmsave " + systemUser + " " + debug_flag;
				rtnCode = system(cmd.c_str());
				if (rtnCode == 0) {
					cmd = "mv Calpont.xml.rpmsave " + systemDir + "/Calpont.xml";
					rtnCode = system(cmd.c_str());
					if ( rtnCode != 0 ) {
						cout << "ERROR: No system Calpont.xml or Calpont.xml.rpmsave found, exiting" << endl;
						cerr << "ERROR: No system Calpont.xml or Calpont.xml.rpmsave found, exiting" << endl;
						exit(1);
					}
				}

				//update release Calpont.xml with System Configuration info from system Calpont.xml.rpmsave
				cout << "Run Calpont.xml autoConfigure                 " << flush;
				cmd = "cd " + systemDir + ";../../autoConfigure";
				rtnCode = system(cmd.c_str());
				if (rtnCode != 0) {
					cout << "  FAILED, exiting..." << endl;
					cerr << "  FAILED, exiting..." << endl;
					exit(1);
				}
				else {
					cout << "DONE" << endl;
					configFile = "Calpont.xml.new";
				}
			}
		}
	}

CONFIGDONE:

	Config* sysConfigOld;
	try {
		sysConfigOld = Config::makeConfig( systemDir + "/Calpont.xml");
	}
	catch(...)
	{
		cout << "ERROR: Problem reading Calpont.xml files, exiting" << endl;
		cerr << "ERROR: Problem reading Calpont.xml files, exiting" << endl;
		exit(1);
	}

	//get Parent OAM Module Name of load being installed
	string parentOAMModuleModuleName;
	string parentOAMModuleIPAddr;

	try{
		parentOAMModuleModuleName = sysConfigOld->getConfig("SystemConfig", "ParentOAMModuleName");
		parentOAMModuleIPAddr = sysConfigOld->getConfig("ProcStatusControl", "IPAddr");
	}
	catch(...)
	{
		cout << "ERROR: Problem updating the InfiniDB System Configuration file, exiting" << endl;
		cerr << "ERROR: Problem updating the InfiniDB System Configuration file, exiting" << endl;
		exit(1);
	}

	if ( parentOAMModuleModuleName.empty() || parentOAMModuleIPAddr.empty() ||
			parentOAMModuleIPAddr == "127.0.0.1" ||
			parentOAMModuleIPAddr == "0.0.0.0" ) {
		cout << " ERROR: ParentOAMModuleModuleName or ParentOAMModuleIPAddr are invalid in " << sysConfigOld->configFile() << ", exiting" << endl;
		cerr << " ERROR: ParentOAMModuleModuleName or ParentOAMModuleIPAddr are invalid in " << sysConfigOld->configFile() << ", exiting" << endl;
		exit(1);
	}

	int serverTypeInstall;
	try {
		serverTypeInstall = atoi(sysConfigOld->getConfig("Installation", "ServerTypeInstall").c_str());
	}
	catch(...)
	{
		cout << "ERROR: Problem reading serverTypeInstall from the InfiniDB System Configuration file, exiting" << endl;
		cerr << "ERROR: Problem reading serverTypeInstall from the InfiniDB System Configuration file, exiting" << endl;
		exit(1);
	}

	bool HDFS = false;
	string DBRootStorageType;
	try {
		DBRootStorageType = sysConfigOld->getConfig("Installation", "DBRootStorageType");
	}
	catch(...)
	{
		cout << "ERROR: Problem reading DBRootStorageType from the InfiniDB System Configuration file, exiting" << endl;
		cerr << "ERROR: Problem reading DBRootStorageType from the InfiniDB System Configuration file, exiting" << endl;
		exit(1);
	}

	if ( DBRootStorageType == "hdfs" )
		HDFS = true;

	if ( serverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM && CE == "1")
	{
		cout << "Community-Edition only work on single-server systems, exiting" << endl;
		cerr << "Community-Edition only work on single-server systems, exiting" << endl;
		exit (0);
	}

	string::size_type pos = parentOAMModuleIPAddr.find("208",0);
	if( pos != string::npos) {
		string newIP = parentOAMModuleIPAddr.substr(0,pos) + "8" + parentOAMModuleIPAddr.substr(pos+3,80);
		parentOAMModuleIPAddr = newIP;
	}

	if ( parentOAMModuleIPAddr == "0.0.0.0" ) {
		cout << "System Calpont.xml is un-configured, exiting" << endl;
		cerr << "System Calpont.xml is un-configured, exiting" << endl;
		exit (0);
	}

	//set Version Key Flags
	Config* sysConfigNew;
	try {
		sysConfigNew = Config::makeConfig( systemDir + "/" + configFile);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading Calpont.xml files, exiting" << endl;
		cerr << "ERROR: Problem reading Calpont.xml files, exiting" << endl;
		exit(1);
	}

	try{
		sysConfigNew->setConfig("SystemConfig", "Flags", "75635259");
		sysConfigNew->write();
	}
	catch(...)
	{}

	//set EE package Type to 'binary', if needed
	if ( systemPackage == "*.x86_64.rpm" )
	{
		try{
			sysConfigNew->setConfig("Installation", "EEPackageType", "rpm");
			sysConfigNew->write();
		}
		catch(...)
		{}
	}
	else
	{
		try{
			sysConfigNew->setConfig("Installation", "EEPackageType", "binary");
			sysConfigNew->write();
		}
		catch(...)
		{}
	}

	cout << "Shutdown System                               " << flush;

	cmd = "./remote_command.sh " + installParentModuleIPAddr + " " + systemUser + " " + password + " '" + installDir + "/Calpont/bin/calpontConsole shutdownsystem y' 'Successful shutdown' Error 60 " + debug_flag;
	rtnCode = system(cmd.c_str());
	if (rtnCode == 0)
		cout << "DONE" << endl;

	// run calpontUninstaller script
	cout << "Run calpontUninstall script                   " << flush;

	cmd = "./remote_command.sh " + installParentModuleIPAddr + " " + systemUser + " " + password + " '" + installDir + "/Calpont/bin/calpontUninstall.sh -d -p " + password + "' 'Uninstall Completed' FAILED 120 " + debug_flag;
	rtnCode = system(cmd.c_str());
	if (rtnCode == 0)
		cout << "DONE" << endl;

	// Copy the Packages and Configuration files down to Parent OAM Module
	string installer;
	if ( systemPackage != "*.x86_64.rpm" )
	{	//do binary install
		if (idbver >= 4)
			installer = "parent_binary_installer.sh";
		else
			installer = "parent_binary_installer_v3.sh";
		cmd = "cd " + systemDir + ";../../" + installer + " " + installParentModuleIPAddr + " " +
			password + " " + systemPackage + " " + release + " " + configFile + " " + systemUser + " " +
			installDir + " " + debug_flag;
	}
	else
	{
		if ( serverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM )
		{
			if (idbver >= 4)
				installer = "dm_parent_installer.sh";
			else
				installer = "dm_parent_installer_v3.sh";
			cmd = "cd " + systemDir + ";../../" + installer + " " + installParentModuleIPAddr + " " +
				password + " " + systemPackage + " " + release + " " + configFile + " " + systemUser + " " + CE + " " + debug_flag;
		}
		else
		{
			if (idbver >= 4)
				installer = "pm_parent_installer.sh";
			else
				installer = "pm_parent_installer_v3.sh";
			cmd = "cd " + systemDir + ";../../" + installer + " " + installParentModuleIPAddr + " " +
				password + " " + systemPackage + " " + release + " " + configFile + " " + currentPrefix + " " +
				systemUser + " " + debug_flag;
		}
	}

	rtnCode = system(cmd.c_str());
	if (rtnCode != 0) {
		cout << endl << "Installation Failed, exiting" << endl;
		cerr << endl << "Installation Failed, exiting" << endl;
		exit(1);
	}

	//
	// Perform System Installing and launch
	//
	cout << "Install System                               " << flush;
	if (idbver < 3)
	{
		cmd = "./remote_command.sh " + installParentModuleIPAddr + " " + systemUser + " " + " " + password + "  '/usr/local/Calpont/bin/installer /root/" + calpontPackage + " /root/" + mysqlRPM + " /root/" + mysqldRPM + " initial " + password + " " + XMpassword + " --nodeps " + MySQLpassword + " 1' 'InfiniDB Install Successfully Completed' ERROR 1200 " + debug_flag;
	}
	else
	{
		if (HDFS)
		{
			string DataFileEnvFile = "setenv-hdfs-20";
			cmd = "./remote_command.sh " + installParentModuleIPAddr + " " + systemUser + " " + password + " 'export JAVA_HOME=/usr/java/jdk1.6.0_31;export LD_LIBRARY_PATH=/usr/java/jdk1.6.0_31/jre/lib/amd64/server;. /root/" + DataFileEnvFile + ";" + installDir + "/Calpont/bin/postConfigure -i " + installDir + "/Calpont -n -mp " + MySQLpassword + " -p " + password + "' 'System is Active' Error 1200 " + debug_flag;
		}
		else
		{
			cmd = "./remote_command.sh " + installParentModuleIPAddr + " " + systemUser + " " + password + " '" + installDir + "/Calpont/bin/postConfigure -i " + installDir + "/Calpont -n -mp " + MySQLpassword + " -p " + password + "' 'System is Active' Error 1200 " + debug_flag;
		}
	}

	rtnCode = system(cmd.c_str());
	if (rtnCode != 0) {
		cout << "Installation Failed, exiting" << endl;
		cerr << "Installation Failed, exiting" << endl;
		exit(1);
	}
	else
		cout << "DONE" << endl;

	//needs to be cerr so buildTester will see it..
	cout << endl << "Install Successfully completed" << endl;
	cerr << endl << "Install Successfully completed" << endl;
}
// vim:ts=4 sw=4:

