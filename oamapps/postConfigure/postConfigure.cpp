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
* $Id: postConfigure.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by post-install configure:
*		Calpont/etc/snmpd.conf
*		Calpont/etc/snmptrapd.conf
*		Calpont/etc/Calpont.xml
*		Calpont/etc/ProcessConfig.xml
*		/etc/rc.local
*		
******************************************************************************************/
/**
 * @file
 */

#include <unistd.h>
#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include <climits>
#include <cstring>
#include <glob.h>

#include <readline/readline.h>
#include <readline/history.h>
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/tokenizer.hpp"

#include "liboamcpp.h"
#include "configcpp.h"
#include "snmpmanager.h"

using namespace std;
using namespace oam;
using namespace config;
using namespace snmpmanager;

#include "helpers.h"
using namespace installer;


typedef struct DBRoot_Module_struct
{
	std::string		moduleName;
	std::string     dbrootID;
} DBRootModule;

typedef std::vector<DBRootModule> DBRootList;

typedef struct Performance_Module_struct
{
	std::string     moduleIP1;
	std::string     moduleIP2;
	std::string     moduleIP3;
	std::string     moduleIP4;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

void snmpAppCheck();
void offLineAppCheck();
bool setOSFiles(string parentOAMModuleName, int serverTypeInstall);
bool checkSaveConfigFile();
string getModuleName();
bool setModuleName(string moduleName);
bool updateSNMPD(string parentOAMModuleIPAddr);
bool updateBash();
bool makeModuleFile(string moduleName, string parentOAMModuleName);
bool updateProcessConfig(int serverTypeInstall);
bool uncommentCalpontXml( string entry);
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall);
bool createDbrootDirs(string DBRootStorageType);
bool pkgCheck();
bool storageSetup(string cloud);
void setSystemName();
bool checkInstanceRunning(string instanceName, string x509Cert, string x509PriKey);
string getInstanceIP(string instanceName, string x509Cert, string x509PriKey);
bool attachVolume(string instanceName, string volumeName, string deviceName, string dbrootPath);
bool singleServerDBrootSetup();
bool copyFstab(string moduleName);
bool copyX509files();

void remoteInstallThread(void *);

string calpontPackage1;
string calpontPackage2;
string calpontPackage3;
string mysqlPackage;
string mysqldPackage;

const char* pcommand = 0;

string parentOAMModuleName;
int pmNumber = 0;
int umNumber = 0;

string DBRootStorageLoc;
string DBRootStorageType;
string UMStorageType;

int DBRootCount;
string deviceName;

Config* sysConfig = Config::makeConfig();
string SystemSection = "SystemConfig";
string InstallSection = "Installation";
string ModuleSection = "SystemModuleConfig";
string prompt;
string serverTypeInstall;
int    IserverTypeInstall;
string parentOAMModuleIPAddr;
string remote_installer_debug = "1";
string thread_remote_installer = "1";

string singleServerInstall = "1";
string reuseConfig ="n";
string oldFileName;
string x509Cert;
string x509PriKey;
string glusterCopies;
string glusterInstalled = "n";
string hadoopInstalled = "n";
string home;
string mysqlPort = oam::UnassignedName;

bool noPrompting = false;
bool rootUser = true;
string USER = "root";
bool hdfs = false;
bool gluster = false;
bool pmwithum = false;
bool mysqlRep = false;
string MySQLRep = "n";
string PMwithUM = "n";

string DataFileEnvFile;

string installDir;

extern string pwprompt;
string mysqlpw = " ";

const char* callReadline(string prompt)
{
    	if ( !noPrompting )
	{
	        const char* ret = readline(prompt.c_str());
	        if( ret == 0 )
	            exit(1);
        	return ret;
	}
    	else 
	{
        	cout << prompt << endl;
        	return "";
    	}
}

void callFree(const char* )
{
	if ( !noPrompting )
		free((void*)pcommand);
	pcommand = 0;
}

/* create thread argument struct for thr_func() */
typedef struct _thread_data_t {
  	std::string command;
} thread_data_t;

int main(int argc, char *argv[])
{
    	Oam oam;
	string parentOAMModuleHostName;
	ChildModuleList childmodulelist;
	ChildModuleList niclist;
	ChildModule childmodule;
	DBRootModule dbrootmodule;
	DBRootList dbrootlist;
	PerformanceModuleList performancemodulelist;
	int DBRMworkernodeID = 0;
	string nodeps = "-h";
	bool startOfflinePrompt = false;
	noPrompting = false;
	string password;

//  	struct sysinfo myinfo; 

	// hidden options
	// -f for force use nodeps on rpm install
	// -o to prompt for process to start offline

	//default
	installDir = installDir + "";
	//see if we can determine our own location
	ostringstream oss;
	oss << "/proc/" << getpid() << "/exe";
	ssize_t rlrc;
	const size_t psz = PATH_MAX;
	char thisexepath[psz+1];
	memset(thisexepath, 0, psz+1);
	rlrc = readlink(oss.str().c_str(), thisexepath, psz);
	if (rlrc > 0)
	{
		thisexepath[rlrc] = 0;
		//should look something like '/usr/local/Calpont/bin/postConfigure'
		char* ptr;
		ptr = strrchr(thisexepath, '/');
		if (ptr)
		{
			*ptr = 0;
			ptr = strrchr(thisexepath, '/');
			if (ptr)
			{
				*ptr = 0;
				installDir = thisexepath;
			}
		}
	}

	//check if root-user
	int user;
	user = getuid();
	if (user != 0)
		rootUser = false;

	char* p= getenv("USER");
	if (p && *p)
   		USER = p;

	for( int i = 1; i < argc; i++ )
	{
		if( string("-h") == argv[i] ) {
			cout << endl;
			cout << "This is the Calpont InfiniDB System Configuration and Installation tool." << endl;
			cout << "It will Configure the Calpont InfiniDB System based on Operator inputs and" << endl;
			cout << "will perform a Package Installation of all of the Modules within the" << endl;
			cout << "System that is being configured." << endl;
			cout << endl;
			cout << "IMPORTANT: This tool should only be run on a Performance Module Server," << endl;
			cout << "           preferably Module #1" << endl;
			cout << endl;
			cout << "Instructions:" << endl << endl;
			cout << "	Press 'enter' to accept a value in (), if available or" << endl;
			cout << "	Enter one of the options within [], if available, or" << endl;
			cout << "	Enter a new value" << endl << endl;
			cout << endl;
   			cout << "Usage: postConfigure [-h][-c][-n][-p][-mp][-s][-lq][-rep][-port]" << endl;
			cout << "   -h  Help" << endl;
			cout << "   -c  Config File to use to extract configuration data, default is Calpont.xml.rpmsave" << endl;
			cout << "   -n  Install with no prompting" << endl;
			cout << "   -p  Password, used with no-prompting option" << endl;
			cout << "   -mp MySQL Password, can be used with no-prompting option" << endl;
			cout << "   -s  Single Threaded Remote Install" << endl;
			cout << "   -lq Enable Local Query Feature" << endl;
			cout << "   -rep Enable MySQL Replication" << endl;
			cout << "   -port MySQL Port Address" << endl;
			exit (0);
		}
      		else if( string("-s") == argv[i] )
			thread_remote_installer = "0";
		else if( string("-f") == argv[i] )
			nodeps = "--nodeps";
		else if( string("-o") == argv[i] )
			startOfflinePrompt = true;
		else if( string("-c") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: Config File not provided" << endl;
				exit (1);
			}
			oldFileName = argv[i];
			if ( oldFileName.find("Calpont.xml") == string::npos ) {
				cout << "   ERROR: Config File is not a Calpont.xml file name" << endl;
				exit (1);
			}			
		}
		else if( string("-p") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: Password not provided" << endl;
				exit (1);
			}
			password = argv[i];
			if ( password.find("-") != string::npos ) {
				cout << "   ERROR: Valid Password not provided" << endl;
				exit (1);
			}			
		}
		else if( string("-mp") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: MySql Password not provided" << endl;
				exit (1);
			}
			mysqlpw = argv[i];
			if ( mysqlpw.find("-") != string::npos ) {
				cout << "   ERROR: Valid MySQL Password not provided" << endl;
				exit (1);
			}			
			if ( mysqlpw == "dummymysqlpw" )
				mysqlpw = " ";
		}
		else if( string("-n") == argv[i] )
			noPrompting = true;
		else if( string("-i") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: install dir not provided" << endl;
				exit (1);
			}
			installDir = argv[i];
			if ( installDir.find("-") != string::npos ) {
				cout << "   ERROR: Valid install dir not provided" << endl;
				exit (1);
			}			
		}
		else if( string("-lq") == argv[i] ) {
			pmwithum = true;
			PMwithUM = "y";
		}
		else if( string("-rep") == argv[i] ) {
			mysqlRep = true;
			MySQLRep = "y";
		}
		else if( string("-port") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: MySQL Port ID not supplied" << endl;
				exit (1);
			}
			mysqlPort = argv[i];
		}
		else
		{
			cout << "   ERROR: Invalid Argument = " << argv[i] << endl;
   			cout << "   Usage: postConfigure [-h][-c][-n][-p][-mp][-s][-lq][-rep][-port]" << endl;
			exit (1);
		}
	}

	if (installDir[0] != '/')
	{
		cout << "   ERROR: Install dir '" << installDir << "' is not absolute" << endl;
		exit(1);
	}

	oldFileName = installDir + "/etc/Calpont.xml.rpmsave";

	home = "/root";
	if (!rootUser) {
		char* p= getenv("HOME");
		if (p && *p)
			home = p;
	}

	cout << endl;
	cout << "This is the Calpont InfiniDB System Configuration and Installation tool." << endl;
	cout << "It will Configure the Calpont InfiniDB System and will perform a Package" << endl;
	cout << "Installation of all of the Servers within the System that is being configured." << endl;
	cout << endl;

	cout << "IMPORTANT: This tool should only be run on the Parent OAM Module" << endl;
	cout << "           which is a Performance Module, preferred Module #1" << endl;
	cout << endl;

	if (!noPrompting) {
		cout << "Prompting instructions:" << endl << endl;
		cout << "	Press 'enter' to accept a value in (), if available or" << endl;
		cout << "	Enter one of the options within [], if available, or" << endl;
		cout << "	Enter a new value" << endl << endl;
	}
	else
	{
		//get current time and date
		time_t now;
		now = time(NULL);
		struct tm tm;
		localtime_r(&now, &tm);
		char timestamp[200];
		strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
		string currentDate = timestamp;

		string postConfigureLog = "/var/log/infinidb-postconfigure-" + currentDate;

		cout << "With the no-Prompting Option being specified, you will be required to have the following:" << endl;
		cout << endl;
		cout << " 1. Root user ssh keys setup between all nodes in the system or" << endl;
		cout << "    use the password command line option." << endl;
		cout << " 2. A Configure File to use to retrieve configure data, default to Calpont.xml.rpmsave" << endl;
		cout << "    or use the '-c' option to point to a configuration file." << endl;
		cout << endl;
//		cout << " Output if being redirected to " << postConfigureLog << endl;

//		redirectStandardOutputToFile(postConfigureLog, false );
	}

	//check if InfiniDB is up and running
	string cmd = installDir + "/bin/infinidb status > /tmp/status.log";
	system(cmd.c_str());
	if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) {
		cout << "InfiniDB is running, can't run postConfigure while InfiniDB is running. Exiting.." << endl;
		exit (0);
	}

	//if InitialInstallFlag is set, then an install has been done
	// run pre-uninstall to clean from previous install and continue
	try {
		string InitialInstallFlag = sysConfig->getConfig(InstallSection, "InitialInstallFlag");
		if ( InitialInstallFlag == "y" ) {
			cmd = installDir + "/bin/pre-uninstall --quiet > /dev/null 2>&1";
			system(cmd.c_str());
		}
	}
	catch(...)
	{}

	//run post install for coverage of possible binary package install
	cmd = installDir + "/bin/post-install --installdir=" + installDir + " > /dev/null 2>&1";
	system(cmd.c_str());

	//check Config saved files
	if ( !checkSaveConfigFile())
	{
		cout << "ERROR: Configuration File not setup" << endl;
		exit(1);
	}

	//check mysql port changes
	string MySQLPort;
	try {
		MySQLPort = sysConfig->getConfig(InstallSection, "MySQLPort");
	}
	catch(...)
	{}

	if ( mysqlPort == oam::UnassignedName )
	{
		if ( MySQLPort.empty() || MySQLPort == "" ) {
			mysqlPort = "3306";
			try {
				sysConfig->setConfig(InstallSection, "MySQLPort", "3306");
			}
			catch(...)
			{}
		}
		else
			mysqlPort = MySQLPort;
	}
	else
	{	// mysql port was providing on command line
		try {
			sysConfig->setConfig(InstallSection, "MySQLPort", mysqlPort);
		}
		catch(...)
		{}
	}

	cout << endl;

	cout << "===== Setup System Server Type Configuration =====" << endl << endl;

	cout << "There are 2 options when configuring the System Server Type: single and multi" << endl << endl;
	cout << "  'single'  - Single-Server install is used when there will only be 1 server configured" << endl;
	cout << "              on the system. It's a shorter install procedure used for POC testing, as an example." << endl;
	cout << "              It can also be used for production systems, if the plan is to stay single-server." << endl << endl;
	cout << "  'multi'   - Multi-Server install is used when you want to configure multiple servers now or" << endl;
	cout << "              in the future. With Multi-Server install, you can still configure just 1 server" << endl;
	cout << "              now and add on addition servers/modules in the future. This is used more for" << endl;
	cout << "              production installs." << endl << endl;

	string temp;
	try {
		temp = sysConfig->getConfig(InstallSection, "SingleServerInstall");
	}
	catch(...)
	{}

	if ( temp == "y" )
		singleServerInstall = "1";
	else
		singleServerInstall = "2";

	while(true) {
		prompt = "Select the type of System Server install [1=single, 2=multi] (" + singleServerInstall + ") > ";
		pcommand = callReadline(prompt.c_str());
		string temp;
		if (pcommand) {
			if (strlen(pcommand) > 0) 
				temp = pcommand;
			else
				temp = singleServerInstall;
			callFree(pcommand);
			if (temp == "1") {
				singleServerInstall = temp;
				cout << endl << "Performing the Single Server Install." << endl; 

				if ( reuseConfig == "n" ) {
					//setup to use the single server Calpont.xml file

					// we know that our Config instance just timestamped itself in the getConfig
					// call above.  if postConfigure is running non-interactively we may get here
					// within the same second which means the changes that are about to happen
					// when Calpont.xml gets overwritten will be ignored because of the Config
					// instance won't know to reload
                    			sleep(2);

					cmd = "rm -f " + installDir + "/etc/Calpont.xml.installSave  > /dev/null 2>&1";
					system(cmd.c_str());
					cmd = "mv -f " + installDir + "/etc/Calpont.xml " + installDir + "/etc/Calpont.xml.installSave  > /dev/null 2>&1";
					system(cmd.c_str());
					cmd = "/bin/cp -f " + installDir + "/etc/Calpont.xml.singleserver " + installDir + "/etc/Calpont.xml  > /dev/null 2>&1";
					system(cmd.c_str());
				}

				setSystemName();
				cout << endl;

				system(cmd.c_str());

				// setup storage
				if (!storageSetup("n"))
				{
					cout << "ERROR: Problem setting up storage" << endl;
					exit(1);
				}

				if (hdfs || !rootUser)
                			if( !updateBash() )
	                    		cout << "updateBash error" << endl;

				// setup storage
				if (!singleServerDBrootSetup())
				{
					cout << "ERROR: Problem setting up dbroot IDs" << endl;
					exit(1);
				}

				//set system dbroot count and check 'files per parition' with number of dbroots
				try {
					sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(DBRootCount));
				}
				catch(...)
				{
					cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
					exit(1);
				}

				checkFilesPerPartion(DBRootCount, sysConfig);

				//check if dbrm data resides in older directory path and inform user if it does
				dbrmDirCheck();

				//check snmp Apps disable option
				snmpAppCheck();
			
				if (startOfflinePrompt)
					offLineAppCheck();

				checkMysqlPort(mysqlPort, sysConfig);

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}

				cout << endl << "===== Performing Configuration Setup and InfiniDB Startup =====" << endl;

				cmd = installDir + "/bin/installer dummy.rpm dummy.rpm dummy.rpm dummy.rpm dummy.rpm initial dummy " + reuseConfig + " --nodeps ' ' 1 " + installDir;
				system(cmd.c_str());
				exit(0);
			}
			else
			{
				if (temp == "2") {
					singleServerInstall = temp;
					break;
				}
			}
			cout << "Invalid Entry, please re-enter" << endl;
			if ( noPrompting )
				exit(1);

			continue;
		}
		break;
	}

	try {
		sysConfig->setConfig(InstallSection, "SingleServerInstall", "n");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting SingleServerInstall from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//
	// Multi-server install
	//

	cout << endl;
	string cloud = oam::UnassignedName;
	string amazonSubNet = oam::UnassignedName;
	bool amazonEC2 = false;
	bool amazonVPC = false;

	try {
		 cloud = sysConfig->getConfig(InstallSection, "Cloud");
	}
	catch(...)
	{
		cloud  = oam::UnassignedName;
	}

	string tcloud = "n";
	if (cloud == oam::UnassignedName)
		tcloud = "n";
	else
	{
	 	if (cloud == "amazon-ec2")
		{
			tcloud = "y";
			amazonEC2 = true;
		}
		else
		{
			if (cloud == "amazon-vpc")
			{
				tcloud = "y";
				amazonVPC = true;

				//get subnetID
				try {
					amazonSubNet = sysConfig->getConfig(InstallSection, "AmazonSubNetID");
				}
				catch(...)
				{}
			}
		}
	}

	//cleanup/create local/etc  directory
	cmd = "rm -rf " + installDir + "/local/etc > /dev/null 2>&1";
	system(cmd.c_str());
	cmd = "mkdir " + installDir + "/local/etc > /dev/null 2>&1";
	system(cmd.c_str());

	while(true) {
		prompt = "Installing on Amazon System (EC2 or VPC services) [y,n] (" + tcloud + ") > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) tcloud = pcommand;
			callFree(pcommand);
		}

		if (tcloud == "y") 
		{
			if (!amazonEC2)
				tcloud = "n";

			prompt = "Using EC2 services [y,n] (" + tcloud + ") > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) tcloud = pcommand;
				callFree(pcommand);
			}
	
			if (tcloud == "n")
			{
				amazonEC2 = false;
				cloud = oam::UnassignedName;

				if (amazonVPC)
					tcloud = "y";

				prompt = "Using VPC services [y,n] (" + tcloud + ") > ";
				pcommand = callReadline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) tcloud = pcommand;
					callFree(pcommand);
				}
		
				if (tcloud == "n")
				{
					amazonVPC = false;
					cloud = oam::UnassignedName;
				}
				else
				{
					amazonVPC = true;
					cloud = "amazon-vpc";

					prompt = "Enter VPC SubNet ID (" + amazonSubNet + ") > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) amazonSubNet = pcommand;
						callFree(pcommand);
					}

					//set subnetID
					try {
						sysConfig->setConfig(InstallSection, "AmazonSubNetID", amazonSubNet);
					}
					catch(...)
					{}
				}
			}
			else
			{
				amazonEC2 = true;
				cloud = "amazon-ec2";
			}

			if ( amazonEC2 || amazonVPC )
			{
				cout << endl << "For Amazon EC2/VPC Instance installs, these files will need to be installed on" << endl;
				cout << "on the local instance:" << endl << endl;
				cout << " 1. X.509 Certificate" << endl;
				cout << " 2. X.509 Private Key" << endl << endl;
	
				while(true) {
					string ready = "y";
					prompt = "Are these files installed and ready to continue [y,n] (y) > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) ready = pcommand;
						callFree(pcommand);
						if (ready == "n") {
							cout << endl << "Please Install these files and re-run postConfigure. exiting..." << endl;
							exit(0);
						}
					}
	
					if ( ready == "y" )
						break;
	
					cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
					if ( noPrompting )
						exit(1);
				}
	
				try {
					x509Cert = sysConfig->getConfig(InstallSection, "AmazonX509Certificate");
					x509PriKey = sysConfig->getConfig(InstallSection, "AmazonX509PrivateKey");
				}
				catch(...)
				{}
	
				cout << endl;
	
				while(true)
				{
					prompt = "Enter Name and directory of the X.509 Certificate (" + x509Cert + ") > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) x509Cert = pcommand;
						callFree(pcommand);
					}
					ifstream File (x509Cert.c_str());
					if (!File) {
						cout << "Error: file not found, please re-enter" << endl;
						if ( noPrompting )
							exit(1);
					}
					else
						break;
				}
	
				while(true)
				{
					prompt = "Enter Name and directory of the X.509 Private Key (" + x509PriKey + ") > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) x509PriKey = pcommand;
						callFree(pcommand);
					}
					ifstream File (x509PriKey.c_str());
					if (!File)
					{
						cout << "Error: file not found, please re-enter" << endl;
						if ( noPrompting )
							exit(1);
					}
					else
						break;
				}
	
				try {
					sysConfig->setConfig(InstallSection, "AmazonX509Certificate", x509Cert);
					sysConfig->setConfig(InstallSection, "AmazonX509PrivateKey", x509PriKey);
				}
				catch(...)
				{}
	
				if( !copyX509files() )
					cout << "copyX509files error" << endl;
	
				break;
			}
		}
		else 
		{
			if (tcloud == "n" ) {
				cloud = oam::UnassignedName;
				break;
			}
		}

		cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
		if ( noPrompting )
			exit(1);
	}

	try {
		 sysConfig->setConfig(InstallSection, "Cloud", cloud);
	}
	catch(...)
	{}

	if ( cloud == "amazon-ec2" || cloud == "amazon-vpc" )
		cloud = "amazon";

	cout << endl << "===== Setup System Module Type Configuration =====" << endl << endl;

	cout << "There are 2 options when configuring the System Module Type: separate and combined" << endl << endl;
	cout << "  'separate' - User and Performance functionality on separate servers." << endl << endl;
	cout << "  'combined' - User and Performance functionality on the same server" << endl << endl;

	try {
		serverTypeInstall = sysConfig->getConfig(InstallSection, "ServerTypeInstall");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting ServerTypeInstall from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	while(true)
	{
		prompt =  "Select the type of System Module Install [1=separate, 2=combined] (" + serverTypeInstall + ") > ";
		pcommand = callReadline(prompt.c_str());
		cout << endl;
		if (pcommand)
		{
			if (strlen(pcommand) > 0) serverTypeInstall = pcommand;
			callFree(pcommand);
		}

		if ( serverTypeInstall != "1" && serverTypeInstall != "2" ) {
			cout << "Invalid Entry, please re-enter" << endl << endl;
			serverTypeInstall = "1";
			if ( noPrompting )
				exit(1);
			continue;
		}

		IserverTypeInstall = atoi(serverTypeInstall.c_str());

		// set Server Type Installation Indicator
		try {
			sysConfig->setConfig(InstallSection, "ServerTypeInstall", serverTypeInstall);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting ServerTypeInstall in the InfiniDB System Configuration file" << endl;
			exit(1);
		}

		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm on a single server
			{
				cout << "Combined Server Installation will be performed." << endl;
				cout << "The Server will be configured as a Performance Module." << endl;
				cout << "All InfiniDB Processes will run on the Performance Modules." << endl;

				if ( pmwithum )
				{
					cout << endl << "NOTE: Local Query Feature is not valid on a 'combined' system, feature turned off" << endl << endl;
					pmwithum = false;
					PMwithUM = "n";
				}

				try {
					 sysConfig->setConfig(InstallSection, "PMwithUM", "n");
				}
				catch(...)
				{}

				//module ProcessConfig.xml to setup all apps on the dm
				if( !updateProcessConfig(IserverTypeInstall) )
					cout << "Update ProcessConfig.xml error" << endl;
	
				break;
			}
			default:	// normal, separate UM and PM
			{
				break;
			}
		}
		break;
	}

	//store local query flag
	try {
		sysConfig->setConfig(InstallSection, "PMwithUM", PMwithUM);
	}
	catch(...)
	{}

	//local query needs mysql replication, make sure its enabled
	if ( pmwithum )
	{
		mysqlRep = true;
		MySQLRep = "y";
	}

	if ( pmwithum )
		cout << "NOTE: Local Query Feature is enabled" << endl;

	if ( mysqlRep )
		cout << "NOTE: MySQL Replication Feature is enabled" << endl;

	//Write out Updated System Configuration File
	try {
		sysConfig->setConfig(InstallSection, "InitialInstallFlag", "n");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting InitialInstallFlag from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) { 
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl; 
		exit(1);
	}

	cout << endl;
	setSystemName();

	cout << endl;

	//get Parent OAM Module Name
	string parentOAMModuleName;
	try{
		parentOAMModuleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting ParentOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	while(true) {
		string newparentOAMModuleName = parentOAMModuleName;
		prompt = "Enter the Local Module Name or exit [pmx,exit] (" + parentOAMModuleName +") > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) newparentOAMModuleName = pcommand;
			callFree(pcommand);
			if (newparentOAMModuleName == "exit")
				exit(0);
		}
		else
			exit(0);

		if ( newparentOAMModuleName.find("pm") == 0 ) {
			parentOAMModuleName = newparentOAMModuleName;
			break;
		}
		cout << "Invalid Module Name, please re-enter" << endl;
		if ( noPrompting )
			exit(1);
	}

	try{
		 sysConfig->setConfig(SystemSection, "ParentOAMModuleName", parentOAMModuleName);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ParentOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	// set Standby Parent OAM module and IP to unassigned
 	try{
		 sysConfig->setConfig(SystemSection, "StandbyOAMModuleName", oam::UnassignedName);
		 sysConfig->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ParentStandbyOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//create associated local/etc directory for parentOAMModuleName
	cmd = "mkdir " + installDir + "/local/etc/" + parentOAMModuleName + " > /dev/null 2>&1";
	system(cmd.c_str());

	//setup local module file name
	if( !makeModuleFile(parentOAMModuleName, parentOAMModuleName) ) {
		cout << "makeModuleFile error" << endl;
		exit(1);
	}

	cout << endl;

	if (startOfflinePrompt)
		offLineAppCheck();

	string parentOAMModuleType = parentOAMModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
	int parentOAMModuleID = atoi(parentOAMModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
	if ( parentOAMModuleID < 1 ) {
		cout << "ERROR: Invalid Module ID of less than 1" << endl;
		exit(1);
	}

	//Get list of configured system modules
	SystemModuleTypeConfig sysModuleTypeConfig;

	try{
		oam.getSystemConfig(sysModuleTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	// 
	// setup storage
	//

	if (!storageSetup(cloud))
	{
		cout << "ERROR: Problem setting up storage" << endl;
		exit(1);
	}

	if (hdfs || !rootUser)
		if( !updateBash() )
			cout << "updateBash error" << endl;

	//
	// setup memory paramater settings
	//

	cout << endl << "===== Setup Memory Configuration =====" << endl << endl;

	switch ( IserverTypeInstall ) {
		case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm on a single server
		{
			// are we using settings from previous config file?
			if ( reuseConfig == "n" ) {
				if( !uncommentCalpontXml("NumBlocksPct") ) {
					cout << "Update Calpont.xml NumBlocksPct Section" << endl;
					exit(1);
				}

				string numBlocksPct = "50";
				if (hdfs)
					numBlocksPct = "25";

				try {
					sysConfig->setConfig("DBBC", "NumBlocksPct", numBlocksPct);

					cout << endl << "NOTE: Setting 'NumBlocksPct' to " << numBlocksPct << endl;
				}
				catch(...)
				{
					cout << "ERROR: Problem setting NumBlocksPct in the InfiniDB System Configuration file" << endl;
					exit(1);
				}

//				try{
//					sysinfo(&myinfo);
//				}
//				catch (...) {}
		
				//get memory stats
//				long long total = myinfo.totalram / 1024 / 1000;

				string percent = "25%";

				if (hdfs) {
					percent = "12%";
				}

				cout << "      Setting 'TotalUmMemory' to " << percent << " of total memory" << endl;

				try {
					sysConfig->setConfig("HashJoin", "TotalUmMemory", percent);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
					exit(1);
				}

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}
			}
			else
			{
				try {
					string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");

					cout << endl;
					if ( numBlocksPct.empty() )
						cout << "NOTE: Using the default setting for 'NumBlocksPct' at 70%" << endl;
					else
						cout << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;
					
					string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

					cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory <<  endl;
				}
				catch(...)
				{
					cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
					exit(1);
				}
			}
			break;
		}
		default:	// normal, separate UM and PM
		{
			// are we using settings from previous config file?
			if ( reuseConfig == "n" ) {

				string numBlocksPct = "70";
				if (hdfs) {
					numBlocksPct = "35";

					try {
						sysConfig->setConfig("DBBC", "NumBlocksPct", numBlocksPct);
	
						cout << "NOTE: Setting 'NumBlocksPct' to " << numBlocksPct << endl;
					}
					catch(...)
					{
						cout << "ERROR: Problem setting NumBlocksPct in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				else
					cout << "NOTE: Using the default setting for 'NumBlocksPct' at " << numBlocksPct << "%" << endl;

//				try{
//					sysinfo(&myinfo);
//				}
//				catch (...) {}
		
				//get memory stats
//				long long total = myinfo.totalram / 1024 / 1000;

				string percent = "50%";
				if (hdfs) {
					percent = "12%";
				}	

				cout << "      Setting 'TotalUmMemory' to " << percent << " of total memory" << endl;

				try {
					sysConfig->setConfig("HashJoin", "TotalUmMemory", percent);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
					exit(1);
				}

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}
			}
			else
			{
				try {
					string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");

					if ( numBlocksPct.empty() )
						cout << "NOTE: Using the default setting for 'NumBlocksPct' at 70%" << endl;
					else
						cout << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;

					string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

					cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory  << endl;
				}
				catch(...)
				{
					cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
					exit(1);
				}
			}	
			break;
		}
	}

	//Write out Updated System Configuration File
	try {
		sysConfig->setConfig(InstallSection, "InitialInstallFlag", "y");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting InitialInstallFlag from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//
	// Module Configuration
	//
	cout << endl;
	cout << "===== Setup the Module Configuration =====" << endl;

	//get OAM Parent Module IP addresses and Host Name, if it exist
	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
		{
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
	
			if ( (*listPT).DeviceName == parentOAMModuleName ) {
				parentOAMModuleIPAddr = (*pt1).IPAddr;
				parentOAMModuleHostName = (*pt1).HostName;
				break;
			}
		}
	}

	unsigned int maxPMNicCount = 1;

	//configure module type
	bool parentOAMmoduleConfig = false;
	bool moduleSkip = false;

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		string moduleDesc = sysModuleTypeConfig.moduletypeconfig[i].ModuleDesc;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleType == "dm") {
			moduleCount = 0;
			try {
				string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
				sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
				continue;
			}
			catch(...)
			{}
		}

		//verify and setup of modules count
		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):
			{
				if ( moduleType == "um") {
					moduleCount = 0;
					try {
						string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
						sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
						continue;
					}
					catch(...)
					{
						cout << "ERROR: Problem setting Module Count in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				break;
			}
			default:
				break;
		}

		cout << endl << "----- " << moduleDesc << " Configuration -----" << endl << endl;

		int oldModuleCount = moduleCount;
		while(true)
		{
			prompt = "Enter number of " + moduleDesc + "s [1," + oam.itoa(oam::MAX_MODULE) + "] (" + oam.itoa(oldModuleCount) + ") > ";
			moduleCount = oldModuleCount;
			pcommand = callReadline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) moduleCount = atoi(pcommand);
				callFree(pcommand);
			}

			if ( moduleCount < 1 || moduleCount > oam::MAX_MODULE ) {
				cout << endl << "ERROR: Invalid Module Count '" + oam.itoa(moduleCount) + "', please re-enter" << endl << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}

			if ( parentOAMModuleType == moduleType && moduleCount == 0 ) {
				cout << endl << "ERROR: Parent OAM Module Type is '" + parentOAMModuleType + "', so you have to have at least 1 of this Module Type, please re-enter" << endl << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}

			//update count
			try {
				string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
				sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
				break;
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Module Count in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
		}

		if ( moduleType == "pm" )
			pmNumber = moduleCount;

		if ( moduleType == "um" )
			umNumber = moduleCount;

		int moduleID = 1;
		while(true) {
			prompt = "Enter Starting Module ID for " + moduleDesc + " [1," + oam.itoa(oam::MAX_MODULE-moduleCount+1) + "] (1) > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) moduleID = atoi(pcommand);
				callFree(pcommand);
			}
	
			if ( moduleID < 1 || moduleID > oam::MAX_MODULE-moduleCount+1 ) {
				cout << endl << "ERROR: Invalid Module ID '" + oam.itoa(moduleID) + "', please re-enter" << endl << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}

			//valid if parent OAM module type and is consistent with parentOAMModuleName
			if ( parentOAMModuleType == moduleType && 
					( parentOAMModuleID < moduleID || parentOAMModuleID > moduleID + (moduleCount-1) ) ) {
				cout << endl << "ERROR: Parent and Starting Module ID out of range, please re-enter" << endl << endl;
				moduleID = 1;
				if ( noPrompting )
					exit(1);
			}
			else
				break;
		}

		int listSize = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.size();

		//clear any Equipped Module IP addresses that aren't in current ID range
		for ( int j = 0 ; j < listSize ; j++ )
		{
			for ( unsigned int k = 1 ; k < MAX_NIC+1 ; k++)
			{
				string ModuleIPAddr = "ModuleIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);
				if ( !(sysConfig->getConfig(ModuleSection, ModuleIPAddr).empty()) ) {
					if ( j+1 < moduleID || j+1 > moduleID + (moduleCount-1) ) {
						string ModuleHostName = "ModuleHostName" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);
	
						sysConfig->setConfig(ModuleSection, ModuleIPAddr, oam::UnassignedIpAddr);
						sysConfig->setConfig(ModuleSection, ModuleHostName, oam::UnassignedName);
					}
				}
			}
		}

		//configure module name
		string newModuleHostName;
		while(true)
		{
			int saveModuleID = moduleID;
			string moduleDisableState;
			int enableModuleCount = moduleCount;
			for( int k=0 ; k < moduleCount ; k++, moduleID++)
			{
				string newModuleName = moduleType + oam.itoa(moduleID);

				if (moduleType == parentOAMModuleType && moduleID != parentOAMModuleID && !parentOAMmoduleConfig) {
					//skip this module for now, need to configure parent OAM Module first
					moduleSkip = true;
					continue;
				}

				if (moduleType == parentOAMModuleType && moduleID == parentOAMModuleID && parentOAMmoduleConfig)
					//skip, aleady configured
					continue;

				if (moduleType == parentOAMModuleType && moduleID == parentOAMModuleID && !parentOAMmoduleConfig)
					parentOAMmoduleConfig = true;

				string moduleNameDesc = moduleDesc + " #" + oam.itoa(moduleID);
				PerformanceModule performancemodule;
	
				if ( newModuleName == parentOAMModuleName )
					cout << endl << "*** Parent OAM Module " << moduleNameDesc << " Configuration ***" << endl << endl;
				else
					cout << endl << "*** " << moduleNameDesc << " Configuration ***" << endl << endl;
	
				moduleDisableState = oam::ENABLEDSTATE;
	
				//setup HostName/IPAddress for each NIC
				for( unsigned int nicID=1 ; nicID < MAX_NIC+1 ; nicID++ )
				{
					string moduleHostName = oam::UnassignedName;
					string moduleIPAddr = oam::UnassignedIpAddr;
		
					DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
					for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
					{
						if (newModuleName == (*listPT).DeviceName) {
							if ( nicID == 1 ) {
								moduleDisableState = (*listPT).DisableState;
								if ( moduleDisableState.empty() ||
									moduleDisableState == oam::UnassignedName ||
									moduleDisableState == oam::AUTODISABLEDSTATE )
									moduleDisableState = oam::ENABLEDSTATE;
							}
	
							HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
							for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
							{
								if ((*pt1).NicID == nicID) {
									moduleHostName = (*pt1).HostName;
									moduleIPAddr = (*pt1).IPAddr;
									break;
								}
							}
						}
					}
	
					if ( nicID == 1 ) {
						if ( moduleDisableState != oam::ENABLEDSTATE ) {
							string disabled = "y";
							while (true)
							{
								if ( enableModuleCount > 1 )
								{
									prompt = "Module '" + newModuleName + "' is Disabled, do you want to leave it Disabled? [y,n] (y) > ";
									pcommand = callReadline(prompt.c_str());
									if (pcommand)
									{
										if (strlen(pcommand) > 0) disabled = pcommand;
										callFree(pcommand);
									}
									if ( disabled == "y" || disabled == "n" ) {
										cout << endl;
										break;
									}
									else
										cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
									if ( noPrompting )
										exit(1);
									disabled = "y";
								}
								else 
								{
									string enable = "y";
									cout << "Module '" + newModuleName + "' is Disabled. It needs to be enabled to startup InfiniDB." << endl;
									prompt = "Do you want to Enable it or exit? [y,exit] (y) > ";
									pcommand = callReadline(prompt.c_str());
									if (pcommand)
									{
										if (strlen(pcommand) > 0) enable = pcommand;
										callFree(pcommand);
									}
									if ( enable == "y" ) {
										disabled = "n";
										break;
									}
									else
									{
										if ( enable == "exit" ) {
											cout << "Exiting postConfigure..." << endl;
											exit (1);
										}
										else
											cout << "Invalid Entry, please enter 'y' for yes or 'exit'" << endl;

										if ( noPrompting )
											exit(1);
										enable = "y";
									}
								}
							}
	
							if ( disabled == "n" )
								moduleDisableState = oam::ENABLEDSTATE;
							else 
								enableModuleCount--;
						}
		
						//set Module Disable State
						string moduleDisableStateParm = "ModuleDisableState" + oam.itoa(moduleID) + "-" + oam.itoa(i+1);
						try {
							sysConfig->setConfig(ModuleSection, moduleDisableStateParm, moduleDisableState);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting ModuleDisableState in the InfiniDB System Configuration file" << endl;
							exit(1);
						}
	
						if ( !writeConfig(sysConfig) ) {
							cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
							exit(1);
						}
	
						//skip configuration if if DISABLED state
						if ( moduleDisableState != oam::ENABLEDSTATE)
							break;
					}

					bool moduleHostNameFound = true;
					if (moduleHostName.empty()) {
						moduleHostNameFound = true;
						moduleHostName = oam::UnassignedName;
					}
	
					if (moduleIPAddr.empty())
						moduleIPAddr = oam::UnassignedIpAddr;
		
					string newModuleIPAddr;
	
					while (true)
					{
						newModuleHostName = moduleHostName;
						if (cloud == "amazon")
							prompt = "Enter EC2 Instance ID  (" + moduleHostName + ") > ";
						else
							prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " Host Name (" + moduleHostName + ") > ";

						pcommand = callReadline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) newModuleHostName = pcommand;	
							callFree(pcommand);
						}
		
						if ( newModuleHostName == oam::UnassignedName && nicID == 1 ) {
							cout << "Invalid Entry, please re-enter" << endl;
							if ( noPrompting )
								exit(1);
						}
						else
						{
							if ( newModuleHostName != oam::UnassignedName  ) {
								//check and see if hostname already used
								bool matchFound = false;
								ChildModuleList::iterator list1 = niclist.begin();
								for (; list1 != niclist.end() ; list1++)
								{
									if ( newModuleHostName == (*list1).hostName ) {
										cout << "Invalid Entry, already assigned to '" + (*list1).moduleName + "', please re-enter" << endl;
										matchFound = true;
										if ( noPrompting )
											exit(1);
										break;
									}
								}
								if ( matchFound )
									continue;

								//check Instance ID and get IP Address if running
								if (cloud == "amazon") {
									cout << "Getting Private IP Address for Instance " << newModuleHostName << ", please wait..." << endl;
									newModuleIPAddr = oam.getEC2InstanceIpAddress(newModuleHostName);
									if (newModuleIPAddr == "stopped") {
										cout << "ERROR: Instance " + newModuleHostName + " not running, please start and hit 'enter'" << endl << endl;
										if ( noPrompting )
											exit(1);
										continue;
									}
									else
									{
										if (newModuleIPAddr == "terminated") {
											cout << "ERROR: Instance " + newModuleHostName + " doesn't have an Private IP Address, please correct and hit 'enter'" << endl << endl;
											if ( noPrompting )
												exit(1);
											continue;
										}
										else
										{
											cout << "Private IP Address of " + newModuleHostName + " is " + newModuleIPAddr << endl << endl;

											moduleIPAddr = newModuleIPAddr;
											break;
										}
									}
								}
								break;
							}
							break;
						}
					}
	
					//set New Module Host Name
					string moduleHostNameParm = "ModuleHostName" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
					try {
						sysConfig->setConfig(ModuleSection, moduleHostNameParm, newModuleHostName);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting Host Name in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( newModuleHostName == oam::UnassignedName )
						newModuleIPAddr = oam::UnassignedIpAddr;
					else
					{
						if (cloud != "amazon") {
							if ( moduleIPAddr == oam::UnassignedIpAddr )
							{
								//get IP Address
								string IPAddress = oam.getIPAddress( newModuleHostName);
								if ( !IPAddress.empty() )
									newModuleIPAddr = IPAddress;
								else
									newModuleIPAddr = oam::UnassignedIpAddr;
							}
							else
								newModuleIPAddr = moduleIPAddr;

							if ( newModuleIPAddr == "127.0.0.1")
								newModuleIPAddr = "unassigned";

							//prompt for IP address
							while (true)
							{
								prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " IP Address of " + newModuleHostName + " (" + newModuleIPAddr + ") > ";
								pcommand = callReadline(prompt.c_str());
								if (pcommand)
								{
									if (strlen(pcommand) > 0) newModuleIPAddr = pcommand;	
									callFree(pcommand);
								}
	
								if (newModuleIPAddr == "127.0.0.1" || newModuleIPAddr == "0.0.0.0") {
									cout << endl << newModuleIPAddr + " is an Invalid IP Address for a multi-server system, please re-enter" << endl << endl;
									newModuleIPAddr = "unassigned";
									if ( noPrompting )
										exit(1);
									continue;
								}
	
								if (oam.isValidIP(newModuleIPAddr)) {
									//check and see if hostname already used
									bool matchFound = false;
									ChildModuleList::iterator list1 = niclist.begin();
									for (; list1 != niclist.end() ; list1++)
									{
										if ( newModuleIPAddr == (*list1).moduleIP ) {
											cout << "Invalid Entry, IP Address already assigned to '" + (*list1).moduleName + "', please re-enter" << endl;
											matchFound = true;
											if ( noPrompting )
												exit(1);
											break;
										}
									}
									if ( matchFound )
										continue;
	
									// run ping test to validate
									string cmdLine = "ping ";
									string cmdOption = " -c 1 -w 5 >> /dev/null";
									string cmd = cmdLine + newModuleIPAddr + cmdOption;
									int rtnCode = system(cmd.c_str());
									if ( WEXITSTATUS(rtnCode) != 0 ) {
										//NIC failed to respond to ping
										string temp = "2";
										while (true)
										{
											cout << endl;
											prompt = "IP Address of '" + newModuleIPAddr + "' failed ping test, please validate. Do you want to continue or re-enter [1=continue, 2=re-enter] (2) > ";
											pcommand = callReadline(prompt.c_str());
											if (pcommand)
											{
												if (strlen(pcommand) > 0) temp = pcommand;	
												callFree(pcommand);
											}
							
											if ( temp == "1" || temp == "2")
												break;
											else
											{
												temp = "2";
												cout << endl << "Invalid entry, please re-enter" << endl;
												if ( noPrompting )
													exit(1);
											}
										}
										cout << endl;
										if ( temp == "1")
											break;
									}
									else	// good ping
										break;
								}
								else
								{
									cout << endl << "Invalid IP Address format, xxx.xxx.xxx.xxx, please re-enter" << endl << endl;
									if ( noPrompting )
										exit(1);
								}
							}
						}
					}
	
					//set Module IP address
					string moduleIPAddrNameParm = "ModuleIPAddr" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
					try {
						sysConfig->setConfig(ModuleSection, moduleIPAddrNameParm, newModuleIPAddr);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting IP address in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( newModuleHostName == oam::UnassignedName && moduleHostNameFound )
						// exit out to next module ID
						break;
	
					if (moduleType == "pm" && moduleDisableState == oam::ENABLEDSTATE) {
		
						switch(nicID) {
							case 1:
								performancemodule.moduleIP1 = newModuleIPAddr;
								break;
							case 2:
								performancemodule.moduleIP2 = newModuleIPAddr;
								break;
							case 3:
								performancemodule.moduleIP3 = newModuleIPAddr;
								break;
							case 4:
								performancemodule.moduleIP4 = newModuleIPAddr;
								break;
						}
						if ( maxPMNicCount < nicID )
							maxPMNicCount = nicID;
					}
	
					//save Nic host name and IP
					childmodule.moduleName = newModuleName;
					childmodule.moduleIP = newModuleIPAddr;
					childmodule.hostName = newModuleHostName;
					niclist.push_back(childmodule);
	
					if ( nicID > 1 )
						continue;

					//save Child modules
					if ( newModuleName != parentOAMModuleName) {
						childmodule.moduleName = newModuleName;
						childmodule.moduleIP = newModuleIPAddr;
						childmodule.hostName = newModuleHostName;
						childmodulelist.push_back(childmodule);
					}
	
					//set port addresses
					if ( newModuleName == parentOAMModuleName ) {
						parentOAMModuleHostName = newModuleHostName;
						parentOAMModuleIPAddr = newModuleIPAddr;

						//set Parent Processes Port IP Address
						string parentProcessMonitor = parentOAMModuleName + "_ProcessMonitor";
						sysConfig->setConfig(parentProcessMonitor, "IPAddr", parentOAMModuleIPAddr);
						sysConfig->setConfig(parentProcessMonitor, "Port", "8800");
						sysConfig->setConfig("ProcMgr", "IPAddr", parentOAMModuleIPAddr);
						//sysConfig->setConfig("ProcHeartbeatControl", "IPAddr", parentOAMModuleIPAddr);
						sysConfig->setConfig("ProcStatusControl", "IPAddr", parentOAMModuleIPAddr);
						string parentServerMonitor = parentOAMModuleName + "_ServerMonitor";
						sysConfig->setConfig(parentServerMonitor, "IPAddr", parentOAMModuleIPAddr);
						string portName = parentOAMModuleName + "_WriteEngineServer";
						sysConfig->setConfig(portName, "IPAddr", parentOAMModuleIPAddr);
						sysConfig->setConfig(portName, "Port", "8630");
	
						if( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) {
	
							//set User Module's IP Addresses
							string Section = "ExeMgr" + oam.itoa(moduleID);
			
							sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(Section, "Port", "8601");
							sysConfig->setConfig(Section, "Module", parentOAMModuleName);

							//set Performance Module's IP's to first NIC IP entered
							sysConfig->setConfig("DDLProc", "IPAddr", newModuleIPAddr);
							sysConfig->setConfig("DMLProc", "IPAddr", newModuleIPAddr);
						}

						//set User Module's IP Addresses
						if ( pmwithum ) {
							string Section = "ExeMgr" + oam.itoa(moduleID+umNumber);
		
							sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(Section, "Port", "8601");
							sysConfig->setConfig(Section, "Module", parentOAMModuleName);

						}
		
						//setup rc.local file in module tmp dir
						if( !makeRClocal(moduleType , newModuleName, IserverTypeInstall) )
							cout << "makeRClocal error" << endl;
					}
					else
					{
						//set child Process Monitor Port IP Address
						string portName = newModuleName + "_ProcessMonitor";
						sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
	
						sysConfig->setConfig(portName, "Port", "8800");
	
						//set child Server Monitor Port IP Address
						portName = newModuleName + "_ServerMonitor";
						sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
						sysConfig->setConfig(portName, "Port", "8622");
		
						//set Performance Module WriteEngineServer Port IP Address
						if ( moduleType == "pm" ) {
							portName = newModuleName + "_WriteEngineServer";
							sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(portName, "Port", "8630");
						}
	
						//set User Module's IP Addresses
						if ( moduleType == "um" ||
							( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
							( moduleType == "um" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM ) ||
							( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_PM_UM ) ||
							( moduleType == "pm" && pmwithum ) ) {

							string Section = "ExeMgr" + oam.itoa(moduleID);
							if ( moduleType == "pm" && pmwithum )
								Section = "ExeMgr" + oam.itoa(moduleID+umNumber);
		
							sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(Section, "Port", "8601");
							sysConfig->setConfig(Section, "Module", newModuleName);
						}
		
						//set Performance Module's IP's to first NIC IP entered
						if ( newModuleName == "um1" ) {
							sysConfig->setConfig("DDLProc", "IPAddr", newModuleIPAddr);
							sysConfig->setConfig("DMLProc", "IPAddr", newModuleIPAddr);
						}
					}
					
					if ( !writeConfig(sysConfig) ) {
						cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
						exit(1);
					}

					//create associated local/etc directory for module
					string cmd = "mkdir " + installDir + "/local/etc/" + newModuleName + " > /dev/null 2>&1";
					system(cmd.c_str());
			
					if ( newModuleName != parentOAMModuleName) {		
						//make module file in local/etc/"modulename"
						if( !makeModuleFile(newModuleName, parentOAMModuleName) )
							cout << "makeModuleFile error" << endl;
					}
	
					//setup rc.local file in module tmp dir
					if( !makeRClocal(moduleType , newModuleName, IserverTypeInstall) )
						cout << "makeRClocal error" << endl;
	
					//if cloud, copy fstab in module tmp dir
					if ( cloud == "amazon" && moduleType == "pm")
						if( !copyFstab(newModuleName) )
							cout << "copyFstab error" << endl;

					//setup DBRM Processes
					if ( newModuleName == parentOAMModuleName )
						sysConfig->setConfig("DBRM_Controller", "IPAddr", newModuleIPAddr);
	
					if ( moduleDisableState == oam::ENABLEDSTATE ) {
						DBRMworkernodeID++;
						string DBRMSection = "DBRM_Worker" + oam.itoa(DBRMworkernodeID);
						sysConfig->setConfig(DBRMSection, "IPAddr", newModuleIPAddr);
						sysConfig->setConfig(DBRMSection, "Module", newModuleName);
					}	

					// only support 1 nic ID per Amazon instance
					if (cloud == "amazon")
						break;

				} //end of nicID loop

				//enter storage for user module
				if ( moduleType == "um" && moduleDisableState == oam::ENABLEDSTATE) {
					//get EC2 volume name and info
					if ( UMStorageType == "external" && cloud == "amazon" &&
							IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM ) {

						string volumeNameID = "UMVolumeName" + oam.itoa(moduleID);
						string volumeName = oam::UnassignedName;
						string deviceNameID = "UMVolumeDeviceName" + oam.itoa(moduleID);
						string deviceName = oam::UnassignedName;
						try {
							volumeName = sysConfig->getConfig(InstallSection, volumeNameID);
							deviceName = sysConfig->getConfig(InstallSection, deviceNameID);
						}
						catch(...)
						{}

						prompt = "Enter Volume Name assigned to module '" + newModuleName + "' (" + volumeName + ") > ";
						pcommand = callReadline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) volumeName = pcommand;	
							callFree(pcommand);
						}

						prompt = "Enter Device Name assigned to module '" + newModuleName + "' (" + deviceName + ") > ";
						pcommand = callReadline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) deviceName = pcommand;	
							callFree(pcommand);
						}

						//write volume and device name
						try {
							sysConfig->setConfig(InstallSection, volumeNameID, volumeName);
							sysConfig->setConfig(InstallSection, deviceNameID, deviceName);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting Volume/Device Names in the InfiniDB System Configuration file" << endl;
							return false;
						}

						if ( !writeConfig(sysConfig) ) {
							cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
							exit(1);
						}

						string pathID = installDir + "/mysql/db";

						// check volume for attach and try to attach if not
						if( !attachVolume(newModuleHostName, volumeName, deviceName, pathID) ) {
							cout << "attachVolume error" << endl;
							exit(1);
						}
					}
				}

				//if upgrade, get list of configure dbroots
				DBRootConfigList dbrootConfigList;
				if ( reuseConfig == "y" ) 
				{
					try {
						oam.getSystemDbrootConfig(dbrootConfigList);
					}
					catch(...) {}
				}

				//enter dbroots for performance module
				if ( moduleType == "pm" && moduleDisableState == oam::ENABLEDSTATE) {
					//get number of dbroots
					string moduledbrootcount = "ModuleDBRootCount" + oam.itoa(moduleID) + "-" + oam.itoa(i+1);
					string tempCount;
					try {
						tempCount = sysConfig->getConfig(ModuleSection, moduledbrootcount);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting dbroot count in the InfiniDB System Configuration file" << endl;
						exit(1);
					}

					unsigned int count = 0;
					if (tempCount != oam::UnassignedName )
						count = atoi(tempCount.c_str());

					string dbrootList;

					for ( unsigned int id = 1 ; id < count+1 ;  )
					{
						string moduledbrootid = "ModuleDBRootID" + oam.itoa(moduleID) + "-" + oam.itoa(id) + "-" + oam.itoa(i+1);
						try {
							string dbrootid = sysConfig->getConfig(ModuleSection, moduledbrootid);

							if ( dbrootid != oam::UnassignedName) {
								sysConfig->setConfig(ModuleSection, moduledbrootid, oam::UnassignedName);

								dbrootList = dbrootList + dbrootid;
								id ++;
								if ( id < count+1 )
									dbrootList = dbrootList + ",";
							}
						}
						catch(...)
						{
							cout << "ERROR: Problem setting DBRoot ID in the InfiniDB System Configuration file" << endl;
							exit(1);
						}
					}

					vector <string> dbroots;
					string tempdbrootList;

					while(true)
					{
						dbroots.clear();
						bool matchFound = false;

						prompt = "Enter the list (Nx,Ny,Nz) or range (Nx-Nz) of dbroot IDs assigned to module '" + newModuleName + "' (" + dbrootList + ") > ";
						pcommand = callReadline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) {
								tempdbrootList = pcommand;
								callFree(pcommand);
							}
							else
								tempdbrootList = dbrootList;
						}

						if ( tempdbrootList.empty())
						{
							if ( noPrompting )
								exit(1);

							continue;
						}

						//check for range
						int firstID;
						int lastID;
						string::size_type pos = tempdbrootList.find("-",0);
						if (pos != string::npos)
						{
							firstID = atoi(tempdbrootList.substr(0, pos).c_str());
							lastID = atoi(tempdbrootList.substr(pos+1, 200).c_str());

							if ( firstID >= lastID ) {
								cout << "Invalid Entry, please re-enter" << endl;
								if ( noPrompting )
									exit(1);

								continue;
							}
							else
							{
								for ( int id = firstID ; id < lastID+1 ; id++ )
								{
									dbroots.push_back(oam.itoa(id));
								}
							}
						}
						else
						{
							boost::char_separator<char> sep(",");
							boost::tokenizer< boost::char_separator<char> > tokens(tempdbrootList, sep);
							for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
									it != tokens.end();
									++it)
							{
								dbroots.push_back(*it);
							}
						}

						//check and see if dbroot ID already used
						std::vector<std::string>::iterator list = dbroots.begin();
						for (; list != dbroots.end() ; list++)
						{
							bool inUse = false;
							matchFound = false;
							DBRootList::iterator list1 = dbrootlist.begin();
							for (; list1 != dbrootlist.end() ; list1++)
							{
								if ( *list == (*list1).dbrootID ) {
									cout << "Invalid Entry, DBRoot ID " + *list + " already assigned to '" + (*list1).moduleName + "', please re-enter" << endl;
									if ( noPrompting )
										exit(1);
									inUse = true;
									break;
								}
							}

							if ( inUse)
								break;
							else 
							{	// if upgrade, dont allow a new dbroot id to be entered
								if ( reuseConfig == "y" )
								{
									DBRootConfigList::iterator pt = dbrootConfigList.begin();
									for( ; pt != dbrootConfigList.end() ; pt++)
									{
										if ( *list == oam.itoa(*pt) ) {
											matchFound = true;
											break;
										}
									}

									if ( !matchFound) {
										//get any unassigned DBRoots
										DBRootConfigList undbrootlist;
										try {
											oam.getUnassignedDbroot(undbrootlist);
										}
										catch(...) {}
						
										if ( !undbrootlist.empty() )
										{
											DBRootConfigList::iterator pt1 = undbrootlist.begin();
											for( ; pt1 != undbrootlist.end() ; pt1++)
											{
												if ( *list == oam.itoa(*pt1) ) {
													matchFound = true;
													break;
												}
											}
										}

										if ( !matchFound) {
											cout << "Invalid Entry, DBRoot ID " + *list + " doesn't exist, can't add a new DBRoot during upgrade process, please re-enter" << endl;
											if ( noPrompting )
												exit(1);
											break;
										}
									}
								}
								else	// new install, set to found
									matchFound = true;
							}
						}

						if ( matchFound )
						break;
					}

					int id=1;
					std::vector<std::string>::iterator it = dbroots.begin();
					for (; it != dbroots.end() ; it++, ++id)
					{
						//save Nic host name and IP
						dbrootmodule.moduleName = newModuleName;
						dbrootmodule.dbrootID = *it;
						dbrootlist.push_back(dbrootmodule);

						//store dbroot ID
						string moduledbrootid = "ModuleDBRootID" + oam.itoa(moduleID) + "-" + oam.itoa(id) + "-" + oam.itoa(i+1);
						try {
							sysConfig->setConfig(ModuleSection, moduledbrootid, *it);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting DBRoot ID in the InfiniDB System Configuration file" << endl;
							exit(1);
						}

						string DBrootID = "DBRoot" + *it;
						string pathID = installDir + "/data" + *it;
				
						try {
							sysConfig->setConfig(SystemSection, DBrootID, pathID);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting DBRoot in the InfiniDB System Configuration file" << endl;
							return false;
						}

						//create data directory
						cmd = "mkdir " + pathID + " > /dev/null 2>&1";
						system(cmd.c_str());
						cmd = "chmod 1777 " + pathID + " > /dev/null 2>&1";
						system(cmd.c_str());

						//get EC2 volume name and info
						if ( DBRootStorageType == "external" && cloud == "amazon") {
							cout << endl;
							string volumeNameID = "PMVolumeName" + *it;
							string volumeName = oam::UnassignedName;
							try {
								volumeName = sysConfig->getConfig(InstallSection, volumeNameID);
							}
							catch(...)
							{}

							prompt = "Enter Volume Name assigned to '" + DBrootID + "' (" + volumeName + ") > ";
							pcommand = callReadline(prompt.c_str());
							if (pcommand)
							{
								if (strlen(pcommand) > 0) volumeName = pcommand;	
								callFree(pcommand);
							}

							string deviceNameID = "PMVolumeDeviceName" + *it;
							string deviceName = oam::UnassignedName;
							try {
								deviceName = sysConfig->getConfig(InstallSection, deviceNameID);
							}
							catch(...)
							{}

							prompt = "Enter Device Name for volume '" + volumeName + "' (" + deviceName + ") > ";
							pcommand = callReadline(prompt.c_str());
							if (pcommand)
							{
								if (strlen(pcommand) > 0) deviceName = pcommand;	
								callFree(pcommand);
							}

							//write volume and device name
							try {
								sysConfig->setConfig(InstallSection, volumeNameID, volumeName);
								sysConfig->setConfig(InstallSection, deviceNameID, deviceName);
							}
							catch(...)
							{
								cout << "ERROR: Problem setting Volume/Device Names in the InfiniDB System Configuration file" << endl;
								return false;
							}

							if ( !writeConfig(sysConfig) ) {
								cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
								exit(1);
							}

							// check volume for attach and try to attach if not
							if( !attachVolume(newModuleHostName, volumeName, deviceName, pathID) ) {
								cout << "attachVolume error" << endl;
								exit(1);
							}
						}
					}

					//store number of dbroots
					moduledbrootcount = "ModuleDBRootCount" + oam.itoa(moduleID) + "-" + oam.itoa(i+1);
					try {
						sysConfig->setConfig(ModuleSection, moduledbrootcount, oam.itoa(dbroots.size()));
					}
					catch(...)
					{
						cout << "ERROR: Problem setting dbroot count in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
					//total dbroots on the system
					DBRootCount = DBRootCount + dbroots.size();
				}

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}

				if (moduleType == "pm" && moduleDisableState == oam::ENABLEDSTATE )
					performancemodulelist.push_back(performancemodule);
			} //end of k (moduleCount) loop

			if ( moduleSkip ) {
				moduleSkip = false;
				moduleID = saveModuleID;
			}
			else
				break;
		}

		if ( !writeConfig(sysConfig) ) {
			cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
			exit(1);
		}

	} //end of i for loop

	try {
		sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(DBRootCount));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//check 'files per parition' with number of dbroots
	checkFilesPerPartion(DBRootCount, sysConfig);

	//setup DBRM Controller
	sysConfig->setConfig("DBRM_Controller", "NumWorkers", oam.itoa(DBRMworkernodeID));

	//set ConnectionsPerPrimProc
	try {
		sysConfig->setConfig("PrimitiveServers", "ConnectionsPerPrimProc", oam.itoa(maxPMNicCount*2));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ConnectionsPerPrimProc in the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//set the PM Ports based on Number of PM modules equipped, if any equipped
	int minPmPorts = 32;
	sysConfig->setConfig("PrimitiveServers", "Count", oam.itoa(pmNumber));

	int pmPorts = pmNumber * (maxPMNicCount*2);
	if ( pmPorts < minPmPorts )
		pmPorts = minPmPorts;

	if ( pmNumber > 0 )
	{
		const string PM = "PMS";

		for ( int pmsID = 1; pmsID < pmPorts+1 ; )
		{
			for (unsigned int j = 1 ; j < maxPMNicCount+1 ; j++)
			{
				PerformanceModuleList::iterator list1 = performancemodulelist.begin();
		
				for (; list1 != performancemodulelist.end() ; list1++)
				{
					string pmName = PM + oam.itoa(pmsID);
					string IpAddr;
					switch(j) {
						case 1:
							IpAddr = (*list1).moduleIP1;
							break;
						case 2:
							IpAddr = (*list1).moduleIP2;
							break;
						case 3:
							IpAddr = (*list1).moduleIP3;
							break;
						case 4:
							IpAddr = (*list1).moduleIP4;
							break;
					}
					if ( !IpAddr.empty() && IpAddr != oam::UnassignedIpAddr ) {
						sysConfig->setConfig(pmName, "IPAddr", IpAddr);
						sysConfig->setConfig(pmName, "Port", "8620");
						pmsID++;
						if ( pmsID > pmPorts )
							break;
					}
				}
				if ( pmsID > pmPorts )
					break;
			}
		}
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//check snmp Apps disable option
	snmpAppCheck();

	//
	// Configure External Devices
	//
	SystemExtDeviceConfig systemextdeviceconfig;

	try{
		oam.getSystemConfig(systemextdeviceconfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	cout << endl << "===== Setup the External Device Configuration =====" << endl << endl;

	cout << "External Devices are devices like a storage array or a Ethernet Switch that can" << endl;
	cout << "be setup to be monitored by InfiniDB with a ping test. If device fails, InfiniDB" << endl;
	cout << "will report a failure alarm." << endl << endl;

	if ( systemextdeviceconfig.Count > 0 ) {

		cout << "Current List of External Devices being monitored" << endl << endl;
	
		cout << "Device Name    IP Address" << endl;
		cout << "--------------------------    ----------------" << endl;

		for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
		{
			cout.setf(ios::left);
			cout.width(30);
			cout << systemextdeviceconfig.extdeviceconfig[i].Name;
			cout << systemextdeviceconfig.extdeviceconfig[i].IPAddr << endl;
//			cout << "Device Name: " << (*pt1).Name << "     IP Address: " << (*pt1).IPAddr << endl;
		}
	
		cout << endl;
	
		string modify = "n";
		while(true) {
			prompt = "Would you like to modify current list? [y,n] (n) > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) modify = pcommand;
				callFree(pcommand);
			}
			if ( modify == "y" || modify == "n" )
				break;
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( modify == "y" ) {
	
			for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
			{
				string name = systemextdeviceconfig.extdeviceconfig[i].Name;
				modify = "n";
				while(true) {
					prompt = "Would you like to modify or delete '" + name + "'? [m,d,n] (n) > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand)
					{
						if (strlen(pcommand) > 0) modify = pcommand;
						callFree(pcommand);
					}
					if ( modify == "m" || modify == "d" || modify == "n")
						break;
					cout << "Invalid Entry, please enter 'm' for modify, 'd' for delete or 'n' for no" << endl;
					if ( noPrompting )
						exit(1);
				}
				
				if ( modify == "d" ) {
					// delete device
					ExtDeviceConfig extdeviceconfig;
	
					extdeviceconfig.Name = oam::UnassignedName;
					extdeviceconfig.IPAddr = oam::UnassignedIpAddr;
					extdeviceconfig.DisableState = oam::ENABLEDSTATE;
	
					try{
						oam.setSystemConfig(name, extdeviceconfig);
					}
					catch(...)
					{
						cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
						exit(1);
					}
					cout << "External Device '" + name + "' deleted" << endl << endl;
				}
				else
				{
					if ( modify == "m" ) {
						string newName = name;
						prompt = "Enter Name (" + newName + ") > ";
						pcommand = callReadline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) newName = pcommand;
							callFree(pcommand);
						}
	
						string newIPAddr = systemextdeviceconfig.extdeviceconfig[i].IPAddr;
						while (true)
						{
							prompt = "Enter IP Address of (" + newIPAddr + ") > ";
							pcommand = callReadline(prompt.c_str());
							if (pcommand)
							{
								if (strlen(pcommand) > 0) newIPAddr = pcommand;
								callFree(pcommand);
							}
	
							if (oam.isValidIP(newIPAddr))
								break;
							else
							{
								cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please re-enter" << endl;
								if ( noPrompting )
									exit(1);
							}
						}
	
						ExtDeviceConfig extdeviceconfig;
		
						extdeviceconfig.Name = newName;
						extdeviceconfig.IPAddr = newIPAddr;
						extdeviceconfig.DisableState = oam::ENABLEDSTATE;
		
						try{
							oam.setSystemConfig(name, extdeviceconfig);
						}
						catch(...)
						{
							cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
							exit(1);
						}
						cout << "External Device '" + name + "' modified" << endl << endl;
					}
				}
			}
		}
	}

	while(true) {
		string add = "n";
		while(true) {
			prompt = "Would you like to add an External Device? [y,n] (n) > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) add = pcommand;
				callFree(pcommand);
			}
			if ( add == "y" || add == "n" )
				break;
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( add == "y" ) {
			cout << endl;
			string newName = oam::UnassignedName;
			while(true) {
				prompt = "Enter Name (" + newName + ") > ";
				pcommand = callReadline(prompt.c_str());
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newName = pcommand;
					callFree(pcommand);
				}
				
				if ( newName == oam::UnassignedName ) {
					cout << "Invalid Entry, please enter valid name or 'abort'" << endl;
					if ( noPrompting )
						exit(1);
				}
				else
					break;
			}

			if ( newName == "abort" )
				continue;

			//get Network IP Address
			string newIPAddr = oam::UnassignedIpAddr;
			string IPAddress = oam.getIPAddress(newName);
			if ( !IPAddress.empty() )
				newIPAddr = IPAddress;

			while (true)
			{
				prompt = "Enter IP Address of (" + newIPAddr + ") > ";
				pcommand = callReadline(prompt.c_str());
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newIPAddr = pcommand;
					callFree(pcommand);
				}
		
				if (oam.isValidIP(newIPAddr))
					break;
				else
					cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please re-enter" << endl;
				if ( noPrompting )
					exit(1);
				newIPAddr = oam::UnassignedIpAddr;
			}
		
			ExtDeviceConfig extdeviceconfig;
		
			extdeviceconfig.Name = newName;
			extdeviceconfig.IPAddr = newIPAddr;
			extdeviceconfig.DisableState = oam::ENABLEDSTATE;
		
			try{
				oam.setSystemConfig(newName, extdeviceconfig);
			}
			catch(...)
			{
				cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
				exit(1);
			}
			cout << endl;
		}
		else
			break;
	}

	//setup local OS Files
	if( !setOSFiles(parentOAMModuleName, IserverTypeInstall) ) {
		cout << "setOSFiles error" << endl;
		exit(1);
	}

	//create directories on dbroot1
	if( !createDbrootDirs(DBRootStorageType) ) {
		cout << "createDbrootDirs error" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}
	
	//check if dbrm data resides in older directory path and inform user if it does
	dbrmDirCheck();

	if ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM && pmNumber == 1) {
		//run the mysql / mysqld setup scripts
		cout << endl << "===== Running the InfiniDB MySQL setup scripts =====" << endl << endl;

		checkMysqlPort(mysqlPort, sysConfig);

		// call the mysql setup scripts
		mysqlSetup();
		sleep(5);
	}

	int thread_id = 0;

	pthread_t thr[childmodulelist.size()];
	
	/* create a thread_data_t argument array */
	thread_data_t thr_data[childmodulelist.size()];

	if ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM || 
			pmNumber > 1 ) {
		//
		// perform remote install of other servers in the system
		//
		cout << endl << "===== System Installation =====" << endl << endl;
	
		string install = "y";
		cout << "System Configuration is complete, System Installation is the next step." << endl;

		while(true)
		{
			pcommand = callReadline("Would you like to continue with the System Installation? [y,n] (y) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) install = pcommand;
				callFree(pcommand);
			}
			if ( install == "y" || install == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			install = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( install == "y" ) {
	
			SystemSoftware systemsoftware;
		
			try
			{
				oam.getSystemSoftware(systemsoftware);
			}
			catch (exception& e)
			{
				cout << " ERROR: reading getSystemSoftware API" << endl;
				exit (1);
			}

			cout << endl;

			//Write out Updated System Configuration File
			string EEPackageType;
			try {
				EEPackageType = sysConfig->getConfig(InstallSection, "EEPackageType");
			}
			catch(...)
			{
				cout << "ERROR: Problem getting EEPackageType from the InfiniDB System Configuration file" << endl;
				exit(1);
			}

			while(true) {
				prompt = "Enter the Package Type being installed to other servers [rpm,deb,binary] (" + EEPackageType + ") > ";
				pcommand = callReadline(prompt);
				if (pcommand) {
					if (strlen(pcommand) > 0) EEPackageType = pcommand;
					callFree(pcommand);
				}

				if ( EEPackageType == "rpm" || EEPackageType == "deb" || EEPackageType == "binary"  ) {
					break;
				}
				cout << "Invalid Package Type, please re-enter" << endl;
				EEPackageType = "rpm";
				if ( noPrompting )
					exit(1);
			}

			if ( EEPackageType == "rpm" )
				cout << "Performing an InfiniDB System install using RPM packages located in the " + home + " directory." << endl;
			else
			{
				if ( EEPackageType == "binary" )
					cout << "Performing an InfiniDB System install using a Binary package located in the " + home + " directory." << endl;
				else
					cout << "Performing an InfiniDB System install using using DEB packages located in the " + home + " directory." << endl;
			}
	
			//Write out Updated System Configuration File
			try {
				sysConfig->setConfig(InstallSection, "EEPackageType", EEPackageType);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting EEPackageType from the InfiniDB System Configuration file" << endl;
				exit(1);
			}
		
			if ( !writeConfig(sysConfig) ) { 
				cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl; 
				exit(1);
			}

			//check if pkgs are located in $HOME directory
			if ( EEPackageType != "binary") {
				string separator = "-";
				if ( EEPackageType == "deb" )
					separator = "_";
				calpontPackage1 = "infinidb-libs" + separator + systemsoftware.Version + "-" + systemsoftware.Release;
				calpontPackage2 = "infinidb-platform" + separator + systemsoftware.Version + "-" + systemsoftware.Release;
				calpontPackage3 = "infinidb-enterprise" + separator + systemsoftware.Version + "-" + systemsoftware.Release;
				mysqlPackage = "infinidb-storage-engine" + separator + systemsoftware.Version + "-" + systemsoftware.Release;
				mysqldPackage = "infinidb-mysql" + separator + systemsoftware.Version + "-" + systemsoftware.Release;

				if( !pkgCheck() ) {
					exit(1);
				}
				else
				{
					calpontPackage1 = home + "/" + calpontPackage1 + "*." + EEPackageType;
					calpontPackage2 = home + "/" + calpontPackage2 + "*." + EEPackageType;
					calpontPackage3 = home + "/" + calpontPackage3 + "*." + EEPackageType;
					mysqlPackage = home + "/" + mysqlPackage  + "*." + EEPackageType;
					mysqldPackage = home + "/" + mysqldPackage  + "*." + EEPackageType;
				}
			}
			else
			{
				string fileName = installDir + "/bin/healthcheck";
				ifstream file (fileName.c_str());
				if (!file)	// CE
					calpontPackage1 = "infinidb-" + systemsoftware.Version + "-" + systemsoftware.Release;
				else		// EE
					calpontPackage1 = "infinidb-ent-" + systemsoftware.Version + "-" + systemsoftware.Release;
				calpontPackage2 = "dummy";
				calpontPackage3 = "dummy";
				mysqlPackage = calpontPackage1;
				mysqldPackage = calpontPackage1;

				if( !pkgCheck() )
					exit(1);
				calpontPackage1 = home + "/" + calpontPackage1 + "*.bin.tar.gz";
				calpontPackage2 = "dummy";
				calpontPackage3 = "dummy";
			}

			//If ent pkg is not there, mark it as such
			{
				glob_t gt;
				memset(&gt, 0, sizeof(gt));
				if (glob(calpontPackage3.c_str(), 0, 0, &gt) != 0)
					calpontPackage3 = "dummy.rpm";
				globfree(&gt);
			}

			//if PM is running with UM functionality
			// install UM packages and run mysql setup scripts
			if ( pmwithum ) {
				//run the mysql / mysqld setup scripts
		
				if ( EEPackageType != "binary") {
					cout << endl << "===== Installing InfiniDB UM Packages and Running the InfiniDB MySQL setup scripts =====" << endl << endl;
					string cmd = "rpm -Uv --force " + mysqlPackage + " " + mysqldPackage;
					if ( EEPackageType == "deb" )
						cmd = "dpkg -i " + mysqlPackage + " " + mysqldPackage;
					system(cmd.c_str());
					cout << endl;
				}
			}
			else
			{
				// run my.cnf upgrade script
				if ( reuseConfig == "y" && MySQLRep == "y" && 
					IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM )
				{
					cmd = installDir + "/bin/mycnfUpgrade  > /tmp/mycnfUpgrade.log 2>&1";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0)
						cout << "Error: Problem upgrade my.cnf, check /tmp/mycnfUpgrade.log" << endl;
				}
			}

			cout << endl;
			cout << "Next step is to enter the password to access the other Servers." << endl;
			cout << "This is either your password or you can default to using a ssh key" << endl;
			cout << "If using a password, the password needs to be the same on all Servers." << endl << endl;

			while(true)
			{	
				char  *pass1, *pass2;

				if ( noPrompting ) {
					cout << "Enter password, hit 'enter' to default to using a ssh key, or 'exit' > " << endl;
					if ( password.empty() )
						password = "ssh";
					break;
				}

				//check for command line option password
				if ( !password.empty() )
					break;

				pass1=getpass("Enter password, hit 'enter' to default to using a ssh key, or 'exit' > ");
				if ( strcmp(pass1, "") == 0 ) {
					password = "ssh";
					break;
				}

				if ( strcmp(pass1, "exit") == 0 )
					exit(0);
				string p1 = pass1;
				pass2=getpass("Confirm password > ");
				string p2 = pass2;
				if ( p1 == p2 ) {
					password = p2;
					break;
				}
				else
					cout << "Password mismatch, please re-enter" << endl;
			}

			//add single quote for special characters
			if ( password != "ssh" )
			{
				password = "'" + password + "'";
			}

			checkSystemMySQLPort(mysqlPort, sysConfig, USER, password, childmodulelist, IserverTypeInstall, pmwithum);

			if ( ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
				( (IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM) && pmwithum ) )
			{
				cout << endl << "===== Running the InfiniDB MySQL setup scripts =====" << endl << endl;

				// run my.cnf upgrade script
				if ( reuseConfig == "y" && MySQLRep == "y" )
				{
					cmd = installDir + "/bin/mycnfUpgrade  > /tmp/mycnfUpgrade.log 2>&1";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0)
						cout << "Error: Problem upgrade my.cnf, check /tmp/mycnfUpgrade.log" << endl;
				}

				// call the mysql setup scripts
				mysqlSetup();
				sleep(5);
			}

			ChildModuleList::iterator list1 = childmodulelist.begin();
			for (; list1 != childmodulelist.end() ; list1++)
			{
				string remoteModuleName = (*list1).moduleName;
				string remoteModuleIP = (*list1).moduleIP;
				string remoteHostName = (*list1).hostName;
				string remoteModuleType = remoteModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
	
				string debug_logfile;
				string logfile;
				if ( remote_installer_debug == "1" ) {
					logfile = "/tmp/";
					logfile += remoteModuleName + "_" + EEPackageType + "_install.log";
					debug_logfile = " > " + logfile;
				}

				if ( remoteModuleType == "um" ||
					(remoteModuleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) ||
					(remoteModuleType == "pm" && pmwithum) )
				{
					cout << endl << "----- Performing Install on '" + remoteModuleName + " / " + remoteHostName + "' -----" << endl << endl;

					if ( remote_installer_debug == "1" )
						cout << "Install log file is located here: " + logfile << endl << endl;

					if ( EEPackageType != "binary" ) {
						string temppwprompt = pwprompt;
						if ( pwprompt == " " )
							temppwprompt = "none";

						//check my.cnf port in-user on remote node
//						checkRemoteMysqlPort(remoteModuleIP, remoteModuleName, USER, password, mysqlPort, sysConfig);

						//run remote installer script
						cmd = installDir + "/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage1 + " " + calpontPackage2 + " " + calpontPackage3 + " " + mysqlPackage + " " + mysqldPackage + " initial " + EEPackageType + " " + nodeps + " " + temppwprompt + " " + mysqlPort + " " + remote_installer_debug + " " + debug_logfile;

						if ( thread_remote_installer == "1" ) {
							thr_data[thread_id].command = cmd;

							int status = pthread_create (&thr[thread_id], NULL, (void*(*)(void*)) &remoteInstallThread, &thr_data[thread_id]);
					
							if ( status != 0 )
							{
								cout << "remoteInstallThread failed for " << remoteModuleName << ", exiting" << endl;
								exit (1);
							}
							thread_id++;
						}
						else
						{
							int rtnCode = system(cmd.c_str());
							if (WEXITSTATUS(rtnCode) != 0) {
								cout << endl << "Error returned from user_installer.sh" << endl;
								exit(1);
							}

							//check for mysql password on remote UM
							if ( pwprompt == " " ) {
								cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + installDir + "/mysql/mysql-Calpont start'";
								int rtnCode = system(cmd.c_str());
								if (WEXITSTATUS(rtnCode) != 0) {
									cout << endl << "Error returned from mysql-Calpont start" << endl;
									exit(1);
								}
	
								string prompt = " *** Enter MySQL password > ";
								for (;;)
								{
									cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + installDir + "/mysql/bin/mysql --defaults-file=" + installDir + "/mysql/my.cnf -u root " + pwprompt + " -e status' 1 > /tmp/idbmysql.log 2>&1";
									rtnCode = system(cmd.c_str());
									if (WEXITSTATUS(rtnCode) != 0) {
										cout << endl << "Error returned from remote_command.sh" << endl;
										exit(1);
									}
		
									if (oam.checkLogStatus("/tmp/idbmysql.log", "ERROR 1045") ) {
	
										if ( prompt == " *** Enter MySQL password > " )
											cout << endl << " MySQL password set on Module '" + remoteModuleName + "', Additional MySQL Install steps being performed" << endl << endl;

										if ( mysqlpw == " " ) {
											if ( noPrompting ) {
												cout << " *** MySQL password required, enter on command line, exiting..." << endl;
												exit(1);
											}
							
											mysqlpw = getpass(prompt.c_str());
										}

										mysqlpw = "'" + mysqlpw + "'";
										pwprompt = "--password=" + mysqlpw;
										prompt = " *** Password incorrect, please re-enter MySQL password > ";
									}
									else
									{
										if (!oam.checkLogStatus("/tmp/idbmysql.log", "InfiniDB") ) {
											cout << endl << "ERROR: MySQL runtime error, exit..." << endl << endl;
											system("cat /tmp/idbmysql.log");
											exit (1);
										}
										else
										{
											cout << endl << "Additional MySQL Installation steps Successfully Completed on '" + remoteModuleName + "'" << endl << endl;
	
											cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + installDir + "/mysql/mysql-Calpont stop'";
											int rtnCode = system(cmd.c_str());
											if (WEXITSTATUS(rtnCode) != 0) {
												cout << endl << "Error returned from mysql-Calpont stop" << endl;
												exit(1);
											}
											unlink("/tmp/idbmysql.log");
											break;
										}
									}
								}
	
								//re-run post-mysqld-install with password
								cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + installDir + "/bin/post-mysql-install " + pwprompt + "'";
								rtnCode = system(cmd.c_str());
								if (WEXITSTATUS(rtnCode) != 0) {
									cout << endl << "Error returned from post-mysql-install" << endl;
									exit(1);
								}
							}
						}
					}
					else
					{	// do a binary package install
						string binservertype = serverTypeInstall;
						if ( pmwithum )
							binservertype = "pmwithum";

						//check my.cnf port in-user on remote node
//						checkRemoteMysqlPort(remoteModuleIP, remoteModuleName, USER, password, mysqlPort, sysConfig);

						cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " +
							remoteModuleIP + " " + password + " " + calpontPackage1 + " " + remoteModuleType +
							" initial " + binservertype + " " + mysqlPort + " " + remote_installer_debug +
							" " + installDir + " " + debug_logfile;

						if ( thread_remote_installer == "1" ) {
							thr_data[thread_id].command = cmd;

							int status = pthread_create (&thr[thread_id], NULL, (void*(*)(void*)) &remoteInstallThread, &thr_data[thread_id]);
					
							if ( status != 0 )
							{
								cout << "remoteInstallThread failed for " << remoteModuleName << ", exiting" << endl;
								exit (1);
							}
					
							thread_id++;
						}
						else
						{
							int rtnCode = system(cmd.c_str());
							if (WEXITSTATUS(rtnCode) != 0) {
								cout << endl << "Error returned from user_installer.sh" << endl;
								exit(1);
							}
						}
					}
				}
				else
				{
					if ( (remoteModuleType == "pm" && IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM) ||
						(remoteModuleType == "pm" && !pmwithum ) )
					{
						cout << endl << "----- Performing Install on '" + remoteModuleName + " / " + remoteHostName + "' -----" << endl << endl;

						if ( remote_installer_debug == "1" )
							cout << "Install log file is located here: " + logfile << endl << endl;

						if ( EEPackageType != "binary" ) {
							//run remote installer script
							cmd = installDir + "/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage1 + " " + calpontPackage2 + " " + calpontPackage3 + " " + mysqlPackage + " " + mysqldPackage + " initial " + EEPackageType + " " + nodeps + " " + remote_installer_debug + " " + debug_logfile;

							if ( thread_remote_installer == "1" ) {
								thr_data[thread_id].command = cmd;

								int status = pthread_create (&thr[thread_id], NULL, (void*(*)(void*)) &remoteInstallThread, &thr_data[thread_id]);
						
								if ( status != 0 )
								{
									cout << "remoteInstallThread failed for " << remoteModuleName << ", exiting" << endl;
									exit (1);
								}
						
								thread_id++;
							}
							else
							{
								int rtnCode = system(cmd.c_str());
								if (WEXITSTATUS(rtnCode) != 0) {
									cout << endl << "Error returned from performance_installer.sh" << endl;
									exit(1);
								}
							}
						}
						else	
						{	// do a binary package install
							string binservertype = serverTypeInstall;
							if ( pmwithum )
								binservertype = "pmwithum";
							cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP +
								" " + password + " " + calpontPackage1 + " " + remoteModuleType + " initial " +
								binservertype + " " + mysqlPort + " " + remote_installer_debug + " " + installDir + " " +
								debug_logfile;

							if ( thread_remote_installer == "1" ) {
								thr_data[thread_id].command = cmd;

								int status = pthread_create (&thr[thread_id], NULL, (void*(*)(void*)) &remoteInstallThread, &thr_data[thread_id]);
						
								if ( status != 0 )
								{
									cout << "remoteInstallThread failed for " << remoteModuleName << ", exiting" << endl;
									exit (1);
								}
						
								thread_id++;
							}
							else
							{
								int rtnCode = system(cmd.c_str());
								if (WEXITSTATUS(rtnCode) != 0) {
									cout << endl << "Error returned from user_installer.sh" << endl;
									exit(1);
								}
							}
						}
					}
				}
			}

			if ( thread_remote_installer == "1" ) {
		
				//wait until remove install Thread Count is at zero or hit timeout
				cout << endl << "InfiniDB Package being installed, please wait ...";
				cout.flush();
			
				/* block until all threads complete */
				for (thread_id = 0; thread_id < (int) childmodulelist.size(); ++thread_id) {
					pthread_join(thr[thread_id], NULL);
				}
		
				cout << "  DONE" << endl;
			}

		}
	}

	//configure data redundancy
	string glusterconfig = installDir + "/bin/glusterconf";

	if (gluster && reuseConfig == "n" )
	{
		cout << endl << "===== Configuring InfiniDB Data Redundancy Functionality =====" << endl << endl;
		int ret = system(glusterconfig.c_str());
		if ( WEXITSTATUS(ret) != 0 )
		{
			cerr << endl << "There was an error in the Data Redundancy setup, exiting..." << endl;
			exit(1);
		}

		//gluster assign dbroot to pm
		DBRootList::iterator list1 = dbrootlist.begin();
		for (; list1 != dbrootlist.end() ; list1++)
		{
			string id = (*list1).dbrootID;
			string pmid = ((*list1).moduleName).substr(2,80);

			try {
				string errmsg;
				int ret = oam.glusterctl(oam::GLUSTER_ASSIGN, id, pmid, errmsg);
				if ( ret != 0 )
				{
					cerr << "FAILURE: Error assigning gluster dbroot# " + id + " to pm" + pmid + ", error: " + errmsg << endl;
					exit (1);
				}
			}
			catch (exception& e)
			{
				cout << endl << "**** glusterctl API exception:  " << e.what() << endl;
				cerr << "FAILURE: Error assigning gluster dbroot# " + id + " to pm" + pmid << endl;
				exit (1);
			}
			catch (...)
			{
				cout << endl << "**** glusterctl API exception: UNKNOWN"  << endl;
				cerr << "FAILURE: Error assigning gluster dbroot# " + id + " to pm" + pmid << endl;
				exit (1);
			}
		}
	}

	//store mysql rep enable flag
	try {
		sysConfig->setConfig(InstallSection, "MySQLRep", MySQLRep);
	}
	catch(...)
	{}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//check if local InfiniDB system logging is working
	cout << endl << "===== Checking InfiniDB System Logging Functionality =====" << endl << endl;

	if ( rootUser)
		cmd = installDir + "/bin/syslogSetup.sh install  > /dev/null 2>&1";
	else
		cmd = "sudo " + installDir + "/bin/syslogSetup.sh --installdir=" + installDir + " install  > /dev/null 2>&1";

	system(cmd.c_str());

	if ( rootUser)
		cmd = installDir + "/bin/syslogSetup.sh status  > /dev/null 2>&1";
	else
		cmd = "sudo " + installDir + "/bin/syslogSetup.sh --installdir=" + installDir + " status  > /dev/null 2>&1";

	int ret = system(cmd.c_str());
	if ( WEXITSTATUS(ret) != 0)
		cerr << "WARNING: The InfiniDB system logging not correctly setup and working" << endl;
	else
		cout << "The InfiniDB system logging is setup and working on local server" << endl;

	cout << endl << "InfiniDB System Configuration and Installation is Completed" << endl;

	//
	// startup infinidb
	//

	if ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM || 
			pmNumber > 1 ) {
		//
		// perform InfiniDB system startup
		//
		cout << endl << "===== Infinidb System Startup =====" << endl << endl;
	
		string start = "y";
		cout << "System Installation is complete. If any part of the install failed," << endl;
		cout << "the problem should be investigated and resolved before continuing." << endl << endl;
		while(true)
		{
			pcommand = callReadline("Would you like to startup the InfiniDB System? [y,n] (y) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) start = pcommand;
				callFree(pcommand);
			}
			if ( start == "y" || start == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			start = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( start == "y" ) {
	
			if (hdfs)
			{
				cout << endl << "----- Starting InfiniDB Service on all Modules -----" << endl << endl;
				string cmd = "pdsh -a '" + installDir + "/bin/infinidb restart' > /tmp/postConfigure.pdsh 2>&1";
				system(cmd.c_str());
				if (oam.checkLogStatus("/tmp/postConfigure.pdsh", "exit") ) {
					cout << endl << "ERROR: Starting InfiniDB Service failue, check /tmp/postConfigure.pdsh. exit..." << endl;
					exit (1);
				}
			}
			else
			{
				if ( password.empty() ) {
					while(true)
					{	
						char  *pass1, *pass2;
		
						if ( noPrompting ) {
							cout << "Enter your password, hit 'enter' to default to using a ssh key, or 'exit' > " << endl;
							if ( password.empty() )
								password = "ssh";
							break;
						}
		
						//check for command line option password
						if ( !password.empty() )
							break;
		
						pass1=getpass("Enter your password, hit 'enter' to default to using a ssh key, or 'exit' > ");
						if ( strcmp(pass1, "") == 0 ) {
							password = "ssh";
							break;
						}
		
						if ( strcmp(pass1, "exit") == 0 )
							exit(0);
						string p1 = pass1;
						pass2=getpass("Confirm password > ");
						string p2 = pass2;
						if ( p1 == p2 ) {
							password = p2;
							break;
						}
						else
							cout << "Password mismatch, please re-enter" << endl;
					}
		
					//add single quote for special characters
					if ( password != "ssh" )
					{
						password = "'" + password + "'";
					}
				}
		
				ChildModuleList::iterator list1 = childmodulelist.begin();
			
				for (; list1 != childmodulelist.end() ; list1++)
				{
					string remoteModuleName = (*list1).moduleName;
					string remoteModuleIP = (*list1).moduleIP;
					string remoteHostName = (*list1).hostName;
			
					//run remote command script
					cout << endl << "----- Starting InfiniDB on '" + remoteModuleName + "' -----" << endl << endl;
					cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password +
						" '" + installDir + "/bin/infinidb restart' 0";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0)
						cout << "Error with running remote_command.sh" << endl;
					else
						cout << "InfiniDB successfully started" << endl;
				}
		
				//start InfiniDB on local server
				cout << endl << "----- Starting InfiniDB on local server -----" << endl << endl;
				cmd = installDir + "/bin/infinidb restart > /dev/null 2>&1";
				int rtnCode = system(cmd.c_str());
				if (WEXITSTATUS(rtnCode) != 0) {
					cout << "Error Starting InfiniDB local module" << endl;
					cout << "Installation Failed, exiting" << endl;
					exit (1);
				}
				else
					cout << "InfiniDB successfully started" << endl;
			}
		}
		else
		{
			cout << endl << "You choose not to Start the InfiniDB Software at this time." << endl;
			exit (1);
		}
	}
	else // Single Server start
	{
		cout << endl << "===== InfiniDB System Startup =====" << endl << endl;
	
		string start = "y";
		cout << "System Installation is complete." << endl;
		cout << "If an error occurred while running the InfiniDB MySQL setup scripts," << endl;
		cout << "this will need to be corrected and postConfigure will need to be re-run." << endl << endl;
		while(true)
		{
			pcommand = callReadline("Would you like to startup the InfiniDB System? [y,n] (y) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) start = pcommand;
				callFree(pcommand);
			}
			if ( start == "y" || start == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			start = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( start == "y" ) {
			//start InfiniDB on local server
			cout << endl << "----- Starting InfiniDB on local Server '" + parentOAMModuleName + "' -----" << endl << endl;
			string cmd = installDir + "/bin/infinidb restart > /dev/null 2>&1";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) != 0) {
				cout << "Error Starting InfiniDB local module" << endl;
				cout << "Installation Failed, exiting" << endl;
				exit (1);
			}
			else
				cout << endl << "InfiniDB successfully started" << endl;
		}
		else
		{
			cout << endl << "You choose not to Start the InfiniDB Software at this time." << endl;
			exit (1);
		}
	}

	cout << endl << "InfiniDB Database Platform Starting, please wait .";
	cout.flush();
	
	if ( waitForActive() ) {
		cout << " DONE" << endl;

		if (hdfs)
			cmd = "bash -c '. " + installDir + "/bin/" + DataFileEnvFile + ";" + installDir + "/bin/dbbuilder 7 > /tmp/dbbuilder.log'";
		else
			cmd = installDir + "/bin/dbbuilder 7 > /tmp/dbbuilder.log";

		system(cmd.c_str());

		if (oam.checkLogStatus("/tmp/dbbuilder.log", "System Catalog created") )
			cout << endl << "System Catalog Successfully Created" << endl;
		else
		{
			if ( oam.checkLogStatus("/tmp/dbbuilder.log", "System catalog appears to exist") ) {

				cout << endl << "Run MySQL Upgrade.. ";
				cout.flush();

				//send message to procmon's to run upgrade script
				int status = sendUpgradeRequest(IserverTypeInstall, pmwithum);
	
				if ( status != 0 ) {
					cout << endl << "InfiniDB Install Failed" << endl << endl;
					exit(1);
				}
				else
					cout << " DONE" << endl;
			}
			else
			{
				cout << endl << "System Catalog Create Failure" << endl;
				cout << "Check latest log file in /tmp/dbbuilder.log.*" << endl;
				exit (1);
			}
		}

		//set mysql replication, if wasn't setup before on system
		if ( ( mysqlRep && pmwithum ) || 
			( mysqlRep && (umNumber > 1) ) ||
			( mysqlRep && (pmNumber > 1) && (IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) ) ) 
		{
			cout << endl << "Run MySQL Replication Setup.. ";
			cout.flush();

			//send message to procmon's to run upgrade script
			int status = sendReplicationRequest(IserverTypeInstall, password, mysqlPort);

			if ( status != 0 ) {
				cout << endl << " InfiniDB Install Failed" << endl << endl;
				exit(1);
			}
			else
				cout << " DONE" << endl;
		}

		cout << endl << "InfiniDB Install Successfully Completed, System is Active" << endl << endl;

		cout << "Enter the following command to define InfiniDB Alias Commands" << endl << endl;

		cout << ". " + installDir + "/bin/calpontAlias" << endl << endl;

		cout << "Enter 'idbmysql' to access the InfiniDB MySQL console" << endl;
		cout << "Enter 'cc' to access the InfiniDB OAM console" << endl << endl;
	}
	else
	{
		cout << " FAILED" << endl;
		cout << endl << "InfiniDB System failed to start, check log files in /var/log/Calpont" << endl;
		exit(1);
	}

	exit(0);
}

/*
 * Check for reuse of RPM saved Calpont.xml
 */
bool checkSaveConfigFile()
{
	string rpmFileName = installDir + "/etc/Calpont.xml";
	string newFileName = installDir + "/etc/Calpont.xml.new";

	string extentMapCheckOnly = " ";

	//check if Calpont.xml.rpmsave exist
	ifstream File (oldFileName.c_str());
	if (!File) {
		if ( noPrompting ) {
			cout << endl << "Old Config File not found '" +  oldFileName + "', exiting" << endl;
			exit(1);
		}
		return true;
	}
	File.close();

	// If 'oldFileName' isn't configured, exit
	Config* oldSysConfig = Config::makeConfig(oldFileName);

	string oldpm1 = oldSysConfig->getConfig("SystemModuleConfig", "ModuleIPAddr1-1-3");
	if ( oldpm1 == "0.0.0.0") {
		if ( noPrompting ) {
			cout << endl << "Old Config File not Configured, PM1 IP Address entry is '0.0.0.0', '" +  oldFileName + "', exiting" << endl;
			exit(1);
		}
		else
			return true;
	}

	// get single-server system install type
	string temp;
	try {
		temp = oldSysConfig->getConfig(InstallSection, "SingleServerInstall");
	}
	catch(...)
	{}

	if ( !temp.empty() )
		singleServerInstall = temp;

	if ( singleServerInstall == "y" )
		singleServerInstall = "1";
	else
		singleServerInstall = "2";

	if ( !noPrompting ) {
		cout << endl << "A copy of the InfiniDB Configuration file has been saved during Package install." << endl;
		if ( singleServerInstall == "1")
			cout << "It's Configured for a Single Server Install." << endl; 
		else
			cout << "It's Configured for a Multi-Server Install." << endl; 
	
		cout << "You have an option of utilizing the configuration data from that file or starting" << endl;
		cout << "with the InfiniDB Configuration File that comes with the InfiniDB Package." << endl;
		cout << "You will only want to utilize the old configuration data when performing the same" << endl;
		cout << "type of install, i.e. Single or Multi-Server" << endl;
	}
	else
	{
		cout << "The Calpont Configuration Data is taken from " << oldFileName << endl;
	}

	cout << endl;
	while(true)
	{
		pcommand = callReadline("Do you want to utilize the configuration data from the saved copy? [y,n]  > ");
		if (pcommand)
		{
			if (strlen(pcommand) > 0)
			{
				reuseConfig = pcommand;
			}
			else
			{
				if ( noPrompting )
					reuseConfig = "y";
				else
				{
					cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
					if ( noPrompting )
						exit(1);
					continue;
				}
			}

			callFree(pcommand);
		}

		string cmd;
		if ( reuseConfig == "y" ) {
			if ( singleServerInstall == "1") {
				cmd = "rm -f " + installDir + "/etc/Calpont.xml.installSave  > /dev/null 2>&1";
				system(cmd.c_str());
				cmd = "mv -f " + installDir + "/etc/Calpont.xml " + installDir + "/etc/Calpont.xml.installSave  > /dev/null 2>&1";
				system(cmd.c_str());
				cmd = "/bin/cp -f " + installDir + "/etc/Calpont.xml.singleserver " + installDir + "/etc/Calpont.xml  > /dev/null 2>&1";
				system(cmd.c_str());
			}
			break;
		}

		if ( reuseConfig == "n" ) {
			extentMapCheckOnly = "-e";
			break;
		}
		else
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
	}

	// clear this entry out to validate updates being made
	Config* sysConfig = Config::makeConfig();
	sysConfig->setConfig("ExeMgr1", "IPAddr", "0.0.0.0");

	for ( int retry = 0 ; retry < 5 ; retry++ )
	{
		string cmd = "mv -f " + rpmFileName + " " + newFileName;
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "Error moving installed version of Calpont.xml" << endl;
			return false;
		}
	
		cmd = "cp " + oldFileName + " " + rpmFileName;
		rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "Error moving pkgsave file" << endl;
			return false;
		}
	
		cmd = "cd " + installDir + "/etc/;../bin/autoConfigure " + extentMapCheckOnly;
		rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "Error running autoConfigure" << endl;
			return false;
		}
	
		cmd = "mv -f " + newFileName + " " + rpmFileName;
		rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "Error moving pkgsave file" << endl;
			return false;
		}

		//check to see if updates were made	
		if ( sysConfig->getConfig("ExeMgr1", "IPAddr") != "0.0.0.0") {
			//Calpont.xml is ready to go, get feature options

			if ( MySQLRep == "n" )
			{
				try {
					MySQLRep = sysConfig->getConfig(InstallSection, "MySQLRep");
				}
				catch(...)
				{}
	
				if ( MySQLRep == "y" )
					mysqlRep = true;
			}

			if ( PMwithUM == "n" ) {
				//get local query / PMwithUM feature flag
				try {
					PMwithUM = sysConfig->getConfig(InstallSection, "PMwithUM");
				}
				catch(...)
				{}
	
				if ( PMwithUM == "y" ) {
					pmwithum = true;
				}
			}
			return true;
		}

		sleep(1);
	}

	if ( reuseConfig == "n" )
		return true;

	cout << "ERROR: Failed to copy data to Calpont.xml" << endl;
	return false;

}

/*
 * Setup OS Files by appending the Calpont versions
 */

// /etc OS Files to be updated
string files[] = {
	"rc.local",
	" "
};

bool setOSFiles(string parentOAMModuleName, int serverTypeInstall)
{
	bool allfound = true;

	//update /etc files
	for ( int i=0;;++i)
	{
		if ( files[i] == " ")
			//end of list
			break;

		//create or update date on file to make sure on exist
		if ( files[i] == "rc.local") {
			string cmd = "touch " + installDir + "/local/etc/" + parentOAMModuleName + "/rc.local.calpont > /dev/null 2>&1"; 
			if ( !rootUser )
				cmd = "sudo touch " + installDir + "/local/etc/" + parentOAMModuleName + "/rc.local.calpont > /dev/null 2>&1"; 
			system(cmd.c_str());
		}

		string fileName = "/etc/" + files[i];

		//make a backup copy before changing
		string cmd = "rm -f " + fileName + ".calpontSave";
		if ( !rootUser )
			cmd = "sudo rm -f " + fileName + ".calpontSave";

		system(cmd.c_str());

		cmd = "cp " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
		if ( !rootUser )
			cmd = "sudo cp " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";

		system(cmd.c_str());

		if ( rootUser )
			cmd = "cat " + installDir + "/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont >> " + fileName; 
		else
			cmd = "sudo bash -c 'sudo cat " + installDir + "/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont >> " + fileName + "'"; 

		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0)
			cout << "Error Updating " + files[i] << endl;

		cmd = "rm -f " + installDir + "/local/ " + files[i] + "*.calpont > /dev/null 2>&1";
		system(cmd.c_str());

		cmd = "cp " + installDir + "/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont " + installDir + "/local/. > /dev/null 2>&1"; 
		system(cmd.c_str());
	}

	return allfound;
}

/*
 * Updated snmpdx.conf with parentOAMModuleIPAddr
 */
bool updateSNMPD(string parentOAMModuleIPAddr)
{
	string fileName = installDir + "/etc/snmpd.conf";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;
	string newLine1;
	while (oldFile.getline(line, 200))
	{
		buf = line;
		string::size_type pos = buf.find("parentOAMIPStub",0);
		if (pos != string::npos)
		{
			newLine = buf.substr(0, pos);
    	    newLine.append(parentOAMModuleIPAddr);

			newLine1 = buf.substr(pos+15, 200);
			newLine.append(newLine1);

			buf = newLine;
		}
		//output to temp file
		lines.push_back(buf);
	}
	
	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);
	return true;
}


/*
 * Update ProcessConfig.xml file for a single server configuration
 * Change the 'um' and 'pm' to 'dm'
 */
bool updateProcessConfig(int serverTypeInstall)
{
	vector <string> oldModule;
	string newModule;

	switch ( serverTypeInstall ) {
		case (oam::INSTALL_COMBINE_DM_UM_PM):
		{
			newModule = ">pm";
			oldModule.push_back(">um");
			oldModule.push_back(">dm");
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM):
		{
			newModule = ">um";
			oldModule.push_back(">dm");
			break;
		}
		case (oam::INSTALL_COMBINE_PM_UM):
		{
			newModule = ">pm";
			oldModule.push_back(">um");
			break;
		}
	}

	string fileName = installDir + "/etc/ProcessConfig.xml";

	//Save a copy of the original version
	string cmd = "/bin/cp -f " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
	system(cmd.c_str());

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;
	string newLine1;

	while (oldFile.getline(line, 200))
	{
		buf = line;
		newLine = line;

		std::vector<std::string>::iterator pt1 = oldModule.begin();
		for( ; pt1 != oldModule.end() ; pt1++)
		{
			int start = 0;
			while(true)
			{
				string::size_type pos = buf.find(*pt1,start);
				if (pos != string::npos)
				{
					newLine = buf.substr(0, pos);
					newLine.append(newModule);
		
					newLine1 = buf.substr(pos+3, 200);
					newLine.append(newLine1);
					start = pos+3;
				}
				else
				{
					buf = newLine;
					start = 0;
					break;
				}
			}
		}
		//output to temp file
		lines.push_back(buf);
	}

	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);
	return true;
}

/*
 * Uncomment entry in Calpont.xml
 */
bool uncommentCalpontXml( string entry)
{
	string fileName = installDir + "/etc/Calpont.xml";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;

	string firstComment = "<!--";
	string lastComment = "--> ";

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find(entry,0);
		if (pos != string::npos)
		{
			pos = buf.find(firstComment,0);
			if (pos == string::npos)
			{
				return true;
			}
			else
			{
				buf = buf.substr(pos+4,80);

				pos = buf.find(lastComment,0);
				if (pos == string::npos)
				{
					return true;
				}
				else
				{
					buf = buf.substr(0,pos);
				}
			}
		}
		//output to temp file
		lines.push_back(buf);
	}

	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);
	return true;
}

/*
 * Make makeRClocal to set mount scheduler
 */
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall)
{
	string fileName = installDir + "/local/etc/" + moduleName + "/rc.local.calpont";

	vector <string> lines;

	string mount1;
	string mount2
;
	switch ( IserverTypeInstall ) {
		case (oam::INSTALL_NORMAL):	// normal
		{
			if ( moduleType == "um" )
				mount1 = "/mnt\\/tmp/";
			else
				if ( moduleType == "pm" )
					mount1 = "/Calpont\\/data/";
				else
					return true;
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm
		{
			if ( moduleType == "pm" ) {
				mount1 = "/mnt\\/tmp/";
				mount2 = "/Calpont\\/data/";
			}
			else
				return true;
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM):	// combined #2 dm/um on a same server
		{
			if ( moduleType == "um" )
				mount1 = "/mnt\\/tmp/";
			else
				if ( moduleType == "pm" )
					mount1 = "/Calpont\\/data/";
				else
					return true;
			break;
		}
		case (oam::INSTALL_COMBINE_PM_UM):	// combined #3 um/pm on a same server
		{
			if ( moduleType == "pm" ) {
				mount1 = "/mnt\\/tmp/";
				mount2 = "/Calpont\\/data/";
			}
			else
				return true;
			break;
		}
	}

	if ( !mount1.empty() ) {
		string line1 = "for scsi_dev in `mount | awk '" + mount1 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
		string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
		string line3 = "done";
	
		lines.push_back(line1);
		lines.push_back(line2);
		lines.push_back(line3);
	}
	else
	{
		if ( !mount2.empty() ) {
			string line1 = "for scsi_dev in `mount | awk '" + mount2 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
			string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
			string line3 = "done";
		
			lines.push_back(line1);
			lines.push_back(line2);
			lines.push_back(line3);
		}
	}

	unlink (fileName.c_str());

	if ( lines.begin() == lines.end()) {
		string cmd = "touch " + fileName;
		system(cmd.c_str());
		return true;
	}

   	ofstream newFile (fileName.c_str());	

	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/*
 * createDbrootDirs 
 */
bool createDbrootDirs(string DBRootStorageType)
{
	int rtnCode;
	string cmd;

	// mount data1 and create directories if configured with storage
	if ( DBRootStorageType == "external" ) {
		string cmd = "mount " + installDir + "/data1 > /tmp/mount.txt 2>&1";
		system(cmd.c_str());

		if ( !rootUser) {
			cmd = "sudo chown -R " + USER + ":" + USER + " " + installDir + "/data1 > /dev/null";
			system(cmd.c_str());
		}

		// create system file directories
		cmd = "mkdir -p " + installDir + "/data1/systemFiles/dbrm > /dev/null 2>&1";
		rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << endl << "Error: failed to make mount dbrm dir" << endl;
			return false;
		}
	
		cmd = "mkdir -p " + installDir + "/data1/systemFiles/dataTransaction/archive > /dev/null 2>&1";
		rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << endl << "Error: failed to make mount dataTransaction dir" << endl;
			return false;
		}
	}

	cmd = "chmod 1777 -R " + installDir + "/data1/systemFiles/dbrm > /dev/null 2>&1";
	system(cmd.c_str());

	return true;
}

/*
 * pkgCheck 
 */
bool pkgCheck()
{
	string home = "/root";
	if (!rootUser) {
		char* p= getenv("HOME");
		if (p && *p)
			home = p;
	}

	while(true) 
	{
		string cmd = "ls " + home + " | grep " + calpontPackage1 + " > /tmp/calpontpkgs";
		system(cmd.c_str());
	
		cmd = "ls " + home + " | grep " + mysqlPackage + " > /tmp/mysqlpkgs";
		system(cmd.c_str());
	
		cmd = "ls " + home + " | grep " + mysqldPackage + " > /tmp/mysqldpkgs";
		system(cmd.c_str());

		string pkg = calpontPackage1;
		string fileName = "/tmp/calpontpkgs";
		ifstream oldFile (fileName.c_str());
		if (oldFile) {
			oldFile.seekg(0, std::ios::end);
			int size = oldFile.tellg();
			if ( size != 0 ) {
				oldFile.close();
				unlink (fileName.c_str());
	
				pkg = mysqlPackage;
				fileName = "/tmp/mysqlpkgs";
				ifstream oldFile1 (fileName.c_str());
				if (oldFile1) {
					oldFile1.seekg(0, std::ios::end);
					size = oldFile1.tellg();
					if ( size != 0 ) {
						oldFile1.close();
						unlink (fileName.c_str());
		
						pkg = mysqldPackage;
						fileName = "/tmp/mysqldpkgs";
						ifstream oldFile2 (fileName.c_str());
						if (oldFile2) {
							oldFile2.seekg(0, std::ios::end);
							size = oldFile2.tellg();
							if ( size != 0 ) {
								oldFile2.close();
								unlink (fileName.c_str());
								// all 3 pkgs found
								return true;
							}
						}
					}
				}
			}
		}
	
		cout << endl << " Error: can't locate " + pkg + " Package in directory " + home << endl << endl;
		if ( noPrompting )
			exit(1);
	
		while(true)
		{
			pcommand = callReadline("Please place a copy of the InfiniDB Packages in directory " + home + " and press <enter> to continue or enter 'exit' to exit the install > ");
			if (pcommand) {
				if (strcmp(pcommand, "exit") == 0)
				{
					callFree(pcommand);
					return false;
				}
				if (strlen(pcommand) == 0)
				{
					callFree(pcommand);
					break;
				}
				callFree(pcommand);
				cout << "Invalid entry, please re-enter" << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}
			break;
		}
	}

	return true;
}

bool storageSetup(string cloud)
{
	Oam oam;

	try {
		DBRootStorageType = sysConfig->getConfig(InstallSection, "DBRootStorageType");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting DB Storage Data from the InfiniDB System Configuration file" << endl;
		return false;
	}

	if ( DBRootStorageType == "hdfs")
		hdfs = true;

	if ( reuseConfig == "y" ) {
		cout << "===== Storage Configuration = " + DBRootStorageType + " =====" << endl << endl;

		if (hdfs)
		{
			//default
			DataFileEnvFile = "setenv-hdfs-20";
			try {
				DataFileEnvFile = sysConfig->getConfig("SystemConfig", "DataFileEnvFile");
			}
			catch(...)
			{
				DataFileEnvFile = "setenv-hdfs-20";
			}

			string DataFilePlugin = installDir + "/lib/hdfs-20.so";
			try {
				DataFilePlugin = sysConfig->getConfig("SystemConfig", "DataFilePlugin");
			}
			catch(...)
			{
				DataFilePlugin = installDir + "/lib/hdfs-20.so";
			}

			while(true)
			{
				cout << " Running HDFS Sanity Test (please wait):    ";
				cout.flush();
				string logdir("/var/log/Calpont");
				if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
				string hdfslog = logdir + "/hdfsCheck.log1";

				string cmd = ". " + installDir + "/bin/" + DataFileEnvFile + ";" + installDir + "/bin/hdfsCheck " + DataFilePlugin +  " > " + hdfslog + " 2>&1";
				system(cmd.c_str());
				if (oam.checkLogStatus(hdfslog, "All HDFS checks passed!")) 
				{
					cout << "  PASSED" << endl;

					try {
						sysConfig->setConfig("SystemConfig", "DataFilePlugin", DataFilePlugin);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting DataFilePlugin in the InfiniDB System Configuration file" << endl;
						return false;
					}
			
					try {
						sysConfig->setConfig("SystemConfig", "DataFileEnvFile", DataFileEnvFile);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting DataFileEnvFile in the InfiniDB System Configuration file" << endl;
						return false;
					}

					if ( !writeConfig(sysConfig) ) {
						cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
						return false;
					}

					return true;
				}
				else
				{
					cout << "  FAILED (Tested with Hadoop Datafile Plugin File (" + DataFilePlugin + "), please re-enter or enter 'exit' to Investigate)" << endl << endl;

					if ( noPrompting )
						exit(1);

					prompt = "Enter the Hadoop Datafile Plugin File (" + DataFilePlugin + ") > ";
					pcommand = callReadline(prompt.c_str());
					if (pcommand)
					{
						if (strlen(pcommand) > 0) DataFilePlugin = pcommand;
						callFree(pcommand);
					}
		
					if ( DataFilePlugin == "exit" )
						exit (1);
		
					if ( DataFilePlugin != installDir + "/lib/hdfs-20.so" )
						DataFileEnvFile = "setenv-hdfs-12";
					else
						DataFileEnvFile = "setenv-hdfs-20";
				}
			}
		}

		return true;
	}

	cout << "===== Setup Storage Configuration =====" << endl << endl;
	
	string storageType;

	if( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM && cloud == "amazon" )
	{
		//
		// get Frontend Data storage type
		//
	
		cout << "----- Setup High Availability Frontend MySQL Data Storage Mount Configuration -----" << endl << endl;
	
		cout << "There are 2 options when configuring the storage: internal and external" << endl << endl;
		cout << "  'internal' -    This is specified when a local disk is used for the MySQL Data storage" << endl;
		cout << "                  or the MySQL Data storage directories are manually mounted externally" << endl;
		cout << "                  but no High Availability Support is required" << endl << endl; 
		cout << "  'external' -    This is specified when the MySQL Data directory is externally mounted and" << endl;
		cout << "                  High Availability Failover Support is required." << endl << endl;
	
		try {
			UMStorageType = sysConfig->getConfig(InstallSection, "UMStorageType");
		}
		catch(...)
		{
			cout << "ERROR: Problem getting UM DB Storage Data from the InfiniDB System Configuration file" << endl;
			return false;
		}
	
		while(true)
		{
			storageType = "1";
			if ( UMStorageType == "external" )
				storageType = "2";
	
			prompt = "Select the type of Data Storage [1=internal, 2=external] (" + storageType + ") > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) storageType = pcommand;
				callFree(pcommand);
			}
			if ( storageType == "1" || storageType == "2")
				break;
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( storageType == "1" )
			UMStorageType = "internal";
		else
			UMStorageType = "external";
	
		try {
			sysConfig->setConfig(InstallSection, "UMStorageType", UMStorageType);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting UMStorageType in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}
	else
	{
		try {
			sysConfig->setConfig(InstallSection, "UMStorageType", "internal");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting UMStorageType in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}

	//check if gluster is installed
	if (rootUser)
		system("which gluster > /tmp/gluster.log 2>&1");
	else
		system("sudo which gluster > /tmp/gluster.log 2>&1");

	ifstream in("/tmp/gluster.log");

	in.seekg(0, std::ios::end);
	int size = in.tellg();
	if ( size == 0 || oam.checkLogStatus("/tmp/gluster.log", "no gluster")) 
	// no gluster
		size=0;
	else
		glusterInstalled = "y";

	//check if hadoop is installed
	system("which hadoop > /tmp/hadoop.log 2>&1");

	ifstream in1("/tmp/hadoop.log");

	in1.seekg(0, std::ios::end);
	size = in1.tellg();
	if ( size == 0 || oam.checkLogStatus("/tmp/hadoop.log", "no hadoop")) 
	// no hadoop
		size=0;
	else
		hadoopInstalled = "y";

	//
	// get Backend Data storage type
	//

	// default to internal
	storageType = "1";
	if ( DBRootStorageType == "external" )
		storageType = "2";
	if ( DBRootStorageType == "gluster" )
		storageType = "3";
	if ( DBRootStorageType == "hdfs" )
		storageType = "4";

	cout << "----- Setup High Availability Data Storage Mount Configuration -----" << endl << endl;

	if ( glusterInstalled == "n" && hadoopInstalled == "n" )
	{
		cout << "There are 2 options when configuring the storage: internal or external" << endl << endl;
		prompt = "Select the type of Data Storage [1=internal, 2=external] (" + storageType + ") > ";
	}

	if ( glusterInstalled == "y" && hadoopInstalled == "n" )
	{
		cout << "There are 3 options when configuring the storage: internal, external, or gluster" << endl << endl;
		prompt = "Select the type of Data Storage [1=internal, 2=external, 3=gluster] (" + storageType + ") > ";
	}

	if ( glusterInstalled == "n" && hadoopInstalled == "y" )
	{
		cout << "There are 3 options when configuring the storage: internal, external, or hdfs" << endl << endl;
		prompt = "Select the type of Data Storage [1=internal, 2=external, 4=hdfs] (" + storageType + ") > ";
	}

	if ( glusterInstalled == "y" && hadoopInstalled == "y" )
	{
		cout << "There are 5 options when configuring the storage: internal, external, gluster, or hdfs" << endl << endl;
		prompt = "Select the type of Data Storage [1=internal, 2=external, 3=gluster, 4=hdfs] (" + storageType + ") > ";
	}

	cout << "  'internal' -    This is specified when a local disk is used for the dbroot storage" << endl;
	cout << "                  or the dbroot storage directories are manually mounted externally" << endl;
	cout << "                  but no High Availability Support is required." << endl << endl; 
	cout << "  'external' -    This is specified when the dbroot directories are externally mounted" << endl;
	cout << "                  and High Availability Failover Support is required." << endl << endl;

	if ( glusterInstalled == "y" )
	{
		cout << "  'gluster' -     This is specified when glusterfs is installed and you want the dbroot" << endl;
		cout << "                  directories to be controlled by glusterfs." << endl << endl;
	}

	if ( hadoopInstalled == "y" )
	{
		cout << "  'hdfs' -        This is specified when hadoop is installed and you want the dbroot" << endl;
 		cout << "                  directories to be controlled by the Hadoop Distributed File System (HDFS)." << endl << endl;
	}

	while(true)
	{
		pcommand = callReadline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) storageType = pcommand;
			callFree(pcommand);
		}

		if ( glusterInstalled == "n" && hadoopInstalled == "n" )
		{
			if ( storageType == "1" || storageType == "2")
				break;
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( glusterInstalled == "y" && hadoopInstalled == "n" )
		{
			if ( storageType == "1" || storageType == "2" || storageType == "3")
				break;
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( glusterInstalled == "n" && hadoopInstalled == "y" )
		{
			if ( storageType == "1" || storageType == "2" || storageType == "4") {
				break;
			}
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}

		if ( glusterInstalled == "y" && hadoopInstalled == "y" )
		{
			if ( storageType == "1" || storageType == "2" || storageType == "3" || storageType == "4")
				break;
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}
	}

	switch ( atoi(storageType.c_str()) ) {
		case (1):
		{
			DBRootStorageType = "internal";
			break;
		}
		case (2):
		{
			DBRootStorageType = "external";
			break;
		}
		case (3):
		{
			DBRootStorageType = "gluster";
			break;
		}
		case (4):
		{
			DBRootStorageType = "hdfs";
			break;
		}
	}

	//set DBRootStorageType
	try {
		sysConfig->setConfig(InstallSection, "DBRootStorageType", DBRootStorageType);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRootStorageType in the InfiniDB System Configuration file" << endl;
		return false;
	}

	if( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM )
	{
		try {
			sysConfig->setConfig(InstallSection, "UMStorageType", DBRootStorageType);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting UMStorageType in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}

	// if gluster
	if ( storageType == "3" )
	{
		gluster = true;
		sysConfig->setConfig(InstallSection, "GlusterConfig", "y");
		sysConfig->setConfig("PrimitiveServers", "DirectIO", "n");
	}
	else
	{
		gluster = false;
		sysConfig->setConfig(InstallSection, "GlusterConfig", "n");
		sysConfig->setConfig("PrimitiveServers", "DirectIO", "y");
	}

	// if hadoop / hdfs
	if ( storageType == "4" )
	{
		hdfs = true;
		string DataFilePlugin = installDir + "/lib/hdfs-20.so";
		try {
			DataFilePlugin = sysConfig->getConfig("SystemConfig", "DataFilePlugin");
		}
		catch(...)
		{
			DataFilePlugin = installDir + "/lib/hdfs-20.so";
		}

		if (DataFilePlugin.empty() || DataFilePlugin == "")
			DataFilePlugin = installDir + "/lib/hdfs-20.so";
	
		DataFileEnvFile = "setenv-hdfs-20";
		try {
			DataFileEnvFile = sysConfig->getConfig("SystemConfig", "DataFileEnvFile");
		}
		catch(...)
		{
			DataFileEnvFile = "setenv-hdfs-20";
		}

		if (DataFileEnvFile.empty() || DataFileEnvFile == "")
			DataFileEnvFile = "setenv-hdfs-20";

		cout << endl;
		while(true)
		{
			prompt = "Enter the Hadoop Datafile Plugin File (" + DataFilePlugin + ") > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) DataFilePlugin = pcommand;
				callFree(pcommand);
			}

			if ( DataFilePlugin == "exit" )
				exit (1);

			if ( DataFilePlugin != installDir + "/lib/hdfs-20.so" )
				DataFileEnvFile = "setenv-hdfs-12";

			ifstream File (DataFilePlugin.c_str());
			if (!File)
				cout << "Error: Hadoop Datafile Plugin File (" + DataFilePlugin + ") doesn't exist, please re-enter" << endl;
			else
			{
				cout << endl << " Running HDFS Sanity Test (please wait):    ";
				cout.flush();
				string logdir("/var/log/Calpont");
				if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
				string hdfslog = logdir + "/hdfsCheck.log1";

				string cmd = installDir + "/bin/hdfsCheck " + DataFilePlugin +  " > " + hdfslog + " 2>&1";
				system(cmd.c_str());
				if (oam.checkLogStatus(hdfslog, "All HDFS checks passed!")) {
					cout << "  PASSED" << endl;
					break;
				}
				else
				{
					cout << "  FAILED (Tested with Hadoop Datafile Plugin File (" + DataFilePlugin + "), please re-enter or enter 'exit' to Investigate)" << endl<< endl;
				}
			}
		}

		try {
			sysConfig->setConfig("SystemConfig", "DataFilePlugin", DataFilePlugin);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DataFilePlugin in the InfiniDB System Configuration file" << endl;
			return false;
		}

		try {
			sysConfig->setConfig("SystemConfig", "DataFileEnvFile", DataFileEnvFile);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DataFileEnvFile in the InfiniDB System Configuration file" << endl;
			return false;
		}

		try {
			sysConfig->setConfig("SystemConfig", "DataFileLog", "OFF");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DataFileLog in the InfiniDB System Configuration file" << endl;
			return false;
		}

		try {
			sysConfig->setConfig("ExtentMap", "ExtentsPerSegmentFile", "1");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting ExtentsPerSegmentFile in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}
	else
	{
		hdfs = false;

		try {
			sysConfig->setConfig("SystemConfig", "DataFilePlugin", "");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DataFilePlugin in the InfiniDB System Configuration file" << endl;
			return false;
		}

		try {
			sysConfig->setConfig("SystemConfig", "DataFileEnvFile", "");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DataFileEnvFile in the InfiniDB System Configuration file" << endl;
			return false;
		}

		if ( !writeConfig(sysConfig) ) {
			cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
			return false;
		}
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		return false;
	}

	return true;
}

void snmpAppCheck()
{
	Oam oam;

	cout << endl << "===== InfiniDB SNMP-Trap Process Check  =====" << endl << endl;
	cout << "InfiniDB is packaged with an SNMP-Trap process." << endl;
	cout << "If the system where InfiniDB is being installed already has an SNMP-Trap process" << endl;
	cout << "running, then you have the option of disabling InfiniDB's SNMP-Trap process." << endl;
	cout << "Not having the InfiniDB SNMP-Trap process will affect the" << endl;
	cout << "generation of InfiniDB alarms and associated SNMP traps." << endl;
	cout << "Please reference the Calpont InfiniDB Installation Guide for" << endl;
	cout << "additional information." << endl << endl;

	string enableSNMP = "y";
	if (geteuid() == 0)
		enableSNMP = sysConfig->getConfig(InstallSection, "EnableSNMP");
	else
		enableSNMP = "n";

	if (enableSNMP.empty())
		enableSNMP = "y";

	while(true)
	{
		if ( enableSNMP == "y" ) {
			string disable = "n";
			pcommand = callReadline("InfiniDB SNMP-Trap process is enabled, would you like to disable it [y,n] (n) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) disable = pcommand;
				callFree(pcommand);
			}

			if ( disable == "y" ) {
				enableSNMP = "n";
				break;
			}
			else if ( disable == "n" ) {
				enableSNMP = "y";
				break;
			}
	
			cout << "Invalid Entry, please retry" << endl;
			if ( noPrompting )
				exit(1);
		}
		else
		{
			string enable = "n";
			pcommand = callReadline("InfiniDB SNMP-Trap process is disabled, would you like to enable it (y,n) [n] > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) enable = pcommand;
				callFree(pcommand);
			}

			if ( enable == "y" || enable == "n" ) {
				enableSNMP = enable;
				break;
			}

			cout << "Invalid Entry, please retry" << endl;
			if ( noPrompting )
				exit(1);
		}
	}

	sysConfig->setConfig(InstallSection, "EnableSNMP", enableSNMP);

	if (enableSNMP == "y") {
		//
		// Configure SNMP / NMS Addresses
		//
		try
		{
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "BootLaunch", "1");
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "LaunchID", "3");

			cout << endl << "InfiniDB SNMP Process successfully enabled" << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
		}

		//set OAM Parent IP Address in snmpd.conf
		if( !updateSNMPD(parentOAMModuleIPAddr) )
			cout << "updateSNMPD error" << endl;
	
		//get and set NMS IP address
		string currentNMSIPAddress;
		string NMSIPAddress;
		SNMPManager sm;
		sm.getNMSAddr(currentNMSIPAddress);
	
		NMSIPAddress = currentNMSIPAddress;

		cout << endl << "===== Setup the Network Management System (NMS) Server Configuration =====" << endl << endl;

		cout << "This would be used to receive SNMP Traps from InfiniDB, like a Network Control Center" << endl;
		prompt = "Enter IP Address(es) of NMS Server (" + currentNMSIPAddress + ") > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) NMSIPAddress = pcommand;
			callFree(pcommand);
		}
	
		sm.setNMSAddr(NMSIPAddress);
	}
	else
	{	//disabled, update config file
		try
		{
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "BootLaunch", "0");
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "LaunchID", "0");

			cout << endl << "InfiniDB SNMP-Trap Process successfully disabled" << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
		}
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	return;
}


void setSystemName()
{
	//setup System Name
	string systemName;
	try {
		systemName = sysConfig->getConfig(SystemSection, "SystemName");
	}
	catch(...)
	{
		systemName = oam::UnassignedName;
	}

	if ( systemName.empty() )
		systemName = oam::UnassignedName;

	prompt = "Enter System Name (" + systemName + ") > ";
	pcommand = callReadline(prompt.c_str());
	if (pcommand)
	{
		if (strlen(pcommand) > 0) systemName = pcommand;
		callFree(pcommand);
	}

	try {
		 sysConfig->setConfig(SystemSection, "SystemName", systemName);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting SystemName from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}
}

/*
 * Copy fstab file
 */
bool copyFstab(string moduleName)
{
	string cmd = "/bin/cp -f /etc/fstab " + installDir + "/local/etc/" + moduleName + "/. > /dev/null 2>&1";
	system(cmd.c_str());

	return true;
}

/*
 * Copy x.509 file
 */
bool copyX509files()
{
	string cmd = "/bin/cp -f " + x509Cert + " " + installDir + "/local/etc/. > /dev/null 2>&1";
	system(cmd.c_str());

	cmd = "/bin/cp -f " + x509PriKey + " " + installDir + "/local/etc/. > /dev/null 2>&1";
	system(cmd.c_str());

	return true;
}


/*
 * Create a module file
 */
bool makeModuleFile(string moduleName, string parentOAMModuleName)
{
	string fileName;
	if ( moduleName == parentOAMModuleName)
		fileName = installDir + "/local/module";
	else
		fileName = installDir + "/local/etc/" + moduleName + "/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	string cmd = "echo " + moduleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

	return true;
}

/*
 * Create a module file
 */
bool updateBash()
{
	string fileName = home + "/.bashrc";

   	ifstream newFile (fileName.c_str());

	if (!rootUser)
	{
		string cmd = "echo export INFINIDB_INSTALL_DIR=" + installDir + " >> " + fileName;
		system(cmd.c_str());
	
		cmd = "echo export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INFINIDB_INSTALL_DIR/lib:$INFINIDB_INSTALL_DIR/mysql/lib/mysql >> " + fileName;
		system(cmd.c_str());
	}

	if ( hdfs ) 
	{	
		string cmd = "echo . " + installDir + "/bin/" + DataFileEnvFile + " >> " + fileName;
		system(cmd.c_str());

		if ( rootUser)
			cmd = "su - hdfs -c 'hadoop fs -mkdir -p " + installDir + ";hadoop fs -chown root:root " + installDir + "' >/dev/null 2>&1";
		else
			cmd = "sudo su - hdfs -c 'hadoop fs -mkdir -p " + installDir + ";hadoop fs -chown " + USER + ":" + USER + " " + installDir + "' >/dev/null 2>&1";

		system(cmd.c_str());
	}

	newFile.close();

	return true;

}

void offLineAppCheck()
{
	//check for system startup type, process offline or online option
	cout << endl << "===== Setup Process Startup offline Configuration =====" << endl << endl;

	string systemStartupOffline;
	try {
		systemStartupOffline = sysConfig->getConfig(InstallSection, "SystemStartupOffline");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting systemStartupOffline from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	cout << endl;
	string temp = "y";
	while(true)
	{
		prompt = "Do you want the Database Processes started automatically at system startup [y,n] (y) > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) temp = pcommand;
			callFree(pcommand);
			if ( temp == "n" || temp == "y") {
				break;
			}
			cout << "Invalid Option, please re-enter" << endl;
			if ( noPrompting )
				exit(1);
		}
		else
			break;
	}

	if ( temp == "y" )
		systemStartupOffline = "n";
	else
		systemStartupOffline = "y";

	try {
		sysConfig->setConfig(InstallSection, "SystemStartupOffline", systemStartupOffline);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting systemStartupOffline in the InfiniDB System Configuration file" << endl;
		exit(1);
	}
}

bool attachVolume(string instanceName, string volumeName, string deviceName, string dbrootPath)
{
	Oam oam;

	//just return of debug set, called from amazonInstaller
	if( thread_remote_installer == "1")
		return true;

	cout << "Checking if Volume " << volumeName << " is attached , please wait..." << endl;	

	string status = oam.getEC2VolumeStatus(volumeName);
	if ( status == "attached" ) {
		cout << "Volume " << volumeName << " is attached " << endl;
		cout << "Make sure it's device " << deviceName << " is mounted to dbroot directory " << dbrootPath << endl;
		return true;
	}

	if ( status != "available" ) {
		cout << "ERROR: Volume " << volumeName << " status is " << status << endl;
		cout << "Please resolve and re-run postConfigure" << endl;
		return false;
	}
	else
	{
		cout << endl;
		string temp = "y";
		while(true)
		{
			prompt = "Volume is unattached and available, do you want to attach it? [y,n] (y) > ";
			pcommand = callReadline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) temp = pcommand;
				callFree(pcommand);
				if ( temp == "n" || temp == "y") {
					break;
				}
				cout << "Invalid Option, please re-enter" << endl;
				if ( noPrompting )
					exit(1);
			}
			else
				break;
		}
	
		if ( temp == "y" ) {
			cout << "Attaching, please wait..." << endl;
			if(oam.attachEC2Volume(volumeName, deviceName, instanceName)) {
				cout << "Volume " << volumeName << " is now attached " << endl;
				cout << "Make sure it's device " << deviceName << " is mounted to dbroot directory " << dbrootPath << endl;
				return true;
			}
			else
			{
				cout << "ERROR: Volume " << volumeName << " failed to attach" << endl;
				cout << "Please resolve and re-run postConfigure" << endl;
				return false;
			}
		}
		else
		{
			cout << "Volume " << volumeName << " will need to be attached before completing the install" << endl;
			cout << "Please resolve and re-run postConfigure" << endl;
			return false;
		}
	}
}

bool singleServerDBrootSetup()
{
	Oam oam;

	cout << endl;

	//get number of dbroots
	string moduledbrootcount = "ModuleDBRootCount1-3";
	unsigned int count;
	try {
		count = atoi(sysConfig->getConfig(ModuleSection, moduledbrootcount).c_str());
	}
	catch(...)
	{
		cout << "ERROR: Problem setting dbroot count in the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	string dbrootList;

	for ( unsigned int id = 1 ; id < count+1 ;  )
	{
		string moduledbrootid = "ModuleDBRootID1-" + oam.itoa(id) + "-3";
		try {
			string dbrootid = sysConfig->getConfig(ModuleSection, moduledbrootid);

			if ( dbrootid != oam::UnassignedName) {
				sysConfig->setConfig(ModuleSection, moduledbrootid, oam::UnassignedName);

				dbrootList = dbrootList + dbrootid;
				id ++;
				if ( id < count+1 )
					dbrootList = dbrootList + ",";
			}
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot ID in the InfiniDB System Configuration file" << endl;
			exit(1);
		}
	}

	vector <string> dbroots;
	string tempdbrootList;

	while(true)
	{
		dbroots.clear();

		prompt = "Enter the list (Nx,Ny,Nz) or range (Nx-Nz) of dbroot IDs assigned to module 'pm1' (" + dbrootList + ") > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) {
				tempdbrootList = pcommand;
				callFree(pcommand);
			}
			else
				tempdbrootList = dbrootList;
		}

		if ( tempdbrootList.empty())
			continue;

		//check for range
		int firstID;
		int lastID;
		string::size_type pos = tempdbrootList.find("-",0);
		if (pos != string::npos)
		{
			firstID = atoi(tempdbrootList.substr(0, pos).c_str());
			lastID = atoi(tempdbrootList.substr(pos+1, 200).c_str());

			if ( firstID >= lastID ) {
				cout << "Invalid Entry, please re-enter" << endl;
				continue;
			}
			else
			{
				for ( int id = firstID ; id < lastID+1 ; id++ )
				{
					dbroots.push_back(oam.itoa(id));
				}
			}
		}
		else
		{
			boost::char_separator<char> sep(",");
			boost::tokenizer< boost::char_separator<char> > tokens(tempdbrootList, sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				dbroots.push_back(*it);
			}
		}

		break;
	}

	int id=1;
	std::vector<std::string>::iterator it = dbroots.begin();
	for (; it != dbroots.end() ; it++, ++id)
	{
		//store dbroot ID
		string moduledbrootid = "ModuleDBRootID1-" + oam.itoa(id) + "-3";
		try {
			sysConfig->setConfig(ModuleSection, moduledbrootid, *it);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot ID in the InfiniDB System Configuration file" << endl;
			exit(1);
		}

		string DBrootID = "DBRoot" + *it;
		string pathID = installDir + "/data" + *it;

		try {
			sysConfig->setConfig(SystemSection, DBrootID, pathID);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}

	//store number of dbroots
	moduledbrootcount = "ModuleDBRootCount1-3";
	try {
		sysConfig->setConfig(ModuleSection, moduledbrootcount, oam.itoa(dbroots.size()));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting dbroot count in the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//total dbroots on the system
	DBRootCount = DBRootCount + dbroots.size();

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	return true;
}
 
pthread_mutex_t THREAD_LOCK;

void remoteInstallThread(void *arg)
{
  	thread_data_t *data = (thread_data_t *)arg;

	int rtnCode = system((data->command).c_str());
	if (WEXITSTATUS(rtnCode) != 0) {
		pthread_mutex_lock(&THREAD_LOCK);
		cout << endl << "Failure with a remote module install, check install log files in /tmp" << endl;
		exit(1);
	}

	// exit thread
	pthread_exit(0);
}
// vim:ts=4 sw=4:

