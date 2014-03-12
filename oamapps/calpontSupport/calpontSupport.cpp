/* Copyright (C) 2013 Calpont Corp. */


/******************************************************************************************
* $Id: calpontSupport.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
*		
******************************************************************************************/
/**
 * @file
 */

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
#include "stdio.h"
#include "ctype.h"
#include <netdb.h>
#include <readline/readline.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "installdir.h"

using namespace std;
using namespace oam;
using namespace config;

typedef struct Child_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

string currentDate;
string systemName;
string localModule;
string localModuleHostName;
ChildModuleList childmodulelist;
ChildModuleList parentmodulelist;
ChildModule childmodule;

string rootPassword = "";
string debug_flag = "0";
string mysqlpw = " ";

int runningThreads = 0;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

typedef boost::tuple<ChildModuleList::iterator, string > threadInfo_t;

bool LOCAL = false;

void title(string outputFile = "calpontSupportReport.txt")
{
	string cmd = "echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%' >> " + outputFile;
	system(cmd.c_str());
	cmd = "echo ' ' >> " + outputFile;
	system(cmd.c_str());
	cmd = "echo ' System " + systemName + "' >> " + outputFile;
	system(cmd.c_str());
	cmd = "echo ' calpontSupportReport script ran from Module " + localModule + " on " + currentDate + "' >> " + outputFile;
	system(cmd.c_str());
	cmd = "echo ' ' >> " + outputFile;
	system(cmd.c_str());
	cmd = "echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%' >> " + outputFile;
	system(cmd.c_str());
}

void childReportThread(threadInfo_t& st)
{
	ChildModuleList::iterator& list = boost::get<0>(st);
	string reportType = boost::get<1>(st);

	string remoteModuleName = (*list).moduleName;
	string remoteModuleIP = (*list).moduleIP;
	string remoteHostName = (*list).hostName;

	string installDir(startup::StartUp::installDir());

	pthread_mutex_lock( &mutex1 );
	runningThreads++;
//cout << "++ " << runningThreads << endl;
	pthread_mutex_unlock( &mutex1 );

	string outputFile;
	if (reportType == "log")
	{
		outputFile = remoteModuleName + "_" + reportType + "Report.tar.gz";
	}
	else if (reportType == "hadoop")
	{
		outputFile = "hadoopReport.txt";
	}
	else
	{
		outputFile = remoteModuleName + "_" + reportType + "Report.txt";
	
		title(outputFile);
	
		string cmd = "echo '=======================================================================' >> " + outputFile;
		system(cmd.c_str());
		cmd = "echo '=                    " + reportType + " report                                  =' >> " + outputFile;
		system(cmd.c_str());
		cmd = "echo '=======================================================================' >> " + outputFile;
		system(cmd.c_str());
	}

	//run remote report script
	if (reportType == "hadoop")
		cout << "Get " + reportType + " report data" << endl;
	else
		cout << "Get " + reportType + " report data for " +  remoteModuleName + "      " << endl;
	cout.flush();

	string cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + rootPassword + " '" +
		installDir + "/bin/" + reportType + "Report.sh " + remoteModuleName + " " + installDir +
		"' " + debug_flag + " - forcetty";
	int rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 0) {
		cout << "Error with running remote_command.sh, exiting..." << endl;
	}
	cmd = installDir + "/bin/remote_scp_get.sh " + remoteModuleIP + " " + rootPassword + " /tmp/" + outputFile + " > /dev/null 2>&1";
	rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 0)
		cout << "ERROR: failed to retrieve /tmp/" << outputFile << " from " + remoteHostName << endl;
	pthread_mutex_lock( &mutex1 );
	runningThreads--;
//cout << "-- " << runningThreads << endl;
	pthread_mutex_unlock( &mutex1 );

	// exit thread
	pthread_exit(0);
}

void reportThread(string reporttype)
{
	string reportType = reporttype;
	
	string installDir(startup::StartUp::installDir());
	Oam oam;

	pthread_mutex_lock( &mutex1 );
	runningThreads++;
//cout << "++ " << runningThreads << endl;
	pthread_mutex_unlock( &mutex1 );

	string outputFile = localModule + "_" + reportType + "Report.txt";

	// run on child servers and get report
	if (!LOCAL)
	{
		ChildModuleList::iterator list1 = childmodulelist.begin();
		for (; list1 != childmodulelist.end() ; list1++)
		{
			threadInfo_t *st = new threadInfo_t;
			*st = boost::make_tuple(list1, reportType);

			pthread_t childreportthread;
			int status = pthread_create (&childreportthread, NULL, (void*(*)(void*)) &childReportThread, st);
	
			if ( status != 0 ) {
				cout <<  "ERROR: childreportthread: pthread_create failed, return status = " + oam.itoa(status) << endl;
			}

			sleep(1);
		}
	}

	// run report on local server
	cout << "Get " + reportType + " report data for " + localModule  << endl;

	if (reportType == "log")
	{
			string cmd = installDir + "/bin/logReport.sh " + localModule + " " + installDir;
			system(cmd.c_str());

			cmd = "mv -f /tmp/" + localModule + "_logReport.tar.gz .";
			system(cmd.c_str());

			cmd = "tar -zcf " + localModule + "_mysqllogReport.tar.gz " + installDir + "/mysql/db/*.err* 2>/dev/null";
			system(cmd.c_str());
	
			// run log config on local server
			cout << "Get log config data for " + localModule << endl;
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** Log Configuration  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getLogConfig ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getLogConfig >> " + outputFile;
			system(cmd.c_str());
	}
	else
	{
		//get local report
		title(outputFile);
	
		string cmd = "echo '=======================================================================' >> " + outputFile;
		system(cmd.c_str());
		cmd = "echo '=                    " + reportType + " report                                  =' >> " + outputFile;
		system(cmd.c_str());
		cmd = "echo '=======================================================================' >> " + outputFile;
		system(cmd.c_str());
	
		cmd = installDir + "/bin/" + reportType + "Report.sh " + localModule + " " + installDir;
		system(cmd.c_str());
		cmd = " mv -f /tmp/" + localModule + "_" + reportType + "Report.txt .";
		system(cmd.c_str());

		if (reportType == "config" )
		{
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Network Configuration ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getSystemNetworkConfig ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getSystemNetworkConfig >> " + outputFile;
			system(cmd.c_str());
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Module Configure  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getModuleTypeConfig ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getModuleTypeConfig >> " + outputFile;
			system(cmd.c_str());
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Storage Configuration  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getStorageConfig ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getStorageConfig >> " + outputFile;
			system(cmd.c_str());
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Storage Status  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getStorageStatus ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getStorageStatus >> " + outputFile;
			system(cmd.c_str());
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Status  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# calpontConsole getSystemInfo ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = installDir + "/bin/calpontConsole getSystemInfo >> " + outputFile;
			system(cmd.c_str());
	
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '******************** System Configuration File  ********************' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo '################# cat /etc/Calpont.xml ################# ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "echo ' ' >> " + outputFile;
			system(cmd.c_str());
			cmd = "cat " + installDir + "/etc/Calpont.xml >> " + outputFile;
			system(cmd.c_str());
		}
	
		if (reportType == "resource" )
		{
			if (LOCAL) {
				cmd = "echo '################# calpontConsole getModuleResourceUsage ################# ' >> " + outputFile;
				system(cmd.c_str());
				cmd = "echo ' ' >> " + outputFile;
				system(cmd.c_str());
				string cmd = installDir + "/bin/calpontConsole getModuleResourceUsage " + localModule + " >> " + outputFile;
				system(cmd.c_str());
			}
			else
			{
				cmd = "echo '################# calpontConsole getSystemResourceUsage ################# ' >> " + outputFile;
				system(cmd.c_str());
				cmd = "echo ' ' >> " + outputFile;
				system(cmd.c_str());
				string cmd = installDir + "/bin/calpontConsole getSystemResourceUsage >> " + outputFile;
				system(cmd.c_str());
			}
		}
	}

	// exit thread
	pthread_mutex_lock( &mutex1 );
	runningThreads--;
//cout << "-- " << runningThreads << endl;
	pthread_mutex_unlock( &mutex1 );

	pthread_exit(0);
}

int main(int argc, char *argv[])
{
    Oam oam;
	string installDir(startup::StartUp::installDir());

	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";
	string InstallSection = "Installation";

	bool HARDWARE = false;
	bool SOFTWARE = false;
	bool CONFIG = false;
	bool DBMS = false;
	bool RESOURCE = false;
	bool LOG = false;
	bool BULKLOG = false;
	bool HADOOP = false;

	//get current time and date
	time_t now;
	now = time(NULL);
	struct tm tm;
	localtime_r(&now, &tm);
	char timestamp[200];
	strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
	currentDate = timestamp;

	char helpArg[3] = "-h";

	// Get System Name
	try{
		oam.getSystemConfig("SystemName", systemName);
	}
	catch(...)
	{ 
		systemName = "unassigned";
	}

	// get Local Module Name and Server Install Indicator
	string singleServerInstall;

	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		localModule = boost::get<0>(st);
	}
	catch (...) {
		cout << endl << "**** Failed : Failed to read Local Module Name" << endl;
		exit(-1);
	}

	try{
		oam.getSystemConfig("SingleServerInstall", singleServerInstall);
	}
	catch(...)
	{ 
		singleServerInstall = "y";
	}

    if (argc == 1) {
		argv[1] = &helpArg[0];
		argc = 2;
	}

	string DataFilePlugin;
	try{
		DataFilePlugin = sysConfig->getConfig(SystemSection, "DataFilePlugin");
	}
	catch(...)
	{
		cout << "ERROR: Problem accessing InfiniDB configuration file" << endl;
		exit(-1);
	}
 
   for( int i = 1; i < argc; i++ )
   {
		if( string("-h") == argv[i] ) {
			cout << endl;
			cout << "'calpontSupport' generates a Set of System Support Report Files in a tar file" << endl;
			cout << "called calpontSupportReport.'system-name'.tar.gz in the local directory." << endl;
			cout << "It should be run on the server with the DBRM front-end." << endl;
			cout << "Check the Admin Guide for additional information." << endl;
			cout << endl;
			cout << "Usage: calpontSupport [-h][-a][-hw][-s][-c][-db][-r][-l][-bl][-lc][-p 'root-password'][-mp 'mysql-root-password'][-de]";
			// if hdfs set up print the hadoop option
			if (!DataFilePlugin.empty())
				cout << "[-hd]";
			cout << endl;
			cout << "			-h  help" << endl;
			cout << "			-a  Output all Reports (excluding Bulk Logs Reports)" << endl;
			cout << "			-hw Output Hardware Reports only" << endl;
			cout << "			-s  Output Software Reports only" << endl;
			cout << "			-c  Output Configuration/Status Reports only" << endl;
			cout << "			-db Output DBMS Reports only" << endl;
			cout << "			-r  Output Resource Reports only" << endl;
			cout << "			-l  Output Calpont Log/Alarms Reports only" << endl;
			cout << "			-bl Output Calpont Bulk Log Reports only" << endl;
			cout << "			-lc Output Reports for Local Server only" << endl;
			cout << "			-p  password (multi-server systems), root-password or 'ssh' to use 'ssh keys'" << endl;
			cout << "			-mp mysql root user password" << endl;
			cout << "			-de Debug Flag" << endl;
			// if hdfs set up print the hadoop option
			if (!DataFilePlugin.empty())
			cout << "			-hd Output hadoop reports only" << endl;
			
			exit (0);
		}
		else
		{
			if( string("-a") == argv[i] ) {
				HARDWARE = true;
				SOFTWARE = true;
				CONFIG = true;
				DBMS = true;
				RESOURCE = true;
				LOG = true;
				HADOOP = (DataFilePlugin.empty()? false : true);
			}
			else if( string("-hw") == argv[i] )
				HARDWARE = true;
			else if( string("-s") == argv[i] )
				SOFTWARE = true;
			else if( string("-c") == argv[i] )
				CONFIG = true;
			else if( string("-db") == argv[i] )
				DBMS = true;
			else if( string("-r") == argv[i] )
				RESOURCE = true;
			else if( string("-l") == argv[i] )
				LOG = true;
			else if( string("-bl") == argv[i] )
				BULKLOG = true;
			else if( string("-lc") == argv[i] )
				LOCAL = true;
			else if( string("-p") == argv[i] ) {
				i++;
				if ( argc == i ) {
					cout << "ERROR: missing root password argument" << endl;
					exit(-1);
				}
				rootPassword = argv[i];
				//add single quote for special characters
				if ( rootPassword != "ssh" )
				{
					rootPassword = "'" + rootPassword + "'";
				}
			}
			else if( string("-mp") == argv[i] ) {
				i++;
				if ( argc == i ) {
					cout << "ERROR: missing mysql root user password argument" << endl;
					exit(-1);
				}
				mysqlpw = argv[i];
				mysqlpw = "'" + mysqlpw + "'";
			}
			else if( string("-de") == argv[i] )
				debug_flag = "1";
			else if ( string("-hd") == argv[i] )
			{
				HADOOP = (DataFilePlugin.empty()? false : true);
			}
			else
			{
				cout << "Invalid Option of '" << argv[i] << "', run with '-h' for help" << endl;
				exit (1);
			}
		}
	}

	//default to -a if nothing is set
	if ( !HARDWARE && !SOFTWARE && !CONFIG && !DBMS && !RESOURCE && !LOG && !BULKLOG && !HADOOP)
	{
		HARDWARE = true;
		SOFTWARE = true;
		CONFIG = true;
		DBMS = true;
		RESOURCE = true;
		LOG = true;
		HADOOP = (DataFilePlugin.empty()? false : true);
	}

	//get Parent OAM Module Name and setup of it's Custom OS files
	string PrimaryUMModuleName;
	try{
		PrimaryUMModuleName = sysConfig->getConfig(SystemSection, "PrimaryUMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
		exit(-1);
	}

	if ( PrimaryUMModuleName == "unassigned" )
		PrimaryUMModuleName = localModule;

	if ( (localModule != PrimaryUMModuleName) && DBMS )
	{
		char* pcommand = 0;
		char *p;
		string argument = "n";
	
		while(true)
		{
			cout << endl << "You selected to get the DBMS data." << endl;
			cout << "You need to run the calpontSupport command on module '" << PrimaryUMModuleName << "' to get that information." << endl;
			cout << "Or you can proceed on to get all data except the DBMS." << endl;

			pcommand = readline("           Do you want to proceed: (y or n) [n]: ");
	
			if (pcommand && *pcommand) {
				p = strtok(pcommand," ");
				argument = p;
				free(pcommand);
				pcommand = 0;
			}
	
			if (pcommand) {
				free(pcommand);
				pcommand = 0;
			}
	
			if( argument == "y") {
				cout << endl;
				break;
			}
			else if( argument == "n")
				exit (1);
		}
	}

	//get number of worker-nodes, will tell us if a single server system
	//get Parent OAM Module Name and setup of it's Custom OS files
	try{
		string NumWorkers = sysConfig->getConfig("DBRM_Controller", "NumWorkers");
		if ( NumWorkers == "1" )
			singleServerInstall = "y";
	}
	catch(...)
	{}

	if ( singleServerInstall == "n" && !LOCAL)
		if ( HARDWARE || SOFTWARE || CONFIG || RESOURCE || LOG || HADOOP )
			if ( rootPassword.empty() ) {
				cout << "ERROR: Multi-Module System, Password Argument required or use '-lc' option, check help for more information" << endl;
				exit(-1);
			}

	//get Parent OAM Module Name and setup of it's Custom OS files
	//string parentOAMModuleName;
	ChildModule parentOAMModule;
	try{
		parentOAMModule.moduleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
		exit(-1);
	}

	//Get list of configured system modules
	SystemModuleTypeConfig sysModuleTypeConfig;

	try{
		oam.getSystemConfig(sysModuleTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the Calpont System Configuration file" << endl;
		exit(-1);
	}

	string ModuleSection = "SystemModuleConfig";

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleCount == 0 )
			//no modules equipped for this Module Type, skip
			continue;

		//get IP addresses and Host Names
		DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
		{
			string moduleName = (*listPT).DeviceName;
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			string moduleIPAddr = (*pt1).IPAddr;
			string moduleHostName = (*pt1).HostName;

			if ( moduleName == localModule) {
				localModuleHostName = moduleHostName;
			}

			//save Child modules
			if ( moduleName != localModule && moduleType != "xm") {
				childmodule.moduleName = moduleName;
				childmodule.moduleIP = moduleIPAddr;
				childmodule.hostName = moduleHostName;
				childmodulelist.push_back(childmodule);
			}
			
			if (moduleName == parentOAMModule.moduleName)
			{
				parentOAMModule.moduleIP = moduleIPAddr;
				parentOAMModule.hostName = moduleHostName;
				parentOAMModule.moduleName = moduleName;
			}
		}
	} //end of i for loop

	// create a clean Calpont Support Report
	system("rm -f *_configReport.txt");
	system("rm -f *_dbmsReport.txt");
	system("rm -f *_hardwareReport.txt");
	system("rm -f *_logReport.txt");
	system("rm -f *_bulklogReport.txt");
	system("rm -f *_resourceReport.txt");
	system("rm -f *_softwareReport.txt");
	system("rm -f hadoopReport.txt");

	//
	// Software
	//

	if ( SOFTWARE ) {
		string reportType = "software";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// Configuration
	//

	if ( CONFIG ) {
		string reportType = "config";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// Alarms and Calpont Logs
	//

	if ( LOG ) {
		string reportType = "log";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// Bulk Logs
	//

	if ( BULKLOG ) {
		string reportType = "bulklog";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// Hardware
	//

	if ( HARDWARE ) {
		string reportType = "hardware";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// Resources
	//

	if ( RESOURCE ) {
		string reportType = "resource";
		pthread_t reportthread;
		int status = pthread_create (&reportthread, NULL, (void*(*)(void*)) &reportThread, &reportType);

		if ( status != 0 ) {
			cout <<  "ERROR: reportthread: pthread_create failed, return status = " + oam.itoa(status);
		}

		sleep(5);
	}

	//
	// DBMS
	//

	if ( DBMS ) {
		system("rm -f calpontSupportReport.txt;touch calpontSupportReport.txt");
		title();

		system("echo '=======================================================================' >> calpontSupportReport.txt");
		system("echo '=                    DBMS Report                                      =' >> calpontSupportReport.txt");
		system("echo '=======================================================================' >> calpontSupportReport.txt");

		// run DBMS report on local server
		cout << "Get dbms report data for " << localModule << endl;

		bool FAILED = false;
		if ( localModule != PrimaryUMModuleName )
		{
			cout << "     FAILED: run calpontSupport on '" << PrimaryUMModuleName << "' to get the dbrm report" << endl;
			FAILED = true;
		}
		else
		{
			// check if mysql is supported and get info
			string calpontMysql = installDir + "/mysql/bin/mysql --defaults-file=" + installDir + "/mysql/my.cnf -u root ";
			string cmd = calpontMysql + " -e 'status' > /tmp/idbmysql.log 2>&1";
			system(cmd.c_str());

			//check for mysql password set
			string pwprompt = " ";
	
			if (oam.checkLogStatus("/tmp/idbmysql.log", "ERROR 1045") ) {
				cout << "NOTE: MySQL root user password is set" << endl;
				//needs a password, was password entered on command line
				if ( mysqlpw == " " )
				{	//go check my.cnf
					string file = installDir + "/mysql/my.cnf";
					ifstream oldFile (file.c_str());
					
					vector <string> lines;
					char line[200];
					string buf;
					while (oldFile.getline(line, 200))
					{
						buf = line;
						string::size_type pos = buf.find("password",0);
						if (pos != string::npos) {
							string::size_type pos1 = buf.find("=",0);
							if (pos1 != string::npos) {
								pos = buf.find("#",0);
								if (pos == string::npos) {
									//password arg in my.cnf, go get password
									cout << "NOTE: Using password from my.cnf" << endl;
									mysqlpw = buf.substr(pos1+1,80);
									cout << mysqlpw << endl;
									break;
								}
							}
						}
					}

					oldFile.close();

					if ( mysqlpw == " " )
					{
						cout << "NOTE: No password provide on command line or found uncommented in my.cnf" << endl;
						cout << endl;
						string prompt = " *** Enter MySQL password > ";
						mysqlpw = getpass(prompt.c_str());
					}
				}

				//check for mysql password set
				pwprompt = "--password=" + mysqlpw;

				string cmd = calpontMysql + pwprompt + " -e 'status' > /tmp/idbmysql.log 2>&1";
				system(cmd.c_str());

				if (oam.checkLogStatus("/tmp/idbmysql.log", "ERROR 1045") ) {
					cout << "FAILED: Failed login using MySQL root user password '" << mysqlpw << "'" << endl;
					FAILED = true;
				}
			}

			if (!FAILED)
			{	
				// check if mysql is supported and get info
				string calpontMysql = installDir + "/mysql/bin/mysql --defaults-file=" + installDir + "/mysql/my.cnf -u root " + pwprompt;
				string cmd = calpontMysql + " -V > /dev/null 2>&1";
				int ret = system(cmd.c_str());
				if ( WEXITSTATUS(ret) == 0) {
					// run DBMS report info
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS InfiniDB Mysql Version ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " -e status ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'status' >> calpontSupportReport.txt";
					system(cmd.c_str());
			
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql Calpont System Column  ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " -e desc calpontsys.syscolumn ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'desc calpontsys.syscolumn;' >> calpontSupportReport.txt";
					system(cmd.c_str());
			
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql Calpont System Table  ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " -e desc calpontsys.systable ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'desc calpontsys.systable;' >> calpontSupportReport.txt";
					system(cmd.c_str());
			
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql Calpont System Catalog Data ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " calpontsys < " + installDir + "/mysql/dumpcat_mysql.sql ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " calpontsys < " + installDir + "/mysql/dumpcat_mysql.sql >> calpontSupportReport.txt";
					system(cmd.c_str());
			
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql Calpont System Table Data ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " -e select * from calpontsys.systable ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'select * from calpontsys.systable;' >> calpontSupportReport.txt";
					system(cmd.c_str());
			
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql Calpont Usernames ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " -e show databases ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'show databases;' >> calpontSupportReport.txt";
					system(cmd.c_str());
		
					system("echo ' ' >> calpontSupportReport.txt");
					system("echo '******************** DBMS Mysql InfiniDB variables ********************' >> calpontSupportReport.txt");
					system("echo ' ' >> calpontSupportReport.txt");
					cmd = "echo '################# " + calpontMysql + " show variables ################# ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = "echo ' ' >> calpontSupportReport.txt";
					system(cmd.c_str());
					cmd = calpontMysql + " -e 'show variables;' >> calpontSupportReport.txt";
					system(cmd.c_str());
				}
			}
		}

		system("echo ' ' >> calpontSupportReport.txt");
		system("echo '******************** Database Size Report ********************' >> calpontSupportReport.txt");
		system("echo ' ' >> calpontSupportReport.txt");
		string cmd = "echo '################# /bin/databaseSizeReport ################# ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = "echo ' ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = installDir + "/bin/databaseSizeReport >> calpontSupportReport.txt";
		system(cmd.c_str());

		system("echo ' ' >> calpontSupportReport.txt");
		system("echo '******************** DBMS Mysql InfiniDB config file ********************' >> calpontSupportReport.txt");
		system("echo ' ' >> calpontSupportReport.txt");
		cmd = "echo '################# cat /mysql/my.cnf ################# ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = "echo ' ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = "cat " + installDir + "/mysql/my.cnf 2>/dev/null >> calpontSupportReport.txt";
		system(cmd.c_str());

		system("echo ' ' >> calpontSupportReport.txt");
		system("echo '******************** Active Queries ********************' >> calpontSupportReport.txt");
		system("echo ' ' >> calpontSupportReport.txt");
		cmd = "echo '################# calpontConsole getActiveSqlStatement ################# ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = "echo ' ' >> calpontSupportReport.txt";
		system(cmd.c_str());
		cmd = installDir + "/bin/calpontConsole getActiveSqlStatement >> calpontSupportReport.txt";
		system(cmd.c_str());

		cmd = "cat calpontSupportReport.txt > " + localModule + "_dbmsReport.txt";
		system(cmd.c_str());
	}
	
	//
	// HADOOP
	//

	if (HADOOP)
	{
		if (LOCAL || childmodulelist.empty())
		{
			cout << "Get hadoop report data" << endl;
			string cmd = installDir + "/bin/hadoopReport.sh " + localModule + " " + installDir + "\n";
			cmd += " mv -f /tmp/hadoopReport.txt .";
			FILE* pipe = popen(cmd.c_str(), "r");
			if (!pipe) 
			{
				cout << "Failed to get a pipe for hadoop health check commands" << endl;
				exit(-1);
			}
			pclose(pipe);
		}
		else
		{
			// only get hadoop report from parentOAMModule, because it's consistant view.
			parentmodulelist.push_back(parentOAMModule);
			threadInfo_t *st = new threadInfo_t;
			ChildModuleList::iterator iter = parentmodulelist.begin();
			*st = boost::make_tuple(iter, "hadoop");

			pthread_t hdthread;
			int status = pthread_create (&hdthread, NULL, (void*(*)(void*)) &childReportThread, st);
			if ( status != 0 ) 
			{
				cout <<  "ERROR: childreportthread: pthread_create failed, return status = " + oam.itoa(status) << endl;
			}
		}
	}
	
	//wait for all threads to complete
	sleep(5);
	int wait = 0;
	while (true)
	{
//cout << "check " << runningThreads << endl;
		if (runningThreads < 1)
			break;

		sleep(2);
		wait++;
		// give it 60 minutes to complete
		if ( wait >= 3600 * 5)
		{
			cout << "Timed out (60 minutes) waiting for Requests to complete" << endl;
		}
	}

	system("rm -f calpontSupportReport.txt");

	system("unix2dos *Report.txt > /dev/null 2>&1");
	system("rm -rf calpontSupportReport;mkdir calpontSupportReport;mv *Report.txt calpontSupportReport/. > /dev/null 2>&1;mv *Report.tar.gz calpontSupportReport/. > /dev/null 2>&1");
	string cmd = "tar -zcf calpontSupportReport." + systemName + ".tar.gz calpontSupportReport/*";
	system(cmd.c_str());


	cout << endl << "Calpont Support Script Successfully completed, files located in calpontSupportReport." + systemName + ".tar.gz" << endl;
}
