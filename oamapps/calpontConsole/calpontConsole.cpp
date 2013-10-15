/*

Copyright (C) 2009-2013 Calpont Corporation.

Use of and access to the Calpont InfiniDB Community software is subject to the
terms and conditions of the Calpont Open Source License Agreement. Use of and
access to the Calpont InfiniDB Enterprise software is subject to the terms and
conditions of the Calpont End User License Agreement.

This program is distributed in the hope that it will be useful, and unless
otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
Please refer to the Calpont Open Source License Agreement and the Calpont End
User License Agreement for more details.

You should have received a copy of either the Calpont Open Source License
Agreement or the Calpont End User License Agreement along with this program; if
not, it is your responsibility to review the terms and conditions of the proper
Calpont license agreement by visiting http://www.calpont.com for the Calpont
InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
the Calpont InfiniDB Community Calpont Open Source License Agreement.

Calpont may make changes to these license agreements from time to time. When
these changes are made, Calpont will make a new copy of the Calpont End User
License Agreement available at http://www.calpont.com and a new copy of the
Calpont Open Source License Agreement available at http:///www.infinidb.org.
You understand and agree that if you use the Program after the date on which
the license agreement authorizing your use has changed, Calpont will treat your
use as acceptance of the updated License.

*/

/******************************************************************************************
 * $Id: calpontConsole.cpp 3110 2013-06-20 18:09:12Z dhill $
 *
 ******************************************************************************************/

#include <clocale>

#include "calpontConsole.h"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/tokenizer.hpp"
#include "sessionmanager.h"
#include "dbrm.h"
namespace fs = boost::filesystem;

using namespace snmpmanager;
using namespace std;
using namespace oam;
using namespace config;

#include "installdir.h"

// Variables shared in both main and functions

Config* fConfig = 0;
string Section;
int CmdID = 0;
string CmdList[cmdNum];
int CmdListID[cmdNum];
string cmdName;
const string SECTION_NAME = "Cmd";
int serverInstallType;
string systemName;
string parentOAMModule;
string localModule;
bool rootUser = true;

bool repeatStop;

static void checkPromptThread();

bool waitForActive() 
{
    Oam oam;
    SystemStatus systemstatus;
    SystemProcessStatus systemprocessstatus;
    bool bfirst = true;

    for (int i = 0 ; i < 1200 ; i ++)
    {
        sleep (3);
        try
        {
            oam.getSystemStatus(systemstatus);
            if (systemstatus.SystemOpState == ACTIVE)
            {
                return true;
            }
            if (systemstatus.SystemOpState == FAILED)
            {
                return false;
            }
            if (systemstatus.SystemOpState == MAN_OFFLINE)
            {
                return false;
            }
            cout << "." << flush;

            // Check DMLProc for a switch to BUSY_INIT.
            // In such a case, we need to print a message that rollbacks
            // are occurring and will take some time.
            if (bfirst) // Once we've printed our message, no need to waste cpu looking
            {
                oam.getProcessStatus(systemprocessstatus);
                for (unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
                {
                    if (systemprocessstatus.processstatus[i].ProcessName  == "DMLProc")
                    {
                        if (systemprocessstatus.processstatus[i].ProcessOpState == oam::ROLLBACK_INIT)
                        {
							cout << endl << endl <<"   System Not Ready, DMLProc is checking/processing rollback of abandoned transactions. Processing could take some time, please wait..." << flush;
                            bfirst = false;
                        }
                        // At this point, we've found our DMLProc, so there's no need to spin the for loop
                        // any further.
                        break;
                    }
                }
            }
        }
        catch (...)
        {
			// At some point, we need to give up, ProcMgr just isn't going to respond.
			if (i > 60) // 3 minutes
			{
				cout << "ProcMgr not responding while waiting for system to start";
				break;
			}
        }
    }

    return false;
}

bool waitForStop() 
{
    Oam oam;
    SystemStatus systemstatus;
    SystemProcessStatus systemprocessstatus;

    for (int i = 0 ; i < 1200 ; i ++)
    {
        sleep (3);
        try
        {
            oam.getSystemStatus(systemstatus);
            if (systemstatus.SystemOpState == MAN_OFFLINE)
            {
                return true;
            }
            if (systemstatus.SystemOpState == FAILED)
            {
                return false;
            }
            cout << "." << flush;
        }
        catch (...)
        {
			// At some point, we need to give up, ProcMgr just isn't going to respond.
			if (i > 60) // 3 minutes
			{
				cout << "ProcMgr not responding while waiting for system to start";
				break;
			}
        }
    }

    return false;
}

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigTerm(int i)
{
    std::cout << "Received SIGTERM to terminate Calpont Console..." << std::endl;

}

//------------------------------------------------------------------------------
// Signal handler to catch Control-C signal to terminate the process
//------------------------------------------------------------------------------
void handleControlC(int i)
{
    std::cout << "Received Control-C to terminate the console..." << std::endl;
	exit(0);
}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------
void setupSignalHandlers()
{
#ifdef _MSC_VER
    //FIXME
#else
	// Control-C signal to terminate a command
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = handleControlC;
    sigaction(SIGINT, &act, 0);

    // catch SIGTERM signal to terminate the program
//    memset(&act, 0, sizeof(act));
//    act.sa_handler = handleSigTerm;
//    sigaction(SIGTERM, &act, 0);
#endif
}

int main(int argc, char *argv[])
{
#ifndef _MSC_VER
    setuid(0); // set effective ID to root; ignore return status
#endif
    setlocale(LC_ALL, "");

    Oam oam;
    char* pcommand = 0;
    string arguments[ArgNum];
	const char* p = getenv("HOME");
	if (!p) p = "";
	string ccHistoryFile = string(p) + "/.cc_history";

	string cf = startup::StartUp::installDir() + "/etc/" + ConsoleCmdsFile;
	fConfig = Config::makeConfig(cf);

//	setupSignalHandlers();

	// Get System Name
	try{
		oam.getSystemConfig("SystemName", systemName);
	}
	catch(...)
	{
		cout << endl << "**** Failed : Failed to read systemName Name" << endl;
		exit(-1);
	}

	//get parentModule Name
	parentOAMModule = getParentOAMModule();

	// get Local Module Name and Single Server Install Indicator
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		localModule = boost::get<0>(st);
		serverInstallType = boost::get<5>(st);
	}
	catch (...) {
		cout << endl << "**** Failed : Failed to read Local Module Name" << endl;
		exit(-1);
	}

 	//check if root-user
	int user;
	user = getuid();
	if (user != 0)
		rootUser = false;

    // create/open command log file if not created

    logFile.open(DEFAULT_LOG_FILE.c_str(), ios::app);

    if (geteuid() == 0 && !logFile)
    {
        cerr << "UI Command log file cannot be opened" << endl;
    }
    writeLog("Start of a command session!!!");

    // get and sort command list for future help display

    for(int i=0; i<cmdNum ;i++)
    {
        // get cmd name

        Section = SECTION_NAME + oam.itoa(i);

        cmdName = fConfig->getConfig(Section, "Name");
        if (cmdName.empty())
            // no command skip
            continue;

        CmdList[i] = cmdName;
        CmdListID[i] = i;

        // sort

        for(int j=0; j<i ;j++)
        {
            if( CmdList[i] < CmdList[j] )
            {
                cmdName = CmdList[i];
                CmdList[i] = CmdList[j];
                CmdList[j] = cmdName;
                CmdID = CmdListID[i];
                CmdListID[i] = CmdListID[j];
                CmdListID[j] = CmdID;
            }
        }
    }

	if ( localModule != parentOAMModule ) {
		// issue message informing user they aren't logged into Active OAm Parent
		cout << endl;
		cout << "WARNING: running on non Parent OAM Module, can't make configuration changes in this session." << endl;
		cout << "         Access Console from '" << parentOAMModule << "' if you need to make changes." << endl << endl;
	}

    // check for arguments passed in as a request

    if(argc > 1)
    {
		int j=0;
		string command;
        for(; argc > 1; j++, argc--)
        {
            arguments[j] = argv[j+1];
			command.append(arguments[j]);
			command.append(" ");
        }
            // add to history and UI command log file
			read_history(ccHistoryFile.c_str());
            add_history (command.c_str());
            writeLog(command.c_str());
			write_history(ccHistoryFile.c_str());

			checkRepeat(arguments, j);
    }
    else
    {
        cout << endl << "Calpont InfiniDB Command Console" << endl;
        cout << "   enter 'help' for list of commands" << endl;
        cout << "   enter 'exit' to exit the Calpont InfiniDB Command Console" << endl;
        cout << "   use up/down arrows to recall commands" << endl << endl;

		// output current active alarm stats
		printAlarmSummary();
		printCriticalAlarms();

		//read readline history file
		read_history(ccHistoryFile.c_str());

        while (true)
        {
			//get parentModule Name
			parentOAMModule = getParentOAMModule();

            // flush agument list
            for(int j=0; j < ArgNum; j++)
            {
                arguments[j].clear();
            }

            // read input
            pcommand = readline("InfiniDB> ");

            if (!pcommand)                        // user hit <Ctrl>-D
                pcommand = strdup("exit");

            else if (!*pcommand)
            {
                // just an enter-key was entered, ignore and reprompt
                continue;
            }

            // add to history and UI command log file
            add_history (pcommand);
            writeLog(pcommand);
			write_history(ccHistoryFile.c_str());

			string command = pcommand;

			//check if a argument was entered as a set of char with quotes around them
			int commandLoc=0;
			int numberArgs=0;
			bool validCMD=true;

            for(int i=0; i < ArgNum; i++)
            {
				string::size_type pos = command.find(" ", commandLoc);
				string::size_type pos1 = command.find("\"", commandLoc);
				string::size_type pos3 = command.find("\'", commandLoc);
				if ( (pos == string::npos && pos1 == string::npos) ||
						(pos == string::npos && pos3 == string::npos) ) {
					//end of the command
					string argtemp = command.substr(commandLoc,80);
					if ( argtemp != "" ) {
						arguments[numberArgs] = argtemp;
						numberArgs++;
					}
					break;
				}
				if (pos < pos1 && pos < pos3) {
					// hit ' ' first
					string argtemp = command.substr(commandLoc, pos-commandLoc);
					if ( argtemp != "" ) {
						arguments[numberArgs] = argtemp;
						numberArgs++;
					}
					commandLoc = pos+1;
				}
				else
				{
					if ( pos >= pos1 ) {
						//hit " first
						string::size_type pos2 = command.find("\"", pos1+1);
						if (pos2 != string::npos) {
							arguments[numberArgs] = command.substr(pos1+1,pos2-pos1-1);
							numberArgs++;
							commandLoc = pos2+1;
						}
						else
						{
							cout << "Invalid Command, mismatching use of quotes" << endl;
							validCMD=false;
							break;
						}
					}
					else
					{
						//hit ' first
						string::size_type pos2 = command.find("\'", pos3+1);
						if (pos2 != string::npos) {
							arguments[numberArgs] = command.substr(pos3+1,pos2-pos3-1);
							numberArgs++;
							commandLoc = pos2+1;
						}
						else
						{
							cout << "Invalid Command, mismatching use of quotes" << endl;
							validCMD=false;
							break;
						}
					}
				}
			}
			if (validCMD)
				checkRepeat(arguments, numberArgs);

            free (pcommand);
        }
    }
}

void checkRepeat(string* arguments, int argNumber)
{
    Oam oam;
	bool repeat = false;
	int repeatCount = 5;
	for ( int i=0; i < argNumber ; i++)
	{
		if( arguments[i].find("-r") == 0) {
			// entered
			if ( arguments[i] != "-r") {
				//strip report count off
				repeatCount = atoi(arguments[i].substr(2,10).c_str());
				if ( repeatCount < 1 || repeatCount > 60 ) {
					cout << "Failed: incorrect repeat count entered, valid range is 1-60, set to default of 5" << endl;
					repeatCount = 5;
				}
			}
			repeat = true;
			arguments[i].clear();
			cout << "repeating the command '" << arguments[0] << "' every " << repeatCount << " seconds, enter CTRL-D to stop" << endl;
			sleep(5);
			break;
		}
	}

	bool threadCreate = false;
	if (repeat) {
		while(true) {
			system("clear");
			if ( processCommand(arguments) )
				return;
			else {
				if ( !threadCreate ) {
					threadCreate = true;
					repeatStop = false;
					pthread_t PromptThread;
					pthread_create (&PromptThread, NULL, (void*(*)(void*)) &checkPromptThread, NULL);
				}

				for ( int i = 0 ; i < repeatCount ; i ++ )
				{
					if (repeatStop)
						break;
					sleep(1);
				}

				if (repeatStop)
					break;
			}
		}
	}
	else
		processCommand(arguments);
}

int processCommand(string* arguments)
{
    Oam oam;
	// Possible command line arguments
	GRACEFUL_FLAG gracefulTemp = GRACEFUL;
	ACK_FLAG ackTemp = ACK_YES;
	CC_SUSPEND_ANSWER suspendAnswer = CANCEL;
	bool bNeedsConfirm = true;
	string password;

    // get command info from Command config file
    CmdID = -1;

	// put inputted command into lowercase
	string inputCmd = arguments[0];
	transform (inputCmd.begin(), inputCmd.end(), inputCmd.begin(), to_lower());

    for (int i = 0; i < cmdNum; i++)
    {
        // put table command into lowercase
        string cmdName_LC = CmdList[i];
        transform (cmdName_LC.begin(), cmdName_LC.end(), cmdName_LC.begin(), to_lower());

        if (cmdName_LC.find(inputCmd) == 0)
        {
            // command found, ECHO command
    		cout << cmdName_LC << "   " << oam.getCurrentTime() << endl;
            CmdID = CmdListID[i];
            break;
        }
    }
    if (CmdID == -1)
    {
		// get is command in the Support Command list
		for (int i = 0;;i++)
		{
			if (supportCmds[i] == "")
				// end of list
				break;
       		if (supportCmds[i].find(inputCmd) == 0) {
				// match found, go process it
    			cout << supportCmds[i] << "   " << oam.getCurrentTime() << endl;
				int status = ProcessSupportCommand(i, arguments);
				if ( status == -1 )
					// didn't process it for some reason
					break;
				return 1;
			}
		}

        // command not valid
        cout << arguments[0] << ": Unknown Command, type help for list of commands" << endl << endl;
        return 1;
    }

    switch( CmdID )
    {
        case 0: // help
        case 1: // ?
        {
            const string DESC_NAME = "Desc";
            string desc;
            string descName;
            const string ARG_NAME = "Arg";
            string arg;
            string argName;

            string argument1_LC = arguments[1];
            transform (argument1_LC.begin(), argument1_LC.end(), argument1_LC.begin(), to_lower());

            if (argument1_LC.find("-a") == 0 || argument1_LC == "")
            {
                // list commands and brief description (Desc1)
                cout << endl << "List of commands:" << endl;
                cout << "Note: the command must be the first entry entered on the command line" << endl << endl;
                cout.setf(ios::left);
                cout.width(34);
                cout << "Command" << "Description" << endl;
                cout.setf(ios::left);
                cout.width(34);
                cout << "------------------------------" << "--------------------------------------------------------" << endl;

                for(int i=0; i<cmdNum ;i++)
                {
                    // get cmd name

                    Section = SECTION_NAME + oam.itoa(CmdListID[i]);

                    cmdName = fConfig->getConfig(Section, "Name");
                    if (cmdName.empty()  || cmdName == "AVAILABLE")
                        // no command skip
                        continue;
                    cout.setf(ios::left);
                    cout.width(34);
                    cout << cmdName << fConfig->getConfig(Section, "Desc1") << endl;
                }
                cout << endl << "For help on a command, enter 'help' followed by command name" << endl;
            }
            else
            {
		        if (argument1_LC.find("-v") == 0)
                {
                    // list of commands with their descriptions
                    cout << endl << "List of commands and descriptions:" << endl << endl;
                    for(int k=0 ; k < cmdNum ; k++)
                    {
                        Section = SECTION_NAME + oam.itoa(CmdListID[k]);
                        cmdName = fConfig->getConfig(Section, "Name");
	                    if (cmdName.empty()  || cmdName == "AVAILABLE")
                            //no command skip
                            continue;
                        cout << "Command: " << cmdName << endl << endl;
                        int i=2;
                        cout << "   Description: " << fConfig->getConfig(Section, "Desc1") << endl;
                        while (true)
                        {
                            desc = DESC_NAME + oam.itoa(i);
                            descName = fConfig->getConfig(Section, desc);
                            if (descName.empty())
                                //end of Desc list
                                break;
                            cout << "                " << descName << endl;
                            i++;
                        }
                        i=2;
                        cout << endl << "   Arguments:   " << fConfig->getConfig(Section, "Arg1") << endl;
                        while (true)
                        {
                            arg = ARG_NAME + oam.itoa(i);
                            argName = fConfig->getConfig(Section, arg);
                            if (argName.empty())
                                //end of arg list
                                break;
                            cout << "                " << argName << endl;
                            i++;
                        }
                        cout << endl;
                    }
                }
                else
                { // description for a single command
                    int j=0;
                    for (j = 0; j < cmdNum; j++)
                    {
                        // get cmd description

                        Section = SECTION_NAME + oam.itoa(j);

                        cmdName = fConfig->getConfig(Section, "Name");

                        string cmdName_LC = cmdName;
                        transform (cmdName_LC.begin(), cmdName_LC.end(), cmdName_LC.begin(), to_lower());

                        if (cmdName_LC == argument1_LC)
                        {
                            // command found, output description
                            cout << endl << "   Command:     " << cmdName << endl << endl;
                            int i=2;
                            cout << "   Description: " << fConfig->getConfig(Section, "Desc1") << endl;
                            while (true)
                            {
                                desc = DESC_NAME + oam.itoa(i);
                                descName = fConfig->getConfig(Section, desc);
                                if (descName.empty())
                                    //end of Desc list
                                    break;
                                cout << "                " << descName << endl;
                                i++;
                            }
                            i=2;
                            cout << endl << "   Arguments:   " << fConfig->getConfig(Section, "Arg1") << endl;
                            while (true)
                            {
                                arg = ARG_NAME + oam.itoa(i);
                                argName = fConfig->getConfig(Section, arg);
                                if (argName.empty())
                                    //end of arg list
                                    break;
                                cout << "                " << argName << endl;
                                i++;
                            }
                            break;
                        }
                    }
                    if (j == cmdNum)
                    {
                        // command not valid
                        cout << arguments[1] << ": Unknown Command, type help for list of commands" << endl << endl;
                        break;
                    }
                }
            }
            cout << endl;
        }
        break;

        case 2: // exit
        case 3: // quit
        {
            // close the log file
            writeLog("End of a command session!!!");
            logFile.close();
            cout << "Exiting the Calpont Command Console" << endl;

            exit (0);
        }
        break;

        case 4: // getSystemConfig
        {
            SystemSoftware systemsoftware;
            SystemConfig systemconfig;
            string returnValue;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display System config parameters

                try
                {
					oam.getSystemSoftware(systemsoftware);
					oam.getSystemConfig(systemconfig);

					cout << endl << "System Configuration" << endl << endl;

					cout << "SystemName = " << systemconfig.SystemName << endl;
					cout << "SoftwareVersion = " << systemsoftware.Version << endl;
					cout << "SoftwareRelease = " << systemsoftware.Release << endl;

                    cout << "ParentOAMModuleName = " << systemconfig.ParentOAMModule << endl;
                    cout << "StandbyOAMModuleName = " << systemconfig.StandbyOAMModule << endl;
                    cout << "NMSIPAddr = " << systemconfig.NMSIPAddr << endl;
                    cout << "ModuleHeartbeatPeriod = " << systemconfig.ModuleHeartbeatPeriod << endl;
                    cout << "ModuleHeartbeatCount = " << systemconfig.ModuleHeartbeatCount << endl;
//                    cout << "ProcessHeartbeatPeriod = " << systemconfig.ProcessHeartbeatPeriod << endl;
                    cout << "DBRootCount = " << systemconfig.DBRootCount << endl;

					std::vector<std::string>::iterator pt = systemconfig.DBRoot.begin();
					int id=1;
					for( ; pt != systemconfig.DBRoot.end() ; pt++)
					{
						string dbroot = *pt;
                   		cout << "DBRoot" + oam.itoa(id) + " = " + dbroot << endl;
						++id;
					}

                    cout << "DBRMRoot = " << systemconfig.DBRMRoot << endl;
                    cout << "ExternalCriticalThreshold = " << systemconfig.ExternalCriticalThreshold << endl;
                    cout << "ExternalMajorThreshold = " << systemconfig.ExternalMajorThreshold << endl;
                    cout << "ExternalMinorThreshold = " << systemconfig.ExternalMinorThreshold << endl;
                    cout << "MaxConcurrentTransactions = " << systemconfig.MaxConcurrentTransactions << endl;
                    cout << "SharedMemoryTmpFile = " << systemconfig.SharedMemoryTmpFile << endl;
                    cout << "NumVersionBufferFiles = " << systemconfig.NumVersionBufferFiles << endl;
                    cout << "VersionBufferFileSize = " << systemconfig.VersionBufferFileSize << endl;
                    cout << "OIDBitmapFile = " << systemconfig.OIDBitmapFile << endl;
                    cout << "FirstOID = " << systemconfig.FirstOID << endl;
                    cout << "TransactionArchivePeriod = " << systemconfig.TransactionArchivePeriod << endl;

                 }
                catch (exception& e)
                {
                    cout << endl << "**** getSystemConfig Failed :  " << e.what() << endl;
                }
            }
            else
            { // get a single parameter

                try
                {
                    oam.getSystemConfig(arguments[1], returnValue);
                    cout << endl << "   " << arguments[1] << " = " << returnValue << endl << endl;
                }
                catch (exception& e)
                {
                    cout << endl << "**** getSystemConfig Failed :  " << e.what() << endl;
                }
            }
        }
        break;

        case 5: // setSystemConfig - parameters: Module name, Parameter name and value
        {
			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setSystemConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** setSystemConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
            try
            {
				if ( arguments[2] == "=" ) {
                	cout << endl << "**** setSystemConfig Failed : Invalid Value of '=', please re-enter" << endl;
					break;
				}
                oam.setSystemConfig(arguments[1], arguments[2]);
                cout << endl << "   Successfully set " << arguments[1] << " = " << arguments[2] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** setSystemConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 6: // getModuleTypeConfig
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            ModuleConfig moduleconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();
            string returnValue;
            string Argument;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Module config parameters

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl << "Module Type Configuration" << endl << endl;

                    for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

						if ( moduleCount < 1 )
							continue;

						string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                        cout << "ModuleType '" << moduletype	<< "' Configuration information" << endl << endl;

                        cout << "ModuleDesc = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc << endl;
                        cout << "RunType = " << systemmoduletypeconfig.moduletypeconfig[i].RunType << endl;
                        cout << "ModuleCount = " << moduleCount << endl;

						if ( moduleCount > 0 )
						{
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
							{
								string modulename = (*pt).DeviceName;
								HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
								for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
								{
									string ipAddr = (*pt1).IPAddr;
									string servername = (*pt1).HostName;
									cout << "ModuleHostName and ModuleIPAddr for NIC ID " + oam.itoa((*pt1).NicID) + " on  module '" << modulename << "' = " << servername  << " , " << ipAddr << endl;
								}
							}
						}

						DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt++)
						{
							if ( (*pt).dbrootConfigList.size() > 0 )
							{
								cout << "DBRootIDs assigned to module 'pm" << (*pt).DeviceID << "' = "; 
								DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
								for( ; pt1 != (*pt).dbrootConfigList.end() ; )
								{
									cout << *pt1;
									pt1++;
									if (pt1 != (*pt).dbrootConfigList.end())
										cout << ", ";
								}
							}
							cout << endl;
						}

                        cout << "ModuleCPUCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUCriticalThreshold << endl;
                        cout << "ModuleCPUMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMajorThreshold << endl;
                        cout << "ModuleCPUMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorThreshold << endl;
                        cout << "ModuleCPUMinorClearThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorClearThreshold << endl;
						cout << "ModuleDiskCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskCriticalThreshold << endl;
                        cout << "ModuleDiskMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMajorThreshold << endl;
						cout << "ModuleDiskMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDiskMinorThreshold << endl;
                        cout << "ModuleMemCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemCriticalThreshold << endl;
                        cout << "ModuleMemMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMajorThreshold << endl;
                        cout << "ModuleMemMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleMemMinorThreshold << endl;
                        cout << "ModuleSwapCriticalThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapCriticalThreshold << endl;
                        cout << "ModuleSwapMajorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMajorThreshold << endl;
                        cout << "ModuleSwapMinorThreshold % = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleSwapMinorThreshold << endl;

						DiskMonitorFileSystems::iterator pt2 = systemmoduletypeconfig.moduletypeconfig[i].FileSystems.begin();
						int id=1;
						for( ; pt2 != systemmoduletypeconfig.moduletypeconfig[i].FileSystems.end() ; pt2++)
						{
							string fs = *pt2;
                        	cout << "ModuleDiskMonitorFileSystem#" << id << " = " << fs << endl;
							++id;
						}
                        cout << endl;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                }
            }
            else
            { // get a single module type config
                if (arguments[2] == "")
                {
                    try
                    {
                        oam.getSystemConfig(arguments[1], moduletypeconfig);

                        cout << endl << "Module Type Configuration for " << arguments[1] << endl << endl;

						int moduleCount = moduletypeconfig.ModuleCount;
						string moduletype = moduletypeconfig.ModuleType;

                        cout << "ModuleDesc = " << moduletypeconfig.ModuleDesc << endl;
                        cout << "ModuleCount = " << moduleCount << endl;
                        cout << "RunType = " << moduletypeconfig.RunType << endl;

						if ( moduleCount > 0 )
						{
							DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
							for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
							{
								string modulename = (*pt).DeviceName;
								HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
								for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
								{
									string ipAddr = (*pt1).IPAddr;
									string servername = (*pt1).HostName;
									cout << "ModuleHostName and ModuleIPAddr for NIC ID " + oam.itoa((*pt1).NicID) + " on  module " << modulename << " = " << servername  << " , " << ipAddr << endl;
								}
							}
						}

						int dbrootCount = moduletypeconfig.ModuleDBRootList.size();

                       cout << "DBRootCount = " << dbrootCount << endl;

						if ( dbrootCount > 0 )
						{
							DeviceDBRootList::iterator pt = moduletypeconfig.ModuleDBRootList.begin();
							for( ; pt != moduletypeconfig.ModuleDBRootList.end() ; pt++)
							{
								cout << "DBRoot IDs assigned to 'pm" + oam.itoa((*pt).DeviceID) + "' = ";

								DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
								for( ; pt1 != (*pt).dbrootConfigList.end() ; )
								{
									cout << *pt1;
									pt1++;
									if (pt1 != (*pt).dbrootConfigList.end())
										cout << ", ";
								}
								cout << endl;
							}
						}

                        cout << "ModuleCPUCriticalThreshold % = " << moduletypeconfig.ModuleCPUCriticalThreshold << endl;
                        cout << "ModuleCPUMajorThreshold % = " << moduletypeconfig.ModuleCPUMajorThreshold << endl;
                        cout << "ModuleCPUMinorThreshold % = " << moduletypeconfig.ModuleCPUMinorThreshold << endl;
                        cout << "ModuleCPUMinorClearThreshold % = " << moduletypeconfig.ModuleCPUMinorClearThreshold << endl;
						cout << "ModuleDiskCriticalThreshold % = " << moduletypeconfig.ModuleDiskCriticalThreshold << endl;
                        cout << "ModuleDiskMajorThreshold % = " << moduletypeconfig.ModuleDiskMajorThreshold << endl;
						cout << "ModuleDiskMinorThreshold % = " << moduletypeconfig.ModuleDiskMinorThreshold << endl;
                        cout << "ModuleMemCriticalThreshold % = " << moduletypeconfig.ModuleMemCriticalThreshold << endl;
                        cout << "ModuleMemMajorThreshold % = " << moduletypeconfig.ModuleMemMajorThreshold << endl;
                        cout << "ModuleMemMinorThreshold % = " << moduletypeconfig.ModuleMemMinorThreshold << endl;
                        cout << "ModuleSwapCriticalThreshold % = " << moduletypeconfig.ModuleSwapCriticalThreshold << endl;
                        cout << "ModuleSwapMajorThreshold % = " << moduletypeconfig.ModuleSwapMajorThreshold << endl;
                        cout << "ModuleSwapMinorThreshold % = " << moduletypeconfig.ModuleSwapMinorThreshold << endl;

						DiskMonitorFileSystems::iterator pt = moduletypeconfig.FileSystems.begin();
						int id=1;
						for( ; pt != moduletypeconfig.FileSystems.end() ; pt++)
						{
							string fs = *pt;
                        	cout << "ModuleDiskMonitorFileSystem#" << id << " = " << fs << endl;
							++id;
						}
                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                { // get a parameter for a module type
					try {
                    	oam.getSystemConfig(systemmoduletypeconfig);
					}
					catch(...) 
					{}

                    unsigned int i = 0;
                    for( i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if(systemmoduletypeconfig.moduletypeconfig[i].ModuleType == arguments[1])
                        {
							string argument2 = arguments[2];
							string::size_type pos = arguments[2].rfind("#",200);
							if (pos != string::npos)
							{
								string ID = arguments[2].substr(pos+1,5);
								arguments[2] = arguments[2].substr(0,pos);
								arguments[2] = arguments[2] + ID + "-";
							}

                            Argument = arguments[2] + oam.itoa(i+1);
                            try
                            {
                                oam.getSystemConfig(Argument, returnValue);
                                cout << endl << "   " << argument2 << " = " << returnValue << endl << endl;
                                break;
                            }
                            catch (exception& e)
                            {
                                cout << endl << "**** getModuleTypeConfig Failed =  " << e.what() << endl;
                                break;
                            }
                        }
                    }
                    if( i == systemmoduletypeconfig.moduletypeconfig.size() )
                    {
                        // module type not found
                        cout << endl << "**** getModuleTypeConfig Failed : Invalid Module Type" << endl;
                        break;
                    }
                }
            }
        }
        break;

        case 7: // setModuleTypeConfig - parameters: Module type, Parameter name and value
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            string Argument;

			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setModuleTypeConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[3] == "")
            {
                // need 3 arguments
                cout << endl << "**** setModuleTypeConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
			if ( arguments[3] == "=" ) {
				cout << endl << "**** setModuleTypeConfig Failed : Invalid Value of '=', please re-enter" << endl;
				break;
			}

			try {
            	oam.getSystemConfig(systemmoduletypeconfig);
			}
			catch(...) 
			{}

            unsigned int i = 0;
            for( i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
            {
                if(systemmoduletypeconfig.moduletypeconfig[i].ModuleType == arguments[1])
                {
					string argument2 = arguments[2];
					string::size_type pos = arguments[2].rfind("#",200);
					if (pos != string::npos)
					{
						string ID = arguments[2].substr(pos+1,5);
						arguments[2] = arguments[2].substr(0,pos);
						arguments[2] = arguments[2] + ID + "-";
					}

                    Argument = arguments[2] + oam.itoa(i+1);
                    try
                    {
                        oam.setSystemConfig(Argument, arguments[3]);
                        cout << endl << "   Successfully set " << argument2 << " = " << arguments[3] << endl << endl;
                        break;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** setModuleTypeConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }
            }
            if( i == systemmoduletypeconfig.moduletypeconfig.size() )
            {
                // module type not found
                cout << endl << "**** setModuleTypeConfig Failed : Invalid Module Type" << endl;
                break;
            }
        }
        break;

        case 8: // getProcessConfig
        {
            SystemProcessConfig systemprocessconfig;
            ProcessConfig processconfig;
            string returnValue;

            if (arguments[1] == "all" || arguments[1] == "")
            {
                // get and all display Process config parameters

                try
                {
                    oam.getProcessConfig(systemprocessconfig);

                    cout << endl << "Process Configuration" << endl << endl;

                    for( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
                    {
                        cout << "Process #" << i+1 << " Configuration information" << endl;

                        cout << "ProcessName = " << systemprocessconfig.processconfig[i].ProcessName  << endl;
                        cout << "ModuleType = " << systemprocessconfig.processconfig[i].ModuleType << endl;
                        cout << "ProcessLocation = " << systemprocessconfig.processconfig[i].ProcessLocation << endl;

                        for( int j = 0 ; j < oam::MAX_ARGUMENTS; j++)
                        {
                            if (systemprocessconfig.processconfig[i].ProcessArgs[j].empty())
                                break;
                            cout << "ProcessArg" << j+1 << " = " << systemprocessconfig.processconfig[i].ProcessArgs[j] << endl;
                        }
                        cout << "BootLaunch = " << systemprocessconfig.processconfig[i].BootLaunch << endl;
                        cout << "LaunchID = " << systemprocessconfig.processconfig[i].LaunchID << endl;

                        for( int j = 0 ; j < MAX_DEPENDANCY; j++)
                        {
                            if (systemprocessconfig.processconfig[i].DepProcessName[j].empty())
                                break;
                            cout << "DepModuleName" << j+1 << " = " << systemprocessconfig.processconfig[i].DepModuleName[j] << endl;
                            cout << "DepProcessName" << j+1 << " = " << systemprocessconfig.processconfig[i].DepProcessName[j] << endl;
                        }
						// display Process Group variables, if they exist

						cout << "RunType = " << systemprocessconfig.processconfig[i].RunType << endl;
						cout << "LogFile = " << systemprocessconfig.processconfig[i].LogFile << endl;

                        cout << endl;
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                }
            }
            else
            { // get a single process info - parameters: module-name, process-name
                if (arguments[2] == "")
                {
                    cout << endl << "**** getProcessConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                    break;
                }
                if (arguments[3] == "")
                {
                    //**** Add API to get single process info
                    try
                    {
                        oam.getProcessConfig(arguments[1], arguments[2], processconfig);

                        cout << endl << "Process Configuration for " << arguments[1] << " on module " << arguments[2] << endl << endl;

                        cout << "ProcessName = " << processconfig.ProcessName  << endl;
                        cout << "ModuleType = " << processconfig.ModuleType << endl;
                        cout << "ProcessLocation = " << processconfig.ProcessLocation  << endl;
                        for( int j = 0 ; j < oam::MAX_ARGUMENTS; j++)
                        {
                            if (processconfig.ProcessArgs[j].empty())
                                break;
                            cout << "ProcessArg" << j+1 << " = " << processconfig.ProcessArgs[j] << endl;
                        }
                        cout << "BootLaunch = " << processconfig.BootLaunch << endl;
                        cout << "LaunchID = " << processconfig.LaunchID << endl;

                       for( int j = 0 ; j < MAX_DEPENDANCY; j++)
                        {
                            if (processconfig.DepProcessName[j].empty())
                                break;
                            cout << "DepProcessName" << j+1 << " = " << processconfig.DepProcessName[j] << endl;
                            cout << "DepModuleName" << j+1 << " = " << processconfig.DepModuleName[j] << endl;
                        }

						cout << "RunType = " << processconfig.RunType << endl;
						cout << "LogFile = " << processconfig.LogFile << endl;

                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                { // get a parameter for a process - parameters: module-name, process-name,
                    // parameter-name
                    // get module ID from module name entered, then get parameter
                    try
                    {
                        oam.getProcessConfig(arguments[1], arguments[2], arguments[3], returnValue);
                        cout << endl << "   " << arguments[3] << " = " << returnValue << endl << endl;
                        break;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getProcessConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }
            }
        }
        break;

        case 9: // setProcessConfig - parameters: Module name, Process Name, Parameter name and value
        {
			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setProcessConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[4] == "")
            {
                cout << endl << "**** setProcessConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
            try
            {
				if ( arguments[4] == "=" ) {
                	cout << endl << "**** setProcessConfig Failed : Invalid Value of '=', please re-enter" << endl;
					break;
				}

                oam.setProcessConfig(arguments[1], arguments[2], arguments[3], arguments[4]);
                cout << endl << "   Successfully set " << arguments[3] << " = " << arguments[4] << endl << endl;
                break;
            }
            catch (exception& e)
            {
                cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 10: // getAlarmConfig- parameters: all or AlarmID
        {
            AlarmConfig alarmconfig;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Alarm config parameters

                cout << endl << "Alarm Configuration" << endl << endl;

                for( int alarmID = 1 ; alarmID < MAX_ALARM_ID; alarmID++)
                {
                    try
                    {
                        oam.getAlarmConfig(alarmID, alarmconfig);

                        cout << "Alarm ID #" << alarmID << " Configuration information" << endl;

                        cout << "BriefDesc = " << alarmconfig.BriefDesc  << endl;
                        cout << "DetailedDesc = " << alarmconfig.DetailedDesc << endl;
                        //	cout << "EmailAddr = " << alarmconfig.EmailAddr  << endl;
                        //	cout << "PagerNum = " << alarmconfig.PagerNum << endl;

                        switch(alarmconfig.Severity)
                        {
                            case CRITICAL:
                                cout << "Severity = CRITICAL" << endl;
                                break;
                            case MAJOR:
                                cout << "Severity = MAJOR" << endl;
                                break;
                            case MINOR:
                                cout << "Severity = MINOR" << endl;
                                break;
                            case WARNING:
                                cout << "Severity = WARNING" << endl;
                                break;
                            default:
                                cout << "Severity = INFORMATIONAL" << endl;
                                break;
                        }

                        cout << "Threshold = " << alarmconfig.Threshold   << endl;
                        //	cout << "Occurrences = " << alarmconfig.Occurrences << endl;
                        //	cout << "LastIssueTime = " << alarmconfig.LastIssueTime  << endl << endl;
                        cout << endl;
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getAlarmConfig Failed =  " << e.what() << endl;
                        break;
                    }
                }
                break;
            }
            else
            { // get a single Alarm info
                try
                {
                    oam.getAlarmConfig(atoi(arguments[1].c_str()), alarmconfig);

                    cout << endl << "Alarm ID #" << arguments[1] << " Configuration information" << endl;

                    cout << "BriefDesc = " << alarmconfig.BriefDesc  << endl;
                    cout << "DetailedDesc = " << alarmconfig.DetailedDesc << endl;
                    //	cout << "EmailAddr = " << alarmconfig.EmailAddr  << endl;
                    //	cout << "PagerNum = " << alarmconfig.PagerNum << endl;

                    switch(alarmconfig.Severity)
                    {
                        case CRITICAL:
                            cout << "Severity = CRITICAL" << endl;
                            break;
                        case MAJOR:
                            cout << "Severity = MAJOR" << endl;
                            break;
                        case MINOR:
                            cout << "Severity = MINOR" << endl;
                            break;
                        case WARNING:
                            cout << "Severity = WARNING" << endl;
                            break;
                        default:
                            cout << "Severity = INFORMATIONAL" << endl;
                            break;
                    }

                    cout << "Threshold = " << alarmconfig.Threshold   << endl;
                    //	cout << "Occurrences = " << alarmconfig.Occurrences << endl;
                    //	cout << "LastIssueTime = " << alarmconfig.LastIssueTime  << endl << endl;
                    cout << endl;
                    break;
                }
                catch (exception& e)
                {
                    cout << endl << "**** getAlarmConfig Failed =  " << e.what() << endl;
                    break;
                }
            }
        }
        break;

        case 11: // setAlarmConfig - parameters: AlarmID, Parameter name and value
        {
			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setAlarmConfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[3] == "")
            {
                // need 3 arguments
                cout << endl << "**** setAlarmConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
            try
            {
				if ( arguments[3] == "=" ) {
                	cout << endl << "**** setAlarmConfig Failed : Invalid Value of '=', please re-enter" << endl;
					break;
				}

				if ( arguments[2] == "Threshold" && arguments[3] != "0" && atoi(arguments[3].c_str()) == 0 ) {
					cout << endl << "**** setAlarmConfig Failed : New value must be a number" << endl;
					break;
				}

                oam.setAlarmConfig(atoi(arguments[1].c_str()), arguments[2], atoi(arguments[3].c_str()));
                cout << endl << "   Successfully set " << arguments[2] << " = " << arguments[3] << endl << endl;
                break;
            }
            catch (exception& e)
            {
                cout << endl << "**** setAlarmConfig Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 12: // getActiveAlarms - parameters: none
        {
            AlarmList alarmList;
			try {
            	oam.getActiveAlarms(alarmList);
			}
			catch(...)
			{
               // need arguments
                cout << endl << "**** getActiveAlarms Failed : Error in oam.getActiveAlarms" << endl;
                break;
			}

            cout << endl << "Active Alarm List:" << endl << endl;

            AlarmList :: iterator i;
            for (i = alarmList.begin(); i != alarmList.end(); ++i)
            {
                cout << "AlarmID           = " << i->second.getAlarmID() << endl;
                cout << "Brief Description = " << i->second.getDesc() << endl;
                cout << "Alarm Severity    = ";
                switch (i->second.getSeverity())
                {
                    case CRITICAL:
                        cout << "CRITICAL" << endl;
                        break;
                    case MAJOR:
                        cout << "MAJOR" << endl;
                        break;
                    case MINOR:
                        cout << "MINOR" << endl;
                        break;
                    case WARNING:
                        cout << "WARNING" << endl;
                        break;
                    case INFORMATIONAL:
                        cout << "INFORMATIONAL" << endl;
                        break;
                }
                cout << "Time Issued       = " << i->second.getTimestamp() << endl;
                cout << "Reporting Module  = " << i->second.getSname() << endl;
                cout << "Reporting Process = " << i->second.getPname() << endl;
                cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;
            }
        }
        break;

        case 13: // getStorageConfig
        {
            try
            {
                systemStorageInfo_t t;
                t = oam.getStorageConfig();

				string cloud;
				try {
					oam.getSystemConfig("Cloud", cloud);
				}
				catch(...) {}

				cout << endl << "System Storage Configuration" << endl << endl;

				cout << "Performance Module (DBRoot) Storage Type = " << boost::get<0>(t) << endl;
				if ( cloud == "amazon" )
					cout << "User Module Storage Type = " << boost::get<3>(t) << endl;
				cout << "System Assigned DBRoot Count = " << boost::get<1>(t) << endl;

				DeviceDBRootList moduledbrootlist = boost::get<2>(t);

				typedef std::vector<int> dbrootList;
				dbrootList dbrootlist;

				DeviceDBRootList::iterator pt = moduledbrootlist.begin();
				for( ; pt != moduledbrootlist.end() ; pt++)
				{
					cout << "DBRoot IDs assigned to 'pm" + oam.itoa((*pt).DeviceID) + "' = ";
					DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
					for( ; pt1 != (*pt).dbrootConfigList.end() ;)
					{
						cout << *pt1;
						dbrootlist.push_back(*pt1);
						pt1++;
						if (pt1 != (*pt).dbrootConfigList.end())
							cout << ", ";
					}
					cout << endl;
				}

				//get any unassigned DBRoots
				DBRootConfigList undbrootlist;
				try {
					oam.getUnassignedDbroot(undbrootlist);
				}
				catch(...) {}

				if ( !undbrootlist.empty() )
				{
					cout << endl << "DBRoot IDs unassigned = ";
					DBRootConfigList::iterator pt1 = undbrootlist.begin();
					for( ; pt1 != undbrootlist.end() ;)
					{
						cout << *pt1;
						pt1++;
						if (pt1 != undbrootlist.end())
							cout << ", ";
					}
					cout << endl;
				}

				cout << endl;
				// um volumes
				if (cloud == "amazon" && boost::get<3>(t) == "external")
				{
					ModuleTypeConfig moduletypeconfig;
					oam.getSystemConfig("um", moduletypeconfig);
					for ( int id = 1; id < moduletypeconfig.ModuleCount+1 ; id++)
					{
						string volumeNameID = "UMVolumeName" + oam.itoa(id);
						string volumeName = oam::UnassignedName;
						string deviceNameID = "UMVolumeDeviceName" + oam.itoa(id);
						string deviceName = oam::UnassignedName;
						try {
							oam.getSystemConfig( volumeNameID, volumeName);
							oam.getSystemConfig( deviceNameID, deviceName);
						}
						catch(...)
						{}

						cout << "Amazon EC2 Volume Name/Device Name for 'um" << id << "': " << volumeName << ", " << deviceName << endl;
					}
				}

				// pm volumes
				if (cloud == "amazon" && boost::get<0>(t) == "external")
				{
					cout << endl;

					DBRootConfigList dbrootConfigList;
					try
					{
						oam.getSystemDbrootConfig(dbrootConfigList);
		
						DBRootConfigList::iterator pt = dbrootConfigList.begin();
						for( ; pt != dbrootConfigList.end() ; pt++)
						{
							string volumeNameID = "PMVolumeName" + oam.itoa(*pt);
							string volumeName = oam::UnassignedName;
							string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt);
							string deviceName = oam::UnassignedName;
							try {
								oam.getSystemConfig( volumeNameID, volumeName);
								oam.getSystemConfig( deviceNameID, deviceName);
							}
							catch(...)
							{
								continue;
							}
	
							cout << "Amazon EC2 Volume Name/Device Name for DBRoot" << oam.itoa(*pt) << ": " << volumeName << ", " << deviceName << endl;
						}
					}
					catch (exception& e)
					{
						cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
					}

					DBRootConfigList::iterator pt1 = undbrootlist.begin();
					for( ; pt1 != undbrootlist.end() ; pt1++)
					{
						string volumeNameID = "PMVolumeName" + oam.itoa(*pt1);
						string volumeName = oam::UnassignedName;
						string deviceNameID = "PMVolumeDeviceName" + oam.itoa(*pt1);
						string deviceName = oam::UnassignedName;
						try {
							oam.getSystemConfig( volumeNameID, volumeName);
							oam.getSystemConfig( deviceNameID, deviceName);
						}
						catch(...)
						{
							continue;
						}

						cout << "Amazon EC2 Volume Name/Device Name for DBRoot" << oam.itoa(*pt1) << ": " << volumeName << ", " << deviceName << endl;
					}
				}
 
				string GlusterConfig;
				string GlusterCopies;
				string GlusterStorageType;
				try {
					oam.getSystemConfig("GlusterConfig", GlusterConfig);
					oam.getSystemConfig("GlusterCopies", GlusterCopies);
					oam.getSystemConfig("GlusterStorageType", GlusterStorageType);
				}
				catch(...) {}

				if ( GlusterConfig == "y" )
				{
					cout << endl << "Data Redundant Configuration" << endl << endl;
					cout << "Copies Per DBroot = " << GlusterCopies << endl;
					cout << "Storage Type = " << GlusterStorageType << endl;

					oamModuleInfo_t st;
					string moduleType;
					try {
						st = oam.getModuleInfo();
						moduleType = boost::get<1>(st);
					}
					catch (...) {}

					if ( moduleType != "pm")
						break;

					try
					{
						DBRootConfigList dbrootConfigList;
						oam.getSystemDbrootConfig(dbrootConfigList);
		
						DBRootConfigList::iterator pt = dbrootConfigList.begin();
						for( ; pt != dbrootConfigList.end() ; pt++)
						{
							cout << "DBRoot #" << oam.itoa(*pt) << " has copies on PMs = ";

							string pmList = "";
							try {
								string errmsg;
								oam.glusterctl(oam::GLUSTER_WHOHAS, oam.itoa(*pt), pmList, errmsg);
							}
							catch (...)
							{}
		
							boost::char_separator<char> sep(" ");
							boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);
							for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
									it != tokens.end();
									++it)
							{
								cout << *it << " ";
							}
					
							cout << endl;
						}
						cout << endl;
					}
					catch (exception& e)
					{
						cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
					}
				}
           }
            catch (exception& e)
            {
                cout << endl << "**** getStorageConfig Failed :  " << e.what() << endl;
            }

			cout << endl;

			break;
         }

        case 14: // addDbroot parameters: dbroot-number
        {
			string GlusterConfig = "n";
			try {
				oam.getSystemConfig( "GlusterConfig", GlusterConfig);
			}
			catch(...)
			{}

			if (GlusterConfig == "y") {
				cout << endl << "**** addDbroot Not Supported on Data Redundancy Configured System, use addModule command to expand your capacity" << endl;
				break;
			}

			if ( localModule != parentOAMModule ) {
				// exit out since not on active module
				cout << endl << "**** addDbroot Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
				break;
			}

            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** addDbroot Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            if (arguments[2] != "")
            {
                // error out if extra arguments exist to catch if they meant to use assigndbrootpmconfig
                cout << endl << "**** addDbroot Failed : Extra Parameter passed, enter 'help' for additional information" << endl;
                break;
            }

			int dbrootNumber = atoi(arguments[1].c_str());

			//get dbroots ids for reside PM
            try
            {
				DBRootConfigList dbrootlist;
                oam.addDbroot(dbrootNumber, dbrootlist);

				cout << endl << " New DBRoot IDs added = ";

				DBRootConfigList::iterator pt = dbrootlist.begin();
				for( ; pt != dbrootlist.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != dbrootlist.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** addDbroot Failed: " << e.what() << endl;
				break;
            }

			cout << endl;
        }
        break;

        case 15: // removeDbroot parameters: dbroot-list
        {
			if ( localModule != parentOAMModule ) {
				// exit out since not on active module
				cout << endl << "**** removeDbroot Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
				break;
			}

            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** removeDbroot Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			systemStorageInfo_t t;
            try
            {
                t = oam.getStorageConfig();
			}
			catch(...) {}

			string dbrootIDs = arguments[1];

			DBRootConfigList dbrootlist;

			bool assign = false;
			boost::char_separator<char> sep(", ");
			boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				//check if dbroot is assigned to a pm
				DeviceDBRootList moduledbrootlist = boost::get<2>(t);
	
				DeviceDBRootList::iterator pt = moduledbrootlist.begin();
				for( ; pt != moduledbrootlist.end() ; pt++)
				{
					DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
					for( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
					{
						if ( atoi((*it).c_str()) == *pt1 ) {
							cout << endl << "**** removeDbroot Failed, dbroot " << *it << " is assigned to a module, unassign first before removing" << endl;
							assign = true;
							break;
						}
					}
				}

				if (assign)
					break;

				dbrootlist.push_back(atoi((*it).c_str()));
			}

			if (assign)
				break;

			cout << endl;

            try
            {
                oam.removeDbroot(dbrootlist);

                cout << endl << "   Successful Removal of DBRoots " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** removeDbroot Failed: " << e.what() << endl;
				break;
            }
        }
        break;

        case 16: // stopSystem - parameters: graceful flag, Ack flag
        {
			BRM::DBRM dbrm;
			bool bDBRMReady = dbrm.isDBRMReady();
			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

			if ( gracefulTemp == INSTALL ) 
			{
				cout << endl << "Invalid Parameter, INSTALL option not supported. Please use shutdownSystem Command" << endl << endl;
				break;
            }

			cout << endl << "This command stops the processing of applications on all Modules within the Calpont System" << endl;

            try
            {
    			if (gracefulTemp != GRACEFUL ||
    				!bDBRMReady ||
    				dbrm.isReadWrite())
    			{
    				suspendAnswer = FORCE;
    			}

    			if (suspendAnswer == CANCEL)	// We don't have an answer from the command line or some other state.
    			{
    				// If there are bulkloads, ddl or dml happening, Ask what to do.
    				bool bIsDbrmUp = true;
    				execplan::SessionManager sessionManager; 
    				BRM::SIDTIDEntry blockingsid;
    				std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
    				bool bActiveTransactions = false;
    				if (!tableLocks.empty())
    				{
    					oam.DisplayLockedTables(tableLocks, &dbrm);
    					bActiveTransactions = true;
    				}
    				if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
    				{
    					cout << endl << "There are active transactions being processed" << endl;
    					bActiveTransactions = true;
    				}

    				if (bActiveTransactions)
    				{
    					suspendAnswer = AskSuspendQuestion(CmdID);
    //					if (suspendAnswer == FORCE)
    //					{
    //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances")) 
    //						{
    //							break;
    //						}
    //					}
    					bNeedsConfirm = false;
    				}
    				else
    				{
    					suspendAnswer = FORCE;
    				}
    			}
    			if (suspendAnswer == CANCEL)
    			{
    				// We're outa here.
    				break;
    			}

    			if (bNeedsConfirm)
    			{
    				if (confirmPrompt(""))
    					break;
    			}

    			switch (suspendAnswer)
    			{
    				case WAIT:
    					cout << endl << "   Waiting for all transactions to complete" << flush;
    					dbrm.setSystemShutdownPending(true, false, false);
    					gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    					break;
    				case ROLLBACK:
    					cout << endl << "   Rollback of all transactions" << flush;
    					dbrm.setSystemShutdownPending(true, true, false);
    					gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    					break;
    				case FORCE:
    					cout << endl << "   System being stopped now...";
    					if (bDBRMReady)
    					{
    						dbrm.setSystemShutdownPending(true, false, true);
    					}
    					break;
    				case CANCEL:
    					break;
    			}

                oam.stopSystem(gracefulTemp, ackTemp);

				if ( waitForStop() )
					cout << endl << "   Successful stop of System " << endl << endl;
				else
					cout << endl << "**** stopSystem Failed : check log files" << endl;

				checkForDisabledModules();
            }
            catch (exception& e)
            {
				string Failed = e.what();

				if (Failed.find("Connection refused") != string::npos)
				{
                	cout << endl << "**** stopSystem Failure : ProcessManager not Active" << endl;
					cout << "Retry or Run 'shutdownSystem FORCEFUL' command" << endl << endl;
				}
				else
				{
					cout << endl << "**** stopSystem Failure : " << e.what() << endl;
					cout << "Retry or Run 'shutdownSystem FORCEFUL' command" << endl << endl;
				}
            }
        }
        break;

        case 17: // shutdownSystem - parameters: graceful flag, Ack flag, suspendAnswer
        {
			BRM::DBRM dbrm;
			bool bDBRMReady = dbrm.isDBRMReady();
            getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

			cout << endl << "This command stops the processing of applications on all Modules within the Calpont System" << endl;

            try
            {
    			if (gracefulTemp != GRACEFUL ||
    				!bDBRMReady ||
    				dbrm.isReadWrite())
    			{
    				suspendAnswer = FORCE;
    			}

    			if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
    			{
    				// If there are bulkloads, ddl or dml happening, Ask what to do.
    				bool bIsDbrmUp = true;
    				execplan::SessionManager sessionManager; 
    				BRM::SIDTIDEntry blockingsid;
    				std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
    				bool bActiveTransactions = false;
    				if (!tableLocks.empty())
    				{
    					oam.DisplayLockedTables(tableLocks, &dbrm);
    					bActiveTransactions = true;
    				}
    				if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
    				{
    					cout << endl << "There are active transactions being processed" << endl;
    					bActiveTransactions = true;
    				}

    				if (bActiveTransactions)
    				{
    					suspendAnswer = AskSuspendQuestion(CmdID);
    //					if (suspendAnswer == FORCE)
    //					{
    //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances")) 
    //						{
    //							break;
    //						}
    //					}
    					bNeedsConfirm = false;
    				}
    				else
    				{
    					suspendAnswer = FORCE;
    				}
    			}
    			if (suspendAnswer == CANCEL)
    			{
    				// We're outa here.
    				break;
    			}

    			if (bNeedsConfirm)
    			{
    				if (confirmPrompt(""))
    					break;
    			}

    			switch (suspendAnswer)
    			{
    				case WAIT:
    					cout << endl << "   Waiting for all transactions to complete" << flush;
    					dbrm.setSystemShutdownPending(true, false, false);
    					gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    					break;
    				case ROLLBACK:
    					cout << endl << "   Rollback of all transactions" << flush;
    					dbrm.setSystemShutdownPending(true, true, false);
    					gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    					break;
    				case FORCE:
    					cout << endl << "   System being shutdown now...";
    					if (bDBRMReady)
    					{
    						dbrm.setSystemShutdownPending(true, false, true);
    					}
    					break;
    				case CANCEL:
    					break;
    			}

				// This won't return until the system is shutdown. It might take a while to finish what we're working on first.

                oam.stopSystem(gracefulTemp, ackTemp);

				if ( waitForStop() )
					cout << endl << "   Successful stop of System " << endl << endl;
				else
					cout << endl << "**** stopSystem Failed : check log files" << endl;

                oam.shutdownSystem(gracefulTemp, ackTemp);

				//hdfs / hadoop config 
				string DBRootStorageType;
				try {
					oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
				}
				catch(...) {}
			
				if ( DBRootStorageType == "hdfs")
				{
					string cmd = "pdsh -a '/etc/init.d/infinidb stop' > /tmp/cc-stop.pdsh 2>&1";
					system(cmd.c_str());
				}
            }
            catch (exception& e)
            {
				string Failed = e.what();

				if ( gracefulTemp == FORCEFUL )
				{
					string cmd = startup::StartUp::installDir() + "/bin/infinidb stop > /tmp/status.log";
					system(cmd.c_str());
					cout << endl << "   Successful shutdown of System (stopped local infinidb service) " << endl << endl;
					break;
				}

				if (Failed.find("Connection refused") != string::npos)
				{
                	cout << endl << "**** shutdownSystem Error : ProcessManager not Active, stopping infinidb service" << endl;
					string cmd = startup::StartUp::installDir() + "/bin/infinidb stop > /tmp/status.log";
					system(cmd.c_str());
					cout << endl << "   Successful stop of local infinidb service " << endl << endl;
				}
				else
				{
					cout << endl << "**** shutdownSystem Failure : " << e.what() << endl;
					cout << "Retry using FORCEFUL option" << endl << endl;
				}

				//hdfs / hadoop config 
				string DBRootStorageType;
				try {
					oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
				}
				catch(...) {}
			
				if ( DBRootStorageType == "hdfs")
				{
					string cmd = "pdsh -a '/etc/init.d/infinidb stop' > /tmp/cc-stop.pdsh 2>&1";
					system(cmd.c_str());
				}
            }
        }
        break;

        case 18: // startSystem - parameters: Ack flag
        {
			// startSystem Command

			//don't start if a disable module has a dbroot assigned to it
			if (!checkForDisabledModules()) {
				cout << endl << "Error: startSystem command can't be performed: disabled module has a dbroot assigned to it" << endl;
				break;
			}

			// if infinidb service is down, then start system by starting all of the infinidb services
			// this would be used after a shutdownSystem command
			// if infinidb service is up, send message to ProcMgr to start system (which starts all processes)

			string cmd = startup::StartUp::installDir() + "/bin/infinidb status > /tmp/status.log";
			system(cmd.c_str());
			if (!oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) 
			{
				cout << "startSystem command, 'infinidb' service is down, sending command to" << endl;
				cout << "start the 'infinidb' service on all modules" << endl << endl;

				SystemModuleTypeConfig systemmoduletypeconfig;
				ModuleTypeConfig moduletypeconfig;
				ModuleConfig moduleconfig;
				systemmoduletypeconfig.moduletypeconfig.clear();
				int systemModuleNumber = 0;
				try
				{
					oam.getSystemConfig(systemmoduletypeconfig);

					for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
							// end of list
							break;
		
						systemModuleNumber = systemModuleNumber + systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
					break;
				}

				if ( systemModuleNumber > 1 ) 
				{
					if (arguments[1] != "")
						password = arguments[1];
					else
						password = "ssh";

					//
					// perform start of InfiniDB of other servers in the system
					//
	
					DeviceNetworkList::iterator pt;
					string modulename;
					for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin(); 
							 pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); 
							 ++pt)
						{
							modulename = (*pt).DeviceName;
							if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
									(*pt).DisableState == oam::AUTODISABLEDSTATE ) 
							{
								cout << "   Module '" << modulename << "' is disabled and will not be started" << endl;
							}
						}
					}
					cout << endl << "   System being started, please wait...";
					cout.flush();
					bool FAILED = false;
	
					//hdfs / hadoop config 
					string DBRootStorageType;
					try {
						oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
					}
					catch(...) {}
				
					if ( DBRootStorageType == "hdfs")
					{
						string cmd = "pdsh -a '/etc/init.d/infinidb restart' > /tmp/cc-restart.pdsh 2>&1";
						system(cmd.c_str());
					}
					else
					{
						for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
								// end of list
								break;
			
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if ( moduleCount == 0 )
								// skip if no modules
								continue;
			
							for (pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin(); 
								pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); 
								++pt)
							{
								modulename = (*pt).DeviceName;
	
								if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
										(*pt).DisableState == oam::AUTODISABLEDSTATE ) 
								{
									continue;
								}
	
								if ( modulename == localModule ) 
								{
									cmd = startup::StartUp::installDir() + "/bin/infinidb restart > /tmp/start.log 2>&1";
									int rtnCode = system(cmd.c_str());
									if (geteuid() == 0 && WEXITSTATUS(rtnCode) != 0) 
									{
										cout << endl << "error with running 'infinidb restart' on local module " << endl;
										cout << endl << "**** startSystem Failed" << endl;
										break;
									}
	
									continue;
								}
	
								HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
								for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
								{
									//run remote command script
									cmd = startup::StartUp::installDir() + "/bin/remote_command.sh " + (*pt1).IPAddr + " " + password + " '" + startup::StartUp::installDir() + "/bin/infinidb restart' 0";
									int rtnCode = system(cmd.c_str());
									if (WEXITSTATUS(rtnCode) < 0) {
										cout << endl << "error with running 'infinidb start' on module " + modulename << endl;
										cout << endl << "**** startSystem Failed" << endl;
										FAILED = true;
										break;
									}
									else
									{
										if (rtnCode > 0) {
											cout << endl << "Invalid Password when running 'infinidb start' on module " + modulename << ", can retry by providing password as the second argument" << endl;
											cout << endl << "**** startSystem Failed" << endl;
											FAILED = true;
											break;
										}
									}
								}
								if (FAILED)
									break;
							}
						}
						if (FAILED)
							break;
					}
	
					if (FAILED)
						break;
				}
				else
				{
					//just kick off local server
					cout << "   System being started, please wait...";
					cout.flush();
					cmd = startup::StartUp::installDir() + "/bin/infinidb restart > /tmp/start.log 2>&1";
					int rtnCode = system(cmd.c_str());
					if (geteuid() == 0 && WEXITSTATUS(rtnCode) != 0) {
						cout << endl << "error with running 'infinidb restart' on local module " << endl;
						cout << endl << "**** startSystem Failed" << endl;
						break;
					}
				}

				if ( waitForActive() )
					cout << endl << "   Successful start of System " << endl << endl;
				else
					cout << endl << "**** startSystem Failed : check log files" << endl;
			}
			else
			{
				getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);
	
				try
				{
					cout << "   System being started, please wait...";
					cout.flush();
					oam.startSystem(ackTemp);
                    if ( waitForActive() )
                        cout << endl << "   Successful start of System " << endl << endl;
                    else
                        cout << endl << "**** startSystem Failed : check log files" << endl;
				}
				catch (exception& e)
				{
					cout << endl << "**** startSystem Failed :  " << e.what() << endl;
					string Failed = e.what();
					if (Failed.find("Database Test Error") != string::npos)
						cout << "Database Test Error occurred, check Alarm and Logs for addition Information" << endl;
				}
			}
        }
        break;

        case 19: // restartSystem - parameters: graceful flag, Ack flag
        {
			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm, &password);

			//don't restart if a disable module has a dbroot assigned to it
			if (!checkForDisabledModules()) {
				cout << endl << "Error: restartSystem command can't be performed: disabled module has a dbroot assigned to it" << endl;
				break;
			}

			cout << endl << "This command stops and restarts the processing of applications on all Modules within the Calpont System" << endl;

			// if infinidb service is down, then restart system by starting all of the infinidb services
			// this would be used after a shutdownSystem command
			// if infinidb service is up, send message to ProcMgr to restart system (which starts all processes)
			string cmd = startup::StartUp::installDir() + "/bin/infinidb status > /tmp/status.log";
			system(cmd.c_str());
			if (!oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) 
			{
				if (bNeedsConfirm) 
				{
					if (confirmPrompt("")) // returns true if user wants to quit.
						break;
				}
				cout << endl << "restartSystem command, 'infinidb' service is down, sending command to" << endl;
				cout << "start the 'infinidb' service on all modules" << endl << endl;

				SystemModuleTypeConfig systemmoduletypeconfig;
				ModuleTypeConfig moduletypeconfig;
				ModuleConfig moduleconfig;
				systemmoduletypeconfig.moduletypeconfig.clear();
				int systemModuleNumber = 0;
				try
				{
					oam.getSystemConfig(systemmoduletypeconfig);

					for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
							// end of list
							break;
		
						systemModuleNumber = systemModuleNumber + systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
					break;
				}

				if ( systemModuleNumber > 1 ) 
				{
					//
					// perform start of InfiniDB of other servers in the system
					//
					DeviceNetworkList::iterator pt;
					string modulename;
					for (unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						for(pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin(); 
							pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); 
							++pt)
						{
							modulename = (*pt).DeviceName;
							if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
									(*pt).DisableState == oam::AUTODISABLEDSTATE ) 
							{
								cout << "   Module '" << modulename << "' is disabled and will not be restarted" << endl;
							}
						}
					}
					cout << "   System being restarted, please wait...";
					cout.flush();
					bool FAILED = false;
	
					//hdfs / hadoop config 
					string DBRootStorageType;
					try {
						oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
					}
					catch(...) {}
				
					if ( DBRootStorageType == "hdfs")
					{
						string cmd = "pdsh -a '/etc/init.d/infinidb restart' > /tmp/cc-restart.pdsh 2>&1";
						system(cmd.c_str());
					}
					else
					{
						for (unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
								// end of list
								break;
			
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if ( moduleCount == 0 )
								// skip if no modules
								continue;
			
							for(pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin(); 
								pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); 
								++pt)
							{
								string modulename = (*pt).DeviceName;
								if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
										(*pt).DisableState == oam::AUTODISABLEDSTATE ) 
									continue;

								if ( modulename == localModule ) {
									string cmd = startup::StartUp::installDir() + "/bin/infinidb restart > /tmp/start.log 2>&1";
									int rtnCode = system(cmd.c_str());
									if (WEXITSTATUS(rtnCode) != 0) {
										cout << endl << "error with running 'infinidb start' on local module " << endl;
										cout << endl << "**** startSystem Failed" << endl;
										break;
									}

									continue;
								}
							}
		
							string DBRootStorageType;
							try 
							{
								oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
							}
							catch(...) {}

							HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
							for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
							{
								//run remote command script
								cmd = startup::StartUp::installDir() + "/bin/remote_command.sh " + (*pt1).IPAddr + " " + password + " '" + startup::StartUp::installDir() + "/bin/infinidb restart' 0";

								int rtnCode = system(cmd.c_str());
								if (WEXITSTATUS(rtnCode) < 0) {
									cout << endl << "error with running 'infinidb start' on module " + modulename << endl;
									cout << endl << "**** startSystem Failed" << endl;
									FAILED = true;
									break;
								}
								else
								{
									if (rtnCode > 0) {
										cout << endl << "Invalid Password when running 'infinidb start' on module " + modulename << ", can retry by providing password as the second argument" << endl;
										cout << endl << "**** startSystem Failed" << endl;
										FAILED = true;
										break;
									}
								}
							}
							if (FAILED)
								break;
						}
						if (FAILED)
							break;
					}
	
					if (FAILED)
						break;
				}
				else
				{
					//just kick off local server
					cout << "   System being restarted, please wait...";
					cout.flush();
					string cmd = startup::StartUp::installDir() + "/bin/infinidb restart > /tmp/start.log 2>&1";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0) {
						cout << endl << "error with running 'infinidb start' on local module " << endl;
						cout << endl << "**** restartSystem Failed" << endl;
						break;
					}
				}

				if ( waitForActive() )
					cout << endl << "   Successful restart of System " << endl << endl;
				else
					cout << endl << "**** restartSystem Failed : check log files" << endl;
			}
			else
			{
				BRM::DBRM dbrm;
				bool bDBRMReady = dbrm.isDBRMReady();

                try
                {
    				if (gracefulTemp != GRACEFUL ||
    					!bDBRMReady ||
    					dbrm.isReadWrite())
    				{
    					suspendAnswer = FORCE;
    				}

    				if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
    				{
    					// If there are bulkloads, ddl or dml happening, Ask what to do.
    					bool bIsDbrmUp = true;
    					execplan::SessionManager sessionManager; 
    					BRM::SIDTIDEntry blockingsid;
    					std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
    					bool bActiveTransactions = false;
    					if (!tableLocks.empty())
    					{
    						oam.DisplayLockedTables(tableLocks, &dbrm);
    						bActiveTransactions = true;
    					}
    					if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
    					{
    						cout << endl << "There are active transactions being processed" << endl;
    						bActiveTransactions = true;
    					}

    					if (bActiveTransactions)
    					{
    						suspendAnswer = AskSuspendQuestion(CmdID);
    //						if (suspendAnswer == FORCE)
    //						{
    //							if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances")) 
    //							{
    //								break;
    //							}
    //						}
    						bNeedsConfirm = false;
    					}
    					else
    					{
    						suspendAnswer = FORCE;
    					}
    				}
    				if (suspendAnswer == CANCEL)
    				{
    					// We're outa here.
    					break;
    				}
    				if (bNeedsConfirm)
    				{
    					if (confirmPrompt(""))
    						break;
    				}
    				switch (suspendAnswer)
    				{
    					case WAIT:
    						cout << endl << "   Waiting for all transactions to complete" << flush;
    						dbrm.setSystemShutdownPending(true, false, false);
    						gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    						break;
    					case ROLLBACK:
    						cout << endl << "   Rollback of all transactions" << flush;
    						dbrm.setSystemShutdownPending(true, true, false);
    						gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
    						break;
    					case FORCE:
    						cout << endl << "   System being restarted now ..." << flush;
    						if (bDBRMReady)
    						{
    							dbrm.setSystemShutdownPending(true, false, true);
    						}
    						break;
    					case CANCEL:
    						break;
    				}

					int returnStatus = oam.restartSystem(gracefulTemp, ackTemp);
                    switch (returnStatus)
                    { 
                        case API_SUCCESS:
                            if ( waitForActive() )
                                cout << endl << "   Successful restart of System " << endl << endl;
                            else
                                cout << endl << "**** restartSystem Failed : check log files" << endl;
                            break;
                        case API_CANCELLED:
                            cout << endl << "   Restart of System canceled" << endl << endl;
                            break;
                        default:
                            cout << endl << "**** restartSystem Failed : Check system logs" << endl;
                            break;
                    }
				}
				catch (exception& e)
				{
					cout << endl << "**** restartSystem Failed :  " << e.what() << endl;
					string Failed = e.what();
					if (Failed.find("Database Test Error") != string::npos)
						cout << "Database Test Error occurred, check Alarm and Logs for additional Information" << endl;
				}
			}
        }
        break;

        case 20: // getSystemStatus - parameters: NONE
        {
			printSystemStatus();
        }
        break;

        case 21: // getProcessStatus - parameters: NONE
        {
			printProcessStatus();
        }
        break;

        case 22: // system - UNIX system command
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** system Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            for(int j=2; j < ArgNum; j++)
            {
                arguments[1].append(" ");
                arguments[1].append(arguments[j]);
            }

            system (arguments[1].c_str());
        }
        break;

        case 23: // getAlarmHistory
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** getAlarmHistory Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			if ( arguments[1].size() != 8 ) {
				cout << "date not in correct format, enter MM/DD/YY" << endl;
				break;
			}

			if ( !(arguments[1].substr(2,1) == "/" && arguments[1].substr(5,1) == "/") ) {
				cout << "date not in correct format, enter MM/DD/YY" << endl;
				break;
			}

            AlarmList alarmList;
			try {
            	oam.getAlarms(arguments[1], alarmList);
            }
            catch (exception& e)
            {
                cout << endl << "**** getAlarms Failed =  " << e.what() << endl;
                break;
            }

            cout << endl << "Historical Alarm List for " + arguments[1] + " :" << endl << endl;

            AlarmList :: iterator i;
			int counter = 0;
            for (i = alarmList.begin(); i != alarmList.end(); ++i)
            {
                switch (i->second.getState())
                {
                    case SET:
                        cout << "SET" << endl;
                        break;
                    case CLEAR:
                        cout << "CLEAR" << endl;
                        break;
                }
                cout << "AlarmID           = " << i->second.getAlarmID() << endl;
                cout << "Brief Description = " << i->second.getDesc() << endl;
                cout << "Alarm Severity    = ";
                switch (i->second.getSeverity())
                {
                    case CRITICAL:
                        cout << "CRITICAL" << endl;
                        break;
                    case MAJOR:
                        cout << "MAJOR" << endl;
                        break;
                    case MINOR:
                        cout << "MINOR" << endl;
                        break;
                    case WARNING:
                        cout << "WARNING" << endl;
                        break;
                    case INFORMATIONAL:
                        cout << "INFORMATIONAL" << endl;
                        break;
                }
                cout << "Time Issued       = " << i->second.getTimestamp() << endl;
                cout << "Reporting Module  = " << i->second.getSname() << endl;
                cout << "Reporting Process = " << i->second.getPname() << endl;
                cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;

				counter++;
				if ( counter > 4 ) {
					// continue prompt
					if (confirmPrompt("Displaying Alarm History"))
						break;
					counter=0;
				}
            }
        }
        break;

        case 24: // monitorAlarms
        {
            cout << endl << "Monitor for System Alarms" << endl;
            cout << " Enter control-C to return to command line" << endl << endl;

            string cmd = "tail -n 0 -f " + snmpmanager::ALARM_FILE;
            system(cmd.c_str());
        }
        break;

        case 25: // resetAlarm
        {
            if (arguments[1] == "")
            {
                // need 3 arguments
                cout << endl << "**** resetAlarm Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
            try
            {
                // check if requested alarm is Active
				AlarmList alarmList;
				Oam oam;
				try {
					oam.getActiveAlarms(alarmList);
				}
				catch (exception& e)
				{
					cout << endl << "**** getActiveAlarm Failed =  " << e.what() << endl;
					break;
				}

                bool found = false;
                AlarmList::iterator i;
                for (i = alarmList.begin(); i != alarmList.end(); ++i)
                {
                    // check if matching ID
					if ( arguments[1] != "ALL" ) {
						if (atoi(arguments[1].c_str()) != (i->second).getAlarmID() )
							continue;

						if ( arguments[2] != "ALL") {
							if (arguments[2].compare((i->second).getSname()) != 0)
								continue;

							if ( arguments[3] != "ALL") {
								if (arguments[3].compare((i->second).getComponentID()) != 0 )
								continue;
							}
						}
					}

	                SNMPManager aManager;
					aManager.sendAlarmReport((i->second).getComponentID().c_str(),
												(i->second).getAlarmID(),
												CLEAR,
												(i->second).getSname(), 
												"calpontConsole");

					cout << endl << "   Alarm Successfully Reset: ";
					cout << "ID = " << oam.itoa((i->second).getAlarmID());
					cout << " / Module = " << (i->second).getSname();
					cout << " / Device = " << (i->second).getComponentID() << endl;
					found = true;
                }
                // check is a SET alarm was found, if not return
                if (!found)
                {
                    cout << endl << "**** resetAlarm Failed : Requested Alarm is not Set" << endl;
                    break;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** resetAlarm Failed =  " << e.what() << endl;
                break;
            }
        }
        break;

        case 26: // enableLog
        {
            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** Failed : enableLog Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            // covert second argument (level) into lowercase
            transform (arguments[2].begin(), arguments[2].end(), arguments[2].begin(), to_lower());

            try
            {
                oam.updateLog(ENABLEDSTATE, arguments[1], arguments[2]);
                cout << endl << "   Successful Enabling of Logging " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** enableLog Failed :  " << e.what() << endl;
            }
        }
        break;

        case 27: // disableLog
        {
            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** disableLog Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

            // covert second argument (level) into lowercase
            transform (arguments[2].begin(), arguments[2].end(), arguments[2].begin(), to_lower());

            try
            {
                oam.updateLog(MANDISABLEDSTATE, arguments[1], arguments[2]);
                cout << endl << "   Successful Disabling of Logging " << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** disableLog Failed :  " << e.what() << endl;
            }
        }
        break;

        case 28: // switchParentOAMModule
        {
			BRM::DBRM dbrm;
			bool bDBRMReady = dbrm.isDBRMReady();
			string module;
			bool bUseHotStandby = true;
			// First get the values for the standard arguments
			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);
			// Now check for arguments unique to this command. In this case, a valid
			// module name.
			for (int i = 1; i < ArgNum; i++)
			{
				if (arguments[i].size() > 0)
				{
					if (oam.validateModule(arguments[i]) == API_SUCCESS)
					{
						module = arguments[i];
						bUseHotStandby = false;
						break;
					}
				}
			}
			//check if there are more than 1 pm modules to start with
			ModuleTypeConfig moduletypeconfig;
			oam.getSystemConfig("pm", moduletypeconfig);
			if ( moduletypeconfig.ModuleCount < 2 ) 
			{
				cout << endl << "**** switchParentOAMModule Failed : Command only support on systems with Multiple Performance Modules" << endl;
//				break;
			}

			string DBRootStorageType;
			try 
			{
				oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
			}
			catch(...) {}
	
			string GlusterConfig = "n";
			try 
			{
				oam.getSystemConfig( "GlusterConfig", GlusterConfig);
			}
			catch(...)
			{}

			if (DBRootStorageType == "internal" && GlusterConfig == "n")
			{
				cout << endl << "**** switchParentOAMModule Failed : DBRoot Storage type =  internal/non-data-replication" << endl;
//				break;
			}

			if (bUseHotStandby)
            {
				oam.getSystemConfig("StandbyOAMModuleName", module);
				if ( module.empty() || module == oam::UnassignedName ) 
				{
                	cout << endl << "**** switchParentOAMModule Failed : There's no hot standby defined" << endl << "     enter a Performance Module" << endl;
                	break;
				}

				cout << endl << "Switching to the Hot-Standby Parent OAM Module '" << module << "'" << endl;
            }
			else
			{
				parentOAMModule = getParentOAMModule();
				if ( module == parentOAMModule ) 
				{
					cout << endl << "**** switchParentOAMModule Failed : " << module << " is already the Active Parent OAM Module" << endl;
					break;
				}

				cout << endl << "Switching to the Performance Module '" << module << "'" << endl;
			}

			if (bNeedsConfirm) 
			{
				// confirm request
				if (confirmPrompt("This command switches the Active Parent OAM Module and should only be executed on an idle system."))
					break;
			}

            try
            {

                if (!bDBRMReady ||
                    dbrm.isReadWrite() != 0)
                {
                    suspendAnswer = FORCE;
                }

                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager; 
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;
                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }
                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
    //					if (suspendAnswer == FORCE)
    //					{
    //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances")) 
    //						{
    //							break;
    //						}
    //					}
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }
                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }
                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemShutdownPending(true, false, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;
                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemShutdownPending(true, true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;
                    case FORCE:
                        cout << endl << "   Active Parent OAM switching now" << endl;
                        if (bDBRMReady)
                        {
                            dbrm.setSystemShutdownPending(true, false, true);
                        }
                        break;
                    case CANCEL:
                        break;
                }

                if (oam.switchParentOAMModule(module, gracefulTemp))
                {
                    if (waitForActive()) {
                        // give time for new ProcMgr to go active
                        sleep (5);
                        cout << endl << "   Successful Switch Active Parent OAM Module" << endl << endl;
                    }
                    else
                        cout << endl << "**** Switch Active Parent OAM Module failed : check log files" << endl;
                }
                else
                {
                    // give time for new ProcMgr to go active
                    sleep (5);
                    cout << endl << "   Successful Switch Active Parent OAM Module" << endl << endl;
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** switchParentOAMModule Failed :  " << e.what() << endl;
                break;
            }
        }
        break;

        case 29: // getStorageStatus
        {
			SystemStatus systemstatus;
			Oam oam;
		
			cout << "System External DBRoot Storage Statuses" << endl << endl;
			cout << "Component     Status                       Last Status Change" << endl;
			cout << "------------  --------------------------   ------------------------" << endl;
		
			try
			{
				oam.getSystemStatus(systemstatus);

				if ( systemstatus.systemdbrootstatus.dbrootstatus.size() == 0 )
				{
					cout << " No External DBRoot Storage Configured" << endl;
					break;
				}

				for( unsigned int i = 0 ; i < systemstatus.systemdbrootstatus.dbrootstatus.size(); i++)
				{
					if( systemstatus.systemdbrootstatus.dbrootstatus[i].Name.empty() )
						// end of list
						break;
		
					cout << "DBRoot #";
					cout.setf(ios::left);
					cout.width(6);
					cout << systemstatus.systemdbrootstatus.dbrootstatus[i].Name;
					cout.width(29);
					int state = systemstatus.systemdbrootstatus.dbrootstatus[i].OpState;
					printState(state, " ");
					cout.width(24);
					string stime = systemstatus.systemdbrootstatus.dbrootstatus[i].StateChangeDate ;
					stime = stime.substr (0,24);
					cout << stime << endl;
				}
				cout << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** getSystemStatus Failed =  " << e.what() << endl;
			}
        }
        break;

        case 30: // getLogConfig
        {
            try
            {
                SystemLogConfigData systemconfigdata;
                LogConfigData logconfigdata;

                oam.getLogConfig(systemconfigdata);

				string configFileName;
				oam.getSystemConfig("SystemLogConfigFile", configFileName);

                cout << endl << "Calpont System Log Configuration Data" << endl << endl;

				cout << "System Logging Configuration File being used: " <<  configFileName << endl << endl;

                cout << "Module    Configured Log Levels" << endl;
                cout << "------    ---------------------------------------" << endl;

                SystemLogConfigData::iterator pt = systemconfigdata.begin();
                for(; pt != systemconfigdata.end() ; pt++)
                {
                    logconfigdata = *pt;
                    string module = logconfigdata.moduleName;
                    int data = logconfigdata.configData;
                    if ( data < API_MAX )
                    {
                        // failure API status returned
						cout.setf(ios::left);
						cout.width(10);
                        cout << logconfigdata.moduleName;
                        cout << "getLogConfig Failed - Error : " << data << endl;
                    }
                    else
                    {
						cout.setf(ios::left);
						cout.width(10);
                        cout << logconfigdata.moduleName;

                        data = data - API_MAX;
                        if( data == 0 )
                            // no level configured
                            cout << "None Configured" << endl;
                        else
                        {
                            if ( ((data & LEVEL_CRITICAL) ? 1 : 0) == 1 )
                                cout << "Critical ";
                            if ( ((data & LEVEL_ERROR) ? 1 : 0) == 1 )
                                cout << "Error ";
                            if ( ((data & LEVEL_WARNING) ? 1 : 0) == 1 )
                                cout << "Warning ";
                            if ( ((data & LEVEL_INFO) ? 1 : 0) == 1 )
                                cout << "Info ";
                            if ( ((data & LEVEL_DEBUG) ? 1 : 0) == 1 )
                                cout << "Debug ";
                            if ( ((data & LEVEL_DATA) ? 1 : 0) == 1 )
                                cout << "Data";
                            cout << endl;
                        }
                    }
                }
            }
            catch (exception& e)
            {
                cout << endl << "**** getLogConfig Failed :  " << e.what() << endl;
                break;
            }

        }
        break;

        case 31: // movePmDbrootConfig parameters: pm-reside dbroot-list pm-to
        {
			if ( localModule != parentOAMModule ) {
				// exit out since not on active module
				cout << endl << "**** movePmDbrootConfig Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
				break;
			}

			//check the system status / service status and only allow command when System is MAN_OFFLINE
			string cmd = startup::StartUp::installDir() + "/bin/infinidb status > /tmp/status.log";
			system(cmd.c_str());
			if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) 
			{
				SystemStatus systemstatus;
				try {
					oam.getSystemStatus(systemstatus);
		
					if (systemstatus.SystemOpState != oam::MAN_OFFLINE ) {
					cout << endl << "**** movePmDbrootConfig Failed,  System has to be in a MAN_OFFLINE state, stop system first" << endl;
						break;
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** movePmDbrootConfig Failed : " << e.what() << endl;
					break;
				}
				catch(...)
				{
					cout << endl << "**** movePmDbrootConfig Failed,  Failed return from getSystemStatus API" << endl;
					break;
				}
			}

            if (arguments[3] == "")
            {
                // need arguments
                cout << endl << "**** movePmDbrootConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			string residePM = arguments[1];
			string dbrootIDs = arguments[2];
			string toPM = arguments[3];

			string residePMID = residePM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;
			string toPMID = toPM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;

			// check module status
			try{
				bool degraded;
				int opState;
				oam.getModuleStatus(toPM, opState, degraded);

				if (opState == oam::AUTO_DISABLED ||
					opState == oam::MAN_DISABLED)
				{
					cout << "**** movePmDbrootConfig Failed: " << toPM << " is DISABLED." << endl;
					cout << "Run alterSystem-EnableModule to enable module" << endl;
					break;
				}
			}
			catch (exception& ex)
			{}

			bool moveDBRoot1 = false;
			bool found = false;
			boost::char_separator<char> sep(", ");
			boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				if (*it == "1" ) {
					moveDBRoot1 = true;
					break;
				}

				//if gluster, check if toPM is has a copy
				string GlusterConfig;
				try {
					oam.getSystemConfig("GlusterConfig", GlusterConfig);
				}
				catch(...) {}
		
				if ( GlusterConfig == "y" )
				{
					string pmList = "";
					try {
						string errmsg;
						oam.glusterctl(oam::GLUSTER_WHOHAS, *it, pmList, errmsg);
					}
					catch (...)
					{}
	
					boost::char_separator<char> sep(" ");
					boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);
					for ( boost::tokenizer< boost::char_separator<char> >::iterator it1 = tokens.begin();
							it1 != tokens.end();
							++it1)
					{
						if ( *it1 == toPMID )
						{
							found = true;
							break;
						}
					}
				
					if (!found)
					{
						cout << endl << "**** movePmDbrootConfig Failed : Data Redundancy Configured, DBRoot #" << *it << " doesn't have a copy on " << toPM << endl;
						cout << "Run getStorageConfig to get copy information" << endl << endl;
						break;
					}
				}
				else
					found = true;
			}

			if (moveDBRoot1) {
				cout << endl << "**** movePmDbrootConfig Failed : Can't move dbroot #1" << endl << endl;
				break;
			}

			if (!found)
			{
				break;
			}
			

			if (residePM.find("pm") == string::npos ) {
				cout << endl << "**** movePmDbrootConfig Failed : Parmameter 1 is not a Performance Module name, enter 'help' for additional information" << endl;
				break;
			}

			if (toPM.find("pm") == string::npos ) {
				cout << endl << "**** movePmDbrootConfig Failed : Parmameter 3 is not a Performance Module name, enter 'help' for additional information" << endl;
				break;
			}

			if (residePM == toPM ) {
				cout << endl << "**** movePmDbrootConfig Failed : Reside and To Performance Modules are the same" << endl;
				break;
			}

			//get dbroots ids for reside PM
			DBRootConfigList residedbrootConfigList;
            try
            {
                oam.getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

				cout << endl << "DBRoot IDs currently assigned to '" + residePM + "' = ";

				DBRootConfigList::iterator pt = residedbrootConfigList.begin();
				for( ; pt != residedbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != residedbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << residePM << "' : " << e.what() << endl;
				break;
            }

			//get dbroots ids for reside PM
			DBRootConfigList todbrootConfigList;
            try
            {
                oam.getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

				cout << "DBRoot IDs currently assigned to '" + toPM + "' = ";

				DBRootConfigList::iterator pt = todbrootConfigList.begin();
				for( ; pt != todbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != todbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
				break;
            }

			cout << endl << "DBroot IDs being moved, please wait..." << endl << endl;

			try {
				oam.manualMovePmDbroot(residePM, dbrootIDs, toPM);
			}
			catch (...)
			{
				cout << endl << "**** manualMovePmDbroot Failed : API Failure" << endl;
				break;
			}

			//get dbroots ids for reside PM
            try
            {
				residedbrootConfigList.clear();
                oam.getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

				cout << "DBRoot IDs newly assigned to '" + residePM + "' = ";

				DBRootConfigList::iterator pt = residedbrootConfigList.begin();
				for( ; pt != residedbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != residedbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
				break;
            }

            try
            {
				todbrootConfigList.clear();
                oam.getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

				cout << "DBRoot IDs newly assigned to '" + toPM + "' = ";

				DBRootConfigList::iterator pt = todbrootConfigList.begin();
				for( ; pt != todbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != todbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
				break;
            }

        }
        break;

        case 32: // suspendDatabaseWrites
		{
			BRM::DBRM dbrm;
			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

			cout << endl << "This command suspends the DDL/DML writes to the Calpont Database" << endl;
            try
            {

                if (!dbrm.isDBRMReady())
                {
                    cout << endl << "   The Controller Node is not responding.\n   The system can't be set into write suspend mode" << endl <<  flush;
                    break;
                }
                else
                if (dbrm.isReadWrite() != 0)
                {
                    suspendAnswer = FORCE;
                }

                // If there are bulkloads, ddl or dml happening, refuse the request
                if (suspendAnswer == CANCEL)	// We don't have an answer from the command line.
                {
                    // If there are bulkloads, ddl or dml happening, Ask what to do.
                    bool bIsDbrmUp = true;
                    execplan::SessionManager sessionManager; 
                    BRM::SIDTIDEntry blockingsid;
                    std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();
                    bool bActiveTransactions = false;
                    if (!tableLocks.empty())
                    {
                        oam.DisplayLockedTables(tableLocks, &dbrm);
                        bActiveTransactions = true;
                    }
                    if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
                    {
                        cout << endl << "There are active transactions being processed" << endl;
                        bActiveTransactions = true;
                    }

                    if (bActiveTransactions)
                    {
                        suspendAnswer = AskSuspendQuestion(CmdID);
    //					if (suspendAnswer == FORCE)
    //					{
    //						if (confirmPrompt("Force may cause data problems and should only be used in extreme circumstances")) 
    //						{
    //							break;
    //						}
    //					}
                        bNeedsConfirm = false;
                    }
                    else
                    {
                        suspendAnswer = FORCE;
                    }
                }
                if (suspendAnswer == CANCEL)
                {
                    // We're outa here.
                    break;
                }
                if (bNeedsConfirm)
                {
                    if (confirmPrompt(""))
                        break;
                }
                switch (suspendAnswer)
                {
                    case WAIT:
                        cout << endl << "   Waiting for all transactions to complete" << flush;
                        dbrm.setSystemSuspendPending(true, false);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;
                    case ROLLBACK:
                        cout << endl << "   Rollback of all transactions" << flush;
                        dbrm.setSystemSuspendPending(true, true);
                        gracefulTemp = GRACEFUL_WAIT;		// Causes procmgr to wait for all table locks to free and all transactions to finish before shutdown
                        break;
                    case FORCE:
                    case CANCEL:
                    default:
                        gracefulTemp = FORCEFUL;
                        break;
                }

                // stop writes to Calpont Database
                oam.SuspendWrites(gracefulTemp, ackTemp);
            }
			catch (exception& e)
			{
				cout << endl << "**** stopDatabaseWrites Failed: " << e.what() << endl;
			}
			catch(...)
			{
				cout << endl << "**** stopDatabaseWrites Failed" << endl;
				break;
			}
			break;
		}

        case 33: // resumeDatabaseWrites
        {
			if ( arguments[1] != "y" ) {
				if (confirmPrompt("This command resumes the DDL/DML writes to the Calpont Database"))
					break;
			}

			// resume writes to Calpont Database

			try{
				SystemProcessStatus systemprocessstatus;
				BRM::DBRM dbrm;

				dbrm.setSystemSuspended(false);

				oam.getProcessStatus(systemprocessstatus);
				for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
				{
					if (systemprocessstatus.processstatus[i].ProcessName  == "DMLProc")
					{
						oam.setProcessStatus(systemprocessstatus.processstatus[i].ProcessName, systemprocessstatus.processstatus[i].Module, ACTIVE, 1);
					}
					if (systemprocessstatus.processstatus[i].ProcessName  == "DDLProc")
					{
						oam.setProcessStatus(systemprocessstatus.processstatus[i].ProcessName, systemprocessstatus.processstatus[i].Module, ACTIVE, 1);
					}
					if (systemprocessstatus.processstatus[i].ProcessName  == "WriteEngineServer")
					{
						oam.setProcessStatus(systemprocessstatus.processstatus[i].ProcessName, systemprocessstatus.processstatus[i].Module, ACTIVE, 1);
					}
				}
				oam.setSystemStatus(ACTIVE);
				cout << endl << "Resume Calpont Database Writes Request successfully completed" << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** resumeDatabaseWrites Failed: " << e.what() << endl;
			}
			catch(...)
			{
				cout << endl << "**** resumeDatabaseWrites Failed" << endl;
				break;
			}
			break;
		}

        case 34: // AVAILABLE
        {
        }
        break;

        case 35: // assignDbrootPmConfig parameters: pm dbroot-list
        {
			if ( localModule != parentOAMModule ) {
				// exit out since not on active module
				cout << endl << "**** assignDbrootPmConfig Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
				break;
			}

			//check the system status / service status and only allow command when System is MAN_OFFLINE
			string cmd = startup::StartUp::installDir() + "/bin/infinidb status > /tmp/status.log";
			system(cmd.c_str());
			if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) 
			{
				SystemStatus systemstatus;
				try {
					oam.getSystemStatus(systemstatus);
		
					if (systemstatus.SystemOpState != oam::MAN_OFFLINE ) {
					cout << endl << "**** assignDbrootPmConfig Failed,  System has to be in a MAN_OFFLINE state, stop system first" << endl;
						break;
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** assignDbrootPmConfig Failed : " << e.what() << endl;
					break;
				}
				catch(...)
				{
					cout << endl << "**** assignDbrootPmConfig Failed,  Failed return from getSystemStatus API" << endl;
					break;
				}
			}

           if (arguments[2] == "")
            {
                // need atleast 2 arguments
                cout << endl << "**** assignDbrootPmConfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			string dbrootIDs = arguments[1];
			string toPM = arguments[2];

			if (arguments[2].find("pm") == string::npos ) {
				cout << endl << "**** assignDbrootPmConfig Failed : Parmameter 2 is not a Performance Module name, enter 'help' for additional information" << endl;
				break;
			}

			DBRootConfigList dbrootlist;

			boost::char_separator<char> sep(", ");
			boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				dbrootlist.push_back(atoi((*it).c_str()));
			}

			cout << endl;

			//get dbroots ids for reside PM
            try
            {
                oam.assignDbroot(toPM, dbrootlist);

                cout << endl << "   Successfully Assigned DBRoots " << endl << endl;

				try {
					string DBRootStorageType;
					oam.getSystemConfig("DBRootStorageType", DBRootStorageType);

					if (DBRootStorageType == "external" ){
						string GlusterConfig = "n";
						string cloud = "n";
						try {
							oam.getSystemConfig("Cloud", cloud);
							oam.getSystemConfig( "GlusterConfig", GlusterConfig);
						}
						catch(...)
						{}

						if ( GlusterConfig == "n" && cloud != "amazon")
							cout << "   REMINDER: Update the /etc/fstab on " << toPM << " to include these dbroot mounts" << endl << endl;
						break;

					}
				}
				catch(...) {}
		
            }
            catch (exception& e)
            {
                cout << endl << "**** Failed Assign of DBRoots: " << e.what() << endl;
				break;
            }
        }
        break;

        case 36: // getAlarmSummary
        {
			printAlarmSummary();
        }
        break;

        case 37: // getSystemInfo
        {
			printSystemStatus();
			printProcessStatus();
			printAlarmSummary();
        }
        break;

        case 38: // getModuleConfig
        {
            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;
            ModuleConfig moduleconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();
            string returnValue;
            string Argument;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Module Name config parameters

                try
                {
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl << "Module Name Configuration" << endl;

                    for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
                        if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                            // end of list
                            break;

						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if ( moduleCount == 0 )
							// skip if no modules
							continue;

						string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
						{
							string modulename = (*pt).DeviceName;
							string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
							cout << endl << "Module '" << modulename	<< "' Configuration information" << endl << endl;
	
							cout << "ModuleType = " << moduletype << endl;
							cout << "ModuleDesc = " << systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc << " #" << moduleID << endl;

							HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
							for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
							{
								cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
								cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
							}

							DeviceDBRootList::iterator pt3 = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();
							for( ; pt3 != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt3++)
							{
								if ( (*pt3).DeviceID == atoi(moduleID.c_str()) ) {
									cout << "DBRootIDs assigned  = "; 
									DBRootConfigList::iterator pt2 = (*pt3).dbrootConfigList.begin();
									for( ; pt2 != (*pt3).dbrootConfigList.end() ;)
									{
										cout << oam.itoa(*pt2);
										pt2++;
										if (pt2 != (*pt3).dbrootConfigList.end() )
											cout << ", ";
									}
									cout << endl;
								}
							}
						}
                    }
                }
                catch (exception& e)
                {
                    cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
                }
            }
            else
            { // get a single module name info
                if (arguments[2] == "")
                {
                    try
                    {
                        oam.getSystemConfig(arguments[1], moduleconfig);

                        cout << endl << "Module Name Configuration for " << arguments[1] << endl << endl;

                        cout << "ModuleType = " << moduleconfig.ModuleType << endl;
                        cout << "ModuleDesc = " << moduleconfig.ModuleDesc << endl;
						HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
						for( ; pt1 != moduleconfig.hostConfigList.end() ; pt1++)
						{
							cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
							cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
						}

						if ( moduleconfig.ModuleType == "pm" )
						{

							cout << "DBRootIDs assigned  = "; 
	
							DBRootConfigList::iterator pt2 = moduleconfig.dbrootConfigList.begin();
							for( ; pt2 != moduleconfig.dbrootConfigList.end() ; )
							{
								cout << oam.itoa(*pt2);
								pt2++;
								if (pt2 != moduleconfig.dbrootConfigList.end())
									cout << ", ";
							}
							cout << endl << endl;
						}
                    }
                    catch (exception& e)
                    {
                        cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
                    }
                }
                else
                { // get a parameter for a module
                    // get module ID from module name entered, then get parameter
                    oam.getSystemConfig(systemmoduletypeconfig);

                    cout << endl;
                    bool found = false;
                    for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
                    {
						string moduleType = arguments[1].substr(0,MAX_MODULE_TYPE_SIZE);
						string moduleID = arguments[1].substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if ( moduleCount == 0 )
							// skip if no modules
							continue;

                        if(systemmoduletypeconfig.moduletypeconfig[i].ModuleType == moduleType )
                        {
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
							{
								if ( (*pt).DeviceName != arguments[1] )
									continue;

								found = true;
								if ( arguments[2] == "ModuleIPAdd" || arguments[2] == "ModuleHostName") {
									HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
									for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
									{
										if ( arguments[2] == "ModuleIPAdd" )
											cout << "ModuleIPAdd NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).IPAddr << endl;
										else
											cout << "ModuleHostName NIC ID " + oam.itoa((*pt1).NicID) + " = " << (*pt1).HostName << endl;
									}
								}
								else 
								{
									Argument = arguments[2] + oam.itoa(i+1);
									try
									{
										oam.getSystemConfig(Argument, returnValue);
										cout << endl << "   " << arguments[2] << " = " << returnValue << endl << endl;
										break;
									}
									catch (exception& e)
									{
										cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
										break;
									}
								}
							}
                        }
                    }
                    if( !found )
                    {
                        // module name not found
                        cout << endl << "**** getModuleConfig Failed : Invalid Module Name" << endl;
                        break;
                    }
                    cout << endl;
                }
            }
        }
        break;

       case 39: // addExternalDevice
		{
            ExtDeviceConfig extdeviceconfig;

            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** addExternalDevice Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
			}

			// check if ext device exist
			try
			{
				oam.getSystemConfig(arguments[1], extdeviceconfig);
				cout << endl << "**** addExternalDevice Failed : Device already configured" << endl;
				break;
			}
			catch (...)
			{
			}

			extdeviceconfig.Name = arguments[1];
			extdeviceconfig.IPAddr = arguments[2];
			extdeviceconfig.DisableState = oam::ENABLEDSTATE;

			try
			{
				oam.setSystemConfig(arguments[1], extdeviceconfig);
				cout << endl << "   Successfully Added: " << arguments[1] << endl << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** addExternalDevice Failed =  " << e.what() << endl;
				break;
			}
			catch (...)
			{
				cout << endl << "**** addExternalDevice Failed" << endl;
				break;
			}
		}
		break;

        case 40: // getexternaldeviceconfig
        {
            SystemExtDeviceConfig systemextdeviceconfig;
            ExtDeviceConfig extdeviceconfig;
            systemextdeviceconfig.extdeviceconfig.clear();
            string returnValue;
            string Argument;

            if (arguments[1] == "all" || arguments[1] == "")
            {

                // get and all display Ext Devices Name config parameters

                try
                {
                    oam.getSystemConfig(systemextdeviceconfig);

                    cout << endl << "External Device Configuration" << endl << endl;

					for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
					{
						cout << "Name = ";
						cout.setf(ios::left);
						cout.width(30);
						cout << systemextdeviceconfig.extdeviceconfig[i].Name;
						cout << "IPAddr = " << systemextdeviceconfig.extdeviceconfig[i].IPAddr << endl;
					}

					cout << endl;
                }
                catch (exception& e)
                {
                    cout << endl << "**** getextdeviceconfig Failed =  " << e.what() << endl;
                }
            }
            else
            { // get a single ext device name info
				try
				{
					oam.getSystemConfig(arguments[1], extdeviceconfig);

						cout << endl << "Name = ";
						cout.setf(ios::left);
						cout.width(30);
						cout << extdeviceconfig.Name;
						cout << "IPAddr = " << extdeviceconfig.IPAddr << endl;
					cout << endl;
				}
				catch (...)
				{
					cout << endl << "**** getextdeviceconfig Failed, invalid device name" << endl;
				}
           }
        }
        break;

        case 41: // setexternaldeviceconfig - parameters: Ext Device name, Parameter name and value
        {
            ExtDeviceConfig extdeviceconfig;
            string Argument;

			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setexternaldeviceconfig Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[3] == "")
            {
                // need 3 arguments
                cout << endl << "**** setextdeviceconfig Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }
			if ( arguments[3] == "=" ) {
				cout << endl << "**** setextdeviceconfig Failed : Invalid Value of '=', please re-enter" << endl;
				break;
			}

			// check if ext device exist
			try
			{
				oam.getSystemConfig(arguments[1], extdeviceconfig);
			}
			catch (...)
			{
				cout << endl << "**** getextdeviceconfig Failed : Invalid device name " << arguments[1] << endl;
				break;
			}

			if ( arguments[2] == "Name" )
				extdeviceconfig.Name = arguments[3];
			else
				if ( arguments[2] == "IPAddr")
					extdeviceconfig.IPAddr = arguments[3];
				else {
					cout << endl << "**** setextdeviceconfig Failed : Invalid Parameter, please re-enter" << endl;
					break;
				}
			try
			{
				oam.setSystemConfig(arguments[1], extdeviceconfig);
                cout << endl << "   Successfully set " << arguments[2] << " = " << arguments[3] << endl << endl;
			}
			catch (...)
			{
				cout << endl << "**** setextdeviceconfig Failed : Invalid device name " << arguments[1] << endl;
				break;
			}

         }
        break;

       case 42: // removeExternalDevice
		{
            ExtDeviceConfig extdeviceconfig;

            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** removeExternalDevice Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
			}

			// check if ext device exist
			try
			{
				oam.getSystemConfig(arguments[1], extdeviceconfig);
			}
			catch (...)
			{
				cout << endl << "**** removeExternalDevice Failed : Device not configured" << endl;
				break;
			}

			extdeviceconfig.Name = oam::UnassignedName;
			extdeviceconfig.IPAddr = oam::UnassignedIpAddr;

			try
			{
				oam.setSystemConfig(arguments[1], extdeviceconfig);
				cout << endl << "   Successfully removed: " << arguments[1] << endl << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** removeExternalDevice Failed =  " << e.what() << endl;
				break;
			}
			catch (...)
			{
				cout << endl << "**** removeExternalDevice Failed" << endl;
				break;
			}
		}
        break;

        case 43: // assignElasticIPAddress
		{
			//get cloud configuration data
			string cloud = "n";
			try{
				oam.getSystemConfig("Cloud", cloud);
			}
			catch(...) {}

			if ( cloud != "amazon" )
			{
				cout << endl << "**** assignElasticIPAddress Not Supported : For Amazon Systems only" << endl;
				break;
			}

            if (arguments[2] == "")
            {
                // need 2 arguments
                cout << endl << "**** assignElasticIPAddress Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
			}

			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** assignElasticIPAddress Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

			string IPaddress = arguments[1];
			string moduleName = arguments[2];

			if ( oam.validateModule(moduleName) != API_SUCCESS) {
				cout << endl << "**** assignElasticIPAddress Failed : Invalid Module name" << endl;
				break;
			}

			if ( moduleName == localModule )
			{
				if ( arguments[3] != "y") {
					string warning = "Warning: Assigning Elastic IP Address to local module will lock up this terminal session.";
					// confirm request
					if (confirmPrompt(warning))
						break;
				}
			}

			//check and add Elastic IP Address
			int AmazonElasticIPCount = 0;
			try{
				oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
			}
			catch(...) {
				AmazonElasticIPCount = 0;
			}

			bool found = false;
			int id = 1;
			for (  ; id < AmazonElasticIPCount+1 ; id++ )
			{
				string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
				string ELmoduleName;
				string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
				string ELIPaddress;
				try{
					oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
					oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
				}
				catch(...) {}

				if ( ELmoduleName == moduleName &&
						ELIPaddress == IPaddress)
				{	//assign again incase it got unconnected
					//get instance id
					string instanceName = oam::UnassignedName;
					try
					{
						ModuleConfig moduleconfig;
						oam.getSystemConfig(moduleName, moduleconfig);
						HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
						instanceName = (*pt1).HostName;
					}
					catch(...)
					{}
		
					try{
						oam.assignElasticIP(instanceName, IPaddress);
            			cout << endl << "   Successfully completed Assigning Elastic IP Address " << endl << endl;
					}
					catch(...) {}
					found = true;
                	break;
				}

				if ( ELmoduleName == moduleName )
				{	
                	cout << endl << "**** assignElasticIPAddress Failed : module already assigned IP Address " << ELIPaddress << endl;
					found = true;
                	break;
				}

				if ( ELIPaddress == IPaddress )
				{
                	cout << endl << "**** assignElasticIPAddress Failed : IP Address already assigned to module " << ELmoduleName << endl;
					found = true;
                	break;
				}
			}

			if (found)
				break;

			AmazonElasticIPCount++;

			//get instance id
			string instanceName = oam::UnassignedName;
			try
			{
				ModuleConfig moduleconfig;
				oam.getSystemConfig(moduleName, moduleconfig);
				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				instanceName = (*pt1).HostName;
			}
			catch(...)
			{}

			try{
				oam.assignElasticIP(instanceName, IPaddress);
			}
			catch(...) {
                cout << endl << "**** assignElasticIPAddress Failed : assignElasticIP API Error" << endl;
				break;
			}

			//add to configuration
			string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
			string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);

			Config* sysConfig = Config::makeConfig();
			try {
				sysConfig->setConfig("Installation", "AmazonElasticIPCount", oam.itoa(AmazonElasticIPCount));
				sysConfig->setConfig("Installation", AmazonElasticModule, moduleName);
				sysConfig->setConfig("Installation", AmazonElasticIPAddr, IPaddress);
				sysConfig->write();
			}
			catch(...)
			{
				cout << "ERROR: Problem setting AmazonElasticModule in the Calpont System Configuration file" << endl;
				break;
			}

            cout << endl << "   Successfully completed Assigning Elastic IP Address " << endl << endl;
		}
        break;

        case 44: // unassignElasticIPAddress
		{
			//get cloud configuration data
			string cloud = "n";
			try{
				oam.getSystemConfig("Cloud", cloud);
			}
			catch(...) {}

			if ( cloud != "amazon" )
			{
				cout << endl << "**** unassignElasticIPAddress Not Supported : For Amazon Systems only" << endl;
				break;
			}

            if (arguments[1] == "")
            {
                // need 2 arguments
                cout << endl << "**** unassignElasticIPAddress Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
			}

			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** unassignElasticIPAddress Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

			string IPaddress = arguments[1];

			//check and add Elastic IP Address
			int AmazonElasticIPCount = 0;
			try{
				oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
			}
			catch(...) {
				AmazonElasticIPCount = 0;
			}

			bool found = false;
			int id = 1;
			for (  ; id < AmazonElasticIPCount+1 ; id++ )
			{
				string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
				string ELmoduleName;
				string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
				string ELIPaddress;
				try{
					oam.getSystemConfig(AmazonElasticIPAddr, ELmoduleName);
					oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
				}
				catch(...) {}

				if ( ELIPaddress == IPaddress )
				{
					found = true;
					try{
						oam.deassignElasticIP(IPaddress);
					}
					catch(...) {
						cout << endl << "**** deassignElasticIPAddress Failed : deassignElasticIP API Error";
						break;
					}
		
					int oldAmazonElasticIPCount = AmazonElasticIPCount;

					Config* sysConfig = Config::makeConfig();
					//move up any others
					if ( oldAmazonElasticIPCount > id )
					{
						for ( int newid = id+1 ; newid < oldAmazonElasticIPCount+1 ; newid++ )
						{
							AmazonElasticModule = "AmazonElasticModule" + oam.itoa(newid);
							AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(newid);

							try{
								oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
								oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
							}
							catch(...) {}

							AmazonElasticModule = "AmazonElasticModule" + oam.itoa(newid-1);
							AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(newid-1);

							try{
								oam.setSystemConfig(AmazonElasticModule, ELmoduleName);
								oam.setSystemConfig(AmazonElasticIPAddr, ELIPaddress);
							}
							catch(...) {}
						}
					}

					AmazonElasticModule = "AmazonElasticModule" + oam.itoa(oldAmazonElasticIPCount);
					AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(oldAmazonElasticIPCount);

					//delete last entry and update count
					AmazonElasticIPCount--;
					try {
						sysConfig->setConfig("Installation", "AmazonElasticIPCount", oam.itoa(AmazonElasticIPCount));
						sysConfig->delConfig("Installation", AmazonElasticModule);
						sysConfig->delConfig("Installation", AmazonElasticIPAddr);
						sysConfig->write();
					}
					catch(...)
					{
						cout << "ERROR: Problem setting AmazonElasticModule in the Calpont System Configuration file" << endl;
						break;
					}
				}
			}

			if (!found) {
            	cout << endl << "   Elastic IP Address " << IPaddress << " not assigned to a module" << endl << endl;
				break;
			}

            cout << endl << "   Successfully completed Unassigning Elastic IP Address " << endl << endl;

		}
        break;

        case 45: // getSystemNetworkConfig
        {
			// get and display Module Network Config
            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

			//check and add Elastic IP Address
			int AmazonElasticIPCount = 0;
			try{
				oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
			}
			catch(...) {
				AmazonElasticIPCount = 0;
			}

			// get max length of a host name for header formatting

			int maxSize = 9;
			try
			{
				oam.getSystemConfig(systemmoduletypeconfig);

				for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
				{
					if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
						// end of list
						break;

					int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                    string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

					if ( moduleCount > 0 )
					{
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
						{
							HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
							for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
							{
								if ( maxSize < (int) (*pt1).HostName.size() )
									maxSize = (*pt1).HostName.size();
							}
						}
					}
				}
			}
			catch (exception& e)
			{
				cout << endl << "**** getSystemNetworkConfig Failed =  " << e.what() << endl;
			}

			cout << endl << "System Network Configuration" << endl << endl;

			cout.setf(ios::left);
			cout.width(15);
			cout << "Module Name";
			cout.width(30);
			cout << "Module Description";
			cout.width(10);
			cout << "NIC ID";
			cout.width(maxSize + 5);
			cout << "Host Name";
			cout.width(20);
			cout << "IP Address";
			cout.width(14);
			cout << "Status";
			if ( AmazonElasticIPCount > 0 )
			{
				cout.width(20);
				cout << "Elastic IP Address";
			}
			cout << endl;
			cout.width(15);
			cout << "-----------";
			cout.width(30);
			cout << "-------------------------";
			cout.width(10);
			cout << "------";
			for ( int i=0 ; i < maxSize ; i++ )
			{
				cout << "-";
			}
			cout << "     ";
			cout.width(20);
			cout << "---------------";
			cout.width(14);
			cout << "------------";
			if ( AmazonElasticIPCount > 0 )
			{
				cout.width(20);
				cout << "------------------";
			}
			cout << endl;

			try
			{
				oam.getSystemConfig(systemmoduletypeconfig);

				for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
				{
					if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
						// end of list
						break;

					int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
                    string moduletypedesc = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;

					if ( moduleCount > 0 )
					{
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
						{
							string modulename = (*pt).DeviceName;
							string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
							string modulenamedesc = moduletypedesc + " #" + moduleID;

							cout.setf(ios::left);
							cout.width(15);
							cout << modulename;
							cout.width(33);
							cout << modulenamedesc;

							HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
							for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
							{
								string ipAddr = (*pt1).IPAddr;
								string hostname = (*pt1).HostName;
								string nicID = oam.itoa((*pt1).NicID);
								int state;

                                if ( nicID != "1" ) { 
                                    cout.width(48); 
                                    cout << " "; 
                                } 

								cout.width(7);
								cout << nicID;
								cout.width(maxSize + 5);
								cout << hostname;
								cout.width(20);
								cout << ipAddr;
								cout.width(14);

								try {
									oam.getNICStatus(hostname, state);
		
									printState(state, " ");
								}
								catch (exception& e)
								{
									cout << INITIALSTATE;
								}

								if ( nicID == "1" && AmazonElasticIPCount > 0 )
								{
									int id = 1;
									for (  ; id < AmazonElasticIPCount+1 ; id++ )
									{
										string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
										string ELmoduleName;
										string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
										string ELIPaddress;
										try{
											oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
											oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
										}
										catch(...) {}

										if ( modulename == ELmoduleName )
										{
											cout.width(20);
											cout << ELIPaddress;
											break;
										}
									}
								}
								cout << endl;
							}
						}
					}
				}
			}
			catch (exception& e)
			{
				cout << endl << "**** getSystemNetworkConfig Failed =  " << e.what() << endl;
			}

			//get cloud configuration data
			string cloud = "n";
			try{
				oam.getSystemConfig("Cloud", cloud);
			}
			catch(...) {}

			if ( cloud == "amazon" )
			{
				cout << endl << "Amazon Instance Configuration" << endl << endl;

				string PMInstanceType = oam::UnassignedName;
				string UMInstanceType = oam::UnassignedName;
				try{
					oam.getSystemConfig("PMInstanceType", PMInstanceType);
					oam.getSystemConfig("UMInstanceType", UMInstanceType);

					cout << "PMInstanceType = " << PMInstanceType << endl;
					cout << "UMInstanceType = " << UMInstanceType << endl;
				}
				catch(...) {}
			}

			cout << endl;

			// get and all display Ext Devices Name config parameters

			try
			{
	            SystemExtDeviceConfig systemextdeviceconfig;
				oam.getSystemConfig(systemextdeviceconfig);
		
				if ( systemextdeviceconfig.Count == 0 )
					break;

				cout << endl << "External Device Configuration" << endl << endl;

				cout.setf(ios::left);
				cout.width(30);
				cout << "Device Name";
				cout.width(20);
				cout << "IP Address";
				cout.width(10);
				cout << "Status";
				cout << endl;
				cout.width(30);
				cout << "---------------------";
				cout.width(20);
				cout << "---------------";
				cout.width(12);
				cout << "------------";
				cout << endl;

				for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
				{
					cout.setf(ios::left);
					cout.width(30);
					cout << systemextdeviceconfig.extdeviceconfig[i].Name;
					cout.width(20);
					cout << systemextdeviceconfig.extdeviceconfig[i].IPAddr;
					cout.width(12);

					int state;
					try {
						oam.getExtDeviceStatus(systemextdeviceconfig.extdeviceconfig[i].Name, state);

						printState(state, " ");
					}
					catch (exception& e)
					{
						cout << INITIALSTATE;
					}
					cout << endl;
				}

				cout << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** getextdeviceconfig Failed =  " << e.what() << endl;
			}

			cout << endl;

			break;
		}

        case 46: // AVAILABLE
        {
		}

        case 47: // getCalpontSoftwareInfo
        {
			cout << endl;
			if ( rootUser)
			{
				system("rpm -qi infinidb-platform > /tmp/calpont.txt 2>&1");
				if (oam.checkLogStatus("/tmp/calpont.txt", "Name"))
					system("cat /tmp/calpont.txt");
				else {
					system("dpkg -s calpont > /tmp/calpont.txt 2>&1");
					if (oam.checkLogStatus("/tmp/calpont.txt", "Status: install"))
						system("cat /tmp/calpont.txt");
					else {
						SystemSoftware systemsoftware;
						oam.getSystemSoftware(systemsoftware);
	
						cout << "SoftwareVersion = " << systemsoftware.Version << endl;
						cout << "SoftwareRelease = " << systemsoftware.Release << endl;
					}
				}
			}
			else
			{
				SystemSoftware systemsoftware;
				oam.getSystemSoftware(systemsoftware);

				cout << "SoftwareVersion = " << systemsoftware.Version << endl;
				cout << "SoftwareRelease = " << systemsoftware.Release << endl;
			}
			cout << endl;
			break;
		}

        case 48: // addModule - parameters: Module type/Module Name, Number of Modules, Server Hostnames,
					// Server root password optional
        {
            if (arguments[1] == "")
            {
                // need at least  arguments
                cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			switch ( serverInstallType ) {
				case (oam::INSTALL_COMBINE_DM_UM_PM):
				{
					if (arguments[1].find("um") != string::npos ) {
		                cout << endl << "**** addModule Failed : User Module Types not supported on this Combined Server Installation" << endl;
						return(0);
					}
					break;
				}
			}

			string GlusterConfig = "n";
			int GlusterCopies;
			string cloud = "n";
			string GlusterStorageType;
			try {
				oam.getSystemConfig("GlusterConfig", GlusterConfig);
				oam.getSystemConfig("GlusterCopies", GlusterCopies);
				oam.getSystemConfig("Cloud", cloud);
				oam.getSystemConfig("GlusterStorageType", GlusterStorageType);
			}
			catch(...) {}

			ModuleTypeConfig moduletypeconfig;
			DeviceNetworkConfig devicenetworkconfig;
			DeviceNetworkList devicenetworklist;
			DeviceNetworkList enabledevicenetworklist;
			HostConfig hostconfig;

			string moduleType;
			string moduleName;
			int moduleCount;
			string password;
			typedef std::vector<string> hostNames;
			hostNames hostnames;
			typedef std::vector<string> umStorageNames;
			umStorageNames umstoragenames;
			int hostArg;
			int dbrootPerPM = 0;

			//check if module type or module name was entered
			if ( arguments[1].size() == 2 ) 
			{	//Module Type was entered
				if (arguments[3] == "" && cloud == "n")
				{
					// need at least  arguments
					cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
					break;
				}

				//Module Type was entered
				moduleType = arguments[1];
				moduleCount = atoi(arguments[2].c_str());
				hostArg = 3;
				if (arguments[4] != "")
					password = arguments[4];
				else
					password = "ssh";

				if (arguments[5] != "")
					dbrootPerPM = atoi(arguments[5].c_str());
			}
			else
			{
				//Module Name was entered
				if (arguments[2] == "" && cloud == "n")
				{
					// need at least  arguments
					cout << endl << "**** addModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
					break;
				}

				moduleName = arguments[1];
				moduleType = arguments[1].substr(0,MAX_MODULE_TYPE_SIZE);
				moduleCount = 1;
				hostArg = 2;
				if (arguments[3] != "")
					password = arguments[3];
				else
					password = "ssh";

				if (arguments[4] != "")
					dbrootPerPM = atoi(arguments[4].c_str());
			}

//do we needed this check????
			if ( moduleCount < 1 || moduleCount > 10  ) {
				cout << endl << "**** addModule Failed : Failed to Add Module, invalid number-of-modules entered (1-10)" << endl;
				break;
			}

			if ( GlusterConfig == "y" && moduleType == "pm" ) {
				if ( localModule != parentOAMModule ) {
					// exit out since not on active module
					cout << endl << "**** addModule Failed : Can only run command on Active OAM Parent Module (" << parentOAMModule << ")." << endl;
					break;
				}

				if ( fmod((float) moduleCount , (float) GlusterCopies) != 0 ) {
					cout << endl << "**** addModule Failed : Failed to Add Module, invalid number-of-modules: must be multiple of Data Redundancy Copies, which is " << GlusterCopies << endl;
					break;
				}
			}

			//check and parse Server Hostname
			if (arguments[hostArg] != "") {
				boost::char_separator<char> sep(", ");
				boost::tokenizer< boost::char_separator<char> > tokens(arguments[hostArg], sep);
				for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
						it != tokens.end();
						++it)
				{
					hostnames.push_back(*it);
				}
			}

			if ( hostnames.size() < (unsigned) moduleCount ) {
				if ( cloud != "amazon" )
				{
					cout << endl << "**** addModule Failed : Failed to Add Module, number of hostnames is less than Module Count" << endl;
					break;
				}
				else
				{
					cout << endl << "number of Instance-IDs (" << hostnames.size() << ") is less than Module Count (" << moduleCount << "), will launch new Instance(s)" << endl;
					for ( int id = hostnames.size() ; id < moduleCount ; id++ )
					{
						hostnames.push_back(oam::UnassignedName);
					}
				}
			}

			//get configured moduleNames
			try{
				oam.getSystemConfig(moduleType, moduletypeconfig);
			}
			catch(...)
			{
				cout << endl << "**** addModule Failed : Failed to Add Module, getSystemConfig API Failed" << endl;
				break;
			}

			//get module names already in-use and Number of NIC IDs for module
			typedef std::vector<string>	moduleNameList;
			moduleNameList modulenamelist;
			int nicNumber=1;

			DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
			for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
			{
				modulenamelist.push_back((*pt).DeviceName);
				HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
				for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
				{
					if ( (*pt1).HostName != oam::UnassignedName ) {
						if ( nicNumber < (*pt1).NicID )
							nicNumber = (*pt1).NicID;
					}
				}
			}

			if ( ((unsigned) nicNumber * moduleCount) != hostnames.size() && cloud == "n" ) {
				cout << endl << "**** addModule Failed : Failed to Add Module, invalid number of hostNames entered. Enter " + oam.itoa(nicNumber * moduleCount) +  " hostname(s), which is the number of NICs times the number of modules" << endl;
				break;
			}

			int moduleID = 1;
			hostNames::const_iterator listPT1 = hostnames.begin();
			umStorageNames::const_iterator listPT2 = umstoragenames.begin();
			for ( int i = 0 ; i < moduleCount ; i++ )
			{
				//validate or determine module name
				moduleNameList::const_iterator listPT = modulenamelist.begin();
				for( ; listPT != modulenamelist.end() ; listPT++)
				{
					if ( !moduleName.empty() ) {
						//add by moduleName, validate that Entered module name doesn't exist
						if ( moduleName == (*listPT) ) {
							cout << endl << "**** addModule Failed : Module Name already exist" << endl;
							return 1;
						}
					}
					else
					{
						//add by moduleType, get available module name
						string newModuleName = moduleType + oam.itoa(moduleID);
						if ( newModuleName == (*listPT) )
							moduleID++;
						else
						{
							moduleName = newModuleName;
							moduleID++;					
							break;
						}
					}
				}

				if ( moduleName.empty() ) {
					moduleName = moduleType + oam.itoa(moduleID);
					moduleID++;
				}

				// store module name
				devicenetworkconfig.DeviceName = moduleName;
				enabledevicenetworklist.push_back(devicenetworkconfig);

				for ( int j = 0 ; j < nicNumber ; j ++ )
				{
					//get/check Server Hostnames IP address
					string hostName = *listPT1;
					//get Network IP Address
					string IPAddress;
					if ( cloud == "amazon")
					{
						if ( hostName != oam::UnassignedName )
						{
							IPAddress = oam.getEC2InstanceIpAddress(hostName);
							if (IPAddress == "stopped" || IPAddress == "terminated") {
								cout << "ERROR: Instance " + hostName + " not running, please start and retry" << endl << endl;
								return 1;
							}
						}
						else
							IPAddress = oam::UnassignedName;
					}
					else
					{
						IPAddress = oam.getIPAddress(hostName);
						if ( IPAddress.empty() ) {
							// prompt for IP Address
							string prompt = "IP Address of " + hostName + " not found, enter IP Address or enter 'abort'";
							IPAddress = dataPrompt(prompt);
							if ( IPAddress == "abort" || !oam.isValidIP(IPAddress))
								return 1;
						}
					}

					hostconfig.IPAddr = IPAddress;
					hostconfig.HostName = hostName;
					hostconfig.NicID = j+1;
					devicenetworkconfig.hostConfigList.push_back(hostconfig);
					listPT1++;
				}

				devicenetworklist.push_back(devicenetworkconfig);
				devicenetworkconfig.hostConfigList.clear();
				moduleName.clear();
			}

			DBRootConfigList dbrootlist;
			int dbrootNumber=-1;
			typedef std::vector<string>	storageDeviceList;
			storageDeviceList storagedevicelist;
			string deviceType;

			if ( GlusterConfig == "y" && moduleType == "pm")
			{
				cout << endl << "System is configured with InfiniDB Data Redundancy, DBRoot Storage will" << endl;
				cout << "will be created with the Modules during this command." << endl;
				cout << "Also the InfiniDB Data Redundancy Packages should already be installed on the" << endl;
				cout << "Performance Modules being added and password-less ssh should be setup on those modules.";

				// confirm request
				if (confirmPrompt(" "))
					break;

				if ( dbrootPerPM == 0) {
					cout << endl;
					// prompt for number of DBRoot
					string prompt = "Number of DBRoots Per Performance Module you want to add";
					dbrootPerPM = atoi(dataPrompt(prompt).c_str());
				}
				else
					cout << endl << "Number of DBRoots Per Performance Module to be added is " << oam.itoa(dbrootPerPM) << endl;
				
				dbrootNumber = dbrootPerPM * moduleCount;

				if ( GlusterStorageType == "storage" )
				{
					cout << endl << "Data Redundancy Storage Type is configured for 'storage'" << endl;

					cout << "You will need " << oam.itoa(dbrootNumber*GlusterCopies);
					cout << " total storage locations and " << oam.itoa(dbrootPerPM*GlusterCopies) << " storage locations per PM. You will now " << endl;
					cout << "be asked to enter the device names for the storage locations. You will enter " << endl;
					cout << "them for each PM, on one line, separated by spaces (" << oam.itoa(dbrootPerPM*GlusterCopies) << " names on each line)." << endl;

					DeviceNetworkList::iterator pt = devicenetworklist.begin();
					string firstPM = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
					for( ; pt != devicenetworklist.end() ; pt++)
					{
						cout << endl;
						string prompt = "Storage Device Names for " + (*pt).DeviceName;
						string devices = dataPrompt(prompt);
						storagedevicelist.push_back(devices);
					}

					cout << endl;
					string prompt = "Filesystem type for these storage locations (ext2,ext3,xfs,etc)";
					deviceType = dataPrompt(prompt);
				}

			}

			try{
				cout << endl << "Adding Modules ";
				DeviceNetworkList::iterator pt = devicenetworklist.begin();
				string firstPM = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
				for( ; pt != devicenetworklist.end() ; pt++)
				{
					cout << (*pt).DeviceName << ", ";
				}

				cout << "please wait..." << endl;

				oam.addModule(devicenetworklist, password);

				cout << "Add Module(s) successfully completed" << endl;

				if ( GlusterConfig == "y" && moduleType == "pm" ) {

					if ( GlusterStorageType == "storage" ) {
						//send messages to update fstab to new modules, if needed
						DeviceNetworkList::iterator pt2 = devicenetworklist.begin();
						storageDeviceList::iterator pt3 = storagedevicelist.begin();
						for( ; pt2 != devicenetworklist.end() ; pt2++, pt3++)
						{
							string moduleName = (*pt2).DeviceName;
							string devices = *pt3;
							int brinkID = 1;
							boost::char_separator<char> sep(" ");
							boost::tokenizer< boost::char_separator<char> > tokens(devices, sep);
							for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
									it != tokens.end();
									++it)
							{
								string deviceName = *it;
								string entry = deviceName + " " + startup::StartUp::installDir() + "/gluster/brick" + oam.itoa(brinkID)  + " " + deviceType + " defaults 1 2";
	
								//send update pm
								oam.distributeFstabUpdates(entry, moduleName);
								brinkID++;
							}
						}
					}

					//enable modules
					try
					{
						cout << endl << "Enabling Modules " << endl;
						oam.enableModule(enabledevicenetworklist);
						cout << "Successful Enable of Modules " << endl;
					}
					catch (exception& e)
					{
						cout << endl << "**** enableModule Failed :  " << e.what() << endl;
						break;
					}

					cout << endl << "Adding DBRoots" << endl;
	
					//add dbroots
					string firstDBroot;
					try
					{
						oam.addDbroot(dbrootNumber, dbrootlist);
		
						cout << "New DBRoot IDs added = ";
						DBRootConfigList::iterator pt1 = dbrootlist.begin();
						firstDBroot = oam.itoa(*pt1);
						for( ; pt1 != dbrootlist.end() ;)
						{
							cout << oam.itoa(*pt1);
							pt1++;
							if (pt1 != dbrootlist.end())
								cout << ", ";
						}
						cout << endl;
		
					}
					catch (exception& e)
					{
						cout << endl << "**** addDbroot Failed: " << e.what() << endl;
						break;
					}

					cout << endl << "Assigning DBRoots" << endl << endl;

					DeviceNetworkList::iterator pt = devicenetworklist.begin();
					DBRootConfigList::iterator pt1 = dbrootlist.begin();
					for( ; pt != devicenetworklist.end() ; pt++)
					{
						string moduleName = (*pt).DeviceName;
	
						DBRootConfigList dbrootlist;
	
						for( int dbrootNum = 0; dbrootNum < dbrootPerPM ; dbrootNum++)
						{
							dbrootlist.push_back(*pt1);
							pt1++;
						}
	
						//assign dbroots to pm
						try
						{
							oam.assignDbroot(moduleName, dbrootlist);
			
							cout << endl << "Successfully Assigned DBRoots " << endl;
			
						}
						catch (exception& e)
						{
							cout << endl << "**** Failed Assign of DBRoots: " << e.what() << endl;
							break;
						}
					}

					cout << endl << "Run Data Redundancy Add DBRoots" << endl;

					try {
						string errmsg;
						oam.glusterctl(oam::GLUSTER_ADD, firstPM, firstDBroot, errmsg);

						cout << endl << "Successfully Completed Data Redundancy Add DBRoots " << endl;

					}
					catch (...)
					{
						cout << endl << "**** glusterctl GLUSTER_ADD Failed" << endl;
						break;
					}

					cout << endl << "addModule Command Successfully completed: Run startSystem command to Activate newly added Performance Modules" << endl << endl;
				}
				else
				{
					cout << "addModule Command Successfully completed: Modules are Disabled, run alterSystem-enableModule command to enable them" << endl << endl;
				}

			}
			catch (exception& e)
			{
				cout << endl << "**** addModule Failed: " << e.what() << endl;
			}
			catch(...)
			{
				cout << endl << "**** addModule Failed : Failed to Add Module" << endl;
			}
			break;
		}

        case 49: // removeModule - parameters: Module name/type, number-of-modules
        {
            if (arguments[1] == "")
            {
                // need atleast 1 arguments
                cout << endl << "**** removeModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			parentOAMModule = getParentOAMModule();
			if ( arguments[1] == parentOAMModule ) {
				// exit out since you can't manually remove OAM Parent Module
                cout << endl << "**** removeModule Failed : can't manually remove the Active OAM Parent Module." << endl;
                break;
			}

			switch ( serverInstallType ) {
				case (oam::INSTALL_COMBINE_DM_UM_PM):
				{
					if (arguments[1].find("um") != string::npos ) {
		                cout << endl << "**** removeModule Failed : User Modules not supported on the Combined  Server Installation" << endl;
						return 0;
					}
				}
			}

			ModuleTypeConfig moduletypeconfig;
			DeviceNetworkConfig devicenetworkconfig;
			DeviceNetworkList devicenetworklist;
			bool quit = false;

			string moduleType;

			//check if module type or module name was entered
			if ( arguments[1].size() == 2 ) {
				//Module Type was entered

				if ( arguments[3] != "y") {
					cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
					string warning = "This command does a remove a module from the Calpont System";
					// confirm request
					if (confirmPrompt(warning))
						break;
				}

				int moduleCount = atoi(arguments[2].c_str());
				if ( moduleCount < 1 || moduleCount > 10  ) {
					cout << endl << "**** removeModule Failed : Failed to Remove Module, invalid number-of-modules entered (1-10)" << endl;
					break;
				}

				cout << endl;

				moduleType = arguments[1];

				//store moduleNames
				try{
					oam.getSystemConfig(moduleType, moduletypeconfig);
				}
				catch(...)
				{
					cout << endl << "**** removeModule Failed : Failed to Remove Module, getSystemConfig API Failed" << endl;
					break;
				}
			
				int currentModuleCount = moduletypeconfig.ModuleCount;

				if ( moduleCount > currentModuleCount ) {
					cout << endl << "**** removeModule Failed : Failed to Remove Module, mount count entered to larger than configured" << endl;
					break;
				}
				if ( moduleCount == currentModuleCount ) {
					if ( moduleType == "pm" ) {
						cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last Director Module" << endl;
						break;
					}
				}

				//get module names in-use
				typedef std::vector<string>	moduleNameList;
				moduleNameList modulenamelist;
	
				DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
				for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
				{
					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					if ( (*pt1).HostName != oam::UnassignedName )
						modulenamelist.push_back((*pt).DeviceName);
				}
	
				moduleNameList::reverse_iterator pt1 = modulenamelist.rbegin();
				for ( int i=0 ; i < moduleCount ; i++)
				{
					devicenetworkconfig.DeviceName = *pt1;
					pt1++;
					devicenetworklist.push_back(devicenetworkconfig);
				}
			}
			else
			{
				//Module Name was entered

				if ( arguments[2] != "y") {
					cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
					string warning = "This command does a stop and remove a module from the Calpont System";
					// confirm request
					if (confirmPrompt(warning))
						break;
				}

				cout << endl;

				//parse module names
				boost::char_separator<char> sep(", ");
				boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);
				for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
						it != tokens.end();
						++it)
				{
					devicenetworkconfig.DeviceName = *it;
					devicenetworklist.push_back(devicenetworkconfig);

					moduleType = (*it).substr(0,MAX_MODULE_TYPE_SIZE);
	
					try{
						oam.getSystemConfig(moduleType, moduletypeconfig);
					}
					catch(...)
					{
						cout << endl << "**** removeModule Failed : Failed to Remove Module, getSystemConfig API Failed" << endl;
						quit = true;
						break;
					}
				
					int currentModuleCount = moduletypeconfig.ModuleCount;
	
					if ( moduleType == "pm" && currentModuleCount == 1) {
						cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last Performance Module" << endl;
						quit = true;
						break;
					}

					if ( moduleType == "um" && currentModuleCount == 1) {
						cout << endl << "**** removeModule Failed : Failed to Remove Module, you can't remove last User Module" << endl;
						quit = true;
						break;
					}
				}
			}

			if (quit)
				break;

			DeviceNetworkList::iterator pt = devicenetworklist.begin();
			DeviceNetworkList::iterator endpt = devicenetworklist.end();

			// check for module status and if any dbroots still assigned
			for( ; pt != endpt ; pt++)
			{
				if ( moduleType == "pm" ) {
					// check for dbroots assigned
					DBRootConfigList dbrootConfigList;
					string moduleID = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
					try {
						oam.getPmDbrootConfig(atoi(moduleID.c_str()), dbrootConfigList);
					}
					catch(...) 
					{}
					
					if ( !dbrootConfigList.empty() ) {
						cout << "**** removeModule Failed : " << (*pt).DeviceName << " has dbroots still assigned. Please run movePmDbrootConfig.";
						quit = true;
						cout << endl;
						break;
					}
				}
	
				// check module status
				try{
					bool degraded;
					int opState;
					oam.getModuleStatus((*pt).DeviceName, opState, degraded);

					if (opState == oam::MAN_OFFLINE ||
						opState == oam::MAN_DISABLED)
						continue;
					else
					{
						cout << "**** removeModule Failed : " << (*pt).DeviceName << " is not MAN_OFFLINE OR DISABLED.";
						quit = true;
						cout << endl;
						break;
					}
				}
				catch (exception& ex)
				{}
			}

			if (quit) {
				cout << endl;
				break;
			}

			try{
				cout << endl << "Removing Module(s) ";
				DeviceNetworkList::iterator pt = devicenetworklist.begin();
				for( ; pt != devicenetworklist.end() ; pt++)
				{
					cout << (*pt).DeviceName << ", ";
				}
				cout << "please wait..." << endl;

				oam.removeModule(devicenetworklist);
				cout << endl << "Remove Module successfully completed" << endl << endl;
			}
			catch (exception& e)
			{
				cout << endl << "Failed to Remove Module: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** removeModule Failed : Failed to Remove Module" << endl << endl;
				break;
			}
			break;
		}

        case 50: // AVAILABLE
        {
		}

        case 51: // AVAILABLE
        {
		}

        case 52: // getModuleCpuUsers
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleCpuUsers Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			int topNumber = 5;

            if (arguments[2] != "")
            {
				topNumber = atoi(arguments[2].c_str());
				if ( topNumber < 1 || topNumber > 10 ) {
                cout << endl << "**** getModuleCpuUsers Failed : Invalid top Number entered" << endl;
                break;
				}
            }

			TopProcessCpuUsers topprocesscpuusers;
			try{
				oam.getTopProcessCpuUsers(arguments[1], topNumber, topprocesscpuusers);

				printModuleCpuUsers(topprocesscpuusers);

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
				break;
			}
			break;
		}

        case 53: // getSystemCpuUsers
        {
			int topNumber = 5;

            if (arguments[1] != "")
            {
				topNumber = atoi(arguments[1].c_str());
				if ( topNumber < 1 || topNumber > 10 ) {
                cout << endl << "**** getSystemCpuUsers Failed : Invalid top Number entered" << endl;
                break;
				}
            }

			cout << endl << "System Process Top CPU Users per Module" << endl << endl;

			SystemTopProcessCpuUsers systemtopprocesscpuusers;
			TopProcessCpuUsers topprocesscpuusers;
			try{
				oam.getTopProcessCpuUsers(topNumber, systemtopprocesscpuusers);

				for( unsigned int i = 0 ; i < systemtopprocesscpuusers.topprocesscpuusers.size(); i++)
				{
					printModuleCpuUsers(systemtopprocesscpuusers.topprocesscpuusers[i]);
				}

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getSystemCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
				break;
			}
			break;
		}

        case 54: // getModuleCpu
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleCpu Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			ModuleCpu modulecpu;
			try{
				oam.getModuleCpuUsage(arguments[1], modulecpu);

				printModuleCpu(modulecpu);

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
				break;
			}
			break;
		}

        case 55: // getSystemCpu
        {
			cout << endl << "System CPU Usage per Module" << endl << endl;

			SystemCpu systemcpu;

			try{
				oam.getSystemCpuUsage(systemcpu);

				for( unsigned int i = 0 ; i < systemcpu.modulecpu.size(); i++)
				{
					printModuleCpu(systemcpu.modulecpu[i]);
				}

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getSystemCpu Failed : Failed to get CPU Usage" << endl << endl;
				break;
			}
			break;
		}

        case 56: // getModuleMemoryUsers
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleMemoryUsers Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			int topNumber = 5;

            if (arguments[2] != "")
            {
				topNumber = atoi(arguments[2].c_str());
				if ( topNumber < 1 || topNumber > 10 ) {
                cout << endl << "**** getModuleMemoryUsers Failed : Invalid top Number entered" << endl;
                break;
				}
            }

			TopProcessMemoryUsers topprocessmemoryusers;
			try{
				oam.getTopProcessMemoryUsers(arguments[1], topNumber, topprocessmemoryusers);

				printModuleMemoryUsers(topprocessmemoryusers);

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
				break;
			}
			break;
		}

        case 57: // getSystemMemoryUsers
        {
			int topNumber = 5;

            if (arguments[1] != "")
            {
				topNumber = atoi(arguments[1].c_str());
				if ( topNumber < 1 || topNumber > 10 ) {
                cout << endl << "**** getSystemMemoryUsers Failed : Invalid top Number entered" << endl;
                break;
				}
            }

			cout << endl << "System Process Top Memory Users per Module" << endl << endl;

			SystemTopProcessMemoryUsers systemtopprocessmemoryusers;
			TopProcessMemoryUsers topprocessmemoryusers;
			try{
				oam.getTopProcessMemoryUsers(topNumber, systemtopprocessmemoryusers);

				for( unsigned int i = 0 ; i < systemtopprocessmemoryusers.topprocessmemoryusers.size(); i++)
				{
					printModuleMemoryUsers(systemtopprocessmemoryusers.topprocessmemoryusers[i]);
				}

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getSystemMemoryUsers Failed : Failed to get Top CPU Users" << endl << endl;
				break;
			}
			break;
		}

        case 58: // getModuleMemory
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleMemory Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			ModuleMemory modulememory;
			try{
				oam.getModuleMemoryUsage(arguments[1], modulememory);

				cout << endl << "Module Memory Usage (in K bytes)" << endl << endl;
	
				cout.setf(ios::left);
				cout.width(8);
				cout << "Module";
				cout.width(11);
				cout << "Mem Total";
				cout.width(9);
				cout << "Mem Used";
				cout.width(9);
				cout << "cache";
				cout.width(12);
				cout << "Mem Usage %";
				cout.width(11);
				cout << "Swap Total";
				cout.width(10);
				cout << "Swap Used";
				cout.width(13);
				cout << "Swap Usage %";
				cout << endl;
	
				cout.setf(ios::left);
				cout.width(8);
				cout << "------";
				cout.width(11);
				cout << "---------";
				cout.width(9);
				cout << "-------";
				cout.width(9);
				cout << "-------";
				cout.width(12);
				cout << "----------";
				cout.width(11);
				cout << "----------";
				cout.width(10);
				cout << "---------";
				cout.width(13);
				cout << "-----------";
				cout << endl;

				printModuleMemory(modulememory);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
				break;
			}
			break;
		}

        case 59: // getSystemMemory
        {
			cout << endl << "System Memory Usage per Module (in K bytes)" << endl << endl;

			cout.setf(ios::left);
			cout.width(8);
			cout << "Module";
			cout.width(11);
			cout << "Mem Total";
			cout.width(10);
			cout << "Mem Used";
			cout.width(9);
			cout << "Cache";
			cout.width(13);
			cout << "Mem Usage %";
			cout.width(12);
			cout << "Swap Total";
			cout.width(11);
			cout << "Swap Used";
			cout.width(14);
			cout << "Swap Usage %";
			cout << endl;

			cout.setf(ios::left);
			cout.width(8);
			cout << "------";
			cout.width(11);
			cout << "---------";
			cout.width(10);
			cout << "--------";
			cout.width(9);
			cout << "-------";
			cout.width(13);
			cout << "-----------";
			cout.width(12);
			cout << "----------";
			cout.width(11);
			cout << "---------";
			cout.width(14);
			cout << "------------";
			cout << endl;

			SystemMemory systemmemory;

			try{
				oam.getSystemMemoryUsage(systemmemory);

				for( unsigned int i = 0 ; i < systemmemory.modulememory.size(); i++)
				{
					printModuleMemory(systemmemory.modulememory[i]);
				}

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getSystemCpu Failed : Failed to get Memory Usage" << endl << endl;
				break;
			}
			break;
		}

        case 60: // getModuleDisk
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleDisk Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			ModuleDisk moduledisk;
			try{
				oam.getModuleDiskUsage(arguments[1], moduledisk);

				printModuleDisk(moduledisk);

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
				break;
			}
			break;
		}

        case 61: // getSystemDisk
        {
			cout << endl << "System Disk Usage per Module" << endl << endl;

			SystemDisk systemdisk;

			try{
				oam.getSystemDiskUsage(systemdisk);

				for( unsigned int i = 0 ; i < systemdisk.moduledisk.size(); i++)
				{
					printModuleDisk(systemdisk.moduledisk[i]);
				}

			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
			}
			catch(...)
			{
				cout << endl << "**** getSystemCpu Failed : Failed to get Memory Usage" << endl << endl;
				break;
			}
			break;
		}

        case 62: // getModuleResources
        {
            if (arguments[1] == "")
            {
                // need 1 arguments
                cout << endl << "**** getModuleResources Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			int topNumber = 5;

			TopProcessCpuUsers topprocesscpuusers;
			try{
				oam.getTopProcessCpuUsers(arguments[1], topNumber, topprocesscpuusers);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
				break;
			}

			ModuleCpu modulecpu;
			try{
				oam.getModuleCpuUsage(arguments[1], modulecpu);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
				break;
			}

			TopProcessMemoryUsers topprocessmemoryusers;
			try{
				oam.getTopProcessMemoryUsers(arguments[1], topNumber, topprocessmemoryusers);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
				break;
			}

			ModuleMemory modulememory;
			try{
				oam.getModuleMemoryUsage(arguments[1], modulememory);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
				break;
			}

			ModuleDisk moduledisk;
			try{
				oam.getModuleDiskUsage(arguments[1], moduledisk);
			}
			catch (exception& e)
			{
				cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
				break;
			}

			printModuleResources(topprocesscpuusers, modulecpu, topprocessmemoryusers, modulememory, moduledisk);

			break;
		}

        case 63: // getSystemResources
        {
			cout << endl << "System Resource Usage per Module" << endl << endl;

			int topNumber = 5;

            SystemModuleTypeConfig systemmoduletypeconfig;
            ModuleTypeConfig moduletypeconfig;

			try
			{
				oam.getSystemConfig(systemmoduletypeconfig);

				for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
				{
					if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
						// end of list
						continue;

					int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

					if ( moduleCount == 0 )
						continue;

					DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
					for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
					{
						string modulename = (*pt).DeviceName;
						if ( modulename == "unknown" )
							continue;

						TopProcessCpuUsers topprocesscpuusers;
						try{
							oam.getTopProcessCpuUsers(modulename, topNumber, topprocesscpuusers);
						}
						catch (exception& e)
						{
							cout << endl << "Failed to get Top CPU Users: " << e.what() << endl << endl;
							break;
						}
						catch(...)
						{
							cout << endl << "**** getModuleCpuUsers Failed : Failed to get Top CPU Users" << endl << endl;
							break;
						}
			
						ModuleCpu modulecpu;
						try{
							oam.getModuleCpuUsage(modulename, modulecpu);
						}
						catch (exception& e)
						{
							cout << endl << "Failed to get CPU Usage: " << e.what() << endl << endl;
							break;
						}
						catch(...)
						{
							cout << endl << "**** getModuleCpu Failed : Failed to get Module CPU Usage" << endl << endl;
							break;
						}
			
						TopProcessMemoryUsers topprocessmemoryusers;
						try{
							oam.getTopProcessMemoryUsers(modulename, topNumber, topprocessmemoryusers);
						}
						catch (exception& e)
						{
							cout << endl << "Failed to get Top Memory Users: " << e.what() << endl << endl;
							break;
						}
						catch(...)
						{
							cout << endl << "**** getModuleMemoryUsers Failed : Failed to get Top Memory Users" << endl << endl;
							break;
						}
			
						ModuleMemory modulememory;
						try{
							oam.getModuleMemoryUsage(modulename, modulememory);
						}
						catch (exception& e)
						{
							cout << endl << "Failed to get Memory Usage: " << e.what() << endl << endl;
							break;
						}
						catch(...)
						{
							cout << endl << "**** getModuleMemory Failed : Failed to get Module Memory Usage" << endl << endl;
							break;
						}
			
						ModuleDisk moduledisk;
						try{
							oam.getModuleDiskUsage(modulename, moduledisk);
						}
						catch (exception& e)
						{
							cout << endl << "Failed to get Disk Usage: " << e.what() << endl << endl;
							break;
						}
						catch(...)
						{
							cout << endl << "**** getModuleDisk Failed : Failed to get Module Disk Usage" << endl << endl;
							break;
						}
			
						printModuleResources(topprocesscpuusers, modulecpu, topprocessmemoryusers, modulememory, moduledisk);
					}
				}
			}
            catch (exception& e)
            {
                cout << endl << "**** getSystemResources Failed :  " << e.what() << endl;
            }
			break;
		}

        case 64: // getActiveSQLStatements
        {
            cout << endl << "Get List of Active SQL Statements" << endl;
            cout <<         "=================================" << endl << endl;

            ActiveSqlStatements activesqlstatements;

            try
            {
                oam.getActiveSQLStatements(activesqlstatements);

                if ( activesqlstatements.size() == 0 )
                {
                    cout << "No Active SQL Statements at this time" << endl << endl;
                    break;
                }

                cout << "Start Time        Time (hh:mm:ss)   Session ID             SQL Statement" << endl;
                cout << "----------------  ----------------  --------------------   ------------------------------------------------------------" << endl;

                for ( unsigned int i = 0 ; i < activesqlstatements.size(); i++)
                {
                    struct tm tmStartTime;
                    char timeBuf[36];
                    time_t startTime = activesqlstatements[i].starttime;
                    localtime_r(&startTime, &tmStartTime);
                    (void)strftime(timeBuf, 36, "%b %d %H:%M:%S", &tmStartTime);

                    cout.setf(ios::left);
                    cout.width(21);
                    cout << timeBuf;

                    //get current time in Epoch
                    time_t cal;
                    time (&cal);

                    int runTime = cal - activesqlstatements[i].starttime;
                    int runHours = runTime/3600;
                    int runMinutes = (runTime - (runHours*3600))/60;
                    int runSeconds = runTime - (runHours*3600)  - (runMinutes*60);

                    cout.width(15);
                    string hours = oam.itoa(runHours);
                    string minutes = oam.itoa(runMinutes);
                    string seconds = oam.itoa(runSeconds);

                    string run;
                    if ( hours.size() == 1 )
                        run = "0" + hours + ":";
                    else
                        run = hours + ":";

                    if ( minutes.size() == 1 )
                        run = run + "0" + minutes + ":";
                    else
                        run = run + minutes + ":";

                    if ( seconds.size() == 1 )
                        run = run + "0" + seconds;
                    else
                        run = run + seconds;

                    cout << run;

                    cout.width(23);
                    cout << activesqlstatements[i].sessionid;

                    string SQLStatement = activesqlstatements[i].sqlstatement;
                    int pos=0;
                    for ( ;; )
                    {
                        string printSQL = SQLStatement.substr(pos, 60);
                        pos=pos+60;
                        cout << printSQL << endl;

                        if ( printSQL.size() < 60 )
                            break;

                        cout.width(59);
                        cout << " ";
                    }
                    cout << endl;
                }

            }
            catch (exception& e)
            {
                cout << endl << "Failed to get List of Active SQL Statements: " << e.what() << endl << endl;
            }
            catch (...)
            {
                cout << endl << "**** getActiveSQLStatements Failed : Failed to get List of Active SQL Statements" << endl << endl;
                break;
            }
            break;
        }

        case 65: // alterSystem-disableModule
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** alterSystem-disableModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			parentOAMModule = getParentOAMModule();
			if ( arguments[1] == parentOAMModule ) {
				// exit out since you can't manually remove OAM Parent Module
                cout << endl << "**** alterSystem-disableModule Failed : can't manually disable the Active OAM Parent Module." << endl;
                break;
			}

			string moduleType = arguments[1].substr(0,MAX_MODULE_TYPE_SIZE);

            gracefulTemp = INSTALL;

            // confirm request
			if ( arguments[2] != "y" ) {
				if (confirmPrompt("This command stops the processing of applications on a Module within the Calpont System"))
					break;
			}

			//parse module names
			DeviceNetworkConfig devicenetworkconfig;
			DeviceNetworkList devicenetworklist;

			boost::char_separator<char> sep(", ");
			boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				devicenetworkconfig.DeviceName = *it;
				devicenetworklist.push_back(devicenetworkconfig);
			}

			DeviceNetworkList::iterator pt = devicenetworklist.begin();
			DeviceNetworkList::iterator endpt = devicenetworklist.end();

			bool quit = false;

			// check for module status and if any dbroots still assigned
			if ( moduleType == "pm" ) {
				for( ; pt != endpt ; pt++)
				{
					// check for dbroots assigned
					DBRootConfigList dbrootConfigList;
					string moduleID = (*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
					try {
						oam.getPmDbrootConfig(atoi(moduleID.c_str()), dbrootConfigList);
					}
					catch(...) 
					{}
	
					if ( !dbrootConfigList.empty() ) {
						cout << endl << "**** alterSystem-disableModule Failed : " << (*pt).DeviceName << " has dbroots still assigned and will not be disabled. Please run movePmDbrootConfig.";
						quit = true;
						cout << endl;
						break;
					}
				}

				if (quit) {
					cout << endl;
					break;
				}
			}

			if ( devicenetworklist.empty() ) {
				cout << endl << "quiting, no modules to remove." << endl << endl;
				break;
			}

			// stop module
			try
			{
				cout << endl << "   Stopping Modules" << endl;
				oam.stopModule(devicenetworklist, gracefulTemp, ackTemp);
				cout << "   Successful stop of Modules " << endl;
			}
			catch (exception& e)
			{
                string Failed = e.what();
                if (Failed.find("Disabled") != string::npos)
					cout << endl << "   Successful stop of Modules " << endl;
				else {
					cout << endl << "**** stopModule Failed :  " << e.what() << endl;
					break;
				}
			}

			// disable module
			try
			{
				cout << endl << "   Disabling Modules" << endl;
				oam.disableModule(devicenetworklist);
				cout << "   Successful disable of Modules " << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** disableModule Failed :  " << e.what() << endl;
				break;
			}

			cout << endl;
			break;
		}

        case 66: // alterSystem-enableModule
        {
            if (arguments[1] == "")
            {
                // need arguments
                cout << endl << "**** alterSystem-enableModule Failed : Missing a required Parameter, enter 'help' for additional information" << endl;
                break;
            }

			string moduleType = arguments[1].substr(0,MAX_MODULE_TYPE_SIZE);

            ACK_FLAG ackTemp = ACK_YES;

            // confirm request
			if ( arguments[2] != "y" ) {
				if (confirmPrompt("This command starts the processing of applications on a Module within the Calpont System"))
					break;
			}

			//parse module names
			DeviceNetworkConfig devicenetworkconfig;
			DeviceNetworkList devicenetworklist;
			boost::char_separator<char> sep(", ");
			boost::tokenizer< boost::char_separator<char> > tokens(arguments[1], sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				devicenetworkconfig.DeviceName = *it;
				devicenetworklist.push_back(devicenetworkconfig);
			}

			//get the system status, enable modules and startmodules if system is ACTIVE
			SystemStatus systemstatus;
			try {
				oam.getSystemStatus(systemstatus);

				// enable module
				try
				{
					cout << endl << "   Enabling Modules " << endl;
					oam.enableModule(devicenetworklist);
					cout << "   Successful enable of Modules " << endl;
				}
				catch (exception& e)
				{
					cout << endl << "**** enableModule Failed :  " << e.what() << endl;
					break;
				}
	
				if ( moduleType == "pm" )
				{
					cout << endl << "   Performance Module(s) Enabled, run movePmDbrootConfig or assignDbrootPmConfig to assign dbroots, if needed" << endl << endl;
					break;
				}
				else
				{
					if (systemstatus.SystemOpState == oam::ACTIVE ) {
						try
						{
							cout << endl << "   Starting Modules" << endl;
							oam.startModule(devicenetworklist, ackTemp);
			
							//reload DBRM with new configuration, needs to be done here after startModule
							string cmd = startup::StartUp::installDir() + "/bin/dbrmctl reload > /dev/null 2>&1";
							system(cmd.c_str());
							sleep(15);
			
							cout << "   Successful start of Modules " << endl;
						}
						catch (exception& e)
						{
							cout << endl << "**** startModule Failed :  " << e.what() << endl;
							break;
						}
					}
					else
						cout << endl << "   System not Active, run 'startSystem' to start system if needed" << endl;
				}
			}
			catch (exception& e)
			{
				cout << endl << "**** alterSystem-enableModule Failed : " << e.what() << endl;
				break;
			}
			catch(...)
			{
				cout << endl << "**** alterSystem-enableModule Failed,  Failed return from getSystemStatus API" << endl;
				break;
			}

			cout << endl;

			break;
		}

        case 67: // AVAILABLE
        {
		}


      default:
		{
            cout << arguments[0] << ": Unknown Command, type help for list of commands" << endl << endl;
			return 1;
		}
    }
	return 0;
}

/******************************************************************************************
 * @brief	ProcessSupportCommand
 *
 * purpose:	Process Support commands
 *
 ******************************************************************************************/
int ProcessSupportCommand(int CommandID, std::string arguments[])
{
	Oam oam;
	GRACEFUL_FLAG gracefulTemp = GRACEFUL;
	ACK_FLAG ackTemp = ACK_YES;
	CC_SUSPEND_ANSWER suspendAnswer = WAIT;
	bool bNeedsConfirm = true;

	switch( CommandID )
    {
        case 0: // helpsupport
        {
        	// display commands in the Support Command list
            cout << endl << "List of Support commands" << endl << endl;

			for (int i = 1;;i++)
			{
				if (supportCmds[i] == "")
					// end of list
					break;
            	cout << "   " << supportCmds[i] << endl;
			}
			cout << endl;
        }
        break;

        case 1: // stopprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** stopprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

			// don't allow stopping of Process-Monitor
			if ( arguments[1] == "ProcessMonitor" ) {
            	cout << "ProcessMonitor is controlled by 'init' and can not be stopped" << endl;
                break;
			}
			else
			{
				// give warning for Process-Monitor
				if ( arguments[1] == "ProcessManager" ) {
            		if (confirmPrompt("ProcessManager is the Interface for the Console and should only be removed as part of a Calpont Package installation"))
                		break;
				}
				else
				{
					if ( arguments[3] != "y" ) {
						getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);
						// confirm request
						if (confirmPrompt("This command stops the processing of an application on a Module within the Calpont System"))
							break;
					}
				}
			}

            try
            {
                oam.stopProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful stop of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 2: // startprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** startprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            try
            {
                oam.startProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful start of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 3: // restartprocess - parameters: Process-name, Module-name, Graceful flag, Ack flag
        {
            if (arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** restartprocess Failed : Missing a required Parameter, Enter Process and Module names" << endl;
                break;
            }

			getFlags(arguments, gracefulTemp, ackTemp, suspendAnswer, bNeedsConfirm);

            // confirm request
            if (confirmPrompt("This command restarts the processing of an application on a Module within the Calpont System"))
                break;

            try
            {
                oam.restartProcess(arguments[2], arguments[1], gracefulTemp, ackTemp);
                cout << endl << "   Successful restart of Process " << arguments[1] << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 4: // killpid
		{
            if (arguments[1] == "" || arguments[2] != "")
            {
                // need arguments
                cout << endl << "**** killpid Failed : Invalid or Missing Parameter, Enter local Process-ID" << endl;
                break;
            }
			
			pid_t PID = atoi(arguments[1].c_str());
			if ( PID <= 0 ) {
                cout << endl << "**** killpid Failed : Invalid Process-ID Entered" << endl;
                break;
			}

			int status = kill( PID, SIGTERM);
			if ( status != API_SUCCESS)
            	cout << endl << "   Failure in kill of Process-ID " << arguments[1] << ", Failed: " << errno << endl << endl;
			else
            	cout << endl << "   Successful kill of Process-ID " << arguments[1] << endl << endl;
		}
	    break;

        case 5: // rebootsystem - parameters: password
        {
			if ( !rootUser)
			{
                cout << endl << "**** rebootsystem Failed : command not available when running as non-root user" << endl;
                break;
			}

			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** rebootsystem Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

            if (arguments[1] == "" || arguments[1] == "y")
            {
                // need arguments
                cout << endl << "**** rebootsystem Failed : Invalid or Missing Parameter, Provide root-password" << endl;
                break;
            }
			
			string password = arguments[1];
			if ( arguments[2] != "y") {
				cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
				string warning = "This command stops the Processing of applications and reboots all modules within the Calpont System";
				// confirm request
				if (confirmPrompt(warning))
					break;
			}
            cout << endl << "   Stop System being performed, please wait..." << endl;

            try
            {
                cout << endl << "   System being stopped, please wait... " << endl;
                oam.stopSystem(GRACEFUL, ACK_YES);

				if ( waitForStop() )
					cout << endl << "   Successful stop of System " << endl << endl;
				else
					cout << endl << "**** stopSystem Failed : check log files" << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopSystem Failed :  " << e.what() << endl;
				string warning = "stopSystem command failed,";
				// confirm request
				if (confirmPrompt(warning))
					break;
			}

            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

			try
			{
				oam.getSystemConfig(systemmoduletypeconfig);

				bool FAILED = false;
				for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
				{
					if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
						// end of list
						break;
					int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
					if ( moduleCount > 0 )
					{
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
						{
							string modulename = (*pt).DeviceName;
							if (modulename == parentOAMModule ) {
								//do me last
								continue;
							}

							//skip modules in MAN_DISABLED state
							try{
								int opState;
								bool degraded;
								oam.getModuleStatus(modulename, opState, degraded);
				
								if (opState == oam::MAN_DISABLED )
									//skip
									continue;
							}
							catch (exception& ex)
							{}
							
							//run remote command script
							HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
							string cmd = startup::StartUp::installDir() + "/bin/remote_command.sh " + (*pt1).IPAddr + " " + password + " reboot " ;
							int rtnCode = system(cmd.c_str());
							if (WEXITSTATUS(rtnCode) != 0) {
								cout << "Failed with running remote_command.sh" << endl;
								FAILED = true;
							}
							else
            					cout << endl << "   Successful reboot request of Module " << modulename << endl;
						}
					}
				}

				if ( FAILED )
					break;

				//reboot local module
				int rtnCode = system("reboot");
				if (WEXITSTATUS(rtnCode) != 0)
					cout << "Failed rebooting local module" << endl;
				else
				{
					cout << endl << "   Successful reboot request of local Module" << endl;
					// close the log file
					writeLog("End of a command session!!!");
					logFile.close();
					cout << endl << "Exiting the Calpont Command Console" << endl;
					exit (0);
				}
			}
			catch(...)
			{
				cout << endl << "**** rebootsystem Failed : Failed on getSystemConfig API" << endl;
				break;
			}
         }
        break;

        case 6: // rebootnode - parameters: module-name password
        {
			if ( !rootUser)
			{
                cout << endl << "**** rebootnode Failed : command not available when running as non-root user" << endl;
                break;
			}

            if (arguments[1] == "" || arguments[2] == "")
            {
                // need arguments
                cout << endl << "**** rebootnode Failed : Invalid or Missing Parameter, Enter module-name and root-password" << endl;
                break;
            }
			
			string inputModuleName = arguments[1];
			string password = arguments[2];
			if ( arguments[3] != "y") {
				cout << endl << "!!!!! DESTRUCTIVE COMMAND !!!!!" << endl;
				string warning = "This command reboots a node within the Calpont System";
				// confirm request
				if (confirmPrompt(warning))
					break;
			}

            SystemModuleTypeConfig systemmoduletypeconfig;
            systemmoduletypeconfig.moduletypeconfig.clear();

			try
			{
				oam.getSystemConfig(systemmoduletypeconfig);
				unsigned int i = 0;
				for(  ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
				{
					if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() ) {
						// end of list
						break;
					}
					int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
					string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
					if ( moduleCount > 0 )
					{
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
						{
							string modulename = (*pt).DeviceName;
							if (inputModuleName == modulename ) {
								if (inputModuleName == localModule ) {
									//reboot local module
									int rtnCode = system("reboot");
									if (WEXITSTATUS(rtnCode) != 0)
										cout << "Failed rebooting local node" << endl;
									else
									{
										cout << endl << "   Successful reboot request of Node " << modulename << endl;
										// close the log file
										writeLog("End of a command session!!!");
										logFile.close();
										cout << endl << "Exiting the Calpont Command Console" << endl;
										exit (0);
									}
								}
								else
								{
									HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
									string ipAddr = (*pt1).IPAddr;
									//run remote command script
									string cmd = startup::StartUp::installDir() + "/bin/remote_command.sh " + ipAddr + " " + password + " reboot " ;
									int rtnCode = system(cmd.c_str());
									if (WEXITSTATUS(rtnCode) != 0)
										cout << "Failed with running remote_command.sh" << endl;
									else
										cout << endl << "   Successful reboot request of Node " << modulename << endl;
									return(0);
								}
							}
						}
					}
				}
			}
			catch(...)
			{
				cout << endl << "**** rebootnode Failed : Failed on getSystemConfig API" << endl;
				break;
			}
         }
        break;

        case 7: // stopdbrmprocess
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command stops the dbrm processes within the Calpont System"))
					break;
			}

            try
            {
                oam.stopProcessType("DBRM");
                cout << endl << "   Successful stop of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 8: // startdbrmprocess
        {
            try
            {
                oam.startProcessType("DBRM");
                cout << endl << "   Successful Start of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 9: // restartdbrmprocess
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command restarts the dbrm processes within the Calpont System"))
					break;
			}

            try
            {
                oam.restartProcessType("DBRM");
                cout << endl << "   Successful Restart of DBRM Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartdbrmprocess Failed :  " << e.what() << endl;
            }
        }
        break;

        case 10: // setsystemstartupstate
        {
			Config* sysConfig = Config::makeConfig();
			
			parentOAMModule = getParentOAMModule();
			if ( localModule != parentOAMModule ) {
				// exit out since not on Parent OAM Module
                cout << endl << "**** setsystemstartupstate Failed : only should be run on the Parent OAM Module, which is '" << parentOAMModule << "'" << endl;
                break;
			}

			string systemStartupOffline;
			try {
				systemStartupOffline = sysConfig->getConfig("Installation", "SystemStartupOffline");
				cout << "SystemStartupOffline currently set to '" + systemStartupOffline + "'" << endl;
			}
			catch(...)
			{
				cout << "ERROR: Problem getting systemStartupOffline from the Calpont System Configuration file" << endl;
				return 1;
			}
		
			while(true)
			{
				char* pcommand = 0;
				string prompt;
				string temp = "cancel";
				prompt = "Set system startup state to offline: (y,n,cancel) [cancel]: ";
				pcommand = readline(prompt.c_str());
				if (pcommand)
				{
					if (strlen(pcommand) > 0) temp = pcommand;
					free(pcommand);
					pcommand = 0;
				}
				if ( temp == "cancel" )
					return 0; 
				if ( temp == "n" || temp == "y") {
					systemStartupOffline = temp;
					break;
				}
				cout << "Invalid Option, please re-enter" << endl;
			}
		
			try {
				sysConfig->setConfig("Installation", "SystemStartupOffline", systemStartupOffline);
				sysConfig->write();
			}
			catch(...)
			{
				cout << "ERROR: Problem setting systemStartupOffline in the Calpont System Configuration file" << endl;
				exit(-1);
			}
            cout << endl << "   Successful setting of systemStartupOffline to '" << systemStartupOffline << "'" << endl << endl;
        }
        break;

        case 11: // stopPrimProcs
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command stops the PrimProc processes within the Calpont System"))
					break;
			}

            try
            {
                oam.stopProcessType("PrimProc");
                cout << endl << "   Successful stop of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 12: // startPrimProcs
        {
            try
            {
                oam.startProcessType("PrimProc");
                cout << endl << "   Successful Start of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 13: // restartPrimProcs
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command restarts the PrimProc processes within the Calpont System"))
					break;
			}

            try
            {
                oam.restartProcessType("PrimProc");
                cout << endl << "   Successful Restart of PrimProc Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartPrimProcs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 14: // stopExeMgrs
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command stops the ExeMgr processes within the Calpont System"))
					break;
			}

            try
            {
                oam.stopProcessType("ExeMgr");
                cout << endl << "   Successful stop of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** stopExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 15: // startExeMgrs
        {
            try
            {
                oam.startProcessType("ExeMgr");
                cout << endl << "   Successful Start of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** startExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 16: // restartExeMgrs
        {
			if ( arguments[1] != "y" ) {
				// confirm request
				if (confirmPrompt("This command restarts the ExeMgr processes within the Calpont System"))
					break;
			}

            try
            {
                oam.restartProcessType("ExeMgr");
                cout << endl << "   Successful Restart of ExeMgr Processes" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** restartExeMgrs Failed :  " << e.what() << endl;
            }
        }
        break;

        case 17: // getProcessStatusStandby - parameters: NONE
        {
			printProcessStatus("ProcStatusControlStandby");
        }
        break;

        case 18: // distributeconfigfile - parameters: option, moduleName
        {
			string name = "system";

			if ( arguments[1] != "" )
				name = arguments[1];

            try
            {
				oam.distributeConfigFile(name);
                cout << endl << "   Successful Distribution of Calpont Config File" << endl << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** Distribution of Calpont Config File Failed :  " << e.what() << endl;
            }
        }
        break;

        case 19: // getPmDbrootConfig - paramaters: pm id
        {
			string pmID;
			if (arguments[1] == "") {
				cout << endl;
				string prompt = "Enter the Performance Module ID";
				pmID = dataPrompt(prompt);
			}
			else
				pmID = arguments[1];
			
            try
            {
				DBRootConfigList dbrootConfigList;
                oam.getPmDbrootConfig(atoi(pmID.c_str()), dbrootConfigList);

				cout << "DBRoot IDs assigned to 'pm" + pmID + "' = ";

				DBRootConfigList::iterator pt = dbrootConfigList.begin();
				for( ; pt != dbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != dbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getPmDbrootConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 20: // getDbrootPmConfig - parameters dbroot id
        {
			string dbrootID;
			if (arguments[1] == "") {
				cout << endl;
				string prompt = "Enter the DBRoot ID";
				dbrootID = dataPrompt(prompt);
			}
			else
				dbrootID = arguments[1];
			
            try
            {
				int pmID;
                oam.getDbrootPmConfig(atoi(dbrootID.c_str()), pmID);

				cout << endl << " DBRoot ID " << dbrootID << " is assigned to 'pm" << pmID << "'" << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getDbrootPmConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 21: // getSystemDbrootConfig
        {
			cout << endl << "System DBroot Configuration" << endl << endl;

            try
            {
				DBRootConfigList dbrootConfigList;
                oam.getSystemDbrootConfig(dbrootConfigList);

				cout << "System DBRoot IDs = ";
				DBRootConfigList::iterator pt = dbrootConfigList.begin();
				for( ; pt != dbrootConfigList.end() ;)
				{
					cout << oam.itoa(*pt);
					pt++;
					if (pt != dbrootConfigList.end())
						cout << ", ";
				}
				cout << endl;
            }
            catch (exception& e)
            {
                cout << endl << "**** getSystemDbrootConfig Failed :  " << e.what() << endl;
            }
        }
        break;

        case 22: // checkDBFunctional
        {
			try {
				oam.checkDBFunctional(false);
               	cout << endl << "   checkDBFunctional Successful" << endl << endl;
			}
            catch (exception& e)
            {
                cout << endl << "**** checkDBFunctional Failed :  " << e.what() << endl;
               	cout << endl << "     can check UM /tmp/dbfunctional.log for possible additional information" << endl << endl;
            }
			catch(...)
			{
               	cout << endl << "   checkDBFunctional Failed: check UM /tmp/dbfunctional.log" << endl << endl;
			}
        }
        break;

		default: // shouldn't get here, but...
			return 1;

	} // end of switch

	return 0;
}

/******************************************************************************************
 * @brief	getFlags
 *
 * purpose:	get and convert Graceful and Ack flags
 *
 ******************************************************************************************/
void getFlags(const string* arguments, GRACEFUL_FLAG& gracefulTemp, ACK_FLAG& ackTemp, oam::CC_SUSPEND_ANSWER& suspendAnswer, bool& bNeedsConfirm, string* password)
{
    gracefulTemp = GRACEFUL;                      // default
    ackTemp = ACK_YES;                             // default
	suspendAnswer = CANCEL;
	bNeedsConfirm = true;
    for( int i = 1; i < ArgNum; i++)
    {
		if (strcasecmp(arguments[i].c_str(), "Y") == 0)
			bNeedsConfirm = false;
		else
		if (strcasecmp(arguments[i].c_str(), "N") == 0)
			bNeedsConfirm = true;
		else
        if (strcasecmp(arguments[i].c_str(), "GRACEFUL") == 0)
            gracefulTemp = oam::GRACEFUL;
		else
        if (strcasecmp(arguments[i].c_str(), "FORCEFUL") == 0)
            gracefulTemp = FORCEFUL;
		else
        if (strcasecmp(arguments[i].c_str(), "INSTALL") == 0)
            gracefulTemp = INSTALL;
		else
        if (strcasecmp(arguments[i].c_str(), "ACK_YES") == 0 || strcasecmp(arguments[i].c_str(), "YES_ACK") == 0)
            ackTemp = ACK_YES;
		else
        if (strcasecmp(arguments[i].c_str(), "ACK_NO") == 0 || strcasecmp(arguments[i].c_str(), "NO_ACK") == 0)
            ackTemp = ACK_NO;
		else
		if (strcasecmp(arguments[i].c_str(), "WAIT") == 0)
			suspendAnswer = WAIT;
		else
		if (strcasecmp(arguments[i].c_str(), "ROLLBACK") == 0)
			suspendAnswer = ROLLBACK;
		else
		if (strcasecmp(arguments[i].c_str(), "FORCE") == 0)
			suspendAnswer = FORCE;
		else
		if (password && arguments[i].length() > 0)
			*password = arguments[i];
    }
}


/******************************************************************************************
 * @brief	confirmPrompt
 *
 * purpose:	Confirmation prompt
 *
 ******************************************************************************************/
int confirmPrompt(std::string warningCommand)
{
    char* pcommand = 0;
    char *p;
    string argument = "n";

    while(true)
    {
        // read input
		if (warningCommand.size() > 0)
		{
			cout << endl << warningCommand << endl;
		}
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

        // covert argument into lowercase
        transform (argument.begin(), argument.end(), argument.begin(), to_lower());

        if( argument == "y")
            return 0;
        else if( argument == "n")
            return 1;
    }
}

/******************************************************************************************
 * @brief	dataPrompt
 *
 * purpose:	Prompt for additional data
 *
 ******************************************************************************************/
std::string dataPrompt(std::string promptCommand)
{
    char data[CmdSize];
    char* pdata = data;
    char *pd;
    string argument;

    while(true)
    {
        // read input
        cout << promptCommand << endl;
        pdata = readline("           Please enter: ");

        if (!pdata)                            // user hit <Ctrl>-D
            pdata = strdup("exit");

        else if (!*pdata)
            // just an enter-key was entered, ignore and reprompt
            continue;

        pd = pdata;
        argument = pd;

		return argument;
    }
}


/******************************************************************************************
 * @brief	writeLog for command
 *
 * purpose:	write command to the log file
 *
 ******************************************************************************************/
void writeLog(string command)
{
    Oam oam;

	//filter off password on reboot commands

    logFile << oam.getCurrentTime() << ": " << command << endl;
    logFile.flush();
    return;
}

/******************************************************************************************
 * @brief	printAlarmSummary
 *
 * purpose:	get active alarms and produce a summary
 *
 ******************************************************************************************/
void printAlarmSummary()
{
	AlarmList alarmList;
	Oam oam;
	try {
		oam.getActiveAlarms(alarmList);
	}
	catch(...)
	{
		return;
	}

	int critical = 0, major = 0, minor = 0, warning = 0, info = 0;
	AlarmList :: iterator i;
	for (i = alarmList.begin(); i != alarmList.end(); ++i)
	{
		switch (i->second.getSeverity())
		{
			case CRITICAL:
				++critical;
				break;
			case MAJOR:
				++major;
				break;
			case MINOR:
				++minor;
				break;
			case WARNING:
				++warning;
				break;
			case INFORMATIONAL:
				++info;
				break;
		}
	}
	cout << endl << "Active Alarm Counts: ";
	cout << "Critical = " << critical;
	cout << ", Major = " << major;
	cout << ", Minor = " << minor;
	cout << ", Warning = " << warning;
	cout << ", Info = " << info;
	cout << endl;
}

/******************************************************************************************
 * @brief	printCriticalAlarms
 *
 * purpose:	get active Critical alarms
 *
 ******************************************************************************************/
void printCriticalAlarms()
{
	AlarmList alarmList;
	Oam oam;
	try {
		oam.getActiveAlarms(alarmList);
	}
	catch(...)
	{
		return;
	}

	cout << endl << "Critical Active Alarms:" << endl << endl;

	AlarmList :: iterator i;
	for (i = alarmList.begin(); i != alarmList.end(); ++i)
	{
		switch (i->second.getSeverity())
		{
			case CRITICAL:
				cout << "AlarmID           = " << i->second.getAlarmID() << endl;
				cout << "Brief Description = " << i->second.getDesc() << endl;
				cout << "Alarm Severity    = ";
				cout << "CRITICAL" << endl;
				cout << "Time Issued       = " << i->second.getTimestamp() << endl;
				cout << "Reporting Module  = " << i->second.getSname() << endl;
				cout << "Reporting Process = " << i->second.getPname() << endl;
				cout << "Reported Device   = " << i->second.getComponentID() << endl << endl;
				break;
			case MAJOR:
			case MINOR:
			case WARNING:
			case INFORMATIONAL:
				break;
		}
	}
}

/******************************************************************************************
 * @brief	printSystemStatus
 *
 * purpose:	get and Display System and Module Statuses
 *
 ******************************************************************************************/
void printSystemStatus()
{
	SystemStatus systemstatus;
	Oam oam;
	BRM::DBRM dbrm(true);

	cout << endl << "System " << systemName << endl << endl;
	cout << "System and Module statuses" << endl << endl;
	cout << "Component     Status                       Last Status Change" << endl;
	cout << "------------  --------------------------   ------------------------" << endl;

	try
	{
		oam.getSystemStatus(systemstatus);
		cout << "System        ";
		cout.setf(ios::left);
		cout.width(29);
		int state = systemstatus.SystemOpState;
		string extraInfo = " ";
		bool bRollback = false;
		bool bForce = false;

		if (dbrm.isDBRMReady())
		{
			if (dbrm.getSystemSuspended() > 0)
			{
				extraInfo = " WRITE SUSPENDED";
			}
			else
			if (dbrm.getSystemSuspendPending(bRollback) > 0)
			{
				extraInfo = " WRITE SUSPEND PENDING";
			}
			else
			if (dbrm.getSystemShutdownPending(bRollback, bForce) > 0)
			{
				extraInfo = " SHUTDOWN PENDING";
			}
		}
		printState(state, extraInfo);
		cout.width(24);
		string stime = systemstatus.StateChangeDate;
        stime = stime.substr (0,24);
		cout << stime << endl << endl;

		for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
		{
			if( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
				// end of list
				break;

			cout << "Module ";
			cout.setf(ios::left);
			cout.width(7);
			cout << systemstatus.systemmodulestatus.modulestatus[i].Module;
			cout.width(29);
			state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;

			// get NIC functional state (degraded or not)
			bool degraded = false;
			try {
				int state;
				oam.getModuleStatus(systemstatus.systemmodulestatus.modulestatus[i].Module, state, degraded);
			}
			catch (...)
			{}

			string nicFun = " ";
			if (degraded)
				nicFun = "/" + DEGRADEDSTATE;

			printState(state, nicFun);

			cout.width(24);
			string stime = systemstatus.systemmodulestatus.modulestatus[i].StateChangeDate ;
			stime = stime.substr (0,24);
			cout << stime << endl;
		}
		cout << endl;

		if ( systemstatus.systemmodulestatus.modulestatus.size() > 1)
			// get and display Parent OAM Module
			cout << "Active Parent OAM Performance Module is '" << getParentOAMModule() << "'" << endl << endl;

	}
	catch (exception& e)
	{
		cout << endl << "**** printSystemStatus Failed =  " << e.what() << endl;
	}
}

/******************************************************************************************
 * @brief	printProcessStatus
 *
 * purpose:	get and Display Process Statuses
 *
 ******************************************************************************************/
void printProcessStatus(std::string port)
{
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;
	ModuleTypeConfig moduletypeconfig;
	Oam oam;
	BRM::DBRM dbrm(true);

	int state;
	string extraInfo = " ";
	bool bRollback = false;
	bool bForce = false;
	bool bSuspend = false;
	if (dbrm.isDBRMReady())
	{
		if (dbrm.getSystemSuspended() > 0)
		{
			bSuspend = true;
			extraInfo = "WRITE_SUSPEND";
		}
		else
		if (dbrm.getSystemSuspendPending(bRollback) > 0)
		{
			bSuspend = true;
			if (bRollback)
			{
				extraInfo = "ROLLBACK";
			}
			else
			{
				extraInfo = "SUSPEND_PENDING";
			}
		}
		else
		if (dbrm.getSystemShutdownPending(bRollback, bForce) > 0)
		{
			bSuspend = true;
			if (bRollback)
			{
				extraInfo = "ROLLBACK";
			}
			else
			{
				extraInfo = "SHUTDOWN_PENDING";
			}
		}
	}

	cout << endl << "Calpont Process statuses" << endl << endl;
	cout << "Process             Module    Status            Last Status Change        Process ID" << endl;
	cout << "------------------  ------    ---------------   ------------------------  ----------" << endl;
	try
	{
		oam.getProcessStatus(systemprocessstatus, port);

		string prevModule = systemprocessstatus.processstatus[0].Module;

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if( prevModule != systemprocessstatus.processstatus[i].Module)
				cout << endl;	//added a space line between different modules

			cout.setf(ios::left);
			cout.width(20);
			cout << systemprocessstatus.processstatus[i].ProcessName;
			cout.width(10);
			cout << systemprocessstatus.processstatus[i].Module;
			cout.width(18);
			state = systemprocessstatus.processstatus[i].ProcessOpState;
			// For these processes, if state is ACTIVE and we're in write
			// suspend, then we want to display the extra data instead of state.
			// Otherwise, we ignore extra data and display state.
			if (state == ACTIVE && bSuspend &&
				(   systemprocessstatus.processstatus[i].ProcessName == "DMLProc"
			     || systemprocessstatus.processstatus[i].ProcessName == "DDLProc"
			     || systemprocessstatus.processstatus[i].ProcessName == "WriteEngineServer"))
			{
				printState(LEAVE_BLANK, extraInfo);
			}
			else
			{
				state = systemprocessstatus.processstatus[i].ProcessOpState;
				printState(state, " ");
			}
			cout.width(24);
			string stime = systemprocessstatus.processstatus[i].StateChangeDate ;
			stime = stime.substr (0,24);
			cout << stime;
			if ( state == COLD_STANDBY ) {
				cout << endl;				
				continue;
			}
			else
			{
				cout.setf(ios::right);
				cout.width(12);
				if ( systemprocessstatus.processstatus[i].ProcessID != 0 )
					cout << systemprocessstatus.processstatus[i].ProcessID << endl;
				else
					cout << endl;
			}
			cout.unsetf(ios::right);

			prevModule = systemprocessstatus.processstatus[i].Module;

		}
	}
	catch (exception& e)
	{
		cout << endl << "**** printProcessStatus Failed =  " << e.what() << endl;
	}
}

/******************************************************************************************
 * @brief	printModuleCpuUsers
 *
 * purpose:	get and Display Module TOP CPU users
 *
 ******************************************************************************************/
void printModuleCpuUsers(TopProcessCpuUsers topprocesscpuusers)
{
	cout << "Module '" + topprocesscpuusers.ModuleName + "' Top CPU Users" << endl << endl;
	cout << "Process             CPU Usage %" << endl;
	cout << "-----------------   -----------" << endl;

	for( unsigned int i = 0 ; i < topprocesscpuusers.processcpuuser.size(); i++)
	{
		cout.setf(ios::left);
		cout.width(25);
		cout << topprocesscpuusers.processcpuuser[i].ProcessName;
		cout.width(10);
		cout << topprocesscpuusers.processcpuuser[i].CpuUsage << endl;
	}
	cout << endl;
}

/******************************************************************************************
 * @brief	printModuleCpu
 *
 * purpose:	get and Display Module CPU Usage
 *
 ******************************************************************************************/
void printModuleCpu(ModuleCpu modulecpu)
{
	Oam oam;

	cout << endl << "Module '" + modulecpu.ModuleName + "' CPU Usage % = " + oam.itoa(modulecpu.CpuUsage) << endl;
}

/******************************************************************************************
 * @brief	printModuleMemoryUsers
 *
 * purpose:	get and Display Module TOP Memory users
 *
 ******************************************************************************************/
void printModuleMemoryUsers(TopProcessMemoryUsers topprocessmemoryusers)
{
	cout << "Module '" + topprocessmemoryusers.ModuleName + "' Top Memory Users (in bytes)" << endl << endl;
	cout << "Process             Memory Used  Memory Usage %" << endl;
	cout << "-----------------   -----------  --------------" << endl;

	for( unsigned int i = 0 ; i < topprocessmemoryusers.processmemoryuser.size(); i++)
	{
		cout.setf(ios::left);
		cout.width(20);
		cout << topprocessmemoryusers.processmemoryuser[i].ProcessName;
		cout.width(19);
		cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsed;
		cout.width(3);
		cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsage << endl;
	}
	cout << endl;
}

/******************************************************************************************
 * @brief	printModuleMemory
 *
 * purpose:	get and Display Module Memory Usage
 *
 ******************************************************************************************/
void printModuleMemory(ModuleMemory modulememory)
{
	Oam oam;
	cout.setf(ios::left);
	cout.width(8);
	cout << modulememory.ModuleName;
	cout.width(11);
	cout << oam.itoa(modulememory.MemoryTotal);
	cout.width(10);
	cout << oam.itoa(modulememory.MemoryUsed);
	cout.width(13);
	cout << oam.itoa(modulememory.cache);
	cout.width(9);
	cout << oam.itoa(modulememory.MemoryUsage);
	cout.width(12);
	cout << oam.itoa(modulememory.SwapTotal);
	cout.width(16);
	cout << oam.itoa(modulememory.SwapUsed);
	cout.width(7);
	cout << oam.itoa(modulememory.SwapUsage);
	cout << endl;
}

/******************************************************************************************
 * @brief	printModuleDisk
 *
 * purpose:	get and Display Module disk usage
 *
 ******************************************************************************************/
void printModuleDisk(ModuleDisk moduledisk)
{
	Oam oam;

	cout << "Module '" + moduledisk.ModuleName + "' Disk Usage (in 1K blocks)" << endl << endl;
	cout << "Mount Point                    Total Blocks  Used Blocks   Usage %" << endl;
	cout << "-----------------------------  ------------  ------------  -------" << endl;

	string etcdir = startup::StartUp::installDir() + "/etc";
	for( unsigned int i = 0 ; i < moduledisk.diskusage.size(); i++)
	{
		//skip mounts to other server disk
		if ( moduledisk.diskusage[i].DeviceName.find("/mnt", 0) == string::npos &&
				moduledisk.diskusage[i].DeviceName.find(etcdir, 0) == string::npos ) {
			cout.setf(ios::left);
			cout.width(31);
			cout << moduledisk.diskusage[i].DeviceName;
			cout.width(14);
			cout << oam.itoa(moduledisk.diskusage[i].TotalBlocks);
			cout.width(17);
			cout << moduledisk.diskusage[i].UsedBlocks;
			cout.width(2);
			cout << moduledisk.diskusage[i].DiskUsage << endl;
		}
	}
	cout << endl;
}

/******************************************************************************************
 * @brief	printModuleResources
 *
 * purpose:	get and Display Module resource usage
 *
 ******************************************************************************************/
void printModuleResources(TopProcessCpuUsers topprocesscpuusers, ModuleCpu modulecpu, TopProcessMemoryUsers topprocessmemoryusers, ModuleMemory modulememory, ModuleDisk moduledisk)
{
	Oam oam;
	string etcdir = startup::StartUp::installDir() + "/etc";

	cout << endl << "Module '" + topprocesscpuusers.ModuleName + "' Resource Usage" << endl << endl;

	cout << "CPU: " + oam.itoa(modulecpu.CpuUsage) << "% Usage" << endl;

	cout << "Mem:  " << oam.itoa(modulememory.MemoryTotal) << "k total, " << oam.itoa(modulememory.MemoryUsed);
	cout << "k used, " << oam.itoa(modulememory.cache) << "k cache, " << oam.itoa(modulememory.MemoryUsage) << "% Usage" << endl;
	cout << "Swap: " << oam.itoa(modulememory.SwapTotal) << " k total, " << oam.itoa(modulememory.SwapUsed);
	cout << "k used, " << oam.itoa(modulememory.SwapUsage) << "% Usage" << endl;

	cout << "Top CPU Process Users: ";

	for( unsigned int i = 0 ; i < topprocesscpuusers.processcpuuser.size(); i++)
	{
		cout << topprocesscpuusers.processcpuuser[i].ProcessName << " ";
		cout << topprocesscpuusers.processcpuuser[i].CpuUsage;
		if ( i+1 != topprocesscpuusers.processcpuuser.size() )
			cout << "%, ";
		else
			cout << "%";
	}
	cout << endl;

	cout << "Top Memory Process Users: ";

	for( unsigned int i = 0 ; i < topprocessmemoryusers.processmemoryuser.size(); i++)
	{
		cout << topprocessmemoryusers.processmemoryuser[i].ProcessName << " ";
		cout << topprocessmemoryusers.processmemoryuser[i].MemoryUsage;
		if ( i+1 != topprocessmemoryusers.processmemoryuser.size() )
			cout << "%, ";
		else
			cout << "%";
	}
	cout << endl;

	cout << "Disk Usage: ";

	for( unsigned int i = 0 ; i < moduledisk.diskusage.size(); i++)
	{
		//skip mounts to other server disk
		if ( moduledisk.diskusage[i].DeviceName.find("/mnt", 0) == string::npos &&
				moduledisk.diskusage[i].DeviceName.find(etcdir, 0) == string::npos ) {
			cout << moduledisk.diskusage[i].DeviceName << " ";
			cout << moduledisk.diskusage[i].DiskUsage;
			if ( i+1 != moduledisk.diskusage.size() )
				cout << "%, ";
			else
				cout << "%";
		}
	}
	cout << endl << endl;
}

/******************************************************************************************
 * @brief	printModuleResources
 *
 * purpose:	get and Display Module resource usage
 *
 ******************************************************************************************/
void printState(int state, std::string addInfo)
{
	switch (state) {
		case MAN_OFFLINE:
			cout << MANOFFLINE + addInfo;
			break;
		case AUTO_OFFLINE:
			cout << AUTOOFFLINE + addInfo;
			break;
		case MAN_INIT:
			cout << MANINIT + addInfo;
			break;
		case AUTO_INIT:
			cout << AUTOINIT + addInfo;
			break;
		case ACTIVE:
			cout << ACTIVESTATE + addInfo;
			break;
		case LEAVE_BLANK:
			cout << addInfo;
			break;
		case STANDBY:
			cout << STANDBYSTATE + addInfo;
			break;
		case FAILED:
			cout << FAILEDSTATE + addInfo;
			break;
		case UP:
			cout << UPSTATE + addInfo;
			break;
		case DOWN:
			cout << DOWNSTATE + addInfo;
			break;
		case COLD_STANDBY:
			cout << COLDSTANDBYSTATE + addInfo;
			break;
		case INITIAL:
			cout << INITIALSTATE + addInfo;
			break;
		case MAN_DISABLED:
			cout << MANDISABLEDSTATE + addInfo;
			break;
		case AUTO_DISABLED:
			cout << AUTODISABLEDSTATE + addInfo;
			break;
		case STANDBY_INIT:
			cout << STANDBYINIT + addInfo;
			break;
		case BUSY_INIT:
			cout << BUSYINIT + addInfo;
			break;
		case DEGRADED:
			cout << DEGRADEDSTATE + addInfo;
			break;
		default:
			cout << INITIALSTATE + addInfo;
			break;
	}
}

/******************************************************************************************
 * @brief	checkPromptThread
 *
 * purpose:	check for exit out of repeat command
 *
 ******************************************************************************************/
static void checkPromptThread()
{
    char* pcommand = 0;

	while(true)
	{
		// check input
		pcommand = readline("");
	
		if (!pcommand) {                        // user hit <Ctrl>-D
			repeatStop = true;
			break;
		}
		free(pcommand);
		pcommand = 0;
	}
	pthread_exit(0);
	return;
}

/******************************************************************************************
 * @brief	getParentOAMModule
 *
 * purpose:	get Parent OAm Module name
 *
 ******************************************************************************************/
std::string getParentOAMModule()
{
	Oam oam;

	// Get Parent OAM module Name
	try{
		string parentOAMModule;
		oam.getSystemConfig("ParentOAMModuleName", parentOAMModule);
		return parentOAMModule;
	}
	catch(...)
	{
		cout << endl << "**** Failed : Failed to read Parent OAM Module Name" << endl;
		exit(-1);
	}
}

/******************************************************************************************
 * @brief	checkForDisabledModules
 *
 * purpose:	Chcek and report any modules in a disabled state
 *
 ******************************************************************************************/
bool checkForDisabledModules()
{

	SystemModuleTypeConfig systemmoduletypeconfig;
	Oam oam;

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch(...)
	{
		return false;
	}

	bool found = false;
	bool dbroot = false;
	for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
		if( moduleCount == 0)
			continue;

		DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
		{
			string moduleName = (*pt).DeviceName;

			// report DISABLED modules
			try{
				int opState;
				bool degraded;
				oam.getModuleStatus(moduleName, opState, degraded);

				if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED) {
					if (!found) {
						cout << "   NOTE: These module(s) are DISABLED: ";
						found = true;
					}
					cout << moduleName << " ";

					//check if module has any dbroots assigned to it
					string PMID = moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;
					DBRootConfigList dbrootConfigList;
					try
					{
						oam.getPmDbrootConfig(atoi(PMID.c_str()), dbrootConfigList);

						if ( dbrootConfigList.size() != 0 )
							dbroot = true;
					}
					catch (exception& e)
					{}
				}
			}
			catch(...)
			{}
		}
	}

	if(found)
		cout << endl << endl;

	if(dbroot)
		return false;

	return true;
}

/** @brief Ask the user for cancel/wait/rollback/force
 *  
 *  When a Shutdown, stop, restart or suspend operation is
 *  requested but there are active transactions of some sort,
 *  we ask the user what to do.
 */
CC_SUSPEND_ANSWER AskSuspendQuestion(int CmdID)
{
	char* szAnswer = 0;
	char *p;
	string argument = "cancel";

	const char* szCommand = "Unknown";
	switch (CmdID)
	{
		case 16:
			szCommand = "stop";
			break;
		case 17:
			szCommand = "shutdown";
			break;
		case 19:
			szCommand = "restart";
			break;
		case 28:
			szCommand = "switch parent oam";
			break;
		case 32:
			szCommand = "suspend";
			break;
		default:
			return CANCEL;
			break;
	}
	cout << "Your options are:" << endl
		 << "    Cancel    -- Cancel the " << szCommand << " request" << endl
		 << "    Wait      -- Wait for write operations to end and then " << szCommand << endl;
//		 << "    Rollback  -- Rollback all transactions and then " << szCommand << endl;
	if (CmdID != 28 && CmdID != 32)
	{
		cout << "    Force     -- Force a " << szCommand << endl;
	}

	while(true)
	{
        argument = "cancel";
		// read input
		szAnswer = readline("What would you like to do: [Cancel]: ");

		if (szAnswer && *szAnswer) 
		{
			p = strtok(szAnswer," ");
			argument = p;
			free(szAnswer);
			szAnswer = 0;
		}

		// In case they just hit return.
		if (szAnswer) 
		{
			free(szAnswer);
			szAnswer = 0;
		}

		// convert argument into lowercase
		transform(argument.begin(), argument.end(), argument.begin(), to_lower());

		if( argument == "cancel")
		{
			return CANCEL;
		}
		else if( argument == "wait")
		{
			return WAIT;
		}
//		else if( argument == "rollback")
//		{
//			return ROLLBACK;
//		}
		else if( argument == "force" && (CmdID == 16 || CmdID == 17 || CmdID == 19))
		{
			return FORCE;
		}
		else
		{
			cout << argument << " is an invalid response" << endl;
		}
	}
}

// vim:ts=4 sw=4:

