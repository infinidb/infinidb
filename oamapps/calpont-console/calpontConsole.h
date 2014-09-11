/******************************************************************************************
 * $Id: calpontConsole.h 2273 2012-02-22 19:37:17Z dhill $
 *
 ******************************************************************************************/
/**
 * @file
 */
#ifndef CALPONTCONSOLE_H
#define CALPONTCONSOLE_H

#include <iostream>
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
#include <sys/signal.h>
#include <sys/types.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "snmpmanager.h"
#include "snmpglobal.h"


const int CmdSize = 80;
const int ArgNum = 10;
const int DescNumMax = 10;
const int cmdNum = 68;

const std::string  DEFAULT_LOG_FILE = "/var/log/Calpont/uiCommands.log";
std::ofstream   logFile;

/**
 * write the command to the log file
 */
void    writeLog(std::string command);

/** @brief location of the Process Configuration file
 */
//const string ConsoleCmdsFile= "/home/dhill/genii/oam/calpont-console/ConsoleCmds.xml";
const std::string ConsoleCmdsFile= "/usr/local/Calpont/etc/ConsoleCmds.xml";

void getFlags(const std::string* arguments, oam::GRACEFUL_FLAG& gracefulTemp, oam::ACK_FLAG& ackTemp);
int confirmPrompt(std::string warningCommand);
std::string dataPrompt(std::string promptCommand);
int processCommand(std::string*);
int ProcessSupportCommand(int CommandID, std::string arguments[]);
void getAlarmSummary();
void getCriticalAlarms();
void checkRepeat(std::string*, int);
void getSystemStatus();
void getProcessStatus(std::string port = "ProcStatusControl");
void printModuleCpuUsers(oam::TopProcessCpuUsers topprocesscpuusers);
void printModuleCpu(oam::ModuleCpu modulecpu);
void printModuleMemoryUsers(oam::TopProcessMemoryUsers topprocessmemoryusers);
void printModuleMemory(oam::ModuleMemory modulememory);
void printModuleDisk(oam::ModuleDisk moduledisk);
void printModuleResources(oam::TopProcessCpuUsers topprocesscpuusers, oam::ModuleCpu modulecpu, oam::TopProcessMemoryUsers topprocessmemoryusers, oam::ModuleMemory modulememory, oam::ModuleDisk moduledisk);
void printState(int state, std::string addInfo);
std::string getParentOAMModule();

class to_lower
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return tolower(c);
        }
};

/** @brief Hidden Support commands in lower-case
*/
const std::string supportCmds[] = {	"helpsupport",
									"stopprocess",
									"startprocess",
									"restartprocess",
									"killpid",
									"rebootsystem",
									"rebootnode",
									"stopdbrmprocess",
									"startdbrmprocess",
									"restartdbrmprocess",
									"setsystemstartupstate",
									"stopprimprocs",
									"startprimprocs",
									"restartprimprocs",
									"stopexemgrs",
									"startexemgrs",
									"restartexemgrs",
									"getprocessstatusstandby",
									"distributeconfigfile",
									"checkdbfunctional",
									""
};

#endif
