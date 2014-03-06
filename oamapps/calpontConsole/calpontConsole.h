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
 * $Id: calpontConsole.h 3071 2013-04-04 18:45:53Z rdempsey $
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
#include "calpontsystemcatalog.h"
#include "brmtypes.h"


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
const std::string ConsoleCmdsFile= "ConsoleCmds.xml";

void getFlags(const std::string* arguments, oam::GRACEFUL_FLAG& gracefulTemp, oam::ACK_FLAG& ackTemp, oam::CC_SUSPEND_ANSWER& suspendAnswer, bool& bNeedsConfirm, std::string* password = NULL);
int confirmPrompt(std::string warningCommand);
std::string dataPrompt(std::string promptCommand);
int processCommand(std::string*);
int ProcessSupportCommand(int CommandID, std::string arguments[]);
void printAlarmSummary();
void printCriticalAlarms();
void checkRepeat(std::string*, int);
void printSystemStatus();
void printProcessStatus(std::string port = "ProcStatusControl");
void printModuleCpuUsers(oam::TopProcessCpuUsers topprocesscpuusers);
void printModuleCpu(oam::ModuleCpu modulecpu);
void printModuleMemoryUsers(oam::TopProcessMemoryUsers topprocessmemoryusers);
void printModuleMemory(oam::ModuleMemory modulememory);
void printModuleDisk(oam::ModuleDisk moduledisk);
void printModuleResources(oam::TopProcessCpuUsers topprocesscpuusers, oam::ModuleCpu modulecpu, oam::TopProcessMemoryUsers topprocessmemoryusers, oam::ModuleMemory modulememory, oam::ModuleDisk moduledisk);
void printState(int state, std::string addInfo);
std::string getParentOAMModule();
bool checkForDisabledModules();
oam::CC_SUSPEND_ANSWER AskSuspendQuestion(int CmdID);



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
									"getpmdbrootconfig",
									"getdbrootpmconfig",
									"getsystemdbrootconfig",
									"checkdbfunctional",
									""
};


#endif
