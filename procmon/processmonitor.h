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

/***************************************************************************
* $Id: processmonitor.h 1993 2013-04-04 18:33:48Z rdempsey $
*
 ***************************************************************************/


#ifndef _PROCESSMONITOR_H_
#define _PROCESSMONITOR_H_

#include <string>
#include <sys/shm.h>
#include <sys/wait.h>
#include <errno.h>

#include "liboamcpp.h"
#include "shmkeys.h"
#include "snmpglobal.h"
#include "socketclosed.h"

namespace processmonitor {

#define INIT_SUPPORT 0	// 0 = 'INIT -0' COMMAND COMPILED IN, 1 = 'INIT -0' COMMANDS COMPILED OUT 

#define MAXARGUMENTS 15
#define MAXDEPENDANCY 6

/**
 * @brief processStructure Process Config Structure
 */
typedef   struct processStructure
{
	std::string ProcessModuleType;
	std::string ProcessName;
	std::string ProcessLocation;
	std::string	ProcessArgs[MAXARGUMENTS]; 
	time_t 		currentTime;
	uint16_t 	launchID;
	pid_t  		processID;
	uint16_t	state;
	uint16_t 	BootLaunch;
	uint16_t 	dieCounter;
	std::string RunType;
	std::string	DepProcessName[MAXDEPENDANCY]; 
	std::string	DepModuleName[MAXDEPENDANCY];
	std::string LogFile;
} processInfo;

typedef   std::vector<processInfo>		processList;


/**
 * @brief shmProcessStatus Process Status Structure
 */
struct shmProcessStatus
{
	pid_t     	ProcessID;                    //!< Process ID number
	char 		StateChangeDate[24];          //!< Last time/date state change
	uint16_t 	ProcessOpState;               //!< Process Operational State
};

/**
 * @brief processStatus Process Status Structure
 */
typedef   struct processStatus
{
	std::string ProcessName;
	std::string ModuleName;	
	uint16_t 	tableIndex;
} processstatus;

typedef   std::vector<processstatus>	processStatusList;

/**
 * @brief shmDeviceStatus Status Structure
 */
#define NAMESIZE	128
#define DATESIZE	24

struct shmDeviceStatus
{
	char 		Name[NAMESIZE];                      //!< Device Name
	char 		StateChangeDate[DATESIZE];          //!< Last time/date state change
	uint16_t 	OpState;                      		//!< Device Operational State
};


/**
 * @brief MonitorConfig class builds a list of process it starts
 *
 */
class MonitorConfig
{
public:
    	/**
	 * @brief constructor
	 *
	 */
    MonitorConfig();
    	/**
	 * @brief destructor
	 *
	 */
    ~MonitorConfig();

    /**
     * moduleName accessor
     */
    const std::string&	moduleName() const
    {
	return flocalModuleName;
    }

    /**
     * moduleType accessor
     */
    const std::string&	moduleType() const
    {
	return flocalModuleType;
    }

     /**
     * moduleName accessor
     */
    const uint16_t&	moduleID() const
    {
	return flocalModuleID;
    }

     /**
     * parentName accessor
     */
    const std::string&	OAMParentName() const
    {
	return fOAMParentModuleName;
    }

   /**
     * parentFlag accessor
     */
    const bool&	OAMParentFlag() const
    {
	return fOAMParentModuleFlag;
    }

   /**
     * ServerInstallType accessor
     */
    const uint16_t&	ServerInstallType() const
    {
	return fserverInstallType;
    }

     /**
     * StandbyName accessor
     */
    const std::string&	OAMStandbyName() const
    {
	return fOAMStandbyModuleName;
    }

   /**
     * standbyParentFlag accessor
     */
    const bool&	OAMStandbyParentFlag() const
    {
	return fOAMStandbyModuleFlag;
    }

   /**
     * SoftwareVersion accessor
     */
    const std::string&	SoftwareVersion() const
    {
	return fsoftwareVersion;
    }

   /**
     * SoftwareRelease accessor
     */
    const std::string&	SoftwareRelease() const
    {
	return fsoftwareRelease;
    }

    /**
     * Build a list of processes the monitor started
     */
    void	buildList(std::string ProcessModuleType,
						std::string processName,
						std::string ProcessLocation, 
						std::string arg_list[MAXARGUMENTS], 
						uint16_t launchID, 
						pid_t processID, 
						uint16_t state,
  						uint16_t BootLaunch,
						std::string RunType,
						std::string DepProcessName[MAXDEPENDANCY], 
						std::string DepModuleName[MAXDEPENDANCY],
						std::string logFile);

    /**
     * return the process list
     */
    processList*	monitoredListPtr();

     /**
     * return the process ID
     */
    
    void  findProcessInfo (pid_t processID, processInfo& info);

  	/**
	 * @brief copy constructor
	 *
	 */
    MonitorConfig(const MonitorConfig& rhs);
  	/**
	 * @brief copy assignment operator
	 *
	 */
    MonitorConfig& operator=(const MonitorConfig& rhs);


private:
    std::string flocalModuleName;
    std::string flocalModuleType;
	uint16_t	flocalModuleID;
    std::string fOAMParentModuleName;
    bool 		fOAMParentModuleFlag;
	uint16_t		fserverInstallType;
    std::string fOAMStandbyModuleName;
	bool			fOAMStandbyModuleFlag;

    processList fmonitoredListPtr;
	std::string fsoftwareVersion;
	std::string fsoftwareRelease;
};

/**
 * @brief MonitorLog class logs the activities between Process Monitor
 * and its child processes for debugging purpose.
 */
class MonitorLog
{
public:
    /**
     * Constructor:open the log file for writing
     */
    MonitorLog();

    /**
     * Destructor:close the log file
     */
    ~MonitorLog();

  /**
   * @brief Write the message to the log
   */
    void writeLog(const int lineNumber, const std::string logContent, const logging::LOG_TYPE logType = logging::LOG_TYPE_INFO);

  /**
   * @brief Write the message to the log
   */
    void writeLog(const int lineNumber, const int logContent, const logging::LOG_TYPE logType = logging::LOG_TYPE_INFO);

 
private:
    std::ofstream	logFile;
    	/**
	 * @brief copy constructor
	 *
	 */
    MonitorLog(const MonitorLog& rhs);
    	/**
	 * @brief copy assignment operator
	 *
	 */
    MonitorLog& operator=(const MonitorLog& rhs);
    
};


/**
 * @brief ProcessMonitor class takes requests from Process Manager for
 * starting, stopping, and restarting processes. It monitors the processes it started,
 * logs its events, and restarts the died processes.
 */
class ProcessMonitor
{
public:
    /**
     * Constructor
     */
    ProcessMonitor(MonitorConfig &config, MonitorLog &log);

    /**
     * Default Destructor
     */
    ~ProcessMonitor();
    /**
     * Start a process
     */
    pid_t	startProcess(std::string processModuleType, std::string processName, std::string processLocation, 
						std::string arg_list[MAXARGUMENTS], uint16_t launchID, uint16_t BootLaunch, 
						std::string RunType, std::string DepProcessName[MAXDEPENDANCY] , 
						std::string DepModuleName[MAXDEPENDANCY], std::string LogFile, uint16_t startType,
						uint16_t actIndicator = oam::FORCEFUL);

    /**
     * get Alarm Data and send to requester
     */
	int getAlarmData(messageqcpp::IOSocket mq, int type, std::string date);

    /**
     * Stop a process
     */
    int		stopProcess(pid_t processID, std::string processName, std::string processLocation, int actionIndicator, bool manualFlag);

    /**
     * Re-init a process
     */
    int		reinitProcess(pid_t processID, std::string processName, int actionIndicator);

    /**
     * Stop all processes started by this monitor
     */
    int	  	stopAllProcess(int actionIndicator);

    /**
     * receive and process message
     */
    void	processMessage(messageqcpp::ByteStream msg, messageqcpp::IOSocket mq);

    /**
     * send message to the monitored process or the process manager
     */
    int		sendMessage(const std::string& toWho, const std::string& message);

    /**
     * check child process heart beat
     */
    int		checkHeartBeat(const std::string processName);

     /**
     * send a trap and log the process information
     */
    void	sendAlarm(std::string alarmItem, oam::ALARMS alarmID, int action);

    /**
     *@brief update process disk state
     */
	bool 	updateProcessInfo(std::string processName, int state, pid_t pid);

    /**
     *@brief update log configuration
     */
	int 	updateLog(std::string action, std::string level);

    /**
     *@brief  get log configuration
     */
	int 	getConfigLog();

    /**
     *@brief  change Log File priviledges
     */
	void 	changeModLog();

    /**
     *@brief  check Power-On Test results
     */
	void 	checkPowerOnResults();

    /**
     *@brief  update Config
     */
	int  updateConfig();

    /**
     *@brief  build System Tables
     */
	int  buildSystemTables();

    /**
     *@brief  reconfigure Module fucntionality
     */
	int reconfigureModule(std::string reconfigureModuleName);

    /**
     *@brief  check Single Process State
     */
	int checkSpecialProcessState( std::string processName, std::string runType, std::string processModuleType );

    /**
     *@brief  Check if Mate Module is Active
     */
	int checkMateModuleState();

    /**
     *@brief  Create the Calpont Data directories
     */

	int createDataDirs(std::string cloud);

    /**
     *@brief  Process Restarted, inform Process Mgr
     */

	int processRestarted( std::string processName, bool manual = true );

    /**
     *@brief  update crontab to have logs archived at midnight
     */

	int changeCrontab();

    /**
     *@brief  Update Transaction Log
     */

	int changeTransactionLog();

    /**
     *@brief update Core Dump configuration
     */
	int updateCore(std::string action);

    /**
     *@brief setup Tranaction Logging
     */
	void setTransactionLog(bool action);

    /**
     *@brief Remove XM ProcMon setup
     */
	int removeXMProcMon();

    /**
     *@brief unmount from associated system
     */
	int umountSystem();

    /**
     *@brief Runs DB sanity test
     */
	int runStartupTest();

    /**
     *@brief Runs HDFS sanity test
     */
	int runHDFSTest();

     /**
     *@brief Update Calpont Config File
     */
	int updateConfigFile(messageqcpp::ByteStream msg);

	int getDBRMdata();

    /**
     *@brief Send Msg to Process Monitor
     */
	std::string sendMsgProcMon1( std::string module, messageqcpp::ByteStream msg, int requestID );

    /**
     *@brief check if module failover is needed due to a process outage
     */
	void checkProcessFailover( std::string processName);

    /**
     *@brief run upgrade script
     */
	int runUpgrade(std::string mysqlpw);

    /**
     *@brief change my.cnf
     */
	int changeMyCnf(std::string type);

    /**
     *@brief run Master Replication script
     */
	int runMasterRep(std::string& mysqlpw, std::string& masterLogFile, std::string& masterLogPos);

    /**
     *@brief run Master Distribution
     */
	int runMasterDist(std::string& password, std::string& slaveModule);

    /**
     *@brief run Slave Replication script
     */
	int runSlaveRep(std::string& mysqlpw, std::string& masterLogFile, std::string& masterLogPos);

    /**
     *@brief Amazon Instance and IP check
     */
	bool amazonIPCheck();

    /**
     *@brief UnMOunt any extra dbroots
     */
	void unmountExtraDBroots();

    /**
     *@brief Calculate TotalUmMemory
     */
	void calTotalUmMemory();

    /**
     *@brief Amazon Volume check
     */
	bool amazonVolumeCheck(int dbrootID = 0);

    /**
     *@brief  Check Data Mounts
     */
	int checkDataMount();

	/** @brief flush inode cache
		*/
	void flushInodeCache();


	/**
	* return the process list
	*/
	processStatusList*	statusListPtr();

	processStatusList fstatusListPtr;

private:
    /*need define copy ans assignment constructor */
    MonitorConfig	&config;
    MonitorLog		&log;
};

}   //end of namespace


	
#endif // _PROCESSMONITOR_H_
