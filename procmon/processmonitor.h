/***************************************************************************
* $Id: processmonitor.h 1610 2012-02-28 21:25:08Z dhill $
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
//#define MAX_PROC_RESTART_COUNT 10

//const unsigned int MAX_PROC_RESTART_WINDOW=120;
const std::string  POWERON_TEST_RESULTS_FILE = "/usr/local/Calpont/post/st_status";
const std::string  POWERON_SYSTEM_CONFIG_FILE = "/usr/local/Calpont/post/calpont_config";

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
	uint16_t    ProcessID;                    //!< Process ID number
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
     *@brief  take action on Syslog process
     */
//	void syslogAction( std::string action);

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
     *@brief  Set Data Mounts
     */
	int setDataMount( std::string option );

    /**
     *@brief  Create the Calpont Data directories
     */

	int createDataDirs(std::string option);

    /**
     *@brief  setup the PM mount
     */

	int setupPMmount();

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
