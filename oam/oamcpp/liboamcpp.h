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
 ******************************************************************************************/
/**
 * @file
 */
#ifndef LIBOAMCPP_H
#define LIBOAMCPP_H

#include <exception>
#include <stdexcept>
#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <limits.h>
#include <sstream>
#include <vector>
#ifdef __linux__
#include <sys/sysinfo.h>
#include <netdb.h>
#endif
#include <fcntl.h>

#include "bytestream.h"
#include "configcpp.h"
#include "boost/tuple/tuple.hpp"
#include "snmpmanager.h"
#include "dbrm.h"
#ifndef SKIP_SNMP
#include "license.h"
#endif

#include "messagequeue.h"

#if defined(_MSC_VER) && defined(xxxLIBOAM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace oam
{

    /*
     * 	Global OAM parmaters
     */

    /** @brief Maximum Number of Modules within the Calpont System
     */
    const int MAX_MODULE = 1024;

    /** @brief Maximum Number of DBRoots within the Calpont System
     */
    const int MAX_DBROOT = 10240;
    const int MAX_DBROOT_AMAZON = 190;	//DUE TO DEVICE NAME LIMIT

   /** @brief Maximum Number of Modules Types within the Calpont System
     */
    const int MAX_MODULE_TYPE = 3;

    /** @brief Maximum Number of External Devices within the Calpont System
     */
    const int MAX_EXT_DEVICE = 20;

    /** @brief Maximum Number of Arguments per process
     */
    const int MAX_ARGUMENTS = 15;

    /** @brief Maximum Number of Dependancy processes per process
     */
    const int MAX_DEPENDANCY = 6;

    /** @brief Maximum Number of processes within the Calpont System
     */
    const int MAX_PROCESS_PER_MODULE = 15;
    const int MAX_PROCESS = MAX_MODULE*MAX_PROCESS_PER_MODULE;

    /** @brief Maximum Number of Parameters per process
     */
    const int MAX_PARAMS = 13;

    /** @brief Maximum Module Type Size
     */
    const int MAX_MODULE_TYPE_SIZE = 2;

    /** @brief Maximum Module ID Size
     */
    const int MAX_MODULE_ID_SIZE = 4;

    /** @brief Maximum Number of NICs per Module
     */
    const int MAX_NIC = 4;

    /** @brief Unassigned Name and IP Address Value
     */
    const std::string UnassignedIpAddr = "0.0.0.0";
    const std::string UnassignedName = "unassigned";


    /** @brief Calpont System Configuration file sections
     */
    const std::string configSections[] = {	"SystemConfig",
											"SystemModuleConfig",
											"SystemModuleConfig",
											"SystemExtDeviceConfig",
											"SessionManager",
											"VersionBuffer",
											"OIDManager",
											"PrimitiveServers",
											"Installation",
											"ExtentMap",
											""
	};

    /** @brief gluster control commands
     */
    enum GLUSTER_COMMANDS
    {
        GLUSTER_STATUS,
        GLUSTER_SETDDC,
        GLUSTER_ASSIGN,
        GLUSTER_WHOHAS,
        GLUSTER_UNASSIGN,
        GLUSTER_ADD
	};


    /** @brief mysql-Calpont Action
     */
    enum MYSQLCALPONT_ACTION
    {
        MYSQL_START,
        MYSQL_STOP,
        MYSQL_RESTART,
        MYSQL_RELOAD,
		MYSQL_FORCE_RELOAD,
		MYSQL_STATUS
	};

    /** @brief Device Notification Type
     */
    enum NOTIFICATION_TYPE
    {
        NOTIFICATION_TYPE_RESERVED,  // 0 = not used
		START_PM_MASTER_DOWN,
		START_PM_STANDBY_DOWN,
		START_PM_COLD_DOWN,
		START_UM_DOWN,
		MODULE_DOWN,
		START_STANDBY_TO_MASTER,
		PM_MASTER_ACTIVE,
		PM_STANDBY_ACTIVE,
		PM_COLD_ACTIVE,
		UM_ACTIVE,
		PM_MASTER_FAILED_DISABLED,
		DBROOT_DOWN,
		DBROOT_UP,
		DB_HEALTH_CHECK_FAILED,
		DBROOT_MOUNT_FAILURE,
		MODULE_UP
    };

	const uint32_t NOTIFICATIONKEY = 0x49444231;

    /** @brief Server Type Installs
     */

    enum INSTALLTYPE
    {
        RESERVED,                              		// 0 = not used
        INSTALL_NORMAL,                       		// 1 = Normal - dm/um/pm on a seperate servers
        INSTALL_COMBINE_DM_UM_PM,                   // 2 = dm/um/pm on a single server
        INSTALL_COMBINE_DM_UM,                      // 3 = dm/um on a same server
        INSTALL_COMBINE_PM_UM                       // 4 = pm/um on a same server
    };

    /** @brief Server Monitor Message Request options
     */

    enum SERVERMONITOR_TYPE_REQUEST
    {
		GET_PROC_CPU_USAGE,
		GET_MODULE_CPU_USAGE,
		GET_PROC_MEMORY_USAGE,
		GET_MODULE_MEMORY_USAGE,
		GET_MODULE_DISK_USAGE,
		GET_ACTIVE_SQL_QUERY,
		RUN_DBHEALTH_CHECK
    };


    /** @brief OAM API Return values
     */

    enum API_STATUS
    {
        API_SUCCESS,
        API_FAILURE,
        API_INVALID_PARAMETER,
        API_FILE_OPEN_ERROR,
        API_TIMEOUT,
        API_DISABLED,
		API_FILE_ALREADY_EXIST,
		API_ALREADY_IN_PROGRESS,
        API_MINOR_FAILURE,
		API_FAILURE_DB_ERROR,
		API_INVALID_STATE,
		API_READONLY_PARAMETER,
		API_TRANSACTIONS_COMPLETE,
		API_CONN_REFUSED,
        API_CANCELLED,
        API_STILL_WORKING,
        API_MAX
    };

    /** @brief OAM Parent Module Indicator
     */

    enum OAM_MASTER_MODULE
    {
        PARENT_NO,
        PARENT_YES
    };

    /** @brief Realtime Linux OS Module Indicator
     */

/*    enum RT_LINUX_Module
    {
        RT_LINUX_NO,
        RT_LINUX_YES
    };
*/
    /** @brief Process and Hardware States
     */

    enum STATE
    {
        MAN_OFFLINE,                              // 0 = Manual disable mode
        AUTO_OFFLINE,                             // 1 = Auto disable, due to a fault
        MAN_INIT,                                 // 2 = Manual initialization mode
        AUTO_INIT,                                // 3 = Auto initialization mode
        ACTIVE,                                   // 4 = Active mode
        LEAVE_BLANK,							  // when this was standby, 'PuTTY' would show up in the console
        STANDBY,                                  // 6 = Hot Standby mode
        FAILED,                                   // 7 = Failed restoral mode
        UP,                                       // 8 = Up mode, for hardware devices
        DOWN,                                     // 9 = Down mode, for hardware devices
        COLD_STANDBY,                             // 10 = Cold Standby mode
        UNEQUIP,                                  // 11 = Unequipped mode
        EQUIP,                                    // 12 = Equipped mode
        DEGRADED,                                 // 13 = Degraded mode
        MAN_DISABLED,                             // 14 = Manual Disabled mode
        AUTO_DISABLED,                            // 15 = Auto Disabled mode
        ENABLED,                                  // 16 = Enabled mode
        INITIAL,                                  // 17 = Initial mode
		STANDBY_INIT,							  // 18 = Standby init
		BUSY_INIT, 							  	  // 19 = Busy init
		STATE_MAX								  // 20 = Max value
    };

    /** @brief Process and Hardware String States
     */

    const std::string MANOFFLINE = "MAN_OFFLINE";
    const std::string AUTOOFFLINE = "AUTO_OFFLINE";
    const std::string MANINIT = "MAN_INIT";
    const std::string AUTOINIT = "AUTO_INIT";
    const std::string ACTIVESTATE = "ACTIVE";
    const std::string STANDBYSTATE = "HOT_STANDBY";
    const std::string FAILEDSTATE = "FAILED";
    const std::string UPSTATE = "UP";
    const std::string DOWNSTATE = "DOWN";
    const std::string COLDSTANDBYSTATE = "COLD_STANDBY";
    const std::string INITIALSTATE = "INITIAL";
    const std::string DEGRADEDSTATE = "DEGRADED";
    const std::string ENABLEDSTATE = "ENABLED";
    const std::string MANDISABLEDSTATE = "MAN_DISABLED";
    const std::string AUTODISABLEDSTATE = "AUTO_DISABLED";
    const std::string STANDBYINIT = "STANDBY_INIT";
    const std::string BUSYINIT = "BUSY_INIT";

    /** @brief Module/Process Run Types
     */

    const std::string ACTIVE_STANDBY = "ACTIVE_STANDBY";
    const std::string LOADSHARE = "LOADSHARE";
    const std::string BROADCAST = "BROADCAST";
    const std::string SIMPLEX = "SIMPLEX";

    /** @brief Module Equippage states
     */

    const std::string EQUIP_YES = "EQ";
    const std::string EQUIP_NO = "NE";


    /** @brief Update Logging Levels
     */
    const std::string LogLevel[] =
    {
        "critical",
        "error",
        "warning",
        "info",
        "debug",
        "all",
        ""
    };

    /** @brief Logging Level file name
     *
     * NOTE: make sure this list is insync with above LogLevel list
     */
    const std::string LogFile[] =
    {
        "local1.=crit /var/log/Calpont/crit.log",
        "local1.=err /var/log/Calpont/err.log",
        "local1.=warning /var/log/Calpont/warning.log",
        "local1.=info /var/log/Calpont/info.log",
        "local1.=debug /var/log/Calpont/debug.log",
        "local2.=crit /var/log/Calpont/data/data_mods.log",
        ""
    };

    /** @brief Log Config Data map
     */
    typedef struct LogConfigData_struct
    {
        std::string     moduleName;
        int             configData;
    }
    LogConfigData;

    typedef std::vector<LogConfigData> SystemLogConfigData;

    /** @brief LogConfigData level bitmap
     */
    enum LEVEL_FLAGS
    {
        LEVEL_CRITICAL = 0x1,
        LEVEL_ERROR = 0x2,
        LEVEL_WARNING = 0x4,
        LEVEL_INFO = 0x8,
        LEVEL_DEBUG = 0x10,
        LEVEL_DATA = 0x20
    };

    /** @brief Alarm IDs
     */

    enum ALARMS
    {
        NO_ALARM,                                 // 0 = NO ALARM
        CPU_USAGE_HIGH,                           // 1 = CPU Usage High threahold crossed
        CPU_USAGE_MED,                            // 2 = CPU Usage Medium threshold crossed
        CPU_USAGE_LOW,                            // 3 = CPU Usage Low threashold crossed
        DISK_USAGE_HIGH,                          // 4 = DISK Usage High threahold crossed
        DISK_USAGE_MED,                           // 5 = DISK Usage Medium threshold crossed
        DISK_USAGE_LOW,                           // 6 = DISK Usage Low threashold crossed
        MEMORY_USAGE_HIGH,                        // 7 = MEMORY Usage High threahold crossed
        MEMORY_USAGE_MED,                         // 8 = MEMORY Usage Medium threshold crossed
        MEMORY_USAGE_LOW,                         // 9 = MEMORY Usage Low threashold crossed
        SWAP_USAGE_HIGH,                          // 10 = SWAP Usage High threahold crossed
        SWAP_USAGE_MED,                           // 11 = SWAP Usage Medium threshold crossed
        SWAP_USAGE_LOW,                           // 12 = SWAP Usage Low threashold crossed
        PROCESS_DOWN_AUTO,                        // 13 = Process is down due to fault
        MODULE_DOWN_AUTO,                         // 14 = Module is down due to fault
        SYSTEM_DOWN_AUTO,                         // 15 = System is down due to fault
        POWERON_TEST_SEVERE,                      // 16 = Power-On test Module Warning error
        POWERON_TEST_WARNING,                     // 17 = Power-On test Warning error
        HARDWARE_HIGH,                            // 18 = Hardware Critical alarm
        HARDWARE_MED,                             // 19 = Hardware Major alarm
        HARDWARE_LOW,                             // 20 = Hardware Minor alarm
        PROCESS_DOWN_MANUAL,                      // 21 = Process is down due to operator request
        MODULE_DOWN_MANUAL,                       // 22 = Module is down due to operator request
        SYSTEM_DOWN_MANUAL,                       // 23 = System is down due to operator request
        EXT_DEVICE_DOWN_AUTO,                     // 24 = External Device is down due to fault
        PROCESS_INIT_FAILURE,                     // 25 = Process Initization Failure
        NIC_DOWN_AUTO,                            // 26 = NIC is down due to fault
        DBRM_LOAD_DATA_ERROR,                     // 27 = DBRM Load Data error
        INVALID_SW_VERSION,                       // 28 = Invalid Software Version
        STARTUP_DIAGNOTICS_FAILURE,               // 29 = Module Startup Dianostics Failure
        CONN_FAILURE,                             // 30 = Connect Failure
        DBRM_READ_ONLY,                           // 31 = The DBRM is read-only
        EE_LICENSE_EXPIRED,                       // 32 = Enterprise License has expired
        MODULE_SWITCH_ACTIVE,                     // 33 = PM Failover / Switchover
        ROLLBACK_FAILURE,                     	  // 34 = DB Rollback Failure
        GLUSTER_DISK_FAILURE,                     // 35 = Gluster Disk Copy Failure
        MAX_ALARM_ID
    };

    /** @brief Alarm Severity
     */

    enum ALARM_SEVERITY
    {
        NO_SEVERITY,                              // 0 = N/A
        CRITICAL,                                 // 1 = CRITICAL
        MAJOR,                                    // 2 = MAJOR
        MINOR,                                    // 3 = MINOR
        WARNING,                                  // 4 = WARNING
        INFORMATIONAL                             // 5 = INFORMATIONAL
    };

    /** @brief OAM Hardware Management User Authorization level
     */

    enum AUTH_LEVEL
    {
        ADMINISTRATION,                           // 0 = Admin Level
        MAINTENANCE                               // 1 = Maintenance Level
    };

    /** @brief Boot Launch flag
     */

    enum LAUNCH_FLAG
    {
        INIT_LAUNCH,                              // 0 = Process launched by OS Init
        BOOT_LAUNCH,                              // 1 = Process launched by ProcMon at boot time
        MGR_LAUNCH                                // 2 = Process lanuched by ProcMgr after System reboot
    };

    /** @brief Process Management API request options
     *
     * 		Message from a UI to Process Manager
     */

    enum PROC_MGT_MSG_REQUEST
    {
        STOPMODULE,
        STARTMODULE,
        RESTARTMODULE,
        ENABLEMODULE,
        DISABLEMODULE,
        STARTSYSTEM,
        STOPSYSTEM,
        RESTARTSYSTEM,
        SHUTDOWNMODULE,
        SHUTDOWNSYSTEM,
        STOPPROCESS,
        STARTPROCESS,
        RESTARTPROCESS,
        UPDATELOG,
        GETCONFIGLOG,
        REINITPROCESS,
        UPDATECONFIG,
		BUILDSYSTEMTABLES,
        ADDMODULE,
        REMOVEMODULE,
        RECONFIGUREMODULE,
        STOPPROCESSTYPE,
        STARTPROCESSTYPE,
        RESTARTPROCESSTYPE,
        REINITPROCESSTYPE,
		DISTRIBUTECONFIG,
		SWITCHOAMPARENT,
		UNMOUNT,
		MOUNT,
        SUSPENDWRITES,
		FSTABUPDATE
    };

    /** @brief Process Management - Mgr to Mon request options
     *
     * 		Message from a Process Manager to Process Monitor
     */

    enum PROC_MGR_MSG_REQUEST
    {
        STOPALL,
        STOP,
        START,
        RESTART,
        STARTALL,
        PROCREINITPROCESS
    };

    /** @brief Process Management API type options
     *
     *   	Message from Process Manager to Process Monitor
     *		Process Monitor to Manager to UI
     */

    enum PROC_MGT_TYPE_REQUEST
    {
        REQUEST,
        ACK,
        REPORT_STATUS,
        PROCUPDATELOG,
        PROCGETCONFIGLOG,
        CHECKPOWERON,
		PROCUPDATECONFIG,
        HEARTBEAT_REGISTER,
        HEARTBEAT_DEREGISTER,
        HEARTBEAT_SEND,
		PROCBUILDSYSTEMTABLES,
		LOCALHEARTBEAT,
		RECONFIGURE,
		PROCESSRESTART,
		GETSOFTWAREINFO,
		UPDATEEXPORTS,
		UPDATEPARENTNFS,
		OAMPARENTACTIVE,
		UPDATECONFIGFILE,
		GETDBRMDATA,
		GETPARENTOAMMODULE,
		OAMPARENTCOLD,
		UPDATESNMPD,
		GETALARMDATA,
		GETACTIVEALARMDATA,
		RUNUPGRADE,
		PROCUNMOUNT,
		PROCMOUNT,
		PROCFSTABUPDATE
    };


    /** @brief Hardware and process shutdown flag
     */

    enum GRACEFUL_FLAG
    {
        GRACEFUL,
        FORCEFUL,
		INSTALL,
		REMOVE,
		GRACEFUL_STANDBY,
		STATUS_UPDATE,
        GRACEFUL_WAIT            // Wait for all table locks and transactions to finish.
    };

    /** @brief Acknowledgment indication flag
     */

    enum ACK_FLAG
    {
        ACK_NO,
        ACK_YES
    };

    /** @brief Responses to cancel/wait/rollback/force question
     *  
     *  When a suspend, stop, restart or shutdown of system is
     *  requested, the user is asked this question.
      */
    enum CC_SUSPEND_ANSWER
    {
        CANCEL,
        WAIT,
        ROLLBACK,
        FORCE
    };

    /** @brief Process Management Status Request types
      */

    enum STATUS_TYPE_REQUEST
    {
		GET_PROC_STATUS,
		SET_PROC_STATUS,
		GET_ALL_PROC_STATUS,
		GET_PROC_STATUS_BY_PID,
		GET_SYSTEM_STATUS,
		SET_SYSTEM_STATUS,
		SET_MODULE_STATUS,
		SET_EXT_DEVICE_STATUS,
		ADD_MODULE,
		REMOVE_MODULE,
		RECONFIGURE_MODULE,
		SET_NIC_STATUS,
		SET_PM_IPS,
		ADD_EXT_DEVICE,
		REMOVE_EXT_DEVICE,
		GET_SHARED_MEM,
		SET_DBROOT_STATUS,
		ADD_DBROOT,
		REMOVE_DBROOT
    };

    /** @brief System Software Package Structure
     *
     *   Structure that is returned by the getSystemSoftware API
     */

    struct SystemSoftware_s
    {
        std::string Version;                      //!< System Software Version
        std::string Release;                      //!< System Software Release
    };
    typedef struct SystemSoftware_s SystemSoftware;

    /** @brief System Software Package parse data
     */
    const std::string SoftwareData[] =
    {
        "version=",
        "release=",
        ""
    };

    /** @brief System Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   System Configuration data stored in the System Configuration file
     */

    struct SystemConfig_s
    {
        std::string SystemName;                    //!< System Name
        int32_t ModuleHeartbeatPeriod;           //!< Module Heartbeat period in minutes
        uint32_t ModuleHeartbeatCount;            //!< Module Heartbeat failure count
//        int32_t ProcessHeartbeatPeriod;          //!< Process Heartbeat period in minutes
        std::string NMSIPAddr;                    //!< NMS system IP address
        std::string DNSIPAddr;                    //!< DNS IP address
        std::string LDAPIPAddr;                   //!< LDAP IP address
        std::string NTPIPAddr;                    //!< NTP IP address
        uint32_t DBRootCount;                  		//!< Database Root directory Count
		std::vector<std::string> DBRoot;			//!< Database Root directories
        std::string DBRMRoot;                     //!< DBRM Root directory
        uint32_t ExternalCriticalThreshold;     	  //!< External Disk Critical Threahold %
        uint32_t ExternalMajorThreshold;        	  //!< External Disk Major Threahold %
        uint32_t ExternalMinorThreshold;        	  //!< External Disk Minor Threahold %
        uint32_t MaxConcurrentTransactions;       //!< Session Mgr Max Current Trans
        std::string SharedMemoryTmpFile;          //!< Session Mgr Shared Mem Temp file
        uint32_t NumVersionBufferFiles;       		//!< Version Buffer number of files
        uint32_t VersionBufferFileSize;       		//!< Version Buffer file size
        std::string OIDBitmapFile;                  //!< OID Mgr Bitmap File name
        uint32_t FirstOID;       			        //!< OID Mgr First O
		std::string ParentOAMModule;				//!< Parent OAM Module Name
		std::string StandbyOAMModule;				//!< Standby Parent OAM Module Name
        uint32_t TransactionArchivePeriod;			//!< Tranaction Archive Period in minutes
    };
    typedef struct SystemConfig_s SystemConfig;

    /** @brief Host/IP Address Config Structure
     *
     */

    struct HostConfig_s
    {
        std::string HostName;         			//!< Host Name
        std::string IPAddr;                 	//!< IP address
        uint16_t NicID;                 		//!< NIC ID
    };
    typedef struct HostConfig_s HostConfig;

    /** @brief Host/IP Address Config List
     *
     */

	typedef std::vector<HostConfig> HostConfigList;

    /** @brief Device Network Config Structure
     *
     */

    struct DeviceNetworkConfig_s
    {
        std::string DeviceName;                 //!< Device Name
        std::string UserTempDeviceName;         //!< User Temp Device Name
        std::string DisableState;               //!< Disabled State
		HostConfigList hostConfigList;	        //!< IP Address and Hostname List
    };

    typedef struct DeviceNetworkConfig_s DeviceNetworkConfig;

    /** @brief Device Network Config List
     *
     */

	typedef std::vector<DeviceNetworkConfig> DeviceNetworkList;

    /** @brief Disk Monitor File System List
     *
     */

	typedef std::vector<std::string> DiskMonitorFileSystems;

    /** @brief DBRoot Config List
     *
     */

	typedef std::vector<uint16_t> DBRootConfigList;

    /** @brief Device DBRoot Config Structure
     *
     */

    struct DeviceDBRootConfig_s
    {
        uint16_t DeviceID;                 		//!< Device ID
		DBRootConfigList dbrootConfigList;	    //!< DBRoot List
    };

    typedef struct DeviceDBRootConfig_s DeviceDBRootConfig;

    /** @brief Device DBRoot Config List
     *
     */

	typedef std::vector<DeviceDBRootConfig> DeviceDBRootList;

    /** @brief Module Type Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   Module Type Configuration data stored in the System Configuration file
     */

    struct PmDBRootCount_s
    {
        uint16_t pmID;                 		//!< PM ID
		uint16_t count;	    				//!< DBRoot Count
    };

    struct ModuleTypeConfig_s
    {
        std::string ModuleType;                   //!< Module Type
        std::string ModuleDesc;                   //!< Module Description
        std::string RunType;                      //!< Run Type
        uint16_t ModuleCount;                     //!< Module Equipage Count
        uint16_t ModuleCPUCriticalThreshold;      //!< CPU Critical Threahold %
        uint16_t ModuleCPUMajorThreshold;         //!< CPU Major Threahold %
        uint16_t ModuleCPUMinorThreshold;         //!< CPU Minor Threahold %
        uint16_t ModuleCPUMinorClearThreshold;    //!< CPU Minor Clear Threahold %
        uint16_t ModuleMemCriticalThreshold;      //!< Mem Critical Threahold %
        uint16_t ModuleMemMajorThreshold;         //!< Mem Major Threahold %
        uint16_t ModuleMemMinorThreshold;         //!< Mem Minor Threahold %
        uint16_t ModuleDiskCriticalThreshold;     //!< Disk Critical Threahold %
        uint16_t ModuleDiskMajorThreshold;        //!< Disk Major Threahold %
        uint16_t ModuleDiskMinorThreshold;        //!< Disk Minor Threahold %
        uint16_t ModuleSwapCriticalThreshold;     //!< Swap Critical Threahold %
        uint16_t ModuleSwapMajorThreshold;        //!< Swap Major Threahold %
        uint16_t ModuleSwapMinorThreshold;        //!< Swap Minor Threahold %
		DeviceNetworkList ModuleNetworkList;	  //!< Module IP Address and Hostname List
        DiskMonitorFileSystems FileSystems; 	  //!< Module Disk File System list
		DeviceDBRootList ModuleDBRootList;		  //!< Module DBRoot 
    };
    typedef struct ModuleTypeConfig_s ModuleTypeConfig;


    /** @brief System Module Type Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   System Module Configuration data stored in the System Configuration file
     */

    struct SystemModuleTypeConfig_s
    {
        std::vector<ModuleTypeConfig> moduletypeconfig;   //!< Module Type Configuration Structure
    };
    typedef struct SystemModuleTypeConfig_s SystemModuleTypeConfig;

    /** @brief Module Name Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   Module Name Configuration data stored in the System Configuration file
     */

    struct ModuleConfig_s
    {
        std::string ModuleName;                   //!< Module Name
        std::string ModuleType;                   //!< Module Type
        std::string ModuleDesc;                   //!< Module Description
        std::string DisableState;                 //!< Disabled State
		HostConfigList hostConfigList;	          //!< IP Address and Hostname List
		DBRootConfigList dbrootConfigList;	      //!< DBRoot ID list
    };
    typedef struct ModuleConfig_s ModuleConfig;


    /** @brief External Device Name Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   External Device Name Configuration data stored in the System Configuration file
     */

    struct ExtDeviceConfig_s
    {
        std::string Name;                   		//!< Name
        std::string IPAddr;           	  			//!< IP address
        std::string DisableState;                 	//!< Disabled State
    };
    typedef struct ExtDeviceConfig_s ExtDeviceConfig;

    /** @brief System External Device Configuration Structure
     *
     *   Structure that is returned by the getSystemConfigFile API for the
     *   External Device Type Configuration data stored in the System Configuration file
     */

    struct SystemExtDeviceConfig_s
    {
        uint16_t Count;                     			//!< External Device Equipage Count
        std::vector<ExtDeviceConfig> extdeviceconfig;   //!< External Device IP Address and name List
    };
    typedef struct SystemExtDeviceConfig_s SystemExtDeviceConfig;

    /** @brief Module Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Status data stored in the System Status file
     */

    struct ModuleStatus_s
    {
        std::string Module;                   //!< Module Name
        uint16_t ModuleOpState;                //!< Operational State
        std::string StateChangeDate;              //!< Last time/date state change
    };
    typedef struct ModuleStatus_s ModuleStatus;

    /** @brief System Module Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Module Status data stored in the System Status file
     */

    struct SystemModuleStatus_s
    {
        std::vector<ModuleStatus> modulestatus;   //!< Module Status Structure
    };
    typedef struct SystemModuleStatus_s SystemModuleStatus;


    /** @brief Ext Device Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Status data stored in the System Status file
     */

    struct ExtDeviceStatus_s
    {
        std::string Name;                   		//!< External Device Name
        uint16_t OpState;                			//!< Operational State
        std::string StateChangeDate;              	//!< Last time/date state change
    };
    typedef struct ExtDeviceStatus_s ExtDeviceStatus;

    /** @brief System Ext Device Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System System Ext Status data stored in the System Status file
     */

    struct SystemExtDeviceStatus_s
    {
        std::vector<ExtDeviceStatus> extdevicestatus;   //!< External Device Status Structure
    };
    typedef struct SystemExtDeviceStatus_s SystemExtDeviceStatus;


    /** @brief DBRoot Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Status data stored in the System Status file
     */

    struct DbrootStatus_s
    {
        std::string Name;                   		//!< Dbroot Name
        uint16_t OpState;                			//!< Operational State
        std::string StateChangeDate;              	//!< Last time/date state change
    };
    typedef struct DbrootStatus_s DbrootStatus;

    /** @brief Dbroot Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System System Ext Status data stored in the System Status file
     */

    struct SystemDbrootStatus_s
    {
        std::vector<DbrootStatus> dbrootstatus;   //!< Dbroot Status Structure
    };
    typedef struct SystemDbrootStatus_s SystemDbrootStatus;

    /** @brief NIC Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Status data stored in the System Status file
     */

    struct NICStatus_s
    {
        std::string HostName;                   //!< NIC Name
        uint16_t NICOpState;                	//!< Operational State
        std::string StateChangeDate;              //!< Last time/date state change
    };
    typedef struct NICStatus_s NICStatus;

    /** @brief System NIC Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System NIC Status data stored in the System Status file
     */

    struct SystemNICStatus_s
    {
        std::vector<NICStatus> nicstatus;   //!< NIC Status Structure
    };
    typedef struct SystemNICStatus_s SystemNICStatus;

    /** @brief System Status Structure
     *
     *   Structure that is returned by the getSystemStatus API for the
     *   System Status data stored in the System Status file
     */

    struct SystemStatus_s
    {
        uint16_t SystemOpState;                   		//!< System Operational State
        std::string StateChangeDate;              		//!< Last time/date state change
        SystemModuleStatus systemmodulestatus;    		//!< System Module status
        SystemExtDeviceStatus systemextdevicestatus;    //!< System Ext Device status
        SystemNICStatus systemnicstatus;    			//!< System NIC status
        SystemDbrootStatus systemdbrootstatus;    		//!< System DBroot status
    };
    typedef struct SystemStatus_s SystemStatus;

    /** @brief Process Configuration Structure
     *
     *   Structure that is returned by the getSystemProcessConfig API for the
     *   Process Configuration data stored in the Process Configuration file
     */

    struct ProcessConfig_s
    {
        std::string ProcessName;                  //!< Process Name
        std::string ModuleType;                   //!< Module Type that process is running on
        std::string ProcessLocation;              //!< Process launch location
        std::string ProcessArgs[MAX_ARGUMENTS];   //!< Process Arguments
        uint16_t    BootLaunch;                   //!< Boot Launch flag, 0 = init, 1 = boot, 2 = Mgr
        uint16_t    LaunchID;                     //!< Launch ID number
        std::string DepProcessName[MAX_DEPENDANCY]; //!< Dependent Processes
        std::string DepModuleName[MAX_DEPENDANCY];//!< Dependent Process Module Name
        std::string RunType;               	      //!< Process Run Type
        std::string LogFile;               	      //!< Process Log File Indicator
    };
    typedef struct ProcessConfig_s ProcessConfig;

    /** @brief System Process Configuration Structure
     *
     *   Structure that is returned by the getSystemProcessConfig API for the
     *   System Process Configuration data stored in the Process Configuration file
     */

    struct SystemProcessConfig_s
    {
        std::vector<ProcessConfig> processconfig; //!< Process Configuration Structure
    };
    typedef struct SystemProcessConfig_s SystemProcessConfig;

    /** @brief Process Status Structure
     *
     *   Structure that is returned by the getProcessStatus API for the
     *   Process Status data stored in the Process Status file
     */

    struct ProcessStatus_s
    {
        std::string ProcessName;                  //!< Process Name
        std::string Module;                   //!< Module Name that process is running on
        pid_t    	ProcessID;                    //!< Process ID number
        std::string StateChangeDate;              //!< Last time/date state change
        uint16_t 	ProcessOpState;               //!< Process Operational State
    };	
    typedef struct ProcessStatus_s ProcessStatus;


    /** @brief System Process Status Structure
     *
     *   Structure that is returned by the getProcessStatus API for the
     *   System Process Status data stored in the Process Status file
     */

    struct SystemProcessStatus_s
    {
        std::vector<ProcessStatus> processstatus; //!< Process Status Structure
    };
    typedef struct SystemProcessStatus_s SystemProcessStatus;

    /** @brief Alarm Configuration Structure
     *
     *   Structure that is returned by the getAlarmConfig API for the
     *   Alarm Configuration data stored in the Alarm Configuration file
     */

    struct AlarmConfig_s
    {
        uint16_t AlarmID;                         //!< Alarm ID
        std::string BriefDesc;                    //!< Brief Description
        std::string DetailedDesc;                 //!< Detailed Description
        uint16_t Severity;                        //!< Severity - 1=Critical, 2=Major, 3=Minor, 4=Warning, 5=Informational
        uint16_t Threshold;                       //!< Stop reporting threshold
        uint16_t Occurrences;                     //!< Alarm Occurrences within 30 min window
        uint32_t LastIssueTime;                   //!< last time alarms was issued
    };
    typedef struct AlarmConfig_s AlarmConfig;

    /** @brief Local Module OAM Configuration StructureLOG_
     *
     *   Structure that is returned by the getModuleInfo API for the
     *   Local Module OAM Configuration data stored in the Local Module
     *   Configuration file
	 *   Returns: Local Module Name, Local Module Type, Local Module ID, 
	 *					OAM Parent Module Name, OAM Parent Flag,
	 *					Server Type Install ID, OAM Standby Parent Module Name,
	 *					OAM Standby Parent Flag,
     */

    typedef boost::tuple<std::string, std::string, uint16_t, std::string, bool, uint16_t, std::string, bool > oamModuleInfo_t;

    /** @brief My Process OAM Status Structure
     *
     *   Structure that is returned by the getMyProcessStatus API for the
     *   Local Process OAM Status data stored in the Process Status file
     *	 Returns: Process ID, Process Name, and Process State
     */

    typedef boost::tuple<uint16_t, std::string, uint16_t> myProcessStatus_t;

    /** @brief User Configuration Structure
     *
     *   Structure that is returned by the getHardwareUserConfig API for the
     *   User Configuration data
     */

    struct UserConfig_s
    {
        std::string UserName;                     //!< User Name
        AUTH_LEVEL UserAuthLevel;                 //!< User Authorization level
        bool UserActiveFlag;                      //!< User Actively logged in
    };
    typedef struct UserConfig_s UserConfig;

    /** @brief System User Configuration Structure
     *
     *   Structure that is returned by the getHardwareUserConfig API for the
     *   System User Configuration data
     */

    struct SystemUserConfig_s
    {
        std::vector<UserConfig> userconfig;       //!< User Configuration Structure
    };
    typedef struct SystemUserConfig_s SystemUserConfig;



    /** @brief Process Cpu User Structure
     *
     */

    struct ProcessCpuUser_s
    {
        std::string ProcessName;              	//!< Process Name
        uint16_t 	CpuUsage;               	//!< Process Cpu Usage %
    };	
    typedef struct ProcessCpuUser_s ProcessCpuUser;

    /** @brief TOP Process Cpu User Structure
     *
     *   Structure that is returned by the getTopProcessCpuUsers API
     */

    struct TopProcessCpuUsers_s
    {
        std::string ModuleName;                    		//!< Module Name
        uint16_t    numberTopUsers;                    	//!< Number of TOP Users
        std::vector<ProcessCpuUser> processcpuuser;   	//!< TOP Users
    };	
    typedef struct TopProcessCpuUsers_s TopProcessCpuUsers;

    /** @brief System TOP Process Cpu User Structure
     *
     *   Structure that is returned by the getTopProcessCpuUsers API
     */

    struct SystemTopProcessCpuUsers_s
    {
        std::vector<TopProcessCpuUsers> topprocesscpuusers; //!< TOP Process Cpu User Structure
    };
    typedef struct SystemTopProcessCpuUsers_s SystemTopProcessCpuUsers;

    /** @brief Module Cpu Structure
     *
     */

    struct ModuleCpu_s
    {
        std::string ModuleName;              	//!< Module Name
        uint16_t 	CpuUsage;               	//!< Module Cpu Usage %
    };	
    typedef struct ModuleCpu_s ModuleCpu;

    /** @brief System Module Cpu Structure
     *
     *   Structure that is returned by the getTopProcessCpuUsers API
     */

    struct SystemCpu_s
    {
        std::vector<ModuleCpu> modulecpu; //!< Module Cpu
    };
    typedef struct SystemCpu_s SystemCpu;


    /** @brief Process Memory User Structure
     *
     */

    struct ProcessMemoryUser_s
    {
        std::string ProcessName;              	//!< Process Name
        uint32_t 	MemoryUsed;               	//!< Process Memory Used
        uint16_t 	MemoryUsage;               	//!< Process Memory Usage %
    };	
    typedef struct ProcessMemoryUser_s ProcessMemoryUser;

    /** @brief TOP Process Memory User Structure
     *
     *   Structure that is returned by the getTopProcessMemoryUsers API
     */

    struct TopProcessMemoryUsers_s
    {
        std::string ModuleName;                    		//!< Module Name
        uint16_t    numberTopUsers;                    	//!< Number of TOP Users
        std::vector<ProcessMemoryUser> processmemoryuser; //!< TOP Users
    };	
    typedef struct TopProcessMemoryUsers_s TopProcessMemoryUsers;

    /** @brief System TOP Process Memory User Structure
     *
     *   Structure that is returned by the getTopProcessMemoryUsers API
     */

    struct SystemTopProcessMemoryUsers_s
    {
        std::vector<TopProcessMemoryUsers> topprocessmemoryusers; //!< TOP Process Memory User Structure
    };
    typedef struct SystemTopProcessMemoryUsers_s SystemTopProcessMemoryUsers;

    /** @brief Module Memory Structure
     *
     */

    struct ModuleMemory_s
    {
        std::string ModuleName;              	//!< Module Name
		uint32_t MemoryTotal;              		//!< Memory Total
		uint32_t MemoryUsed;              		//!< Memory Used
		uint32_t cache;              			//!< Cache Used
		uint16_t MemoryUsage;              		//!< Memory Usage Percent
		uint32_t SwapTotal;              		//!< Swap Total
		uint32_t SwapUsed;              		//!< Swap Used
		uint16_t SwapUsage;              		//!< Swap Usage Percent
    };	
    typedef struct ModuleMemory_s ModuleMemory;

    /** @brief System Module Cpu Structure
     *
     *   Structure that is returned by the getTopProcessCpuUsers API
     */

    struct SystemMemory_s
    {
        std::vector<ModuleMemory> modulememory; //!< Module Memory
    };
    typedef struct SystemMemory_s SystemMemory;

    /** @brief Disk Usage Structure
     *
     */

    struct DiskUsage_s
    {
        std::string DeviceName;              	//!< Device Name
		uint32_t TotalBlocks;					//!< Total Blocks
		uint32_t UsedBlocks;					//!< Used Blocks
        uint16_t DiskUsage;               		//!< Disk Usage %
    };	
    typedef struct DiskUsage_s DiskUsage;

    /** @brief Module Disk Usage Structure
     *
     *   Structure that is returned by the getTopProcessMemoryUsers API
     */

    struct ModuleDisk_s
    {
        std::string ModuleName;               	//!< Module Name
        std::vector<DiskUsage> diskusage;   	//!< Disk Usage
    };	
    typedef struct ModuleDisk_s ModuleDisk;

    /** @brief System Disk Usage Structure
     *
     *   Structure that is returned by the getTopProcessMemoryUsers API
     */

    struct SystemDisk_s
    {
        std::vector<ModuleDisk> moduledisk; 	//!< Module Disk Usage
    };
    typedef struct SystemDisk_s SystemDisk;

    /** @brief Active Sql Statement Structure
     *
     */

    struct ActiveSqlStatement
    {
        std::string sqlstatement;
        unsigned    starttime;
        uint64_t    sessionid;
    };	
    typedef std::vector<ActiveSqlStatement> ActiveSqlStatements;


    // username / password for smbclient use
	const std::string USERNAME = "oamuser";
	const std::string PASSWORD = "Calpont1"; 

    /** @brief System Storage Configuration Structure
     *
     *   Structure that is returned by the getStorageConfig API
	 *   Returns: Storage Type, System DBRoot count, PM dbroot info, 
     */

    typedef boost::tuple<std::string, uint16_t, DeviceDBRootList, std::string > systemStorageInfo_t;

	typedef std::vector<std::string> dbrootList;

    /** @brief OAM API I/F class
     *
     *  Operations, Administration, and Maintenance C++ APIs. These APIS are utilized
     *  to Configure the Hardware and the Software on the Calpont Database Appliance.
     *  They are also used to retrieve the configuration, hardware and process status,
     *  alarms, logs, and performance data.
     *
     */

    class Oam
    {
        public:
            /** @brief OAM C++ API Class constructor
             */
            EXPORT Oam();

            /** @brief OAM C++ API Class destructor
             */
            EXPORT virtual ~Oam();

            /** @brief get System Software information
             *
             * get System Software information from the System software RPM.
             * @param systemconfig Returned System Software Structure
             */
            EXPORT void getSystemSoftware(SystemSoftware& systemsoftware);

            /** @brief get System Configuration information
             *
             * get System Configuration information from the system config file.
             * @param systemconfig Returned System Configuration Structure
             */
            EXPORT void getSystemConfig(SystemConfig& systemconfig);

            /** @brief get System Module Configuration information
             *
             * get System Module Configuration information value from the system config file.
             * @param systemmoduletypeconfig Returned System Module Configuration Structure
             */
            EXPORT void getSystemConfig(SystemModuleTypeConfig& systemmoduletypeconfig);

            /** @brief get System Module Type Configuration information for a Module
             *
             * get System Module Type Configuration information for a Module from the system config file.
             * @param moduletype the Module Type to get information
             * @param moduletypeconfig Returned System Module Configuration Structure
             */
            EXPORT void getSystemConfig(const std::string&moduletype, ModuleTypeConfig& moduletypeconfig);

            /** @brief get System Module Name Configuration information for a Module
             *
             * get System Module Name Configuration information for a Module from the system config file.
             * @param moduleName the Module Name to get information
             * @param moduleconfig Returned System Module Configuration Structure
             */
            EXPORT void getSystemConfig(const std::string&moduleName, ModuleConfig& moduleconfig);

            /** @brief get System Module Configuration information for local Module
             *
             * get System Module Name Configuration information for local Module from the system config file.
             * @param Moduleconfig Returned System Configuration Structure
             */
            EXPORT void getSystemConfig(ModuleConfig& moduleconfig);

            /** @brief get System Module Type Configuration information for local Module Type
             *
             * get System Module Name Configuration information for local Module from the system config file.
             * @param moduletypeconfig Returned System Configuration Structure
             */
            EXPORT void getSystemConfig(ModuleTypeConfig& moduletypeconfig);

            /** @brief get System Ext Device Name Configuration information
             *
             * get System Ext Device Name Configuration information for a System Ext from the system config file.
             * @param name the Ext Device Name to get information
             * @param extdeviceConfig Returned System Ext Device Configuration Structure
             */
            EXPORT void getSystemConfig(const std::string&name, ExtDeviceConfig& extdeviceConfig);

            /** @brief get System Ext Device Configuration information
             *
             * get System Ext Device Name Configuration information for local System Ext from the system config file.
             * @param extdeviceConfig Returned System Configuration Structure
             */
            EXPORT void getSystemConfig(SystemExtDeviceConfig& systemextdeviceConfig);

            /** @brief set Ext Device Configuration information
             *
             * Set Ext Device Configuration information
             * @param deviceName the Device Name to get information
             * @param extdeviceConfig Ext Device Configuration Structure
             */
    		EXPORT void setSystemConfig(const std::string deviceName, ExtDeviceConfig extdeviceConfig);

           /** @brief get System Configuration String Parameter
             *
             * get System Configuration String Parameter from the system config file.
             * @param name the Parameter Name to get value
             * @param value Returned Parameter Value
             */
            EXPORT void getSystemConfig(const std::string&name, std::string& value);

            /** @brief get System Configuration Integer Parameter
             *
             * get System Configuration Integer Parameter from the system config file.
             * @param name the Parameter Name to get value
             * @param value Returned Parameter Value
             */
            EXPORT void getSystemConfig(const std::string&name, int& value);

            /** @brief get Module Name for IP Address
             *
             * get Module Name for given IP address from the system config file.
             * @param IpAddress the Patamater IP Address
             * @param moduleName Returned Parameter Value
             */
            EXPORT void getModuleNameByIPAddr(const std::string IpAddress, std::string& moduleName);

            /** @brief set System Configuration String Parameter
             *
             * set System Configuration String Parameter from the system config file.
             * @param name the Parameter Name to set value
             * @param value the Parameter Value to set
             */
            EXPORT void setSystemConfig(const std::string name, const std::string value);

            /** @brief set System Configuration Integer Parameter
             *
             * set System Configuration Integer Parameter from the system config file.
             * @param name the Parameter Name to set value
             * @param value the Parameter Value to set
             */
            EXPORT void setSystemConfig(const std::string name, const int value);

            /** @brief set System Module Type Configuration information for a Module
             *
             * set System Module Type Configuration information for a Module from the system config file.
             * @param moduletype the Module Type to get information
             * @param moduletypeconfig System Module Configuration Structure
             */
            EXPORT void setSystemConfig(const std::string moduletype, ModuleTypeConfig moduletypeconfig);


            /** @brief set System Module Name Configuration information for a Module
             *
             * Set System Module Name Configuration information for a Module from the system config file.
             * @param moduleName the Module Name to get information
             * @param moduleconfig System Module Configuration Structure
             */
    		EXPORT void setSystemConfig(const std::string module, ModuleConfig moduleconfig);


            /** @brief add Module
             *
             * Add module to the system config file.
             * @param DeviceNetworkConfig the Modules added
             * @param password Host Root Password
            */
            EXPORT void addModule(DeviceNetworkList devicenetworklist, const std::string password);

            /** @brief remove Module
             *
             * Remove module from the system config file.
             * @param DeviceNetworkConfig the Modules to be removed
             */
            EXPORT void removeModule(DeviceNetworkList devicenetworklist);

            /** @brief reconfigure Module
             *
             * Add module to the system config file.
             * @param DeviceNetworkConfig the Module Name to be reconfigured
             */
            EXPORT void reconfigureModule(DeviceNetworkList devicenetworklist);

            /** @brief get System Status information
             *
             * get System Status information from the system status file.
             * @param systemstatus Returned System Status Structure
             */
            EXPORT void getSystemStatus(SystemStatus& systemstatus);

            /** @brief set System Status information
             *
             * set System Status information in the system status file.
             * @param state System Operational State
             */
            EXPORT void setSystemStatus(const int state);

            /** @brief get Module Status information
             *
             * get Module Status information from the system status file.
             * @param name Module Name
             * @param state Returned Operational State
             */
            EXPORT void getModuleStatus(const std::string name, int& state, bool& degraded);

            /** @brief set Module Status information
             *
             * set Module Status information in the system status file.
             * @param name Module Name
             * @param state Module Operational State
             */
            EXPORT void setModuleStatus(const std::string name, const int state);

            /** @brief get Ext Device Status information
             *
             * get Ext Device Status information from the system status file.
             * @param name Ext Device Name
             * @param state Returned Operational State
             */
            EXPORT void getExtDeviceStatus(const std::string name, int& state);

            /** @brief set Ext Device Status information
             *
             * set Ext Device Status information in the system status file.
             * @param name Ext Device Name
             * @param state System Ext Operational State
             */
            EXPORT void setExtDeviceStatus(const std::string name, const int state);

            /** @brief get Dbroot Status information
             *
             * get DBroot Status information in the system status file.
             * @param name DBroot Name
             * @param state System  Operational State
             */
            EXPORT void getDbrootStatus(const std::string name, int& state);

            /** @brief set Dbroot Status information
             *
             * set DBroot Status information in the system status file.
             * @param name DBroot Name
             * @param state System  Operational State
             */
            EXPORT void setDbrootStatus(const std::string name, const int state);

            /** @brief get NIC Status information
             *
             * get NIC Status information.
             * @param name NIC HostName
             * @param state Returned Operational State
             */
            EXPORT void getNICStatus(const std::string name, int& state);

            /** @brief set NIC Status information
             *
             * set NIC Status information.
             * @param name NIC HostName
             * @param state NIC Operational State
             */
            EXPORT void setNICStatus(const std::string name, const int state);

            /** @brief get System Process Configuration information
             *
             * get System Configuration Process information from the Process config file.
             * @param systemprocessconfig Returned System Process Configuration Structure
             */
            EXPORT void getProcessConfig(SystemProcessConfig& systemprocessconfig);

            /** @brief get Process Configuration information
             *
             * get System Process information from the Process config file.
             * @param process the Process Name to get value
             * @param module the Module Name for the Process to get value
             * @param processconfig Returned Process Configuration Structure
             */
            EXPORT void getProcessConfig(const std::string process, const std::string module, ProcessConfig& processconfig);

            /** @brief get Process Configuration String Parameter
             *
             * get Process Configuration String Parameter from the Process config file.
             * @param process the Process Name to get value
             * @param module the Module Name for the Process to get value
             * @param name the Parameter Name to get value
             * @param value the Parameter Value to get
             */

            EXPORT void getProcessConfig(const std::string process, const std::string module, const std::string name, std::string& value);

            /** @brief get Process Configuration Integer Parameter
             *
             * get Process Configuration Integer Parameter from the Process config file.
             * @param process the Process Name to get value
             * @param module the Module Name for the Process to get value
             * @param name the Parameter Name to get value
             * @param value the Parameter Value to get
             */
            EXPORT void getProcessConfig(const std::string process, const std::string module, const std::string name, int& value);

            /** @brief set Process Configuration String Parameter
             *
             * set Process Configuration String Parameter from the Process config file.
             * @param process the Process Name to set value
             * @param module the Module Name for the Process to set value
             * @param name the Parameter Name to set value
             * @param value the Parameter Value to set
             */

            EXPORT void setProcessConfig(const std::string process, const std::string module, const std::string name, const std::string value);

            /** @brief set Process Configuration Integer Parameter
             *
             * set Process Configuration Integer Parameter from the Process config file.
             * @param process the Process Name to set value
             * @param module the Module Name for the Process to set value
             * @param name the Parameter Name to set value
             * @param value the Parameter Value to set
             */
            EXPORT void setProcessConfig(const std::string process, const std::string module, const std::string name, const int value);

            /** @brief get System Process Status information
             *
             * get System Process Status information from the Process status  file.
             * @param systemprocessconfig Returned System Process Status Structure
             */
            EXPORT void getProcessStatus(SystemProcessStatus& systemprocessstatus, std::string port = "ProcStatusControl");

            /** @brief get Process Status information
             *
             * get Process information from the Process Status file.
             * @param process the Process Name to get value
             * @param module the Module Name for the Process to get value
             * @param processconfig Returned Process Status Structure
             */
            EXPORT void getProcessStatus(const std::string process, const std::string module, ProcessStatus& processstatus);

            /** @brief set Process Status
             *
             * set Process Status
             * @param process the Process Name to set value
             * @param module the Module Name for the Process to set value
             * @param state the Operational state
             * @param PID the Process ID
             */

            EXPORT void setProcessStatus(const std::string process, const std::string module, const int state, pid_t PID);

            /** @brief Process Init Complete
             *
             * Process Init Complete
             * 
             */

            EXPORT void processInitComplete(std::string processName, int STATE = oam::ACTIVE);

            /** @brief Process Init Failure
             *
             * Process Init Failure
             * 
             */

            EXPORT void processInitFailure();

            /** @brief get Local Process Status Data
             *
             * get Local PID, Name, and Status from Process Status file
             * @return myProcessStatus_t structure, which contains the local process OAM
             *         Status Data
             */
            EXPORT myProcessStatus_t getMyProcessStatus(pid_t processID = 0);

            /** @brief get Local Module Configuration Data
             *
             * get Local Module Name, OAM Parent Flag, and Realtime Linux OS Flag from
             * local config file.
             * @return oamModuleInfo_t structure, which contains the local Module OAM
             *         Configuration Data
             */
            EXPORT oamModuleInfo_t getModuleInfo();

            /** @brief get Alarm Configuration information
             *
             * get Alarm Configuration information from the alarm config file.
             * @param alarmid the Alarm ID for the parameter value
             * @param alarmconfig Returned Alarm Configuration Structure
             */
            EXPORT void getAlarmConfig(const int alarmid, AlarmConfig& alarmconfig);

            /** @brief get Alarm Configuration String Parameter
             *
             * get Alarm Configuration String Parameter from the Alarm config file.
             * @param alarmid the Alarm ID to get Alarm Configuration information
             * @param name the Parameter Name for the parameter value
             * @param value returned Parameter Value
             */
            EXPORT void getAlarmConfig(const int alarmid, const std::string name, std::string& value);

            /** @brief get Alarm Configuration Integer Parameter
             *
             * get Alarm Configuration Integer Parameter from the Alarm config file.
             * @param alarmid the Alarm ID to get the parameter value
             * @param name the Parameter Name for the parameter value
             * @param value returned Parameter Value
             */
            EXPORT void getAlarmConfig(const int alarmid, const std::string name, int& value);

            /** @brief set Alarm Configuration String Parameter
             *
             * set Alarm Configuration String Parameter from the Alarm config file.
             * @param alarmid the Alarm ID to set the parameter value
             * @param name the Parameter Name to set
             * @param value the Parameter Value to set
             */
            EXPORT void setAlarmConfig(const int alarmid, const std::string name, const std::string value);

            /** @brief set Alarm Configuration Integer Parameter
             *
             * set Alarm Configuration Integer Parameter from the Alarm config file.
             * @param alarmid the Alarm ID to set the parameter value
             * @param name the Parameter Name to set
             * @param value the Parameter Value to set
             */
            EXPORT void setAlarmConfig(const int alarmid, const std::string name, const int value);

            /** @brief OAM Hardware Management Login
             *
             * Login into the system to utilizes the OAM APIs from a user application
             * @param username the Login User Name
             * @param password the Login Password
             */
            EXPORT void login(const std::string username, const std::string password);

            /** @brief OAM Hardware Management Self Logout
             *
             * Logout from OAM Hardware Management system
             */
            EXPORT void logout();

            /** @brief OAM Hardware Management Logout
             *
             * Logout another user from OAM Hardware Management system
             * @param username the Login User Name
             * @param password the Login Password
             */
            EXPORT void logout(const std::string username, const std::string password);

            /** @brief Add OAM Hardware Management User
             *
             * Add a new user to the OAM Hardware Management system
             * @param username the new User Name
             * @param password the new User Password
             * @param authlevel the Authorization Level for the new user
             */
            EXPORT void addHardwareUser(const std::string username, const std::string password, AUTH_LEVEL authlevel);

            /** @brief Change OAM Hardware Management User Password
             *
             * Change a current OAM Hardware Management User's password
             * @param username the User Name
             * @param oldpassword the old User Password
             * @param newpassword the new User Password
             */
            EXPORT void changeHardwareUserPassword(const std::string username, const std::string oldpassword, const std::string newpassword);

            /** @brief Delete OAM Hardware Management User
             *
             * Delete a current OAM Hardware Management User
             * @param username the User Name
             */
            EXPORT void deleteHardwareUser(const std::string username);

            /** @brief Get all OAM Hardware Management User Configuration
             *
             * Get OAM Hardware Management User Configuration for a single user
             * @param systemuserconfig Returned System User Configuration Structure
             */
            EXPORT void getHardwareUserConfig(SystemUserConfig& systemuserconfig);

            /** @brief Get OAM Hardware Management User Configuration
             *
             * Get all OAM Hardware Management User Configuration
             * @param username the User Name
             * @param userconfig Returned User Configuration Structure
             */
            EXPORT void getHardwareUserConfig(const std::string username, UserConfig& userconfig);

            /** @brief Stop Module
             *
             * Stop's a Module within the Calpont Database Appliance
             * @param name the Module Name to stop
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
//            EXPORT void stopModule(const std::string name, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Shutdown Module
             *
             * Shutdown's a Module within the Calpont Database Appliance
             * @param name the Module Name to stop
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
//            EXPORT void shutdownModule(const std::string name, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Start Module
             *
             * Start's a stopped Module within the Calpont Database Appliance
             * @param name the Module Name to stop
             * @param ackflag Acknowledgment flag
             */
//            EXPORT void startModule(const std::string name, ACK_FLAG ackflag);

            /** @brief Restart Module
             *
             * Restart's a Module within the Calpont Database Appliance
             * @param name the Module Name to restart
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
//            EXPORT void restartModule(const std::string name, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Disable Module
             *
             * Disable a Module within the Calpont Database Appliance
             * @param name the Module Name to disable
             */
//            EXPORT void disableModule(const std::string name);

            /** @brief Enable Module
             *
             * Enable a Module within the Calpont Database Appliance
             * @param name the Module Name to enable
             */
//            EXPORT void enableModule(const std::string name);

            /** @brief Stop Module
             *
             * Stop's a Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be stopped
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void stopModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Shutdown Module
             *
             * Shutdown's a Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be shutdown
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void shutdownModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Start Module
             *
             * Start's a stopped Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be started
             * @param ackflag Acknowledgment flag
             */
            EXPORT void startModule(DeviceNetworkList devicenetworklist, ACK_FLAG ackflag);

            /** @brief Restart Module
             *
             * Restart's a Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be restarted
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void restartModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Disable Module
             *
             * Disable a Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be disabled
             */
            EXPORT void disableModule(DeviceNetworkList devicenetworklist);

            /** @brief Enable Module
             *
             * Enable a Module within the Calpont Database Appliance
             * @param DeviceNetworkConfig the Modules to be enabled
             */
            EXPORT void enableModule(DeviceNetworkList devicenetworklist);

            /** @brief Stop System
             *
             * Stop's the Calpont Database Appliance System
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void stopSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Shutdown System
             *
             * Shutdown's the Calpont Database Appliance System
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void shutdownSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Suspend Database Writes
             *
             * Suspends writing to the database. This should be done before backup 
             * activities occur. 
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void SuspendWrites(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Start System
             *
             * Start's the stopped Calpont Database Appliance System
             * @param ackflag Acknowledgment flag
             */
            EXPORT void startSystem(ACK_FLAG ackflag);

            /** @brief Restart System
             *
             * Restart's the active/stopped Calpont Database Appliance System
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT int restartSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Display a list of locked tables
             */
            void DisplayLockedTables(std::vector<BRM::TableLockInfo>& tableLocks, BRM::DBRM* pDBRM = NULL);

            /** @brief Get Active Alarms
             *
             * Get's the Active Alarm list for the Calpont Database Appliance
             * @param activealarm Returned Active Alarm list Structure
             */

            EXPORT void getActiveAlarms(snmpmanager::AlarmList& activealarm);

            /** @brief Get Historical Alarms
             *
             * Get's the Alarm list for the Calpont Database Appliance
			 * @param date date of alarms, "today" or date in YYYYMMDD
             * @param activealarm Returned Alarm list Structure
             */

            EXPORT void getAlarms(std::string date, snmpmanager::AlarmList& alarm);

            /** @brief check Active Alarm
             *
             * Check if alarm is in Active Alarm file
             * @param alarmid the Alarm ID
             * @param moduleName the Module Name
             * @param deviceName the Alarm device Name
             */
            EXPORT bool checkActiveAlarm(const int alarmid, const std::string moduleName, const std::string deviceName);

            /** @brief update Log
             *
             * Enable/Disable Logging with the system or on a specific Module at
             * a specific level
             * @param action enabled/disable
             * @param deviceid the device which logging is being enabled/disable
             * @param loglevel the level of logging being enabled/disable
             */
            EXPORT void updateLog(const std::string action, const std::string deviceid, const std::string loglevel);

            /** @brief get Log data file location for today
             *
             * Get Log file location for specific Module at a specific level
             * @param moduleName the Module where the log file is located
             * @param loglevel the level of logging
             * @param filelocation Returned: the location path of the log file
             */
            EXPORT void getLogFile(const std::string moduleName, const std::string loglevel, std::string& filelocation);

            /** @brief get Log data file location
             *
             * Get Log file location for specific Module at a specific level
             * @param moduleName the Module where the log file is located
             * @param loglevel the level of logging
             * @param date date of log file, either "today" or 
             * @param filelocation Returned: the location path of the log file
             */
            EXPORT void getLogFile(const std::string moduleName, const std::string loglevel, const std::string date, std::string& filelocation);

            /** @brief get Log configuration data
             *
             * Get Log Config data, which is the File IDs in the Module syslog.conf file
             * @param moduleName the Module where the log file is located
             * @param fileIDs Returned: list of Log File IDs
             */
            EXPORT void getLogConfig(SystemLogConfigData& configData );

            /** @brief get current time in string format
             *
             * get current time in string format
             */
            EXPORT std::string getCurrentTime();

            /** @brief get free diskspace in bytes
             *
             */
            EXPORT double getFreeSpace(std::string path);

            // Integer to ASCII convertor

            EXPORT std::string itoa(const int);

            /** @brief Stop Process
             *
             * Stop's a process on a Module within the Calpont Database Appliance
             * @param moduleName the Module Name
             * @param processName the Process Name to stopped
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void stopProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Start Process
             *
             * Start's a process on a Module within the Calpont Database Appliance
             * @param moduleName the Module Name
             * @param processName the Process Name to started
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void startProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

            /** @brief Restart Process
             *
             * Restart's a process on a Module within the Calpont Database Appliance
             * @param moduleName the Module Name
             * @param processName the Process Name to restarted
             * @param gracefulflag Graceful/Forceful flag
             * @param ackflag Acknowledgment flag
             */
            EXPORT void restartProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag);

              /** @brief Stop Process Type
             *
             * Stop's processes within the Calpont Database Appliance
             */
            EXPORT void stopProcessType(std::string type);

            /** @brief Start Process Type
             *
             * Start's processes within the Calpont Database Appliance
             */
            EXPORT void startProcessType(std::string type);

            /** @brief Restart Process Type
             *
             * Restart's process within the Calpont Database Appliance
             */
            EXPORT void restartProcessType(std::string type);

            /** @brief Reinit Process Type
             *
             * Reinit's process within the Calpont Database Appliance
             */
            EXPORT void reinitProcessType(std::string type);

            /** @brief Get Local DBRM ID for Module
			 *
             * @param moduleName the Module Name
             */
            EXPORT int getLocalDBRMID(const std::string moduleName);

            /** @brief build empty set of System Tables
             */
			EXPORT void buildSystemTables();

            /** @brief local exception control function
             * @param function Function throwing the exception
             * @param returnStatus 
             * @param msg A message to be included 
             */
            EXPORT void exceptionControl(std::string function, int returnStatus, const char* extraMsg = NULL);

            /** @brief get IP Address from Hostname
             */
			EXPORT std::string getIPAddress(std::string hostName);

            /** @brief get System TOP Process CPU Users 
             *
             * get System TOP Process CPU Users 
             * @param topNumber Number of TOP processes to retrieve
             * @param systemtopprocesscpuusers Returned System Top Process CPU Users Structure
             */
            EXPORT void getTopProcessCpuUsers(int topNumber, SystemTopProcessCpuUsers& systemtopprocesscpuusers);

            /** @brief get Module TOP Process CPU Users 
             *
             * get Module TOP Process CPU Users 
             * @param topNumber Number of TOP processes to retrieve
             * @param topprocesscpuusers Returned Top Process CPU Users Structure
             */
            EXPORT void getTopProcessCpuUsers(const std::string module, int topNumber, TopProcessCpuUsers& topprocesscpuusers);

            /** @brief get System CPU Usage
             *
             * get System CPU Usage
             * @param systemcpu Returned System CPU Usage Structure
             */
            EXPORT void getSystemCpuUsage(SystemCpu& systemcpu);

            /** @brief get Module CPU Usage
             *
             * get Module CPU Usage
             * @param module Module Name
             * @param modulecpu Returned Top Process CPU Users Structure
             */
            EXPORT void getModuleCpuUsage(const std::string module, ModuleCpu& modulecpu);

            /** @brief get System TOP Process Memory Users 
             *
             * get System TOP Process Memory Users 
             * @param topNumber Number of Memory processes to retrieve
             * @param systemtopprocessmemoryusers Returned System Top Process Memory Users Structure
             */
            EXPORT void getTopProcessMemoryUsers(int topNumber, SystemTopProcessMemoryUsers& systemtopprocessmemoryusers);

            /** @brief get Module TOP Process Memory Users 
             *
             * get Module TOP Process Memory Users 
             * @param module Module Name
             * @param topNumber Number of TOP processes to retrieve
             * @param topprocessmemoryusers Returned Top Process Memory Users Structure
             */
            EXPORT void getTopProcessMemoryUsers(const std::string module, int topNumber, TopProcessMemoryUsers& topprocessmemoryusers);

            /** @brief get System Memory Usage
             *
             * get System Memory Usage
             * @param systemmemory Returned System memory Usage Structure
             */
            EXPORT void getSystemMemoryUsage(SystemMemory& systemmemory);

            /** @brief get Module Memory Usage
             *
             * get Module Memory Usage
             * @param module Module Name
             * @param modulememory Returned Module Memory Usage Structure
             */
            EXPORT void getModuleMemoryUsage(const std::string module, ModuleMemory& modulememory);

            /** @brief get System Disk Usage
             *
             * get System Disk Usage
             * @param systemdisk Returned System Disk Usage Structure
             */
            EXPORT void getSystemDiskUsage(SystemDisk& systemdisk);

            /** @brief get Module Disk Usage
             *
             * get Module Disk Usage
             * @param module Module Name
             * @param moduledisk Returned Module Disk Usage Structure
             */
            EXPORT void getModuleDiskUsage(const std::string module, ModuleDisk& moduledisk);

            /** @brief get Active SQL Statements
             *
             * get Active SQL Statements
             * @param activesqlstatements Returned Active Sql Statement Structure
             */
            EXPORT void getActiveSQLStatements(ActiveSqlStatements& activesqlstatements);

            /** @brief Valid IP Address
             *
             * Validate IP Address format
             */
			EXPORT bool isValidIP(const std::string ipAddress);

			/**
			*@brief Check for a phrase in a log file and return status
			*/
			EXPORT bool checkLogStatus(std::string filename, std::string phase);

			/**
			*@brief Get PM with read-write mount
			*/
			EXPORT std::string getWritablePM();

			/**
			*@brief Get PM with read-write mount
			*/
			EXPORT std::string getHotStandbyPM();

			/**
			*@brief Get PM with read-write mount
			*/
			EXPORT void setHotStandbyPM(std::string moduleName);

			/**
			*@brief Distribute Calpont Configure File
			*/
			EXPORT void distributeConfigFile(std::string name = "system", std::string file = "Calpont.xml");

			/**
			*@brief Switch Parent OAM Module 
			*  Return true if we need to wait for systme restart 
			*/
			EXPORT bool switchParentOAMModule(std::string moduleName, GRACEFUL_FLAG gracefulflag);

			/**
			*@brief Get Storage Config Data
			*/
			EXPORT systemStorageInfo_t getStorageConfig();

			/**
			*@brief Get PM - DBRoot Config data
			*/
			EXPORT void getPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist);

			/**
			*@brief Get DBRoot - PM Config data
			*/
			EXPORT void getDbrootPmConfig(const int dbrootid, int& pmid);

			EXPORT void getDbrootPmConfig(const int dbrootid, std::string& pmid);

			/**
			*@brief Get System DBRoot Config data
			*/
			EXPORT void getSystemDbrootConfig(DBRootConfigList& dbrootconfiglist);

			/**
			*@brief Set PM - DBRoot Config data
			*/
			EXPORT void setPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist);

			/**
			*@brief Manual Move PM - DBRoot data
			*/
			EXPORT void manualMovePmDbroot(std::string residePM, std::string dbrootIDs, std::string toPM);

			/**
			*@brief Auto Move PM - DBRoot data
			*/
			EXPORT bool autoMovePmDbroot(std::string residePM);

			/**
			*@brief Auto Un-Move PM - DBRoot data
			*/
			EXPORT bool autoUnMovePmDbroot(std::string toPM);

			/**
			*@brief add DBRoot
			*/
			EXPORT void addDbroot(const int dbrootNumber, DBRootConfigList& dbrootlist);

			/**
			*@brief distribute Fstab Updates
			*/
			EXPORT void distributeFstabUpdates(std::string entry, std::string toPM = "system" );

			/**
			*@brief assign DBRoot
			*/
			EXPORT void assignDbroot(std::string toPM, DBRootConfigList& dbrootlist);

			/**
			*@brief unassign DBRoot
			*/
			EXPORT void unassignDbroot(std::string residePM, DBRootConfigList& dbrootlist);

			/**
			*@brief get unassigned DBRoot list
			*/
			EXPORT void getUnassignedDbroot(DBRootConfigList& dbrootlist);

			/**
			*@brief remove DBRoot
			*/
			EXPORT void removeDbroot(DBRootConfigList& dbrootlist);

			/**
			*@brief get AWS Device Name for DBRoot ID
			*/
			EXPORT std::string getAWSdeviceName( const int dbrootid);

			/**
			*@brief set System DBRoot Count
			*/
			EXPORT void setSystemDBrootCount();

			/**
			*@brief set FilesPerColumnPartition based on value of old
			* FilePerColumnPartition and old DbRootCount that is given
			*/
			EXPORT void setFilesPerColumnPartition( int oldDbRootCount );

            /** @brief send Device Notification Msg
             */
            EXPORT int sendDeviceNotification(std::string deviceName, NOTIFICATION_TYPE type, std::string payload = "");

            /** @brief run DBHealth Check
             */
 			EXPORT void checkDBFunctional(bool action = true);

            /** @brief mysql-Calpont service command
             */
 			EXPORT void actionMysqlCalpont(MYSQLCALPONT_ACTION action);

            /** @brief validate Module name
             */
            EXPORT int validateModule(const std::string name);

            /** @brief getEC2LocalInstance
             */
            EXPORT std::string getEC2LocalInstance(std::string name = "dummy");

            /** @brief getEC2LocalInstanceType
             */
            EXPORT std::string getEC2LocalInstanceType(std::string name = "dummy");

            /** @brief launchEC2Instance
             */
            EXPORT std::string launchEC2Instance(const std::string name = "dummy", const std::string type = oam::UnassignedName, const std::string group = oam::UnassignedName);

            /** @brief getEC2InstanceIpAddress
             */
            EXPORT std::string getEC2InstanceIpAddress(std::string instanceName);

            /** @brief terminateEC2Instance
             */
            EXPORT void terminateEC2Instance(std::string instanceName);

            /** @brief stopEC2Instance
             */
            EXPORT void stopEC2Instance(std::string instanceName);

            /** @brief startEC2Instance
             */
            EXPORT bool startEC2Instance(std::string instanceName);

            /** @brief assignElasticIP
             */
            EXPORT bool assignElasticIP(std::string instanceName, std::string IpAddress);

            /** @brief deassignElasticIP
             */
            EXPORT bool deassignElasticIP(std::string IpAddress);

            /** @brief createEC2Volume
             */
            EXPORT std::string createEC2Volume(std::string size, std::string name = "dummy");

            /** @brief getEC2VolumeStatus
             */
            EXPORT std::string getEC2VolumeStatus(std::string volumeName);

            /** @brief attachEC2Volume
             */
            EXPORT bool attachEC2Volume(std::string volumeName, std::string deviceName,std::string instanceName);

            /** @brief detachEC2Volume
             */
            EXPORT bool detachEC2Volume(std::string volumeName);

            /** @brief deleteEC2Volume
             */
            EXPORT bool deleteEC2Volume(std::string volumeName);

            /** @brief createEC2tag
             */
            EXPORT bool createEC2tag(std::string resourceName, std::string tagName, std::string tagValue);

			/**
			*@brief  take action on Syslog process
			*/
			EXPORT void syslogAction( std::string action);

			/**
			*@brief  call dbrm control
			*/
			EXPORT void dbrmctl(std::string command);

            /** @brief Wait for system to close transactions
             *  
             *  When a Shutdown, stop, restart or suspend operation is
             *  requested but there are active transactions of some sort,
             *  We wait for all transactions to close before performing
             *  the action.
             */
            EXPORT bool waitForSystem(PROC_MGT_MSG_REQUEST request, messageqcpp::IOSocket& ios, messageqcpp::ByteStream& stillWorkingMsg);

    		void amazonReattach(std::string toPM, dbrootList dbrootConfigList, bool attach = false);
            void mountDBRoot(dbrootList dbrootConfigList, bool mount = true);

			/**
			*@brief  gluster control
			*/
			EXPORT int glusterctl(GLUSTER_COMMANDS command, std::string argument1, std::string& argument2, std::string& errmsg);


        private:

            /** @brief check Gluster Log after a Gluster control call
             */
            int checkGlusterLog(std::string logFile, std::string& errmsg);

		    int sendMsgToProcMgr3(messageqcpp::ByteStream::byte requestType, snmpmanager::AlarmList& alarmlist, const std::string date);

            /** @brief build and send request message to Process Manager
             */
            int sendMsgToProcMgr(messageqcpp::ByteStream::byte requestType, const std::string name = "",
                GRACEFUL_FLAG gracefulflag = FORCEFUL, ACK_FLAG ackflag = ACK_NO,
                const std::string argument1 = "", const std::string argument2 = "", int timeout = 600);

            /** @brief build and send request message to Process Manager 2
             */
    		int sendMsgToProcMgr2(messageqcpp::ByteStream::byte requestType, DeviceNetworkList devicenetworklist,
        		GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag, const std::string password = " ");

            /** @brief build and send request message to Process Manager
             *  Check for status messages
             */
            int sendMsgToProcMgrWithStatus(messageqcpp::ByteStream::byte requestType, const std::string name = "",
                GRACEFUL_FLAG gracefulflag = GRACEFUL, ACK_FLAG ackflag = ACK_YES,
                const std::string argument1 = "", const std::string argument2 = "", int timeout = 600);

            // check for Ack message from Process Manager
            //	int checkMsgFromProcMgr(messageqcpp::ByteStream::byte requestType, const std::string name);

            /** @brief validate Process name
             */
    		int validateProcess(const std::string moduleName, std::string processName);

            /** @brief send status updates to process monitor
             */
    		void sendStatusUpdate(messageqcpp::ByteStream obs, messageqcpp::ByteStream::byte returnRequestType);

			/**
			* @brief Write the message to the log
			*/
			void writeLog(const std::string logContent, const logging::LOG_TYPE logType = logging::LOG_TYPE_INFO);

			std::string CalpontConfigFile;
			std::string AlarmConfigFile;
			std::string ProcessConfigFile;
			std::string InstallDir;

    };	// end of class

}	// end of namespace

namespace procheartbeat
{

    class ProcHeartbeat
    {
        public:
            /** @brief ProcHeartbeat Class constructor
             */
            ProcHeartbeat();

            /** @brief ProcHeartbeat Class destructor
             */
            virtual ~ProcHeartbeat();

            /** @brief Register for Proc Heartbeat
             *
             */
            void registerHeartbeat(int ID = 1);

            /** @brief De-Register for Proc Heartbeat
             *
             * DeregisterHeartbeat
             */
            void deregisterHeartbeat(int ID = 1);

            /** @brief Send Proc Heartbeat
             *
             */
            void sendHeartbeat(int ID = 1, oam::ACK_FLAG ackFlag = oam::ACK_NO);
	};
}

#undef EXPORT

#endif
// vim:ts=4 sw=4:

