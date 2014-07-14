/* Copyright (C) 2013 Calpont Corp.

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
#include <unistd.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/progress.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#if defined(__linux__)
#include <sys/statfs.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#elif defined (_MSC_VER)
#elif defined (__FreeBSD__)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/mount.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <stdexcept>
#include <csignal>
#include <sstream>

#include "ddlpkg.h"
#include "dmlpkg.h"
#define LIBOAM_DLLEXPORT
#include "liboamcpp.h"
#undef LIBOAM_DLLEXPORT

#ifdef _MSC_VER
#include "idbregistry.h"
#endif
#include "installdir.h"
#include "dbrm.h"
#include "sessionmanager.h"
#include "IDBPolicy.h"
#include "IDBDataFile.h"

#if defined(__GNUC__)
#include <string>
static const std::string optim("Build is "
#if !defined(__OPTIMIZE__)
"NOT "
#endif
"optimized");
#endif

namespace fs = boost::filesystem;

using namespace snmpmanager;
using namespace config;
using namespace std;
using namespace messageqcpp;
using namespace oam;
using namespace logging;
using namespace BRM;

namespace oam
{
	// flag to tell us ctrl-c was hit
	uint32_t    ctrlc = 0;

	// flag for using HDFS
	//   -1: non-hdfs
	//    0: unknown
	//    1: hdfs
	int Oam::UseHdfs = 0;

	//------------------------------------------------------------------------------
	// Signal handler to catch Control-C signal to terminate the process
	// while waiting for a shutdown or suspend action
	//------------------------------------------------------------------------------
	void handleControlC(int i)
	{
		std::cout << "Received Control-C to terminate the command..." << std::endl;
		ctrlc = 1;
	}

    Oam::Oam()
    {
		// Assigned pointers to Config files
		string calpontfiledir;
		const char* cf=0;

		InstallDir = startup::StartUp::installDir();
		calpontfiledir = InstallDir + "/etc";

		//FIXME: we should not use this anymore. Everything should be based off the install dir
		//If CALPONT_HOME is set, use it for etc directory
#ifdef _MSC_VER
		cf = 0;
		string cfStr = IDBreadRegistry("CalpontHome");
		if (!cfStr.empty())
			cf = cfStr.c_str();
#else        
		cf = getenv("CALPONT_HOME");
#endif
		if (cf != 0 && *cf != 0)
			calpontfiledir = cf;

		CalpontConfigFile = calpontfiledir + "/Calpont.xml";
	
		AlarmConfigFile = calpontfiledir + "/AlarmConfig.xml";
	
		ProcessConfigFile = calpontfiledir + "/ProcessConfig.xml";
		if (UseHdfs == 0)
		{
			try {
				Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
				string tmp = sysConfig->getConfig("Installation", "DBRootStorageType");
				if (boost::iequals(tmp, "hdfs"))
					UseHdfs = 1;
				else
					UseHdfs = -1;
			}
			catch(...) {} // defaulted to false
		}
	}

    Oam::~Oam()
    {}

    /********************************************************************
     *
     * get System Software information
     *
     ********************************************************************/

    void Oam::getSystemSoftware(SystemSoftware& systemsoftware)
    {
        // parse releasenum file

        string rn = InstallDir + "/releasenum";
		ifstream File(rn.c_str());
        if (!File)
            // Open File error
            return;

        char line[400];
        string buf;
        while (File.getline(line, 400))
        {
            buf = line;
            for ( unsigned int i = 0;; i++)
            {
                if ( SoftwareData[i] == "")
                    //end of list
                    break;

                string data = "";
                string::size_type pos = buf.find(SoftwareData[i],0);
                if (pos != string::npos)
                {
                    string::size_type pos1 = buf.find("=",pos);
                    if (pos1 != string::npos)
                    {
                     	data = buf.substr(pos1+1, 80);
					}
                    else
                        // parse error
                        exceptionControl("getSystemSoftware", API_FAILURE);

					//strip off any leading or trailing spaces
					for(;;)
					{
						string::size_type pos = data.find(' ',0);
						if (pos == string::npos)
							// no more found
							break;
						// strip leading
						if (pos == 0) {
							data = data.substr(pos+1,10000);
						}
						else 
						{ // strip trailing
							data = data.substr(0, pos);
						}
					}	

                    switch(i)
                    {
                        case(0):                  // line up with SoftwareData[]
                            systemsoftware.Version = data;
                            break;
                        case(1):
                            systemsoftware.Release = data;
                            break;
                    }
                }
            }                                     //end of for loop
        }                                         //end of while

        File.close();
    }
    /********************************************************************
     *
     * get System Configuration Information
     *
     ********************************************************************/

    void Oam::getSystemConfig(SystemConfig& systemconfig)
    {
	
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";

        // get numberic variables
        systemconfig.DBRootCount = strtol(sysConfig->getConfig(Section, "DBRootCount").c_str(), 0, 0);
        systemconfig.ModuleHeartbeatPeriod = strtol(sysConfig->getConfig(Section, "ModuleHeartbeatPeriod").c_str(), 0, 0);
        systemconfig.ModuleHeartbeatCount = strtol(sysConfig->getConfig(Section, "ModuleHeartbeatCount").c_str(), 0, 0);
//        systemconfig.ProcessHeartbeatPeriod = strtol(sysConfig->getConfig(Section, "ProcessHeartbeatPeriod").c_str(), 0, 0);
        systemconfig.ExternalCriticalThreshold = strtol(sysConfig->getConfig(Section, "ExternalCriticalThreshold").c_str(), 0, 0);
        systemconfig.ExternalMajorThreshold = strtol(sysConfig->getConfig(Section, "ExternalMajorThreshold").c_str(), 0, 0);
        systemconfig.ExternalMinorThreshold = strtol(sysConfig->getConfig(Section, "ExternalMinorThreshold").c_str(), 0, 0);
        systemconfig.TransactionArchivePeriod = strtol(sysConfig->getConfig(Section, "TransactionArchivePeriod").c_str(), 0, 0);

        // get string variables
		for ( unsigned int dbrootID = 1 ; dbrootID < systemconfig.DBRootCount + 1 ; dbrootID++)
		{
			systemconfig.DBRoot.push_back(sysConfig->getConfig(Section, "DBRoot" + itoa(dbrootID)));
		}

        systemconfig.SystemName = sysConfig->getConfig(Section, "SystemName");
        systemconfig.DBRMRoot = sysConfig->getConfig(Section, "DBRMRoot");
        systemconfig.ParentOAMModule  = sysConfig->getConfig(Section, "ParentOAMModuleName");
        systemconfig.StandbyOAMModule  = sysConfig->getConfig(Section, "StandbyOAMModuleName");

        // added by Zhixuan
        try
        {
            SNMPManager sm;
            sm.getNMSAddr (systemconfig.NMSIPAddr);
        }
        catch(...)
        {
            systemconfig.NMSIPAddr = UnassignedIpAddr;
        }
        // end

        Section = "SessionManager";

        systemconfig.MaxConcurrentTransactions = strtol(sysConfig->getConfig(Section, "MaxConcurrentTransactions").c_str(), 0, 0);
        systemconfig.SharedMemoryTmpFile = sysConfig->getConfig(Section, "SharedMemoryTmpFile");

        Section = "VersionBuffer";

        systemconfig.NumVersionBufferFiles = strtol(sysConfig->getConfig(Section, "NumVersionBufferFiles").c_str(), 0, 0);
        systemconfig.VersionBufferFileSize = strtol(sysConfig->getConfig(Section, "VersionBufferFileSize").c_str(), 0, 0);

        Section = "OIDManager";

        systemconfig.OIDBitmapFile = sysConfig->getConfig(Section, "OIDBitmapFile");
        systemconfig.FirstOID = strtol(sysConfig->getConfig(Section, "FirstOID").c_str(), 0, 0);
    }

    /********************************************************************
     *
     * get System Module Type Configuration Information
     *
     ********************************************************************/

    void Oam::getSystemConfig(SystemModuleTypeConfig& systemmoduletypeconfig)
    {
        const string Section = "SystemModuleConfig";
        const string MODULE_TYPE = "ModuleType";
        systemmoduletypeconfig.moduletypeconfig.clear();

        for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE+1; moduleTypeID++)
        {
        	ModuleTypeConfig moduletypeconfig;
	
            Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

            // get Module info

            string moduleType = MODULE_TYPE + itoa(moduleTypeID);

            Oam::getSystemConfig(sysConfig->getConfig(Section, moduleType), moduletypeconfig );

            if (moduletypeconfig.ModuleType.empty())
                continue;

            systemmoduletypeconfig.moduletypeconfig.push_back(moduletypeconfig);
        }
    }

    /********************************************************************
     *
     * get System Module Configuration Information by Module Type
     *
     ********************************************************************/

    void Oam::getSystemConfig(const std::string&moduletype, ModuleTypeConfig& moduletypeconfig)
    {
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemModuleConfig";
        const string MODULE_TYPE = "ModuleType";
        const string MODULE_DESC = "ModuleDesc";
        const string MODULE_RUN_TYPE = "RunType";
        const string MODULE_COUNT = "ModuleCount";
        const string MODULE_DISABLE_STATE = "ModuleDisableState";
        const string MODULE_CPU_CRITICAL = "ModuleCPUCriticalThreshold";
        const string MODULE_CPU_MAJOR = "ModuleCPUMajorThreshold";
        const string MODULE_CPU_MINOR = "ModuleCPUMinorThreshold";
        const string MODULE_CPU_MINOR_CLEAR = "ModuleCPUMinorClearThreshold";
        const string MODULE_DISK_CRITICAL = "ModuleDiskCriticalThreshold";
        const string MODULE_DISK_MAJOR = "ModuleDiskMajorThreshold";
        const string MODULE_DISK_MINOR = "ModuleDiskMinorThreshold";
        const string MODULE_MEM_CRITICAL = "ModuleMemCriticalThreshold";
        const string MODULE_MEM_MAJOR = "ModuleMemMajorThreshold";
        const string MODULE_MEM_MINOR = "ModuleMemMinorThreshold";
        const string MODULE_SWAP_CRITICAL = "ModuleSwapCriticalThreshold";
        const string MODULE_SWAP_MAJOR = "ModuleSwapMajorThreshold";
        const string MODULE_SWAP_MINOR = "ModuleSwapMinorThreshold";
        const string MODULE_IP_ADDR = "ModuleIPAddr";
        const string MODULE_SERVER_NAME = "ModuleHostName";
        const string MODULE_DISK_MONITOR_FS = "ModuleDiskMonitorFileSystem";
        const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";
        const string MODULE_DBROOT_ID = "ModuleDBRootID";

        for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE+1; moduleTypeID++)
        {
            string moduleType = MODULE_TYPE + itoa(moduleTypeID);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype)
            {
                string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);
                string ModuleType = MODULE_TYPE + itoa(moduleTypeID);
                string ModuleDesc = MODULE_DESC + itoa(moduleTypeID);
                string ModuleRunType = MODULE_RUN_TYPE + itoa(moduleTypeID);
                string ModuleCPUCriticalThreshold = MODULE_CPU_CRITICAL + itoa(moduleTypeID);
                string ModuleCPUMajorThreshold = MODULE_CPU_MAJOR + itoa(moduleTypeID);
                string ModuleCPUMinorThreshold = MODULE_CPU_MINOR + itoa(moduleTypeID);
                string ModuleCPUMinorClearThreshold = MODULE_CPU_MINOR_CLEAR + itoa(moduleTypeID);
                string ModuleDiskCriticalThreshold = MODULE_DISK_CRITICAL + itoa(moduleTypeID);
                string ModuleDiskMajorThreshold = MODULE_DISK_MAJOR + itoa(moduleTypeID);
                string ModuleDiskMinorThreshold = MODULE_DISK_MINOR + itoa(moduleTypeID);
                string ModuleMemCriticalThreshold = MODULE_MEM_CRITICAL + itoa(moduleTypeID);
                string ModuleMemMajorThreshold = MODULE_MEM_MAJOR + itoa(moduleTypeID);
                string ModuleMemMinorThreshold = MODULE_MEM_MINOR + itoa(moduleTypeID);
                string ModuleSwapCriticalThreshold = MODULE_SWAP_CRITICAL + itoa(moduleTypeID);
                string ModuleSwapMajorThreshold = MODULE_SWAP_MAJOR + itoa(moduleTypeID);
                string ModuleSwapMinorThreshold = MODULE_SWAP_MINOR + itoa(moduleTypeID);

                moduletypeconfig.ModuleCount = strtol(sysConfig->getConfig(Section, ModuleCount).c_str(), 0, 0);
                moduletypeconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
                moduletypeconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc);
                moduletypeconfig.RunType = sysConfig->getConfig(Section, ModuleRunType);
                moduletypeconfig.ModuleCPUCriticalThreshold = strtol(sysConfig->getConfig(Section, ModuleCPUCriticalThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleCPUMajorThreshold = strtol(sysConfig->getConfig(Section, ModuleCPUMajorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleCPUMinorThreshold = strtol(sysConfig->getConfig(Section, ModuleCPUMinorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleCPUMinorClearThreshold = strtol(sysConfig->getConfig(Section, ModuleCPUMinorClearThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleDiskCriticalThreshold = strtol(sysConfig->getConfig(Section, ModuleDiskCriticalThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleDiskMajorThreshold = strtol(sysConfig->getConfig(Section, ModuleDiskMajorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleDiskMinorThreshold = strtol(sysConfig->getConfig(Section, ModuleDiskMinorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleMemCriticalThreshold = strtol(sysConfig->getConfig(Section, ModuleMemCriticalThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleMemMajorThreshold = strtol(sysConfig->getConfig(Section, ModuleMemMajorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleMemMinorThreshold = strtol(sysConfig->getConfig(Section, ModuleMemMinorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleSwapCriticalThreshold = strtol(sysConfig->getConfig(Section, ModuleSwapCriticalThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleSwapMajorThreshold = strtol(sysConfig->getConfig(Section, ModuleSwapMajorThreshold).c_str(), 0, 0);
                moduletypeconfig.ModuleSwapMinorThreshold = strtol(sysConfig->getConfig(Section, ModuleSwapMinorThreshold).c_str(), 0, 0);

				int moduleFound = 0;
				//get NIC IP address/hostnames
				for (int moduleID = 1; moduleID < MAX_MODULE ; moduleID++)
				{
					DeviceNetworkConfig devicenetworkconfig;
					HostConfig hostconfig;

					for (int nicID= 1; nicID < MAX_NIC+1 ; nicID++)
					{
						string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
	
						string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
						if (ipAddr.empty() || ipAddr == UnassignedIpAddr )
							continue;
	
						string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
						string serverName = sysConfig->getConfig(Section, ModuleHostName);
	
						hostconfig.IPAddr = ipAddr;
						hostconfig.HostName = serverName;
						hostconfig.NicID = nicID;

						devicenetworkconfig.hostConfigList.push_back(hostconfig);
					}

					if ( !devicenetworkconfig.hostConfigList.empty() ) {
		                string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);
						devicenetworkconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

						devicenetworkconfig.DeviceName = moduletypeconfig.ModuleType + itoa(moduleID);
						moduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
						devicenetworkconfig.hostConfigList.clear();

						moduleFound++;
						if ( moduleFound >= moduletypeconfig.ModuleCount )
							break;
					}
				}

				// get filesystems
				DiskMonitorFileSystems fs;
				for (int fsID = 1;; fsID++)
				{
	            	string ModuleDiskMonitorFS = MODULE_DISK_MONITOR_FS + itoa(fsID) + "-" + itoa(moduleTypeID);

                	string fsName = sysConfig->getConfig(Section, ModuleDiskMonitorFS);
					if (fsName.empty())
						break;

					fs.push_back(fsName);
				}
				moduletypeconfig.FileSystems = fs;

				// get dbroot IDs
				moduleFound = 0;
				for (int moduleID = 1; moduleID < MAX_MODULE+1 ; moduleID++)
				{
					string ModuleDBRootCount = MODULE_DBROOT_COUNT + itoa(moduleID) + "-" + itoa(moduleTypeID);
					string temp = sysConfig->getConfig(Section, ModuleDBRootCount).c_str();
					if ( temp.empty() || temp == oam::UnassignedName)
						continue;

					int moduledbrootcount = strtol(temp.c_str(), 0, 0);
	
					DeviceDBRootConfig devicedbrootconfig;
					DBRootConfigList dbrootconfiglist;

					if ( moduledbrootcount < 1 ) {
						dbrootconfiglist.clear();
					}
					else
					{
	
						int foundIDs = 0;
						for (int dbrootID = 1; dbrootID < moduledbrootcount+1 ; dbrootID++)
						{
							string DBRootID = MODULE_DBROOT_ID + itoa(moduleID) + "-" + itoa(dbrootID) + "-" + itoa(moduleTypeID);
		
							string dbrootid = sysConfig->getConfig(Section, DBRootID);
							if (dbrootid.empty() || dbrootid == oam::UnassignedName
								|| dbrootid == "0")
								continue;
	
							dbrootconfiglist.push_back(atoi(dbrootid.c_str()));
							foundIDs++;
							if ( moduledbrootcount == foundIDs)
								break;
						}
					}

					sort ( dbrootconfiglist.begin(), dbrootconfiglist.end() );
					devicedbrootconfig.DeviceID = moduleID;
					devicedbrootconfig.dbrootConfigList = dbrootconfiglist;
					moduletypeconfig.ModuleDBRootList.push_back(devicedbrootconfig);
					devicedbrootconfig.dbrootConfigList.clear();

					moduleFound++;
					if ( moduleFound >= moduletypeconfig.ModuleCount )
						break;
				}

                return;
            }
        }

        // Module Not found
        exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * get System Module Configuration Information by Module Name
     *
     ********************************************************************/

    void Oam::getSystemConfig(const std::string&module, ModuleConfig& moduleconfig)
    {
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemModuleConfig";
        const string MODULE_TYPE = "ModuleType";
        const string MODULE_DESC = "ModuleDesc";
        const string MODULE_COUNT = "ModuleCount";
        const string MODULE_IP_ADDR = "ModuleIPAddr";
        const string MODULE_SERVER_NAME = "ModuleHostName";
        const string MODULE_DISABLE_STATE = "ModuleDisableState";
        const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";
        const string MODULE_DBROOT_ID = "ModuleDBRootID";

		string moduletype = module.substr(0,MAX_MODULE_TYPE_SIZE);
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		if ( moduleID < 1 )
			//invalid ID
        	exceptionControl("getSystemConfig", API_INVALID_PARAMETER);

        for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE+1; moduleTypeID++)
        {
            string moduleType = MODULE_TYPE + itoa(moduleTypeID);
            string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype  )
            {
                string ModuleType = MODULE_TYPE + itoa(moduleTypeID);
                string ModuleDesc = MODULE_DESC + itoa(moduleTypeID);
	            string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);

                moduleconfig.ModuleName = module;
                moduleconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
                moduleconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc) + " #" + itoa(moduleID);
                moduleconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

				string ModuleDBRootCount = MODULE_DBROOT_COUNT + itoa(moduleID) + "-" + itoa(moduleTypeID);
				string temp = sysConfig->getConfig(Section, ModuleDBRootCount).c_str();

				int moduledbrootcount = 0;
				if ( temp.empty() || temp != oam::UnassignedName)
					moduledbrootcount = strtol(temp.c_str(), 0, 0);

				HostConfig hostconfig;

				//get NIC IP address/hostnames
				moduleconfig.hostConfigList.clear();
				for (int nicID = 1; nicID < MAX_NIC+1 ; nicID++)
				{
					string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);

					string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
					if (ipAddr.empty() || ipAddr == UnassignedIpAddr )
						continue;

					string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
					string serverName = sysConfig->getConfig(Section, ModuleHostName);

					hostconfig.IPAddr = ipAddr;
					hostconfig.HostName = serverName;
					hostconfig.NicID = nicID;

					moduleconfig.hostConfigList.push_back(hostconfig);
				}

				//get DBroot IDs
				moduleconfig.dbrootConfigList.clear();
				for (int dbrootID = 1; dbrootID < moduledbrootcount+1 ; dbrootID++)
				{
					string ModuleDBRootID = MODULE_DBROOT_ID + itoa(moduleID) + "-" + itoa(dbrootID) + "-" + itoa(moduleTypeID);

					string moduleDBRootID = sysConfig->getConfig(Section, ModuleDBRootID);
					if (moduleDBRootID.empty() || moduleDBRootID == oam::UnassignedName
						|| moduleDBRootID == "0")
						continue;

					moduleconfig.dbrootConfigList.push_back(atoi(moduleDBRootID.c_str()));
				}

				sort ( moduleconfig.dbrootConfigList.begin(), moduleconfig.dbrootConfigList.end() );

                return;
            }
        }

        // Module Not found
        exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * get Local Module Configuration Information
     *
     ********************************************************************/

    void Oam::getSystemConfig(ModuleConfig& moduleconfig)
    {
        // get Local Module Name

        oamModuleInfo_t t = Oam::getModuleInfo();

        string module = boost::get<0>(t);

        // get Module info

        Oam::getSystemConfig(module, moduleconfig);
    }

    /********************************************************************
     *
     * get Local Module Type Configuration Information
     *
     ********************************************************************/

    void Oam::getSystemConfig(ModuleTypeConfig& moduletypeconfig)
    {
        // get Local Module Name

        oamModuleInfo_t t = Oam::getModuleInfo();

        string module = boost::get<0>(t);
		string moduleType = module.substr(0,MAX_MODULE_TYPE_SIZE);

        // get Module info

        Oam::getSystemConfig(moduleType, moduletypeconfig);
    }

    /********************************************************************
     *
     *	get System External Device Configuration information
     *
     ********************************************************************/

	void Oam::getSystemConfig(SystemExtDeviceConfig& systemextdeviceconfig)
	{
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemExtDeviceConfig";
        const string NAME = "Name";
        const string IPADDR = "IPAddr";
        const string DISABLE_STATE = "DisableState";
 
        systemextdeviceconfig.Count = strtol(sysConfig->getConfig(Section, "Count").c_str(), 0, 0);

		int configCount = 0;
        for (int extDeviceID = 1; extDeviceID < MAX_EXT_DEVICE+1; extDeviceID++)
        {
	        ExtDeviceConfig Extdeviceconfig;

			string name = NAME + itoa(extDeviceID);

			try {
				Extdeviceconfig.Name = sysConfig->getConfig(Section, name);
			}
			catch(...)
			{
				continue;
			}

            if (Extdeviceconfig.Name == oam::UnassignedName ||
				Extdeviceconfig.Name.empty())
                continue;

			string ipaddr = IPADDR + itoa(extDeviceID);
			string disablestate = DISABLE_STATE + itoa(extDeviceID);

			Extdeviceconfig.IPAddr = sysConfig->getConfig(Section, ipaddr);
			Extdeviceconfig.DisableState = sysConfig->getConfig(Section, disablestate);

            systemextdeviceconfig.extdeviceconfig.push_back(Extdeviceconfig);
			configCount++;
		}
		//correct count if not matching
		if ( systemextdeviceconfig.Count != configCount ) {
			systemextdeviceconfig.Count = configCount;

			sysConfig->setConfig(Section, "Count", itoa(configCount));

			try
			{
	        	sysConfig->write();
			}
			catch(...)
			{
				exceptionControl("getSystemConfig", API_FAILURE);
			}
		}
	}

    /********************************************************************
     *
     *	get System External Device Configuration information
     *
     ********************************************************************/

    void Oam::getSystemConfig(const std::string&extDevicename, ExtDeviceConfig& extdeviceconfig)
	{
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemExtDeviceConfig";
        const string NAME = "Name";
        const string IPADDR = "IPAddr";
        const string DISABLE_STATE = "DisableState";

        for (int extDeviceID = 1; extDeviceID < MAX_EXT_DEVICE+1; extDeviceID++)
        {
			string name = NAME + itoa(extDeviceID);

			extdeviceconfig.Name = sysConfig->getConfig(Section, name);

            if (extdeviceconfig.Name != extDevicename)
                continue;

			string ipaddr = IPADDR + itoa(extDeviceID);
			string disablestate = DISABLE_STATE + itoa(extDeviceID);

			extdeviceconfig.IPAddr = sysConfig->getConfig(Section, ipaddr);
			extdeviceconfig.DisableState = sysConfig->getConfig(Section, disablestate);
			return;
		}

        // Ext Device Not found
        exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
    }


    /********************************************************************
     *
     * set Ext Device Configuration information
     *
     ********************************************************************/

    void Oam::setSystemConfig(const std::string deviceName, ExtDeviceConfig extdeviceconfig)
    {
		if ( deviceName == oam::UnassignedName )
			return;

        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemExtDeviceConfig";
        const string NAME = "Name";
        const string IPADDR = "IPAddr";
        const string DISABLE_STATE = "DisableState";

        int count = strtol(sysConfig->getConfig(Section, "Count").c_str(), 0, 0);

		int entry = 0;
		int extDeviceID = 1;
        for (; extDeviceID < MAX_EXT_DEVICE+1; extDeviceID++)
        {
			string name = NAME + itoa(extDeviceID);

            if (sysConfig->getConfig(Section, name) == oam::UnassignedName)
				entry = extDeviceID;

            if ((sysConfig->getConfig(Section, name)).empty() && entry == 0)
				entry = extDeviceID;

            if (sysConfig->getConfig(Section, name) != deviceName)
                continue;

			string ipaddr = IPADDR + itoa(extDeviceID);
			string disablestate = DISABLE_STATE + itoa(extDeviceID);

			sysConfig->setConfig(Section, name, extdeviceconfig.Name);
			sysConfig->setConfig(Section, ipaddr, extdeviceconfig.IPAddr);
			sysConfig->setConfig(Section, disablestate, extdeviceconfig.DisableState);

			if ( extdeviceconfig.Name == oam::UnassignedName ) {
				// entry deleted decrement count
				count--;
				if ( count < 0 )
					count = 0 ; 
				sysConfig->setConfig(Section, "Count", itoa(count));

				//
				//send message to Process Monitor to remove external device to shared memory
				//
				try
				{
					ByteStream obs;
			
					obs << (ByteStream::byte) REMOVE_EXT_DEVICE;
					obs << deviceName;
			
					sendStatusUpdate(obs, REMOVE_EXT_DEVICE);
				}
				catch(...)
				{
					exceptionControl("setSystemConfig", API_INVALID_PARAMETER);
				}

			}

			try
			{
	        	sysConfig->write();
			}
			catch(...)
			{
				exceptionControl("setSystemConfig", API_FAILURE);
			}

			return;
		}

		if ( entry == 0 )
			entry = extDeviceID;

        // Ext Device Not found, add it

		sysConfig->setConfig(Section, "Count", itoa(count+1));

		string name = NAME + itoa(entry);
		string ipaddr = IPADDR + itoa(entry);
		string disablestate = DISABLE_STATE + itoa(entry);

		sysConfig->setConfig(Section, name, extdeviceconfig.Name);
		sysConfig->setConfig(Section, ipaddr, extdeviceconfig.IPAddr);
		if (extdeviceconfig.DisableState.empty() )
			 extdeviceconfig.DisableState = oam::ENABLEDSTATE;

		sysConfig->setConfig(Section, disablestate, extdeviceconfig.DisableState);
		try
		{
			sysConfig->write();
		}
		catch(...)
		{
			exceptionControl("setSystemConfig", API_FAILURE);
		}

		//
		//send message to Process Monitor to add new external device to shared memory
		//
		try
		{
			ByteStream obs;
	
			obs << (ByteStream::byte) ADD_EXT_DEVICE;
			obs << extdeviceconfig.Name;
	
			sendStatusUpdate(obs, ADD_EXT_DEVICE);
		}
		catch(...)
		{
			exceptionControl("setSystemConfig", API_INVALID_PARAMETER);
		}

		return;
    }

    /********************************************************************
     *
     * get System Configuration String Parameter value
     *
     ********************************************************************/

    void Oam::getSystemConfig(const std::string&name, std::string& value)
    {
        // added by Zhixuan
        // special handle to NMSIPAddr, which is in snmpdx.conf file
        if (name.compare("NMSIPAddr") == 0)
        {
            try
            {
                SNMPManager sm;
                sm.getNMSAddr (value);
                return;
            }
            catch(...)
            {
                // error with SM API
                exceptionControl("getSystemConfig", API_FAILURE);
            }
        }

	
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

        // get string variables

        for( int i = 0;;i++)
        {
            if ( configSections[i] == "" )
                // end of section list
                break;

            value = sysConfig->getConfig(configSections[i], name);

            if (!(value.empty()))
            {
                // match found
                return;
            }
        }
        // no match found
        exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * get System Configuration Integer Parameter value
     *
     ********************************************************************/

    void Oam::getSystemConfig(const std::string&name, int& value)
    {
        string returnValue;

        // get string variables

        Oam::getSystemConfig(name, returnValue);

        // covert returned Parameter value to Interger

        value = atoi(returnValue.c_str());
    }

    /********************************************************************
     *
     * get Module Name for IP Address
     *
     ********************************************************************/

    void Oam::getModuleNameByIPAddr(const std::string IpAddress, std::string& moduleName)
    {
		SystemModuleTypeConfig systemmoduletypeconfig;
		ModuleTypeConfig moduletypeconfig;
		ModuleConfig moduleconfig;
		systemmoduletypeconfig.moduletypeconfig.clear();
		string returnValue;
		string Argument;

		try
		{
			Oam::getSystemConfig(systemmoduletypeconfig);

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

					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
					{
						if ( IpAddress == (*pt1).IPAddr ) {
							moduleName = modulename;
							return;
						}
					}
				}
			}
			moduleName = oam::UnassignedName;
			return;
		}
		catch (exception&)
		{
			exceptionControl("getModuleNameByIPAddr", API_FAILURE);
		}
    }


    /********************************************************************
     *
     * set System Configuration String Parameter value
     *
     ********************************************************************/

    void Oam::setSystemConfig(const std::string name, const std::string value)
    {
        // added by Zhixuan
        // special handle to SNMP config, which is in snmpdx.conf file
        string mem = "Mem";
        string disk = "Disk";
        string swap = "Swap";
        string threshold = "Threshold";
        string critical = "Critical";
        string major = "Major";
        string minor = "Minor";

        if (name.find("NMSIPAddr") != string::npos)
        {
            try
            {
                SNMPManager sm;
                sm.setNMSAddr (value);
                return;
            }
            catch(...)
            {
                // error with SM API
                exceptionControl("setSystemConfig", API_FAILURE);
            }
        }

	
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string returnValue;

        // find and write new value to disk

        for( int i = 0;;i++)
        {
            if ( configSections[i] == "" )
                // end of section list, no match found
                exceptionControl("setSystemConfig", API_INVALID_PARAMETER);

            returnValue = sysConfig->getConfig(configSections[i], name);

            if (!(returnValue.empty()))
            {
                // match found
                sysConfig->setConfig(configSections[i], name, value);
				try
				{
					sysConfig->write();
				}
				catch(...)
				{
					exceptionControl("setSystemConfig", API_FAILURE);
				}
                break;
            }
        }

       return;
    }

    /********************************************************************
     *
     * set System Configuration Interger Parameter value
     *
     ********************************************************************/

    void Oam::setSystemConfig(const std::string name, const int value)
    {
        string valueString;

        // convert Incoming Interger Parameter value to String

        valueString = itoa(value);

        // write parameter to disk

        Oam::setSystemConfig(name, valueString);
    }

    /********************************************************************
     *
     * set System Module Configuration Information by Module Type
     *
     ********************************************************************/

    void Oam::setSystemConfig(const std::string moduletype, ModuleTypeConfig moduletypeconfig)
    {
        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        const string Section = "SystemModuleConfig";
        const string MODULE_TYPE = "ModuleType";
        const string MODULE_DESC = "ModuleDesc";
        const string MODULE_RUN_TYPE = "RunType";
        const string MODULE_COUNT = "ModuleCount";
        const string MODULE_CPU_CRITICAL = "ModuleCPUCriticalThreshold";
        const string MODULE_CPU_MAJOR = "ModuleCPUMajorThreshold";
        const string MODULE_CPU_MINOR = "ModuleCPUMinorThreshold";
        const string MODULE_CPU_MINOR_CLEAR = "ModuleCPUMinorClearThreshold";
        const string MODULE_DISK_CRITICAL = "ModuleDiskCriticalThreshold";
        const string MODULE_DISK_MAJOR = "ModuleDiskMajorThreshold";
        const string MODULE_DISK_MINOR = "ModuleDiskMinorThreshold";
        const string MODULE_MEM_CRITICAL = "ModuleMemCriticalThreshold";
        const string MODULE_MEM_MAJOR = "ModuleMemMajorThreshold";
        const string MODULE_MEM_MINOR = "ModuleMemMinorThreshold";
        const string MODULE_SWAP_CRITICAL = "ModuleSwapCriticalThreshold";
        const string MODULE_SWAP_MAJOR = "ModuleSwapMajorThreshold";
        const string MODULE_SWAP_MINOR = "ModuleSwapMinorThreshold";
        const string MODULE_IP_ADDR = "ModuleIPAddr";
        const string MODULE_SERVER_NAME = "ModuleHostName";
        const string MODULE_DISK_MONITOR_FS = "ModuleDiskMonitorFileSystem";
        const string MODULE_DISABLE_STATE = "ModuleDisableState";

        for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE+1; moduleTypeID++)
        {
            string moduleType = MODULE_TYPE + itoa(moduleTypeID);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype)
            {
                string ModuleType = MODULE_TYPE + itoa(moduleTypeID);
                string ModuleDesc = MODULE_DESC + itoa(moduleTypeID);
                string ModuleRunType = MODULE_RUN_TYPE + itoa(moduleTypeID);
                string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);
                string ModuleCPUCriticalThreshold = MODULE_CPU_CRITICAL + itoa(moduleTypeID);
                string ModuleCPUMajorThreshold = MODULE_CPU_MAJOR + itoa(moduleTypeID);
                string ModuleCPUMinorThreshold = MODULE_CPU_MINOR + itoa(moduleTypeID);
                string ModuleCPUMinorClearThreshold = MODULE_CPU_MINOR_CLEAR + itoa(moduleTypeID);
                string ModuleDiskCriticalThreshold = MODULE_DISK_CRITICAL + itoa(moduleTypeID);
                string ModuleDiskMajorThreshold = MODULE_DISK_MAJOR + itoa(moduleTypeID);
                string ModuleDiskMinorThreshold = MODULE_DISK_MINOR + itoa(moduleTypeID);
                string ModuleMemCriticalThreshold = MODULE_MEM_CRITICAL + itoa(moduleTypeID);
                string ModuleMemMajorThreshold = MODULE_MEM_MAJOR + itoa(moduleTypeID);
                string ModuleMemMinorThreshold = MODULE_MEM_MINOR + itoa(moduleTypeID);
                string ModuleSwapCriticalThreshold = MODULE_SWAP_CRITICAL + itoa(moduleTypeID);
                string ModuleSwapMajorThreshold = MODULE_SWAP_MAJOR + itoa(moduleTypeID);
                string ModuleSwapMinorThreshold = MODULE_SWAP_MINOR + itoa(moduleTypeID);

                int oldModuleCount = atoi(sysConfig->getConfig(Section, ModuleCount).c_str());

                sysConfig->setConfig(Section, ModuleType, moduletypeconfig.ModuleType);
                sysConfig->setConfig(Section, ModuleDesc, moduletypeconfig.ModuleDesc);
                sysConfig->setConfig(Section, ModuleRunType, moduletypeconfig.RunType);
                sysConfig->setConfig(Section, ModuleCount, itoa(moduletypeconfig.ModuleCount));
                sysConfig->setConfig(Section, ModuleCPUCriticalThreshold, itoa(moduletypeconfig.ModuleCPUCriticalThreshold));
                sysConfig->setConfig(Section, ModuleCPUMajorThreshold, itoa(moduletypeconfig.ModuleCPUMajorThreshold));
                sysConfig->setConfig(Section, ModuleCPUMinorThreshold, itoa(moduletypeconfig.ModuleCPUMinorThreshold));
                sysConfig->setConfig(Section, ModuleCPUMinorClearThreshold, itoa(moduletypeconfig.ModuleCPUMinorClearThreshold));
                sysConfig->setConfig(Section, ModuleDiskCriticalThreshold, itoa(moduletypeconfig.ModuleDiskCriticalThreshold));
                sysConfig->setConfig(Section, ModuleDiskMajorThreshold, itoa(moduletypeconfig.ModuleDiskMajorThreshold));
                sysConfig->setConfig(Section, ModuleDiskMinorThreshold, itoa(moduletypeconfig.ModuleDiskMinorThreshold));
                sysConfig->setConfig(Section, ModuleMemCriticalThreshold, itoa(moduletypeconfig.ModuleMemCriticalThreshold));
                sysConfig->setConfig(Section, ModuleMemMajorThreshold, itoa(moduletypeconfig.ModuleMemMajorThreshold));
                sysConfig->setConfig(Section, ModuleMemMinorThreshold, itoa(moduletypeconfig.ModuleMemMinorThreshold));
                sysConfig->setConfig(Section, ModuleSwapCriticalThreshold, itoa(moduletypeconfig.ModuleSwapCriticalThreshold));
                sysConfig->setConfig(Section, ModuleSwapMajorThreshold, itoa(moduletypeconfig.ModuleSwapMajorThreshold));
                sysConfig->setConfig(Section, ModuleSwapMinorThreshold, itoa(moduletypeconfig.ModuleSwapMinorThreshold));

				// clear out hostConfig info before adding in new contents
				if ( oldModuleCount > 0) {
					for (int moduleID = 1; moduleID < MAX_MODULE ; moduleID++)
					{
						//get NIC IP address/hostnames
						for (int nicID = 1; nicID < MAX_NIC+1 ; nicID++)
						{
							string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
		
							string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
							if (ipAddr.empty())
								continue;
		
							string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(nicID) + "-" + itoa(moduleTypeID);
							string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);

							sysConfig->setConfig(Section, ModuleIpAddr, UnassignedIpAddr);
							sysConfig->setConfig(Section, ModuleHostName, UnassignedName);
							sysConfig->setConfig(Section, ModuleDisableState, oam::ENABLEDSTATE);
						}
					}
				}

				if ( moduletypeconfig.ModuleCount > 0 )
				{
					DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
					for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
					{
						int ModuleID = atoi((*pt).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

						string ModuleDisableState = MODULE_DISABLE_STATE + itoa(ModuleID) + "-" + itoa(moduleTypeID);
						sysConfig->setConfig(Section, ModuleDisableState, (*pt).DisableState);

						HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
						for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
						{
							int nidID = (*pt1).NicID;
							string ModuleIpAddr = MODULE_IP_ADDR + itoa(ModuleID) + "-" + itoa(nidID) + "-" + itoa(moduleTypeID);
							sysConfig->setConfig(Section, ModuleIpAddr, (*pt1).IPAddr);
	
							string ModuleHostName = MODULE_SERVER_NAME + itoa(ModuleID) + "-" + itoa(nidID) + "-" + itoa(moduleTypeID);
							sysConfig->setConfig(Section, ModuleHostName, (*pt1).HostName);
						}
					}
				}

				DiskMonitorFileSystems::iterator pt = moduletypeconfig.FileSystems.begin();
				int id=1;
				for( ; pt != moduletypeconfig.FileSystems.end() ; pt++)
				{
	            	string ModuleDiskMonitorFS = MODULE_DISK_MONITOR_FS + itoa(id) + "-" + itoa(moduleTypeID);
                	sysConfig->setConfig(Section, ModuleDiskMonitorFS, *pt);
					++id;
				}
				try
				{
					sysConfig->write();
				}
				catch(...)
				{
					exceptionControl("getSystemConfig", API_FAILURE);
				}

                return;
            }
        }

        // Module Not found
        exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set System Module Configuration Information by Module Name
     *
     ********************************************************************/

    void Oam::setSystemConfig(const std::string module, ModuleConfig moduleconfig)
    {
        Config* sysConfig100 = Config::makeConfig(CalpontConfigFile.c_str());

        const string MODULE_TYPE = "ModuleType";
        const string Section = "SystemModuleConfig";
        const string MODULE_COUNT = "ModuleCount";
        const string MODULE_IP_ADDR = "ModuleIPAddr";
        const string MODULE_SERVER_NAME = "ModuleHostName";
        const string MODULE_DISABLE_STATE = "ModuleDisableState";
        const string MODULE_DBROOTID = "ModuleDBRootID";
        const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";

		string moduletype = module.substr(0,MAX_MODULE_TYPE_SIZE);
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		if ( moduleID < 1 )
			//invalid ID
        	exceptionControl("setSystemConfig", API_INVALID_PARAMETER);

        for (int moduleTypeID = 1; moduleTypeID < MAX_MODULE_TYPE+1; moduleTypeID++)
        {
            string moduleType = MODULE_TYPE + itoa(moduleTypeID);
            string ModuleCount = MODULE_COUNT + itoa(moduleTypeID);

            if( sysConfig100->getConfig(Section, moduleType) ==  moduletype )
            {
	            string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(moduleTypeID);
				sysConfig100->setConfig(Section, ModuleDisableState, moduleconfig.DisableState);

				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				for( ; pt1 != moduleconfig.hostConfigList.end() ; pt1++)
				{
					string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa((*pt1).NicID) + "-" + itoa(moduleTypeID);
					sysConfig100->setConfig(Section, ModuleIpAddr, (*pt1).IPAddr);
	
					string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa((*pt1).NicID) + "-" + itoa(moduleTypeID);
					sysConfig100->setConfig(Section, ModuleHostName, (*pt1).HostName);
				}

				int id = 1;
				if ( moduleconfig.dbrootConfigList.size() == 0 )
				{
					string ModuleDBrootID = MODULE_DBROOTID + itoa(moduleID) + "-" + itoa((id)) + "-" + itoa(moduleTypeID);
					sysConfig100->setConfig(Section, ModuleDBrootID, oam::UnassignedName);
				}
				else
				{
					DBRootConfigList::iterator pt2 = moduleconfig.dbrootConfigList.begin();
					for( ; pt2 != moduleconfig.dbrootConfigList.end() ; pt2++, id++)
					{
						string ModuleDBrootID = MODULE_DBROOTID + itoa(moduleID) + "-" + itoa((id)) + "-" + itoa(moduleTypeID);
						sysConfig100->setConfig(Section, ModuleDBrootID, itoa((*pt2)));
					}
				}

				//set entries no longer configured to unsassigned
				for ( int extraid = id ; id < MAX_DBROOT ; extraid++ )
				{
					string ModuleDBrootID = MODULE_DBROOTID + itoa(moduleID) + "-" + itoa((extraid)) + "-" + itoa(moduleTypeID);
					if ( sysConfig100->getConfig(Section, ModuleDBrootID).empty() || 
							sysConfig100->getConfig(Section, ModuleDBrootID) == oam::UnassignedName )
						break;
					sysConfig100->setConfig(Section, ModuleDBrootID, oam::UnassignedName);
				}

				string ModuleDBRootCount = MODULE_DBROOT_COUNT + itoa(moduleID) + "-" + itoa(moduleTypeID);
				sysConfig100->setConfig(Section, ModuleDBRootCount, itoa(moduleconfig.dbrootConfigList.size()));

				try
				{
					sysConfig100->write();
				}
				catch(...)
				{
					exceptionControl("setSystemConfig", API_FAILURE);
				}

                return;
            }
        }

        // Module Not found
        exceptionControl("setSystemConfig", API_INVALID_PARAMETER);
    }


    /********************************************************************
     *
     * add Module
     *
     ********************************************************************/

    void Oam::addModule(DeviceNetworkList devicenetworklist, const std::string password)
	{
        // build and send msg
        int returnStatus = sendMsgToProcMgr2(ADDMODULE, devicenetworklist, FORCEFUL, ACK_YES, password);

        if (returnStatus != API_SUCCESS)
            exceptionControl("addModule", returnStatus);

	}

    /********************************************************************
     *
     * remove Module
     *
     ********************************************************************/

    void Oam::removeModule(DeviceNetworkList devicenetworklist)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("removeModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(REMOVEMODULE, devicenetworklist, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("removeModule", returnStatus);
	}

    /********************************************************************
     *
     * reconfigure Module
     *
     ********************************************************************/

    void Oam::reconfigureModule(DeviceNetworkList devicenetworklist)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		// validate Module name
		int returnStatus = validateModule((*pt).DeviceName);
		if (returnStatus != API_SUCCESS)
			exceptionControl("reconfigureModule", returnStatus);

        // build and send msg
        returnStatus = sendMsgToProcMgr2(RECONFIGUREMODULE, devicenetworklist, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("reconfigureModule", returnStatus);
	}


    /********************************************************************
     *
     * get System Status Information
     *
     ********************************************************************/

    void Oam::getSystemStatus(SystemStatus& systemstatus)
    {
#ifdef _MSC_VER
        // TODO: Remove when we create OAM for Windows
        return;
#endif
        ModuleStatus modulestatus;
        systemstatus.systemmodulestatus.modulestatus.clear();
        ExtDeviceStatus extdevicestatus;
        systemstatus.systemextdevicestatus.extdevicestatus.clear();
        NICStatus nicstatus;
        systemstatus.systemnicstatus.nicstatus.clear();
        DbrootStatus dbrootstatus;
        systemstatus.systemdbrootstatus.dbrootstatus.clear();

        try
        {
            MessageQueueClient processor("ProcStatusControl");
			processor.syncProto(false);
            ByteStream::byte ModuleNumber;
            ByteStream::byte ExtDeviceNumber;
            ByteStream::byte dbrootNumber;
            ByteStream::byte NICNumber;
			ByteStream::byte state;
			std::string name;
			std::string date;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_SYSTEM_STATUS;

			try {
				struct timespec ts = { 10, 0 };
				processor.write(obs, &ts);
			}
			catch (exception& e)
			{
				processor.shutdown();
				string error = e.what();
				writeLog("getSystemStatus: write exception: " + error, LOG_TYPE_ERROR);
				exceptionControl("getSystemStatus", API_FAILURE);
			}
			catch(...)
			{
				processor.shutdown();
				writeLog("getSystemStatus: write exception: unknown", LOG_TYPE_ERROR);
				exceptionControl("getSystemStatus", API_FAILURE);
			}

			// wait 20 seconds for ACK from Process Monitor
			try {
				struct timespec ts = { 20, 0 };
				ibs = processor.read(&ts);
			}
			catch (exception& e)
			{
				processor.shutdown();
				string error = e.what();
				writeLog("getSystemStatus: read exception: " + error, LOG_TYPE_ERROR);
				exceptionControl("getSystemStatus", API_FAILURE);
			}
			catch(...)
			{
				processor.shutdown();
				writeLog("getSystemStatus: read exception: unknown", LOG_TYPE_ERROR);
				exceptionControl("getSystemStatus", API_FAILURE);
			}

			if (ibs.length() > 0)
			{
				ibs >> ModuleNumber;

				for( int i=0 ; i < ModuleNumber ; ++i)
				{
					ibs >> name;
					ibs >> state;
					ibs >> date;
					if ( name.find("system") != string::npos ) {
						systemstatus.SystemOpState = state;
						systemstatus.StateChangeDate = date;
					}
					else
					{
						modulestatus.Module = name;
						modulestatus.ModuleOpState = state;
						modulestatus.StateChangeDate = date;
						systemstatus.systemmodulestatus.modulestatus.push_back(modulestatus);
					}
				}

				ibs >> ExtDeviceNumber;

				for( int i=0 ; i < ExtDeviceNumber ; ++i)
				{
					ibs >> name;
					ibs >> state;
					ibs >> date;
					extdevicestatus.Name = name;
					extdevicestatus.OpState = state;
					extdevicestatus.StateChangeDate = date;
					systemstatus.systemextdevicestatus.extdevicestatus.push_back(extdevicestatus);
				}

				ibs >> NICNumber;

				for( int i=0 ; i < NICNumber ; ++i)
				{
					ibs >> name;
					ibs >> state;
					ibs >> date;
					nicstatus.HostName = name;
					nicstatus.NICOpState = state;
					nicstatus.StateChangeDate = date;
					systemstatus.systemnicstatus.nicstatus.push_back(nicstatus);
				}

				ibs >> dbrootNumber;

				for( int i=0 ; i < dbrootNumber ; ++i)
				{
					ibs >> name;
					ibs >> state;
					ibs >> date;
					dbrootstatus.Name = name;
					dbrootstatus.OpState = state;
					dbrootstatus.StateChangeDate = date;
					systemstatus.systemdbrootstatus.dbrootstatus.push_back(dbrootstatus);
				}

				processor.shutdown();
				return;
			}
			// timeout ocurred, shutdown connection
			processor.shutdown();
			writeLog("getSystemStatus: read 0 length", LOG_TYPE_ERROR);
        }
		catch (exception& e)
		{
			string error = e.what();
			writeLog("getSystemStatus: MessageQueueClient exception: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			writeLog("getSystemStatus: MessageQueueClient exception: unknown", LOG_TYPE_ERROR);
		}

		exceptionControl("getSystemStatus", API_FAILURE);
    }

    /********************************************************************
     *
     * set System Status information
     *
     ********************************************************************/

    void Oam::setSystemStatus(const int state)
    {
		//send and wait for ack and resend if not received
		//retry 3 time max
		for ( int i=0; i < 3 ; i++)
		{
			try
			{
				ByteStream obs;
				obs << (ByteStream::byte) SET_SYSTEM_STATUS;
				obs << (ByteStream::byte) state;
	
	    		sendStatusUpdate(obs, SET_SYSTEM_STATUS);
				return;
			}
			catch(...)
			{}
		}
		exceptionControl("setSystemStatus", API_FAILURE);
    }

    /********************************************************************
     *
     * get Module Status information
     *
     ********************************************************************/

    void Oam::getModuleStatus(const std::string name, int& state, bool& degraded)
    {
		SystemStatus systemstatus;
        ModuleConfig moduleconfig;
        std::vector<int> NICstates;
		degraded = false;

		try
		{
			getSystemStatus(systemstatus);

			for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
			{
				if( systemstatus.systemmodulestatus.modulestatus[i].Module == name ) {
					state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;

					// get NIC status for degraded state info
                    try
                    {
                        getSystemConfig(name, moduleconfig);

						HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
						for( ; pt1 != moduleconfig.hostConfigList.end() ; pt1++)
						{
							try {
								int state;
								getNICStatus((*pt1).HostName, state);
								NICstates.push_back(state);
							}
							catch (...)
							{}
						}
						
						vector<int>::iterator pt = NICstates.begin();
						for( ; pt != NICstates.end() ; pt++)
						{
							if ( (*pt) == oam::DOWN ) {
								degraded = true;
								break;
							}
						}
						return;
                    }
                    catch (...)
                    {}
				}
			}
		}
		catch(...)
		{}

        // no match found
        exceptionControl("getModuleStatus", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set Module Status information
     *
     ********************************************************************/

    void Oam::setModuleStatus(const std::string name, const int state)
    {
		//send and wait for ack and resend if not received
		//retry 3 time max
		for ( int i=0; i < 3 ; i++)
		{
			try
			{
				ByteStream obs;
	
				obs << (ByteStream::byte) SET_MODULE_STATUS;
				obs << name;
				obs << (ByteStream::byte) state;
	
	    		sendStatusUpdate(obs, SET_MODULE_STATUS);
				return;
			}
			catch(...)
			{}
		}
		exceptionControl("setModuleStatus", API_FAILURE);
    }


    /********************************************************************
     *
     * get External Device Status information
     *
     ********************************************************************/

    void Oam::getExtDeviceStatus(const std::string name, int& state)
    {
		SystemStatus systemstatus;

		try
		{
			getSystemStatus(systemstatus);

			for( unsigned int i = 0 ; i < systemstatus.systemextdevicestatus.extdevicestatus.size(); i++)
			{
				if( systemstatus.systemextdevicestatus.extdevicestatus[i].Name == name ) {
					state = systemstatus.systemextdevicestatus.extdevicestatus[i].OpState;
					return;
				}
			}
		}
		catch (exception&)
		{
	        exceptionControl("getExtDeviceStatus", API_FAILURE);
		}

        // no match found
        exceptionControl("getExtDeviceStatus", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set External Device Status information
     *
     ********************************************************************/

    void Oam::setExtDeviceStatus(const std::string name, const int state)
    {
		//send and wait for ack and resend if not received
		//retry 3 time max
		for ( int i=0; i < 3 ; i++)
		{
			try
			{
				ByteStream obs;
	
				obs << (ByteStream::byte) SET_EXT_DEVICE_STATUS;
				obs << name;
				obs << (ByteStream::byte) state;
	
	    		sendStatusUpdate(obs, SET_EXT_DEVICE_STATUS);
				return;
			}
			catch(...)
			{}
		}
        exceptionControl("setExtDeviceStatus", API_FAILURE);
    }

    /********************************************************************
     *
     * get DBroot Status information
     *
     ********************************************************************/

    void Oam::getDbrootStatus(const std::string name, int& state)
    {
		SystemStatus systemstatus;

		try
		{
			getSystemStatus(systemstatus);

			for( unsigned int i = 0 ; i < systemstatus.systemdbrootstatus.dbrootstatus.size(); i++)
			{
				if( systemstatus.systemdbrootstatus.dbrootstatus[i].Name == name ) {
					state = systemstatus.systemdbrootstatus.dbrootstatus[i].OpState;
					return;
				}
			}
		}
		catch (exception&)
		{
	        exceptionControl("getDbrootStatus", API_FAILURE);
		}

        // no match found
        exceptionControl("getDbrootStatus", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set DBroot Status information
     *
     ********************************************************************/

    void Oam::setDbrootStatus(const std::string name, const int state)
    {
		//send and wait for ack and resend if not received
		//retry 3 time max
		for ( int i=0; i < 3 ; i++)
		{
			try
			{
				ByteStream obs;
	
				obs << (ByteStream::byte) SET_DBROOT_STATUS;
				obs << name;
				obs << (ByteStream::byte) state;
	
	    		sendStatusUpdate(obs, SET_DBROOT_STATUS);
				return;
			}
			catch(...)
			{}
		}
        exceptionControl("setDbrootStatus", API_FAILURE);
    }

    /********************************************************************
     *
     * get NIC Status information
     *
     ********************************************************************/

    void Oam::getNICStatus(const std::string name, int& state)
    {
		SystemStatus systemstatus;

		try
		{
			getSystemStatus(systemstatus);

			for( unsigned int i = 0 ; i < systemstatus.systemnicstatus.nicstatus.size(); i++)
			{
				if( systemstatus.systemnicstatus.nicstatus[i].HostName == name ) {
					state = systemstatus.systemnicstatus.nicstatus[i].NICOpState;
					return;
				}
			}
		}
		catch (exception&)
		{
	        exceptionControl("getNICStatus", API_FAILURE);
		}

        // no match found
        exceptionControl("getNICStatus", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set NIC Status information
     *
     ********************************************************************/

    void Oam::setNICStatus(const std::string name, const int state)
    {
		//send and wait for ack and resend if not received
		//retry 3 time max
		for ( int i=0; i < 3 ; i++)
		{
			try
			{
				ByteStream obs;
	
				obs << (ByteStream::byte) SET_NIC_STATUS;
				obs << name;
				obs << (ByteStream::byte) state;
	
	    		sendStatusUpdate(obs, SET_NIC_STATUS);
				return;
			}
			catch(...)
			{}
		}
		exceptionControl("setNICStatus", API_FAILURE);
    }

    /********************************************************************
     *
     * get Process Configuration Information
     *
     ********************************************************************/

    void Oam::getProcessConfig(const std::string process, const std::string module, ProcessConfig& processconfig)
    {

        Config* proConfig = Config::makeConfig(ProcessConfigFile.c_str());
        const string SECTION_NAME = "PROCESSCONFIG";
        const string ARG_NAME = "ProcessArg";
        string argName;
        const string DEP_NAME = "DepProcessName";
        const string DEP_MDLNAME = "DepModuleName";
        string depName;
        string depMdlName;
		string moduleType = module.substr(0,MAX_MODULE_TYPE_SIZE);

        for (int processID = 1; processID < MAX_PROCESS+1; processID++)
        {
            string sectionName = SECTION_NAME + itoa(processID);

            if( proConfig->getConfig(sectionName, "ProcessName") == process )
			{
            	string ModuleType = proConfig->getConfig(sectionName, "ModuleType");

           		if ( ModuleType == "ParentOAMModule" 
					|| ModuleType == "ChildExtOAMModule"
					|| ( ModuleType == "ChildOAMModule" && moduleType != "xm" )
					|| ModuleType == moduleType)
            	{
					// get string variables
					processconfig.ProcessName = process;
					processconfig.ModuleType = ModuleType;
	
					processconfig.ProcessLocation = proConfig->getConfig(sectionName, "ProcessLocation");
					processconfig.LogFile = proConfig->getConfig(sectionName, "LogFile");;
	
					// get Integer variables
					processconfig.BootLaunch = strtol(proConfig->getConfig(sectionName, "BootLaunch").c_str(), 0, 0);
					processconfig.LaunchID = strtol(proConfig->getConfig(sectionName, "LaunchID").c_str(), 0, 0);;
	
					// get Auguments
					for (int argID = 0; argID < MAX_ARGUMENTS; argID++)
					{
						argName = ARG_NAME + itoa(argID+1);
						processconfig.ProcessArgs[argID] = proConfig->getConfig(sectionName, argName);
					}
	
					// get process dependencies
					for (int depID = 0; depID < MAX_DEPENDANCY; depID++)
					{
						depName = DEP_NAME + itoa(depID+1);
						processconfig.DepProcessName[depID] = proConfig->getConfig(sectionName, depName);
					}
					// get dependent process Module name
					for (int moduleID = 0; moduleID < MAX_DEPENDANCY; moduleID++)
					{
						depMdlName = DEP_MDLNAME + itoa(moduleID+1);
						processconfig.DepModuleName[moduleID] = proConfig->getConfig(sectionName, depMdlName);
					}
	
					// get optional group id and type
					try {
						processconfig.RunType = proConfig->getConfig(sectionName, "RunType");
					}
					catch(...)
					{
						processconfig.RunType = "LOADSHARE";
					}
	
					return;
				}
            }
        }

        // Process Not found
        exceptionControl("getProcessConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * get System Process Configuration Information
     *
     ********************************************************************/

    void Oam::getProcessConfig(SystemProcessConfig& systemprocessconfig)
    {

        const string SECTION_NAME = "PROCESSCONFIG";
        systemprocessconfig.processconfig.clear();

        for (int processID = 1; processID < MAX_PROCESS+1; processID++)
        {
	        ProcessConfig processconfig;

            Config* proConfig = Config::makeConfig(ProcessConfigFile.c_str());

            // get process info

            string sectionName = SECTION_NAME + itoa(processID);

            Oam::getProcessConfig(proConfig->getConfig(sectionName, "ProcessName"),
                proConfig->getConfig(sectionName, "ModuleType"),
                processconfig );

            if (processconfig.ProcessName.empty())
                continue;

            systemprocessconfig.processconfig.push_back(processconfig);
        }
    }

    /********************************************************************
     *
     * get Process Configuration String Parameter value
     *
     ********************************************************************/

    void Oam::getProcessConfig(const std::string process, const std::string module, 
								const std::string name, std::string& value)
    {

        Config* proConfig = Config::makeConfig(ProcessConfigFile.c_str());
        const string SECTION_NAME = "PROCESSCONFIG";
		string moduleType = module.substr(0,MAX_MODULE_TYPE_SIZE);

        for (int processID = 1; processID < MAX_PROCESS+1; processID++)
        {
            string sectionName = SECTION_NAME + itoa(processID);

            if( proConfig->getConfig(sectionName, "ProcessName") == process )
			{
            	string ModuleType = proConfig->getConfig(sectionName, "ModuleType");

           		if ( ModuleType == "ParentOAMModule" 
					|| ModuleType == "ChildExtOAMModule"
					|| ( ModuleType == "ChildOAMModule" && moduleType != "xm" )
					|| ModuleType == moduleType)
            	{
					// get string variables
	
					value = proConfig->getConfig(sectionName, name);
	
					if (value.empty())
					{
						exceptionControl("getProcessConfig", API_INVALID_PARAMETER);
					}
					return;
				}
            }
        }

        // Process Not found

        exceptionControl("getProcessConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * get Process Configuration Integer Parameter value
     *
     ********************************************************************/

    void Oam::getProcessConfig(const std::string process, const std::string module, 
								const std::string name, int& value)
    {
        string returnValue;

        Oam::getProcessConfig(process, module, name, returnValue);

        value = atoi(returnValue.c_str());
    }

    /********************************************************************
     *
     * set Process Configuration String Parameter value
     *
     ********************************************************************/

    void Oam::setProcessConfig(const std::string process, const std::string module, 
								const std::string name, const std::string value)
    {

        Config* proConfig = Config::makeConfig(ProcessConfigFile.c_str());
        const string SECTION_NAME = "PROCESSCONFIG";
        string returnValue;
		string moduleType = module.substr(0,MAX_MODULE_TYPE_SIZE);

        for (int processID = 1; processID < MAX_PROCESS+1; processID++)
        {
            string sectionName = SECTION_NAME + itoa(processID);

            if( proConfig->getConfig(sectionName, "ProcessName") == process )
			{
            	string ModuleType = proConfig->getConfig(sectionName, "ModuleType");

           		if ( ModuleType == "ParentOAMModule" 
					|| ModuleType == "ChildExtOAMModule"
					|| ( ModuleType == "ChildOAMModule" && moduleType != "xm" )
					|| ModuleType == moduleType)
            	{
					// check if parameter exist
	
					Oam::getProcessConfig(process, module, name, returnValue);
	
					// Set string variables
					proConfig->setConfig(sectionName, name, value);
					try
					{
						proConfig->write();
					}
					catch(...)
					{
						exceptionControl("setProcessConfig", API_FAILURE);
					}
	
					// build and send msg to inform Proc-Mgt that Configuration is updated
					// don't care if fails, sincet his can be called with Proc-Mgr enable
					sendMsgToProcMgr(UPDATECONFIG, "", FORCEFUL, ACK_NO);
					return;
				}
            }
        }

        // Process Not found

        exceptionControl("setProcessConfig", API_INVALID_PARAMETER);
    }

    /********************************************************************
     *
     * set Process Configuration Interger Parameter value
     *
     ********************************************************************/

    void Oam::setProcessConfig(const std::string process, const std::string module, 
								const std::string name, const int value)
    {
        string valueString;

        // convert Incoming Interger Parameter value to String

        valueString = itoa(value);

        // write parameter to disk

        Oam::setProcessConfig(process, module, name, valueString);
    }

    /********************************************************************
     *
     * System Process Status information from the Process status file.
     *
     ********************************************************************/

    void Oam::getProcessStatus(SystemProcessStatus& systemprocessstatus, string port)
    {
        ProcessStatus processstatus;
        systemprocessstatus.processstatus.clear();

        try
        {
            MessageQueueClient processor(port);
			processor.syncProto(false);
            ByteStream::quadbyte processNumber;
			ByteStream::byte state;
			ByteStream::quadbyte PID;
			std::string changeDate;
			std::string processName;
			std::string moduleName;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_ALL_PROC_STATUS;

			try {
				struct timespec ts = { 3, 0 };
				processor.write(obs, &ts);
			}
			catch(...)
			{
				exceptionControl("getProcessStatus", API_TIMEOUT);
			}

			// wait 10 seconds for ACK from Process Monitor
			struct timespec ts = { 15, 0 };

			ibs = processor.read(&ts);

			if (ibs.length() > 0)
			{
				ibs >> processNumber;

				for( unsigned i=0 ; i < processNumber ; ++i)
				{
					ibs >> processName;
					ibs >> moduleName;
					ibs >> state;
					ibs >> PID;
					ibs >> changeDate;

					processstatus.ProcessName = processName;
					processstatus.Module = moduleName;
					processstatus.ProcessOpState = state;
					processstatus.ProcessID = PID;
					processstatus.StateChangeDate = changeDate;

					systemprocessstatus.processstatus.push_back(processstatus);
				}
				processor.shutdown();
				return;
			}
            // timeout occurred, shutdown connection
            processor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getProcessStatus", API_FAILURE);
		}

		exceptionControl("getProcessStatus", API_TIMEOUT);
    }

    /********************************************************************
     *
     * get Process information from the Process Status file.
     *
     ********************************************************************/

    void Oam::getProcessStatus(const std::string process, const std::string module, ProcessStatus& processstatus)
    {
#ifdef _MSC_VER
        // TODO: Remove when we create OAM for Windows
        return;
#endif
        try
        {
            MessageQueueClient processor("ProcStatusControl");
			processor.syncProto(false);
			ByteStream::byte status, state;
			ByteStream::quadbyte PID;
			std::string changeDate;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_PROC_STATUS;
            obs << module;
            obs << process;

			try {
				struct timespec ts = { 3, 0 };
				processor.write(obs, &ts);
			}
			catch(...)
			{
				processor.shutdown();
				exceptionControl("getProcessStatus", API_TIMEOUT);
			}

			// wait 10 seconds for ACK from Process Monitor
			struct timespec ts = { 15, 0 };

			ibs = processor.read(&ts);

			if (ibs.length() > 0)
			{
				ibs >> status;
				if ( status == oam::API_SUCCESS ) {
					ibs >> state;
					ibs >> PID;
					ibs >> changeDate;
				}
				else
				{
					// shutdown connection
					processor.shutdown();
					exceptionControl("getProcessStatus", API_FAILURE);
				}

				processstatus.ProcessName = process;
				processstatus.Module = module;
				processstatus.ProcessOpState = state;
				processstatus.ProcessID = PID;
				processstatus.StateChangeDate = changeDate;

				processor.shutdown();
				return;
			}
            // timeout occurred, shutdown connection
            processor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getProcessStatus", API_FAILURE);
		}

        exceptionControl("getProcessStatus", API_TIMEOUT);

    }


    /********************************************************************
     *
     * set Process Status String Parameter from the Process Status file.
     *
     ********************************************************************/

    void Oam::setProcessStatus(const std::string process, const std::string module, const int state, pid_t PID)
    {
		//send and wait for ack and resend if not received
		//retry 5 time max
		for ( int i=0; i < 5 ; i++)
		{
			try
			{
				ByteStream obs;
	
				obs << (ByteStream::byte) SET_PROC_STATUS;
				obs << module;
				obs << process;
				obs << (ByteStream::byte) state;
				obs << (ByteStream::quadbyte) PID;
	
	    		sendStatusUpdate(obs, SET_PROC_STATUS);
				return;
			}
			catch(...)
			{}
#ifdef _MSC_VER
			Sleep(1 * 1000);
#else
			sleep(1);
#endif
		}
        exceptionControl("setProcessStatus", API_TIMEOUT);
    }

    /********************************************************************
     *
     * Process Initization Successful Completed, Mark Process ACTIVE
     *
     ********************************************************************/

    void Oam::processInitComplete(std::string processName, int state)
	{
//This method takes too long on Windows and doesn't do anything there anyway...
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
		// get current Module name
		string moduleName;
		oamModuleInfo_t st;
		for ( int i = 0 ; i < 5 ; i++)
		{
			try {
				st = getModuleInfo();
				moduleName = boost::get<0>(st);
			}
			catch (...) {
				//system("touch /var/log/Calpont/test2");
			}
	
			//set process
			try
			{
				setProcessStatus(processName, moduleName, state, getpid());
				return;
			}
			catch(...)
			{
				//system("touch /var/log/Calpont/test3");
			}
		}
		exceptionControl("processInitComplete", API_FAILURE);
#endif
	}

    /********************************************************************
     *
     * Process Initization Failed, Mark Process FAILED
     *
     ********************************************************************/

    void Oam::processInitFailure()
	{
		// get current process name
		string processName;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
       		exceptionControl("processInitFailure", API_FAILURE);
		}

		// get current Module name
		string moduleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			moduleName = boost::get<0>(st);
		}
		catch (...) {
       		exceptionControl("processInitFailure", API_FAILURE);
		}

		//set process to FAILED
        try
        {
			setProcessStatus(processName, moduleName, FAILED, 0);
        }
        catch(...)
        {
        	exceptionControl("processInitFailure", API_FAILURE);
		}

		//set MODULE to FAILED
        try
        {
			setModuleStatus(moduleName, FAILED);
        }
        catch(...)
        {
        	exceptionControl("processInitFailure", API_FAILURE);
		}
	}

    /********************************************************************
     *
     * get Alarm Configuration Information by Alarm ID
     *
     ********************************************************************/

    void Oam::getAlarmConfig(const int alarmid, AlarmConfig& alarmconfig)
    {

        Config* alaConfig = Config::makeConfig(AlarmConfigFile.c_str());
        string temp;
        string Section = "AlarmConfig";

        // validate Alarm ID

        if( alarmid > MAX_ALARM_ID )
            exceptionControl("getAlarmConfig", API_INVALID_PARAMETER);

        // convert Alarm ID to ASCII

        Section.append(itoa(alarmid));

        // get string variables

        temp = alaConfig->getConfig(Section, "AlarmID");

        if( temp.empty())
        {
            exceptionControl("getAlarmConfig", API_INVALID_PARAMETER);
        }

        alarmconfig.BriefDesc = alaConfig->getConfig(Section, "BriefDesc");
        alarmconfig.DetailedDesc = alaConfig->getConfig(Section, "DetailedDesc");

        // get numberic variables

        alarmconfig.AlarmID = strtol(alaConfig->getConfig(Section, "alarmid").c_str(), 0, 0);
        alarmconfig.Severity = strtol(alaConfig->getConfig(Section, "Severity").c_str(), 0, 0);
        alarmconfig.Threshold = strtol(alaConfig->getConfig(Section, "Threshold").c_str(), 0, 0);
        alarmconfig.Occurrences = strtol(alaConfig->getConfig(Section, "Occurrences").c_str(), 0, 0);
        alarmconfig.LastIssueTime = strtol(alaConfig->getConfig(Section, "LastIssueTime").c_str(), 0, 0);

    }

    /********************************************************************
     *
     * get Alarm Configuration String Parameter value
     *
     ********************************************************************/

    void Oam::getAlarmConfig(const int alarmid, const std::string name, std::string& value)
    {

        Config* alaConfig = Config::makeConfig(AlarmConfigFile.c_str());
        string Section = "AlarmConfig";

        // validate Alarm ID

        if( alarmid > MAX_ALARM_ID )
            exceptionControl("getSystemConfig", API_INVALID_PARAMETER);

        // convert Alarm ID to ASCII

        Section.append(itoa(alarmid));

        // get string variables

        value = alaConfig->getConfig(Section, name);

        if (value.empty())
        {
            exceptionControl("getSystemConfig", API_INVALID_PARAMETER);
        }
    }

    /********************************************************************
     *
     * get Alarm Configuration Integer Parameter value
     *
     ********************************************************************/

    void Oam::getAlarmConfig(const int alarmid, const std::string name, int& value)
    {
        string returnValue;

        // get string variables

        Oam::getAlarmConfig(alarmid, name, returnValue);

        value = atoi(returnValue.c_str());
    }

    /********************************************************************
     *
     * set Alarm Configuration String Parameter value by Alarm ID
     *
     ********************************************************************/

    void Oam::setAlarmConfig(const int alarmid, const std::string name, const std::string value)
    {
        string Section = "AlarmConfig";
        int returnValue;

        // validate Alarm ID

        if( alarmid > MAX_ALARM_ID )
            exceptionControl("setAlarmConfig", API_INVALID_PARAMETER);

        // convert Alarm ID to ASCII

        Section.append(itoa(alarmid));

        // check if parameter exist

        Oam::getAlarmConfig(alarmid, name, returnValue);

		// only allow user to change these levels
		if ( name != "Threshold" && 
				name != "Occurrences" &&
				name != "LastIssueTime" )
            exceptionControl("setAlarmConfig", API_READONLY_PARAMETER);

        //  write parameter to disk

        Config* alaConfig = Config::makeConfig(AlarmConfigFile.c_str());
        alaConfig->setConfig(Section, name, value);
		try
		{
			alaConfig->write();
		}
		catch(...)
		{
			exceptionControl("setAlarmConfig", API_FAILURE);
		}

    }

    /********************************************************************
     *
     * set Alarm Configuration Interger Parameter value by Alarm ID
     *
     ********************************************************************/

    void Oam::setAlarmConfig(const int alarmid, const std::string name, const int value)
    {
        string Section = "AlarmConfig";
        string valueString;

        // convert Incoming Interger Parameter value to String

        valueString = itoa(value);

        //  write parameter to disk

        Oam::setAlarmConfig(alarmid, name, valueString);
    }

    /********************************************************************
     *
     * get Active Alarm List
     *
     ********************************************************************/

    void Oam::getActiveAlarms(AlarmList& activeAlarm)
    {
        // check if on Active OAM Parent
        bool OAMParentModuleFlag;
        oamModuleInfo_t st;
        try {
            st = getModuleInfo();
            OAMParentModuleFlag = boost::get<4>(st);

            if (OAMParentModuleFlag) {
                //call getAlarm API directly
                SNMPManager sm;
                sm.getActiveAlarm(activeAlarm);
                return;
            }
        }
        catch (...) {
            exceptionControl("getActiveAlarms", API_FAILURE);
        }

        int returnStatus = API_SUCCESS;
        if (UseHdfs > 0)
        {
            // read from HDFS files
            returnStatus = readHdfsActiveAlarms(activeAlarm);
        }
        else
        {
            // build and send msg
            returnStatus = sendMsgToProcMgr3(GETACTIVEALARMDATA, activeAlarm, "");
        }

        if (returnStatus != API_SUCCESS)
            exceptionControl("getActiveAlarms", returnStatus);
    }

    /********************************************************************
     *
     * get Historical Alarm List
     *
     ********************************************************************/

    void Oam::getAlarms(std::string date, AlarmList& alarmlist)
    {
        // check if on Active OAM Parent
        bool OAMParentModuleFlag;
        oamModuleInfo_t st;
        try {
            st = getModuleInfo();
            OAMParentModuleFlag = boost::get<4>(st);

            if (OAMParentModuleFlag) {
                //call getAlarm API directly
                SNMPManager sm;
                sm.getAlarm(date, alarmlist);
                return;
            }
        }
        catch (...) {
            exceptionControl("getAlarms", API_FAILURE);
        }

        // build and send msg
        int returnStatus = sendMsgToProcMgr3(GETALARMDATA, alarmlist, date);
        if (returnStatus != API_SUCCESS)
            exceptionControl("getAlarms", returnStatus);
    }

    /********************************************************************
     *
     * check Active Alarm
     *
     ********************************************************************/

    bool Oam::checkActiveAlarm(const int alarmid, const std::string moduleName,
                               const std::string deviceName)
    {
        AlarmList activeAlarm;

        // check if on Active OAM Parent
        bool OAMParentModuleFlag;
        oamModuleInfo_t st;
        try {
            st = getModuleInfo();
            OAMParentModuleFlag = boost::get<4>(st);

            if (OAMParentModuleFlag) {
                //call getAlarm API directly
                SNMPManager sm;
                sm.getActiveAlarm(activeAlarm);
            }
            else if (UseHdfs > 0)
            {
                // read from HDFS files
                if (readHdfsActiveAlarms(activeAlarm) != API_SUCCESS)
                    return false;
            }
            else
            {
                // build and send msg
                int returnStatus = sendMsgToProcMgr3(GETACTIVEALARMDATA, activeAlarm, "");
                if (returnStatus != API_SUCCESS)
                    return false;
            }
        }
        catch (...) {
            return false;
        }

        for (AlarmList::iterator i = activeAlarm.begin(); i != activeAlarm.end(); ++i)
        {
            // check if matching ID
            if (alarmid != (i->second).getAlarmID() )
                continue;

        	//check for moduleName of wildcard "*", if so return if alarm set on any module
            if (deviceName.compare((i->second).getComponentID()) == 0 &&
                moduleName == "*")
                return true;

            // check if the same fault component on same Module
            if (deviceName.compare((i->second).getComponentID()) == 0 &&
                moduleName.compare((i->second).getSname()) == 0)
                return true;
        }
        return false;
    }

    /********************************************************************
     *
     * get Local Module Information from Local Module Configuration file
	 *
	 *   Returns: Local Module Name, Local Module Type, Local Module ID, 
	 *					OAM Parent Module Name, and OAM Parent Flag
     *
     ********************************************************************/

    oamModuleInfo_t Oam::getModuleInfo()
    {
        string localModule;
        string localModuleType;
        int localModuleID;

		// Get Module Name from module-file
		string fileName = InstallDir + "/local/module";
	
		ifstream oldFile (fileName.c_str());
		
		char line[400];
		while (oldFile.getline(line, 400))
		{
			localModule = line;
			break;
		}
		oldFile.close();

		if (localModule.empty() ) {
			// not found
			//system("touch /var/log/Calpont/test8");
       		exceptionControl("getModuleInfo", API_FAILURE);
		}

		localModuleType = localModule.substr(0,MAX_MODULE_TYPE_SIZE);
		localModuleID = atoi(localModule.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

		// Get Parent OAM Module name
	
		string ParentOAMModule = oam::UnassignedName;
		string StandbyOAMModule = oam::UnassignedName;
        bool parentOAMModuleFlag = false;
        bool standbyOAMModuleFlag = false;
		int serverTypeInstall = 1;

		try {
			Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
			string Section = "SystemConfig";
			ParentOAMModule = sysConfig->getConfig(Section, "ParentOAMModuleName");
			StandbyOAMModule = sysConfig->getConfig(Section, "StandbyOAMModuleName");
	
			if ( localModule == ParentOAMModule )
				parentOAMModuleFlag = true;

			if ( localModule == StandbyOAMModule )
				standbyOAMModuleFlag = true;

			// Get Server Type Install ID
		
			serverTypeInstall = atoi(sysConfig->getConfig("Installation", "ServerTypeInstall").c_str());
		}
		catch (...) {}

        return boost::make_tuple(localModule, localModuleType, localModuleID, ParentOAMModule, parentOAMModuleFlag, serverTypeInstall, StandbyOAMModule, standbyOAMModuleFlag);
    }

    /********************************************************************
     *
     * get My Process Status from Process Status file
     *
     ********************************************************************/

    myProcessStatus_t Oam::getMyProcessStatus(pid_t processID)
    {
        string returnValue;
		ByteStream::quadbyte pid;

		if ( processID == 0 )
        	// get current process PID
        	pid = getpid();
		else
			pid = processID;

		// get process current Module
		string moduleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			moduleName = boost::get<0>(st);
		}
		catch (...) {
			//system("touch /var/log/Calpont/test4");
       		exceptionControl("getMyProcessStatus", API_FAILURE);
		}

        try
        {
            MessageQueueClient processor("ProcStatusControl");
			processor.syncProto(false);
			ByteStream::byte status, state;
			std::string processName;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_PROC_STATUS_BY_PID;
            obs << moduleName;
            obs << pid;

			try
			{
				struct timespec ts1 = { 3, 0 };
				processor.write(obs, &ts1);
	
				// wait 10 seconds for ACK from Process Monitor
				struct timespec ts = { 10, 0 };

				try
				{	
					ibs = processor.read(&ts);
		
					if (ibs.length() > 0)
					{
						ibs >> status;
						if ( status == oam::API_SUCCESS ) {
							ibs >> state;
							ibs >> processName;
						}
						else
						{
							// shutdown connection
							processor.shutdown();
							//system("touch /var/log/Calpont/test5");
							exceptionControl("getMyProcessStatus", API_FAILURE);
						}
		
						// shutdown connection
						processor.shutdown();
		
						return boost::make_tuple((pid_t) pid, processName, state);
					}
				}
				catch(...)
				{
					//system("touch /var/log/Calpont/test6");
					processor.shutdown();
					exceptionControl("getMyProcessStatus", API_INVALID_PARAMETER);
				}
			}
			catch(...)
			{
				//system("touch /var/log/Calpont/test7");
				processor.shutdown();
				exceptionControl("getMyProcessStatus", API_INVALID_PARAMETER);
			}

            // timeout occurred, shutdown connection
            processor.shutdown();
        }
        catch(...)
        {
			//system("touch /var/log/Calpont/test8");
        	exceptionControl("getMyProcessStatus", API_INVALID_PARAMETER);
		}

		//system("touch /var/log/Calpont/test9");
        exceptionControl("getMyProcessStatus", API_TIMEOUT);

        return boost::make_tuple(-1, "", -1);
    }

    /********************************************************************
     *
     * Stop Module
     *
     ********************************************************************/

    void Oam::stopModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("stopModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(STOPMODULE, devicenetworklist, gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("stopModule", returnStatus);
	}

    /********************************************************************
     *
     * Shutdown Module - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::shutdownModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("shutdownModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(SHUTDOWNMODULE, devicenetworklist, gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("shutdownModule", returnStatus);
	}

    /********************************************************************
     *
     * Start Module - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::startModule(DeviceNetworkList devicenetworklist, ACK_FLAG ackflag)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("startModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(STARTMODULE, devicenetworklist, FORCEFUL, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("startModule", returnStatus);
	}

    /********************************************************************
     *
     * Restart Module - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::restartModule(DeviceNetworkList devicenetworklist, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("restartModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(RESTARTMODULE, devicenetworklist, gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("restartModule", returnStatus);
	}

    /********************************************************************
     *
     * Disable Module - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::disableModule(DeviceNetworkList devicenetworklist)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("disableModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(DISABLEMODULE, devicenetworklist, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("disableModule", returnStatus);
	}

    /********************************************************************
     *
     * Enable Module - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::enableModule(DeviceNetworkList devicenetworklist)
	{
		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			// validate Module name
			int returnStatus = validateModule((*pt).DeviceName);
			if (returnStatus != API_SUCCESS)
				exceptionControl("enableModule", returnStatus);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr2(ENABLEMODULE, devicenetworklist, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("enableModule", returnStatus);
	}

    /********************************************************************
     *
     * Stop System - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::stopSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
        // build and send msg
		int returnStatus = sendMsgToProcMgrWithStatus(STOPSYSTEM, "stopped", gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("stopSystem", returnStatus);
    }

    /********************************************************************
     *
     * Shutdown System - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::shutdownSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
		int returnStatus = sendMsgToProcMgrWithStatus(SHUTDOWNSYSTEM, "shutdown", gracefulflag, ackflag);

		//Wait for everything to settle down
		sleep(10);

		switch (returnStatus)
		{ 
			case API_SUCCESS:
				cout << endl << "   Successful shutdown of System " << endl << endl;
				break;
			case API_CANCELLED:
				cout << endl << "   Shutdown of System canceled" << endl << endl;
				break;
			default:
				exceptionControl("shutdownSystem", returnStatus);
				break;
		}
    }

	/********************************************************************
	 *
	 * Suspend Database Writes - build and send message to Process Manager
	 *
	 ********************************************************************/

	void Oam::SuspendWrites(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
	{
		SystemProcessStatus systemprocessstatus;

		// Send the message to suspend and wait for it to finish
		int returnStatus = sendMsgToProcMgrWithStatus(SUSPENDWRITES, "write suspended", gracefulflag, ackflag);

		// An error throws here.
		switch (returnStatus)
		{ 
			case API_SUCCESS:
				cout << endl << "Suspend Calpont Database Writes Request successfully completed" << endl;
				break;
			case API_FAILURE_DB_ERROR:
				cout << endl << "**** stopDatabaseWrites Failed: save_brm Failed" << endl;
				break;
			case API_CANCELLED:
				cout << endl << "   Suspension of database writes canceled" << endl << endl;
				break;
			default:
				exceptionControl("suspendWrites", returnStatus);
				break;
		}
	}

    /********************************************************************
     *
     * Start System - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::startSystem(ACK_FLAG ackflag)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(STARTSYSTEM, "", FORCEFUL, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("startSystem", returnStatus);
    }

    /********************************************************************
     *
     * Restart System - build and send message to Process Manager
     *
     ********************************************************************/

    int Oam::restartSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
		// Send the restart message (waits for completion)
		int returnStatus = sendMsgToProcMgrWithStatus(RESTARTSYSTEM, "restarted", gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS && returnStatus != API_CANCELLED)
        {
            exceptionControl("restartSystem", returnStatus);
        }
        return returnStatus;
    }

    /********************************************************************
     *
     * Stop Process - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::stopProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
        // validate Process name
        int returnStatus = validateProcess(moduleName, processName);
        if (returnStatus != API_SUCCESS)
            exceptionControl("stopProcess", returnStatus);

		// validate Process Name, don't allow Process-Monitor / Process-Manager

		if ( processName == "ProcessMonitor" || processName == "ProcessManager" )
            exceptionControl("stopProcess", API_INVALID_PARAMETER);
		
		// validate Process Name, don't allow COLD-STANDBY process
		ProcessStatus procstat;
		getProcessStatus(processName, moduleName, procstat);
		if ( procstat.ProcessOpState == oam::COLD_STANDBY )
			exceptionControl("stopProcess", API_INVALID_STATE);

        // build and send msg
        returnStatus = sendMsgToProcMgr(STOPPROCESS, processName, gracefulflag, ackflag, moduleName);

        if (returnStatus != API_SUCCESS)
            exceptionControl("stopProcess", returnStatus);
    }

    /********************************************************************
     *
     * Start Process - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::startProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
        // validate Process name
        int returnStatus = validateProcess(moduleName, processName);
        if (returnStatus != API_SUCCESS)
            exceptionControl("startProcess", returnStatus);

		// validate Process Name, don't allow COLD-STANDBY process
//		ProcessStatus procstat;
//		getProcessStatus(processName, moduleName, procstat);
//		if ( procstat.ProcessOpState == oam::COLD_STANDBY )
//			exceptionControl("startProcess", API_INVALID_STATE);

        // build and send msg
        returnStatus = sendMsgToProcMgr(STARTPROCESS, processName, gracefulflag, ackflag, moduleName);

        if (returnStatus != API_SUCCESS)
            exceptionControl("startProcess", returnStatus);
    }

    /********************************************************************
     *
     * Restart Process - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::restartProcess(const std::string moduleName, const std::string processName, GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
        // validate Process name
        int returnStatus = validateProcess(moduleName, processName);
        if (returnStatus != API_SUCCESS)
             exceptionControl("restartProcess", returnStatus);

        // build and send msg
        returnStatus = sendMsgToProcMgr(RESTARTPROCESS, processName, gracefulflag, ackflag, moduleName);

        if (returnStatus != API_SUCCESS)
            exceptionControl("restartProcess", returnStatus);
    }

    /********************************************************************
     *
     * Stop Process - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::stopProcessType(std::string type)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(STOPPROCESSTYPE, type);

        if (returnStatus != API_SUCCESS)
            exceptionControl("stopProcessType", returnStatus);
    }

    /********************************************************************
     *
     * Start Processes - build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::startProcessType(std::string type)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(STARTPROCESSTYPE, type);

        if (returnStatus != API_SUCCESS)
            exceptionControl("startProcessType", returnStatus);
    }

    /********************************************************************
     *
     * Restart Process Type- build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::restartProcessType(std::string type)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(RESTARTPROCESSTYPE, type);

        if (returnStatus != API_SUCCESS)
            exceptionControl("restartProcessType", returnStatus);
    }

    /********************************************************************
     *
     * Reinit Process Type- build and send message to Process Manager
     *
     ********************************************************************/

    void Oam::reinitProcessType(std::string type)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(REINITPROCESSTYPE, type, FORCEFUL);
 
        if (returnStatus != API_SUCCESS)
            exceptionControl("reinitProcessType", returnStatus);
    }

    /********************************************************************
     *
     * Update Logging - Enable/Disable Logging with the system or on a specific
     *                  Module at a specific level
     *
     ********************************************************************/
    void Oam::updateLog(const std::string action, const std::string deviceid, const std::string loglevel)
    {
        // validate the loglevel
        for( int i = 0;;i++)
        {
            if ( LogLevel[i] == "" )
            {
                // end of section list
                exceptionControl("updateLog", API_INVALID_PARAMETER);
            }
            if ( loglevel == LogLevel[i] )
            {
                // build and send msg
                int returnStatus = sendMsgToProcMgr(UPDATELOG, deviceid, FORCEFUL, ACK_YES, action, loglevel);
                if (returnStatus != API_SUCCESS)
                    exceptionControl("updateLog", returnStatus);

                return;
            }
        }
    }

    /********************************************************************
     *
     * Get Log File - Get Log file location for specific Module at a specific level
     *
     ********************************************************************/
    void Oam::getLogFile(const std::string moduleName, const std::string loglevel, std::string& filelocation)
    {
        // validate Module name
        int returnStatus = validateModule(moduleName);
        if (returnStatus != API_SUCCESS)
            exceptionControl("getLogFile", returnStatus);

        string path;

		// Get Parent OAM Module name
	
		Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";
        string ParentOAMModule = sysConfig->getConfig(Section, "ParentOAMModuleName");

		if (moduleName == ParentOAMModule)
			path = "//";
		else
			path = "/mnt/" + moduleName;

        // get log file name for level
        string logFile;
        for( int i = 0;;i++)
        {
            if ( LogLevel[i] == "" )
            {
                // end of list
                exceptionControl("getLogFile", API_INVALID_PARAMETER);
                break;
            }
            if ( loglevel == LogLevel[i] )
            {
                // match found, get and strip off to '/'
                logFile = LogFile[i];
                string::size_type pos = logFile.find('/',0);
                if (pos != string::npos)
                {
                    logFile = logFile.substr(pos,200);
                    break;
                }
            }
        }

        filelocation = path + logFile;

    }

    /********************************************************************
     *
     * Get Log File - Get Log file location for specific Module at a specific level
     *
     ********************************************************************/
    void Oam::getLogFile(const std::string moduleName, const std::string loglevel, const std::string date, 
							std::string& filelocation)
    {
        // validate Module name
        int returnStatus = validateModule(moduleName);
        if (returnStatus != API_SUCCESS)
            exceptionControl("getLogFile", returnStatus);

        string path;

		// Get Parent OAM Module name
	
		Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";
        string ParentOAMModule = sysConfig->getConfig(Section, "ParentOAMModuleName");

		if (moduleName == ParentOAMModule)
			path = "/";
		else
			path = "/mnt/" + moduleName;

        // get log file name for level
        string logFile;
		string logFileName;
        for( int i = 0;;i++)
        {
            if ( LogLevel[i] == "" )
            {
                // end of list
                exceptionControl("getLogFile", API_INVALID_PARAMETER);
                break;
            }
            if ( loglevel == LogLevel[i] )
            {
                // match found, get and strip off to '/'
                logFile = LogFile[i];
                string::size_type pos = logFile.find('/',0);
                if (pos != string::npos)
                {
                    logFile = logFile.substr(pos,200);

					pos = logFile.rfind('/',200);
					logFileName = logFile.substr(pos+1,200);
                    break;
                }
            }
        }

		logFile = path + logFile;

		string tempLogFile = "/tmp/logs";
	
		//make 1 log file made up of archive and current *.log
		(void)system("touch /tmp/logs");
	
		string logdir("/var/log/Calpont");
		if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
		string cmd = "ls " + path + logdir + "/archive | grep '" + logFileName + "' > /tmp/logfiles";
		(void)system(cmd.c_str());
	
		string fileName = "/tmp/logfiles";
	
		ifstream oldFile (fileName.c_str());
		if (oldFile) {
			char line[400];
			string buf;
			while (oldFile.getline(line, 400))
			{
				buf = line;
				cmd = "cat " + path + logdir + "/archive/" + buf + " >> /tmp/logs";
				(void)system(cmd.c_str());
			}
		
			oldFile.close();
			unlink (fileName.c_str());
		}
	
		cmd = "cat " + logFile + " >> /tmp/logs";
		(void)system(cmd.c_str());
	
		//validate and get mm / dd from incoming date
		if ( date.substr(2,1) != "/" )
            exceptionControl("getLogFile", oam::API_INVALID_PARAMETER);

		string dd = date.substr(3,2);
		if (dd.substr(0,1) == "0" )
			dd = " " + dd.substr(1,1);

		int mmName = atoi(date.substr(0,2).c_str());
		string mm;

		switch ( mmName ) {
			case (1):
			{
				mm = "Jan";
				break;
			}
			case (2):
			{
				mm = "Feb";
				break;
			}
			case (3):
			{
				mm = "Mar";
				break;
			}
			case (4):
			{
				mm = "Apr";
				break;
			}
			case (5):
			{
				mm = "May";
				break;
			}
			case (6):
			{
				mm = "Jun";
				break;
			}
			case (7):
			{
				mm = "Jul";
				break;
			}
			case (8):
			{
				mm = "Aug";
				break;
			}
			case (9):
			{
				mm = "Sep";
				break;
			}
			case (10):
			{
				mm = "Oct";
				break;
			}
			case (11):
			{
				mm = "Nov";
				break;
			}
			case (12):
			{
				mm = "Dec";
				break;
			}
			default:
			{
				filelocation = "";
				return;
			}
		}
	
		string findDate = mm + " " + dd;
	
		ifstream file (tempLogFile.c_str());
		vector <string> lines;
	
		if (file) {
			char line[400];
			string buf;
			while (file.getline(line, 400))
			{
				buf = line;
				string::size_type pos = buf.find(findDate,0);
				if (pos != string::npos)
					lines.push_back(buf);
			}
		
			unlink (tempLogFile.c_str());
		}
	
		fileName = "/tmp/logsByDate";
		ofstream newFile (fileName.c_str());	
		
		//create new file
		int fd = open(fileName.c_str(),O_RDWR|O_CREAT, 0666);
		
		copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
		newFile.close();
		
		close(fd);
	
		filelocation = fileName;
    }

    /********************************************************************
     *
     * Get Log Config - Get Log Config data, which is the File IDs in the
     *                  Module syslog.conf file
     *
     ********************************************************************/
    void Oam::getLogConfig(SystemLogConfigData& configdata )
    {
        SystemModuleTypeConfig systemmoduletypeconfig;
        LogConfigData logconfigdata;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getLogConfig", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;

				int returnStatus = sendMsgToProcMgr(GETCONFIGLOG,
									moduleName,
									FORCEFUL,
									ACK_YES);
	
				logconfigdata.moduleName = moduleName;
				logconfigdata.configData = returnStatus;
	
				configdata.push_back(logconfigdata);
			}
        }
    }

    /******************************************************************************************
     * @brief	DisplayLockedTables
     *
     * purpose:	Show the details of all the locks in tableLocks
     *          Used when attempting to suspend or stop the
     *          database, but there are table locks.
     *
     ******************************************************************************************/
    void Oam::DisplayLockedTables(std::vector<BRM::TableLockInfo>& tableLocks, BRM::DBRM* pDBRM)
    {
        cout << "The following tables are locked:" << endl;

        // Initial widths of columns to display. We pass thru the table
        // and see if we need to grow any of these.
        unsigned int lockIDColumnWidth    = 6;  // "LockID"
        unsigned int tableNameColumnWidth = 12; // "Name"
        unsigned int ownerColumnWidth     = 7;  // "Process"
        unsigned int pidColumnWidth       = 3;  // "PID"
        unsigned int sessionIDColumnWidth = 7;  // "Session"
		unsigned int createTimeColumnWidth= 12; // "CreationTime"
		unsigned int dbrootColumnWidth        = 7;  // "DBRoots"
		unsigned int stateColumnWidth     = 9;  // "State"

        // Initialize System Catalog object used to get table name
        boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
            execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0);

        std::string fullTblName;
		const char* tableState;

        // Make preliminary pass through the table locks in order to determine our
        // output column widths based on the data.  Min column widths are based on
        // the width of the column heading (except for the 'state' column).
        uint64_t maxLockID                = 0;
        uint32_t maxPID                   = 0;
        int32_t  maxSessionID             = 0;
        int32_t  minSessionID             = 0;
		std::vector<std::string> createTimes;
		std::vector<std::string> pms;
		char cTimeBuffer[64];

        execplan::CalpontSystemCatalog::TableName tblName;

        for (unsigned idx=0; idx<tableLocks.size(); idx++)
        {
            if (tableLocks[idx].id > maxLockID)
            {
                maxLockID = tableLocks[idx].id;
            }
            try
            {
                tblName = systemCatalogPtr->tableName(tableLocks[idx].tableOID);
            }
            catch (...)
            {
                tblName.schema.clear();
                tblName.table.clear();
            }
            fullTblName = tblName.toString();
            if (fullTblName.size() > tableNameColumnWidth)
            {
                tableNameColumnWidth = fullTblName.size();
            }
            if (tableLocks[idx].ownerName.length() > ownerColumnWidth)
            {
                ownerColumnWidth = tableLocks[idx].ownerName.length();
            }
            if (tableLocks[idx].ownerPID > maxPID)
            {
                maxPID = tableLocks[idx].ownerPID;
            }
            if (tableLocks[idx].ownerSessionID > maxSessionID)
            {
                maxSessionID = tableLocks[idx].ownerSessionID;
            }
            if (tableLocks[idx].ownerSessionID < minSessionID)
            {
                minSessionID = tableLocks[idx].ownerSessionID;
            }
			// Creation Time.
			// While we're at it, we save the time string off into a vector
			// so we can display it later without recalcing it.
			struct tm timeTM;
			localtime_r(&tableLocks[idx].creationTime,&timeTM);
			ctime_r(&tableLocks[idx].creationTime, cTimeBuffer);
			strftime(cTimeBuffer, 64, "%F %r:", &timeTM);
			cTimeBuffer[strlen(cTimeBuffer)-1] = '\0'; // strip trailing '\n'
			std::string cTimeStr( cTimeBuffer );
			if (cTimeStr.length() > createTimeColumnWidth)
			{
				createTimeColumnWidth = cTimeStr.length();
			}
			createTimes.push_back(cTimeStr);
        }
        tableNameColumnWidth  += 1;
        ownerColumnWidth      += 1;
		createTimeColumnWidth += 1;

        std::ostringstream idString;
        idString << maxLockID;
        if (idString.str().length() > lockIDColumnWidth)
            lockIDColumnWidth = idString.str().length();
        lockIDColumnWidth += 1;

        std::ostringstream pidString;
        pidString << maxPID;
        if (pidString.str().length() > pidColumnWidth)
            pidColumnWidth = pidString.str().length();
        pidColumnWidth += 1;

        const std::string sessionNoneStr("BulkLoad");
        std::ostringstream sessionString;
        sessionString << maxSessionID;
        if (sessionString.str().length() > sessionIDColumnWidth)
            sessionIDColumnWidth = sessionString.str().length();
        if ((minSessionID < 0) &&
            (sessionNoneStr.length() > sessionIDColumnWidth))
            sessionIDColumnWidth = sessionNoneStr.length();
        sessionIDColumnWidth += 1;

        // write the column headers before the first entry
        cout.setf(ios::left, ios::adjustfield);
        cout << setw(lockIDColumnWidth)     << "LockID"       <<
                setw(tableNameColumnWidth)  << "Name"         <<
                setw(ownerColumnWidth)      << "Process"      <<
                setw(pidColumnWidth)        << "PID"          <<
                setw(sessionIDColumnWidth)  << "Session"      <<
				setw(createTimeColumnWidth) << "CreationTime" <<
                setw(stateColumnWidth)      << "State"        <<
				setw(dbrootColumnWidth)     << "DBRoots"      << endl;

        for (unsigned idx=0; idx<tableLocks.size(); idx++)
        {
            try
            {

                tblName = systemCatalogPtr->tableName(tableLocks[idx].tableOID);
            }
            catch(...)
            {
                tblName.schema.clear();
                tblName.table.clear();
            }
            fullTblName = tblName.toString();
            cout << 
                setw(lockIDColumnWidth)    << tableLocks[idx].id         <<
                setw(tableNameColumnWidth) << fullTblName                <<
                setw(ownerColumnWidth)     << tableLocks[idx].ownerName  <<
                setw(pidColumnWidth)       << tableLocks[idx].ownerPID;

			// Log session ID, or "BulkLoad" if session is -1
			if (tableLocks[idx].ownerSessionID < 0)
				cout << setw(sessionIDColumnWidth) << sessionNoneStr;
			else
				cout << setw(sessionIDColumnWidth) <<
					tableLocks[idx].ownerSessionID;

			// Creation Time
			cout << setw(createTimeColumnWidth) << createTimes[idx];

			// Processor State
			if (pDBRM && !pDBRM->checkOwner(tableLocks[idx].id))
			{
				tableState = "Abandoned";
			}
			else
			{
				tableState = ((tableLocks[idx].state==BRM::LOADING) ?
					"LOADING" : "CLEANUP");
			}
			cout << setw(stateColumnWidth) << tableState;

			// PM List
			cout << setw(dbrootColumnWidth);
			for (unsigned k=0; k<tableLocks[idx].dbrootList.size(); k++)
			{
				if (k > 0)
					cout << ',';
				cout << tableLocks[idx].dbrootList[k];
			}
			cout << endl;
        } // end of loop through table locks
    }

    /******************************************************************************************
     * @brief	getCurrentTime
     *
     * purpose:	get time/date in string format
     *
     ******************************************************************************************/
    string Oam::getCurrentTime()
    {
        time_t cal;
        time (&cal);
        string stime;
        char ctime[26];
        ctime_r (&cal, ctime);
		stime = ctime;
//        string stime = ctime_r (&cal);
        // strip off cr/lf
        stime = stime.substr (0,24);
        return stime;
    }

    /******************************************************************************************
     * @brief	Get Local DBRM ID
     *
     * purpose:	Get Local DBRM ID for Module
     *
     ******************************************************************************************/
	int Oam::getLocalDBRMID(const std::string moduleName)
	{
		string cmd = "touch " + CalpontConfigFile;
		(void)system(cmd.c_str());

        string SECTION = "DBRM_Worker";

        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

		int numWorker = atoi(sysConfig->getConfig("DBRM_Controller", "NumWorkers").c_str());
        for (int workerID = 1; workerID < numWorker+1; workerID++)
        {
            string section = SECTION + itoa(workerID);

            if( sysConfig->getConfig(section, "Module") == moduleName )
				return workerID;
        }
		// not found
        exceptionControl("getLocalDBRMID", API_INVALID_PARAMETER);
		return -1;
    }

    /******************************************************************************************
     * @brief	build empty set of System Tables
     *
     * purpose:	build empty set of System Tables
     *
     ******************************************************************************************/
	void Oam::buildSystemTables()
	{
		//determine active PM (DDLProc is ACTIVE) to send request to
		SystemProcessStatus systemprocessstatus;
		string PMmodule;
		int returnStatus = API_FAILURE;
		try
		{
			getProcessStatus(systemprocessstatus);
	
			for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
			{
				if ( systemprocessstatus.processstatus[i].ProcessName == "DDLProc" &&
						systemprocessstatus.processstatus[i].ProcessOpState == oam::ACTIVE) {
					PMmodule = systemprocessstatus.processstatus[i].Module;

					// build and send msg
					returnStatus = sendMsgToProcMgr(BUILDSYSTEMTABLES, PMmodule, FORCEFUL, ACK_YES);
				}
			}
		}
        catch(...)
		{
        	exceptionControl("buildSystemTables", API_FAILURE);
		}

		if (returnStatus != API_SUCCESS)
			exceptionControl("buildSystemTables", returnStatus);
		else
			return;
	}

    /******************************************************************************************
     * @brief	Get Network IP Address for Host Name
     *
     * purpose:	Get Network IP Address for Host Name
     *
     ******************************************************************************************/
	string Oam::getIPAddress(string hostName)
	{
		static u_long my_bind_addr;
		struct hostent *ent;
		string IPAddr = "";
	
		ent=gethostbyname(hostName.c_str());
		if (ent != 0) {
			my_bind_addr = (u_long) ((in_addr*)ent->h_addr_list[0])->s_addr;
	
			u_int8_t split[4];
			u_int32_t ip = my_bind_addr;
			split[0] = (ip & 0xff000000) >> 24;
			split[1] = (ip & 0x00ff0000) >> 16;
			split[2] = (ip & 0x0000ff00) >> 8;
			split[3] = (ip & 0x000000ff);
	
			IPAddr = itoa(split[3]) + "." + itoa(split[2]) + "." +itoa(split[1]) + "." + itoa(split[0]);
		}
		
		return IPAddr;
	}

    /******************************************************************************************
     * @brief	Get System TOP Process CPU Users
     *
     * purpose:	Get System TOP Process CPU Users
     *
     ******************************************************************************************/
	void Oam::getTopProcessCpuUsers(int topNumber, SystemTopProcessCpuUsers& systemtopprocesscpuusers)
	{
        SystemModuleTypeConfig systemmoduletypeconfig;
        TopProcessCpuUsers Topprocesscpuusers;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getTopProcessCpuUsers", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;
				try {
					getTopProcessCpuUsers(moduleName, topNumber, Topprocesscpuusers);
	
					systemtopprocesscpuusers.topprocesscpuusers.push_back(Topprocesscpuusers);
				}
                catch (exception&)
                {
                }
			}
        }
	}

    /******************************************************************************************
     * @brief	Get Module TOP Process CPU Users 
     *
     * purpose:	Get SModule TOP Process CPU Users 
     *
     ******************************************************************************************/
	void Oam::getTopProcessCpuUsers(const std::string module, int topNumber, TopProcessCpuUsers& topprocesscpuusers)
	{
        ByteStream msg;
        ByteStream receivedMSG;
		ByteStream::byte count;
        string processName;
		ByteStream::quadbyte cpuUsage;
    	ProcessCpuUser Processcpuuser;
		topprocesscpuusers.processcpuuser.clear();

		// validate Module name
		if ( module.find("xm") != string::npos )
			exceptionControl("getTopProcessCpuUsers", API_INVALID_PARAMETER);

		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getTopProcessCpuUsers", returnStatus);

        // setup message
        msg << (ByteStream::byte) GET_PROC_CPU_USAGE;
        msg << (ByteStream::byte) topNumber;

		topprocesscpuusers.ModuleName = module;
		topprocesscpuusers.numberTopUsers = topNumber;

        try
        {
            //send the msg to Server Monitor
            MessageQueueClient servermonitor(module + "_ServerMonitor");
            servermonitor.write(msg);

			// wait 10 seconds for ACK from Server Monitor
			struct timespec ts = { 30, 0 };

			receivedMSG = servermonitor.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> count;

				for ( int i=0 ; i < count ; i++)
				{
					receivedMSG >> processName;
					receivedMSG >> cpuUsage;

        			Processcpuuser.ProcessName = processName;
        			Processcpuuser.CpuUsage = cpuUsage;

					topprocesscpuusers.processcpuuser.push_back(Processcpuuser);
				}

			}
			else// timeout
        		exceptionControl("getTopProcessCpuUsers", API_TIMEOUT);

            // shutdown connection
            servermonitor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getTopProcessCpuUsers", API_FAILURE);
        }
	}

    /******************************************************************************************
     * @brief	get System CPU Usage
     *
     * purpose:	get System CPU Usage
     *
     ******************************************************************************************/
	void Oam::getSystemCpuUsage(SystemCpu& systemcpu)
	{
        SystemModuleTypeConfig systemmoduletypeconfig;
        ModuleCpu Modulecpu;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getSystemCpuUsage", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;

				try{
					getModuleCpuUsage(moduleName, Modulecpu);
		
					systemcpu.modulecpu.push_back(Modulecpu);
				}
                catch (exception&)
                {
                }
			}
        }
	}

    /******************************************************************************************
     * @brief	get Module CPU Usage
     *
     * purpose:	get Module CPU Usage
     *
     ******************************************************************************************/
	void Oam::getModuleCpuUsage(const std::string module, ModuleCpu& modulecpu)
	{
        ByteStream msg;
        ByteStream receivedMSG;
        string processName;
		ByteStream::byte cpuUsage;

		// validate Module name
		if ( module.find("xm") != string::npos )
			exceptionControl("getModuleCpuUsage", API_INVALID_PARAMETER);

		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getModuleCpuUsage", returnStatus);

        // setup message
        msg << (ByteStream::byte) GET_MODULE_CPU_USAGE;

		modulecpu.ModuleName = module;

        try
        {
            //send the msg to Server Monitor
            MessageQueueClient servermonitor(module + "_ServerMonitor");
            servermonitor.write(msg);

			// wait 30 seconds for ACK from Server Monitor
			struct timespec ts = { 30, 0 };

			receivedMSG = servermonitor.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> cpuUsage;

				modulecpu.CpuUsage = cpuUsage;
			}
			else	// timeout
        		exceptionControl("getModuleCpuUsage", API_TIMEOUT);

            // shutdown connection
            servermonitor.shutdown();
        }
        catch(...)
        {
			exceptionControl("getModuleCpuUsage", API_FAILURE);
        }
	}

    /******************************************************************************************
     * @brief	get System TOP Process Memory Users 
     *
     * purpose:	get System TOP Process Memory Users 
     *
     ******************************************************************************************/
	void Oam::getTopProcessMemoryUsers(int topNumber, SystemTopProcessMemoryUsers& systemtopprocessmemoryusers)
	{
        SystemModuleTypeConfig systemmoduletypeconfig;
        TopProcessMemoryUsers Topprocessmemoryusers;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getTopProcessMemoryUsers", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;

				try {
					getTopProcessMemoryUsers(moduleName, topNumber, Topprocessmemoryusers);
		
					systemtopprocessmemoryusers.topprocessmemoryusers.push_back(Topprocessmemoryusers);
				}
                catch (exception&)
                {
                }
			}
        }
	}
 
    /******************************************************************************************
     * @brief	get Module TOP Process Memory Users 
     *
     * purpose:	get Module TOP Process Memory Users 
     *
     ******************************************************************************************/
	void Oam::getTopProcessMemoryUsers(const std::string module, int topNumber, TopProcessMemoryUsers& topprocessmemoryusers)
	{
        ByteStream msg;
        ByteStream receivedMSG;
		ByteStream::byte count;
        string processName;
		ByteStream::quadbyte memoryUsed;
		ByteStream::byte memoryUsage;
    	ProcessMemoryUser Processmemoryuser;
		topprocessmemoryusers.processmemoryuser.clear();

		// validate Module name
		if ( module.find("xm") != string::npos )
			exceptionControl("getTopProcessMemoryUsers", API_INVALID_PARAMETER);

		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getTopProcessMemoryUsers", returnStatus);

        // setup message
        msg << (ByteStream::byte) GET_PROC_MEMORY_USAGE;
        msg << (ByteStream::byte) topNumber;

		topprocessmemoryusers.ModuleName = module;
		topprocessmemoryusers.numberTopUsers = topNumber;

        try
        {
            //send the msg to Server Monitor
            MessageQueueClient servermonitor(module + "_ServerMonitor");
            servermonitor.write(msg);

			// wait 30 seconds for ACK from Server Monitor
			struct timespec ts = { 30, 0 };

			receivedMSG = servermonitor.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> count;

				for ( int i=0 ; i < count ; i++)
				{
					receivedMSG >> processName;
					receivedMSG >> memoryUsed;
					receivedMSG >> memoryUsage;

        			Processmemoryuser.ProcessName = processName;
        			Processmemoryuser.MemoryUsed = memoryUsed;
        			Processmemoryuser.MemoryUsage = memoryUsage;

					topprocessmemoryusers.processmemoryuser.push_back(Processmemoryuser);
				}

			}
			else	// timeout
        		exceptionControl("getTopProcessMemoryUsers", API_TIMEOUT);

            // shutdown connection
            servermonitor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getTopProcessMemoryUsers", API_FAILURE);
        }
	}

    /******************************************************************************************
     * @brief	get System Memory Usage
     *
     * purpose:	get System Memory Usage
     *
     ******************************************************************************************/
	void Oam::getSystemMemoryUsage(SystemMemory& systemmemory)
	{
        SystemModuleTypeConfig systemmoduletypeconfig;
        ModuleMemory Modulememory;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getSystemMemoryUsage", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;

				try {
					getModuleMemoryUsage(moduleName, Modulememory);
		
					systemmemory.modulememory.push_back(Modulememory);
				}
                catch (exception&)
                {
                }
			}
        }
	}

    /******************************************************************************************
     * @brief	get Module Memory Usage
     *
     * purpose:	get Module Memory Usage
     *
     ******************************************************************************************/
	void Oam::getModuleMemoryUsage(const std::string module, ModuleMemory& modulememory)
	{
        ByteStream msg;
        ByteStream receivedMSG;
        string processName;
		ByteStream::quadbyte mem_total;
		ByteStream::quadbyte mem_used;
		ByteStream::quadbyte cache;
		ByteStream::byte memoryUsagePercent;
		ByteStream::quadbyte swap_total;
		ByteStream::quadbyte swap_used;
		ByteStream::byte swapUsagePercent;

		// validate Module name
		if ( module.find("xm") != string::npos )
			exceptionControl("getModuleMemoryUsage", API_INVALID_PARAMETER);

		// validate Module name
		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getModuleMemoryUsage", returnStatus);

        // setup message
        msg << (ByteStream::byte) GET_MODULE_MEMORY_USAGE;

		modulememory.ModuleName = module;

        try
        {
            //send the msg to Server Monitor
            MessageQueueClient servermonitor(module + "_ServerMonitor");
            servermonitor.write(msg);

			// wait 30 seconds for ACK from Server Monitor
			struct timespec ts = { 30, 0 };

			receivedMSG = servermonitor.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> mem_total;
				receivedMSG >> mem_used;
				receivedMSG >> cache;
				receivedMSG >> memoryUsagePercent;
				receivedMSG >> swap_total;
				receivedMSG >> swap_used;
				receivedMSG >> swapUsagePercent;

				modulememory.MemoryTotal = mem_total;
				modulememory.MemoryUsed = mem_used;
				modulememory.cache = cache;
				modulememory.MemoryUsage = memoryUsagePercent;
				modulememory.SwapTotal = swap_total;
				modulememory.SwapUsed = swap_used;
				modulememory.SwapUsage = swapUsagePercent;
			}
			else	// timeout
        		exceptionControl("getModuleMemoryUsage", API_TIMEOUT);

            // shutdown connection
            servermonitor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getModuleMemoryUsage", API_FAILURE);
        }
	}

    /******************************************************************************************
     * @brief	get System Disk Usage
     *
     * purpose:	get System Disk Usage
     *
     ******************************************************************************************/
	void Oam::getSystemDiskUsage(SystemDisk& systemdisk)
	{
        SystemModuleTypeConfig systemmoduletypeconfig;
        ModuleDisk Moduledisk;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getSystemMemoryUsage", API_FAILURE);
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName = (*pt).DeviceName;

				try {
					getModuleDiskUsage(moduleName, Moduledisk);
		
					systemdisk.moduledisk.push_back(Moduledisk);
				}
                catch (exception&)
                {
                }
			}
        }
	}

    /******************************************************************************************
     * @brief	get Module Disk Usage
     *
     * purpose:	get Module Disk Usage
     *
     ******************************************************************************************/
	void Oam::getModuleDiskUsage(const std::string module, ModuleDisk& moduledisk)
	{
		ByteStream msg;
        ByteStream receivedMSG;
        string processName;
    	DiskUsage Diskusage;
		moduledisk.diskusage.clear();

		// validate Module name
		if ( module.find("xm") != string::npos )
			exceptionControl("getModuleDiskUsage", API_INVALID_PARAMETER);

		// validate Module name
		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getModuleDiskUsage", returnStatus);

		ByteStream::byte entries;
		string deviceName;
		uint64_t totalBlocks;
		uint64_t usedBlocks;
		uint8_t diskUsage;

        // setup message
        msg << (ByteStream::byte) GET_MODULE_DISK_USAGE;

		moduledisk.ModuleName = module;

        try
        {
            //send the msg to Server Monitor
            MessageQueueClient servermonitor(module + "_ServerMonitor");
            servermonitor.write(msg);

			// wait 30 seconds for ACK from Server Monitor
			struct timespec ts = { 30, 0 };

			receivedMSG = servermonitor.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> entries;

				for ( int i=0 ; i < entries ; i++)
				{
					receivedMSG >> deviceName;
					receivedMSG >> totalBlocks;
					receivedMSG >> usedBlocks;
					receivedMSG >> diskUsage;
	
					Diskusage.DeviceName = deviceName;
					Diskusage.TotalBlocks = totalBlocks;
					Diskusage.UsedBlocks = usedBlocks;
					Diskusage.DiskUsage = diskUsage;

					moduledisk.diskusage.push_back(Diskusage);
				}
			}
			else	// timeout
        		exceptionControl("getModuleDiskUsage", API_TIMEOUT);

            // shutdown connection
            servermonitor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getModuleDiskUsage", API_FAILURE);
        }
	}

    /******************************************************************************************
     * @brief	get Active SQL Statements
     *
     * purpose:	get Active SQL Statements
     *
     ******************************************************************************************/
    void Oam::getActiveSQLStatements(ActiveSqlStatements& activesqlstatements)
    {
        SystemModuleTypeConfig systemmoduletypeconfig;
        ByteStream msg;
        ByteStream receivedMSG;
        ByteStream::byte entries;
		ByteStream::byte retStatus;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);

            // get Server Type Install ID
            int serverTypeInstall = oam::INSTALL_NORMAL;
            oamModuleInfo_t st;
            st = getModuleInfo();
            serverTypeInstall = boost::get<5>(st);

            string sendModule;
            switch (serverTypeInstall)
            {
                case oam::INSTALL_NORMAL:
                case oam::INSTALL_COMBINE_DM_UM:
                    sendModule = "um";
                    break;
                case oam::INSTALL_COMBINE_PM_UM:
                case oam::INSTALL_COMBINE_DM_UM_PM:
                    sendModule = "pm";
                    break;
            }

            //send request to modules
            for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
            {
                if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                    //end of file
                    break;

                if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType == sendModule )
                {
                    if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
                        break;

                    DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
                    for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                    {
                        string module = (*pt).DeviceName;

                        // setup message
                        msg << (ByteStream::byte) GET_ACTIVE_SQL_QUERY;

                        //send the msg to Server Monitor
                        MessageQueueClient servermonitor(module + "_ServerMonitor");
                        servermonitor.write(msg);

                        // wait 30 seconds for ACK from Server Monitor
                        struct timespec ts = { 30, 0 };

                        receivedMSG = servermonitor.read(&ts);
                        if (receivedMSG.length() > 0)
                        {
							receivedMSG >> retStatus;
							if ( retStatus != oam::API_SUCCESS ) {
                       			// shutdown connection
                        		servermonitor.shutdown();
                            	exceptionControl("getActiveSQLStatements", (int) retStatus);
							}

                            receivedMSG >> entries;
                            ActiveSqlStatement activeSqlStatement;
                            for (int i = 0; i < entries; i++)
                            {
                                receivedMSG >> activeSqlStatement.sqlstatement;
                                receivedMSG >> activeSqlStatement.starttime;
                                receivedMSG >> activeSqlStatement.sessionid;
                                activesqlstatements.push_back(activeSqlStatement);
                            }
                        }
                        else
                        {
                            // timeout
                            exceptionControl("getActiveSQLStatements", API_TIMEOUT);
                        }

                        // shutdown connection
                        servermonitor.shutdown();
                    }
                    break;
                }
            }
        }
        catch (std::exception& ex)
        {
            exceptionControl("getActiveSQLStatements", API_FAILURE, ex.what());
        }
        catch (...)
        {
            exceptionControl("getActiveSQLStatements", API_FAILURE);
        }
    }

    /********************************************************************
     *
     * IsValidIP - Validate IP Address format
     *
     ********************************************************************/
	bool Oam::isValidIP(const std::string ipAddress)
	{
		int currentPos = 0;
		for ( int i = 0 ; i < 4 ; i++)
		{
			string::size_type pos = ipAddress.find(".",currentPos);
			if (pos != string::npos) {
				if ( (pos - currentPos) > 3 || (pos - currentPos) <= 0)
					return false;
				currentPos = pos+1;
			}
			else
			{
				if ( i < 3 )
					return false;
				if ( (ipAddress.size() - currentPos) > 3 || (ipAddress.size() - currentPos) <= 0)
					return false;
				else
					return true;
			}
		}	
		return false;
	}


    /********************************************************************
     *
     * incrementIPAddress - Increment IP Address
     *
     ********************************************************************/
	std::string Oam::incrementIPAddress(const std::string ipAddress)
	{
		string newipAddress = ipAddress;
		string::size_type pos = ipAddress.rfind(".",80);
		if (pos != string::npos) {
			string last = ipAddress.substr(pos+1,80);
			int Ilast = atoi(last.c_str());
			Ilast++;

			if ( Ilast > 255 )
			{
				writeLog("incrementIPAddress: new address invalid, larger than 255", LOG_TYPE_ERROR );
				exceptionControl("incrementIPAddress", API_FAILURE);
			}

			last = itoa(Ilast);
			newipAddress = ipAddress.substr(0,pos+1);
			newipAddress = newipAddress + last;
		}
		else
		{
			writeLog("incrementIPAddress: passed address invalid: " + ipAddress, LOG_TYPE_ERROR );
			exceptionControl("incrementIPAddress", API_FAILURE);
		}

		return newipAddress;
	}

    /********************************************************************
     *
     * checkLogStatus - Check for a phrase in a log file and return status
     *
     ********************************************************************/
	bool Oam::checkLogStatus(std::string fileName, std::string phrase )
	{
		ifstream file (fileName.c_str());
	
		char line[400];
		string buf;
	
		while (file.getline(line, 400))
		{
			buf = line;
	
			string::size_type pos = buf.find(phrase,0);
			if (pos != string::npos)
				//found phrase
				return true;
		}
		file.close();
	
		return false;
	}

    /********************************************************************
     *
     * fixRSAkey - Fix RSA key
     *
     ********************************************************************/
	void Oam::fixRSAkey(std::string logFile)
	{
		ifstream file (logFile.c_str());
	
		char line[400];
		string buf;
	
		while (file.getline(line, 400))
		{
			buf = line;
	
			string::size_type pos = buf.find("Offending RSA key",0);
			if (pos != string::npos) {
				// line ID
				pos = buf.find(":",0);
				string lineID = buf.substr(pos+1,80);
				//remove non alphanumber characters
				for (size_t i = 0; i < lineID.length();)
				{
					if (!isdigit(lineID[i]))
						lineID.erase(i, 1);
					else
						 i++;
				}

				//get user
				string USER = "root";
				char* p= getenv("USER");
				if (p && *p)
					USER = p;

				string userDir = USER;
				if ( USER != "root")
					userDir = "home/" + USER;

				string cmd = "sed '" + lineID + "d' /" + userDir + "/.ssh/known_hosts > /" + userDir + "/.ssh/known_hosts";
				cout << cmd << endl;
				system(cmd.c_str());
				return;
			}

		}
		file.close();

		return;
	}

    /********************************************************************
     *
     * getWritablePM - Get PM with read-write mount
     *
     ********************************************************************/
	string Oam::getWritablePM()
	{
		string moduleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			moduleName = boost::get<3>(st);
			if ( moduleName == oam::UnassignedName )
				return "";
			return moduleName;
		}
		catch (...) {
       		exceptionControl("getWritablePM", API_FAILURE);
		}

		return "";
	}

    /********************************************************************
     *
     * getHotStandbyPM
     *
     ********************************************************************/
	string Oam::getHotStandbyPM()
	{
		string fileName = InstallDir + "/local/hotStandbyPM";
		string module;

		ifstream oldFile (fileName.c_str());
		if (!oldFile)
			return module;
		
		char line[400];
		while (oldFile.getline(line, 400))
		{
			module = line;
			break;
		}
		oldFile.close();

		return module;

	}

    /********************************************************************
     *
     * setHotStandbyPM
     *
     ********************************************************************/
	void Oam::setHotStandbyPM(std::string moduleName)
	{
		string fileName = InstallDir + "/local/hotStandbyPM";
	
		unlink (fileName.c_str());

		if ( moduleName.empty() || moduleName == " " )
			return;

		ofstream newFile (fileName.c_str());
	
		string cmd = "echo " + moduleName + " > " + fileName;
		(void)system(cmd.c_str());
	
		newFile.close();
	
		return;
	}

    /********************************************************************
     *
     * Distribute Calpont Configure File
     *
     ********************************************************************/
	void Oam::distributeConfigFile(std::string name, std::string file)
	{
		ACK_FLAG ackflag = oam::ACK_YES;
		if ( name == "system" )
			ackflag = oam::ACK_NO;

        // build and send msg
		int returnStatus = sendMsgToProcMgr(DISTRIBUTECONFIG, name, oam::FORCEFUL, ackflag, file, "", 30);

        if (returnStatus != API_SUCCESS)
            exceptionControl("distributeConfigFile", returnStatus);

		return;
	}

    /********************************************************************
     *
     * Switch Parent OAM Module
     *
     ********************************************************************/
	bool Oam::switchParentOAMModule(std::string moduleName, GRACEFUL_FLAG gracefulflag)
	{
		int returnStatus;
		// We assume that moduleName is a valid pm

		// check if current Active Parent Process-Manager is down and running on Standby Module
		// if so, send signal to Standby Process-Manager to start failover
		Config* sysConfig = Config::makeConfig();
	
		string IPAddr = sysConfig->getConfig("ProcStatusControl", "IPAddr");
	
		string cmdLine = "ping ";
		string cmdOption = " -w 1 >> /dev/null";
		string cmd = cmdLine + IPAddr + cmdOption;
		if ( system(cmd.c_str()) != 0 ) {
			//ping failure
			try{
				string standbyOAMModule;
				getSystemConfig("StandbyOAMModuleName", standbyOAMModule);

				oamModuleInfo_t t = Oam::getModuleInfo();
				string localModule = boost::get<0>(t);

				if (standbyOAMModule == localModule )
					// send SIGUSR1
					system("pkill -SIGUSR1 ProcMgr");
			}
			catch(...)
			{
				exceptionControl("switchParentOAMModule", API_FAILURE);
			}

			return false;
		}

		// only make call if system is ACTIVE and module switching to is ACTIVE
		SystemStatus systemstatus;

		try {
			getSystemStatus(systemstatus);
		}
		catch (exception& )
		{}

		if (systemstatus.SystemOpState == oam::MAN_INIT ||
			systemstatus.SystemOpState == oam::AUTO_INIT ||
			systemstatus.SystemOpState == oam::UP ||
			systemstatus.SystemOpState == oam::BUSY_INIT ||
			systemstatus.SystemOpState == oam::UP )
			exceptionControl("switchParentOAMModule", API_INVALID_STATE);

		if (systemstatus.SystemOpState == oam::ACTIVE ||
			systemstatus.SystemOpState == oam::FAILED )
		{
			// build and send msg to stop system
			returnStatus = sendMsgToProcMgrWithStatus(STOPSYSTEM, "OAM Module switched", gracefulflag, ACK_YES);
	
			if ( returnStatus != API_SUCCESS )
				exceptionControl("stopSystem", returnStatus);
		}
	
        // build and send msg to switch configuration
		cout << endl << "   Switch Active Parent OAM to Module '" << moduleName << "', please wait...";
        returnStatus = sendMsgToProcMgr(SWITCHOAMPARENT, moduleName, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("switchParentOAMModule", returnStatus);

		if (systemstatus.SystemOpState == oam::ACTIVE ||
			systemstatus.SystemOpState == oam::FAILED )
		{
			//give  time for ProcMon/ProcMgr to get fully active on new pm
			sleep(10);
	
			// build and send msg to restart system
			returnStatus = sendMsgToProcMgr(RESTARTSYSTEM, "", FORCEFUL, ACK_YES);
	
			if (returnStatus != API_SUCCESS)
				exceptionControl("startSystem", returnStatus);
			return true; // Caller should wait for system to come up.
		}

		return false; // Caller should not wait for system to come up.
	}

    /********************************************************************
     *
     * Get Storage Config Data
     *
     ********************************************************************/
	systemStorageInfo_t Oam::getStorageConfig()
	{
		DeviceDBRootList deviceDBRootList;
		std::string storageType = "";
		std::string UMstorageType = "";
		int SystemDBRootCount = 0;

		try {
			getSystemConfig("DBRootStorageType", storageType);
		}
		catch(...)
		{
			exceptionControl("getStorageConfig", oam::API_FAILURE);
		}

		try {
			getSystemConfig("UMStorageType", UMstorageType);
		}
		catch(...)
		{
			exceptionControl("getStorageConfig", oam::API_FAILURE);
		}

		try {
			getSystemConfig("DBRootCount", SystemDBRootCount);
		}
		catch(...)
		{
			exceptionControl("getStorageConfig", oam::API_FAILURE);
		}

		try
		{
			SystemModuleTypeConfig systemmoduletypeconfig;
			getSystemConfig(systemmoduletypeconfig);

			for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
			{
				if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
					// end of list
					break;

				int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

				string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

				if ( moduleCount > 0 && moduletype == "pm") {
					deviceDBRootList = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList;
					return boost::make_tuple(storageType, SystemDBRootCount, deviceDBRootList, UMstorageType);
				}
			}
		}
		catch(...)
		{
			exceptionControl("getStorageConfig", oam::API_FAILURE);
		}

		return boost::make_tuple(storageType, SystemDBRootCount, deviceDBRootList, UMstorageType);
	}

    /********************************************************************
     *
     * Get PM - DBRoot Config data
     *
     ********************************************************************/
	void Oam::getPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist)
	{
		string module = "pm" + itoa(pmid);
		// validate Module name
		int returnStatus = validateModule(module);
		if (returnStatus != API_SUCCESS)
			exceptionControl("getPmDbrootConfig", returnStatus);

		try
		{
			ModuleConfig moduleconfig;
			getSystemConfig(module, moduleconfig);

			DBRootConfigList::iterator pt1 = moduleconfig.dbrootConfigList.begin();
			for( ; pt1 != moduleconfig.dbrootConfigList.end() ; pt1++)
			{
				dbrootconfiglist.push_back((*pt1));
			}	
		}
		catch (...)
		{
			// dbrootid not found, return with error
			exceptionControl("getPmDbrootConfig", API_INVALID_PARAMETER);
		}
	}

    /********************************************************************
     *
     * Get DBRoot - PM Config data
     *
     ********************************************************************/
	void Oam::getDbrootPmConfig(const int dbrootid, int& pmid)
	{
		SystemModuleTypeConfig systemmoduletypeconfig;
		ModuleTypeConfig moduletypeconfig;
		ModuleConfig moduleconfig;

		try
		{
			getSystemConfig(systemmoduletypeconfig);

			for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
			{
				if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
					// end of list
					break;

				int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

				string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

				if ( moduleCount > 0 && moduletype == "pm")
				{
					DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();
					for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt++)
					{
						DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
						for( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
						{
							if (*pt1 == dbrootid) {
								pmid = (*pt).DeviceID;
								return;
							}
						}
					}
				}
			}
			// dbrootid not found, return with error
			exceptionControl("getDbrootPmConfig", API_INVALID_PARAMETER);
		}
		catch (exception& )
		{}
		
		// dbrootid not found, return with error
		exceptionControl("getDbrootPmConfig", API_INVALID_PARAMETER);
	}

    /********************************************************************
     *
     * Get DBRoot - PM Config data
     *
     ********************************************************************/
	void Oam::getDbrootPmConfig(const int dbrootid, std::string& pmid)
	{
		try {
			int PMid;
			getDbrootPmConfig(dbrootid, PMid);
			pmid = itoa(PMid);
			return;
		}
		catch (exception& )
		{}
		
		// dbrootid not found, return with error
		exceptionControl("getDbrootPmConfig", API_INVALID_PARAMETER);
	}

    /********************************************************************
     *
     * Get System DBRoot Config data
     *
     ********************************************************************/
	void Oam::getSystemDbrootConfig(DBRootConfigList& dbrootconfiglist)
	{
		SystemModuleTypeConfig systemmoduletypeconfig;
		ModuleTypeConfig moduletypeconfig;
		ModuleConfig moduleconfig;

		try
		{
			getSystemConfig(systemmoduletypeconfig);

			for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
			{
				if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
					// end of list
					break;

				int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

				string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

				if ( moduleCount > 0 && moduletype == "pm")
				{
					DeviceDBRootList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();
					for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt++)
					{
						DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
						for( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
						{
							dbrootconfiglist.push_back(*pt1);
						}
					}
				}
			}

			sort ( dbrootconfiglist.begin(), dbrootconfiglist.end() );
		}
		catch (...)
		{		// dbrootid not found, return with error
			exceptionControl("getSystemDbrootConfig", API_INVALID_PARAMETER);
		}
		return;
	}

    /********************************************************************
     *
     * Set PM - DBRoot Config data
     *
     ********************************************************************/
	void Oam::setPmDbrootConfig(const int pmid, DBRootConfigList& dbrootconfiglist)
	{
		ModuleConfig moduleconfig;

		string module = "pm" + itoa(pmid);
		try
		{
			getSystemConfig(module, moduleconfig);

			moduleconfig.dbrootConfigList = dbrootconfiglist;

			try
			{
				setSystemConfig(module, moduleconfig);
				return;
			}
			catch(...)
			{
				writeLog("ERROR: setSystemConfig api failure for " + module  , LOG_TYPE_ERROR );
				cout << endl << "ERROR: setSystemConfig api failure for " + module << endl;
				exceptionControl("getSystemDbrootConfig", API_INVALID_PARAMETER);
			}
		}
		catch(...)
		{
			writeLog("ERROR: getSystemConfig api failure for " + module  , LOG_TYPE_ERROR );
			cout << endl << "ERROR: getSystemConfig api failure for " + module << endl;
			exceptionControl("getSystemDbrootConfig", API_INVALID_PARAMETER);
		}

		//set System DBRoot Count
		try
		{
			setSystemDBrootCount();
		}
		catch (exception& )
		{
			cout << endl << "**** setSystemDBrootCount Failed" << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}
	}

    /********************************************************************
     *
     * Manual Move PM - DBRoot data
     *
     ********************************************************************/
	void Oam::manualMovePmDbroot(std::string residePM, std::string dbrootIDs, std::string toPM)
	{
		typedef std::vector<string> dbrootList;
		dbrootList dbrootlist;
		dbrootList tempdbrootlist;

		string GlusterConfig = "n";
		try {
			getSystemConfig( "GlusterConfig", GlusterConfig);
		}
		catch(...)
		{
			GlusterConfig = "n";
		}

		boost::char_separator<char> sep(", ");
		boost::tokenizer< boost::char_separator<char> > tokens(dbrootIDs, sep);
		for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
				it != tokens.end();
				++it)
		{
			dbrootlist.push_back(*it);
			tempdbrootlist.push_back(*it);
		}

		string residePMID = residePM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;
		string toPMID = toPM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;

		//get dbroots ids for reside PM
		DBRootConfigList residedbrootConfigList;
		try
		{
			getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

			DBRootConfigList::iterator pt = residedbrootConfigList.begin();
			for( ; pt != residedbrootConfigList.end() ; pt++)
			{
				//check if entered dbroot id is in residing pm
				dbrootList::iterator pt1 = tempdbrootlist.begin();
				for( ; pt1 != tempdbrootlist.end() ; pt1++)
				{
					if ( itoa(*pt) == *pt1 ) {
						tempdbrootlist.erase(pt1);
						break;
					}
				}
			}

			if ( !tempdbrootlist.empty() ) {
				// there is a entered dbroot id not in the residing pm
				writeLog("ERROR: dbroot IDs not assigned to " + residePM , LOG_TYPE_ERROR );
				cout << endl << "ERROR: these dbroot IDs not assigned to '" << residePM << "' : ";
				dbrootList::iterator pt1 = tempdbrootlist.begin();
				for( ; pt1 != tempdbrootlist.end() ;)
				{
					cout << *pt1;
					pt1++;
					if (pt1 != tempdbrootlist.end())
						cout << ", ";
				}
				cout << endl << endl;
				exceptionControl("manualMovePmDbroot", API_FAILURE);
			}
		}
		catch (exception& )
		{
			writeLog("ERROR: getPmDbrootConfig api failure for pm" + residePMID , LOG_TYPE_ERROR );
			cout << endl << "ERROR: getPmDbrootConfig api failure for pm" + residePMID << endl;
			exceptionControl("manualMovePmDbroot", API_FAILURE);
		}

		//get dbroots ids for reside PM
		DBRootConfigList todbrootConfigList;
		try
		{
			getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);
		}
		catch (exception& )
		{
			writeLog("ERROR: getPmDbrootConfig api failure for pm" + toPMID , LOG_TYPE_ERROR );
			cout << endl << "ERROR: getPmDbrootConfig api failure for pm" + toPMID << endl;
			exceptionControl("manualMovePmDbroot", API_FAILURE);
		}

		//remove entered dbroot IDs from reside PM list
		dbrootList::iterator pt1 = dbrootlist.begin();
		for( ; pt1 != dbrootlist.end() ; pt1++)
		{
			DBRootConfigList::iterator pt2 = residedbrootConfigList.begin();
			for( ; pt2 != residedbrootConfigList.end() ; pt2++)
			{
				if ( itoa(*pt2) == *pt1 ) {

					dbrootList dbroot1;
					dbroot1.push_back(*pt1);

					//send msg to unmount dbroot if module is not offline
					int opState;
					bool degraded;
					try {
						getModuleStatus(residePM, opState, degraded);
					}
					catch(...)
					{}
		
					if (opState != oam::AUTO_OFFLINE || opState != oam::AUTO_DISABLED) {
//						bool unmountPass = true;
						try
						{
							mountDBRoot(dbroot1, false);
						}
						catch (exception& )
						{
							writeLog("ERROR: dbroot failed to unmount", LOG_TYPE_ERROR );
							cout << endl << "ERROR: umountDBRoot api failure" << endl;
							exceptionControl("manualMovePmDbroot", API_FAILURE);
//							unmountPass = false;
						}
	
//						if ( !unmountPass) {
//							dbrootlist.erase(pt1);
//							break;
//						}
					}

					//check for amazon moving required
					try
					{
						amazonReattach(toPM, dbroot1);
					}
					catch (exception& )
					{
						writeLog("ERROR: amazonReattach api failure", LOG_TYPE_ERROR );
						cout << endl << "ERROR: amazonReattach api failure" << endl;
						exceptionControl("manualMovePmDbroot", API_FAILURE);
					}

					//if Gluster, do the assign command
					if ( GlusterConfig == "y")
					{
						try {
							string errmsg;
							int ret = glusterctl(oam::GLUSTER_ASSIGN, *pt1, toPMID, errmsg);
							if ( ret != 0 )
							{
								cerr << "FAILURE: Error assigning gluster dbroot# " + *pt1 + " to pm" + toPMID + ", error: " + errmsg << endl;
								exceptionControl("manualMovePmDbroot", API_FAILURE);
							}
						}
						catch (exception& e)
						{
							cout << endl << "**** glusterctl API exception:  " << e.what() << endl;
							cerr << "FAILURE: Error assigning gluster dbroot# " + *pt1 + " to pm" + toPMID << endl;
							exceptionControl("manualMovePmDbroot", API_FAILURE);
						}
						catch (...)
						{
							cout << endl << "**** glusterctl API exception: UNKNOWN"  << endl;
							cerr << "FAILURE: Error assigning gluster dbroot# " + *pt1 + " to pm" + toPMID << endl;
							exceptionControl("manualMovePmDbroot", API_FAILURE);
						}
					}

					todbrootConfigList.push_back(*pt2);

					residedbrootConfigList.erase(pt2);

					break;
				}
			}
		}

		//set the 2 pms dbroot config
		try
		{
			setPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);
		}
		catch (exception& )
		{
			writeLog("ERROR: setPmDbrootConfig api failure for pm" + residePMID , LOG_TYPE_ERROR );
			cout << endl << "ERROR: setPmDbrootConfig api failure for pm" + residePMID << endl;
			exceptionControl("manualMovePmDbroot", API_FAILURE);
		}

		try
		{
			setPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);
		}
		catch (exception& )
		{
			writeLog("ERROR: setPmDbrootConfig api failure for pm" + toPMID , LOG_TYPE_ERROR );
			cout << endl << "ERROR: setPmDbrootConfig api failure for pm" + toPMID << endl;
			exceptionControl("manualMovePmDbroot", API_FAILURE);
		}

		//send msg to mount dbroot
		try
		{
    		mountDBRoot(dbrootlist);
		}
		catch (exception& )
		{
			writeLog("ERROR: mountDBRoot api failure", LOG_TYPE_DEBUG );
			cout << endl << "ERROR: mountDBRoot api failure" << endl;
		}

		//get updated Calpont.xml distributed
		distributeConfigFile("system");

		return;

	}


	bool comparex(const PmDBRootCount_s& x, const PmDBRootCount_s& y)
	{
		return x.count < y.count;
	}

    /********************************************************************
     *
     * Auto Move PM - DBRoot data
     *
     ********************************************************************/
	bool Oam::autoMovePmDbroot(std::string residePM)
	{
		string DBRootStorageType;
		try {
			getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}

		string GlusterConfig = "n";
		try {
			getSystemConfig( "GlusterConfig", GlusterConfig);
		}
		catch(...)
		{
			GlusterConfig = "n";
		}

		if (DBRootStorageType == "internal" && GlusterConfig == "n")
			return 1;

		// get current Module name
		string localModuleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			localModuleName = boost::get<0>(st);
		}
		catch (...) 
		{}

		int localPMID = atoi(localModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

		int residePMID = atoi(residePM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

		//get dbroot ids for reside PM
		DBRootConfigList residedbrootConfigList;
		try
		{
			getPmDbrootConfig(residePMID, residedbrootConfigList);
			if ( residedbrootConfigList.empty() ) {
				writeLog("ERROR: residedbrootConfigList empty", LOG_TYPE_ERROR );
				exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
			}
		}
		catch (...)
		{
			writeLog("ERROR: getPmDbrootConfig failure", LOG_TYPE_ERROR );
			exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
		}

		//get dbroot id for other PMs
		systemStorageInfo_t t;
		DeviceDBRootList moduledbrootlist;
		try
		{
			t = getStorageConfig();
			moduledbrootlist = boost::get<2>(t);
		}
		catch (exception& )
		{
			writeLog("ERROR: getStorageConfig failure", LOG_TYPE_ERROR );
			exceptionControl("autoMovePmDbroot", API_FAILURE);
		}

		// get list of dbroot count for each pm
        typedef std::vector<PmDBRootCount_s> PMdbrootList;
		PMdbrootList pmdbrootList;
		PmDBRootCount_s pmdbroot;

		DeviceDBRootList::iterator pt = moduledbrootlist.begin();
		for( ; pt != moduledbrootlist.end() ; pt++)
		{
			// only put pms with dbroots assigned, if 0 then that pm is disabled
			if ( (*pt).dbrootConfigList.size() > 0 )
			{
				pmdbroot.pmID = (*pt).DeviceID;
				pmdbroot.count = (*pt).dbrootConfigList.size();
				pmdbrootList.push_back(pmdbroot);
			}
		}

		sort ( pmdbrootList.begin(), pmdbrootList.end(), comparex );

		//clear reside IDs
		DBRootConfigList clearresidedbrootConfigList;
		try
		{
			setPmDbrootConfig(residePMID, clearresidedbrootConfigList);
		}
		catch (...)
		{
			writeLog("ERROR: setPmDbrootConfig failure - clear reside ID", LOG_TYPE_ERROR );
			exceptionControl("autoMovePmDbroot", API_FAILURE);
		}

		//distribute dbroot IDs to other PMs starting with lowest count
		bool exceptionFailure = false;
		bool dbroot1 = false;
		DBRootConfigList::iterator pt2 = residedbrootConfigList.begin();
		for( ; pt2 != residedbrootConfigList.end() ; )
		{
			int dbrootID = *pt2;

			//dbroot #1 always get moved to local module
			if ( dbrootID == 1 )
			{
				dbroot1 = true;
				//get dbroot ids for PM
				DBRootConfigList todbrootConfigList;
				try
				{
					getPmDbrootConfig(localPMID, todbrootConfigList);
				}
				catch (...)
				{
					writeLog("ERROR: getPmDbrootConfig failure", LOG_TYPE_ERROR );
					exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
				}

				//get the first dbroot assigned to this pm, so it can be auto unmoved later instead of dbroot1
				DBRootConfigList::iterator pt = todbrootConfigList.begin();
				int subDBRootID = *pt;

				todbrootConfigList.push_back(dbrootID);

				try
				{
					setPmDbrootConfig(localPMID, todbrootConfigList);
					writeLog("autoMovePmDbroot/setPmDbrootConfig : " + localModuleName + ":" + itoa(dbrootID), LOG_TYPE_DEBUG);
					sleep(5);

					//send msg to toPM to mount dbroot
					try
					{
						typedef std::vector<string> dbrootList;
						dbrootList dbrootlist;
						dbrootlist.push_back(itoa(dbrootID));
						mountDBRoot(dbrootlist);
					}
					catch (exception& )
					{
						writeLog("ERROR: mountDBRoot api failure", LOG_TYPE_DEBUG );
						cout << endl << "ERROR: mountDBRoot api failure" << endl;
					}
				}
				catch (...)
				{
					writeLog("ERROR: setPmDbrootConfig failure", LOG_TYPE_ERROR );
					exceptionControl("autoMovePmDbroot", API_FAILURE);
				}
		
				if ( GlusterConfig == "y")
				{
					try {
						string lPMid = itoa(localPMID);
						string errmsg;
						int ret = glusterctl(oam::GLUSTER_ASSIGN, itoa(dbrootID), lPMid, errmsg);
						if ( ret != 0 )
						{
							cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + itoa(localPMID) + ", error: " + errmsg << endl;
							exceptionControl("assignPmDbrootConfig", API_FAILURE);
						}
					}
					catch (exception& e)
					{
						cout << endl << "**** glusterctl API exception:  " << e.what() << endl;
						cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + itoa(localPMID) << endl;
						exceptionControl("assignPmDbrootConfig", API_FAILURE);
					}
					catch (...)
					{
						cout << endl << "**** glusterctl API exception: UNKNOWN"  << endl;
						cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + itoa(localPMID) << endl;
						exceptionControl("assignPmDbrootConfig", API_FAILURE);
					}
				}

				//store in move dbroot transaction file
				string fileName = InstallDir + "/local/moveDbrootTransactionLog";
			
				string cmd = "echo '" + residePM + "|" + localModuleName + "|" + itoa(subDBRootID) + "' >> " + fileName;
				system(cmd.c_str());
			
				//check for amazon moving required
				try
				{
					typedef std::vector<string> dbrootList;
					dbrootList dbrootlist;
					dbrootlist.push_back(itoa(dbrootID));

					amazonReattach(localModuleName, dbrootlist, true);
				}
				catch (exception& )
				{
					writeLog("ERROR: amazonReattach failure", LOG_TYPE_ERROR );
					exceptionControl("autoMovePmDbroot", API_FAILURE);
				}

				pt2++;
				if ( pt2 == residedbrootConfigList.end() )
					break;
			}
			else
			{
				//if Gluster, get it's list for DBroot and move to one of those
				string toPmID;
				if ( GlusterConfig == "y")
				{
					string pmList = "";
					try {
						string errmsg;
						int ret = glusterctl(oam::GLUSTER_WHOHAS, itoa(dbrootID), pmList, errmsg);
						if ( ret != 0 )
						{
							writeLog("ERROR: glusterctl failure getting pm list for dbroot " + itoa(dbrootID) + " , error: " + errmsg, LOG_TYPE_ERROR );
							exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
						}
					}
					catch (exception& )
					{
						writeLog("ERROR: glusterctl failure getting pm list for dbroot " + itoa(dbrootID), LOG_TYPE_ERROR );
						exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
					}
					catch (...)
					{
						writeLog("ERROR: glusterctl failure getting pm list for dbroot " + itoa(dbrootID), LOG_TYPE_ERROR );
						exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
					}

					bool found = false;
					boost::char_separator<char> sep(" ");
					boost::tokenizer< boost::char_separator<char> > tokens(pmList, sep);
					for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
							it != tokens.end();
							++it)
					{
						if ( atoi((*it).c_str()) != residePMID ) {
							found = true;
							toPmID = *it;

							string toPM = "pm" + toPmID;

							try {
								string errmsg;
								int ret = glusterctl(oam::GLUSTER_ASSIGN, itoa(dbrootID), toPmID, errmsg);
								if ( ret != 0 )
								{
									cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + toPmID + ", error: " + errmsg << endl;
									exceptionControl("manualMovePmDbroot", API_FAILURE);
								}
							}
							catch (exception& e)
							{
								cout << endl << "**** glusterctl API exception:  " << e.what() << endl;
								cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + toPmID << endl;
								exceptionControl("manualMovePmDbroot", API_FAILURE);
							}
							catch (...)
							{
								cout << endl << "**** glusterctl API exception: UNKNOWN"  << endl;
								cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(dbrootID) + " to pm" + toPmID << endl;
								exceptionControl("manualMovePmDbroot", API_FAILURE);
							}

							DBRootConfigList todbrootConfigList;
							try
							{
								getPmDbrootConfig(atoi(toPmID.c_str()), todbrootConfigList);
							}
							catch (...)
							{
								writeLog("ERROR: getPmDbrootConfig failure", LOG_TYPE_ERROR );
								exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
							}

							todbrootConfigList.push_back(dbrootID);
			
							try
							{
								setPmDbrootConfig(atoi(toPmID.c_str()), todbrootConfigList);
								writeLog("autoMovePmDbroot/setPmDbrootConfig : " + toPM + ":" + itoa(dbrootID), LOG_TYPE_DEBUG);
								sleep(5);
		
								//send msg to toPM to mount dbroot
								try
								{
									typedef std::vector<string> dbrootList;
									dbrootList dbrootlist;
									dbrootlist.push_back(itoa(dbrootID));
		
									mountDBRoot(dbrootlist);
								}
								catch (exception& )
								{
									writeLog("ERROR: mountDBRoot api failure", LOG_TYPE_DEBUG );
									cout << endl << "ERROR: mountDBRoot api failure" << endl;
								}
							}
							catch (...)
							{
								writeLog("ERROR: setPmDbrootConfig failure", LOG_TYPE_ERROR );
								exceptionFailure = true;
							}
					
							//store in move dbroot transaction file
							string fileName = InstallDir + "/local/moveDbrootTransactionLog";
						
							string cmd = "echo '" + residePM + "|" + toPM + "|" + itoa(dbrootID) + "' >> " + fileName;
							system(cmd.c_str());

							pt2++;
							if ( pt2 == residedbrootConfigList.end() )
								break;
							dbrootID = *pt2;
						}
					}

					if (!found) {
						writeLog("ERROR: no available pm found for DBRoot " + itoa(dbrootID), LOG_TYPE_ERROR );
						exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
					}
				}
				else
				{  // not gluster, pmdbrootList = available pms for assigning
					PMdbrootList::iterator pt1 = pmdbrootList.begin();
					for( ; pt1 != pmdbrootList.end() ; pt1++)
					{
						//if dbroot1 was moved, skip local module the first time through
						if ( dbroot1 )
						{
							if ( (*pt1).pmID == localPMID ) {
								dbroot1 = false;
								continue;
							}
						}

						if ( (*pt1).pmID != residePMID ) {
			
							string toPM = "pm" + itoa((*pt1).pmID);
					
							//get dbroot ids for PM
							DBRootConfigList todbrootConfigList;
							try
							{
								getPmDbrootConfig((*pt1).pmID, todbrootConfigList);
							}
							catch (...)
							{
								writeLog("ERROR: getPmDbrootConfig failure", LOG_TYPE_ERROR );
								exceptionControl("autoMovePmDbroot", API_INVALID_PARAMETER);
							}
					
							todbrootConfigList.push_back(dbrootID);
			
							try
							{
								setPmDbrootConfig((*pt1).pmID, todbrootConfigList);
								writeLog("autoMovePmDbroot/setPmDbrootConfig : " + toPM + ":" + itoa(dbrootID), LOG_TYPE_DEBUG);
								sleep(5);
		
								//send msg to toPM to mount dbroot
								try
								{
									typedef std::vector<string> dbrootList;
									dbrootList dbrootlist;
									dbrootlist.push_back(itoa(dbrootID));
	
									mountDBRoot(dbrootlist);
								}
								catch (exception& )
								{
									writeLog("ERROR: mountDBRoot api failure", LOG_TYPE_DEBUG );
									cout << endl << "ERROR: mountDBRoot api failure" << endl;
								}
							}
							catch (...)
							{
								writeLog("ERROR: setPmDbrootConfig failure", LOG_TYPE_ERROR );
								exceptionFailure = true;
							}
					
							//store in move dbroot transaction file
							string fileName = InstallDir + "/local/moveDbrootTransactionLog";
						
							string cmd = "echo '" + residePM + "|" + toPM + "|" + itoa(dbrootID) + "' >> " + fileName;
							system(cmd.c_str());
				
							//check for amazon moving required
							try
							{
								typedef std::vector<string> dbrootList;
								dbrootList dbrootlist;
								dbrootlist.push_back(itoa(dbrootID));
		
								amazonReattach(toPM, dbrootlist, true);
							}
							catch (exception& )
							{
								writeLog("ERROR: amazonReattach failure", LOG_TYPE_ERROR );
								exceptionFailure = true;
							}
		
							pt2++;
							if ( pt2 == residedbrootConfigList.end() )
								break;
							dbrootID = *pt2;
						}
					}
				}
			}
		}

		if (exceptionFailure)
			exceptionControl("autoMovePmDbroot", API_FAILURE);

		return 0;

	}

    /********************************************************************
     *
     * Auto Move PM - DBRoot data
     *
     ********************************************************************/
	bool Oam::autoUnMovePmDbroot(std::string toPM)
	{
		string residePM;
		string fromPM;
		string dbrootIDs;

		string DBRootStorageType;
		try {
			getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}

		string GlusterConfig = "n";
		try {
			getSystemConfig( "GlusterConfig", GlusterConfig);
		}
		catch(...)
		{
			GlusterConfig = "n";
		}

		if (DBRootStorageType == "internal" && GlusterConfig == "n")
			return 1;

		//store in move dbroot transaction file
		string fileName = InstallDir + "/local/moveDbrootTransactionLog";
	
		ifstream oldFile (fileName.c_str());
		if (!oldFile) {
			ofstream newFile (fileName.c_str());	
			int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
			newFile.close();
			close(fd);
		}

		vector <string> lines;
		char line[200];
		string buf;
		string newLine;
		bool found = false;
		while (oldFile.getline(line, 200))
		{
			buf = line;
			string::size_type pos = buf.find("|",0);
			if (pos != string::npos)
			{
				residePM = buf.substr(0,pos);
				if ( residePM == toPM ) {
					string::size_type pos1 = buf.find("|",pos+1);
					if (pos1 != string::npos)
					{
						fromPM = buf.substr(pos+1,pos1-pos-1);
						dbrootIDs = buf.substr(pos1+1,80);
						found = true;

						try {
							manualMovePmDbroot(fromPM, dbrootIDs, toPM);
							writeLog("autoUnMovePmDbroot/manualMovePmDbroot : " + fromPM + ":" + dbrootIDs + ":" + toPM, LOG_TYPE_DEBUG);
						}
						catch (...)
						{
							writeLog("ERROR: manualMovePmDbroot failure: " + fromPM + ":" + dbrootIDs + ":" + toPM, LOG_TYPE_ERROR );
							cout << "ERROR: manualMovePmDbroot failure" << endl;
						}
					}
				}
				else
					lines.push_back(buf);
			}
		}

		if (!found) {
			writeLog("ERROR: no dbroots found in ../Calpont/local/moveDbrootTransactionLog", LOG_TYPE_ERROR );
			cout << "ERROR: no dbroots found in " << fileName << endl;
			exceptionControl("autoUnMovePmDbroot", API_FAILURE);
		}

		oldFile.close();
		unlink (fileName.c_str());
		ofstream newFile (fileName.c_str());	
		
		//create new file
		int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
		
		copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
		newFile.close();
		
		close(fd);

		return 0;
	}

    /***************************************************************************
     *
     * Function:  addDbroot
     *
     * Purpose:   add DBRoot
     *
     ****************************************************************************/

    void Oam::addDbroot(const int dbrootNumber, DBRootConfigList& dbrootlist)
	{
		int SystemDBRootCount = 0;
		string cloud;
		string DBRootStorageType;
		string volumeSize;
		Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";

		try {
			getSystemConfig("DBRootCount", SystemDBRootCount);
			getSystemConfig("Cloud", cloud);
			getSystemConfig("DBRootStorageType", DBRootStorageType);
			getSystemConfig("PMVolumeSize", volumeSize);
		}
		catch(...) {}

		int newSystemDBRootCount = SystemDBRootCount + dbrootNumber;
		if ( newSystemDBRootCount > MAX_DBROOT )
		{
			cout << "ERROR: Failed add, total Number of DBRoots would be over maximum of " << MAX_DBROOT << endl;
			exceptionControl("addDbroot", API_INVALID_PARAMETER);
		}

		if ( (cloud == "amazon-ec2" || cloud == "amazon-vpc") && 
				DBRootStorageType == "external" )
		{
			if ( newSystemDBRootCount > MAX_DBROOT_AMAZON )
			{
				cout << "ERROR: Failed add, total Number of DBRoots would be over maximum of " << MAX_DBROOT_AMAZON << endl;
				exceptionControl("addDbroot", API_INVALID_PARAMETER);
			}
		}

		//get assigned DBRoots IDs
		DBRootConfigList dbrootConfigList;
		try {
			getSystemDbrootConfig(dbrootConfigList);
		}
		catch(...) {}

		//get unassigned DBRoots IDs
		DBRootConfigList undbrootlist;
		try {
			getUnassignedDbroot(undbrootlist);
		}
		catch(...) {}

		//combined list
		DBRootConfigList::iterator pt1 = undbrootlist.begin();
		for( ; pt1 != undbrootlist.end() ; pt1++)
		{
			dbrootConfigList.push_back(*pt1);
		}

		int newID = 1;
		for ( int count = 0 ; count < dbrootNumber ; count++ )
		{
			//check for match
			while (true)
			{
				bool found = false;
				DBRootConfigList::iterator pt = dbrootConfigList.begin();
				for( ; pt != dbrootConfigList.end() ; pt++)
				{
					if ( newID == *pt ) {
						newID++;
						found = true;
						break;
					}
				}

				if (!found)
				{
					dbrootlist.push_back(newID);
					newID++;
					break;
				}
			}
		}

		if ( dbrootlist.size() == 0 )
		{
			cout << "ERROR: Failed add, No DBRoot IDs available" << endl;
			exceptionControl("addDbroot", API_INVALID_PARAMETER);
		}

		//if amazon cloud with external volumes, create AWS volumes
		if ( (cloud == "amazon-ec2" || cloud == "amazon-vpc") && 
				DBRootStorageType == "external" )
		{
			//get local instance name (pm1)
			string localInstance = getEC2LocalInstance();
			if ( localInstance == "failed" || localInstance.empty() || localInstance == "") 
			{
				cout << endl << "ERROR: Failed to get Instance ID" << endl;
				exceptionControl("addDbroot", API_INVALID_PARAMETER);
			}

			string Section = "Installation";
	
			DBRootConfigList::iterator pt1 = dbrootlist.begin();
			for( ; pt1 != dbrootlist.end() ; pt1++)
			{
				cout << "  Create AWS Volume for DBRoot #" << itoa(*pt1) << endl;
				//create volume
				string volumeName;
				int retry = 0;
				for ( ; retry < 5 ; retry++ )
				{
					volumeName = createEC2Volume(volumeSize);
				
					if ( volumeName == "failed" || volumeName.empty() )
						retry = retry;
					else
						break;
				}
			
				if ( retry >= 5 )
				{
					cout << " *** ERROR: Failed to create a Volume for dbroot " << *pt1 << endl;
					exceptionControl("addDbroot", API_FAILURE);
				}

				string autoTagging;
				string systemName;
		
				try {
					getSystemConfig("AmazonAutoTagging", autoTagging);
					getSystemConfig("SystemName", systemName);
				}
				catch(...) {}

				if ( autoTagging == "y" ) {
					string tagValue = systemName + "-dbroot" + itoa(*pt1);
					createEC2tag( volumeName, "Name", tagValue );
				}

				//get device name based on dbroot ID
				string deviceName = getAWSdeviceName( *pt1 );

				//attach volumes to local instance
				retry = 0;
				for ( ; retry < 5 ; retry++ )
				{
					if (!attachEC2Volume(volumeName, deviceName, localInstance)) {
						detachEC2Volume(volumeName);
					}
					else
						break;
				}

				if ( retry >= 5 )
				{
					cout << " *** ERROR: Volume " << volumeName << " failed to attach to local instance" << endl;
					exceptionControl("addDbroot", API_FAILURE);
				}
			
				//format attached volume
				cout << "  Formatting DBRoot #" << itoa(*pt1) << ", please wait..." << endl;
				string cmd = "mkfs.ext2 -F " + deviceName + " > /dev/null 2>&1";
				system(cmd.c_str());

				//detach
				detachEC2Volume(volumeName);

				string volumeNameID = "PMVolumeName" + itoa(*pt1);
				string deviceNameID = "PMVolumeDeviceName" + itoa(*pt1);
	
				//write volume and device name
				try {
					sysConfig->setConfig(Section, volumeNameID, volumeName);
					sysConfig->setConfig(Section, deviceNameID, deviceName);
				}
				catch(...)
				{}
	
				//update /etc/fstab with mount
				string entry = deviceName + " " + InstallDir + "/data" + itoa(*pt1) + " ext2 noatime,nodiratime,noauto 0 0";
	
				//use from addmodule later
				cmd = "echo " + entry + " >> " + InstallDir + "/local/etc/pm1/fstab";
				system(cmd.c_str());

				//send update pms
				distributeFstabUpdates(entry);
			}
		}
	
		//update Calpont.xml entries
		DBRootConfigList::iterator pt2 = dbrootlist.begin();
		for( ; pt2 != dbrootlist.end() ; pt2++)
		{
			string DBrootID = "DBRoot" + itoa(*pt2);
			string pathID = InstallDir + "/data" + itoa(*pt2);
	
			try {
				sysConfig->setConfig(Section, DBrootID, pathID);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRoot in the InfiniDB System Configuration file" << endl;
				exceptionControl("setConfig", API_FAILURE);
			}
		}

		try
		{
			sysConfig->write();
		}
		catch(...)
		{
			exceptionControl("sysConfig->write", API_FAILURE);
		}

		//get updated Calpont.xml distributed
		distributeConfigFile("system");

		//
		//send message to Process Monitor to add new dbroot to shared memory
		//
		pt2 = dbrootlist.begin();
		for( ; pt2 != dbrootlist.end() ; pt2++)
		{
			try
			{
				ByteStream obs;
		
				obs << (ByteStream::byte) ADD_DBROOT;
				obs << itoa(*pt2);
		
				sendStatusUpdate(obs, ADD_DBROOT);
			}
			catch(...)
			{
				exceptionControl("setSystemConfig", API_INVALID_PARAMETER);
			}
		}

		return;
	}

    /***************************************************************************
     *
     * Function:  distributeFstabUpdates
     *
     * Purpose:   distribute Fstab Updates
     *
     ****************************************************************************/

    void Oam::distributeFstabUpdates(std::string entry, std::string toPM)
	{
		ACK_FLAG ackflag = oam::ACK_YES;
        // build and send msg
        int returnStatus = sendMsgToProcMgr(FSTABUPDATE, toPM, FORCEFUL, ackflag, entry);

        if (returnStatus != API_SUCCESS)
            exceptionControl("distributeFstabUpdates", returnStatus);
	}

    /***************************************************************************
     *
     * Function:  assignDbroot
     *
     * Purpose:   assign DBRoot
     *
     ****************************************************************************/

    void Oam::assignDbroot(std::string toPM, DBRootConfigList& dbrootlist)
	{
		//make sure this new DBroot IDs aren't being used already
		try
		{
			systemStorageInfo_t t;
			t = getStorageConfig();

			DeviceDBRootList moduledbrootlist = boost::get<2>(t);

			DBRootConfigList::iterator pt3 = dbrootlist.begin();
			for( ; pt3 != dbrootlist.end() ; pt3++)
			{
				DeviceDBRootList::iterator pt = moduledbrootlist.begin();
				for( ; pt != moduledbrootlist.end() ; pt++)
				{
					string moduleID = itoa((*pt).DeviceID);
					DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
					for( ; pt1 != (*pt).dbrootConfigList.end() ; pt1++)
					{
						if ( *pt3 == *pt1) {
							cout << endl << "**** assignPmDbrootConfig Failed : DBRoot ID " + itoa(*pt3) + " already assigned to 'pm" + moduleID << "'" << endl;
							exceptionControl("assignPmDbrootConfig", API_INVALID_PARAMETER);
						}
					}
				}
			}
		}
		catch (exception& e)
		{
			cout << endl << "**** getStorageConfig Failed :  " << e.what() << endl;
		}

		//make sure it's exist and unassigned
		DBRootConfigList undbrootlist;
		try {
			getUnassignedDbroot(undbrootlist);
		}
		catch(...) {}

		if ( undbrootlist.empty() )
		{
			cout << endl << "**** assignPmDbrootConfig Failed : no available dbroots are unassigned" << endl;
			exceptionControl("assignPmDbrootConfig", API_INVALID_PARAMETER);
		}

		DBRootConfigList::iterator pt1 = dbrootlist.begin();
		for( ; pt1 != dbrootlist.end() ; pt1++)
		{
			bool found = false;
			DBRootConfigList::iterator pt2 = undbrootlist.begin();
			for( ; pt2 != undbrootlist.end() ; pt2++)
			{
				if ( *pt1 == * pt2 ) {
					found = true;
					break;
				}
			}

			if (!found)
			{
				cout << endl << "**** assignPmDbrootConfig Failed : dbroot "  << *pt1 << " doesn't exist" << endl;
				exceptionControl("assignPmDbrootConfig", API_INVALID_PARAMETER);
			}
		}

		string toPMID = toPM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;

		//get dbroots ids for to PM
		DBRootConfigList todbrootConfigList;
		try
		{
			getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

			cout << "DBRoot IDs assigned to '" + toPM + "' = ";

			DBRootConfigList::iterator pt = todbrootConfigList.begin();
			for( ; pt != todbrootConfigList.end() ;)
			{
				cout << itoa(*pt);
				pt++;
				if (pt != todbrootConfigList.end())
					cout << ", ";
			}
			cout << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		cout << endl << "Changes being applied" << endl << endl;

		//added entered dbroot IDs to to-PM list and do Gluster assign if needed
		string GlusterConfig = "n";
		try {
			getSystemConfig( "GlusterConfig", GlusterConfig);
		}
		catch(...)
		{
			GlusterConfig = "n";
		}

		DBRootConfigList::iterator pt3 = dbrootlist.begin();
		for( ; pt3 != dbrootlist.end() ; pt3++)
		{
			todbrootConfigList.push_back(*pt3);

			if ( GlusterConfig == "y")
			{
				try {
					string errmsg;
					int ret = glusterctl(oam::GLUSTER_ASSIGN, itoa(*pt3), toPMID, errmsg);
					if ( ret != 0 )
					{
						cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(*pt3) + " to pm" + toPMID + ", error: " + errmsg << endl;
						exceptionControl("assignPmDbrootConfig", API_FAILURE);
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** glusterctl API exception:  " << e.what() << endl;
					cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(*pt3) + " to pm" + toPMID << endl;
					exceptionControl("assignPmDbrootConfig", API_FAILURE);
				}
				catch (...)
				{
					cout << endl << "**** glusterctl API exception: UNKNOWN"  << endl;
					cerr << "FAILURE: Error assigning gluster dbroot# " + itoa(*pt3) + " to pm" + toPMID << endl;
					exceptionControl("assignPmDbrootConfig", API_FAILURE);
				}
			}
		}

		try
		{
			setPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);
		}
		catch (exception& e)
		{
			cout << endl << "**** setPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		//get dbroots ids for to-PM
		try
		{
			todbrootConfigList.clear();
			getPmDbrootConfig(atoi(toPMID.c_str()), todbrootConfigList);

			cout << "DBRoot IDs assigned to '" + toPM + "' = ";

			DBRootConfigList::iterator pt = todbrootConfigList.begin();
			for( ; pt != todbrootConfigList.end() ;)
			{
				cout << itoa(*pt);
				pt++;
				if (pt != todbrootConfigList.end())
					cout << ", ";
			}
			cout << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** getPmDbrootConfig Failed for '" << toPM << "' : " << e.what() << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		//get old System DBRoot Count
		int oldSystemDbRootCount = 0;
		try
		{
			getSystemConfig("DBRootCount", oldSystemDbRootCount);
			if (oldSystemDbRootCount < 1)
				throw runtime_error("SystemDbRootCount not > 0");
		}
		catch (exception& e)
		{
			cout << endl << "**** getSystemConfig for DBRootCount failed; " <<
				e.what() << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		//set new System DBRoot Count
		try
		{
			setSystemDBrootCount();
		}
		catch (exception& )
		{
			cout << endl << "**** setSystemDBrootCount Failed" << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		//set FilesPerColumnPartition
		try
		{
			setFilesPerColumnPartition( oldSystemDbRootCount );
		}
		catch (exception& )
		{
			cout << endl << "**** setFilesPerColumnPartition Failed" << endl;
			exceptionControl("assignPmDbrootConfig", API_FAILURE);
		}

		//get updated Calpont.xml distributed
		distributeConfigFile("system");

		return;
	}

    /***************************************************************************
     *
     * Function:  unassignDbroot
     *
     * Purpose:   unassign DBRoot
     *
     ****************************************************************************/

    void Oam::unassignDbroot(std::string residePM, DBRootConfigList& dbrootlist)
	{
		string residePMID = residePM.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);;

		//get dbroots ids for reside PM
		DBRootConfigList residedbrootConfigList;
		try
		{
			getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

			cout << "DBRoot IDs assigned to '" + residePM + "' = ";

			DBRootConfigList::iterator pt = residedbrootConfigList.begin();
			for( ; pt != residedbrootConfigList.end() ;)
			{
				cout << itoa(*pt);
				pt++;
				if (pt != residedbrootConfigList.end())
					cout << ", ";
			}
			cout << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** getPmDbrootConfig Failed for '" << residePM << "' : " << e.what() << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		cout << endl << "Changes being applied" << endl << endl;

		//remove entered dbroot IDs from reside PM list
		DBRootConfigList::iterator pt1 = dbrootlist.begin();
		for( ; pt1 != dbrootlist.end() ; pt1++)
		{
			DBRootConfigList::iterator pt2 = residedbrootConfigList.begin();
			for( ; pt2 != residedbrootConfigList.end() ; pt2++)
			{
				if ( *pt2 == *pt1 ) {

					dbrootList dbroot1;
					dbroot1.push_back(itoa(*pt1));

					//send msg to unmount dbroot if module is not offline
					int opState;
					bool degraded;
					try {
						getModuleStatus(residePM, opState, degraded);
					}
					catch(...)
					{}
		
					if (opState != oam::AUTO_OFFLINE || opState != oam::AUTO_DISABLED) {
						try
						{
							mountDBRoot(dbroot1, false);
						}
						catch (exception& )
						{
							writeLog("ERROR: dbroot failed to unmount", LOG_TYPE_ERROR );
							cout << endl << "ERROR: umountDBRoot api failure" << endl;
							exceptionControl("unassignPmDbrootConfig", API_FAILURE);
						}
					}

					//get volume name and detach it

					string volumeNameID = "PMVolumeName" + itoa(*pt1);
					string volumeName = oam::UnassignedName;
					try {
						getSystemConfig( volumeNameID, volumeName);
					}
					catch(...)
					{}
	
					if ( volumeName != oam::UnassignedName )
						detachEC2Volume(volumeName);

					residedbrootConfigList.erase(pt2);

					break;
				}
			}
		}

		try
		{
			setPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);
		}
		catch (exception& e)
		{
			cout << endl << "**** setPmDbrootConfig Failed for '" << residePM << "' : " << e.what() << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		//get dbroots ids for reside-PM
		try
		{
			residedbrootConfigList.clear();
			getPmDbrootConfig(atoi(residePMID.c_str()), residedbrootConfigList);

			cout << "DBRoot IDs assigned to '" + residePM + "' = ";

			DBRootConfigList::iterator pt = residedbrootConfigList.begin();
			for( ; pt != residedbrootConfigList.end() ;)
			{
				cout << itoa(*pt);
				pt++;
				if (pt != residedbrootConfigList.end())
					cout << ", ";
			}
			cout << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** getPmDbrootConfig Failed for '" << residePM << "' : " << e.what() << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		//get old System DBRoot Count
		int oldSystemDbRootCount = 0;
		try
		{
			getSystemConfig("DBRootCount", oldSystemDbRootCount);
			if (oldSystemDbRootCount < 1)
				throw runtime_error("SystemDbRootCount not > 0");
		}
		catch (exception& e)
		{
			cout << endl << "**** getSystemConfig for DBRootCount failed; " <<
				e.what() << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		//set new System DBRoot Count
		try
		{
			setSystemDBrootCount();
		}
		catch (exception& )
		{
			cout << endl << "**** setSystemDBrootCount Failed" << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		//set FilesPerColumnPartition
		try
		{
			setFilesPerColumnPartition( oldSystemDbRootCount );
		}
		catch (exception& )
		{
			cout << endl << "**** setFilesPerColumnPartition Failed" << endl;
			exceptionControl("unassignPmDbrootConfig", API_FAILURE);
		}

		return;
	}

    /***************************************************************************
     *
     * Function:  getUnassignedDbroot
     *
     * Purpose:   get unassigned DBRoot list
     *
     ****************************************************************************/

    void Oam::getUnassignedDbroot(DBRootConfigList& dbrootlist)
	{

		//get assigned dbroots IDs
		DBRootConfigList dbrootConfigList;
		try
		{
			getSystemDbrootConfig(dbrootConfigList);

		}
		catch(...) {}

        // get string variables
		Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";
		for ( int dbrootID = 1 ; dbrootID < MAX_DBROOT ; dbrootID++)
		{
			string dbrootPath;
			try
			{
				dbrootPath = sysConfig->getConfig(Section, "DBRoot" + itoa(dbrootID));
			}
			catch(...) {}

			if (dbrootPath.empty() || dbrootPath == oam::UnassignedName)
				continue;

			bool found = false;
			DBRootConfigList::iterator pt = dbrootConfigList.begin();
			for( ; pt != dbrootConfigList.end() ; pt++)
			{
				if ( dbrootID == *pt ) {
					found = true;
					break;
				}
			}

			if (!found)
				dbrootlist.push_back(dbrootID);
		}

		return;
	}

    /***************************************************************************
     *
     * Function:  removeDbroot
     *
     * Purpose:   remove DBRoot
     *
     ****************************************************************************/

    void Oam::removeDbroot(DBRootConfigList& dbrootlist)
	{
		int SystemDBRootCount = 0;
		string cloud;
		string DBRootStorageType;
		try {
			getSystemConfig("DBRootCount", SystemDBRootCount);
			getSystemConfig("Cloud", cloud);
			getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}

		int dbrootNumber = dbrootlist.size();

		if ( dbrootNumber < 1 )
		{
			cout << "ERROR: Failed remove, total Number of DBRoots to remove is less than 1 " << endl;
			exceptionControl("removeDbroot", API_INVALID_PARAMETER);
		}

		Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
        string Section = "SystemConfig";

		//check if dbroot requested to be removed is empty and dboot #1 is requested to be removed
		DBRootConfigList::iterator pt = dbrootlist.begin();
		for( ; pt != dbrootlist.end() ; pt++)
		{
			int dbrootID = *pt;

			//see if dbroot exist
			string DBrootpath = "DBRoot" + itoa(dbrootID);
			string dbrootdir;
			try {
				dbrootdir = sysConfig->getConfig(Section, DBrootpath);
			}
			catch(...)
			{}

			if ( dbrootdir.empty() || dbrootdir == oam::UnassignedName )
			{
				cout << "ERROR: DBRoot doesn't exist: " << itoa(dbrootID) << endl;
				exceptionControl("removeDbroot", API_FAILURE);
			}

			if ( dbrootID == 1 )
			{
				cout << "ERROR: Failed remove, can't remove dbroot #1" << endl;
				exceptionControl("removeDbroot", API_INVALID_PARAMETER);
			}

			//check if dbroot is empty
			bool isEmpty = false;
			string errMsg;
			try {
				BRM::DBRM dbrm;
				if ( dbrm.isDBRootEmpty(dbrootID, isEmpty, errMsg) != 0) {
					cout << "ERROR: isDBRootEmpty API error, dbroot #" << itoa(dbrootID) << " :" << errMsg << endl;
					exceptionControl("removeDbroot", API_FAILURE);
				}
			}
			catch (exception& )
			{}

			if (!isEmpty)
			{
				cout << "ERROR: Failed remove, dbroot #" << itoa(dbrootID) << " is not empty" << endl;
				exceptionControl("removeDbroot", API_FAILURE);
			}

			//check if dbroot is assigned to a pm and if so, unassign it
			int pmid = 0;
			try {
				getDbrootPmConfig(dbrootID, pmid);
			}
			catch (exception& )
			{}

			if ( pmid > 0 )
			{
				//unassign dbroot from pm
				DBRootConfigList pmdbrootlist;
				pmdbrootlist.push_back(dbrootID);

				try {
					unassignDbroot("pm" + itoa(pmid), pmdbrootlist);
				}
				catch (exception& )
				{
					cout << endl << "**** unassignDbroot Failed" << endl;
					exceptionControl("removeDbroot", API_FAILURE);
				}
			}

			try {
				sysConfig->delConfig(Section, DBrootpath);
			}
			catch(...)
			{
				cout << "ERROR: Problem deleting DBRoot in the InfiniDB System Configuration file" << endl;
				exceptionControl("deleteConfig", API_FAILURE);
			}
		}

		try
		{
			sysConfig->write();
		}
		catch(...)
		{
			exceptionControl("sysConfig->write", API_FAILURE);
		}

		//get updated Calpont.xml distributed
		distributeConfigFile("system");

		//
		//send message to Process Monitor to add new dbroot to shared memory
		//
		pt = dbrootlist.begin();
		for( ; pt != dbrootlist.end() ; pt++)
		{
			try
			{
				ByteStream obs;
		
				obs << (ByteStream::byte) REMOVE_DBROOT;
				obs << itoa(*pt);
		
				sendStatusUpdate(obs, REMOVE_DBROOT);
			}
			catch(...)
			{
				exceptionControl("setSystemConfig", API_INVALID_PARAMETER);
			}
		}

		return;
	}

	//current amazon max dbroot id support = 190;
	string PMdeviceName = "/dev/sd";
	string deviceLetter[] = {"g","h","i","j","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","end"};

    /***************************************************************************
     *
     * Function:  getAWSdeviceName
     *
     * Purpose:   get AWS Device Name for DBRoot ID
     *
     ****************************************************************************/

	std::string Oam::getAWSdeviceName( const int dbrootid)
	{
		//calulate id numbers from DBRoot ID
		int lid = (dbrootid-1) / 10;
		int did = dbrootid - (dbrootid * lid);

		return PMdeviceName + deviceLetter[lid] + itoa(did);
	}

    /***************************************************************************
     *
     * Function:  setSystemDBrootCount
     *
     * Purpose:   set System DBRoot Count
     *
     ****************************************************************************/

    void Oam::setSystemDBrootCount()
	{
		//set the system dbroot number
		try
		{
			DBRootConfigList dbrootConfigList;
			getSystemDbrootConfig(dbrootConfigList);

			try {
				setSystemConfig("DBRootCount", dbrootConfigList.size());
			}
			catch(...)
			{
				writeLog("ERROR: setSystemConfig DBRootCount " + dbrootConfigList.size() , LOG_TYPE_ERROR );
				cout << endl << "ERROR: setSystemConfig DBRootCount " + dbrootConfigList.size() << endl;
				exceptionControl("setSystemConfig", API_FAILURE);
			}
		}
		catch(...)
		{
			writeLog("ERROR: getSystemDbrootConfig ", LOG_TYPE_ERROR );
			cout << endl << "ERROR: getSystemDbrootConfig "  << endl;
			exceptionControl("getSystemDbrootConfig", API_INVALID_PARAMETER);
		}

		return;
	}

    /***************************************************************************
     *
     * Function:  setFilesPerColumnPartition
     *
     * Purpose:   set FilesPerColumnPartition
	 *            This function takes the old DBRootCount as an input arg
	 *            and expects that the new DBRootCount has been set in the
	 *            Calpont.xml file.  Function assumes oldSystemDBRootCount
	 *            has already been validated to be > 0 (else we could get a
	 *            divide by 0 error).
     *
     ****************************************************************************/

    void Oam::setFilesPerColumnPartition( int oldSystemDBRootCount )
	{
		int newSystemDBRootCount = 0;
		int oldFilesPerColumnPartition = 4;

		try {
			getSystemConfig("DBRootCount", newSystemDBRootCount);
		}
		catch(...)
		{
			writeLog("ERROR: getSystemConfig DBRootCount ", LOG_TYPE_ERROR );
			cout << endl << "ERROR: getSystemConfig DBRootCount"  << endl;
			exceptionControl("setFilesPerColumnPartition", API_INVALID_PARAMETER);
		}

		try {
			getSystemConfig("FilesPerColumnPartition", oldFilesPerColumnPartition);
		}
		catch(...)
		{
			writeLog("ERROR: getSystemConfig FilesPerColumnPartition ", LOG_TYPE_ERROR );
			cout << endl << "ERROR: getSystemConfig FilesPerColumnPartition"  << endl;
			exceptionControl("setFilesPerColumnPartition", API_INVALID_PARAMETER);
		}

		if ( oldFilesPerColumnPartition != oldSystemDBRootCount *
			(oldFilesPerColumnPartition/oldSystemDBRootCount) ) {
			writeLog("ERROR: old FilesPerColumnPartition not a multiple of DBRootCount", LOG_TYPE_ERROR );
			cout << endl << "ERROR: old FilesPerColumnPartition not a multiple of DBRootCount " << endl;
			exceptionControl("setFilesPerColumnPartition", API_INVALID_PARAMETER);
		}

		int newFilesPerColumnPartition = (oldFilesPerColumnPartition/oldSystemDBRootCount) * newSystemDBRootCount;

		try {
			setSystemConfig("FilesPerColumnPartition", newFilesPerColumnPartition);
		}
		catch(...)
		{
			writeLog("ERROR: setSystemConfig FilesPerColumnPartition " + newFilesPerColumnPartition , LOG_TYPE_ERROR );
			cout << endl << "ERROR: setSystemConfig FilesPerColumnPartition " + newFilesPerColumnPartition << endl;
			exceptionControl("setFilesPerColumnPartition", API_FAILURE);
		}
	}

#pragma pack(push,1)
	struct NotifyMsgStruct
	{
		uint32_t magic;
		uint32_t msgno;
		char node[8];
		uint32_t paylen;
	};
#pragma pack(pop)

    /***************************************************************************
     *
     * Function:  sendDeviceNotification
     *
     * Purpose:   send Device Notification Msg
     *
     ****************************************************************************/

    int Oam::sendDeviceNotification(std::string deviceName, NOTIFICATION_TYPE type, std::string payload)
    {
		//first check if there are any CMP entries configured
		try {
			Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());
			string CMPsection = "CMP";
			for ( int id = 1 ;; id++)
			{
				string CMP = CMPsection + itoa(id);
				try {
					string ipaddr = sysConfig->getConfig(CMP, "IPAddr");
	
					if (ipaddr.empty())
						return API_SUCCESS;

					string port = sysConfig->getConfig(CMP, "Port");

					NotifyMsgStruct msg;
					memset(&msg, 0, sizeof(msg));
					msg.magic = NOTIFICATIONKEY;
					msg.msgno = type;
					strncpy(msg.node, deviceName.c_str(), 7);
					if (!payload.empty())
						msg.paylen = payload.length() + 1;

					// send notification msg to this exchange
					try
					{
						int ds = -1;
						ds = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
						struct sockaddr_in serv_addr;
						struct in_addr la;
						::inet_aton(ipaddr.c_str(), &la);
						memset(&serv_addr, 0, sizeof(serv_addr));
						serv_addr.sin_family = AF_INET;
						serv_addr.sin_addr.s_addr = la.s_addr;
						serv_addr.sin_port = htons(atoi(port.c_str()));
						int rc = -1;
						rc = ::connect(ds, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
						if (rc < 0) throw runtime_error("socket connect error");
						rc = ::write(ds, &msg, sizeof(msg));
						if (rc < 0) throw runtime_error("socket write error");
						if (msg.paylen > 0)
						{
							rc = ::write(ds, payload.c_str(), msg.paylen);
							if (rc < 0) throw runtime_error("socket write error");
						}
						::shutdown(ds, SHUT_RDWR);
						::close(ds);
					}
					catch (std::runtime_error&)
					{
						//There's other reasons, but this is the most likely...
						return API_CONN_REFUSED;
					}
					catch (std::exception&)
					{
						return API_FAILURE;
					}
					catch (...)
					{
						return API_FAILURE;
					}
				}
				catch (...) {
					return API_SUCCESS;
				}
			}
		}
		catch (...) {}
		
		return API_SUCCESS;
	}

    /***************************************************************************
     *
     * Function:  actionMysqlCalpont
     *
     * Purpose:   mysql-Calpont service command
     *
     ****************************************************************************/

    void Oam::actionMysqlCalpont(MYSQLCALPONT_ACTION action)
	{
		// check if mysql-Capont is installed
		string mysqlscript = InstallDir + "/mysql/mysql-Calpont";
        if (access(mysqlscript.c_str(), X_OK) != 0)
            exceptionControl("actionMysqlCalpont", API_FILE_OPEN_ERROR);

		string command;

		// get current Module name
		string moduleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			moduleName = boost::get<0>(st);
		}
		catch (...) 
		{}

        switch(action)
        {
 			case MYSQL_START:
			{
				command = "start > /dev/null 2>&1";
				break;
			}
			case MYSQL_STOP:
			{
				command = "stop > /dev/null 2>&1";

				//set process status
				try
				{
					setProcessStatus("mysqld", moduleName, MAN_OFFLINE, 0);
				}
				catch(...)
				{}

				break;
			}
			case MYSQL_RESTART:
			{
				command = "restart > /dev/null 2>&1";
				break;
			}
			case MYSQL_RELOAD:
			{
				command = "reload > /dev/null 2>&1";
				break;
			}
			case MYSQL_FORCE_RELOAD:
			{
				command = "force-reload > /dev/null 2>&1";
				break;
			}
			case MYSQL_STATUS:
			{
				command = "status > /tmp/mysql.status";
				break;
			}

			default:
			{
            	exceptionControl("actionMysqlCalpont", API_INVALID_PARAMETER);
			}
		}

		string cmd = mysqlscript + " " + command;
		int status = system(cmd.c_str());
		if (WEXITSTATUS(status) != 0 && action != MYSQL_STATUS)
			exceptionControl("actionMysqlCalpont", API_FAILURE);

		if (action == MYSQL_START || action == MYSQL_RESTART) {
			//get pid
			cmd = "cat " + InstallDir + "/mysql/db/*.pid > /tmp/mysql.pid";
			system(cmd.c_str());
			ifstream oldFile ("/tmp/mysql.pid");
			char line[400];
			string pid;
			while (oldFile.getline(line, 400))
			{
				pid = line;
				break;
			}
			oldFile.close();

			//set process status
			try
			{
				setProcessStatus("mysqld", moduleName, ACTIVE, atoi(pid.c_str()));
			}
			catch(...)
			{}
		}

		if (action == MYSQL_STATUS ) {
			ProcessStatus procstat;
			getProcessStatus("mysqld", moduleName, procstat);
			int state = procstat.ProcessOpState;
			pid_t pidStatus = procstat.ProcessID;

			if (checkLogStatus("/tmp/mysql.status", "MySQL running")) {
				if ( state != ACTIVE )
				{
					//get pid
					cmd = "cat " + InstallDir + "/mysql/db/*.pid > /tmp/mysql.pid";
					system(cmd.c_str());
					ifstream oldFile ("/tmp/mysql.pid");
					char line[400];
					string pid;
					while (oldFile.getline(line, 400))
					{
						pid = line;
						break;
					}
					oldFile.close();

					//set process status
					try
					{
						setProcessStatus("mysqld", moduleName, ACTIVE, atoi(pid.c_str()));
					}
					catch(...)
					{}

					return;
				}
				else
				{	//check if pid has changed
					cmd = "cat " + InstallDir + "/mysql/db/*.pid > /tmp/mysql.pid";
					system(cmd.c_str());
					ifstream oldFile ("/tmp/mysql.pid");
					char line[400];
					string pid;
					while (oldFile.getline(line, 400))
					{
						pid = line;
						break;
					}
					oldFile.close();

					if ( pidStatus != atoi(pid.c_str()) )
					{
						//set process status
						try
						{
							setProcessStatus("mysqld", moduleName, ACTIVE, atoi(pid.c_str()));
						}
						catch(...)
						{}
					}
				}

				//check module status, if DEGRADED set to ACTIVE
				int opState;
				bool degraded;
				try {
					getModuleStatus(moduleName, opState, degraded);
				}
				catch(...)
				{}
		
				if (opState == oam::DEGRADED) {
					try
					{
						setModuleStatus(moduleName, ACTIVE);
					}
					catch(...)
					{
						exceptionControl("processInitFailure", API_FAILURE);
					}
				}

			}
			else
			{
				if ( state != MAN_OFFLINE )
				{
					//set process status
					try
					{
						setProcessStatus("mysqld", moduleName, MAN_OFFLINE, 0);
					}
					catch(...)
					{}
				}

				//check module status, if ACTIVE set to DEGRADED
				int opState;
				bool degraded;
				try {
					getModuleStatus(moduleName, opState, degraded);
				}
				catch(...)
				{}
		
				if (opState == oam::ACTIVE) {
					try
					{
						setModuleStatus(moduleName, DEGRADED);
					}
					catch(...)
					{
						exceptionControl("processInitFailure", API_FAILURE);
					}
				}
				return;
			}
		}

		return;
	}

    /******************************************************************************************
     * @brief	run DBHealth Check
     *
     * purpose:	test the health of the DB
     *
     ******************************************************************************************/
	void Oam::checkDBFunctional(bool action)
	{
		ByteStream msg;
        ByteStream receivedMSG;

		// only make call if system is active
		SystemStatus systemstatus;
		try {
			getSystemStatus(systemstatus);
		}
		catch (exception& )
		{}
		
		if (systemstatus.SystemOpState != oam::ACTIVE )
            exceptionControl("checkDBHealth", API_INVALID_STATE);

        SystemModuleTypeConfig systemmoduletypeconfig;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("checkDBHealth", API_FAILURE);
        }

		// get Server Type Install ID
		int serverTypeInstall = oam::INSTALL_NORMAL;
		string OAMParentModuleName;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			OAMParentModuleName = boost::get<3>(st);
			serverTypeInstall = boost::get<5>(st);
		}
		catch (...) {
       		exceptionControl("getMyProcessStatus", API_FAILURE);
		}

		string module;
		switch ( serverTypeInstall ) {
			case (oam::INSTALL_NORMAL):
			case (oam::INSTALL_COMBINE_DM_UM):
			{
				module = "um1";
				break;
			}
			case (oam::INSTALL_COMBINE_PM_UM):
			case (oam::INSTALL_COMBINE_DM_UM_PM):
			{
				module = OAMParentModuleName;
				break;
			}
		}

	// setup message
	msg << (ByteStream::byte) RUN_DBHEALTH_CHECK;
	msg << (ByteStream::byte) action;

	try
	{
		//send the msg to Server Monitor
		MessageQueueClient servermonitor(module + "_ServerMonitor");
		servermonitor.write(msg);

		// wait 30 seconds for ACK from Server Monitor
		struct timespec ts = { 30, 0 };

		receivedMSG = servermonitor.read(&ts);
		if (receivedMSG.length() > 0)
		{
			ByteStream::byte returnType;
			receivedMSG >> returnType;

			if ( returnType == RUN_DBHEALTH_CHECK ) {
				ByteStream::byte returnStatus;
				receivedMSG >> returnStatus;
				if ( returnStatus == oam::API_SUCCESS ) {
					// succesfull
					servermonitor.shutdown();
					return;
				}
			}
			// shutdown connection
			servermonitor.shutdown();
	
			exceptionControl("checkDBHealth", API_FAILURE);
		}
		else
		{ // timeout
			// shutdown connection
			servermonitor.shutdown();
	
			exceptionControl("checkDBHealth", API_TIMEOUT);
		}
	}
	catch(...)
	{
		exceptionControl("checkDBHealth", API_FAILURE);
		return;
	}

	return;
	}

    /***************************************************************************
     *
     * Function:  validateModule
     *
     * Purpose:   Validate Module Name
     *
     ****************************************************************************/

    int Oam::validateModule(const std::string name)
    {
		if ( name.size() < 3 )
			// invalid ID
            return API_INVALID_PARAMETER;
		
		string moduletype = name.substr(0,MAX_MODULE_TYPE_SIZE);
		int moduleID = atoi(name.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		if ( moduleID < 1 )
			// invalid ID
            return API_INVALID_PARAMETER;

        SystemModuleTypeConfig systemmoduletypeconfig;

        try
        {
            getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            return API_INVALID_PARAMETER;
        }

        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if (systemmoduletypeconfig.moduletypeconfig[i].ModuleType == moduletype ) {
				if (systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
					return API_INVALID_PARAMETER;

				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					if ( name == (*pt).DeviceName )
						return API_SUCCESS;
				}
			}
        }
        return API_INVALID_PARAMETER;
    }

    /***************************************************************************
     *
     * Function:  getEC2InstanceIpAddress
     *
     * Purpose:   Check Amazon EC2 is running and returns Private IP address
     *
     ****************************************************************************/

    std::string Oam::getEC2InstanceIpAddress(std::string instanceName)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh getPrivateIP " + instanceName + " > /tmp/getCloudIP_" + instanceName;
		system(cmd.c_str());

		if (checkLogStatus("/tmp/getCloudIP_" + instanceName, "stopped") )
			return "stopped";

		if (checkLogStatus("/tmp/getCloudIP_" + instanceName, "terminated") )
			return "terminated";

		// get IP Address
		string IPAddr;
		string file = "/tmp/getCloudIP_" + instanceName;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			IPAddr = line;
		}
		oldFile.close();

		if (isValidIP(IPAddr))
			return IPAddr;

		return "terminated";
	}

    /***************************************************************************
     *
     * Function:  getEC2LocalInstance
     *
     * Purpose:   Get Amazon EC2 local Instance Name
     *
     ****************************************************************************/

    std::string Oam::getEC2LocalInstance(std::string name)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh getInstance  > /tmp/getInstanceInfo_" + name;
		int status = system(cmd.c_str());
		if (WEXITSTATUS(status) != 0 )
			return "failed";

		// get Instance Name
		string instanceName;
		string file = "/tmp/getInstanceInfo_" + name;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			instanceName = line;
		}
		oldFile.close();

		return instanceName;

	}

    /***************************************************************************
     *
     * Function:  getEC2LocalInstanceType
     *
     * Purpose:   Get Amazon EC2 local Instance Type
     *
     ****************************************************************************/

    std::string Oam::getEC2LocalInstanceType(std::string name)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh getType  > /tmp/getInstanceType_" + name;
		int status = system(cmd.c_str());
		if (WEXITSTATUS(status) != 0 )
			return "failed";

		// get Instance Name
		string instanceType;
		string file = "/tmp/getInstanceType_" + name;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			instanceType = line;
		}
		oldFile.close();

		return instanceType;

	}

    /***************************************************************************
     *
     * Function:  launchEC2Instance
     *
     * Purpose:   Launch Amazon EC2 Instance
     *
     ****************************************************************************/

    std::string Oam::launchEC2Instance( const std::string name, const std::string IPAddress, const std::string type, const std::string group)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh launchInstance " + IPAddress + " " + type + " " + group + " > /tmp/getInstance_" + name;
		int status = system(cmd.c_str());
		if (WEXITSTATUS(status) != 0 )
			return "failed";

		if (checkLogStatus("/tmp/getInstance", "Required") )
			return "failed";

		// get Instance ID
		string instance;
		string file = "/tmp/getInstance_" + name;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			instance = line;
		}
		oldFile.close();

		if (instance.empty())
			return "failed";

		if (instance == "unknown")
			return "failed";

		if (instance.find("i-") == string::npos)
			return "failed";

		return instance;
	}

    /***************************************************************************
     *
     * Function:  terminateEC2Instance
     *
     * Purpose:   Terminate Amazon EC2 Instance
     *
     ****************************************************************************/

    void Oam::terminateEC2Instance(std::string instanceName)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh terminateInstance " + instanceName + " > /tmp/terminateEC2Instance_" + instanceName;
		system(cmd.c_str());

		return;
	}

    /***************************************************************************
     *
     * Function:  stopEC2Instance
     *
     * Purpose:   Terminate Amazon EC2 Instance
     *
     ****************************************************************************/

    void Oam::stopEC2Instance(std::string instanceName)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh stopInstance " + instanceName + " > /tmp/stopEC2Instance_" + instanceName;
		system(cmd.c_str());

		return;
	}

    /***************************************************************************
     *
     * Function:  startEC2Instance
     *
     * Purpose:   Start Amazon EC2 Instance
     *
     ****************************************************************************/

    bool Oam::startEC2Instance(std::string instanceName)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh startInstance " + instanceName + " > /tmp/startEC2Instance_" + instanceName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return false;

		return true;
	}

    /***************************************************************************
     *
     * Function:  assignElasticIP
     *
     * Purpose:   assign Elastic IP Address on Amazon
     *
     ****************************************************************************/

    bool Oam::assignElasticIP(std::string instanceName, std::string IpAddress)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh assignElasticIP " + instanceName + " " + IpAddress + " > /tmp/assignElasticIP_" + instanceName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
            exceptionControl("assignElasticIP", oam::API_FAILURE);

		return true;
	}

    /***************************************************************************
     *
     * Function:  deassignElasticIP
     *
     * Purpose:   deassign Elastic IP Address on Amazon
     *
     ****************************************************************************/

    bool Oam::deassignElasticIP(std::string IpAddress)
	{
		// run script to get Instance status and IP Address
		string cmd = InstallDir + "/bin/IDBInstanceCmds.sh deassignElasticIP " + IpAddress + " > /tmp/deassignElasticIP_" + IpAddress;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
            exceptionControl("deassignElasticIP", oam::API_FAILURE);

		return true;
	}

    /***************************************************************************
     *
     * Function:  getEC2VolumeStatus
     *
     * Purpose:   get Volume Status
     *
     ****************************************************************************/

	std::string Oam::getEC2VolumeStatus(std::string volumeName)
	{
		// run script to get Volume Status
		string cmd = InstallDir + "/bin/IDBVolumeCmds.sh describe " + volumeName + " > /tmp/getVolumeStatus_" + volumeName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return "failed";

		// get status
		string status;
		string file = "/tmp/getVolumeStatus_" + volumeName;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			status = line;
			break;
		}
		oldFile.close();

		return status;
	}

    /***************************************************************************
     *
     * Function:  createEC2Volume
     *
     * Purpose:   create a EC2 Volume
     *
     ****************************************************************************/

	std::string Oam::createEC2Volume(std::string size, std::string name)
	{
		// run script to get Volume Status
		string cmd = InstallDir + "/bin/IDBVolumeCmds.sh create " + size + " > /tmp/createVolumeStatus_" + name;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return "failed";

		// get status
		string volumeName;
		string file = "/tmp/createVolumeStatus_" + name;
		ifstream oldFile (file.c_str());
		char line[400];
		while (oldFile.getline(line, 400))
		{
			volumeName = line;
		}
		oldFile.close();

		if ( volumeName == "unknown" )
			return "failed";

		if ( volumeName == "unknown" )
			return "failed";

		if (volumeName.find("vol-") == string::npos)
			return "failed";

		return volumeName;
	}

    /***************************************************************************
     *
     * Function:  attachEC2Volume
     *
     * Purpose:   attach EC2 Volume
     *
     ****************************************************************************/

	bool Oam::attachEC2Volume(std::string volumeName, std::string deviceName, std::string instanceName)
	{
		// add 1 retry if it fails by dettaching then attaching
		int ret = 0;
		string status;
		for ( int retry = 0 ; retry < 2 ; retry++ )
		{
			// run script to attach Volume
			string cmd = InstallDir + "/bin/IDBVolumeCmds.sh attach " + volumeName + " " + instanceName + " " + deviceName + " > /tmp/attachVolumeStatus_" + volumeName;
			ret = system(cmd.c_str());

			if (WEXITSTATUS(ret) == 0 )
				return true;
			
			//failing to attach, dettach and retry
			detachEC2Volume(volumeName);
		}

		if (ret == 0 )
			return true;
		else
			return false;
	}

    /***************************************************************************
     *
     * Function:  detachEC2Volume
     *
     * Purpose:   detach EC2 Volume
     *
     ****************************************************************************/

	bool Oam::detachEC2Volume(std::string volumeName)
	{
		// run script to attach Volume
		string cmd = InstallDir + "/bin/IDBVolumeCmds.sh detach " + volumeName + " > /tmp/detachVolumeStatus_" + volumeName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return false;

		return true;
	}

    /***************************************************************************
     *
     * Function:  deleteEC2Volume
     *
     * Purpose:   detach EC2 Volume
     *
     ****************************************************************************/

	bool Oam::deleteEC2Volume(std::string volumeName)
	{
		// run script to delete Volume
		string cmd = InstallDir + "/bin/IDBVolumeCmds.sh delete " + volumeName + " > /tmp/deleteVolumeStatus_" + volumeName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return false;

		return true;
	}

    /***************************************************************************
     *
     * Function:  createEC2tag
     *
     * Purpose:   create EC2 tag
     *
     ****************************************************************************/

	bool Oam::createEC2tag(std::string resourceName, std::string tagName, std::string tagValue)
	{
		// run script to create a tag
		string cmd = InstallDir + "/bin/IDBVolumeCmds.sh createTag " + resourceName + " " + tagName + " " + tagValue + " > /tmp/createTagStatus_" + resourceName;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) != 0 )
			return false;

		return true;
	}

	/******************************************************************************************
	* @brief	syslogAction
	*
	* purpose:	Take Action on Syslog Process
	*
	******************************************************************************************/
    void Oam::syslogAction( std::string action)
	{
#ifndef _MSC_VER
		writeLog("syslogAction: " + action, LOG_TYPE_DEBUG );

		string systemlog = "syslog";
	
		string fileName;
		getSystemConfig("SystemLogConfigFile", fileName);
		if (fileName == oam::UnassignedName ) {
			return;
		}
	
		string::size_type pos = fileName.find("syslog-ng",0);
		if (pos != string::npos)
			systemlog = "syslog-ng";
		else
		{
			pos = fileName.find("rsyslog",0);
			if (pos != string::npos)
				systemlog = "rsyslog";	
		}
	
		string cmd;
		if ( action == "sighup" ) {
			if ( systemlog == "syslog" || systemlog == "rsyslog")
				systemlog = systemlog + "d";
			cmd = "pkill -hup " + systemlog + " > /dev/null 2>&1";
		}
		else
		{
			int user;
			user = getuid();
			if (user == 0)
				cmd = "/etc/init.d/" + systemlog + " " + action + " > /dev/null 2>&1";
			else
				cmd = "sudo /etc/init.d/" + systemlog + " " + action + " > /dev/null 2>&1";
		}
		// take action on syslog service
		writeLog("syslogAction cmd: " + cmd, LOG_TYPE_DEBUG );
		system(cmd.c_str());
	
		// delay to give time for syslog to get up and going
		sleep(2);
#endif
	}

	/******************************************************************************************
	* @brief	dbrmctl
	*
	* purpose:	call dbrm control
	*
	******************************************************************************************/
	void Oam::dbrmctl(std::string command)
	{
		//reload DBRM with new configuration
		string cmd = InstallDir + "/bin/dbrmctl " + command + " > /dev/null 2>&1";
		system(cmd.c_str());
	
		return;
	}


	/******************************************************************************************
	* @brief	glusterctl
	*
	* purpose:	gluster control and glusteradd
	*
	* commands:	status - Used to check status of kuster and disk good/bad 
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*			setddc	- Set Number of gluster disk copies
	*					 argument = #
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*			assign - Used to assign a dbroot to a primary pm
	*					 argument1 = dbroot#
	*					 argument2 = pm#
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*			unassign - Used to assign a dbroot to a primary pm
	*					 argument1 = dbroot#
	*					 argument2 = pm#
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*			whohas - Used to get secondary pm for a dbroot for moving
	*					  argument1 = dbroot#
	*					return argument #2 - pm#
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*			add - Used to add new gluster pm and dbroot
	*					  argument1 = firstnewpm#
	*					  argument2 = firstnewdbroot#
	*					 returns
	*						NOTINSTALLED
	*						OK
	*						FAILED! erorrmsg
	*
 	******************************************************************************************/
	int Oam::glusterctl(GLUSTER_COMMANDS command, std::string argument1, std::string& argument2, std::string& errmsg)
	{
#ifndef _MSC_VER
		int user;
		user = getuid();
		string glustercmd = InstallDir + "/bin/glusterctl";

		if (access(glustercmd.c_str(), X_OK) != 0 )
			exceptionControl("glusterctl", API_DISABLED);

		errmsg = "";

		switch ( command ) {
			case (oam::GLUSTER_STATUS):
			{
				glustercmd = glustercmd + " status > /tmp/gluster_status.log 2>&1";

				int ret;
				ret = system(glustercmd.c_str());
				if ( WEXITSTATUS(ret) == 0 )
					return 0;

            	ret = checkGlusterLog("/tmp/gluster_status.log", errmsg);
				return ret;
			}

			case (oam::GLUSTER_SETDDC):
			{
				string copies = argument1;

				writeLog("glusterctl: GLUSTER_SETDDC: " + copies, LOG_TYPE_DEBUG );
				glustercmd = glustercmd + " setddc " + copies + " > /tmp/gluster_setddc.log 2>&1";
				int ret;
				ret = system(glustercmd.c_str());
				if ( WEXITSTATUS(ret) == 0 )
					return 0;

            	ret = checkGlusterLog("/tmp/gluster_setddc.log", errmsg);
				return ret;
			}

			case (oam::GLUSTER_ASSIGN):
			{

				string dbrootID = argument1;
				string pmID = argument2;

				writeLog("glusterctl call: GLUSTER_ASSIGN: dbroot = " + dbrootID + " pm = " + pmID, LOG_TYPE_DEBUG );
				glustercmd = glustercmd + " assign " + dbrootID + " " + pmID + " > /tmp/gluster_assign.log 2>&1";
				int ret;
				ret = system(glustercmd.c_str());
				writeLog("glusterctl return: GLUSTER_ASSIGN: dbroot = " + dbrootID + " pm = " + pmID, LOG_TYPE_DEBUG );
				if ( WEXITSTATUS(ret) == 0 )
					return 0;

            	ret = checkGlusterLog("/tmp/gluster_assign.log", errmsg);
				return ret;

				break;
			}

			case (oam::GLUSTER_WHOHAS):
			{

				string dbrootID = argument1;
				string msg;

				for ( int retry = 0 ; retry < 5 ; retry++ )
				{
					writeLog("glusterctl: GLUSTER_WHOHAS for dbroot : " + dbrootID, LOG_TYPE_DEBUG );
					glustercmd = glustercmd + " whohas " + dbrootID + " > /tmp/gluster_howhas.log 2>&1";
					system(glustercmd.c_str());
	
					int ret;
					ret = checkGlusterLog("/tmp/gluster_howhas.log", msg);
	
					if ( ret == 0 )
					{ // OK return, get pm list
						if ( msg.empty() )
						{
							writeLog("glusterctl: GLUSTER_WHOHAS: empty pm list", LOG_TYPE_ERROR );
							exceptionControl("glusterctl", API_FAILURE);
						}
						else
						{
							writeLog("glusterctl: GLUSTER_WHOHAS: pm list = " + msg, LOG_TYPE_DEBUG );
							argument2 = msg;
							return 0;
						}
					}

					// retry failure
					writeLog("glusterctl: GLUSTER_WHOHAS: failure, retrying (restarting gluster) " + msg, LOG_TYPE_ERROR );

					string cmd = "/etc/init.d/glusterd restart > /dev/null 2>&1";
					if (user != 0)
						cmd = "sudo " + cmd;

					system(cmd.c_str());

					sleep(1);
				}

				break;
			}

			case (oam::GLUSTER_UNASSIGN):
			{

				string dbrootID = argument1;
				string pmID = argument2;

				writeLog("glusterctl: GLUSTER_UNASSIGN: dbroot = " + dbrootID + " pm = " + pmID, LOG_TYPE_DEBUG );
				glustercmd = glustercmd + " unassign " + dbrootID + " " + pmID + " > /tmp/gluster_unassign.log 2>&1";
				int ret;
				ret = system(glustercmd.c_str());
				if ( WEXITSTATUS(ret) == 0 )
					return 0;

            	ret = checkGlusterLog("/tmp/gluster_unassign.log", errmsg);
				return ret;

				break;
			}

			case (oam::GLUSTER_ADD):
			{
				string pmID = argument1;
				string dbrootID = argument2;

				string glustercmd = InstallDir + "/bin/glusteradd";
				writeLog("glusterctl: GLUSTER_ADD: dbroot = " + dbrootID + " pm = " + pmID, LOG_TYPE_DEBUG );
				glustercmd = glustercmd + " " + pmID  + " " + dbrootID + " > /tmp/gluster_add.log 2>&1";
				int ret;
				//writeLog("glusterctl: cmd = " + glustercmd, LOG_TYPE_DEBUG );
				ret = system(glustercmd.c_str());
				if ( WEXITSTATUS(ret) == 0 )
					return 0;

            	ret = checkGlusterLog("/tmp/gluster_add.log", errmsg);
				return ret;

				break;
			}
			default:
				break;
		}
#endif
		return 0;
	}

    /***************************************************************************
     * PRIVATE FUNCTIONS
     ***************************************************************************/


    /***************************************************************************
     *
     * Function:  checkGlusterLog
     *
     * Purpose:   check Gluster Log after a Gluster control call
     *
     ****************************************************************************/
    int Oam::checkGlusterLog(std::string logFile, std::string& msg)
    {
		if (checkLogStatus(logFile, "OK")) 
		{
			if ( logFile == "/tmp/gluster_howhas.log" )
			{
				ifstream File(logFile.c_str());
	
				char line[100];
				string buf;
				while (File.getline(line, 100))
				{
					buf = line;
					string::size_type pos = buf.find("OK",0);
					if (pos != string::npos)
					{
						msg = buf.substr(3,100);
						return 0;
					}
				}
	
				msg = "";
				return 1;
			}

			msg = "";
			return 0;
		}

		if (checkLogStatus(logFile, "NOTINSTALLED")) {
			writeLog("checkGlusterLog: NOTINSTALLED", LOG_TYPE_DEBUG );
			exceptionControl("glusterctl", API_DISABLED);
		}

		if (checkLogStatus(logFile, "FAILED"))
		{
			ifstream File(logFile.c_str());

			char line[100];
			string buf;
			while (File.getline(line, 100))
			{
				buf = line;
				string::size_type pos = buf.find("FAILED",0);
				if (pos != string::npos)
				{
					msg = buf.substr(7,100);
					writeLog("checkGlusterLog: " + buf, LOG_TYPE_ERROR);
					return 1;
				}
			}

			writeLog("checkGlusterLog: FAILURE", LOG_TYPE_ERROR);

			if ( logFile == "/tmp/gluster_howhas.log" )
				return 2;
			else
				exceptionControl("glusterctl", API_FAILURE);
		}

		writeLog("checkGlusterLog: FAILURE - no log file match", LOG_TYPE_ERROR);
		exceptionControl("glusterctl", API_FAILURE);

		return 1;
	}

    /***************************************************************************
     *
     * Function:  exceptionControl
     *
     * Purpose:   exception control function
     *
     ****************************************************************************/

    void Oam::exceptionControl(std::string function, int returnStatus, const char* extraMsg)
    {
		std::string msg;
        switch(returnStatus)
        {
            case API_INVALID_PARAMETER:
            {
                msg = "Invalid Parameter passed in ";
                msg.append(function);
                msg.append(" API");
            }
            break;
            case API_FILE_OPEN_ERROR:
            {
                msg = "File Open error from ";
                msg.append(function);
                msg.append(" API");
            }
            break;
            case API_TIMEOUT:
            {
                msg = "Timeout error from ";
                msg.append(function);
                msg.append(" API");
            }
            break;
            case API_DISABLED:
            {
                msg = "API Disabled: ";
                msg.append(function);
            }
            break;
            case API_FILE_ALREADY_EXIST:
            {
                msg = "File Already Exist";
            }
            break;
            case API_ALREADY_IN_PROGRESS:
            {
                msg = "Already In Process";
            }
            break;
            case API_FAILURE_DB_ERROR:
            {
                msg = "Database Test Error";
            }
            break;
            case API_INVALID_STATE:
            {
                msg = "Target in an invalid state";
            }
            break;
            case API_READONLY_PARAMETER:
            {
                msg = "Parameter is Read-Only, can't update";
            }
            break;
            case API_TRANSACTIONS_COMPLETE:
            {
                msg = "Finished waiting for transactions";
            }
            break;
            case API_CONN_REFUSED:
            {
                msg = "Connection refused";
            }
            break;
			case API_CANCELLED:
			{
				msg = "Operation Cancelled";
			}
			break;

            default:
            {
                msg = "API Failure return in ";
                msg.append(function);
                msg.append(" API");
            }
			break;
        } // end of switch

		if (extraMsg)
		{
			msg.append(":\n    ");
			msg.append(extraMsg);
		}
        throw runtime_error(msg);
    }

    /***************************************************************************
     *
     * Function:  getFreeSpace
     *
     * Purpose:   get free disk space in bytes
     *
     ****************************************************************************/
    double Oam::getFreeSpace(std::string path)
    {
        double free_space = 0.0;
#ifdef _MSC_VER
		ULARGE_INTEGER freeBytesAvail;
		if (GetDiskFreeSpaceEx(path.c_str(), &freeBytesAvail, 0, 0) != 0)
			free_space = (double)freeBytesAvail.QuadPart;
#else
        struct statfs statBuf;

        if ( statfs(path.c_str(), &statBuf) == 0)
        {
            free_space  = ((double)statBuf.f_bavail) * ((double)statBuf.f_bsize);
            return free_space;
        }
        else
        {
            exceptionControl("statvfs failed", API_FAILURE );
        }

#endif
        return free_space;
    }

    /***************************************************************************
     *
     * Function:  itoa
     *
     * Purpose:   Integer to ASCII convertor
     *
     ****************************************************************************/

    std::string Oam::itoa(const int i)
    {
        stringstream ss;
        ss << i;
        return ss.str();
    }

    /***************************************************************************
     *
     * Function:  sendMsgToProcMgr
     *
     * Purpose:   Build and send request message to Process Manager
     *
     ****************************************************************************/

    int Oam::sendMsgToProcMgr(messageqcpp::ByteStream::byte requestType, const std::string name,
        GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag, const std::string argument1,
        const std::string argument2, int timeout)
    {
        int returnStatus = API_SUCCESS;           //default
        ByteStream msg;
        ByteStream receivedMSG;
        ByteStream::byte msgType;
        ByteStream::byte actionType;
        string target;
        ByteStream::byte status;

		// get current requesting process, an error will occur if process is a UI tool (not kept in Status Table)
		// this will be used to determine if this is a manually or auto request down within Process-Monitor
		bool requestManual;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			requestManual = false;	// set to auto
		}
		catch (...) {
			requestManual = true;	// set to manual
		}

        // setup message
        msg << (ByteStream::byte) REQUEST;
        msg << requestType;
        msg << name;
        msg << (ByteStream::byte) gracefulflag;
        msg << (ByteStream::byte) ackflag;
        msg << (ByteStream::byte) requestManual;

        if (!argument1.empty())
            msg << argument1;
        if (!argument2.empty())
            msg << argument2;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcMgr");
            procmgr.write(msg);

            // check for Ack msg if needed
            if ( ackflag == ACK_YES )
            {
				// wait for ACK from Process Manager
				struct timespec ts = { timeout, 0 };

				receivedMSG = procmgr.read(&ts);
				if (receivedMSG.length() > 0)
				{
					receivedMSG >> msgType;
					receivedMSG >> actionType;
					receivedMSG >> target;
					receivedMSG >> status;

					if ( msgType == oam::ACK &&  actionType == requestType && target == name)
					{
						// ACK for this request
						returnStatus = status;
					}
				}
				else	// timeout
					returnStatus = API_TIMEOUT;
            }
            else
                // No ACK, assume write success
                returnStatus = API_SUCCESS;

            // shutdown connection
            procmgr.shutdown();
        }
        catch (std::runtime_error&)
        {
			//There's other reasons, but this is the most likely...
            returnStatus = API_CONN_REFUSED;
        }
        catch (std::exception&)
        {
            returnStatus = API_FAILURE;
        }
        catch (...)
        {
            returnStatus = API_FAILURE;
        }

        return returnStatus;
    }


    /***************************************************************************
     *
     * Function:  sendMsgToProcMgr2
     *
     * Purpose:   Build and send request message to Process Manager
     *
     ****************************************************************************/

    int Oam::sendMsgToProcMgr2(messageqcpp::ByteStream::byte requestType, DeviceNetworkList devicenetworklist,
        GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag, const std::string password)
    {
        int returnStatus = API_TIMEOUT;           //default
        ByteStream msg;
        ByteStream receivedMSG;
        ByteStream::byte msgType;
        ByteStream::byte actionType;
        ByteStream::byte status;

		// get current requesting process, an error will occur if process is a UI tool (not kept in Status Table)
		// this will be used to determine if this is a manually or auto request down within Process-Monitor
		bool requestManual;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			requestManual = false;	// set to auto
		}
		catch (...) {
			requestManual = true;	// set to manual
		}

        // setup message
        msg << (ByteStream::byte) REQUEST;
        msg << requestType;
        msg << (std::string) " ";
        msg << (ByteStream::byte) gracefulflag;
        msg << (ByteStream::byte) ackflag;
        msg << (ByteStream::byte) requestManual;

        msg << (uint16_t) devicenetworklist.size();

		DeviceNetworkList::iterator pt = devicenetworklist.begin();
		for( ; pt != devicenetworklist.end() ; pt++)
		{
			msg << (*pt).DeviceName;

			if ( (*pt).UserTempDeviceName.empty() )
				msg << " ";
			else
				msg << (*pt).UserTempDeviceName;

			if ( (*pt).DisableState.empty() )
				msg << " ";
			else
				msg << (*pt).DisableState;

	        msg << (uint16_t) (*pt).hostConfigList.size();

			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
			{
				msg << (*pt1).IPAddr;
				msg << (*pt1).HostName;
				msg << (*pt1).NicID;
			}
		}

        msg << password;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcMgr");
            procmgr.write(msg);

            // check for Ack msg if needed
            if ( ackflag == ACK_YES )
            {
                // wait 15 minutes for ACK from Process Manager
				struct timespec ts = { 900, 0 };

				receivedMSG = procmgr.read(&ts);
				if (receivedMSG.length() > 0)
				{
					receivedMSG >> msgType;
					receivedMSG >> actionType;
					receivedMSG >> status;

					if ( msgType == oam::ACK &&  actionType == requestType)
					{
						// ACK for this request
						returnStatus = status;
					}
				}
				else	// timeout
            		returnStatus = API_TIMEOUT;
            }
            else
                // No ACK, assume write success
                returnStatus = API_SUCCESS;

            // shutdown connection
            procmgr.shutdown();
        }
        catch(...)
        {
            returnStatus = API_FAILURE;
        }

        return returnStatus;
    }

    /***************************************************************************
     *
     * Function:  sendMsgToProcMgr3
     *
     * Purpose:   Build and send Alarm request message to Process Manager
     *
     ****************************************************************************/

    int Oam::sendMsgToProcMgr3(messageqcpp::ByteStream::byte requestType, AlarmList& alarmlist, const std::string date)
    {
        int returnStatus = API_SUCCESS;           //default
        ByteStream msg;
        ByteStream receivedMSG;
        ByteStream::byte msgType;
        ByteStream::byte actionType;
        ByteStream::byte status;

        // setup message
        msg << requestType;
        msg << date;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcMgr");
            procmgr.write(msg);

			// wait 30 seconds for ACK from Process Manager
			struct timespec ts = { 30, 0 };

			receivedMSG = procmgr.read(&ts);
			if (receivedMSG.length() > 0)
			{
				receivedMSG >> msgType;
				receivedMSG >> actionType;
				receivedMSG >> status;

				if ( msgType == oam::ACK &&  actionType == requestType && status == API_SUCCESS )
				{
					ByteStream::byte numAlarms;

					while(true)
					{
						//number of alarms
						receivedMSG >> numAlarms;

						//check for end-of-list
						if ( numAlarms == 0)
							break;

						for ( int i = 0 ; i < numAlarms ; i++ )
						{
							Alarm alarm;
							ByteStream::doublebyte value;
							string svalue;
	
							receivedMSG >> value;
							alarm.setAlarmID(value);
							receivedMSG >> svalue;
							alarm.setDesc(svalue);
							receivedMSG >> value;
							alarm.setSeverity(value);
							receivedMSG >> svalue;
							alarm.setTimestamp(svalue);
							receivedMSG >> svalue;
							alarm.setSname(svalue);
							receivedMSG >> svalue;
							alarm.setPname(svalue);
							receivedMSG >> svalue;
							alarm.setComponentID(svalue);
	
							alarmlist.insert (AlarmList::value_type(alarm.getTimestampSeconds(), alarm));
						}
						break;
					}
				}
				else
					returnStatus = API_FAILURE;
			}
			else	// timeout
				returnStatus = API_TIMEOUT;

            // shutdown connection
            procmgr.shutdown();
        }
        catch(...)
        {
            returnStatus = API_FAILURE;
        }

        return returnStatus;
    }

	/***************************************************************************
	 *
	 * Function:  sendMsgToProcMgrWithStatus
	 *
	 * Purpose:   Build and send a request message to Process Manager
	 * Check for status messages and display on stdout.
	 * 
	 * This is used only in manual mode.
	 *
	 ****************************************************************************/

	int Oam::sendMsgToProcMgrWithStatus(messageqcpp::ByteStream::byte requestType, const std::string name,
		GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag,
        const std::string argument1, const std::string argument2, int timeout)
	{
		int returnStatus = API_STILL_WORKING;
		ByteStream msg;
		ByteStream receivedMSG;
		ByteStream::byte msgType;
		ByteStream::byte actionType;
		string target;
		ByteStream::byte status;
		struct timespec ts = {timeout, 0};
		bool requestManual = true;
		std::stringstream buffer; 
		BRM::DBRM dbrm;
#ifndef _MSC_VER
		struct sigaction ctrlcHandler;
		struct sigaction oldCtrlcHandler;
		memset(&ctrlcHandler, 0, sizeof(ctrlcHandler));
#endif
		// setup message
		msg << (ByteStream::byte) REQUEST;
		msg << requestType;
		msg << name;
		msg << (ByteStream::byte) gracefulflag;
		msg << (ByteStream::byte) ackflag;
		msg << (ByteStream::byte) requestManual;

		if (!argument1.empty())
			msg << argument1;
		if (!argument2.empty())
			msg << argument2;

		if (gracefulflag == GRACEFUL_WAIT)
		{
			// Control-C signal to terminate the shutdown command
			ctrlc = 0;
#ifdef _MSC_VER
			//FIXME:
#else
			ctrlcHandler.sa_handler = handleControlC;
			sigaction(SIGINT, &ctrlcHandler, &oldCtrlcHandler);
#endif
		}
		try
		{
			//send the msg to Process Manager
			MessageQueueClient procmgr("ProcMgr");
			procmgr.write(msg);

			// check for Ack msg if needed
			if (ackflag == ACK_YES)
			{
				while (returnStatus == API_STILL_WORKING)
				{
					// wait for ACK from Process Manager
					receivedMSG = procmgr.read(&ts);

					// If user hit ctrl-c, we've been cancelled
					if (ctrlc == 1)
					{
						writeLog("Clearing System Shutdown pending", LOG_TYPE_INFO );
						dbrm.setSystemShutdownPending(false);
						dbrm.setSystemSuspendPending(false);
						returnStatus = API_CANCELLED;
						break;
					}

					if (receivedMSG.length() > 0)
					{
						receivedMSG >> msgType;
						receivedMSG >> actionType;
						receivedMSG >> target;
						receivedMSG >> status;

						if ( msgType == oam::ACK &&  actionType == requestType && target == name)
						{
							if (status == API_TRANSACTIONS_COMPLETE)
							{
								cout << endl << "   System being " << name << ", please wait..." << flush;
								// More work to wait on....
								// At this point, the requirement is to have ctrl-c drop us out of calpont console
								// so we'll restore the handler to default.
								if (gracefulflag == GRACEFUL_WAIT)
								{
#ifdef _MSC_VER
									//FIXME:
#else
									sigaction(SIGINT, &oldCtrlcHandler, NULL);
#endif
								}
							}
							else
							{
								returnStatus = status;
							}
						}

						if (returnStatus == API_STILL_WORKING)
						{
							cout << "." << flush;
						}
					}
					else	// timeout
					{
						returnStatus = API_TIMEOUT;
					}
				}
			}
			else
			{
				// No ACK, assume write success
				returnStatus = API_SUCCESS;
			}

			// shutdown connection
			procmgr.shutdown();
		}
		catch (std::runtime_error&)
		{
			//There's other reasons, but this is the most likely...
			returnStatus = API_CONN_REFUSED;
		}
		catch (std::exception&)
		{
			returnStatus = API_FAILURE;
		}
		catch (...)
		{
			returnStatus = API_FAILURE;
		}

		if (gracefulflag == GRACEFUL_WAIT)
		{
			// Just in case we errored out and bypassed the normal restore, 
			// restore ctrl-c to previous handler.
#ifdef _MSC_VER
			//FIXME:
#else
			sigaction(SIGINT, &oldCtrlcHandler, NULL);
#endif
		}
		return returnStatus;
	}

	/***************************************************************************
     *
     * Function:  validateProcess
     *
     * Purpose:   Validate Process Name
     *
     ****************************************************************************/

    int Oam::validateProcess(const std::string moduleName, std::string processName)
    {
		SystemProcessStatus systemprocessstatus;
		ProcessStatus processstatus;
	
		try
		{
			getProcessStatus(systemprocessstatus);
	
			for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
			{
				if ( systemprocessstatus.processstatus[i].Module == moduleName  &&
						systemprocessstatus.processstatus[i].ProcessName == processName)
					// found it
					return API_SUCCESS;
			}
		}
		catch(...)
		{
			return API_INVALID_PARAMETER;
		}
		return API_INVALID_PARAMETER;
    }

    /***************************************************************************
     *
     * Function:  sendStatusUpdate
     *
     * Purpose:   Send Status Update to Process Monitor
     *
     ****************************************************************************/

    void Oam::sendStatusUpdate(ByteStream obs, ByteStream::byte returnRequestType)
    {
		try
		{
			MessageQueueClient processor("ProcStatusControl");
			processor.syncProto(false);
			ByteStream ibs;

			try {
				struct timespec ts = { 3, 0 };
				processor.write(obs, &ts);
			}
			catch(...)
			{
				processor.shutdown();
				throw std::runtime_error("error");
			}
			

			try {
				struct timespec ts1 = { 15, 0 };
				ibs = processor.read(&ts1);
			}
			catch(...)
			{
				processor.shutdown();
				throw std::runtime_error("error");
			}

			ByteStream::byte returnRequestType;

			if (ibs.length() > 0) {
				ibs >> returnRequestType;

				if ( returnRequestType == returnRequestType ) {
					processor.shutdown();
				}
			}
			else
			{
				// timeout occurred, shutdown connection and retry
				processor.shutdown();
				throw std::runtime_error("timeout");
			}
		}
		catch(...)
		{
			throw std::runtime_error("error");
		}

		return;
	}

    /***************************************************************************
     *
     * Function:  amazonReattach
     *
     * Purpose:   Amazon EC2 volume reattach needed
     *
     ****************************************************************************/

    void Oam::amazonReattach(std::string toPM, dbrootList dbrootConfigList, bool attach)
    {
		//if amazon cloud with external volumes, do the detach/attach moves
		string cloud;
		string DBRootStorageType;
		try {
			getSystemConfig("Cloud", cloud);
			getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}

		if ( (cloud == "amazon-ec2" || cloud == "amazon-vpc") && 
			DBRootStorageType == "external" )
		{
			//get Instance Name for to-pm
			string toInstanceName = oam::UnassignedName;
			try
			{
				ModuleConfig moduleconfig;
				getSystemConfig(toPM, moduleconfig);
				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				toInstanceName = (*pt1).HostName;
			}
			catch(...)
			{}

			if ( toInstanceName == oam::UnassignedName || toInstanceName.empty() )
			{
				cout << "   ERROR: amazonReattach, invalid Instance Name for " << toPM << endl;
				writeLog("ERROR: amazonReattach, invalid Instance Name " + toPM, LOG_TYPE_ERROR );
				exceptionControl("amazonReattach", API_INVALID_PARAMETER);
			}

			dbrootList::iterator pt3 = dbrootConfigList.begin();
			for( ; pt3 != dbrootConfigList.end() ; pt3++)
			{
				string dbrootid = *pt3;
				string volumeNameID = "PMVolumeName" + dbrootid;
				string volumeName = oam::UnassignedName;
				string deviceNameID = "PMVolumeDeviceName" + dbrootid;
				string deviceName = oam::UnassignedName;
				try {
					getSystemConfig( volumeNameID, volumeName);
					getSystemConfig( deviceNameID, deviceName);
				}
				catch(...)
				{}

				if ( volumeName == oam::UnassignedName || deviceName == oam::UnassignedName )
				{
					cout << "   ERROR: amazonReattach, invalid configure " + volumeName + ":" + deviceName << endl;
					writeLog("ERROR: amazonReattach, invalid configure " + volumeName + ":" + deviceName, LOG_TYPE_ERROR );
					exceptionControl("amazonReattach", API_INVALID_PARAMETER);
				}

				if (!attach)
				{
					//send msg to to-pm to umount volume
					int returnStatus = sendMsgToProcMgr(UNMOUNT, dbrootid, FORCEFUL, ACK_YES);
					if (returnStatus != API_SUCCESS) {
						writeLog("ERROR: amazonReattach, umount failed on " + dbrootid, LOG_TYPE_ERROR );
					}
				}

				if (!detachEC2Volume(volumeName)) {
					cout << "   ERROR: amazonReattach, detachEC2Volume failed on " + volumeName << endl;
					writeLog("ERROR: amazonReattach, detachEC2Volume failed on " + volumeName , LOG_TYPE_ERROR );
					exceptionControl("amazonReattach", API_FAILURE);
				}

				writeLog("amazonReattach, detachEC2Volume passed on " + volumeName , LOG_TYPE_DEBUG );

				if (!attachEC2Volume(volumeName, deviceName, toInstanceName)) {
					cout << "   ERROR: amazonReattach, attachEC2Volume failed on " + volumeName + ":" + deviceName + ":" + toInstanceName << endl;
					writeLog("ERROR: amazonReattach, attachEC2Volume failed on " + volumeName + ":" + deviceName + ":" + toInstanceName, LOG_TYPE_ERROR );
					exceptionControl("amazonReattach", API_FAILURE);
				}

				writeLog("amazonReattach, attachEC2Volume passed on " + volumeName + ":" + toPM, LOG_TYPE_DEBUG );
			}
		}
	}

    /***************************************************************************
     *
     * Function:  mountDBRoot
     *
     * Purpose:   Send msg to ProcMon to mount/unmount a external DBRoot
     *
     ****************************************************************************/

    void Oam::mountDBRoot(dbrootList dbrootConfigList, bool mount)
    {
		//if external volumes, mount to device
		string DBRootStorageType;
		try {
			getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}

		string GlusterConfig = "n";
		try {
			getSystemConfig( "GlusterConfig", GlusterConfig);
		}
		catch(...)
		{
			GlusterConfig = "n";
		}

		if ( (DBRootStorageType == "external" && GlusterConfig == "n") 
			||
			(DBRootStorageType == "internal" && GlusterConfig == "y" && !mount) )
		{
			dbrootList::iterator pt3 = dbrootConfigList.begin();
			for( ; pt3 != dbrootConfigList.end() ; pt3++)
			{
				string dbrootid = *pt3;

				int mountCmd = oam::MOUNT;
				if (!mount) {
					mountCmd = oam::UNMOUNT;
					writeLog("mountDBRoot api, umount dbroot" + dbrootid, LOG_TYPE_DEBUG);
				}
				else
					writeLog("mountDBRoot api, mount dbroot" + dbrootid, LOG_TYPE_DEBUG);

				//send msg to to-pm to umount volume
				int returnStatus = sendMsgToProcMgr(mountCmd, dbrootid, FORCEFUL, ACK_YES);
		
				if (returnStatus != API_SUCCESS) {
					if ( mountCmd == oam::MOUNT ) {
						writeLog("ERROR: mount failed on dbroot" + dbrootid, LOG_TYPE_ERROR );
						cout << "   ERROR: mount failed on dbroot" + dbrootid << endl;
					}
					else
					{
						writeLog("ERROR: unmount failed on dbroot" + dbrootid, LOG_TYPE_ERROR );
						cout << "   ERROR: unmount failed on dbroot" + dbrootid << endl;
						exceptionControl("mountDBRoot", API_FAILURE);
					}
				}
			}
		}

		return;
	}

	/******************************************************************************************
	* @brief	writeLog
	*
	* purpose:	Write the message to the log
	*
	******************************************************************************************/
	void Oam::writeLog(const string logContent, const LOG_TYPE logType)
	{
		LoggingID lid(8);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add(logContent);
		msg.format(args);
	
		switch(logType) {
			case LOG_TYPE_DEBUG:
				ml.logDebugMessage(msg);
				break;
			case LOG_TYPE_INFO:
				ml.logInfoMessage(msg);
				break;
			case LOG_TYPE_WARNING:
				ml.logWarningMessage(msg);
				break;
			case LOG_TYPE_ERROR:
				ml.logErrorMessage(msg);
				break;
			case LOG_TYPE_CRITICAL:
				ml.logCriticalMessage(msg);
				break;
		}
		return;
	}

	/***************************************************************************
	 *
	 * Function:  waitForSystem
	 *
	 * Purpose:   When a Shutdown, stop, restart or suspend
	 *  		  operation is requested but there are active
	 *  		  transactions of some sort, We wait for all
	 *  		  transactions to close before performing the
	 *  		  action.
	 ****************************************************************************/
	bool Oam::waitForSystem(PROC_MGT_MSG_REQUEST request, messageqcpp::IOSocket& ios, messageqcpp::ByteStream& stillWorkingMsg)
	{
		// Use ios to send back periodic still working messages
		BRM::DBRM dbrm;
		execplan::SessionManager sessionManager; 
		bool bIsDbrmUp;
		BRM::SIDTIDEntry blockingsid;
		std::vector<BRM::TableLockInfo> tableLocks;
		bool bActiveTransactions = true;
		bool bRollback;
		bool bForce;
		bool ret = false;
		size_t idx;

		try
		{
			while (bActiveTransactions)
			{
				sleep(3);
				ios.write(stillWorkingMsg);

				bActiveTransactions = false;
				// Any table locks still set?
				tableLocks = dbrm.getAllTableLocks();
				for (idx = 0; idx < tableLocks.size(); ++idx)
				{
					if (dbrm.checkOwner(tableLocks[idx].id))
					{
						bActiveTransactions = true;
						break;
					}
				}
				// Any active transactions?
				if (sessionManager.checkActiveTransaction(0, bIsDbrmUp, blockingsid))
				{
					bActiveTransactions = true;
				}

				// check to see if the user canceled the request.
				if (request == SUSPENDWRITES)
				{
					if (dbrm.getSystemSuspendPending(bRollback) == 0)	// Means we no longer are going to suspend
					{
						writeLog("System Suspend Canceled in wait", LOG_TYPE_INFO );
						break;
					}
				}
				else
				{
					if (dbrm.getSystemShutdownPending(bRollback, bForce) == 0)	// Means we no longer are going to shutdown
					{
						writeLog("System Shutdown Canceled in wait", LOG_TYPE_INFO );
						break;
					}
				}

				if (!bActiveTransactions)
				{
					ret =  true;
				}
			}
		}
		catch (...)
		{
			writeLog("Communication with calpont console failed while waiting for transactions", LOG_TYPE_ERROR);
		}
//		writeLog("Returning from wait with value " + itoa(ret), LOG_TYPE_INFO );
		return ret;
	}

	int Oam::readHdfsActiveAlarms(AlarmList& alarmList)
	{
		int returnStatus = API_FAILURE;
		Alarm alarm;

		// the alarm file will be pushed to all nodes every 10 seconds, retry 1 second
		for (int i = 0; i < 10 && returnStatus != API_SUCCESS; i++)
		{
			try
			{
				ifstream activeAlarm(ACTIVE_ALARM_FILE.c_str(), ios::in);
				if (!activeAlarm.is_open())
				{
					// file may be temporary not available due to dpcp.
					usleep(10000);

					activeAlarm.open(ACTIVE_ALARM_FILE.c_str(), ios::in);
					if (!activeAlarm.is_open())
					{
						// still cannot open, treat as no activeAlarms file.
						returnStatus = API_SUCCESS;
						continue;
					}
				}

				// read from opened file.
				while (!activeAlarm.eof() && activeAlarm.good())
				{
					activeAlarm >> alarm;
					if (alarm.getAlarmID() != INVALID_ALARM_ID)
						alarmList.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
				}
				activeAlarm.close();

				returnStatus = API_SUCCESS;
			}
			catch (...)
			{
			}

			// wait a second and try again
			if (returnStatus != API_SUCCESS)
				usleep (100000);
		}

		return returnStatus;
	}

} //namespace oam


namespace procheartbeat
{
/*
    ProcHeartbeat::ProcHeartbeat()
        {}

    ProcHeartbeat::~ProcHeartbeat()
        {}
*/
    /********************************************************************
     *
     * Register for Proc Heartbeat
     *
     ********************************************************************/
/*
    void ProcHeartbeat::registerHeartbeat(int ID)
    {
		Oam oam;
		// get current Module name
		string Module;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        exceptionControl("registerHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        exceptionControl("registerHeartbeat", API_FAILURE);
		}

        ByteStream msg;

        // setup message
        msg << (ByteStream::byte) HEARTBEAT_REGISTER;
        msg << Module;
        msg << processName;
        msg << (ByteStream::byte) ID;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcHeartbeatControl");
            procmgr.write(msg);
            procmgr.shutdown();
        }
        catch(...)
        {
	        exceptionControl("registerHeartbeat", API_FAILURE);
        }

	}
*/
    /********************************************************************
     *
     * De-Register for Proc Heartbeat
     *
     ********************************************************************/
/*
    void ProcHeartbeat::deregisterHeartbeat(int ID)
    {
		Oam oam;
		// get current Module name
		string Module;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        exceptionControl("deregisterHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        exceptionControl("deregisterHeartbeat", API_FAILURE);
		}

        ByteStream msg;

        // setup message
        msg << (ByteStream::byte) HEARTBEAT_DEREGISTER;
        msg << Module;
        msg << processName;
        msg << (ByteStream::byte) ID;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcHeartbeatControl");
            procmgr.write(msg);
            procmgr.shutdown();
        }
        catch(...)
        {
	        exceptionControl("deregisterHeartbeat", API_FAILURE);
        }

	}
*/
    /********************************************************************
     *
     * Send Proc Heartbeat
     *
     ********************************************************************/
/*
    void ProcHeartbeat::sendHeartbeat(int ID, oam::ACK_FLAG ackFlag)
    {
		Oam oam;

		// get process heartbeat period
		int processHeartbeatPeriod = 60;	//default
	
		try {
			getSystemConfig("ProcessHeartbeatPeriod", processHeartbeatPeriod);
		}
		catch(...)
		{
		}

		//skip sending if Heartbeat is disable
		if( processHeartbeatPeriod == -1 )
	        exceptionControl("sendHeartbeat", API_DISABLED);

		// get current Module name
		string Module;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        exceptionControl("sendHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        exceptionControl("sendHeartbeat", API_FAILURE);
		}

        ByteStream msg;

        // setup message
        msg << (ByteStream::byte) HEARTBEAT_SEND;
        msg << Module;
        msg << processName;
        msg << getCurrentTime();
        msg << (ByteStream::byte) ID;
        msg << (ByteStream::byte) ackFlag;

        try
        {
            //send the msg to Process Manager
            MessageQueueClient procmgr("ProcHeartbeatControl");
            procmgr.write(msg);

			//check for ACK
			if ( ackFlag == oam::ACK_YES ) {
		        ByteStream ackMsg;
				ByteStream::byte type;
				// wait for ACK from Process Manager
				struct timespec ts = { processHeartbeatPeriod, 0 };

				ackMsg = procmgr.read(&ts);

				if (ackMsg.length() > 0)
				{
					ackMsg >> type;
					if ( type != HEARTBEAT_SEND ) {
						//Ack not received
	           			procmgr.shutdown();
	        			exceptionControl("sendHeartbeat", API_TIMEOUT);
					}
				}
				else
				{
	           		procmgr.shutdown();
	        		exceptionControl("sendHeartbeat", API_TIMEOUT);
				}
			}
            procmgr.shutdown();
        }
        catch(...)
        {
	        exceptionControl("sendHeartbeat", API_FAILURE);
        }
	}
*/
} // end of namespace
// vim:ts=4 sw=4:

