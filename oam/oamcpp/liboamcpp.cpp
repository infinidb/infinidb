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
#include <unistd.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/progress.hpp>
#include <boost/tokenizer.hpp>
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
#endif
#include <stdexcept>

#include "ddlpkg.h"
#include "dmlpkg.h"
#define LIBOAM_DLLEXPORT
#include "liboamcpp.h"
#undef LIBOAM_DLLEXPORT

#ifdef _MSC_VER
#include "idbregistry.h"
#endif

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

//default directory
#ifdef _MSC_VER
const std::string CalpontFileDir = "C:\\Calpont\\etc";
#else
const std::string CalpontFileDir = "/usr/local/Calpont/etc";
#endif

const std::string CalpontFile = "Calpont.xml";
const std::string AlarmFile = "AlarmConfig.xml";
const std::string ProcessFile = "ProcessConfig.xml";

namespace oam
{
    Oam::Oam()
    {
		// Assigned pointers to Config files
		string calpontfiledir;

		const char* cf;
#ifdef _MSC_VER
		string cfStr = IDBreadRegistry("CalpontHome");
		if (!cfStr.empty())
			cf = cfStr.c_str();
#else        
		cf = getenv("CALPONT_HOME");
#endif
		if (cf == 0 || *cf == 0)
			calpontfiledir = CalpontFileDir;
		else
			calpontfiledir = cf;

		CalpontConfigFile = calpontfiledir + "/" + CalpontFile;
	
		AlarmConfigFile = calpontfiledir + "/" + AlarmFile;
	
		ProcessConfigFile = calpontfiledir + "/" + ProcessFile;

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

#ifdef _MSC_VER
		ifstream File ("C:\\Calpont\\releasenum");
#else
        ifstream File ("/usr/local/Calpont/releasenum");
#endif
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
        systemconfig.RAIDCriticalThreshold = strtol(sysConfig->getConfig(Section, "RAIDCriticalThreshold").c_str(), 0, 0);
        systemconfig.RAIDMajorThreshold = strtol(sysConfig->getConfig(Section, "RAIDMajorThreshold").c_str(), 0, 0);
        systemconfig.RAIDMinorThreshold = strtol(sysConfig->getConfig(Section, "RAIDMinorThreshold").c_str(), 0, 0);
        systemconfig.TransactionArchivePeriod = strtol(sysConfig->getConfig(Section, "TransactionArchivePeriod").c_str(), 0, 0);

        // get string variables
		for ( unsigned int i = 1 ; i < systemconfig.DBRootCount + 1 ; i++)
		{
			systemconfig.DBRoot.push_back(sysConfig->getConfig(Section, "DBRoot" + itoa(i)));
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

        for (int j = 0; j < MAX_MODULE; j++)
        {
        	ModuleTypeConfig moduletypeconfig;
	
            Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

            // get Module info

            string moduleType = MODULE_TYPE + itoa(j+1);

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

        for (int j = 0; j < MAX_MODULE; j++)
        {
            string moduleType = MODULE_TYPE + itoa(j+1);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype)
            {
                string ModuleType = MODULE_TYPE + itoa(j+1);
                string ModuleDesc = MODULE_DESC + itoa(j+1);
                string ModuleRunType = MODULE_RUN_TYPE + itoa(j+1);
                string ModuleCount = MODULE_COUNT + itoa(j+1);
                string ModuleCPUCriticalThreshold = MODULE_CPU_CRITICAL + itoa(j+1);
                string ModuleCPUMajorThreshold = MODULE_CPU_MAJOR + itoa(j+1);
                string ModuleCPUMinorThreshold = MODULE_CPU_MINOR + itoa(j+1);
                string ModuleCPUMinorClearThreshold = MODULE_CPU_MINOR_CLEAR + itoa(j+1);
                string ModuleDiskCriticalThreshold = MODULE_DISK_CRITICAL + itoa(j+1);
                string ModuleDiskMajorThreshold = MODULE_DISK_MAJOR + itoa(j+1);
                string ModuleDiskMinorThreshold = MODULE_DISK_MINOR + itoa(j+1);
                string ModuleMemCriticalThreshold = MODULE_MEM_CRITICAL + itoa(j+1);
                string ModuleMemMajorThreshold = MODULE_MEM_MAJOR + itoa(j+1);
                string ModuleMemMinorThreshold = MODULE_MEM_MINOR + itoa(j+1);
                string ModuleSwapCriticalThreshold = MODULE_SWAP_CRITICAL + itoa(j+1);
                string ModuleSwapMajorThreshold = MODULE_SWAP_MAJOR + itoa(j+1);
                string ModuleSwapMinorThreshold = MODULE_SWAP_MINOR + itoa(j+1);

                moduletypeconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
                moduletypeconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc);
                moduletypeconfig.RunType = sysConfig->getConfig(Section, ModuleRunType);
                moduletypeconfig.ModuleCount = strtol(sysConfig->getConfig(Section, ModuleCount).c_str(), 0, 0);
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

				for (int i = 1; i < MAX_MODULE ; i++)
				{

					DeviceNetworkConfig devicenetworkconfig;
					HostConfig hostconfig;

					//get NIC IP address/hostnames
					for (int k = 1; k < MAX_NIC+1 ; k++)
					{
						string ModuleIpAddr = MODULE_IP_ADDR + itoa(i) + "-" + itoa(k) + "-" + itoa(j+1);
	
						string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
						if (ipAddr.empty() || ipAddr == UnassignedIpAddr )
							continue;
	
						string ModuleHostName = MODULE_SERVER_NAME + itoa(i) + "-" + itoa(k) + "-" + itoa(j+1);
						string serverName = sysConfig->getConfig(Section, ModuleHostName);
	
						hostconfig.IPAddr = ipAddr;
						hostconfig.HostName = serverName;
						hostconfig.NicID = k;

						devicenetworkconfig.hostConfigList.push_back(hostconfig);
					}

					if ( !devicenetworkconfig.hostConfigList.empty() ) {
		                string ModuleDisableState = MODULE_DISABLE_STATE + itoa(i) + "-" + itoa(j+1);
						devicenetworkconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

						devicenetworkconfig.DeviceName = moduletypeconfig.ModuleType + itoa(i);
						moduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
						devicenetworkconfig.hostConfigList.clear();
					}
				}

				DiskMonitorFileSystems fs;
				for (int i = 1;; i++)
				{
	            	string ModuleDiskMonitorFS = MODULE_DISK_MONITOR_FS + itoa(i) + "-" + itoa(j+1);

                	string fsName = sysConfig->getConfig(Section, ModuleDiskMonitorFS);
					if (fsName.empty())
						break;

					fs.push_back(fsName);
				}
				moduletypeconfig.FileSystems = fs;

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

		string moduletype = module.substr(0,MAX_MODULE_TYPE_SIZE);
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		if ( moduleID < 1 )
			//invalid ID
        	exceptionControl("getSystemConfig", API_INVALID_PARAMETER);

        for (int j = 0; j < MAX_MODULE; j++)
        {
            string moduleType = MODULE_TYPE + itoa(j+1);
            string ModuleCount = MODULE_COUNT + itoa(j+1);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype  )
            {
                string ModuleType = MODULE_TYPE + itoa(j+1);
                string ModuleDesc = MODULE_DESC + itoa(j+1);
	            string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(j+1);

                moduleconfig.ModuleName = module;
                moduleconfig.ModuleType = sysConfig->getConfig(Section, ModuleType);
                moduleconfig.ModuleDesc = sysConfig->getConfig(Section, ModuleDesc) + " #" + itoa(moduleID);
                moduleconfig.DisableState = sysConfig->getConfig(Section, ModuleDisableState);

				HostConfig hostconfig;

				//get NIC IP address/hostnames
				for (int k = 1; k < MAX_NIC+1 ; k++)
				{
					string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa(k) + "-" + itoa(j+1);

					string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
					if (ipAddr.empty() || ipAddr == UnassignedIpAddr )
						continue;

					string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa(k) + "-" + itoa(j+1);
					string serverName = sysConfig->getConfig(Section, ModuleHostName);

					hostconfig.IPAddr = ipAddr;
					hostconfig.HostName = serverName;
					hostconfig.NicID = k;

					moduleconfig.hostConfigList.push_back(hostconfig);
				}

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
        for (int j = 1; j < MAX_EXT_DEVICE+1; j++)
        {
	        ExtDeviceConfig Extdeviceconfig;

			string name = NAME + itoa(j);

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

			string ipaddr = IPADDR + itoa(j);
			string disablestate = DISABLE_STATE + itoa(j);

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

        for (int j = 1; j < MAX_EXT_DEVICE+1; j++)
        {
			string name = NAME + itoa(j);

			extdeviceconfig.Name = sysConfig->getConfig(Section, name);

            if (extdeviceconfig.Name != extDevicename)
                continue;

			string ipaddr = IPADDR + itoa(j);
			string disablestate = DISABLE_STATE + itoa(j);

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
		int j = 1;
        for (; j < MAX_EXT_DEVICE+1; j++)
        {
			string name = NAME + itoa(j);

            if (sysConfig->getConfig(Section, name) == oam::UnassignedName)
				entry = j;

            if ((sysConfig->getConfig(Section, name)).empty() && entry == 0)
				entry = j;

            if (sysConfig->getConfig(Section, name) != deviceName)
                continue;

			string ipaddr = IPADDR + itoa(j);
			string disablestate = DISABLE_STATE + itoa(j);

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
			entry = j;

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

        for (int j = 0; j < MAX_MODULE; j++)
        {
            string moduleType = MODULE_TYPE + itoa(j+1);

            if( sysConfig->getConfig(Section, moduleType) ==  moduletype)
            {
                string ModuleType = MODULE_TYPE + itoa(j+1);
                string ModuleDesc = MODULE_DESC + itoa(j+1);
                string ModuleRunType = MODULE_RUN_TYPE + itoa(j+1);
                string ModuleCount = MODULE_COUNT + itoa(j+1);
                string ModuleCPUCriticalThreshold = MODULE_CPU_CRITICAL + itoa(j+1);
                string ModuleCPUMajorThreshold = MODULE_CPU_MAJOR + itoa(j+1);
                string ModuleCPUMinorThreshold = MODULE_CPU_MINOR + itoa(j+1);
                string ModuleCPUMinorClearThreshold = MODULE_CPU_MINOR_CLEAR + itoa(j+1);
                string ModuleDiskCriticalThreshold = MODULE_DISK_CRITICAL + itoa(j+1);
                string ModuleDiskMajorThreshold = MODULE_DISK_MAJOR + itoa(j+1);
                string ModuleDiskMinorThreshold = MODULE_DISK_MINOR + itoa(j+1);
                string ModuleMemCriticalThreshold = MODULE_MEM_CRITICAL + itoa(j+1);
                string ModuleMemMajorThreshold = MODULE_MEM_MAJOR + itoa(j+1);
                string ModuleMemMinorThreshold = MODULE_MEM_MINOR + itoa(j+1);
                string ModuleSwapCriticalThreshold = MODULE_SWAP_CRITICAL + itoa(j+1);
                string ModuleSwapMajorThreshold = MODULE_SWAP_MAJOR + itoa(j+1);
                string ModuleSwapMinorThreshold = MODULE_SWAP_MINOR + itoa(j+1);

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
					for (int i = 1; i < MAX_MODULE ; i++)
					{
						//get NIC IP address/hostnames
						for (int k = 1; k < MAX_NIC+1 ; k++)
						{
							string ModuleIpAddr = MODULE_IP_ADDR + itoa(i) + "-" + itoa(k) + "-" + itoa(j+1);
		
							string ipAddr = sysConfig->getConfig(Section, ModuleIpAddr);
							if (ipAddr.empty())
								continue;
		
							string ModuleHostName = MODULE_SERVER_NAME + itoa(i) + "-" + itoa(k) + "-" + itoa(j+1);
							string ModuleDisableState = MODULE_DISABLE_STATE + itoa(i) + "-" + itoa(j+1);

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

						HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
						for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
						{
							int nidID = (*pt1).NicID;
							string ModuleIpAddr = MODULE_IP_ADDR + itoa(ModuleID) + "-" + itoa(nidID) + "-" + itoa(j+1);
							sysConfig->setConfig(Section, ModuleIpAddr, (*pt1).IPAddr);
	
							string ModuleHostName = MODULE_SERVER_NAME + itoa(ModuleID) + "-" + itoa(nidID) + "-" + itoa(j+1);
							sysConfig->setConfig(Section, ModuleHostName, (*pt1).HostName);
						}
					}
				}

				DiskMonitorFileSystems::iterator pt = moduletypeconfig.FileSystems.begin();
				int id=1;
				for( ; pt != moduletypeconfig.FileSystems.end() ; pt++)
				{
	            	string ModuleDiskMonitorFS = MODULE_DISK_MONITOR_FS + itoa(id) + "-" + itoa(j+1);
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

		string moduletype = module.substr(0,MAX_MODULE_TYPE_SIZE);
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		if ( moduleID < 1 )
			//invalid ID
        	exceptionControl("setSystemConfig", API_INVALID_PARAMETER);

        for (int j = 0; j < MAX_MODULE; j++)
        {
            string moduleType = MODULE_TYPE + itoa(j+1);
            string ModuleCount = MODULE_COUNT + itoa(j+1);

            if( sysConfig100->getConfig(Section, moduleType) ==  moduletype )
            {
	            string ModuleDisableState = MODULE_DISABLE_STATE + itoa(moduleID) + "-" + itoa(j+1);
				sysConfig100->setConfig(Section, ModuleDisableState, moduleconfig.DisableState);

				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				for( ; pt1 != moduleconfig.hostConfigList.end() ; pt1++)
				{
					string ModuleIpAddr = MODULE_IP_ADDR + itoa(moduleID) + "-" + itoa((*pt1).NicID) + "-" + itoa(j+1);
					sysConfig100->setConfig(Section, ModuleIpAddr, (*pt1).IPAddr);
	
					string ModuleHostName = MODULE_SERVER_NAME + itoa(moduleID) + "-" + itoa((*pt1).NicID) + "-" + itoa(j+1);
					sysConfig100->setConfig(Section, ModuleHostName, (*pt1).HostName);
				}
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
        ModuleStatus modulestatus;
        systemstatus.systemmodulestatus.modulestatus.clear();
        ExtDeviceStatus extdevicestatus;
        systemstatus.systemextdevicestatus.extdevicestatus.clear();
        NICStatus nicstatus;
        systemstatus.systemnicstatus.nicstatus.clear();

        try
        {
            MessageQueueClient processor("ProcStatusControl");
			processor.syncProto(false);
            ByteStream::byte ModuleNumber;
            ByteStream::byte ExtDeviceNumber;
            ByteStream::byte NICNumber;
			ByteStream::byte state;
			std::string name;
			std::string date;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_SYSTEM_STATUS;

            processor.write(obs);

			// wait 10 seconds for ACK from Process Monitor
			struct timespec ts = { 10, 0 };

			ibs = processor.read(&ts);

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

				processor.shutdown();
				return;
			}
			// timeout ocurred, shutdown connection
			processor.shutdown();
        }
        catch(...)
        {
        	exceptionControl("getSystemStatus", API_FAILURE);
		}
		exceptionControl("getSystemStatus", API_TIMEOUT);
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

        for (int j = 0; j < MAX_PROCESS; j++)
        {
            string sectionName = SECTION_NAME + itoa(j+1);

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
					for (int k = 0; k < MAX_ARGUMENTS; k++)
					{
						argName = ARG_NAME + itoa(k+1);
						processconfig.ProcessArgs[k] = proConfig->getConfig(sectionName, argName);
					}
	
					// get process dependencies
					for (int k = 0; k < MAX_DEPENDANCY; k++)
					{
						depName = DEP_NAME + itoa(k+1);
						processconfig.DepProcessName[k] = proConfig->getConfig(sectionName, depName);
					}
					// get dependent process Module name
					for (int k = 0; k < MAX_DEPENDANCY; k++)
					{
						depMdlName = DEP_MDLNAME + itoa(k+1);
						processconfig.DepModuleName[k] = proConfig->getConfig(sectionName, depMdlName);
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

        for (int j = 0; j < MAX_PROCESS; j++)
        {
	        ProcessConfig processconfig;

            Config* proConfig = Config::makeConfig(ProcessConfigFile.c_str());

            // get process info

            string sectionName = SECTION_NAME + itoa(j+1);

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

        for (int j = 0; j < MAX_PROCESS; j++)
        {
            string sectionName = SECTION_NAME + itoa(j+1);

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

        for (int j = 0; j < MAX_PROCESS; j++)
        {
            string sectionName = SECTION_NAME + itoa(j+1);

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
            ByteStream::byte processNumber;
			ByteStream::byte state;
			ByteStream::quadbyte PID;
			std::string changeDate;
			std::string processName;
			std::string moduleName;
            ByteStream obs, ibs;

            obs << (ByteStream::byte) GET_ALL_PROC_STATUS;

            processor.write(obs);

			// wait 10 seconds for ACK from Process Monitor
			struct timespec ts = { 10, 0 };

			ibs = processor.read(&ts);

			if (ibs.length() > 0)
			{
				ibs >> processNumber;

				for( int i=0 ; i < processNumber ; ++i)
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

            processor.write(obs);

			// wait 10 seconds for ACK from Process Monitor
			struct timespec ts = { 10, 0 };

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

    void Oam::setProcessStatus(const std::string process, const std::string module, const int state, const int PID)
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
			sleep(1);
		}
        exceptionControl("setProcessStatus", API_TIMEOUT);
    }

    /********************************************************************
     *
     * Process Initization Sucessfull Completed, Mark Process ACTIVE
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

			if ( OAMParentModuleFlag ) {
				//call getAlarm API directly
				SNMPManager sm;
				sm.getActiveAlarm(activeAlarm);
				return;
			}
		}
		catch (...) {
       		exceptionControl("getActiveAlarms", API_FAILURE);
		}

        // build and send msg
        int returnStatus = sendMsgToProcMgr3(GETACTIVEALARMDATA, activeAlarm, "");

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

			if ( OAMParentModuleFlag ) {
				//call getAlarm API directly
	    	    SNMPManager sm;
    	    	sm.getAlarm(date, alarmlist);
				return;
			}
		}
		catch (...) {
       		exceptionControl("getActiveAlarms", API_FAILURE);
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

    bool Oam::checkActiveAlarm(const int alarmid, const std::string moduleName, const std::string deviceName)
    {
        AlarmList activeAlarm;

        // get list of active alarms
        SNMPManager sm;
        sm.getActiveAlarm(activeAlarm);

        AlarmList::iterator i;
        for (i = activeAlarm.begin(); i != activeAlarm.end(); ++i)
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

#ifdef _MSC_VER
		string fileName = "C:\\Calpont\\local\\module";
#else
		string fileName = "/usr/local/Calpont/local/module";
#endif
	
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

    myProcessStatus_t Oam::getMyProcessStatus(const int processID)
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
				processor.write(obs);
	
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
		
						return boost::make_tuple((int) pid, processName, state);
					}
				}
				catch(...)
				{
					//system("touch /var/log/Calpont/test6");
					exceptionControl("getMyProcessStatus", API_INVALID_PARAMETER);
				}
			}
			catch(...)
			{
				//system("touch /var/log/Calpont/test7");
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
        int returnStatus = sendMsgToProcMgr(STOPSYSTEM, "", gracefulflag, ackflag);

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
        // build and send msg
        int returnStatus = sendMsgToProcMgr(SHUTDOWNSYSTEM, "", gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("shutdownSystem", returnStatus);
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

    void Oam::restartSystem(GRACEFUL_FLAG gracefulflag, ACK_FLAG ackflag)
    {
        // build and send msg
        int returnStatus = sendMsgToProcMgr(RESTARTSYSTEM, "", gracefulflag, ackflag);

        if (returnStatus != API_SUCCESS)
            exceptionControl("restartSystem", returnStatus);
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
		ProcessStatus procstat;
		getProcessStatus(processName, moduleName, procstat);
		if ( procstat.ProcessOpState == oam::COLD_STANDBY )
			exceptionControl("startProcess", API_INVALID_STATE);

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
	
		string cmd = "ls " + path + "/var/log/Calpont/archive | grep '" + logFileName + "' > /tmp/logfiles";
		(void)system(cmd.c_str());
	
		string fileName = "/tmp/logfiles";
	
		ifstream oldFile (fileName.c_str());
		if (oldFile) {
			char line[400];
			string buf;
			while (oldFile.getline(line, 400))
			{
				buf = line;
				string cmd = "cat " + path + "/var/log/Calpont/archive/" + buf + " >> /tmp/logs";
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
     * @brief	stopDMLProcessing
     *
     * purpose:	tell the DML Processor to stop accepting incoming DML packages
     *
     ******************************************************************************************/
    int Oam::stopDMLProcessing()
    {
        int retval = sendMsgToDMLProcessor(dmlpackage::DML_STOP);

		if ( retval != API_SUCCESS )
        	exceptionControl("stopDMLProcessing", retval);

        return retval;
    }

    /******************************************************************************************
     * @brief	stopDDLProcessing
     *
     * purpose:	tell the DDL Processor to stop accepting incoming DDL packages
     *
     ******************************************************************************************/
    int Oam::stopDDLProcessing()
    {
        int retval = sendMsgToDDLProcessor(ddlpackage::DDL_STOP);

		if ( retval != API_SUCCESS )
        	exceptionControl("stopDDLProcessing", retval);

        return retval;
    }

    /******************************************************************************************
     * @brief	resumeDMLProcessing
     *
     * purpose:	tell the DML Processor to resume accepting DML packages
     *
     ******************************************************************************************/
    int Oam::resumeDMLProcessing()
    {
        int retval = sendMsgToDMLProcessor(dmlpackage::DML_RESUME);

		if ( retval != API_SUCCESS )
        	exceptionControl("resumeDMLProcessing", retval);

        return retval;
    }

    /******************************************************************************************
     * @brief	resumeDDLProcessing
     *
     * purpose:	tell the DDL Processor to resume accepting DDL packages
     *
     ******************************************************************************************/
    int Oam::resumeDDLProcessing()
    {
        int retval = sendMsgToDDLProcessor(ddlpackage::DDL_RESUME);

		if ( retval != API_SUCCESS )
        	exceptionControl("resumeDDLProcessing", retval);

        return retval;
    }

    /******************************************************************************************
     * @brief	sendMsgToDDLProcessor
     *
     * purpose:	send a message to the DDL processor
     *
     ******************************************************************************************/
    int Oam::sendMsgToDDLProcessor(messageqcpp::ByteStream::quadbyte requestType)
    {
        int retval = -1;

        try
        {

            MessageQueueClient processor("DDLProc");
            ByteStream::byte status;
            ByteStream obs, ibs;
			//Serialize DDL,DML
			u_int32_t sessionID = 0;
			obs << sessionID;
            obs << requestType;

            processor.write(obs);

            ibs = processor.read();

            if (ibs.length() > 0)
            {
                ibs >> status;
                retval = (int)status;
            }
        }
        catch(...)
            {}

        return retval;
    }

    /******************************************************************************************
     * @brief	sendMsgToDMLProcessor
     *
     * purpose:	send a message to the DML processor
     *
     ******************************************************************************************/
    int Oam::sendMsgToDMLProcessor(messageqcpp::ByteStream::byte requestType)
    {
        int retval = -1;

        try
        {

            MessageQueueClient processor("DMLProc");
            ByteStream::byte status;
            ByteStream obs, ibs;
			//Serialize DDL,DML
			u_int32_t sessionID = 0;
			obs << sessionID;
            obs << requestType;

            processor.write(obs);

            ibs = processor.read();

            if (ibs.length() > 0)
            {
                ibs >> status;
                retval = (int)status;
            }
        }
        catch(...)
            {}

        return retval;
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
        string SECTION = "DBRM_Worker";

        Config* sysConfig = Config::makeConfig(CalpontConfigFile.c_str());

		int numWorker = atoi(sysConfig->getConfig("DBRM_Controller", "NumWorkers", true).c_str());
        for (int i = 1; i < numWorker+1; i++)
        {
            string section = SECTION + itoa(i);

            if( sysConfig->getConfig(section, "Module") == moduleName )
				return i;
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

			// wait 30 seconds for ACK from Server Monitor
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
		ByteStream::quadbyte totalBlocks;
		ByteStream::quadbyte usedBlocks;
		ByteStream::byte diskUsage;

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
		string SqlStatement;
		string startTime;
		string sessionID;

        try
        {
            Oam::getSystemConfig(systemmoduletypeconfig);
        }
        catch(...)
        {
            exceptionControl("getActiveSQLStatements", API_FAILURE);
        }

		// get Server Type Install ID
		int serverTypeInstall = oam::INSTALL_NORMAL;
		oamModuleInfo_t st;
		try {
			st = getModuleInfo();
			serverTypeInstall = boost::get<5>(st);
		}
		catch (...) {
       		exceptionControl("getMyProcessStatus", API_FAILURE);
		}

		string sendModule;
		switch ( serverTypeInstall ) {
			case (oam::INSTALL_NORMAL):
			case (oam::INSTALL_COMBINE_DM_UM):
			{
				sendModule = "um";
				break;
			}
			case (oam::INSTALL_COMBINE_PM_UM):
			case (oam::INSTALL_COMBINE_DM_UM_PM):
			{
				sendModule = "pm";
				break;
			}
		}

		//send request to modules
        for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
        {
            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
                //end of file
                break;

            if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType == sendModule )
			{
				if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0 )
					break;
	
				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					string module = (*pt).DeviceName;

					// setup message
					msg << (ByteStream::byte) GET_ACTIVE_SQL_QUERY;
			
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
								receivedMSG >> SqlStatement;
								receivedMSG >> startTime;
								receivedMSG >> sessionID;
								//skip dummy info
								if ( SqlStatement != "-1" ) {
									activesqlstatements.sqlstatements.push_back(SqlStatement);
									activesqlstatements.starttime.push_back(startTime);
									activesqlstatements.sessionid.push_back(sessionID);
								}
							}
						}
						else	// timeout
							exceptionControl("getActiveSQLStatements", API_TIMEOUT);
			
						// shutdown connection
						servermonitor.shutdown();
					}
					catch(...)
					{
						exceptionControl("getActiveSQLStatements", API_FAILURE);
					}
				}

				break;
			}
        }
	}

    /******************************************************************************************
     * @brief	run DBHealth Check
     *
     * purpose:	test the health of the DB
     *
     ******************************************************************************************/
	void Oam::checkDBHealth(bool action)
	{
		ByteStream msg;
        ByteStream receivedMSG;

		// only make call if system is active
		SystemStatus systemstatus;
		try {
			getSystemStatus(systemstatus);
		}
		catch (exception& ex)
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
		struct timespec ts = { 60, 0 };

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
#ifdef _MSC_VER
		string fileName = "C:\\Calpont\\local\\hotStandbyPM";
#else
		string fileName = "/usr/local/Calpont/local/hotStandbyPM";
#endif
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
#ifdef _MSC_VER
		string fileName = "C:\\Calpont\\loca\\hotStandbyPM";
#else
		string fileName = "/usr/local/Calpont/local/hotStandbyPM";
#endif
	
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
	void Oam::switchParentOAMModule(std::string moduleName)
	{
		ModuleTypeConfig moduletypeconfig;
		getSystemConfig("pm", moduletypeconfig);
		if ( moduletypeconfig.ModuleCount < 2 )
			exceptionControl("switchParentOAMModule", API_INVALID_PARAMETER);

		if ( moduleName.empty() || moduleName == " ") {
			getSystemConfig("StandbyOAMModuleName", moduleName);
			if ( moduleName.empty() || moduleName == oam::UnassignedName )
			exceptionControl("switchParentOAMModule", API_INVALID_PARAMETER);
		}

		// Get Parent OAM module Name and error on match
		string parentOAMModule;
		try{
			getSystemConfig("ParentOAMModuleName", parentOAMModule);
		}
		catch(...)
		{
			exceptionControl("switchParentOAMModule", API_INVALID_PARAMETER);
		}

		if (parentOAMModule == moduleName )
			exceptionControl("switchParentOAMModule", API_INVALID_PARAMETER);

		// validate Module name
		int returnStatus = validateModule(moduleName);
		if (returnStatus != API_SUCCESS)
			exceptionControl("switchParentOAMModule", returnStatus);

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

			return;
		}

		// only make call if system is ACTIVE and module switching to is ACTIVE
		SystemStatus systemstatus;

		try {
			getSystemStatus(systemstatus);
		}
		catch (exception& ex)
		{}

		if (systemstatus.SystemOpState != oam::ACTIVE )
			exceptionControl("switchParentOAMModule", API_INVALID_STATE);

		int opState;
		bool degraded;
		try {
			getModuleStatus(moduleName, opState, degraded);
		}
		catch(...)
		{}

		if (opState != oam::ACTIVE)
			exceptionControl("switchParentOAMModule", API_INVALID_STATE);

        // build and send msg to stop system
        returnStatus = sendMsgToProcMgr(STOPSYSTEM, "", FORCEFUL, ACK_YES);

		if ( returnStatus != API_SUCCESS )
			exceptionControl("stopSystem", returnStatus);

        // build and send msg to switch configuration
        returnStatus = sendMsgToProcMgr(SWITCHOAMPARENT, moduleName, FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("switchParentOAMModule", returnStatus);

		//give  time for ProcMon/ProcMgr to get fully active on new pm
		sleep(10);

		// build and send msg to restart system
        returnStatus = sendMsgToProcMgr(STARTSYSTEM, "", FORCEFUL, ACK_YES);

        if (returnStatus != API_SUCCESS)
            exceptionControl("startSystem", returnStatus);

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
        ifstream File ("/etc/init.d/mysql-Calpont");
        if (!File)
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

		string cmd = "/etc/init.d/mysql-Calpont " + command;
		int status = system(cmd.c_str());
		if (status != 0 && action != MYSQL_STATUS)
			exceptionControl("actionMysqlCalpont", API_FAILURE);

		if (action == MYSQL_START || action == MYSQL_RESTART) {
			//get pid
			system("cat /usr/local/Calpont/mysql/db/*.pid > /tmp/mysql.pid");
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

			if (checkLogStatus("/tmp/mysql.status", "MySQL running")) {
				if ( state != ACTIVE )
				{
					//get pid
					system("cat /usr/local/Calpont/mysql/db/*.pid > /tmp/mysql.pid");
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
	* @brief	syslogAction
	*
	* purpose:	Take Action on Syslog Process
	*
	******************************************************************************************/
    void Oam::syslogAction( std::string action)
	{
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
			cmd = "/etc/init.d/" + systemlog + " " + action + " > /dev/null 2>&1";
	
		// take action on syslog service to make sure it running
		system(cmd.c_str());
	
		// if start/restart, delay to give time for syslog to get up and going
		pos = action.find("start",0);
		if (pos != string::npos)
			sleep(2);
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
		string cmd = "/usr/local/Calpont/bin/dbrmctl " + command + " > /dev/null 2>&1";
		system(cmd.c_str());
	
		return;
	}


    /***************************************************************************
     * PRIVATE FUNCTIONS
     ***************************************************************************/
    /***************************************************************************
     *
     * Function:  copyDatabaseFiles
     *
     * Purpose:   copy database files from src to dst
     *
     ****************************************************************************/
    void Oam::copyDatabaseFiles(std::string src, std::string dst)
    {
        std::vector<std::string> dbFileNames;
        std::string file_name;
        fs::path sourceDir(src);
        fs::directory_iterator iter(sourceDir);
        fs::directory_iterator end_iter;
        double total_bytes = 0;
        for( ; iter != end_iter ; ++iter)
        {
            fs::path source = *iter;
            if (!fs::is_directory(source) )
            {
                file_name = iter->leaf();
                if (file_name.find(".dat", file_name.length() - 4) != string::npos)
                {
                    dbFileNames.push_back(file_name);
                    total_bytes += fs::file_size(source);
                }
            }
        }

        // make sure there is enough disk space
        double avail_bytes = getFreeSpace(dst);
        if (avail_bytes < total_bytes)
        {
            char error[256];
            sprintf(error, "Insufficient Disk Space. Avail: %8.0f kb, Needed: %8.0f kb\n", avail_bytes/1024, total_bytes/1024);
            throw std::runtime_error(error);
        }

        boost::progress_display show_progress(dbFileNames.size());
        std::vector<std::string>::iterator dbfiles_iter = dbFileNames.begin();
        for( ; dbfiles_iter != dbFileNames.end() ; ++dbfiles_iter)
        {
            file_name = *dbfiles_iter;
            std::string sourceFile = src;
            std::string destFile = dst;

            sourceFile += "/";
            sourceFile += file_name;

            destFile += "/";
            destFile += file_name;

            fs::path source = sourceFile;
            fs::path destination  = destFile;
            if (fs::exists(destination))
                fs::remove(destination);
            fs::copy_file( source, destination );

            ++show_progress;
        }

    }

    /***************************************************************************
     *
     * Function:  exceptionControl
     *
     * Purpose:   exception control function
     *
     ****************************************************************************/

    void Oam::exceptionControl(std::string function, int returnStatus)
    {
        switch(returnStatus)
        {
            case API_INVALID_PARAMETER:
            {
                string msg = "Invalid Parameter passed in ";
                msg.append(function);
                msg.append(" API");
                throw runtime_error(msg);
            }
            break;
            case API_FILE_OPEN_ERROR:
            {
                string msg = "File Open error from ";
                msg.append(function);
                msg.append(" API");
                throw runtime_error(msg);
            }
            break;
            case API_TIMEOUT:
            {
                string msg = "Timeout error from ";
                msg.append(function);
                msg.append(" API");
                throw runtime_error(msg);
            }
            break;
            case API_DISABLED:
            {
                string msg = "API Disabled: ";
                msg.append(function);
                throw runtime_error(msg);
            }
            break;
            case API_FILE_ALREADY_EXIST:
            {
                string msg = "File Already Exist";
                throw runtime_error(msg);
            }
            break;
            case API_ALREADY_IN_PROGRESS:
            {
                string msg = "Already In Process";
                throw runtime_error(msg);
            }
            break;
            case API_FAILURE_DB_ERROR:
            {
                string msg = "Database Test Error";
                throw runtime_error(msg);
            }
            break;
            case API_INVALID_STATE:
            {
                string msg = "Target in an invalid state";
                throw runtime_error(msg);
            }
            break;
            case API_READONLY_PARAMETER:
            {
                string msg = "Parameter is Read-Only, can't update";
                throw runtime_error(msg);
            }
            break;
            case API_CPIMPORT_ACTIVE:
            {
                string msg = "A Bulk Load Process 'cpimport' is Active";
                throw runtime_error(msg);
            }
            break;
            case API_CONN_REFUSED:
            {
                string msg = "Connection refused";
                throw runtime_error(msg);
            }
            break;

            default:
            {
                string msg = "API Failure return in ";
                msg.append(function);
                msg.append(" API");
                throw runtime_error(msg);
            }
			break;
        } // end of switch
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
					//number of alarms
					receivedMSG >> numAlarms;

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

						alarmlist.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
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
				struct timespec ts = { 1, 0 };
				processor.write(obs, &ts);
			}
			catch(...)
			{
				processor.shutdown();
				throw std::runtime_error("error");
			}
			

			try {
				struct timespec ts1 = { 5, 0 };
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
			st = oam.getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        oam.exceptionControl("registerHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = oam.getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        oam.exceptionControl("registerHeartbeat", API_FAILURE);
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
	        oam.exceptionControl("registerHeartbeat", API_FAILURE);
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
			st = oam.getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        oam.exceptionControl("deregisterHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = oam.getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        oam.exceptionControl("deregisterHeartbeat", API_FAILURE);
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
	        oam.exceptionControl("deregisterHeartbeat", API_FAILURE);
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
			oam.getSystemConfig("ProcessHeartbeatPeriod", processHeartbeatPeriod);
		}
		catch(...)
		{
		}

		//skip sending if Heartbeat is disable
		if( processHeartbeatPeriod == -1 )
	        oam.exceptionControl("sendHeartbeat", API_DISABLED);

		// get current Module name
		string Module;
		oamModuleInfo_t st;
		try {
			st = oam.getModuleInfo();
			Module = boost::get<0>(st);
		}
		catch (...) {
	        oam.exceptionControl("sendHeartbeat", API_FAILURE);
		}

		// get current process Name
		string processName;
		myProcessStatus_t t;
		try {
			t = oam.getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
	        oam.exceptionControl("sendHeartbeat", API_FAILURE);
		}

        ByteStream msg;

        // setup message
        msg << (ByteStream::byte) HEARTBEAT_SEND;
        msg << Module;
        msg << processName;
        msg << oam.getCurrentTime();
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
	        			oam.exceptionControl("sendHeartbeat", API_TIMEOUT);
					}
				}
				else
				{
	           		procmgr.shutdown();
	        		oam.exceptionControl("sendHeartbeat", API_TIMEOUT);
				}
			}
            procmgr.shutdown();
        }
        catch(...)
        {
	        oam.exceptionControl("sendHeartbeat", API_FAILURE);
        }
	}
*/
} // end of namespace
// vim:ts=4 sw=4:

