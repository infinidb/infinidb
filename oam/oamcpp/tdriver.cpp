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
 *   dhill@srvengcm1.calpont.com 
 *
 *   Purpose: OAM C++ API tester
 *
 ***************************************************************************/

#include <string>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "liboamcpp.h"
using namespace oam;

using namespace snmpmanager;

class getModuleInfoTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getModuleInfoTest );

CPPUNIT_TEST( test1 );

CPPUNIT_TEST_SUITE_END();

private:
		string Svalue;
		int Ivalue;
		bool Bvalue;

public:
	void setUp() {
		setenv("CALPONT_HOME", "/home/buildslave/Buildbot/nightly/export/etc/", 1);
//		setenv("CALPONT_HOME", "/home/dhill/genii/export/etc/", 1);
	}

	void tearDown() {
	}

	void test1() {

		Oam oamapi;
		Svalue = oamapi.getCurrentTime();
		cout << "Current time is " << Svalue;
		CPPUNIT_ASSERT(!Svalue.empty());

		Bvalue = oamapi.isValidIP("111.222.333.444");
		CPPUNIT_ASSERT(Bvalue == true);

		Bvalue = oamapi.isValidIP("111.222.333");
		CPPUNIT_ASSERT(Bvalue == false);

		Bvalue = oamapi.isValidIP("1.2.3.4");
		CPPUNIT_ASSERT(Bvalue == true);

		Bvalue = oamapi.isValidIP("1.2.3.4444");
		CPPUNIT_ASSERT(Bvalue == false);

		Bvalue = oamapi.isValidIP("1111.222.333.444");
		CPPUNIT_ASSERT(Bvalue == false);


// can test on deve machine
//		oamModuleInfo_t t;

//		t = oamapi.getModuleInfo();

//		Svalue = get<0>(t);
//		CPPUNIT_ASSERT(Svalue == "dm1");

//		Svalue = get<1>(t);
//		CPPUNIT_ASSERT(Svalue == "dm1");

//		Ivalue = get<2>(t);
//		CPPUNIT_ASSERT(Ivalue == MASTER_YES);
	};
}; 

class getSystemConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getSystemConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
//CPPUNIT_TEST( test3 );
//CPPUNIT_TEST( test4 );
//CPPUNIT_TEST( test5 );
CPPUNIT_TEST_EXCEPTION( test6, std::runtime_error );
//CPPUNIT_TEST_EXCEPTION( test7, std::runtime_error );
CPPUNIT_TEST( test8 );
CPPUNIT_TEST_EXCEPTION( test9, std::runtime_error );
CPPUNIT_TEST( test10 );


CPPUNIT_TEST_SUITE_END();

private:
	string Svalue;
	int Ivalue;

public:
	void setUp() {
		setenv("CALPONT_HOME", "/home/buildslave/Buildbot/nightly/export/etc/", 1);
//		setenv("CALPONT_HOME", "/home/dhill/genii/export/etc/", 1);
	}

	void tearDown() {
	}

	void test1() {
		SystemConfig systemconfig;

		Oam oamapi;
		oamapi.getSystemConfig(systemconfig);

		Ivalue = systemconfig.ModuleHeartbeatPeriod;
		CPPUNIT_ASSERT(Ivalue != -1);

		Ivalue = systemconfig.ModuleHeartbeatCount;
		CPPUNIT_ASSERT(Ivalue != -1);

//		Ivalue = systemconfig.ProcessHeartbeatPeriod;
//		CPPUNIT_ASSERT(Ivalue != -2);

		Svalue = systemconfig.NMSIPAddr;
		CPPUNIT_ASSERT(!Svalue.empty());
	};

	void test2() {
		SystemModuleTypeConfig systemmoduletypeconfig;

		Oam oamapi;
		oamapi.getSystemConfig(systemmoduletypeconfig);

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
				// end of list
				break;

			Svalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
			CPPUNIT_ASSERT(!Svalue.empty());

			Svalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleDesc;
			CPPUNIT_ASSERT(!Svalue.empty());

			Ivalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			CPPUNIT_ASSERT(Ivalue != -1);

			Ivalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUCriticalThreshold;
			CPPUNIT_ASSERT(Ivalue != -1);

			Ivalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMajorThreshold;
			CPPUNIT_ASSERT(Ivalue != -1);

			Ivalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorThreshold;
			CPPUNIT_ASSERT(Ivalue != -1);

			Ivalue = systemmoduletypeconfig.moduletypeconfig[i].ModuleCPUMinorClearThreshold;
			CPPUNIT_ASSERT(Ivalue != -1);
		}
	};

/*	void test3() {
		ModuleConfig moduleconfig;
		const string Modulename = "dm1";

		Oam oamapi;
		oamapi.getSystemConfig(Modulename, moduleconfig);

		Svalue = moduleconfig.ModuleName;
		CPPUNIT_ASSERT(!Svalue.empty());

	};
*/
	void test4() {
		ModuleConfig moduleconfig;

		Oam oamapi;
		oamapi.getSystemConfig(moduleconfig);

		Svalue = moduleconfig.ModuleName;
		CPPUNIT_ASSERT(!Svalue.empty());

	};

	void test5() {
		Oam oamapi;
		oamapi.setSystemConfig("SystemVersion", "V2.0.2.3");

		oamapi.getSystemConfig("SystemVersion", Svalue);

		CPPUNIT_ASSERT(Svalue == "V2.0.2.3");
	};

	void test6() {
		Oam oamapi;
		oamapi.getSystemConfig("SystemVersionBad", Svalue);
		CPPUNIT_ASSERT(Svalue.size() == 0);
	};

	void test7() {
		Oam oamapi;
		oamapi.setSystemConfig("SystemVersionBad", "V2.0.2.3");
	};

	void test8() {
		Oam oamapi;
		oamapi.setSystemConfig("ModuleHeartbeatPeriod", 5);

		oamapi.getSystemConfig("ModuleHeartbeatPeriod", Ivalue);

		CPPUNIT_ASSERT(Ivalue == 5);
	};

	void test9() {
		Oam oamapi;
		oamapi.getSystemConfig("ModuleHeartbeatPeriodBad", Ivalue);
		CPPUNIT_ASSERT(Ivalue == 0);
	};

	void test10() {
		Oam oamapi;
		oamapi.setSystemConfig("ModuleCPUMajorThreshold1", 7500);

		oamapi.getSystemConfig("ModuleCPUMajorThreshold1", Ivalue);

		CPPUNIT_ASSERT(Ivalue == 7500);
	};


};
/*
class getSystemStatusTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getSystemStatusTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );


CPPUNIT_TEST_SUITE_END();

private:
	Oam oamapi;
	string Svalue;
	int Ivalue;

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		SystemStatus systemstatus;

		oamapi.getSystemStatus(systemstatus);

		Svalue = systemstatus.SystemOpState;
		CPPUNIT_ASSERT(!Svalue.empty());

		for( unsigned int i = 0 ; i < systemstatus.systemModulestatus.Modulestatus.size(); i++)
		{
			if( systemstatus.systemModulestatus.Modulestatus[i].Module.empty() )
				// end of list
				break;

			Svalue = systemstatus.systemModulestatus.Modulestatus[i].Module;
			CPPUNIT_ASSERT(!Svalue.empty());

			Svalue = systemstatus.systemModulestatus.Modulestatus[i].ModuleOpState;
			CPPUNIT_ASSERT(!Svalue.empty());
		}
	};

	void test2() {
		oamapi.getModuleStatus("dm1", Svalue);

		CPPUNIT_ASSERT(!Svalue.empty());
	};

	void test3() {
		oamapi.setSystemStatus("ACTIVE");

		SystemStatus systemstatus;

		oamapi.getSystemStatus(systemstatus);

		Svalue = systemstatus.SystemOpState;

		CPPUNIT_ASSERT(Svalue == "ACTIVE");
		oamapi.setSystemStatus("AUTO_OFFLINE");
	};

	void test4() {
		oamapi.setModuleStatus("dm1", "ACTIVE");

		oamapi.getModuleStatus("dm1", Svalue);

		CPPUNIT_ASSERT(Svalue == "ACTIVE");
		oamapi.setModuleStatus("dm1", "AUTO_OFFLINE");
	};


};
*/
class getProcessConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getProcessConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST_EXCEPTION( test3, std::runtime_error );
CPPUNIT_TEST( test4 );
CPPUNIT_TEST_EXCEPTION( test5, std::runtime_error );

CPPUNIT_TEST_SUITE_END();

private:
	string Svalue;
	int Ivalue;

public:
	void setUp() {
		setenv("CALPONT_HOME", "/home/buildslave/Buildbot/nightly/export/etc/", 1);
//		setenv("CALPONT_HOME", "/home/dhill/genii/export/etc/", 1);
	}

	void tearDown() {
	}

	void test1() {
		SystemProcessConfig systemprocessconfig;

		Oam oamapi;
		oamapi.getProcessConfig(systemprocessconfig);

		for( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
		{
			Svalue = systemprocessconfig.processconfig[i].ProcessName;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Svalue = systemprocessconfig.processconfig[i].ModuleType;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Svalue = systemprocessconfig.processconfig[i].ProcessLocation;
			CPPUNIT_ASSERT(!Svalue.empty());

			for( int j = 0 ; j < oam::MAX_ARGUMENTS; j++) {
				if (systemprocessconfig.processconfig[i].ProcessArgs[j].empty())
					break;
				Svalue = systemprocessconfig.processconfig[i].ProcessArgs[j];
				CPPUNIT_ASSERT(!Svalue.empty());
			}

			Ivalue = systemprocessconfig.processconfig[i].BootLaunch;
			CPPUNIT_ASSERT(Ivalue != -1);
	
			Ivalue = systemprocessconfig.processconfig[i].LaunchID;
			CPPUNIT_ASSERT(Ivalue != -1);
	
			for( int j = 0 ; j < MAX_DEPENDANCY; j++) {
				if (systemprocessconfig.processconfig[i].DepProcessName[j].empty())
					break;
				Svalue = systemprocessconfig.processconfig[i].DepProcessName[j];
				CPPUNIT_ASSERT(!Svalue.empty());
				Svalue = systemprocessconfig.processconfig[i].DepModuleName[j];
				CPPUNIT_ASSERT(!Svalue.empty());
			}
		}
	};

	void test2() {
		ProcessConfig processconfig;

		Oam oamapi;
		oamapi.getProcessConfig("ProcessManager", "dm1", processconfig);
		
		Svalue = processconfig.ProcessName;
		CPPUNIT_ASSERT(!Svalue.empty());

		Svalue = processconfig.ModuleType;
		CPPUNIT_ASSERT(!Svalue.empty());

		Svalue = processconfig.ProcessLocation;
		CPPUNIT_ASSERT(!Svalue.empty());

		for( int j = 0 ; j < oam::MAX_ARGUMENTS; j++) {
			if (processconfig.ProcessArgs[j].empty())
				break;
			Svalue = processconfig.ProcessArgs[j];
			CPPUNIT_ASSERT(!Svalue.empty());
		}

		Ivalue = processconfig.BootLaunch;
		CPPUNIT_ASSERT(Ivalue != -1);

		Ivalue = processconfig.LaunchID;
		CPPUNIT_ASSERT(Ivalue != -1);

		for( int j = 0 ; j < MAX_DEPENDANCY; j++) {
			if (processconfig.DepProcessName[j].empty())
				break;
			Svalue = processconfig.DepProcessName[j];
			CPPUNIT_ASSERT(!Svalue.empty());
			Svalue = processconfig.DepModuleName[j];
			CPPUNIT_ASSERT(!Svalue.empty());
		}
	};

	void test3() {
		ProcessConfig processconfig;
		Oam oamapi;
		oamapi.getProcessConfig("SNMPTrapDaemonBAD", "dm1", processconfig);
		CPPUNIT_ASSERT(Svalue.size() == 0);
	};

	void test4() {
		Oam oamapi;
		oamapi.setProcessConfig("ProcessManager", "dm1", "BootLaunch", 10);

		oamapi.getProcessConfig("ProcessManager", "dm1", "BootLaunch", Ivalue);

		CPPUNIT_ASSERT(Ivalue == 10);
	};

	void test5() {
		ProcessConfig processconfig;
		Oam oamapi;
		oamapi.getProcessConfig("ProcessManager", "dm1", "ModuleTypeBAD", Svalue);
		CPPUNIT_ASSERT(Svalue.size() == 0);
	};

};
/*
class getProcessStatusTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getProcessConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST_EXCEPTION( test3, std::runtime_error );
//CPPUNIT_TEST( test4 );
CPPUNIT_TEST_EXCEPTION( test5, std::runtime_error );

CPPUNIT_TEST_SUITE_END();

private:
	Oam oamapi;
	string Svalue;
	int Ivalue;

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		SystemProcessStatus systemprocessstatus;

		oamapi.getProcessStatus(systemprocessstatus);

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			Svalue = systemprocessstatus.processstatus[i].ProcessName;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Svalue = systemprocessstatus.processstatus[i].Module;
			CPPUNIT_ASSERT(!Svalue.empty());

			Ivalue = systemprocessstatus.processstatus[i].ProcessID;
			CPPUNIT_ASSERT(Ivalue != -1);
	
			Svalue = systemprocessstatus.processstatus[i].StateChangeDate;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Svalue = systemprocessstatus.processstatus[i].ProcessOpState;
			CPPUNIT_ASSERT(!Svalue.empty());

		}
	};

	void test2() {
		ProcessStatus processstatus;

		oamapi.getProcessStatus("ProcessManager", "dm1", processstatus);
		
		Svalue = processstatus.ProcessName;
		CPPUNIT_ASSERT(!Svalue.empty());

		Svalue = processstatus.Module;
		CPPUNIT_ASSERT(!Svalue.empty());

		Ivalue = processstatus.ProcessID;
		CPPUNIT_ASSERT(Ivalue != -1);

		Svalue = processstatus.StateChangeDate;
		CPPUNIT_ASSERT(!Svalue.empty());

		Svalue = processstatus.ProcessOpState;
		CPPUNIT_ASSERT(!Svalue.empty());
	
	};

	void test3() {
		ProcessStatus processstatus;
		oamapi.getProcessStatus("SNMPTrapDaemonBAD", "dm1", processstatus);
		CPPUNIT_ASSERT(Svalue.size() == 0);
	};

	void test4() {
		oamapi.setProcessStatus("ProcessManager", "dm1", "StateChangeDate", "1234567");

		oamapi.getProcessStatus("ProcessManager", "dm1", "StateChangeDate", Svalue);

		CPPUNIT_ASSERT(Svalue == "1234567");
	};

	void test5() {
		oamapi.getProcessStatus("ProcessManager", "dm1", "StateChangeDateBAD", Svalue);
		CPPUNIT_ASSERT(Svalue.size() == 0);
	};

	void test6() {
		oamapi.setProcessStatus("ProcessManager", "dm1", "ProcessID", 10);

		oamapi.getProcessStatus("ProcessManager", "dm1", "ProcessID", Ivalue);

		CPPUNIT_ASSERT(Ivalue == 10);
	};

};
*/
class getAlarmConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( getAlarmConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST_EXCEPTION( test3, std::runtime_error );
//CPPUNIT_TEST( test4 );

CPPUNIT_TEST_SUITE_END();

private:
	string Svalue;
	int Ivalue;

public:
	void setUp() {
		setenv("CALPONT_HOME", "/home/buildslave/Buildbot/nightly/export/etc/", 1);
//		setenv("CALPONT_HOME", "/home/dhill/genii/export/etc/", 1);
	}

	void tearDown() {
	}

	void test1() {
		AlarmConfig alarmconfig;
		Oam oamapi;

		for( int alarmID = 1 ; alarmID < MAX_ALARM_ID; alarmID++)
		{
			oamapi.getAlarmConfig(alarmID, alarmconfig);

			Svalue = alarmconfig.BriefDesc;
			CPPUNIT_ASSERT(!Svalue.empty());

			Svalue = alarmconfig.DetailedDesc;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Svalue = alarmconfig.Severity;
			CPPUNIT_ASSERT(!Svalue.empty());
	
			Ivalue = alarmconfig.Threshold;
			CPPUNIT_ASSERT(Ivalue != -1);
	
			Ivalue = alarmconfig.Occurrences;
			CPPUNIT_ASSERT(Ivalue != -1);
	
			Svalue = alarmconfig.LastIssueTime;
			CPPUNIT_ASSERT(!Svalue.empty());
			}
 	};

	void test2() {
		Oam oamapi;
		oamapi.setAlarmConfig(CPU_USAGE_MED, "Threshold", 20);

		oamapi.getAlarmConfig(CPU_USAGE_MED, "Threshold", Ivalue);

		CPPUNIT_ASSERT(Ivalue == 20);
	};

	void test3() {
		Oam oamapi;
		oamapi.getAlarmConfig(CPU_USAGE_MED, "ThresholdBAD", Ivalue);
		CPPUNIT_ASSERT(Ivalue == 0);
	};

/*	void test4() {
		// test getActiveAlarm API
		AlarmList activeAlarm;
		#if 1
		Oam oamapi;
		oamapi.getActiveAlarms (activeAlarm);
		#endif
	};
*/
}; 

CPPUNIT_TEST_SUITE_REGISTRATION( getModuleInfoTest );
CPPUNIT_TEST_SUITE_REGISTRATION( getSystemConfigTest );
//CPPUNIT_TEST_SUITE_REGISTRATION( getSystemStatusTest );
//CPPUNIT_TEST_SUITE_REGISTRATION( getProcessStatusTest );
CPPUNIT_TEST_SUITE_REGISTRATION( getProcessConfigTest );
CPPUNIT_TEST_SUITE_REGISTRATION( getAlarmConfigTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


