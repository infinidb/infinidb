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
* $Id: tdriver.cpp 3072 2013-04-04 19:04:45Z rdempsey $
*
 *   dhill@srvengcm1.calpont.com 
 *
 *   Purpose: Calpont Console tester
 *
 ***************************************************************************/

#include <stdexcept>
#include <iostream>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

class GeneralCmdsTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( GeneralCmdsTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );
CPPUNIT_TEST( test5 );
CPPUNIT_TEST( test6 );
CPPUNIT_TEST( test7 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole help > tdriver.output");
 	}

	void test2() {
		system("./calpontConsole ? >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole help verbose >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole help getAlarmConfig >> tdriver.output");
 	}

	void test5() {
		system("./calpontConsole exit >> tdriver.output");
 	}

	void test6() {
		system("./calpontConsole quit >> tdriver.output");
 	}

	void test7() {
		system("./calpontConsole system ls -ltr >> tdriver.output");
 	}
}; 

class AlarmConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( AlarmConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole getAlarmConfig >> tdriver.output");
 	}

	void test2() {
		system("./calpontConsole getAlarmConfig 1 >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole setAlarmConfig 1 Threshold 50 >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole getAlarmConfig 1 >> tdriver.output");
 	}

}; 

class ProcessConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ProcessConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );
CPPUNIT_TEST( test5 );
CPPUNIT_TEST( test6 );
CPPUNIT_TEST( test7 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole getProcessConfig >> tdriver.output");
 	}

	void test2() {
		system("./calpontConsole getProcessConfig ProcessManager dm1 >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole getProcessConfig ProcessManager dm1 LaunchID >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole setProcessConfig ProcessManager dm1 LaunchID 1 >> tdriver.output");
 	}

	void test5() {
		system("./calpontConsole getProcessConfig ProcessManager dm1 LaunchID >> tdriver.output");
 	}

	void test6() {
		system("./calpontConsole setProcessConfig ProcessManager dm1 LaunchID 3 >> tdriver.output");
 	}

	void test7() {
		system("./calpontConsole getProcessConfig ProcessManager dm1 LaunchID >> tdriver.output");
 	}

};

class ServerConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ServerConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );
CPPUNIT_TEST( test5 );
CPPUNIT_TEST( test6 );
CPPUNIT_TEST( test7 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole getModuleConfig >> tdriver.output");
 	}

	void test2() {
		system("./calpontConsole getModuleConfig dm1 >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole getModuleConfig dm1 ServerCPUCriticalThreshold >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole setModuleConfig dm1 ServerCPUCriticalThreshold 9999 >> tdriver.output");
 	}

	void test5() {
		system("./calpontConsole getModuleConfig dm1 ServerCPUCriticalThreshold >> tdriver.output");
 	}

	void test6() {
		system("./calpontConsole setModuleConfig dm1 ServerCPUCriticalThreshold 9000 >> tdriver.output");
 	}

	void test7() {
		system("./calpontConsole getModuleConfig dm1 ServerCPUCriticalThreshold >> tdriver.output");
 	}

};

class SystemConfigTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( SystemConfigTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole getSystemConfig >> tdriver.output");
 	}

	void test2() {
		system("./calpontConsole getSystemConfig SystemVersion >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole setSystemConfig SystemVersion 2.0.0.0 >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole getSystemConfig SystemVersion >> tdriver.output");
 	}

};

class SystemActionTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( SystemActionTest );

CPPUNIT_TEST( test1 );		// valid, but will fail command
CPPUNIT_TEST( test2 );		// valid, but will fail command
CPPUNIT_TEST( test3 );		// valid, but will fail command
CPPUNIT_TEST( test4 );		// valid, but will fail command


CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		system("./calpontConsole stopSystem >> tdriver.output"); 
 	}

	void test2() {
		system("./calpontConsole shutdownSystem >> tdriver.output");
 	}

	void test3() {
		system("./calpontConsole startSystem >> tdriver.output");
 	}

	void test4() {
		system("./calpontConsole restartSystem >> tdriver.output");
 	}

};


//CPPUNIT_TEST_SUITE_REGISTRATION( SystemActionTest );
CPPUNIT_TEST_SUITE_REGISTRATION( SystemConfigTest );
CPPUNIT_TEST_SUITE_REGISTRATION( ServerConfigTest );
CPPUNIT_TEST_SUITE_REGISTRATION( ProcessConfigTest );
CPPUNIT_TEST_SUITE_REGISTRATION( AlarmConfigTest );
CPPUNIT_TEST_SUITE_REGISTRATION( GeneralCmdsTest );

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


