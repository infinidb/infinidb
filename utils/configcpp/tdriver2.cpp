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

#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <iostream>
#include <iomanip>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "configcpp.h"
#include "writeonce.h"
using namespace config;

namespace
{
const string cf("./woparms.dat");
}

class WOConfigFileTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( WOConfigFileTest );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST_EXCEPTION( test2, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( test3, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( test4, std::runtime_error );
CPPUNIT_TEST( test5 );

CPPUNIT_TEST_SUITE_END();

private:

public:
void setUp() {
	unlink(cf.c_str());
}

void tearDown() {
	unlink(cf.c_str());
}

void test1() {
	WriteOnceConfig woc(cf);
	CPPUNIT_ASSERT(woc.owns("PrimitiveServers", "LBID_Shift"));
	CPPUNIT_ASSERT(woc.owns("SystemConfig", "DBRootCount"));
	CPPUNIT_ASSERT(woc.owns("SystemConfig", "DBRMRoot"));

	CPPUNIT_ASSERT(!woc.owns("dummy", "dummy"));

	int vali;

	vali = Config::fromText(woc.getConfig("PrimitiveServers", "LBID_Shift"));
	CPPUNIT_ASSERT(vali == 13);

	woc.setConfig("SystemConfig", "DBRootCount", "10");
	vali = Config::fromText(woc.getConfig("SystemConfig", "DBRootCount"));
	CPPUNIT_ASSERT(vali == 10);

	WriteOnceConfig woc2(cf.c_str());
	vali = Config::fromText(woc2.getConfig("SystemConfig", "DBRootCount"));
	CPPUNIT_ASSERT(vali == 10);
}

void test2() {
	WriteOnceConfig woc(cf);
	woc.getConfig("dummy", "dummy");
}

void test3() {
	WriteOnceConfig woc(cf);
	woc.setConfig("dummy", "dummy", "100");
}

void test4() {
	WriteOnceConfig woc(cf);
	woc.setConfig("SystemConfig", "DBRootCount", "10");
	woc.setConfig("SystemConfig", "DBRootCount", "11");
}

void test5() {
	WriteOnceConfig woc(cf);
	woc.setConfig("SystemConfig", "DBRootCount", "10");
	woc.setConfig("SystemConfig", "DBRootCount", "11", true);
}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( WOConfigFileTest );

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

