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
#include <fstream>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
#include "configstream.h"
using namespace config;

class ConfigFileTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ConfigFileTest );

CPPUNIT_TEST( test1 );

CPPUNIT_TEST_EXCEPTION( test2, std::runtime_error );
CPPUNIT_TEST( test3 );
CPPUNIT_TEST( test4 );
CPPUNIT_TEST_EXCEPTION( test5, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( test6, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( test7, std::invalid_argument );
CPPUNIT_TEST_EXCEPTION( test8, std::invalid_argument );
CPPUNIT_TEST( test9 );
CPPUNIT_TEST( test10 );
CPPUNIT_TEST( test11 );
CPPUNIT_TEST( test12 );
CPPUNIT_TEST_EXCEPTION( test13_1, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( test13_2, std::runtime_error );
CPPUNIT_TEST( test14 );
CPPUNIT_TEST_SUITE_END();

private:
public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		Config* c1 = Config::makeConfig("./Calpont.xml");
		string value;
		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value == "Message");

		value = c1->getConfig("Message", "xName");
		CPPUNIT_ASSERT(value.size() == 0);
                Config::deleteInstanceMap();
	}

	void test2() {
		Config* c1 = Config::makeConfig("./yadayada.xml");
		string value;

		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value.size() == 0);

		value = c1->getConfig("Message", "xName");
		CPPUNIT_ASSERT(value.size() == 0);

                Config::deleteInstanceMap();
	}

	void test3() {
		Config* c1;
		string value;

		for (int i = 0; i < 1000; i++)
		{
			c1 = Config::makeConfig("./Calpont.xml");
			value = c1->getConfig("Message", "Name");
			assert(value == "Message");
		}
                Config::deleteInstanceMap();
	}

	void test4() {
		Config* c1 = Config::makeConfig("./Calpont.xml");
		string value;

		value = c1->getConfig("SystemConfig", "SystemVersion");
		c1->setConfig("SystemConfig", "SystemVersion", "2.2.versionversionversion");
		value = c1->getConfig("SystemConfig", "SystemVersion");
		CPPUNIT_ASSERT(value == "2.2.versionversionversion");

		::unlink("./Calpont.xml.new");
		c1->write("./Calpont.xml.new");

		value = c1->getConfig("SystemConfig", "SystemVersion");
		CPPUNIT_ASSERT(value == "2.2.versionversionversion");

		c1->setConfig("SystemConfig", "SystemVersion1", "V1.x");
		value = c1->getConfig("SystemConfig", "SystemVersion1");
		CPPUNIT_ASSERT(value == "V1.x");

		c1->setConfig("SystemConfig1", "SystemVersion1", "Vx.x");
		value = c1->getConfig("SystemConfig1", "SystemVersion1");
		CPPUNIT_ASSERT(value == "Vx.x");

		c1->write("./Calpont.xml.new");
		Config* c2 = Config::makeConfig("./Calpont.xml.new");
		value = c2->getConfig("SystemConfig1", "SystemVersion1");
		CPPUNIT_ASSERT(value == "Vx.x");
		c2->setConfig("SystemConfig", "SystemVersion1", "V1.1");
		value = c2->getConfig("SystemConfig", "SystemVersion1");
		CPPUNIT_ASSERT(value == "V1.1");
		c2->write();
                Config::deleteInstanceMap();
	}

	void test5() {
		Config* c1 = Config::makeConfig("./Calpont.xml");
		c1->write("/cantwritethis");
                Config::deleteInstanceMap();
	}

	void test6() {
		Config* c1 = Config::makeConfig("./XCalpont.xml");
		// compiler warning...we won't actually get here
		c1 = 0;
	}

	void test7() {
		Config* c1 = Config::makeConfig("./Calpont.xml");
		string s;
		string n;
		string v;
		c1->setConfig(s, n, v);
                Config::deleteInstanceMap();
	}

	void test8() {
		Config* c1 = Config::makeConfig("./Calpont.xml");
		string s;
		string n;
		string v;
		v = c1->getConfig(s, n);
                Config::deleteInstanceMap();
	}

	void test9() {
		string value;

		Config* c1 = Config::makeConfig("./Calpont.xml");
		Config* c2 = Config::makeConfig("./Calpont.xml.new");

		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value == "Message");

		value = c2->getConfig("SystemConfig", "SystemVersion1");
		CPPUNIT_ASSERT(value == "V1.1");
                Config::deleteInstanceMap();
	}

	void test10() {
		string value;

		setenv("CALPONT_CONFIG_FILE", "./Calpont.xml", 1);
		Config* c1 = Config::makeConfig();

		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value == "Message");
                Config::deleteInstanceMap();

	}

	void test11() {
		string value;
		struct stat stat_buf;
		struct utimbuf utime_buf;

		CPPUNIT_ASSERT(stat("./Calpont.xml.new", &stat_buf) == 0);

		Config* c1 = Config::makeConfig("./Calpont.xml.new");

		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value == "Message");

		utime_buf.actime = utime_buf.modtime = stat_buf.st_mtime + 1;
		CPPUNIT_ASSERT(utime("./Calpont.xml.new", &utime_buf) == 0);

		value = c1->getConfig("Message", "Name");
		CPPUNIT_ASSERT(value == "Message");
                Config::deleteInstanceMap();
	}

	void test12() {
		string value;
		int64_t ival;
		uint64_t uval;

		value = "10";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == 10);
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 10);

		value = "0x10";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == 0x10);
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 0x10);

		value = "010";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == 010);
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 010);

		value = "-10";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == -10);

		value = "10K";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024));

		value = "10k";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024));

		value = "10M";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024 * 1024));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024 * 1024));

		value = "10m";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024 * 1024));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024 * 1024));

		value = "10G";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024 * 1024 * 1024LL));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024 * 1024 * 1024ULL));

		value = "10g";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024 * 1024 * 1024LL));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024 * 1024 * 1024ULL));

		value = "10MB";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == (10 * 1024 * 1024));
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == (10 * 1024 * 1024));

		value = "0x7afafafafafafafa";
		ival = Config::fromText(value);
		CPPUNIT_ASSERT(ival == 0x7afafafafafafafaLL);
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 0x7afafafafafafafaULL);

		value = "-0x7afafafafafafafa";
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 0x8505050505050506ULL);

		value = "-1";
		uval = Config::uFromText(value);
		CPPUNIT_ASSERT(uval == 0xffffffffffffffffULL);

	}

	void test13_1() {
		string value;
		int64_t ival;

		value = "2.2MB"; //invalid char causes throw
		ival = Config::fromText(value);
	}

	void test13_2() {
		string value;
		int64_t ival;

		value = "10,000"; //invalid char causes throw
		ival = Config::fromText(value);
	}

	void test14() {
		ByteStream bs;
		ifstream ifs("./Calpont.xml");
		ifs >> bs;
		string id(".");
		string value;
		{
			ConfigStream cs(bs, id);
			value = cs.getConfig("Message", "Name");
			CPPUNIT_ASSERT(value == "Message");
		}
		string bss(reinterpret_cast<const char*>(bs.buf()), bs.length());
		{
			ConfigStream cs(bss, id);
			value = cs.getConfig("Message", "Name");
			CPPUNIT_ASSERT(value == "Message");
		}
		{
			ConfigStream cs(bss.c_str(), id);
			value = cs.getConfig("Message", "Name");
			CPPUNIT_ASSERT(value == "Message");
		}
	}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( ConfigFileTest );

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



