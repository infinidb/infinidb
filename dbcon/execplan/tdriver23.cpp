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
#include <typeinfo>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>

#include "calpontsystemcatalog.h"
using namespace execplan;

class TDriver23 : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( TDriver23 );

CPPUNIT_TEST( test1 );
CPPUNIT_TEST( test2 );

CPPUNIT_TEST_SUITE_END();

private:

public:

boost::shared_ptr<CalpontSystemCatalog> syscat;
CalpontSystemCatalog::OID o;
CalpontSystemCatalog::ColType ct;
   
void setUp() {
	syscat = CalpontSystemCatalog::makeCalpontSystemCatalog();
}

void tearDown() {
	CalpontSystemCatalog::removeCalpontSystemCatalog();
}

void test1() {
	o = 1009;
	ct = syscat->colType(o);
/*
cw: 4 dt: int do: 0 lo: 0 to: 0 cp: 8 sc: 0 pr: 10 od: 1009 ct: 0
*/

	CPPUNIT_ASSERT(ct.columnOID == 1009);
	CPPUNIT_ASSERT(ct.colWidth == 4);
	CPPUNIT_ASSERT(ct.colPosition == 8);
	CPPUNIT_ASSERT(ct.compressionType == 0);

	o = 2001;
	ct = syscat->colType(o);
	CPPUNIT_ASSERT(ct.columnOID == 0);

	ct = syscat->colTypeDct(o);
/*
cw: 65 dt: varchar do: 2001 lo: 2002 to: 2003 cp: 0 sc: 0 pr: 10 od: 1001 ct: 0
*/
	CPPUNIT_ASSERT(ct.columnOID == 1001);
	CPPUNIT_ASSERT(ct.colWidth == 65);
	CPPUNIT_ASSERT(ct.ddn.dictOID == 2001);
	CPPUNIT_ASSERT(ct.colPosition == 0);
	CPPUNIT_ASSERT(ct.compressionType == 0);

	o = 3024;
	ct = syscat->colType(o);
/*
        int32_t colWidth;
        ConstraintType constraintType;
        ColDataType colDataType;
        DictOID ddn;
        boost::any defaultValue;
        int32_t colPosition;    // temporally put here. may need to have ColInfo struct later
        int32_t scale;  //number after decimal points
        int32_t precision; 
		int32_t compressionType;
        OID columnOID;
cw: 25 dt: char do: 3027 lo: -2147483648 to: -2147483648 cp: 1 sc: 0 pr: 10 od: 3024 comtype: 0
*/
	CPPUNIT_ASSERT(ct.columnOID == 3024);
	CPPUNIT_ASSERT(ct.colWidth == 25);
	CPPUNIT_ASSERT(ct.ddn.dictOID == 3027);
	CPPUNIT_ASSERT(ct.colPosition == 1);
	CPPUNIT_ASSERT(ct.scale == 0);
	CPPUNIT_ASSERT(ct.precision == 10);
	CPPUNIT_ASSERT(ct.compressionType == 0);

	o = 3027;
	ct = syscat->colType(o);
/*
cw: 0 dt: medint do: 0 lo: 0 to: 0 cp: -1 sc: 0 pr: -1 od: 0 comtype: 0
*/
	CPPUNIT_ASSERT(ct.columnOID == 0);

	ct = syscat->colTypeDct(o);
	CPPUNIT_ASSERT(ct.columnOID == 3024);
	CPPUNIT_ASSERT(ct.colWidth == 25);
	CPPUNIT_ASSERT(ct.ddn.dictOID == 3027);
	CPPUNIT_ASSERT(ct.colPosition == 1);
	CPPUNIT_ASSERT(ct.scale == 0);
	CPPUNIT_ASSERT(ct.precision == 10);
	CPPUNIT_ASSERT(ct.compressionType == 0);

}  

void test2()
{
	syscat->identity(CalpontSystemCatalog::EC);
	test1();
}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( TDriver23 );

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


