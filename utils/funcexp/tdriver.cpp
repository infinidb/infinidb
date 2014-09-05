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

/**
* $Id$
*/

#include <iostream>
#include <string>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include "dataconvert.h"
#include "funchelpers.h"
using namespace funcexp;

class FuncExpTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( FuncExpTest );

CPPUNIT_TEST( fe_getnumbertest );
CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

void fe_getnumbertest()
{
	const int MAX_VALS=5;
	struct Check {
		const char* str;
		int         numvals;
		int         vals[MAX_VALS];
	};

	Check tests[] =
	{
			{ " ", 0, {} },
			{ "0", 1, {0} },
			{ "0.0", 2, {0,0} },
			{ "0:0.0", 3, {0,0,0} },
			{ "-0", 1, {0} },
			{ "0000-000", 1, {0} },
			{ "-2", 1, {-2} },
			{ "2223", 1, {2223} },
			{ "37", 1, {37} },
			{ "393", 1, {393} },
			{ "39-3", 1, {39} },
			{ "17-", 1, {17} },
			{ "10.2303", 2, {10,2303} },
			{ "20:10.2303", 3, {20,10,2303} },
			{ "17:37", 2, {17,37} },
	};

	string func = "ADD";
	int a1[MAX_VALS] = { 0xdeadbeef };

	for (unsigned i = 0; i < sizeof(tests)/sizeof(Check); i++)
	{
		int rc1 = helpers::getNumbers(tests[i].str,a1,func);

		cout << "For input \"" << tests[i].str << "\", numbers = " << rc1 << ",[";

		bool check = true;
		if (rc1 != tests[i].numvals)
			check = false;
		for (int j = 0; j < rc1; ++j)
		{
			if (j > 0)
				cout << ',';
			cout << a1[j];
			if (a1[j] != tests[i].vals[j])
			{
				check = false;
			}
		}
		cout << ']' << endl;

		CPPUNIT_ASSERT( check );
	}
}

};

CPPUNIT_TEST_SUITE_REGISTRATION( FuncExpTest );

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



