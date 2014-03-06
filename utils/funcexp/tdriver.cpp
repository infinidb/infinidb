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
using namespace dataconvert;
#define MAX_DAY_NUMBER 3652424L

#include "functioncolumn.h"
#include "intervalcolumn.h"
using namespace execplan;

#include "timeextract.h"

class FuncExpTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( FuncExpTest );

CPPUNIT_TEST( fe_getnumbertest );
CPPUNIT_TEST( fe_strtodatetest );
CPPUNIT_TEST( fe_dateaddtest );
CPPUNIT_TEST( fe_daynametest );
CPPUNIT_TEST( fe_fromunixtimetest );
CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void show_datetime_debugs( const string & nm, const DateTime& d1 )
	{
		cout << nm << ".day     = " << setw(10) << d1.day << endl;
		cout << nm << ".month   = " << setw(10) << d1.month << endl;
		cout << nm << ".year    = " << setw(10) << d1.year << endl;
		cout << nm << ".hour    = " << setw(10) << d1.hour << endl;
		cout << nm << ".minute  = " << setw(10) << d1.minute << endl;
		cout << nm << ".second  = " << setw(10) << d1.second <<  endl;
		cout << nm << ".msecond = " << setw(10) << d1.msecond << endl;
	}

	void fe_strtodatetest()
	{
		struct DateCheck {
			const char* inputstr;
			const char* formatstr;
			DateTime    date;
		};

		DateCheck date_tests[] =
		{
			{ "2009","%Y",DateTime(2009,0,0,0,0,0,0) } ,
			{ "     2009","%Y",DateTime(2009,0,0,0,0,0,0) } ,
			{ "     2009","  %Y",DateTime(2009,0,0,0,0,0,0) } ,
			{ "jan","%b",DateTime(0,1,0,0,0,0,0) } ,
			{ "2009feb","%Y%b",DateTime(2009,2,0,0,0,0,0) } ,
			{ "  2009    ApR"," %Y %b",DateTime(2009,4,0,0,0,0,0) } ,
			{ "200910","%Y%m",DateTime(2009,10,0,0,0,0,0) } ,
			{ "  2009    10"," %Y %m",DateTime(2009,10,0,0,0,0,0) } ,
			{ "200910","%Y%c",DateTime(2009,10,0,0,0,0,0) } ,
			{ "  2009    10"," %Y %c",DateTime(2009,10,0,0,0,0,0) } ,
			{ "01,5,2013","%d,%m,%Y",DateTime(2013,5,1,0,0,0,0) },
			{ "01,5,2013","%e,%m,%Y",DateTime(2013,5,1,0,0,0,0) },
			{ "7,5th,2012","%m,%D,%Y",DateTime(2012,7,5,0,0,0,0) },
			{ "a09:30:17","a%h:%i",DateTime(0,0,0,9,30,0,0) } ,
			{ "a09:30:17","%h:%i",DateTime(0,0,0,0,0,0,0) } ,
			{ "9:30.170","%h:%i.%f",DateTime(0,0,0,9,30,0,170000) } ,
			{ "178546 17","%f  %H",DateTime(0,0,0,17,0,0,178546) } ,
			{ "2009march415","%Y%b",DateTime(2009,3,0,0,0,0,0) } ,
			{ "  2009    July"," %Y %b",DateTime(2009,7,0,0,0,0,0) } ,
			{ "09  pm","%h %p",DateTime(0,0,0,21,0,0,0) } ,
			{ "09:13:14pm","%r",DateTime(0,0,0,9,13,14,0) } ,
			{ "12:13:14am","%r",DateTime(0,0,0,12,13,14,0) } ,
			{ "92","    %Y",DateTime(1992,0,0,0,0,0,0) } ,
			{ "9","    %Y",DateTime(2009,0,0,0,0,0,0) } ,
			{ "2013 31","%Y %j",DateTime(2013,1,31,0,0,0,0) } ,
			{ "2013 1 3","%Y %U %w",DateTime(2013,1,9,0,0,0,0) } ,
			{ "2013 1 3","%Y %u %w",DateTime(2013,1,9,0,0,0,0) } ,
			{ "2007 1 3","%Y %U %w",DateTime(2007,1,10,0,0,0,0) } ,
			{ "2007 1 3","%Y %u %w",DateTime(2007,1,3,0,0,0,0) } ,
			{ "2013 1 3","%X %V %w",DateTime(2013,1,9,0,0,0,0) } ,
			{ "2013 1 3","%X %v %w",DateTime(2013,1,9,0,0,0,0) } ,
			{ "2007 1 3","%X %V %w",DateTime(2007,1,10,0,0,0,0) } ,
			{ "2007 1 3","%X %v %w",DateTime(2007,1,3,0,0,0,0) } ,
			{ "33:30.170","%h:%i.%f",DateTime(0,0,0,0,0,0,0) } ,
			{ "01,25,2013","%d,%m,%Y",DateTime(0,0,0,0,0,0,0) },
			{ "%09:30:17","%%%h:%i:%s",DateTime(0,0,0,9,30,17,0) } ,
			{ "2009 foobar","%Y",DateTime(2009,0,0,0,0,0,0) } ,
			{ "2009-12-31","%h:%i:%s %p",DateTime(0,0,0,0,0,0,0) } ,
			{ "12:00:01 AM","%h:%i:%s %p",DateTime(0,0,0,0,0,1,0) } ,
			{ "12:00:01 PM","%h:%i:%s %p",DateTime(0,0,0,12,0,1,0) } ,
			{ "11:00:01 PM","%h:%i:%s %p",DateTime(0,0,0,23,0,1,0) } ,
			{ "11:00:01 AM","%h:%i:%s %p",DateTime(0,0,0,11,0,1,0) } ,
			{ "10000-01-02","%Y:%m:%d",DateTime(0,0,0,0,0,0,0) } ,
		};

		for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
		{
			DateTime dt;
			dt.year = 0;
			dt.month = 0;
			dt.day = 0;
			dt.hour = 0;
			dt.minute = 0;
			dt.second = 0;
			dt.msecond = 0;

			TimeExtractor extractor;
			int ret = extractor.extractTime(date_tests[i].inputstr,date_tests[i].formatstr,dt);
			if( ret != 0 )
				cout << "Extractor reported error for "
					<< date_tests[i].inputstr << "," << date_tests[i].formatstr << endl;
			bool check = ((*(reinterpret_cast<uint64_t *> (&date_tests[i].date))) ==
					((*(reinterpret_cast<uint64_t *> (&dt)))));
			if (!check)
			{
				printf("For input \"%s\", format \"%s\", check 0x%016lx vs 0x%016lx\n",
							date_tests[i].inputstr,
							date_tests[i].formatstr,
							*(reinterpret_cast<uint64_t *> (&dt)),
							*(reinterpret_cast<uint64_t*>(&date_tests[i].date)) );
				show_datetime_debugs("check",dt);
				show_datetime_debugs("ref",date_tests[i].date);
			}
			CPPUNIT_ASSERT( check );
		}

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

		int a1[MAX_VALS] = { 0xdeadbeef };

		for (unsigned i = 0; i < sizeof(tests)/sizeof(Check); i++)
		{
			int rc1 = helpers::getNumbers(tests[i].str,a1,execplan::OP_ADD);

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

	void fe_dateaddtest()
	{
		struct DateCheck {
			const char* intervalstr;
			IntervalColumn::interval_type unit;
			OpType      funcType;
			DateTime    date;
			DateTime    ref;
		};

		DateCheck date_tests[] =
		{
			{ "111111:22",IntervalColumn::INTERVAL_YEAR_MONTH,execplan::OP_ADD,DateTime(1997,01,01,0,0,0,0),DateTime(0,0,0,0,0,0,0) } ,
			{ "-30",IntervalColumn::INTERVAL_DAY,execplan::OP_ADD,DateTime(2013,3,1,0,0,0,0),DateTime(2013,1,30,0,0,0,0) } ,
		};

		for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
		{
			DateTime dt;

			uint64_t timeval = *(reinterpret_cast<uint64_t *> (&date_tests[i].date));
			uint64_t value = helpers::dateAdd( timeval, date_tests[i].intervalstr, date_tests[i].unit, false, date_tests[i].funcType );

			*(reinterpret_cast<uint64_t *> (&dt)) = value;

			bool check = ( value == *(reinterpret_cast<uint64_t *> (&date_tests[i].ref)));
			if (!check)
			{
				printf("For interval \"%s\", check 0x%016lx vs 0x%016lx\n",
							date_tests[i].intervalstr,
							*(reinterpret_cast<uint64_t *> (&dt)),
							*(reinterpret_cast<uint64_t*>(&date_tests[i].ref)) );
				show_datetime_debugs("check",dt);
				show_datetime_debugs("ref",date_tests[i].ref);
			}
			CPPUNIT_ASSERT( check );
		}
	}

	void fe_daynametest()
	{
		struct DateCheck {
			DateTime    date;
			const char* dayname;
		};

		DateCheck date_tests[] =
		{
			{ DateTime(1997,01,01,0,0,0,0),"Wednesday" } ,
		};

		for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
		{
			boost::gregorian::date d(date_tests[i].date.year, date_tests[i].date.month, date_tests[i].date.day);
			uint32_t dayofweek = helpers::calc_mysql_weekday(date_tests[i].date.year, date_tests[i].date.month, date_tests[i].date.day, false);

			bool check = ( strcmp( helpers::weekdayFullNames[dayofweek].c_str(), date_tests[i].dayname ) == 0 );
			if (!check)
			{
				printf("For date %s, check %s vs %s\n",
							to_simple_string(d).c_str(),
							helpers::weekdayFullNames[dayofweek].c_str(),
							date_tests[i].dayname );
			}
			CPPUNIT_ASSERT( check );
		}
	}

	DateTime getDateTime(int64_t val)
	{
		if (val < 0 || val > helpers::TIMESTAMP_MAX_VALUE)
			return 0;

		DateTime dt;

		struct tm tmp_tm;
		time_t tmp_t= (time_t)val;
		localtime_r(&tmp_t, &tmp_tm);

		//to->neg=0;
		dt.year = (int64_t) ((tmp_tm.tm_year+1900) % 10000);
		dt.month = (int64_t) tmp_tm.tm_mon+1;
		dt.day = (int64_t) tmp_tm.tm_mday;
		dt.hour = (int64_t) tmp_tm.tm_hour;
		dt.minute = (int64_t) tmp_tm.tm_min;
		dt.second = (int64_t) tmp_tm.tm_sec;
		dt.msecond = 0;
		return dt;
	}

	void fe_fromunixtimetest()
	{
		struct DateCheck {
			int64_t     ts;
			const char* formatstr;
			const char* refstr;
		};

		DateCheck date_tests[] =
		{
			{ -26, "%H:%i:%s", "NULL" },
			{ 26, "%H:%i:%s", "18:00:26" },
			{ 0, "%H:%i:%s", "18:00:00" },
		};

		for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
		{
			bool check = false;
			string ret;
			DateTime dt = getDateTime(date_tests[i].ts);
			if (*reinterpret_cast<int64_t*>(&dt) == 0)
			{
				check = ( strcmp( date_tests[i].refstr, "NULL" ) == 0 );
			}
			else
			{
				ret = helpers::IDB_date_format(dt, date_tests[i].formatstr);
				check = ( strcmp( date_tests[i].refstr, ret.c_str() ) == 0 );
			}

			if (!check)
			{
				printf("For timestamp %ld, check %s vs %s\n",
							date_tests[i].ts,
							ret.c_str(),
							date_tests[i].refstr );
			}
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



