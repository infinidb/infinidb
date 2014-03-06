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
* $Id: tdriver.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*/

#include <iostream>
#include <string>
using namespace std;

#include <boost/any.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

class DataConvertTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DataConvertTest );

CPPUNIT_TEST( dc1 );
CPPUNIT_TEST( dc2 );
CPPUNIT_TEST( dc3 );
CPPUNIT_TEST( dc4 );
CPPUNIT_TEST( dc5 );
CPPUNIT_TEST( dc6 );
CPPUNIT_TEST( dc7 );
CPPUNIT_TEST( dc8 );
CPPUNIT_TEST( dc_datetest );
CPPUNIT_TEST( dc_datetostrtest );
CPPUNIT_TEST( dc_datetostr1test );
CPPUNIT_TEST( dc_datetomysqlinttest );
CPPUNIT_TEST( dc_datetest_perf );
CPPUNIT_TEST( dc_datetimetest );
CPPUNIT_TEST( dc_datetimetostrtest );
CPPUNIT_TEST( dc_datetimetostr1test );
CPPUNIT_TEST( dc_datetimetest_perf );
CPPUNIT_TEST( dc_datetimetomysqlinttest );
CPPUNIT_TEST( dc_datevalidtest );
CPPUNIT_TEST( dc9 );
CPPUNIT_TEST( dc10 );
CPPUNIT_TEST_SUITE_END();

private:
	DataConvert converter;
	boost::any anyval;
	CalpontSystemCatalog::ColType ct;
	string data;

public:
	void setUp() {
	}

	void tearDown() {
	}

/* from calpontsystemcatalog.h
    struct ColType
    {
        int colWidth;
        ConstraintType constraintType;
        ColDataType colDataType;
        DictOID ddn;
        boost::any defaultValue;
        int colPosition;    // temporally put here. may need to have ColInfo struct later
        int scale;  //number after decimal points
        int precision; 
    };
        colWidth
        constraintType
        colDataType
        defaultValue
        scale
        precision
*/
 
// mid-range positive integers
void dc1()
{
	ct.colWidth = 1;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::TINYINT;
	ct.scale = -1;
	ct.precision = 2;
	data = "12";
	bool pushWarning;

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(char));
	char tinyintval = any_cast<char>(anyval);
	CPPUNIT_ASSERT(tinyintval == 12);

	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = -1;
	ct.precision = 4;
	data = "1234";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 1234);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = -1;
	ct.precision = 9;
	data = "12345678";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 12345678);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::MEDINT;
	ct.scale = -1;
	ct.precision = 9;
	data = "12345678";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 12345678);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = -1;
	ct.precision = 18;
	data = "1234567890123456";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123456LL);

}

// mid-range negative integers
void dc2()
{
	ct.colWidth = 1;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::TINYINT;
	ct.scale = -1;
	ct.precision = 2;
	data = "-12";
	bool pushWarning;

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(char));
	char tinyintval = any_cast<char>(anyval);
	CPPUNIT_ASSERT(tinyintval == -12);

	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = -1;
	ct.precision = 4;
	data = "-1234";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -1234);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-12345678";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -12345678);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::MEDINT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-12345678";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -12345678);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = -1;
	ct.precision = 18;
	data = "-1234567890123456";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -1234567890123456LL);

}

// limit positive integers
void dc3()
{
	ct.colWidth = 1;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::TINYINT;
	ct.scale = -1;
	ct.precision = 3;
	data = "127";
	bool pushWarning;

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(char));
	char tinyintval = any_cast<char>(anyval);
	CPPUNIT_ASSERT(tinyintval == 127);

	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = -1;
	ct.precision = 5;
	data = "32767";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 32767);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = -1;
	ct.precision = 10;
	data = "2147483647";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 2147483647);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::MEDINT;
	ct.scale = -1;
	ct.precision = 10;
	data = "2147483647";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 2147483647);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = 0;
	ct.precision = 18;
	data = "9223372036854775807";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 9223372036854775807LL);

}

// limit negative integers
void dc4()
{
	ct.colWidth = 1;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::TINYINT;
	ct.scale = -1;
	ct.precision = 3;
	data = "-126";
	bool pushWarning;

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(char));
	char tinyintval = any_cast<char>(anyval);
	CPPUNIT_ASSERT(tinyintval == -126);

	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = -1;
	ct.precision = 4;
	data = "-32766";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -32766);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-2147483646";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -2147483646);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::MEDINT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-2147483646";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -2147483646);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = -1;
	ct.precision = 18;
	data = "-9223372036854775806";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -9223372036854775806LL);

}

// mid-range positive decimals (2 fixed points)
void dc5()
{
	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = 2;
	ct.precision = 4;
	bool pushWarning;

	data = "12.34";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 1234);

	data = "2.345";

	anyval = converter.convertColumnData(ct, data, pushWarning, false, true);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 234);

	data = "(2.34)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 234);

	data = "2.3";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 230);

	data = "2.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 200);

	data = "2";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == 200);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = 6;
	ct.precision = 9;
	data = "0.000481";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 481);
	
	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = 2;
	ct.precision = 9;
	data = "1234567.89";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 123456789);

	data = "34567.8999";

	anyval = converter.convertColumnData(ct, data, pushWarning, false, true);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 3456789);

	data = "(34567.89)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 3456789);

	data = "34567.8";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 3456780);

	data = "34567.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 3456700);

	data = "34567";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == 3456700);
	
	

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = 2;
	ct.precision = 18;

	data = "12345678901234.56";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123456LL);

	data = "45678901234.56093";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 4567890123456LL);

	data = "(45678901234.56)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 4567890123456LL);

	data = "12345678901234.5";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123450LL);

	data = "12345678901234.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123400LL);

	data = "12345678901234";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123400LL);

}

// mid-range positive decimals (fixed-point)
void dc6()
{
	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = 1;
	ct.precision = 18;
	bool pushWarning;

	data = "123456789012345.6";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123456LL);

	ct.scale = 3;

	data = "1234567890123.456";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123456LL);

	ct.scale = 4;

	data = "123456789012.3456";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == 1234567890123456LL);

}

// mid-range negative decimals (2 fixed points)
void dc7()
{
	ct.colWidth = 2;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::SMALLINT;
	ct.scale = 2;
	ct.precision = 4;
	bool pushWarning;

	data = "-12.34";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	int16_t smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -1234);

	data = "-2.345";

	anyval = converter.convertColumnData(ct, data, pushWarning, false, true);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -234);

	data = "(-2.34)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -234);

	data = "-2.3";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -230);

	data = "-2.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int16_t));
	smallintval = any_cast<int16_t>(anyval);
	CPPUNIT_ASSERT(smallintval == -200);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = 2;
	ct.precision = 9;
	data = "-1234567.89";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -123456789);

	data = "-34567.8999";

	anyval = converter.convertColumnData(ct, data, pushWarning, false, true);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -3456789);

	data = "(-34567.89)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -3456789);

	data = "-34567.8";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -3456780);

	data = "-34567.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -3456700);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = 2;
	ct.precision = 18;

	data = "-12345678901234.56";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -1234567890123456LL);

	data = "-45678901234.56093";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -4567890123456LL);

	data = "(-45678901234.56)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -4567890123456LL);

	data = "-12345678901234.5";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -1234567890123450LL);

	data = "-12345678901234.";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -1234567890123400LL);

}

void dc8()
{
	data = "2007-05-11";
	int status = 0;
	uint32_t anyval;
	anyval = converter.convertColumnDate( data.c_str(), CALPONTDATE_ENUM, status, data.length());
	CPPUNIT_ASSERT(anyval == 131551998);
	std::string backToString = converter.dateToString(anyval);
	CPPUNIT_ASSERT( backToString == data );
}

void show_date_debugs( const Date& d1 )
{
	cout << "d1.spare = " << d1.spare << endl;
	cout << "d1.day   = " << d1.day << endl;
	cout << "d1.month = " << d1.month << endl;
	cout << "d1.year  = " << d1.year << endl;
}

#include <stdlib.h>
#include <sys/time.h>

/* Subtract the `struct timeval' values X and Y,
   storing the result in RESULT.
   Return 1 if the difference is negative, otherwise 0.  */

int
timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (x->tv_usec - y->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }

  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}

void dc_datetest_perf()
{
	data = "2007-05-11";

	struct timeval tv1, tv2, delta;
    const int iters = 10000000;

    // test the performance of the default date conversion with full format support
    gettimeofday(&tv1, 0);
	for (int i = 0; i < iters; i++)
	{
		converter.stringToDate(data);
	}
    gettimeofday(&tv2, 0);

    timeval_subtract(&delta, &tv2, &tv1);
    cout << iters << " operations performed in " << delta.tv_sec << "." << delta.tv_usec << " seconds"
    		<< " using the default stringToDate method" << endl;
    unsigned long total_usec = delta.tv_sec * 1000000 + delta.tv_usec;
	CPPUNIT_ASSERT( total_usec < 3000000 );

    // now run the performance of the "fast path" date conversions to check relative performance
	int status = 0;
    gettimeofday(&tv1, 0);
	for (int i = 0; i < iters; i++)
	{
		converter.convertColumnDate( data.c_str(), CALPONTDATE_ENUM, status, data.length());
	}
    gettimeofday(&tv2, 0);

    timeval_subtract(&delta, &tv2, &tv1);
    cout << iters << " operations performed in " << delta.tv_sec << "." << delta.tv_usec << " seconds"
    		<< " using the \"fastpath\" convertColumnDate method" << endl;
}

void dc_datetimetest_perf()
{
	data = "2007-05-11 10:30:45";

	struct timeval tv1, tv2, delta;
    const int iters = 10000000;

    // test the performance of the default datetime conversion with full format support
    gettimeofday(&tv1, 0);
	for (int i = 0; i < iters; i++)
	{
		converter.stringToDatetime(data);
	}
    gettimeofday(&tv2, 0);

    timeval_subtract(&delta, &tv2, &tv1);
    cout << iters << " operations performed in " << delta.tv_sec << "." << delta.tv_usec << " seconds"
    		<< " using the default stringToDatetime method" << endl;
    unsigned long total_usec = delta.tv_sec * 1000000 + delta.tv_usec;
	CPPUNIT_ASSERT( total_usec < 3000000 );

    // now run the performance of the "fast path" datetime conversions to check relative performance
	int status = 0;
    gettimeofday(&tv1, 0);
	for (int i = 0; i < iters; i++)
	{
		converter.convertColumnDatetime( data.c_str(), CALPONTDATETIME_ENUM, status, data.length());
	}
    gettimeofday(&tv2, 0);

    timeval_subtract(&delta, &tv2, &tv1);
    cout << iters << " operations performed in " << delta.tv_sec << "." << delta.tv_usec << " seconds"
    		<< " using the \"fastpath\" convertColumnDatetime method" << endl;
}

void dc_datetest()
{
	struct DateCheck {
		const char* str;
		Date        date;
	};

	DateCheck date_tests[] =
	{
			{ "0000-00-00", Date() },             // bogus date
			{ "2000-01-01", Date(2000, 1, 1) },
			{ "2000-00-01", Date() },             // 0 not a valid monht
			{ "2000-Jan-01", Date() },            // no conversion from month names
		    { "2000-January-01", Date() },        // no conversion from month names
		    { "2000-Jac-01", Date() },            // no conversion from month names
			{ "2000-jan-01", Date() },            // no conversion from month names
			{ "20000101", Date(2000,1,1) },
			{ "2000", Date() },                   // no month or day
			{ "200001", Date() },                 // no day
			{ "000101", Date(2000,1,1) },
			{ "01-01-2000", Date() },             // can not swap field order
			//{ "100-10-1", Date(100,10,1) },     // boost min date is 1400-1-1
			{ "10-100-1", Date() },               // invalid month=100
			//{ "5-02-28", Date(5,2,28) },        // boost min date is 1400-1-1
			{ "2000-02-28", Date(2000,2,28) },
			{ "2000-02-29", Date(2000,2,29) },
			{ "2004-02-29", Date(2004,2,29) },
			{ "2003-12-31", Date(2003,12,31) },
			{ "2003-12-32", Date() },             // no Dec. 32nd
			{ "03-11-30", Date(2003,11,30) },
			{ "07-07-31", Date(2007,7,31) },
			{ "2005-06-23 ", Date(2005,6,23) },
			{ "2005-06-23 tomorrow", Date() },    // no trailing text
			{ "2005-06-23tomorrow ", Date() },    // no trailing text
			{ "2006-011-29 ", Date(2006,11,29) },
			{ "2006-11-029 ", Date(2006,11,29) },
			{ "10000-01-02 ", Date() },
			{ "1999-0102 ", Date() },
	};

	for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
	{
		uint32_t val = converter.stringToDate(date_tests[i].str);
		bool check = (((uint32_t)(val & 0xFFFFFFC0)) == ((*(reinterpret_cast<uint32_t *> (&date_tests[i].date))) & 0xFFFFFFC0));
		if (!check)
		{
			printf("For input \"%s\", check 0x%08x vs 0x%08x\n", date_tests[i].str,(val & 0xFFFFFFC0),(*(reinterpret_cast<uint32_t *> (&date_tests[i].date))) & 0xFFFFFFC0);
		}
		CPPUNIT_ASSERT( check );
	}
}

void dc_datetostrtest()
{
	struct DateCheck {
		const char* str;
		Date        date;
	};

	DateCheck date_tests[] =
	{
			{ "2000-01-01", Date(2000, 1, 1) },
			{ "0100-10-01", Date(100,10,1) },
			{ "0005-02-28", Date(5,2,28) },
			{ "2000-02-28", Date(2000,2,28) },
			{ "2000-02-29", Date(2000,2,29) },
			{ "2004-02-29", Date(2004,2,29) },
			{ "2003-12-31", Date(2003,12,31) },
			{ "2003-11-30", Date(2003,11,30) },
			{ "2007-07-31", Date(2007,7,31) },
			{ "2005-06-23", Date(2005,6,23) },
			{ "2006-11-29", Date(2006,11,29) }
	};

	for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
	{
		string val = converter.dateToString(*(reinterpret_cast<uint32_t*>(&date_tests[i].date)));

		bool check = (val == date_tests[i].str);
		if (!check)
		{
			cout << "\"" << date_tests[i].str << "\" != \"" << val << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void dc_datetostr1test()
{
	struct DateCheck {
		const char* str;
		Date        date;
	};

	DateCheck date_tests[] =
	{
			{ "20000101", Date(2000, 1, 1) },
			{ "01001001", Date(100,10,1) },
			{ "00050228", Date(5,2,28) },
			{ "20000228", Date(2000,2,28) },
			{ "20000229", Date(2000,2,29) },
			{ "20040229", Date(2004,2,29) },
			{ "20031231", Date(2003,12,31) },
			{ "20031130", Date(2003,11,30) },
			{ "20070731", Date(2007,7,31) },
			{ "20050623", Date(2005,6,23) },
			{ "20061129", Date(2006,11,29) }
	};

	for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
	{
		string val = converter.dateToString1(*(reinterpret_cast<uint32_t*>(&date_tests[i].date)));

		bool check = (val == date_tests[i].str);
		if (!check)
		{
			cout << "\"" << date_tests[i].str << "\" != \"" << val << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void dc_datetomysqlinttest()
{
	struct DateCheck {
		int32_t     intval;
		Date        date;
	};

	DateCheck date_tests[] =
	{
			{ 20000101, Date(2000, 1, 1) },
			{ 1001001, Date(100,10,1) },
			{ 50228, Date(5,2,28) },
			{ 20000228, Date(2000,2,28) },
			{ 20000229, Date(2000,2,29) },
			{ 20040229, Date(2004,2,29) },
			{ 20031231, Date(2003,12,31) },
			{ 20031130, Date(2003,11,30) },
			{ 20070731, Date(2007,7,31) },
			{ 20050623, Date(2005,6,23) },
			{ 20061129, Date(2006,11,29) }
	};

	for (unsigned i = 0; i < sizeof(date_tests)/sizeof(DateCheck); i++)
	{
		int64_t val = date_tests[i].date.convertToMySQLint();

		bool check = (val == date_tests[i].intval);
		if (!check)
		{
			cout << val << " != " << date_tests[i].intval << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void show_datetime_debugs( const DateTime& d1 )
{
	cout << "d1.day     = " << setw(10) << d1.day << endl;
	cout << "d1.month   = " << setw(10) << d1.month << endl;
	cout << "d1.year    = " << setw(10) << d1.year << endl;
	cout << "d1.hour    = " << setw(10) << d1.hour << endl;
	cout << "d1.minute  = " << setw(10) << d1.minute << endl;
	cout << "d1.second  = " << setw(10) << d1.second <<  endl;
	cout << "d1.msecond = " << setw(10) << d1.msecond << endl;
}

void dc_datetimetest()
{
	struct DateTimeCheck {
		const char* str;
		DateTime    dtime;
	};

	DateTimeCheck dtime_tests[] =
	{
			{ "2010-03-31 10:30:45.000PM", DateTime() },   // AM/PM not supported
			{ "2010-03-31 10:30:45.000 PM", DateTime() },  // AM/PM not supported
			{ "2010-03-31 10:30:45PM", DateTime() },       // AM/PM not allowed without microsecs
			{ "2010-03-31 10:30:45AM", DateTime() },       // AM/PM not allowed without microsecs
			{ "2010-03-31 10:30:45 PM", DateTime() },      // AM/PM not allowed without microsecs
			{ "2008-01-01 10:00", DateTime(2008,1,1,10,0,0,0) },
			{ "2008-01-01 10:30:02", DateTime(2008,1,1,10,30,2,0) },
			{ "2008-01-01 10:45:01.1111", DateTime(2008,1,1,10,45,1,111100) },
			{ "2008-01-01 10:45:02.222222", DateTime(2008,1,1,10,45,2,222222) },
			{ "2008-01-01 10:45:02.2222223", DateTime() }, // microsecs field too long
			{ "2008-01-01 10", DateTime(2008,1,1,10,0,0,0) },
			{ "200903311030", DateTime() },                // Interprets as YYMMDDHHMMSS so hour(31) is invalid
			{ "20090331103017", DateTime(2009,03,31,10,30,17,0) },
			// { "2009-04-28 11.34.55", DateTime(2009,4,28,11,34,55,0) }, // no more support for arbitrary separators
			{ "20100228T134501", DateTime(2010,02,28,13,45,1,0)  },
			{ "10:45:00 2010-01-31", DateTime() },         // can't switch time/date order
			{ "0000-00-00 00:00:00", DateTime() },          // invalid date/time
			{ "2009-12-31 23:59:56" , DateTime(2009,12,31,23,59,56,0) }          // invalid date/time
	};

	for (unsigned i = 0; i < sizeof(dtime_tests)/sizeof(DateTimeCheck); i++)
	{
		u_int64_t val = converter.stringToDatetime(dtime_tests[i].str);
		// this is a little screwy, but the way things are working is that stringToDateTime
		// returns a -1 when the stringToDatetime operation fails.  The default value for
		// datetime, though is -2, so if we get back the error value then we need to mask
		// off the LSB
		if( val == 0xFFFFFFFFFFFFFFFF )
			val = (val & 0xFFFFFFFFFFFFFFFE);
		bool check = ( val == *(reinterpret_cast<uint64_t*>(&dtime_tests[i].dtime)) );
		if (!check)
		{
			printf("For input \"%s\", check 0x%016lx vs 0x%016lx\n", dtime_tests[i].str,val,*(reinterpret_cast<uint64_t*>(&dtime_tests[i].dtime)));
		}

		if (!check)
			show_datetime_debugs( DateTime(val) );
		CPPUNIT_ASSERT( check );
	}
}

void dc_datetimetostrtest()
{
	struct DateTimeCheck {
		const char* str;
		DateTime    dtime;
	};

	DateTimeCheck dtime_tests[] =
	{
			{ "2008-01-01 10:00:00", DateTime(2008,1,1,10,0,0,0) },
			{ "2008-01-01 10:30:02", DateTime(2008,1,1,10,30,2,0) },
			{ "2008-01-01 10:45:01", DateTime(2008,1,1,10,45,1,111100) },
			{ "2008-01-01 10:45:02", DateTime(2008,1,1,10,45,2,222222) },
			{ "2008-01-01 10:00:00", DateTime(2008,1,1,10,0,0,0) },
			{ "2009-03-31 10:30:17", DateTime(2009,03,31,10,30,17,0) },
			{ "2009-04-28 11:34:55", DateTime(2009,4,28,11,34,55,0) },
			{ "2010-02-28 13:45:01", DateTime(2010,02,28,13,45,1,0)  },
			{ "2009-12-31 23:59:56" , DateTime(2009,12,31,23,59,56,0) }          // invalid date/time
	};

	for (unsigned i = 0; i < sizeof(dtime_tests)/sizeof(DateTimeCheck); i++)
	{
		string val = converter.datetimeToString(*(reinterpret_cast<uint64_t*>(&dtime_tests[i].dtime)));

		bool check = (val == dtime_tests[i].str);
		if (!check)
		{
			cout << "\"" << dtime_tests[i].str << "\" != \"" << val << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void dc_datetimetostr1test()
{
	struct DateTimeCheck {
		const char* str;
		DateTime    dtime;
	};

	DateTimeCheck dtime_tests[] =
	{
			{ "20080101100000000000", DateTime(2008,1,1,10,0,0,0) },
			{ "20080101103002000000", DateTime(2008,1,1,10,30,2,0) },
			{ "20080101104501111100", DateTime(2008,1,1,10,45,1,111100) },
			{ "20080101104502222222", DateTime(2008,1,1,10,45,2,222222) },
			{ "20080101100000000000", DateTime(2008,1,1,10,0,0,0) },
			{ "20090331103017000000", DateTime(2009,03,31,10,30,17,0) },
			{ "20090428113455000000", DateTime(2009,4,28,11,34,55,0) },
			{ "20100228134501000000", DateTime(2010,02,28,13,45,1,0)  },
			{ "20091231235956000000", DateTime(2009,12,31,23,59,56,0) }
	};

	for (unsigned i = 0; i < sizeof(dtime_tests)/sizeof(DateTimeCheck); i++)
	{
		string val = converter.datetimeToString1(*(reinterpret_cast<uint64_t*>(&dtime_tests[i].dtime)));

		bool check = (val == dtime_tests[i].str);
		if (!check)
		{
			cout << "\"" << dtime_tests[i].str << "\" != \"" << val << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void dc_datetimetomysqlinttest()
{
	struct DateTimeCheck {
		int64_t     intval;
		DateTime    dtime;
	};

	DateTimeCheck dtime_tests[] =
	{
			{ 20080101100000, DateTime(2008,1,1,10,0,0,0) },
			{ 20080101103002, DateTime(2008,1,1,10,30,2,0) },
			{ 20080101104501, DateTime(2008,1,1,10,45,1,111100) },
			{ 20080101104502, DateTime(2008,1,1,10,45,2,222222) },
			{ 20080101100000, DateTime(2008,1,1,10,0,0,0) },
			{ 20090331103017, DateTime(2009,03,31,10,30,17,0) },
			{ 20090428113455, DateTime(2009,4,28,11,34,55,0) },
			{ 20100228134501, DateTime(2010,02,28,13,45,1,0)  },
			{ 20091231235956, DateTime(2009,12,31,23,59,56,0) }
	};

	for (unsigned i = 0; i < sizeof(dtime_tests)/sizeof(DateTimeCheck); i++)
	{
		int64_t val = dtime_tests[i].dtime.convertToMySQLint();

		bool check = (val == dtime_tests[i].intval);
		if (!check)
		{
			cout << val << " != " << dtime_tests[i].intval << endl;
		}

		CPPUNIT_ASSERT( check );
	}
}

void check_date( int year, int month, int day, bool valid, bool boost_only = false )
{
	bool idbval = isDateValid( day, month, year );

	bool boostval;
	try
	{
		boost::gregorian::date d( year, month, day );
		boostval = true;
	}
	catch (...)
	{
		boostval = false;
	}

	bool check = boost_only ? (idbval == boostval) :
					(idbval == valid && ((boostval == idbval) || boostval));
	if (!check)
	{
		cout << "(" << year << "," << month << "," << day << "), expected:"
				<< valid << ", idb=" << idbval << ", boost=" << boostval << endl;
	}

	CPPUNIT_ASSERT( check );
}

void dc_datevalidtest()
{
	struct Check {
		int         year;
		int         month;
		int         day;
		bool        valid;
	};

	Check tests[] =
	{
		{ 2000, 2, 29, true },
		{ 2001, 2, 29, false },
		{ 1900, 2, 29, false },
		{ 1900, 2, 28, true },
		{ 1399, 12, 31, false },
		{ 10000, 1, 1, false },
		{ 2000, 0, 29, false },
		{ 2000, 13, 29, false },
		{ 2000, 1, 0, false },
		{ 2000, 1, 32, false },
	};

	// predefined tests shown above
	for (unsigned i = 0; i < sizeof(tests)/sizeof(Check); i++)
	{
		check_date( tests[i].year, tests[i].month, tests[i].day, tests[i].valid );
	}
	// randomly generated date tests
	for (unsigned i = 0; i < 1000000; i++)
	{
		int year = random() % 9000 + 1200; // some spillage on either side
		if( year == 10000 )
			--year; // year 10000 is not good because boost calls it good but we don't want to
		int month = random() % 14; // some spillage on either side
		int day = random() % 33; // some spillage on either side
		check_date( year, month, day, true, true );
	}
}

// Testing equality of floating point numbers is not good practice, rather make sure
//  the conversion is within a tolerance (usually +/- 1 LSD).
inline bool inTolerance(double d, double a, double t)
{
	return ((d > (a - t)) && (d < (a + t)));
}

// Float tests 6 digits of accuracy
void dc9()
{
	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::FLOAT;
	ct.scale = 0;
	ct.precision = 4;
	bool pushWarning;

	data = "0.123456";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(float));
	float floatval = any_cast<float>(anyval);
	CPPUNIT_ASSERT(inTolerance(floatval, 0.123456, 0.000001));

	data = "3456.01";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(float));
	floatval = any_cast<float>(anyval);
	CPPUNIT_ASSERT(inTolerance(floatval, 3456.01, 0.01));

	data = "(3456.01)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(float));
	floatval = any_cast<float>(anyval);
	CPPUNIT_ASSERT(inTolerance(floatval, 3456.01, 0.01));

	data = "6.02214E+23";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(float));
	floatval = any_cast<float>(anyval);
	CPPUNIT_ASSERT(inTolerance(floatval, 6.02214E+23, 0.00001E+23));

	data = "1.60217E-19";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(float));
	floatval = any_cast<float>(anyval);
	CPPUNIT_ASSERT(inTolerance(floatval, 1.60217E-19, 0.00001E-19));

}

// Double tests 15 digits of accuracy
void dc10()
{
	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::DOUBLE;
	ct.scale = 0;
	ct.precision = 4;
	bool pushWarning;

	data = "0.123456789012345";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	double doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 0.123456789012345, 0.000000000000001));

	data = "123456.000000001";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 123456.000000001, 0.000000001));

	data = "(123456.000000001)";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 123456.000000001, 0.000000001));

	data = "6.02214179000000E+23";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 6.02214179000000E+23, 0.00000000000001E+23));

	data = "1.60217653140000E-19";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 1.60217653140000E-19, 0.00000000000001E-19));

	data = "3.14159265358979";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(double));
	doubleval = any_cast<double>(anyval);
	CPPUNIT_ASSERT(inTolerance(doubleval, 3.14159265358979, 0.00000000000001));

}

}; 


CPPUNIT_TEST_SUITE_REGISTRATION( DataConvertTest );

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

