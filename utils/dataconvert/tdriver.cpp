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
* $Id: tdriver.cpp 2386 2011-02-03 17:56:55Z rdempsey $
*/

#include <iostream>
#include <string>
using namespace std;

#include <boost/any.hpp>
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
	CPPUNIT_ASSERT(smallintval == -9999);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::INT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-2147483646";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	int32_t intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -999999999);

	ct.colWidth = 4;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::MEDINT;
	ct.scale = -1;
	ct.precision = 9;
	data = "-2147483646";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(int32_t));
	intval = any_cast<int32_t>(anyval);
	CPPUNIT_ASSERT(intval == -999999999);

	ct.colWidth = 8;
	ct.constraintType = CalpontSystemCatalog::NO_CONSTRAINT;
	ct.colDataType = CalpontSystemCatalog::BIGINT;
	ct.scale = -1;
	ct.precision = 18;
	data = "-9223372036854775806";

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

	CPPUNIT_ASSERT(anyval.type() == typeid(long long));
	int64_t bigintval = static_cast<int64_t>(any_cast<long long>(anyval));
	CPPUNIT_ASSERT(bigintval == -999999999999999999LL);

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

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

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

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

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

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

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

	anyval = converter.convertColumnData(ct, data, pushWarning, false);

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
	u_int32_t anyval;
	anyval = converter.convertColumnDate( data, CALPONTDATE_ENUM, status);
	CPPUNIT_ASSERT(anyval == 131551998);
	std::string backToString = converter.dateToString(anyval);
	CPPUNIT_ASSERT( backToString == data );
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

