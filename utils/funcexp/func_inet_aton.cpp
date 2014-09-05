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

/****************************************************************************
* $Id: func_inet_aton.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include "functor_real.h"

#include "calpontsystemcatalog.h"
#include "functioncolumn.h"
#include "joblisttypes.h"
#include "rowgroup.h"
//#include <iostream> // included when debugging

namespace funcexp
{

//------------------------------------------------------------------------------
// Return input argument type.
// See IDB_add in udfsdk.h for explanation of this function.
//------------------------------------------------------------------------------
execplan::CalpontSystemCatalog::ColType Func_inet_aton::operationType(
	FunctionParm& fp,
	execplan::CalpontSystemCatalog::ColType& resultType)
{
	return fp[0]->data()->resultType(); // input type
}

//------------------------------------------------------------------------------
// Return IP address as a long long int value.
// SELECT ... WHERE inet_aton(ipstring) = 11111111 will call getIntVal()
//------------------------------------------------------------------------------
int64_t Func_inet_aton::getIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_aton::getIntVal" << std::endl;

	int64_t iValue = joblist::NULL_INT64;

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iVal = convertAton( sValue, isNull );
		if (!isNull)
			iValue = iVal;
	}
	
	return iValue;
}

//------------------------------------------------------------------------------
// Return IP address as a double value.
// SELECT ... WHERE inet_aton(ipstring) = '11111111' will call getDoubleVal()
//------------------------------------------------------------------------------
double Func_inet_aton::getDoubleVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_aton::getDoubleVal" << std::endl;

	double dValue = doubleNullVal();

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iValue = convertAton( sValue, isNull );
		if (!isNull)
			dValue = iValue;
	}

	return dValue;
}

//------------------------------------------------------------------------------
// Return IP address as a string value.
// We are starting with a string value, and we want to return a string value,
// so we might be tempted to just return the result from getStrVal(), as-is.
// But we still call convertAton() to validate that the IP address we have is
// a syntactically valid one.
// Don't know if this function will ever be called.
//------------------------------------------------------------------------------
std::string Func_inet_aton::getStrVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_aton::getStrVal" << std::endl;

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		convertAton( sValue, isNull ); // ignore return value
	}

	return sValue;
}

//------------------------------------------------------------------------------
// Return IP address as a boolean?
// getBoolVal() makes no sense for inet_aton() but we will implement anyway.
// Don't know if this function will ever be called.
//------------------------------------------------------------------------------
bool Func_inet_aton::getBoolVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
	bool bValue = false;

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iVal = convertAton( sValue, isNull );
		if ((!isNull) && (iVal != 0))
			bValue = true;
	}
	
	return bValue;
}

//------------------------------------------------------------------------------
// Return IP address as a decimal value.
// SELECT ... WHERE inet_aton(ipstring) = 11111111. will call getDecimalVal()
//------------------------------------------------------------------------------
execplan::IDB_Decimal Func_inet_aton::getDecimalVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_aton::getDecimalVal" << std::endl;

	execplan::IDB_Decimal dValue ( joblist::NULL_INT64, 0, 0 );

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iValue = convertAton( sValue, isNull );
		if (!isNull)
			return execplan::IDB_Decimal( iValue, 0, 0 );
	}

	return dValue;
}

//------------------------------------------------------------------------------
// Return IP address as a date?
// getDateIntVal() makes no sense for inet_aton() but we will implement anyway.
// Don't know if this function will ever be called.
//------------------------------------------------------------------------------
int32_t Func_inet_aton::getDateIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
	int32_t iValue = joblist::DATENULL;

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iVal = convertAton( sValue, isNull );
		if (!isNull)
			iValue = iVal;
	}
	
	return iValue;
}

//------------------------------------------------------------------------------
// Return IP address as a date/time?
// getDatetimeIntVal() makes no sense for inet_aton() but we will implement
// anyway.
// Don't know if this function will ever be called.
//------------------------------------------------------------------------------
int64_t Func_inet_aton::getDatetimeIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
	int64_t iValue = joblist::DATETIMENULL;

	std::string sValue = fp[0]->data()->getStrVal(row, isNull);
	if (!isNull)
	{
		int64_t iVal = convertAton( sValue, isNull );
		if (!isNull)
			iValue = iVal;
	}
	
	return iValue;
}

//------------------------------------------------------------------------------
// Convert an ascii IP address string to it's integer equivalent.
// isNull is set to true if the IP address string has invalid content.
// Source code based on MySQL source (Item_func_inet_aton() in item_func.cc).
//------------------------------------------------------------------------------
int64_t Func_inet_aton::convertAton(
	const std::string& ipString,
	bool& isNull )
{
	char c = '.';
	int dot_count = 0;
	unsigned int  byte_result = 0;
	unsigned long long result = 0;

	const char* p   = ipString.c_str();
	const char* end = p + ipString.length();

	// Loop through bytes in the IP address string
	while (p < end)
	{
		c = *p++;

		int digit = (int) (c - '0');  // Assume ascii
		if (digit >= 0 && digit <= 9)
		{
			// Add the next digit from the string to byte_result
			if ((byte_result = byte_result * 10 + digit) > 255)
			{
				// Wrong address
				isNull = true;
				return 0;
			}
		}
		// Detect end of one portion of the IP address.
		// Shift current result over 8 bits, and add next byte (byte_result)
		else if (c == '.')
		{
			dot_count++;
			result= (result << 8) + (unsigned long long) byte_result;
			byte_result = 0;
		}
		// Exit loop if/when we encounter end of string for fixed length column,
		// that is padded with '\0' at the end.
		else if (c == '\0')
		{
			break;
		}
		else
		{
			// Invalid character
			isNull = true;
			return 0;
		}
	}

	if (c != '.')  // IP number can't end on '.'
	{
		//
		// Handle short-forms addresses according to standard. Examples:
		// 127     -> 0.0.0.127
		// 127.1   -> 127.0.0.1
		// 127.2.1 -> 127.2.0.1
		//
		switch (dot_count)
		{
			case 1: result<<= 8; /* Fall through */
			case 2: result<<= 8; /* Fall through */
		}

//		std::cout << "aton: " <<
//			(result << 8) + (unsigned long long) byte_result << std::endl;

		return (result << 8) + (unsigned long long) byte_result;
	}

	// Invalid IP address ended in '.'
	isNull = true;
	return 0;
}

}
