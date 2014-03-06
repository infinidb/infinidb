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
* $Id: func_inet_ntoa.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#ifndef _MSC_VER
#include <arpa/inet.h>
#include <netinet/in.h>
#endif
#include <iostream> // included when debugging
#include <sstream>
#include <climits>

#include "functor_str.h"

#include "calpontsystemcatalog.h"
#include "functioncolumn.h"
#include "joblisttypes.h"
#include "rowgroup.h"

namespace funcexp
{

//------------------------------------------------------------------------------
// The only accessor function that appears to make sense in conjunction with
// inet_ntoa() is getStrVal().  I tested in MySQL (outside infinidb), and
// comparing inet_ntoa(col) to anything other than a string "seemed" to return
// an empty set.  So I implemented the other get functions to return a null.
//
// Later, Daniel found that if you compare inet_ntoa(col) to an integer (ex: 1)
// or a double (ex: 1.1), that the getDoubleVal() version of inet_ntoa(col),
// will get called.  As stated in previous paragraph, I thought I tested this,
// but I apparently overlooked something.  So getIntVal() and getDoubleVal()
// are now implemented.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Return input argument type.
// See IDB_add in udfsdk.h for explanation of this function.
//------------------------------------------------------------------------------
execplan::CalpontSystemCatalog::ColType Func_inet_ntoa::operationType(
	FunctionParm& fp,
	execplan::CalpontSystemCatalog::ColType& resultType)
{
	return fp[0]->data()->resultType(); // input type
}

//------------------------------------------------------------------------------
// Return IP address as a long long int value.
// Not sure this is ever called, but emulated getDoubleVal() implementation,
// to be safe.  (See getDoubleVal() description)
//------------------------------------------------------------------------------
int64_t Func_inet_ntoa::getIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getIntVal" << std::endl;

	std::string sValue = getStrVal( row, fp, isNull, op_ct );
	int64_t iValue = joblist::NULL_INT64;
	if ( !isNull )
	{
		unsigned int newLength      = sValue.length();
		std::string::size_type dot1 = sValue.find('.');
		if (dot1 != std::string::npos)
		{
			newLength = dot1;
		}

		if (newLength != sValue.length())
			sValue.resize(newLength);
		std::istringstream iss( sValue );
		iss >> iValue;
	}

	return iValue;
}

//------------------------------------------------------------------------------
// Return IP address as a double value.
// SELECT ... WHERE inet_ntoa(ipstring) = 1.1 will call getDoubleVal()
// SELECT ... WHERE inet_ntoa(ipstring) = 1   will also call getDoubleVal()
//------------------------------------------------------------------------------
double Func_inet_ntoa::getDoubleVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getDoubleVal" << std::endl;

	std::string sValue = getStrVal( row, fp, isNull, op_ct );
	double dValue = doubleNullVal();
	if ( !isNull )
	{
		unsigned int newLength      = sValue.length();
		std::string::size_type dot1 = sValue.find('.');
		if ((dot1 != std::string::npos) && (sValue.length() > dot1+1))
		{
			std::string::size_type dot2 = sValue.find('.', dot1+1);
			if (dot2 != std::string::npos)
			{
				newLength = dot2;
			}
		}

		if (newLength != sValue.length())
			sValue.resize(newLength);
		std::istringstream iss( sValue );
		iss >> dValue;
	}

	return dValue;
}

//------------------------------------------------------------------------------
// Return IP address as a string value.
// This is the get function that makes sense to use.
//------------------------------------------------------------------------------
std::string Func_inet_ntoa::getStrVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getStrVal" << std::endl;

	std::string sValue;

	int64_t iValue = 0;

	// @bug 3628 reopened: get double and round up, if necessary;
	//      else just get integer value
	if ((fp[0]->data()->resultType().colDataType ==
		execplan::CalpontSystemCatalog::DECIMAL) ||
		(fp[0]->data()->resultType().colDataType ==
		execplan::CalpontSystemCatalog::FLOAT)   ||
		(fp[0]->data()->resultType().colDataType ==
		execplan::CalpontSystemCatalog::DOUBLE))
	{
		double d = fp[0]->data()->getDoubleVal(row, isNull);
		if (d >= 0.0)
			iValue = (int64_t)(d + 0.5);
		else
			iValue = (int64_t)(d - 0.5);
	}
	else
	{
		iValue = fp[0]->data()->getIntVal(row, isNull);
	}

	if (!isNull)
	{
		// @bug 3628 reopened: add check for out of range values
		if ((iValue < 0) || (iValue > UINT_MAX))
			isNull = true;
		else
			convertNtoa( iValue, sValue );
	}

	return sValue;
}

//------------------------------------------------------------------------------
// Return IP address as a boolean.
// N/A so returning null.  See explanation at the top of this source file.
//------------------------------------------------------------------------------
bool Func_inet_ntoa::getBoolVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getBoolVal" << std::endl;
	bool bValue = false;
	isNull = true;

	return bValue;
}

//------------------------------------------------------------------------------
// Return IP address as a decimal value.
// N/A so returning null.  See explanation at the top of this source file.
//------------------------------------------------------------------------------
execplan::IDB_Decimal Func_inet_ntoa::getDecimalVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getDecimalVal" << std::endl;

//	IDB_Decimal dValue = fp[0]->data()->getDecimalVal(row, isNull);
	execplan::IDB_Decimal dValue ( joblist::NULL_INT64, 0, 0 );
	isNull = true;

	return dValue;
}

//------------------------------------------------------------------------------
// Return IP address as a date.
// N/A so returning null.  See explanation at the top of this source file.
//------------------------------------------------------------------------------
int32_t Func_inet_ntoa::getDateIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getDateIntVal" << std::endl;

//	int32_t iValue = fp[0]->data()->getDateIntVal(row, isNull);
	int32_t iValue = joblist::DATENULL;
	isNull = true;

	return iValue;
}

//------------------------------------------------------------------------------
// Return IP address as a date/time.
// N/A so returning null.  See explanation at the top of this source file.
//------------------------------------------------------------------------------
int64_t Func_inet_ntoa::getDatetimeIntVal(rowgroup::Row& row,
	FunctionParm& fp,
	bool& isNull,
	execplan::CalpontSystemCatalog::ColType& op_ct)
{
//	std::cout << "In Func_inet_ntoa::getDatetimeVal" << std::endl;

//	int64t iValue = fp[0]->data()->getDatetimeIntVal(row, isNull);
	int64_t iValue = joblist::DATETIMENULL;
	isNull = true;

	return iValue;
}

//------------------------------------------------------------------------------
// Convert an integer IP address to its equivalent IP address string.
// Source code based on MySQL source (Item_func_inet_ntoa() in item_strfunc.cc).
//------------------------------------------------------------------------------
void Func_inet_ntoa::convertNtoa(
	int64_t      ipNum,
	std::string& ipString )
{
    struct sockaddr_in sa;
    sa.sin_addr.s_addr = htonl(ipNum);

    // now get it back and print it
#ifdef _MSC_VER
    ipString = inet_ntoa(sa.sin_addr);
#else
    char str[INET_ADDRSTRLEN];
    ipString = inet_ntop(AF_INET, &(sa.sin_addr), str, INET_ADDRSTRLEN);
#endif
}

}
