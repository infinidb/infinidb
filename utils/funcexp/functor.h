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

//  $Id: functor.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#ifndef FUNCTOR_H
#define FUNCTOR_H

#include <cstdlib>
#include <string>
#include <sstream>
#include <string>

#include "parsetree.h"
#include "exceptclasses.h"
#include "errorids.h"
#include "idberrorinfo.h"

#include "calpontsystemcatalog.h"

namespace rowgroup
{
	class Row;
}

namespace execplan
{
extern const std::string colDataTypeToString(CalpontSystemCatalog::ColDataType cdt);
}

namespace funcexp
{
//typedef std::vector<execplan::STNP> FunctionParm;
typedef std::vector<execplan::SPTP> FunctionParm;


/** @brief Func class
  */
class Func
{
public:
	Func();
	Func(const std::string& funcName);
	virtual ~Func() {}

	const std::string funcName() const { return fFuncName; }
	void funcName(const std::string funcName) { fFuncName = funcName; }
	virtual execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType) = 0;

	virtual int64_t getIntVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

    virtual uint64_t getUintVal(rowgroup::Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return static_cast<uint64_t>(getIntVal(row,fp,isNull,op_ct)); }

    virtual double getDoubleVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

	virtual std::string getStrVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct) = 0;

	virtual execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return execplan::IDB_Decimal(getIntVal(row, fp, isNull, op_ct),0,0); }

	virtual int32_t getDateIntVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return intToDate(getIntVal(row, fp, isNull, op_ct)); }

	virtual int64_t getDatetimeIntVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return intToDatetime(getIntVal(row, fp, isNull, op_ct)); }

	virtual bool getBoolVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
	{ int64_t intVal = getIntVal(row, fp, isNull, op_ct); return (!isNull && intVal); }

	virtual float getFloatVal(rowgroup::Row& row,
								FunctionParm& fp,
								bool& isNull,
								execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return getDoubleVal(row, fp, isNull, op_ct); }

	const float floatNullVal() const { return fFloatNullVal; }
	const double doubleNullVal() const { return fDoubleNullVal; }

protected:
	virtual uint32_t stringToDate(std::string);
	virtual uint64_t stringToDatetime(std::string);

	virtual uint32_t intToDate(int64_t);
	virtual uint64_t intToDatetime(int64_t);

	virtual std::string intToString(int64_t);
	virtual std::string doubleToString(double);

	std::string fFuncName;

private:
	//defaults okay
	//Func(const Func& rhs);
	//Func& operator=(const Func& rhs);

	void init();

	float fFloatNullVal;
	double fDoubleNullVal;
};


}

#endif



