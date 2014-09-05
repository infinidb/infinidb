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

//  $Id: functor_dtm.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#ifndef FUNCTOR_DTM_H
#define FUNCTOR_DTM_H

#include "functor.h"


namespace funcexp
{

/** @brief Func_Dtm class
  *    For function that returns a date or datetime result.
  *        Must implement getIntVal() and getStrVal()
  *        Implement any other methods that behave differently from the default.
  *        Note: need to pay attention to the difference between
  *              getIntVal and getDateIntVal/getDatetimeIntVal.
  */
class Func_Dtm : public Func
{
public:
	Func_Dtm() {}
	Func_Dtm(const std::string& funcName) : Func(funcName) {}
	virtual ~Func_Dtm() {}

/*
	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
*/

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return (double) getIntVal(row, fp, isNull, op_ct); }

/*
	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
*/
};


/** @brief Func_date class
  */
class Func_date : public Func_Dtm
{
public:
	Func_date() : Func_Dtm("date") {}
	virtual ~Func_date() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_time class
  */
class Func_time : public Func_Dtm
{
public:
	Func_time() : Func_Dtm("time") {}
	virtual ~Func_time() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);


};


/** @brief Func_timediff class
  */
class Func_timediff : public Func_Dtm
{
public:
	Func_timediff() : Func_Dtm("timediff") {}
	virtual ~Func_timediff() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_date_add class
  */
class Func_date_add : public Func_Dtm
{
public:
	Func_date_add() : Func_Dtm("date_add") {}
	virtual ~Func_date_add() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_cast_date class
  */
class Func_cast_date : public Func_Dtm
{
public:
	Func_cast_date() : Func_Dtm("cast_date") {}
	virtual ~Func_cast_date() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_cast_datetime class
  */
class Func_cast_datetime : public Func_Dtm
{
public:
	Func_cast_datetime() : Func_Dtm("cast_datetime") {}
	virtual ~Func_cast_datetime() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_from_days class
  */
class Func_from_days : public Func_Dtm
{
public:
	Func_from_days() : Func_Dtm("from_days") {}
	virtual ~Func_from_days() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_add_time class
  */
class Func_add_time : public Func_Dtm
{
public:
	Func_add_time() : Func_Dtm("add_time"){}
	virtual ~Func_add_time() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_timestampdiff class
  */
class Func_timestampdiff : public Func_Dtm
{
public:
	Func_timestampdiff() : Func_Dtm("timestamp_diff"){}
	virtual ~Func_timestampdiff() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_sysdate class
  */
class Func_sysdate : public Func_Dtm
{
public:
	Func_sysdate() : Func_Dtm("sysdate") {}
	virtual ~Func_sysdate() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_from_unixtime
  */
class Func_from_unixtime : public Func_Dtm
{
public:
	Func_from_unixtime() : Func_Dtm("from_unixtime") {}
	virtual ~Func_from_unixtime() {}
	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
							
	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_str_to_date class
  */
class Func_str_to_date : public Func_Dtm
{
public:
	Func_str_to_date() : Func_Dtm("str_to_date") {}
	virtual ~Func_str_to_date() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int32_t getDateIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	int64_t getDatetimeIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_makedate class
  */
class Func_makedate : public Func_Dtm
{
public:
	Func_makedate() : Func_Dtm("makedate") {}
	virtual ~Func_makedate() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};



}

#endif
