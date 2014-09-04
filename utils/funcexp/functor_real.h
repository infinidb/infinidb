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

//  $Id: functor_real.h 3048 2012-04-04 15:33:45Z rdempsey $

/** @file */

#ifndef FUNCTOR_REAL_H
#define FUNCTOR_REAL_H


#include "functor.h"


namespace funcexp
{

/** @brief Func_Real class
  *    For function that returns a int/decimal/double result.
  *        Must implement getDoubleVal()
  *        Implement any other methods that behave differently from the default.
  *        Note: getIntVal is a good candidate to be implemented, too.
  */
class Func_Real : public Func
{
public:
	Func_Real() {}
	Func_Real(const std::string& funcName) : Func(funcName) {}
	virtual ~Func_Real() {}

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return ((int64_t) getDoubleVal(row, fp, isNull, op_ct)); }

/*
	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct) = 0;
*/

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return doubleToString(getDoubleVal(row, fp, isNull, op_ct)); }
};


/** @brief Func_abs class
  */
class Func_abs : public Func_Real
{
public:
	Func_abs(): Func_Real("abs") {}
	virtual ~Func_abs() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_exp class
  */
class Func_exp : public Func_Real
{
public:
	Func_exp() : Func_Real("exp") {}
	virtual ~Func_exp() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_pow class
  */
class Func_pow : public Func_Real
{
public:
	Func_pow() : Func_Real("pow") {}
	virtual ~Func_pow() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_round class
  */
class Func_round : public Func_Real
{
public:
	Func_round() : Func_Real("round") {}
	virtual ~Func_round() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_truncate class
  */
class Func_truncate : public Func_Real
{
public:
	Func_truncate() : Func_Real("truncate") {}
	virtual ~Func_truncate() {}

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
};


/** @brief Func_ceil class
  */
class Func_ceil : public Func_Real
{
public:
	Func_ceil() : Func_Real("ceil") {}
	virtual ~Func_ceil() {}

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
};


/** @brief Func_floor class
  */
class Func_floor : public Func_Real
{
public:
	Func_floor() : Func_Real("floor") {}
	virtual ~Func_floor() {}

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
};


/** @brief Func_cast_decimal class
  */
class Func_cast_decimal : public Func_Real
{
public:
	Func_cast_decimal() : Func_Real("cast_decimal") {}
	virtual ~Func_cast_decimal() {}

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
};


/** @brief Func_mod class
  */
class Func_mod : public Func_Real
{
public:
	Func_mod() : Func_Real("mod") {}
	virtual ~Func_mod() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_acos class
  */
class Func_acos : public Func_Real
{
public:
	Func_acos() : Func_Real("acos") {}
	virtual ~Func_acos() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_asin class
  */
class Func_asin : public Func_Real
{
public:
	Func_asin() : Func_Real("asin") {}
	virtual ~Func_asin() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_atan class
  */
class Func_atan : public Func_Real
{
public:
	Func_atan() : Func_Real("atan") {}
	virtual ~Func_atan() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_cos class
  */
class Func_cos : public Func_Real
{
public:
	Func_cos() : Func_Real("cos") {}
	virtual ~Func_cos() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_cot class
  */
class Func_cot : public Func_Real
{
public:
	Func_cot() : Func_Real("cot") {}
	virtual ~Func_cot() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_log class
  */
class Func_log : public Func_Real
{
public:
	Func_log() : Func_Real("log") {}
	virtual ~Func_log() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_log2 class
  */
class Func_log2 : public Func_Real
{
public:
	Func_log2() : Func_Real("log2") {}
	virtual ~Func_log2() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_log10 class
  */
class Func_log10 : public Func_Real
{
public:
	Func_log10() : Func_Real("log10") {}
	virtual ~Func_log10() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_sin class
  */
class Func_sin : public Func_Real
{
public:
	Func_sin() : Func_Real("sin") {}
	virtual ~Func_sin() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_sqrt class
  */
class Func_sqrt : public Func_Real
{
public:
	Func_sqrt() : Func_Real("sqrt"){}
	virtual ~Func_sqrt() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_tan class
  */
class Func_tan : public Func_Real
{
public:
	Func_tan() : Func_Real("tan") {}
	virtual ~Func_tan() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_radians class
  */
class Func_radians : public Func_Real
{
public:
	Func_radians() : Func_Real("radians") {}
	virtual ~Func_radians() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};


/** @brief Func_div class
  */
class Func_div : public Func_Real
{
public:
	Func_div() : Func_Real("DIV") {}
	virtual ~Func_div() {}

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
};


/** @brief Func_inet_aton class to convert ascii IP address to big-endian
  * (network ordered) int
  */
class Func_inet_aton : public Func_Real
{
public:
	Func_inet_aton() : Func_Real("inet_aton") {}
	virtual ~Func_inet_aton() {}

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

	bool getBoolVal(rowgroup::Row& row,
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

private:
	int64_t convertAton(const std::string& ipString, bool& isNull);
};

/** @brief Func_degrees class
  */
class Func_degrees : public Func_Real
{
public:
	Func_degrees() : Func_Real("degrees") {}
	virtual ~Func_degrees() {}

	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);
};



}

#endif
