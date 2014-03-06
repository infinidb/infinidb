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

//  $Id: functor_export.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#ifndef FUNCTOR_EXPORT_H
#define FUNCTOR_EXPORT_H


#include "functor.h"


namespace funcexp
{

/** @brief Func_rand class
  *    This function is exported, so visible to FunctionColumn in dbcon.
  */
class Func_rand : public Func
{
public:
	Func_rand() : Func("rand"), fSeed1(0), fSeed2(0), fSeedSet(false){}
	virtual ~Func_rand() {}

	double getRand();
	void seedSet(bool seedSet) { fSeedSet = seedSet; }
	execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp, execplan::CalpontSystemCatalog::ColType& resultType);

	int64_t getIntVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return ((int64_t) getDoubleVal(row, fp, isNull, op_ct)); }

	double getDoubleVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct);

	std::string getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
	{ return doubleToString(getDoubleVal(row, fp, isNull, op_ct)); }

private:
	uint64_t fSeed1;
	uint64_t fSeed2;
	bool     fSeedSet;
};


}

#endif
