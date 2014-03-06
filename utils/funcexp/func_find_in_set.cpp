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
* $Id: func_find_in_set.cpp 2675 2011-06-22 04:58:07Z chao $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
using namespace boost;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace funcexp
{

CalpontSystemCatalog::ColType Func_find_in_set::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

int64_t Func_find_in_set::getIntVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						CalpontSystemCatalog::ColType& op_ct)
{
	const string& searchStr = parm[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return 0;
	const string& setString = parm[1]->data()->getStrVal(row, isNull);
	if (isNull)
		return 0;
	
	if (searchStr.find(",") != string::npos)
		return 0;
		
	string newSearchStr(searchStr.substr(0, strlen(searchStr.c_str())));
	string newSetString(setString.substr(0, strlen(setString.c_str())));
	//tokenize the setStr with comma as seprator.
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep( ",");
	tokenizer tokens(newSetString, sep);
	
	unsigned i = 0;
	size_t pos = 0;
	for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
	{
		pos = (*tok_iter).find(newSearchStr);
		i++;
		if (( pos != string::npos) && (newSearchStr.length() == (*tok_iter).length()))
			return i;
	}
	
	return 0;
}

double Func_find_in_set::getDoubleVal(rowgroup::Row& row,
						FunctionParm& parm,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& ct)
{
	return (double)getIntVal(row, parm, isNull, ct);
}


string Func_find_in_set::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
	return intToString(getIntVal(row, parm, isNull, ct));
}

execplan::IDB_Decimal Func_find_in_set::getDecimalVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType& op_ct)
{
	IDB_Decimal decimal;
	decimal.value = getIntVal(row, fp, isNull, op_ct);
	decimal.scale = op_ct.scale;
	return decimal;
}

} // namespace funcexp
// vim:ts=4 sw=4:
