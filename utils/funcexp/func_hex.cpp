/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/****************************************************************************
* $Id: func_hex.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
#include <limits>
using namespace std;

#include <boost/scoped_array.hpp>
using namespace boost;

#include "functor_str.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "funchelpers.h"

namespace
{
char digit_upper[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

void octet2hex(char *to, const char *str, uint len)
{
  const char *str_end= str + len;
  for (; str != str_end; ++str)
  {
    *to++= digit_upper[((uint8_t) *str) >> 4];
    *to++= digit_upper[((uint8_t) *str) & 0x0F];
  }
  *to = '\0';
}
}

namespace funcexp
{
CalpontSystemCatalog::ColType Func_hex::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

string Func_hex::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& ct)
{
    string retval;
	uint64_t dec;
	char ans[65];

	switch (parm[0]->data()->resultType().colDataType)
	{
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::DATETIME:
		case CalpontSystemCatalog::DATE:
		{
			const string& arg= parm[0]->data()->getStrVal(row, isNull);
			scoped_array<char> hexPtr(new char[strlen(arg.c_str())*2+1]);
			octet2hex(hexPtr.get(), arg.c_str(), strlen(arg.c_str()));
			return string(hexPtr.get(), strlen(arg.c_str())*2);
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::DECIMAL:
		{
			/* Return hex of unsigned longlong value */
			double val= parm[0]->data()->getDoubleVal(row, isNull);
			if ((val <= (double) numeric_limits<int64_t>::min()) || 
			   (val >= (double) numeric_limits<int64_t>::max()))
				dec=  ~(int64_t) 0;
			else
				dec= (uint64_t) (val + (val > 0 ? 0.5 : -0.5));
			retval = helpers::convNumToStr(dec, ans, 16);
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
		{
			const string& arg = parm[0]->data()->getStrVal(row, isNull);
			uint64_t hexLen = arg.size() * 2;
			scoped_array<char> hexPtr(new char[hexLen + 1]);  // "+ 1" for the last \0
			octet2hex(hexPtr.get(), arg.data(), arg.size());
			return string(hexPtr.get(), hexLen);
		}
		default:
		{
			dec= (uint64_t)parm[0]->data()->getIntVal(row, isNull);
			retval = helpers::convNumToStr(dec, ans, 16);
			if (retval.length() > (uint)ct.colWidth)
				retval = retval.substr(retval.length()-ct.colWidth, ct.colWidth);
		}
	}

	return retval;
}


} // namespace funcexp
// vim:ts=4 sw=4:
