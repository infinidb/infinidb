/* Copyright (C) 2013 Calpont Corp.

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
* $Id: func_insert.cpp 2477 2011-04-01 16:07:35Z rdempsey $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "utf8.h"
using namespace utf8;

#define STRCOLL_ENH__

namespace funcexp
{

CalpontSystemCatalog::ColType Func_insert::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
	// operation type is not used by this functor
	return fp[0]->data()->resultType();
}

string insertStr(const string& src, int pos, int len, const string& targ)
{
	int64_t strLen = static_cast<int64_t>(src.length());

    if ((pos <= 0) || ((pos-1) > strLen))
        return src;

    if ((len < 0) || (len > strLen))
        len = strLen;

    const char* srcptr = src.c_str();
    advance(srcptr,pos-1,srcptr+strLen);
    // srcptr now pointing to where we need to insert targ string

    uint srcPos = srcptr - src.c_str();

    uint finPos = strLen;
	const char* finptr = src.c_str();
    if ((strLen - (pos-1+len)) >= 0)
    {
	    advance(finptr,(pos-1+len),finptr+strLen);
    	// finptr now pointing to the end of the string to replace
		finPos = finptr - src.c_str();
    }

    string out;
    out.reserve(srcPos + targ.length() + strLen-finPos + 1);
    out.append( src.c_str(), srcPos );
    out.append( targ.c_str(), targ.length() );
    out.append( src.c_str() + finPos, strLen-finPos );

    return out;
}

std::string Func_insert::getStrVal(rowgroup::Row& row,
						FunctionParm& fp,
						bool& isNull,
						execplan::CalpontSystemCatalog::ColType&)
{	
	const string& tstr = stringValue(fp[0], row, isNull);
	if (isNull)
		return "";

	const string& tnewstr = stringValue(fp[3], row, isNull);
	if (isNull)
		return "";

	int64_t pos = fp[1]->data()->getIntVal(row, isNull);
	if (isNull)
		return "";

	int64_t len = fp[2]->data()->getIntVal(row, isNull);
	if (isNull)
		return "";

    return insertStr( tstr, pos, len, tnewstr );	
}


} // namespace funcexp
// vim:ts=4 sw=4:

