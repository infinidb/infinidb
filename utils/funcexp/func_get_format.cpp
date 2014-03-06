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
* $Id: func_get_format.cpp 2665 2011-06-01 20:42:52Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

class to_upper
{
    public:
        char operator() (char c) const            // notice the return type
        {
            return toupper(c);
        }
};


namespace funcexp
{

string known_date_time_formats[5][4]=
{
  {"USA", "%m.%d.%Y", "%Y-%m-%d %H.%i.%s", "%h:%i:%s %p" },
  {"JIS", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%H:%i:%s" },
  {"ISO", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%H:%i:%s" },
  {"EUR", "%d.%m.%Y", "%Y-%m-%d %H.%i.%s", "%H.%i.%s" },
  {"INTERNAL", "%Y%m%d",   "%Y%m%d%H%i%s", "%H%i%s" }
};

string know_types[3]=
{
	"DATE", "DATETIME", "TIME"
};

CalpontSystemCatalog::ColType Func_get_format::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
	return resultType;
}

string Func_get_format::getStrVal(rowgroup::Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType&)
{
	// parm[0] -- format
	// parm[1] -- type
	string format = parm[0]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	transform (format.begin(), format.end(), format.begin(), to_upper());

	string type = parm[1]->data()->getStrVal(row, isNull);
	if (isNull)
		return "";

	transform (type.begin(), type.end(), type.begin(), to_upper());

	int itype = 0;
	for ( ; itype < 3 ; itype++ )
	{
		if ( know_types[itype] == type )
			break;
	}

	// check for match
	if ( itype == 3 )
		return "";

	for ( int i = 0 ; i < 5 ; i ++ )
	{
		if ( known_date_time_formats[i][0] == format )
		{
			switch (itype) {
				case 0:
					return known_date_time_formats[i][2];
					break;
				default:
					return "";
			}
		}
	}

	return "";
}


} // namespace funcexp
// vim:ts=4 sw=4:
