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

//  $Id: windowframe.cpp 3821 2013-05-17 23:58:16Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "windowframe.h"


namespace
{
string UnitStr[] = {"ROWS", "RANGE"};
}


namespace windowfunction
{


pair<uint64_t, uint64_t> WindowFrame::getWindow(int64_t b, int64_t e, int64_t c)
{
	int64_t upper = fUpper->getBound(b, e, c);
	int64_t lower = fLower->getBound(b, e, c);

	//     case 1       ||         case 2           ||        case 3
	if ((upper > lower) || (upper < b && lower < b) || (upper > e && lower > e))
	{
		// construct an empty window
		upper = b+1;
		lower = b;
	}

	if (upper < b)  // case 2, lower >= b
	{
		upper = b;
	}

	if (lower > e)  // case 3, upper <= e
	{
		lower = e;
	}

	return make_pair(upper, lower);
}


const string WindowFrame::toString() const
{
	string ret(UnitStr[fUnit]);
	ret = ret + " between " + fUpper->toString() + " and " + fLower->toString();

	return ret;
}

}   //namespace
// vim:ts=4 sw=4:

