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

//  $Id: framebound.cpp 3828 2013-05-22 17:58:14Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "idborderby.h"
using namespace ordering;

#include "framebound.h"


namespace windowfunction
{


int64_t FrameBound::getBound(int64_t b, int64_t e, int64_t c)
{
	if (fBoundType == WF__UNBOUNDED_PRECEDING)
		return b;

	return e;
}


const string FrameBound::toString() const
{
	ostringstream oss;
	switch (fBoundType)
	{
		case WF__UNBOUNDED_PRECEDING:
			oss << "unbound preceding";
			break;
		case WF__UNBOUNDED_FOLLOWING:
			oss << "unbound following";
			break;
		case WF__CONSTANT_PRECEDING:
			oss << "constant preceding";
			break;
		case WF__CONSTANT_FOLLOWING:
			oss << "constant following";
			break;
		case WF__EXPRESSION_PRECEDING:
			oss << "expression preceding";
			break;
		case WF__EXPRESSION_FOLLOWING:
			oss << "expression following";
			break;
		case WF__CURRENT_ROW:
		default:
			oss << "current row";
			break;
	}
	oss << endl;

	return oss.str();
}


}   //namespace
// vim:ts=4 sw=4:

