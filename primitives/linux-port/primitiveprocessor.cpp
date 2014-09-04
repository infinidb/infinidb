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

/*****************************************************************************
 * $Id: primitiveprocessor.cpp 1855 2012-04-04 18:20:09Z rdempsey $
 *
 ****************************************************************************/

//#define NDEBUG
#include <cassert>
#ifdef __linux__
#include <sys/types.h>
#endif
using namespace std;

#include "primitiveprocessor.h"

namespace primitives
{

PrimitiveProcessor::PrimitiveProcessor(int debugLevel) :
	fDebugLevel(debugLevel), fStatsPtr(NULL), logicalBlockMode(false)
{

// 	This does
//	masks[11] = { 0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023 };
	int acc, i;

	for (acc = 0, i = 0; i < 11; i++) {
		masks[i] = acc;
		acc = acc << 1 | 1;
	}

}

PrimitiveProcessor::~PrimitiveProcessor()
{
}


void PrimitiveProcessor::setBlockPtr(int *data)
{
	block = data;
}

ParsedColumnFilter::ParsedColumnFilter() : columnFilterMode(STANDARD), likeOps(0)
{
}

ParsedColumnFilter::~ParsedColumnFilter()
{
}

} // namespace primitives

