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

//
// $Id: funcexpwrapper.cpp 3495 2013-01-21 14:09:51Z rdempsey $
//
// C++ Implementation: funcexpwrapper
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "funcexpwrapper.h"
#include "objectreader.h"

using namespace messageqcpp;
using namespace boost;
using namespace rowgroup;
using namespace execplan;

namespace funcexp {

FuncExpWrapper::FuncExpWrapper()
{
	fe = FuncExp::instance();
}

FuncExpWrapper::FuncExpWrapper(const FuncExpWrapper &f)
{
	uint32_t i;

	fe = FuncExp::instance();

	filters.resize(f.filters.size());
	for (i = 0; i < f.filters.size(); i++)
		filters[i].reset(new ParseTree(*(f.filters[i])));

	rcs.resize(f.rcs.size());
	for (i = 0; i < f.rcs.size(); i++)
		rcs[i].reset(f.rcs[i]->clone());
}

FuncExpWrapper::~FuncExpWrapper()
{ }

void FuncExpWrapper::operator=(const FuncExpWrapper &f)
{
	uint32_t i;

	filters.resize(f.filters.size());
	for (i = 0; i < f.filters.size(); i++)
		filters[i].reset(new ParseTree(*(f.filters[i])));

	rcs.resize(f.rcs.size());
	for (i = 0; i < f.rcs.size(); i++)
		rcs[i].reset(f.rcs[i]->clone());

}

void FuncExpWrapper::serialize(ByteStream &bs) const
{
	uint32_t i;

	bs << (uint32_t) filters.size();
	bs << (uint32_t) rcs.size();
	for (i = 0; i < filters.size(); i++)
		ObjectReader::writeParseTree(filters[i].get(), bs);
	for (i = 0; i < rcs.size(); i++)
		rcs[i]->serialize(bs);
}

void FuncExpWrapper::deserialize(ByteStream &bs)
{
	uint32_t fCount, rcsCount, i;

	bs >> fCount;
	bs >> rcsCount;
	for (i = 0; i < fCount; i++)
		filters.push_back(shared_ptr<ParseTree>(ObjectReader::createParseTree(bs)));
	for (i = 0; i < rcsCount; i++) {
		ReturnedColumn *rc = (ReturnedColumn *) ObjectReader::createTreeNode(bs);
		rcs.push_back(shared_ptr<ReturnedColumn>(rc));
	}
}

bool FuncExpWrapper::evaluate(Row *r)
{
	uint32_t i;

	for (i = 0; i < filters.size(); i++)
		if (!fe->evaluate(*r, filters[i].get()))
			return false;

	fe->evaluate(*r, rcs);

	return true;
}

void FuncExpWrapper::addFilter(const shared_ptr<ParseTree>& f)
{
	filters.push_back(f);
}

void FuncExpWrapper::addReturnedColumn(const shared_ptr<ReturnedColumn>& rc)
{
	rcs.push_back(rc);
}

};
