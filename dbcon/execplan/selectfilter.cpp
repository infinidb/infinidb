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

/***********************************************************************
*   $Id: selectfilter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include "selectfilter.h"
#include "bytestream.h"
#include "objectreader.h"

namespace execplan{
/**
 * Constructors/Destructors
 */
SelectFilter::SelectFilter()
{}

SelectFilter::SelectFilter(const vector<SRCP>& cols, 
                           const SOP& op, 
                           SCSEP& sub,
                           bool correlated) :
	fCols(cols),
	fOp(op),
	fSub(sub),
	fCorrelated(correlated),
	fData("subselect"),
	fReturnedColPos(0)
{}

SelectFilter::SelectFilter(const SelectFilter& rhs):
	fCols(rhs.fCols),
	fOp(rhs.fOp),
	fSub(rhs.fSub),
	fCorrelated(rhs.fCorrelated),
	fData(rhs.fData),
	fReturnedColPos(0)
{}


SelectFilter::~SelectFilter()
{}

const string SelectFilter::toString() const
{
    ostringstream oss;
    oss << "SelectFilter " << "returnedColPos=" << fReturnedColPos << endl;
    for (uint32_t i = 0; i < fCols.size(); i++)
    	oss << fCols[i]->toString();
    oss << fOp->toString() << endl;
    oss << *(fSub.get());
	return oss.str();
}

ostream& operator<<(ostream& output, const SelectFilter& rhs)
{
	output << rhs.toString();
	return output;
}

void SelectFilter::serialize(messageqcpp::ByteStream& b) const
{
	b << static_cast<ObjectReader::id_t>(ObjectReader::SELECTFILTER);
	Filter::serialize(b);
	b << static_cast<uint32_t>(fCols.size());
	for (uint32_t i = 0; i < fCols.size(); i++)
	{
		if (fCols[i] != NULL)
			fCols[i]->serialize(b);
		else
			b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	}
	if (fOp != NULL)
		fOp->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	if (fSub.get() != NULL)
		fSub->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	b << static_cast<uint64_t>(fReturnedColPos);
}

void SelectFilter::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::SELECTFILTER);
	
	Filter::unserialize(b);
	fCols.clear();
	uint32_t size;
	SRCP srcp;
	b >> size;
	for (uint32_t i = 0; i < size; i++)
	{
		srcp.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
		fCols.push_back(srcp);
	}
	fOp.reset(dynamic_cast<Operator*>(ObjectReader::createTreeNode(b)));
	fSub.reset(dynamic_cast<CalpontSelectExecutionPlan*>(ObjectReader::createExecutionPlan(b)));
	b >> static_cast<uint64_t&>(fReturnedColPos);
}

bool SelectFilter::operator==(const SelectFilter& t) const
{
	const Filter *f1, *f2;

	f1 = static_cast<const Filter*>(this);
	f2 = static_cast<const Filter*>(&t);
	if (*f1 != *f2)
		return false;
	if (fCols.size() != t.fCols.size())
		return false;
	for (uint32_t i = 0; i < fCols.size(); i++)
	{
		if (fCols[i] != NULL) {
			if (*(fCols[i]) != *(t.fCols[i]))
				return false;
		}
		else if (t.fCols[i].get() != NULL)
			return false;
	}
	
	if (fOp != NULL) {
		if (*fOp != *t.fOp)
			return false;
	}
	else if (t.fOp != NULL)
		return false;
	
	if (fSub != NULL) {
		if (*fSub != t.fSub.get())
			return false;
	}
	else if (t.fSub != NULL)
		return false;
	
	if (fData != t.fData)
		return false;
	if (fReturnedColPos != t.fReturnedColPos)
		return false;
	return true;
}

bool SelectFilter::operator==(const TreeNode* t) const
{
	const SelectFilter *o;

	o = dynamic_cast<const SelectFilter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool SelectFilter::operator!=(const SelectFilter& t) const
{
	return (!(*this == t));
}

bool SelectFilter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

}
