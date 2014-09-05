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
*   $Id: outerjoinonfilter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
#include <sstream>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;
#include "objectreader.h"
#include "outerjoinonfilter.h"

namespace execplan {
/**
 * Constructors/Destructors
 */
OuterJoinOnFilter::OuterJoinOnFilter():
	fData("Outer Join On Filter")
{}

OuterJoinOnFilter::OuterJoinOnFilter(const SPTP& pt):
	fPt(new ParseTree (*(pt.get()))),
	fData("Outer Join On Filter")
{}

OuterJoinOnFilter::OuterJoinOnFilter(const OuterJoinOnFilter& rhs):
	fPt (rhs.fPt),
	fData (rhs.fData)
{}

OuterJoinOnFilter::~OuterJoinOnFilter()
{}

const string OuterJoinOnFilter::toString() const
{
	ostringstream oss;
	oss << "OuterJoinOnFilter" << endl;
	if (fPt.get())
		oss << fPt->toString();
	else
		oss << "Empty Tree." << endl;
	oss << "End OuterJoinOnFilter" << endl;
	return oss.str();
}

ostream& operator<<(ostream& output, const OuterJoinOnFilter& rhs)
{
	output << rhs.toString();
    return output;
}

void OuterJoinOnFilter::serialize(messageqcpp::ByteStream& b) const
{
	b << static_cast<ObjectReader::id_t>(ObjectReader::OUTERJOINONFILTER);
	Filter::serialize(b);
	if (fPt.get() != NULL)
		ObjectReader::writeParseTree(fPt.get(), b);		
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
}

void OuterJoinOnFilter::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::OUTERJOINONFILTER);
	Filter::unserialize(b);
		fPt.reset(ObjectReader::createParseTree(b));
}

bool OuterJoinOnFilter::operator==(const OuterJoinOnFilter& t) const
{
	const Filter *f1, *f2;

	f1 = static_cast<const Filter*>(this);
	f2 = static_cast<const Filter*>(&t);
	if (*f1 != *f2)
		return false;
	if (*(fPt.get()) != *(t.fPt.get()))
		return false;

	return true;
}

bool OuterJoinOnFilter::operator==(const TreeNode* t) const
{
	const OuterJoinOnFilter *o;

	o = dynamic_cast<const OuterJoinOnFilter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool OuterJoinOnFilter::operator!=(const OuterJoinOnFilter& t) const
{
	return (!(*this == t));
}

bool OuterJoinOnFilter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

}

