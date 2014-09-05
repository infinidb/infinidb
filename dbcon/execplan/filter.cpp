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
*   $Id: filter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
using namespace std;

#include "bytestream.h"
#include "filter.h"
#include "objectreader.h"
#include "simplefilter.h"
#include "constantfilter.h"

namespace execplan {
/**
 * Constructors/Destructors
 */
Filter::Filter()
{
    fCardinality = 0;
}

Filter::Filter(const string& sql) :
    fData(sql)
{
    fCardinality = 0;
}

Filter::~Filter()
{}


/**
 * The serialization interface
 */
void Filter::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::FILTER;
	b << fData;
	b << (uint64_t)fCardinality;
}

void Filter::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::FILTER);
	b >> fData;
	b >> (uint64_t&)fCardinality;
}
	
const string Filter::toString() const
{
	return string(">Filter<");
}

bool Filter::operator==(const Filter& t) const
{
	if (fData == t.fData)
		return true;
	return false;
}

bool Filter::operator==(const TreeNode* t) const
{
	const Filter *o;

	o = dynamic_cast<const Filter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool Filter::operator!=(const Filter& t) const
{
	return (!(*this == t));
}

bool Filter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

Filter* Filter::combinable(Filter* f, Operator* op)
{
    SimpleFilter *sf = dynamic_cast<SimpleFilter*>(f);
    if (sf) 
        return sf->combinable(this, op);
    ConstantFilter *cf = dynamic_cast<ConstantFilter*>(f);
    if (cf)
        return cf->combinable(this, op);
    return NULL;
}

/**
 * Friend function
 */
ostream& operator<<(ostream& output, const Filter& rhs)
{
	output << rhs.toString();
	return output;
} 

} // namespace execplan
