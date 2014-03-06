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
*   $Id: existsfilter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
#include <sstream>
using namespace std;

#include "existsfilter.h"
#include "bytestream.h"
using namespace messageqcpp;
#include "objectreader.h"

namespace execplan {
/**
 * Constructors/Destructors
 */
ExistsFilter::ExistsFilter():
    fNotExists (false),
    fCorrelated (false),
    fData("Exists Filter")
{}

ExistsFilter::ExistsFilter( const SCSEP& sub, 
	                          const bool existFlag,
	                          const bool correlated):
	  fSub (sub),
    fNotExists (existFlag),
    fCorrelated (correlated),
    fData ("Exists Filter")
{}

ExistsFilter::ExistsFilter(const ExistsFilter& rhs):
	fSub (rhs.fSub),
	fNotExists (rhs.fNotExists),
	fCorrelated (rhs.fCorrelated),
	fData (rhs.fData)
{}

ExistsFilter::~ExistsFilter()
{}

const string ExistsFilter::toString() const
{
	ostringstream oss;
	oss << "ExistsFilter " << "correlated=" << fCorrelated << " notExists=" << fNotExists << endl;
	oss << *(fSub.get());
	return oss.str();
}

ostream& operator<<(ostream& output, const ExistsFilter& rhs)
{
	output << rhs.toString();
    return output;
}

void ExistsFilter::serialize(messageqcpp::ByteStream& b) const
{
	b << static_cast<ObjectReader::id_t>(ObjectReader::EXISTSFILTER);
	Filter::serialize(b);
	if (fSub.get() != NULL)
		fSub->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	b << static_cast<const ByteStream::doublebyte>(fNotExists);			
	b << static_cast<const ByteStream::doublebyte>(fCorrelated);
}

void ExistsFilter::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::EXISTSFILTER);
	Filter::unserialize(b);
		fSub.reset(dynamic_cast<CalpontSelectExecutionPlan*>(ObjectReader::createExecutionPlan(b)));
	b >> reinterpret_cast< ByteStream::doublebyte&>(fNotExists);	
	b >> reinterpret_cast< ByteStream::doublebyte&>(fCorrelated);
}

bool ExistsFilter::operator==(const ExistsFilter& t) const
{
	const Filter *f1, *f2;

	f1 = static_cast<const Filter*>(this);
	f2 = static_cast<const Filter*>(&t);
	if (*f1 != *f2)
		return false;
	if (*(fSub.get()) != t.fSub.get())
		return false;
	if (fNotExists != t.fNotExists)
		return false;
	if (fCorrelated != t.fCorrelated)
		return false;
	
	return true;
}

bool ExistsFilter::operator==(const TreeNode* t) const
{
	const ExistsFilter *o;

	o = dynamic_cast<const ExistsFilter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool ExistsFilter::operator!=(const ExistsFilter& t) const
{
	return (!(*this == t));
}

bool ExistsFilter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

}

