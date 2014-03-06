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

#include <sstream>
#include <cstring>

using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "simplefilter.h"
#include "constantfilter.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "objectreader.h"
#include "groupconcatcolumn.h"

namespace execplan {

/**
 * Constructors/Destructors
 */
GroupConcatColumn::GroupConcatColumn():
		AggregateColumn()
{
}

GroupConcatColumn::GroupConcatColumn(const uint32_t sessionID):
		AggregateColumn(sessionID)
{
}

GroupConcatColumn::GroupConcatColumn(const GroupConcatColumn& rhs, const uint32_t sessionID):
		AggregateColumn(dynamic_cast<const AggregateColumn&>(rhs)),
		fOrderCols(rhs.fOrderCols),
		fSeparator(rhs.fSeparator)
{
}

GroupConcatColumn::~GroupConcatColumn()
{
}

/**
 * Methods
 */

const string GroupConcatColumn::toString() const
{
	ostringstream output;
	output << "GroupConcatColumn " << data() << endl;
	output << AggregateColumn::toString() << endl;
	output << "Group Concat Order Columns: " << endl;
	for (uint32_t i = 0; i < fOrderCols.size(); i++)
	{
		output << *fOrderCols[i];
	}
	output << "\nSeparator: " << fSeparator << endl;
	return output.str();
}

ostream& operator<<(ostream& output, const GroupConcatColumn& rhs)
{
	output << rhs.toString();
	return output;
}

void GroupConcatColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (uint8_t) ObjectReader::GROUPCONCATCOLUMN;
	AggregateColumn::serialize(b);

	CalpontSelectExecutionPlan::ReturnedColumnList::const_iterator rcit;
	b << static_cast<uint32_t>(fOrderCols.size());
	for (rcit = fOrderCols.begin(); rcit != fOrderCols.end(); ++rcit)
		(*rcit)->serialize(b);
	b << fSeparator;
}

void GroupConcatColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::GROUPCONCATCOLUMN);
	AggregateColumn::unserialize(b);
	fOrderCols.erase(fOrderCols.begin(), fOrderCols.end());

	uint32_t size, i;
	ReturnedColumn *rc;
	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
		SRCP srcp(rc);
		fOrderCols.push_back(srcp);
	}
	b >> fSeparator;
}

bool GroupConcatColumn::operator==(const GroupConcatColumn& t) const
{
	const AggregateColumn *rc1, *rc2;

	rc1 = static_cast<const AggregateColumn*>(this);
	rc2 = static_cast<const AggregateColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	for (uint32_t i = 0; i < fOrderCols.size(); i++)
	{
		if (fOrderCols[i].get() != NULL)
		{
			if (t.fOrderCols[i] == NULL)
				return false;
			if (*(fOrderCols[i].get()) != t.fOrderCols[i].get())
				return false;
		}
		else
			if (t.fOrderCols[i].get() != NULL)
				return false;
	}
	if (fSeparator != t.fSeparator)
		return false;
	return true;
}

bool GroupConcatColumn::operator==(const TreeNode* t) const
{
	const GroupConcatColumn *ac;

	ac = dynamic_cast<const GroupConcatColumn*>(t);
	if (ac == NULL)
		return false;
	return *this == *ac;
}

bool GroupConcatColumn::operator!=(const GroupConcatColumn& t) const
{
	return !(*this == t);
}

bool GroupConcatColumn::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

} // namespace execplan
