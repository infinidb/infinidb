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
*   $Id: rowcolumn.cpp 6309 2010-03-04 19:33:12Z zzhu $
*
*
***********************************************************************/

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sstream>

using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "objectreader.h"
#include "calpontselectexecutionplan.h"

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "dataconvert.h"

#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "constantfilter.h"
#include "rowcolumn.h"

namespace execplan
{

/**
 * Constructors/Destructors
 */
RowColumn::RowColumn(const uint32_t sessionID):
    ReturnedColumn(sessionID)
{}    

RowColumn::RowColumn (const RowColumn& rhs, const uint32_t sessionID):
                ReturnedColumn(rhs, sessionID)                
{
	fColumnVec.clear();
	//fColumnVec = rhs.fColumnVec;
	SRCP srcp;
	for (uint32_t i = 0; i < rhs.fColumnVec.size(); i++)
	{
		srcp.reset(rhs.fColumnVec[i]->clone());
		fColumnVec.push_back(srcp);
	}
}

RowColumn::~RowColumn()
{}

/**
 * Methods
 */

RowColumn& RowColumn::operator=(const RowColumn& rhs)
{
	if (this != &rhs)
	{
		fColumnVec.clear();
		fColumnVec = rhs.fColumnVec;
	}

	return *this;
}

ostream& operator<<(ostream& output, const RowColumn& rhs)
{
	output << rhs.toString();

	return output;
}

const string RowColumn::toString() const
{
	ostringstream output;
	output << "RowColumn" << endl;
	for (uint32_t i = 0; i < fColumnVec.size(); i++)
		output << fColumnVec[i]->toString();

	return output.str();
}

void RowColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::ROWCOLUMN;
	ReturnedColumn::serialize(b);
	b << (uint32_t)fColumnVec.size();
	for (uint32_t i = 0; i < fColumnVec.size(); i++)
		fColumnVec[i]->serialize(b);
}

void RowColumn::unserialize(messageqcpp::ByteStream& b)
{
	fColumnVec.clear();
	ObjectReader::checkType(b, ObjectReader::ROWCOLUMN);
	ReturnedColumn::unserialize(b);
	uint32_t size;
	SRCP srcp;
	b >> (uint32_t&)size;
	for (uint32_t i = 0; i < size; i++)
	{
		srcp.reset(dynamic_cast<ReturnedColumn*>((ObjectReader::createTreeNode(b))));
		fColumnVec.push_back(srcp);
	}
}

bool RowColumn::operator==(const RowColumn& t) const
{
	if (fColumnVec.size() != t.columnVec().size())
		return false;
	for (uint32_t i = 0; i < fColumnVec.size(); i++)
	{
		if (fColumnVec[i].get() != NULL)
		{
			if (t.columnVec()[i].get() == NULL)
				return false;
			if (*(fColumnVec[i].get()) != t.columnVec()[i].get())
				return false;
		}
		else
			if (t.columnVec()[i].get() != NULL)
				return false;
	}
	return true;
}

bool RowColumn::operator==(const TreeNode* t) const
{
	const RowColumn *rc;

	rc = dynamic_cast<const RowColumn*>(t);
	if (rc == NULL)
		return false;
	return *this == *rc;
}

bool RowColumn::operator!=(const RowColumn& t) const
{
	return !(*this == t);
}

bool RowColumn::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

ostream& operator<<(ostream &output, const SubSelect& ss)
{
	output << ss.toString() << endl;
	return output;
}

const string SubSelect::toString() const
{
	return string(">SubSelect<");
}

} // namespace execplan
