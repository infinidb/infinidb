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
*   $Id: logicoperator.cpp 7409 2011-02-08 14:38:50Z rdempsey $
*
*
***********************************************************************/
#include <iostream>

#include "bytestream.h"
#include "objectreader.h"
#include "logicoperator.h"

using namespace std;

namespace {
    
/**@brief util struct for converting string to lower case */
struct to_lower
{
    char operator() (char c) const { return tolower(c); }
};

//Trim any leading/trailing ws
const string lrtrim(const string& in)
{
	string::size_type p1;
	p1 = in.find_first_not_of(" \t\n");
	if (p1 == string::npos) p1 = 0;
	string::size_type p2;
	p2 = in.find_last_not_of(" \t\n");
	if (p2 == string::npos) p2 = in.size() - 1;
	return string(in, p1, (p2 - p1 + 1));
}

}

namespace execplan {
    
/**
 * Constructors/Destructors
 */
LogicOperator::LogicOperator()
{
}

LogicOperator::LogicOperator(const string& operatorName)
{
	data(operatorName);
}					

LogicOperator::LogicOperator(const LogicOperator& rhs) : Operator(rhs)
{
	data(rhs.fData);
}

LogicOperator:: ~LogicOperator()
{
}

/**
 * Operations
 */


/**
 * friend function
 */
ostream& operator<<(ostream &output, const LogicOperator& rhs) 
{
	output << rhs.toString();
	return output;
} 

/**
 * The serialization interface
 */
void LogicOperator::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::LOGICOPERATOR;
	//b << fData;
	Operator::serialize(b);
}

void LogicOperator::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::LOGICOPERATOR);
	//b >> fData;
	Operator::unserialize(b);
}

bool LogicOperator::operator==(const LogicOperator& t) const
{
	if (fData == t.fData)
		return true;
	return false;
}

bool LogicOperator::operator==(const TreeNode* t) const
{
	const LogicOperator *o;

	o = dynamic_cast<const LogicOperator*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool LogicOperator::operator!=(const LogicOperator& t) const
{
	return (!(*this == t));
}

bool LogicOperator::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

}  // namespace
