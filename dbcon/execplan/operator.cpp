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
*   $Id: operator.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <iostream>
#include <sstream>

#include "bytestream.h"
#include "operator.h"
#include "objectreader.h"

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
Operator::Operator()
{
}

Operator::Operator(const string& operatorName)
{
	data(operatorName);
}					

Operator::Operator(const Operator& rhs) :
	TreeNode(rhs),
	fOp(rhs.op())	
{
	data(rhs.fData);
}

Operator:: ~Operator()
{
}

/**
 * Operations
 */
void Operator::data(const string data) 
{ 	
	fData = lrtrim(data);
	transform (fData.begin(), fData.end(), fData.begin(), to_lower()); 
	if (fData == "+")
	{
		fOp = OP_ADD;
	}
	else if (fData == "-")
	{
		fOp = OP_SUB;
	}
	else if (fData == "*")
	{
		fOp = OP_MUL;
	}
	else if (fData == "/")
	{
		fOp = OP_DIV;
	}
	else if (fData == "=")
	{
		fOp = OP_EQ;
	}
	else if (fData == "!=" || fData == "<>")
	{
		fOp = OP_NE;
	}
	else if (fData == ">")
	{
		fOp = OP_GT;
	}
	else if (fData == ">=")
	{
		fOp = OP_GE;
	}
	else if (fData == "<")
	{
		fOp = OP_LT;
	}
	else if (fData == "<=")
	{
		fOp = OP_LE;
	}
	else if (fData == "and")
	{
		fOp = OP_AND;
	}
	else if (fData == "or")
	{
		fOp = OP_OR;
	}
	else if (fData == "like")
	{
		fOp = OP_LIKE;
	}
	else if (fData == "not like")
	{
		fOp = OP_NOTLIKE;
		fData = "not like";
	}
	else if (fData == "isnull")
	{
		fOp = OP_ISNULL;
		fData = "is null";
	}
	else if (fData == "isnotnull")
	{
		fOp = OP_ISNOTNULL;
		fData = "is not null";
	}
	else if (fData == "xor")
	{
		fOp = OP_XOR;
	}
	else
	{
		fOp = OP_UNKNOWN;
	}
}

const string Operator::toString() const
{
	ostringstream oss;
	oss << string("Operator: " + fData) << " fOp=" << fOp;
	oss << " " << "opType=" << fOperationType.colDataType;
	return oss.str();
}

Operator* Operator::opposite() const
{
	if (fData.compare(">") == 0)
			return new Operator("<");
	if (fData.compare("<") == 0)
			return new Operator(">");
	if (fData.compare(">=") == 0)
			return new Operator("<=");
	if (fData.compare("<=") == 0)
			return new Operator(">=");		
	return this->clone();
}

/**
 * friend function
 */
ostream& operator<<(ostream &output, const Operator& rhs) 
{
	output << rhs.toString();
	return output;
} 

/**
 * The serialization interface
 */
void Operator::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::OPERATOR;
	b << fData;
	fResultType.serialize(b);
	fOperationType.serialize(b);
	b << (uint32_t)fOp;
}

void Operator::unserialize(messageqcpp::ByteStream& b)
{
	uint32_t val;
	ObjectReader::checkType(b, ObjectReader::OPERATOR);
	b >> fData;
	fResultType.unserialize(b);
	fOperationType.unserialize(b);	
	
	b >> (uint32_t&)val;
	fOp = (OpType)val;

	fResult.decimalVal.scale = fResultType.scale;
	fResult.decimalVal.precision = fResultType.precision;
}

bool Operator::operator==(const Operator& t) const
{
	if (fOp == t.fOp)
		return true;
	return false;
}

bool Operator::operator==(const TreeNode* t) const
{
	const Operator *o;

	o = dynamic_cast<const Operator*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool Operator::operator!=(const Operator& t) const
{
	return (!(*this == t));
}

bool Operator::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

void Operator::reverseOp()
{
	switch (fOp)
	{
		case OP_EQ:
			fOp = OP_NE;
			fData = "!=";
			break;
		case OP_NE:
			fOp = OP_EQ;
			fData = "=";
			break;
		case OP_GT:
			fOp = OP_LT;
			fData = "<";
			break;
		case OP_GE:
			fOp = OP_LE;
			fData = "<=";
			break;
		case OP_LT:
			fOp = OP_GT;
			fData = ">";
			break;
		case OP_LE:
			fOp = OP_GE;
			fData = ">=";
			break;
		case OP_LIKE:
			fOp = OP_NOTLIKE;
			fData = "not like";
			break;
		case OP_NOTLIKE:
			fOp = OP_LIKE;
			fData = "like";
			break;
		case OP_ISNULL:
			fOp = OP_ISNOTNULL;
			fData = "isnotnull";
			break;
		case OP_ISNOTNULL:
			fOp = OP_ISNULL;
			fData = "isnull";
			break;
		case OP_BETWEEN:
			fOp = OP_NOTBETWEEN;
			fData = "not between";
			break;
		case OP_NOTBETWEEN:
			fOp = OP_BETWEEN;
			fData = "between";
			break;
		case OP_IN:
			fOp = OP_NOTIN;
			fData = "not in";
			break;
		case OP_NOTIN:
			fOp = OP_IN;
			fData = "in";
			break;
		default:
			fOp = OP_UNKNOWN;
			fData = "unknown";
			break;
	}
}

}  // namespace
