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
*   $Id: simplefilter.cpp 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
#include <exception>
#include <stdexcept>
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include "returnedcolumn.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
#include "operator.h"
#include "constantfilter.h"
#include "bytestream.h"
#include "objectreader.h"
#include "functioncolumn.h"
#include "arithmeticcolumn.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"

namespace execplan {

/**
 * Constructors/Destructors
 */
SimpleFilter::SimpleFilter():
			fLhs(0),
			fRhs(0),
			fIndexFlag(NOINDEX),
			fJoinFlag (EQUA)
{}

// TODO: only handled simplecolumn operands for now
SimpleFilter::SimpleFilter(const string& sql):
		Filter(sql)
{
	parse(sql);
}

SimpleFilter::SimpleFilter(const SOP& op, ReturnedColumn* lhs, ReturnedColumn* rhs) :
	fOp(op), fLhs(lhs), fRhs(rhs), fIndexFlag(NOINDEX), fJoinFlag(EQUA)
{
	convertConstant();
}

SimpleFilter::SimpleFilter(const SimpleFilter& rhs) :
	fOp(rhs.op()),
	fIndexFlag(rhs.indexFlag()),
	fJoinFlag(rhs.joinFlag())
{
	fLhs = rhs.lhs()->clone();
	fRhs = rhs.rhs()->clone();

	fSimpleColumnList.clear();
	fAggColumnList.clear();
	fWindowFunctionColumnList.clear();

	SimpleColumn *lsc = dynamic_cast<SimpleColumn*>(fLhs);
	FunctionColumn *lfc = dynamic_cast<FunctionColumn*>(fLhs);
	ArithmeticColumn *lac = dynamic_cast<ArithmeticColumn*>(fLhs);
	WindowFunctionColumn *laf = dynamic_cast<WindowFunctionColumn*>(fLhs);
	AggregateColumn *lagc = dynamic_cast<AggregateColumn*>(fLhs);
	SimpleColumn *rsc = dynamic_cast<SimpleColumn*>(fRhs);
	FunctionColumn *rfc = dynamic_cast<FunctionColumn*>(fRhs);
	ArithmeticColumn *rac = dynamic_cast<ArithmeticColumn*>(fRhs);
	AggregateColumn *ragc = dynamic_cast<AggregateColumn*>(fRhs);
	WindowFunctionColumn *raf = dynamic_cast<WindowFunctionColumn*>(fRhs);

	if (lsc)
	{
		fSimpleColumnList.push_back(lsc);
	}
	else if (lagc)
	{
		fAggColumnList.push_back(lagc);
	}
	else if (lfc)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), lfc->simpleColumnList().begin(), lfc->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), lfc->aggColumnList().begin(), lfc->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), lfc->windowfunctionColumnList().begin(), lfc->windowfunctionColumnList().end());
	}
	else if (lac)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), lac->simpleColumnList().begin(), lac->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), lac->aggColumnList().begin(), lac->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), lac->windowfunctionColumnList().begin(), lac->windowfunctionColumnList().end());
	}
	else if (laf)
	{
		fWindowFunctionColumnList.push_back(laf);
	}

	if (rsc)
	{
		fSimpleColumnList.push_back(rsc);
	}
	else if (ragc)
	{
		fAggColumnList.push_back(ragc);
	}
	else if (rfc)
	{
		fSimpleColumnList.insert
		  (fSimpleColumnList.end(), rfc->simpleColumnList().begin(), rfc->simpleColumnList().end());
		fAggColumnList.insert
		  (fAggColumnList.end(), rfc->aggColumnList().begin(), rfc->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), rfc->windowfunctionColumnList().begin(), rfc->windowfunctionColumnList().end());
	}
	else if (rac)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), rac->simpleColumnList().begin(), rac->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), rac->aggColumnList().begin(), rac->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), rac->windowfunctionColumnList().begin(), rac->windowfunctionColumnList().end());
	}
	else if (raf)
	{
		fWindowFunctionColumnList.push_back(raf);
	}
}

SimpleFilter::~SimpleFilter()
{
   	//delete fOp;
	delete fLhs;
	delete fRhs;
}

/**
 * Methods
 */

void SimpleFilter::lhs(ReturnedColumn* lhs)
{
	fLhs = lhs;
	if (fLhs && fRhs)
		convertConstant();
}

void SimpleFilter::rhs(ReturnedColumn* rhs)
{
	fRhs = rhs;
	if (fLhs && fRhs)
		convertConstant();
}

const string SimpleFilter::data() const
{
	string rhs, lhs;
	if (dynamic_cast<ConstantColumn*>(fRhs) &&
		  (fRhs->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
		   fRhs->resultType().colDataType == CalpontSystemCatalog::CHAR ||
		   fRhs->resultType().colDataType == CalpontSystemCatalog::VARBINARY ||
		   fRhs->resultType().colDataType == CalpontSystemCatalog::DATE ||
		   fRhs->resultType().colDataType == CalpontSystemCatalog::DATETIME))
		rhs = "'" + fRhs->data() + "'";
	else
		rhs = fRhs->data();
	if (dynamic_cast<ConstantColumn*>(fLhs) &&
		  (fLhs->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
		   fLhs->resultType().colDataType == CalpontSystemCatalog::CHAR ||
		   fLhs->resultType().colDataType == CalpontSystemCatalog::VARBINARY ||
		   fLhs->resultType().colDataType == CalpontSystemCatalog::DATE ||
		   fLhs->resultType().colDataType == CalpontSystemCatalog::DATETIME))
		lhs = "'" + fLhs->data() + "'";
	else
		lhs = fLhs->data();
	return lhs + " " + fOp->data() + " " + rhs;
}

const string SimpleFilter::toString() const
{
	ostringstream output;
	output << "SimpleFilter(indexflag=" << fIndexFlag;
	output << " joinFlag= " << fJoinFlag;
	output << " card= " << fCardinality << ")"<< endl;
	output << "  " << *fLhs;
	output << "  " << *fOp;
	output << "  " << *fRhs;
	return output.str();
}

void SimpleFilter::parse(string sql)
{
	fLhs = 0;
	fRhs = 0;
	string delimiter[7] = {">=", "<=", "<>", "!=", "=", "<", ">"};
	string::size_type pos;
	for (int i = 0; i < 7; i++)
	{
		pos = sql.find(delimiter[i], 0);
		if (pos == string::npos)
			continue;
		fOp.reset(new Operator (delimiter[i]));
		string lhs = sql.substr(0, pos);
		if (lhs.at(0) == ' ')
			lhs = lhs.substr(1, pos);
		if (lhs.at(lhs.length()-1) == ' ')
			lhs = lhs.substr(0, pos-1);
		fLhs = new SimpleColumn(lhs);

		pos = pos + delimiter[i].length();
		string rhs = sql.substr(pos,sql.length());
		if (rhs.at(0) == ' ')
			rhs = rhs.substr(1, pos);
		if (rhs.at(rhs.length()-1) == ' ')
			rhs = rhs.substr(0, pos-1);
		fRhs = new SimpleColumn (rhs);
		break;
	}
	if (fLhs == NULL || fRhs == NULL)
		throw runtime_error ("invalid sql for simple filter\n" );
}

ostream& operator<<(ostream& output, const SimpleFilter& rhs)
{
	output << rhs.toString();
	return output;
}

void SimpleFilter::serialize(messageqcpp::ByteStream& b) const
{
	b << static_cast<ObjectReader::id_t>(ObjectReader::SIMPLEFILTER);
	Filter::serialize(b);
	if (fOp != NULL)
		fOp->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	if (fLhs != NULL)
		fLhs->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	if (fRhs != NULL)
		fRhs->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	b << static_cast<uint32_t>(fIndexFlag);
	b << static_cast<uint32_t>(fJoinFlag);
}

void SimpleFilter::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::SIMPLEFILTER);

	//delete fOp;
	delete fLhs;
	delete fRhs;
	Filter::unserialize(b);
	fOp.reset(dynamic_cast<Operator*>(ObjectReader::createTreeNode(b)));
	fLhs = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
	fRhs = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
	b >> reinterpret_cast<uint32_t&>(fIndexFlag);
	b >> reinterpret_cast<uint32_t&>(fJoinFlag);

	fSimpleColumnList.clear();
	fAggColumnList.clear();
	fWindowFunctionColumnList.clear();

	SimpleColumn *lsc = dynamic_cast<SimpleColumn*>(fLhs);
	FunctionColumn *lfc = dynamic_cast<FunctionColumn*>(fLhs);
	ArithmeticColumn *lac = dynamic_cast<ArithmeticColumn*>(fLhs);
	WindowFunctionColumn *laf = dynamic_cast<WindowFunctionColumn*>(fLhs);
	AggregateColumn *lagc = dynamic_cast<AggregateColumn*>(fLhs);
	SimpleColumn *rsc = dynamic_cast<SimpleColumn*>(fRhs);
	FunctionColumn *rfc = dynamic_cast<FunctionColumn*>(fRhs);
	ArithmeticColumn *rac = dynamic_cast<ArithmeticColumn*>(fRhs);
	AggregateColumn *ragc = dynamic_cast<AggregateColumn*>(fRhs);
	WindowFunctionColumn *raf = dynamic_cast<WindowFunctionColumn*>(fRhs);

	if (lsc)
	{
		fSimpleColumnList.push_back(lsc);
	}
	else if (lagc)
	{
		fAggColumnList.push_back(lagc);
	}
	else if (lfc)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), lfc->simpleColumnList().begin(), lfc->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), lfc->aggColumnList().begin(), lfc->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), lfc->windowfunctionColumnList().begin(), lfc->windowfunctionColumnList().end());
	}
	else if (lac)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), lac->simpleColumnList().begin(), lac->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), lac->aggColumnList().begin(), lac->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), lac->windowfunctionColumnList().begin(), lac->windowfunctionColumnList().end());
	}
	else if (laf)
	{
		fWindowFunctionColumnList.push_back(laf);
	}

	if (rsc)
	{
		fSimpleColumnList.push_back(rsc);
	}
	else if (ragc)
	{
		fAggColumnList.push_back(ragc);
	}
	else if (rfc)
	{
		fSimpleColumnList.insert
		  (fSimpleColumnList.end(), rfc->simpleColumnList().begin(), rfc->simpleColumnList().end());
		fAggColumnList.insert
		  (fAggColumnList.end(), rfc->aggColumnList().begin(), rfc->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), rfc->windowfunctionColumnList().begin(), rfc->windowfunctionColumnList().end());
	}
	else if (rac)
	{
		fSimpleColumnList.insert(fSimpleColumnList.end(), rac->simpleColumnList().begin(), rac->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), rac->aggColumnList().begin(), rac->aggColumnList().end());
		fWindowFunctionColumnList.insert
		  (fWindowFunctionColumnList.end(), rac->windowfunctionColumnList().begin(), rac->windowfunctionColumnList().end());
	}
	else if (raf)
	{
		fWindowFunctionColumnList.push_back(raf);
	}

	// construct regex constant for like operator
	if (fOp->op() == OP_LIKE || fOp->op() == OP_NOTLIKE)
	{
		ConstantColumn *rcc = dynamic_cast<ConstantColumn*>(fRhs);
		if (rcc)
			rcc->constructRegex();
		ConstantColumn *lcc = dynamic_cast<ConstantColumn*>(fLhs);
		if (lcc)
			lcc->constructRegex();
	}
}

bool SimpleFilter::operator==(const SimpleFilter& t) const
{
	const Filter *f1, *f2;

	f1 = static_cast<const Filter*>(this);
	f2 = static_cast<const Filter*>(&t);
	if (*f1 != *f2)
		return false;

	if (fOp != NULL) {
		if (*fOp != *t.fOp)
			return false;
	}
	else if (t.fOp != NULL)
		return false;

	if (fLhs != NULL) {
		if (*fLhs != t.fLhs)
			return false;
	}
	else if (t.fLhs != NULL)
		return false;

	if (fRhs != NULL) {
		if (*fRhs != t.fRhs)
			return false;
	}
	else if (t.fRhs != NULL)
		return false;

	else if (fIndexFlag != t.fIndexFlag)
		return false;

	else if (fJoinFlag != t.fJoinFlag)
		return false;

	return true;
}

bool SimpleFilter::operator==(const TreeNode* t) const
{
	const SimpleFilter *o;

	o = dynamic_cast<const SimpleFilter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool SimpleFilter::operator!=(const SimpleFilter& t) const
{
	return (!(*this == t));
}

bool SimpleFilter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

bool SimpleFilter::pureFilter()
{
	if (typeid (*fLhs) == typeid(ConstantColumn) &&
		typeid (*fRhs) != typeid(ConstantColumn))
	{
		// make sure constantCol sit on right hand side
		ReturnedColumn* temp = fLhs;
		fLhs = fRhs;
		fRhs = temp;
		// also switch indexFlag
		if (fIndexFlag == SimpleFilter::LEFT) fIndexFlag = SimpleFilter::RIGHT;
		else if (fIndexFlag == SimpleFilter::RIGHT) fIndexFlag = SimpleFilter::LEFT;
		return true;
	}
	if (typeid (*fRhs) == typeid(ConstantColumn) &&
			 typeid (*fLhs) != typeid(ConstantColumn))
		return true;
	return false;
}

void SimpleFilter::convertConstant()
{
	if (fOp->op() == OP_ISNULL || fOp->op() == OP_ISNOTNULL)
		return;

	ConstantColumn *lcc = dynamic_cast<ConstantColumn*>(fLhs);
	ConstantColumn *rcc = dynamic_cast<ConstantColumn*>(fRhs);
	if (lcc)
	{
		Result result = lcc->result();
		if (fRhs->resultType().colDataType == CalpontSystemCatalog::DATE)
		{
			if (lcc->constval().empty())
			{
				lcc->constval("0000-00-00");
				result.intVal = 0;
				result.strVal = lcc->constval();
			}
			else
			{
				result.intVal = dataconvert::DataConvert::dateToInt(result.strVal);
			}
		}
		else if (fRhs->resultType().colDataType == CalpontSystemCatalog::DATETIME)
		{
			if (lcc->constval().empty())
			{
				lcc->constval("0000-00-00 00:00:00");
				result.intVal = 0;
				result.strVal = lcc->constval();
			}
			else
			{
				result.intVal = dataconvert::DataConvert::datetimeToInt(result.strVal);
			}
		}
		lcc->result(result);
	}
	if (rcc)
	{
		Result result = rcc->result();
		if (fLhs->resultType().colDataType == CalpontSystemCatalog::DATE)
		{
			if (rcc->constval().empty())
			{
				rcc->constval("0000-00-00");
				result.intVal = 0;
				result.strVal = rcc->constval();
			}
			else
			{
				result.intVal = dataconvert::DataConvert::dateToInt(result.strVal);
			}
		}
		else if (fLhs->resultType().colDataType == CalpontSystemCatalog::DATETIME)
		{
			if (rcc->constval().empty())
			{
				rcc->constval("0000-00-00 00:00:00");
				result.intVal = 0;
				result.strVal = rcc->constval();
			}
			else
			{
				result.intVal = dataconvert::DataConvert::datetimeToInt(result.strVal);
			}
		}
		rcc->result(result);
	}
}

void SimpleFilter::setDerivedTable()
{
	string lDerivedTable = "";
	string rDerivedTable = "";

	if (fLhs)
	{
		fLhs->setDerivedTable();
		lDerivedTable = fLhs->derivedTable();
	}
	else
	{
		lDerivedTable = "*";
	}

	if (fRhs)
	{
		fRhs->setDerivedTable();
		rDerivedTable = fRhs->derivedTable();
	}
	else
	{
		rDerivedTable = "*";
	}

	if (lDerivedTable == "*")
	{
		fDerivedTable = rDerivedTable;
	}
	else if (rDerivedTable == "*")
	{
		fDerivedTable = lDerivedTable;
	}
	else if (lDerivedTable == rDerivedTable)
	{
		fDerivedTable = lDerivedTable; // should be the same as rhs
	}
	else
	{
		fDerivedTable = "";
	}
}

void SimpleFilter::replaceRealCol(CalpontSelectExecutionPlan::ReturnedColumnList& derivedColList)
{
	SimpleColumn *sc = NULL;
	if (fLhs)
	{
		sc = dynamic_cast<SimpleColumn*>(fLhs);
		if (sc)
		{
			ReturnedColumn* tmp = derivedColList[sc->colPosition()]->clone();
			delete fLhs;
			fLhs = tmp;
		}
		else
		{
			fLhs->replaceRealCol(derivedColList);
		}
	}
	if (fRhs)
	{
		sc = dynamic_cast<SimpleColumn*>(fRhs);
		if (sc)
		{
			ReturnedColumn* tmp = derivedColList[sc->colPosition()]->clone();
			delete fRhs;
			fRhs = tmp;
		}
		else
		{
			fRhs->replaceRealCol(derivedColList);
		}
	}
}


} // namespace execplan
