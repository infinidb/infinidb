/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: aggregatecolumn.cpp 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
#include <sstream>
#include <cstring>
using namespace std;

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "aggregatecolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "objectreader.h"

namespace execplan {

void getAggCols(execplan::ParseTree* n, void* obj)
{
	vector<AggregateColumn*>* list = reinterpret_cast< vector<AggregateColumn*>*>(obj);
	TreeNode* tn = n->data();
	AggregateColumn *sc = dynamic_cast<AggregateColumn*>(tn);
	FunctionColumn *fc = dynamic_cast<FunctionColumn*>(tn);
	ArithmeticColumn *ac = dynamic_cast<ArithmeticColumn*>(tn);
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	ConstantFilter *cf = dynamic_cast<ConstantFilter*>(tn);
	if (sc)
		list->push_back(sc);
	else if (fc)
		list->insert(list->end(), fc->aggColumnList().begin(), fc->aggColumnList().end());
	else if (ac)
		list->insert(list->end(), ac->aggColumnList().begin(), ac->aggColumnList().end());
	else if (sf)
		list->insert(list->end(), sf->aggColumnList().begin(), sf->aggColumnList().end());
	else if (cf)
		list->insert(list->end(), cf->aggColumnList().begin(), cf->aggColumnList().end());
}

/**
 * Constructors/Destructors
 */
AggregateColumn::AggregateColumn():
	fAggOp(NOOP),
	fAsc(false)
{
}

AggregateColumn::AggregateColumn(const uint32_t sessionID):
	ReturnedColumn(sessionID),
	fAggOp(NOOP),
	fAsc(false)
{
}

AggregateColumn::AggregateColumn(const AggOp aggOp, ReturnedColumn* parm, const uint32_t sessionID):
    ReturnedColumn(sessionID),
    fAggOp(aggOp),
    fAsc(false),
    fData(aggOp + "(" + parm->data() + ")")
{
	fFunctionParms.reset(parm);
}

AggregateColumn::AggregateColumn(const AggOp aggOp, const string& content, const uint32_t sessionID):
    ReturnedColumn(sessionID),
    fAggOp(aggOp),
    fAsc(false),
    fData(aggOp + "(" + content + ")")
{
	// TODO: need to handle distinct
	fFunctionParms.reset(new ArithmeticColumn(content));
}

// deprecated constructor. use function name as string
AggregateColumn::AggregateColumn(const std::string& functionName, ReturnedColumn* parm, const uint32_t sessionID):
	ReturnedColumn(sessionID),
	fFunctionName(functionName),
	fAggOp(NOOP),
	fAsc(false),
	fData(functionName + "(" + parm->data() + ")")
{
	fFunctionParms.reset(parm);
}

// deprecated constructor. use function name as string
AggregateColumn::AggregateColumn(const string& functionName, const string& content, const uint32_t sessionID):
	ReturnedColumn(sessionID),
	fFunctionName(functionName),
	fAggOp(NOOP),
	fAsc(false),
	fData(functionName + "(" + content + ")")
{
	// TODO: need to handle distinct
	fFunctionParms.reset(new ArithmeticColumn(content));
}

AggregateColumn::AggregateColumn( const AggregateColumn& rhs, const uint32_t sessionID ):
    ReturnedColumn(rhs, sessionID),
    fFunctionName (rhs.fFunctionName),
    fAggOp(rhs.fAggOp),
    fFunctionParms(rhs.fFunctionParms),
    fTableAlias(rhs.tableAlias()),
    fAsc(rhs.asc()),
    fData(rhs.data()),
    fConstCol(rhs.fConstCol)
{
	fAlias = rhs.alias();
}

/**
 * Methods
 */

const string AggregateColumn::toString() const
{
	ostringstream output;
	output << "AggregateColumn " << data() << endl;
	output << "func/distinct: " << (int)fAggOp << "/" << fDistinct << endl;
	output << "expressionId=" << fExpressionId << endl;
	if (fAlias.length() > 0) output << "/Alias: " << fAlias << endl;
	if (fFunctionParms == 0)
	    output << "No arguments" << endl;
	else
	    output << *fFunctionParms << endl;
	if (fConstCol)
	    output << *fConstCol;
	return output.str();
}

ostream& operator<<(ostream& output, const AggregateColumn& rhs)
{
	output << rhs.toString();
	return output;
}

void AggregateColumn::serialize(messageqcpp::ByteStream& b) const
{
	CalpontSelectExecutionPlan::ReturnedColumnList::const_iterator rcit;
	b << (uint8_t) ObjectReader::AGGREGATECOLUMN;
	ReturnedColumn::serialize(b);
	b << fFunctionName;
	b << static_cast<uint8_t>(fAggOp);
	if (fFunctionParms == 0)
		b << (uint8_t) ObjectReader::NULL_CLASS;
	else
		fFunctionParms->serialize(b);
	b << static_cast<uint32_t>(fGroupByColList.size());
	for (rcit = fGroupByColList.begin(); rcit != fGroupByColList.end(); ++rcit)
		(*rcit)->serialize(b);
	b << static_cast<uint32_t>(fProjectColList.size());
	for (rcit = fProjectColList.begin(); rcit != fProjectColList.end(); ++rcit)
		(*rcit)->serialize(b);
	b << fData;
	//b << fAlias;
	b << fTableAlias;
	b << static_cast<const ByteStream::doublebyte>(fAsc);
	if (fConstCol.get() == 0)
		b << (uint8_t) ObjectReader::NULL_CLASS;
	else
		fConstCol->serialize(b);
}

void AggregateColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::AGGREGATECOLUMN);
	fGroupByColList.erase(fGroupByColList.begin(), fGroupByColList.end());
	fProjectColList.erase(fProjectColList.begin(), fProjectColList.end());
	ReturnedColumn::unserialize(b);
	b >> fFunctionName;
	b >> fAggOp;
	//delete fFunctionParms;
	fFunctionParms.reset(
	    dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));

	messageqcpp::ByteStream::quadbyte size;
	messageqcpp::ByteStream::quadbyte i;
	ReturnedColumn *rc;

	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
        SRCP srcp(rc);
		fGroupByColList.push_back(srcp);
	}
	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
        SRCP srcp(rc);
		fProjectColList.push_back(srcp);
	}
	b >> fData;
	//b >> fAlias;
	b >> fTableAlias;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fAsc);
	fConstCol.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
}

bool AggregateColumn::operator==(const AggregateColumn& t) const
{
	const ReturnedColumn *rc1, *rc2;

	rc1 = static_cast<const ReturnedColumn*>(this);
	rc2 = static_cast<const ReturnedColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fFunctionName != t.fFunctionName)
		return false;
	if (fAggOp == COUNT_ASTERISK && t.fAggOp == COUNT_ASTERISK)
		return true;
	if (fAggOp != t.fAggOp)
		return false;
	if (fFunctionParms.get() != NULL && t.fFunctionParms.get() != NULL)
	{
		 if (*fFunctionParms.get() != t.fFunctionParms.get())
			return false;
	}
	else if (fFunctionParms.get() != NULL || t.fFunctionParms.get() != NULL)
		return false;
	//if (fAlias != t.fAlias)
	//	return false;
	if (fTableAlias != t.fTableAlias)
	    return false;
	if (fData != t.fData)
		return false;
	if (fAsc != t.fAsc)
	    return false;
	if ((fConstCol.get() != NULL && t.fConstCol.get() == NULL) ||
		(fConstCol.get() == NULL && t.fConstCol.get() != NULL) ||
		(fConstCol.get() != NULL && t.fConstCol.get() != NULL &&
		 *(fConstCol.get()) != t.fConstCol.get()))
		return false;
	return true;
}

bool AggregateColumn::operator==(const TreeNode* t) const
{
	const AggregateColumn *ac;

	ac = dynamic_cast<const AggregateColumn*>(t);
	if (ac == NULL)
		return false;
	return *this == *ac;
}

bool AggregateColumn::operator!=(const AggregateColumn& t) const
{
	return !(*this == t);
}

bool AggregateColumn::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

bool AggregateColumn::hasAggregate()
{
	fAggColumnList.push_back(this);
	return true;
}

void AggregateColumn::evaluate(Row& row, bool& isNull)
{
	switch (fResultType.colDataType)
	{
	case CalpontSystemCatalog::DATE:
		if (row.equals<4>(DATENULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getUintField<4>(fInputIndex);
		break;
	case CalpontSystemCatalog::DATETIME:
		if (row.equals<8>(DATETIMENULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getUintField<8>(fInputIndex);
		break;
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::STRINT:
		switch (row.getColumnWidth(fInputIndex))
		{
		case 1:
			if (row.equals<1>(CHAR1NULL, fInputIndex))
				isNull = true;
			else
				fResult.origIntVal = row.getUintField<1>(fInputIndex);
			break;
		case 2:
			if (row.equals<2>(CHAR2NULL, fInputIndex))
				isNull = true;
			else
				fResult.origIntVal = row.getUintField<2>(fInputIndex);
			break;
		case 3:
		case 4:
			if (row.equals<4>(CHAR4NULL, fInputIndex))
				isNull = true;
			else
				fResult.origIntVal = row.getUintField<4>(fInputIndex);
			break;
		case 5:
		case 6:
		case 7:
		case 8:
			if (row.equals<8>(CHAR8NULL, fInputIndex))
				isNull = true;
			else
				fResult.origIntVal = row.getUintField<8>(fInputIndex);
			break;
		default:
			if (row.equals(CPNULLSTRMARK, fInputIndex))
				isNull = true;
			else
				fResult.strVal = row.getStringField(fInputIndex);
			// stringColVal is padded with '\0' to colWidth so can't use str.length()
			if (strlen(fResult.strVal.c_str()) == 0)
				isNull = true;
			break;
		}
		if (fResultType.colDataType == CalpontSystemCatalog::STRINT)
			fResult.intVal = uint64ToStr(fResult.origIntVal);
		else
			fResult.intVal = atoll((char*)&fResult.origIntVal);
		break;
	case CalpontSystemCatalog::BIGINT:
		if (row.equals<8>(BIGINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getIntField<8>(fInputIndex);
		break;
	case CalpontSystemCatalog::UBIGINT:
		if (row.equals<8>(UBIGINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.uintVal = row.getUintField<8>(fInputIndex);
		break;
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::MEDINT:
		if (row.equals<4>(INTNULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getIntField<4>(fInputIndex);
		break;
	case CalpontSystemCatalog::UINT:
	case CalpontSystemCatalog::UMEDINT:
		if (row.equals<4>(UINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.uintVal = row.getUintField<4>(fInputIndex);
		break;
	case CalpontSystemCatalog::SMALLINT:
		if (row.equals<2>(SMALLINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getIntField<2>(fInputIndex);
		break;
	case CalpontSystemCatalog::USMALLINT:
		if (row.equals<2>(USMALLINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.uintVal = row.getUintField<2>(fInputIndex);
		break;
	case CalpontSystemCatalog::TINYINT:
		if (row.equals<1>(TINYINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getIntField<1>(fInputIndex);
		break;
	case CalpontSystemCatalog::UTINYINT:
		if (row.equals<1>(UTINYINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.uintVal = row.getUintField<1>(fInputIndex);
		break;
	//In this case, we're trying to load a double output column with float data. This is the
	// case when you do sum(floatcol), e.g.
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::UFLOAT:
		if (row.equals<4>(FLOATNULL, fInputIndex))
			isNull = true;
		else
			fResult.floatVal = row.getFloatField(fInputIndex);
		break;
	case CalpontSystemCatalog::DOUBLE:
	case CalpontSystemCatalog::UDOUBLE:
		if (row.equals<8>(DOUBLENULL, fInputIndex))
			isNull = true;
		else
			fResult.doubleVal = row.getDoubleField(fInputIndex);
		break;
	case CalpontSystemCatalog::DECIMAL:
	case CalpontSystemCatalog::UDECIMAL:
		switch (fResultType.colWidth)
		{
		case 1:
			if (row.equals<1>(TINYINTNULL, fInputIndex))
				isNull = true;
			else
			{
				fResult.decimalVal.value = row.getIntField<1>(fInputIndex);
				fResult.decimalVal.scale = (unsigned)fResultType.scale;
			}
			break;
		case 2:
			if (row.equals<2>(SMALLINTNULL, fInputIndex))
				isNull = true;
			else
			{
				fResult.decimalVal.value = row.getIntField<2>(fInputIndex);
				fResult.decimalVal.scale = (unsigned)fResultType.scale;
			}
			break;
		case 4:
			if (row.equals<4>(INTNULL, fInputIndex))
				isNull = true;
			else
			{
				fResult.decimalVal.value = row.getIntField<4>(fInputIndex);
				fResult.decimalVal.scale = (unsigned)fResultType.scale;
			}
			break;
		default:
			if (row.equals<8>(BIGINTNULL, fInputIndex))
				isNull = true;
			else
			{
				fResult.decimalVal.value = (int64_t)row.getUintField<8>(fInputIndex);
				fResult.decimalVal.scale = (unsigned)fResultType.scale;
			}
			break;
		}
		break;
	case CalpontSystemCatalog::VARBINARY:
		isNull = true;
		break;
	default:	// treat as int64
		if (row.equals<8>(BIGINTNULL, fInputIndex))
			isNull = true;
		else
			fResult.intVal = row.getUintField<8>(fInputIndex);
		break;
	}
}

/*static*/
AggregateColumn::AggOp AggregateColumn::agname2num(const string& agname)
{
/*
		NOOP = 0,
		COUNT_ASTERISK,
		COUNT,
		SUM,
		AVG,
		MIN,
		MAX,
		CONSTANT,
		DISTINCT_COUNT,
		DISTINCT_SUM,
		DISTINCT_AVG,
		STDDEV_POP,
		STDDEV_SAMP,
		VAR_POP,
		VAR_SAMP,
		BIT_AND,
		BIT_OR,
		BIT_XOR,
		GROUP_CONCAT
*/
	string lfn(agname);
	algorithm::to_lower(lfn);
	if (lfn == "count(*)")
		return COUNT_ASTERISK;
	if (lfn == "count")
		return COUNT;
	if (lfn == "sum")
		return SUM;
	if (lfn == "avg")
		return AVG;
	if (lfn == "min")
		return MIN;
	if (lfn == "max")
		return MAX;
	if (lfn == "std")
		return STDDEV_POP;
	if (lfn == "stddev_pop")
		return STDDEV_POP;
	if (lfn == "stddev_samp")
		return STDDEV_SAMP;
	if (lfn == "stddev")
		return STDDEV_POP;
	if (lfn == "var_pop")
		return VAR_POP;
	if (lfn == "var_samp")
		return VAR_SAMP;
	if (lfn == "variance")
		return VAR_POP;
	return NOOP;
}

} // namespace execplan
