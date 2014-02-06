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
*   $Id: windowfunctioncolumn.cpp 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/

#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost;

#include "bytestream.h"
#include "windowfunctioncolumn.h"
#include "constantcolumn.h"
#include "arithmeticcolumn.h"
#include "simplecolumn.h"
#include "objectreader.h"
#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "functioncolumn.h"

#include "funcexp.h"
#include "functor_export.h"
using namespace funcexp;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#ifdef _MSC_VER
#define strcasecmp stricmp
#endif

namespace execplan {
	
void getWindowFunctionCols(execplan::ParseTree* n, void* obj)
{
	vector<WindowFunctionColumn*>* list = reinterpret_cast< vector<WindowFunctionColumn*>*>(obj);
	TreeNode* tn = n->data();
	WindowFunctionColumn *afc = dynamic_cast<WindowFunctionColumn*>(tn);
	ArithmeticColumn *ac = dynamic_cast<ArithmeticColumn*>(tn);
	FunctionColumn *fc = dynamic_cast<FunctionColumn*>(tn);
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);

	if (afc)
		list->push_back(afc);
	else if (ac)
		list->insert(list->end(), ac->windowfunctionColumnList().begin(), ac->windowfunctionColumnList().end());
	else if (fc)
		list->insert(list->end(), fc->windowfunctionColumnList().begin(), fc->windowfunctionColumnList().end());
	else if (sf)
		list->insert(list->end(), sf->windowfunctionColumnList().begin(), sf->windowfunctionColumnList().end());
}

/**
 * WF_Boundary class methods definition 
 */
const std::string WF_Boundary::toString() const
{
	ostringstream output;

	if (fVal)
	{
		output << "val: ";
		output << fVal->toString() << endl;
	}
	if (fBound)
	{
		output << "bound exp: ";
		output << fBound->toString() << endl;
	}
	
	switch (fFrame)
	{
		case WF_PRECEDING:
			output << "PRECEDING";
			break;
		case WF_FOLLOWING:
			output << "FOLLOWING";
			break;
		case WF_UNBOUNDED_PRECEDING:
			output << "UNBOUNDED PRECEDING";
			break;
		case WF_UNBOUNDED_FOLLOWING:
			output << "UNBOUNDED FOLLOWING";
			break;
		case WF_CURRENT_ROW:
			output << "CURRENT ROW";
			break;
		default:
			output << "UNKNOWN";
	}
	return output.str();
}

void WF_Boundary::serialize(messageqcpp::ByteStream& b) const
{
	b << (uint8_t)fFrame;
	if (fVal)
		fVal->serialize(b);
	else
		b << (uint8_t) ObjectReader::NULL_CLASS;
	if (fBound)
		fBound->serialize(b);
	else
		b << (uint8_t) ObjectReader::NULL_CLASS;
}

void WF_Boundary::unserialize(messageqcpp::ByteStream& b)
{
	b >> (uint8_t&)fFrame;
	fVal.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
	fBound.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
}

/**
 * WF_Frame class methods definition 
 */
const string WF_Frame::toString() const
{
	ostringstream output;
	output << "WindowFrame:" << endl;
	output << "Start:" << endl;
	output << fStart.toString() << endl;
	output << "End:" << endl;
	output << fEnd.toString() << endl;
	return output.str();
}

void WF_Frame::serialize(messageqcpp::ByteStream& b) const
{
	fStart.serialize(b);
	fEnd.serialize(b);
	b << (uint8_t)fIsRange;
}

void WF_Frame::unserialize(messageqcpp::ByteStream& b)
{
	fStart.unserialize(b);
	fEnd.unserialize(b);
	b >> (uint8_t&)fIsRange;
}

/**
 * WF_OrderBy class methods definition 
 */

const string WF_OrderBy::toString() const
{
	ostringstream output;
	output << "order by: " << endl;
	for (uint i = 0; i < fOrders.size(); i++)
		output << fOrders[i]->toString() << endl;
	output << fFrame.toString();
	return output.str();
}

void WF_OrderBy::serialize(messageqcpp::ByteStream& b) const
{
	b << (uint32_t)fOrders.size();
	for (uint i = 0; i < fOrders.size(); i++)
		fOrders[i]->serialize(b);
	fFrame.serialize(b);
}

void WF_OrderBy::unserialize(messageqcpp::ByteStream& b)
{
	uint size;
	b >> (uint32_t&)size;
	SRCP srcp;
	for (uint i = 0; i < size; i++)
	{
		srcp.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
		fOrders.push_back(srcp);
	}
	fFrame.unserialize(b);
}

 /**
 * WindowFunctionColumn class definition 
 */

WindowFunctionColumn::WindowFunctionColumn()
{}

WindowFunctionColumn::WindowFunctionColumn(const string& functionName, const u_int32_t sessionID):
    ReturnedColumn(sessionID),
    fFunctionName(functionName)
{}

WindowFunctionColumn::WindowFunctionColumn( const WindowFunctionColumn& rhs, const u_int32_t sessionID):
    ReturnedColumn(rhs, sessionID),
    fFunctionName(rhs.functionName()),
    fFunctionParms(rhs.functionParms()),
    fPartitions (rhs.partitions()),
    fOrderBy (rhs.orderBy())
{}

const string WindowFunctionColumn::toString() const
{
	ostringstream output;
	output << "WindowFunctionColumn: " << fFunctionName;
	if (distinct())
		output << " DISTINCT";
	output << endl;
	output << "expressionId=" << fExpressionId << endl;
	output << "resultType=" << colDataTypeToString(fResultType.colDataType) << "|" << fResultType.colWidth << endl;
	output << "operationType=" << colDataTypeToString(fOperationType.colDataType) << endl;
	output << "function parm: " << endl;
	for (uint i = 0; i < fFunctionParms.size(); i++)
		output << fFunctionParms[i]->toString() << endl;
	output << "partition by: " << endl;
	for (uint i = 0; i < fPartitions.size(); i++)
		output << fPartitions[i]->toString() << endl;
	output << fOrderBy.toString() << endl;
	output << "getColumnList():" << endl;
	vector<SRCP> colList = getColumnList();
	for (uint i = 0; i < colList.size(); i++)
		output << colList[i]->toString() << endl;
	return output.str();
}

void WindowFunctionColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::WINDOWFUNCTIONCOLUMN;
	ReturnedColumn::serialize(b);
	b << fFunctionName;

	b << (uint32_t)fFunctionParms.size();
	for (uint i = 0; i < fFunctionParms.size(); i++)
		fFunctionParms[i]->serialize(b);
	b << (uint32_t)fPartitions.size();
	for (uint i = 0; i < fPartitions.size(); i++)
		fPartitions[i]->serialize(b);
	fOrderBy.serialize(b);
}

void WindowFunctionColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::WINDOWFUNCTIONCOLUMN);
	ReturnedColumn::unserialize(b);
	uint32_t size;
	SRCP srcp;

	fFunctionParms.clear();
	fPartitions.clear();

	b >> fFunctionName;
	b >> (uint32_t&)size;
	for (uint i = 0; i < size; i++)
	{
		srcp.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
		fFunctionParms.push_back(srcp);
	}
	b >> (uint32_t&)size;
	for (uint i = 0; i < size; i++) 
	{
		srcp.reset(dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b)));
		fPartitions.push_back(srcp);
	}
	fOrderBy.unserialize(b);
}

void WindowFunctionColumn::addToPartition(vector<SRCP>& groupByList)
{
	fPartitions.insert(fPartitions.end(), groupByList.begin(), groupByList.end());
}

vector<SRCP> WindowFunctionColumn::getColumnList() const
{
	vector<SRCP> columnList;
	columnList.insert(columnList.end(), fFunctionParms.begin(), fFunctionParms.end());
	columnList.insert(columnList.end(), fPartitions.begin(), fPartitions.end());
	columnList.insert(columnList.end(), fOrderBy.fOrders.begin(), fOrderBy.fOrders.end());
	if (fOrderBy.fFrame.fStart.fVal)
		columnList.push_back(fOrderBy.fFrame.fStart.fVal);
	if (fOrderBy.fFrame.fStart.fBound)
		columnList.push_back(fOrderBy.fFrame.fStart.fBound);
	if (fOrderBy.fFrame.fEnd.fVal)
		columnList.push_back(fOrderBy.fFrame.fEnd.fVal);
	if (fOrderBy.fFrame.fEnd.fBound)
		columnList.push_back(fOrderBy.fFrame.fEnd.fBound);
	return columnList;
}

bool WindowFunctionColumn::hasWindowFunc()
{
	fWindowFunctionColumnList.push_back(this);
	return true;
}

void WindowFunctionColumn::adjustResultType()
{
	if (fResultType.colDataType == CalpontSystemCatalog::DECIMAL &&
	    !boost::iequals(fFunctionName,"COUNT") &&
	    !boost::iequals(fFunctionName,"COUNT(*)") &&
	    !boost::iequals(fFunctionName,"ROW_NUMBER") &&
	    !boost::iequals(fFunctionName,"RANK") &&
	    !boost::iequals(fFunctionName,"PERCENT_RANK") &&
	    !boost::iequals(fFunctionName,"DENSE_RANK") &&
	    !boost::iequals(fFunctionName,"CUME_DIST") &&
	    !boost::iequals(fFunctionName,"NTILE") &&
	    !boost::iequals(fFunctionName,"PERCENTILE") &&
	    !fFunctionParms.empty() &&
	    fFunctionParms[0]->resultType().colDataType == CalpontSystemCatalog::DOUBLE)
	    fResultType = fFunctionParms[0]->resultType();
	    
	if ((boost::iequals(fFunctionName, "LEAD") ||
	     boost::iequals(fFunctionName, "LAG") ||
	     boost::iequals(fFunctionName, "MIN") ||
	     boost::iequals(fFunctionName, "MAX") ||
	     boost::iequals(fFunctionName, "FIRST_VALUE") ||
	     boost::iequals(fFunctionName, "LAST_VALUE") ||
	     boost::iequals(fFunctionName, "NTH_VALUE")) &&
	    !fFunctionParms.empty())
	    fResultType = fFunctionParms[0]->resultType();
}

void WindowFunctionColumn::evaluate(Row& row, bool& isNull)
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::DATE:
		{
			if (row.equals<4>(DATENULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DATETIME:
		{
			if (row.equals<8>(DATETIMENULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT:
		{
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
		}
		case CalpontSystemCatalog::BIGINT:
		{
			if (row.equals<8>(BIGINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::UBIGINT:
		{
			if (row.equals<8>(UBIGINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.uintVal = row.getUintField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::MEDINT:
		{
			if (row.equals<4>(INTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::UINT:
		case CalpontSystemCatalog::UMEDINT:
		{
			if (row.equals<4>(UINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.uintVal = row.getUintField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::SMALLINT:
		{
			if (row.equals<2>(SMALLINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<2>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::USMALLINT:
		{
			if (row.equals<2>(USMALLINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.uintVal = row.getUintField<2>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::TINYINT:
		{
			if (row.equals<1>(TINYINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<1>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::UTINYINT:
		{
			if (row.equals<1>(UTINYINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.uintVal = row.getUintField<1>(fInputIndex);
			break;
		}
		//In this case, we're trying to load a double output column with float data. This is the
		// case when you do sum(floatcol), e.g.
		case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
		{
			if (row.equals<4>(FLOATNULL, fInputIndex))
				isNull = true;
			else
				fResult.floatVal = row.getFloatField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		case CalpontSystemCatalog::UDOUBLE:
		{
			if (row.equals<8>(DOUBLENULL, fInputIndex))
				isNull = true;
			else
				fResult.doubleVal = row.getDoubleField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		case CalpontSystemCatalog::UDECIMAL:
		{
				switch (fResultType.colWidth)
				{
					case 1:
					{
						if (row.equals<1>(TINYINTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<1>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					case 2:
					{
						if (row.equals<2>(SMALLINTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<2>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					case 4:
					{
						if (row.equals<4>(INTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<4>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					default:
					{
						if (row.equals<8>(BIGINTNULL, fInputIndex))
						isNull = true;
						else
						{
							fResult.decimalVal.value = (int64_t)row.getUintField<8>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
				}
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
			isNull = true;
			break;
		default:	// treat as int64
		{
			if (row.equals<8>(BIGINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<8>(fInputIndex);
			break;
		}
	}
}


ostream& operator<<(ostream& output, const WindowFunctionColumn& rhs)
{
	output << rhs.toString();
	return output;
}

}   //namespace
