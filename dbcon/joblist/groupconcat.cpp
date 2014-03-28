/* Copyright (C) 2013 Calpont Corp.

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

//  $Id: groupconcat.cpp 9705 2013-07-17 20:06:07Z pleblanc $


#include <iostream>
//#define NDEBUG
#include <cassert>
#include <string>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "rowcolumn.h"
#include "groupconcatcolumn.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "rowgroup.h"
#include "rowaggregation.h"
using namespace rowgroup;

#include "dataconvert.h"
using namespace dataconvert;

#define GROUPCONCAT_DLLEXPORT
#include "groupconcat.h"
#undef GROUPCONCAT_DLLEXPORT

#include "idborderby.h"
using namespace ordering;

#include "jobstep.h"
#include "jlf_common.h"
#include "limitedorderby.h"

namespace joblist
{


// GroupConcatInfo class implementation
GroupConcatInfo::GroupConcatInfo()
{
}


GroupConcatInfo::~GroupConcatInfo()
{
}


void GroupConcatInfo::prepGroupConcat(JobInfo& jobInfo)
{
	RetColsVector::iterator i = jobInfo.groupConcatCols.begin();
	while (i != jobInfo.groupConcatCols.end())
	{
		GroupConcatColumn* gcc = dynamic_cast<GroupConcatColumn*>(i->get());
		const RowColumn* rcp = dynamic_cast<const RowColumn*>(gcc->functionParms().get());

		SP_GroupConcat groupConcat(new GroupConcat);
		groupConcat->fSeparator = gcc->separator();
		groupConcat->fDistinct = gcc->distinct();
		groupConcat->fSize = gcc->resultType().colWidth;
		groupConcat->fRm = &(jobInfo.rm);

		int key = -1;
		const vector<SRCP>& cols = rcp->columnVec();
		for (uint64_t j = 0, k = 0; j < cols.size(); j++)
		{
			const ConstantColumn* cc = dynamic_cast<const ConstantColumn*>(cols[j].get());
			if (cc == NULL)
			{
				key = getColumnKey(cols[j], jobInfo);
				fColumns.insert(key);
				groupConcat->fGroupCols.push_back(make_pair(key, k++));
			}
			else
			{
				groupConcat->fConstCols.push_back(make_pair(cc->constval(), j));
			}
		}

		vector<SRCP>& orderCols = gcc->orderCols();
		for (vector<SRCP>::iterator k = orderCols.begin(); k != orderCols.end(); k++)
		{
			if (dynamic_cast<const ConstantColumn*>(k->get()) != NULL)
				continue;

			key = getColumnKey(*k, jobInfo);
			fColumns.insert(key);
			groupConcat->fOrderCols.push_back(make_pair(key, k->get()->asc()));
		}

		fGroupConcat.push_back(groupConcat);

		i++;
	}

	// Rare case: all columns in group_concat are constant columns, use a column in column map.
	if (jobInfo.groupConcatCols.size() > 0 && fColumns.size() == 0)
	{
		int key = -1;
		for (vector<uint32_t>::iterator i = jobInfo.tableList.begin();
				i != jobInfo.tableList.end() && key == -1;
				i++)
		{
			if (jobInfo.columnMap[*i].size() > 0)
			{
				key = *(jobInfo.columnMap[*i].begin());
			}
		}

		if (key != -1)
		{
			fColumns.insert(key);
		}
		else
		{
			throw runtime_error("Empty column map.");
		}
	}
}


uint GroupConcatInfo::getColumnKey(const SRCP& srcp, JobInfo& jobInfo)
{
	int colKey = -1;
	const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp.get());
	if (sc != NULL)
	{
		if (sc->schemaName().empty())
		{
			// bug3839, handle columns from subquery.
			SimpleColumn tmp(*sc, jobInfo.sessionId);
			tmp.oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
			colKey = getTupleKey(jobInfo, &tmp);
		}
		else
		{
			colKey = getTupleKey(jobInfo, sc);
		}

		// check if this is a dictionary column
		if (jobInfo.keyInfo->dictKeyMap.find(colKey) != jobInfo.keyInfo->dictKeyMap.end())
			colKey = jobInfo.keyInfo->dictKeyMap[colKey];
	}
	else
	{
		const ArithmeticColumn* ac = dynamic_cast<const ArithmeticColumn*>(srcp.get());
		const FunctionColumn* fc = dynamic_cast<const FunctionColumn*>(srcp.get());
		if (ac != NULL || fc != NULL)
		{
			colKey = getExpTupleKey(jobInfo, srcp->expressionId());
		}
		else
		{
			cerr << "Unsupported GROUP_CONCAT column. " << srcp->toString() << endl;
			throw runtime_error("Unsupported GROUP_CONCAT column.");
		}
	}

	return colKey;
}


void GroupConcatInfo::mapColumns(const RowGroup& projRG)
{
	map<uint, uint> projColumnMap;
	const vector<uint>& keysProj = projRG.getKeys();
	for (uint64_t i = 0; i < projRG.getColumnCount(); i++)
		projColumnMap[keysProj[i]] = i;

	for (vector<SP_GroupConcat>::iterator k = fGroupConcat.begin(); k!= fGroupConcat.end(); k++)
	{
		vector<uint> pos;
		vector<uint> oids;
		vector<uint> keys;
		vector<uint> scale;
		vector<uint> precision;
		vector<CalpontSystemCatalog::ColDataType> types;
		pos.push_back(2);

		vector<pair<uint, uint> >::iterator i1 = (*k)->fGroupCols.begin();
		while (i1 != (*k)->fGroupCols.end())
		{
			map<uint, uint>::iterator j = projColumnMap.find(i1->first);
			if(j == projColumnMap.end())
			{
				cerr << "Concat Key:" << i1->first << " is not projected." << endl;
				throw runtime_error("Project error.");
			}

			pos.push_back(pos.back() + projRG.getColumnWidth(j->second));
			oids.push_back(projRG.getOIDs()[j->second]);
			keys.push_back(projRG.getKeys()[j->second]);
			types.push_back(projRG.getColTypes()[j->second]);
			scale.push_back(projRG.getScale()[j->second]);
			precision.push_back(projRG.getPrecision()[j->second]);

			i1++;
		}

		vector<pair<uint, bool> >::iterator i2 = (*k)->fOrderCols.begin();
		while (i2 != (*k)->fOrderCols.end())
		{
			map<uint, uint>::iterator j = projColumnMap.find(i2->first);
			if(j == projColumnMap.end())
			{
				cerr << "Order Key:" << i2->first << " is not projected." << endl;
				throw runtime_error("Project error.");
			}

			vector<uint>::iterator i3 = find(keys.begin(), keys.end(), j->first);
			int idx = 0;
			if (i3 == keys.end())
			{
				idx = keys.size();

				pos.push_back(pos.back() + projRG.getColumnWidth(j->second));
				oids.push_back(projRG.getOIDs()[j->second]);
				keys.push_back(projRG.getKeys()[j->second]);
				types.push_back(projRG.getColTypes()[j->second]);
				scale.push_back(projRG.getScale()[j->second]);
				precision.push_back(projRG.getPrecision()[j->second]);
			}
			else
			{
				idx = distance(keys.begin(), i3);
			}

			(*k)->fOrderCond.push_back(make_pair(idx, i2->second));

			i2++;
		}

		(*k)->fRowGroup = RowGroup(oids.size(), pos, oids, keys, types, scale, precision, projRG.getStringTableThreshold(), false);
		(*k)->fMapping = makeMapping(projRG, (*k)->fRowGroup);
	}
}


shared_array<int> GroupConcatInfo::makeMapping(const RowGroup& in, const RowGroup& out)
{
	// For some reason using the rowgroup mapping fcns don't work completely right in this class
	shared_array<int> mapping(new int[out.getColumnCount()]);

	for (uint64_t i = 0; i < out.getColumnCount(); i++)
	{
		for (uint64_t j = 0; j < in.getColumnCount(); j++)
		{
			if ((out.getKeys()[i] == in.getKeys()[j]))
			{
				mapping[i] = j;
				break;
			}
		}
	}

	return mapping;
}


const string GroupConcatInfo::toString() const
{
	ostringstream oss;
	oss << "GroupConcatInfo: toString() to be implemented.";
	oss << endl;

	return oss.str();
}


GroupConcatAgUM::GroupConcatAgUM(rowgroup::SP_GroupConcat& gcc) : GroupConcatAg(gcc)
{
	initialize();
}

GroupConcatAgUM::~GroupConcatAgUM()
{
}


void GroupConcatAgUM::initialize()
{
	if (fGroupConcat->fDistinct || fGroupConcat->fOrderCols.size() > 0)
		fConcator.reset(new GroupConcatOrderBy());
	else
		fConcator.reset(new GroupConcatNoOrder());

	fConcator->initialize(fGroupConcat);

	fGroupConcat->fRowGroup.initRow(&fRow, true);
	fData.reset(new uint8_t[fRow.getSize()]);
	fRow.setData(fData.get());
}


void GroupConcatAgUM::processRow(const rowgroup::Row& inRow)
{
	applyMapping(fGroupConcat->fMapping, inRow);
	fConcator->processRow(fRow);
}


void GroupConcatAgUM::merge(const rowgroup::Row& inRow, int64_t i)
{
	uint8_t* data = inRow.getData();
	joblist::GroupConcatAgUM* gccAg = *((joblist::GroupConcatAgUM**)(data + inRow.getOffset(i)));

	fConcator->merge(gccAg->concator().get());
//	don't reset
//	gccAg->orderBy().reset();
}


void GroupConcatAgUM::getResult(uint8_t* buff)
{
	fConcator->getResult(buff, fGroupConcat->fSeparator);
}

uint8_t * GroupConcatAgUM::getResult()
{
	return fConcator->getResult(fGroupConcat->fSeparator);
}


void GroupConcatAgUM::applyMapping(const boost::shared_array<int>& mapping, const Row& row)
{
	// For some reason the rowgroup mapping fcns don't work right in this class.
	for (uint64_t i = 0; i < fRow.getColumnCount(); i++)
	{
		if (fRow.getColumnWidth(i) > 8 &&
			(fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::CHAR ||
			 fRow.getColTypes()[i] == execplan::CalpontSystemCatalog::VARCHAR))
		{
			fRow.setStringField(row.getStringPointer(mapping[i]), row.getStringLength(mapping[i]), i);
		}
		else
		{
			fRow.setIntField(row.getIntField(mapping[i]), i);
		}
	}
}


// GroupConcator class implementation
GroupConcator::GroupConcator() : fCurrentLength(0), fGroupConcatLen(0), fConstantLen(0)
{
}


GroupConcator::~GroupConcator()
{
}


void GroupConcator::initialize(const rowgroup::SP_GroupConcat& gcc)
{
	fGroupConcatLen = gcc->fSize;
	fCurrentLength -= strlen(gcc->fSeparator.c_str());

	fOutputString.reset(new uint8_t[fGroupConcatLen+2]);
	memset(fOutputString.get(), 0, fGroupConcatLen+2);
	
	fConstCols = gcc->fConstCols;
	fConstantLen = strlen(gcc->fSeparator.c_str());
	for (uint64_t i = 0; i < fConstCols.size(); i++)
		fConstantLen += strlen(fConstCols[i].first.c_str());
}


void GroupConcator::outputRow(std::ostringstream& oss, const rowgroup::Row& row)
{
	const CalpontSystemCatalog::ColDataType* types = row.getColTypes();
	vector<uint32_t>::iterator i = fConcatColumns.begin();
	vector<pair<string, uint> >::iterator j = fConstCols.begin();

	uint64_t groupColCount = fConcatColumns.size() + fConstCols.size();

	for (uint64_t k = 0; k < groupColCount; k++)
	{
		if (j != fConstCols.end() &&  k == j->second)
		{
			oss << j->first;
			j++;
			continue;
		}

		switch (types[*i])
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
			{
				int64_t intVal = row.getIntField(*i);
				int scale = (int) row.getScale(*i);
				if (scale == 0)
				{
					oss << intVal;
				}
				else
				{
					long double dblVal = intVal / pow(10.0, (double)scale);
					oss << fixed << setprecision(scale) << dblVal;
				}
				break;
			}
			case CalpontSystemCatalog::UTINYINT:
			case CalpontSystemCatalog::USMALLINT:
			case CalpontSystemCatalog::UMEDINT:
			case CalpontSystemCatalog::UINT:
			case CalpontSystemCatalog::UBIGINT:
			{
				uint64_t uintVal = row.getUintField(*i);
				int scale = (int) row.getScale(*i);
				if (scale == 0)
				{
					oss << uintVal;
				}
				else
				{
					long double dblVal = uintVal / pow(10.0, (double)scale);
					oss << fixed << setprecision(scale) << dblVal;
				}
				break;
			}
            case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				oss << row.getStringField(*i).c_str();
				//oss << row.getStringField(*i);
				break;
			}
			case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
			{
				oss << setprecision(15) << row.getDoubleField(*i);
				break;
			}
			case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
			{
				oss << row.getFloatField(*i);
				break;
			}
			case CalpontSystemCatalog::DATE:
			{
				oss << DataConvert::dateToString(row.getUintField(*i));
				break;
			}
			case CalpontSystemCatalog::DATETIME:
			{
				oss << DataConvert::datetimeToString(row.getUintField(*i));
				break;
			}
			default:
			{
				break;
			}
		}

		i++;
	}
}


bool GroupConcator::concatColIsNull(const rowgroup::Row& row)
{
	bool ret = false;

	for (vector<uint32_t>::iterator i = fConcatColumns.begin(); i != fConcatColumns.end(); i++)
	{
		if (row.isNullValue(*i))
		{
			ret = true;
			break;
		}
	}

	return ret;
}


int64_t GroupConcator::lengthEstimate(const rowgroup::Row& row)
{
	int64_t rowLen = fConstantLen;  // fixed constant and separator length
	const CalpontSystemCatalog::ColDataType* types = row.getColTypes();

	// null values are already skipped.
	for (vector<uint32_t>::iterator i = fConcatColumns.begin(); i != fConcatColumns.end(); i++)
	{
		if (row.isNullValue(*i))
			continue;

		int64_t fieldLen = 0;
		switch (types[*i])
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			{
				int64_t v = row.getIntField(*i);
				if (v < 0) fieldLen++;
				while ((v /=10) != 0) fieldLen++;
				fieldLen += 1;
				break;
			}
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            {
                uint64_t v = row.getUintField(*i);
                while ((v /=10) != 0) fieldLen++;
                fieldLen += 1;
                break;
            }
			case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
			{
				int64_t v = row.getIntField(*i);
				double scale = row.getScale(*i);
				if (scale > 0)
				{
					v /= (int64_t) pow(10.0, scale);
					if (v < 0) fieldLen++;
					while ((v /=10) != 0) fieldLen++;
					fieldLen += (int64_t) scale + 2;;
				}
				else
				{
					if (v < 0) fieldLen++;
					while ((v /=10) != 0) fieldLen++;
					fieldLen += 1;;
				}
				break;
			}
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				int64_t colWidth = row.getStringLength(*i);
				fieldLen += colWidth;   // getStringLength() does the same thing as below
				//assert(!row.usesStringTable());
				//int64_t colWidth = row.getColumnWidth(*i);
				//uint8_t* pStr = row.getData() + row.getOffset(*i);
				//while ((*pStr++ > 0) && (fieldLen < colWidth))
				//	fieldLen++;
				break;
			}
			case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
			{
				fieldLen = 1; // minimum length
				break;
			}
			case CalpontSystemCatalog::DATE:
			{
				fieldLen = 10; // YYYY-MM-DD
				break;
			}
			case CalpontSystemCatalog::DATETIME:
			{
				fieldLen = 19; // YYYY-MM-DD HH24:MI:SS
				break;
			}
			default:
			{
				break;
			}
		}

		rowLen += fieldLen;
	}

	return rowLen;
}


const string GroupConcator::toString() const
{
	ostringstream oss;
	oss << "GroupConcat size-" << fGroupConcatLen;
	oss << "Concat   cols: ";
	vector<uint32_t>::const_iterator i = fConcatColumns.begin();
	vector<pair<string, uint> >::const_iterator j = fConstCols.begin();
	uint64_t groupColCount = fConcatColumns.size() + fConstCols.size();
	for (uint64_t k = 0; k < groupColCount; k++)
	{
		if (j != fConstCols.end() &&  k == j->second)
		{
			oss << 'c' << " ";
			j++;
		}
		else
		{
			oss << (*i) << " ";	
			i++;
		}
	}
	oss << endl;

	return oss.str();
}


// GroupConcatOrderBy class implementation
GroupConcatOrderBy::GroupConcatOrderBy()
{
	fRule.fIdbCompare = this;
}


GroupConcatOrderBy::~GroupConcatOrderBy()
{
}


void GroupConcatOrderBy::initialize(const rowgroup::SP_GroupConcat& gcc)
{
	GroupConcator::initialize(gcc);

	fOrderByCond.resize(0);
	for (uint64_t i = 0; i < gcc->fOrderCond.size(); i++)
		fOrderByCond.push_back(IdbSortSpec(gcc->fOrderCond[i].first, gcc->fOrderCond[i].second));

	fDistinct = gcc->fDistinct;
	fRowsPerRG = 128;
	fErrorCode = ERR_AGGREGATION_TOO_BIG;
	fRm = gcc->fRm;

	vector<std::pair<uint, uint> >::iterator i = gcc->fGroupCols.begin();
	while (i != gcc->fGroupCols.end())
		fConcatColumns.push_back((*(i++)).second);

	IdbOrderBy::initialize(gcc->fRowGroup);
}


uint64_t GroupConcatOrderBy::getKeyLength() const
{
		// only distinct the concatenated columns
	//return (fRow0.getOffset(fConcatColumns.size()) - 2);
	return fConcatColumns.size()-1;   // cols 0 to fConcatColumns will be conpared
}


void GroupConcatOrderBy::processRow(const rowgroup::Row& row)
{
	// check if this is a distinct row
	if (fDistinct && fDistinctMap->find(row.getPointer()) != fDistinctMap->end())
		return;

	// this row is skipped if any concatenated column is null.
	if (concatColIsNull(row))
		return;

	// if the row count is less than the limit
	if (fCurrentLength < fGroupConcatLen)
	{
		copyRow(row, &fRow0);
		//cout << "length < GB limit: " << fRow0.toString() << endl;
		//memcpy(fRow0.getData(), row.getData(), row.getSize());
		// the RID is no meaning here, use it to store the estimated length.
		int16_t estLen = lengthEstimate(fRow0);
		fRow0.setRid(estLen);
		OrderByRow newRow(fRow0, fRule);
		fOrderByQueue.push(newRow);
		fCurrentLength += estLen;

		// add to the distinct map
		if (fDistinct)
			fDistinctMap->insert(fRow0.getPointer());

		fRowGroup.incRowCount();
		fRow0.nextRow();

		if (fRowGroup.getRowCount() >= fRowsPerRG)
		{
			fDataQueue.push(fData);

			uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
			if (!fRm->getMemory(newSize))
			{
				cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
					 << " @" << __FILE__ << ":" << __LINE__;
				throw IDBExcept(fErrorCode);
			}
			fMemSize += newSize;

			fData.reinit(fRowGroup, fRowsPerRG);
			fRowGroup.setData(&fData);
			fRowGroup.resetRowGroup(0);
			fRowGroup.getRow(0, &fRow0);
		}
	}

	else if (fOrderByCond.size() > 0 && fRule.less(row.getPointer(), fOrderByQueue.top().fData))
	{
		OrderByRow swapRow = fOrderByQueue.top();
		fRow1.setData(swapRow.fData);
		fCurrentLength -= fRow1.getRelRid();
		fRow2.setData(swapRow.fData);
		
		if (!fDistinct)
		{
			copyRow(row, &fRow1);
		}
		else
		{
			// only the copyRow does useful work here
			fDistinctMap->erase(swapRow.fData);
			copyRow(row, &fRow2);
			fDistinctMap->insert(swapRow.fData);
		}

		int16_t estLen = lengthEstimate(fRow2);
		fRow2.setRid(estLen);
		fCurrentLength += estLen;

		fOrderByQueue.pop();
		fOrderByQueue.push(swapRow);
	}
}


void GroupConcatOrderBy::merge(GroupConcator* gc)
{
	GroupConcatOrderBy* go = dynamic_cast<GroupConcatOrderBy*>(gc);
	while (go->fOrderByQueue.empty() == false)
	{
		const OrderByRow& row = go->fOrderByQueue.top();
		
		// check if the distinct row already exists
		if (fDistinct && fDistinctMap->find(row.fData) != fDistinctMap->end())
		{
			; // no op;
		}

		// if the row count is less than the limit
		else if (fCurrentLength < fGroupConcatLen)
		{
			fOrderByQueue.push(row);
			row1.setData(row.fData);
			fCurrentLength += row1.getRelRid();

			// add to the distinct map
			if (fDistinct)
				fDistinctMap->insert(row.fData);
				//fDistinctMap->insert(make_pair((row.fData+2), row.fData));
		}

		else if (fOrderByCond.size() > 0 && fRule.less(row.fData, fOrderByQueue.top().fData))
		{
			OrderByRow swapRow = fOrderByQueue.top();
			row1.setData(swapRow.fData);
			fCurrentLength -= row1.getRelRid();
			
			if (fDistinct)
			{
				fDistinctMap->erase(swapRow.fData);
				fDistinctMap->insert(row.fData);
				//fDistinctMap->erase(fDistinctMap->find(swapRow.fData + 2));
				//fDistinctMap->insert(make_pair((row.fData+2), row.fData));
			}

			row1.setData(row.fData);
			fCurrentLength += row1.getRelRid();

			fOrderByQueue.pop();
			fOrderByQueue.push(row);
		}

		go->fOrderByQueue.pop();
	}
}


void GroupConcatOrderBy::getResult(uint8_t* buff, const string &sep)
{
#if 1
	ostringstream oss;
	bool addSep = false;

	// need to reverse the order
	stack<OrderByRow> rowStack;
	while (fOrderByQueue.size() > 0)
	{
		rowStack.push(fOrderByQueue.top());
		fOrderByQueue.pop();
	}

	while (rowStack.size() > 0)
	{
		if (addSep)
			oss << sep;
		else
			addSep = true;

		const OrderByRow& topRow = rowStack.top();
		fRow0.setData(topRow.fData);
		outputRow(oss, fRow0);
		rowStack.pop();
	}

	strncpy((char*) buff, oss.str().c_str(), fGroupConcatLen);
#else
	ostringstream oss;
	bool addSep = false;
	priority_queue<OrderByRow>::reverse_iterator rit;
	
	for (rit = fOrderByQueue.rbegin(); rit != fOrderByQueue.rend(); ++rit) {
		if (addSep)
			oss << sep;
		else
			addSep = true;
		
		const OrderByRow &topRow = *rit;
		fRow0.setData(topRow.fData);
		outputRow(oss, fRow0);
	}
	fOrderByQueue.clear();
	
	strncpy((char *) buff, oss.str().c_str(), fGroupConcatLen);
#endif
}

uint8_t * GroupConcator::getResult(const string &sep)
{
	getResult(fOutputString.get(), sep);
	return fOutputString.get();
}

const string GroupConcatOrderBy::toString() const
{
	string baseStr = GroupConcator::toString();

	ostringstream oss;
	oss << "OrderBy   cols: ";
	vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();
	for (; i != fOrderByCond.end(); i++)
		oss << "(" << i->fIndex << ","
			<< ((i->fAsc)?"Asc":"Desc") << ","
			<< ((i->fNf)?"null first":"null last") << ") ";
	if (fDistinct)
		oss << endl << " distinct";
	oss << endl;

	return (baseStr + oss.str());
}


// GroupConcatNoOrder class implementation
GroupConcatNoOrder::GroupConcatNoOrder() :
    fRowsPerRG(128), fErrorCode(ERR_AGGREGATION_TOO_BIG), fMemSize(0), fRm(NULL)
{
}


GroupConcatNoOrder::~GroupConcatNoOrder()
{
	if (fRm)
		fRm->returnMemory(fMemSize);
}


void GroupConcatNoOrder::initialize(const rowgroup::SP_GroupConcat& gcc)
{
	GroupConcator::initialize(gcc);

	fRowGroup = gcc->fRowGroup;
	fRowsPerRG = 128;
	fErrorCode = ERR_AGGREGATION_TOO_BIG;
	fRm = gcc->fRm;

	vector<std::pair<uint, uint> >::iterator i = gcc->fGroupCols.begin();
	while (i != gcc->fGroupCols.end())
		fConcatColumns.push_back((*(i++)).second);

	uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();
	if (!fRm->getMemory(newSize))
	{
		cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
			 << " @" << __FILE__ << ":" << __LINE__;
		throw IDBExcept(fErrorCode);
	}

	fMemSize += newSize;
	//fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
	fData.reinit(fRowGroup, fRowsPerRG);
	fRowGroup.setData(&fData);
	fRowGroup.resetRowGroup(0);
	fRowGroup.initRow(&fRow);
	fRowGroup.getRow(0, &fRow);
}


void GroupConcatNoOrder::processRow(const rowgroup::Row& row)
{
	// if the row count is less than the limit
	if (fCurrentLength < fGroupConcatLen && concatColIsNull(row) == false)
	{
		copyRow(row, &fRow);
		//memcpy(fRow.getData(), row.getData(), row.getSize());

		// the RID is no meaning here, use it to store the estimated length.
		int16_t estLen = lengthEstimate(fRow);
		fRow.setRid(estLen);
		fCurrentLength += estLen;
		fRowGroup.incRowCount();
		fRow.nextRow();

		if (fRowGroup.getRowCount() >= fRowsPerRG)
		{
			uint64_t newSize = fRowsPerRG * fRowGroup.getRowSize();

			if (!fRm->getMemory(newSize))
			{
				cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
					 << " @" << __FILE__ << ":" << __LINE__;
				throw IDBExcept(fErrorCode);
			}
			fMemSize += newSize;

			fDataQueue.push(fData);
			fData.reinit(fRowGroup, fRowsPerRG);
			//fData.reset(new uint8_t[fRowGroup.getDataSize(fRowsPerRG)]);
			fRowGroup.setData(&fData);
			fRowGroup.resetRowGroup(0);
			fRowGroup.getRow(0, &fRow);
		}
	}
}


void GroupConcatNoOrder::merge(GroupConcator* gc)
{
	GroupConcatNoOrder* in = dynamic_cast<GroupConcatNoOrder*>(gc);
	while (in->fDataQueue.size() > 0)
	{
		fDataQueue.push(in->fDataQueue.front());
		in->fDataQueue.pop();
	}

	fDataQueue.push(in->fData);
	fMemSize += in->fMemSize;
	in->fMemSize = 0;
}


void GroupConcatNoOrder::getResult(uint8_t* buff, const string &sep)
{
	ostringstream oss;
	bool addSep = false;

	fDataQueue.push(fData);
	while (fDataQueue.size() > 0)
	{
		fRowGroup.setData(&fDataQueue.front());
		fRowGroup.getRow(0, &fRow);
		for (uint64_t i = 0; i < fRowGroup.getRowCount(); i++)
		{
			if (addSep)
				oss << sep;
			else
				addSep = true;

			outputRow(oss, fRow);
			fRow.nextRow();
		}

		fDataQueue.pop();
	}

	strncpy((char*) buff, oss.str().c_str(), fGroupConcatLen);
}


const string GroupConcatNoOrder::toString() const
{
	return GroupConcator::toString();
}


}
// vim:ts=4 sw=4:

