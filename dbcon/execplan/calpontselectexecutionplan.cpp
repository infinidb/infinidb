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
*   $Id: calpontselectexecutionplan.cpp 9576 2013-05-29 21:02:11Z zzhu $
*
*
***********************************************************************/
#include <iostream>
#include <algorithm>
using namespace std;

#include <boost/uuid/uuid_io.hpp>

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "objectreader.h"
#include "filter.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"
#include "querystats.h"

#include "querytele.h"
using namespace querytele;

namespace {

template<class T> struct deleter : public unary_function<T&, void>
{
	void operator()(T& x) { delete x; x = 0; }
};

}

namespace execplan
{

/** Static */
CalpontSelectExecutionPlan::ColumnMap CalpontSelectExecutionPlan::fColMap;

/**
 * Constructors/Destructors
 */
CalpontSelectExecutionPlan::CalpontSelectExecutionPlan(const int location):
							fLocalQuery (GLOBAL_QUERY),
							fFilters (0),
							fHaving (0),
							fLocation (location),
							fDependent (false),
							fTxnID(-1),
							fTraceFlags(TRACE_NONE),
							fStatementID(0),
							fDistinct(false),
							fOverrideLargeSideEstimate(false),
							fDistinctUnionNum(0),
							fSubType(MAIN_SELECT),
							fLimitStart(0),
							fLimitNum(-1),
							fHasOrderBy(false),
							fStringScanThreshold(ULONG_MAX),
							fQueryType(SELECT),
							fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL),
							fStringTableThreshold(20),
							fDJSSmallSideLimit(0),
							fDJSLargeSideLimit(0),
							fDJSPartitionSize(100 * 1024 * 1024),  // 100MB mem usage for disk based join,
							fUMMemLimit(numeric_limits<int64_t>::max()),
							fIsDML(false)
{
	fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::CalpontSelectExecutionPlan(
							   const ReturnedColumnList& returnedCols,
							   ParseTree* filters,
							   const SelectList& subSelects,
							   const GroupByColumnList& groupByCols,
							   ParseTree* having,
							   const OrderByColumnList& orderByCols,
							   const string alias,
							   const int location,
							   const bool dependent) :
							 fLocalQuery (GLOBAL_QUERY),
							 fReturnedCols (returnedCols),
							 fFilters (filters),
							 fSubSelects (subSelects),
							 fGroupByCols (groupByCols),
							 fHaving (having),
							 fOrderByCols (orderByCols),
							 fTableAlias (alias),
							 fLocation (location),
							 fDependent (dependent),
							 fTxnID(-1),
							 fTraceFlags(TRACE_NONE),
							 fStatementID(0),
							 fDistinct(false),
							 fOverrideLargeSideEstimate(false),
							 fDistinctUnionNum(0),
							 fSubType(MAIN_SELECT),
							 fLimitStart(0),
							 fLimitNum(-1),
							 fHasOrderBy(false),
							 fStringScanThreshold(ULONG_MAX),
							 fQueryType(SELECT),
							 fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL),
							 fStringTableThreshold(20),
							 fDJSSmallSideLimit(0),
							 fDJSLargeSideLimit(0),
							 fDJSPartitionSize(100 * 1024 * 1024),  // 100MB mem usage for disk based join
							 fUMMemLimit(numeric_limits<int64_t>::max()),
							 fIsDML(false)
{
	fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::CalpontSelectExecutionPlan (string data) :
							 fLocalQuery (GLOBAL_QUERY),
							 fData(data),
							 fTxnID(-1),
							 fTraceFlags(TRACE_NONE),
							 fStatementID(0),
							 fDistinct(false),
							 fOverrideLargeSideEstimate(false),
							 fDistinctUnionNum(0),
							 fSubType(MAIN_SELECT),
							 fLimitStart(0),
							 fLimitNum(-1),
							 fHasOrderBy(false),
							 fStringScanThreshold(ULONG_MAX),
							 fQueryType(SELECT),
							 fPriority(querystats::DEFAULT_USER_PRIORITY_LEVEL),
							 fStringTableThreshold(20),
							 fDJSSmallSideLimit(0),
 							 fDJSLargeSideLimit(0),
 							 fDJSPartitionSize(100 * 1024 * 1024),  // 100MB mem usage for disk based join
							 fUMMemLimit(numeric_limits<int64_t>::max()),
 							 fIsDML(false)
{
	fUuid = QueryTeleClient::genUUID();
}

CalpontSelectExecutionPlan::~CalpontSelectExecutionPlan()
{
	if (fFilters != NULL)
		delete fFilters;
	if (fHaving != NULL)
		delete fHaving;
	fFilters = NULL;
	fHaving = NULL;
}

/**
 * Methods
 */

void CalpontSelectExecutionPlan::filterTokenList( FilterTokenList& filterTokenList)
{
	fFilterTokenList = filterTokenList;

	Parser parser;
	std::vector<Token> tokens;
	Token t;

	for (unsigned int i = 0; i < filterTokenList.size(); i++)
	{
		t.value = filterTokenList[i];
		tokens.push_back(t);
	}
	if (tokens.size() > 0)
		filters(parser.parse(tokens.begin(), tokens.end()));
}

void CalpontSelectExecutionPlan::havingTokenList( const FilterTokenList& havingTokenList)
{
	fHavingTokenList = havingTokenList;

	Parser parser;
	std::vector<Token> tokens;
	Token t;

	for (unsigned int i = 0; i < havingTokenList.size(); i++)
	{
		t.value = havingTokenList[i];
		tokens.push_back(t);
	}
	if (tokens.size() > 0)
		having(parser.parse(tokens.begin(), tokens.end()));
}

string CalpontSelectExecutionPlan::toString() const
{
	ostringstream output;

	output << ">SELECT " ;
	if (distinct())
		output << "DISTINCT ";
	output << "limit: " << limitStart() << " - " << limitNum() << endl;

	switch (location())
	{
		case CalpontSelectExecutionPlan::MAIN:
		output << "MAIN" << endl;
		break;
	case CalpontSelectExecutionPlan::FROM:
		output << "FROM" << endl;
		break;
	case CalpontSelectExecutionPlan::WHERE:
		output << "WHERE" << endl;
		break;
	case CalpontSelectExecutionPlan::HAVING:
		output << "HAVING" << endl;
		break;
	}

	// Returned Column
	CalpontSelectExecutionPlan::ReturnedColumnList retCols = returnedCols();
	output << ">>Returned Columns" << endl;
	uint32_t seq = 0;
	for (unsigned int i = 0; i < retCols.size(); i++)
	{
		output << *retCols[i] << endl;
		if (retCols[i]->colSource() & SELECT_SUB)
		{
			output << "select sub -- " << endl;
			CalpontSelectExecutionPlan *plan = dynamic_cast<CalpontSelectExecutionPlan*>(fSelectSubList[seq++].get());
			if (plan)
				output << "{" << *plan << "}" << endl;
		}
	}

	// From Clause
	CalpontSelectExecutionPlan::TableList tables = tableList();
	output << ">>From Tables" << endl;
	seq = 0;
	for (unsigned int i = 0; i < tables.size(); i++)
	{
		// derived table
		if (tables[i].schema.length() == 0 && tables[i].table.length() == 0)
		{
			output << "derived table - " << tables[i].alias << endl;
			CalpontSelectExecutionPlan *plan = dynamic_cast<CalpontSelectExecutionPlan*>(fDerivedTableList[seq++].get());
			if (plan)
				output << "{" << *plan << "}" << endl;
		}
		else
		{
			output << tables[i] << endl;
		}
	}

	// Filters
	output << ">>Filters" << endl;
	if (filters() != 0)
		filters()->walk (ParseTree::print, output);
	else
		output << "empty filter tree" << endl;

	// Group by columns
	const CalpontSelectExecutionPlan::GroupByColumnList& gbc = groupByCols();
	if (gbc.size() > 0)
	{
	output << ">>Group By Columns" << endl;
	for (unsigned int i = 0; i < gbc.size(); i++)
		output << *gbc[i] << endl;
	}

	// Having
	if (having() != 0)
	{
		output << ">>Having" << endl;
		having()->walk (ParseTree::print, output);
	}

	// Order by columns
	const CalpontSelectExecutionPlan::OrderByColumnList& obc = orderByCols();
	if (obc.size() > 0)
	{
		output << ">>Order By Columns" << endl;
		for (unsigned int i = 0; i < obc.size(); i++)
			output << *obc[i] << endl;
	}
	output << "SessionID: " << fSessionID << endl;
	output << "TxnID: " << fTxnID << endl;
	output << "VerID: " << fVerID << endl;
	output << "TraceFlags: " << fTraceFlags << endl;
	output << "StatementID: " << fStatementID << endl;
	output << "DistUnionNum: " << (int)fDistinctUnionNum << endl;
	output << "Limit: " << fLimitStart << " - " << fLimitNum << endl;
	output << "String table threshold: " << fStringTableThreshold << endl;

	output << "--- Column Map ---" << endl;
	CalpontSelectExecutionPlan::ColumnMap::const_iterator iter;
	for (iter = columnMap().begin(); iter != columnMap().end(); iter++)
		output << (*iter).first << " : " << (*iter).second << endl;
	output << "UUID: " << fUuid << endl;
	output << "QueryType: " << queryType() << endl;

	if (!unionVec().empty())
		output << "\n--- Union Unit ---" << endl;
	for (unsigned i = 0; i < unionVec().size(); i++)
	{
		CalpontSelectExecutionPlan *plan =
			 dynamic_cast<CalpontSelectExecutionPlan*>(unionVec()[i].get());
		if (plan)
			output << "{" << *plan << "}\n" << endl;
	}

	return output.str();
}

string CalpontSelectExecutionPlan::queryTypeToString(const uint32_t queryType)
{
	switch (queryType)
	{
		case SELECT:
			return "SELECT";
		case UPDATE:
			return "UPDATE";
		case DELETE:
			return "DELETE";
		case INSERT_SELECT:
			return "INSERT_SELECT";
		case CREATE_TABLE:
			return "CREATE_TABLE";
		case DROP_TABLE:
			return "DROP_TABLE";
		case ALTER_TABLE:
			return "ALTER_TABLE";
		case INSERT:
			return "INSERT";
		case LOAD_DATA_INFILE:
			return "LOAD_DATA_INFILE";
	}
	return "UNKNOWN";
}

void CalpontSelectExecutionPlan::serialize(messageqcpp::ByteStream& b) const
{
	ReturnedColumnList::const_iterator rcit;
	vector<ReturnedColumn*>::const_iterator it;
	ColumnMap::const_iterator mapiter;
	TableList::const_iterator tit;

	b << static_cast<ObjectReader::id_t>(ObjectReader::CALPONTSELECTEXECUTIONPLAN);

	b << static_cast<uint32_t>(fReturnedCols.size());
	for (rcit = fReturnedCols.begin(); rcit != fReturnedCols.end(); ++rcit)
		(*rcit)->serialize(b);

	b << static_cast<uint32_t>(fTableList.size());
	for (tit = fTableList.begin(); tit != fTableList.end(); ++tit)
	{
		(*tit).serialize(b);
	}

	ObjectReader::writeParseTree(fFilters, b);

	b << static_cast<uint32_t>(fSubSelects.size());
	for (uint32_t i = 0; i < fSubSelects.size(); i++)
		fSubSelects[i]->serialize(b);

	b << static_cast<uint32_t>(fGroupByCols.size());
	for (rcit = fGroupByCols.begin(); rcit != fGroupByCols.end(); ++rcit)
		(*rcit)->serialize(b);

	ObjectReader::writeParseTree(fHaving, b);

	b << static_cast<uint32_t>(fOrderByCols.size());
	for (rcit = fOrderByCols.begin(); rcit != fOrderByCols.end(); ++rcit)
		(*rcit)->serialize(b);

	b << static_cast<uint32_t>(fColumnMap.size());
	for (mapiter = fColumnMap.begin(); mapiter != fColumnMap.end(); ++mapiter)
	{
		b << (*mapiter).first;
		(*mapiter).second->serialize(b);
	}

	b << static_cast<uint32_t>(frmParms.size());
	for (RMParmVec::const_iterator it = frmParms.begin(); it != frmParms.end(); ++it)
	{
		b << it->sessionId;
		b << it->id;
			b << it->value;
	}

	b << fTableAlias;
	b << static_cast<uint32_t>(fLocation);

	b << static_cast< ByteStream::byte>(fDependent);

	// ? not sure if this needs to be added
	b << fData;
	b << static_cast<uint32_t>(fSessionID);
	b << static_cast<uint32_t>(fTxnID);
	b << fVerID;
	b << fTraceFlags;
	b << fStatementID;
	b << static_cast<const ByteStream::byte>(fDistinct);
	b << static_cast<uint8_t>(fOverrideLargeSideEstimate);

	// for union
	b << (uint8_t)fDistinctUnionNum;
	b << (uint32_t)fUnionVec.size();
	for (uint32_t i = 0; i < fUnionVec.size(); i++)
		fUnionVec[i]->serialize(b);

	b << (uint64_t)fSubType;

	// for FROM subquery
	b << static_cast<uint32_t>(fDerivedTableList.size());
	for (uint32_t i = 0; i < fDerivedTableList.size(); i++)
		fDerivedTableList[i]->serialize(b);

	b << (uint64_t)fLimitStart;
	b << (uint64_t)fLimitNum;
	b << static_cast<const ByteStream::byte>(fHasOrderBy);

	b << static_cast<uint32_t>(fSelectSubList.size());
	for (uint32_t i = 0; i < fSelectSubList.size(); i++)
		fSelectSubList[i]->serialize(b);

	b << (uint64_t)fStringScanThreshold;
	b << (uint32_t)fQueryType;
	b << fPriority;
	b << fStringTableThreshold;
	b << fSchemaName;
	b << fLocalQuery;
	b << fUuid;
	b << fDJSSmallSideLimit;
	b << fDJSLargeSideLimit;
	b << fDJSPartitionSize;
	b << fUMMemLimit;
	b << (uint8_t) fIsDML;
}

void CalpontSelectExecutionPlan::unserialize(messageqcpp::ByteStream& b)
{
	ReturnedColumn *rc;
	CalpontExecutionPlan *cep;
	string colName;
	uint8_t tmp8;

	ObjectReader::checkType(b, ObjectReader::CALPONTSELECTEXECUTIONPLAN);

	// erase elements, otherwise vectors contain null pointers
	fReturnedCols.clear();
	fSubSelects.clear();
	fGroupByCols.clear();
	fOrderByCols.clear();
	fTableList.clear();
	fColumnMap.clear();
	fUnionVec.clear();
	frmParms.clear();
	fDerivedTableList.clear();
	fSelectSubList.clear();

	if (fFilters != 0) {
		delete fFilters;
		fFilters = 0;
	}
	if (fHaving != 0) {
		delete fHaving;
		fHaving = 0;
	}

	messageqcpp::ByteStream::quadbyte size;
	messageqcpp::ByteStream::quadbyte i;

	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
			SRCP srcp(rc);
		fReturnedCols.push_back(srcp);
	}

	b >> size;
	CalpontSystemCatalog::TableAliasName tan;
	for (i = 0; i < size; i++)
	{
		tan.unserialize(b);
		fTableList.push_back(tan);
	}

	fFilters = ObjectReader::createParseTree(b);

	b >> size;
	for (i = 0; i < size; i++) {
		cep = ObjectReader::createExecutionPlan(b);
		fSubSelects.push_back(SCEP(cep));
	}

	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
		SRCP srcp(rc);
		fGroupByCols.push_back(srcp);
	}

	fHaving = ObjectReader::createParseTree(b);

	b >> size;
	for (i = 0; i < size; i++) {
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
		SRCP srcp(rc);
		fOrderByCols.push_back(srcp);
	}

	b >> size;
	for (i = 0; i < size; i++) {
		b >> colName;
		rc = dynamic_cast<ReturnedColumn*>(ObjectReader::createTreeNode(b));
		SRCP srcp(rc);
		fColumnMap.insert(ColumnMap::value_type(colName, srcp));
	}

	b >> size;
	messageqcpp::ByteStream::doublebyte id;
	messageqcpp::ByteStream::quadbyte sessionId;
	messageqcpp::ByteStream::octbyte memory;
	for (i = 0; i < size; i++) {
		b >> sessionId;
		b >> id;
		b >> memory;
		frmParms.push_back(RMParam(sessionId, id, memory));
	}

	b >> fTableAlias;
	b >> reinterpret_cast<uint32_t&>(fLocation);
	b >> reinterpret_cast< ByteStream::byte&>(fDependent);

	// ? not sure if this needs to be added
	b >> fData;
	b >> reinterpret_cast<uint32_t&>(fSessionID);
	b >> reinterpret_cast<uint32_t&>(fTxnID);
	b >> fVerID;
	b >> fTraceFlags;
	b >> fStatementID;
	b >> reinterpret_cast< ByteStream::byte&>(fDistinct);
	uint8_t val;
	b >> reinterpret_cast<uint8_t&>(val);
	fOverrideLargeSideEstimate = (val != 0);

	// for union
	b >> (uint8_t&)(fDistinctUnionNum);
	b >> size;
	for (i = 0; i < size; i++)
	{
		cep = ObjectReader::createExecutionPlan(b);
		fUnionVec.push_back(SCEP(cep));
	}
	b >> (uint64_t&)fSubType;

	// for FROM subquery
	b >> size;
	for (i = 0; i < size; i++)
	{
		cep = ObjectReader::createExecutionPlan(b);
		fDerivedTableList.push_back(SCEP(cep));
	}

	b >> (uint64_t&)fLimitStart;
	b >> (uint64_t&)fLimitNum;
	b >> reinterpret_cast< ByteStream::byte&>(fHasOrderBy);

	// for SELECT subquery
	b >> size;
	for (i = 0; i < size; i++)
	{
		cep = ObjectReader::createExecutionPlan(b);
		fSelectSubList.push_back(SCEP(cep));
	}

	b >> (uint64_t&)fStringScanThreshold;
	b >> (uint32_t&)fQueryType;
	b >> fPriority;
	b >> fStringTableThreshold;
	b >> fSchemaName;
	b >> fLocalQuery;
	b >> fUuid;
	b >> fDJSSmallSideLimit;
	b >> fDJSLargeSideLimit;
	b >> fDJSPartitionSize;
	b >> fUMMemLimit;
	b >> tmp8;
	fIsDML = tmp8;
}

bool CalpontSelectExecutionPlan::operator==(const CalpontSelectExecutionPlan& t) const
{

	// If we use this outside the serialization tests, we should
	// reorder these comparisons to speed up the common case

	ReturnedColumnList::const_iterator rcit;
	ReturnedColumnList::const_iterator rcit2;
	vector<ReturnedColumn*>::const_iterator it, it2;
	SelectList::const_iterator sit, sit2;
	ColumnMap::const_iterator map_it, map_it2;

	//fReturnedCols
	if (fReturnedCols.size() != t.fReturnedCols.size())
		return false;
	for	(rcit = fReturnedCols.begin(), rcit2 = t.fReturnedCols.begin();
		rcit != fReturnedCols.end(); ++rcit, ++rcit2)
			if (**rcit != **rcit2)
				return false;

	//fFilters
	if (fFilters != NULL && t.fFilters != NULL) {
		if (*fFilters != *t.fFilters)
			return false;
	}
	else if (fFilters != NULL || t.fFilters != NULL)
		return false;

	//fSubSelects
	if (fSubSelects.size() != t.fSubSelects.size())
		return false;
	for	(sit = fSubSelects.begin(), sit2 = t.fSubSelects.begin();
			sit != fSubSelects.end(); ++sit, ++sit2)
		if (*((*sit).get()) != (*sit2).get())
			return false;

	//fGroupByCols
	if (fGroupByCols.size() != t.fGroupByCols.size())
		return false;
	for	(rcit = fGroupByCols.begin(), rcit2 = t.fGroupByCols.begin();
			rcit != fGroupByCols.end(); ++rcit, ++rcit2)
		if (**rcit != **rcit2)
			return false;

	//fHaving
	if (fHaving != NULL && t.fHaving != NULL) {
		if (*fHaving != *t.fHaving)
			return false;
	}
	else if (fHaving != NULL || t.fHaving != NULL)
		return false;

	//fOrderByCols
	if (fOrderByCols.size() != t.fOrderByCols.size())
		return false;
	for	(rcit = fOrderByCols.begin(), rcit2 = t.fOrderByCols.begin();
			rcit != fOrderByCols.end(); ++rcit, ++rcit2)
		if (**rcit != **rcit2)
			return false;

	//fColumnMap
	if (fColumnMap.size() != t.fColumnMap.size())
		return false;
	for	(map_it = fColumnMap.begin(), map_it2 = t.fColumnMap.begin();
			map_it != fColumnMap.end(); ++map_it, ++map_it2)
		if (*(map_it->second) != *(map_it2->second))
			return false;

	if (fTableAlias != t.fTableAlias)
		return false;
	if (fLocation != t.fLocation)
		return false;
	if (fDependent != t.fDependent)
		return false;
	// Trace flags don't affect equivalency?
	//if (fTraceFlags != t.fTraceFlags) return false;
	if (fStatementID != t.fStatementID)
		return false;
	if (fSubType != t.fSubType)
		return false;
	if (fPriority != t.fPriority)
		return false;
	if (fStringTableThreshold != t.fStringTableThreshold)
		return false;
	if (fDJSSmallSideLimit != t.fDJSSmallSideLimit)
		return false;
	if (fDJSLargeSideLimit != t.fDJSLargeSideLimit)
		return false;
	if (fDJSPartitionSize != t.fDJSPartitionSize)
		return false;
	if (fUMMemLimit != t.fUMMemLimit)
		return false;

	return true;
}

bool CalpontSelectExecutionPlan::operator==(const CalpontExecutionPlan* t) const
{
	const CalpontSelectExecutionPlan *ac;

	ac = dynamic_cast<const CalpontSelectExecutionPlan*>(t);
	if (ac == NULL)
		return false;
	return *this == *ac;
}

bool CalpontSelectExecutionPlan::operator!=(const CalpontSelectExecutionPlan& t) const
{
	return !(*this == t);
}

bool CalpontSelectExecutionPlan::operator!=(const CalpontExecutionPlan* t) const
{
	return !(*this == t);
}

void CalpontSelectExecutionPlan::columnMap (const ColumnMap& columnMap)
{
	ColumnMap::const_iterator map_it1, map_it2;
	fColumnMap.erase(fColumnMap.begin(), fColumnMap.end());

	SRCP srcp;
	for (map_it2 = columnMap.begin(); map_it2 != columnMap.end(); ++map_it2)
	{
		srcp.reset(map_it2->second->clone());
		fColumnMap.insert(ColumnMap::value_type(map_it2->first, srcp));
	}
}

void CalpontSelectExecutionPlan::rmParms (const RMParmVec& parms)
{

	frmParms.clear();
	frmParms.assign(parms.begin(), parms.end());
}


} // namespace execplan
