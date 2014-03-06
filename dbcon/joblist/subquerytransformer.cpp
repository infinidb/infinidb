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

//  $Id: subquerytransformer.cpp 6406 2010-03-26 19:18:37Z xlou $


#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost;

#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "jobstep.h"
#include "jlf_common.h"
#include "distributedenginecomm.h"
#include "expressionstep.h"
#include "tuplehashjoin.h"
#include "subquerystep.h"
#include "subquerytransformer.h"


namespace joblist
{

SubQueryTransformer::SubQueryTransformer(JobInfo* jobInfo, SErrorInfo& err) :
	fOutJobInfo(jobInfo), fSubJobInfo(NULL), fErrorInfo(err)
{
}


SubQueryTransformer::SubQueryTransformer(JobInfo* jobInfo, SErrorInfo& err, const string& alias) :
	fOutJobInfo(jobInfo), fSubJobInfo(NULL), fErrorInfo(err)
{
	fVtable.alias(algorithm::to_lower_copy(alias));
}


SubQueryTransformer::~SubQueryTransformer()
{
	// OK to delete NULL ptr
	delete fSubJobInfo;
	fSubJobInfo = NULL;
}


SJSTEP& SubQueryTransformer::makeSubQueryStep(execplan::CalpontSelectExecutionPlan* csep,
                                              bool subInFromClause)
{
	// Setup job info, job list and error status relation.
	fSubJobInfo = new JobInfo(fOutJobInfo->rm);
	fSubJobInfo->sessionId = fOutJobInfo->sessionId;
	fSubJobInfo->txnId = fOutJobInfo->txnId;
	fSubJobInfo->verId = fOutJobInfo->verId;
	fSubJobInfo->statementId = fOutJobInfo->statementId;
	fSubJobInfo->queryType = fOutJobInfo->queryType;
	fSubJobInfo->csc = fOutJobInfo->csc;
	fSubJobInfo->trace = fOutJobInfo->trace;
	fSubJobInfo->traceFlags = fOutJobInfo->traceFlags;
	fSubJobInfo->isExeMgr = fOutJobInfo->isExeMgr;
	fSubJobInfo->subLevel = fOutJobInfo->subLevel + 1;
	fSubJobInfo->keyInfo = fOutJobInfo->keyInfo;
	fSubJobInfo->stringScanThreshold = fOutJobInfo->stringScanThreshold;
	fSubJobInfo->tryTuples = true;
	fSubJobInfo->errorInfo = fErrorInfo;
	fOutJobInfo->subNum++;
	fSubJobInfo->subCount = fOutJobInfo->subCount;
	fSubJobInfo->subId = ++(*(fSubJobInfo->subCount));
	fSubJobInfo->pSubId = fOutJobInfo->subId;
	fSubJobList.reset(new TupleJobList(true));
	fSubJobList->priority(csep->priority());
	fSubJobInfo->projectingTableOID = fSubJobList->projectingTableOIDPtr();
	fSubJobInfo->jobListPtr = fSubJobList.get();
	fSubJobInfo->stringTableThreshold = fOutJobInfo->stringTableThreshold;
	fSubJobInfo->localQuery = fOutJobInfo->localQuery;
	fSubJobInfo->uuid = fOutJobInfo->uuid;
	fOutJobInfo->jobListPtr->addSubqueryJobList(fSubJobList);

	// Update v-table's alias.
	fVtable.name("$sub");
	if (fVtable.alias().empty())
	{
		ostringstream oss;
		oss << "$sub_"
			<< fSubJobInfo->subId
			<< "_" << fSubJobInfo->subLevel
			<< "_" << fOutJobInfo->subNum;
		fVtable.alias(oss.str());
	}

	// Make the jobsteps out of the execution plan.
	JobStepVector querySteps;
	JobStepVector projectSteps;
	DeliveredTableMap deliverySteps;
	if (csep->unionVec().size() == 0)
		makeJobSteps(csep, *fSubJobInfo, querySteps, projectSteps, deliverySteps);
	else if (subInFromClause)
		makeUnionJobSteps(csep, *fSubJobInfo, querySteps, projectSteps, deliverySteps);
	else
		throw IDBExcept(ERR_UNION_IN_SUBQUERY);

	if (fSubJobInfo->trace)
	{
		cout << (*csep) << endl;
		ostringstream oss;
		oss << boldStart
			<< "\nsubquery " << fSubJobInfo->subLevel << "." << fOutJobInfo->subNum << " steps:"
			<< boldStop << endl;
		ostream_iterator<JobStepVector::value_type> oIter(oss, "\n");
		copy(querySteps.begin(), querySteps.end(), oIter);
		cout << oss.str();
	}

	// Add steps to the joblist.
	fSubJobList->addQuery(querySteps);
	fSubJobList->addDelivery(deliverySteps);
	fSubJobList->putEngineComm(DistributedEngineComm::instance(fOutJobInfo->rm));

	// Get the correlated steps
	fCorrelatedSteps = fSubJobInfo->correlateSteps;
	fSubReturnedCols = fSubJobInfo->deliveredCols;

	// Convert subquery to step.
	SubQueryStep* sqs =
		new SubQueryStep(*fSubJobInfo);
	sqs->tableOid(fVtable.tableOid());
	sqs->alias(fVtable.alias());
	sqs->subJoblist(fSubJobList);
	sqs->setOutputRowGroup(fSubJobList->getOutputRowGroup());
	AnyDataListSPtr spdl(new AnyDataList());
	RowGroupDL* dl = new RowGroupDL(1, fSubJobInfo->fifoSize);
	spdl->rowGroupDL(dl);
	dl->OID(fVtable.tableOid());
	JobStepAssociation jsa;
	jsa.outAdd(spdl);
	(querySteps.back())->outputAssociation(jsa);
	sqs->outputAssociation(jsa);
	fSubQueryStep.reset(sqs);

	// Update the v-table columns and rowgroup
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	CalpontSystemCatalog::OID tblOid = fVtable.tableOid();
	string tableName = fVtable.name();
	string alias = fVtable.alias();
	const RowGroup& rg = fSubJobList->getOutputRowGroup();
	Row row;
	rg.initRow(&row);
	for (uint64_t i = 0; i < fSubReturnedCols.size(); i++)
	{
		fVtable.addColumn(fSubReturnedCols[i], fOutJobInfo->subView);

		// make sure the column type is the same as rowgroup
		CalpontSystemCatalog::ColType ct = fVtable.columnType(i);
		CalpontSystemCatalog::ColDataType colDataTypeInRg = row.getColTypes()[i];
		if (dynamic_cast<AggregateColumn*>(fSubReturnedCols[i].get()) != NULL ||
			dynamic_cast<WindowFunctionColumn*>(fSubReturnedCols[i].get()) != NULL)
		{
			// skip char/varchar/varbinary column because the colWidth in row is fudged.
			if (colDataTypeInRg != CalpontSystemCatalog::VARCHAR &&
				colDataTypeInRg != CalpontSystemCatalog::CHAR &&
				colDataTypeInRg != CalpontSystemCatalog::VARBINARY)
			{
				ct.colWidth = row.getColumnWidth(i);
				ct.colDataType = row.getColTypes()[i];
				ct.scale = row.getScale(i);
				if (ct.scale != 0 && ct.precision != -1)
					ct.colDataType = CalpontSystemCatalog::DECIMAL;
				ct.precision = row.getPrecision(i);
				fVtable.columnType(ct, i);
			}
		}
		// MySQL date/datetime type is different from IDB type
		else if (colDataTypeInRg == CalpontSystemCatalog::DATE ||
					colDataTypeInRg == CalpontSystemCatalog::DATETIME)
		{
			ct.colWidth = row.getColumnWidth(i);
			ct.colDataType = row.getColTypes()[i];
			ct.scale = row.getScale(i);
			ct.precision = row.getPrecision(i);
			fVtable.columnType(ct, i);
		}

		// build tuple info to export to outer query
		TupleInfo ti(setTupleInfo(fVtable.columnType(i), fVtable.columnOid(i), *fOutJobInfo,
									tblOid, fVtable.columns()[i].get(), alias));
		pos.push_back(pos.back() + ti.width);
		oids.push_back(ti.oid);
		keys.push_back(ti.key);
		types.push_back(ti.dtype);
		scale.push_back(ti.scale);
		precision.push_back(ti.precision);

		fOutJobInfo->vtableColTypes[UniqId(fVtable.columnOid(i), fVtable.alias(), "", "")] =
				fVtable.columnType(i);
	}

	RowGroup rg1(oids.size(),pos,oids,keys,types,scale,precision,csep->stringTableThreshold());
	rg1.setUseStringTable(rg.usesStringTable());

	dynamic_cast<SubQueryStep*>(fSubQueryStep.get())->setOutputRowGroup(rg1);

	return fSubQueryStep;
}


void SubQueryTransformer::checkCorrelateInfo(TupleHashJoinStep* thjs, const JobInfo& jobInfo)
{
	int pos = (thjs->correlatedSide() == 1) ? thjs->sequence2() : thjs->sequence1();

	if (pos == -1 || (size_t) pos >= fVtable.columns().size())
	{
		uint64_t id = (thjs->correlatedSide() == 1) ? thjs->tupleId2() : thjs->tupleId1();
		string alias = jobInfo.keyInfo->tupleKeyVec[id].fTable;
		string name = jobInfo.keyInfo->keyName[id];
		if (!name.empty() && alias.length() > 0)
			name = alias + "." + name;

		Message::Args args;
		args.add(name);
		string errMsg(IDBErrorInfo::instance()->errorMsg(ERR_CORRELATE_COL_MISSING, args));
		cerr << errMsg << ": " << pos << endl;
		throw IDBExcept(errMsg, ERR_CORRELATE_COL_MISSING);
	}
}


void SubQueryTransformer::updateCorrelateInfo()
{
	// put vtable into the table list to resolve correlated filters
	// Temp fix for @bug3932 until outer join has no dependency on table order.
	// Insert at [1], not to mess with OUTER join and hint(INFINIDB_ORDERED -- bug2317).
	fOutJobInfo->tableList.insert(fOutJobInfo->tableList.begin()+1, makeTableKey(
		*fOutJobInfo, fVtable.tableOid(), fVtable.name(), fVtable.alias(), "", ""));

	// tables in outer level
	set<uint32_t> outTables;
	for (uint64_t i = 0; i < fOutJobInfo->tableList.size(); i++)
		outTables.insert(fOutJobInfo->tableList[i]);

	// tables in subquery level
	set<uint32_t> subTables;
	for (uint64_t i = 0; i < fSubJobInfo->tableList.size(); i++)
		subTables.insert(fSubJobInfo->tableList[i]);

	// Update correlated steps
	const map<UniqId, uint32_t>& subMap = fVtable.columnMap();
	for (JobStepVector::iterator i = fCorrelatedSteps.begin(); i != fCorrelatedSteps.end(); i++)
	{
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
		ExpressionStep* es = dynamic_cast<ExpressionStep*>(i->get());
		if (thjs)
		{
			if (thjs->getJoinType() & CORRELATED)
			{
				checkCorrelateInfo(thjs, *fSubJobInfo);
				thjs->setJoinType(thjs->getJoinType() ^ CORRELATED);
				if (thjs->correlatedSide() == 1)
				{
					int pos = thjs->sequence2();
					thjs->tableOid2(fVtable.tableOid());
					thjs->oid2(fVtable.columnOid(pos));
					thjs->alias2(fVtable.alias());
					thjs->view2(fVtable.columns()[pos]->viewName());
					thjs->schema2(fVtable.columns()[pos]->schemaName());
					thjs->dictOid2(0);
					thjs->sequence2(-1);
					thjs->joinId(fOutJobInfo->joinNum++);

					CalpontSystemCatalog::ColType ct2 = fVtable.columnType(pos);
					TupleInfo ti(setTupleInfo(ct2, thjs->oid2(), *fOutJobInfo, thjs->tableOid2(),
						fVtable.columns()[pos].get(), thjs->alias2()));
					thjs->tupleId2(ti.key);
				}
				else
				{
					int pos = thjs->sequence1();
					thjs->tableOid1(fVtable.tableOid());
					thjs->oid1(fVtable.columnOid(pos));
					thjs->alias1(fVtable.alias());
					thjs->view1(fVtable.columns()[pos]->viewName());
					thjs->schema1(fVtable.columns()[pos]->schemaName());
					thjs->dictOid1(0);
					thjs->sequence1(-1);
					thjs->joinId(fOutJobInfo->joinNum++);

					CalpontSystemCatalog::ColType ct1 = fVtable.columnType(pos);
					TupleInfo ti(setTupleInfo(ct1, thjs->oid1(), *fOutJobInfo, thjs->tableOid1(),
						fVtable.columns()[pos].get(), thjs->alias1()));
					thjs->tupleId1(ti.key);
				}
			}
/*
			else // oid1 == 0, dictionary column
			{
				CalpontSystemCatalog::ColType dct;
				dct.colDataType = CalpontSystemCatalog::BIGINT;
				dct.colWidth = 8;
				dct.scale = 0;
				dct.precision = 0;
			}
*/
		}
		else if (es)
		{
			vector<ReturnedColumn*>& scList = es->columns();
			vector<CalpontSystemCatalog::OID>& tableOids = es->tableOids();
			vector<CalpontSystemCatalog::OID>& oids = es->oids();
			vector<string>& aliases = es->aliases();
			vector<string>& views = es->views();
			vector<string>& schemas = es->schemas();
			vector<uint32_t>& tableKeys = es->tableKeys();
			vector<uint32_t>& columnKeys = es->columnKeys();
			for (uint64_t j = 0; j < scList.size(); j++)
			{
				SimpleColumn* sc = dynamic_cast<SimpleColumn*>(scList[j]);
				if (sc != NULL)
				{
					if (subTables.find(tableKeys[j]) != subTables.end())
					{
						const map<UniqId, uint32_t>::const_iterator k =
							subMap.find(UniqId(oids[j], aliases[j], schemas[j], views[j]));
						if (k == subMap.end())
							//throw CorrelateFailExcept();
							throw IDBExcept(logging::ERR_NON_SUPPORT_SUB_QUERY_TYPE);

						sc->schemaName("");
						sc->tableName(fVtable.name());
						sc->tableAlias(fVtable.alias());
						sc->oid(fVtable.columnOid(k->second));
						sc->columnName(fVtable.columns()[k->second]->columnName());
						const CalpontSystemCatalog::ColType& ct = fVtable.columnType(k->second);
						TupleInfo ti = setTupleInfo(
							ct, sc->oid(), *fOutJobInfo, fVtable.tableOid(), sc, fVtable.alias());

						tableOids[j] = execplan::CNX_VTABLE_ID;
						oids[j] = sc->oid();
						aliases[j] = sc->tableAlias();
						views[j] = sc->viewName();
						schemas[j] = sc->schemaName();
						columnKeys[j] = ti.key;
						tableKeys[j] = getTableKey(*fOutJobInfo, ti.key);
					}
					else
					{
						if ((sc->joinInfo() & JOIN_CORRELATED) != 0)
						{
							sc->joinInfo(0);
						}

						const CalpontSystemCatalog::ColType&
							ct = fOutJobInfo->keyInfo->colType[columnKeys[j]];
						sc->joinInfo(0);
						TupleInfo ti = setTupleInfo(
							ct, sc->oid(), *fOutJobInfo, tableOids[j], sc, aliases[j]);

						columnKeys[j] = ti.key;
						tableKeys[j] = getTableKey(*fOutJobInfo, ti.key);
					}
				}
				else if (dynamic_cast<WindowFunctionColumn*>(scList[j]) != NULL)
				{
					// workaround for window function IN/EXISTS subquery
					const map<UniqId, uint32_t>::const_iterator k =
						subMap.find(UniqId(scList[j]->expressionId(), "", "", ""));
					if (k == subMap.end())
							throw IDBExcept(logging::ERR_NON_SUPPORT_SUB_QUERY_TYPE);

					sc = fVtable.columns()[k->second].get();
					es->substitute(j, fVtable.columns()[k->second]);
					CalpontSystemCatalog::ColType ct = sc->colType();
					string alias(extractTableAlias(sc));
					CalpontSystemCatalog::OID tblOid = fVtable.tableOid();
					TupleInfo ti(setTupleInfo(ct, sc->oid(), *fOutJobInfo, tblOid, sc, alias));

					tableOids[j] = execplan::CNX_VTABLE_ID;
					columnKeys[j] = ti.key;
					tableKeys[j] = getTableKey(*fOutJobInfo, ti.key);
				}
			}

			es->updateColumnOidAlias(*fOutJobInfo);
		}
		else
		{
			JobStep* j = i->get();
			uint32_t tid = -1;
			uint64_t cid = j->tupleId();
			if (cid != (uint64_t) -1)
				tid = getTableKey(*fOutJobInfo, cid);
			else
				tid = getTableKey(*fOutJobInfo, j);

			if (outTables.find(tid) == outTables.end())
			{
				if (subMap.find(
						UniqId(j->oid(), j->alias(), j->schema(), j->view(), 0)) != subMap.end())
					//throw CorrelateFailExcept();
					throw IDBExcept(logging::ERR_NON_SUPPORT_SUB_QUERY_TYPE);
			}
		}
	}
}


void SubQueryTransformer::run()
{
	// not to be called for base class
}


// ------ SimpleScalarTransformer ------
SimpleScalarTransformer::SimpleScalarTransformer(JobInfo* jobInfo, SErrorInfo& status, bool e) :
	SubQueryTransformer(jobInfo, status),
	fInputDl(NULL),
	fDlIterator(-1),
	fEmptyResultSet(true),
	fExistFilter(e)
{
}


SimpleScalarTransformer::SimpleScalarTransformer(const SubQueryTransformer& rhs) :
	SubQueryTransformer(rhs.outJobInfo(), rhs.errorInfo()),
	fInputDl(NULL),
	fDlIterator(-1),
	fEmptyResultSet(true),
	fExistFilter(false)
{
	fSubJobList = rhs.subJobList();
	fSubQueryStep = rhs.subQueryStep();
}


SimpleScalarTransformer::~SimpleScalarTransformer()
{
}


void SimpleScalarTransformer::run()
{
	// set up receiver
	fRowGroup = dynamic_cast<SubQueryStep*>(fSubQueryStep.get())->getOutputRowGroup();
	fRowGroup.initRow(&fRow, true);
	fInputDl = fSubQueryStep->outputAssociation().outAt(0)->rowGroupDL();
	fDlIterator = fInputDl->getIterator();

	// run the subquery
	fSubJobList->doQuery();

	// retrieve the scalar result
	getScalarResult();

	// check result count
	if (fErrorInfo->errCode == ERR_MORE_THAN_1_ROW)
		throw MoreThan1RowExcept();
}


void SimpleScalarTransformer::getScalarResult()
{
	RGData rgData;
	bool more;

    more = fInputDl->next(fDlIterator, &rgData);
	while (more)
	{
		fRowGroup.setData(&rgData);

		// Only need one row for scalar filter
		if (fEmptyResultSet && fRowGroup.getRowCount() == 1)
		{
			fEmptyResultSet = false;
			Row row;
			fRowGroup.initRow(&row);
			fRowGroup.getRow(0, &row);
			fRowData.reset(new uint8_t[fRow.getSize()]);
			fRow.setData(fRowData.get());
			copyRow(row, &fRow);

			// For exist filter, stop the query after one or more rows retrieved.
			if (fExistFilter)
			{
				fErrorInfo->errMsg = IDBErrorInfo::instance()->errorMsg(ERR_MORE_THAN_1_ROW);
				fErrorInfo->errCode = ERR_MORE_THAN_1_ROW;
			}
		}

		// more than 1 row case:
		//   not empty set, and get new rows
		//   empty set, but get more than 1 rows
		else if (fRowGroup.getRowCount() > 0)
		{
			// Stop the query after some rows retrieved.
			fEmptyResultSet = false;
			fErrorInfo->errMsg = IDBErrorInfo::instance()->errorMsg(ERR_MORE_THAN_1_ROW);
			fErrorInfo->errCode = ERR_MORE_THAN_1_ROW;
		}

		// For scalar filter, have to check all blocks to ensure only one row.
		if (fErrorInfo->errCode != 0)
			 while (more) more = fInputDl->next(fDlIterator, &rgData);
		else
	    	more = fInputDl->next(fDlIterator, &rgData);
	}
}


}
// vim:ts=4 sw=4:

