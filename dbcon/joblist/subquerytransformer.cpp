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
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "jobstep.h"
#include "jlf_common.h"
#include "expressionstep.h"
#include "tuplehashjoin.h"
#include "subquerystep.h"
#include "subquerytransformer.h"


namespace joblist
{

SubQueryTransformer::SubQueryTransformer(JobInfo* jobInfo, SErrorInfo& status) :
	fJobInfo(jobInfo), fStatus(status)
{
}


SubQueryTransformer::SubQueryTransformer(JobInfo* jobInfo, SErrorInfo& status, const string& alias) :
	fJobInfo(jobInfo), fStatus(status)
{
	fVtable.alias(algorithm::to_lower_copy(alias)); 
}


SubQueryTransformer::~SubQueryTransformer()
{
}


SJSTEP& SubQueryTransformer::makeSubQueryStep(execplan::CalpontSelectExecutionPlan* csep,
                                              bool subInFromClause)
{
	// Setup job info, job list and error status relation.
	JobInfo jobInfo(fJobInfo->rm);
	jobInfo.sessionId = fJobInfo->sessionId;
	jobInfo.txnId = fJobInfo->txnId;
	jobInfo.verId = fJobInfo->verId;
	jobInfo.statementId = fJobInfo->statementId;
	jobInfo.csc = fJobInfo->csc;
	jobInfo.trace = fJobInfo->trace;
	jobInfo.traceFlags = fJobInfo->traceFlags;
	jobInfo.isExeMgr = fJobInfo->isExeMgr;
	jobInfo.subLevel = fJobInfo->subLevel + 1;
	jobInfo.keyInfo = fJobInfo->keyInfo;
	jobInfo.stringScanThreshold = fJobInfo->stringScanThreshold;
	jobInfo.tryTuples = true;
	jobInfo.status = fStatus;
	fJobInfo->subNum++;
	jobInfo.subCount = fJobInfo->subCount;
	jobInfo.subId = ++(*(jobInfo.subCount));
	jobInfo.pSubId = fJobInfo->subId;
	fSubJobList.reset(new TupleJobList(true));
	jobInfo.projectingTableOID = fSubJobList->projectingTableOIDPtr();
	jobInfo.jobListPtr = fSubJobList.get();
	fJobInfo->jobListPtr->addSubqueryJobList(fSubJobList);

	// Update v-table's alias.
	fVtable.name("$sub");
	if (fVtable.alias().empty())
	{
		ostringstream oss;
		oss << "$sub_" << jobInfo.subId << "_" << jobInfo.subLevel << "_" << fJobInfo->subNum;
		fVtable.alias(oss.str()); 
	}

	// Make the jobsteps out of the execution plan.
	JobStepVector querySteps;
	JobStepVector projectSteps;
	DeliveredTableMap deliverySteps;
	if (csep->unionVec().size() == 0)
		makeJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
	else if (subInFromClause)
		makeUnionJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
	else
		throw IDBExcept(ERR_UNION_IN_SUBQUERY);

    if (jobInfo.trace)
	{
		ostringstream oss;
		oss << boldStart
			<< "\nsubquery " << jobInfo.subLevel << "." << fJobInfo->subNum << " steps:"
			<< boldStop << endl;
		ostream_iterator<JobStepVector::value_type> oIter(oss, "\n");
		copy(querySteps.begin(), querySteps.end(), oIter);
		cout << oss.str();
	}

	// Add steps to the joblist.
	fSubJobList->addQuery(querySteps);
	fSubJobList->addDelivery(deliverySteps);
	fSubJobList->putEngineComm(DistributedEngineComm::instance(fJobInfo->rm));

	// Get the correlated steps
	fCorrelatedSteps = jobInfo.correlateSteps;
	fSubReturnedCols = jobInfo.deliveredCols;

	// Convert subquery to step.
	SubQueryStep* sqs = new SubQueryStep(jobInfo.sessionId, jobInfo.txnId, jobInfo.statementId);
	sqs->tableOid(fVtable.tableOid());
	sqs->alias(fVtable.alias());
	sqs->subJoblist(fSubJobList);
	sqs->setOutputRowGroup(fSubJobList->getOutputRowGroup());
	AnyDataListSPtr spdl(new AnyDataList());
	RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
	spdl->rowGroupDL(dl);
	dl->OID(fVtable.tableOid());
	JobStepAssociation jsa(jobInfo.status);
	jsa.outAdd(spdl);
	(querySteps.back())->outputAssociation(jsa);
	sqs->outputAssociation(jsa);
	fSubQueryStep.reset(sqs);

	// Update the v-table columns and rowgroup
	vector<uint> pos;
	vector<uint> oids;
	vector<uint> keys;
	vector<uint> scale;
	vector<uint> precision;
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
		fVtable.addColumn(fSubReturnedCols[i], fJobInfo->subView);

		// make sure the column type is the same as rowgroup
		CalpontSystemCatalog::ColType ct = fVtable.columnType(i);
		CalpontSystemCatalog::ColDataType colDataTypeInRg = row.getColTypes()[i];
		if (dynamic_cast<AggregateColumn*>(fSubReturnedCols[i].get()) != NULL)
		{
			// skip char/varchar/varbinary column because the colWidth in row is fudged.
			if (colDataTypeInRg != CalpontSystemCatalog::VARCHAR &&
				colDataTypeInRg != CalpontSystemCatalog::CHAR &&
				colDataTypeInRg != CalpontSystemCatalog::VARBINARY)
			{
				ct.colWidth = row.getColumnWidth(i);
				ct.colDataType = row.getColTypes()[i];
				ct.scale = row.getScale(i);
				if (ct.scale != 0)
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
		TupleInfo ti(setTupleInfo(fVtable.columnType(i), fVtable.columnOid(i), *fJobInfo,
									tblOid, fVtable.columns()[i].get(), alias));
		pos.push_back(pos.back() + ti.width);
		oids.push_back(ti.oid);
		keys.push_back(ti.key);
		types.push_back(ti.dtype);
		scale.push_back(ti.scale);
		precision.push_back(ti.precision);

		fJobInfo->vtableColTypes[UniqId(fVtable.columnOid(i), fVtable.alias(), "")] =
				fVtable.columnType(i);
	}
	RowGroup rg1(oids.size(), pos, oids, keys, types, scale, precision);
	dynamic_cast<SubQueryStep*>(fSubQueryStep.get())->setOutputRowGroup(rg1);

	return fSubQueryStep;
}


void SubQueryTransformer::updateCorrelateInfo()
{
	// put vtable into the table list to resolve correlated filters 
//	fJobInfo->tableList.push_back(tableKey(*fJobInfo, fVtable.tableOid(),
//											"", fVtable.name(), fVtable.alias(), ""));
	// Temp fix for @bug3932 until outer join has no dependency on table order.
	// Insert at [1], not to mess with OUTER join and hint(INFINIDB_ORDERED -- bug2317).
	fJobInfo->tableList.insert(fJobInfo->tableList.begin()+1, tableKey(
		*fJobInfo, fVtable.tableOid(), "", fVtable.name(), fVtable.alias(), ""));

	// tables in outer level
	set<uint32_t> oTables;
	for (uint64_t i = 0; i < fJobInfo->tableList.size(); i++)
		oTables.insert(fJobInfo->tableList[i]);

	// Update correlated steps
	const map<UniqId, uint>& subMap = fVtable.columnMap();
	for (JobStepVector::iterator i = fCorrelatedSteps.begin(); i != fCorrelatedSteps.end(); i++)
	{
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
		ExpressionStep* es = dynamic_cast<ExpressionStep*>(i->get());
		if (thjs)
		{
			if (thjs->getJoinType() & CORRELATED)
			{
				thjs->setJoinType(thjs->getJoinType() ^ CORRELATED);
				if (thjs->correlatedSide() == 1)
				{
					int pos = thjs->sequence2();
					thjs->tableOid2(fVtable.tableOid());
					thjs->oid2(fVtable.columnOid(pos));
					thjs->alias2(fVtable.alias());
					thjs->view2(fVtable.columns()[pos]->viewName());
					thjs->dictOid2(0);
					thjs->sequence2(-1);
					thjs->joinId(fJobInfo->joinNum++);

					CalpontSystemCatalog::ColType ct2 = fVtable.columnType(pos);
					TupleInfo ti(setTupleInfo(ct2, thjs->oid2(), *fJobInfo, thjs->tableOid2(),
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
					thjs->dictOid1(0);
					thjs->sequence1(-1);
					thjs->joinId(fJobInfo->joinNum++);

					CalpontSystemCatalog::ColType ct1 = fVtable.columnType(pos);
					TupleInfo ti(setTupleInfo(ct1, thjs->oid1(), *fJobInfo, thjs->tableOid1(),
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
			for (vector<ReturnedColumn*>::iterator j = scList.begin(); j != scList.end(); j++)
			{
				SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*j);
				if (sc != NULL)
				{
					CalpontSystemCatalog::OID tblOid = tableOid(sc, fJobInfo->csc);
					string alias(extractTableAlias(sc));
					string view(sc->viewName());
            		uint32_t tid = tableKey(*fJobInfo, tblOid, alias, view);
					if (oTables.find(tid) != oTables.end())
					{
						CalpontSystemCatalog::ColType ct = fJobInfo->csc->colType(sc->oid());
						sc->joinInfo(0);
						setTupleInfo(ct, sc->oid(), *fJobInfo, tblOid, sc, alias);
					}
					else
					{
						const map<UniqId, uint>::const_iterator k =
							subMap.find(UniqId(sc->oid(), alias, view));
						if (k == subMap.end())
							//throw CorrelateFailExcept();
							throw IDBExcept(logging::ERR_NON_SUPPORT_SUB_QUERY_TYPE);

						sc->schemaName("");
						sc->tableName(fVtable.name());
						sc->tableAlias(fVtable.alias());
						sc->oid(fVtable.columnOid(k->second));
						sc->columnName(fVtable.columns()[k->second]->columnName());
						CalpontSystemCatalog::ColType ct = fVtable.columnType(k->second);
						setTupleInfo(ct, sc->oid(), *fJobInfo, fVtable.tableOid(), sc, sc->alias());
					}
				}
			}

			es->updateColumnOidAlias(*fJobInfo);
		}
		else
		{
            uint32_t tid = -1;
            uint64_t cid = (*i)->tupleId();
            if (cid != (uint64_t) -1)
                tid = getTableKey(*fJobInfo, cid);
            else
                tid = getTupleKey(*fJobInfo, (*i)->tableOid(), (*i)->alias(), (*i)->view());

			if (oTables.find(tid) == oTables.end())
			{
				if (subMap.find(UniqId((*i)->oid(), (*i)->alias(), (*i)->view())) != subMap.end())
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
	SubQueryTransformer(rhs),
	fInputDl(NULL),
	fDlIterator(-1),
	fEmptyResultSet(true),
	fExistFilter(false)
{
}


SimpleScalarTransformer::~SimpleScalarTransformer()
{
}


void SimpleScalarTransformer::run()
{
	// set up receiver
	fRowGroup = dynamic_cast<SubQueryStep*>(fSubQueryStep.get())->getOutputRowGroup();
	fRowGroup.initRow(&fRow);
	fInputDl = fSubQueryStep->outputAssociation().outAt(0)->rowGroupDL();
	fDlIterator = fInputDl->getIterator();

	// run the subquery
	fSubJobList->doQuery();

	// retrieve the scalar result
	getScalarResult();

	// check result count
	if (fStatus->errCode == ERR_MORE_THAN_1_ROW)
		throw MoreThan1RowExcept();
}


void SimpleScalarTransformer::getScalarResult()
{
	shared_array<uint8_t> rgData;
	bool more = fInputDl->next(fDlIterator, &rgData);
	fRowGroup.setData(rgData.get());

	if (more)
	{
		// Only need one row for scalar filter
		if (fEmptyResultSet && fRowGroup.getRowCount() == 1)
		{
			fEmptyResultSet = false;
			fRowData.reset(new uint8_t[fRow.getSize()]);
			fRowGroup.getRow(0, &fRow);
			memcpy(fRowData.get(), fRow.getData(), fRow.getSize());
			fRow.setData(fRowData.get());

			// For exist filter, stop the query after one or more rows retrieved.
			if (fExistFilter)
				fStatus->errCode = ERR_MORE_THAN_1_ROW;
		}

		// more than 1 row case:
		//   not empty set, and get new rows
		//   empty set, but get more than 1 rows 
		else if (fRowGroup.getRowCount() > 0)
		{
			// Stop the query after some rows retrieved.
			fEmptyResultSet = false;
			fStatus->errCode = ERR_MORE_THAN_1_ROW;
		}

		// For scalar filter, have to check all blocks to ensure only one row.
		if (fStatus->errCode == 0)
			getScalarResult();
		else
			while (fInputDl->next(fDlIterator, &rgData));
	}
}


}
// vim:ts=4 sw=4:

