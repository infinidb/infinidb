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

//  $Id: jlf_tuplejoblist.cpp 9728 2013-07-26 22:08:20Z xlou $


#include <iostream>
#include <stack>
#include <iterator>
#include <algorithm>
//#define NDEBUG
//#include <cassert>
#include <vector>
#include <set>
#include <map>
#include <limits>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "calpontsystemcatalog.h"
#include "logicoperator.h"
using namespace execplan;

#include "rowgroup.h"
#include "rowaggregation.h"
using namespace rowgroup;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "dataconvert.h"
using namespace dataconvert;

#include "elementtype.h"
#include "jlf_common.h"
#include "limitedorderby.h"
#include "jobstep.h"
#include "primitivestep.h"
#include "crossenginestep.h"
#include "expressionstep.h"
#include "subquerystep.h"
#include "tupleaggregatestep.h"
#include "tupleannexstep.h"
#include "tupleconstantstep.h"
#include "tuplehashjoin.h"
#include "tuplehavingstep.h"
#include "tupleunion.h"
#include "windowfunctionstep.h"
#include "configcpp.h"
#include "jlf_tuplejoblist.h"
using namespace joblist;


namespace
{


// construct a pcolstep from column key
void tupleKeyToProjectStep(uint32_t key, JobStepVector& jsv, JobInfo& jobInfo)
{
	// this JSA is for pcolstep construct, is not taking input/output
	// because the pcolstep is to be added into TBPS
	CalpontSystemCatalog::OID oid = jobInfo.keyInfo->tupleKeyVec[key].fId;
	DictOidToColOidMap::iterator mit = jobInfo.keyInfo->dictOidToColOid.find(oid);

	// if the key is for a dictionary, start with its token key
	if (mit != jobInfo.keyInfo->dictOidToColOid.end())
	{
		oid = mit->second;
		for (map<uint32_t, uint32_t>::iterator i = jobInfo.keyInfo->dictKeyMap.begin();
				i != jobInfo.keyInfo->dictKeyMap.end();
				i++)
		{
			if (key == i->second)
			{
				key = i->first;
				break;
			}
		}

		jobInfo.tokenOnly[key] = false;
	}

	CalpontSystemCatalog::OID tableOid = jobInfo.keyInfo->tupleKeyToTableOid.find(key)->second;
//	JobStepAssociation dummyJsa;
//	AnyDataListSPtr adl(new AnyDataList());
//	RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
//	dl->OID(oid);
//	adl->rowGroupDL(dl);
//	dummyJsa.outAdd(adl);

	CalpontSystemCatalog::ColType ct = jobInfo.keyInfo->colType[key];
	if (jobInfo.keyInfo->token2DictTypeMap.find(key) != jobInfo.keyInfo->token2DictTypeMap.end())
		ct = jobInfo.keyInfo->token2DictTypeMap[key];
	uint32_t pt = jobInfo.keyInfo->pseudoType[key];

	SJSTEP sjs;
	if (pt == 0)
		sjs.reset(new pColStep(oid, tableOid, ct, jobInfo));
	else
		sjs.reset(new PseudoColStep(oid, tableOid, pt, ct, jobInfo));
	sjs->alias(jobInfo.keyInfo->tupleKeyVec[key].fTable);
	sjs->view(jobInfo.keyInfo->tupleKeyVec[key].fView);
	sjs->schema(jobInfo.keyInfo->tupleKeyVec[key].fSchema);
	sjs->name(jobInfo.keyInfo->keyName[key]);
	sjs->tupleId(key);

	jsv.push_back(sjs);

	bool tokenOnly = false;
	map<uint32_t, bool>::iterator toIt = jobInfo.tokenOnly.find(key);
	if (toIt != jobInfo.tokenOnly.end())
		tokenOnly = toIt->second;

	if (sjs.get()->isDictCol() && !tokenOnly)
	{
		// Need a dictionary step
		uint32_t dictKey = jobInfo.keyInfo->dictKeyMap[key];
		CalpontSystemCatalog::OID dictOid = jobInfo.keyInfo->tupleKeyVec[dictKey].fId;
		sjs.reset(new pDictionaryStep(dictOid, tableOid, ct, jobInfo));
		sjs->alias(jobInfo.keyInfo->tupleKeyVec[dictKey].fTable);
		sjs->view(jobInfo.keyInfo->tupleKeyVec[dictKey].fView);
		sjs->schema(jobInfo.keyInfo->tupleKeyVec[dictKey].fSchema);
		sjs->name(jobInfo.keyInfo->keyName[dictKey]);
		sjs->tupleId(dictKey);

		jobInfo.keyInfo->dictOidToColOid[dictOid] = oid;

		jsv.push_back(sjs);
	}
}


inline void addColumnToRG(uint32_t tid, uint32_t cid, vector<uint32_t>& pos, vector<uint32_t>& oids,
	vector<uint32_t>& keys, vector<uint32_t>& scale, vector<uint32_t>& precision,
	vector<CalpontSystemCatalog::ColDataType>& types, JobInfo& jobInfo)
{
	TupleInfo ti(getTupleInfo(tid, cid, jobInfo));
	pos.push_back(pos.back() + ti.width);
	oids.push_back(ti.oid);
	keys.push_back(ti.key);
	types.push_back(ti.dtype);
	scale.push_back(ti.scale);
	precision.push_back(ti.precision);
}


inline void addColumnInExpToRG(uint32_t tid, uint32_t cid, vector<uint32_t>& pos, vector<uint32_t>& oids,
	vector<uint32_t>& keys, vector<uint32_t>& scale, vector<uint32_t>& precision,
	vector<CalpontSystemCatalog::ColDataType>& types, JobInfo& jobInfo)
{
	if (jobInfo.keyInfo->dictKeyMap.find(cid) != jobInfo.keyInfo->dictKeyMap.end())
		cid = jobInfo.keyInfo->dictKeyMap[cid];

	if (find(keys.begin(), keys.end(), cid) == keys.end())
		addColumnToRG(tid, cid, pos, oids, keys, scale, precision, types, jobInfo);
}


inline void addColumnsToRG(uint32_t tid, vector<uint32_t>& pos, vector<uint32_t>& oids,
	vector<uint32_t>& keys, vector<uint32_t>& scale, vector<uint32_t>& precision,
	vector<CalpontSystemCatalog::ColDataType>& types,
	TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	// -- the selected columns
	vector<uint32_t>& pjCol = tableInfoMap[tid].fProjectCols;
	for (unsigned i = 0; i < pjCol.size(); i++)
	{
		addColumnToRG(tid, pjCol[i], pos, oids, keys, scale, precision, types, jobInfo);
	}

	// -- any columns will be used in cross-table exps
	vector<uint32_t>& exp2 = tableInfoMap[tid].fColsInExp2;
	for (unsigned i = 0; i < exp2.size(); i++)
	{
		addColumnInExpToRG(tid, exp2[i], pos, oids, keys, scale, precision, types, jobInfo);
	}

	// -- any columns will be used in returned exps
	vector<uint32_t>& expr = tableInfoMap[tid].fColsInRetExp;
	for (unsigned i = 0; i < expr.size(); i++)
	{
		addColumnInExpToRG(tid, expr[i], pos, oids, keys, scale, precision, types, jobInfo);
	}

	// -- any columns will be used in final outer join expression
	vector<uint32_t>& expo = tableInfoMap[tid].fColsInOuter;
	for (unsigned i = 0; i < expo.size(); i++)
	{
		addColumnInExpToRG(tid, expo[i], pos, oids, keys, scale, precision, types, jobInfo);
	}
}


void constructJoinedRowGroup(RowGroup& rg, uint32_t large, uint32_t prev, bool root,
	set<uint32_t>& tableSet, TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	// Construct the output rowgroup for the join.
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	// -- start with the join keys
	// lead by joinkeys -- to have more controls on joins
	//    [loop throuh the key list to support compound join]
	if (root == false)  // not root
	{
		vector<uint32_t>& joinKeys = jobInfo.tableJoinMap[make_pair(large, prev)].fLeftKeys;
		for (vector<uint32_t>::iterator i = joinKeys.begin(); i != joinKeys.end(); i++)
			addColumnToRG(large, *i, pos, oids, keys, scale, precision, types, jobInfo);
	}

	// -- followed by the columns in select or expression
	for (set<uint32_t>::iterator i = tableSet.begin(); i != tableSet.end(); i++)
		addColumnsToRG(*i, pos, oids, keys, scale, precision, types, tableInfoMap, jobInfo);

	RowGroup tmpRg(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
	rg = tmpRg;
}


void constructJoinedRowGroup(RowGroup& rg, set<uint32_t>& tableSet, TableInfoMap& tableInfoMap,
	JobInfo& jobInfo)
{
	// Construct the output rowgroup for the join.
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	for (set<uint32_t>::iterator i = tableSet.begin(); i != tableSet.end(); i++)
	{
		// columns in select or expression
		addColumnsToRG(*i, pos, oids, keys, scale, precision, types, tableInfoMap, jobInfo);

		// keys to be joined if not already in the rowgroup
		vector<uint32_t>& adjList = tableInfoMap[*i].fAdjacentList;
		for (vector<uint32_t>::iterator j = adjList.begin(); j != adjList.end(); j++)
		{
			if (find(tableSet.begin(), tableSet.end(), *j) == tableSet.end())
			{
				// not joined
				vector<uint32_t>& joinKeys = jobInfo.tableJoinMap[make_pair(*i, *j)].fLeftKeys;
				for (vector<uint32_t>::iterator k = joinKeys.begin(); k != joinKeys.end(); k++)
				{
					if (find(keys.begin(), keys.end(), *k) == keys.end())
						addColumnToRG(*i, *k, pos, oids, keys, scale, precision, types, jobInfo);
				}
			}
		}
	}

	RowGroup tmpRg(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
	rg = tmpRg;
}


void updateExp2Cols(JobStepVector& expSteps, TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	for (JobStepVector::iterator it = expSteps.begin(); it != expSteps.end(); it++)
	{
		ExpressionStep* exps = dynamic_cast<ExpressionStep*>(it->get());
		const vector<uint32_t>& tables = exps->tableKeys();
		const vector<uint32_t>& columns = exps->columnKeys();
		for (uint64_t i = 0; i < tables.size(); ++i)
		{
			vector<uint32_t>& exp2 = tableInfoMap[tables[i]].fColsInExp2;
			vector<uint32_t>::iterator cit = find(exp2.begin(), exp2.end(), columns[i]);
			if (cit != exp2.end())
				exp2.erase(cit);
		}
	}
}


void adjustLastStep(JobStepVector& querySteps, DeliveredTableMap& deliverySteps, JobInfo& jobInfo)
{
	SJSTEP spjs = querySteps.back();
	BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(spjs.get());
	TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(spjs.get());
	SubAdapterStep* sas = dynamic_cast<SubAdapterStep*>(spjs.get());
	if (!bps && !thjs && !sas)
		throw runtime_error("Bad last step");

	// original output rowgroup of the step
	TupleJobStep* tjs = dynamic_cast<TupleJobStep*>(spjs.get());
	const RowGroup* rg0 = &(tjs->getOutputRowGroup());
	if (jobInfo.trace) cout << "Output RowGroup 0: " << rg0->toString() << endl;

	// Construct a rowgroup that matches the select columns
	TupleInfoVector v = jobInfo.pjColList;
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	for (unsigned i = 0; i < v.size(); i++)
	{
		pos.push_back(pos.back() + v[i].width);
		oids.push_back(v[i].oid);
		keys.push_back(v[i].key);
		types.push_back(v[i].dtype);
		scale.push_back(v[i].scale);
		precision.push_back(v[i].precision);
	}
	RowGroup rg1(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);

	// evaluate the returned/groupby expressions if any
	JobStepVector& expSteps = jobInfo.returnedExpressions;
	if (expSteps.size() > 0)
	{
		// create a RG has the keys not in rg0
		pos.clear();
		oids.clear();
		keys.clear();
		scale.clear();
		precision.clear();
		types.clear();
		pos.push_back(2);

		const vector<uint32_t>& keys0 = rg0->getKeys();
		for (unsigned i = 0; i < v.size(); i++)
		{
			if (find(keys0.begin(), keys0.end(), v[i].key) == keys0.end())
			{
				pos.push_back(pos.back() + v[i].width);
				oids.push_back(v[i].oid);
				keys.push_back(v[i].key);
				types.push_back(v[i].dtype);
				scale.push_back(v[i].scale);
				precision.push_back(v[i].precision);
			}
		}

		// for v0.9.3.0, the output and input to the expression are in the same row
		// add the returned column into the rg0 as rg01
		RowGroup rg01 = *rg0 + RowGroup(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
		if (jobInfo.trace) cout << "Output RowGroup 01: " << rg01.toString() << endl;

		map<uint32_t, uint32_t> keyToIndexMap0;  // maps key to the index in the input RG
		for (uint64_t i = 0; i < rg01.getKeys().size(); ++i)
			keyToIndexMap0.insert(make_pair(rg01.getKeys()[i], i));

		vector<SRCP> exps; // columns to be evaluated
		for (JobStepVector::iterator eit = expSteps.begin(); eit != expSteps.end(); ++eit)
		{
			ExpressionStep* es = dynamic_cast<ExpressionStep*>(eit->get());
			es->updateInputIndex(keyToIndexMap0, jobInfo);
			es->updateOutputIndex(keyToIndexMap0, jobInfo); // same row as input
			exps.push_back(es->expression());
		}

		// last step can be tbps (no join) or thjs, either one can have a group 3 expression
		if (bps || thjs)
		{
			tjs->setOutputRowGroup(rg01);
			tjs->setFcnExpGroup3(exps);
			tjs->setFE23Output(rg1);
		}
		else if (sas)
		{
			sas->setFeRowGroup(rg01);
			sas->addExpression(exps);
			sas->setOutputRowGroup(rg1);
		}
	}
	else
	{
		if (thjs && thjs->hasFcnExpGroup2())
			thjs->setFE23Output(rg1);
		else
			tjs->setOutputRowGroup(rg1);
	}

	if (jobInfo.trace) cout << "Output RowGroup 1: " << rg1.toString() << endl;

	if (jobInfo.hasAggregation == false)
	{
		if (thjs != NULL) //setup a few things for the final thjs step...
			thjs->outputAssociation(JobStepAssociation());

		deliverySteps[CNX_VTABLE_ID] = spjs;
	}
	else
	{
		TupleDeliveryStep* tds = dynamic_cast<TupleDeliveryStep*>(spjs.get());
		idbassert(tds != NULL);
		SJSTEP ads = TupleAggregateStep::prepAggregate(spjs, jobInfo);
		querySteps.push_back(ads);
		if (ads.get() != NULL)
			deliverySteps[CNX_VTABLE_ID] = ads;
		else
			throw std::logic_error("Failed to prepare Aggregation Delivery Step.");
	}

	if (jobInfo.havingStep)
	{
		TupleDeliveryStep* ds =
							dynamic_cast<TupleDeliveryStep*>(deliverySteps[CNX_VTABLE_ID].get());

		AnyDataListSPtr spdlIn(new AnyDataList());
		RowGroupDL* dlIn = new RowGroupDL(1, jobInfo.fifoSize);
		dlIn->OID(CNX_VTABLE_ID);
		spdlIn->rowGroupDL(dlIn);
		JobStepAssociation jsaIn;
		jsaIn.outAdd(spdlIn);
		dynamic_cast<JobStep*>(ds)->outputAssociation(jsaIn);
		jobInfo.havingStep->inputAssociation(jsaIn);

		AnyDataListSPtr spdlOut(new AnyDataList());
		RowGroupDL* dlOut = new RowGroupDL(1, jobInfo.fifoSize);
		dlOut->OID(CNX_VTABLE_ID);
		spdlOut->rowGroupDL(dlOut);
		JobStepAssociation jsaOut;
		jsaOut.outAdd(spdlOut);
		jobInfo.havingStep->outputAssociation(jsaOut);

		querySteps.push_back(jobInfo.havingStep);
		dynamic_cast<TupleHavingStep*>(jobInfo.havingStep.get())
			->initialize(ds->getDeliveredRowGroup(), jobInfo);
		deliverySteps[CNX_VTABLE_ID] = jobInfo.havingStep;
	}

	if (jobInfo.windowCols.size() > 0)
	{
		spjs = querySteps.back();
		SJSTEP ws = WindowFunctionStep::makeWindowFunctionStep(spjs, jobInfo);
		idbassert(ws.get());
		querySteps.push_back(ws);
		deliverySteps[CNX_VTABLE_ID] = ws;
	}

	if ((jobInfo.limitCount != (uint64_t) -1) ||
		(jobInfo.constantCol == CONST_COL_EXIST) ||
		(jobInfo.hasDistinct))
	{
		if (jobInfo.annexStep.get() == NULL)
			jobInfo.annexStep.reset(new TupleAnnexStep(jobInfo));

		TupleAnnexStep* tas = dynamic_cast<TupleAnnexStep*>(jobInfo.annexStep.get());
		tas->setLimit(jobInfo.limitStart, jobInfo.limitCount);

		if (jobInfo.limitCount != (uint64_t) -1)
		{
			if (jobInfo.orderByColVec.size() > 0)
				tas->addOrderBy(new LimitedOrderBy());
		}

		if (jobInfo.constantCol == CONST_COL_EXIST)
			tas->addConstant(new TupleConstantStep(jobInfo));

		if (jobInfo.hasDistinct)
			tas->setDistinct();
	}

	if (jobInfo.annexStep)
	{
		TupleDeliveryStep* ds =
							dynamic_cast<TupleDeliveryStep*>(deliverySteps[CNX_VTABLE_ID].get());
		RowGroup rg2 = ds->getDeliveredRowGroup();
		if (jobInfo.trace) cout << "Output RowGroup 2: " << rg2.toString() << endl;

		AnyDataListSPtr spdlIn(new AnyDataList());
		RowGroupDL* dlIn = new RowGroupDL(1, jobInfo.fifoSize);
		dlIn->OID(CNX_VTABLE_ID);
		spdlIn->rowGroupDL(dlIn);
		JobStepAssociation jsaIn;
		jsaIn.outAdd(spdlIn);
		dynamic_cast<JobStep*>(ds)->outputAssociation(jsaIn);
		jobInfo.annexStep->inputAssociation(jsaIn);

		AnyDataListSPtr spdlOut(new AnyDataList());
		RowGroupDL* dlOut = new RowGroupDL(1, jobInfo.fifoSize);
		dlOut->OID(CNX_VTABLE_ID);
		spdlOut->rowGroupDL(dlOut);
		JobStepAssociation jsaOut;
		jsaOut.outAdd(spdlOut);
		jobInfo.annexStep->outputAssociation(jsaOut);

		querySteps.push_back(jobInfo.annexStep);
		dynamic_cast<TupleAnnexStep*>(jobInfo.annexStep.get())->initialize(rg2, jobInfo);
		deliverySteps[CNX_VTABLE_ID] = jobInfo.annexStep;
	}

	// Check if constant false
	if (jobInfo.constantFalse)
	{
		TupleConstantBooleanStep* tcs = new TupleConstantBooleanStep(jobInfo, false);
		tcs->outputAssociation(querySteps.back().get()->outputAssociation());
		TupleDeliveryStep* tds =
			dynamic_cast<TupleDeliveryStep*>(deliverySteps[CNX_VTABLE_ID].get());
		tcs->initialize(tds->getDeliveredRowGroup(), jobInfo);

		JobStepVector::iterator it = querySteps.begin();
		while (it != querySteps.end())
		{
			if ((dynamic_cast<TupleAggregateStep*>(it->get()) != NULL) ||
				(dynamic_cast<TupleAnnexStep*>(it->get()) != NULL))
				break;

			it++;
		}

		SJSTEP bs(tcs);

		if (it != querySteps.end())
			tcs->outputAssociation((*it)->inputAssociation());
		else
			deliverySteps[CNX_VTABLE_ID] = bs;

		querySteps.erase(querySteps.begin(), it);
		querySteps.insert(querySteps.begin(), bs);
	}

	if (jobInfo.trace)
	{
		TupleDeliveryStep* ds=dynamic_cast<TupleDeliveryStep*>(deliverySteps[CNX_VTABLE_ID].get());
		if (ds) cout << "Delivered RowGroup: " << ds->getDeliveredRowGroup().toString() << endl;
	}
}


// add the project steps into the query TBPS and construct the output rowgroup
void addProjectStepsToBps(TableInfoMap::iterator& mit, BatchPrimitive* bps, JobInfo& jobInfo)
{
	// make sure we have a good tuple bps
	if (bps == NULL)
		throw runtime_error("BPS is null");

	// construct a pcolstep for each joinkey to be projected
	vector<uint32_t>& joinKeys = mit->second.fJoinKeys;
	JobStepVector keySteps;
	for (vector<uint32_t>::iterator kit = joinKeys.begin(); kit != joinKeys.end(); kit++)
		tupleKeyToProjectStep(*kit, keySteps, jobInfo);

	// construct pcolstep for columns in expresssions
	JobStepVector expSteps;
	vector<uint32_t>& exp1 = mit->second.fColsInExp1;
	for (vector<uint32_t>::iterator kit = exp1.begin(); kit != exp1.end(); kit++)
		tupleKeyToProjectStep(*kit, expSteps, jobInfo);
	vector<uint32_t>& exp2 = mit->second.fColsInExp2;
	for (vector<uint32_t>::iterator kit = exp2.begin(); kit != exp2.end(); kit++)
		tupleKeyToProjectStep(*kit, expSteps, jobInfo);
	vector<uint32_t>& expRet = mit->second.fColsInRetExp;
	for (vector<uint32_t>::iterator kit = expRet.begin(); kit != expRet.end(); kit++)
		tupleKeyToProjectStep(*kit, expSteps, jobInfo);
	vector<uint32_t>& expOut = mit->second.fColsInOuter;
	for (vector<uint32_t>::iterator kit = expOut.begin(); kit != expOut.end(); kit++)
		tupleKeyToProjectStep(*kit, expSteps, jobInfo);

	// for output rowgroup
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	// this psv is a copy of the project steps, the original vector in mit is not changed
	JobStepVector psv = mit->second.fProjectSteps;             // columns being selected
	psv.insert(psv.begin(), keySteps.begin(), keySteps.end()); // add joinkeys to project
	psv.insert(psv.end(), expSteps.begin(), expSteps.end());   // add expressions to project
	set<uint32_t> seenCols;                                    // columns already processed

	// for passthru conversion
	// passthru is disabled (default lastTupleId to -1) unless the TupleBPS::bop is BOP_AND.
	uint64_t lastTupleId = -1;
	TupleBPS* tbps = dynamic_cast<TupleBPS*>(bps);
	if (tbps != NULL && tbps->getBOP() == BOP_AND && exp1.size() == 0)
		lastTupleId = tbps->getLastTupleId();

	for (JobStepVector::iterator it = psv.begin(); it != psv.end(); it++)
	{
		JobStep* js = it->get();
		uint32_t tupleKey = js->tupleId();
		if (seenCols.find(tupleKey) != seenCols.end())
			continue;

		// update processed column set
		seenCols.insert(tupleKey);

		// if the projected column is the last accessed predicate
		pColStep* pcol = dynamic_cast<pColStep*>(js);
		if (pcol != NULL && js->tupleId() == lastTupleId)
		{
			PassThruStep* pts = new PassThruStep(*pcol);
			if (dynamic_cast<PseudoColStep*>(pcol))
				pts->pseudoType(dynamic_cast<PseudoColStep*>(pcol)->pseudoColumnId());
			pts->alias(pcol->alias());
			pts->view(pcol->view());
			pts->name(pcol->name());
			pts->tupleId(pcol->tupleId());
			it->reset(pts);
		}

		// add projected column to TBPS
		bool tokenOnly = false;
		map<uint32_t, bool>::iterator toIt = jobInfo.tokenOnly.find(js->tupleId());
		if (toIt != jobInfo.tokenOnly.end())
			tokenOnly = toIt->second;
		if (it->get()->isDictCol() && !tokenOnly)
		{
//			if (jobInfo.trace && bps->tableOid() >= 3000)
//				cout << "1 setting project BPP for " << tbps->toString() << " with " <<
//					it->get()->toString() << " and " << (it+1)->get()->toString() << endl;
			bps->setProjectBPP(it->get(),(it+1)->get());

			// this is a two-step project step, remove the token step from id vector
			vector<uint32_t>& pjv = mit->second.fProjectCols;
			uint32_t tokenKey = js->tupleId();
			for (vector<uint32_t>::iterator i = pjv.begin(); i != pjv.end(); ++i)
			{
				if (*i == tokenKey)
				{
					pjv.erase(i);
					break;
				}
			}

			// move to the dictionary step
			js = (++it)->get();
			tupleKey = js->tupleId();
			seenCols.insert(tupleKey);
		}
		else
		{
//			if (jobInfo.trace && bps->tableOid() >= 3000)
//				cout << "2 setting project BPP for " << tbps->toString() << " with " <<
//					it->get()->toString() << " and " << "NULL" << endl;
			bps->setProjectBPP(it->get(), NULL);
		}

		// add the tuple info of the column into the RowGroup
		TupleInfo ti(getTupleInfo(mit->first, tupleKey, jobInfo));
		pos.push_back(pos.back() + ti.width);
		oids.push_back(ti.oid);
		keys.push_back(ti.key);
		types.push_back(ti.dtype);
		scale.push_back(ti.scale);
		precision.push_back(ti.precision);
	}

	// construct RowGroup
	RowGroup rg(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);

	// fix the output association
	AnyDataListSPtr spdl(new AnyDataList());
	RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
	spdl->rowGroupDL(dl);
	dl->OID(mit->first);
	JobStepAssociation jsa;
	jsa.outAdd(spdl);
	bps->outputAssociation(jsa);
	bps->setOutputRowGroup(rg);
}


// add one-table expression steps into the query TBPS
void addExpresssionStepsToBps(TableInfoMap::iterator& mit, SJSTEP& sjsp, JobInfo& jobInfo)
{
	BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(sjsp.get());
	CalpontSystemCatalog::OID tableOid = mit->second.fTableOid;
	JobStepVector& exps = mit->second.fOneTableExpSteps;
	ExpressionStep* exp0 = dynamic_cast<ExpressionStep*>(exps[0].get());

	if (bps == NULL)
	{
		if (tableOid > 0)
		{
			uint32_t key0 = exp0->columnKey();
			CalpontSystemCatalog::ColType ct = jobInfo.keyInfo->colType[key0];
			map<uint32_t, CalpontSystemCatalog::ColType>::iterator dkMit;
			if (jobInfo.keyInfo->token2DictTypeMap.find(key0) !=
				jobInfo.keyInfo->token2DictTypeMap.end())
				ct = jobInfo.keyInfo->token2DictTypeMap[key0];

			scoped_ptr<pColScanStep> pcss(
					new pColScanStep(exp0->oid(), tableOid, ct, jobInfo));

			sjsp.reset(new TupleBPS(*pcss, jobInfo));
			TupleBPS* tbps = dynamic_cast<TupleBPS*>(sjsp.get());
			tbps->setJobInfo(&jobInfo);
			tbps->setFirstStepType(SCAN);

			// add the first column to BPP's filterSteps
			tbps->setBPP(pcss.get());

			bps = tbps;
		}
		else
		{
			sjsp.reset(new  CrossEngineStep(mit->second.fSchema,
											mit->second.fName,
											mit->second.fAlias,
											jobInfo));

			bps = dynamic_cast<CrossEngineStep*>(sjsp.get());
		}
	}

	// rowgroup for evaluating the expression
	vector<uint32_t> pos;
	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<CalpontSystemCatalog::ColDataType> types;
	pos.push_back(2);

	vector<uint32_t>& cols = mit->second.fColsInExp1;
	uint32_t index = 0;                 // index in the rowgroup
	map<uint32_t, uint32_t> keyToIndexMap;  // maps key to the index in the RG
	for (vector<uint32_t>::iterator kit = cols.begin(); kit != cols.end(); kit++)
	{
		uint32_t key = *kit;
		if (jobInfo.keyInfo->dictKeyMap.find(key) != jobInfo.keyInfo->dictKeyMap.end())
			key = jobInfo.keyInfo->dictKeyMap[key];

		// check if this key is already in
		if (keyToIndexMap.find(key) != keyToIndexMap.end())
			continue;

		// update processed column set
		keyToIndexMap.insert(make_pair(key, index++));

		// add the tuple info of the column into the RowGroup
		TupleInfo ti(getTupleInfo(mit->first, key, jobInfo));
		pos.push_back(pos.back() + ti.width);
		oids.push_back(ti.oid);
		keys.push_back(ti.key);
		types.push_back(ti.dtype);
		scale.push_back(ti.scale);
		precision.push_back(ti.precision);
	}

	// construct RowGroup and add to TBPS
	RowGroup rg(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
	bps->setFE1Input(rg);

	if (jobInfo.trace) cout << "FE1 input RowGroup: " << rg.toString() << endl << endl;

	// add the expression steps into TBPS, the input-indices are set in SCs.
	for (JobStepVector::iterator it = exps.begin(); it != exps.end(); it++)
	{
		ExpressionStep* e = dynamic_cast<ExpressionStep*>(it->get());
		e->updateInputIndex(keyToIndexMap, jobInfo);
		boost::shared_ptr<ParseTree> sppt(new ParseTree);
		sppt->copyTree(*(e->expressionFilter()));
		bps->addFcnExpGroup1(sppt);
	}
}


bool combineJobStepsByTable(TableInfoMap::iterator& mit, JobInfo& jobInfo)
{
	TableInfo& tableInfo = mit->second;
	JobStepVector& qsv = tableInfo.fQuerySteps;
	JobStepVector newSteps;             // store combined steps
	RowGroup rgOut;                     // rowgroup of combined steps
	CalpontSystemCatalog::OID tableOid = tableInfo.fTableOid;

	if (tableOid != CNX_VTABLE_ID)
	{
		// real table
		if (qsv.size() == 0)
		{
			// find a column in FE1, FE2, or FE3
			uint32_t key = -1;
			if (tableInfo.fColsInExp1.size() > 0)
				key = tableInfo.fColsInExp1[0];
			else if (tableInfo.fColsInExp2.size() > 0)
				key = tableInfo.fColsInExp2[0];
			else if (tableInfo.fColsInRetExp.size() > 0)
				key = tableInfo.fColsInRetExp[0];
			else if (tableInfo.fColsInOuter.size() > 0)
				key = tableInfo.fColsInOuter[0];
			else if (tableInfo.fColsInColMap.size() > 0)
				key = tableInfo.fColsInColMap[0];
			else
				throw runtime_error("No query step");

			// construct a pcolscanstep to initialize the tbps
			CalpontSystemCatalog::OID oid = jobInfo.keyInfo->tupleKeyVec[key].fId;
			CalpontSystemCatalog::ColType ct = jobInfo.keyInfo->colType[key];
			map<uint32_t, CalpontSystemCatalog::ColType>::iterator dkMit;
			if (jobInfo.keyInfo->token2DictTypeMap.find(key) !=
				jobInfo.keyInfo->token2DictTypeMap.end())
				ct = jobInfo.keyInfo->token2DictTypeMap[key];

			SJSTEP sjs(new pColScanStep(oid, tableOid, ct, jobInfo));
			sjs->alias(jobInfo.keyInfo->tupleKeyVec[key].fTable);
			sjs->view(jobInfo.keyInfo->tupleKeyVec[key].fView);
			sjs->schema(jobInfo.keyInfo->tupleKeyVec[key].fSchema);
			sjs->name(jobInfo.keyInfo->keyName[key]);
			sjs->tupleId(key);
			qsv.push_back(sjs);
		}

		SJSTEP sjsp;                        // shared_ptr for the new BatchPrimitive
		BatchPrimitive* bps = NULL;         // pscan/pcol/filter/etc combined to
		vector<DictionaryScanInfo> pdsVec;  // pds for string filters
		JobStepVector::iterator begin = qsv.begin();
		JobStepVector::iterator end = qsv.end();
		JobStepVector::iterator it = begin;

		// make sure there is a pcolscan if there is a pcolstep
		while (it != end)
		{
			if (typeid(*(it->get())) == typeid(pColScanStep))
				break;

			if (typeid(*(it->get())) == typeid(pColStep))
			{
				pColStep* pcs = dynamic_cast<pColStep*>(it->get());
				(*it).reset(new pColScanStep(*pcs));
				break;
			}

			it++;
		}

		// ---- predicates ----
		// setup TBPS and dictionaryscan
		it = begin;
		while (it != end)
		{
			if (typeid(*(it->get())) == typeid(pColScanStep))
			{
				if (bps == NULL)
				{
					if (tableOid > 0)
					{
						sjsp.reset(new TupleBPS(*(dynamic_cast<pColScanStep*>(it->get())),jobInfo));
						TupleBPS* tbps = dynamic_cast<TupleBPS*>(sjsp.get());
						tbps->setJobInfo(&jobInfo);
						tbps->setFirstStepType(SCAN);
						bps = tbps;
					}
					else
					{
						sjsp.reset(new  CrossEngineStep(mit->second.fSchema,
														mit->second.fName,
														mit->second.fAlias,
														jobInfo));
						bps = dynamic_cast<CrossEngineStep*>(sjsp.get());
					}
				}
				else
				{
					pColScanStep* pcss = dynamic_cast<pColScanStep*>(it->get());
					(*it).reset(new pColStep(*pcss));
				}
			}

			unsigned itInc = 1;               // iterator increase number
			unsigned numOfStepsAddToBps = 0;  // # steps to be added into TBPS
			if ((distance(it, end) > 2 &&
				 dynamic_cast<pDictionaryScan*>(it->get()) != NULL &&
				 (dynamic_cast<pColScanStep*>((it + 1)->get()) != NULL ||
				  dynamic_cast<pColStep*>((it + 1)->get()) != NULL) &&
				 dynamic_cast<TupleHashJoinStep*>((it + 2)->get()) != NULL) ||
				(distance(it, end) > 1 &&
				 dynamic_cast<pDictionaryScan*>(it->get()) != NULL &&
				 dynamic_cast<TupleHashJoinStep*>((it + 1)->get()) != NULL))
			{
				// string access predicate
				// setup pDictionaryScan
				pDictionaryScan* pds = dynamic_cast<pDictionaryScan*>(it->get());
				vector<uint32_t> pos;
				vector<uint32_t> oids;
				vector<uint32_t> keys;
				vector<uint32_t> scale;
				vector<uint32_t> precision;
				vector<CalpontSystemCatalog::ColDataType> types;
				pos.push_back(2);

				pos.push_back(2 + 8);
				CalpontSystemCatalog::OID coid = jobInfo.keyInfo->dictOidToColOid[pds->oid()];
				oids.push_back(coid);
				uint32_t keyId = pds->tupleId();
				keys.push_back(keyId);
				types.push_back(CalpontSystemCatalog::BIGINT);
				scale.push_back(0);
				precision.push_back(0);

				RowGroup rg(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold);
				if (jobInfo.trace) cout << "RowGroup pds(and): " << rg.toString() << endl;
				pds->setOutputRowGroup(rg);
				newSteps.push_back(*it);

				DictionaryScanInfo pdsInfo;
				pdsInfo.fTokenId = keyId;
				pdsInfo.fDl = pds->outputAssociation().outAt(0);
				pdsInfo.fRowGroup = rg;
				pdsVec.push_back(pdsInfo);

				// save the token join to the last
				itInc = 1;
				numOfStepsAddToBps = 0;
			}
			else if (distance(begin, it) > 1 &&
				(dynamic_cast<pDictionaryScan*>((it-1)->get()) != NULL ||
				 dynamic_cast<pDictionaryScan*>((it-2)->get()) != NULL) &&
				dynamic_cast<TupleHashJoinStep*>(it->get()) != NULL)
			{
				// save the token join to the last, by pdsInfo
				itInc = 1;
				numOfStepsAddToBps = 0;
			}
			else if (distance(it, end) > 2 &&
				dynamic_cast<pColStep*>((it + 1)->get()) != NULL &&
				dynamic_cast<FilterStep*>((it + 2)->get()) != NULL)
			{
				itInc = 3;
				numOfStepsAddToBps = 3;
			}
			else if (distance(it, end) > 3 &&
				dynamic_cast<pColStep*>((it + 1)->get()) != NULL &&
				dynamic_cast<pDictionaryStep*>((it + 2)->get()) != NULL &&
				dynamic_cast<FilterStep*>((it + 3)->get()) != NULL)
			{
				itInc = 4;
				numOfStepsAddToBps = 4;
			}
			else if (distance(it, end) > 3 &&
				dynamic_cast<pDictionaryStep*>((it + 1)->get()) != NULL &&
				dynamic_cast<pColStep*>((it + 2)->get()) != NULL &&
				dynamic_cast<FilterStep*>((it + 3)->get()) != NULL)
			{
				itInc = 4;
				numOfStepsAddToBps = 4;
			}
			else if (distance(it, end) > 4 &&
				dynamic_cast<pDictionaryStep*>((it + 1)->get()) != NULL &&
				dynamic_cast<pColStep*>((it + 2)->get()) != NULL &&
				dynamic_cast<pDictionaryStep*>((it + 3)->get()) != NULL &&
				dynamic_cast<FilterStep*>((it + 4)->get()) != NULL)
			{
				itInc = 5;
				numOfStepsAddToBps = 5;
			}
			else if (distance(it, end) > 1 &&
				(dynamic_cast<pColStep*>(it->get()) != NULL ||
				 dynamic_cast<pColScanStep*>(it->get()) != NULL) &&
				dynamic_cast<pDictionaryStep*>((it + 1)->get()) != NULL)
			{
				itInc = 2;
				numOfStepsAddToBps = 2;
			}
			else if (dynamic_cast<pColStep*>(it->get()) != NULL)
			{
				pColStep* pcol = dynamic_cast<pColStep*>(it->get());
				if (pcol->getFilters().size() == 0)
				{
					// not an access predicate, pcol for token will be added later if necessary
					numOfStepsAddToBps = 0;
				}
				else
				{
					numOfStepsAddToBps = 1;
				}

				itInc = 1;
			}
			else if (dynamic_cast<pColScanStep*>(it->get()) != NULL)
			{
				numOfStepsAddToBps = 1;
				itInc = 1;
			}
			else
			{
				// Not a combinable step, or step pattern not recognized.
				cerr << boldStart << "Try to combine " << typeid(*(it->get())).name() << ": "
						<< it->get()->oid() << " into TBPS" << boldStop << endl;
				return false;
			}

			// now add the steps into the TBPS
			if (numOfStepsAddToBps > 0 && bps == NULL)
				throw runtime_error("BPS not created 1");

			for (unsigned i = 0; i < numOfStepsAddToBps; i++)
			{
				bps->setBPP((it+i)->get());
				bps->setStepCount();
				bps->setLastTupleId((it+i)->get()->tupleId());
			}

			it += itInc;
		}

		// add one-table expression steps to TBPS
		if (tableInfo.fOneTableExpSteps.size() > 0)
			addExpresssionStepsToBps(mit, sjsp, jobInfo);

		// add TBPS to the step vector
		newSteps.push_back(sjsp);

		// ---- projects ----
		// now, add the joinkeys to the project step vector
		addProjectStepsToBps(mit, bps, jobInfo);

		// rowgroup has the joinkeys and selected columns
		// this is the expected output of this table
		rgOut = bps->getOutputRowGroup();

		// add token joins
		if (pdsVec.size() > 0)
		{
			// ---- token joins ----
			// construct a TupleHashJoinStep
			TupleBPS* tbps = dynamic_cast<TupleBPS*>(bps);
			TupleHashJoinStep* thjs= new TupleHashJoinStep(jobInfo);
			thjs->tableOid1(0);
			thjs->tableOid2(tableInfo.fTableOid);
			thjs->alias1(tableInfo.fAlias);
			thjs->alias2(tableInfo.fAlias);
			thjs->view1(tableInfo.fView);
			thjs->view2(tableInfo.fView);
			thjs->schema1(tableInfo.fSchema);
			thjs->schema2(tableInfo.fSchema);
			thjs->setLargeSideBPS(tbps);
			thjs->joinId(-1); // token join is a filter force it done before other joins
			thjs->setJoinType(INNER);
			thjs->tokenJoin(mit->first);
			tbps->incWaitToRunStepCnt();
			SJSTEP spthjs(thjs);

			// rowgroup of the TBPS side
			// start with the expected output of the table, tokens will be appended
			RowGroup rgTbps = rgOut;

			// input jobstepassociation
			// 1.  small sides -- pdictionaryscan steps
			vector<RowGroup> rgPdsVec;
			map<uint32_t, uint32_t> addedCol;
			vector<JoinType> jointypes;
			vector<bool> typeless;
			vector<vector<uint32_t> > smallKeyIndices;
			vector<vector<uint32_t> > largeKeyIndices;
			vector<string> tableNames;
			JobStepAssociation inJsa;
			for (vector<DictionaryScanInfo>::iterator i = pdsVec.begin(); i != pdsVec.end(); i++)
			{
				// add the token steps to the TBPS
				uint32_t tupleKey = i->fTokenId;
				map<uint32_t, uint32_t>::iterator k = addedCol.find(tupleKey);
				unsigned largeSideIndex = rgTbps.getColumnCount();
				if (k == addedCol.end())
				{
					SJSTEP sjs(new pColStep(jobInfo.keyInfo->tupleKeyVec[tupleKey].fId,
											tableInfo.fTableOid,
											jobInfo.keyInfo->token2DictTypeMap[tupleKey],
											jobInfo));
					sjs->alias(tableInfo.fAlias);
					sjs->view(tableInfo.fView);
					sjs->schema(tableInfo.fSchema);
					sjs->name(jobInfo.keyInfo->keyName[tupleKey]);
					sjs->tupleId(tupleKey);
					bps->setProjectBPP(sjs.get(), NULL);

					// Update info, which will be used to config the hashjoin later.
					rgTbps += i->fRowGroup;
					addedCol[tupleKey] = largeSideIndex;
				}
				else
				{
					largeSideIndex = k->second;
				}

				inJsa.outAdd(i->fDl);
				tableNames.push_back(jobInfo.keyInfo->tupleKeyVec[tupleKey].fTable);
				rgPdsVec.push_back(i->fRowGroup);
				jointypes.push_back(INNER);
				typeless.push_back(false);
				smallKeyIndices.push_back(vector<uint32_t>(1, 0));
				largeKeyIndices.push_back(vector<uint32_t>(1, largeSideIndex));
			}

			// 2. large side
			if (jobInfo.trace) cout << "RowGroup bps(and): " << rgTbps.toString() << endl;
			bps->setOutputRowGroup(rgTbps);
			inJsa.outAdd(bps->outputAssociation().outAt(0));

			// set input jobstepassociation
			thjs->inputAssociation(inJsa);
			thjs->setLargeSideDLIndex(inJsa.outSize() - 1);

			// output jobstepassociation
			AnyDataListSPtr spdl(new AnyDataList());
			RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
			spdl->rowGroupDL(dl);
			dl->OID(mit->first);
			JobStepAssociation jsaOut;
			jsaOut.outAdd(spdl);
			thjs->outputAssociation(jsaOut);

			// config the tuplehashjoin
			thjs->configSmallSideRG(rgPdsVec, tableNames);
			thjs->configLargeSideRG(rgTbps);
			thjs->configJoinKeyIndex(jointypes, typeless, smallKeyIndices, largeKeyIndices);
			thjs->setOutputRowGroup(rgOut);
			newSteps.push_back(spthjs);
		}
	}
	else
	{
		// table derived from subquery
		SubQueryStep* subStep = NULL;
		SubAdapterStep* adaStep = NULL;
		for (JobStepVector::iterator it = qsv.begin(); it != qsv.end(); it++)
		{
			if (((subStep = dynamic_cast<SubQueryStep*>(it->get())) != NULL) ||
				((adaStep = dynamic_cast<SubAdapterStep*>(it->get())) != NULL))
				newSteps.push_back(*it);
		}

		if (subStep == NULL && adaStep == NULL)
			throw runtime_error("No step for subquery.");

		if (subStep)
		{
			rgOut = subStep->getOutputRowGroup();
		}
		else
		{
			// add one-table expression steps to the adapter
			if (tableInfo.fOneTableExpSteps.size() > 0)
				adaStep->addExpression(tableInfo.fOneTableExpSteps, jobInfo);

			rgOut = adaStep->getOutputRowGroup();
		}
	}

	tableInfo.fDl = newSteps.back()->outputAssociation().outAt(0);
	tableInfo.fRowGroup = rgOut;

	if (jobInfo.trace)
		cout << "RowGroup for " << mit->first << " : " << mit->second.fRowGroup.toString() << endl;

	qsv.swap(newSteps);

	return true;
}


void spanningTreeCheck(TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	bool spanningTree = true;
	unsigned errcode = 0;
	Message::Args args;

	if (jobInfo.trace)
	{
		cout << "Table Connection:" << endl;
		for (TableInfoMap::iterator i = tableInfoMap.begin(); i != tableInfoMap.end(); i++)
		{
			cout << i->first << " :";
			vector<uint32_t>::iterator j = i->second.fAdjacentList.begin();
			while (j != i->second.fAdjacentList.end())
				cout << " " << *j++;
			cout << endl;
		}
		cout << endl;
	}

	if (tableInfoMap.size() < 1)
	{
		spanningTree = false;
		cerr << boldStart << "No table information." << boldStop << endl;
		throw logic_error("No table information.");
	}
	else if (tableInfoMap.size() > 1)
	{
		// make sure all tables are joined if not a single table query
		set<pair<uint32_t, uint32_t> > joinPaths;
		vector<uint32_t> joinedTables;
		joinedTables.push_back((tableInfoMap.begin())->first);
		for (size_t i = 0; i < joinedTables.size(); i++)
		{
			vector<uint32_t>& v = tableInfoMap[joinedTables[i]].fAdjacentList;
			for (vector<uint32_t>::iterator j = v.begin(); j != v.end(); j++)
			{
				if (find(joinedTables.begin(), joinedTables.end(), *j) == joinedTables.end())
					joinedTables.push_back(*j);

				joinPaths.insert(make_pair(joinedTables[i], *j));
			}
		}

		// 1. connected
		if (joinedTables.size() < tableInfoMap.size())
		{
			vector<uint32_t> notJoinedTables;
			for (TableInfoMap::iterator i = tableInfoMap.begin(); i != tableInfoMap.end(); i++)
			{
				if (find(joinedTables.begin(), joinedTables.end(), i->first) == joinedTables.end())
					notJoinedTables.push_back(i->first);
			}

			vector<uint32_t>::iterator k = joinedTables.begin();
			set<string> views1;
			set<string> tables1;
			for (; k != joinedTables.end(); k++)
			{
				if (jobInfo.keyInfo->tupleKeyVec[*k].fView.empty())
					tables1.insert(jobInfo.keyInfo->tupleKeyToName[*k]);
				else
					views1.insert(jobInfo.keyInfo->tupleKeyVec[*k].fView);
			}

			k = notJoinedTables.begin();
			set<string> views2;
			set<string> tables2;
			string errStr;
			for (; k != notJoinedTables.end(); k++)
			{
				if (jobInfo.keyInfo->tupleKeyVec[*k].fView.empty())
					tables2.insert(jobInfo.keyInfo->tupleKeyToName[*k]);
				else
					views2.insert(jobInfo.keyInfo->tupleKeyVec[*k].fView);

				if (jobInfo.incompatibleJoinMap.find(*k) != jobInfo.incompatibleJoinMap.end())
				{
					errcode = ERR_INCOMPATIBLE_JOIN;

					uint32_t key2 = jobInfo.incompatibleJoinMap[*k];
					if (jobInfo.keyInfo->tupleKeyVec[*k].fView.length() > 0)
					{
						string view2 = jobInfo.keyInfo->tupleKeyVec[key2].fView;
						if (jobInfo.keyInfo->tupleKeyVec[*k].fView == view2)
						{
							//  same view
							errStr += "Tables in '" + view2 + "' have";
						}
						else if (view2.empty())
						{
							// view and real table
							errStr += "'" + jobInfo.keyInfo->tupleKeyVec[*k].fView + "' and '" +
										jobInfo.keyInfo->tupleKeyToName[key2] + "' have";
						}
						else
						{
							// two views
							errStr += "'" + jobInfo.keyInfo->tupleKeyVec[*k].fView + "' and '" +
										view2 + "' have";
						}
					}
					else
					{
						string view2 = jobInfo.keyInfo->tupleKeyVec[key2].fView;
						if (view2.empty())
						{
							// two real tables
							errStr += "'" + jobInfo.keyInfo->tupleKeyToName[*k] + "' and '" +
										jobInfo.keyInfo->tupleKeyToName[key2] + "' have";
						}
						else
						{
							// real table and view
							errStr += "'" + jobInfo.keyInfo->tupleKeyToName[*k] + "' and '" +
										view2 + "' have";
						}
					}

					break;
				}

			}

			if (errStr.empty())
			{
				errcode = ERR_MISS_JOIN;

				// 1. check if all tables in a view are joined
				for (set<string>::iterator s = views1.begin(); s != views1.end(); s++)
				{
					if (views2.find(*s) != views2.end())
					{
						errStr = "Tables in '" + (*s) + "' are";
					}
				}

				// 2. tables and views are joined
				if (errStr.empty())
				{
					string set1;
					for (set<string>::iterator s = views1.begin(); s != views1.end(); s++)
					{
						if (set1.empty())
							set1 = "'";
						else
							set1 += ", ";

						set1 += (*s);
					}

					for (set<string>::iterator s = tables1.begin(); s != tables1.end(); s++)
					{
						if (set1.empty())
							set1 = "'";
						else
							set1 += ", ";

						set1 += (*s);
					}

					string set2;
					for (set<string>::iterator s = views2.begin(); s != views2.end(); s++)
					{
						if (set2.empty())
							set2 = "'";
						else
							set2 += ", ";

						set2 += (*s);
					}

					for (set<string>::iterator s = tables2.begin(); s != tables2.end(); s++)
					{
						if (set2.empty())
							set2 = "'";
						else
							set2 += ", ";

						set2 += (*s);
					}

					errStr = set1 + "' and " + set2 + "' are";
				}
			}

			args.add(errStr);
			spanningTree = false;
		}

		// 2. no cycles
		if (spanningTree && (joinedTables.size() - joinPaths.size()/2 != 1))
		{
			errcode = ERR_CIRCULAR_JOIN;
			spanningTree = false;
		}
	}

	if (!spanningTree)
	{
		cerr << boldStart << IDBErrorInfo::instance()->errorMsg(errcode, args) << boldStop << endl;
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
	}
}


void outjoinPredicateAdjust(TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	set<uint32_t>::iterator i = jobInfo.outerOnTable.begin();
	for (; i != jobInfo.outerOnTable.end(); i++)
	{
		// resetTableFilters(tableInfoMap[*i], jobInfo)
		TableInfo& tblInfo = tableInfoMap[*i];
		if (tblInfo.fTableOid != CNX_VTABLE_ID)
		{
			JobStepVector::iterator k = tblInfo.fQuerySteps.begin();
			JobStepVector onClauseFilterSteps;  //@bug5887, 5311
			for (; k != tblInfo.fQuerySteps.end(); k++)
			{
				if ((*k)->onClauseFilter())
				{
					onClauseFilterSteps.push_back(*k);
					continue;
				}

				uint32_t colKey = -1;
				pColStep* pcs = dynamic_cast<pColStep*>(k->get());
				pColScanStep* pcss = dynamic_cast<pColScanStep*>(k->get());
				pDictionaryScan* pdss = dynamic_cast<pDictionaryScan*>(k->get());
				pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>(k->get());
				vector<const execplan::Filter*>* filters = NULL;
				int8_t bop = -1;
				if (pcs != NULL)
				{
					filters = &(pcs->getFilters());
					bop = pcs->BOP();
					colKey = pcs->tupleId();
				}
				else if (pcss != NULL)
				{
					filters = &(pcss->getFilters());
					bop = pcss->BOP();
					colKey = pcss->tupleId();
				}
				else if (pdss != NULL)
				{
					filters = &(pdss->getFilters());
					bop = pdss->BOP();
					colKey = pdss->tupleId();
				}
				else if (pdsp != NULL)
				{
					filters = &(pdsp->getFilters());
					bop = pdsp->BOP();
					colKey = pdsp->tupleId();
				}

				if (filters != NULL && filters->size() > 0)
				{
					ParseTree* pt = new ParseTree((*filters)[0]->clone());
					for (size_t i = 1; i < filters->size(); i++)
					{
						ParseTree* left = pt;
						ParseTree* right =
							new ParseTree((*filters)[i]->clone());
						ParseTree* op = (BOP_OR == bop) ?
							new ParseTree(new LogicOperator("or")) :
							new ParseTree(new LogicOperator("and"));
						op->left(left);
						op->right(right);

						pt = op;
					}

					ExpressionStep* es = new ExpressionStep(jobInfo);
					if (es == NULL)
						throw runtime_error ("Failed to new ExpressionStep 2");

					es->expressionFilter(pt, jobInfo);
					SJSTEP sjstep(es);
					jobInfo.outerJoinExpressions.push_back(sjstep);
					tblInfo.fColsInOuter.push_back(colKey);

					delete pt;
				}
			}

			// Do not apply the primitive filters if there is an "IS NULL" in where clause.
			if (jobInfo.tableHasIsNull.find(*i) != jobInfo.tableHasIsNull.end())
				tblInfo.fQuerySteps = onClauseFilterSteps;
		}

		jobInfo.outerJoinExpressions.insert(jobInfo.outerJoinExpressions.end(),
				tblInfo.fOneTableExpSteps.begin(), tblInfo.fOneTableExpSteps.end());
		tblInfo.fOneTableExpSteps.clear();

		tblInfo.fColsInOuter.insert(tblInfo.fColsInOuter.end(),
									tblInfo.fColsInExp1.begin(), tblInfo.fColsInExp1.end());
	}
}


uint32_t getLargestTable(JobInfo& jobInfo, TableInfoMap& tableInfoMap, bool overrideLargeSideEstimate)
{
	// Subquery in FROM clause assumptions:
	//   hint will be ignored, if the 1st table in FROM clause is a derived table.
	if (jobInfo.keyInfo->tupleKeyVec[jobInfo.tableList[0]].fId < 3000)
		overrideLargeSideEstimate = false;

	// Bug 2123. Added logic to dynamically determine the large side table unless the SQL statement
	// contained a hint saying to skip the estimation and use the FIRST table in the from clause.
	// Prior to this, we were using the LAST table in the from clause.  We switched it as there
	// were some outer join sqls that couldn't be written with the large table last.
	// Default to the first table in the from clause if:
	//   the user set the hint; or
	//   there is only one table in the query.
	uint32_t ret = jobInfo.tableList.front();

	if(jobInfo.tableList.size() <= 1)
	{
		return ret;
	}

	// Algorithm to dynamically determine the largest table.
	uint64_t largestCardinality = 0;
	uint64_t estimatedRowCount = 0;

	// Loop through the tables and find the one with the largest estimated cardinality.
	for (uint32_t i = 0; i < jobInfo.tableList.size(); i++)
	{
		jobInfo.tableSize[jobInfo.tableList[i]] = 0;
		TableInfoMap::iterator it = tableInfoMap.find(jobInfo.tableList[i]);
		if (it != tableInfoMap.end())
		{
			// @Bug 3771.  Loop through the query steps until the tupleBPS is found instead of
			// just looking at the first one.  Tables in the query that included a filter on a
			// dictionary column were not getting their row count estimated.
			for(JobStepVector::iterator jsIt = it->second.fQuerySteps.begin();
					jsIt != it->second.fQuerySteps.end(); jsIt++)
			{
				TupleBPS* tupleBPS = dynamic_cast<TupleBPS*>((*jsIt).get());
				if (tupleBPS != NULL)
				{
					estimatedRowCount = tupleBPS->getEstimatedRowCount();
					jobInfo.tableSize[jobInfo.tableList[i]] = estimatedRowCount;
					if (estimatedRowCount > largestCardinality)
					{
						ret = jobInfo.tableList[i];
						largestCardinality = estimatedRowCount;
					}
					break;
				}
			}
		}
	}

	// select /*! INFINIDB_ORDERED */
	if(overrideLargeSideEstimate)
	{
		ret = jobInfo.tableList.front();
		jobInfo.tableSize[ret] = numeric_limits<uint64_t>::max();
	}

	return ret;
}


uint32_t getPrevLarge(uint32_t n, TableInfoMap& tableInfoMap)
{
	// root node : no previous node;
	// other node: only one immediate previous node;
	int prev = -1;
	vector<uint32_t>& adjList = tableInfoMap[n].fAdjacentList;
	for (vector<uint32_t>::iterator i = adjList.begin(); i != adjList.end() && prev < 0; i++)
	{
		if (tableInfoMap[*i].fVisited == true)
			prev = *i;
	}

	return prev;
}


uint32_t getKeyIndex(uint32_t key, const RowGroup& rg)
{
	vector<uint32_t>::const_iterator i = rg.getKeys().begin();
	for (; i != rg.getKeys().end(); ++i)
		if (key == *i)
			break;

	if (i == rg.getKeys().end())
		throw runtime_error("No key found.");

	return distance(rg.getKeys().begin(), i);
}


bool joinInfoCompare(const SP_JoinInfo& a, const SP_JoinInfo& b)
{
	return (a->fJoinData.fJoinId < b->fJoinData.fJoinId);
}


string joinTypeToString(const JoinType& joinType)
{
	string ret;
	if (joinType & INNER)
		ret = "inner";
	else if (joinType & LARGEOUTER)
		ret = "largeOuter";
	else if (joinType & SMALLOUTER)
		ret = "smallOuter";

	if (joinType & SEMI)
		ret += "+semi";
	if (joinType & ANTI)
		ret += "+ant";
	if (joinType & SCALAR)
		ret += "+scalar";
	if (joinType & MATCHNULLS)
		ret += "+matchnulls";
	if (joinType & WITHFCNEXP)
		ret += "+exp";
	if (joinType & CORRELATED)
		ret += "+correlated";

	return ret;
}


SP_JoinInfo joinToLargeTable(uint32_t large, TableInfoMap& tableInfoMap,
							 JobInfo& jobInfo, vector<uint32_t>& joinOrder)
{
	vector<SP_JoinInfo> smallSides;
	tableInfoMap[large].fVisited = true;
	tableInfoMap[large].fJoinedTables.insert(large);
	set<uint32_t>& tableSet = tableInfoMap[large].fJoinedTables;
	vector<uint32_t>& adjList = tableInfoMap[large].fAdjacentList;
	uint32_t prevLarge = (uint32_t) getPrevLarge(large, tableInfoMap);
	bool root = (prevLarge == (uint32_t) -1) ? true : false;
	uint32_t link = large;
	uint32_t cId = -1;

	// Get small sides ready.
	for (vector<uint32_t>::iterator i = adjList.begin(); i != adjList.end(); i++)
	{
		if (tableInfoMap[*i].fVisited == false)
		{
			cId = *i;
			smallSides.push_back(joinToLargeTable(*i, tableInfoMap, jobInfo, joinOrder));

			tableSet.insert(tableInfoMap[*i].fJoinedTables.begin(),
						 	tableInfoMap[*i].fJoinedTables.end());
		}
	}

	// Join with its small sides, if not a leaf node.
	if (smallSides.size() > 0)
	{
		// non-leaf node, need a join
		SJSTEP spjs = tableInfoMap[large].fQuerySteps.back();
		BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(spjs.get());
		SubAdapterStep* tsas = dynamic_cast<SubAdapterStep*>(spjs.get());
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(spjs.get());

		// @bug6158, try to put BPS on large side if possible
		if (tsas && smallSides.size() == 1)
		{
			SJSTEP sspjs = tableInfoMap[cId].fQuerySteps.back(),get();
			BatchPrimitive* sbps = dynamic_cast<BatchPrimitive*>(sspjs.get());
			TupleHashJoinStep* sthjs = dynamic_cast<TupleHashJoinStep*>(sspjs.get());
			if (sbps || (sthjs && sthjs->tokenJoin() == cId))
			{
				SP_JoinInfo largeJoinInfo(new JoinInfo);
				largeJoinInfo->fTableOid = tableInfoMap[large].fTableOid;
				largeJoinInfo->fAlias = tableInfoMap[large].fAlias;
				largeJoinInfo->fView = tableInfoMap[large].fView;
				largeJoinInfo->fSchema = tableInfoMap[large].fSchema;

				largeJoinInfo->fDl = tableInfoMap[large].fDl;
				largeJoinInfo->fRowGroup = tableInfoMap[large].fRowGroup;

				TableJoinMap::iterator mit = jobInfo.tableJoinMap.find(make_pair(large, cId));
				if (mit == jobInfo.tableJoinMap.end())
					throw runtime_error("Join step not found.");
				largeJoinInfo->fJoinData = mit->second;

				// switch large and small sides
				joinOrder.back() = large;
				large = cId;
				smallSides[0] = largeJoinInfo;
				tableInfoMap[large].fJoinedTables = tableSet;

				bps = sbps;
				thjs = sthjs;
				tsas = NULL;
			}
		}

		if (!bps && !thjs && !tsas)
		{
			if (dynamic_cast<SubQueryStep*>(spjs.get()))
				throw IDBExcept(ERR_NON_SUPPORT_SUB_QUERY_TYPE);
			throw runtime_error("Not supported join.");
		}

		size_t dcf = 0; // for dictionary column filters, 0 if thjs is null.
		RowGroup largeSideRG = tableInfoMap[large].fRowGroup;
		if (thjs && thjs->tokenJoin() == large)
		{
			dcf = thjs->getLargeKeys().size();
			largeSideRG = thjs->getLargeRowGroup();
		}

		// info for debug trace
		vector<string> tableNames;
		vector<string> traces;

		// sort the smallsides base on the joinId
		sort(smallSides.begin(), smallSides.end(), joinInfoCompare);
		int64_t lastJoinId = smallSides.back()->fJoinData.fJoinId;

		// get info to config the TupleHashjoin
		DataListVec smallSideDLs;
		vector<RowGroup> smallSideRGs;
		vector<JoinType> jointypes;
		vector<bool> typeless;
		vector<vector<uint32_t> > smallKeyIndices;
		vector<vector<uint32_t> > largeKeyIndices;
		for (vector<SP_JoinInfo>::iterator i = smallSides.begin(); i != smallSides.end(); i++)
		{
			JoinInfo* info = i->get();
			smallSideDLs.push_back(info->fDl);
			smallSideRGs.push_back(info->fRowGroup);
			jointypes.push_back(info->fJoinData.fTypes[0]);
			typeless.push_back(info->fJoinData.fTypeless);

			vector<uint32_t> smallIndices;
			vector<uint32_t> largeIndices;
			const vector<uint32_t>& keys1 = info->fJoinData.fLeftKeys;
			const vector<uint32_t>& keys2 = info->fJoinData.fRightKeys;
			vector<uint32_t>::const_iterator k1 = keys1.begin();
			vector<uint32_t>::const_iterator k2 = keys2.begin();
			tableNames.push_back(jobInfo.keyInfo->tupleKeyVec[*k1].fTable);
			for (; k1 != keys1.end(); ++k1, ++k2)
			{
				smallIndices.push_back(getKeyIndex(*k1, info->fRowGroup));
				largeIndices.push_back(getKeyIndex(*k2, largeSideRG));
			}

			smallKeyIndices.push_back(smallIndices);
			largeKeyIndices.push_back(largeIndices);

			if (jobInfo.trace)
			{
				// small side column
				ostringstream smallKey, smallIndex;
				string alias1 = jobInfo.keyInfo->tupleKeyVec[keys1.front()].fTable;
				smallKey << alias1 << "-";
				for (k1 = keys1.begin(); k1 != keys1.end(); ++k1)
				{
					CalpontSystemCatalog::OID oid1 = jobInfo.keyInfo->tupleKeyVec[*k1].fId;
					CalpontSystemCatalog::TableColName tcn1 = jobInfo.csc->colName(oid1);
					smallKey << "(" << tcn1.column << ":" << oid1 << ":" << *k1 << ")";
					smallIndex << " " << getKeyIndex(*k1,  info->fRowGroup);
				}
				// large side column
				ostringstream largeKey, largeIndex;
				string alias2 = jobInfo.keyInfo->tupleKeyVec[keys2.front()].fTable;
				largeKey << alias2 << "-";
				for (k2 = keys2.begin(); k2 != keys2.end(); ++k2)
				{
					CalpontSystemCatalog::OID oid2 = jobInfo.keyInfo->tupleKeyVec[*k2].fId;
					CalpontSystemCatalog::TableColName tcn2 = jobInfo.csc->colName(oid2);
					largeKey << "(" << tcn2.column << ":" << oid2 << ":" << *k2 << ")";
					largeIndex << " " << getKeyIndex(*k2, largeSideRG);
				}

				ostringstream oss;
				oss << smallKey.str() << " join on " << largeKey.str()
					<< " joinType: " << info->fJoinData.fTypes.front()
					<< "(" << joinTypeToString(info->fJoinData.fTypes.front()) << ")"
					<< (info->fJoinData.fTypeless ? " " : " !") << "typeless" << endl;
				oss << "smallSideIndex-largeSideIndex :" << smallIndex.str() << " --"
					<< largeIndex.str() << endl;
				oss << "small side RG" << endl << info->fRowGroup.toString() << endl;
				traces.push_back(oss.str());
			}
		}

		if (jobInfo.trace)
		{
			ostringstream oss;
			oss << "large side RG" << endl << largeSideRG.toString() << endl;
			traces.push_back(oss.str());
		}

		if (bps || tsas)
		{
			thjs= new TupleHashJoinStep(jobInfo);
			thjs->tableOid1(smallSides[0]->fTableOid);
			thjs->tableOid2(tableInfoMap[large].fTableOid);
			thjs->alias1(smallSides[0]->fAlias);
			thjs->alias2(tableInfoMap[large].fAlias);
			thjs->view1(smallSides[0]->fView);
			thjs->view2(tableInfoMap[large].fView);
			thjs->schema1(smallSides[0]->fSchema);
			thjs->schema2(tableInfoMap[large].fSchema);
			thjs->setLargeSideBPS(bps);
			thjs->joinId(lastJoinId);

			if (dynamic_cast<TupleBPS*>(bps) != NULL)
				bps->incWaitToRunStepCnt();

			SJSTEP spjs(thjs);

			JobStepAssociation inJsa;
			inJsa.outAdd(smallSideDLs, 0);
			inJsa.outAdd(tableInfoMap[large].fDl);
			thjs->inputAssociation(inJsa);
			thjs->setLargeSideDLIndex(inJsa.outSize() - 1);

			JobStepAssociation outJsa;
			AnyDataListSPtr spdl(new AnyDataList());
			RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
			spdl->rowGroupDL(dl);
			dl->OID(large);
			outJsa.outAdd(spdl);
			thjs->outputAssociation(outJsa);

			thjs->configSmallSideRG(smallSideRGs, tableNames);
			thjs->configLargeSideRG(tableInfoMap[large].fRowGroup);
			thjs->configJoinKeyIndex(jointypes, typeless, smallKeyIndices, largeKeyIndices);

			tableInfoMap[large].fQuerySteps.push_back(spjs);
			tableInfoMap[large].fDl = spdl;
		}
		else
		{
			JobStepAssociation inJsa = thjs->inputAssociation();
			if (inJsa.outSize() < 2)
				throw runtime_error("Not enough input to a hashjoin.");
			size_t last = inJsa.outSize() - 1;
			inJsa.outAdd(smallSideDLs, last);
			thjs->inputAssociation(inJsa);
			thjs->setLargeSideDLIndex(inJsa.outSize() - 1);

			thjs->addSmallSideRG(smallSideRGs, tableNames);
			thjs->addJoinKeyIndex(jointypes, typeless, smallKeyIndices, largeKeyIndices);
		}

		RowGroup rg;
		constructJoinedRowGroup(rg, link, prevLarge, root, tableSet, tableInfoMap, jobInfo);
		thjs->setOutputRowGroup(rg);
		tableInfoMap[large].fRowGroup = rg;
		if (jobInfo.trace)
		{
			cout << boldStart  << "\n====== join info ======\n" << boldStop;
			for (vector<string>::iterator t = traces.begin(); t != traces.end(); ++t)
				cout << *t;
			cout << "RowGroup join result: " << endl << rg.toString() << endl << endl;
		}

		// check if any cross-table expressions can be evaluated after the join
		JobStepVector readyExpSteps;
		JobStepVector& expSteps = jobInfo.crossTableExpressions;
		JobStepVector::iterator eit = expSteps.begin();
		while (eit != expSteps.end())
		{
			ExpressionStep* exp = dynamic_cast<ExpressionStep*>(eit->get());
			if (exp == NULL)
				throw runtime_error("Not an expression.");
			const vector<uint32_t>& tables = exp->tableKeys();
			uint64_t i = 0;
			for (; i < tables.size(); i++)
			{
				if (tableSet.find(tables[i]) == tableSet.end())
					break;
			}

			// all tables for this expression are joined
			if (tables.size() ==  i)
			{
				readyExpSteps.push_back(*eit);
				eit = expSteps.erase(eit);
			}
			else
			{
				eit++;
			}
		}

		// if root, handle the delayed outer join filters
		if (root && jobInfo.outerJoinExpressions.size() > 0)
			readyExpSteps.insert(readyExpSteps.end(),
								 jobInfo.outerJoinExpressions.begin(),
								 jobInfo.outerJoinExpressions.end());

		// check additional compares for semi-join
		if (readyExpSteps.size() > 0)
		{
			map<uint32_t, uint32_t> keyToIndexMap; // map keys to the indices in the RG
			for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
				keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));

			// tables have additional comparisons
			map<uint32_t, int> correlateTables;          // index in thjs
			map<uint32_t, ParseTree*> correlateCompare;  // expression
			for (size_t i = 0; i != smallSides.size(); i++)
			{
				if ((jointypes[i] & SEMI) || (jointypes[i] & ANTI) || (jointypes[i] & SCALAR))
				{
					uint32_t  tid = getTableKey(jobInfo,
											smallSides[i]->fTableOid,
											smallSides[i]->fAlias,
											smallSides[i]->fSchema,
											smallSides[i]->fView);
					correlateTables[tid] = i;
					correlateCompare[tid] = NULL;
				}
			}

			if (correlateTables.size() > 0)
			{
				// separate additional compare for each table pair
				JobStepVector::iterator eit = readyExpSteps.begin();
				while (eit != readyExpSteps.end())
				{
					ExpressionStep* e = dynamic_cast<ExpressionStep*>(eit->get());
					if (e->selectFilter())
					{
						// @bug3780, leave select filter to normal expression
						eit++;
						continue;
					}

					const vector<uint32_t>& tables = e->tableKeys();
					map<uint32_t, int>::iterator j = correlateTables.end();

					for (size_t i = 0; i < tables.size(); i++)
					{
						j = correlateTables.find(tables[i]);
						if (j != correlateTables.end())
							break;
					}

					if (j == correlateTables.end())
					{
						eit++;
						continue;
					}

					// map the input column index
					e->updateInputIndex(keyToIndexMap, jobInfo);
					ParseTree* pt = correlateCompare[j->first];
					if (pt == NULL)
					{
						// first expression
						pt = new ParseTree;
						pt->copyTree(*(e->expressionFilter()));
					}
					else
					{
						// combine the expressions
						ParseTree* left = pt;
						ParseTree* right = new ParseTree;
						right->copyTree(*(e->expressionFilter()));
						pt = new ParseTree(new LogicOperator("and"));
						pt->left(left);
						pt->right(right);
					}

					correlateCompare[j->first] = pt;
					eit = readyExpSteps.erase(eit);
				}

				map<uint32_t, int>::iterator k = correlateTables.begin();
				while (k != correlateTables.end())
				{
					ParseTree* pt = correlateCompare[k->first];
					if (pt != NULL)
					{
						boost::shared_ptr<ParseTree> sppt(pt);
						thjs->addJoinFilter(sppt, dcf + k->second);
					}

					k++;
				}

				thjs->setJoinFilterInputRG(rg);
			}

			// normal expression if any
			if (readyExpSteps.size() > 0)
			{
				// add the expression steps in where clause can be solved by this join to bps
				ParseTree* pt = NULL;
				JobStepVector::iterator eit = readyExpSteps.begin();
				for (; eit != readyExpSteps.end(); eit++)
				{
					// map the input column index
					ExpressionStep* e = dynamic_cast<ExpressionStep*>(eit->get());
					e->updateInputIndex(keyToIndexMap, jobInfo);

					if (pt == NULL)
					{
						// first expression
						pt = new ParseTree;
						pt->copyTree(*(e->expressionFilter()));
					}
					else
					{
						// combine the expressions
						ParseTree* left = pt;
						ParseTree* right = new ParseTree;
						right->copyTree(*(e->expressionFilter()));
						pt = new ParseTree(new LogicOperator("and"));
						pt->left(left);
						pt->right(right);
					}
				}

				boost::shared_ptr<ParseTree> sppt(pt);
				thjs->addFcnExpGroup2(sppt);
			}

			// update the fColsInExp2 and construct the output RG
			updateExp2Cols(readyExpSteps, tableInfoMap, jobInfo);
			constructJoinedRowGroup(rg, link, prevLarge, root, tableSet, tableInfoMap, jobInfo);
			if (thjs->hasFcnExpGroup2())
				thjs->setFE23Output(rg);
			else
				thjs->setOutputRowGroup(rg);
			tableInfoMap[large].fRowGroup = rg;
			if (jobInfo.trace)
			{
				cout << "RowGroup of " << tableInfoMap[large].fAlias << " after EXP G2: " << endl
					 << rg.toString() << endl << endl;
			}
		}
	}


	// Prepare the current table info to join with its large side.
	SP_JoinInfo joinInfo(new JoinInfo);
	joinInfo->fTableOid = tableInfoMap[large].fTableOid;
	joinInfo->fAlias = tableInfoMap[large].fAlias;
	joinInfo->fView = tableInfoMap[large].fView;
	joinInfo->fSchema = tableInfoMap[large].fSchema;

	joinInfo->fDl = tableInfoMap[large].fDl;
	joinInfo->fRowGroup = tableInfoMap[large].fRowGroup;

	if (root == false)  // not root
	{
		TableJoinMap::iterator mit = jobInfo.tableJoinMap.find(make_pair(link, prevLarge));
		if (mit == jobInfo.tableJoinMap.end())
			throw runtime_error("Join step not found.");

		joinInfo->fJoinData = mit->second;
	}

	joinOrder.push_back(large);

	return joinInfo;
}


bool joinStepCompare(const SJSTEP& a, const SJSTEP& b)
{
	return (dynamic_cast<TupleHashJoinStep*>(a.get())->joinId() <
			dynamic_cast<TupleHashJoinStep*>(b.get())->joinId());
}


struct JoinOrderData
{
	uint32_t fTid1;
	uint32_t fTid2;
	uint32_t fJoinId;
};


void getJoinOrder(vector<JoinOrderData>& joins, JobStepVector& joinSteps, JobInfo& jobInfo)
{
	sort(joinSteps.begin(), joinSteps.end(), joinStepCompare);
	for (JobStepVector::iterator i = joinSteps.begin(); i < joinSteps.end(); i++)
	{
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
		JoinOrderData jo;
		jo.fTid1 = getTableKey(jobInfo, thjs->tupleId1());
		jo.fTid2 = getTableKey(jobInfo, thjs->tupleId2());
		jo.fJoinId = thjs->joinId();

		// not fastest, but good for a small list
		vector<JoinOrderData>::iterator j;
		for (j = joins.begin(); j < joins.end(); j++)
		{
			if ((j->fTid1 == jo.fTid1 && j->fTid2 == jo.fTid2) ||
				(j->fTid1 == jo.fTid2 && j->fTid2 == jo.fTid1))
			{
				j->fJoinId = jo.fJoinId;
				break;
			}
		}

		// insert unique join pair
		if (j == joins.end())
			joins.push_back(jo);
	}
}

inline void updateJoinSides(uint32_t small, uint32_t large, map<uint32_t, SP_JoinInfo>& joinInfoMap,
		vector<SP_JoinInfo>& smallSides, TableInfoMap& tableInfoMap, JobInfo& jobInfo)
{
	TableJoinMap::iterator mit = jobInfo.tableJoinMap.find(make_pair(small, large));
	if (mit == jobInfo.tableJoinMap.end())
		throw runtime_error("Join step not found.");
	joinInfoMap[small]->fJoinData = mit->second;
	tableInfoMap[small].fJoinedTables.insert(small);
	smallSides.push_back(joinInfoMap[small]);

	tableInfoMap[large].fJoinedTables.insert(
		tableInfoMap[small].fJoinedTables.begin(), tableInfoMap[small].fJoinedTables.end());
	tableInfoMap[large].fJoinedTables.insert(large);
}


// For OUTER JOIN bug @2422/2633/3437/3759, join table based on join order.
// The largest table will be always the streaming table, other tables are always on small side.
void joinTablesInOrder(uint32_t largest, JobStepVector& joinSteps, TableInfoMap& tableInfoMap,
						JobInfo& jobInfo, vector<uint32_t>& joinOrder)
{
	// populate the tableInfo for join
	map<uint32_t, SP_JoinInfo> joinInfoMap;          // <table, JoinInfo>

	// <table,  <last step involved, large priority> >
	// large priority:
	//     -1 - must be on small side, like derived tables for semi join;
	//      0 - prefer to be on small side, like FROM subquery;
	//      1 - can be on either large or small side;
	//      2 - must be on large side.
	map<uint32_t, pair<SJSTEP, int> > joinStepMap;

	BatchPrimitive* bps = NULL;
	SubAdapterStep* tsas = NULL;
	TupleHashJoinStep* thjs = NULL;
	for (vector<uint32_t>::iterator i = jobInfo.tableList.begin(); i < jobInfo.tableList.end(); i++)
	{
		SP_JoinInfo joinInfo(new JoinInfo);
		joinInfo->fTableOid = tableInfoMap[*i].fTableOid;
		joinInfo->fAlias = tableInfoMap[*i].fAlias;
		joinInfo->fView = tableInfoMap[*i].fView;
		joinInfo->fSchema = tableInfoMap[*i].fSchema;

		joinInfo->fDl = tableInfoMap[*i].fDl;
		joinInfo->fRowGroup = tableInfoMap[*i].fRowGroup;

		joinInfoMap[*i] = joinInfo;

		SJSTEP spjs = tableInfoMap[*i].fQuerySteps.back();
		bps = dynamic_cast<BatchPrimitive*>(spjs.get());
		tsas = dynamic_cast<SubAdapterStep*>(spjs.get());
		thjs = dynamic_cast<TupleHashJoinStep*>(spjs.get());
		TupleBPS* tbps = dynamic_cast<TupleBPS*>(spjs.get());
		if (*i == largest)
			joinStepMap[*i] = make_pair(spjs, 2);
		else if (tbps || thjs)
			joinStepMap[*i] = make_pair(spjs, 1);
		else if (tsas)
			joinStepMap[*i] = make_pair(spjs, 0);
		else
			joinStepMap[*i] = make_pair(spjs, -1);
	}

	// sort the join steps based on join ID.
	vector<JoinOrderData> joins;
	getJoinOrder(joins, joinSteps, jobInfo);

	// join the steps
	int64_t lastJoinId = -1;
	uint32_t large = (uint32_t) -1;
	uint32_t small = (uint32_t) -1;
	uint32_t prevLarge = (uint32_t) -1;
	bool umstream = false;
	vector<uint32_t> joinedTable;
	uint32_t lastJoin = joins.size() - 1;
	bool isSemijoin = true;

	for (uint64_t js = 0; js < joins.size(); js++)
	{
		set<uint32_t> smallSideTid;

		if (joins[js].fJoinId != 0)
			isSemijoin = false;

		vector<SP_JoinInfo> smallSides;
		uint32_t tid1 = joins[js].fTid1;
		uint32_t tid2 = joins[js].fTid2;
		lastJoinId = joins[js].fJoinId;

		// largest has already joined, and this join cannot be merged.
		if (prevLarge == largest && tid1 != largest && tid2 != largest)
			umstream = true;

		if (joinStepMap[tid1].second > joinStepMap[tid2].second) // high priority
		{
			large = tid1;
			small = tid2;
		}
		else if (joinStepMap[tid1].second == joinStepMap[tid2].second &&
				 jobInfo.tableSize[tid1] >= jobInfo.tableSize[tid2]) // favor t1 for hint
		{
			large = tid1;
			small = tid2;
		}
		else
		{
			large = tid2;
			small = tid1;
		}

		updateJoinSides(small, large, joinInfoMap, smallSides, tableInfoMap, jobInfo);
		if (find(joinedTable.begin(), joinedTable.end(), small) == joinedTable.end())
			joinedTable.push_back(small);

		smallSideTid.insert(small);

		// merge in the next step if the large side is the same
		for (uint64_t ns  = js + 1; ns < joins.size(); js++, ns++)
		{
			uint32_t tid1 = joins[ns].fTid1;
			uint32_t tid2 = joins[ns].fTid2;
			uint32_t small = (uint32_t) -1;

			if ((tid1 == large) &&
				((joinStepMap[tid1].second > joinStepMap[tid2].second) ||
				 (joinStepMap[tid1].second == joinStepMap[tid2].second &&
				  jobInfo.tableSize[tid1] >= jobInfo.tableSize[tid2])))
			{
				small = tid2;
			}
			else if ((tid2 == large) &&
					 ((joinStepMap[tid2].second > joinStepMap[tid1].second) ||
					  (joinStepMap[tid2].second == joinStepMap[tid1].second &&
					   jobInfo.tableSize[tid2] >= jobInfo.tableSize[tid1])))
			{
				small = tid1;
			}
			else
			{
				break;
			}

			// check if FE needs table in previous smallsides
			if (jobInfo.joinFeTableMap[joins[ns].fJoinId].size() > 0)
			{
				set<uint32_t>& tids = jobInfo.joinFeTableMap[joins[ns].fJoinId];
				for (set<uint32_t>::iterator si = smallSideTid.begin(); si != smallSideTid.end(); si++)
				{
					if (tids.find(*si) != tids.end())
						throw runtime_error("On clause filter involving a table not directly involved in the outer join is currently not supported.");
				}
			}

			updateJoinSides(small, large, joinInfoMap, smallSides, tableInfoMap, jobInfo);
			lastJoinId = joins[ns].fJoinId;
			if (find(joinedTable.begin(), joinedTable.end(), small) == joinedTable.end())
				joinedTable.push_back(small);
			smallSideTid.insert(small);
		}

		joinedTable.push_back(large);

		SJSTEP spjs = joinStepMap[large].first;
		bps = dynamic_cast<BatchPrimitive*>(spjs.get());
		tsas = dynamic_cast<SubAdapterStep*>(spjs.get());
		thjs = dynamic_cast<TupleHashJoinStep*>(spjs.get());
		if (!bps && !thjs && !tsas)
		{
			if (dynamic_cast<SubQueryStep*>(spjs.get()))
				throw IDBExcept(ERR_NON_SUPPORT_SUB_QUERY_TYPE);
			throw runtime_error("Not supported join.");
		}

		size_t startPos = 0; // start point to add new smallsides
		RowGroup largeSideRG = joinInfoMap[large]->fRowGroup;
		if (thjs && thjs->tokenJoin() == large)
			largeSideRG = thjs->getLargeRowGroup();

		// get info to config the TupleHashjoin
		vector<string> traces;
		vector<string> tableNames;
		DataListVec smallSideDLs;
		vector<RowGroup> smallSideRGs;
		vector<JoinType> jointypes;
		vector<bool> typeless;
		vector<vector<uint32_t> > smallKeyIndices;
		vector<vector<uint32_t> > largeKeyIndices;

		// bug5764, make sure semi joins acting as filter is after outer join.
		{
			// the inner table filters have to be performed after outer join
			vector<uint64_t> semijoins;
			vector<uint64_t> smallouts;
			for (size_t i = 0; i < smallSides.size(); i++)
			{
				// find the the small-outer and semi-join joins
				JoinType jt = smallSides[i]->fJoinData.fTypes[0];
				if (jt & SMALLOUTER)
					smallouts.push_back(i);
				else if (jt & (SEMI | ANTI | SCALAR | CORRELATED))
					semijoins.push_back(i);
			}

			// check the join order, re-arrange if necessary
			if (smallouts.size() > 0 && semijoins.size() > 0)
			{
				uint64_t lastSmallOut = smallouts.back();
				uint64_t lastSemijoin = semijoins.back();
				if (lastSmallOut > lastSemijoin)
				{
					vector<SP_JoinInfo> temp1;
					vector<SP_JoinInfo> temp2;
					size_t j = 0;
					for (size_t i = 0; i < smallSides.size(); i++)
					{
						if (j < semijoins.size() && i == semijoins[j])
						{
							temp1.push_back(smallSides[i]);
							j++;
						}
						else
						{
							temp2.push_back(smallSides[i]);
						}

						if (i == lastSmallOut)
							temp2.insert(temp2.end(), temp1.begin(), temp1.end());
					}

					smallSides = temp2;
				}
			}
		}

		for (vector<SP_JoinInfo>::iterator i = smallSides.begin(); i != smallSides.end(); i++)
		{
			JoinInfo* info = i->get();
			smallSideDLs.push_back(info->fDl);
			smallSideRGs.push_back(info->fRowGroup);
			jointypes.push_back(info->fJoinData.fTypes[0]);
			typeless.push_back(info->fJoinData.fTypeless);

			vector<uint32_t> smallIndices;
			vector<uint32_t> largeIndices;
			const vector<uint32_t>& keys1 = info->fJoinData.fLeftKeys;
			const vector<uint32_t>& keys2 = info->fJoinData.fRightKeys;
			vector<uint32_t>::const_iterator k1 = keys1.begin();
			vector<uint32_t>::const_iterator k2 = keys2.begin();
			tableNames.push_back(jobInfo.keyInfo->tupleKeyVec[*k1].fTable);
			for (; k1 != keys1.end(); ++k1, ++k2)
			{
				smallIndices.push_back(getKeyIndex(*k1, info->fRowGroup));
				largeIndices.push_back(getKeyIndex(*k2, largeSideRG));
			}

			smallKeyIndices.push_back(smallIndices);
			largeKeyIndices.push_back(largeIndices);

			if (jobInfo.trace)
			{
				// small side column
				ostringstream smallKey, smallIndex;
				string alias1 = jobInfo.keyInfo->tupleKeyVec[keys1.front()].fTable;
				smallKey << alias1 << "-";
				for (k1 = keys1.begin(); k1 != keys1.end(); ++k1)
				{
					CalpontSystemCatalog::OID oid1 = jobInfo.keyInfo->tupleKeyVec[*k1].fId;
					CalpontSystemCatalog::TableColName tcn1 = jobInfo.csc->colName(oid1);
					smallKey << "(" << tcn1.column << ":" << oid1 << ":" << *k1 << ")";
					smallIndex << " " << getKeyIndex(*k1,  info->fRowGroup);
				}
				// large side column
				ostringstream largeKey, largeIndex;
				string alias2 = jobInfo.keyInfo->tupleKeyVec[keys2.front()].fTable;
				largeKey << alias2 << "-";
				for (k2 = keys2.begin(); k2 != keys2.end(); ++k2)
				{
					CalpontSystemCatalog::OID oid2 = jobInfo.keyInfo->tupleKeyVec[*k2].fId;
					CalpontSystemCatalog::TableColName tcn2 = jobInfo.csc->colName(oid2);
					largeKey << "(" << tcn2.column << ":" << oid2 << ":" << *k2 << ")";
					largeIndex << " " << getKeyIndex(*k2, largeSideRG);
				}

				ostringstream oss;
				oss << smallKey.str() << " join on " << largeKey.str()
					<< " joinType: " << info->fJoinData.fTypes.front()
					<< "(" << joinTypeToString(info->fJoinData.fTypes.front()) << ")"
					<< (info->fJoinData.fTypeless ? " " : " !") << "typeless" << endl;
				oss << "smallSideIndex-largeSideIndex :" << smallIndex.str() << " --"
					<< largeIndex.str() << endl;
				oss << "small side RG" << endl << info->fRowGroup.toString() << endl;
				traces.push_back(oss.str());
			}
		}

		if (jobInfo.trace)
		{
			ostringstream oss;
			oss << "large side RG" << endl << largeSideRG.toString() << endl;
			traces.push_back(oss.str());
		}

		if (bps || tsas || umstream || (thjs && joinStepMap[large].second < 1))
		{
			thjs= new TupleHashJoinStep(jobInfo);
			thjs->tableOid1(smallSides[0]->fTableOid);
			thjs->tableOid2(tableInfoMap[large].fTableOid);
			thjs->alias1(smallSides[0]->fAlias);
			thjs->alias2(tableInfoMap[large].fAlias);
			thjs->view1(smallSides[0]->fView);
			thjs->view2(tableInfoMap[large].fView);
			thjs->schema1(smallSides[0]->fSchema);
			thjs->schema2(tableInfoMap[large].fSchema);
			thjs->setLargeSideBPS(bps);
			thjs->joinId(lastJoinId);

			if (dynamic_cast<TupleBPS*>(bps) != NULL)
				bps->incWaitToRunStepCnt();

			spjs.reset(thjs);

			JobStepAssociation inJsa;
			inJsa.outAdd(smallSideDLs, 0);
			inJsa.outAdd(joinInfoMap[large]->fDl);
			thjs->inputAssociation(inJsa);
			thjs->setLargeSideDLIndex(inJsa.outSize() - 1);

			JobStepAssociation outJsa;
			AnyDataListSPtr spdl(new AnyDataList());
			RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
			spdl->rowGroupDL(dl);
			dl->OID(large);
			outJsa.outAdd(spdl);
			thjs->outputAssociation(outJsa);

			thjs->configSmallSideRG(smallSideRGs, tableNames);
			thjs->configLargeSideRG(joinInfoMap[large]->fRowGroup);
			thjs->configJoinKeyIndex(jointypes, typeless, smallKeyIndices, largeKeyIndices);

			tableInfoMap[large].fQuerySteps.push_back(spjs);
			tableInfoMap[large].fDl = spdl;
		}
		else // thjs && joinStepMap[large].second >= 1
		{
			JobStepAssociation inJsa = thjs->inputAssociation();
			if (inJsa.outSize() < 2)
				throw runtime_error("Not enough input to a hashjoin.");
			startPos = inJsa.outSize() - 1;
			inJsa.outAdd(smallSideDLs, startPos);
			thjs->inputAssociation(inJsa);
			thjs->setLargeSideDLIndex(inJsa.outSize() - 1);

			thjs->addSmallSideRG(smallSideRGs, tableNames);
			thjs->addJoinKeyIndex(jointypes, typeless, smallKeyIndices, largeKeyIndices);
		}

		RowGroup rg;
		set<uint32_t>& tableSet = tableInfoMap[large].fJoinedTables;
		constructJoinedRowGroup(rg, tableSet, tableInfoMap, jobInfo);
		thjs->setOutputRowGroup(rg);
		tableInfoMap[large].fRowGroup = rg;
		tableSet.insert(large);
		if (jobInfo.trace)
		{
			cout << boldStart  << "\n====== join info ======\n" << boldStop;
			for (vector<string>::iterator t = traces.begin(); t != traces.end(); ++t)
				cout << *t;
			cout << "RowGroup join result: " << endl << rg.toString() << endl << endl;
		}

		// on clause filter association
		map<uint64_t, size_t> joinIdIndexMap;
		for (size_t i = 0; i < smallSides.size(); i++)
		{
			if (smallSides[i]->fJoinData.fJoinId > 0)
				joinIdIndexMap[smallSides[i]->fJoinData.fJoinId] = i;
		}

		// check if any cross-table expressions can be evaluated after the join
		JobStepVector readyExpSteps;
		JobStepVector& expSteps = jobInfo.crossTableExpressions;
		JobStepVector::iterator eit = expSteps.begin();
		while (eit != expSteps.end())
		{
			ExpressionStep* exp = dynamic_cast<ExpressionStep*>(eit->get());
			if (exp == NULL)
				throw runtime_error("Not an expression.");
			const vector<uint32_t>& tables = exp->tableKeys();
			uint64_t i = 0;
			for (; i < tables.size(); i++)
			{
				if (tableInfoMap[large].fJoinedTables.find(tables[i]) ==
						tableInfoMap[large].fJoinedTables.end())
					break;
			}

			// all tables for this expression are joined
			bool ready = (tables.size() == i);

			// for on clause condition, need check join ID
			if (ready && exp->associatedJoinId() != 0)
			{
				map<uint64_t, size_t>::iterator x = joinIdIndexMap.find(exp->associatedJoinId());
				ready = (x != joinIdIndexMap.end());
			}

			if (ready)
			{
				readyExpSteps.push_back(*eit);
				eit = expSteps.erase(eit);
			}
			else
			{
				eit++;
			}
		}

		// if last join step, handle the delayed outer join filters
		if (js == lastJoin && jobInfo.outerJoinExpressions.size() > 0)
			readyExpSteps.insert(readyExpSteps.end(),
								 jobInfo.outerJoinExpressions.begin(),
								 jobInfo.outerJoinExpressions.end());

		// check additional compares for semi-join
		if (readyExpSteps.size() > 0)
		{
			map<uint32_t, uint32_t> keyToIndexMap; // map keys to the indices in the RG
			for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
				keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));

			// tables have additional comparisons
			map<uint32_t, int> correlateTables;          // index in thjs
			map<uint32_t, ParseTree*> correlateCompare;  // expression
			for (size_t i = 0; i != smallSides.size(); i++)
			{
				if ((jointypes[i] & SEMI) || (jointypes[i] & ANTI) || (jointypes[i] & SCALAR))
				{
					uint32_t  tid = getTableKey(jobInfo,
											smallSides[i]->fTableOid,
											smallSides[i]->fAlias,
											smallSides[i]->fSchema,
											smallSides[i]->fView);
					correlateTables[tid] = i;
					correlateCompare[tid] = NULL;
				}
			}

			if (correlateTables.size() > 0)
			{
				// separate additional compare for each table pair
				JobStepVector::iterator eit = readyExpSteps.begin();
				while (eit != readyExpSteps.end())
				{
					ExpressionStep* e = dynamic_cast<ExpressionStep*>(eit->get());
					if (e->selectFilter())
					{
						// @bug3780, leave select filter to normal expression
						eit++;
						continue;
					}

					const vector<uint32_t>& tables = e->tableKeys();
					map<uint32_t, int>::iterator j = correlateTables.end();

					for (size_t i = 0; i < tables.size(); i++)
					{
						j = correlateTables.find(tables[i]);
						if (j != correlateTables.end())
							break;
					}

					if (j == correlateTables.end())
					{
						eit++;
						continue;
					}

					// map the input column index
					e->updateInputIndex(keyToIndexMap, jobInfo);
					ParseTree* pt = correlateCompare[j->first];
					if (pt == NULL)
					{
						// first expression
						pt = new ParseTree;
						pt->copyTree(*(e->expressionFilter()));
					}
					else
					{
						// combine the expressions
						ParseTree* left = pt;
						ParseTree* right = new ParseTree;
						right->copyTree(*(e->expressionFilter()));
						pt = new ParseTree(new LogicOperator("and"));
						pt->left(left);
						pt->right(right);
					}

					correlateCompare[j->first] = pt;
					eit = readyExpSteps.erase(eit);
				}

				map<uint32_t, int>::iterator k = correlateTables.begin();
				while (k != correlateTables.end())
				{
					ParseTree* pt = correlateCompare[k->first];
					if (pt != NULL)
					{
						boost::shared_ptr<ParseTree> sppt(pt);
						thjs->addJoinFilter(sppt, startPos + k->second);
					}

					k++;
				}

				thjs->setJoinFilterInputRG(rg);
			}

			// normal expression if any
			if (readyExpSteps.size() > 0)
			{
				// add the expression steps in where clause can be solved by this join to bps
				ParseTree* pt = NULL;
				JobStepVector::iterator eit = readyExpSteps.begin();
				for (; eit != readyExpSteps.end(); eit++)
				{
					// map the input column index
					ExpressionStep* e = dynamic_cast<ExpressionStep*>(eit->get());
					e->updateInputIndex(keyToIndexMap, jobInfo);

					// short circuit on clause expressions
					map<uint64_t, size_t>::iterator x = joinIdIndexMap.find(e->associatedJoinId());
					if (x != joinIdIndexMap.end())
					{
						ParseTree* joinFilter = new ParseTree;
						joinFilter->copyTree(*(e->expressionFilter()));
						boost::shared_ptr<ParseTree> sppt(joinFilter);
						thjs->addJoinFilter(sppt, startPos + x->second);
						thjs->setJoinFilterInputRG(rg);
						continue;
					}

					if (pt == NULL)
					{
						// first expression
						pt = new ParseTree;
						pt->copyTree(*(e->expressionFilter()));
					}
					else
					{
						// combine the expressions
						ParseTree* left = pt;
						ParseTree* right = new ParseTree;
						right->copyTree(*(e->expressionFilter()));
						pt = new ParseTree(new LogicOperator("and"));
						pt->left(left);
						pt->right(right);
					}
				}

				if (pt != NULL)
				{
					boost::shared_ptr<ParseTree> sppt(pt);
					thjs->addFcnExpGroup2(sppt);
				}
			}

			// update the fColsInExp2 and construct the output RG
			updateExp2Cols(readyExpSteps, tableInfoMap, jobInfo);
			constructJoinedRowGroup(rg, tableSet, tableInfoMap, jobInfo);
			if (thjs->hasFcnExpGroup2())
				thjs->setFE23Output(rg);
			else
				thjs->setOutputRowGroup(rg);
			tableInfoMap[large].fRowGroup = rg;
			if (jobInfo.trace)
			{
				cout << "RowGroup of " << tableInfoMap[large].fAlias << " after EXP G2: " << endl
					 << rg.toString() << endl << endl;
			}
		}

		// update the info maps
		int l = (joinStepMap[large].second == 2) ? 2 : 0;
		if (isSemijoin)
			joinStepMap[large] = make_pair(spjs, joinStepMap[large].second);
		else
			joinStepMap[large] = make_pair(spjs, l);

		for (set<uint32_t>::iterator i = tableSet.begin(); i != tableSet.end(); i++)
		{
			joinInfoMap[*i]->fDl = tableInfoMap[large].fDl;
			joinInfoMap[*i]->fRowGroup = tableInfoMap[large].fRowGroup;

			if (*i != large)
			{
				//@bug6117, token should be done for small side tables.
				SJSTEP smallJs = joinStepMap[*i].first;
				TupleHashJoinStep* smallThjs = dynamic_cast<TupleHashJoinStep*>(smallJs.get());
				if (smallThjs && smallThjs->tokenJoin())
					smallThjs->tokenJoin(-1);

				// Set join priority for smallsides.
				joinStepMap[*i] = make_pair(spjs, l);

				// Mark joined tables, smalls and large, as a group.
				tableInfoMap[*i].fJoinedTables = tableInfoMap[large].fJoinedTables;
			}
		}

		prevLarge = large;
	}

	// Keep join order by the table last used for picking the right delivery step.
	{
		for (vector<uint32_t>::reverse_iterator i = joinedTable.rbegin(); i < joinedTable.rend(); i++)
		{
			if (find(joinOrder.begin(), joinOrder.end(), *i) == joinOrder.end())
				joinOrder.push_back(*i);
		}

		const uint64_t n = joinOrder.size();
		const uint64_t h = n / 2;
		const uint64_t e = n - 1;
		for (uint64_t i = 0; i < h; i++)
			std::swap(joinOrder[i], joinOrder[e - i]);
	}
}


inline void joinTables(JobStepVector& joinSteps, TableInfoMap& tableInfoMap, JobInfo& jobInfo,
						vector<uint32_t>& joinOrder, const bool overrideLargeSideEstimate)
{
	uint32_t largestTable = getLargestTable(jobInfo, tableInfoMap, overrideLargeSideEstimate);
	if (jobInfo.outerOnTable.size() == 0)
		joinToLargeTable(largestTable, tableInfoMap, jobInfo, joinOrder);
	else
		joinTablesInOrder(largestTable, joinSteps, tableInfoMap, jobInfo, joinOrder);
}


void makeNoTableJobStep(JobStepVector& querySteps, JobStepVector& projectSteps,
						DeliveredTableMap& deliverySteps, JobInfo& jobInfo)
{
	querySteps.clear();
	projectSteps.clear();
	deliverySteps.clear();
	querySteps.push_back(TupleConstantStep::addConstantStep(jobInfo));
	deliverySteps[CNX_VTABLE_ID] = querySteps.back();
}


}


namespace joblist
{
void associateTupleJobSteps(JobStepVector& querySteps, JobStepVector& projectSteps,
							DeliveredTableMap& deliverySteps, JobInfo& jobInfo,
							const bool overrideLargeSideEstimate)
{
	if (jobInfo.trace)
	{
		const boost::shared_ptr<TupleKeyInfo>& keyInfo = jobInfo.keyInfo;
		cout << "query steps:" << endl;
		for (JobStepVector::iterator i = querySteps.begin(); i != querySteps.end(); ++i)
		{
			TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
			if (thjs == NULL)
			{
				int64_t id = ((*i)->tupleId() != (uint64_t) -1) ? (*i)->tupleId() : -1;
				cout << typeid(*(i->get())).name() << ": " << (*i)->oid() << " " << id << " "
					 << (int)((id != -1) ? getTableKey(jobInfo, id) : -1) << endl;
			}
			else
			{
				int64_t id1 = (thjs->tupleId1() != (uint64_t) -1) ? thjs->tupleId1() : -1;
				int64_t id2 = (thjs->tupleId2() != (uint64_t) -1) ? thjs->tupleId2() : -1;
				cout << typeid(*thjs).name() << ": " << thjs->oid1() << " " << id1 << " "
					 << (int)((id1 != -1) ? getTableKey(jobInfo, id1) : -1) << " - "
					 << thjs->getJoinType() << " - " << thjs->oid2() << " " << id2 << " "
					 << (int)((id2 != -1) ? getTableKey(jobInfo, id2) : -1) << endl;
			}
		}
		cout << "project steps:" << endl;
		for (JobStepVector::iterator i = projectSteps.begin(); i != projectSteps.end(); ++i)
		{
			cout << typeid(*(i->get())).name() << ": " << (*i)->oid() << " "
				 << (*i)->tupleId() << " " << getTableKey(jobInfo, (*i)->tupleId()) << endl;
		}
		cout << "delivery steps:" << endl;
		for (DeliveredTableMap::iterator i = deliverySteps.begin(); i != deliverySteps.end(); ++i)
			cout << typeid(*(i->second.get())).name() << endl;

		TupleInfoMap::iterator iter = keyInfo->tupleInfoMap.begin();
		cout << "\nTupleInfoMap:  (tupleKey  oid  tableName view sub)" << endl;
		while (iter != keyInfo->tupleInfoMap.end())
		{
			string alias = keyInfo->tupleKeyVec[iter->first].fTable;
			if (alias.length() < 1) alias = "N/A";
			string view  =  keyInfo->tupleKeyVec[iter->first].fView;
			if (view.length() < 1) view = "N/A";
			int sid = keyInfo->tupleKeyVec[iter->first].fSubId;
			cout << iter->first << "\t" << keyInfo->tupleKeyVec[iter->first].fId
				 << "\t" << alias << "\t" << view << "\t" << hex << sid << dec << endl;
			++iter;
		}

		cout << "\nTupleKey vector:  (tupleKey  oid  name  alias view sub)" << endl;
		for (uint32_t i = 0; i < keyInfo->tupleKeyVec.size(); ++i)
		{
			CalpontSystemCatalog::OID oid = keyInfo->tupleKeyVec[i].fId;
			string alias = keyInfo->tupleKeyVec[i].fTable;
			if (alias.length() < 1) alias = "N/A";
			// Expression IDs are borrowed from systemcatalog IDs, which are not used in tuple.
			string name = keyInfo->keyName[i];
			if (keyInfo->dictOidToColOid.find(oid) != keyInfo->dictOidToColOid.end())
			{
				name += "[d]";  // indicate this is a dictionary column
			}
			if (jobInfo.keyInfo->pseudoType[i] > 0)
			{
				name += "[p]";  // indicate this is a pseudo column
			}
			if (name.empty())
			{
				name = "unknown";
			}
			string view = keyInfo->tupleKeyVec[i].fView;
			if (view.length() < 1) view = "N/A";
			int sid = keyInfo->tupleKeyVec[i].fSubId;
			cout << i << "\t" << oid << "\t" << name << "\t" << alias << "\t" << view
				 << "\t" << hex << sid << dec << endl;
		}
		cout << endl;
	}


	// @bug 2771, handle no table select query
	if (jobInfo.tableList.size() < 1)
	{
		makeNoTableJobStep(querySteps, projectSteps, deliverySteps, jobInfo);
		return;
	}

	// Create a step vector for each table in the from clause.
	TableInfoMap tableInfoMap;
	for (uint64_t i = 0; i < jobInfo.tableList.size(); i++)
	{
		uint32_t tableUid = jobInfo.tableList[i];
		tableInfoMap[tableUid] = TableInfo();
		tableInfoMap[tableUid].fTableOid = jobInfo.keyInfo->tupleKeyVec[tableUid].fId;
		tableInfoMap[tableUid].fName = jobInfo.keyInfo->keyName[tableUid];
		tableInfoMap[tableUid].fAlias = jobInfo.keyInfo->tupleKeyVec[tableUid].fTable;
		tableInfoMap[tableUid].fView = jobInfo.keyInfo->tupleKeyVec[tableUid].fView;
		tableInfoMap[tableUid].fSchema = jobInfo.keyInfo->tupleKeyVec[tableUid].fSchema;
		tableInfoMap[tableUid].fSubId = jobInfo.keyInfo->tupleKeyVec[tableUid].fSubId;
		tableInfoMap[tableUid].fColsInColMap = jobInfo.columnMap[tableUid];
	}

	// Set of the columns being projected.
	for (TupleInfoVector::iterator i = jobInfo.pjColList.begin(); i != jobInfo.pjColList.end(); i++)
		jobInfo.returnColSet.insert(i->key);

	// Strip constantbooleanquerySteps
	for (uint64_t i = 0; i < querySteps.size(); )
	{
		TupleConstantBooleanStep* bs = dynamic_cast<TupleConstantBooleanStep*>(querySteps[i].get());
		ExpressionStep* es = dynamic_cast<ExpressionStep*>(querySteps[i].get());
		if (bs != NULL)
		{
			// cosntant step
			if (bs->boolValue() == false)
				jobInfo.constantFalse = true;

			querySteps.erase(querySteps.begin()+i);
		}
		else if (es != NULL && es->tableKeys().size() == 0)
		{
			// constant expression
			ParseTree* p = es->expressionFilter();  // filter
			if (p != NULL)
			{
				Row r; // dummy row
				if (funcexp::FuncExp::instance()->evaluate(r, p) == false)
					jobInfo.constantFalse = true;

				querySteps.erase(querySteps.begin()+i);
			}
		}
		else
		{
			i++;
		}
	}

	// Concatenate query and project steps
	JobStepVector steps = querySteps;
	steps.insert(steps.end(), projectSteps.begin(), projectSteps.end());

	// Make sure each query step has an output DL
	// This is necessary for toString() method on most steps
	for (JobStepVector::iterator it = steps.begin(); it != steps.end(); ++it)
	{
		//if (dynamic_cast<OrDelimiter*>(it->get()))
		//	continue;

		if (it->get()->outputAssociation().outSize() == 0)
		{
			JobStepAssociation jsa;
			AnyDataListSPtr adl(new AnyDataList());
			RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
			dl->OID(it->get()->oid());
			adl->rowGroupDL(dl);
			jsa.outAdd(adl);
			it->get()->outputAssociation(jsa);
		}
	}

	// Populate the TableInfo map with the job steps keyed by table ID.
	JobStepVector joinSteps;
	JobStepVector& expSteps = jobInfo.crossTableExpressions;
	JobStepVector::iterator it = querySteps.begin();
	JobStepVector::iterator end = querySteps.end();
	while (it != end)
	{
		// Separate table joins from other predicates.
		TupleHashJoinStep* thjs = dynamic_cast<TupleHashJoinStep*>(it->get());
		ExpressionStep* exps = dynamic_cast<ExpressionStep*>(it->get());
		SubAdapterStep* subs = dynamic_cast<SubAdapterStep*>(it->get());
		if (thjs != NULL && thjs->tupleId1() != thjs->tupleId2())
		{
			// simple column and constant column semi join
			if (thjs->tableOid2() == 0 && thjs->schema2().empty())
			{
				jobInfo.correlateSteps.push_back(*it++);
				continue;
			}

			// check correlated join step
			JoinType joinType = thjs->getJoinType();
			if (joinType & CORRELATED)
			{
				// one of the tables is in outer query
				jobInfo.correlateSteps.push_back(*it++);
				continue;
			}

			// Save the join topology.
			uint32_t key1 = thjs->tupleId1();
			uint32_t key2 = thjs->tupleId2();
			uint32_t tid1 = getTableKey(jobInfo, key1);
			uint32_t tid2 = getTableKey(jobInfo, key2);
			if (thjs->dictOid1() > 0)
				key1 = jobInfo.keyInfo->dictKeyMap[key1];
			if (thjs->dictOid2() > 0)
				key2 = jobInfo.keyInfo->dictKeyMap[key2];

			// not correlated
			joinSteps.push_back(*it);
			tableInfoMap[tid1].fJoinKeys.push_back(key1);
			tableInfoMap[tid2].fJoinKeys.push_back(key2);

			// keep a join map
			pair<uint32_t, uint32_t> tablePair(tid1, tid2);
			TableJoinMap::iterator m1 = jobInfo.tableJoinMap.find(tablePair);
			TableJoinMap::iterator m2 = jobInfo.tableJoinMap.end();
			if (m1 == jobInfo.tableJoinMap.end())
			{
				tableInfoMap[tid1].fAdjacentList.push_back(tid2);
				tableInfoMap[tid2].fAdjacentList.push_back(tid1);

				m1 = jobInfo.tableJoinMap.insert(m1, make_pair(make_pair(tid1,tid2), JoinData()));
				m2 = jobInfo.tableJoinMap.insert(m1, make_pair(make_pair(tid2,tid1), JoinData()));

				TupleInfo ti1(getTupleInfo(tid1, key1, jobInfo));
				TupleInfo ti2(getTupleInfo(tid2, key2, jobInfo));
				if (ti1.width > 8 || ti2.width > 8)
					m1->second.fTypeless = m2->second.fTypeless = true;
				else
					m1->second.fTypeless = m2->second.fTypeless = false;
			}
			else
			{
				m2 = jobInfo.tableJoinMap.find(make_pair(tid2, tid1));
				m1->second.fTypeless = m2->second.fTypeless = true;
			}

			if (m1 == jobInfo.tableJoinMap.end() || m2 == jobInfo.tableJoinMap.end())
				throw runtime_error("Bad table map.");

			// Keep a map of the join (table, key) pairs
			m1->second.fLeftKeys.push_back(key1);
			m1->second.fRightKeys.push_back(key2);

			m2->second.fLeftKeys.push_back(key2);
			m2->second.fRightKeys.push_back(key1);

			// Keep a map of the join type between the keys.
			// OUTER join and SEMI/ANTI join are mutually exclusive.
			if (joinType == LEFTOUTER)
			{
				m1->second.fTypes.push_back(SMALLOUTER);
				m2->second.fTypes.push_back(LARGEOUTER);
				jobInfo.outerOnTable.insert(tid2);
			}
			else if (joinType == RIGHTOUTER)
			{
				m1->second.fTypes.push_back(LARGEOUTER);
				m2->second.fTypes.push_back(SMALLOUTER);
				jobInfo.outerOnTable.insert(tid1);
			}
			else if ((joinType & SEMI) &&
					 ((joinType & LEFTOUTER) == LEFTOUTER || (joinType & RIGHTOUTER) == RIGHTOUTER))
			{
				// @bug3998, DML UPDATE borrows "SEMI" flag,
				// allowing SEMI and LARGEOUTER combination to support update with outer join.
				if ((joinType & LEFTOUTER) == LEFTOUTER)
				{
					joinType ^= LEFTOUTER;
					m1->second.fTypes.push_back(joinType);
					m2->second.fTypes.push_back(joinType | LARGEOUTER);
					jobInfo.outerOnTable.insert(tid2);
				}
				else
				{
					joinType ^= RIGHTOUTER;
					m1->second.fTypes.push_back(joinType | LARGEOUTER);
					m2->second.fTypes.push_back(joinType);
					jobInfo.outerOnTable.insert(tid1);
				}
			}
			else
			{
				m1->second.fTypes.push_back(joinType);
				m2->second.fTypes.push_back(joinType);
			}

			// need id to keep the join order
			m1->second.fJoinId = m2->second.fJoinId = thjs->joinId();
		}
		// Separate the expressions
		else if (exps != NULL && subs == NULL)
		{
			const vector<uint32_t>& tables = exps->tableKeys();
			const vector<uint32_t>& columns = exps->columnKeys();
			bool  tableInOuterQuery = false;
			set<uint32_t> tableSet;	           // involved unique tables
			for (uint64_t i = 0; i < tables.size(); ++i)
			{
				if (find(jobInfo.tableList.begin(), jobInfo.tableList.end(), tables[i]) !=
								jobInfo.tableList.end())
					tableSet.insert(tables[i]);
				else
					tableInOuterQuery = true;
			}

			if (tableInOuterQuery)
			{
				// all columns in subquery scope to be projected
				for (uint64_t i = 0; i < tables.size(); ++i)
				{
					// outer-query columns
					if (tableSet.find(tables[i]) == tableSet.end())
						continue;

					// subquery columns
					uint32_t c = columns[i];
					if (jobInfo.returnColSet.find(c) == jobInfo.returnColSet.end())
					{
						tableInfoMap[tables[i]].fProjectCols.push_back(c);
						jobInfo.pjColList.push_back(getTupleInfo(tables[i], c, jobInfo));
						jobInfo.returnColSet.insert(c);
						const SimpleColumn* sc =
										dynamic_cast<const SimpleColumn*>(exps->columns()[i]);
						if (sc != NULL)
							jobInfo.deliveredCols.push_back(SRCP(sc->clone()));
					}
				}

				jobInfo.correlateSteps.push_back(*it++);
				continue;
			}

			// is the expression cross tables?
			if (tableSet.size() == 1 && exps->associatedJoinId() == 0)
			{
				// single table and not in join on clause
				uint32_t tid = tables[0];
				for (uint64_t i = 0; i < columns.size(); ++i)
					tableInfoMap[tid].fColsInExp1.push_back(columns[i]);

				tableInfoMap[tid].fOneTableExpSteps.push_back(*it);
			}
			else
			{
				// WORKAROUND for limitation on join with filter
				if (exps->associatedJoinId() != 0)
				{
					for (uint64_t i = 0; i < exps->columns().size(); ++i)
					{
						jobInfo.joinFeTableMap[exps->associatedJoinId()].insert(tables[i]);
					}
				}

				// resolve after join: cross table or on clause conditions
				for (uint64_t i = 0; i < columns.size(); ++i)
				{
					uint32_t cid = columns[i];
					uint32_t tid = getTableKey(jobInfo, cid);
					tableInfoMap[tid].fColsInExp2.push_back(cid);
				}

				expSteps.push_back(*it);
			}
		}
		// Separate the other steps by unique ID.
		else
		{
			uint32_t tid = -1;
			uint64_t cid = (*it)->tupleId();
			if (cid != (uint64_t) -1)
				tid = getTableKey(jobInfo, (*it)->tupleId());
			else
				tid = getTableKey(jobInfo, it->get());
			if (find(jobInfo.tableList.begin(), jobInfo.tableList.end(), tid) !=
						jobInfo.tableList.end())
			{
				tableInfoMap[tid].fQuerySteps.push_back(*it);
			}
			else
			{
				jobInfo.correlateSteps.push_back(*it);
			}
		}
		it++;
	}

	// @bug2634, delay isNull filter on outerjoin key
	// @bug5374, delay predicates for outerjoin
	outjoinPredicateAdjust(tableInfoMap, jobInfo);

	// @bug4021, make sure there is real column to scan
	for (TableInfoMap::iterator it = tableInfoMap.begin(); it != tableInfoMap.end(); it++)
	{
		uint32_t tableUid = it->first;
		if (jobInfo.pseudoColTable.find(tableUid) == jobInfo.pseudoColTable.end())
			continue;

		JobStepVector& steps = tableInfoMap[tableUid].fQuerySteps;
		JobStepVector::iterator s = steps.begin();
		JobStepVector::iterator p = steps.end();
		for (; s != steps.end(); s++)
		{
			if (typeid(*(s->get())) == typeid(pColScanStep) ||
				typeid(*(s->get())) == typeid(pColStep))
				break;

			// @bug5893, iterator to the first pseudocolumn
			if (typeid(*(s->get())) == typeid(PseudoColStep) && p == steps.end())
				p = s;
		}

		if (s == steps.end())
		{
			map<uint64_t, SRCP>::iterator t = jobInfo.tableColMap.find(tableUid);
			if (t == jobInfo.tableColMap.end())
			{
				string msg = jobInfo.keyInfo->tupleKeyToName[tableUid];
				msg += " has no column in column map.";
				throw runtime_error(msg);
			}

			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(t->second.get());
			CalpontSystemCatalog::OID oid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			CalpontSystemCatalog::ColType ct = sc->colType();
			string alias(extractTableAlias(sc));
			SJSTEP sjs(new pColScanStep(oid, tblOid, ct, jobInfo));
			sjs->alias(alias);
			sjs->view(sc->viewName());
			sjs->schema(sc->schemaName());
			sjs->name(sc->columnName());
			TupleInfo ti(setTupleInfo(ct, oid, jobInfo, tblOid, sc, alias));
			sjs->tupleId(ti.key);
			steps.insert(steps.begin(), sjs);

			if (isDictCol(ct) && jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
				jobInfo.tokenOnly[ti.key] = true;
		}
		else if (s > p)
		{
			// @bug5893, make sure a pcol is in front of any pseudo step.
			SJSTEP t = *s;
			*s = *p;
			*p = t;
		}
	}

	// @bug3767, error out scalar subquery with aggregation and correlated additional comparison.
	if (jobInfo.hasAggregation && (jobInfo.correlateSteps.size() > 0))
	{
		// expression filter
		ExpressionStep* exp = NULL;
		for (it = jobInfo.correlateSteps.begin(); it != jobInfo.correlateSteps.end(); it++)
		{
			if ((exp = dynamic_cast<ExpressionStep*>(it->get())) != NULL)
				break;
		}

		// correlated join step
		TupleHashJoinStep* thjs = NULL;
		for (it = jobInfo.correlateSteps.begin(); it != jobInfo.correlateSteps.end(); it++)
		{
			if ((thjs = dynamic_cast<TupleHashJoinStep*>(it->get())) != NULL)
				break;
		}

		// @bug5202, error out not equal correlation and aggregation in subquery.
		if ((exp != NULL) && (thjs != NULL) && (thjs->getJoinType() & CORRELATED))
			throw IDBExcept(
						IDBErrorInfo::instance()->errorMsg(ERR_NON_SUPPORT_NEQ_AGG_SUB),
						ERR_NON_SUPPORT_NEQ_AGG_SUB);
	}

	it = projectSteps.begin();
	end = projectSteps.end();
	while (it != end)
	{
		uint32_t tid = getTableKey(jobInfo, (*it)->tupleId());
		tableInfoMap[tid].fProjectSteps.push_back(*it);
		tableInfoMap[tid].fProjectCols.push_back((*it)->tupleId());
		it++;
	}

	for (TupleInfoVector::iterator j = jobInfo.pjColList.begin(); j != jobInfo.pjColList.end(); j++)
	{
		if (jobInfo.keyInfo->tupleKeyVec[j->tkey].fId == 3000)
			continue;

		vector<uint32_t>& projectCols = tableInfoMap[j->tkey].fProjectCols;
		if (find(projectCols.begin(), projectCols.end(), j->key) == projectCols.end())
			projectCols.push_back(j->key);
	}

	JobStepVector& retExp = jobInfo.returnedExpressions;
	for (it = retExp.begin(); it != retExp.end(); ++it)
	{
		ExpressionStep* exp = dynamic_cast<ExpressionStep*>(it->get());
		if (exp == NULL)
			throw runtime_error("Not an expression.");
		for (uint64_t i = 0; i < exp->columnKeys().size(); ++i)
		{
			tableInfoMap[exp->tableKeys()[i]].fColsInRetExp.push_back(exp->columnKeys()[i]);
		}
	}

	// reset all step vector
	querySteps.clear();
	projectSteps.clear();
	deliverySteps.clear();

	// Check if the tables and joins can be used to construct a spanning tree.
	spanningTreeCheck(tableInfoMap, jobInfo);

	// 1. combine job steps for each table
	TableInfoMap::iterator mit;
	for (mit = tableInfoMap.begin(); mit != tableInfoMap.end(); mit++)
		if (combineJobStepsByTable(mit, jobInfo) == false)
			throw runtime_error("combineJobStepsByTable failed.");

	// 2. join the combined steps together to form the spanning tree
	vector<uint32_t> joinOrder;
	joinTables(joinSteps, tableInfoMap, jobInfo, joinOrder, overrideLargeSideEstimate);

	// 3. put the steps together
	for (vector<uint32_t>::iterator i = joinOrder.begin(); i != joinOrder.end(); ++i)
		querySteps.insert(querySteps.end(),
							tableInfoMap[*i].fQuerySteps.begin(),
								tableInfoMap[*i].fQuerySteps.end());

	adjustLastStep(querySteps, deliverySteps, jobInfo);  // to match the select clause
}


SJSTEP unionQueries(JobStepVector& queries, uint64_t distinctUnionNum, JobInfo& jobInfo)
{
	vector<RowGroup> inputRGs;
	vector<bool> distinct;
	uint64_t colCount = jobInfo.deliveredCols.size();

	vector<uint32_t> oids;
	vector<uint32_t> keys;
	vector<uint32_t> scale;
	vector<uint32_t> precision;
	vector<uint32_t> width;
	vector<CalpontSystemCatalog::ColDataType> types;
	JobStepAssociation jsaToUnion;

	// bug4388, share code with connector for column type coversion
	vector<vector<CalpontSystemCatalog::ColType> > queryColTypes;
	for (uint64_t j = 0; j < colCount; ++j)
		queryColTypes.push_back(vector<CalpontSystemCatalog::ColType>(queries.size()));

	for (uint64_t i = 0; i < queries.size(); i++)
	{
		SJSTEP& spjs = queries[i];
		TupleDeliveryStep* tds = dynamic_cast<TupleDeliveryStep*>(spjs.get());
		if (tds == NULL)
		{
			throw runtime_error("Not a deliverable step.");
		}

		const RowGroup& rg = tds->getDeliveredRowGroup();
		inputRGs.push_back(rg);

		const vector<uint32_t>& scaleIn = rg.getScale();
		const vector<uint32_t>& precisionIn = rg.getPrecision();
		const vector<CalpontSystemCatalog::ColDataType>& typesIn = rg.getColTypes();

		for (uint64_t j = 0; j < colCount; ++j)
		{
			queryColTypes[j][i].colDataType = typesIn[j];
			queryColTypes[j][i].scale = scaleIn[j];
			queryColTypes[j][i].precision = precisionIn[j];
			queryColTypes[j][i].colWidth = rg.getColumnWidth(j);
		}

		if (i == 0)
		{
			const vector<uint32_t>& oidsIn = rg.getOIDs();
			const vector<uint32_t>& keysIn = rg.getKeys();
			oids.insert(oids.end(), oidsIn.begin(), oidsIn.begin() + colCount);
			keys.insert(keys.end(), keysIn.begin(), keysIn.begin() + colCount);
		}

		// if all union types are UNION_ALL, distinctUnionNum is 0.
		distinct.push_back(distinctUnionNum > i);

		AnyDataListSPtr spdl(new AnyDataList());
		RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
		spdl->rowGroupDL(dl);
		dl->OID(CNX_VTABLE_ID);
		JobStepAssociation jsa;
		jsa.outAdd(spdl);
		spjs->outputAssociation(jsa);
		jsaToUnion.outAdd(spdl);
	}

	AnyDataListSPtr spdl(new AnyDataList());
	RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
	spdl->rowGroupDL(dl);
	dl->OID(CNX_VTABLE_ID);
	JobStepAssociation jsa;
	jsa.outAdd(spdl);
	TupleUnion *unionStep = new  TupleUnion(CNX_VTABLE_ID, jobInfo);
	unionStep->inputAssociation(jsaToUnion);
	unionStep->outputAssociation(jsa);

	// get unioned column types
	for (uint64_t j = 0; j < colCount; ++j)
	{
		CalpontSystemCatalog::ColType colType = DataConvert::convertUnionColType(queryColTypes[j]);
		types.push_back(colType.colDataType);
		scale.push_back(colType.scale);
		precision.push_back(colType.precision);
		width.push_back(colType.colWidth);
	}

	vector<uint32_t> pos;
	pos.push_back(2);
	for (uint64_t i = 0; i < oids.size(); ++i)
		pos.push_back(pos[i] + width[i]);
	unionStep->setInputRowGroups(inputRGs);
	unionStep->setDistinctFlags(distinct);
	unionStep->setOutputRowGroup(RowGroup(oids.size(), pos, oids, keys, types, scale, precision, jobInfo.stringTableThreshold));

	// Fix for bug 4388 adjusts the result type at connector side, this workaround is obsolete.
	// bug 3067, update the returned column types.
	// This is a workaround as the connector always uses the first query' returned columns.
		// ct.colDataType = types[i];
		// ct.scale = scale[i];
		// ct.colWidth = width[i];

	for (size_t i = 0; i < jobInfo.deliveredCols.size(); i++)
	{
		CalpontSystemCatalog::ColType ct = jobInfo.deliveredCols[i]->resultType();
//XXX remove after connector change
ct.colDataType = types[i];
ct.scale = scale[i];
ct.colWidth = width[i];

		// varchar/varbinary column width has been fudged, see fudgeWidth in jlf_common.cpp.
		if (ct.colDataType == CalpontSystemCatalog::VARCHAR)
			ct.colWidth--;
		else if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
			ct.colWidth -= 2;

		jobInfo.deliveredCols[i]->resultType(ct);
	}

	if (jobInfo.trace)
	{
		cout << boldStart << "\ninput RGs: (distinct=" << distinctUnionNum << ")\n" << boldStop;
		for (vector<RowGroup>::iterator i = inputRGs.begin(); i != inputRGs.end(); i++)
			cout << i->toString() << endl << endl;
		cout << boldStart << "output RG:\n" << boldStop
			 << unionStep->getDeliveredRowGroup().toString() << endl;
	}

	return SJSTEP(unionStep);
}

}
// vim:ts=4 sw=4:

