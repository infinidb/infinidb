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

//  $Id: windowfunctionstep.cpp 9681 2013-07-11 22:58:05Z xlou $


//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/algorithm/string.hpp>  //  to_upper_copy
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread.hpp>
#include <boost/uuid/uuid_io.hpp>
using namespace boost;

#include "atomicops.h"
using namespace atomicops;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "configcpp.h"
using namespace config;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "pseudocolumn.h"
#include "simplefilter.h"
#include "windowfunctioncolumn.h"
using namespace execplan;

#include "windowfunction.h"
#include "windowfunctiontype.h"
#include "framebound.h"
#include "frameboundrange.h"
#include "frameboundrow.h"
#include "windowframe.h"
using namespace windowfunction;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "funcexp.h"
using namespace funcexp;

#include "querytele.h"
using namespace querytele;

#include "jlf_common.h"
#include "jobstep.h"
#include "windowfunctionstep.h"
using namespace joblist;


namespace
{

string keyName(uint64_t i, uint32_t key, const joblist::JobInfo& jobInfo)
{
	string name = jobInfo.projectionCols[i]->alias();
	if (name.empty())
	{
		name = jobInfo.keyInfo->tupleKeyToName[key];
		if (jobInfo.keyInfo->tupleKeyVec[key].fId < 100)
			name = "Expression/Function";
	}

	return name = "'" + name + "'";
}


uint64_t getColumnIndex(const SRCP& c, const map<uint64_t, uint64_t>& m, JobInfo& jobInfo)
{
	uint64_t key = getTupleKey(jobInfo, c, true);
	const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(c.get());
	if (sc != NULL && !sc->schemaName().empty())
	{
		// special handling for dictionary
		CalpontSystemCatalog::ColType ct = sc->colType();
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
		if (!(dynamic_cast<const PseudoColumn*>(sc)))
			ct = jobInfo.csc->colType(sc->oid());
//X
		CalpontSystemCatalog::OID dictOid = isDictCol(ct);
		string alias(extractTableAlias(sc));
		if (dictOid > 0)
		{
			TupleInfo ti =
				setTupleInfo(ct, dictOid, jobInfo, tableOid(sc, jobInfo.csc), sc, alias);
			key = ti.key;
		}
	}

	map<uint64_t, uint64_t>::const_iterator j = m.find(key);
	if (j == m.end())
	{
		string name = jobInfo.keyInfo->tupleKeyToName[key];
		cerr << name << " is not in tuple, key=" << key << endl;
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_COLUMN_MISSING, name),
			ERR_WF_COLUMN_MISSING);
	}

	return j->second;
}


}


namespace joblist
{


WindowFunctionStep::WindowFunctionStep(const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fCatalog(jobInfo.csc),
	fRowsReturned(0),
	fEndOfResult(false),
	fIsSelect(true),
	fUseSSMutex(false),
	fInputDL(NULL),
	fOutputDL(NULL),
	fInputIterator(-1),
	fOutputIterator(-1),
	fFunctionCount(0),
	fTotalThreads(1),
	fNextIndex(0),
	fMemUsage(0),
	fRm(jobInfo.rm)
{
	fTotalThreads = fRm.windowFunctionThreads();
	fExtendedInfo = "WFS: ";
	fQtc.stepParms().stepType = StepTeleStats::T_WFS;
}


WindowFunctionStep::~WindowFunctionStep()
{
	if (fMemUsage > 0)
		fRm.returnMemory(fMemUsage);
}


void WindowFunctionStep::run()
{
	if (fInputJobStepAssociation.outSize() == 0)
		throw logic_error("No input data list for window function step.");

	fInputDL = fInputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fInputDL == NULL)
		throw logic_error("Input is not a RowGroup data list in window function step.");

	fInputIterator = fInputDL->getIterator();

	if (fOutputJobStepAssociation.outSize() == 0)
		throw logic_error("No output data list for window function step.");

	fOutputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
	if (fOutputDL == NULL)
		throw logic_error("Output of window function step is not a RowGroup data list.");

	if (fDelivery == true)
	{
		fOutputIterator = fOutputDL->getIterator();
	}

	fRunner.reset(new boost::thread(Runner(this)));
}


void WindowFunctionStep::join()
{
	if (fRunner)
		fRunner->join();
}


uint32_t WindowFunctionStep::nextBand(messageqcpp::ByteStream &bs)
{
	RGData rgDataOut;
	bool more = false;
	uint32_t rowCount = 0;

	try
	{
		bs.restart();

		more = fOutputDL->next(fOutputIterator, &rgDataOut);

		if (more && !cancelled())
		{
			fRowGroupDelivered.setData(&rgDataOut);
			fRowGroupDelivered.serializeRGData(bs);
			rowCount = fRowGroupDelivered.getRowCount();
		}
		else
		{
			while (more)
				more = fOutputDL->next(fOutputIterator, &rgDataOut);
			fEndOfResult = true;
		}
	}
	catch (IDBExcept& iex)
	{
		handleException(iex.what(), iex.errorCode());
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(const std::exception& ex)
	{
		handleException(ex.what(), ERR_IN_DELIVERY);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}
	catch(...)
	{
		handleException("WindowFunctionStep caught an unknown exception", ERR_IN_DELIVERY);
		while (more)
			more = fOutputDL->next(fOutputIterator, &rgDataOut);
		fEndOfResult = true;
	}

	if (fEndOfResult)
	{
		// send an empty / error band
		rgDataOut.reinit(fRowGroupDelivered, 0);
		fRowGroupDelivered.setData(&rgDataOut);
		fRowGroupDelivered.resetRowGroup(0);
		fRowGroupDelivered.setStatus(status());
		fRowGroupDelivered.serializeRGData(bs);
	}

	return rowCount;
}


void WindowFunctionStep::setOutputRowGroup(const RowGroup& rg)
{
	idbassert(0);
}


const RowGroup& WindowFunctionStep::getOutputRowGroup() const
{
	return fRowGroupOut;
}


const RowGroup& WindowFunctionStep::getDeliveredRowGroup() const
{
	return fRowGroupDelivered;
}


void WindowFunctionStep::deliverStringTableRowGroup(bool b)
{
	fRowGroupOut.setUseStringTable(b);
	fRowGroupDelivered.setUseStringTable(b);
}


bool WindowFunctionStep::deliverStringTableRowGroup() const
{
	idbassert(fRowGroupOut.usesStringTable() == fRowGroupDelivered.usesStringTable());
	return fRowGroupDelivered.usesStringTable();
}


const string WindowFunctionStep::toString() const
{
	ostringstream oss;
	oss << "WindowFunctionStep   ses:" << fSessionId << " txn:" << fTxnId << " st:" << fStepId;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i);

	if (fOutputJobStepAssociation.outSize() > 0)
	{
		oss << " out:";
		for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
			oss << fOutputJobStepAssociation.outAt(i);
	}

	return oss.str();
}

void WindowFunctionStep::checkWindowFunction(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	// window functions in select clause, selected or in expression
	jobInfo.windowDels = jobInfo.deliveredCols;
	for (RetColsVector::iterator i=jobInfo.windowDels.begin(); i<jobInfo.windowDels.end(); i++)
	{
		const vector<WindowFunctionColumn*>& wcl = (*i)->windowfunctionColumnList();
		RetColsVector wcList;
		for (vector<WindowFunctionColumn*>::const_iterator j = wcl.begin(); j != wcl.end(); j++)
			wcList.push_back(SRCP((*j)->clone()));

		if (!wcList.empty())
		{
			jobInfo.windowExps.push_back(*i);
			jobInfo.windowSet.insert(getTupleKey(jobInfo, *i, true));
		}

		if (dynamic_cast<WindowFunctionColumn*>(i->get()) != NULL)
		{
			jobInfo.windowCols.push_back(*i);
			jobInfo.windowSet.insert(getTupleKey(jobInfo, *i, true));
		}
		else if (!wcList.empty())
		{
			jobInfo.windowCols.insert(jobInfo.windowCols.end(), wcList.begin(), wcList.end());
			for (RetColsVector::const_iterator k = wcList.begin(); k < wcList.end(); k++)
			{
				jobInfo.windowSet.insert(getTupleKey(jobInfo, *k, true));
			}
		}
	}

	// window functions in order by clause
	const CalpontSelectExecutionPlan::OrderByColumnList& orderByCols = csep->orderByCols();
	RetColsVector wcInOrderby;
	for (uint64_t i = 0; i < orderByCols.size(); i++)
	{
		if (orderByCols[i]->orderPos() == (uint64_t)(-1))
		{
			WindowFunctionColumn* wc = dynamic_cast<WindowFunctionColumn*>(orderByCols[i].get());
			const vector<WindowFunctionColumn*>& wcl = orderByCols[i]->windowfunctionColumnList();
			RetColsVector wcList;
			for (vector<WindowFunctionColumn*>::const_iterator j = wcl.begin(); j != wcl.end(); j++)
				wcList.push_back(SRCP((*j)->clone()));

			if (wc == NULL && wcList.empty())
				continue;

			// an window function or expression of window functions
			wcInOrderby.push_back(orderByCols[i]);

			if (!wcList.empty())
			{
				jobInfo.windowExps.push_back(orderByCols[i]);
				jobInfo.windowSet.insert(getTupleKey(jobInfo, orderByCols[i], true));
			}

			if (dynamic_cast<WindowFunctionColumn*>(orderByCols[i].get()) != NULL)
			{
				jobInfo.windowCols.push_back(orderByCols[i]);
				jobInfo.windowSet.insert(getTupleKey(jobInfo, orderByCols[i], true));
			}
			else if (!wcList.empty())
			{
				jobInfo.windowCols.insert(
					jobInfo.windowCols.end(), wcList.begin(), wcList.end());
				for (RetColsVector::const_iterator k = wcList.begin(); k < wcList.end(); k++)
				{
					jobInfo.windowSet.insert(getTupleKey(jobInfo, *k, true));
				}
			}
		}
	}

	// no window function involved in the query
	if (jobInfo.windowCols.empty())
		return;

	// reconstruct the delivered column list with auxiliary columns
	set<uint64_t> colSet;
	jobInfo.deliveredCols.resize(0);
	for (RetColsVector::iterator i=jobInfo.windowDels.begin(); i<jobInfo.windowDels.end(); i++)
	{
		jobInfo.deliveredCols.push_back(*i);
		uint64_t key = getTupleKey(jobInfo, *i, true);

		// TODO: remove duplicates in select clause
		colSet.insert(key);
	}

	// add window columns in orderby
	for (RetColsVector::iterator i = wcInOrderby.begin(); i < wcInOrderby.end(); i++)
	{
		jobInfo.deliveredCols.push_back(*i);
		uint64_t key = getTupleKey(jobInfo, *i, true);
		colSet.insert(key);
	}

	// add non-duplicate auxiliary columns
	RetColsVector colsInAf;
	for (RetColsVector::iterator i=jobInfo.windowCols.begin(); i<jobInfo.windowCols.end(); i++)
	{
		uint64_t key = getTupleKey(jobInfo, *i, true);
		if (colSet.find(key) == colSet.end())
			jobInfo.deliveredCols.push_back(*i);

		RetColsVector columns = dynamic_cast<WindowFunctionColumn*>(i->get())->getColumnList();
		for (RetColsVector::iterator j = columns.begin(); j < columns.end(); j++)
		{
			if (dynamic_cast<ConstantColumn*>(j->get()) != NULL)
				continue;

			key = getTupleKey(jobInfo, *j, true);
			if (colSet.find(key) == colSet.end())
				jobInfo.deliveredCols.push_back(*j);

			colSet.insert(key);
		}
	}

	// for handling order by and limit in outer query
	jobInfo.wfqLimitStart = csep->limitStart();
	jobInfo.wfqLimitCount = csep->limitNum();
	csep->limitStart(0);
	csep->limitNum(-1);
	if (csep->orderByCols().size() > 0)
	{
		jobInfo.wfqOrderby = csep->orderByCols();
		csep->orderByCols().clear();

		// add order by columns
		for (RetColsVector::iterator i=jobInfo.wfqOrderby.begin(); i<jobInfo.wfqOrderby.end(); i++)
		{
			if (dynamic_cast<ConstantColumn*>(i->get()) != NULL)
				continue;

			uint64_t key = getTupleKey(jobInfo, *i, true);
			if (colSet.find(key) == colSet.end())
				jobInfo.deliveredCols.push_back(*i);

			colSet.insert(key);
		}
	}
}


SJSTEP WindowFunctionStep::makeWindowFunctionStep(SJSTEP& step, JobInfo& jobInfo)
{
	// create a window function step
	WindowFunctionStep* ws = new WindowFunctionStep(jobInfo);

	// connect to the feeding step
	JobStepAssociation jsa;
	AnyDataListSPtr spdl(new AnyDataList());
	RowGroupDL* dl = new RowGroupDL(1, jobInfo.fifoSize);
	dl->OID(execplan::CNX_VTABLE_ID);
	spdl->rowGroupDL(dl);
	jsa.outAdd(spdl);
	ws->inputAssociation(jsa);
	ws->stepId(step->stepId()+1);
	step->outputAssociation(jsa);

	AnyDataListSPtr spdlOut(new AnyDataList());
	RowGroupDL* dlOut = new RowGroupDL(1, jobInfo.fifoSize);
	dlOut->OID(CNX_VTABLE_ID);
	spdlOut->rowGroupDL(dlOut);
	JobStepAssociation jsaOut;
	jsaOut.outAdd(spdlOut);
	ws->outputAssociation(jsaOut);

	// configure the rowgroups and index mapping
	TupleDeliveryStep* ds = dynamic_cast<TupleDeliveryStep*>(step.get());
	idbassert(ds != NULL);
	ws->initialize(ds->getDeliveredRowGroup(), jobInfo);

	// restore the original delivery coloumns
	jobInfo.deliveredCols = jobInfo.windowDels;
	jobInfo.nonConstDelCols.clear();
	for (RetColsVector::iterator i=jobInfo.windowDels.begin(); i<jobInfo.windowDels.end(); i++)
	{
        if (NULL == dynamic_cast<const ConstantColumn*>(i->get()))
            jobInfo.nonConstDelCols.push_back(*i);
	}

	return SJSTEP(ws);
}


void WindowFunctionStep::initialize(const RowGroup& rg, JobInfo& jobInfo)
{
	if (jobInfo.trace) cout << "Input to WindowFunctionStep: " << rg.toString() << endl;

	// query type decides the output by dbroot or partition
	// @bug 5631. Insert select should be treated as select
	fIsSelect = (jobInfo.queryType == "SELECT" || 
                     jobInfo.queryType == "INSERT_SELECT");

	// input row meta data
	fRowGroupIn = rg;
	fRowGroupIn.initRow(&fRowIn);

	// make an input map(id, index)
	map<uint64_t, uint64_t> colIndexMap;
	uint64_t colCntIn = rg.getColumnCount();
	const vector<uint32_t>& pos = rg.getOffsets();
	const vector<uint32_t>& oids = rg.getOIDs();
	const vector<uint32_t>& keys = rg.getKeys();
	const vector<CalpontSystemCatalog::ColDataType>& types = rg.getColTypes();
	const vector<uint32_t>& scales = rg.getScale();
	const vector<uint32_t>& precisions = rg.getPrecision();
	for (uint64_t i = 0; i < colCntIn; i++)
		colIndexMap.insert(make_pair(keys[i], i));

	// @bug6065, window functions that will update string table
	int64_t wfsUpdateStringTable = 0;
	for (RetColsVector::iterator i=jobInfo.windowCols.begin(); i<jobInfo.windowCols.end(); i++)
	{
		// window function type
		WindowFunctionColumn* wc = dynamic_cast<WindowFunctionColumn*>(i->get());
		uint64_t ridx = getColumnIndex(*i, colIndexMap, jobInfo);    // result index
		// @bug6065, window functions that will update string table
		{
			CalpontSystemCatalog::ColType rt = wc->resultType();
			if ((types[ridx] == CalpontSystemCatalog::CHAR || 
			     types[ridx] == CalpontSystemCatalog::VARCHAR) &&
			    rg.getColumnWidth(ridx) >= jobInfo.stringTableThreshold)
			{
				wfsUpdateStringTable++;
			} 
		}

		vector<int64_t> fields;
		fields.push_back(ridx);  // result
		const RetColsVector& parms = wc->functionParms();
		for (uint64_t i = 0; i < parms.size(); i++)                  // arguments
		{
			// skip constant column
			if (dynamic_cast<const ConstantColumn*>(parms[i].get()) == NULL)
				fields.push_back(getColumnIndex(parms[i], colIndexMap, jobInfo));
			else
				fields.push_back(-1);
		}

		// partition & order by
		const RetColsVector& partitions = wc->partitions();
		vector<uint64_t> eqIdx;
		vector<uint64_t> peerIdx;
		vector<IdbSortSpec> sorts;
		for (uint64_t i = 0; i < partitions.size(); i++)
		{
			// skip constant column
			if (dynamic_cast<const ConstantColumn*>(partitions[i].get()) != NULL)
				continue;

			// get column index
			uint64_t idx = getColumnIndex(partitions[i], colIndexMap, jobInfo);
			eqIdx.push_back(idx);
			sorts.push_back(IdbSortSpec(idx, partitions[i]->asc(), partitions[i]->nullsFirst()));
		}

		const RetColsVector& orders = wc->orderBy().fOrders;
		for (uint64_t i = 0; i < orders.size(); i++)
		{
			// skip constant column
			if (dynamic_cast<const ConstantColumn*>(orders[i].get()) != NULL)
				continue;

			// get column index
			uint64_t idx = getColumnIndex(orders[i], colIndexMap, jobInfo);
			peerIdx.push_back(idx);
			sorts.push_back(IdbSortSpec(idx, orders[i]->asc(), orders[i]->nullsFirst()));
		}

		// functors for sorting
		boost::shared_ptr<EqualCompData> parts(new EqualCompData(eqIdx, rg));
		boost::shared_ptr<OrderByData> orderbys(new OrderByData(sorts, rg));
		boost::shared_ptr<EqualCompData> peers(new EqualCompData(peerIdx, rg));

		// column type for functor templates
		int ct = 0;
		// make sure index is in range
		if (fields.size() > 1 && fields[1] >= 0 && static_cast<uint64_t>(fields[1]) < types.size())
			ct = types[fields[1]];

		// workaround for functions using "within group (order by)" syntax
		string fn = boost::to_upper_copy(wc->functionName());
		if ( (fn == "MEDIAN" || fn == "PERCENTILE_CONT" || fn == "PERCENTILE_DISC") &&
				peerIdx[0] >= 0 && peerIdx[0] < types.size() )
			ct = types[peerIdx[0]];

		// create the functor based on function name
		boost::shared_ptr<WindowFunctionType> func =
			WindowFunctionType::makeWindowFunction(fn, ct);

		// parse parms after peer and fields are set
		// functions may need to set order column index
		func->peer(peers);
		func->fieldIndex(fields);
		func->parseParms(parms);

		// window frame
		const WF_Frame& frame = wc->orderBy().fFrame;
		int frameUnit = (frame.fIsRange) ? WF__FRAME_RANGE : WF__FRAME_ROWS;
		if (frame.fStart.fFrame == WF_UNBOUNDED_PRECEDING &&
			frame.fEnd.fFrame == WF_UNBOUNDED_FOLLOWING)
				frameUnit = WF__FRAME_ROWS;
		boost::shared_ptr<FrameBound> upper = parseFrameBound(
			frame.fStart, colIndexMap, orders, peers, jobInfo, !frame.fIsRange, true);
		boost::shared_ptr<FrameBound> lower = parseFrameBound(
			frame.fEnd, colIndexMap, orders, peers, jobInfo, !frame.fIsRange, false);
		boost::shared_ptr<WindowFrame> windows(new WindowFrame(frameUnit, upper, lower));
		func->frameUnit(frameUnit);

		// add to the function list
		fFunctions.push_back(boost::shared_ptr<WindowFunction>(
			new WindowFunction(func, parts, orderbys, windows, rg, fRowIn)));
		fFunctionCount++;
	}

	// initialize window function expresssions
	fExpression = jobInfo.windowExps;
	for (RetColsVector::iterator i = fExpression.begin(); i < fExpression.end(); i++)
	{
		// output index
		(*i)->outputIndex(getColumnIndex(*i, colIndexMap, jobInfo));

		// map the input indices
		const vector<SimpleColumn*>& scols = (*i)->simpleColumnList();
		for (vector<SimpleColumn*>::const_iterator j = scols.begin(); j != scols.end(); j++)
		{
			uint64_t key = getTupleKey(jobInfo, *j);
			CalpontSystemCatalog::OID dictOid = joblist::isDictCol((*j)->colType());
			if (dictOid > 0)
			{
				key = jobInfo.keyInfo->dictKeyMap[key];
			}

			map<uint64_t, uint64_t>::iterator k = colIndexMap.find(key);

			if (k != colIndexMap.end())
			{
				(*j)->inputIndex(k->second);
			}
			else
			{
				string name = jobInfo.keyInfo->tupleKeyToName[key];
				cerr << name << " is not in tuple, key=" << key << endl;
				throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_COLUMN_MISSING, name),
					ERR_WF_COLUMN_MISSING);
			}
		}

		ArithmeticColumn* ac = dynamic_cast<ArithmeticColumn*>((*i).get());
		FunctionColumn*   fc = dynamic_cast<FunctionColumn*>((*i).get());
		if (ac != NULL)
		{
			updateWindowCols(ac->expression(), colIndexMap, jobInfo);
		}
		else if (fc != NULL)
		{
//			RetColsVector wcList = fc->windowfunctionColumnList();
//			for (RetColsVector::iterator j = wcList.begin(); j != wcList.end(); j++)
//				(*j)->inputIndex(getColumnIndex(*j, colIndexMap, jobInfo));
			funcexp::FunctionParm parms = fc->functionParms();
			for (vector<execplan::SPTP>::iterator j = parms.begin(); j < parms.end(); j++)
				updateWindowCols(j->get(), colIndexMap, jobInfo);
		}
	}

	// order by part
	if (jobInfo.wfqOrderby.size() > 0)
	{
		// query order by
		vector<uint64_t> eqIdx;
		vector<IdbSortSpec> sorts;
		const RetColsVector& orderby = jobInfo.wfqOrderby;
		for (uint64_t i = 0; i < orderby.size(); i++)
		{
			// skip constant column
			if (dynamic_cast<const ConstantColumn*>(orderby[i].get()) != NULL)
				continue;

			// get column index
			uint64_t idx = getColumnIndex(orderby[i], colIndexMap, jobInfo);
			sorts.push_back(IdbSortSpec(idx, orderby[i]->asc(), orderby[i]->nullsFirst()));
		}

		fQueryOrderBy.reset(new OrderByData(sorts, rg));
	}

	// limit part
	fQueryLimitStart = jobInfo.wfqLimitStart;
	fQueryLimitCount = jobInfo.wfqLimitCount;

	// fix the delivered rowgroup data
	vector<uint64_t> delColIdx;
	for (RetColsVector::iterator i=jobInfo.windowDels.begin(); i<jobInfo.windowDels.end(); i++)
	{
		// find the none constantant columns in the deliver
		// leave constants to annexstep for now.
		if (dynamic_cast<const ConstantColumn*>((*i).get()) != NULL)
			continue;

		delColIdx.push_back(getColumnIndex(*i, colIndexMap, jobInfo));
	}

	size_t retColCount = delColIdx.size();
	vector<uint32_t> pos1;
	vector<uint32_t> oids1;
	vector<uint32_t> keys1;
	vector<uint32_t> scales1;
	vector<uint32_t> precisions1;
	vector<CalpontSystemCatalog::ColDataType> types1;
	pos1.push_back(2);

	for (size_t i = 0; i < retColCount; i++)
	{
		size_t j = delColIdx[i];
		pos1.push_back(pos1[i] + (pos[j+1] - pos[j]));
		oids1.push_back(oids[j]);
		keys1.push_back(keys[j]);
		scales1.push_back(scales[j]);
		precisions1.push_back(precisions[j]);
		types1.push_back(types[j]);
	}

	fRowGroupDelivered = RowGroup(
		retColCount,pos1,oids1,keys1,types1,scales1,precisions1,jobInfo.stringTableThreshold);

	if (jobInfo.trace)
 		cout << "delivered RG: " << fRowGroupDelivered.toString() << endl << endl;

	if (wfsUpdateStringTable > 1)
		fUseSSMutex = true;

	fRowGroupOut = fRowGroupDelivered;
}


void WindowFunctionStep::execute()
{
	RGData rgData;
	Row row;
	fRowGroupIn.initRow(&row);
	bool more = fInputDL->next(fInputIterator, &rgData);
	uint64_t i = 0; // for RowGroup index in the fInRowGroupData

	if (traceOn()) dlTimes.setFirstReadTime();

	StepTeleStats sts;
	sts.query_uuid = fQueryUuid;
	sts.step_uuid = fStepUuid;
	sts.msg_type = StepTeleStats::ST_START;
	sts.total_units_of_work = 1;
	postStepStartTele(sts);

	try
	{
		while (more && !cancelled())
		{
			fRowGroupIn.setData(&rgData);
			fRowGroupIn.getRow(0, &row);
			uint64_t rowCnt = fRowGroupIn.getRowCount();
			if (rowCnt > 0)
			{
				fInRowGroupData.push_back(rgData);
				uint64_t memAdd = fRowGroupIn.getSizeWithStrings() + rowCnt * sizeof(RowPosition);
				if (fRm.getMemory(memAdd) == false)
					throw IDBExcept(ERR_WF_DATA_SET_TOO_BIG);
				fMemUsage += memAdd;

				for (uint64_t j = 0; j < rowCnt; ++j)
				{
					if (i > 0x0000FFFFFFFFFFFFULL || j > 0x000000000000FFFFULL)
						throw IDBExcept(ERR_WF_DATA_SET_TOO_BIG);

					fRows.push_back(RowPosition(i, j));
					row.nextRow();
				}

				//@bug6065, make StringStore::storeString() thread safe, default to false.
				rgData.useStoreStringMutex(fUseSSMutex);

				// window function does not change row count
				fRowsReturned += rowCnt;

				i++;
			}
			more = fInputDL->next(fInputIterator, &rgData);
		}
	} // try
	catch(const IDBExcept &idb) {
		handleException(idb.what(), idb.errorCode());
	}
	catch(const std::exception& ex)
	{
		handleException(ex.what(), ERR_READ_INPUT_DATALIST);
	}
	catch(...)
	{
		handleException("WindowFunctionStep caught an unknown exception", ERR_READ_INPUT_DATALIST);
	}

	if (traceOn())
		dlTimes.setLastReadTime();

	// no need for the window function if aborted or result set is empty.
	if (cancelled() || fRows.size() == 0)
	{
		while (more)
			more = fInputDL->next(fInputIterator, &rgData);

		fOutputDL->endOfInput();

		sts.msg_type = StepTeleStats::ST_SUMMARY;
		sts.total_units_of_work = sts.units_of_work_completed = 1;
		sts.rows = fRowsReturned;
		postStepSummaryTele(sts);

		if (traceOn())
		{
			dlTimes.setEndOfInputTime();
			printCalTrace();
		}

		return;
	}

	// got something to work on
	try
	{
		if (fFunctionCount == 1)
		{
			doFunction();
		}
		else
		{
			if (fTotalThreads > fFunctionCount)
				fTotalThreads = fFunctionCount;

			for (uint64_t i = 0; i < fTotalThreads && !cancelled(); i++)
				fFunctionThreads.push_back(
					boost::shared_ptr<boost::thread>(new boost::thread(WFunction(this))));

			// If cancelled, not all thread is started.
			for (uint64_t i = 0; i < fFunctionThreads.size(); i++)
				fFunctionThreads[i]->join();
		}

		if (!(cancelled()))
		{
			if (fIsSelect)
				doPostProcessForSelect();
			else
				doPostProcessForDml();
		}

	}
	catch(const std::exception& ex)
	{
		handleException(ex.what(), ERR_EXECUTE_WINDOW_FUNCTION);
	}
	catch(...)
	{
		handleException("WindowFunctionStep caught an unknown exception",
			ERR_EXECUTE_WINDOW_FUNCTION);
	}

	fOutputDL->endOfInput();

	sts.msg_type = StepTeleStats::ST_SUMMARY;
	sts.total_units_of_work = sts.units_of_work_completed = 1;
	sts.rows = fRowsReturned;
	postStepSummaryTele(sts);

	if (traceOn())
	{
		dlTimes.setEndOfInputTime();
		printCalTrace();
	}

	return;
}


uint64_t WindowFunctionStep::nextFunctionIndex()
{
	uint64_t idx = atomicInc(&fNextIndex);

	// return index in the function array
	return --idx;
}


void WindowFunctionStep::doFunction()
{
	uint64_t i = 0;
	try
	{
		while (((i = nextFunctionIndex()) < fFunctionCount) && !cancelled())
		{
			uint64_t memAdd = fRows.size() * sizeof(RowPosition);
			if (fRm.getMemory(memAdd) == false)
				throw IDBExcept(ERR_WF_DATA_SET_TOO_BIG);
			fMemUsage += memAdd;
			fFunctions[i]->setCallback(this, i);
			(*fFunctions[i].get())();
		}
	}
	catch (IDBExcept& iex)
	{
		handleException(iex.what(), iex.errorCode());
	}
	catch(const std::exception& ex)
	{
		handleException(ex.what(), ERR_EXECUTE_WINDOW_FUNCTION);
	}
	catch(...)
	{
		handleException("doFunction caught an unknown exception", ERR_EXECUTE_WINDOW_FUNCTION);
	}
}


void WindowFunctionStep::doPostProcessForSelect()
{
    FuncExp* fe = funcexp::FuncExp::instance();
	boost::shared_array<int> mapping = makeMapping(fRowGroupIn, fRowGroupOut);
	Row rowIn, rowOut;
	fRowGroupIn.initRow(&rowIn);
	fRowGroupOut.initRow(&rowOut);
	RGData rgData;
	vector<RowPosition>& rowData = *(fFunctions.back()->fRowData.get());
	int64_t rowsLeft = rowData.size();
	int64_t rowsInRg = 0;
	int64_t rgCapacity = 0;

	int64_t begin = fQueryLimitStart;
	int64_t count = (fQueryLimitCount == (uint64_t) -1) ? rowsLeft : fQueryLimitCount;
	int64_t end = begin + count;
	end = (end < rowsLeft) ? end : rowsLeft;
	rowsLeft = (end > begin) ? (end - begin) : 0;

	if (fQueryOrderBy.get() != NULL)
		sort(rowData.begin(), rowData.size());

	for (int64_t i = begin; i < end; i++)
	{
		if (rgData.rowData.get() == NULL)
		{
			rgCapacity = ((rowsLeft > 8192) ? 8192 : rowsLeft);
			rowsLeft -= rgCapacity;
			rgData.reinit(fRowGroupOut, rgCapacity);

			fRowGroupOut.setData(&rgData);
			fRowGroupOut.resetRowGroup(0);
			fRowGroupOut.setDBRoot(0);           // not valid dbroot
			fRowGroupOut.getRow(0, &rowOut);
			rowsInRg = 0;
		}

		rowIn.setData(getPointer(rowData[i], fRowGroupIn, rowIn));

		// evaluate the window function expressions before apply mapping
		if (fExpression.size() > 0)
			fe->evaluate(rowIn, fExpression);

		applyMapping(mapping, rowIn, &rowOut);
		rowOut.nextRow();
		rowsInRg++;

		if (rowsInRg == rgCapacity)
		{
			fRowGroupOut.setRowCount(rowsInRg);
			fOutputDL->insert(rgData);
			rgData.clear();
		}
	}
}


void WindowFunctionStep::doPostProcessForDml()
{
    FuncExp* fe = funcexp::FuncExp::instance();
	boost::shared_array<int> mapping = makeMapping(fRowGroupIn, fRowGroupOut);
	Row rowIn, rowOut;
	fRowGroupIn.initRow(&rowIn);
	fRowGroupOut.initRow(&rowOut);

	for (vector<RGData>::iterator i = fInRowGroupData.begin();
			i < fInRowGroupData.end(); i++)
	{
		fRowGroupIn.setData(&(*i));
		RGData rgData = RGData(fRowGroupIn, fRowGroupIn.getRowCount());
		fRowGroupOut.setData(&rgData);
		// @bug 5631. reset rowgroup before the data is populated.
		fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
		fRowGroupOut.setDBRoot(fRowGroupIn.getDBRoot());
                fRowGroupOut.setRowCount(fRowGroupIn.getRowCount());

		fRowGroupIn.getRow(0, &rowIn);
		fRowGroupOut.getRow(0, &rowOut);

		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i)
		{
			// evaluate the window function expressions before apply mapping
			if (fExpression.size() > 0)
				fe->evaluate(rowIn, fExpression);

			applyMapping(mapping, rowIn, &rowOut);
			rowIn.nextRow();
			rowOut.nextRow();
		}

		//fRowGroupOut.resetRowGroup(fRowGroupIn.getBaseRid());
		//fRowGroupOut.setRowCount(fRowGroupIn.getRowCount());

		fOutputDL->insert(rgData);
	}
}


void WindowFunctionStep::handleException(string errStr, int errCode)
{
	cerr << "Exception: " << errStr << endl;
	catchHandler(errStr, errCode, fErrorInfo, fSessionId);
}


boost::shared_ptr<FrameBound> WindowFunctionStep::parseFrameBoundRows(
                                                         const execplan::WF_Boundary& b,
                                                         const map<uint64_t, uint64_t>& m,
                                                         JobInfo& jobInfo)
{
	boost::shared_ptr<FrameBound> fb;
	if (b.fFrame == WF_CURRENT_ROW)
	{
		fb.reset(new FrameBoundRow(WF__CURRENT_ROW));
		return fb;
	}

	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(b.fVal.get());
	if (cc != NULL)
	{
		Row dummy;
		bool isNull = false;
		int val = cc->getIntVal(dummy, isNull);
		if (val >= 0 && isNull == false)
		{
			int type = (b.fFrame==WF_PRECEDING)?WF__CONSTANT_PRECEDING:WF__CONSTANT_FOLLOWING;
			fb.reset(new FrameBoundConstantRow(type, val));
		}
		else
		{
			string str("NULL");
			if (!isNull)
			{
				ostringstream oss;
				oss << val;
				str = oss.str();
			}

			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_BOUND_OUT_OF_RANGE, str),
				ERR_WF_BOUND_OUT_OF_RANGE);
		}
	}
	else
	{
		int type = (b.fFrame==WF_PRECEDING)?WF__EXPRESSION_PRECEDING:WF__EXPRESSION_FOLLOWING;
		uint64_t id = getTupleKey(jobInfo, b.fVal);
		uint64_t idx = getColumnIndex(b.fVal, m, jobInfo);
		TupleInfo ti = getTupleInfo(getTableKey(jobInfo, id), id, jobInfo);
		switch(ti.dtype)
		{
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::BIGINT:
			case execplan::CalpontSystemCatalog::DECIMAL:
			{
				fb.reset(new FrameBoundExpressionRow<int64_t>(type, id, idx));
				break;
			}

			case execplan::CalpontSystemCatalog::DOUBLE:
			case execplan::CalpontSystemCatalog::UDOUBLE:
			{
				fb.reset(new FrameBoundExpressionRow<double>(type, id, idx));
				break;
			}

			case execplan::CalpontSystemCatalog::FLOAT:
			case execplan::CalpontSystemCatalog::UFLOAT:
			{
				fb.reset(new FrameBoundExpressionRow<float>(type, id, idx));
				break;
			}

			case execplan::CalpontSystemCatalog::UTINYINT:
			case execplan::CalpontSystemCatalog::USMALLINT:
			case execplan::CalpontSystemCatalog::UMEDINT:
			case execplan::CalpontSystemCatalog::UINT:
			case execplan::CalpontSystemCatalog::UBIGINT:
			case execplan::CalpontSystemCatalog::UDECIMAL:
			case execplan::CalpontSystemCatalog::DATE:
			case execplan::CalpontSystemCatalog::DATETIME:
			{
				fb.reset(new FrameBoundExpressionRow<uint64_t>(type, id, idx));
				break;
			}

			default:
			{
				string str = windowfunction::colType2String[ti.dtype];
				throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_BOUND_TYPE, str),
					ERR_WF_INVALID_BOUND_TYPE);
				break;
			}
		}
	}

	return fb;
}


boost::shared_ptr<FrameBound> WindowFunctionStep::parseFrameBoundRange(const execplan::WF_Boundary& b,
                                                          const map<uint64_t, uint64_t>& m,
                                                          const vector<SRCP>& o,
                                                          JobInfo& jobInfo)
{
	boost::shared_ptr<FrameBound> fb;
	if (b.fFrame == WF_CURRENT_ROW)
	{
		fb.reset(new FrameBoundRange(WF__CURRENT_ROW));
		return fb;
	}

	bool isConstant = false;
	bool isNull = false;
	Row  dummy;
	ConstantColumn* cc = dynamic_cast<ConstantColumn*>(b.fVal.get());
	if (cc != NULL)
	{
		isConstant = true;
		double val = cc->getDoubleVal(dummy, isNull);
		if (val < 0 || isNull)
		{
			string str("NULL");
			if (!isNull)
			{
				ostringstream oss;
				oss << val;
				str = oss.str();
			}

			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_BOUND_OUT_OF_RANGE, str),
				ERR_WF_BOUND_OUT_OF_RANGE);
		}
	}

	int type = 0;
	vector<uint64_t> ids;
	vector<int> index;
	ids.push_back(getTupleKey(jobInfo, o[0]));
	index.push_back(getColumnIndex(o[0], m, jobInfo));
	if (isConstant)
	{
		type = (b.fFrame==WF_PRECEDING)?WF__CONSTANT_PRECEDING:WF__CONSTANT_FOLLOWING;
		ids.push_back(-1);   // dummy, n/a for constant
		index.push_back(-1); // dummy, n/a for constant
	}
	else
	{
		type = (b.fFrame==WF_PRECEDING)?WF__EXPRESSION_PRECEDING:WF__EXPRESSION_FOLLOWING;
		ids.push_back(getTupleKey(jobInfo, b.fVal));
		index.push_back(getColumnIndex(b.fVal, m, jobInfo));
	}
	ids.push_back(getTupleKey(jobInfo, b.fBound));
	index.push_back(getColumnIndex(b.fBound, m, jobInfo));

	FrameBoundRange* fbr = NULL;
	TupleInfo ti = getTupleInfo(getTableKey(jobInfo, ids[0]), ids[0], jobInfo);
	bool asc = o[0]->asc();
	bool nlf = o[0]->nullsFirst();
	switch(ti.dtype)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		{
			if (isConstant)
			{
				int64_t v = cc->getIntVal(dummy, isNull);
				fbr = new FrameBoundConstantRange<int64_t>(type, asc, nlf, &v);
				fbr->isZero((v == 0));
			}
			else
			{
				fbr = new FrameBoundExpressionRange<int64_t>(type, asc, nlf);
			}
			break;
		}

		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			if (isConstant)
			{
				double v = cc->getDoubleVal(dummy, isNull);
				fbr = new FrameBoundConstantRange<double>(type, asc, nlf, &v);
				fbr->isZero((v == 0.0));
			}
			else
			{
				fbr = new FrameBoundExpressionRange<double>(type, asc, nlf);
			}
			break;
		}

		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			if (isConstant)
			{
				float v = cc->getFloatVal(dummy, isNull);
				fbr = new FrameBoundConstantRange<float>(type, asc, nlf, &v);
				fbr->isZero((v == 0.0));
			}
			else
			{
				fbr = new FrameBoundExpressionRange<float>(type, asc, nlf);
			}
			break;
		}

		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			if (isConstant)
			{
				uint64_t v = cc->getUintVal(dummy, isNull);
				fbr = new FrameBoundConstantRange<uint64_t>(type, asc, nlf, &v);
				fbr->isZero((v == 0));
			}
			else
			{
				fbr = new FrameBoundExpressionRange<uint64_t>(type, asc, nlf);
			}
			break;
		}

		default:
		{
			string str = windowfunction::colType2String[ti.dtype];
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_BOUND_TYPE, str),
				ERR_WF_INVALID_BOUND_TYPE);
			break;
		}
	}

	fbr->setTupleId(ids);
	fbr->setIndex(index);
	fb.reset(fbr);

	return fb;
}


boost::shared_ptr<FrameBound> WindowFunctionStep::parseFrameBound(const execplan::WF_Boundary& b,
                                                     const map<uint64_t, uint64_t>& m,
                                                     const vector<SRCP>& o,
                                                     const boost::shared_ptr<EqualCompData>& p,
                                                     JobInfo& j,
                                                     bool rows,
                                                     bool s)
{
	boost::shared_ptr<FrameBound> fb;

	switch(b.fFrame)
	{
		case WF_UNBOUNDED_PRECEDING:
		{
			fb.reset(new FrameBound(WF__UNBOUNDED_PRECEDING));
			break;
		}

		case WF_UNBOUNDED_FOLLOWING:
		{
			fb.reset(new FrameBound(WF__UNBOUNDED_FOLLOWING));
			break;
		}

		case WF_CURRENT_ROW:
		case WF_PRECEDING:
		case WF_FOLLOWING:
		{
			if (rows)
			{
				fb = parseFrameBoundRows(b, m, j);
			}
			else
			{
				fb = parseFrameBoundRange(b, m, o, j);
			}
			break;
		}

		default:  //  unknown
		{
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_UNKNOWN_BOUND, b.fFrame),
				ERR_WF_UNKNOWN_BOUND);
			break;
		}
	}

	fb->peer(p);
	fb->start(s);

	return fb;
}


void WindowFunctionStep::updateWindowCols(ReturnedColumn* rc,
                                      const map<uint64_t, uint64_t>& m,
                                      JobInfo& jobInfo)
{
	if (rc == NULL)
		return;

	ArithmeticColumn*     ac = dynamic_cast<ArithmeticColumn*>(rc);
	FunctionColumn*       fc = dynamic_cast<FunctionColumn*>(rc);
	SimpleFilter*         sf = dynamic_cast<SimpleFilter*>(rc);
	WindowFunctionColumn* wc = dynamic_cast<WindowFunctionColumn*>(rc);

	if (wc)
	{
		uint64_t key = getExpTupleKey(jobInfo, wc->expressionId());
		map<uint64_t, uint64_t>::const_iterator j = m.find(key);
		if (j == m.end())
		{
			string name = jobInfo.keyInfo->tupleKeyToName[key];
			cerr << name << " is not in tuple, key=" << key << endl;
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_COLUMN_MISSING, name),
				ERR_WF_COLUMN_MISSING);
		}

		wc->inputIndex(j->second);
	}
	else if (ac)
	{
		updateWindowCols(ac->expression(), m, jobInfo);
	}
	else if (fc)
	{
		funcexp::FunctionParm parms = fc->functionParms();
		for (vector<execplan::SPTP>::iterator i = parms.begin(); i < parms.end(); i++)
			updateWindowCols(i->get(), m, jobInfo);
	}
	else if (sf)
	{
		updateWindowCols(sf->lhs(), m, jobInfo);
		updateWindowCols(sf->rhs(), m, jobInfo);
	}
}


void WindowFunctionStep::updateWindowCols(ParseTree* pt,
                                      const map<uint64_t, uint64_t>& m,
                                      JobInfo& jobInfo)
{
	if (pt == NULL)
		return;

	updateWindowCols(pt->left(), m, jobInfo);
	updateWindowCols(pt->right(), m, jobInfo);

	TreeNode* tn = pt->data();
	ArithmeticColumn*     ac = dynamic_cast<ArithmeticColumn*>(tn);
	FunctionColumn*       fc = dynamic_cast<FunctionColumn*>(tn);
	SimpleFilter*         sf = dynamic_cast<SimpleFilter*>(tn);
	WindowFunctionColumn* wc = dynamic_cast<WindowFunctionColumn*>(tn);

	if (wc)
	{
		uint64_t key = getExpTupleKey(jobInfo, wc->expressionId());
		map<uint64_t, uint64_t>::const_iterator j = m.find(key);
		if (j == m.end())
		{
			string name = jobInfo.keyInfo->tupleKeyToName[key];
			cerr << name << " is not in tuple, key=" << key << endl;
			throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_COLUMN_MISSING, name),
				ERR_WF_COLUMN_MISSING);
		}

		wc->inputIndex(j->second);
	}
	else if (ac)
	{
		updateWindowCols(ac->expression(), m, jobInfo);
	}
	else if (fc)
	{
		funcexp::FunctionParm parms = fc->functionParms();
		for (vector<execplan::SPTP>::iterator i = parms.begin(); i < parms.end(); i++)
			updateWindowCols(i->get(), m, jobInfo);
	}
	else if (sf)
	{
		updateWindowCols(sf->lhs(), m, jobInfo);
		updateWindowCols(sf->rhs(), m, jobInfo);
	}
}


void WindowFunctionStep::sort(std::vector<RowPosition>::iterator v, uint64_t n)
{
    // recursive function termination condition.
    if (n < 2 || cancelled())
        return;

    RowPosition                   p = *(v + n/2);   // pivot value
    vector<RowPosition>::iterator l = v;            // low   address
    vector<RowPosition>::iterator h = v + (n - 1);  // high  address
    while (l <= h && !cancelled())
    {
        // Can use while here, but need check boundary and cancel status.
        if (fQueryOrderBy->operator()(getPointer(*l), getPointer(p)))
        {
            l++;
        }
        else if (fQueryOrderBy->operator()(getPointer(p), getPointer(*h)))
        {
            h--;
        }
        else
        {
            RowPosition t = *l;    // temp value for swap
            *l++ = *h;
            *h-- = t;
        }
    }

    sort(v, distance(v, h) + 1);
    sort(l, distance(l, v) + n);
}


void WindowFunctionStep::printCalTrace()
{
	time_t t = time (0);
	char timeString[50];
	ctime_r (&t, timeString);
	timeString[strlen (timeString )-1] = '\0';
	ostringstream logStr;
	logStr  << "ses:" << fSessionId << " st: " << fStepId << " finished at "<< timeString
			<< "; total rows returned-" << fRowsReturned << endl
			<< "\t1st read " << dlTimes.FirstReadTimeString()
			<< "; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-"
			<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
			<< "s;\n\tUUID " << uuids::to_string(fStepUuid) << endl
			<< "\tJob completion status " << status() << endl;
	logEnd(logStr.str().c_str());
	fExtendedInfo += logStr.str();
	formatMiniStats();
}


void WindowFunctionStep::formatMiniStats()
{
	ostringstream oss;
	oss << "WFS "
		<< "UM "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< "- "
		<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
		<< fRowsReturned << " ";
	fMiniInfo += oss.str();
}


}   //namespace
// vim:ts=4 sw=4:

