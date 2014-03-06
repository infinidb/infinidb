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

// $Id: passthrustep.cpp 9655 2013-06-25 23:08:13Z xlou $
//
// This is a simple optimization to take DV's out of an IDL and insert them into an ODL
// without having to get them from PrimProc

#include <string>
#include <cassert>
#include <sys/time.h>
#include <ctime>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "jlf_common.h"
#include "datalist.h"
#include "primitivestep.h"
using namespace execplan;

//namespace {
//using namespace joblist;
//class ptt
//{
//public:
//	ptt(DataList_t* i, DataList_t* o, bool swallowRows, PassThruStep* pStep) :
//		idl(i), odl(o), fSwallowRows(swallowRows), fpStep(pStep) { }
//
//	void operator()()
//	{
//		struct timeval firstRead  = {0, 0};
//		struct timeval lastRead   = {0, 0};
//		struct timeval firstWrite = {0, 0};
//		struct timeval lastWrite  = {0, 0};
//		int it = -1;
//		bool more;
//		bool isExeMgr = fpStep->isExeMgr();
//		bool isDictColumn = fpStep->isDictCol();
//		uint8_t colWidth = fpStep->getColWidth();
//		uint8_t *pos;
//		// @bug 807. add outsize to passthru
//		uint64_t outSize = 0;
//
//		if (fpStep->traceOn())
//		{
//			gettimeofday(&firstRead, 0);
//			firstWrite = firstRead;
//		}
//
//		UintRowGroup rwIn, rwOut;
//		FifoDataList* odlrw = reinterpret_cast<FifoDataList*>(odl);
//		if (typeid(*idl) == typeid(FifoDataList))
//		{
//			FifoDataList* idlrw = reinterpret_cast<FifoDataList*>(idl);
//			it = idlrw->getIterator();
//			more = idlrw->next(it, &rwIn);
//			while (more)
//			{
//				// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is
//				//            TRACE_FLAGS::TRACE_NO_ROWS4.  Running with this on will swallow rows at
//				// 	      projection.
//				/* XXXPAT: If this feeds a pDictionary, is fSwallowRows always false? */
//				if (!fSwallowRows || isDictColumn)
//				{
//					if (isDictColumn || !isExeMgr) {
//						for (uint64_t i = 0; i < rwIn.count; ++i) {
//							rwOut.et[rwOut.count++] = rwIn.et[i];
//							if (rwOut.count == rwOut.ElementsPerGroup)
//							{
//								odlrw->insert(rwOut);
//								outSize += rwOut.count;
//								rwOut.count = 0;
//							}
//						}
//					}
//					else {
//						pos = ((uint8_t *) &rwOut.et[0]);
//						for (uint64_t i = 0; i < rwIn.count; ++i)
//						{
//							switch (colWidth) {
//								case 1: pos[rwOut.count++] = rwIn.et[i].second; break;
//								case 2: ((uint16_t *) pos)[rwOut.count++] = rwIn.et[i].second;
//									break;
//								case 3:
//								case 4: ((uint32_t *) pos)[rwOut.count++] = rwIn.et[i].second;
//									break;
//								case 5:
//								case 6:
//								case 7:
//								case 8: ((uint64_t *) pos)[rwOut.count++] = rwIn.et[i].second;
//									break;
//								default:
//									cout << "PassThruStep: bad column width of " <<
//										fpStep->getColWidth() << endl;
//								throw logic_error("PassThruStep: bad column width");
//							}
//
//							if (rwOut.count == rwOut.ElementsPerGroup)
//							{
//								odlrw->insert(rwOut);
//								outSize += rwOut.count;
//								rwOut.count = 0;
//							}
//						}
//					}
//				}
//				more = idlrw->next(it, &rwIn);
//			}
//		}
//		else
//		{
//			ElementType e;
//			it = idl->getIterator();
//			more = idl->next(it, &e);
//			while (more)
//			{
//				// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is
//				//            TRACE_FLAGS::TRACE_NO_ROWS4.  Running with this on will swallow rows at
//				// 	      projection.
//
//				if (!fSwallowRows || isDictColumn)
//				{
//					if (isDictColumn || !isExeMgr)
//						rwOut.et[rwOut.count++] = e;
//					else {
//						pos = ((uint8_t *) &rwOut.et[0]);
//						switch (colWidth) {
//							case 1: pos[rwOut.count++] = e.second; break;
//							case 2: ((uint16_t *) pos)[rwOut.count++] = e.second;
//								break;
//							case 3:
//							case 4: ((uint32_t *) pos)[rwOut.count++] = e.second;
//								break;
//							case 5:
//							case 6:
//							case 7:
//							case 8: ((uint64_t *) pos)[rwOut.count++] = e.second;
//								break;
//							default:
//								cout << "PassThruStep: bad column width of " <<
//									fpStep->getColWidth() << endl;
//							throw logic_error("PassThruStep: bad column width");
//						}
//					}
//					if (rwOut.count == rwOut.ElementsPerGroup)
//					{
//						odlrw->insert(rwOut);
//						outSize += rwOut.count;
//						rwOut.count = 0;
//					}
//				}
//				more = idl->next(it, &e);
//			}
//		}
//
//		if (rwOut.count > 0)
//		{
//			odlrw->insert(rwOut);
//			outSize += rwOut.count;
//		}
//
//        odlrw->totalSize(outSize);
//		odlrw->endOfInput();
//
//		if (fpStep->traceOn())
//		{
//			gettimeofday(&lastRead, 0);
//			lastWrite = lastRead;
//			fpStep->syslogProcessingTimes(16,  // exemgr subsystem
//						firstRead,     // first datalist read
//						lastRead,      // last  datalist read
//						firstWrite,    // first datalist write
//						lastWrite);    // last (endOfInput) datalist write
//			fpStep->syslogEndStep(16,  // exemgr subsystem
//						0,             // no blocked datalist input  to report
//						0);            // no blocked datalist output to report
//		}
//	}
//
//private:
//	//ptt(const ptt& rhs);
//	//ptt& operator=(const ptt& rhs);
//
//	DataList_t* idl;
//	DataList_t* odl;
//	bool fSwallowRows;
//	PassThruStep* fpStep;
//};
//
//}

namespace joblist {

PassThruStep::PassThruStep(
	execplan::CalpontSystemCatalog::OID oid,
	execplan::CalpontSystemCatalog::OID tableOid,
	const execplan::CalpontSystemCatalog::ColType& colType,
	const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fOid(oid),
	fTableOid(tableOid),
	isEM(jobInfo.isExeMgr),
	fSwallowRows(false),
	fRm(jobInfo.rm)
{
	colWidth = colType.colWidth;
	realWidth = colType.colWidth;
	isDictColumn = ((colType.colDataType == CalpontSystemCatalog::VARCHAR && colType.colWidth > 7)
					|| (colType.colDataType == CalpontSystemCatalog::CHAR && colType.colWidth > 8));
	fColType = colType;
	fPseudoType = 0;

}

PassThruStep::PassThruStep(const pColStep& rhs) : JobStep(rhs), fRm(rhs.fRm)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	colWidth = rhs.fColType.colWidth;
	realWidth = rhs.realWidth;
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	fSwallowRows = rhs.getSwallowRows();
	isDictColumn = rhs.isDictCol();
	fColType = rhs.colType();
	isEM = rhs.isExeMgr();

	const PseudoColStep* pcs = dynamic_cast<const PseudoColStep*>(&rhs);
	if (pcs)
		fPseudoType = pcs->pseudoColumnId();
		
}

PassThruStep::PassThruStep(const PseudoColStep& rhs) : JobStep(rhs), fRm(rhs.fRm)
{
	fInputJobStepAssociation = rhs.inputAssociation();
	fOutputJobStepAssociation = rhs.outputAssociation();
	colWidth = rhs.fColType.colWidth;
	realWidth = rhs.realWidth;
	fOid = rhs.oid();
	fTableOid = rhs.tableOid();
	fSwallowRows = rhs.getSwallowRows();
	isDictColumn = rhs.isDictCol();
	fColType = rhs.colType();
	fPseudoType = rhs.pseudoColumnId();
	isEM = rhs.isExeMgr();
}

PassThruStep::~PassThruStep()
{
}

void PassThruStep::run()
{
//	if (traceOn())
//	{
//		syslogStartStep(16,               // exemgr subsystem
//			std::string("PassThruStep")); // step name
//	}
//
//	DataList_t* idl = fInputJobStepAssociation.outAt(0)->dataList();
//	idbassert(idl);
//	DataList_t* odl = fOutputJobStepAssociation.outAt(0)->dataList();
//	idbassert(odl);
//	ptt ptt(idl, odl, fSwallowRows, this);
//	fPTThd = new boost::thread(ptt);
}

void PassThruStep::join()
{
//	fPTThd->join();
//	delete fPTThd;
}

const string PassThruStep::toString() const
{
	ostringstream oss;
	oss << "PassThruStep    ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	oss << " " << omitOidInDL
		<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	if (fSwallowRows)
		oss << " (sink)";
	return oss.str();
}

}

