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

//  $Id: joblist.cpp 9227 2013-01-28 22:58:06Z xlou $


#include "errorcodes.h"
#include <iterator>
#include <stdexcept>
//#define NDEBUG
#include <cassert>
using namespace std;

#define JOBLIST_DLLEXPORT
#include "joblist.h"
#undef JOBLIST_DLLEXPORT

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "errorids.h"
#include "jobstep.h"
#include "primitivestep.h"
#include "crossenginestep.h"
#include "subquerystep.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "tupleaggregatestep.h"


namespace joblist
{

struct JSJoiner
{
	JSJoiner(JobStep *j): js(j) { }
	void operator()() {
		js->abort();
		js->join();
	}
	JobStep *js;
};

JobList::JobList(bool isEM) :
	fIsRunning(false),
	fIsExeMgr(isEM),
	fPmConnected(false),
	projectingTableOID(0),
#ifdef _MSC_VER
	fAborted(0),
#else
	fAborted(false),
#endif
	_priority(50)
{
}

JobList::~JobList()
{
	vector<boost::thread *> joiners;
	boost::thread *tmp;
	try
	{
		if (fIsRunning)
		{
			JobStepVector::iterator iter;
			JobStepVector::iterator end;

			iter = fQuery.begin();
			end = fQuery.end();

			// Wait for all the query steps to finish
			while (iter != end)
			{
				tmp = new boost::thread(JSJoiner(iter->get()));
				joiners.push_back(tmp);
				++iter;
			}

			iter = fProject.begin();
			end = fProject.end();

			// wait for the projection steps
			while (iter != end)
			{
				tmp = new boost::thread(JSJoiner(iter->get()));
				joiners.push_back(tmp);
				++iter;
			}

			for (uint i = 0; i < joiners.size(); i++) {
				joiners[i]->join();
				delete joiners[i];
			}
		}
	}
	catch (exception& ex)
	{
		cerr << "JobList::~JobList: exception caught: " << ex.what() << endl;
	}
	catch (...)
	{
		cerr << "JobList::~JobList: exception caught!" << endl;
	}
}

int JobList::doQuery()
{
	// Don't start the steps if there is no PrimProc connection.
	if (!fPmConnected)
		return 0;

	JobStep *js;

	// Set the priority on the jobsteps
	for (uint i = 0; i < fQuery.size(); i++)
		fQuery[i]->priority(_priority);
	for (uint i = 0; i < fProject.size(); i++)
		fProject[i]->priority(_priority);

	// I put this logging in a separate loop rather than including it in the
	// other loop that calls run(), to insure that these logging msgs would
	// not be interleaved with any logging coming from the calls to run().
	JobStepVector::iterator iter2 = fQuery.begin();
	JobStepVector::iterator end2  = fQuery.end();
	int rc = -1;
	while (iter2 != end2)
	{
		js = iter2->get();
		if (js->traceOn())
		{
			if (js->delayedRun())
			{
				std::ostringstream oss;
				oss << "Session: " << js->sessionId() <<
					"; delaying start of query step "<< js->stepId() <<
					"; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
				std::cout << oss.str();
			}
		}
		++iter2;
	}
	iter2 = fProject.begin();
	end2  = fProject.end();
	while (iter2 != end2)
	{
		js = iter2->get();
		if (js->traceOn())
		{
			if (js->delayedRun())
			{
				std::ostringstream oss;
				oss << "Session: " << js->sessionId() <<
					"; delaying start of project step "<< js->stepId() <<
					"; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
				std::cout << oss.str();
			}
		}
		++iter2;
	}

	JobStepVector::iterator iter = fQuery.begin();
	JobStepVector::iterator end  = fQuery.end();

 	//try
 	//{
		// Start the query running
		while (iter != end)
		{
			js = iter->get();
			if (!js->delayedRun())
			{
				js->run();
			}
			++iter;
		}

		iter = fProject.begin();
		end = fProject.end();

		// Fire up the projection steps
		while (iter != end)
		{
			if (!iter->get()->delayedRun())
			{
				iter->get()->run();
			}
			++iter;
		}
	//}
	/*
	catch (exception& ex)
	{
		cerr << "JobList::doQuery: exception caught: " << ex.what() << endl;
		return -1;
	}
	catch (...)
	{
		cerr << "JobList::doQuery: exception caught!" << endl;
		return -1;
	} */
	fIsRunning = true;
	rc = 0;
	return rc;
}

int JobList::putEngineComm(DistributedEngineComm* dec)
{
	fPmConnected = (dec->connectedPmServers() > 0);
	if (!fPmConnected)
	{
		logging::LoggingID lid(05);
		logging::MessageLog ml(lid);
		logging::Message::Args args;
		logging::Message m(0);
		args.add("No PrimProcs are running");
		m.format(args);
		ml.logDebugMessage(m);
		if (!errInfo)
			errInfo.reset(new ErrorInfo);
		errInfo->errCode = logging::ERR_NO_PRIMPROC;
		errInfo->errMsg  = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NO_PRIMPROC);

		return errInfo->errCode;
	}

	JobStepVector::iterator iter = fQuery.begin();
	JobStepVector::iterator end = fQuery.end();

	while (iter != end)
	{
		SJSTEP sjstep = *iter;
		JobStep* jsp = sjstep.get();
//		if (typeid(*jsp) == typeid(pColScanStep))
//		{
//			pColScanStep* step = dynamic_cast<pColScanStep*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(pColStep))
//		{
//			pColStep* step = dynamic_cast<pColStep*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(pDictionaryScan))
		if (typeid(*jsp) == typeid(pDictionaryScan))
		{
			pDictionaryScan* step = dynamic_cast<pDictionaryScan*>(jsp);
			step->dec(dec);
		}
//		else if (typeid(*jsp) == typeid(pDictionaryStep))
//		{
//			pDictionaryStep* step = dynamic_cast<pDictionaryStep*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(pIdxWalk))
//		{
//			pIdxWalk* step = dynamic_cast<pIdxWalk*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(pIdxList))
//		{
//			pIdxList* step = dynamic_cast<pIdxList*>(jsp);
//			step->dec(dec);
//		}
		else if (typeid(*jsp) == typeid(TupleBPS))
		{
			BatchPrimitive* step = dynamic_cast<BatchPrimitive*>(jsp);
			step->setBppStep();
			step->dec(dec);
		}
		++iter;
	}

	iter = fProject.begin();
	end = fProject.end();

	while (iter != end)
	{
		SJSTEP sjstep = *iter;
		JobStep* jsp = sjstep.get();
//		if (typeid(*jsp) == typeid(pColStep))
//		{
//			pColStep* step = dynamic_cast<pColStep*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(pDictionaryStep))
//		{
//			pDictionaryStep* step = dynamic_cast<pDictionaryStep*>(jsp);
//			step->dec(dec);
//		}
//		else if (typeid(*jsp) == typeid(TupleBPS))
		if (typeid(*jsp) == typeid(TupleBPS))
		{
			BatchPrimitive* step = dynamic_cast<BatchPrimitive*>(jsp);
			step->setBppStep();
			step->dec(dec);
		}
		++iter;
	}

	return 0;
}

// -- TupleJobList
/* returns row count */
uint TupleJobList::projectTable(CalpontSystemCatalog::OID, messageqcpp::ByteStream& bs)
{
	uint ret = ds->nextBand(bs);
	moreData = (ret != 0);
	return ret;
}

const rowgroup::RowGroup& TupleJobList::getOutputRowGroup() const
{
	if (fDeliveredTables.empty())
		throw runtime_error("Empty delivery!");

	TupleDeliveryStep* tds =
				dynamic_cast<TupleDeliveryStep*>(fDeliveredTables.begin()->second.get());

	if (tds == NULL)
		throw runtime_error("Not a TupleDeliveryStep!!");

	return tds->getDeliveredRowGroup();
}

void TupleJobList::setDeliveryFlag(bool f)
{
	DeliveredTableMap::iterator iter = fDeliveredTables.begin();
	SJSTEP dstep = iter->second;
	ds = dynamic_cast<TupleDeliveryStep *>(dstep.get());
	if (ds) // if not dummy step
		ds->setIsDelivery(f);
}

//------------------------------------------------------------------------------
// Retrieve, accumulate, and return a summary of stat totals for this joblist.
// Stat totals are determined by adding up the individual counts from each step.
// It is currently intended that this method only be called after the completion
// of the query, because no mutex locking is being employed when we access the
// data attributes from the jobsteps and datalists.
//------------------------------------------------------------------------------
void JobList::querySummary(bool extendedStats)
{
	fMiniInfo += "\n";

	try
	{
		// bug3438, print subquery info prior to outer query
		for (vector<SJLP>::iterator i = subqueryJoblists.begin(); i != subqueryJoblists.end(); i++)
		{
			i->get()->querySummary(extendedStats);
			fStats += i->get()->queryStats();
			fExtendedInfo += i->get()->extendedInfo();
			fMiniInfo += i->get()->miniInfo();
		}

		JobStepVector::const_iterator qIter = fQuery.begin();
		JobStepVector::const_iterator qEnd  = fQuery.end();
		JobStep *js;

		//
		//...Add up the I/O and msg counts for the query job steps
		//
		while ( qIter != qEnd )
		{
			js = qIter->get();

			fStats.fPhyIO     += js->phyIOCount();
			fStats.fCacheIO   += js->cacheIOCount();
			if (typeid(*js) == typeid(TupleBPS))
			{
				fStats.fMsgRcvCnt += js->blockTouched();
			}
			else
			{
				fStats.fMsgRcvCnt += js->msgsRcvdCount();
			}
			fStats.fMsgBytesIn += js->msgBytesIn();
			fStats.fMsgBytesOut += js->msgBytesOut();

			//...As long as we only have 2 job steps that involve casual
			//...partioning, we just define blkSkipped() in those 2 classes,
			//...and use typeid to find/invoke those methods.  If we start
			//...adding blkSkipped() to more classes, we should probably make
			//...blkSkipped() a pure virtual method of the base JobStep class
			//...so that we don't have to do this type checking and casting.
			uint64_t skipCnt = 0;
			if (typeid(*js) == typeid(pColStep))
				skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped ();
			else if (typeid(*js) == typeid(pColScanStep))
				skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped ();
			else if (typeid(*js) == typeid(TupleBPS))
				skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped ();
			fStats.fCPBlocksSkipped += skipCnt;
#if 0
			cout << "qstep-"   << js->stepId() <<
				"; adding phy, cache, msg, blkSkip, msgBytesIn, msgBytesOut: "<<
				js->phyIOCount()    << ", "  <<
				js->cacheIOCount()  << ", "  <<
				js->msgsRcvdCount() << ", "  <<
				skipCnt             << ", "  <<
				js->msgBytesIn()    << ", "  <<
				js->msgBytesOut()   << endl;
#endif
//			//
//			//...Iterate through the output job step associations; adding up the
//			//...temp file counts and sizes for the ZDL and Bucket datalists.
//			//
//			for (unsigned int i = 0;
//				i < js->outputAssociation().outSize();
//				i++)
//			{
//				uint64_t nFiles = 0;
//				uint64_t nBytes = 0;
//
//				ZonedDL*              pZdl    = 0;
//				StringZonedDL*        pStrZdl = 0;
//				BucketDataList*       pBdl    = 0;
//				StringBucketDataList* pStrBdl = 0;
//				FifoDataList*         pFdl    = 0;
//				DeliveryWSDL*         pDWSDL  = 0;
//				bool                  updateTotals = false;
//				//string              dlType;
//
//				const AnyDataListSPtr& pDl =
//					js->outputAssociation().outAt(i);
//				if      ((pZdl = pDl->zonedDL()) != 0)
//				{
//					pZdl->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "zdl";
//				}
//				else if ((pStrZdl = pDl->stringZonedDL()) != 0)
//				{
//					pStrZdl->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "strZdl";
//				}
//				else if ((pBdl = pDl->bucketDL()) != 0)
//				{
//					pBdl->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "bucketdl";
//				}
//				else if ((pStrBdl = pDl->stringBucketDL()) != 0)
//				{
//					pStrBdl->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "strBucketdl";
//				}
//				else if ((pFdl = pDl->fifoDL()) != 0)
//				{
//					pFdl->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "fifo";
//				}
//				else if ((pDWSDL = pDl->deliveryWSDL()) != 0)
//				{
//					pDWSDL->totalFileCounts(nFiles, nBytes);
//					updateTotals = true;
//					//dlType = "deliveryWSDL";
//				}
//
//				if ( updateTotals )
//				{
//#if 0
//					cout << "jobstep : " << js->stepId() <<  " qstep outlist " << i << " is type " << dlType <<
//						": files-" << nFiles <<
//						"; bytes-" << nBytes << endl;
//#endif
//					fStats.fNumFiles += nFiles;
//					fStats.fFileBytes+= nBytes;
//				}
//			}

			if (extendedStats)
			{
				string ei;
				int max = 0;
				ei = js->extendedInfo();
				while (max < 4 && ei.size() <= 10)
				{
					ei = js->extendedInfo();
					max++;
				}

				fExtendedInfo += ei;
				fMiniInfo += js->miniInfo() + "\n";
			}

			++qIter;
		}

		JobStepVector::const_iterator pIter = fProject.begin();
		JobStepVector::const_iterator pEnd  = fProject.end();

		//
		//...Add up the I/O and msg counts for the projection job steps
		//
		while ( pIter != pEnd )
		{
			js = pIter->get();

			fStats.fPhyIO   += js->phyIOCount();
			fStats.fCacheIO += js->cacheIOCount();
			if (typeid(*js) == typeid(TupleBPS))
			{
				fStats.fMsgRcvCnt += js->blockTouched();
			}
			else
			{
				fStats.fMsgRcvCnt += js->msgsRcvdCount();
			}
			fStats.fMsgBytesIn += js->msgBytesIn();
			fStats.fMsgBytesOut += js->msgBytesOut();

			uint64_t skipCnt = 0;
			if (typeid(*js) == typeid(pColStep))
				skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped ();
			else if (typeid(*js) == typeid(pColScanStep))
				skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped ();
			else if (typeid(*js) == typeid(TupleBPS))
				skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped ();
			fStats.fCPBlocksSkipped += skipCnt;
#if 0
			cout << "pstep-"   << js->stepId() <<
				"; adding phy, cache, msg, blkSkip, msgBytesIn, msgBytesOut: "<<
				js->phyIOCount()    << ", "  <<
				js->cacheIOCount()  << ", "  <<
				js->msgsRcvdCount() << ", "  <<
				skipCnt             << ", "  <<
				js->msgBytesIn()    << ", "  <<
				js->msgBytesOut()   << endl;
#endif
			++pIter;
		}

		if ((!fProject.empty()) && extendedStats)
		{
			DeliveredTableMap::iterator dsi = fDeliveredTables.begin();
			while (dsi != fDeliveredTables.end())
			{
				js = dynamic_cast<const JobStep*>(dsi->second.get());
				string ei;
				int max = 0;
				ei = js->extendedInfo();
				while (max < 4 && ei.size() <= 10)
				{
					ei = js->extendedInfo();
					max++;
				}

				fExtendedInfo += ei;
				fMiniInfo += js->miniInfo() + "\n";

				++dsi;
			}
		}
	}
	catch (exception& ex)
	{
		cerr << "JobList::querySummary: exception caught: " << ex.what() <<endl;
		return;
	}
	catch (...)
	{
		cerr << "JobList::querySummary: exception caught!" << endl;
		return;
	}

	return;
}

// @bug 828. Added additional information to the graph at the end of execution
void JobList::graph(uint32_t sessionID)
{
	// Graphic view draw
	ostringstream oss;
	struct timeval tvbuf;
	gettimeofday(&tvbuf, 0);
	struct tm tmbuf;
	localtime_r(reinterpret_cast<time_t*>(&tvbuf.tv_sec), &tmbuf);
	oss << "jobstep_results." << setfill('0')
		<< setw(4) << (tmbuf.tm_year+1900)
		<< setw(2) << (tmbuf.tm_mon+1)
		<< setw(2) << (tmbuf.tm_mday)
		<< setw(2) << (tmbuf.tm_hour)
		<< setw(2) << (tmbuf.tm_min)
		<< setw(2) << (tmbuf.tm_sec)
		<< setw(6) << (tvbuf.tv_usec)
		<< ".dot";
	string jsrname(oss.str());
	//it's too late to set this here. ExeMgr has already returned ei to dm...
	//fExtendedInfo += "Graphs are in " + jsrname;
	std::ofstream dotFile(jsrname.c_str(), std::ios::out);
	dotFile << "digraph G {" << std::endl;
	JobStepVector::iterator qsi;
	JobStepVector::iterator psi;
	DeliveredTableMap::iterator dsi;
	CalpontSystemCatalog *csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	CalpontSystemCatalog::TableColName tcn;
	uint64_t outSize = 0;
	uint64_t msgs = 0;
	uint64_t pio = 0;
	uint64_t cio = 0;
	int ctn = 0;
	bool diskIo = false;
	uint64_t saveTime = 0;
	uint64_t loadTime = 0;

	// merge in the subquery steps
	JobStepVector querySteps = fQuery;
	{
		SubQueryStep* subquery = NULL;
		qsi = querySteps.begin();
		while (qsi != querySteps.end())
		{
			if((subquery = dynamic_cast<SubQueryStep*>(qsi->get())) != NULL)
			{
				querySteps.erase(qsi);
				JobStepVector subSteps = subquery->subJoblist()->querySteps();
				querySteps.insert(querySteps.end(), subSteps.begin(), subSteps.end());
				qsi = querySteps.begin();
			}
			else
			{
				qsi++;
			}
		}
	}

	for (qsi = querySteps.begin(); qsi != querySteps.end(); ctn++, qsi++)
	{
		//HashJoinStep* hjs = 0;

		//if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
		//	continue;

		// @bug 1042. clear column name first at each loop
		tcn.column = "";

		uint16_t stepidIn = qsi->get()->stepId();
		dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";

		// @Bug 1033.  colName was being called for dictionary steps that don't have column names.  Added if condition below.
		if ( typeid(*(qsi->get())) == typeid(pColScanStep) ||
			 typeid(*(qsi->get())) == typeid(pColStep))
			tcn = csc->colName(qsi->get()->oid());

		dotFile << "(";
		if (!tcn.column.empty())
			dotFile << tcn.column << "/";

		if (typeid(*(qsi->get())) == typeid(TupleBPS))
		{
			BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
			OIDVector projectOids = bps->getProjectOids();
			if ( projectOids.size() > 0 )
			{
				dotFile << "\\l";
				dotFile << "PC:";
				dotFile << "\\l";

				for ( unsigned int i = 0; i < projectOids.size(); i++ )
				{
					tcn = csc->colName(projectOids[i]);
					if (!tcn.column.empty())
						dotFile << tcn.column << "   ";
					if ( (i+1) % 3 == 0 )
						dotFile << "\\l";
				}
			}
			else
			{
				tcn = csc->colName(qsi->get()->oid());
				dotFile << tcn.column << "/";
			}
		}
		else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
		{
			tcn.schema = qsi->get()->schema();
			tcn.table = qsi->get()->alias();
		}

		dotFile << JSTimeStamp::tsdiffstr(qsi->get()->dlTimes.EndOfInputTime(), qsi->get()->dlTimes.FirstReadTime()) << "s";

		dotFile << ")";

		// oracle predict card
		dotFile << "\\l#: " << (*qsi)->cardinality();

		if (typeid(*(qsi->get())) == typeid(pColStep))
		{
			dotFile << "\"" << " shape=ellipse";
		}
		else if (typeid(*(qsi->get())) == typeid(pColScanStep))
		{
			dotFile << "\"" << " shape=box";
		}
		else if (typeid(*(qsi->get())) == typeid(TupleBPS))
		{
			BatchPrimitive* bps =
				dynamic_cast<BatchPrimitive*>(qsi->get());

			// if BPS not run, a BucketReuseStep was substituted, so draw dashed
			if (bps->wasStepRun())
			{
				dotFile << "\"" << " shape=box style=bold";
				if (typeid(*(qsi->get())) == typeid(TupleBPS))
					dotFile << " peripheries=2";
			}
			else
				dotFile << "\"" << " shape=box style=dashed";
		}
		else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
		{
			dotFile << "\"" << " shape=box style=dashed";
		}
//		else if (typeid(*(qsi->get())) == typeid(HashJoinStep) ||
//				 typeid(*(qsi->get())) == typeid(StringHashJoinStep))
//		{
//			if (typeid(*(qsi->get())) == typeid(HashJoinStep))
//			{
//				hjs = static_cast<HashJoinStep*>(qsi->get());
//				dotFile << "\\; rm: " << hjs->getUmMemoryTime() ;
//				switch (hjs->hashJoinMode())
//				{
//					case HashJoinStep::PM_MEMORY:
//						dotFile << "\"" << " shape=diamond style=dashed";
//						break;
//					case HashJoinStep::LARGE_HASHJOIN_CARD:
//						dotFile << "\"" << " shape=diamond style=solid peripheries=2";
//						break;
//					case HashJoinStep::LARGE_HASHJOIN_RUNTIME:
//						dotFile << "\"" << " shape=diamond style=solid peripheries=3";
//						break;
//					case HashJoinStep::UM_MEMORY:
//					default:
//						dotFile << "\"" << " shape=diamond style=solid";
//						break;
//				}
//			}
//			else
//			{
//				dotFile << "\"" << " shape=diamond";
//			}
//		}
		else if (typeid(*(qsi->get())) == typeid(TupleHashJoinStep))
		{
			dotFile << "\"";
			dotFile << " shape=diamond style=dashed peripheries=2";
		}
		else if (typeid(*(qsi->get())) == typeid(TupleUnion) )
		{
			dotFile << "\"" << " shape=triangle";
		}
		else if (typeid(*(qsi->get())) == typeid(pDictionaryStep))
		{
			dotFile << "\"" << " shape=trapezium";
		}
//		else if (typeid(*(qsi->get())) == typeid(ReduceStep))
//		{
//			dotFile << "\"" << " shape=triangle orientation=180";
//		}
		else if (typeid(*(qsi->get())) == typeid(FilterStep))
		{
			dotFile << "\"" << " shape=house orientation=180";
		}
		else if (typeid(*(qsi->get())) == typeid(TupleBPS))
		{
			dotFile << "\"" << " shape=box style=bold";
			dotFile << " peripheries=2";
		}
		else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
		{
			dotFile << "\"" << " shape=box style=bold";
			dotFile << " peripheries=2";
		}
//		else if (typeid(*(qsi->get())) == typeid(AggregateFilterStep))
//		{
//			dotFile << "\"" << " shape=hexagon peripheries=2 style=bold";
//		}
//		else if (typeid(*(qsi->get())) == typeid(BucketReuseStep))
//		{
//			dotFile << "\"" << " shape=box style=dashed";
//
//		}
		else
			dotFile << "\"";
		dotFile << "]" << endl;

		// msgsRecived, physicalIO, cacheIO
		msgs = qsi->get()->msgsRcvdCount();
		pio = qsi->get()->phyIOCount();
		cio = qsi->get()->cacheIOCount();

		for (unsigned int i = 0; i < qsi->get()->outputAssociation().outSize(); i++)
		{
			ptrdiff_t dloutptr = 0;
			DataList_t* dlout;
			StrDataList* sdl;
//			TupleDataList* tdl;

			if ((dlout = qsi->get()->outputAssociation().outAt(i)->dataList()))
			{
				dloutptr = (ptrdiff_t)dlout;
				outSize = dlout->totalSize();
				diskIo = dlout->totalDiskIoTime(saveTime, loadTime);
			}
			else if ((sdl = qsi->get()->outputAssociation().outAt(i)->stringDataList()))
			{
				dloutptr = (ptrdiff_t)sdl;
				outSize = sdl->totalSize();
				diskIo = sdl->totalDiskIoTime(saveTime, loadTime);
			}
//			else if ((tdl = qsi->get()->outputAssociation().outAt(i)->tupleDataList()))
//			{
//				dloutptr = (ptrdiff_t)tdl;
//				outSize = tdl->totalSize();
//				diskIo = tdl->totalDiskIoTime(saveTime, loadTime);
//			}

			// if HashJoinStep, determine if output fifo was cached to disk
			bool hjTempDiskFlag = false;
//			if (hjs)
//			{
//				hjTempDiskFlag = hjs->didOutputAssocUseDisk(i);
//			}

			for (unsigned int k = 0; k < querySteps.size(); k++)
			{
				uint16_t stepidOut = querySteps[k].get()->stepId();
				JobStepAssociation queryInputSA = querySteps[k].get()->inputAssociation();
				for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
				{
					ptrdiff_t dlinptr = 0;
					DataList_t *dlin = queryInputSA.outAt(j)->dataList();
					StrDataList* sdl = 0;
//					TupleDataList* tdl = 0;

					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else if ((sdl = queryInputSA.outAt(j)->stringDataList()))
					{
						dlinptr = (ptrdiff_t)sdl;
					}
//					else if ((tdl = queryInputSA.outAt(j)->tupleDataList()))
//						dlinptr = (ptrdiff_t)tdl;

					if (dloutptr == dlinptr)
					{
						dotFile << stepidIn << " -> " << stepidOut;

						dotFile << " [label=\" r: " << outSize;
						if (hjTempDiskFlag)
						{
							dotFile << "*";
						}
						dotFile << "\\l";

						if (msgs != 0)
						{
							dotFile << " m: " << msgs << "\\l";
							if (typeid(*(qsi->get())) == typeid(TupleBPS))
							{
								dotFile << " b: " << qsi->get()->blockTouched() << "\\l";
							}
							dotFile << " p: " << pio << "\\l";
						}
						if (diskIo == true)
						{
							dotFile << " wr: " << saveTime << "s\\l";
							dotFile << " rd: " << loadTime << "s\\l";
						}
						dotFile << "\"]" << endl;
					}
				}
			}
			for (psi = fProject.begin(); psi < fProject.end(); psi++)
			{
				uint16_t stepidOut = psi->get()->stepId();
				JobStepAssociation projectInputSA = psi->get()->inputAssociation();
				for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
				{
					ptrdiff_t dlinptr;
					DataList_t *dlin = projectInputSA.outAt(j)->dataList();
					StrDataList* sdl = 0;

					if (dlin)
					{
						dlinptr = (ptrdiff_t)dlin;
					}
					else
					{
						sdl = projectInputSA.outAt(j)->stringDataList();
						dlinptr = (ptrdiff_t)sdl;
					}
					if (dloutptr == dlinptr)
					{
						dotFile << stepidIn << " -> " << stepidOut;

						dotFile << " [label=\" r: " << outSize;
						if (hjTempDiskFlag)
						{
							dotFile << "*";
						}
						dotFile << "\\l";

						if (msgs != 0)
						{
							dotFile << " m: " << msgs << "\\l";
							dotFile << " p: " << pio << "\\l";
						}
						if (diskIo == true)
						{
							dotFile << " wr: " << saveTime << "s\\l";
							dotFile << " rd: " << loadTime << "s\\l";
						}
						dotFile << "\"]" << endl;
					}
				}
			}
		}
		//@Bug 921
		if (typeid(*(qsi->get())) == typeid(TupleBPS))
		{
			BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
			CalpontSystemCatalog::OID tableOIDProject = bps->tableOid();
			if (bps->getOutputType() == TABLE_BAND || bps->getOutputType() == ROW_GROUP)
			{
				outSize  = bps->getRows();
				for (dsi = fDeliveredTables.begin(); dsi != fDeliveredTables.end(); dsi++)
				{
					BatchPrimitive* bpsDelivery = dynamic_cast<BatchPrimitive*>((dsi->second).get());
					TupleHashJoinStep* thjDelivery = dynamic_cast<TupleHashJoinStep*>((dsi->second).get());
					if (bpsDelivery)
					{
						CalpontSystemCatalog::OID tableOID = bpsDelivery->tableOid();
						dotFile << tableOID << " [label=" << bpsDelivery->alias() <<
							" shape=plaintext]" << endl;
						JobStepAssociation deliveryInputSA = bpsDelivery->inputAssociation();
						if (tableOIDProject == tableOID)
						{
							dotFile << stepidIn << " -> " << tableOID;

							dotFile << " [label=\" r: " << outSize << "\\l";
							dotFile << " m: " << bpsDelivery->msgsRcvdCount() << "\\l";
							dotFile << " b: " << bpsDelivery->blockTouched() << "\\l";
							dotFile << " p: " << bpsDelivery->phyIOCount() << "\\l";
							dotFile << "\"]" << endl;
						}
					}
					else if (thjDelivery)
					{
						CalpontSystemCatalog::OID tableOID = thjDelivery->tableOid();
						dotFile << tableOID << " [label=" << "vtable" << " shape=plaintext]" << endl;
						JobStepAssociation deliveryInputSA = thjDelivery->inputAssociation();
						if (tableOIDProject == tableOID)
						{
							dotFile << stepidIn << " -> " << tableOID;

							dotFile << " [label=\" r: " << outSize << "\\l";
							dotFile << " m: " << thjDelivery->msgsRcvdCount() << "\\l";
							dotFile << " b: " << thjDelivery->blockTouched() << "\\l";
							dotFile << " p: " << thjDelivery->phyIOCount() << "\\l";
							dotFile << "\"]" << endl;
						}
					}
				}
			}
		}
		else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
		{
			outSize = dynamic_cast<CrossEngineStep*>(qsi->get())->getRows();
			dotFile << "0" << " [label=" << qsi->get()->alias() << " shape=plaintext]" << endl;
			dotFile << stepidIn << " -> 0";
			dotFile << " [label=\" r: " << outSize << "\\l";
			dotFile << "\"]" << endl;
		}
	}

	for (psi = fProject.begin(), ctn = 0; psi != fProject.end(); ctn++, psi++)
	{
		tcn.column = "";
		uint16_t stepidIn = psi->get()->stepId();
		dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";
		tcn = csc->colName(psi->get()->oid());
		dotFile << "(";
		BatchPrimitive* bps = 0;
		if (typeid(*(psi->get())) == typeid(TupleBPS))
		{
			bps = dynamic_cast<BatchPrimitive*>(psi->get());
			OIDVector projectOids = bps->getProjectOids();
			for ( unsigned int i = 0; i < projectOids.size(); i++ )
			{
				tcn = csc->colName(projectOids[i]);
				if (!tcn.column.empty())
				{
					dotFile << tcn.column;
					if ( i != (projectOids.size()-1) )
						dotFile << "/  ";
				}
				if ( (i+1) % 3 == 0 )
					dotFile << "\\l";
			}
		}
		else {
			if (!tcn.column.empty())
				dotFile << tcn.column << "/";
		}
		dotFile << JSTimeStamp::tsdiffstr(psi->get()->dlTimes.EndOfInputTime(), psi->get()->dlTimes.FirstReadTime()) << "s";
		dotFile << ")";

		if (typeid(*(psi->get())) == typeid(pColStep))
		{
			dotFile << "\"" << " shape=ellipse";
		}
		else if (typeid(*(psi->get())) == typeid(pColScanStep))
		{
			dotFile << "\"" << " shape=box";
		}
		else if (typeid(*(psi->get())) == typeid(TupleBPS))
		{
			dotFile << "\"" << " shape=box style=bold";
			if (typeid(*(psi->get())) == typeid(TupleBPS))
				dotFile << " peripheries=2";
		}
//		else if (typeid(*(psi->get())) == typeid(HashJoinStep))
//		{
//			dotFile << "\"" << " shape=diamond";
//		}
//		else if (typeid(*(psi->get())) == typeid(UnionStep))
//		{
//			dotFile << "\"" << " shape=triangle";
//		}
		else if (typeid(*(psi->get())) == typeid(pDictionaryStep))
		{
			dotFile << "\"" << " shape=trapezium";
		}
		else if (typeid(*(psi->get())) == typeid(PassThruStep))
		{
			dotFile << "\"" << " shape=octagon";
		}
//		else if (typeid(*(psi->get())) == typeid(ReduceStep))
//		{
//			dotFile << "\"" << " shape=triangle orientation=180";
//		}
		else if (typeid(*(psi->get())) == typeid(FilterStep))
		{
			dotFile << "\"" << " shape=house orientation=180";
		}
		else
			dotFile << "\"";
		dotFile << "]" << endl;

		// msgsRecived, physicalIO, cacheIO
		msgs = psi->get()->msgsRcvdCount();
		pio = psi->get()->phyIOCount();
		cio = psi->get()->cacheIOCount();

		CalpontSystemCatalog::OID tableOIDProject = 0;
		if (bps)
			tableOIDProject = bps->tableOid();
		//@Bug 921
		for (dsi = fDeliveredTables.begin(); dsi != fDeliveredTables.end(); dsi++)
		{
			BatchPrimitive* dbps = dynamic_cast<BatchPrimitive*>((dsi->second).get());
			if (dbps)
			{
				outSize = dbps->getRows();
				CalpontSystemCatalog::OID tableOID = dbps->tableOid();
				dotFile << tableOID << " [label=" << dbps->alias() << " shape=plaintext]" << endl;
				JobStepAssociation deliveryInputSA = dbps->inputAssociation();
				if ( tableOIDProject == tableOID )
				{
					dotFile << stepidIn << " -> " << tableOID;

					dotFile << " [label=\" r: " << outSize << "\\l";
					dotFile << " m: " << dbps->msgsRcvdCount() << "\\l";
					dotFile << " b: " << dbps->blockTouched() << "\\l";
					dotFile << " p: " << dbps->phyIOCount() << "\\l";
					dotFile << "\"]" << endl;

				}
			}
		}
	}

	dotFile << "}" << std::endl;
	dotFile.close();
}

void JobList::validate() const
{
//	uint i;
//	DeliveredTableMap::const_iterator it;

	/* Make sure there's at least one query step and that they're the right type */
	idbassert(fQuery.size() > 0);
//	for (i = 0; i < fQuery.size(); i++)
//		idbassert(dynamic_cast<BatchPrimitiveStep *>(fQuery[i].get()) ||
//			dynamic_cast<HashJoinStep *>(fQuery[i].get()) ||
//			dynamic_cast<UnionStep *>(fQuery[i].get()) ||
//			dynamic_cast<AggregateFilterStep *>(fQuery[i].get()) ||
//			dynamic_cast<BucketReuseStep *>(fQuery[i].get()) ||
//			dynamic_cast<pDictionaryScan *>(fQuery[i].get()) ||
//			dynamic_cast<FilterStep *>(fQuery[i].get()) ||
//			dynamic_cast<OrDelimiter *>(fQuery[i].get())
//		);
//
//	/* Make sure there's at least one projected table and that they're the right type */
//	idbassert(fDeliveredTables.size() > 0);
//	for (i = 0; i < fProject.size(); i++)
//		idbassert(dynamic_cast<BatchPrimitiveStep *>(fProject[i].get()));
//
//	/* Check that all JobSteps use the right status pointer */
//	for (i = 0; i < fQuery.size(); i++) {
//		idbassert(fQuery[i]->inputAssociation().statusPtr().get() == statusPtr().get());
//		idbassert(fQuery[i]->outputAssociation().statusPtr().get() == statusPtr().get());
//	}
//	for (i = 0; i < fProject.size(); i++) {
//		idbassert(fProject[i]->inputAssociation().statusPtr().get() == statusPtr().get());
//		idbassert(fProject[i]->outputAssociation().statusPtr().get() == statusPtr().get());
//	}
//	for (it = fDeliveredTables.begin(); it != fDeliveredTables.end(); ++it) {
//		idbassert(it->second->inputAssociation().statusPtr().get() == statusPtr().get());
//		idbassert(it->second->outputAssociation().statusPtr().get() == statusPtr().get());
//	}
}

void TupleJobList::validate() const
{
	uint i, j;
	DeliveredTableMap::const_iterator it;

	idbassert(fQuery.size() > 0);
	for (i = 0; i < fQuery.size(); i++) {
		idbassert(dynamic_cast<TupleBPS *>(fQuery[i].get()) ||
			dynamic_cast<TupleHashJoinStep *>(fQuery[i].get()) ||
			dynamic_cast<TupleAggregateStep *>(fQuery[i].get()) ||
			dynamic_cast<TupleUnion *>(fQuery[i].get()) ||
			dynamic_cast<pDictionaryScan *>(fQuery[i].get())
		);
	}

	/* Duplicate check */
	for (i = 0; i < fQuery.size(); i++)
		for (j = i + 1; j < fQuery.size(); j++)
			idbassert(fQuery[i].get() != fQuery[j].get());

	/****  XXXPAT: An indication of a possible problem: The next assertion fails
		occasionally */
// 	idbassert(fDeliveredTables.begin()->first == 100);  //fake oid for the vtable
// 	cout << "Delivered TableOID is " << fDeliveredTables.begin()->first << endl;
	idbassert(fProject.size() == 0);
	idbassert(fDeliveredTables.size() == 1);
	idbassert(dynamic_cast<TupleDeliveryStep *>(fDeliveredTables.begin()->second.get()));

	/* Check that all JobSteps use the right status pointer */
	for (i = 0; i < fQuery.size(); i++) {
		idbassert(fQuery[i]->inputAssociation().statusPtr().get() == statusPtr().get());
		idbassert(fQuery[i]->outputAssociation().statusPtr().get() == statusPtr().get());
	}
	for (i = 0; i < fProject.size(); i++) {
		idbassert(fProject[i]->inputAssociation().statusPtr().get() == statusPtr().get());
		idbassert(fProject[i]->outputAssociation().statusPtr().get() == statusPtr().get());
	}
	for (it = fDeliveredTables.begin(); it != fDeliveredTables.end(); ++it) {
		idbassert(it->second->inputAssociation().statusPtr().get() == statusPtr().get());
		idbassert(it->second->outputAssociation().statusPtr().get() == statusPtr().get());
	}
}

void JobList::abort()
{
	uint i;
#ifdef _MSC_VER
	//If the current value of fAborted is 0 then set it to 1 and return the initial value
	// of fAborted (which will be 0), else do nothing and return fAborted (which will be 1)
	if (InterlockedCompareExchange(&fAborted, 1, 0) == 0) {
#else
	//If the current value of fAborted is false then set it to true and return true, else
	// do not change fAborted and return false
	if (__sync_bool_compare_and_swap(&fAborted, false, true)) {
#endif
		for (i = 0; i < fQuery.size(); i++)
			fQuery[i]->abort();
		for (i = 0; i < fProject.size(); i++)
			fProject[i]->abort();
	}
}

void JobList::abortOnLimit(JobStep* js)
{
#ifdef _MSC_VER
	//If the current value of fAborted is 0 then set it to 1 and return the initial value
	// of fAborted (which will be 0), else do nothing and return fAborted (which will be 1)
	if (InterlockedCompareExchange(&fAborted, 1, 0) == 0) {
#else
	//If the current value of fAborted is false then set it to true and return true, else
	// do not change fAborted and return false
	if (__sync_bool_compare_and_swap(&fAborted, false, true)) {
#endif
		for (uint i = 0; i < fQuery.size(); i++) {
			if (fQuery[i].get() == js)
				break;

			fQuery[i]->abort();
		}
	}
}

string JobList::toString() const
{
	uint i;
	string ret;

	ret = "Filter Steps:\n";
	for (i = 0; i < fQuery.size(); i++)
		ret += fQuery[i]->toString();
	//ret += "\nProjection Steps:\n";
	//for (i = 0; i < fProject.size(); i++)
	//	ret += fProject[i]->toString();
	ret += "\n";
	return ret;
}

TupleJobList::TupleJobList(bool isEM) : JobList(isEM), ds(NULL), moreData(true)
{
}

TupleJobList::~TupleJobList() 
{ 
	abort();
}

void TupleJobList::abort()
{
#ifdef _MSC_VER
	if (fAborted == 0 && fIsRunning) {
#else
	if (!fAborted && fIsRunning) {
#endif
		JobList::abort();
		messageqcpp::ByteStream bs;
		if (ds && moreData)
			while (ds->nextBand(bs));
	}
}

void init_mysqlcl_idb()
{
	CrossEngineStep::init_mysqlcl_idb();
}

}

// vim:ts=4 sw=4:

