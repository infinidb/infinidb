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
*   $Id: bucketreusestep.cpp 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

#include <iomanip>
#include <iostream>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

#include "bucketreuse.h"
#include "jobstep.h"

namespace joblist
{
using namespace std;
using namespace boost;

BucketReuseStep::BucketReuseStep(const pColScanStep& scan) :
	fInputJobStepAssociation(scan.fInputJobStepAssociation),
	fOutputJobStepAssociation(scan.fOutputJobStepAssociation),
	fOid(scan.fOid),
	fTableOid(scan.fTableOid),
	fSessionId(scan.fSessionId),
	fTxnId(scan.fTxnId),
	fVerId(scan.fVerId),
	fStatementId(scan.fStatementId),
	fStepId(scan.fStepId),
	fColWidth(scan.fColType.colWidth),
	fDictColumn(false)
{
    fAlias = scan.fAlias;
    fTraceFlags = scan.fTraceFlags;
    fCardinality = scan.cardinality();
}

// Alternate constructor that can be used instead of
// BucketReuseStep(pColScanStep); warning, that output jobstep association
// is not set with this constructor.  Application code must be sure to set
// via the outputAssociation() mutator.
BucketReuseStep::BucketReuseStep(
	execplan::CalpontSystemCatalog::OID colOid,
	execplan::CalpontSystemCatalog::OID tblOid,
	uint32_t                            sessionId,
	uint32_t                            txnId,
	uint32_t                            verId,
	uint32_t                            statementId) :
	fOid(colOid),
	fTableOid(tblOid),
	fSessionId(sessionId),
	fTxnId(txnId),
	fVerId(verId),
	fStatementId(statementId),
	fStepId(0),
	fColWidth(0),
	fDictColumn(false)
{
    fTraceFlags  = 0;
    fCardinality = 0;
}

BucketReuseStep::BucketReuseStep(const pColScanStep& scan, pDictionaryStep& dict) :
	fInputJobStepAssociation(scan.fInputJobStepAssociation),
	fOutputJobStepAssociation(dict.fOutputJobStepAssociation),
	fOid(dict.fOid),
	fTableOid(dict.fTableOid),
	fSessionId(scan.fSessionId),
	fTxnId(dict.fTxnId),
	fVerId(dict.fVerId),
	fStatementId(dict.fStatementId),
	fStepId(dict.fStepId),
	fColWidth(scan.fColType.colWidth),
	fDictColumn(true)
{
    fAlias = scan.fAlias;
    fTraceFlags = scan.fTraceFlags;
    fCardinality = scan.cardinality();
}

BucketReuseStep::~BucketReuseStep()
{
}

void BucketReuseStep::run()
{
	if (traceOn())
	{
		syslogStartStep(16,           // exemgr subsystem
		string("BucketReuseStep"));   // step name
	}

	runner.reset(new boost::thread(Runner(this)));
}

void BucketReuseStep::join()
{
	runner->join();
}

void BucketReuseStep::reuseBuckets()
{
	if (0 < fInputJobStepAssociation.status())
		fOutputJobStepAssociation.status(fInputJobStepAssociation.status());
	else
	if (fDictColumn)
		reuseBuckets(fOutputJobStepAssociation.outAt(0)->stringBucketDL());
	else
		reuseBuckets(fOutputJobStepAssociation.outAt(0)->bucketDL());
}

template<typename element_t>
void BucketReuseStep::reuseBuckets(BucketDL<element_t>* dl)
{
try 
{
	if (dl == NULL)
	{
		throw logic_error("output is not bucket datalist");
	}

	if (dl->reuseControl() == NULL)
	{
		throw logic_error("reuse control in bucket datalist is not set");
	}

	dlTimes.setFirstReadTime();
	dlTimes.setFirstInsertTime();
	if (dl->reuseControl()->fileStatus() == BucketReuseControlEntry::progress_c)
	{
		boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
		dl->reuseControl()->stateChange().wait(lock);
	}
}
catch(const logging::LargeDataListExcept& ex)
{
    fOutputJobStepAssociation.status(logging::bucketReuseStepLargeDataListFileErr);
    catchHandler(ex.what(), fSessionId);
}
catch(const std::exception& ex)
{
  fOutputJobStepAssociation.status(logging::bucketReuseStepErr);
  catchHandler(ex.what(), fSessionId);
}
catch(...)
{
	string msg("BucketReuseStep: reuseBuckets caught unknown exception");
	fOutputJobStepAssociation.status(logging::bucketReuseStepErr);
	catchHandler(msg, fSessionId);
}

	dlTimes.setLastReadTime();
	dlTimes.setLastInsertTime();
	dl->restoreBucketInformation();

	dlTimes.setEndOfInputTime();
	dl->endOfInput();

	if (fTableOid >= 3000)
	{
		//...Print job step completion information
		time_t t = time(0);
		char timeString[50];
		ctime_r(&t, timeString);
		timeString[strlen(timeString)-1] = '\0';

		ostringstream logStr;
		logStr << "ses:" << fSessionId << " st: " << fStepId <<
		" finished at " << timeString <<
		"; output size-" << dl->totalSize() << endl <<
		"\t1st read " << dlTimes.FirstReadTimeString() <<
		"; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
		JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())<< "s" << endl;
		
		logEnd(logStr.str().c_str());
		
		if (traceOn())
		{
		syslogProcessingTimes(16,      // exemgr subsystem
			dlTimes.FirstReadTime(),   // first datalist read
			dlTimes.LastReadTime(),    // last  datalist read
			dlTimes.FirstInsertTime(), // first datalist write
			dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
		syslogEndStep(16,              // exemgr subsystem
			0,                         // blocked datalist input
			0);                        // blocked datalist output
		}
	}
}

const string BucketReuseStep::toString() const
{
	ostringstream oss;
	oss << "BucketReuse     ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId
		<< " st:" << fStepId << " tb/col:" << fTableOid << "/" << fOid;
	if (alias().length()) oss << " alias:" << alias();
	
	oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;

	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << fInputJobStepAssociation.outAt(i) << ",";

	return oss.str();
}

}
