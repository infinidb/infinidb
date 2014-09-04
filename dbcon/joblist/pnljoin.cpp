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

/*****************************************************************************
 * $Id: pnljoin.cpp 8436 2012-04-04 18:18:21Z rdempsey $
 *
 ****************************************************************************/

#include "elementtype.h"
#include "jobstep.h"

using namespace std;

namespace joblist {

#ifdef PROFILE
extern void timespec_sub(const struct timespec &tv1, const struct timespec &tv2,
	struct timespec &diff);
#endif


struct PNLJRunner
{
	PNLJRunner(PNLJoin* p) : joiner(p)
    {}
	PNLJoin *joiner;
    void operator()()
    {
        try
        {
			if (joiner->out.outSize() > 1)
	            joiner->doPNLJoin_reduce();
			else
				joiner->doPNLJoin();
        }
        catch(runtime_error& re)
        { 
		catchHandler(re.what());
	}
       catch(...)
        { 
		catchHandler("PNLJoin caught an unknown exception");
	}
    }
};

/* Override operator < so sets sort on value not rid */
class VElementType : public ElementType
{
	public:
		VElementType() : ElementType() { }
		VElementType(uint64_t f, uint64_t s) : ElementType(f, s) { }
		VElementType(const VElementType &d) : ElementType(d) { }
		VElementType(const ElementType &d) : ElementType(d) { }
		~VElementType() { };
		void operator=(const ElementType &d) { ElementType::operator=(d); }
		bool operator<(const VElementType &d) { return (second < d.second); }
};


PNLJoin::PNLJoin(const JobStepAssociation &i, const JobStepAssociation &o,
	DistributedEngineComm* de,	execplan::CalpontSystemCatalog* cat,
	uint32_t session, uint32_t txn, uint16_t step, uint32_t statement, ResourceManager& rm) 
	:
	in(i),
	out(o),
	sessionID(session),
	txnID(txn),
	stepID(step),
	statementID(statement),
	dec(de),
	syscat(cat),
	fRm(rm)
{
}

PNLJoin::~PNLJoin()
{ }		

const JobStepAssociation& PNLJoin::inputAssociation() const
{
	return in;
}

void PNLJoin::inputAssociation(const JobStepAssociation& inputAssociation)
{
	in = inputAssociation;
}

const JobStepAssociation& PNLJoin::outputAssociation() const
{
	return out;
}

void PNLJoin::outputAssociation(const JobStepAssociation& outputAssociation)
{
	out = outputAssociation;
}
		
const std::string PNLJoin::toString() const
{
	// XXXPAT: TBD.
	return string("PNLJoin");
}

void PNLJoin::stepId(uint16_t s)
{
	stepID = s;
}

uint16_t PNLJoin::stepId() const
{
	return stepID;
}

uint32_t PNLJoin::sessionId() const
{
	return sessionID;
}

uint32_t PNLJoin::txnId() const
{
	return txnID;
}

uint32_t PNLJoin::statementId() const
{
	return statementID;
}

void PNLJoin::run()
{
	if (traceOn())
	{
		syslogStartStep(16,          // exemgr subsystem
			std::string("PNLJoin")); // step name
	}

	runner.reset(new boost::thread(PNLJRunner(this)));
}

void PNLJoin::join()
{
	runner->join();
}

void PNLJoin::doPNLJoin()
{
	boost::shared_ptr<pColStep> step;
	boost::shared_ptr<pColScanStep> scan;
	JobStepAssociation inJSA, outJSA;
	AnyDataList adl;
	DataList<ElementType> *filterList;
	ElementType e;
	int it;
	bool more;
	try
	{
	if (0 < in.status())
		out.status(in.status());
	else
	{

	filterList = in.outAt(0)->dataList();

	// if no RID list, do pcolscan
	if (in.outSize() == 1) {
	
	/*
		make pColScanStep
			get in[0].oid
			set the output list to out[0]
			read in[0], step.addFilters()
		run step
	*/

		/* XXXPAT:  Will there be confusion here using the same stepID? */
		scan.reset(new pColScanStep(inJSA, out, dec, syscat, filterList->OID(), 
			0 /* fudge the tableoid */,	sessionID, txnID, txnID /* fudge the verID */,
					    stepID, statementID, fRm));
		
		it = filterList->getIterator();
		for (more = filterList->next(it, &e);
		  more;
		  more = filterList->next(it, &e))
			scan->addFilter(COMPARE_EQ, *((int64_t *) &e.second));
		
		scan->run();
		scan->join();
	}
	
	// if RID list, do pcolstep
	else {

	/*
		make pColStep
			get in[0].oid
			set the output list to out[0]
			read in[0], step.addFilters()
			set ridlist to in[1]
		run step
	*/

		inJSA.outAdd(in.outAt(1));
		step.reset(new pColStep(inJSA, out, dec, syscat, filterList->OID(),
			0 /* fudge the tableoid */,	sessionID, txnID, txnID /* fudge the verID */,
					stepID, statementID, fRm));

		it = filterList->getIterator();
		for (more = filterList->next(it, &e);
		  more;
		  more = filterList->next(it, &e))
			step->addFilter(COMPARE_EQ, *((int64_t *) &e.second));

		step->run();
		step->join();
	
	}
	}// in.status() == 0
   } //try
  catch(const exception& e)
    {
      catchHandler(e.what());
      out.status(logging::pnlJoinErr);
    }
  catch(...)
    {
      catchHandler("PNLJoin caught an unknown exception.");
      out.status(logging::pnlJoinErr);
    }


	if (traceOn())
	{
		syslogEndStep(16, // exemgr subsystem
			0,            // no blocked datalist input  to report
			0);           // no blocked datalist output to report
	}
}


void PNLJoin::doPNLJoin_reduce()
{
	// These BucketDLs are tuned to use ~500MB of memory apiece
  BucketDL<ElementType> bucketValueList(BUCKETS, 1, PERBUCKET, fRm);
  BucketDL<ElementType> stepResult(BUCKETS, 1, PERBUCKET, fRm);
	boost::shared_ptr<pColStep> step;
	boost::shared_ptr<pColScanStep> scan;
	JobStepAssociation inJSA, outJSA;
	DataList<ElementType> *filterList;
	DataList<ElementType> *out0, *out1;
	AnyDataListSPtr adl1(new AnyDataList());
	set<ElementType> searchSet;
	ElementType e;
	int it, i;
	bool more;


	filterList = in.outAt(0)->dataList();
	out0 = out.outAt(0)->dataList();
	out1 = out.outAt(1)->dataList();
	adl1->disown();
	adl1->bucketDL(&stepResult);
	outJSA.outAdd(adl1);
	bucketValueList.setHashMode(1);  	// use only the values for hash data
	stepResult.setHashMode(1);

	try
	{
	if (0 < in.status())
		out.status(in.status());
	else
	{

	if (in.outSize() == 1) {

	/*
		make pColScanStep
			get in[0].oid
			set the output list to stepResult
			read in[0] -> step.addFilters(), build in0tmp
		run step
	*/
		scan.reset(new pColScanStep(in, outJSA, dec, syscat, filterList->OID(), 
			0 /* fudge the tableoid */,	sessionID, txnID, txnID /* fudge the verID */,
					    stepID, statementID, fRm));
		
		it = filterList->getIterator();
		for (more = filterList->next(it, &e);
		  more;
		  more = filterList->next(it, &e)) {
			scan->addFilter(COMPARE_EQ, *((int64_t *) &e.second));
			bucketValueList.insert(e);
		}
		bucketValueList.endOfInput();

		scan->run();
		scan->join();
	

	}
	
	// if RID list, do pcolstep
	else {

	/*
		make pColStep
			get in[0].oid
			set the output list to stepResult
			read in[0] -> step.addFilters(), build in0tmp
			set ridlist to in[1]
		run step
	*/	

		inJSA.outAdd(in.outAt(1));
		step.reset(new pColStep(inJSA, outJSA, dec, syscat, filterList->OID(),
			0 /* fudge the tableoid */,	sessionID, txnID, txnID /* fudge the verID */,
					stepID, statementID, fRm));

		it = filterList->getIterator();
		for (more = filterList->next(it, &e);
		  more;
		  more = filterList->next(it, &e)) {
			step->addFilter(COMPARE_EQ, *((int64_t *) &e.second));
			bucketValueList.insert(e);
		}
		bucketValueList.endOfInput();

		step->run();
		step->join();

	}

	/* At this point, stepResult contains the result of the column operation,
		and bucketValueList contains the filter list in a bucketDL */

	/*
		reduce bucketValueList with stepResult into out[0] and out[1]

		Iterate over buckets,
			read stepResult,
				fill the set,
				copy into out[0].
			read bucketValueList
				set.find(), if match, put result in out[1]
	*/
	for (i = 0; i < BUCKETS; i++) {
		it = stepResult.getIterator(i);
		more = stepResult.next(i, it, &e);
		while (more) {
			searchSet.insert(VElementType(e));
			out0->insert(e);
			more = stepResult.next(i, it, &e);
		}

		it = bucketValueList.getIterator(i);
		more = bucketValueList.next(i, it, &e);
		while (more) {
			if (searchSet.find(VElementType(e)) != searchSet.end())
				out1->insert(e);
			more = bucketValueList.next(i, it, &e);
		}
		searchSet.clear();
	}

	} // if in.status == 0
        }// try
  catch(const exception& e)
    {
      catchHandler(e.what());
      out.status(logging::pnlJoinErr);
    }
  catch(...)
    {
      catchHandler("PNLJoin reduce caught an unknown exception.");
      out.status(logging::pnlJoinErr);
    }

	out0->endOfInput();
	out1->endOfInput();

	if (traceOn())
	{
		syslogEndStep(16, // exemgr subsystem
			0,            // no blocked datalist input  to report
			0);           // no blocked datalist output to report
	}
}


}  //namespace
