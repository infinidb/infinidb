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
 * $Id: unionstep.cpp 8326 2012-02-15 18:58:10Z xlou $
 *
 ****************************************************************************/
#include "jobstep.h"
#include "datalist.h"
#include "calpontsystemcatalog.h"
#include "exceptclasses.h"

#include <time.h>
#include <sstream>
#include <set>

using namespace std;
using namespace execplan;


namespace {
using namespace joblist;

struct Unionizer {
	UnionStep *step;

	Unionizer(UnionStep *s) : step(s)
	{ }

	void operator()()
	{
		try
		{
			step->doUnion();
		}
		catch(const logging::LargeDataListExcept& ex)
		{
			cout << "unionstep: caught: " << ex.what() << endl;
			step->unblockDatalists(logging::unionStepLargeDataListFileErr);
			catchHandler(ex.what(), step->sessionId());
		}
		catch (const exception &e)
		{
			cout << "unionstep: caught: " << e.what() << endl;
			step->unblockDatalists(logging::unionStepErr);
			catchHandler(e.what(), step->sessionId()); //throw;  //@bug 632
		}
		catch (...)
		{
			string msg("Unionstep caught something not an exception!");
			cout << msg << endl;
			step->unblockDatalists(logging::unionStepErr);
			catchHandler(msg); //throw;  //@bug 632
		}
	}
};
};

namespace joblist {

UnionStep::UnionStep(const JobStepAssociation& in, const JobStepAssociation& out,
	CalpontSystemCatalog::OID tableOID,
	uint32_t session,
	uint32_t txn,
	uint32_t ver,
	uint16_t step,
	uint32_t statement) : JobStep(),
	inJSA(in),
	outJSA(out),
	fTableOID(tableOID),
	sessionID(session),
	txnID(txn),
	verID(ver),
	stepID(step),
	statementID(statement),
	inputSize(0),
	outputSize(0),
	fAlias1(),
	fAlias2(),
	fTimeSet()
{
}

UnionStep::~UnionStep()
{

}

const JobStepAssociation & UnionStep::inputAssociation() const
{
	return inJSA;
}

void UnionStep::inputAssociation(const JobStepAssociation &in)
{
	inJSA = in;
}

const JobStepAssociation & UnionStep::outputAssociation() const
{
	return outJSA;
}

void UnionStep::outputAssociation(const JobStepAssociation &out)
{
	outJSA = out;
}

void UnionStep::join()
{
	runner->join();
}

const string UnionStep::toString() const
{
	DataList_t* dl = outJSA.outAt(0)->dataList();
	ostringstream oss;
 	oss << "UnionStep       ses:" << sessionID << " txn:" << txnID << " ver:" << verID;
	oss << " st:" << stepId() << " tb/col:" << fTableOID << "/" << dl->OID();
	oss << " " << omitOidInDL
		<< outJSA.outAt(0) << showOidInDL;
	oss << " alias1: " << ((fAlias1.length()) ? fAlias1 : "none");
	oss << " alias2: " << ((fAlias2.length()) ? fAlias2 : "none");
	oss << " in:";
    for (unsigned i = 0; i < inJSA.outSize(); i++)
	{
		oss << inJSA.outAt(i) << ", ";
	}

	return oss.str();
}

void UnionStep::stepId(uint16_t id)
{
	stepID = id;
}

uint16_t UnionStep::stepId() const
{
	return stepID;
}

uint32_t UnionStep::sessionId() const
{
	return sessionID;
}

uint32_t UnionStep::txnId() const
{
	return txnID;
}

uint32_t UnionStep::statementId() const
{
	return statementID;
}

CalpontSystemCatalog::OID UnionStep::tableOid() const
{
	return fTableOID;
}


void UnionStep::run()
{
	if (traceOn())
	{
		syslogStartStep(16,         // exemgr subsystem
			string("UnionStep")); 	// step name
	}
	runner.reset(new boost::thread(Unionizer(this)));
}


void UnionStep::unblockDatalists(uint16_t status)
{
	outJSA.status(status);
	DataList<ElementType> *dlout = outJSA.outAt(0)->dataList();
	FIFO<RowWrapper<ElementType> > *fifo = outJSA.outAt(0)->fifoDL();
	StrDataList *strdlout = outJSA.outAt(0)->stringDataList();

	if (fifo) fifo->endOfInput();
	else if (dlout) dlout->endOfInput();
	else if (strdlout) strdlout->endOfInput();
}


void UnionStep::doUnion()
{
	DataList<ElementType> *dlIn1=inJSA.outAt(0)->dataList();
	DataList<ElementType> *dlIn2=inJSA.outAt(1)->dataList();
	ZDL<ElementType> *zdlIn1=inJSA.outAt(0)->zonedDL();
	ZDL<ElementType> *zdlIn2=inJSA.outAt(1)->zonedDL();
	FIFO<RowWrapper<ElementType> > *fifoIn1=inJSA.outAt(0)->fifoDL();
	FIFO<RowWrapper<ElementType> > *fifoIn2=inJSA.outAt(1)->fifoDL();

	DataList<StringElementType> *sdlIn1=inJSA.outAt(0)->strDataList();
	DataList<StringElementType> *sdlIn2=inJSA.outAt(1)->strDataList();
	ZDL<StringElementType> *szdlIn1=inJSA.outAt(0)->stringZonedDL();
	ZDL<StringElementType> *szdlIn2=inJSA.outAt(1)->stringZonedDL();
	FIFO<RowWrapper<StringElementType> > *sfifoIn1=inJSA.outAt(0)->stringDL();
	FIFO<RowWrapper<StringElementType> > *sfifoIn2=inJSA.outAt(1)->stringDL();

	if (0 < inJSA.status())
	{
		unblockDatalists(inJSA.status());
	}
	else
		{
		// make sure the input datalists are either ordered fifo or zdl
		// if not, throw an exception
		bool dlTypeOK = true;
		if (dlIn1 != NULL)
			dlTypeOK = dlTypeOK && (zdlIn1 != NULL || (fifoIn1 != NULL && fifoIn1->inOrder()));
		else if (sdlIn1 != NULL)
			dlTypeOK = dlTypeOK && (szdlIn1 != NULL || (sfifoIn1 != NULL && sfifoIn1->inOrder()));

		if (dlIn2 != NULL)
			dlTypeOK = dlTypeOK && (zdlIn2 != NULL || (fifoIn2 != NULL && fifoIn2->inOrder()));
		else if (sdlIn2 != NULL)
			dlTypeOK = dlTypeOK && (szdlIn2 != NULL || (sfifoIn2 != NULL && sfifoIn2->inOrder()));

		if (dlTypeOK == false)
			throw logic_error("UnionStep: input datalist is not ZDL or ordered FIFO.");

		// input lists are fifo
		if (fifoIn1 != NULL && fifoIn2 != NULL)
			unionByOrderedFifo(fifoIn1, fifoIn2);
		else if (sfifoIn1 != NULL && sfifoIn2 != NULL)
			unionByOrderedFifo(sfifoIn1, sfifoIn2);
		else if (fifoIn1 != NULL && sfifoIn2 != NULL)
			unionByOrderedFifo(fifoIn1, sfifoIn2);
		else if (sfifoIn1 != NULL && fifoIn2 != NULL)
			unionByOrderedFifo(sfifoIn1, fifoIn2);

		// input lists are zdl
		else if (zdlIn1 != NULL && zdlIn2 != NULL)
			unionByZdl(zdlIn1, zdlIn2);
		else if (sdlIn1 != NULL && sdlIn2 != NULL)
			unionByZdl(szdlIn1, szdlIn2);
		else if (dlIn1 != NULL && szdlIn2 != NULL)
			unionByZdl(zdlIn1, szdlIn2);
		else if (sdlIn1 != NULL && zdlIn2 != NULL)
			unionByZdl(szdlIn1, zdlIn2);

		// one is fifo, the other is zdl
		else if (fifoIn1 != NULL && zdlIn2 != NULL)
			unionByMixedDl(fifoIn1, zdlIn2);
		else if (fifoIn1 != NULL && szdlIn2 != NULL)
			unionByMixedDl(fifoIn1, szdlIn2);
		else if (sfifoIn1 != NULL && zdlIn2 != NULL)
			unionByMixedDl(sfifoIn1, zdlIn2);
		else if (sfifoIn1 != NULL && szdlIn2 != NULL)
			unionByMixedDl(sfifoIn1, szdlIn2);
		else if (fifoIn2 != NULL && zdlIn1 != NULL)
			unionByMixedDl(fifoIn2, zdlIn1);
		else if (fifoIn2 != NULL && szdlIn1 != NULL)
			unionByMixedDl(fifoIn2, szdlIn1);
		else if (sfifoIn2 != NULL && zdlIn1 != NULL)
			unionByMixedDl(sfifoIn2, zdlIn1);
		else // (sfifoIn2 != NULL && szdlIn1 != NULL)
			unionByMixedDl(sfifoIn2, szdlIn1);
	}// else inJSA.status == 0

	if (traceOn())
	{
		ostringstream oss;
		oss << "ses:" << sessionID << " st: " << stepID
			<< " finished at " << dlTimes.EndOfInputTimeString()
			<< "; input size-" << inputSize << "; output size-" << outputSize << endl
			<< "\t1st read " << dlTimes.FirstReadTimeString()
			<< "; EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
			   JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << "s"
			<< endl << "\tWork times: " << mergeTime << fTimeSet.totalTime(mergeTime) << " "
			<< insertTime << fTimeSet.totalTime(insertTime) 
			<< ",s\n\tJob completion status " << outJSA.status() << endl;

		logEnd(oss.str().c_str());

		syslogProcessingTimes(16,      // exemgr subsystem
			dlTimes.FirstReadTime(),   // first datalist read
			dlTimes.LastReadTime(),    // last  datalist read
			dlTimes.FirstReadTime(),   // first datalist write(use 1st read)
			dlTimes.LastInsertTime()); // last (endOfInput) datalist write
		syslogEndStep(16, // exemgr subsystem
			0,            // no blocked datalist input  to report
			0);           // no blocked datalist input  to report
	}
}

template<typename element1_t, typename element2_t>
void UnionStep::unionByOrderedFifo(FIFO<element1_t> *dl1, FIFO<element2_t> *dl2)
{
	DataList<ElementType> *dlout = outJSA.outAt(0)->dataList();
	FIFO<RowWrapper<ElementType> > *fifo = outJSA.outAt(0)->fifoDL();

	if (fifo)
	{
		element1_t              rw1;
		element2_t              rw2;
		RowWrapper<ElementType> rw3;

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &rw1);
		bool more2 = dl2->next(id2, &rw2);

		dlTimes.setFirstReadTime();

		uint64_t i=0, j=0;
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (rw1.et[i].first < rw2.et[j].first)
			{
				rw3.et[rw3.count++].first = rw1.et[i++].first;
			}
			else if (rw1.et[i].first > rw2.et[j].first)
			{
				rw3.et[rw3.count++].first = rw2.et[j++].first;
			}
			else
			{
				rw3.et[rw3.count++].first = rw1.et[i].first;
				++i;
				++j;
				++inputSize;
			}

			++inputSize;

			if (rw3.count == rw3.ElementsPerGroup)
			{
				fTimeSet.setTimer(insertTime);
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
				fTimeSet.holdTimer(insertTime);
			}

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
			if (j == rw2.count)
			{
				more2 = dl2->next(id2, &rw2);
				j = 0;
			}
		}
		fTimeSet.setTimer(mergeTime, false);

		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = rw1.et[i++].first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
		}

		while (more2 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = rw2.et[j++].first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			if (j == rw2.count)
			{
				more2 = dl2->next(id2, &rw2);
				j = 0;
			}
		}

		if (rw3.count > 0)
		{
			fifo->insert(rw3);
			outputSize += rw3.count;
			rw3.count = 0;
		}
		fTimeSet.holdTimer(insertTime);

		dlTimes.setLastReadTime();

		fifo->totalSize(outputSize);

		fifo->endOfInput();
		dlTimes.setEndOfInputTime();
	}

	else
	{
		element1_t  rw1;
		element2_t  rw2;
		ElementType e3;

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &rw1);
		bool more2 = dl2->next(id2, &rw2);

		dlTimes.setFirstReadTime();

		uint64_t i=0, j=0;
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (rw1.et[i].first < rw2.et[j].first)
			{
				e3.first = rw1.et[i++].first;
			}
			else if (rw1.et[i].first > rw2.et[j].first)
			{
				e3.first = rw2.et[j++].first;
			}
			else
			{
				e3.first = rw1.et[i].first;
				++i;
				++j;
				++inputSize;
			}

			dlout->insert(e3);
			++inputSize;

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
			if (j == rw2.count)
			{
				more2 = dl2->next(id2, &rw2);
				j = 0;
			}
		}
		fTimeSet.setTimer(mergeTime, false);
		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			while (i < rw1.count)
			{
				++inputSize;
				e3.first = rw1.et[i++].first;
				dlout->insert(e3);
			}
			more1 = dl1->next(id1, &rw1);
			i = 0;
		}

		while (more2 && !die)
		{
			while (j < rw2.count)
			{
				++inputSize;
				e3.first = rw2.et[j++].first;
				dlout->insert(e3);
			}
			more2 = dl2->next(id2, &rw2);
			j = 0;
		}
		fTimeSet.setTimer(insertTime, false);

		dlTimes.setLastReadTime();

		dlout->endOfInput();
		dlTimes.setEndOfInputTime();

		outputSize = dlout->totalSize();
	}
}


template<typename element1_t, typename element2_t>
void UnionStep::unionByZdl(ZDL<element1_t> *dl1, ZDL<element2_t> *dl2)
{
	DataList<ElementType> *dlout = outJSA.outAt(0)->dataList();
	FIFO<RowWrapper<ElementType> > *fifo = outJSA.outAt(0)->fifoDL();

	if (fifo)
	{
		element1_t              e1;
		element2_t              e2;
		RowWrapper<ElementType> rw3;

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &e1);
		bool more2 = dl2->next(id2, &e2);

		dlTimes.setFirstReadTime();
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (e1.first < e2.first)
			{
				rw3.et[rw3.count++].first = e1.first;
				more1 = dl1->next(id1, &e1);
			}
			else if (e1.first > e2.first)
			{
				rw3.et[rw3.count++].first = e2.first;
				more2 = dl2->next(id2, &e2);
			}
			else
			{
				rw3.et[rw3.count++].first = e1.first;
				++inputSize;
				more1 = dl1->next(id1, &e1);
				more2 = dl2->next(id2, &e2);
			}

			++inputSize;

			if (rw3.count == rw3.ElementsPerGroup)
			{
				fTimeSet.setTimer(insertTime);
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
				fTimeSet.holdTimer(insertTime);
			}
		}
		fTimeSet.setTimer(mergeTime,false);
		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = e1.first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			more1 = dl1->next(id1, &e1);
		}

		while (more2 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = e2.first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			more2 = dl2->next(id2, &e2);
		}

		// no more input
		if (rw3.count > 0 && !die)
		{
		    fifo->insert(rw3);
			outputSize += rw3.count;
			rw3.count = 0;
		}
		fTimeSet.holdTimer(insertTime);
		dlTimes.setLastReadTime();

		fifo->totalSize(outputSize);

		fifo->endOfInput();
		dlTimes.setEndOfInputTime();
	}

	else
	{
		element1_t  e1;
		element2_t  e2;
		ElementType e3;

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &e1);
		bool more2 = dl2->next(id2, &e2);

		dlTimes.setFirstReadTime();
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (e1.first < e2.first)
			{
				e3.first = e1.first;
				more1 = dl1->next(id1, &e1);
			}
			else if (e1.first > e2.first)
			{
				e3.first = e2.first;
				more2 = dl2->next(id2, &e2);
			}
			else
			{
				e3.first = e1.first;
				++inputSize;
				more1 = dl1->next(id1, &e1);
				more2 = dl2->next(id2, &e2);
			}

			dlout->insert(e3);
			++inputSize;
		}
		fTimeSet.setTimer(mergeTime, false);
		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			++inputSize;
			e3.first = e1.first;
			dlout->insert(e3);
			more1 = dl1->next(id1, &e1);
		}

		while (more2 && !die)
		{
			++inputSize;
			e3.first = e2.first;
			dlout->insert(e3);
			more2 = dl2->next(id2, &e2);
		}
		fTimeSet.setTimer(insertTime,false);

		dlTimes.setLastReadTime();

		dlout->endOfInput();
		dlTimes.setEndOfInputTime();

		outputSize = dlout->totalSize();
	}
}


template<typename element1_t, typename element2_t>
void UnionStep::unionByMixedDl(FIFO<element1_t> *dl1, ZDL<element2_t> *dl2)
{
	DataList<ElementType> *dlout = outJSA.outAt(0)->dataList();
	FIFO<RowWrapper<ElementType> > *fifo = outJSA.outAt(0)->fifoDL();

	if (fifo)
	{
		element1_t              rw1;
		element2_t              e2;
		RowWrapper<ElementType> rw3;

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &rw1);
		bool more2 = dl2->next(id2, &e2);

		dlTimes.setFirstReadTime();

		uint64_t i=0;
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (rw1.et[i].first < e2.first)
			{
				rw3.et[rw3.count++].first = rw1.et[i++].first;
			}
			else if (rw1.et[i].first > e2.first)
			{
				rw3.et[rw3.count++].first = e2.first;
				more2 = dl2->next(id2, &e2);
			}
			else
			{
				rw3.et[rw3.count++].first = rw1.et[i++].first;
				more2 = dl2->next(id2, &e2);
				++inputSize;
			}

			++inputSize;

			if (rw3.count == rw3.ElementsPerGroup)
			{
				fTimeSet.setTimer(insertTime);
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
				fTimeSet.holdTimer(insertTime);
			}

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
		}
		fTimeSet.setTimer(mergeTime, false);
		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = rw1.et[i++].first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
		}

		while (more2 && !die)
		{
			++inputSize;
			rw3.et[rw3.count++].first = e2.first;
			if (rw3.count == rw3.ElementsPerGroup)
			{
				fifo->insert(rw3);
				outputSize += rw3.count;
				rw3.count = 0;
			}

			more2 = dl2->next(id2, &e2);
		}

		if (rw3.count > 0)
		{
			fifo->insert(rw3);
			outputSize += rw3.count;
			rw3.count = 0;
		}
		fTimeSet.holdTimer(insertTime);

		dlTimes.setLastReadTime();

		fifo->totalSize(outputSize);

		fifo->endOfInput();
		dlTimes.setEndOfInputTime();
	}

	else
	{
		element1_t  rw1;
		element2_t  e2;
		ElementType e3;
		
		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();

		bool more1 = dl1->next(id1, &rw1);
		bool more2 = dl2->next(id2, &e2);

		dlTimes.setFirstReadTime();

		uint64_t i=0;
		fTimeSet.setTimer(mergeTime);
		while (more1 && more2 && !die)
		{
			if (rw1.et[i].first < e2.first)
			{
				e3.first = rw1.et[i++].first;
			}
			else if (rw1.et[i].first > e2.first)
			{
				e3.first = e2.first;
				more2 = dl2->next(id2, &e2);
			}
			else
			{
				e3.first = e2.first;
				more2 = dl2->next(id2, &e2);
				++i;
				++inputSize;
			}

			dlout->insert(e3);
			++inputSize;

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
		}
		fTimeSet.setTimer(mergeTime, false);
		fTimeSet.setTimer(insertTime);
		while (more1 && !die)
		{
			++inputSize;
			e3.first = rw1.et[i++].first;
			dlout->insert(e3);

			if (i == rw1.count)
			{
				more1 = dl1->next(id1, &rw1);
				i = 0;
			}
		}

		while (more2 && !die)
		{
			++inputSize;
			e3.first = e2.first;
			dlout->insert(e3);
			more2 = dl2->next(id2, &e2);
		}
		fTimeSet.setTimer(insertTime,false);
		dlTimes.setLastReadTime();

		dlout->endOfInput();
		dlTimes.setEndOfInputTime();

		outputSize = dlout->totalSize();
	}
}

};  //namespace
