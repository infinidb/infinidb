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

//   $Id: joblistfactory.cpp 9128 2012-11-29 21:08:49Z xlou $


#include <iostream>
#include <stack>
#include <iterator>
#include <algorithm>
//#define NDEBUG
#include <cassert>
#include <vector>
#include <set>
#include <map>
#include <limits>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#define JOBLISTFACTORY_DLLEXPORT
#include "joblistfactory.h"
#undef JOBLISTFACTORY_DLLEXPORT

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "dbrm.h"
#include "filter.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "existsfilter.h"
#include "selectfilter.h"
#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "groupconcatcolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "rowcolumn.h"
#include "treenodeimpl.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "configcpp.h"
using namespace config;

#include "messagelog.h"
using namespace logging;

#include "elementtype.h"
#include "bucketdl.h"
#include "hashjoin.h"
#include "joblist.h"
#include "jobstep.h"
#include "jl_logger.h"
#include "pidxlist.h"
#include "pidxwalk.h"
#include "largedatalist.h"
#include "jlf_execplantojoblist.h"
#include "rowaggregation.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "expressionstep.h"
#include "tupleconstantstep.h"
#include "tuplehavingstep.h"

#include "jlf_common.h"
#include "jlf_graphics.h"
#include "jlf_subquery.h"
#include "jlf_tuplejoblist.h"

#include "rowgroup.h"
using namespace rowgroup;

#define FIFODEBUG() {} // do{cout<<"new FifoDataList allocated at: "<<__FILE__<<':'<<__LINE__<<endl;}while(0)

namespace
{
using namespace joblist;

//..Structure used to maintain info about BP steps during HJ step preparation.
//  o BPInfo objects are stored in a map where the key is a BP step id.
//  o bBPFeedSmallSideHj indicates if BP output feeds small-side of a HJ
//  o pInputLargeSideHj  HJ input to BP if BP is reading large-side of the HJ
struct BPInfo
{
	BPInfo() : bBPFeedSmallSideHj(false), pInputLargeSideHj(0) { }
	bool          bBPFeedSmallSideHj;
	HashJoinStep* pInputLargeSideHj;
};

//Find the next step downstream from *in. Assumes only the first such step is needed.
const JobStepVector::iterator getNextStep(JobStepVector::iterator& in, JobStepVector& list)
{
	JobStepVector::iterator end = list.end();

	for (unsigned i = 0; i < in->get()->outputAssociation().outSize(); ++i)
	{
		JobStepVector::iterator iter = list.begin();
		AnyDataListSPtr outAdl = in->get()->outputAssociation().outAt(i);

		while (iter != end)
		{
			if (iter != in)
			{
				AnyDataListSPtr inAdl;
				for (unsigned j = 0; j < iter->get()->inputAssociation().outSize(); j++)
				{
					inAdl = iter->get()->inputAssociation().outAt(j);
					if (inAdl.get() == outAdl.get())
						return iter;
				}
			}
			++iter;
		}
	}

	return end;
}

//Find the 2 parent steps of this THJS (these will either be 2 TupleBPS's or 1 TupleBPS & 1 THJS)
const pair<JobStepVector::iterator, JobStepVector::iterator>
	getParentSteps(JobStepVector::iterator& in, JobStepVector& list)
{
	pair<JobStepVector::iterator, JobStepVector::iterator> p;
	JobStepVector::iterator iter;
	JobStepVector::iterator end = list.end();
	AnyDataListSPtr inAdl;

	p = make_pair(end, end);

	iter = list.begin();
	inAdl = in->get()->inputAssociation().outAt(0);
	while (iter != end)
	{
		if (iter != in)
		{
			AnyDataListSPtr outAdl;
			for (unsigned i = 0; i < iter->get()->outputAssociation().outSize(); i++)
			{
				outAdl = iter->get()->outputAssociation().outAt(i);
				if (inAdl.get() == outAdl.get())
				{
					p.first = iter;
					break;
				}
			}
		}
		++iter;
	}

	iter = list.begin();
	inAdl = in->get()->inputAssociation().outAt(1);
	while (iter != end)
	{
		if (iter != in)
		{
			AnyDataListSPtr outAdl;
			for (unsigned i = 0; i < iter->get()->outputAssociation().outSize(); i++)
			{
				outAdl = iter->get()->outputAssociation().outAt(i);
				if (inAdl.get() == outAdl.get())
				{
					p.second = iter;
					break;
				}
			}
		}
		++iter;
	}

	return p;
}

//Used by replaceDataList to replace the correct datalist in a
//HashJoinStep step by matching the tableoid and alias.
void  linkHashJoinJSA(const HashJoinStep*  hjStep, JobStepAssociation& jsa, AnyDataListSPtr& indatalist, CalpontSystemCatalog::OID tableOid, const string& alias, const string& view)
{
	indatalist->zonedDL()->setMultipleProducers(true);
	if (tableOid == hjStep->tableOid2() && alias == hjStep->alias2() && view == hjStep->view2())
	{
		jsa.outAdd(hjStep->outputAssociation().outAt(0));
		jsa.outAdd(indatalist);
	}
	else
	{
		jsa.outAdd(indatalist);
		jsa.outAdd(hjStep->outputAssociation().outAt(1));
	}
}

//Used by replaceDataList to replace the correct datalist in a
//StringHashJoinStep step by matching the tableoid and alias.
void  linkHashJoinJSAStr(const StringHashJoinStep*  hjStep, JobStepAssociation& jsa, AnyDataListSPtr& indatalist, CalpontSystemCatalog::OID tableOid, const string& alias, const string& view)
{
	indatalist->zonedDL()->setMultipleProducers(true);
	if (tableOid == hjStep->tableOid2() && alias == hjStep->alias2() && view == hjStep->view2())
	{
		jsa.outAdd(hjStep->outputAssociation().outAt(0));
		jsa.outAdd(indatalist);
	}
	else
	{
		jsa.outAdd(indatalist);
		jsa.outAdd(hjStep->outputAssociation().outAt(1));
	}
}

//Only used for handling self-joins?
void replaceDataList(JobStep* jobStep, JobStepAssociation& jsa, AnyDataListSPtr& anydl, CalpontSystemCatalog::OID tableOid, const string& alias, const string& view, const JobInfo& jobInfo)
{
	HashJoinStep* hjStep = dynamic_cast<HashJoinStep*>(jobStep);
	StringHashJoinStep* shjStep = 0;
	if (!hjStep)
		shjStep = dynamic_cast<StringHashJoinStep*>(jobStep);
	ZonedDL* zdl = new ZonedDL(1, jobInfo.rm);
	zdl->OID(tableOid);
	anydl->zonedDL(zdl);

	if (hjStep)
	{
		linkHashJoinJSA(hjStep, jsa, anydl, tableOid, alias, view);
	}
	else if (shjStep)
	{
		linkHashJoinJSAStr(shjStep, jsa, anydl, tableOid, alias, view);
	}
	else
	{
		jsa.outAdd(anydl);
	}
	jobStep->outputAssociation(jsa);
}

/** put a copied shared_ptr of all child jobsteps of si to the passed vector
  * until the end of jsvec.
  */
void getChildJobSteps(const JobStep&       si,
					  const JobStepVector& jsvec,
					  JobStepVector&       childVec)
{
	childVec.clear();
	for (unsigned int i = 0; i < si.outputAssociation().outSize(); i++)
	{
		ptrdiff_t dloutptr = 0;
		DataList_t* dlout;
		StrDataList* sdl;
		TupleDataList* tdl;

		if ((dlout = si.outputAssociation().outAt(i)->dataList()))
			dloutptr = (ptrdiff_t)dlout;
		else if ((sdl = si.outputAssociation().outAt(i)->stringDataList()))
			dloutptr = (ptrdiff_t)sdl;
		else if ((tdl = si.outputAssociation().outAt(i)->tupleDataList()))
			dloutptr = (ptrdiff_t)tdl;

		for (unsigned int k = 0; k < jsvec.size(); k++)
		{
			JobStepAssociation queryInputSA = jsvec[k].get()->inputAssociation();
			for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
			{
				ptrdiff_t dlinptr = 0;
				DataList_t *dlin = queryInputSA.outAt(j)->dataList();
				StrDataList* sdl = 0;
				TupleDataList* tdl = 0;

				if (dlin)
					dlinptr = (ptrdiff_t)dlin;
				else if ((sdl = queryInputSA.outAt(j)->stringDataList()))
				{
					dlinptr = (ptrdiff_t)sdl;
				}
				else if ((tdl = queryInputSA.outAt(j)->tupleDataList()))
					dlinptr = (ptrdiff_t)tdl;

				if (dloutptr == dlinptr)
				{
					childVec.push_back(jsvec[k]);
				}
			}
		}
	}
}

void getChildJobSteps(const SJSTEP&        si,
					  const JobStepVector& jsvec,
					  JobStepVector&       childVec)
{
	getChildJobSteps(*si, jsvec, childVec);
}

//------------------------------------------------------------------------------
// put a copied shared_ptr of all parent jobsteps of si into parentVec
// until the end of jsvec.
// si               - search for parents of this jobstep
// jsvec            - this is the jobstep vector to be searched
// parentVec        - resulting parent jobstep vector
// parentToIgnore   - potential parent to be ignored/omitted from parentVec
//                    (this can be used to exclude one parent from the search
//                    if you already know that parent, and only need the other)
//------------------------------------------------------------------------------
void getParentJobSteps(const SJSTEP&        si,
					   const JobStepVector& jsvec,
					   JobStepVector&       parentVec,
					   AnyDataList*         pParentToIgnoreDL)
{
	parentVec.clear();

	ptrdiff_t dlIgnorePtr = 0;
	if (pParentToIgnoreDL)
	{
		DataList_t*    dlIgnore  = 0;
		StrDataList*   sdlIgnore = 0;
		TupleDataList* tdlIgnore = 0;

		if ((dlIgnore = pParentToIgnoreDL->dataList()))
			dlIgnorePtr = (ptrdiff_t)dlIgnore;
		else if ((sdlIgnore = pParentToIgnoreDL->stringDataList()))
			dlIgnorePtr = (ptrdiff_t)sdlIgnore;
		else if ((tdlIgnore = pParentToIgnoreDL->tupleDataList()))
			dlIgnorePtr = (ptrdiff_t)tdlIgnore;
	}

	// Loop thru input associations for specified child jobstep
	for (unsigned int i = 0; i < si->inputAssociation().outSize(); i++)
	{
		ptrdiff_t      dlinptr = 0;
		DataList_t*    dlin    = 0;
		StrDataList*   sdlin   = 0;
		TupleDataList* tdlin   = 0;

		if ((dlin = si->inputAssociation().outAt(i)->dataList()))
			dlinptr = (ptrdiff_t)dlin;
		else if ((sdlin = si->inputAssociation().outAt(i)->stringDataList()))
			dlinptr = (ptrdiff_t)sdlin;
		else if ((tdlin = si->inputAssociation().outAt(i)->tupleDataList()))
			dlinptr = (ptrdiff_t)tdlin;

		// Loop thru jsvec looking for parent jobsteps
		for (unsigned int k = 0; k < jsvec.size(); k++)
		{
			const JobStepAssociation& queryOutputSA =
				jsvec[k]->outputAssociation();

			// Loop thru output associations for each jobstep in jsvec
			for (unsigned int j = 0; j < queryOutputSA.outSize(); j++)
			{
				ptrdiff_t      dloutptr = 0;
				DataList_t*    dlout    = queryOutputSA.outAt(j)->dataList();
				StrDataList*   sdlout   = 0;
				TupleDataList* tdlout   = 0;

				if (dlout)
					dloutptr = (ptrdiff_t)dlout;
				else if ((sdlout = queryOutputSA.outAt(j)->stringDataList()))
					dloutptr = (ptrdiff_t)sdlout;
				else if ((tdlout = queryOutputSA.outAt(j)->tupleDataList()))
					dloutptr = (ptrdiff_t)tdlout;

				// If we find a match, then we found a parent.  Add to
				// parentVec as long as it is not the parent to be ignored.
				if (dloutptr == dlinptr)
				{
					if ((dlIgnorePtr) && (dloutptr == dlIgnorePtr))
					{
						// skip over this parent
						// cout << "Match is ignored (same as self): " << jsvec[k]->toString() << endl;
					}
					else
					{
						// if (jsvec[k]->oid() >= 3000) cout << "Match for " << si->toString() << " added to list: " << jsvec[k]->toString() << endl;
						parentVec.push_back(jsvec[k]);
					}
				}
			} // end of loop thru output associations of jobstep in jsvec
		}     // end of loop thru jobsteps in jsvec
	}         // end of loop thru input associations of child jobstep
}

void fixNumberConsumers(JobStepVector& querySteps, JobStepVector& projectSteps)
{
	//@bug 653. Walk through all job steps to check each datalist for number of consumers.
	//If not right, reset it.
	//Need to keep a map of number of input associations for each datalist
	typedef map<DataList_t*, uint> dlConMap;
	typedef map<StrDataList*, uint> strdlConMap;
	dlConMap dlConsumersMap;
	strdlConMap strdlConsumersMap;
	JobStepVector::const_iterator qsi = querySteps.begin();
	JobStepVector::const_iterator psi;

	for (; qsi != querySteps.end(); qsi++)
	{
		for (unsigned int i = 0; i < qsi->get()->inputAssociation().outSize(); i++)
		{
			DataList_t *dlin = qsi->get()->inputAssociation().outAt(i)->dataList();
			if (dlin != NULL)
			{
				dlConsumersMap[dlin]++;
			}
			else
			{
				StrDataList* dlin = qsi->get()->inputAssociation().outAt(i)->stringDataList();
				if (dlin != NULL)
				{
					strdlConsumersMap[dlin]++;
				}
			}
		}
	}

	for (psi = projectSteps.begin(); psi != projectSteps.end(); psi++)
	{
		for (unsigned int i = 0; i < psi->get()->inputAssociation().outSize(); i++)
		{
			DataList_t *dlin = psi->get()->inputAssociation().outAt(i)->dataList();
			if (dlin != NULL)
			{
				dlConsumersMap[dlin]++;

			}
			else
			{
				StrDataList* dlin = psi->get()->inputAssociation().outAt(i)->stringDataList();
				if (dlin != NULL)
				{
					strdlConsumersMap[dlin]++;
				}
			}
		}
	}

	//Finished building the maps. Now  , check whether they have right number of consumers
	dlConMap::const_iterator conIter;
	for (conIter = dlConsumersMap.begin(); conIter != dlConsumersMap.end(); conIter++)
	{
		if (AnyDataList::dlType(conIter->first) == AnyDataList::FIFO_DATALIST)
		{
			FifoDataList* fifoDl = reinterpret_cast< FifoDataList* >(conIter->first);
			if (fifoDl->getNumConsumers() != conIter->second)
			{
				fifoDl->resetNumConsumers(conIter->second);
				fifoDl->setNumConsumers(conIter->second);
			}
		}
		else if (AnyDataList::dlType(conIter->first) == AnyDataList::ZONED_DATALIST)
		{
			ZonedDL* bandedDl = dynamic_cast< ZonedDL* >(conIter->first);
			if (bandedDl->getNumConsumers() != conIter->second)
				bandedDl->resetNumConsumers(conIter->second);
		}
		else if (AnyDataList::dlType(conIter->first) == AnyDataList::ROWGROUP_DATALIST)
		{
			RowGroupDL* rgDl = reinterpret_cast<RowGroupDL*>(conIter->first);
			if (rgDl->getNumConsumers() != conIter->second)
			{
				rgDl->resetNumConsumers(conIter->second);
				rgDl->setNumConsumers(conIter->second);
			}
		}
	}
	strdlConMap::const_iterator strconIter;
	for (strconIter = strdlConsumersMap.begin(); strconIter != strdlConsumersMap.end(); strconIter++)
	{
		if (AnyDataList::strDlType(strconIter->first) == AnyDataList::STRING_DATALIST)
		{
			StringZonedDL* stringDl = dynamic_cast< StringZonedDL* >(strconIter->first);
			if (stringDl->getNumConsumers() != strconIter->second)
			{
				stringDl->resetNumConsumers(strconIter->second);
			}
		}
		else if (AnyDataList::strDlType(strconIter->first) == AnyDataList::STRINGFIFO_DATALIST)
		{
			StringFifoDataList* stringFifoDl = reinterpret_cast< StringFifoDataList* >(strconIter->first);
			if (stringFifoDl->getNumConsumers() != strconIter->second)
			{
				stringFifoDl->resetNumConsumers(strconIter->second);
				stringFifoDl->setNumConsumers(strconIter->second);
			}
		}
	}
}

/** remove unnecessary pcolstep after aggregatefilter step if col value is there
*/
void aggregateOptimize(JobStepVector& querySteps)
{
	JobStepVector::iterator qsi = querySteps.begin();
	JobStepVector::iterator nextqsi = querySteps.end();
	CalpontSystemCatalog::OID oid;
	CalpontSelectExecutionPlan::ReturnedColumnList gblist;
	vector<JobStepVector::iterator> eraseIts;

	for (; qsi != querySteps.end(); qsi++)
	{
		if (typeid(*(qsi->get())) == typeid(AggregateFilterStep))
		{
			assert (qsi->get()->outputAssociation().outSize() == 1);
			// only optimize one consumer case
			if (qsi->get()->outputAssociation().outAt(0)->getNumConsumers() > 1)
				continue;
			ptrdiff_t dloutptr;
			DataList_t* dlout = qsi->get()->outputAssociation().outAt(0)->dataList();
			if (dlout)
				dloutptr = (ptrdiff_t)dlout;
			else
			{
				StrDataList* sdl = qsi->get()->outputAssociation().outAt(0)->stringDataList();
				dloutptr = (ptrdiff_t)sdl;
			}
			for (nextqsi = qsi+1; nextqsi != querySteps.end(); nextqsi++)
			{
				if (nextqsi->get()->inputAssociation().outSize() != 1)
					continue;
				ptrdiff_t dlinptr;
				DataList_t *dlin = nextqsi->get()->inputAssociation().outAt(0)->dataList();
				if (dlin)
					dlinptr = (ptrdiff_t)dlin;
				else
				{
					StrDataList *sdl = nextqsi->get()->inputAssociation().outAt(0)->stringDataList();
					dlinptr = (ptrdiff_t)sdl;
				}
				if (dlinptr == dloutptr)
					break;
			}
			if (nextqsi != querySteps.end() &&
				 (typeid(*(nextqsi->get())) == typeid(pColStep) ||
				  typeid(*(nextqsi->get())) == typeid(pColScanStep)))
			{
				AggregateFilterStep *afs = dynamic_cast<AggregateFilterStep*> (qsi->get());
				pColStep *pcol = dynamic_cast<pColStep*>(nextqsi->get());
				if (pcol)
					oid = pcol->oid();
				else
					oid = dynamic_cast<pColScanStep*>(nextqsi->get())->oid();
				gblist = afs->groupByCols();
				for (uint i = 0; i < gblist.size(); i++)
				{
					SimpleColumn *sc = dynamic_cast<SimpleColumn*>(gblist[i].get());
					if (sc && oid == sc->oid())
					{
						// matched! remove the pcolstep
						qsi->get()->outputAssociation(nextqsi->get()->outputAssociation());
						afs->outputCol(oid);
						eraseIts.push_back(nextqsi);
						break;
					}
				}
			}
		}
	}

	// erase extra steps
	if (eraseIts.size() > 0)
	{
		for (uint i = eraseIts.size(); i > 0; i--)
			querySteps.erase(eraseIts[i-1]);
	}
}

bool checkCombinable(JobStep* jobStepPtr)
{
	if (typeid(*(jobStepPtr)) == typeid(pColScanStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(pColStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(pDictionaryStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(AggregateFilterStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(PassThruStep))
	{
		return true;
	}
	else if (typeid(*(jobStepPtr)) == typeid(FilterStep))
	{
		return true;
	}

	return false;
}

void walkJobStep(JobStepVector& querySteps, JobStepVector& projectSteps, BatchPrimitive* bps,
		std::set<unsigned int>& tobeErased, const JobInfo& jobInfo)
{
	DataList_t *dlin = 0, *dlin2 = 0;
	StrDataList* sdl = 0, *sdl2 = 0;
	DataList_t* dlout = 0;
	ptrdiff_t dloutptr = 0;
	JobStepAssociation outJs(jobInfo.status), inJs(jobInfo.status);
	if ((bps->outputAssociation().outAt(0)->getNumConsumers() > 1) && (!bps->getFeederFlag()))
		return;
	for (unsigned int i = 0; i < bps->outputAssociation().outSize(); i++)
	{
		dlout = bps->outputAssociation().outAt(i)->dataList();
		if (dlout)
		{
			dloutptr = (ptrdiff_t)dlout;
		}
		else
		{
			sdl = bps->outputAssociation().outAt(i)->stringDataList();
			if (sdl)
				dloutptr = (ptrdiff_t)sdl;
		}
		//Find out the steps use this output as their input
		JobStepVector::iterator qsi_in = querySteps.begin();
		std::set<unsigned int>::iterator findIter;
		for (unsigned int k = 0; qsi_in != querySteps.end(); qsi_in++,k++)
		{
			findIter = tobeErased.find(k);
			if (findIter != tobeErased.end())
			{
				continue;
			}
			if (checkCombinable(qsi_in->get()))
			{
				JobStepAssociation queryInputSA = qsi_in->get()->inputAssociation();
				if (queryInputSA.outSize() == 0)
					continue;
				ptrdiff_t dlinptr = 0, dlinptr2 = 0;

				sdl = 0;
				sdl2 = 0;
				if (typeid(*(qsi_in->get())) == typeid(FilterStep))
				{
					dlin = queryInputSA.outAt(0)->dataList();
					dlin2 = queryInputSA.outAt(1)->dataList();
					if (dlin || dlin2)
					{
						if (dlin && !dlin2)
						{
							dlinptr = (ptrdiff_t)dlin;
							sdl2 = queryInputSA.outAt(1)->stringDataList();
							dlinptr2 = (ptrdiff_t)sdl2;
						}
						else if (dlin2 && !dlin)
						{
							dlinptr2 = (ptrdiff_t)dlin2;
							sdl = queryInputSA.outAt(0)->stringDataList();
							dlinptr = (ptrdiff_t)sdl;
						}
						else
						{
							dlinptr = (ptrdiff_t)dlin;
							dlinptr2 = (ptrdiff_t)dlin2;
						}
					}
					else
					{
						sdl = queryInputSA.outAt(0)->stringDataList();
						sdl2 = queryInputSA.outAt(1)->stringDataList();
						if (sdl)
							dlinptr = (ptrdiff_t)sdl;
						else if (sdl2)
							dlinptr2 = (ptrdiff_t)sdl2;
					}
				}
				else
				{
					dlin = queryInputSA.outAt(0)->dataList();
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
					{
						sdl = queryInputSA.outAt(0)->stringDataList();
						if (sdl)
							dlinptr = (ptrdiff_t)sdl;
					}
				}

				if (((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr) ||
					((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr2))
				{
					if (typeid(*(qsi_in->get())) == typeid(FilterStep))
					{
						//Add another input jobstep for filter step
						ptrdiff_t dlFilterInptr = 0;
						if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
							dlFilterInptr = (ptrdiff_t)dlinptr2;
						else
							dlFilterInptr = (ptrdiff_t)dlinptr;

						findIter = tobeErased.find(k-1);
						if (findIter == tobeErased.end())
						{
							if (typeid(*((qsi_in-1)->get())) == typeid(pDictionaryStep))
							{
								findIter = tobeErased.find(k-2);
								if (findIter == tobeErased.end())
								{
									bps->setBPP((qsi_in-2)->get());
									bps->setStepCount();
									bps->setBPP((qsi_in-1)->get());
									bps->setStepCount();
									tobeErased.insert(k-1);
									tobeErased.insert(k-2);
								}
							}
							else
							{
								bps->setBPP((qsi_in-1)->get());
								bps->setStepCount();
								tobeErased.insert(k-1);
							}
						}
					}

					//@bug 1174
					pColStep *pcol = dynamic_cast<pColStep*>(qsi_in->get());
					bool stepTobeSkipped = false;
					if (pcol && (bps->getLastOid() == pcol->oid()) && (pcol->filterCount() == 0))
					{
						//@bug 1915
						JobStepVector::iterator qsi_next = qsi_in + 1;
						if (qsi_next == querySteps.end() ||
							typeid(*(qsi_next->get())) != typeid(FilterStep))
							stepTobeSkipped = true;
					}

					if (!stepTobeSkipped)
					{
						bps->setBPP(qsi_in->get());
						bps->setStepCount();
						bps->setLastOid(qsi_in->get()->oid());
					}

					//change output association
					outJs = qsi_in->get()->outputAssociation();
					bps->outputAssociation(outJs);
					//set output type
					dlout = outJs.outAt(0)->dataList();
					if (dlout)
					{
						bps->setOutputType(BPS_ELEMENT_TYPE);
						dloutptr = (ptrdiff_t)dlout;
					}
					else
					{
						sdl = outJs.outAt(0)->stringDataList();
						if (sdl)
						{
							bps->setOutputType(STRING_ELEMENT_TYPE);
							dloutptr = (ptrdiff_t)sdl;
						}
						else
						{
							bps->setOutputType(TABLE_BAND);
						}
					}
					tobeErased.insert(k);

					if (k >= querySteps.size())
						break;//?need to check
					else
						walkJobStep(querySteps, projectSteps,bps,tobeErased, jobInfo);
				}
			}
		}
	}
}

//------------------------------------------------------------------------------
// Get the necessary RID size for a column OID based on the current HWM
// for that column.  Possible return values are 8 bytes or 4 bytes.
//------------------------------------------------------------------------------
uint32_t getRidSizeBasedOnHwm(
	BRM::DBRM& dbrm,
	const execplan::CalpontSystemCatalog::OID& colOid,
	int        colWidth)
{
	uint32_t sizeOfRid = 8; // default to 8 bytes

	//...Get HWM for the specified column OID
	BRM::HWM_t hwm;
	int err = dbrm.getHWM(colOid, hwm);
	if (err != 0)
	{
		std::ostringstream errmsg;
		errmsg << "Error getting HWM for column OID " << colOid << "; " << err;
		throw std::runtime_error(errmsg.str());
	}

	//...Calculate highest RID, to see if it can be held within a 4 byte
	//   unsigned integer.  For example...
	//   For an 8 byte column, hwm<=4194302 yields RID that fits in uint32_t.
	//   hwm>=4194303 yields RID that requires uint64_t.
	double d_hwm        = hwm;
	double d_blksize    = BLOCK_SIZE;
	double d_colwid     = colWidth;
	double d_highestRID = ((d_hwm + 1.0) * d_blksize) / d_colwid;
	uint64_t highestRID = (uint64_t)d_highestRID;

	if (highestRID <= numeric_limits<uint32_t>::max())
		sizeOfRid = 4;

	return sizeOfRid;
}

#define defineMakeBP(StepType) BatchPrimitive* makeBP(const StepType& rhs, bool wantTuple=false) \
{ \
	BatchPrimitive* bp = 0; \
	if (wantTuple) \
		bp = new TupleBPS(rhs); \
	else \
		bp = new BatchPrimitiveStep(rhs); \
	return bp; \
}

defineMakeBP(pColScanStep)
defineMakeBP(pColStep)
defineMakeBP(pDictionaryStep)
defineMakeBP(PassThruStep)

#undef defineMakeBP

int combineJobSteps(JobStepVector& querySteps, JobStepVector& projectSteps,
		DeliveredTableMap& deliverySteps, const JobInfo& jobInfo)
{
	JobStepVector::iterator qsi = querySteps.begin();
	JobStepVector::iterator psi;
	unsigned int ctn = 0;
	DataList_t *dlin = 0;
	StrDataList* sdl = 0;
	DataList_t* dlout = 0;
	ptrdiff_t dloutptr=0;
	ptrdiff_t dlinputptr=0;
	JobStepAssociation outJs(jobInfo.status), inJs(jobInfo.status);
	SJSTEP newStep;
	BatchPrimitive* bps=0;
	bool swallowRowsFlag = ((jobInfo.traceFlags & CalpontSelectExecutionPlan::TRACE_NO_ROWS4) != 0);

	std::set<unsigned int> tobeErased;
	std::set<unsigned int> tobeErasedProject;
	for (; qsi != querySteps.end(); ctn++,qsi++)
	{
		if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
			continue;
		std::set<unsigned int>::iterator findIter;
		findIter = tobeErased.find(ctn);
		bool hasAggrFilter = false;
		if (findIter != tobeErased.end())
		{
			continue;
		}
		if (checkCombinable(qsi->get()))
		{
			//Build first step for BPS
			if (typeid(*(qsi->get())) == typeid(pColScanStep))
			{
				pColScanStep* pcss = dynamic_cast<pColScanStep*>(qsi->get());
				newStep.reset(makeBP(*pcss, jobInfo.tryTuples));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(SCAN);
				//cout << "BPS:first step is 	pColScanStep" << endl;
			}
			else if (typeid(*(qsi->get())) == typeid(pColStep))
			{
				pColStep* pcsp = dynamic_cast<pColStep*>(qsi->get());
				newStep.reset(makeBP(*pcsp, jobInfo.tryTuples));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(COLSTEP);
				//cout << "BPS:first step is 	pColStep" << endl;
			}
			else if (typeid(*(qsi->get())) == typeid(pDictionaryStep))
			{
				pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>(qsi->get());
				newStep.reset(makeBP(*pdsp, jobInfo.tryTuples));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(DICTIONARY);
				//cout << "BPS:first step is 	pDictionaryStep" << endl;
			}
/*			else if (typeid(*(qsi->get())) == typeid(pDictionaryScan))
			{
				pDictionaryScan* pdss = dynamic_cast<pDictionaryScan*>(qsi->get());
				newStep.reset(makeBP(*pdss, jobInfo.tryTuples));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(DICTIONARYSCAN);
			}
*/
			else if (typeid(*(qsi->get())) == typeid(PassThruStep))
			{
				PassThruStep* ptsp = dynamic_cast<PassThruStep*>(qsi->get());
				newStep.reset(makeBP(*ptsp, jobInfo.tryTuples));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(COLSTEP);
			}
			else if (typeid(*(qsi->get())) == typeid(AggregateFilterStep))
			{
				if (jobInfo.tryTuples) return -1;

				/* find out the steps uses this jobstep's input as its output
				*/
				hasAggrFilter = true;
				AggregateFilterStep* agfsptr = dynamic_cast<AggregateFilterStep*>(qsi->get());
				TupleBucketDataList* tbdl=qsi->get()->inputAssociation().outAt(0)->tupleBucketDL();
				assert(tbdl);
				CalpontSelectExecutionPlan::ReturnedColumnList gblist = agfsptr->groupByCols();
				CalpontSelectExecutionPlan::ReturnedColumnList aggrlist = agfsptr->aggCols();
				execplan::CalpontSystemCatalog::OID tableOid = agfsptr->tableOid ();
				CalpontSystemCatalog::OID dictOid;
				dlinputptr = (ptrdiff_t)qsi->get()->inputAssociation().outAt(0)->tupleBucketDL();
				bool hasPreviousStep = false;
				for (unsigned int m = 0; m < querySteps.size(); m++)
				{
					JobStepAssociation queryOutputSA = (querySteps[m].get())->outputAssociation();
					if (queryOutputSA.outSize() == 0)
						continue;
					ptrdiff_t dloutptr = 0;
					DataList_t *dlout = queryOutputSA.outAt(0)->dataList();
					StrDataList* sdl = 0;
					if (dlout)
						dloutptr = (ptrdiff_t)dlout;
					else
					{
						sdl = queryOutputSA.outAt(0)->stringDataList();
						if (sdl)
							dloutptr = (ptrdiff_t)sdl;
					}

					if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinputptr)
					{
						if ((querySteps[m].get())->outputAssociation().outSize() > 1)
							continue;

						if (checkCombinable(querySteps[m].get()))
						{
							hasPreviousStep = true;
							//Build first step for BPS
							if (typeid(*querySteps[m].get()) == typeid(pColScanStep))
							{
								pColScanStep* pcss =
									dynamic_cast<pColScanStep*>(querySteps[m].get());
								newStep.reset(makeBP(*pcss, jobInfo.tryTuples));
								bps = dynamic_cast<BatchPrimitive*>(newStep.get());
								bps->setJobInfo(&jobInfo);
								bps->setFirstStepType(SCAN);
							}
							else if (typeid(*querySteps[m].get()) == typeid(pColStep))
							{
								pColStep* pcsp = dynamic_cast<pColStep*>(querySteps[m].get());
								newStep.reset(makeBP(*pcsp, jobInfo.tryTuples));
								bps = dynamic_cast<BatchPrimitive*>(newStep.get());
								bps->setJobInfo(&jobInfo);
								bps->setFirstStepType(COLSTEP);
							}
							else if (typeid(*querySteps[m].get()) == typeid(pDictionaryStep))
							{
								pDictionaryStep* pdsp =
									dynamic_cast<pDictionaryStep*>(querySteps[m].get());
								newStep.reset(makeBP(*pdsp, jobInfo.tryTuples));
								bps = dynamic_cast<BatchPrimitive*>(newStep.get());
								bps->setJobInfo(&jobInfo);
								bps->setFirstStepType(DICTIONARY);
							}

							else if (typeid(*querySteps[m].get()) == typeid(PassThruStep))
							{
								PassThruStep* ptsp =
									dynamic_cast<PassThruStep*>(querySteps[m].get());
								newStep.reset(makeBP(*ptsp, jobInfo.tryTuples));
								bps = dynamic_cast<BatchPrimitive*>(newStep.get());
								bps->setJobInfo(&jobInfo);
								bps->setFirstStepType(COLSTEP);
							}
							assert(bps);
							bps->setBPP(querySteps[m].get());
							outJs = (querySteps[m].get())->outputAssociation();
							bps->outputAssociation(outJs);
							bps->setOutputType(TUPLE);
						}
						else if (typeid(*querySteps[m].get()) == typeid(BatchPrimitiveStep))
						{
							hasPreviousStep = true;
							BatchPrimitiveStep* bpst =
								dynamic_cast<BatchPrimitiveStep*>(querySteps[m].get());
							bpst->setOutputType(TUPLE);
						}
						else if (typeid(*querySteps[m].get()) == typeid(TupleBPS))
						{
							hasPreviousStep = true;
						}
						else // need insert a new BPS
						{
							//BatchPrimitiveStep* bps = new BatchPrimitiveStep();
						}
					}

				}
				if (!hasPreviousStep)
				{
					// Add a BatchPrimitiveStep in front of AggregateFilterStep
					CalpontSystemCatalog::ColType ct;
					SimpleColumn *sc = 0;
					bool foundCol = false;
					if (gblist.size() > 0)
					{
						for (uint i = 0; i < gblist.size(); i++)
						{
				   			 sc = dynamic_cast<SimpleColumn*>(gblist[i].get());
							 ct = jobInfo.csc->colType(sc->oid());
							 if (isDictCol(ct)> 0)
							 	continue;
							 else
							 {
							 	foundCol = true;
							 	//break;
							 }
						}
					}
					else if (!foundCol && aggrlist.size() > 0)
					{
						for (uint i = 0; i < aggrlist.size(); i++)
						{
							sc = dynamic_cast<SimpleColumn*>(aggrlist[i].get());
							ct = jobInfo.csc->colType(sc->oid());
							if (isDictCol(ct)> 0)
								continue;
							else
							{
								foundCol = true;
								break;
							}
						}
					}
					if (foundCol)
					{
						boost::scoped_ptr<pColScanStep> pcss(new pColScanStep(
							JobStepAssociation(jobInfo.status), JobStepAssociation(jobInfo.status),
							0, jobInfo.csc, sc->oid(), tableOid, jobInfo.sessionId, jobInfo.txnId,
							jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm));

						newStep.reset(makeBP(*pcss, jobInfo.tryTuples));
						bps = dynamic_cast<BatchPrimitive*>(newStep.get());
						bps->setJobInfo(&jobInfo);
						bps->setFirstStepType(SCAN);
						bps->setOutputType(TUPLE);
						bps->setBPP(pcss.get());
						SimpleColumn *scp = 0;
						BRM::DBRM dbrm;
						uint64_t hashLen = 0;
						uint64_t dataSize = 0;
						uint64_t ridSize = 0;
						//build project steps
						for (uint i = 0; i < gblist.size(); i++)
						{
							scp = dynamic_cast<SimpleColumn*>(gblist[i].get());
							ct = jobInfo.csc->colType(scp->oid());
							//TODO: if char column, need more investigation.
							hashLen += ct.colWidth;
							dataSize += ct.colWidth;
							boost::scoped_ptr<pColStep> pcs(new pColStep(
								JobStepAssociation(jobInfo.status),
								JobStepAssociation(jobInfo.status), 0, jobInfo.csc,
								scp->oid(), tableOid, jobInfo.sessionId, jobInfo.txnId,
								jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm));
							// @bug 884. set rid size based on hwm
							if (i == 0)
								ridSize = getRidSizeBasedOnHwm(dbrm, scp->oid(), ct.colWidth);
							if (scp->sameColumn(sc))
							{
								boost::scoped_ptr<PassThruStep> pts(new PassThruStep(
									*pcs, jobInfo.isExeMgr));
								pts->alias(pcs->alias());
								pts->view(pcs->view());
								pts->name(pcs->name());
								if (pts->isDictCol())
								{
									dictOid = isDictCol(ct);
									boost::scoped_ptr<pDictionaryStep> pds(new pDictionaryStep(
										JobStepAssociation(jobInfo.status),
										JobStepAssociation(jobInfo.status),
										0, jobInfo.csc, dictOid, ct.ddn.compressionType, tableOid,
										jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0,
										jobInfo.statementId, jobInfo.rm));

									pds->name(pcs->name());
									bps->setProjectBPP(pts.get(), pds.get());
									bps->setStepCount();
								}
								else {
									bps->setProjectBPP(pts.get(), NULL);
									bps->setStepCount();
								}
							}
							else
							{
								if (pcs->isDictCol())
								{
									dictOid = isDictCol(ct);
									boost::scoped_ptr<pDictionaryStep> pds(new pDictionaryStep(
										JobStepAssociation(jobInfo.status),
										JobStepAssociation(jobInfo.status),
										0, jobInfo.csc, dictOid, ct.ddn.compressionType,
										tableOid, jobInfo.sessionId, jobInfo.txnId,
										jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm));

									pds->name(pcs->name());
									bps->setProjectBPP(pcs.get(), pds.get());
									bps->setStepCount();
								}
								else
								{
									bps->setProjectBPP(pcs.get(), NULL);
									bps->setStepCount();
								}
							}
						}

						for (uint i = 0; i < aggrlist.size(); i++)
						{
							scp = dynamic_cast<SimpleColumn*>(aggrlist[i].get());
							ct = jobInfo.csc->colType(scp->oid());
							dataSize += ct.colWidth;
							boost::scoped_ptr<pColStep> pcs(new pColStep(
								JobStepAssociation(jobInfo.status),
								JobStepAssociation(jobInfo.status),
								0, jobInfo.csc, scp->oid(), tableOid, jobInfo.sessionId,
								jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm));
							if (scp->sameColumn(sc))
							{
								boost::scoped_ptr<PassThruStep> pts(new PassThruStep(
									*pcs, jobInfo.isExeMgr));
								pts->alias(pcs->alias());
								pts->view(pcs->view());
								pts->name(pcs->name());
								if (pts->isDictCol())
								{
									dictOid = isDictCol(ct);
									boost::scoped_ptr<pDictionaryStep> pds(new pDictionaryStep(
										JobStepAssociation(jobInfo.status),
										JobStepAssociation(jobInfo.status),
										0, jobInfo.csc, dictOid, ct.ddn.compressionType,
										tableOid, jobInfo.sessionId, jobInfo.txnId, jobInfo.verId,
										0, jobInfo.statementId, jobInfo.rm));

									pds->name(pcs->name());
									bps->setProjectBPP(pts.get(), pds.get());
									bps->setStepCount();
								}
								else
								{
									bps->setProjectBPP(pts.get(), NULL);
									bps->setStepCount();
								}
							}
							else
							{
								if (pcs->isDictCol())
								{
									dictOid = isDictCol(ct);
									boost::scoped_ptr<pDictionaryStep> pds(new pDictionaryStep(
										JobStepAssociation(jobInfo.status),
										JobStepAssociation(jobInfo.status),
										0, jobInfo.csc, dictOid, ct.ddn.compressionType, tableOid,
										jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0,
										jobInfo.statementId, jobInfo.rm));

									pds->name(pcs->name());
									bps->setProjectBPP(pcs.get(), pds.get());
									bps->setStepCount();
								}
								else
								{
									bps->setProjectBPP(pcs.get(), NULL);
									bps->setStepCount();
								}
							}
						}
						// set tuple datalist element size
						tbdl->elementLen(ridSize, dataSize);
						tbdl->hashLen(hashLen);
					}
				}
				else //Has previous primitive step
				{

				}

				//set input and output assciation
				inJs = qsi->get()->inputAssociation();
				bps->outputAssociation(inJs);
			}
			else
				continue;

			assert(bps);

			if (!hasAggrFilter)
			{
				bps->setBPP(qsi->get());
				//change in and output association
				outJs = qsi->get()->outputAssociation();
				bps->outputAssociation(outJs);
				//set output type
				dlin = outJs.outAt(0)->dataList();
				if (dlin)
					bps->setOutputType(BPS_ELEMENT_TYPE);
				else
				{
					sdl = outJs.outAt(0)->stringDataList();
					if (sdl)
						bps->setOutputType(STRING_ELEMENT_TYPE);
					else
					{
						bps->setOutputType(TABLE_BAND);
					}
				}
				inJs = qsi->get()->inputAssociation();
				bps->inputAssociation(inJs);
				bps->setLastOid(qsi->get()->oid());
				walkJobStep(querySteps, projectSteps, bps, tobeErased, jobInfo);
			}

			//check the project steps
			for (unsigned int i = 0; i < bps->outputAssociation().outSize(); i++)
			{
				dlout = bps->outputAssociation().outAt(i)->dataList();
				if (dlout)
				{
					dloutptr = (ptrdiff_t)dlout;
				}
				else
				{
					sdl = bps->outputAssociation().outAt(i)->stringDataList();
					dloutptr = (ptrdiff_t)sdl;
				}
				JobStepVector::iterator psi = projectSteps.begin();
				for (unsigned int m=0; psi < projectSteps.end(); m++,psi++)
				{
					std::set<unsigned int>::iterator findIter = tobeErasedProject.find(m);

					if (findIter != tobeErasedProject.end())
					{
						continue;
					}
					JobStepAssociation queryInputSA = psi->get()->inputAssociation();
					if (queryInputSA.outSize() == 0)
						continue;
					ptrdiff_t dlinptr = 0;
					DataList_t *dlin = queryInputSA.outAt(i)->dataList();
					StrDataList* sdl = 0;
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
					{
						sdl = queryInputSA.outAt(i)->stringDataList();
						if (sdl)
							dlinptr = (ptrdiff_t)sdl;
					}

					if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
					{
						bps->setStepCount();
						if (psi->get()->isDictCol())
						{
							if (jobInfo.trace && bps->tableOid() >= 3000)
								cout << "1 setting project BPP for " << bps->toString()
									 << " with " << psi->get()->toString() << " and "
									 << (psi+1)->get()->toString() << endl;
							bps->setProjectBPP(psi->get(),(psi+1)->get());
							outJs = (psi+1)->get()->outputAssociation();
							tobeErasedProject.insert(m);
							tobeErasedProject.insert(m+1);
							m++;
							psi++;
						}
						else
						{
							if (jobInfo.trace && bps->tableOid() >= 3000)
								cout << "2 setting project BPP for " << bps->toString()
									 << " with " << psi->get()->toString() << " and "
									 << "NULL" << endl;
							bps->setProjectBPP(psi->get(), NULL);
							outJs = psi->get()->outputAssociation();
							tobeErasedProject.insert(m);
						}
						bps->outputAssociation(outJs);
						bps->setOutputType(TABLE_BAND);

					}
				}
			}

			if  (bps->getStepCount() >= 1  && !hasAggrFilter)
			{
				*qsi = newStep;
			}
			else if (hasAggrFilter)
			{
				//Inser bps in front of AggreFilterStep
				unsigned int m=0;
				for (; m < querySteps.size(); m++)
				{
					if (typeid(*(querySteps[m].get())) == typeid(AggregateFilterStep))
						break;
				}
				querySteps.insert(querySteps.begin() + m, newStep) ;
				qsi = querySteps.begin() + m + 1;
				ctn++;
			}
			else
			{
				newStep.reset();
			}

			//if there is project step, add delivery step,too
			if (tobeErasedProject.size() > 0)
			{
				DeliveryStep* ds =
					dynamic_cast<DeliveryStep*>(deliverySteps[bps->tableOid()].get());
				assert (ds != 0);
				if (jobInfo.trace && bps->tableOid() >= 3000)
					cout << "3 setting project BPP for " << bps->toString() << " with "
						 << ds->toString() << " and " << "NULL" << endl;
				bps->setProjectBPP(ds, NULL);
				bps->setOutputType(TABLE_BAND);
				bool swallowRows = (bps->tableOid() >= 3000) && (swallowRowsFlag == true);
				bps->setSwallowRows(swallowRows);

				deliverySteps[bps->tableOid()] = newStep;
			}

			//remove the jobstep in tobeErasedProject vector

			JobStepVector newprojectSteps;
			std::set<unsigned int>::iterator findIter;
			for (unsigned int m=0; m < projectSteps.size(); m++)
			{
				if ((findIter=tobeErasedProject.find(m)) != tobeErasedProject.end())
					continue;
				else
				{
					newprojectSteps.push_back(projectSteps[m]);
				}
			}
			tobeErasedProject.clear();
			newprojectSteps.swap(projectSteps);
			newprojectSteps.clear();
		}
	}
	//remove the jobstep in tobeErased list
	JobStepVector newquerySteps;
	std::set<unsigned int>::iterator findIter;
	for (unsigned int m=0; m < querySteps.size(); m++)
	{
		if ((findIter=tobeErased.find(m)) != tobeErased.end())
			continue;
		else
		{
			newquerySteps.push_back(querySteps[m]);
		}
	}
	tobeErased.clear();
	newquerySteps.swap(querySteps) ;
	newquerySteps.clear();

	//combine project step
	//JobStepVector::const_iterator psi;
	ctn = 0;
	tobeErasedProject.clear();
	for (psi = projectSteps.begin(); psi < projectSteps.end(); ctn++,psi++)
	{
		std::set<unsigned int>::iterator findIter = tobeErasedProject.find(ctn);
		if (findIter != tobeErasedProject.end())
		{
			continue;
		}
		if (checkCombinable(psi->get()))
		{
			//Build first step for BPS. nextBand will be implemented
			if (typeid(*(psi->get())) == typeid(pColStep))
			{
				pColStep* pcsp = dynamic_cast<pColStep*>(psi->get());
				newStep.reset(makeBP(*pcsp));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(COLSTEP);
				bps->setIsProjectionOnly();
				if (psi->get()->isDictCol())
				{
					if (jobInfo.trace && bps->tableOid() >= 3000)
						cout << "4 setting project BPP for " << bps->toString() << " with " <<
							psi->get()->toString() << " and " << (psi+1)->get()->toString() << endl;
					bps->setProjectBPP(psi->get(),(psi+1)->get());
					outJs = (psi+1)->get()->outputAssociation();
					tobeErasedProject.insert(ctn+1);
					inJs = psi->get()->inputAssociation();
					//ctn++;
				}
				else
				{
					if (jobInfo.trace && bps->tableOid() >= 3000)
						cout << "5 setting project BPP for " << bps->toString() << " with " <<
							psi->get()->toString() << " and " << "NULL" << endl;
					bps->setProjectBPP(psi->get(), NULL);
					outJs = psi->get()->outputAssociation();
					inJs = psi->get()->inputAssociation();
				}
			}
			else if (typeid(*(psi->get())) == typeid(PassThruStep))
			{
				PassThruStep* passthru = dynamic_cast<PassThruStep*>(psi->get());
				newStep.reset(makeBP(*passthru));
				bps = dynamic_cast<BatchPrimitive*>(newStep.get());
				bps->setJobInfo(&jobInfo);
				bps->setFirstStepType(COLSTEP);
				bps->setIsProjectionOnly();
				if (psi->get()->isDictCol())
				{
					if (jobInfo.trace && bps->tableOid() >= 3000)
						cout << "6 setting project BPP for " << bps->toString() << " with " <<
							psi->get()->toString() << " and " << (psi+1)->get()->toString() << endl;
						bps->setProjectBPP(psi->get(),(psi+1)->get());
					outJs = (psi+1)->get()->outputAssociation();
					tobeErasedProject.insert(ctn+1);
					inJs = psi->get()->inputAssociation();
				}
				else
				{
					if (jobInfo.trace && bps->tableOid() >= 3000)
						cout << "7 setting project BPP for " << bps->toString() << " with " <<
							psi->get()->toString() << " and " << "NULL" << endl;
					bps->setProjectBPP(psi->get(), NULL);
					outJs = psi->get()->outputAssociation();
					inJs = psi->get()->inputAssociation();
				}
			}
			else
				continue;


			//change in and output association
			bps->outputAssociation(outJs);
			bps->inputAssociation(inJs);

			//Check whether there are steps using the same input datalist
			DataList_t *dlin = inJs.outAt(0)->dataList();
			if (dlin)
			{
				dlinputptr = (ptrdiff_t)dlin;
			}
			else
			{
				sdl = inJs.outAt(0)->stringDataList();
				dlinputptr = (ptrdiff_t)sdl;
			}

			JobStepVector::iterator psi_in = projectSteps.begin() + ctn + 1;
			for (unsigned int k = ctn; psi_in != projectSteps.end(); ++psi_in,k++)
			{
				findIter = tobeErasedProject.find(k+1);
				if (findIter != tobeErasedProject.end())
				{
					continue;
				}
				JobStepAssociation queryInputSA = psi_in->get()->inputAssociation();
				ptrdiff_t dlinptr = 0;
				DataList_t *dlin = queryInputSA.outAt(0)->dataList();
				StrDataList* sdl = 0;
//				TupleBucketDataList* tdl = 0;
				if (dlin)
					dlinptr = (ptrdiff_t)dlin;
				else
				{
					sdl = queryInputSA.outAt(0)->stringDataList();
					if (sdl)
					dlinptr = (ptrdiff_t)sdl;
/*					//For tupleBucketDL
					else
					{
						tdl = queryInputSA.outAt(k)->tupleBucketDL();
						if (tdl)
						dlinptr = (ptrdiff_t)tdl;
					}
*/
				}

				if ((ptrdiff_t)dlinputptr == (ptrdiff_t)dlinptr)
				{
					if (psi_in->get()->isDictCol())
					{
						if (jobInfo.trace && bps->tableOid() >= 3000)
							cout << "8 setting project BPP for " << bps->toString() << " with " <<
								psi_in->get()->toString() << " and " <<
								(psi_in+1)->get()->toString() << endl;
						bps->setProjectBPP(psi_in->get(),(psi_in+1)->get());
						outJs = (psi_in+1)->get()->outputAssociation();
						tobeErasedProject.insert(k+1);
						tobeErasedProject.insert(k+2);
						k++;
						psi_in++;
					}
					else
					{
						if (jobInfo.trace && bps->tableOid() >= 3000)
							cout << "9 setting project BPP for " << bps->toString() << " with " <<
								psi_in->get()->toString() << " and " << "NULL" << endl;
						bps->setProjectBPP(psi_in->get(), NULL);
						tobeErasedProject.insert(k+1);
					}
				}
			}
		}

		//Add delivery step
		bps->setOutputType(TABLE_BAND);
		DeliveryStep* ds = dynamic_cast<DeliveryStep*>(deliverySteps[bps->tableOid()].get());
		if (jobInfo.trace && bps->tableOid() >= 3000)
			cout << "10 setting project BPP for " << bps->toString() << " with " << ds->toString() <<
				" and " << "NULL" << endl;
		bps->setProjectBPP(ds, NULL);
		bool swallowRows = (bps->tableOid() >= 3000) && (swallowRowsFlag == true);
		bps->setSwallowRows(swallowRows);
		*psi = newStep;
		deliverySteps[bps->tableOid()] = newStep;
	}

	JobStepVector newprojectSteps;
	for (unsigned int m=0; m < projectSteps.size(); m++)
	{
		if ((findIter=tobeErasedProject.find(m)) != tobeErasedProject.end())
			continue;
		else
		{
			newprojectSteps.push_back(projectSteps[m]);
		}
	}
	tobeErasedProject.clear();
	newprojectSteps.swap(projectSteps) ;
	newprojectSteps.clear();

	return 0;
}

//------------------------------------------------------------------------------
// When hashjoin's large-side is feeding a projection step, this method is
// called to convert the HJ large-side output from FIFO to Delivery WSDL.  The
// Delivery WSDL datalist provides a sink to store the data to disk if we end
// up sending the data to this projection step before the front-end has asked
// for it.
// pHj         - the HashJoinStep feeding the projection step
// largeSide   - 0/1 flag indicating which side of pHj is the large-side
// projectStep - the projection step pHj is feeding
// JobInfo     - joblist's JobInfo structure.
//------------------------------------------------------------------------------
void convertLargeSideHJOutToWsdl(
	HashJoinStep*  pHj,
	unsigned int   largeSide,
	SJSTEP&        projectStep,
	const JobInfo& jobInfo)
{
	const uint32_t MAX_WSDL_ELEMS = (1024*1024)/16;//dmc
	FifoDataList* fdl = pHj->outputAssociation().outAt(largeSide)->fifoDL();
	if (fdl)
	{
		DeliveryWSDL* pDelDL = new DeliveryWSDL(
			fdl->getNumConsumers(),
			fdl->maxElements(),                  // max FIFO elements
			MAX_WSDL_ELEMS,                      // max WSDL elements
			fdl->getDiskElemSize1st(),
			fdl->getDiskElemSize2nd(),
			jobInfo.rm,
			projectStep->tableOid(),
			jobInfo.projectingTableOID);
		if (fdl->getElementMode() == FifoDataList::RID_ONLY)
			pDelDL->setElementMode(DeliveryWSDL::RID_ONLY);
		else
			pDelDL->setElementMode(DeliveryWSDL::RID_VALUE);
		AnyDataListSPtr spdl(new AnyDataList());
		spdl->deliveryWSDL(pDelDL);

		// Place new DelWSDL into input JSA of projection step
		JobStepAssociation projInJsa(jobInfo.status);
		projInJsa.outAdd(spdl);
		projectStep->inputAssociation(projInJsa);

		// Place new delivery WSDL into large-side output JSA of hj step.
		// On the small-side we keep the datalist that is already there.
		JobStepAssociation hjOutJsa(jobInfo.status);
		for (unsigned int i=0; i<pHj->outputAssociation().outSize(); ++i)
		{
			if (i == largeSide)
				hjOutJsa.outAdd(spdl);
			else
				hjOutJsa.outAdd(pHj->outputAssociation().outAt(i));
		}
		pHj->outputAssociation(hjOutJsa);
	}
}

//------------------------------------------------------------------------------
// When hashjoin's small-side is feeding a union step, this method is
// called to convert the HJ small-side output from ZDL to "ordered" FIFO.  By
// default a ZDL is used to connect a hashjoin to a unionstep.  But since
// the small-side output from the hashjoin is guaranteed to be in RID sorted
// order, the small-side that connects to a unionstep can employ an ordered
// FIFO.
// pHj         - the HashJoinStep feeding the union step
// smallSide   - 0/1 flag indicating which side of pHj is the small-side
// unionStep   - the union step pHj is feeding
// JobInfo     - joblist's JobInfo structure.
//------------------------------------------------------------------------------
void convertSmallSideHJOutToOrderedFifo(
	HashJoinStep*  pHj,
	unsigned int   smallSide,
	SJSTEP&        unionStep,
	const JobInfo& jobInfo)
{
	// Create the new ordered FIFO
	AnyDataListSPtr spdlFifo(new AnyDataList());
FIFODEBUG();
	FifoDataList* pFifo = new FifoDataList(1, jobInfo.fifoSize);
	pFifo->inOrder(true);

	// Change JSA for hashjoin output, placing new FIFO on small-side, but
	// on the large-side we keep the datalist that was already there.
	DataList_t* dlSmallIn=pHj->outputAssociation().outAt(smallSide)->dataList();
	pFifo->setDiskElemSize(
		dlSmallIn->getDiskElemSize1st(),
		dlSmallIn->getDiskElemSize2nd());
	pFifo->OID(dlSmallIn->OID());
	spdlFifo->fifoDL(pFifo);
	JobStepAssociation outJsa(jobInfo.status);
	for (unsigned int i=0; i<pHj->outputAssociation().outSize(); ++i)
	{
		if (i == smallSide)
			outJsa.outAdd(spdlFifo);
		else
			outJsa.outAdd(pHj->outputAssociation().outAt(i));
	}
	pHj->outputAssociation(outJsa);

	// Change JSA for unionstep input, placing new FIFO on small-side, but
	// on the large-side we keep the datalist that was already there.
	JobStepAssociation inJsa(jobInfo.status);
	for (unsigned iu=0; iu<unionStep->inputAssociation().outSize(); ++iu)
	{
		AnyDataListSPtr spdlUnionIn = unionStep->inputAssociation().outAt(iu);
		if ((ptrdiff_t)(spdlUnionIn->dataList()) == (ptrdiff_t)dlSmallIn)
			inJsa.outAdd(spdlFifo);
		else
			inJsa.outAdd(spdlUnionIn);
	}
	unionStep->inputAssociation(inJsa);
}

//------------------------------------------------------------------------------
// To help support runtime hashjoin algorithm selection, we try to find the
// smaller hashjoin input side.  HashJoinStep will read that side in first as
// it tries to select the optimal hashjoin algorithm.  If only one side is a
// BP step, then the other side must be treated as the small-side to be read
// first, as the large-side input "must" come from a BP.  This function also
// detects pattern of HJ(large-side)->BP->HJ(small-side).  When this occurs,
// FIFO disk caching used to avoid blocking for the first HJ can be disabled.
// querySteps - query execution job steps
// projectStep- projection job steps
//------------------------------------------------------------------------------
void prepHashJoinSteps(
	JobStepVector& querySteps, JobStepVector& projectSteps,
	const JobInfo& jobInfo)
{
	if(jobInfo.trace)
	{
		ostringstream oss;
		oss << std::endl << "Prepping HashJoinSteps..." << std::endl;
		cout << oss.str();
	}

	// Datatypes used to collect info about BP steps and their HJ connections
	typedef map<uint16_t,BPInfo> BPInfoMap;
	typedef BPInfoMap::iterator  BPInfoMapIter;
	BPInfoMap bpSteps;

	for (unsigned int k = 0; k < querySteps.size(); k++)
	{
		if (typeid(*querySteps[k].get()) == typeid(HashJoinStep) ||
			typeid(*querySteps[k].get()) == typeid(TupleHashJoinStep))
		{
			HashJoinStep* pHj = dynamic_cast<HashJoinStep*>(querySteps[k].get());
			TupleHashJoinStep* pThj = dynamic_cast<TupleHashJoinStep*>(querySteps[k].get());

			assert((pHj && !pThj) || (!pHj && pThj));

			// The 2 steps returned by getParentJobSteps should be for
			// HashJoinStep's inputAssociation[0] and inputAssociation[1]
			// respectively.
			JobStepVector parentSteps;
			getParentJobSteps(querySteps[k], querySteps, parentSteps, 0);

			assert (parentSteps.size() == 2); // Make sure HJ has 2 parents
			const std::type_info& step0Type = typeid(*parentSteps[0]);
			const std::type_info& step1Type = typeid(*parentSteps[1]);

			// We don't need the output for the pDictionaryScan side of a HJ
			if (pHj && step0Type == typeid(pDictionaryScan))
				pHj->setOutputMode(HashJoinStep::RIGHTONLY);
			else if (pHj && step1Type == typeid(pDictionaryScan))
				pHj->setOutputMode(HashJoinStep::LEFTONLY);

			// If only one side is BPS, then we treat that as the large side,
			// else we flag the larger side based on the cardinality.
			unsigned int largeSide = 99;
			unsigned int smallSide = 99;
			if ((step0Type == typeid(BatchPrimitiveStep) &&
					step1Type != typeid(BatchPrimitiveStep)) ||
				(step0Type == typeid(TupleBPS) &&
					step1Type != typeid(TupleBPS)))
			{
				largeSide = 0;
				smallSide = 1;
			}
			else
			if ((step0Type != typeid(BatchPrimitiveStep) &&
					step1Type == typeid(BatchPrimitiveStep)) ||
				(step0Type != typeid(TupleBPS) &&
					step1Type == typeid(TupleBPS)))
			{
				largeSide = 1;
				smallSide = 0;
			}
			else
			if ((step0Type == typeid(BatchPrimitiveStep) &&
					step1Type == typeid(BatchPrimitiveStep)) ||
				(step0Type == typeid(TupleBPS) &&
					step1Type == typeid(TupleBPS)))
			{
				if (parentSteps[0]->cardinality() <= parentSteps[1]->cardinality())
				{
					largeSide = 1;
					smallSide = 0;
				}
				else
				{
					largeSide = 0;
					smallSide = 1;
				}
			}
			else
			{
				// Neither input side to hashjoin is a BatchPrimitiveStep
				ostringstream oss;
				oss << "  " << (pHj? "":"T") << "HJ step " << (pHj?pHj->stepId():pThj->stepId()) <<
					"; neither parent (" << parentSteps[0]->stepId() << ","
					<< parentSteps[1]->stepId() << ") is BPS" << std::endl;
				std::cerr << oss.str();
				assert(largeSide==99);
				continue;
			}

			if (pThj)
			{
//				pDictionaryScan* pds = dynamic_cast<pDictionaryScan*>(parentSteps[smallSide].get());
//				if (!pds)
//					std::swap(largeSide,smallSide);
				// why swap?
				// Only TuplbBPS can be large side anyway
				TupleBPS* tbps = dynamic_cast<TupleBPS*>(parentSteps[smallSide].get());
				if (tbps)
					std::swap(largeSide,smallSide);
			}

			// Flag the BP that feeds the small-side of this hashjoin
			if (typeid(*parentSteps[smallSide]) == typeid(BatchPrimitiveStep) ||
				typeid(*parentSteps[smallSide]) == typeid(TupleBPS))
			{
				bpSteps[parentSteps[smallSide]->stepId()].bBPFeedSmallSideHj = true;
			}

			if(jobInfo.trace)
			{
				ostringstream oss;
				oss << "  " << (pHj? "":"T") << "HJ step " << (pHj?pHj->stepId():pThj->stepId()) <<
				" setting input step " << parentSteps[largeSide]->stepId()<<
				" to be large side" << std::endl;
				cout << oss.str();
			}

			// set delay flag on 'largeside' input jobstep, and pass
			// hashjoin step the necessary small and large side info.
			//cout << "Setting delay flag for queryParentStep " <<
			//	parentSteps[largeSide]->stepId() << std::endl;
			parentSteps[largeSide]->incWaitToRunStepCnt();
			BatchPrimitive* pBp = dynamic_cast<BatchPrimitive*>(parentSteps[largeSide].get());

			ptrdiff_t hjLargeOut=-1;
			ptrdiff_t hjSmallOut=-2;

			if (pHj)
			{
				pHj->setLargeSideBPS(pBp);
				pHj->setSmallSideCardEst(parentSteps[smallSide]->cardinality());
				if (largeSide == 0)
					pHj->setSizeFlag(false);//false means input[0] is largeside
				else
					pHj->setSizeFlag(true); //true  means input[1] is largeside

				// if largeside hashjoin output is FIFO, we need to up the FIFO size
				FifoDataList* fifoDL = pHj->outputAssociation().outAt(largeSide)->fifoDL();
				if ((fifoDL) && (pHj->tableOid2() >= 3000))
				{
					fifoDL->maxElements(jobInfo.fifoSizeLargeSideHj);
				}

				hjLargeOut = (ptrdiff_t)pHj->outputAssociation().outAt(largeSide)->dataList();
				hjSmallOut = (ptrdiff_t)pHj->outputAssociation().outAt(smallSide)->dataList();
			}
			else
			{
				if (!pBp)
					pBp = dynamic_cast<BatchPrimitive*>(parentSteps[smallSide].get());
				assert(pBp);

				pThj->setLargeSideBPS(pBp);
				if (jobInfo.trace) cout << "Large side set to: " << pBp->toString() << endl;

#if 0
				// if largeside hashjoin output is FIFO, we need to up the FIFO size
				FifoDataList* fifoDL = pThj->outputAssociation().outAt(largeSide)->fifoDL();
				if ((fifoDL) && (pThj->tableOid2() >= 3000))
				{
					fifoDL->maxElements(jobInfo.fifoSizeLargeSideHj);
				}

cout << "outSize = " << pThj->outputAssociation().outSize() << ", largeSide = " << largeSide << ", smallSide = " << smallSide << endl;
				assert(pThj->outputAssociation().outSize() > largeSide);
				//assert(pThj->outputAssociation().outSize() > smallSide);
				hjLargeOut = (ptrdiff_t)pThj->outputAssociation().outAt(largeSide)->dataList();
				if (pThj->outputAssociation().outSize() > smallSide)
					hjSmallOut = (ptrdiff_t)pThj->outputAssociation().outAt(smallSide)->dataList();
#endif
			}

			vector<SJSTEP> hjLargeSideStepsOut;
			vector<SJSTEP> hjSmallSideStepsOut;

			if (pHj)
			{
				// set delay flag on child query step(s) and pass reference
				// to this child jobstep to the hashjoin step, so that
				// the hashjoin step can start this child step later on.
				// (also changes JSA between HJ and Union if applicable)
				JobStepVector   childQuerySteps;
				getChildJobSteps(querySteps[k], querySteps, childQuerySteps);
				for (unsigned int m=0; m<childQuerySteps.size(); m++)
				{
					//cout << "Setting delay flag for queryChildStep " <<
					//	childQuerySteps[m]->stepId() << std::endl;
					childQuerySteps[m]->incWaitToRunStepCnt();
					for (unsigned int n=0;
						n<childQuerySteps[m]->inputAssociation().outSize();
						n++)
					{
						ptrdiff_t dlinptr = (ptrdiff_t)
						childQuerySteps[m]->inputAssociation().outAt(n)->dataList();
						if (dlinptr == hjSmallOut)
						{
							// if small-side HJ feeds a union step, we change
							// the connecting association from zdl to ordered fifo.
							if (pHj && typeid(*childQuerySteps[m]) == typeid(UnionStep))
								convertSmallSideHJOutToOrderedFifo(pHj, smallSide,
									childQuerySteps[m], jobInfo);

							hjSmallSideStepsOut.push_back(childQuerySteps[m]);
						}
						else if (dlinptr == hjLargeOut)
						{
							hjLargeSideStepsOut.push_back(childQuerySteps[m]);
						}
					}
				}

				// set delay flag on child projection step(s) and pass reference
				// to this child jobstep to the hashjoin step, so that
				// the hashjoin step can start this child step later on.
				// (also changes JSA between HJ and projection step if applicable)
				JobStepVector childProjSteps;
				getChildJobSteps(querySteps[k], projectSteps, childProjSteps);
				for (unsigned int m=0; m<childProjSteps.size(); m++)
				{
					//cout << "Setting delay flag for ProjStep " <<
					//	childProjSteps[m]->stepId() << std::endl;
					childProjSteps[m]->incWaitToRunStepCnt();
					for (unsigned int n=0;
						n<childProjSteps[m]->inputAssociation().outSize();
						n++)
					{
						ptrdiff_t dlinptr = (ptrdiff_t)
						childProjSteps[m]->inputAssociation().outAt(n)->dataList();
						if (dlinptr == hjLargeOut)
						{
							// if large-side HJ feeds a projection step, we change
							// the connecting association from fifo to a del-wsdl.
							if (pHj)
								convertLargeSideHJOutToWsdl (pHj, largeSide,
									childProjSteps[m], jobInfo);

							hjLargeSideStepsOut.push_back(childProjSteps[m]);
						}
						else if (dlinptr == hjSmallOut)
						{
							hjSmallSideStepsOut.push_back(childProjSteps[m]);
						}
					}
				}

				// Inform HashJoinStep of the output large and small side steps
				pHj->setLargeSideStepsOut(hjLargeSideStepsOut);
				pHj->setSmallSideStepsOut(hjSmallSideStepsOut);
			}
#if 0
			else if (pThj)
			{
				pThj->setLargeSideStepsOut(hjLargeSideStepsOut);
				pThj->setSmallSideStepsOut(hjSmallSideStepsOut);
			}
#endif

			// Flag the BP that reads the large-side of this hashjoin
			// (we only handle the 1 consumer case, which should be good enough)
			if ((hjLargeSideStepsOut.size() == 1) &&
					(typeid(*hjLargeSideStepsOut[0]) == typeid(BatchPrimitiveStep)))
					//typeid(*hjLargeSideStepsOut[0]) == typeid(TupleBPS)))
			{
				bpSteps[hjLargeSideStepsOut[0]->stepId()].pInputLargeSideHj = pHj;
			}

		} // if a query step is a HashJoinStep
	}     // loop through query steps

	// For the case where a HJ-large-side feeds a BP, which then feeds a HJ-
	// small-side, we can instruct the first HJ not to worry about having to
	// cache the large-side output fifo to disk to avoid blocking.  Given the
	// pattern just described, any HJ-large-side blockage should be temporary.
	for (BPInfoMapIter bpIter=bpSteps.begin();
		bpIter!=bpSteps.end(); ++bpIter)
	{
		//cout << "BPStep: " << bpIter->first << "; FeedSmall-" <<
		//	bpIter->second.bBPFeedSmallSideHj << "; hjLargeInput-" <<
		//	(bpIter->second.pInputLargeSideHj ? "yes" : "no") << endl;

		if ((bpIter->second.bBPFeedSmallSideHj) &&
			(bpIter->second.pInputLargeSideHj))
		{
			//cout << "..Disabling large-side fifo to disk for HJ step: " <<
			//	bpIter->second.pInputLargeSideHj->stepId() << endl;
			bpIter->second.pInputLargeSideHj->setAllowLargeSideFifoToDisk(false);
		}
	}
}

void enableDiskIoLog (JobStepVector& querySteps)
{
	for (JobStepVector::iterator qsi = querySteps.begin(); qsi != querySteps.end(); qsi++)
	{
		if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
			continue;

		// enable disk i/o log for bucket dl and zdl
		JobStepAssociation jsa = (*qsi)->outputAssociation();
		for (uint64_t i = 0; i < jsa.outSize(); i++)
		{
			if (jsa.outAt(i)->bucketDL() != NULL)
				jsa.outAt(i)->bucketDL()->enableDiskIoTrace();
			else if (jsa.outAt(i)->zonedDL() != NULL)
				jsa.outAt(i)->zonedDL()->enableDiskIoTrace();
			else if (jsa.outAt(i)->stringBucketDL() != NULL)
				jsa.outAt(i)->stringBucketDL()->enableDiskIoTrace();
			else if (jsa.outAt(i)->stringZonedDL() != NULL)
				jsa.outAt(i)->stringZonedDL()->enableDiskIoTrace();
			else if (jsa.outAt(i)->tupleBucketDL() != NULL)
				jsa.outAt(i)->tupleBucketDL()->enableDiskIoTrace();
		}
	}
}

/** check the datalist input to passthrough step
    Change the elementmode to RID_VALUE for those datalist.
    We do a simlar check to disable the RIDonly flag for FIFO,
    which only applies to how HashJoin may save elements to temp disk.
*/
void checkDataList(JobStepVector& querySteps, JobStepVector& projectSteps)
{
	JobStepVector::const_iterator qsi = querySteps.begin();
	JobStepVector::const_iterator psi;

	for (; qsi != querySteps.end(); qsi++)
	{
		if (typeid(*(qsi->get())) == typeid(PassThruStep))
		{
			for (unsigned int i = 0; i < qsi->get()->inputAssociation().outSize(); i++)
			{
				// only fix ZDL now. bucketdl should already been covered
				ZonedDL *dlin = qsi->get()->inputAssociation().outAt(i)->zonedDL();
				if (dlin)
				{
					dlin->setElementMode(ZonedDL::RID_VALUE);
				}
				else
				{
					// FIFO normally passes RID/Value struct, but an HJ output
					// FIFO only saves RIDs to disk unless we turn off RIDonly.
					FifoDataList *dlin = qsi->get()->inputAssociation().outAt(i)->fifoDL();
					if (dlin)
					{
						dlin->setElementMode(FifoDataList::RID_VALUE);
					}
					else
					{
						StringZonedDL *dlin = qsi->get()->inputAssociation().outAt(i)->stringZonedDL();
						if (dlin)
							dlin->setElementMode(StringZonedDL::RID_VALUE);
					}
				}
			}
		}
	}

	for (psi = projectSteps.begin(); psi != projectSteps.end(); psi++)
	{
		if (typeid(*(psi->get())) == typeid(PassThruStep))
		{
			for (unsigned int i = 0; i < psi->get()->inputAssociation().outSize(); i++)
			{
				// only fix ZDL now. bucketdl should already been covered
				ZonedDL *dlin = psi->get()->inputAssociation().outAt(i)->zonedDL();
				if (dlin)
				{
					dlin->setElementMode(ZonedDL::RID_VALUE);
				}
				else
				{
					FifoDataList *dlin = psi->get()->inputAssociation().outAt(i)->fifoDL();
					if (dlin)
					{
						dlin->setElementMode(FifoDataList::RID_VALUE);
					}
					else
					{
						StringZonedDL *dlin = psi->get()->inputAssociation().outAt(i)->stringZonedDL();
						if (dlin)
							dlin->setElementMode(StringZonedDL::RID_VALUE);
					}
				}
			}
		}
	}
}

/** push dictionary filters to past all the non dictionary filters of the same table */
void checkDictFilters(JobStepVector& querySteps,JobStepVector& projectSteps,const JobInfo& jobInfo)
{
	JobStepVector::iterator qsi = querySteps.begin();
	JobStepVector::iterator end = querySteps.end();
	pColScanStep* firstDictPcs = 0;
	pColStep* firstDictPcst = 0;
	JobStepVector::iterator firstDictPcIter;
	bool contflag = false;

	for (; qsi != end; ++qsi)
	{
		// identify dictionary filter perfomed by hashjoin (DFH)
		if (distance(qsi, end) > 2 &&
			typeid(*(qsi->get())) == typeid(pDictionaryScan) &&
			(typeid(*((qsi + 1)->get())) == typeid(pColStep) ||
			 typeid(*((qsi + 1)->get())) == typeid(pColScanStep)) &&
			typeid(*((qsi + 2)->get())) == typeid(HashJoinStep))
		{
			// this is a DFH
			AnyDataListSPtr spdl;
			HashJoinStep* hj = dynamic_cast<HashJoinStep*>((qsi + 2)->get());
			pDictionaryScan* pds = dynamic_cast<pDictionaryScan*>(qsi->get());
			bool breakflag = false;
			uint dlIndex = 0;

			// get output association of hj. rule out the non used datalist output
			for (uint n = 0; n < hj->outputAssociation().outSize(); n++)
			{
				// ignore the unused branch of this hashjoin
				if (hj->outputAssociation().outAt(n)->dataList()->OID() ==
					pds->outputAssociation().outAt(0)->dataList()->OID())
					continue;
				if (hj->outputAssociation().outAt(n)->getNumConsumers() > 1)
				{
					// only handle one consumer case
					breakflag = true;
					break;
				}
				// this will be the input of the jobsteps after the non-dict filter.
				// save n to restore the datalist required by next step.
				dlIndex = n;
				spdl = hj->outputAssociation().outAt(n);
			}

			// done this run. go find next DFH.
			if (breakflag) continue;

			// if contflag is true, this is the consective DFH. otherwise, update firstDict.
			if (!contflag)
			{
				firstDictPcs = dynamic_cast<pColScanStep*> ((qsi + 1)->get());
				firstDictPcst = dynamic_cast<pColStep*> ((qsi + 1)->get());
				firstDictPcIter = qsi + 1;
			}

			DataList_t* dlout = spdl->dataList();
			ptrdiff_t dloutptr = 0;
			if (dlout)
				dloutptr = (ptrdiff_t)dlout;
			else
				continue;
			for (unsigned int k = 0; k < querySteps.size(); k++)
			{
				if (typeid(*(querySteps[k])) == typeid(pColStep))
				{
					// will only have one input
					ptrdiff_t dlinptr = 0;
					DataList_t *dlin = querySteps[k]->inputAssociation().outAt(0)->dataList();
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
						continue;
					if (dloutptr == dlinptr)
					{
						pColStep *pcs = dynamic_cast<pColStep*>(querySteps[k].get());

						// we should safely break here. because we only handle one consumer
						// for hash join output case
						if (pcs->isDictCol())
						{
							// consequous dict filter, could be swaped together
							contflag = true;
							break;
						}
						else if (pcs->filterCount() == 0)
							break;

						// will do swap here.
						// 1. find the next jobsteps after the non-dict pcolstep
						// and associate them with hashjoin
						contflag = false;
						for (unsigned int i = 0; i < pcs->outputAssociation().outSize(); i++)
						{
							ptrdiff_t dloutptr = 0;
							DataList_t* dlout = pcs->outputAssociation().outAt(i)->dataList();

							if (dlout)
							{
								dloutptr = (ptrdiff_t)dlout;
							}
							else
							{
								StrDataList* sdl = qsi->get()->outputAssociation().outAt(i)->stringDataList();
								dloutptr = (ptrdiff_t)sdl;
							}

							for (unsigned int j = 0; j < querySteps.size(); j++)
							{
								JobStepAssociation queryInJsa = querySteps[j]->inputAssociation();
								for (unsigned int m = 0; m < queryInJsa.outSize(); m++)
								{
									ptrdiff_t dlinptr = 0;
									DataList_t *dlin = queryInJsa.outAt(m)->dataList();
									StrDataList* sdl = 0;
									if (dlin)
										dlinptr = (ptrdiff_t)dlin;
									else
									{
										sdl = queryInJsa.outAt(m)->stringDataList();
										dlinptr = (ptrdiff_t)sdl;
									}

									if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
									{
										JobStepAssociation jsa = hj->outputAssociation();
										jsa.outAt(dlIndex) = queryInJsa.outAt(m);
										hj->outputAssociation(jsa);
									}
								}
							}

							for (unsigned int j = 0; j < projectSteps.size(); j++)
							{
								JobStepAssociation projInJsa = projectSteps[j]->inputAssociation();
								for (unsigned int m = 0; m < projInJsa.outSize(); m++)
								{
									ptrdiff_t dlinptr = 0;
									DataList_t *dlin = projInJsa.outAt(m)->dataList();
									StrDataList* sdl = 0;
									if (dlin)
										dlinptr = (ptrdiff_t)dlin;
									else
									{
										sdl = projInJsa.outAt(m)->stringDataList();
										dlinptr = (ptrdiff_t)sdl;
									}

									if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
									{
										JobStepAssociation jsa = hj->outputAssociation();
										jsa.outAt(dlIndex) = projInJsa.outAt(m);
										hj->outputAssociation(jsa);
									}
								}
							}
						}

						// 2. push this non-dict filter to be before the dict filter at last
						AnyDataListSPtr spdl1(new AnyDataList());
						FifoDataList* dl = new FifoDataList(1, jobInfo.fifoSize);
						spdl1->fifoDL(dl);
						JobStepAssociation outJsa(jobInfo.status);
						outJsa.outAdd(spdl1);
						pcs->outputAssociation(outJsa);
						if (firstDictPcs)
						{
							// pcolscan -- empty jobstep association as input
							pcs->inputAssociation(JobStepAssociation(jobInfo.status));
							firstDictPcs->inputAssociation(pcs->outputAssociation());
							pColScanStep* scanStep = new pColScanStep(*pcs);
							if (scanStep)
								querySteps[k].reset(scanStep);
							pColStep* colStep = new pColStep(*firstDictPcs);
							if (pcs)
								firstDictPcIter->reset(colStep);
							firstDictPcs = 0;
							firstDictPcst = colStep;
						}
						else
						{
							pcs->inputAssociation(firstDictPcst->inputAssociation());
							firstDictPcst->inputAssociation(pcs->outputAssociation());
						}
					}
				}
			}
		}
	}
}

/** @bug 1154. recheck to correct the jobstep cardinality. scan step card should <= previous step */
/* Note: This function may not correctly determine StringHashJoin cardinality */
/*       at this time. but that's okay because the cardinality is not used by */
/*       StringHashJoinStep.                                                  */
void checkCardinality(JobStepVector& querySteps)
{
	JobStepVector::iterator qsi = querySteps.begin();
	for (; qsi != querySteps.end(); ++qsi)
	{
		// find the child filter jobsteps. We don't care about projection
		JobStepVector childVec;
		getChildJobSteps(*qsi, querySteps, childVec);

		// identify the hashjoin dimond case (hashjoin output to another
		// hashjoin between the same tables). make the two pcolstep inputs to
		// the 2nd hashjoin have the same card as the first hashjoin inputs
		if (typeid(*(qsi->get())) == typeid(HashJoinStep) &&
			 childVec.size() == 2)
		{
			JobStepVector childVec1, childVec2;
			HashJoinStep *hj = dynamic_cast<HashJoinStep*>(qsi->get());
			map<execplan::CalpontSystemCatalog::OID, uint64_t>& tbCardMap = hj->tbCardMap();
			pColStep* pcs1 = dynamic_cast<pColStep*> (childVec[0].get());
			pColStep* pcs2 = dynamic_cast<pColStep*> (childVec[1].get());
			if (pcs1 && pcs2)
			{
				getChildJobSteps(childVec[0], querySteps, childVec1);
				getChildJobSteps(childVec[1], querySteps, childVec2);
				if (childVec1.size() == 1 && childVec2.size() == 1 &&
					childVec1[0]->stepId() == childVec2[0]->stepId())
				{
					pcs1->cardinality(tbCardMap[pcs1->tableOid()]);
					//cout << "pcolstep " << pcs1->stepId() << " set card:" << tbCardMap[pcs1->tableOid()] << endl;
					pcs2->cardinality(tbCardMap[pcs2->tableOid()]);
					//cout << "pcolstep " << pcs2->stepId() << " set card:" << tbCardMap[pcs2->tableOid()] << endl;
					continue;
				}
			}
		}

		// force pdictionaryscan cardinality to be 0
		else if (typeid(*(qsi->get())) == typeid(pDictionaryScan))
			(*qsi)->cardinality(0);

		// for every children, if it's card >= the current one, replace it with the
		// current card. the exception is union step. HJ step card is already made
		// sure to be <= the larger input card while generating the job steps.
		for (uint i = 0; i < childVec.size(); i++)
		{
			if (typeid(*(childVec[i].get())) == typeid(UnionStep))
				continue;
			if (typeid(*(childVec[i].get())) == typeid(HashJoinStep))
			{
				// set tbCardMap in hashjoin
				HashJoinStep *hj = dynamic_cast<HashJoinStep*>(childVec[i].get());
				map<execplan::CalpontSystemCatalog::OID, uint64_t>& tbCardMap = hj->tbCardMap();
				tbCardMap[(*qsi)->tableOid()] = (*qsi)->cardinality();
				continue;
			}
			if ((*qsi)->cardinality() != 0 && childVec[i]->cardinality() > (*qsi)->cardinality())
				childVec[i]->cardinality((*qsi)->cardinality());
		}
	}
}

//------------------------------------------------------------------------------
// The purpose of this method is to determine the RID and data sizes necessary
// for saving a datalist to disk.   These sizes are then stored with the
// datalists, making it possible for each datalist to know whether to compress
// the element RIDS and/or values to 4 bytes when they are paged to disk.
//
// The compression of the data values to 4 bytes, does not apply to
// StringElementType, as we are not compressing strings.
//
// This method loops through all the query steps.
// o For the Primitive step types, the RID and data sizes are based on the
//   HWM and the column width of the column file respectively.
// o For the FilterStep, HashJoin, and StringHashJoin steps, the RID and data
//   size from the input associations are copied to the output associations.
//
// There is no need to do anything for the project steps, as no project
// related steps should be sending data to a datalist that pages to disk.
//------------------------------------------------------------------------------
//@bug 743 - Add support for 32 bit RID and/or values
void datalistSaveSize(
	JobStepVector& querySteps,
	const JobInfo& jobInfo)
{
	const uint32_t TOKEN_SIZE = 8; // size of token for dictionary columns
	BRM::DBRM dbrm;

	JobStepVector::const_iterator qsi = querySteps.begin();
	JobStepVector::const_iterator psi;

	for (; qsi != querySteps.end(); qsi++)
	{
		const std::type_info& stepType = typeid(*(qsi->get()));

		//...pColScanStep or pColStep
		//   1. Determine size for saving data based on the col width.(size1st)
		//   2. Determine size for saving RID based on the HWM.       (size2nd)
		//   3. Assign the save sizes to output assoc[0].

		if ((stepType == typeid(pColScanStep)) ||
			(stepType == typeid(pColStep)))
		{
			uint32_t size1st = 8; // size of RIDs   to save to disk
			uint32_t size2nd = 8; // size of values to save to disk
			execplan::CalpontSystemCatalog::OID colOid = qsi->get()->oid();

			//...Determine values for size1st (RID) and size2nd (data).
			CalpontSystemCatalog::ColType ct = jobInfo.csc->colType(colOid);
			if (ct.colWidth <= 4)
				size2nd = 4;

			//...For dictionary col we use TOKEN_SIZE as col width (rather
			//   than ct.colWidth) in calculating num of rows based on HWM.
			int colOidWidth = ct.colWidth;
			if (isDictCol(ct) > 0)
				colOidWidth = TOKEN_SIZE;
			size1st = getRidSizeBasedOnHwm(dbrm, colOid, colOidWidth);

			//...Set element save size in output association[0]
			qsi->get()->outputAssociation().outAt(0)->dataList()->
				setDiskElemSize(size1st, size2nd);
		}

		//...pDictionaryStep or pDictionaryScan
		//   1. We base the RID size on the HWM of the column OID, rather
		//	  than the dictionary OID.  In the case of pDictionaryScan
		//	  we may never need or care about the signature RIDs, but we
		//	  are technically sending them to the HashJoinStep, so we go
		//      ahead and set the RID size.
		//   2. For pDictionaryScan, the save size for the data is the token
		//      length.  For pDictionaryStep, the save size does not matter,
		//      as we are dealing with strings, which we are not compressing.
		//      We can specify a save size of '8' to accomodate both cases.
		//   3. Assign the save sizes to output assoc[0].

		else if ((stepType == typeid(pDictionaryStep)) ||
				 (stepType == typeid(pDictionaryScan)))
		{
			uint32_t size1st = 8;         // size of RIDs to save to disk
			uint32_t size2nd = TOKEN_SIZE;// pDictionaryScan-token width
                                          // pDictionaryStep-string length (n/a)

			//...Get the column OID that corresponds to this step's dict OID
			DictOidToColOidMap::const_iterator mapIter =
				jobInfo.keyInfo->dictOidToColOid.find (qsi->get()->oid());
			if (mapIter == jobInfo.keyInfo->dictOidToColOid.end())
			{
				std::ostringstream errmsg;
				errmsg << "Step " << qsi->get()->stepId() <<
					" Could not find column OID for dictionary OID " <<
					qsi->get()->oid();
				throw std::logic_error(errmsg.str());
			}
			execplan::CalpontSystemCatalog::OID colOid = mapIter->second;

			//...Determine value for size1st (size of RID)
			size1st = getRidSizeBasedOnHwm(dbrm, colOid, TOKEN_SIZE);

			//...Set element save size in output association[0].
			//   DataList_t  is for pDictionaryScan
			//   StrDataList is for pDictionaryStep
			DataList_t* pOutDL =
				qsi->get()->outputAssociation().outAt(0)->dataList();
			if (pOutDL)
			{
				pOutDL->setDiskElemSize(size1st, size2nd);
			}
			else
			{
				StrDataList* pStrOutDL =
					qsi->get()->outputAssociation().outAt(0)->stringDataList();
				if (pStrOutDL)
					pStrOutDL->setDiskElemSize(size1st, size2nd);
			}
		}

		//...FilterStep
		//   1. As I understand the FilterStep, input assoc[0] contains the
		//      master list, and input assoc[1] contains a subset filter list.
		//      So we can just copy the RID and column widths from input
		//      assoc[0] to output assoc[0].

		else if (stepType == typeid(FilterStep))
		{
			DataList_t* pInDL =
				qsi->get()->inputAssociation().outAt(0)->dataList();
			if (pInDL)
			{
				qsi->get()->outputAssociation().outAt(0)->dataList()->
					setDiskElemSize(
						pInDL->getDiskElemSize1st(),
						pInDL->getDiskElemSize2nd());
			}
			else
			{
				StrDataList* pStrInDL =
					qsi->get()->inputAssociation().outAt(0)->stringDataList();
				qsi->get()->outputAssociation().outAt(0)->stringDataList()->
					setDiskElemSize(
						pStrInDL->getDiskElemSize1st(),
						pStrInDL->getDiskElemSize2nd());
			}
		}

		//...HashJoinStep
		//   1. Copy RID and column width from input assoc[0] to output assoc[0]
		//   2. Copy RID and column width from input assoc[1] to output assoc[1]

		else if (stepType == typeid(HashJoinStep))
		{
			for (unsigned i=0;
				 i<qsi->get()->inputAssociation().outSize();
				 i++)
			{
				DataList_t* pInDL =
					qsi->get()->inputAssociation().outAt(i)->dataList();
				qsi->get()->outputAssociation().outAt(i)->dataList()->
					setDiskElemSize(
						pInDL->getDiskElemSize1st(),
						pInDL->getDiskElemSize2nd());
			}
		}

		//...StringHashJoinStep
		//   1. Copy RID width from input assoc[0] to output assoc[0]
		//   2. Copy RID width from input assoc[1] to output assoc[1]
		//   3. Our input is 2 string datalists, but our output is a data-
		//      list of string tokens.  So output col width is TOKEN_SIZE.

		else if (stepType == typeid(StringHashJoinStep))
		{
			const uint32_t size2nd = TOKEN_SIZE;

			for (unsigned i=0;
				 i<qsi->get()->inputAssociation().outSize();
				 i++)
			{
				StrDataList* pStrInDL =
					qsi->get()->inputAssociation().outAt(i)->stringDataList();
				qsi->get()->outputAssociation().outAt(i)->dataList()->
					setDiskElemSize(pStrInDL->getDiskElemSize1st(), size2nd);
			}
		}

		//...BucketReuseStep
		//   the size info is already calculated by pColScanStep/pDictionStep,
		//   setDiskElemSize() will get that data from RestoreInfo

		else if (stepType == typeid(BucketReuseStep))
		{
			BucketDataList* dl = dynamic_cast<BucketDataList*>
					(qsi->get()->outputAssociation().outAt(0)->bucketDL());
			if (dl != NULL)
			{
				dl->setDiskElemSize(dl->reuseControl()->dataSize().first,
									dl->reuseControl()->dataSize().second);
			}
			else
			{
				StringBucketDataList* dl = dynamic_cast<StringBucketDataList*>
				   (qsi->get()->outputAssociation().outAt(0)->stringBucketDL());
				dl->setDiskElemSize(dl->reuseControl()->dataSize().first,
									dl->reuseControl()->dataSize().second);
			}
		}

		// No action necessary for the remaining jobstep types.
		// May need to revisit primitive steps such as pIdxList
		// and/or pIdxWalk at a later time...
		else
		{
			// no action necessary for the remaining step types
		}
	}
}

void checkProjectStepsInput(JobStepVector& projectSteps, JobStepVector& querySteps,
	unsigned numProjectSteps, map<uint, SJSTEP>& tableStepMap, JobInfo& jobInfo)
{
//Go through all the project steps, excluding dictionary steps, and merge inputs for the same table from different
//aliases with a union step.

//The input datalists to the union step should be a zdl datalist, so the datalists for the step that is input to
//the project step must be changed to a zdl datalist.  The input step to a project step is always the last
//query step for a table, and that has been saved in the input tableStepMap.

	typedef multimap<CalpontSystemCatalog::OID, SJSTEP> StepMap_t;
	StepMap_t projectMap;

	JobStepVector::iterator end = projectSteps.end();
	for  (JobStepVector::iterator iter = projectSteps.begin(); iter != end; ++iter)
	{
		//Only count these
		if (typeid(*iter->get()) == typeid(pColStep))
		{
		  projectMap.insert(make_pair(iter->get()->tableOid(), *iter));
		}
	}

	typedef pair<AnyDataListSPtr, uint> DLAlisasPair_t;
	typedef map<AnyDataListSPtr, uint> DLSet_t;

	StepMap_t::iterator pmStart = projectMap.begin();
	StepMap_t::iterator pmEnd = projectMap.end();

	while (pmStart != pmEnd)
	{
		//look for self-joins on a table.  They will have different input datalists
		CalpontSystemCatalog::OID tableOid = pmStart->first;
		StepMap_t::iterator last = projectMap.upper_bound(tableOid);
		DLSet_t dataSources;
		for (StepMap_t::iterator it1 = pmStart; it1 != last; ++it1)
		{
			dataSources.insert(make_pair(it1->second.get()->inputAssociation().outAt(0),
				tableKey(jobInfo,tableOid,it1->second.get()->alias(),it1->second.get()->view())));
		}
		if (1 < dataSources.size() && !jobInfo.tryTuples)
		{
			DLSet_t::iterator ds = dataSources.begin();
			DLSet_t::iterator dsend = dataSources.end();
			string alias(jobInfo.keyInfo->tupleKeyVec[ds->second].fAlias);
			string view(jobInfo.keyInfo->tupleKeyVec[ds->second].fView);

			AnyDataListSPtr lastds(new AnyDataList());
			uint lastStep = ds->second;
			JobStepAssociation firstJSA(jobInfo.status);

			//change the output datalist from the step feeding the project step from a fifo to a zdl
			replaceDataList(tableStepMap[lastStep].get(), firstJSA, lastds, tableOid, alias, view, jobInfo);

			//chain all datalists for the table oid
			for (++ds; ds != dsend; ++ds)
			{
				AnyDataListSPtr spzdl(new AnyDataList());
				JobStepAssociation newJSA(jobInfo.status);
				string a = jobInfo.keyInfo->tupleKeyVec[ds->second].fAlias;
				string v = jobInfo.keyInfo->tupleKeyVec[ds->second].fView;

				lastStep = tableKey(jobInfo, tableOid, a, v);

				replaceDataList(tableStepMap[lastStep].get(),newJSA,spzdl,tableOid,a,v,jobInfo);

				JobStepAssociation inJsa(jobInfo.status);
				inJsa.outAdd(lastds);
				inJsa.outAdd(spzdl);

				AnyDataListSPtr spdl(new AnyDataList());
FIFODEBUG();
				FifoDataList* dl = new FifoDataList(1, jobInfo.fifoSize);
				dl->inOrder(true);
				spdl->fifoDL(dl);

				JobStepAssociation outJsa(jobInfo.status);
				outJsa.outAdd(spdl);

				UnionStep* us = new UnionStep(inJsa, outJsa, tableOid,
					jobInfo.sessionId,
					jobInfo.txnId,
					jobInfo.verId,
					0, // stepId
					jobInfo.statementId);
				SJSTEP step;
				step.reset(us);
				querySteps.push_back(step);
				lastds = spdl;
			}
 			//Change the input job step associations for this table to the last union step output,
			// removing duplicate columns at the same time
			set<CalpontSystemCatalog::OID> columns;
			for (StepMap_t::iterator it2 = pmStart; it2 != last; ++it2)
			{
				if (columns.end() == columns.find(it2->second.get()->oid()))
				{
					columns.insert(it2->second.get()->oid());
					it2->second.get()->inputAssociation(querySteps.back().get()->outputAssociation());
				}
				else
				{	//the duplicate column may have a dictionary step
				  	JobStepVector::iterator psi = find(projectSteps.begin(), projectSteps.end(), it2->second);

					if (1 < distance(psi, projectSteps.end()) && typeid(*((psi + 1)->get())) == typeid(pDictionaryStep))
						projectSteps.erase(psi, psi+2);
					else
						projectSteps.erase(psi);
				}
			}
		}
		pmStart = last;
	}
}

void projectSimpleColumn(const SimpleColumn* sc, JobStepVector& jsv, JobInfo& jobInfo)
{
	if (sc == NULL)
		throw logic_error("projectSimpleColumn: sc is null");

	CalpontSystemCatalog::OID oid = sc->oid();
	CalpontSystemCatalog::OID tbl_oid = tableOid(sc, jobInfo.csc);
	string alias(extractTableAlias(sc));
	string view(sc->viewName());
	CalpontSystemCatalog::OID dictOid = 0;
	CalpontSystemCatalog::ColType ct;
	pColStep* pcs = NULL;
	pDictionaryStep* pds = NULL;
	bool tokenOnly = false;
	TupleInfo ti;

	if (!sc->schemaName().empty())
	{
		SJSTEP sjstep;

		if (!jobInfo.tryTuples)
			jobInfo.tables.insert(make_table(sc->schemaName(), sc->tableName()));

//		if (jobInfo.trace)
//			cout << "doProject Emit pCol for SimpleColumn " << oid << endl;

		pcs = new pColStep(JobStepAssociation(jobInfo.status),
						 JobStepAssociation(jobInfo.status),
						 0,
						 jobInfo.csc,
						 oid,
						 tbl_oid,
						 jobInfo.sessionId,
						 jobInfo.txnId,
						 jobInfo.verId,
						 0,
						 jobInfo.statementId,
						 jobInfo.rm,
						 jobInfo.flushInterval,
						 jobInfo.isExeMgr);
		pcs->logger(jobInfo.logger);
		pcs->alias(alias);
		pcs->view(view);
		pcs->name(sc->columnName());
		pcs->cardinality(sc->cardinality());
		//pcs->setOrderRids(true);

		sjstep.reset(pcs);
		jsv.push_back(sjstep);

		ct = jobInfo.csc->colType(oid);
		dictOid = isDictCol(ct);
		if (jobInfo.tryTuples)
		{
			ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
			pcs->tupleId(ti.key);

			if (dictOid > 0 && jobInfo.hasAggregation)
			{
				map<uint, bool>::iterator it =
					jobInfo.tokenOnly.find(getTupleKey(jobInfo, sc));
				if (jobInfo.tryTuples && it != jobInfo.tokenOnly.end())
					tokenOnly = it->second;
			}
		}

		if (dictOid > 0 && !tokenOnly)
		{
			//This is a double-step step
//			if (jobInfo.trace)
//				cout << "doProject Emit pGetSignature for SimpleColumn " << dictOid << endl;

			pds = new pDictionaryStep(JobStepAssociation(jobInfo.status),
							JobStepAssociation(jobInfo.status),
							0,
							jobInfo.csc,
							dictOid,
							ct.ddn.compressionType,
							tbl_oid,
							jobInfo.sessionId,
							jobInfo.txnId,
							jobInfo.verId,
							0,
							jobInfo.statementId,
							jobInfo.rm,
							jobInfo.flushInterval);
			pds->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid] = oid;
			pds->alias(alias);
			pds->view(view);
			pds->name(sc->columnName());
			pds->cardinality(sc->cardinality());
			//pds->setOrderRids(true);

			//Associate these two linked steps
			JobStepAssociation outJs(jobInfo.status);
			AnyDataListSPtr spdl1(new AnyDataList());
			if (jobInfo.tryTuples)
			{
				RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
				spdl1->rowGroupDL(dl1);
				dl1->OID(oid);

				// not a tokenOnly column
				setTupleInfo(ct, dictOid, jobInfo, tbl_oid, sc, alias);
				jobInfo.tokenOnly[getTupleKey(jobInfo, oid, alias, view)] = false;
			}
			else
			{
				FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
				spdl1->fifoDL(dl1);
				dl1->OID(oid);
			}
			outJs.outAdd(spdl1);

			pcs->outputAssociation(outJs);
			pds->inputAssociation(outJs);

			sjstep.reset(pds);
			jsv.push_back(sjstep);

			if (jobInfo.tryTuples)
			{
				oid = dictOid; // dictionary column
				ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
				pds->tupleId(ti.key);
				jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
			}
		}
	}
	else // must be vtable mode
	{
		oid = (tbl_oid+1) + sc->colPosition();
		ct = jobInfo.vtableColTypes[UniqId(oid, sc->tableAlias(), sc->viewName())];
		ti = setTupleInfo(ct, oid, jobInfo, tbl_oid, sc, alias);
	}

	if (jobInfo.tryTuples)
	{
		if (dictOid > 0 && tokenOnly)
		{
			// scale is not used by string columns
			// borrow it to indicate token is used in projection, not the real string.
			ti.scale = 8;
		}

		jobInfo.pjColList.push_back(ti);
	}
}

const JobStepVector doProject(const RetColsVector& retCols, JobInfo& jobInfo)
{
	JobStepVector jsv;
	SJSTEP sjstep;

	for (unsigned i = 0; i < retCols.size(); i++)
	{
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(retCols[i].get());
		if (sc != NULL)
		{
			projectSimpleColumn(sc, jsv, jobInfo);
		}
		else if (jobInfo.tryTuples) // v-table mode handles ArithmeticColumn and FunctionColumn
		{
			const ArithmeticColumn* ac = NULL;
			const FunctionColumn* fc = NULL;
			uint64_t eid = -1;
			CalpontSystemCatalog::ColType ct;
			ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
													jobInfo.txnId,
													jobInfo.verId,
													jobInfo.statementId);
			es->logger(jobInfo.logger);
			es->expression(retCols[i], jobInfo);
			sjstep.reset(es);

			if ((ac = dynamic_cast<const ArithmeticColumn*>(retCols[i].get())) != NULL)
			{
				eid = ac->expressionId();
				ct = ac->resultType();
			}
			else if ((fc = dynamic_cast<const FunctionColumn*>(retCols[i].get())) != NULL)
			{
				eid = fc->expressionId();
				ct = fc->resultType();
			}
			else
			{
				std::ostringstream errmsg;
				errmsg << "doProject: unhandled returned column: " << typeid(*retCols[i]).name();
				cerr << boldStart << errmsg.str() << boldStop << endl;
				throw logic_error(errmsg.str());
			}

			// set expression tuple Info
			TupleInfo ti(setExpTupleInfo(ct, eid, retCols[i].get()->alias(), jobInfo));
			uint key = ti.key;

			if (find(jobInfo.expressionVec.begin(), jobInfo.expressionVec.end(), key) ==
				jobInfo.expressionVec.end())
				jobInfo.returnedExpressions.push_back(sjstep);

			//put place hold column in projection list
			jobInfo.pjColList.push_back(ti);
		}
		else
		{
			std::ostringstream errmsg;
			errmsg << "doProject: unhandled returned column: " << typeid(*retCols[i]).name();
			cerr << boldStart << errmsg.str() << boldStop << endl;
			throw logic_error(errmsg.str());
		}
	}

	return jsv;
}

void checkHavingClause(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	TupleHavingStep* ths = new  TupleHavingStep(jobInfo.sessionId,
												jobInfo.txnId,
												jobInfo.verId,
												jobInfo.statementId);
	ths->logger(jobInfo.logger);
	ths->expressionFilter(csep->having(), jobInfo);
	jobInfo.havingStep.reset(ths);

	// simple columns in select clause
	set<UniqId> scInSelect;
	for (RetColsVector::iterator		i  = jobInfo.nonConstCols.begin();
										i != jobInfo.nonConstCols.end();
										i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
				sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

			scInSelect.insert(UniqId(sc->oid(), extractTableAlias(sc), sc->viewName()));
		}
	}

	// simple columns in gruop by clause
	set<UniqId> scInGroupBy;
	for (RetColsVector::iterator  i  = csep->groupByCols().begin();
										i != csep->groupByCols().end();
										i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty() && sc->oid() == 0)
			{
				if (sc->colPosition() == (uint64_t) -1)
				{
					// from select subquery
					SRCP ss = csep->returnedCols()[sc->orderPos()];
					(*i) = ss;
				}
				else
				{
					sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
				}
			}

			scInGroupBy.insert(UniqId(sc->oid(), extractTableAlias(sc), sc->viewName()));
		}
	}

	bool  aggInHaving = false;
	const vector<ReturnedColumn*>& columns = ths->columns();
	for (vector<ReturnedColumn*>::const_iterator i = columns.begin(); i != columns.end(); i++)
	{
		// evaluate aggregate columns in having
		AggregateColumn* agc = dynamic_cast<AggregateColumn*>(*i);
		if (agc)
		{
			addAggregateColumn(agc, -1, jobInfo.nonConstCols, jobInfo);
			aggInHaving = true;
		}
		else
		{
			// simple columns used in having and in group by clause must be in rowgroup
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(*i);
			if (sc != NULL)
			{
				if (sc->schemaName().empty())
					sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

				UniqId scId(sc->oid(), extractTableAlias(sc), sc->viewName());
				if (scInGroupBy.find(scId) != scInGroupBy.end() &&
					scInSelect.find(scId)  == scInSelect.end())
				{
					jobInfo.nonConstCols.push_back(SRCP(sc->clone()));
				}
			}
		}
	}

	if (aggInHaving == false)
	{
		// treated the same as where clause if no aggregate column in having.
		jobInfo.havingStep.reset();

		// parse the having expression
		const ParseTree* filters = csep->having();
		if (filters != 0)
		{
			filters->walk(JLF_ExecPlanToJobList::walkTree, &jobInfo);
		}

		if (!jobInfo.stack.empty())
		{
			assert(jobInfo.stack.size() == 1);
			jobInfo.havingStepVec = jobInfo.stack.top();
			jobInfo.stack.pop();
		}
	}
}

void preProcessFunctionOnAggregation(const vector<SimpleColumn*>& scs,
									 const vector<AggregateColumn*>& aggs,
									 JobInfo& jobInfo)
{
	// append the simple columns if not already projected
	set<UniqId> scProjected;
	for (RetColsVector::iterator i  = jobInfo.projectionCols.begin();
								 i != jobInfo.projectionCols.end();
								 i++)
	{
		SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
				sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

			scProjected.insert(UniqId(sc->oid(), extractTableAlias(sc), sc->viewName()));
		}
	}

	for (vector<SimpleColumn*>::const_iterator i = scs.begin(); i != scs.end(); i++)
	{
		if (scProjected.find(UniqId((*i)->oid(), extractTableAlias(*i), (*i)->viewName()))
			== scProjected.end())
		{
			jobInfo.projectionCols.push_back(SRCP((*i)->clone()));
			scProjected.insert(UniqId((*i)->oid(), extractTableAlias(*i), (*i)->viewName()));
		}
	}

	// append the aggregate columns in arithmetic/function cloulmn to the projection list
	for (vector<AggregateColumn*>::const_iterator i = aggs.begin(); i != aggs.end(); i++)
	{
		addAggregateColumn(*i, -1, jobInfo.projectionCols, jobInfo);
	}
}

void checkReturnedColumns(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	for (uint64_t i = 0; i < jobInfo.deliveredCols.size(); i++)
	{
		if (NULL == dynamic_cast<const ConstantColumn*>(jobInfo.deliveredCols[i].get()))
			jobInfo.nonConstCols.push_back(jobInfo.deliveredCols[i]);
	}

	// save the original delivered non constant columns
	jobInfo.nonConstDelCols = jobInfo.nonConstCols;

	if (jobInfo.nonConstCols.size() != jobInfo.deliveredCols.size())
	{
		jobInfo.constantCol = CONST_COL_EXIST;

		// bug 2531, all constant column.
		if (jobInfo.nonConstCols.size() == 0)
		{
			if (csep->columnMap().size() > 0)
				jobInfo.nonConstCols.push_back((*(csep->columnMap().begin())).second);
			else
				jobInfo.constantCol = CONST_COL_ONLY;
		}
	}

	for (uint64_t i = 0; i < jobInfo.nonConstCols.size(); i++)
	{
		AggregateColumn* agc = dynamic_cast<AggregateColumn*>(jobInfo.nonConstCols[i].get());
		if (agc)
			addAggregateColumn(agc, i, jobInfo.nonConstCols, jobInfo);
	}

	if (csep->having() != NULL)
		checkHavingClause(csep, jobInfo);

	jobInfo.projectionCols = jobInfo.nonConstCols;
	for (uint64_t i = 0; i < jobInfo.nonConstCols.size(); i++)
	{
		const ArithmeticColumn* ac =
					dynamic_cast<const ArithmeticColumn*>(jobInfo.nonConstCols[i].get());
		const FunctionColumn* fc =
					dynamic_cast<const FunctionColumn*>(jobInfo.nonConstCols[i].get());
		if (ac != NULL && ac->aggColumnList().size() > 0)
		{
			jobInfo.nonConstCols[i]->outputIndex(i);
			preProcessFunctionOnAggregation(ac->simpleColumnList(), ac->aggColumnList(), jobInfo);
		}
		else if (fc != NULL && fc->aggColumnList().size() > 0)
		{
			jobInfo.nonConstCols[i]->outputIndex(i);
			preProcessFunctionOnAggregation(fc->simpleColumnList(), fc->aggColumnList(), jobInfo);
		}
	}
}

/*
This function is to get a unique non-constant column list for grouping.
After sub-query is supported, GROUP BY column can be a column from SELECT or FROM sub-queries,
which has empty schema name, and 0 oid (if SELECT).  In order to distinguish these columns,
data member fSequence is used to indicate the column position in FROM sub-query's select list,
the table OID for sub-query vtable is assumed to CNX_VTABLE_ID, the column OIDs for that vtable
is caculated based on this table OID and column position.
The data member fOrderPos is used to indicate the column position in the outer select clause,
this value is set to -1 if the column is not selected (implicit group by). For select sub-query,
the fSequence is not set, so orderPos is used to locate the column.
*/
void checkGroupByCols(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	// order by columns may be not in the select and [group by] clause
	const CalpontSelectExecutionPlan::OrderByColumnList& orderByCols = csep->orderByCols();
	for (uint64_t i = 0; i < orderByCols.size(); i++)
	{
		if (orderByCols[i]->orderPos() == (uint64_t)(-1))
		{
			jobInfo.deliveredCols.push_back(orderByCols[i]);

			// @bug 3025
			// Append the non-aggregate orderby column to group by, if there is group by clause.
			// Duplicates will be removed by next if block.
			if (csep->groupByCols().size() > 0)
			{
				// Not an aggregate column and not an expression of aggregation.
				if (dynamic_cast<AggregateColumn*>(orderByCols[i].get()) == NULL &&
					orderByCols[i]->aggColumnList().empty())
					csep->groupByCols().push_back(orderByCols[i]);
			}
		}
	}

	if (csep->groupByCols().size() > 0)
	{
		set<UniqId> colInGroupBy;
		RetColsVector uniqGbCols;
		for (RetColsVector::iterator  i  = csep->groupByCols().begin();
											i != csep->groupByCols().end();
											i++)
		{
			// skip constant columns
			if (dynamic_cast<ConstantColumn*>(i->get()) != NULL)
				continue;

			if ((*i)->orderPos() == (uint64_t) -1)
				jobInfo.hasImplicitGroupBy = true;

			ReturnedColumn *rc = i->get();
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(rc);

			bool selectSubquery = false;
			if (sc && sc->schemaName().empty() && sc->oid() == 0)
			{
				if (sc->colPosition() == (uint64_t) -1)
				{
					// from select subquery
					// sc->orderPos() should NOT be -1 because it is a SELECT sub-query.
					SRCP ss = csep->returnedCols()[sc->orderPos()];
					(*i) = ss;
					selectSubquery = true;

					// At this point whatever sc pointed to is invalid
					// update the rc and sc
					rc = ss.get();
					sc = dynamic_cast<SimpleColumn*>(rc);
				}
				else
				{
					sc->oid(tableOid(sc, jobInfo.csc)+1+sc->colPosition());
				}
			}

			UniqId col;
			if (sc)
				col = UniqId(sc->oid(), extractTableAlias(sc), sc->viewName());
			else
				col = UniqId(rc->expressionId(), rc->alias(), "");

			if (colInGroupBy.find(col) == colInGroupBy.end() || selectSubquery)
			{
				colInGroupBy.insert(col);
				uniqGbCols.push_back(*i);
			}
		}

		if (csep->groupByCols().size() != uniqGbCols.size())
			(csep)->groupByCols(uniqGbCols);
	}
}

void checkAggregation(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	checkGroupByCols(csep, jobInfo);
	checkReturnedColumns(csep, jobInfo);
	RetColsVector& retCols = jobInfo.projectionCols;

	jobInfo.hasDistinct = csep->distinct();
	if (csep->distinct() == true)
	{
		jobInfo.hasAggregation = true;
	}
	else if (csep->groupByCols().size() > 0)
	{
		// groupby without aggregate functions is supported.
		jobInfo.hasAggregation = true;
	}
	else
	{
		for (uint64_t i = 0; i < retCols.size(); i++)
		{
			if (dynamic_cast<AggregateColumn*>(retCols[i].get()) != NULL)
			{
				jobInfo.hasAggregation = true;
				break;
			}
		}
	}
}

void updateAggregateColType(AggregateColumn* ac, const SRCP* srcp, int op, JobInfo& jobInfo)
{
	CalpontSystemCatalog::ColType ct;
	const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp->get());
	const ArithmeticColumn* ar = NULL;
	const FunctionColumn* fc = NULL;
	if (sc != NULL)
		ct = sc->resultType();
	else if ((ar = dynamic_cast<const ArithmeticColumn*>(srcp->get())) != NULL)
		ct = ar->resultType();
	else if ((fc = dynamic_cast<const FunctionColumn*>(srcp->get())) != NULL)
		ct = fc->resultType();

	if (op == AggregateColumn::SUM || op == AggregateColumn::DISTINCT_SUM)
	{
		if (ct.colDataType == CalpontSystemCatalog::TINYINT ||
			ct.colDataType == CalpontSystemCatalog::SMALLINT ||
			ct.colDataType == CalpontSystemCatalog::MEDINT ||
			ct.colDataType == CalpontSystemCatalog::INT ||
			ct.colDataType == CalpontSystemCatalog::BIGINT ||
			ct.colDataType == CalpontSystemCatalog::DECIMAL)
		{
			ct.colWidth = sizeof(int64_t);
			if (ct.scale != 0)
				ct.colDataType = CalpontSystemCatalog::DECIMAL;
			else
				ct.colDataType = CalpontSystemCatalog::BIGINT;
			ct.precision = 19;
		}
	}
	else if (op == AggregateColumn::STDDEV_POP || op == AggregateColumn::STDDEV_SAMP ||
			 op == AggregateColumn::VAR_POP    || op == AggregateColumn::VAR_SAMP)
	{
		ct.colWidth = sizeof(double);
		ct.colDataType = CalpontSystemCatalog::DOUBLE;
		ct.scale = 0;
		ct.precision = 0;
	}
	else
	{
		ct = ac->resultType();
	}

	ac->resultType(ct);

	// update the original if this aggregate column is cloned from function on aggregation
	pair<multimap<ReturnedColumn*, ReturnedColumn*>::iterator,
		 multimap<ReturnedColumn*, ReturnedColumn*>::iterator> range =
			jobInfo.cloneAggregateColMap.equal_range(ac);
	for (multimap<ReturnedColumn*, ReturnedColumn*>::iterator i=range.first; i!=range.second; ++i)
		(i->second)->resultType(ct);
}


const JobStepVector doAggProject(const CalpontSelectExecutionPlan* csep, JobInfo& jobInfo)
{
	map<uint, uint> projectColMap;   // projected column map    -- unique
	RetColsVector pcv;               // projected column vector -- may have duplicates

	// add the groupby cols in the front part of the project column vector (pcv)
	const CalpontSelectExecutionPlan::GroupByColumnList& groupByCols = csep->groupByCols();
	uint64_t lastGroupByPos = 0;
	for (uint64_t i = 0; i < groupByCols.size(); i++)
	{
		pcv.push_back(groupByCols[i]);
		lastGroupByPos++;

		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(groupByCols[i].get());
		const ArithmeticColumn* ac = NULL;
		const FunctionColumn* fc = NULL;
		if (sc != NULL)
		{
			CalpontSystemCatalog::OID gbOid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			CalpontSystemCatalog::OID dictOid = 0;
			CalpontSystemCatalog::ColType ct;
			if (!sc->schemaName().empty())
			{
				ct = jobInfo.csc->colType(gbOid);
				dictOid = isDictCol(ct);
			}
			else
			{
				gbOid = (tblOid+1) + sc->colPosition();
				ct = jobInfo.vtableColTypes[UniqId(gbOid, sc->tableAlias(), sc->viewName())];
			}

			// As of bug3695, make sure varbinary is not used in group by.
			if (ct.colDataType == CalpontSystemCatalog::VARBINARY)
				throw runtime_error ("VARBINARY in group by is not supported.");

			string alias(extractTableAlias(sc));
			string view(sc->viewName());
			TupleInfo ti(setTupleInfo(ct, gbOid, jobInfo, tblOid, sc, alias));
			uint tupleKey = ti.key;
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;

			// for dictionary columns, replace the token oid with string oid
			if (dictOid > 0)
			{
				jobInfo.tokenOnly[tupleKey] = false;
				ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
				jobInfo.keyInfo->dictKeyMap[tupleKey] = ti.key;
				tupleKey = ti.key;
			}
			jobInfo.groupByColVec.push_back(tupleKey);
		}
		else if ((ac = dynamic_cast<const ArithmeticColumn*>(groupByCols[i].get())) != NULL)
		{
			uint64_t eid = ac->expressionId();
			CalpontSystemCatalog::ColType ct = ac->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, ac->alias(), jobInfo));
			uint tupleKey = ti.key;
			jobInfo.groupByColVec.push_back(tupleKey);
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;
		}
		else if ((fc = dynamic_cast<const FunctionColumn*>(groupByCols[i].get())) != NULL)
		{
			uint64_t eid = fc->expressionId();
			CalpontSystemCatalog::ColType ct = fc->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, fc->alias(), jobInfo));
			uint tupleKey = ti.key;
			jobInfo.groupByColVec.push_back(tupleKey);
			if (projectColMap.find(tupleKey) == projectColMap.end())
				projectColMap[tupleKey] = i;
		}
		else
		{
			std::ostringstream errmsg;
			errmsg << "doAggProject: unsupported group by column: "
					 << typeid(*groupByCols[i]).name();
			cerr << boldStart << errmsg.str() << boldStop << endl;
			throw logic_error(errmsg.str());
		}
	}

	// process the returned columns
	RetColsVector& retCols = jobInfo.projectionCols;
	for (uint64_t i = 0; i < retCols.size(); i++)
	{
		GroupConcatColumn* gcc = dynamic_cast<GroupConcatColumn*>(retCols[i].get());
		if (gcc != NULL)
		{
			const SRCP* srcp = &(gcc->functionParms());
			const RowColumn* rcp = dynamic_cast<const RowColumn*>(srcp->get());

			const vector<SRCP>& cols = rcp->columnVec();
			for (vector<SRCP>::const_iterator j = cols.begin(); j != cols.end(); j++)
			{
				if (dynamic_cast<const ConstantColumn*>(j->get()) == NULL)
					retCols.push_back(*j);
			}

			vector<SRCP>& orderCols = gcc->orderCols();
			for (vector<SRCP>::iterator k = orderCols.begin(); k != orderCols.end(); k++)
			{
				if (dynamic_cast<const ConstantColumn*>(k->get()) == NULL)
					retCols.push_back(*k);
			}

			continue;
		}

		const SRCP* srcp = &(retCols[i]);
		const AggregateColumn* ag = dynamic_cast<const AggregateColumn*>(retCols[i].get());
		if (ag != NULL)
			srcp = &(ag->functionParms());

		const ArithmeticColumn* ac = dynamic_cast<const ArithmeticColumn*>(srcp->get());
		const FunctionColumn* fc = dynamic_cast<const FunctionColumn*>(srcp->get());
		if (ac != NULL || fc != NULL)
		{
			// bug 3728, make a dummy expression step for each expression.
			scoped_ptr<ExpressionStep> es(new ExpressionStep(jobInfo.sessionId,
															 jobInfo.txnId,
															 jobInfo.verId,
															 jobInfo.statementId));
			es->logger(jobInfo.logger);
			es->expression(*srcp, jobInfo);
		}
	}

	map<uint, CalpontSystemCatalog::OID> dictMap; // bug 1853, the tupleKey - dictoid map
	for (uint64_t i = 0; i < retCols.size(); i++)
	{
		const SRCP* srcp = &(retCols[i]);
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp->get());
		bool doDistinct = (csep->distinct() && csep->groupByCols().empty());
		uint tupleKey = -1;
		string alias;
		string view;

		// returned column could be groupby column, a simplecoulumn not a agregatecolumn
		int op = 0;
		CalpontSystemCatalog::OID dictOid = 0;
		CalpontSystemCatalog::ColType ct, aggCt;

		if (sc == NULL)
		{
			GroupConcatColumn* gcc = dynamic_cast<GroupConcatColumn*>(retCols[i].get());
			if (gcc != NULL)
			{
				jobInfo.groupConcatCols.push_back(retCols[i]);

				uint64_t eid = gcc->expressionId();
				ct = gcc->resultType();
				TupleInfo ti(setExpTupleInfo(ct, eid, gcc->alias(), jobInfo));
				tupleKey = ti.key;
				jobInfo.returnedColVec.push_back(make_pair(tupleKey, gcc->aggOp()));

				continue;
			}

			AggregateColumn* ac = dynamic_cast<AggregateColumn*>(retCols[i].get());
			if (ac != NULL)
			{
				srcp = &(ac->functionParms());
				sc = dynamic_cast<const SimpleColumn*>(srcp->get());
				if (ac->constCol().get() != NULL)
				{
					// replace the aggregate on constant with a count(*)
					SRCP clone(new AggregateColumn(*ac, ac->sessionID()));
					jobInfo.constAggregate.insert(make_pair(i, clone));
					ac->aggOp(AggregateColumn::COUNT_ASTERISK);
					ac->distinct(false);
				}
				op = ac->aggOp();
				doDistinct = ac->distinct();
				updateAggregateColType(ac, srcp, op, jobInfo);
				aggCt = ac->resultType();

				// As of bug3695, make sure varbinary is not used in aggregation.
				if (sc != NULL && sc->resultType().colDataType == CalpontSystemCatalog::VARBINARY)
					throw runtime_error ("VARBINARY in aggregate function is not supported.");
			}
		}

		// simple column selected or aggregated
		if (sc != NULL)
		{
			// one column only need project once
			CalpontSystemCatalog::OID retOid = sc->oid();
			CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
			alias = extractTableAlias(sc);
			view = sc->viewName();
			if (!sc->schemaName().empty())
			{
				ct = jobInfo.csc->colType(retOid);
				dictOid = isDictCol(ct);
			}
			else
			{
				retOid = (tblOid+1) + sc->colPosition();
				ct = jobInfo.vtableColTypes[UniqId(retOid, sc->tableAlias(), sc->viewName())];
			}

			TupleInfo ti(setTupleInfo(ct, retOid, jobInfo, tblOid, sc, alias));
			tupleKey = ti.key;

			// this is a string column
			if (dictOid > 0)
			{
				map<uint, bool>::iterator findit = jobInfo.tokenOnly.find(tupleKey);
				// if the column has never seen, and the op is count: possible need count only.
				if (AggregateColumn::COUNT == op || AggregateColumn::COUNT_ASTERISK == op)
				{
					if (findit == jobInfo.tokenOnly.end())
						jobInfo.tokenOnly[tupleKey] = true;
				}
				// if aggregate other than count, token is not enough.
				else if (op != 0 || doDistinct)
				{
					jobInfo.tokenOnly[tupleKey] = false;
				}

				findit = jobInfo.tokenOnly.find(tupleKey);
				if (!(findit != jobInfo.tokenOnly.end() && findit->second == true))
				{
					dictMap[tupleKey] = dictOid;
					jobInfo.keyInfo->dictOidToColOid[dictOid] = retOid;
					ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
					jobInfo.keyInfo->dictKeyMap[tupleKey] = ti.key;
				}
			}
		}
		else
		{
			const ArithmeticColumn* ac = NULL;
			const FunctionColumn* fc = NULL;
			bool hasAggCols = false;
			if ((ac = dynamic_cast<const ArithmeticColumn*>(srcp->get())) != NULL)
			{
				if (ac->aggColumnList().size() > 0)
					hasAggCols = true;
			}
			else if ((fc = dynamic_cast<const FunctionColumn*>(srcp->get())) != NULL)
			{
				if (fc->aggColumnList().size() > 0)
					hasAggCols = true;
			}
			else
			{
				std::ostringstream errmsg;
				errmsg << "doAggProject: unsupported column: " << typeid(*(srcp->get())).name();
				cerr << boldStart << errmsg.str() << boldStop << endl;
				throw logic_error(errmsg.str());
			}

			uint64_t eid = srcp->get()->expressionId();
			ct = srcp->get()->resultType();
			TupleInfo ti(setExpTupleInfo(ct, eid, srcp->get()->alias(), jobInfo));
			tupleKey = ti.key;
			if (hasAggCols)
				jobInfo.expressionVec.push_back(tupleKey);
		}

		// add to project list
		if (projectColMap.find(tupleKey) == projectColMap.end())
		{
			RetColsVector::iterator it = pcv.end();
			if (doDistinct)
				it = pcv.insert(pcv.begin()+lastGroupByPos++, *srcp);
			else
				it = pcv.insert(pcv.end(), *srcp);

			projectColMap[tupleKey] = distance(pcv.begin(), it);
		}
		else if (doDistinct) // @bug4250, move forward distinct column if necessary.
		{
			uint pos = projectColMap[tupleKey];
			if (pos >= lastGroupByPos)
			{
				pcv[pos] = pcv[lastGroupByPos];
				pcv[lastGroupByPos] = *srcp;

				// @bug4935, update the projectColMap after swapping
				map<uint, uint>::iterator j = projectColMap.begin();
				for (; j != projectColMap.end(); j++)
					if (j->second == lastGroupByPos)
						j->second = pos;

				projectColMap[tupleKey] = lastGroupByPos;
				lastGroupByPos++;
			}
		}

		if (doDistinct && dictOid > 0)
			tupleKey = jobInfo.keyInfo->dictKeyMap[tupleKey];

		// remember the columns to be returned
		jobInfo.returnedColVec.push_back(make_pair(tupleKey, op));
		if (op == AggregateColumn::AVG || op == AggregateColumn::DISTINCT_AVG)
			jobInfo.scaleOfAvg[tupleKey] = (ct.scale << 8) + aggCt.scale;

		// bug 1499 distinct processing, save unique distinct columns
		if (doDistinct &&
			(jobInfo.distinctColVec.end() ==
				find(jobInfo.distinctColVec.begin(), jobInfo.distinctColVec.end(), tupleKey)))
		{
			jobInfo.distinctColVec.push_back(tupleKey);
		}
	}


	// for dictionary columns not count only, replace the token oid with string oid
	for (vector<pair<uint32_t, int> >::iterator it = jobInfo.returnedColVec.begin();
			it != jobInfo.returnedColVec.end(); it++)
	{
		// if the column is a dictionary column and not count only
		bool tokenOnly = false;
		map<uint, bool>::iterator i = jobInfo.tokenOnly.find(it->first);
		if (i != jobInfo.tokenOnly.end())
			tokenOnly = i->second;

		if (dictMap.find(it->first) != dictMap.end() && !tokenOnly)
		{
			uint tupleKey = jobInfo.keyInfo->dictKeyMap[it->first];
			int op = it->second;
			*it = make_pair(tupleKey, op);
		}
	}

	return doProject(pcv, jobInfo);
}


template <typename T>
class Uniqer : public unary_function<typename T::value_type, void>
{
private:
	typedef typename T::mapped_type Mt_;
	class Pred : public unary_function<const Mt_, bool>
	{
	public:
		Pred(const Mt_& retCol) : fRetCol(retCol) { }
		bool operator()(const Mt_ rc) const
		{
			return fRetCol->sameColumn(rc.get());
		}
	private:
 		const Mt_& fRetCol;
	};
public:
	void operator()(typename T::value_type mapItem)
	{
		Pred pred(mapItem.second);
		RetColsVector::iterator iter;
		iter = find_if(fRetColsVec.begin(), fRetColsVec.end(), pred);
		if (iter == fRetColsVec.end())
		{
			//Add this ReturnedColumn
			fRetColsVec.push_back(mapItem.second);
		}
	}
	RetColsVector fRetColsVec;
};

uint16_t numberSteps(JobStepVector& steps, uint16_t stepNo, uint32_t flags)
{
	JobStepVector::iterator iter = steps.begin();
	JobStepVector::iterator end = steps.end();
	while (iter != end)
	{
		// don't number the delimiters
		if (dynamic_cast<OrDelimiter*>(iter->get()) != NULL)
		{
			++iter;
			continue;
		}

		JobStep* pJobStep = iter->get();
		pJobStep->stepId(stepNo);
		pJobStep->setTraceFlags(flags);
		stepNo++;
		++iter;
	}
	return stepNo;
}

void changePcolStepToPcolScan(JobStepVector::iterator& it, JobStepVector::iterator& end)
{
	pColStep* colStep = dynamic_cast<pColStep*>(it->get());
	pColScanStep* scanStep = 0;
	//Might be a pDictionaryScan step
	if (colStep)
	{
		scanStep = new pColScanStep(*colStep);
	}
	else
	{
		//If we have a pDictionaryScan-pColStep duo, then change the pColStep
		if (typeid(*(it->get())) == typeid(pDictionaryScan) &&
			distance(it, end) > 1 &&
			typeid(*((it + 1)->get())) == typeid(pColStep))
		{
			++it;
			colStep = dynamic_cast<pColStep*>(it->get());
			scanStep = new pColScanStep(*colStep);
		}
	}

	if (scanStep)
	{
		it->reset(scanStep);
	}
}

uint shouldSort(const JobStep* inJobStep, int colWidth)
{
	//only pColStep and pColScan have colType
	const pColStep *inStep = dynamic_cast<const pColStep*>(inJobStep);
	if (inStep && colWidth > inStep->colType().colWidth)
	{
		return 1;
	}
	const pColScanStep *inScan = dynamic_cast<const pColScanStep*>(inJobStep);
	if (inScan && colWidth > inScan->colType().colWidth)
	{
		return 1;
	}
	return 0;
}

void checkBucketReuse(JobStepVector& jsv, JobInfo& jobInfo)
{
	// if CalpontSystemCatalog calls, return
	if (jsv[0]->tableOid() < 3000)
		return;

	// buckets reuse is disabled if there is a transaction
	BRM::DBRM dbrm;
	if (dbrm.getTxnID(jobInfo.sessionId).valid)
		return;

	// lock this check, so the version in reuse manager will not be changed in a race condition
	boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());

	vector<int> posVec;
	bool scan = true;
	JobStepVector::iterator iter = jsv.begin();
	for (int p = 0; iter != jsv.end(); ++p, ++iter)
	{
		if (iter->get()->outputAssociation().outSize() < 1)
			// dummy step, OrDelimiter, etc.
			continue;

		// We used to create a BucketResuseStep at this point, if applicable.
		// But with "new" HashJoinStep, BucketReuse only applies to LHJ, and
		// we don't know till query execution time, whether we will satisfy the
		// hashjoin with LHJ or not.  So we pass BucketReuse info to HashJoin-
		// Step, so that a BucketReuseStep can be created later if needed.

		if (typeid(*(iter->get())) == typeid(pColScanStep))
		{
			if (iter->get()->outputAssociation().outAt(0)->fifoDL() == 0)
				continue;

			JobStepVector childVec;
			getChildJobSteps (*iter, jsv, childVec);
			if ((childVec.size() < 1) ||
				(typeid(*(childVec[0].get())) != typeid(HashJoinStep)))
				continue;

			pColScanStep* pcolscan = dynamic_cast<pColScanStep*>(iter->get());
			string filterString;
			if (pcolscan->filterString().length() > 0)
			{
				filterString = (char*) pcolscan->filterString().buf();
			}

			HashJoinStep* pHj=dynamic_cast<HashJoinStep*>(childVec[0].get());
			FifoDataList* pFifoDL =
					pcolscan->outputAssociation().outAt(0)->fifoDL();
			CalpontSystemCatalog::TableColName tcn = jobInfo.csc->colName(pcolscan->oid());
			pHj->addBucketReuse(pFifoDL, filterString, pcolscan->tableOid(), pcolscan->oid(),
								jobInfo.verId, tcn);
		}

		// (iter-1) may out of range, must check *iter is dictionary first
		else if (typeid(*(iter->get()))     == typeid(pDictionaryStep) &&
				 typeid(*((iter-1)->get())) == typeid(pColScanStep))
		{
			if (iter->get()->outputAssociation().outAt(0)->stringBucketDL() == 0)
				continue;
			pColScanStep* pcolscan = dynamic_cast<pColScanStep*>((iter-1)->get());
			pDictionaryStep* pdict = dynamic_cast<pDictionaryStep*>(iter->get());
			string filterString;
			if (pcolscan->filterString().length() > 0)
			{
				filterString = (char*) pcolscan->filterString().buf();
			}

			uint64_t buckets = pdict->outputAssociation().outAt(0)->stringBucketDL()->bucketCount();
			CalpontSystemCatalog::TableColName tcn = jobInfo.csc->colName(pcolscan->oid());
			BucketReuseControlEntry* entry = BucketReuseManager::instance()->userRegister(
									tcn, filterString, jobInfo.verId, buckets, scan);

			iter->get()->outputAssociation().outAt(0)->stringBucketDL()->reuseControl(entry, !scan);

			if (entry != NULL && scan == false)
			{
				iter->reset(new BucketReuseStep(*pcolscan, *pdict));

				// need remove one job step from jsv later, not to invalid the iterator
				posVec.push_back(p-1);
			}
		}
	}
	lock.unlock();


	// remove the marked steps from jsv, if any
	for (vector<int>::reverse_iterator rit = posVec.rbegin(); rit != posVec.rend(); ++rit)
	{
		jsv.erase(jsv.begin() + *rit);
	}
}

void convertPColStepInProjectToPassThru(JobStepVector& psv, JobInfo& jobInfo)
{
	for (JobStepVector::iterator iter = psv.begin(); iter != psv.end(); ++iter)
	{
		if (typeid(*(iter->get())) == typeid(pColStep))
		{
			JobStepAssociation ia = iter->get()->inputAssociation();
			DataList_t* fifoDlp = ia.outAt(0).get()->dataList();
			pColStep* colStep = dynamic_cast<pColStep*>(iter->get());

			if (fifoDlp)
			{
				if (iter->get()->oid() >= 3000 && iter->get()->oid() == fifoDlp->OID())
				{
					PassThruStep* pts = 0;
					pts = new PassThruStep(*colStep, jobInfo.isExeMgr);
					pts->alias(colStep->alias());
					pts->view(colStep->view());
					pts->name(colStep->name());
					pts->tupleId(iter->get()->tupleId());
					iter->reset(pts);
				}
			}
		}
	}
}

// optimize filter order
//   perform none string filters first because string filter joins the tokens.
void optimizeFilterOrder(JobStepVector& qsv)
{
	// move all none string filters
	uint64_t pdsPos = 0;
	int64_t  orbranch = 0;
	for (; pdsPos < qsv.size(); ++pdsPos)
	{
		// skip the or branches
		OrDelimiterLhs* lhs = dynamic_cast<OrDelimiterLhs*>(qsv[pdsPos].get());
		if (lhs != NULL)
		{
			orbranch++;
			continue;
		}

		if (orbranch > 0)
		{
			UnionStep* us = dynamic_cast<UnionStep*>(qsv[pdsPos].get());
			if (us)
				orbranch--;
		}
		else
		{
			pDictionaryScan* pds = dynamic_cast<pDictionaryScan*>(qsv[pdsPos].get());
			if (pds)
				break;
		}
	}

	// no pDictionaryScan step
	if (pdsPos >= qsv.size())
		return;

	// get the filter steps that are not in or branches
	vector<uint64_t> pcolIdVec;
	JobStepVector pcolStepVec;
	orbranch = 0;
	for (uint64_t i = pdsPos; i < qsv.size(); ++i)
	{
		OrDelimiterLhs* lhs = dynamic_cast<OrDelimiterLhs*>(qsv[pdsPos].get());
		if (lhs != NULL)
		{
			orbranch++;
			continue;
		}

		if (orbranch > 0)
		{
			UnionStep* us = dynamic_cast<UnionStep*>(qsv[pdsPos].get());
			if (us)
				orbranch--;
		}
		else
		{
			pColStep *pcol = dynamic_cast<pColStep*>(qsv[i].get());
			if (pcol != NULL && pcol->filterCount() > 0)
				pcolIdVec.push_back(i);
		}
	}

	for (vector<uint64_t>::reverse_iterator r = pcolIdVec.rbegin(); r < pcolIdVec.rend(); ++r)
	{
		pcolStepVec.push_back(qsv[*r]);
		qsv.erase(qsv.begin() + (*r));
	}

	qsv.insert(qsv.begin() + pdsPos, pcolStepVec.rbegin(), pcolStepVec.rend());
}

void exceptionHandler(JobList* joblist, const JobInfo& jobInfo, const string& logMsg,
	logging::LOG_TYPE logLevel = LOG_TYPE_ERROR)
{
	cerr << "### JobListFactory ses:" << jobInfo.sessionId << " caught: " << logMsg << endl;
	Message::Args args;
	args.add(logMsg);
	jobInfo.logger->logMessage(logLevel, LogMakeJobList, args,
		LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));
	// dummy delivery map, workaround for (qb == 2) in main.cpp
	DeliveredTableMap dtm;
	SJSTEP dummyStep;
	dtm[0] = dummyStep;
	joblist->addDelivery(dtm);
}


void parseExecutionPlan(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	const ParseTree* filters = csep->filters();
	jobInfo.deliveredCols = csep->returnedCols();
	if (filters != 0)
	{
		filters->walk(JLF_ExecPlanToJobList::walkTree, &jobInfo);
	}

	if (jobInfo.trace)
		cout << endl << "Stack: " << endl;

	if (!jobInfo.stack.empty())
	{
		assert(jobInfo.stack.size() == 1);
		querySteps = jobInfo.stack.top();
		jobInfo.stack.pop();

		// do some filter order optimization
		optimizeFilterOrder(querySteps);
	}

	if (jobInfo.selectAndFromSubs.size() > 0)
	{
		querySteps.insert(querySteps.begin(),
							jobInfo.selectAndFromSubs.begin(), jobInfo.selectAndFromSubs.end());
	}

	// bug3391, move forward the aggregation check for no aggregte having clause.
	if (jobInfo.tryTuples)
	{
		checkAggregation(csep, jobInfo);

		// include filters in having clause, if any.
		if (jobInfo.havingStepVec.size() > 0)
			querySteps.insert(querySteps.begin(),
                        		jobInfo.havingStepVec.begin(), jobInfo.havingStepVec.end());
	}

	//Need to change the leading pColStep to a pColScanStep
	//Keep a list of the (table OIDs,alias) that we've already processed for @bug 598 self-join
	set<uint> seenTableIds;

	//Stack of seenTables to make sure the left-hand side and right-hand have the same content
	stack<set<uint> > seenTableStack;

	if (!querySteps.empty())
	{
		JobStepVector::iterator iter = querySteps.begin();
		JobStepVector::iterator end = querySteps.end();
		for (; iter != end; ++iter)
		{
			assert(iter->get());

			// As of bug3695, make sure varbinary is not used in filters.
			if (typeid(*(iter->get())) == typeid(pColStep))
			{
				// only pcolsteps, no pcolscan yet.
				pColStep* pcol = dynamic_cast<pColStep*>(iter->get());
				if (pcol->colType().colDataType == CalpontSystemCatalog::VARBINARY)
				{
					if (pcol->filterCount() != 1)
						throw runtime_error ("VARBINARY in filter or function is not supported.");

					// error out if the filter is not "is null" or "is not null"
					// should block "= null" and "!= null" ???
					messageqcpp::ByteStream filter = pcol->filterString();
					uint8_t op = 0;
					filter >> op;
					bool nullOp = (op == COMPARE_EQ || op == COMPARE_NE || op == COMPARE_NIL);
					filter >> op;  // skip roundFlag
					uint64_t value = 0;
					filter >> value;
					nullOp = nullOp && (value == 0xfffffffffffffffeULL);

					if (nullOp == false)
						throw runtime_error ("VARBINARY in filter or function is not supported.");
				}
			}

			// save the current seentable for right-hand side
			if (typeid(*(iter->get())) == typeid(OrDelimiterLhs))
			{
				seenTableStack.push(seenTableIds);
				continue;
			}

			// restore the seentable
			else if (typeid(*(iter->get())) == typeid(OrDelimiterRhs))
			{
				seenTableIds = seenTableStack.top();
				seenTableStack.pop();
				continue;
			}

			pColStep* colStep = dynamic_cast<pColStep*>(iter->get());
			if (colStep != NULL)
			{
				CalpontSystemCatalog::OID tableOID = colStep->tableOid();
				string alias(colStep->alias());
				string view(colStep->view());
				//If this is the first time we've seen this table or alias
				uint tableId = 0;
				if (jobInfo.tryTuples)
					tableId = getTableKey(jobInfo, colStep->tupleId());
				else
					tableId = tableKey(jobInfo, tableOID, alias, view);

				if (seenTableIds.find(tableId) == seenTableIds.end())
					changePcolStepToPcolScan(iter, end);

				//Mark this OID as seen
				if (tableOID > 0)
					seenTableIds.insert(tableId);
			}
		}
	}

	//build the project steps
	if (jobInfo.tryTuples)
	{
		if (jobInfo.deliveredCols.empty())
		{
			throw logic_error("No delivery column.");
		}

		// if any aggregate columns
		if (jobInfo.hasAggregation == true)
		{
			projectSteps = doAggProject(csep, jobInfo);
		}
		else
		{
			projectSteps = doProject(jobInfo.nonConstCols, jobInfo);
		}

		// bug3736, have jobInfo include the column map info.
		CalpontSelectExecutionPlan::ColumnMap retCols = csep->columnMap();
		CalpontSelectExecutionPlan::ColumnMap::iterator i = retCols.begin();
		for (; i != retCols.end(); i++)
		{
			SimpleColumn* sc = dynamic_cast<SimpleColumn*>(i->second.get());
			if (sc && !sc->schemaName().empty())
			{
				CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
				CalpontSystemCatalog::ColType ct = jobInfo.csc->colType(sc->oid());

				string alias(extractTableAlias(sc));
				TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc, alias));
				uint colKey = ti.key;
				uint tblKey = getTableKey(jobInfo, colKey);
				jobInfo.columnMap[tblKey].push_back(colKey);
			}
		}

		// special case, select without a table, like: select 1;
		if (jobInfo.constantCol == CONST_COL_ONLY)
			return;
	}
	else
	{
		//Get a unique list of returned columns
		CalpontSelectExecutionPlan::ColumnMap retCols = csep->columnMap();
		RetColsVector retColsVec;
		Uniqer<CalpontSelectExecutionPlan::ColumnMap> toUniq;
		toUniq = for_each(retCols.begin(), retCols.end(), Uniqer<CalpontSelectExecutionPlan::ColumnMap>());
		//fRetColsVec is a typedef vector<SRCP> RetColsVector;
		projectSteps = doProject(toUniq.fRetColsVec, jobInfo);
	}

	//If there are no filters (select * from table;) then add one simple scan
	//TODO: more work here...
	// @bug 497 fix. populate a map of tableoid for querysteps. tablescan
	// cols whose table does not belong to the map
	typedef set<uint> tableIDMap_t;
	tableIDMap_t tableIDMap;
	JobStepVector::iterator qsiter = querySteps.begin();
	JobStepVector::iterator qsend = querySteps.end();
	uint tableId = 0;
	while (qsiter != qsend)
	{
		JobStep* js = qsiter->get();
		if (jobInfo.tryTuples && js->tupleId() != (uint64_t) -1)
			tableId = getTableKey(jobInfo, js->tupleId());
		else
			tableId = tableKey(jobInfo, js->tableOid(), js->alias(), js->view());
		tableIDMap.insert(tableId);
		++qsiter;
	}

	JobStepVector::iterator jsiter = projectSteps.begin();
	JobStepVector::iterator jsend = projectSteps.end();
	while (jsiter != jsend)
	{
		JobStep* js = jsiter->get();
		if (jobInfo.tryTuples && js->tupleId() != (uint64_t) -1)
			tableId = getTableKey(jobInfo, js->tupleId());
		else
			tableId = tableKey(jobInfo, js->tableOid(), js->alias(), js->view());

		if (typeid(*(jsiter->get())) == typeid(pColStep) &&
				tableIDMap.find(tableId) == tableIDMap.end())
		{
			SJSTEP step0 = *jsiter;
			pColStep* colStep = dynamic_cast<pColStep*>(step0.get());
			pColScanStep* scanStep = new pColScanStep(*colStep);
			//clear out any output association so we get a nice, new one during association
			scanStep->outputAssociation(JobStepAssociation(jobInfo.status));
			step0.reset(scanStep);
			querySteps.push_back(step0);
			js = step0.get();
			if (jobInfo.tryTuples)
				tableId = getTableKey(jobInfo, js->tupleId());
			else
				tableId = tableKey(jobInfo, js->tableOid(), js->alias(), js->view());

			tableIDMap.insert(tableId);
		}
		++jsiter;
	}

	if (querySteps.size() < tableIDMap.size() && !jobInfo.tryTuples)
	{
		cerr << boldStart << "makeJobList: couldn't find a suitable first query step for some tables!" <<
			boldStop << endl;
	}

	//build the delivery steps from the tables in queryTables
	{
		DeliveredTablesSet::iterator iter = jobInfo.tables.begin();
		DeliveredTablesSet::iterator end = jobInfo.tables.end();
		SJSTEP sjstep;
		while (iter != end)
		{
			DeliveryStep*  ds = new DeliveryStep(JobStepAssociation(jobInfo.status),
						JobStepAssociation(jobInfo.status), *iter,
						jobInfo.csc,
						jobInfo.sessionId,
						jobInfo.txnId,
						jobInfo.statementId,
						jobInfo.flushInterval);
			sjstep.reset(ds);
			deliverySteps[ds->tableOid()] = sjstep;
			++iter;
		}
	}
}


// table mode
void makeTableModeSteps(const CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	//TODO: this needs to be significantly smarter: currently it just links all the steps together. It really
	// needs to link the filter predicates for each table together, then link each table into a join step.

	JobStepVector::iterator iter;
	JobStepVector::iterator end = querySteps.end();
	//Need to keep a map of the current inputJS for each table
	typedef map<uint, JobStepAssociation> JSAMap_t;
	JSAMap_t tableJSAMap;

	// Save the current JSAMap before handling an OR left-hand branch,
	// restore the JSAMap before handling the right-hand branch
	// use stack for nested OR operation
	// This logic could be replaced by a recursive method
	stack<JSAMap_t> jsaMapStack;

	//count the project steps
	//moved numbering the steps to after checkProjectSteps because it may add union steps.
	unsigned numProjectSteps = 0;
	{
		JobStepVector::iterator iter = projectSteps.begin();
		JobStepVector::iterator end = projectSteps.end();
		while (iter != end)
		{
			if (typeid(*iter->get()) != typeid(pDictionaryStep))
			{
				numProjectSteps++;
			}
			++iter;
		}
	}

	//Need to keep a map of the last filter step for each table (to change its output FIFO to a BDL)
	typedef map<uint, JobStepVector::iterator> tableStepMap_t;
	tableStepMap_t tableStepMap;
	map<uint, SJSTEP> lastStepMap;
	for (iter = querySteps.begin(); iter != end; ++iter)
	{
		bool threeStep = false;
		bool twoStep = false;
		bool joinStep = false;
		bool stringJoinStep = false;
		bool filterStep = false;
		bool stringFilterStep = false;
		bool filterStepOne = false;
		bool stringFilterStepOne = false;
		JobStepVector::iterator inIter;
		JobStepVector::iterator outIter;

		inIter = iter;
		outIter = iter;

		// save the current map for right-hand side of an OpOR
		if (typeid(*(iter->get())) == typeid(OrDelimiterLhs))
		{
			jsaMapStack.push(tableJSAMap);
			continue;
		}
		else if (typeid(*(iter->get())) == typeid(OrDelimiterRhs))
		{
			tableJSAMap = jsaMapStack.top();
			jsaMapStack.pop();
			continue;
		}

		if (distance(inIter, end) > 2 &&
			typeid(*(inIter->get())) == typeid(pDictionaryScan) &&
			typeid(*((inIter + 1)->get())) == typeid(pColStep) &&
			(typeid(*((inIter + 2)->get())) == typeid(HashJoinStep) ||
			typeid(*((inIter + 2)->get())) == typeid(TupleHashJoinStep)))
		{
			threeStep = true;
		}
		else if (distance(inIter, end) > 1 &&
			typeid(*(inIter->get())) == typeid(pDictionaryScan) &&
			typeid(*((inIter + 1)->get())) == typeid(pColStep))
		{
			twoStep = true;
		}
		else if (typeid(*(inIter->get())) == typeid(HashJoinStep) ||
			typeid(*(inIter->get())) == typeid(TupleHashJoinStep))
		{
			joinStep = true;
		}
		else if (typeid(*(inIter->get())) == typeid(StringHashJoinStep))
		{
			stringJoinStep = true;
		}
		else if (distance(inIter, end) > 2 &&
			typeid(*((inIter + 1)->get())) == typeid(pColStep) &&
			typeid(*((inIter + 2)->get())) == typeid(FilterStep))
		{
			filterStep = true;
		}
		else if (distance(inIter, end) > 3 &&
			typeid(*((inIter + 1)->get())) == typeid(pColStep) &&
			typeid(*((inIter + 2)->get())) == typeid(pDictionaryStep) &&
			typeid(*((inIter + 3)->get())) == typeid(FilterStep))
		{
			filterStepOne = true;
		}
		else if (distance(inIter, end) > 3 &&
			typeid(*((inIter + 1)->get())) == typeid(pDictionaryStep) &&
			typeid(*((inIter + 2)->get())) == typeid(pColStep) &&
			typeid(*((inIter + 3)->get())) == typeid(FilterStep) )
		{
			stringFilterStepOne = true;
		}
		else if (distance(inIter, end) > 4 &&
			typeid(*((inIter + 1)->get())) == typeid(pDictionaryStep) &&
			typeid(*((inIter + 2)->get())) == typeid(pColStep) &&
			typeid(*((inIter + 3)->get())) == typeid(pDictionaryStep) &&
			typeid(*((inIter + 4)->get())) == typeid(FilterStep) )
		{
			stringFilterStep = true;
		}

		if (threeStep)
		{
			outIter = iter + 2;
		}
		//If this step is a pDictionaryScan and the next is a pColStep, fudge the outIter...
		//TODO: implement a "2-step" step to make this cleaner
		else if (twoStep)
		{
			outIter = iter + 1;
		}
		//If we have a join step, we need to do some more stuff...
		else if (joinStep)
		{
			; //nop
		}
		//If we have a column filter step, we need to do some more stuff...
		else if (filterStep)
		{
			outIter = iter + 2;
		}
		else if (stringFilterStep)
		{
			outIter = iter + 4;
		}
		else if (filterStepOne)
		{
			outIter = iter + 3;
		}
		else if (stringFilterStepOne)
		{
			outIter = iter + 3;
		}
		//TODO: determine the nc value. Mostly it's 1, but the final filter step for a table will feed into
		// all the projection steps for that table
		unsigned nc;

		if (distance(outIter, end) == 1)
			nc = numProjectSteps;
		else
			nc = 1;

		//TODO: look at this
		nc = numProjectSteps;

		if (joinStep || stringJoinStep)
		{

			JobStepAssociation inJs(jobInfo.status);
			CalpontSystemCatalog::OID oid1, oid2;
			if (joinStep)
			{
				//(outIter - 1) points to a feeding pColStep
				//(outIter - 2) points to a feeding pColStep
				inJs.outAdd((outIter - 2)->get()->outputAssociation().outAt(0));
				inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(0));
				oid1 = inJs.outAt(0)->dataList()->OID();
				oid2 = inJs.outAt(1)->dataList()->OID();
			}
			else
			{
				//(outIter - 4) points to a feeding pColStep for pDictionaryStep
				//(outIter - 3) points to a feeding pDictionaryStep for join
				//(outIter - 2) points to a feeding pColStep for pDictionaryStep
				//(outIter - 1) points to a feeding pDictionaryStep for join
				inJs.outAdd((outIter - 3)->get()->outputAssociation().outAt(0));
				inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(0));
				oid1 = inJs.outAt(0)->stringDataList()->OID();
				oid2 = inJs.outAt(1)->stringDataList()->OID();
			}
			outIter->get()->inputAssociation(inJs);

			JobStepAssociation outJs(jobInfo.status), newIJs1(jobInfo.status), newIJs2(jobInfo.status);
			StringHashJoinStep* shjsp = 0;
			HashJoinStep* hjsp = dynamic_cast<HashJoinStep*>(outIter->get());
			TupleHashJoinStep* thjsp = dynamic_cast<TupleHashJoinStep*>(outIter->get());
			if (!hjsp && !thjsp)
			{
				shjsp = dynamic_cast<StringHashJoinStep*>(outIter->get());
				assert(shjsp);
			}

			assert((hjsp && !thjsp && !shjsp) || (!hjsp && thjsp && !shjsp) || (!hjsp && !thjsp && shjsp));

			outJs.toSort(1);
			newIJs1.toSort(1);
			newIJs2.toSort(1);

			if (outIter->get()->outputAssociation().outSize() == 2)
			{
				outJs = outIter->get()->outputAssociation();
				newIJs1.outAdd(outJs.outAt(0));
				newIJs2.outAdd(outJs.outAt(1));
			}
			else
			{
				//@Bug 1308. Put back output datalist back for stringhashjoin
				if (stringJoinStep)
				{
					// datalists for output of the string hash join
					// output of StringHashJoinStep are also zdl,
					// because only need the rid, the string is ignored now.
					AnyDataListSPtr spdl1(new AnyDataList());

					ZonedDL* dl1 = new ZonedDL(nc, jobInfo.rm);
					dl1->setMultipleProducers(true);
					dl1->OID(oid1);
					spdl1->zonedDL(dl1);
					outJs.outAdd(spdl1);
					newIJs1.outAdd(spdl1);
					AnyDataListSPtr spdl2(new AnyDataList());

					// If joining a pDictionaryScan to a pColScanStep, we drop the
					// hashjoin output from the pDictionaryScan, as it is not used
					if ((typeid(*((outIter-2)->get())) == typeid(pDictionaryScan))&&
						(typeid(*((outIter-1)->get())) == typeid(pColScanStep)))
					{
						dl1->dropAllInserts();
					}

					dl1 = new ZonedDL(nc, jobInfo.rm);
					dl1->setMultipleProducers(true);
					dl1->OID(oid2);
					spdl2->zonedDL(dl1);
					outJs.outAdd(spdl2);
					newIJs2.outAdd(spdl2);
				}
				else
				{
					// datalists for output of the hash join or string hash join
					AnyDataListSPtr spdl1(new AnyDataList());
if (oid1 >= 3000)
FIFODEBUG();
					FifoDataList* dl1 = new FifoDataList(nc, jobInfo.fifoSize);
					dl1->OID(oid1);
					spdl1->fifoDL(dl1);
					outJs.outAdd(spdl1);
					newIJs1.outAdd(spdl1);
					AnyDataListSPtr spdl2(new AnyDataList());
if (oid2 >= 3000)
FIFODEBUG();
					dl1 = new FifoDataList(nc, jobInfo.fifoSize);
					dl1->OID(oid2);
					spdl2->fifoDL(dl1);
					outJs.outAdd(spdl2);
					newIJs2.outAdd(spdl2);
				}
			}

			// HashJoinStep
			if (hjsp)
			{
				hjsp->outputAssociation(outJs);
				if (hjsp->tableOid1() > 0)
				{
					tableJSAMap[tableKey(jobInfo, hjsp->tableOid1(),
											hjsp->alias1(), hjsp->view1())] = newIJs1;
					lastStepMap[tableKey(jobInfo, hjsp->tableOid1(),
											hjsp->alias1(), hjsp->view1())] = *outIter;
				}
				tableJSAMap[tableKey(jobInfo, hjsp->tableOid2(),
										hjsp->alias2(), hjsp->view2())] = newIJs2;
				lastStepMap[tableKey(jobInfo, hjsp->tableOid2(),
										hjsp->alias2(), hjsp->view2())] = *outIter;
			}
			else if (thjsp)
			{
				thjsp->outputAssociation(outJs);
				if (thjsp->tableOid1() > 0)
				{
					tableJSAMap[tableKey(jobInfo, thjsp->tableOid1(),
											thjsp->alias1(), thjsp->view1())] = newIJs1;
					lastStepMap[tableKey(jobInfo, thjsp->tableOid1(),
											thjsp->alias1(), thjsp->view1())] = *outIter;
				}
				tableJSAMap[tableKey(jobInfo, thjsp->tableOid2(),
										thjsp->alias2(), thjsp->view2())] = newIJs2;
				lastStepMap[tableKey(jobInfo, thjsp->tableOid2(),
										thjsp->alias2(), thjsp->view2())] = *outIter;
				if (jobInfo.trace) cout << "For THJS " << thjsp->toString() << endl << "oid1 = " <<
					thjsp->tableOid1() << ", oid2 = " << thjsp->tableOid2() << ", alias1 = " <<
					thjsp->alias1() << ", view1 = " << thjsp->view1() << ", alias2 = " <<
					thjsp->alias2() << ", view2 = " << thjsp->view2() << "." << endl;
			}
			else
			// StringHashJoinStep
			{
				shjsp->outputAssociation(outJs);
				if (shjsp->tableOid1() > 0)
				{
					uint t1 = tableKey(jobInfo,shjsp->tableOid1(),shjsp->alias1(),shjsp->view1());
					tableJSAMap[t1] = newIJs1;
					lastStepMap[t1] = *outIter;
				}
				uint t2 = tableKey(jobInfo, shjsp->tableOid2(), shjsp->alias2(), shjsp->view2());
				tableJSAMap[t2] = newIJs2;
				lastStepMap[t2] = *outIter;
			}

			continue;
		}

		if (typeid(*(inIter->get())) == typeid(ReduceStep))
		{
			// make the input association if not set yet
			if (inIter->get()->inputAssociation().outSize() == 0)
			{
				if ((inIter-1)->get()->outputAssociation().outSize() > 1)
				{
					JobStepAssociation inJs(jobInfo.status);
					inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(0));
					inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(1));
					outIter->get()->inputAssociation(inJs);
				}
				else
				{
					throw(std::runtime_error("Not enough info to get input for ReduceStep."));
				}
			}

			// if the outputAssociation is already set, use it
			JobStepAssociation outJs(jobInfo.status);
			if (outIter->get()->outputAssociation().outSize() > 0)
			{
				outJs = outIter->get()->outputAssociation();
			}
			else
			{
				AnyDataListSPtr spdl(new AnyDataList());
FIFODEBUG();
				FifoDataList* dl = new FifoDataList(1, jobInfo.fifoSize);
				dl->inOrder(true);
				spdl->fifoDL(dl);
				outJs.outAdd(spdl);
			}
			ReduceStep* rs = dynamic_cast<ReduceStep*>(outIter->get());
			rs->outputAssociation(outJs);
			//Put both alias into map
			tableJSAMap[tableKey(jobInfo, rs->tableOid(), rs->alias1(), rs->view1())] = outJs;
			tableJSAMap[tableKey(jobInfo, rs->tableOid(), rs->alias2(), rs->view2())] = outJs;
			if (! rs->alias1().empty())
				lastStepMap[tableKey(jobInfo, rs->tableOid(), rs->alias1(), rs->view1())] = *outIter;
			if (! rs->alias2().empty())
				lastStepMap[tableKey(jobInfo, rs->tableOid(), rs->alias2(), rs->view2())] = *outIter;
			continue;
		}

		if (typeid(*(inIter->get())) == typeid(UnionStep))
		{
			// make the input association if not set yet
			if (inIter->get()->inputAssociation().outSize() == 0)
			{
				if ((inIter-1)->get()->outputAssociation().outSize() > 1)
				{
					JobStepAssociation inJs(jobInfo.status);
					inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(0));
					inJs.outAdd((outIter - 1)->get()->outputAssociation().outAt(1));
					outIter->get()->inputAssociation(inJs);
				}
				else
				{
					throw(std::runtime_error("Not enough info to get input for UnionStep."));
				}
			}

			// if the outputAssociation is already set, use it
			JobStepAssociation outJs(jobInfo.status);
			if (outIter->get()->outputAssociation().outSize() > 0)
			{
				outJs = outIter->get()->outputAssociation();
			}
			else
			{
				AnyDataListSPtr spdl(new AnyDataList());
				FifoDataList* dl = new FifoDataList(1, jobInfo.fifoSize);
				dl->inOrder(true);
				spdl->fifoDL(dl);
				outJs.outAdd(spdl);
			}

			UnionStep* us = dynamic_cast<UnionStep*>(outIter->get());
			if (us != NULL)
			{
				us->outputAssociation(outJs);
				//Put both alias into map
				uint t1 = tableKey(jobInfo, us->tableOid(), us->alias1(), us->view1());
				uint t2 = tableKey(jobInfo, us->tableOid(), us->alias2(), us->view2());
				tableJSAMap[t1] = outJs;
				tableJSAMap[t2] = outJs;
				if (! us->alias1().empty())
					lastStepMap[t1] = *outIter;
				if (! us->alias2().empty())
					lastStepMap[t2] = *outIter;
				continue;
			}
		}


		if (filterStep || stringFilterStep || filterStepOne || stringFilterStepOne)
		{
			//Don't associate two pColStep
			CalpontSystemCatalog::OID tableOid = inIter->get()->tableOid();

			if (typeid(*(inIter->get())) == typeid(pColScanStep) &&
				typeid(*((inIter+1)->get())) == typeid(pColStep))
			{
				//NOP?
			}
			else
			{
				inIter->get()->inputAssociation(tableJSAMap[tableKey(
					jobInfo, tableOid, inIter->get()->alias(), inIter->get()->view())]);
			}

			// datalists for output of the filterStep
			if (outIter->get()->outputAssociation().outSize() == 0)
			{
				AnyDataListSPtr spdl1(new AnyDataList());
				if (filterStep || filterStepOne)
				{
FIFODEBUG();
					FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
					dl1->setMultipleProducers(false);
					dl1->OID(outIter->get()->oid());
					dl1->inOrder(true);
					spdl1->fifoDL(dl1);
				}
				else
				{
					StringFifoDataList* dl1 = new StringFifoDataList(1, jobInfo.fifoSize);
					dl1->setMultipleProducers(false);
					dl1->OID(outIter->get()->oid());
					dl1->inOrder(true);
					spdl1->stringDL(dl1);
				}

				JobStepAssociation outJs(jobInfo.status);
				outJs.outAdd(spdl1);
				outIter->get()->outputAssociation(outJs);
			}

			uint tk = tableKey(jobInfo, tableOid, outIter->get()->alias(), outIter->get()->view());
			tableJSAMap[tk] = outIter->get()->outputAssociation();
			tableStepMap[tk] = outIter;
			lastStepMap[tk] = *outIter;
			++iter;
			++iter;
			if (stringFilterStep)
			{
				++iter;
				++iter;
			}
			else if (filterStepOne || stringFilterStepOne)
				++iter;

			continue;
		}

		AnyDataListSPtr spdl1(new AnyDataList());
		FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
		spdl1->fifoDL(dl1);
		dl1->OID(outIter->get()->oid());

		JobStepAssociation outJs(jobInfo.status);
		outJs.outAdd(spdl1);
		CalpontSystemCatalog::OID tableOid = inIter->get()->tableOid();
		if (tableOid > 0)
		{
			if (typeid(*(inIter->get())) != typeid(pColScanStep) &&
				typeid(*(inIter->get())) != typeid(pDictionaryScan) &&
				typeid(*(inIter->get())) != typeid(UnionStep) &&
				typeid(*(inIter->get())) != typeid(AggregateFilterStep))
			{
				uint t = tableKey(jobInfo, tableOid, inIter->get()->alias(), inIter->get()->view());

				inIter->get()->inputAssociation(tableJSAMap[t]);

				//Set sort if inIter's colwidth is greater than the input colStep's colwidth
				//The input step will be in the tableStepMap
				pColStep *step = dynamic_cast<pColStep*>(inIter->get());
				if (step)
				{
					step->toSort(shouldSort(tableStepMap[t]->get(), step->colType().colWidth));
				}
			}
		}

		if (threeStep)
		{
			// threestep: pDictionaryScan, pColStep, HashJoinStep
			// make the input association from previous steps before updating the tableJSAMap
			(outIter - 1)->get()->inputAssociation(tableJSAMap[tableKey(
				jobInfo, tableOid, inIter->get()->alias(), inIter->get()->view())]);
		}

		//If this step already has an output association (probably because it's feeding a hashjoin
		// step, use it.
		if (outIter->get()->outputAssociation().outSize() > 0)
		{
			outJs = outIter->get()->outputAssociation();
		}
		outIter->get()->outputAssociation(outJs);
		if (tableOid > 0)
		{
			tableJSAMap[tableKey(
				jobInfo, tableOid, inIter->get()->alias(), inIter->get()->view())] = outJs;
		}

		//Need to fudge this a little...
		if (twoStep)
		{
			//We need to add the output DL from inIter-1 as an input dl to outIter
			//TODO: needs to search backwards for the right table also...
			JobStepAssociation tJs = (inIter - 1)->get()->outputAssociation();
			AnyDataListSPtr tAdl = tJs.outAt(tJs.outSize() - 1);
			tJs = outIter->get()->inputAssociation();
			tJs.outAdd(tAdl);
			outIter->get()->inputAssociation(tJs);
		}

		//save the current, last step for this table
		if (tableOid > 0)
		{
			uint t = tableKey(jobInfo, tableOid, inIter->get()->alias(), inIter->get()->view());
			tableStepMap[t] = outIter;
			lastStepMap[t] = *outIter;
		}

		if (inIter != outIter) ++iter;
	}

	//do the project steps associations...
	// These all get the same input DL, inherited from the loop above,
	//and all the output DLs are muxed into a single Delivery Step
	iter = projectSteps.begin();
	end = projectSteps.end();
	//Need to keep a map of the current deliveryJs for each table
	map<CalpontSystemCatalog::OID, JobStepAssociation> deliveryJSMap;
	//Set to prevent duplicate columns in delivery step
	set< CalpontSystemCatalog::OID > columnSet;
	for (; iter != end; ++iter)
	{
		CalpontSystemCatalog::OID tableOid = iter->get()->tableOid();
		if (typeid(*(iter->get())) != typeid(pDictionaryStep))
		{
			iter->get()->inputAssociation(
				tableJSAMap[tableKey(jobInfo,tableOid,iter->get()->alias(),iter->get()->view())]);
		}

		//If this step already has an output association, we've (probably) got a pColStep-pDictionaryStep
		// linked step, and we don't want to break that link...
		if (iter->get()->outputAssociation().outSize() > 0)
		{
			// Bug 864
			// Reduce the fifo size of pColStep-pDictionary pair to break the deadlock
			// with PassThruStep when feeded by a ZDL.
			FifoDataList* fifo = iter->get()->outputAssociation().outAt(0)->fifoDL();
			if (fifo != NULL)
			{
				// not just /2, in case jobInfo.fifoSize is 1
				uint64_t fifoSize = fifo->maxElements() - fifo->maxElements()/2;
				fifo->maxElements(fifoSize);
			}

			continue;
		}

		AnyDataListSPtr spdl1(new AnyDataList());
		CalpontSystemCatalog::OID colOid = 0;
		if (typeid(*(iter->get())) == typeid(pDictionaryStep))
		{
			// Bug 864
			uint64_t fifoSize = jobInfo.fifoSize - jobInfo.fifoSize/2;
			StringFifoDataList* dl1 = new StringFifoDataList(1, fifoSize);
			dl1->OID(iter->get()->oid());
			spdl1->stringDL(dl1);
			colOid = dl1->OID();
		}
		else
		{
			FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
			dl1->OID(iter->get()->oid());
			spdl1->fifoDL(dl1);
			colOid = dl1->OID();
		}

		JobStepAssociation outProjJs(jobInfo.status);
		outProjJs.outAdd(spdl1);

		iter->get()->outputAssociation(outProjJs);

		assert(tableOid > 0);

		//Don't add duplicate column to delivery step.
		if (columnSet.end() == columnSet.find(colOid))
		{
			JobStepAssociation dsja = deliveryJSMap[tableOid];
			dsja.outAdd(spdl1);
			deliveryJSMap[tableOid] = dsja;
			columnSet.insert(colOid);
		}
	}

  	checkProjectStepsInput(projectSteps, querySteps, numProjectSteps, lastStepMap, jobInfo);

	//@bug 845
	checkBucketReuse(querySteps, jobInfo);

	uint16_t stepNo = numberSteps(querySteps, 0, jobInfo.traceFlags);
	stepNo = numberSteps(projectSteps, stepNo, jobInfo.traceFlags);

	//do the delivery step associations; also add stepNo and debug tracing
	DeliveredTableMap::iterator dsi = deliverySteps.begin();
	DeliveredTableMap::iterator enddsi = deliverySteps.end();
	while (dsi != enddsi)
	{
		DeliveryStep* ds = dynamic_cast<DeliveryStep*>(dsi->second.get());
		assert(ds);
		assert(ds->tableOid() > 0);
		ds->inputAssociation(deliveryJSMap[ds->tableOid()]);
		ds->stepId(stepNo);
		ds->setTraceFlags(jobInfo.traceFlags);
		stepNo++;
		++dsi;
	}

	// @bug 844. remove unnecessary pcolstep after aggregatefilter step
	aggregateOptimize (querySteps);

	//@bug 743 - Add support for 32 bit RID and/or values
	datalistSaveSize(querySteps, jobInfo);

	// @bug 653. recheck number of consumers
	fixNumberConsumers(querySteps, projectSteps);

	checkDictFilters(querySteps, projectSteps, jobInfo);

	fixNumberConsumers(querySteps, projectSteps);

	checkCardinality(querySteps);

	// @bug 1145. convert pcolstep to passthru when no more changes to the step order.
	convertPColStepInProjectToPassThru(projectSteps, jobInfo);

	// @bug 791. ZDL and Bucket datalist are default rids-only. This routine is to
	// convert them to rids-value datalist when necessary.
	checkDataList(querySteps, projectSteps);

	if (jobInfo.trace)
	{
		ofstream dotFile("jobstep_orig.dot");
		jlf_graphics::writeDotCmds(dotFile, querySteps, projectSteps);
	}

	//Reduce job steps if possible
	bool combineOkay = true;
	if (combineJobSteps(querySteps, projectSteps, deliverySteps, jobInfo) != 0)
		combineOkay = false;

	//@bug 917
	if (jobInfo.traceFlags & CalpontSelectExecutionPlan::TRACE_DISKIO_UM)
		enableDiskIoLog(querySteps);

	//extra work for HashJoinStep steps to support PM and in-memory UM joins
	fixNumberConsumers(querySteps, projectSteps);
	prepHashJoinSteps(querySteps, projectSteps, jobInfo);
}


// v-table mode
void makeVtableModeSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	// support order by and limit in sub-query
	if (csep->orderByCols().size() > 0 || csep->limitNum() != (uint64_t) -1)
		addOrderByAndLimit(csep, jobInfo);

	// Bug 2123.  Added overrideLargeSideEstimate parm below.  True if the query was written
	// with a hint telling us to skip the estimatation process for determining the large side
	// table and instead use the table order in the from clause.
	associateTupleJobSteps(querySteps, projectSteps, deliverySteps,
								jobInfo, csep->overrideLargeSideEstimate());
	fixNumberConsumers(querySteps, projectSteps);
	uint16_t stepNo = jobInfo.subId * 10000;
	numberSteps(querySteps, stepNo, jobInfo.traceFlags);
//	SJSTEP ds = deliverySteps.begin()->second;
	assert(deliverySteps.begin()->second.get());
//	ds->stepId(stepNo);
//	ds->setTraceFlags(jobInfo.traceFlags);
}

}

namespace joblist
{

void makeJobSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps)
{
	// v-table mode, switch to tuple methods and return the tuple joblist.
	if (jobInfo.tryTuples)
	{
		//@Bug 1958 Build table list only for tryTuples.
		const CalpontSelectExecutionPlan::SelectList& fromSubquery = csep->derivedTableList();
		int i = 0;
		for (CalpontSelectExecutionPlan::TableList::const_iterator it = csep->tableList().begin();
			it != csep->tableList().end();
			it++)
		{
			CalpontSystemCatalog::OID oid;
			if (it->schema.size() > 0)
				oid = jobInfo.csc->tableRID(*it).objnum;
			else
				oid = doFromSubquery(fromSubquery[i++].get(), it->alias, it->view, jobInfo);

			uint tableUid = tableKey(jobInfo, oid, it->schema, it->table, it->alias, it->view);
			jobInfo.tableList.push_back(tableUid);
		}

		// add select suqueries
		preprocessSelectSubquery(csep, jobInfo);

		// semi-join may appear in having clause
		if (csep->having() != NULL)
			preprocessHavingClause(csep, jobInfo);

		// parse plan and make jobstep list
		parseExecutionPlan(csep, jobInfo, querySteps, projectSteps, deliverySteps);
		makeVtableModeSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
	}
	else
	{
		parseExecutionPlan(csep, jobInfo, querySteps, projectSteps, deliverySteps);
		makeTableModeSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
	}
}


void makeUnionJobSteps(CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
	JobStepVector& querySteps, JobStepVector&, DeliveredTableMap& deliverySteps)
{
	CalpontSelectExecutionPlan::SelectList& selectVec = csep->unionVec();
	uint8_t distinctUnionNum = csep->distinctUnionNum();
	RetColsVector unionRetCols = csep->returnedCols();
	JobStepVector unionFeeders;
	CalpontSelectExecutionPlan* ep = NULL;
	for (CalpontSelectExecutionPlan::SelectList::iterator cit = selectVec.begin();
		 cit != selectVec.end();
		 cit++)
	{
		JobStepVector qSteps;
		JobStepVector pSteps;
		DeliveredTableMap dSteps;
		JobInfo queryJobInfo = jobInfo;
		ep = dynamic_cast<CalpontSelectExecutionPlan*>((*cit).get());
		makeJobSteps(ep, queryJobInfo, qSteps, pSteps, dSteps);
		querySteps.insert(querySteps.end(), qSteps.begin(), qSteps.end());
		unionFeeders.push_back(dSteps[execplan::CNX_VTABLE_ID]);
	}

	jobInfo.deliveredCols = unionRetCols;
	SJSTEP unionStep(unionQueries(unionFeeders, distinctUnionNum, jobInfo));
	querySteps.push_back(unionStep);
	uint16_t stepNo = jobInfo.subId * 10000;
	numberSteps(querySteps, stepNo, jobInfo.traceFlags);
	deliverySteps[execplan::CNX_VTABLE_ID] = unionStep;
}

}

namespace
{

SJLP makeJobList_(
	CalpontExecutionPlan* cplan,
	ResourceManager& rm,
	bool isExeMgr,
	bool tryTuples,
	unsigned& errCode, string& emsg)
{
	CalpontSelectExecutionPlan* csep = dynamic_cast<CalpontSelectExecutionPlan*>(cplan);
	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(csep->sessionID());

	// We have to go ahead and create JobList now so we can store the joblist's
	// projectTableOID pointer in JobInfo for use during jobstep creation.
	SErrorInfo status(new ErrorInfo());
	shared_ptr<TupleKeyInfo> keyInfo(new TupleKeyInfo);
	shared_ptr<int> subCount(new int);
	*subCount = 0;
	JobList* jl;
	if (!tryTuples)
		jl = new JobList(isExeMgr);
	else
		jl = new TupleJobList(isExeMgr);
	jl->statusPtr(status);
	rm.setTraceFlags(csep->traceFlags());

	//Stuff a util struct with some stuff we always need
	JobInfo jobInfo(rm);
	jobInfo.sessionId = csep->sessionID();
	jobInfo.txnId = csep->txnID();
	jobInfo.verId = csep->verID();
	jobInfo.statementId = csep->statementID();
	jobInfo.csc = csc;
	//TODO: clean up the vestiges of the bool trace
	jobInfo.trace = csep->traceOn();
	jobInfo.traceFlags = csep->traceFlags();
	jobInfo.isExeMgr = isExeMgr;
	jobInfo.tryTuples = tryTuples;
	jobInfo.stringScanThreshold = csep->stringScanThreshold();
	jobInfo.status = status;
	jobInfo.keyInfo = keyInfo;
	jobInfo.subCount = subCount;
	jobInfo.projectingTableOID = jl->projectingTableOIDPtr();
	jobInfo.jobListPtr = jl;

	// set fifoSize to 1 for CalpontSystemCatalog query
	if (csep->sessionID() & 0x80000000)
		jobInfo.fifoSize = 1;
	else if (csep->traceOn())
		cout << csep << endl;

	try
	{
		JobStepVector querySteps;
		JobStepVector projectSteps;
		DeliveredTableMap deliverySteps;

		if (csep->unionVec().size() == 0)
		{
			makeJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
		}
		else
		{
			if (!tryTuples)
				throw std::runtime_error("Query involves not supported feature in table-mode.");

			makeUnionJobSteps(csep, jobInfo, querySteps, projectSteps, deliverySteps);
		}

		fixNumberConsumers(querySteps, projectSteps);
		uint16_t stepNo = numberSteps(querySteps, 0, jobInfo.traceFlags);
		stepNo = numberSteps(projectSteps, stepNo, jobInfo.traceFlags);

		struct timeval stTime;
		if (jobInfo.trace)
		{
			ostringstream oss;
			oss << endl;
			oss << endl << "job parms: " << endl;
			oss << "maxBuckets = " << jobInfo.maxBuckets << ", maxElems = " << jobInfo.maxElems <<
			", flushInterval = " << jobInfo.flushInterval <<
				", fifoSize = " << jobInfo.fifoSize <<
				", ScanLimit/Threshold = " << jobInfo.scanLbidReqLimit << "/" <<
				jobInfo.scanLbidReqThreshold << endl;
			oss << endl << "job filter steps: " << endl;
			ostream_iterator<JobStepVector::value_type> oIter(oss, "\n");
			copy(querySteps.begin(), querySteps.end(), oIter);
			oss << endl << "job project steps: " << endl;
			copy(projectSteps.begin(), projectSteps.end(), oIter);
			oss << endl << "job delivery steps: " << endl;
			DeliveredTableMap::iterator dsi = deliverySteps.begin();
			while (dsi != deliverySteps.end())
			{
				oss << dynamic_cast<const JobStep*>(dsi->second.get()) << endl;
				++dsi;
			}

			oss << endl;
			gettimeofday(&stTime, 0);

			ostringstream ossfn;
			struct tm tmbuf;
#ifdef _MSC_VER
			errno_t p = 0;
			time_t t = stTime.tv_sec;
			p = localtime_s(&tmbuf, &t);
			if (p != 0)
				memset(&tmbuf, 0, sizeof(tmbuf));
#else
			localtime_r(&stTime.tv_sec, &tmbuf);
#endif
			char timestamp[80];
			sprintf(timestamp, "%04d%02d%02d%02d%02d%02d%06ld", tmbuf.tm_year+1900, tmbuf.tm_mon+1, tmbuf.tm_mday,
				tmbuf.tm_hour, tmbuf.tm_min, tmbuf.tm_sec, stTime.tv_usec);
			ossfn << "jobstep." << timestamp << ".dot";
			string jsrname(ossfn.str());
			ofstream dotFile(jsrname.c_str());

			jlf_graphics::writeDotCmds(dotFile, querySteps, projectSteps);
#ifdef _MSC_VER
			t = stTime.tv_sec;
			p = ctime_s(timestamp, 80, &t);
			if (p != 0)
				strcpy(timestamp, "UNKNOWN");
#else
			ctime_r((const time_t*)&stTime.tv_sec, timestamp);
#endif
			oss << "runtime updates: start at " << timestamp;
			cout << oss.str();
			Message::Args args;
			args.add(oss.str());
			jobInfo.logger->logMessage(LOG_TYPE_DEBUG, LogSQLTrace, args,
				LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));
			cout << flush;
		}
		else
		{
			gettimeofday(&stTime, 0);
		}

		// Finish initializing the JobList object
		jl->addQuery(querySteps);
		jl->addProject(projectSteps);
		jl->addDelivery(deliverySteps);

		if (tryTuples)
			dynamic_cast<TupleJobList*>(jl)->setDeliveryFlag(true);
	}
	catch (IDBExcept& iex)
	{
		jobInfo.status->errCode = iex.errorCode();
		errCode = iex.errorCode();
		exceptionHandler(jl, jobInfo, iex.what(), LOG_TYPE_DEBUG);
		emsg = iex.what();
		if (tryTuples) goto bailout;
	}
	catch (QueryDataExcept& uee)
	{
		jobInfo.status->errCode = uee.errorCode();
		errCode = uee.errorCode();
		exceptionHandler(jl, jobInfo, uee.what(), LOG_TYPE_DEBUG);
		emsg = uee.what();
		if (tryTuples) goto bailout;
	}
	catch (const std::exception& ex)
	{
		jobInfo.status->errCode = makeJobListErr;
		errCode = makeJobListErr;
		exceptionHandler(jl, jobInfo, ex.what());
		emsg = ex.what();
		if (tryTuples) goto bailout;
	}
	catch (...)
	{
		jobInfo.status->errCode = makeJobListErr;
		errCode = makeJobListErr;
		exceptionHandler(jl, jobInfo, "an exception");
		emsg = "An unknown internal joblist error";
		if (tryTuples) goto bailout;
	}

	goto done;

bailout:
	delete jl;
	jl = 0;
	if (emsg.empty())
		emsg = "An unknown internal joblist error";

done:
	SJLP jlp(jl);
	return jlp;
}

}

namespace joblist
{

/* static */
SJLP JobListFactory::makeJobList(
	CalpontExecutionPlan* cplan,
	ResourceManager& rm,
	bool tryTuple,
	bool isExeMgr)
{
	SJLP ret;
	string emsg;
	unsigned errCode = 0;
	ret = makeJobList_(cplan, rm, isExeMgr, tryTuple, errCode, emsg);

	if (!ret)
	{
		ret.reset(new JobList(isExeMgr));
		SErrorInfo status(new ErrorInfo);
		status->errCode = errCode;
		status->errMsg  = emsg;
		ret->statusPtr(status);
	}

	return ret;
}

}
// vim:ts=4 sw=4:

