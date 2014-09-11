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

// $Id: jlf_graphics.cpp 7396 2011-02-03 17:54:36Z rdempsey $

#include <iostream>
using namespace std;

#include "joblist.h"
#include "hashjoin.h"
#include "subquerystep.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
using namespace joblist;

#include "jlf_graphics.h"

namespace jlf_graphics
{

ostream& writeDotCmds(ostream& dotFile, const JobStepVector& query, const JobStepVector& project)
{
	// Graphic view draw
	dotFile << "digraph G {" << endl;
	JobStepVector::iterator qsi;
	JobStepVector::iterator psi;
	int ctn = 0;

	// merge in the subquery steps
	JobStepVector querySteps = query;
	JobStepVector projectSteps = project;
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
		if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
			continue;

		uint16_t stepidIn = qsi->get()->stepId();
		dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";
		if (typeid(*(qsi->get())) == typeid(pColStep))
		{
			dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")" << "\"";
			dotFile << " shape=ellipse";
		}
		else if (typeid(*(qsi->get())) == typeid(pColScanStep))
		{
			dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")"<< "\"";
			dotFile << " shape=box";
		}
		else if (typeid(*(qsi->get())) == typeid(HashJoinStep) ||
				 typeid(*(qsi->get())) == typeid(StringHashJoinStep))
		{
			dotFile << "\"";
			dotFile << " shape=diamond";
		}
		else if (typeid(*(qsi->get())) == typeid(TupleHashJoinStep))
		{
			dotFile << "\"";
			dotFile << " shape=diamond peripheries=2";
		}
		else if (typeid(*(qsi->get())) == typeid(UnionStep) ||
				 typeid(*(qsi->get())) == typeid(TupleUnion) )
		{
			dotFile << "\"";
			dotFile << " shape=triangle";
		}
		else if (typeid(*(qsi->get())) == typeid(pDictionaryStep))
		{
			dotFile << "\"";
			dotFile << " shape=trapezium";
		}
		else if (typeid(*(qsi->get())) == typeid(FilterStep))
		{
			dotFile << "\"";
			dotFile << " shape=house orientation=180";
		}
		else if (typeid(*(qsi->get())) == typeid(ReduceStep))
		{
			dotFile << "\"";
			dotFile << " shape=triangle orientation=180";
		}
		else if (typeid(*(qsi->get())) == typeid(BatchPrimitiveStep) || typeid(*(qsi->get())) == typeid(TupleBPS))
		{
			bool isTuple = false;
			BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
			if (dynamic_cast<TupleBPS*>(bps) != 0)
				isTuple = true;
			dotFile << "(" << bps->tableOid() << "/" << bps->oid() ;
			OIDVector projectOids = bps->getProjectOids();
			if ( projectOids.size() > 0 )
			{
				dotFile << "\\l";
				dotFile << "PC: ";
			}
			for ( unsigned int i = 0; i < projectOids.size(); i++ )
			{
				dotFile << projectOids[i] << " ";
				if ( (i+1) % 3 == 0 )
					dotFile << "\\l";
			}
			dotFile << ")\"";
			dotFile << " shape=box style=bold";
			if (isTuple)
				dotFile << " peripheries=2";
		}
		else if (typeid(*(qsi->get())) == typeid(AggregateFilterStep))
		{
			dotFile << "\"";
			dotFile << " shape=hexagon peripheries=2 style=bold";
		}
		else if (typeid(*(qsi->get())) == typeid(BucketReuseStep))
		{
			dotFile << "(" << qsi->get()->tableOid() << "/" << qsi->get()->oid() << ")" << "\"";
			dotFile << " shape=box style=dashed";
		}
		else
			dotFile << "\"";
		dotFile << "]" << endl;
		for (unsigned int i = 0; i < qsi->get()->outputAssociation().outSize(); i++)
		{
			ptrdiff_t dloutptr;
			DataList_t* dlout = qsi->get()->outputAssociation().outAt(i)->dataList();
			uint numConsumers = qsi->get()->outputAssociation().outAt(i)->getNumConsumers();

			if (dlout)
			{
				dloutptr = (ptrdiff_t)dlout;
			}
			else
			{
				StrDataList* sdl = qsi->get()->outputAssociation().outAt(i)->stringDataList();
				dloutptr = (ptrdiff_t)sdl;
			}

			for (unsigned int k = 0; k < querySteps.size(); k++)
			{
				uint16_t stepidOut = querySteps[k].get()->stepId();
				JobStepAssociation queryInputSA = querySteps[k].get()->inputAssociation();
				for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
				{
					ptrdiff_t dlinptr;
					DataList_t *dlin = queryInputSA.outAt(j)->dataList();
					StrDataList* sdl = 0;
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
					{
						sdl = queryInputSA.outAt(j)->stringDataList();
						dlinptr = (ptrdiff_t)sdl;
					}

					if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
					{
						dotFile << stepidIn << " -> " << stepidOut;
						if (dlin)
						{
							dotFile << " [label=\"[" << AnyDataList::dlType(dlin)
									<< "/" << numConsumers << "]\"]" << endl;
						}
						else
						{
							dotFile << " [label=\"[" << AnyDataList::strDlType(sdl)
									<< "/" << numConsumers << "]\"]" << endl;
						}
					}
				}
			}
			for (psi = projectSteps.begin(); psi < projectSteps.end(); psi++)
			{
				uint16_t stepidOut = psi->get()->stepId();
				JobStepAssociation projectInputSA = psi->get()->inputAssociation();
				for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
				{
					ptrdiff_t dlinptr;
					DataList_t *dlin = projectInputSA.outAt(j)->dataList();
					StrDataList* sdl = 0;
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
					{
						sdl = projectInputSA.outAt(j)->stringDataList();
						dlinptr = (ptrdiff_t)sdl;
					}
					if (dloutptr == dlinptr)
					//if ((ptrdiff_t)dlout == (ptrdiff_t)dlin)
					{
						dotFile << stepidIn << " -> " << stepidOut;
						if (dlin)
						{
							dotFile << " [label=\"[" << AnyDataList::dlType(dlin)
									<< "/" << numConsumers << "]\"]" << endl;
						}
						else
						{
							dotFile << " [label=\"[" << AnyDataList::strDlType(sdl)
									<< "/" << numConsumers << "]\"]" << endl;
						}
					}
				}
			}
		}
	}

	for (psi = projectSteps.begin(), ctn = 0; psi != projectSteps.end(); ctn++, psi++)
	{
		uint16_t stepidIn = psi->get()->stepId();
		dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";
		if (typeid(*(psi->get())) == typeid(pColStep))
		{
			dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")" << "\"";
			dotFile << " shape=ellipse";
		}
		else if (typeid(*(psi->get())) == typeid(pColScanStep))
		{
			dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")"<< "\"";
			dotFile << " shape=box";
		}
		else if (typeid(*(psi->get())) == typeid(pDictionaryStep))
		{
			dotFile << "\"";
			dotFile << " shape=trapezium";
		}
		else if (typeid(*(psi->get())) == typeid(PassThruStep))
		{
			dotFile << "(" << psi->get()->tableOid() << "/" << psi->get()->oid() << ")"<< "\"";
			dotFile << " shape=octagon";
		}
		else if (typeid(*(psi->get())) == typeid(BatchPrimitiveStep) || typeid(*(psi->get())) == typeid(TupleBPS))
		{
			bool isTuple = false;
			BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(psi->get());
			if (dynamic_cast<TupleBPS*>(bps) != 0)
				isTuple = true;
			dotFile << "(" << bps->tableOid() << ":\\l";
			OIDVector projectOids = bps->getProjectOids();

			for ( unsigned int i = 0; i < projectOids.size(); i++ )
			{
				dotFile << projectOids[i] << " ";
				if ( (i+1) % 3 == 0 )
					dotFile << "\\l";
			}
			dotFile << ")\"" ;
			dotFile << " shape=box style=bold";
			if (isTuple)
				dotFile << " peripheries=2";
		}
		else
			dotFile << "\"";
		dotFile << "]" << endl;
		for (unsigned int i = 0; i < psi->get()->outputAssociation().outSize(); i++)
		{
			ptrdiff_t dloutptr;
			DataList_t* dlout = psi->get()->outputAssociation().outAt(i)->dataList();
			uint numConsumers = psi->get()->outputAssociation().outAt(i)->getNumConsumers();

			if (dlout)
			{
				dloutptr = (ptrdiff_t)dlout;
			}
			else
			{
				StrDataList* sdl = psi->get()->outputAssociation().outAt(i)->stringDataList();
				dloutptr = (ptrdiff_t)sdl;
			}
			for (unsigned int k = ctn + 1; k < projectSteps.size(); k++)
			{
				uint16_t stepidOut = projectSteps[k].get()->stepId();
				JobStepAssociation projectInputSA = projectSteps[k].get()->inputAssociation();

				for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
				{
					ptrdiff_t dlinptr;
					DataList_t *dlin = projectInputSA.outAt(j)->dataList();
					StrDataList* sdl = 0;
					if (dlin)
						dlinptr = (ptrdiff_t)dlin;
					else
					{
						sdl = projectInputSA.outAt(j)->stringDataList();
						dlinptr = (ptrdiff_t)sdl;
					}
					if ((ptrdiff_t)dloutptr == (ptrdiff_t)dlinptr)
					{
						dotFile << stepidIn << " -> " << stepidOut;
						if (dlin)
						{
							dotFile << " [label=\"[" << AnyDataList::dlType(dlin)
									<< "/" << numConsumers << "]\"]" << endl;
						}
						else
						{
							dotFile << " [label=\"[" << AnyDataList::strDlType(sdl)
									<< "/" << numConsumers << "]\"]" << endl;
						}
					}
				}
			}
		}
	}

	dotFile << "}" << endl;

	return dotFile;

}

} // end namespace jlf_graphics

// vim:ts=4 sw=4 syntax=cpp:

