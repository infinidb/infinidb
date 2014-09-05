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

/*
* $Id: we_dbrootextenttracker.cpp 3672 2012-03-26 12:31:27Z rdempsey $
*/

#include <unistd.h>
#include <algorithm>
#include <sstream>
#include <boost/scoped_ptr.hpp>

#include "dbrm.h"
#include "batchloader.h"

using namespace std;
using namespace boost;
using namespace BRM;
using namespace oam;
using namespace execplan;

namespace batchloader
{

//------------------------------------------------------------------------------
// ChooseStartLoadPM constructor
//
// Mutex lock not needed in this function as it is only called from main thread
//------------------------------------------------------------------------------
BatchLoader::BatchLoader ( uint32_t tableOid,
		execplan::CalpontSystemCatalog::SCN sessionId,
		std::vector<uint>& PMs )
{
	fFirstPm=0;
	fNextIdx=0;
    fPMs = PMs;
	fSessionId = sessionId;
	fTableOid = tableOid;
	OamCache * oamcache = OamCache::makeOamCache();
	oam::OamCache::PMDbrootsMap_t systemPmDbrootMap = oamcache->getPMToDbrootsMap();
	std::map<int, OamCache::dbRoots>::iterator iter = systemPmDbrootMap->begin();
	//cout << "fPMs size is " << fPMs.size() << endl;
	fPmDbrootMap.reset(new OamCache::PMDbrootsMap_t::element_type());
	fDbrootPMmap.reset(new map<int, int>());
	for (uint i=0; i < fPMs.size(); i++)
	{
		iter = systemPmDbrootMap->find(fPMs[i]);
		if (iter != systemPmDbrootMap->end())
		{
			fDbRoots.insert(fDbRoots.end(), (iter->second).begin(), (iter->second).end());
			(*fPmDbrootMap)[fPMs[i]] = iter->second;
		}
	}
	//Build dbroot to PM map
	for (iter = fPmDbrootMap->begin(); iter != fPmDbrootMap->end(); iter++)
	{
		for ( uint i = 0; i < iter->second.size(); i++)
		{
			(*fDbrootPMmap)[iter->second[i]] = iter->first;
		}
	}
}
//------------------------------------------------------------------------------
// Select the first PM to send the first batch of rows.
/*Look up extent map to decide which dbroot to start
	1. If newly created table, starts from the pm where the abbreviated extent created.
	2. If the abbreviated extent has hwm > 0, and other PMs don't have any extent, start from next PM on the list.
	3. If all PMs have extents, count the number of extents under each dbroot, find the dbroot which has the least extents to start
	4. If all dbroots have same number of extents, starts from the dbroot which has least number of blocks on the last partition.
*/
//------------------------------------------------------------------------------
void BatchLoader::selectFirstPM ( uint& PMId)
{
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionId);
	//cout << "calling tableName for oid " << fTableOid << endl;
	CalpontSystemCatalog::TableName aTableName = systemCatalogPtr->tableName(fTableOid);
	
	CalpontSystemCatalog::RIDList ridList;
	ridList = systemCatalogPtr->columnRIDs(aTableName, true); //exception will be handled in caller program.
		
	scoped_ptr<DBRM> dbrmp(new DBRM());	
	
	//Build distVec, start from the PM where the table is created. If not in the PM list, 0 will be used.
	uint16_t createdDbroot = 0;
	int rc = 0;
	std::vector<BRM::EmDbRootHWMInfo_v> allInfo (fPMs.size());
	for (unsigned i = 0; i < fPMs.size(); i++)
	{
		rc = dbrmp->getDbRootHWMInfo((ridList[0].objnum), fPMs[i], allInfo[i]);
		if ( rc !=0 ) //@Bug 4760
			break;
	}
	
	if ( rc != 0 ) {
		ostringstream oss;
		oss << "There is no extent information for table " << aTableName.table;
		throw std::runtime_error(oss.str());
	
	}
		
	std::vector<BRM::EMEntry>	entries;
	uint numDbroot = fDbRoots.size();
    if (numDbroot == 0)
    {
		throw std::runtime_error("There are no dbroots found during selectFirstPM");
    }
	int* rootExtents = (int*)alloca((numDbroot + 1) * sizeof(int));	//array of number extents for each dbroot
	for (unsigned i = 0; i < fDbRoots.size(); i++)
	{
		dbrmp->getExtents_dbroot((ridList[0].objnum), entries, fDbRoots[i]);
		rootExtents[fDbRoots[i]] = entries.size();
	}
		
	bool startDBRootSet = false;
	int* rootBlocks = (int*)alloca((numDbroot + 1) * sizeof(int));	//array of number of blocks for the last partition for each dbroot
	//cout << "allInfo size is " << allInfo.size() << endl;
	for (unsigned i=0; i < allInfo.size(); i++) //All PMs
	{
		BRM::EmDbRootHWMInfo_v emDbRootHWMInfos = allInfo[i]; //one pm
				
		for (unsigned j=0; j < emDbRootHWMInfos.size(); j++)
		{	
			if (emDbRootHWMInfos[j].totalBlocks == 0) {
				//cout << "totalBlocks is 0" << endl;
				continue; }
				
			if ((emDbRootHWMInfos[j].partitionNum == 0) && (emDbRootHWMInfos[j].segmentNum ==0))
			{
				if (emDbRootHWMInfos[j].localHWM == 0)
				{
					//newly created table
					//cout << " This is newly created table. PM id is " << PMId;
					startDBRootSet = true;
					createdDbroot = emDbRootHWMInfos[j].dbRoot;
					break;
				}
				else
				{
					createdDbroot = emDbRootHWMInfos[j].dbRoot;
					//cout << " and createdDbroot is " << createdDbroot << endl;
					rootBlocks[emDbRootHWMInfos[j].dbRoot] = emDbRootHWMInfos[j].totalBlocks;
				}
			}
			else
			{					
				rootBlocks[emDbRootHWMInfos[j].dbRoot] = emDbRootHWMInfos[j].totalBlocks;	
			}
		}
		if (startDBRootSet)
			break;
	}
	
	PMId = 0;
	if ( createdDbroot != 0)
	{
		std::map<int, int>::iterator iter = fDbrootPMmap->begin();
	
		iter = fDbrootPMmap->find(createdDbroot);
		if (iter != fDbrootPMmap->end())
			PMId = iter->second;
	}
	// This will build the batch distribution sequence	
	//cout << "Building BatchDistSeqVector with PMId " << PMId << endl;
	buildBatchDistSeqVector(PMId);	

	//cout << "startDBRootSet = " << startDBRootSet << endl;	
	bool allEqual = true;
	bool allOtherDbrootEmpty = true;
	if (!startDBRootSet)
	{
	
		std::vector<PMRootInfo> rootsExtentsBlocks;
		std::map<int, OamCache::dbRoots>::iterator iter;
		for (unsigned j=0; j < fPmDistSeq.size(); j++)
		{
			PMRootInfo aEntry;
			aEntry.PMId = fPmDistSeq[j];
			iter = fPmDbrootMap->find(aEntry.PMId);
			for (unsigned k=0; k < (iter->second).size(); k++)
			{
				RootExtentsBlocks aRootInfo;
				aRootInfo.DBRoot = (iter->second)[k];
				aRootInfo.numExtents = rootExtents[aRootInfo.DBRoot];
				aRootInfo.numBlocks = rootBlocks[aRootInfo.DBRoot];
				//cout << "aRootInfo DBRoot:numExtents:numBlocks = " << aRootInfo.DBRoot<<":"<<aRootInfo.numExtents<<":"<<aRootInfo.numBlocks<<endl;
				aEntry.rootInfo.push_back(aRootInfo);
			}			
			rootsExtentsBlocks.push_back(aEntry);		
		}
		//cout << "rootsExtentsBlocks size is " << rootsExtentsBlocks.size() << " and allOtherDbrootEmpty is " << allOtherDbrootEmpty<< endl;
		for (unsigned i=1; i < rootsExtentsBlocks.size(); i++)
		{
			if (!allOtherDbrootEmpty)
				break;
			//cout << "createdDbroot is " << createdDbroot << endl;
			if (i != createdDbroot)
			{
				for ( unsigned j=0; j < rootsExtentsBlocks[i].rootInfo.size(); j++)
				{
					if (rootsExtentsBlocks[i].rootInfo[j].numExtents != 0)
					{
						//cout << "setting allOtherDbrootEmpty to false and i:j = " << i<<":"<<j<<endl;
						//cout << "numExtents = " << rootsExtentsBlocks[i].rootInfo[j].numExtents << endl;
						allOtherDbrootEmpty = false;
						break;
					}
				}
			}
		}
		//cout << "allOtherDbrootEmpty is " << allOtherDbrootEmpty << endl;	
		if (allOtherDbrootEmpty)
		{
			//find the next PM id on the list
			startDBRootSet = true;
			buildBatchDistSeqVector();
			
			allEqual = false;	
		}
		else //find the dbroot which has the least extents to start
		{
			//cout << "finding least dbroot to start." << endl;
			uint tmpLeastExtents = rootsExtentsBlocks[0].rootInfo[0].numExtents;
			PMId = rootsExtentsBlocks[0].PMId; //@Bug 4809.
			for ( unsigned j=1; j < rootsExtentsBlocks[0].rootInfo.size(); j++)
			{
				if (tmpLeastExtents > rootsExtentsBlocks[0].rootInfo[j].numExtents) 
					tmpLeastExtents = rootsExtentsBlocks[0].rootInfo[j].numExtents;
			}
			
			for (unsigned i=1; i < rootsExtentsBlocks.size(); i++)
			{
				uint leastExtents = rootsExtentsBlocks[i].rootInfo[0].numExtents;
				for ( unsigned j=0; j < rootsExtentsBlocks[i].rootInfo.size(); j++)
				{
					if (leastExtents > rootsExtentsBlocks[i].rootInfo[j].numExtents)
						leastExtents = rootsExtentsBlocks[i].rootInfo[j].numExtents;
				}
				
				if (leastExtents < tmpLeastExtents)
				{
						tmpLeastExtents = leastExtents;
						PMId = rootsExtentsBlocks[i].PMId;
						allEqual = false;
				}
				else if (leastExtents > tmpLeastExtents)
					allEqual = false;
				
			}
			//cout << "allEqual is " << allEqual << endl;	
			if (allEqual) //Find the dbroot which has least number of blocks on the last partition.
			{
				//cout << "All PMs have equal # of least extents" << endl;
				uint tmpBloks = rootsExtentsBlocks[0].rootInfo[0].numBlocks;
				
				PMId = rootsExtentsBlocks[0].PMId;
				//cout << "tmpBloks:PMId = " << tmpBloks <<":"<<PMId<<endl;
				for ( unsigned j=1; j < rootsExtentsBlocks[0].rootInfo.size(); j++)
				{
					if (tmpBloks > rootsExtentsBlocks[0].rootInfo[j].numBlocks)
						tmpBloks = rootsExtentsBlocks[0].rootInfo[j].numBlocks;
				}
				
				for (unsigned i=1; i < rootsExtentsBlocks.size(); i++)
				{
					uint leastBlocks = rootsExtentsBlocks[i].rootInfo[0].numBlocks;
					//cout << "leastBlocks = " << leastBlocks << endl;
					for ( unsigned j=0; j < rootsExtentsBlocks[i].rootInfo.size(); j++)
					{
						if (leastBlocks > rootsExtentsBlocks[i].rootInfo[j].numBlocks)
							leastBlocks = rootsExtentsBlocks[i].rootInfo[j].numBlocks;
					}
				
					if (leastBlocks < tmpBloks)
					{
							tmpBloks = leastBlocks;
							//cout << "tmpBloks changed to " << tmpBloks << endl;
							PMId = rootsExtentsBlocks[i].PMId;
							//cout << "setting allEqual to false now" << endl;
							allEqual = false;						
					}			
				}
			}
		}
	}
	else
		allEqual = false;
	
	fFirstPm = PMId;
	
	if (!allOtherDbrootEmpty || (PMId == 0))	  
	{
		prepareForSecondPM();		
		//cout << "prepareForSecondPM is called. " << endl;
	}
	if ((allEqual && (PMId == 0)) || allOtherDbrootEmpty)
	{
		PMId = selectNextPM();	
		fFirstPm = PMId;
		//cout << "PMId is now " << PMId << endl;
	}
}

//------------------------------------------------------------------------------
/*
 * Build the sequence of distribution as a vector.
 */

//------------------------------------------------------------------------------
void BatchLoader::buildBatchDistSeqVector()
{
	fPmDistSeq.clear();
	BlIntVec aDbCntVec(fPMs.size());

	std::map<int, OamCache::dbRoots>::iterator iter = fPmDbrootMap->begin();
	for (uint i=0; i < fPMs.size(); i++)
	{
		iter = fPmDbrootMap->find(fPMs[i]);
		if ((iter != fPmDbrootMap->end()) && ((iter->second).begin()!=(iter->second).end()))
		{
			try
			{
				aDbCntVec[i] = (iter->second).size();
				//cout << "PM - "<<fPMs[i] << " Size = " << aDbCntVec[i] << endl;
			}
			catch(std::exception& exp)
			{
				throw runtime_error(exp.what());
			}
		}
		else
			aDbCntVec[i] = 0;
	}

	int aTotDbRoots = 0;
	for (uint i=0; i<aDbCntVec.size(); i++) aTotDbRoots+=aDbCntVec[i];

	int aIdx=0;
	while(aIdx < aTotDbRoots)
	{
		uint aMax=0;
		uint aPmId=0;
		uint aRefIdx=0;
		for (uint i=0; i<aDbCntVec.size(); i++)
		{
			if(aDbCntVec[i] > aMax)
			{
				aMax = aDbCntVec[i];
				aPmId = fPMs[i];
				aRefIdx = i;
			}
		}
		if(aMax>0)
		{
			fPmDistSeq.push_back(aPmId);
			aDbCntVec[aRefIdx]--;
		}
		aIdx++;
	}

}


//------------------------------------------------------------------------------
/*
 * Build the sequence of distribution as a vector.
 */

//------------------------------------------------------------------------------
void BatchLoader::buildBatchDistSeqVector(uint StartPm)
{
	fPmDistSeq.clear();
	BlIntVec aDbCntVec(fPMs.size());
	BlIntVec aPms;

	if((fPMs.size()==0)&&(StartPm!=0))
		throw runtime_error("ERROR : PM list empty while Start != 0");

	//for (uint i=0; i<fPMs.size(); i++)
	//	cout <<"fPM list  "<<i <<" = " << fPMs[i] << endl;
	//cout << "StartPm = "<< StartPm << endl;

	if (StartPm == 0)
		aPms = fPMs;
	else
	{
		aPms.push_back(StartPm);
		uint aLast = fPMs.back();
		uint aFirst = fPMs.front();
		// Add all the PMs with index more than "StartPm"
		for(uint i=0; i<fPMs.size(); i++)
		{
			if((fPMs[i]>StartPm)&&(fPMs[i]<=aLast)) aPms.push_back(fPMs[i]);
		}
		// Add all the PMs with index less than "StartPm"
		for(uint i=0; i<fPMs.size(); i++)
		{
			if((fPMs[i]<StartPm)&&(fPMs[i]>=aFirst)) aPms.push_back(fPMs[i]);
		}
	}


	std::map<int, OamCache::dbRoots>::iterator iter = fPmDbrootMap->begin();
	for (uint i=0; i < aPms.size(); i++)
	{
		iter = fPmDbrootMap->find(aPms[i]);
		if ((iter != fPmDbrootMap->end()) && ((iter->second).begin()!=(iter->second).end()))
		{
				aDbCntVec[i] = (iter->second).size();
				//cout << "PM - "<<aPms[i] << " Size = " << aDbCntVec[i] << endl;
		}
		else
			aDbCntVec[i] = 0;
	}

	int aTotDbRoots = 0;
	for (uint i=0; i<aDbCntVec.size(); i++) aTotDbRoots+=aDbCntVec[i];

	//cout << "DbCntVec Size = " << aDbCntVec.size() << " TotDbRoots = "<<aTotDbRoots << endl;

	int aIdx=0;
	while(aIdx < aTotDbRoots)
	{
		uint aMax=0;
		uint aPmId=0;
		uint aRefIdx=0;
		for (uint i=0; i<aDbCntVec.size(); i++)
		{
			if(aDbCntVec[i] > aMax)
			{
				aMax = aDbCntVec[i];
				aPmId = aPms[i];
				aRefIdx = i;
			}
		}
		if(aMax>0)
		{
			fPmDistSeq.push_back(aPmId);
			aDbCntVec[aRefIdx]--;
		}
		aIdx++;
	}

	//cout <<"PM Distribution vector size "<< fPmDistSeq.size() << endl;
	//for (uint i=0; i<fPmDistSeq.size(); i++)
	//	cout <<"PM Distribution vector "<<i <<" = " << fPmDistSeq[i] << endl;

}

//------------------------------------------------------------------------------
/*
 *  Return the sequence distribution vector.
 */

std::vector<uint> BatchLoader::getBatchDistributionVector()
{
	if(0==fPmDistSeq.size()) this->buildBatchDistSeqVector();
	return fPmDistSeq;
}

//------------------------------------------------------------------------------

uint BatchLoader::selectNextPM()
{
	if(0 == fPmDistSeq.size()) //Dist sequence not ready. First time the function is called
	{
		uint PMId = 0;
		//cout << "selectNextPM: size is 0. " << endl;
		selectFirstPM(PMId);
		return 	PMId;
	}
	
	if(fNextIdx >= fPmDistSeq.size()) fNextIdx=0;	//reset it
	return fPmDistSeq[fNextIdx++];
}

//------------------------------------------------------------------------------

void BatchLoader::reverseSequence()
{
	if(0 == fNextIdx) fNextIdx = fPmDistSeq.size()-1;
	else fNextIdx--;
}

//------------------------------------------------------------------------------

void BatchLoader::prepareForSecondPM()
{
	// We loop thru by calling the selectNextPM()
	// When we get the aSecPm == aFirstPM, we will break so that when
	// we call the same function again when we come back get the next PM
	if((fFirstPm != 0)&&(fPMs.size()>1))
	{
		unsigned int aSecPm = selectNextPM();
		while(fFirstPm!=aSecPm ) aSecPm = selectNextPM();
	}
}

} // end of namespace
