/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
* $Id: we_redistributecontrolthread.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*/

#include <iostream>
#include <set>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

#include "boost/scoped_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
using namespace boost;

#include "installdir.h"

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
#include "oamcache.h"
using namespace oam;

#include "dbrm.h"
using namespace BRM;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "we_messages.h"
#include "we_redistributedef.h"
#include "we_redistributecontrol.h"
#include "we_redistributecontrolthread.h"

namespace redistribute
{

// static variables
boost::mutex RedistributeControlThread::fActionMutex;
volatile bool RedistributeControlThread::fStopAction = false;
string RedistributeControlThread::fWesInUse;


void RedistributeControlThread::setStopAction(bool s)
{
	mutex::scoped_lock lock(fActionMutex);
	fStopAction = s;
}


RedistributeControlThread::RedistributeControlThread(uint32_t act) :
	fAction(act), fMaxDbroot(0), fEntryCount(0), fErrorCode(RED_EC_OK)
{
}


RedistributeControlThread::~RedistributeControlThread()
{
//	fWEClient->removeQueue(uniqueId);
}


void RedistributeControlThread::operator()()
{
	if (fAction == RED_CNTL_START)
		doRedistribute();
	else if (fAction == RED_CNTL_STOP)
		doStopAction();
}


void RedistributeControlThread::doRedistribute()
{
	if (setup() != 0)
		fErrorCode = RED_EC_CNTL_SETUP_FAIL;
	else if (makeRedistributePlan() != 0)
		fErrorCode = RED_EC_MAKEPLAN_FAIL;

	try
	{
		if (fErrorCode == RED_EC_OK && !fStopAction && fEntryCount > 0)
			executeRedistributePlan();
	}
	catch (const std::exception& ex)
	{
		fErrorMsg += ex.what();
		fErrorCode = RED_EC_EXECUTE_FAIL;
	}
	catch (...)
	{
		fErrorMsg += "Error when executing the plan.";
		fErrorCode = RED_EC_EXECUTE_FAIL;
	}

	uint32_t state = RED_STATE_FINISH;
	if (fErrorCode != RED_EC_OK)
		state = RED_STATE_FAILED;

	try
	{
		if (!fStopAction)
			fControl->updateState(state);
	}
	catch (const std::exception& ex)
	{
		fErrorMsg += ex.what();
		if (fErrorCode == RED_EC_OK)
			fErrorCode = RED_EC_UPDATE_STATE;
	}
	catch (...)
	{
		fErrorMsg += "Error when updating state.";
		if (fErrorCode == RED_EC_OK)
			fErrorCode = RED_EC_UPDATE_STATE;
	}

	if (fErrorMsg.empty())
		fControl->logMessage("finished @controlThread::doRedistribute");
	else
		fControl->logMessage(fErrorMsg + " @controlThread::doRedistribute");

	{
		mutex::scoped_lock lock(fActionMutex);
		fWesInUse.clear();
	}
}


int RedistributeControlThread::setup()
{
	int ret = 0;

	try
	{
//		fUniqueId = fDbrm.getUnique64();
//		fWEClient = WriteEngine::WEClients::instance(WriteEngine::WEClients::REDISTRIBUTE);
//		fWEClient->addQueue(uniqueId);
		fConfig = Config::makeConfig();
		fOamCache = oam::OamCache::makeOamCache();
		fControl = RedistributeControl::instance();
//		fOam.reset(new oam::Oam);
//		fDbrm.reset(new BRM::DBRM);

		vector<int>::iterator i = fControl->fSourceList.begin();
		for (; i != fControl->fSourceList.end(); i++)
		{
//			fSourceSet.insert(*i);
			fDbrootSet.insert(*i);
			if (*i > fMaxDbroot)
				fMaxDbroot = *i;
		}

//		vector<int>::iterator j = fControl->fDestinationList.begin();
//		for (; j != fControl->fDestinationList.end(); j++)
//		{
//			fTargetSet.insert(*j);
//			fDbrootSet.insert(*j);
//			if (*j > fMaxDbroot)
//				fMaxDbroot = *j;
//		}
	}
	catch (const std::exception& ex)
	{
		fErrorMsg += ex.what();
		ret = 1;
	}
	catch (...)
	{
		ret = 1;
	}

	return ret;
}


int RedistributeControlThread::makeRedistributePlan()
{
	int ret = 0;
	try
	{
		if (fControl->fPlanFilePtr != NULL)
		{
			// should not happen, just in case.
			fclose(fControl->fPlanFilePtr);
			fControl->fPlanFilePtr = NULL;
		}

		// get all user table oids
		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
		vector<pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> >
			tables = csc->getTables();
		vector<pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> >::iterator i;
		for (i = tables.begin(); i != tables.end(); i++)
		{
			// in case, action is cancelled.
			if (fStopAction)
				break;

			// column oids
			CalpontSystemCatalog::RIDList cols = csc->columnRIDs(i->second, true);
			typedef std::map<PartitionInfo, RedistributeExtentEntry>  PartitionExtentMap;
			PartitionExtentMap   partitionMap;
			vector<EMEntry> entries;

			// sample the first column
			int rc = fControl->fDbrm->getExtents(cols[0].objnum, entries, false, false, true);
			if (rc != 0 || entries.size() == 0)
			{
				ostringstream oss;
				oss << "Error in DBRM getExtents; oid:" << cols[0].objnum << "; returnCode: " << rc;
				throw runtime_error(oss.str());
			}

			for (vector<EMEntry>::iterator j = entries.begin(); j != entries.end(); j++)
			{
				RedistributeExtentEntry redEntry;
				redEntry.oid = cols[0].objnum;
				redEntry.dbroot = j->dbRoot;
				redEntry.partition = j->partitionNum;
				redEntry.segment = j->segmentNum;
				redEntry.lbid = j->range.start;
				redEntry.range = j->range.size * 1024;

				PartitionInfo partInfo(j->dbRoot, j->partitionNum);
				partitionMap.insert(make_pair(partInfo, redEntry));
			}

			// sort partitions by dbroot
			vector<vector<int> > dbPartVec(fMaxDbroot + 1);
			uint64_t totalPartitionCount = 0;
			int      maxPartitionId = 0;
			for (PartitionExtentMap::iterator j = partitionMap.begin();
					j != partitionMap.end(); j++)
			{
				int dbroot = j->first.dbroot;
				if (fDbrootSet.find(dbroot) != fDbrootSet.end())
				{
					// only dbroot in source and target list needs attention
					dbPartVec[dbroot].push_back(j->first.partition);

					if (j->first.partition > maxPartitionId)
						maxPartitionId = j->first.partition;

					totalPartitionCount++;
				}
			}

			// sort the partition
			for (vector<vector<int> >::iterator k = dbPartVec.begin(); k != dbPartVec.end(); k++)
				sort(k->begin(), k->end());

			// divide the dbroots into the source and target sets
			uint64_t average = totalPartitionCount / fDbrootSet.size();
			uint64_t remainder = totalPartitionCount % fDbrootSet.size();
			set<int> sourceDbroots;
			set<int> targetDbroots;
//			list<pair<size_t, int> > targetList;  // to be ordered by partition size
			for (set<int>::iterator j = fDbrootSet.begin(); j != fDbrootSet.end(); ++j)
			{
				if (dbPartVec[*j].size() > average)
				{
					// the last partition is not a candidate for redistribute.
					dbPartVec[*j].pop_back();
					sourceDbroots.insert(*j);
				}
				else if (dbPartVec[*j].size() <= average)
				{
					targetDbroots.insert(*j);
				}
			}

			// After redistribution, partition # is in [average, average+1].
			// When remainder > # of source,  some target will have (average+1) partitions.
			int64_t extra = ((int64_t) remainder) - ((int64_t) sourceDbroots.size());

			// loop through target dbroots
			set<int>::iterator k = sourceDbroots.begin();
			int  sourceCnt = sourceDbroots.size();
			for (set<int>::iterator j = targetDbroots.begin(); j != targetDbroots.end(); j++)
			{
				// check if this target will have average + 1 partitions.
				uint64_t e = 0;
				if (extra-- > 0)
					e = 1;

				// the partitions already on the target dbroot
				set<int> parts(dbPartVec[*j].begin(), dbPartVec[*j].end());
				if (parts.size() >= (average+e))
					continue; // no need to move any partition to this target

				// partitions to be moved to this target
				vector<PartitionInfo> planVec;

				// looking for source partitions start from partition1
				bool done = false; // if target got enough partitions
				int  loop = 0;     // avoid infinity loop, if possible
				while (!done && loop < maxPartitionId)
				{
					// maxPartitionId is the last partition of one of the dbroots, not a candidate.
					for (int p = loop++; p < maxPartitionId && !done; p++)
					{
						bool found = false;
						if (parts.find(p) == parts.end())
						{
							// try to find p in one of the source
							for (int x = 0; x < sourceCnt && !found; ++x)
							{
								vector<int>& v = dbPartVec[*k];
								if (v.size() >= average)
								{
									vector<int>::iterator y = find(v.begin(), v.end(), p);
									if ((y != v.end()) &&
										(v.size() > parts.size())) // @bug4840, tie-break.
									{
										parts.insert(p);
										planVec.push_back(PartitionInfo(*k, p));
										found = true;

										// update the source
										v.erase(y);
									}
								}

								if (++k == sourceDbroots.end())
									k = sourceDbroots.begin();
							} // for source

							if (parts.size() >= (average+e))
								done = true;
						} // !find p
					} // for p
				} // while loop

				// dump the plan for the target to file
				dumpPlanToFile(i->first, planVec, *j);

			} // for target
		} // for tables

	}
	catch (const std::exception& ex)
	{
		fErrorMsg += ex.what();
		ret = 2;
	}
	catch (...)
	{
		ret = 2;
	}

	return ret;
}


void RedistributeControlThread::dumpPlanToFile(uint64_t oid, vector<PartitionInfo>& vec, int target)
{
	// open the plan file, if not already opened, to write.
	if (fControl->fPlanFilePtr == NULL)
	{
		errno = 0;
		fControl->fPlanFilePtr = fopen(fControl->fPlanFilePath.c_str(), "w+");
		if (fControl->fPlanFilePtr == NULL)
		{
			int e = errno;
			ostringstream oss;
			oss << "Failed to open redistribute.plan: " << strerror(e) << " (" << e << ")";
			throw runtime_error(oss.str());
		}
	}

	size_t entryNum = vec.size();
	scoped_array<RedistributePlanEntry> entries(new RedistributePlanEntry[entryNum]);
	for (uint64_t i = 0; i < entryNum; ++i)
	{
		entries[i].table = oid;
		entries[i].source = vec[i].dbroot;
		entries[i].partition = vec[i].partition;
		entries[i].destination = target;
		entries[i].status = RED_TRANS_READY;
	}

	errno = 0;
	size_t n = fwrite(entries.get(), sizeof(RedistributePlanEntry), entryNum, fControl->fPlanFilePtr);
	if (n != entryNum)  // need retry
	{
		int e = errno;
		ostringstream oss;
		oss << "Failed to write into redistribute.plan: " << strerror(e) << " (" << e << ")";
		throw runtime_error(oss.str());
	}

	fEntryCount += entryNum;
}


int RedistributeControlThread::executeRedistributePlan()
{
	// update the info with total partitions to move
	fControl->setEntryCount(fEntryCount);

	// start from the first entry
	rewind(fControl->fPlanFilePtr);

	ByteStream bs;
	uint32_t entryId = 0;
	long entrySize = sizeof(RedistributePlanEntry);
	while (entryId++ < fEntryCount)
	{
		try
		{
// skip system status check in case no OAM
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
			// make sure system is in active state
			bool isActive = false;
			while (!isActive)
			{
				bool noExcept = true;
				SystemStatus systemstatus;
				try
				{
					fControl->fOam->getSystemStatus(systemstatus);
				}
				catch (const std::exception& ex)
				{
					fErrorMsg += ex.what();
					noExcept = false;
				}
				catch (...)
				{
					noExcept = false;
				}

				if (noExcept && ((isActive = (systemstatus.SystemOpState == oam::ACTIVE)) == false))
					sleep(1);;
			}
#endif

			if (fStopAction)
				return RED_EC_USER_STOP;


			RedistributePlanEntry entry;
			errno = 0;
			size_t n = fread(&entry, entrySize, 1, fControl->fPlanFilePtr);
			if (n != 1)
			{
				int e = errno;
				ostringstream oss;
				oss << "Failed to read from redistribute.plan: " << strerror(e) << " (" << e << ")";
				throw runtime_error(oss.str());
			}

			if (entry.status != (int) RED_TRANS_READY)
				continue;

			// send the job to source dbroot
			size_t headerSize = sizeof(RedistributeMsgHeader);
			size_t entrySize = sizeof(RedistributePlanEntry);
			RedistributeMsgHeader header(entry.destination,entry.source,entryId,RED_ACTN_REQUEST);
			if (connectToWes(header.source) == 0)
			{
				bs.restart();
				entry.starttime = time(NULL);
				bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
				bs.append((const ByteStream::byte*) &header, headerSize);
				bs.append((const ByteStream::byte*) &entry, entrySize);
				fMsgQueueClient->write(bs);

				SBS sbs = fMsgQueueClient->read();
				entry.status = RED_TRANS_FAILED;
				if (sbs->length() == 0)
				{
					ostringstream oss;
					oss << "Zero byte read, Network error.  entryID=" << entryId;
					fErrorMsg = oss.str();
				}
				else if (sbs->length() < (headerSize + entrySize + 1))
				{
					ostringstream oss;
					oss << "Short message, length=" << sbs->length() << ". entryID=" << entryId;
					fErrorMsg = oss.str();
				}
				else
				{
					ByteStream::byte wesMsgId;
					*sbs >> wesMsgId;
					// Need check header info
					//const RedistributeMsgHeader* h = (const RedistributeMsgHeader*) sbs->buf();
					sbs->advance(headerSize);
					const RedistributePlanEntry* e = (const RedistributePlanEntry*) sbs->buf();
					sbs->advance(entrySize);
					entry.status = e->status;
					entry.endtime = time(NULL);
//					if (entry.status == (int32_t) RED_TRANS_FAILED)
//						*sbs >> fErrorMsg;
				}

				// done with this connection, may consider to reuse.
				fMsgQueueClient.reset();
			}
			else
			{
				entry.status = RED_TRANS_FAILED;
				ostringstream oss;
				oss << "Connect to PM failed." << ". entryID=" << entryId;
				fErrorMsg += oss.str();
			}

			if (!fErrorMsg.empty())
				throw runtime_error(fErrorMsg);

			errno = 0;
			int rc = fseek(fControl->fPlanFilePtr, -((long)entrySize), SEEK_CUR);
			if (rc != 0)
			{
				int e = errno;
				ostringstream oss;
				oss << "fseek is failed: " << strerror(e) << " (" << e << "); entry id=" << entryId;
				throw runtime_error(oss.str());
			}

			errno = 0;
			n = fwrite(&entry, entrySize, 1, fControl->fPlanFilePtr);
			if (n != 1)  // need retry
			{
				int e = errno;
				ostringstream oss;
				oss << "Failed to update redistribute.plan: " << strerror(e) << " (" << e
					<< "); entry id=" << entryId;
				throw runtime_error(oss.str());
			}

			fflush(fControl->fPlanFilePtr);

			fControl->updateProgressInfo(entry.status, entry.endtime);

		}
		catch (const std::exception& ex)
		{
			fControl->logMessage(string("got exception when executing plan:") + ex.what());
		}
		catch (...)
		{
			fControl->logMessage("got unknown exception when executing plan.");
		}
	}

	return 0;
}


int  RedistributeControlThread::connectToWes(int dbroot)
{
	int ret = 0;
	OamCache::dbRootPMMap_t dbrootToPM = fOamCache->getDBRootToPMMap();
	int pmId = (*dbrootToPM)[dbroot];
	ostringstream oss;
	oss << "pm" << pmId << "_WriteEngineServer";
	try
	{
		mutex::scoped_lock lock(fActionMutex);
		fWesInUse = oss.str();
		fMsgQueueClient.reset(new MessageQueueClient(fWesInUse, fConfig));
	}
	catch (const std::exception& ex)
	{
		fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- " + ex.what();
		ret = 1;
	}
	catch (...)
	{
		fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- unknown";
		ret = 2;
	}

	if (ret != 0)
	{
		mutex::scoped_lock lock(fActionMutex);
		fWesInUse.clear();

		fMsgQueueClient.reset();
	}

	return ret;
}


void RedistributeControlThread::doStopAction()
{
	fConfig = Config::makeConfig();
	fControl = RedistributeControl::instance();

	mutex::scoped_lock lock(fActionMutex);
	if (!fWesInUse.empty())
	{
		// send the stop message to dbroots
		size_t headerSize = sizeof(RedistributeMsgHeader);
		RedistributeMsgHeader header(-1, -1, -1, RED_ACTN_STOP);

		try
		{
			fMsgQueueClient.reset(new MessageQueueClient(fWesInUse, fConfig));
			ByteStream bs;
			bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
			bs.append((const ByteStream::byte*) &header, headerSize);
			fMsgQueueClient->write(bs);

			SBS sbs;
			sbs = fMsgQueueClient->read();
			// no retry yet.
		}
		catch (const std::exception& ex)
		{
			fErrorMsg = "Caught exception when connecting to " + fWesInUse + " -- " + ex.what();
		}
		catch (...)
		{
			fErrorMsg = "Caught exception when connecting to " + fWesInUse + " -- unknown";
		}
	}

	if (!fErrorMsg.empty())
		fControl->logMessage(fErrorMsg + " @controlThread::doStop");
	else
		fControl->logMessage("User stop @controlThread::doStop");

	fWesInUse.clear();
	fMsgQueueClient.reset();
}


} // namespace

// vim:ts=4 sw=4:

