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

/******************************************************************************************
 * $Id: $
 *
 ******************************************************************************************/
#include <string>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <sys/time.h>
using namespace std;

#include "jl_logger.h"
#include "resourcedistributor.h"

namespace joblist {

const unsigned maxSessionsDefault = 100;

uint64_t ResourceDistributor::requestResource(uint32_t sessionID)
{
	uint64_t resource = getSessionResource(sessionID);

	return requestResource(sessionID, resource);
}

uint64_t ResourceDistributor::requestResource(uint32_t sessionID, uint64_t resource)
{
	if (fTraceOn)
		logMessage(logging::LOG_TYPE_DEBUG, LogRDRequest, resource, sessionID);

	boost::mutex::scoped_lock lk (fResourceLock );
	while (fTotalResource < resource)
	{
		if (fTraceOn)
			logMessage(logging::LOG_TYPE_DEBUG, LogRDRequestWait, resource, sessionID);

		fResourceAvailable.wait(lk);

		if (fTraceOn)
			logMessage(logging::LOG_TYPE_DEBUG, LogRDRequest, resource, sessionID);

	}
	fTotalResource -= resource;

	return resource;
}



void  ResourceDistributor::returnResource(uint64_t resource)
{
	if (fTraceOn)
		logMessage(logging::LOG_TYPE_DEBUG, LogRDReturn, resource);

	boost::mutex::scoped_lock lk (fResourceLock );
	fTotalResource += resource;

	fResourceAvailable.notify_all();
}

void ResourceDistributor::logMessage(logging::LOG_TYPE logLevel, logging::Message::MessageID mid, uint64_t value, uint32_t sessionID)
{
	logging::Message::Args args;
	args.add(fJob);
	args.add(fIdentity);
	args.add(fTotalResource);
	if (value) args.add(value);
	Logger log;
	log.logMessage(logLevel, mid, args, logging::LoggingID(5, sessionID));
}

void  LockedSessionMap::updateAging(uint32_t sessionID)
{
	boost::mutex::scoped_lock lock(fSessionLock);
	SessionList::iterator pos = find(fSessionAgingList.begin(), fSessionAgingList.end(), sessionID);
	if (fSessionAgingList.end() != pos)
		fSessionAgingList.splice(fSessionAgingList.end(), fSessionAgingList, find(fSessionAgingList.begin(), fSessionAgingList.end(), sessionID));
	else
		fSessionAgingList.push_back(sessionID);
}

uint64_t  LockedSessionMap::getSessionResource(uint32_t sessionID)
{
	SessionMap::const_iterator it = fSessionMap.find(sessionID);
	if (fSessionMap.end() != it)
	{
		updateAging(sessionID);
		return it->second;
	}
	return fResourceBlock;
}

bool LockedSessionMap::addSession(uint32_t sessionID, uint64_t resource, uint64_t limit)
{
    bool ret = true;
    if (resource > limit)
    {
	resource = limit;
	ret = false;
    }

	boost::mutex::scoped_lock maplock(fMapLock);
	fSessionMap[sessionID] = resource;
	updateAging(sessionID);
	if (fMaxSessions < fSessionMap.size())
	{
		boost::mutex::scoped_lock lock(fSessionLock);
		uint32_t oldsession = fSessionAgingList.front();
		fSessionMap.erase(oldsession);
		fSessionAgingList.erase(fSessionAgingList.begin());
	}
    return ret;

}

void LockedSessionMap::removeSession(uint32_t sessionID)
{
    boost::mutex::scoped_lock maplock(fMapLock);
    fSessionMap.erase(sessionID);
    boost::mutex::scoped_lock listlock(fSessionLock);
    fSessionAgingList.erase(find(fSessionAgingList.begin(), fSessionAgingList.end(), sessionID));
}

ostream& operator<<(ostream& os, const LockedSessionMap&  lsm)
{
	os << "Default Resource Block: " << lsm.fResourceBlock << "\tMax Number of saved sessions: " << lsm.fMaxSessions << endl;
	os << "Session Map:\tsessionID\tvalue\n";
	LockedSessionMap::SessionMap::const_iterator smIt = lsm.fSessionMap.begin(), smEnd =lsm.fSessionMap.end();
	for (; smIt != smEnd; ++smIt)
		os << "\t\t" << smIt->first << "\t\t" << smIt->second << endl;
	os << "\nAging List:\tsessionID\n\t\t";
	copy(lsm.fSessionAgingList.begin(), lsm.fSessionAgingList.end(), ostream_iterator<uint32_t>(os, "\n\t\t"));
	os << endl;
	return os;
}
} //namespace

