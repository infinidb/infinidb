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
 * $Id: resourcemanager.h 4852 2009-02-09 14:21:34Z rdempsey $
 *
 ******************************************************************************************/
/**
 * @file
 */
#ifndef JOBLIST_RESOURCEDISTRIBUTER_H
#define JOBLIST_RESOURCEMANAGER_H

#include <unistd.h>
#include <list>
#include <limits>
#include <boost/thread/condition.hpp>

#include "logger.h"

#undef min
#undef max

namespace joblist
{

  /** @brief ResourceDistributor
   *	Manages a resource.  Distributes up to fTotalResource on request.
   * 	Expects the requester to return the resource when finished.
   *	Blocks a requester when less than fResourceBlock of the resource is available.
   *
   *	Keeps a map of session id and ResourceBlock values so that the fResourceBlock can
   *	be overriden for a session.
   *
   *	The aging logic is needed because exemgr gets a close message after every query, not
   *	for a session, so it does not know when a session is ended.  Therefore LockedSessionMap
   *	must keep all its sessions.  To prevent the map from becoming too large, after it reaches
   *  	fMaxSessions, it removes the oldest used session.  fMaxSessions is defined in
   *	resourcedistributor.cpp.  UpdateAging keeps the last accessed session at the end of the
   *    aging list.  The oldest is at the front.
   *
   */

extern const unsigned maxSessionsDefault;

class LockedSessionMap
{
public:
	LockedSessionMap(uint64_t resource, unsigned maxSessions = maxSessionsDefault):fResourceBlock(resource), fMaxSessions(maxSessions){}
	typedef std::map <uint32_t, uint64_t> SessionMap;
	typedef std::list <uint32_t> SessionList;
	bool addSession(uint32_t sessionID, uint64_t resource, uint64_t limit = std::numeric_limits<uint64_t>::max());
	void removeSession(uint32_t sessionID);
	uint64_t  getSessionResource(uint32_t sessionID);
	friend std::ostream& operator<<(std::ostream& os, const LockedSessionMap& lsm);

private:
	void updateAging(uint32_t sessionID);
	boost::mutex 		fMapLock;
	SessionMap 		fSessionMap;
	uint64_t 		fResourceBlock;
	boost::mutex 		fSessionLock;
	SessionList	 	fSessionAgingList;
	const unsigned  	fMaxSessions;


};


class ResourceDistributor
{
public:

	ResourceDistributor(const std::string& job, const std::string& identity, uint64_t totalResource, uint64_t resourceBlock, bool trace) :
		fJob(job), fIdentity(identity), fTotalResource(totalResource), fSessionMap(resourceBlock), fTraceOn(trace)
		{}

	virtual ~ResourceDistributor(){}

	typedef std::map <uint32_t, uint64_t> SessionMap;

	uint64_t requestResource(uint32_t sessionID);
	uint64_t requestResource(uint32_t sessionID, uint64_t resource);
	void returnResource(uint64_t resource);


	uint64_t  getSessionResource(uint32_t sessionID)
	{
		return fSessionMap.getSessionResource(sessionID);
	}

	uint64_t  getTotalResource() const { return fTotalResource; }


	void setTrace(bool trace) { fTraceOn = trace; }

	bool addSession(uint32_t sessionID, uint64_t resource) {return fSessionMap.addSession(sessionID, resource, fTotalResource);}
	void removeSession(uint32_t sessionID) {fSessionMap.removeSession(sessionID); }

private:

	void logMessage(logging::LOG_TYPE logLevel, logging::Message::MessageID mid, uint64_t value = 0, uint32_t sessionId = 0);

	std::string 	fJob;
	std::string	fIdentity;
	uint64_t   	fTotalResource;
	uint64_t   	fResourceBlock;
	boost::mutex 	fResourceLock;
	boost::condition fResourceAvailable;

	LockedSessionMap fSessionMap;
	uint32_t 	fTraceOn;

};

}

#endif
