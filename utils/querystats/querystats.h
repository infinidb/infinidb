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
*   $Id: querystats.h 4028 2013-08-02 18:49:00Z zzhu $
*
*
***********************************************************************/

#ifndef QUERYSTATS_H_
#define QUERYSTATS_H_

#include <ctime>
#include <string>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>

#include "bytestream.h"
#include "resourcemanager.h"

namespace querystats
{
const uint32_t DEFAULT_USER_PRIORITY_LEVEL = 33; //Low
const std::string DEFAULT_USER_PRIORITY = "LOW";

struct QueryStats
{
	uint64_t fMaxMemPct;           // peak memory percentage used during a query
	uint64_t fNumFiles;            // number of temp files used for a query
	uint64_t fFileBytes;           // number of bytes in temp files
	uint64_t fPhyIO;               // physical block count for a query
	uint64_t fCacheIO;             // cache block count for a query
	uint64_t fMsgRcvCnt;           // msg (block) receive count for a query
	uint64_t fCPBlocksSkipped;     // Casual Partition blks skipped for a query
	uint64_t fMsgBytesIn;          // number of input msg bytes for a query
	uint64_t fMsgBytesOut;         // number of output msg bytes for a query
	uint64_t fRows;                // number of rows return/affected
	time_t   fStartTime;           // query start time
	time_t   fEndTime;             // query end time
	std::string   fStartTimeStr;   // query start time in YYYY-MM-DD HH:MM:SS format
	std::string   fEndTimeStr;     // query end time in YYYY-MM-DD HH:MM:SS format
	uint64_t fErrorNo;             // query error number. 0 if succeed
	uint64_t fBlocksChanged;       // blocks changed for DML queries
	uint64_t fSessionID;           // session id of this query
	std::string fQueryType;        // query type as "select", "update", "delete" ...
	std::string fQuery;            // query text
	std::string fUser;             // user
	std::string fHost;             // host
	std::string fPriority;         // priority
	uint32_t fPriorityLevel;           // priority level
	
	
	QueryStats();
	~QueryStats() {}

	/**
	  reset the stats fields. 
	*/
	void reset();
	
	// only += for fields that make sense for subquery
	QueryStats operator+=(const QueryStats& rhs)
	{
		fNumFiles        += rhs.fNumFiles;
		fFileBytes       += rhs.fFileBytes;
		fPhyIO           += rhs.fPhyIO;
		fCacheIO         += rhs.fCacheIO;
		fMsgRcvCnt       += rhs.fMsgRcvCnt;
		fCPBlocksSkipped += rhs.fCPBlocksSkipped;
		fMsgBytesIn      += rhs.fMsgBytesIn;
		fMsgBytesOut     += rhs.fMsgBytesOut;
		fBlocksChanged   += rhs.fBlocksChanged;

		return *this;
	}
	
	void setStartTime() 
	{ 
		time(&fStartTime);
		char buffer [80];
		struct tm timeinfo;
		localtime_r(&fStartTime, &timeinfo);
		strftime(buffer,80,"%Y-%m-%d %H:%M:%S", &timeinfo);
		fStartTimeStr = buffer;
	}
	
	void setEndTime()
	{ 
		time(&fEndTime);
		char buffer [80];
		struct tm timeinfo;
		localtime_r(&fEndTime, &timeinfo);
		strftime(buffer,80,"%Y-%m-%d %H:%M:%S", &timeinfo);
		fEndTimeStr = buffer;
	}
	
	//joblist::ResourceManager* rm() { return fRm; }
	//void rm(joblist::ResourceManager* rm) 
	//{ 
	//	delete fRm;
	//	fRm = rm;
	//	fIsOwnRm = false;
	//}
		
	void serialize(messageqcpp::ByteStream& bs);
	// unserialize, and merge new stats to this when handling stats comming from different processes.
	void unserialize(messageqcpp::ByteStream& bs);
			
	/**
	 insert to query stats table. The table is pre-created and the number/type of 
	 columns are fixed. If changing the table defintion, this function needs to be 
	 modified accordingly.
	*/
	void insert();
	void handleMySqlError(const char*, unsigned int);
	
	/* User mysql API to query priority table and get this user's assigned priority */
	uint32_t userPriority(std::string host, const std::string user);

private:
//	default okay
//	QueryStats(const QueryStats& stats);    
//	QueryStats& operator=(const QueryStats&); 
};

}

#endif
