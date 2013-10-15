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

/***********************************************************************
*   $Id: querystats.cpp 4028 2013-08-02 18:49:00Z zzhu $
*
*
***********************************************************************/

#include <string>
using namespace std;

#include <boost/scoped_array.hpp>

#include "bytestream.h"
using namespace messageqcpp;

#include "resourcemanager.h"
using namespace joblist;

#include "errorids.h"
using namespace logging;

#include "querystats.h"

#include "libdrizzle-2.0/drizzle.h"
#include "libdrizzle-2.0/drizzle_client.h"

namespace querystats
{
	
const string SCHEMA = "infinidb_querystats";

struct IDB_Drizzle
{
	IDB_Drizzle(): drzp(NULL), drzcp(NULL), drzrp(NULL) {}
	~IDB_Drizzle() 
	{
		// The following API all checks NULL in the implementation.
		drizzle_result_free(drzrp);
		drizzle_con_close(drzcp);
		drizzle_con_free(drzcp);
		drizzle_free(drzp);
	}
	drizzle_st* drzp;
	drizzle_con_st* drzcp;
	drizzle_result_st* drzrp;
};

QueryStats::QueryStats()
{
	reset();
}

void QueryStats::reset()
{
		fMaxMemPct       = 0;
		fNumFiles        = 0;
		fFileBytes       = 0;
		fPhyIO           = 0;
		fCacheIO         = 0;
		fMsgRcvCnt       = 0;
		fCPBlocksSkipped = 0;
		fMsgBytesIn      = 0;
		fMsgBytesOut     = 0;
		fRows            = 0;
		fStartTime       = 0;
		fEndTime         = 0;
		fErrorNo         = 0;
		fBlocksChanged   = 0;
		fSessionID       = (uint64_t)-1;
		fStartTimeStr.clear();
		fEndTimeStr.clear();
		fQueryType.clear();
		fQuery.clear();
		fHost.clear();
		fUser.clear();
		fPriority.clear();
}

void QueryStats::serialize(ByteStream& b)
{
	b << (uint64_t)fSessionID;
	b << (uint64_t)fMaxMemPct;
	b << (uint64_t)fNumFiles;
	b << (uint64_t)fFileBytes;
	b << (uint64_t)fPhyIO;
	b << (uint64_t)fCacheIO;
	b << (uint64_t)fMsgRcvCnt;
	b << (uint64_t)fCPBlocksSkipped;
	b << (uint64_t)fMsgBytesIn;
	b << (uint64_t)fMsgBytesOut;
	b << (uint64_t)fRows;
	b << fStartTimeStr;
	b << fEndTimeStr;
	b << (uint64_t)fErrorNo;
	b << (uint64_t)fBlocksChanged;
	b << fQuery;
	b << fQueryType;
	b << fHost;
	b << fUser;
	b << fPriority;
}

// unserialize new stats and keep the stats set in this.
void QueryStats::unserialize(ByteStream& b)
{
	uint64_t temp = 0;
	string str;
	b >> (uint64_t&)temp;
	fSessionID =  (fSessionID == (uint64_t)-1 ? temp : fSessionID);
	b >> (uint64_t&)temp;
	fMaxMemPct = (fMaxMemPct == 0 ? temp : fMaxMemPct);
	b >> (uint64_t&)temp;
	fNumFiles = (fNumFiles == 0 ? temp : fNumFiles);
	b >> (uint64_t&)temp;
	fFileBytes = (fFileBytes == 0 ? temp : fFileBytes);
	b >> (uint64_t&)temp;
	fPhyIO = (fPhyIO == 0 ? temp : fPhyIO);
	b >> (uint64_t&)temp;
	fCacheIO = (fCacheIO == 0 ? temp : fCacheIO);
	b >> (uint64_t&)temp;
	fMsgRcvCnt = (fMsgRcvCnt == 0 ? temp : fMsgRcvCnt);
	b >> (uint64_t&)temp;
	fCPBlocksSkipped = (fCPBlocksSkipped == 0 ? temp : fCPBlocksSkipped);
	b >> (uint64_t&)temp;
	fMsgBytesIn = (fMsgBytesIn == 0 ? temp : fMsgBytesIn);
	b >> (uint64_t&)temp;
	fMsgBytesOut = (fMsgBytesOut == 0 ? temp : fMsgBytesOut);
	b >> (uint64_t&)temp;
	fRows = (fRows == 0 ? temp : fRows);
	
	b >> str;
	fStartTimeStr = (fStartTimeStr.empty() ? str : fStartTimeStr);
	b >> str;
	fEndTimeStr = (fEndTimeStr.empty() ? str : fEndTimeStr);
	
	b >> (uint64_t&)temp;
	fErrorNo = (fErrorNo == 0 ? temp : fErrorNo);
	b >> (uint64_t&)temp;
	fBlocksChanged = (fBlocksChanged == 0 ? temp : fBlocksChanged);
	
	b >> str;
	fQuery = (fQuery.empty() ? str : fQuery);
	b >> str;
	fQueryType = (fQueryType.empty() ? str : fQueryType);
	b >> str;
	fHost = (fHost.empty() ? str : fHost);
	b >> str;
	fUser = (fUser.empty() ? str: fUser);
	b >> str;
	fPriority = (fPriority.empty() ? str : fPriority);
}

/**
 * QueryStats table definition
   create table QueryStats
   ( 
     queryID           bigint primary key auto_increment,
     sessionID         bigint,
     queryType         char(25),
     query             blob,
     startTime         timestamp,
     endTime           timestamp,
     rows              bigint,
     errno             int,
     phyIO             bigint,
     cacheIO           bigint,
     blocksTouched     bigint,
     CPBlocksSkipped   bigint,
     msgInUM           bigint,
     msgOutUm          bigint,
     maxMemPct         int,    // not populated for now
     blocksChanged     bigint, // for dml
     numTempFiles      bigint, // not populated for now
     tempFileSpace     bigint  // not populated for now
   )
 */
void QueryStats::insert()
{
	ResourceManager rm;
	// check if query stats is enabled in Calpont.xml
	if (!rm.queryStatsEnabled())
		return;
	
	// get configure for every query to allow only changing of connect info
	string host, user, pwd;
	uint port;

	if (rm.getMysqldInfo(host, user, pwd, port) == false)
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_CROSS_ENGINE_CONFIG),
			ERR_CROSS_ENGINE_CONFIG);

	// insert stats to querystats table
	IDB_Drizzle drizzle;

	drizzle.drzp = drizzle_create();
	if (drizzle.drzp == 0)
		handleMySqlError("fatal error initializing querystats lib", -1);

	drizzle.drzcp = drizzle_con_add_tcp(drizzle.drzp, host.c_str(), port, user.c_str(), pwd.c_str(),
		SCHEMA.c_str(), DRIZZLE_CON_MYSQL);
	if (drizzle.drzcp == 0)
		handleMySqlError("fatal error setting up parms in querystats lib", -1);

	drizzle_return_t drzret;
	drzret = drizzle_con_connect(drizzle.drzcp);
	if (drzret != 0)
		handleMySqlError("fatal error connecting to InfiniDB in querystats lib", drzret);

	// escape quote characters
	boost::scoped_array<char> query(new char[fQuery.length()*2+1]);
	drizzle_escape_string(query.get(), fQuery.length()*2, fQuery.c_str(), fQuery.length());

	ostringstream insert;
	insert << "insert delayed into querystats values (0, ";
	insert << fSessionID << ", ";
	insert << "'" << fHost << "', ";
	insert << "'" << fUser << "', ";
	insert << "'" << fPriority << "', ";	 
	insert << "'" << fQueryType << "', ";
	insert << "'" << query.get() << "', ";
	insert << "'" << fStartTimeStr << "', ";
	insert << "'" << fEndTimeStr << "', ";
	insert << fRows << ", ";
	insert << fErrorNo << ", ";
	insert << fPhyIO << ", ";
	insert << fCacheIO << ", ";
	insert << fMsgRcvCnt << ", ";
	insert << fCPBlocksSkipped << ", ";
	insert << fMsgBytesIn << ", ";
	insert << fMsgBytesOut << ", ";
	insert << fMaxMemPct << ", ";
	insert << fBlocksChanged << ", ";
	insert << fNumFiles << ", ";
	insert << fFileBytes << ")"; // the last 2 fields are not populated yet
	
	drizzle.drzrp = drizzle_query_str(drizzle.drzcp, drizzle.drzrp, insert.str().c_str(), &drzret);
	if (drzret != 0 || drizzle.drzrp == 0)
		handleMySqlError("fatal error executing query in querystats lib", drzret);

	drzret = drizzle_result_buffer(drizzle.drzrp);
	if (drzret != 0)
		handleMySqlError("fatal error reading results from InfiniDB in querystats lib", drzret);
}

void QueryStats::handleMySqlError(const char* errStr, unsigned int errCode)
{
	ostringstream oss;
	oss << errStr << " (" << errCode << ")";
	Message::Args args;
	args.add(oss.str());
	throw IDBExcept(ERR_CROSS_ENGINE_CONNECT, args);
}

uint QueryStats::userPriority(string _host, const string _user)
{
	// priority has been set already
	if (!fPriority.empty())
		return fPriorityLevel;
		
	ResourceManager rm;
	fPriorityLevel = DEFAULT_USER_PRIORITY_LEVEL;
	fPriority = DEFAULT_USER_PRIORITY;
		
	// check if query stats is enabled in Calpont.xml
	if (!rm.userPriorityEnabled())
	{
		fPriority = DEFAULT_USER_PRIORITY;
		fPriorityLevel = DEFAULT_USER_PRIORITY_LEVEL;
		return fPriorityLevel;
	}
	
	string host, user, pwd;
	uint port;
	
	// get configure for every query to allow only changing of connect info
	if (rm.getMysqldInfo(host, user, pwd, port) == false)
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_CROSS_ENGINE_CONFIG),
			ERR_CROSS_ENGINE_CONFIG);

	// get user priority
	IDB_Drizzle drizzle;
	drizzle.drzp = drizzle_create();
	if (drizzle.drzp == 0)
		handleMySqlError("fatal error initializing querystats lib", -1);

	drizzle.drzcp = drizzle_con_add_tcp(drizzle.drzp, host.c_str(), port, user.c_str(), pwd.c_str(),
		SCHEMA.c_str(), DRIZZLE_CON_MYSQL);
	if (drizzle.drzcp == 0)
		handleMySqlError("fatal error setting up parms in querystats lib", -1);
	drizzle_return_t drzret;
	drzret = drizzle_con_connect(drizzle.drzcp);
	if (drzret != 0)
		handleMySqlError("fatal error connecting to InfiniDB in querystats lib", drzret);
	
	// get the part of host string befor ':' if there is.
	size_t pos = _host.find(':', 0);
	if (pos != string::npos)
		_host = _host.substr(0, pos);

	ostringstream query;
	query << "select a.priority, priority_level from user_priority a, priority b where \
	          upper(case when INSTR(host, ':') = 0 \
	          then host \
	          else SUBSTR(host, 1, INSTR(host, ':')-1 ) \
	          end)=upper('"
	      << _host
	      << "') and upper(user)=upper('" 
	      << _user 
	      << "') and upper(a.priority) = upper(b.priority)";

	drizzle.drzrp = drizzle_query_str(drizzle.drzcp, drizzle.drzrp, query.str().c_str(), &drzret);
	if (drzret != 0 || drizzle.drzrp == 0)
		handleMySqlError("fatal error executing query in querystats lib", drzret);
	drzret = drizzle_result_buffer(drizzle.drzrp);
	if (drzret != 0)
		handleMySqlError("fatal error reading results from InfiniDB in querystats lib", drzret);

	// only fetch one row. if duplicate user name in the table, the first one will be got.
	drizzle_row_t row;
	row = drizzle_row_next(drizzle.drzrp);
	if (row)
	{
		fPriority = row[0];
		fPriorityLevel = atoi(row[1]);
	}

	return fPriorityLevel;
}

}
