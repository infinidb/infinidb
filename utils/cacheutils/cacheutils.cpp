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

// $Id: cacheutils.cpp 4051 2013-08-09 22:38:47Z wweeks $

#include <unistd.h>

#include "cacheutils.h"

#include <string>
#include <stdint.h>
#include <sstream>
#include <limits>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "primitivemsg.h"

#include "brmtypes.h"
using namespace BRM;

#include "atomicops.h"

namespace
{

//Only one of the cacheutils fcns can run at a time
mutex CacheOpsMutex;

//This global is updated only w/ atomic ops
volatile uint32_t MultiReturnCode;

int32_t extractRespCode(const ByteStream& bs)
{
		if (bs.length() < (sizeof(ISMPacketHeader) + sizeof(int32_t)))
			return 1;
		const uint8_t* bytePtr = bs.buf();
		const ISMPacketHeader* hdrp = reinterpret_cast<const ISMPacketHeader*>(bytePtr);
		if (hdrp->Command != CACHE_OP_RESULTS)
			return 1;
		const int32_t* resp = reinterpret_cast<const int32_t*>(bytePtr + sizeof(ISMPacketHeader));
		return *resp;
}

class CacheOpThread
{
public:
	CacheOpThread(const string& svr, const ByteStream& outBs) : fServerName(svr), fOutBs(outBs) {}
	~CacheOpThread() {}
	void operator()()
	{
		struct timespec ts = { 10, 0 };
		int32_t rc = 0;
		scoped_ptr<MessageQueueClient> cl(new MessageQueueClient(fServerName)); 
		try
		{
			cl->write(fOutBs);
			rc = extractRespCode(cl->read(&ts));
		}
		catch(...)
		{
			rc = 1;
		}
		if (rc != 0)
			atomicops::atomicCAS<uint32_t>(&MultiReturnCode, 0, 1);
	}

private:
	//CacheOpThread(const CacheOpThread& rhs);
	//CacheOpThread& operator=(const CacheOpThread& rhs);

	string fServerName;
	ByteStream fOutBs;
};

int sendToAll(const ByteStream& outBs)
{
	//Not thread-safe: external synchronization is needed!

	// added code here to flush any running primprocs that may be active
	// TODO: we really only need to flush each unique PrimProc, but we can't tell from the
	//  config file which those are, so use the same logic as joblist::DistributedEngineComm
	Config* cf = Config::makeConfig();

	const string section = "PrimitiveServers";
	int cnt = static_cast<int>(Config::fromText(cf->getConfig(section, "Count")));
	if (cnt <= 0) cnt = 1;

	thread_group tg;
	int rc = 0;
	MultiReturnCode = 0;

	for (int i = 0; i < cnt; i++)
	{
		ostringstream oss;
		oss << "PMS" << (i + 1);
		tg.create_thread(CacheOpThread(oss.str(), outBs));
	}

	tg.join_all();

	if (MultiReturnCode != 0)
		rc = -1;

	return rc;
}

}

namespace cacheutils
{

/**
 *
 */
int flushPrimProcCache()
{
	mutex::scoped_lock lk(CacheOpsMutex);

	try
	{
		const int msgsize = sizeof(ISMPacketHeader);
		uint8_t msgbuf[msgsize];
		memset(msgbuf, 0, sizeof(ISMPacketHeader));
		ISMPacketHeader* hdrp = reinterpret_cast<ISMPacketHeader*>(&msgbuf[0]);
		hdrp->Command = CACHE_FLUSH;

		ByteStream bs(msgbuf, msgsize);
		int rc = sendToAll(bs);
		return rc;
	}
	catch (...)
	{
	}
	return -1;
}

/**
 *
 */
int flushPrimProcBlocks(const BRM::BlockList_t& list)
{
	if (list.empty()) return 0;

	mutex::scoped_lock lk(CacheOpsMutex);

#if defined(__LP64__) || defined(_WIN64)
	if (list.size() > numeric_limits<uint32_t>::max()) return -1;
#endif

	try
	{
		const size_t msgsize = sizeof(ISMPacketHeader) + sizeof(uint32_t) + sizeof(LbidAtVer) * list.size();
		scoped_array<uint8_t> msgbuf(new uint8_t[msgsize]);
		memset(msgbuf.get(), 0, sizeof(ISMPacketHeader));
		ISMPacketHeader* hdrp = reinterpret_cast<ISMPacketHeader*>(msgbuf.get());
		hdrp->Command = CACHE_CLEAN_VSS;
		uint32_t* cntp = reinterpret_cast<uint32_t*>(msgbuf.get() + sizeof(ISMPacketHeader));
		*cntp = static_cast<uint32_t>(list.size());
		LbidAtVer* itemp = reinterpret_cast<LbidAtVer*>(msgbuf.get() + sizeof(ISMPacketHeader) + sizeof(uint32_t));
		BlockList_t::const_iterator iter = list.begin();
		BlockList_t::const_iterator end = list.end();
		while (iter != end)
		{
			itemp->LBID = static_cast<uint64_t>(iter->first);
			itemp->Ver = static_cast<uint32_t>(iter->second);
			++itemp;
			++iter;
		}

		ByteStream bs(msgbuf.get(), msgsize);
		int rc = sendToAll(bs);
		return rc;
	}
	catch (...)
	{
	}
	return -1;
}


int flushPrimProcAllverBlocks(const vector<LBID_t> &list)
{
    if (list.empty()) return 0;

    ByteStream bs(sizeof(ISMPacketHeader) + sizeof(uint32_t) + (sizeof(LBID_t) * list.size()));
    ISMPacketHeader *hdr;
    int rc;

    hdr = (ISMPacketHeader *) bs.getInputPtr();
    hdr->Command = FLUSH_ALL_VERSION;
    bs.advanceInputPtr(sizeof(ISMPacketHeader));
    bs << (uint32_t) list.size();
    bs.append((uint8_t *) &list[0], sizeof(LBID_t) * list.size());

    try {
		mutex::scoped_lock lk(CacheOpsMutex);
        rc = sendToAll(bs);
        return rc;
    }
   catch (...)
    {
    }
    return -1;
}

int flushOIDsFromCache(const vector<BRM::OID_t> &oids)
{
	/* Message format:
	 *    ISMPacketHeader
	 *    uint32_t - OID count
	 *    uint32_t * - OID array
	 */

	mutex::scoped_lock lk(CacheOpsMutex, defer_lock_t());

	ByteStream bs;
	ISMPacketHeader ism;
	uint32_t i;

	memset(&ism, 0, sizeof(ISMPacketHeader));
	ism.Command = CACHE_FLUSH_BY_OID;
	bs.load((uint8_t *) &ism, sizeof(ISMPacketHeader));
	bs << (uint32_t) oids.size();
	for (i = 0; i < oids.size(); i++)
		bs << (uint32_t) oids[i];

	lk.lock();
	return sendToAll(bs);
}

int flushPartition(const std::vector<BRM::OID_t> &oids, set<BRM::LogicalPartition>& partitionNums)
{
	/* Message format:
	 * 		ISMPacketHeader
	 * 		uint32_t - partition count
	 * 		LogicalPartition * - partitionNum
	 * 		uint32_t - OID count
	 * 		uint32_t * - OID array
	 */

	mutex::scoped_lock lk(CacheOpsMutex, defer_lock_t());

	ByteStream bs;
	ISMPacketHeader ism;

	memset(&ism, 0, sizeof(ISMPacketHeader));
	ism.Command = CACHE_FLUSH_PARTITION;
	bs.load((uint8_t *) &ism, sizeof(ISMPacketHeader));
	serializeSet<BRM::LogicalPartition>(bs, partitionNums);
	serializeInlineVector<BRM::OID_t>(bs, oids);

	lk.lock();
	return sendToAll(bs);
}


int dropPrimProcFdCache()
{
	const int msgsize = sizeof(ISMPacketHeader);
	uint8_t msgbuf[msgsize];
	memset(msgbuf, 0, sizeof(ISMPacketHeader));
	ISMPacketHeader* hdrp = reinterpret_cast<ISMPacketHeader*>(&msgbuf[0]);
	hdrp->Command = CACHE_DROP_FDS;
	ByteStream bs(msgbuf, msgsize);
	try
	{
		mutex::scoped_lock lk(CacheOpsMutex);
		int rc = sendToAll(bs);
		return rc;
	}
	catch (...)
	{
	}
	return -1;
}

int purgePrimProcFdCache(const std::vector<BRM::FileInfo> files, const int pmId)
{
	const int msgsize = sizeof(ISMPacketHeader);
	uint8_t msgbuf[msgsize];
	memset(msgbuf, 0, sizeof(ISMPacketHeader));
	ISMPacketHeader* hdrp = reinterpret_cast<ISMPacketHeader*>(&msgbuf[0]);
	hdrp->Command = CACHE_PURGE_FDS;
	ByteStream bs(msgbuf, msgsize);
	serializeInlineVector<FileInfo>(bs, files);
	int32_t rc = 0;
	try
	{
		struct timespec ts = { 10, 0 };		
		ostringstream oss;
		oss << "PMS" << pmId;
		scoped_ptr<MessageQueueClient> cl(new MessageQueueClient(oss.str())); 
		cl->write(bs);
		rc = extractRespCode(cl->read(&ts));
	}
	catch (...)
	{
		rc = -1;
	}
	return rc;
}
}

// vim:ts=4 sw=4:

