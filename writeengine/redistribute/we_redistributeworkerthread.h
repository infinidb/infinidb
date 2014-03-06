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
* $Id: we_redistributeworkerthread.h 4450 2013-01-21 14:13:24Z rdempsey $
*/

#ifndef WE_REDISTRIBUTEWORKERTHREAD_H
#define WE_REDISTRIBUTEWORKERTHREAD_H

#include <map>
#include <set>
#include <vector>
#include <cstdio>

#include "boost/shared_ptr.hpp"
#include "boost/shared_array.hpp"
#include "boost/thread/mutex.hpp"

#include "brmtypes.h"
#include "we_redistributedef.h"


// forward reference
namespace config
{
class Config;
}

namespace oam
{
class OamCache;
}

namespace BRM
{
class DBRM;
}

namespace messageqcpp
{
class ByteStream;
class IOSocket;
}

namespace messagequeue
{
class MessageQueueClient;
}


namespace redistribute
{


class RedistributeWorkerThread
{
  public:
	RedistributeWorkerThread(messageqcpp::ByteStream& bs, messageqcpp::IOSocket& ios);
	~RedistributeWorkerThread();

	void operator()();

  private:

	void  handleRequest();
	void  handleStop();
	void  handleData();
	void  handleUnknowJobMsg();

	int   setup();
	int   grabTableLock();
	int   buildEntryList();
	int   sendData();
	int   connectToWes(int);
	int   updateDbrm();
	void  confirmToPeer();
	bool  checkDataTransferAck(SBS&, size_t);

	void  sendResponse(uint32_t);

	void  doAbort();

	void  handleDataInit();
	void  handleDataStart(messageqcpp::SBS&, size_t&);
	void  handleDataCont(messageqcpp::SBS&, size_t&);
	void  handleDataFinish(messageqcpp::SBS&, size_t&);
	void  handleDataCommit(messageqcpp::SBS&, size_t&);
	void  handleDataAbort(messageqcpp::SBS&, size_t&);
	void  handleUnknowDataMsg();

	int   buildFullHdfsPath( std::map<int,std::string>& rootToPathMap,
		int64_t      colOid,
    	int16_t      dbRoot,
    	uint32_t     partition,
    	int16_t      segment,
    	std::string& fullFileName);

	void  closeFile(FILE*);  // for tracing, may remove later.
	void  addToDirSet(const char*, bool);
	void  logMessage(const std::string&, int);

	oam::OamCache*                fOamCache;
	config::Config*               fConfig;
	boost::shared_ptr<messageqcpp::MessageQueueClient>  fMsgQueueClient;

	RedistributeMsgHeader         fMsgHeader;
	messageqcpp::ByteStream&      fBs;
	messageqcpp::IOSocket&        fIOSocket;
	RedistributePlanEntry         fPlanEntry;
	uint64_t                      fTableLockId;
	int32_t                       fErrorCode;
	std::string                   fErrorMsg;
	std::pair<int, int>           fMyId;     // <dbroot, pmid>
	std::pair<int, int>           fPeerId;   // <dbroot, pmid>
	std::set<int16_t>             fSegments;
	std::vector<int64_t>          fOids;     // column oids
	std::vector<BRM::BulkUpdateDBRootArg> fUpdateRtEntries;   // for dbrm update
	std::vector<BRM::BulkSetHWMArg>       fUpdateHwmEntries;  // for dbrm update

	FILE*                         fNewFilePtr;
	FILE*                         fOldFilePtr;
	std::set<std::string>         fNewDirSet;
	std::set<std::string>         fOldDirSet;
	boost::shared_array<char>     fWriteBuffer;

	boost::shared_ptr<BRM::DBRM>  fDbrm;

	// for segment file # workaround
	//uint64_t                      fSegPerRoot;


	static boost::mutex           fActionMutex;
	static volatile bool          fStopAction;
	static volatile bool          fCommitted;
	static std::string            fWesInUse;
};

} // namespace


#endif  // WE_REDISTRIBUTEWORKERTHREAD_H

// vim:ts=4 sw=4:

