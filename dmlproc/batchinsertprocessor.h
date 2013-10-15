/* Copyright (C) 2013 Calpont Corp.

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

// $Id: batchinsertprocessor.h 525 2010-01-19 23:18:05Z xlou $

/** @file */

#ifndef BATCHINSERTPROCESSOR_H__
#define BATCHINSERTPROCESSOR_H__

#include <stdint.h>
#include <queue>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include "insertdmlpackage.h"
#include "resourcemanager.h"
#include "bytestream.h"
#include "dbrm.h"
#include "batchloader.h"
#include "we_clients.h"
namespace dmlprocessor 
{
class BatchInsertProc
{
public:
	typedef std::queue<messageqcpp::ByteStream > pkg_type;	
	typedef boost::shared_ptr<pkg_type>  SP_PKG;
	typedef std::vector<BRM::BulkSetHWMArg> BulkSetHWMArgs;
	BatchInsertProc(bool isAutocommitOn, uint32_t tableOid, execplan::CalpontSystemCatalog::SCN txnId, BRM::DBRM* aDbrm);
	BatchInsertProc(const BatchInsertProc& rhs);
	~BatchInsertProc();
	uint64_t grabTableLock(int32_t sessionId);
	SP_PKG  getInsertQueue ();
	uint getNumDBRoots();
	void setLastPkg (bool lastPkg);
	void addPkg(messageqcpp::ByteStream & insertBs);
	messageqcpp::ByteStream getPkg();
	void setError(int errorCode, std::string errMsg);
	void getError(int & errorCode, std::string & errMsg);
	int sendPkg(int pmId);
	void buildPkg(messageqcpp::ByteStream& bs);
	void buildLastPkg(messageqcpp::ByteStream& bs);
	void sendFirstBatch();
	void sendNextBatch();
	void sendlastBatch();
	void collectHwm();
	void setHwm();
	void receiveAllMsg();
	void receiveOutstandingMsg();
private:
	SP_PKG fInsertPkgQueue;
	boost::condition condvar;
	execplan::CalpontSystemCatalog::SCN fTxnid;
	int fErrorCode;
	std::string fErrMsg;
	bool fLastpkg;
	bool fIsAutocommitOn;
	uint32_t fTableOid;
	uint64_t fUniqueId;
	BRM::DBRM* fDbrm;
	WriteEngine::WEClients* fWEClient;
	oam::OamCache *fOamcache;
	std::vector<uint> fPMs; //active PMs
	batchloader::BatchLoader* fBatchLoader;
	std::map<unsigned, bool> fPmState;
	uint fCurrentPMid;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	messageqcpp::ByteStream::byte tmp8;
	messageqcpp::ByteStream::quadbyte tmp32;
	std::vector<BulkSetHWMArgs> fHwmArgsAllPms;
	uint64_t fTableLockid;
	
	
};

} // namespace dmlprocessor
#endif
// vim:ts=4 sw=4:
