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

#ifndef IOMANAGER_H
#define IOMANAGER_H
// $Id: iomanager.h 2145 2013-08-09 22:38:19Z wweeks $
//
// C++ Interface: iomanager
//
// Description: 
//
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
//
//
//

/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

#include <iostream>
#include <iomanip>
#include <string>
#include <boost/thread.hpp>
#include "writeengine.h"
#include "configcpp.h"
#include "brm.h"
#include "fileblockrequestqueue.h"
#include "filebuffermgr.h"

//#define SHARED_NOTHING_DEMO_2

namespace dbbc {

class ioManager {
	
public:
	
    ioManager(FileBufferMgr& fbm, fileBlockRequestQueue& fbrq, int thrCount, 
		int bsPerRead);
	//ioManager(FileBufferMgr& fbm, int thrCount);
	~ioManager();
	int readerCount() const {return fThreadCount;}
	fileRequest* getNextRequest();
	void go(void);
	void stop();
	FileBufferMgr& fileBufferManager() {return fIOMfbMgr;}
	config::Config* configPtr() {return fConfig;}

    const int localLbidLookup(BRM::LBID_t lbid,
                              BRM::VER_t verid, 
                              bool vbFlag,
                              BRM::OID_t& oid,
                              uint16_t& dbRoot,
                              uint32_t& partitionNum,
                              uint16_t& segmentNum,
                              uint32_t& fileBlockOffset);

    void buildOidFileName(const BRM::OID_t oid,
                        const uint16_t dbRoot,
                        const uint16_t partNum,
                        const uint32_t segNum,
                        char* file_name);

	const uint32_t getExtentRows() { return fdbrm.getExtentRows(); }

	uint32_t blocksPerRead;

	bool IOTrace() const { return fIOTrace;}

	uint32_t MaxOpenFiles() const { return fMaxOpenFiles;}

	uint32_t DecreaseOpenFilesCount() const { return fDecreaseOpenFilesCount;}

	bool FDCacheTrace() const { return fFDCacheTrace;}

	void handleBlockReadError ( fileRequest* fr,
		const std::string& errMsg, bool *copyLocked, int errorCode=fileRequest::FAILED );

	std::ofstream& FDTraceFile() {return fFDTraceFile;}

	BRM::DBRM* dbrm() { return &fdbrm;}
	
	
#ifdef SHARED_NOTHING_DEMO_2
	uint32_t pmCount;
#endif

private:

	FileBufferMgr& fIOMfbMgr;
	fileBlockRequestQueue& fIOMRequestQueue;
	int fThreadCount;
	boost::thread_group fThreadArr;
	void createReaders();
	config::Config* fConfig;
	BRM::DBRM fdbrm;
	WriteEngine::FileOp fFileOp;
	
	// do not implement
	ioManager();
	ioManager(const ioManager& iom);
	const ioManager& operator=(const ioManager& iom);
	bool fIOTrace;
	uint32_t fMaxOpenFiles;
	uint32_t fDecreaseOpenFilesCount;
	bool fFDCacheTrace;
	std::ofstream fFDTraceFile;

};

// @bug2631, for remount filesystem by loadBlock() in primitiveserver
// Shared Nothing update: Remount is no longer necessary, might have a use for these though.
void setReadLock();
void releaseReadLock();
void dropFDCache();
void purgeFDCache(std::vector<BRM::FileInfo>& files);

}
#endif
// vim:ts=4 sw=4:
