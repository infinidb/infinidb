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
// $Id: iomanager.h 655 2008-07-08 16:42:54Z jrodriguez $
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

#include <pthread.h>
#include <string>

#include "writeengine.h"
#include "configcpp.h"
#include "brm.h"
#include "fileblockrequestqueue.h"
#include "filebuffermgr.h"

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
	BRM::LBID_t lbidLookup(BRM::LBID_t lbid,
						   BRM::VER_t verid,
						   bool vbFlag,
						   BRM::OID_t& oid,
						   uint32_t& offset);
	void buildOidFileName(const BRM::OID_t oid, char* file_name);

	uint32_t getExtentSize() { return fdbrm.getExtentSize(); }

	uint32_t blocksPerRead;

	bool IOTrace() const { return fIOTrace;}

private:

	FileBufferMgr& fIOMfbMgr;
	fileBlockRequestQueue& fIOMRequestQueue;
	int fThreadCount;
	pthread_t fPoppertid;
	pthread_t fThreadArr[256];
	int createReaders();
	config::Config* fConfig;
	BRM::DBRM fdbrm;
	WriteEngine::FileOp fFileOp;
	
	// do not implement
	ioManager();
	ioManager(const ioManager& iom);
	const ioManager& operator=(const ioManager& iom);
	bool fIOTrace;
};

}
#endif
// vim:ts=4 sw=4:
