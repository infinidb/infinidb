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

// $Id: iomanager.cpp 724 2008-09-26 16:16:05Z jrodriguez $
//
// C++ Implementation: iomanager
//
// Description: 
//
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
//
//
//

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#define NDEBUG
#include <cassert>

using namespace std;

#include "configcpp.h"
using namespace config;

#include "iomanager.h"
namespace {

using namespace dbbc;
using namespace std;

void timespec_sub(const struct timespec &tv1,
				const struct timespec &tv2,
				double &tm) 
{
	tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9*(tv2.tv_nsec - tv1.tv_nsec);
}

struct IOMThreadArg {
	ioManager* iom;
	int32_t thdId;
};

typedef IOMThreadArg IOMThreadArg_t;

void* thr_popper(void* arg) {
	ioManager* iom = ((IOMThreadArg*)arg)->iom;
	int32_t iomThdId = ((IOMThreadArg*)arg)->thdId;
	FileBufferMgr* fbm;
	int totalRqst=0;
	fileRequest* fr=0;
	BRM::LBID_t lbid=0;
	BRM::OID_t oid=0;
	BRM::VER_t ver=0; 
	int blocksLoaded=0;
	int blocksRead=0;
	const unsigned pageSize = 4096;
	fbm = &iom->fileBufferManager();
	char fileName[WriteEngine::FILE_NAME_SIZE];
	const uint64_t fileBlockSize = BLOCK_SIZE;
	uint32_t offset=0;
	bool flg=false;
	char* fileNamePtr=fileName;
	uint64_t longSeekOffset=0;
	int err;
	uint32_t dlen = 0, acc, readSize, blocksThisRead, j;
	uint32_t blocksRequested=0;
	ssize_t i;
	uint32_t sz=0;
	char* alignedbuff=0;
 	boost::scoped_array<char> realbuff;	
	pthread_t threadId=0;
	ostringstream iomLogFileName;
	ofstream lFile;

	threadId=pthread_self();

	uint32_t readBufferSz=iom->blocksPerRead * BLOCK_SIZE+pageSize;

	realbuff.reset(new char[readBufferSz]);
	if (realbuff.get() == 0) {
		cerr << "thr_popper: Can't allocate space for a whole extent in memory" << endl;
		return 0;
	}

#if __WORDSIZE > 32
	//alignedbuff=(char*)((((ptrdiff_t)&realbuff[0] + pageSize - 1) / pageSize) * pageSize);
	// pagesize == (1 << 12)
	alignedbuff=(char*)((((ptrdiff_t)realbuff.get() >> 12) << 12) + pageSize);
#else
	//alignedbuff=(char*)(((((ptrdiff_t)&realbuff[0] & 0xffffffff) + pageSize - 1) / pageSize) * pageSize);
	alignedbuff=(char*)(((((ptrdiff_t)realbuff.get() >> 12) << 12) & 0xffffffff) + pageSize);
#endif

	idbassert(((ptrdiff_t)alignedbuff - (ptrdiff_t)realbuff.get()) < (ptrdiff_t)pageSize);
	idbassert(((ptrdiff_t)alignedbuff % pageSize) == 0);

	for ( ; ; ) {

		fr = iom->getNextRequest();
		lbid = fr->Lbid();
		ver = fr->Ver();
		flg = fr->Flg();
		blocksLoaded=0;
		blocksRead=0;
		dlen = fr->BlocksRequested();
		blocksRequested = fr->BlocksRequested();
		
		err = iom->lbidLookup(lbid, ver, flg, oid, offset);
		if (err < 0) {
			cerr << "lbid=" << lbid << " ver=" << ver << " flg=" << (flg ? 1 : 0) << endl;
			throw runtime_error("thr_popper: BRM lookup failure");
		}

		longSeekOffset=(uint64_t)offset * (uint64_t)fileBlockSize;
		totalRqst++;
		sz=0; 

		uint32_t readCount=0;
		uint32_t bytesRead=0;
		uint32_t jend = blocksRequested/iom->blocksPerRead;
		for (j = 0; j <= jend; j++) {

			blocksThisRead = std::min(dlen, iom->blocksPerRead);
			readSize = blocksThisRead * BLOCK_SIZE;

			acc = 0;
			while (acc < readSize) {
				i = readSize; //pread(fd, &alignedbuff[acc], readSize - acc, longSeekOffset);
				/* XXXPAT: Need to decide how to handle errors here */
				if (i < 0 && errno == EINTR) {
					continue;
				}
				else if (i < 0) {
					perror("thr_popper::read");
					return 0;			// shuts down this thread, 
									// probably not the right thing to do
				}
				else if (i == 0) {
					try {
						
					} catch (exception& exc) {
						cerr << "FileName Err:" << exc.what() << endl;
					}
					cerr << "thr_popper: Early EOF in file " << fileNamePtr << endl;
					return 0;
				}
				acc += i;
				longSeekOffset += (uint64_t)i;
				readCount++;
				bytesRead+=i;	
			} // while(acc...
			blocksRead+=blocksThisRead;

			for (i = 0; (unsigned)i < blocksThisRead; ++i) {
				if (fbm->insert( (lbid+i) + (j*iom->blocksPerRead), ver, (uint8_t*)&alignedbuff[i*BLOCK_SIZE]))
					++blocksLoaded;
			}

			dlen -= blocksThisRead;
		} // for (j...

		fr->BlocksRead(blocksRead);
		fr->BlocksLoaded(blocksLoaded);

		if (fr->data != 0 && blocksRequested == 1)
			memcpy(fr->data, alignedbuff, BLOCK_SIZE);

		pthread_mutex_lock(&fr->frMutex());
		fr->SetPredicate(fileRequest::COMPLETE);
		pthread_cond_signal(&fr->frCond());
		pthread_mutex_unlock(&fr->frMutex());

	} // for(;;)

	lFile.close();

	return 0;
} // end thr_popper

} // anonymous namespace

namespace dbbc {

ioManager::ioManager(FileBufferMgr& fbm,
					fileBlockRequestQueue& fbrq,
					int thrCount,
					int bsPerRead):
		blocksPerRead(bsPerRead),
		fIOMfbMgr(fbm),
		fIOMRequestQueue(fbrq)
{

	if (thrCount<=0)
		thrCount=1;
	
	if (thrCount > 256)
		thrCount=256;
	
	fConfig = Config::makeConfig();
    string val = fConfig->getConfig("DBBC", "IOMTracing");
	int temp=0;
    if (val.length()>0) temp=static_cast<int>(Config::fromText(val));
        
    if (temp > 0)
		fIOTrace=true;
	else
		fIOTrace=false;

	fThreadCount=thrCount;
	go();

} // ioManager

void ioManager::buildOidFileName(const BRM::OID_t oid, char* file_name) {

	if (fFileOp.getFileName(oid, file_name) != WriteEngine::NO_ERROR) {
		file_name[0]=0;
		throw std::runtime_error("fileOp.getFileName failed");
	}
}


BRM::LBID_t ioManager::lbidLookup(BRM::LBID_t lbid,
							BRM::VER_t verid,
							bool vbFlag,
							BRM::OID_t& oid,
							uint32_t& offset)
{
 int rc =  fdbrm.lookup(lbid, verid, vbFlag, oid, offset);
 return rc;
}

int ioManager::createReaders() {
	int realCnt=0;
	IOMThreadArg_t fThdArgArr[256];
	for (int idx=0; idx<fThreadCount; idx++){
		fThdArgArr[realCnt].iom = this;
		fThdArgArr[realCnt].thdId = realCnt;
		int ret = pthread_create(&fThreadArr[realCnt], 0, thr_popper, &fThdArgArr[realCnt]);
 		if (ret!=0)
 			perror("createReaders::pthread_create");
		else
			realCnt++;
	}
	fThreadCount=realCnt;
	return fThreadCount;
}


ioManager::~ioManager()
{
	stop();
}

void ioManager::go(void) {
	createReaders();
}


void ioManager::stop() {
	for(int idx=0; idx <fThreadCount; idx++) {
		pthread_detach(fThreadArr[idx]);
	}
}


fileRequest* ioManager::getNextRequest() {
	fileRequest* blk = 0;
	try {
		blk = fIOMRequestQueue.pop();
		return blk;
	} catch (exception& e) {
		cerr << "ioManager::getNextRequest() ERROR " << endl;
	}

	return blk;
	
}

}
// vim:ts=4 sw=4:
