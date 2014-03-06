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

//
// C++ Implementation: bcTest
//
// Description:  A simple Test driver for the Disk Block Buffer Cache
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>, (C) 2007
//
//

#include <vector>
#include <string>
#include <iomanip>
#include <iostream>
#include <sys/time.h>
#include <unistd.h>
#include <boost/thread/thread.hpp>

#include "blockrequestprocessor.h"
#include "blockcacheclient.h"
#include "stats.h"
#include "brm.h"
#include "logger.h"
#include "iomanager.h"

using namespace BRM;
using namespace dbbc;
using namespace std;
using namespace logging;
using namespace primitiveprocessor;

Stats* gPMStatsPtr=NULL;
bool gPMProfOn=false;
uint32_t gSession=0;
uint32_t lastRangeListIdx=0;

void timespec_sub(const struct timespec &tv1,
				const struct timespec &tv2,
				double &tm) 
{
	tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9*(tv2.tv_nsec - tv1.tv_nsec);
}

namespace primitiveprocessor
{
	Logger ml;
}


class BCTest {

public:

	struct OidRanges {
		OID_t oid;
		HWM_t hwm;
		LBIDRange_v ranges;
		OidRanges(const OID_t o, const HWM_t h, const LBIDRange_v r)
		{
			oid=o;
			hwm=h;
			ranges=r;
		}
	}; //struct OidRanges

	BCTest(const int cacheSz=64*1024, int readThr=2, int readAhead=1024);

	typedef OidRanges OidRanges_t;
	typedef vector<OidRanges_t>OidRangesList_t;
	OidRangesList_t OidRangesList;

	DBRM dbrm;
	uint32_t extentSize;
	BRM::OID_t maxOid;

	int fCacheSz;
	int fReadThr;
	int fReadAhead;
	uint32_t maxBlocksAvailable;
	uint32_t fExtentSize;

	void setUp();
	int LoadOid(const OidRanges_t& o, uint32_t& loadCount);
	void LoadLbid(const BRM::LBID_t lbid, const BRM::VER_t ver);
	int ReadOidRanges(const OidRanges_t& v);
	void ReadOidLbids(const BRM::LBID_t lbid, const BRM::VER_t ver);

	BlockRequestProcessor BRP;

}; // class BCTest


BCTest::BCTest(int cacheSz, int readThr, int readAhead) :
		fCacheSz(cacheSz),
		fReadThr(readThr),
		fReadAhead(readAhead),
 		BRP(fCacheSz, fReadThr, fReadAhead)
{
	setUp();
} // ctor

//
void BCTest::setUp()
{
	LBIDRange_v r;
	HWM_t hwm;
	OID_t oid=1000;
	extentSize = dbrm.getExtentSize();
	maxBlocksAvailable=0;
	int i=0;
	fExtentSize=dbrm.getExtentSize();

	while ( oid < 5000 )
	{
		int ret=0;
		ret = dbrm.lookup(oid, r);
		if (ret==0 && r.size()>0) {
			dbrm.getHWM(oid, hwm);
			maxBlocksAvailable+=(r.size()*extentSize);
			OidRanges_t oid_range(oid, hwm, r);
			OidRangesList.push_back(oid_range);
			//cout << "Setup i: " << i++ << " o: " << oid
			//	<< " r: " << ret << " s: " << r.size()
			//	<< " m: " << maxBlocksAvailable
			//	<< endl;
			hwm=0;
			r.clear();
		}
		oid++;
	}

	//cout << "\t" << OidRangesList.size() << " oid ranges loaded " << endl << endl;
	i=0;
} // setUp()

int BCTest::LoadOid(const OidRanges_t& o, uint32_t& loadCount)
{
	blockCacheClient bc(BRP);
	uint32_t rCount=0;

	for (uint32_t i =0; i<o.ranges.size() ; i++)
	{
		const InlineLBIDRange r={o.ranges[i].start, o.ranges[i].size};
		if (r.size>0) {
			bc.check(r, 0, rCount);
			//cout <<  "i: " << i << " c: " <<  rCount << " " << o.ranges[i].size << endl;
			loadCount+=rCount;
		}
		rCount=0;
	} // for

	//cout << "hwm: " << o.hwm << " tot: " << loadCount <<  " " << o.ranges.size() << endl;

	return loadCount;

} // LoadOid

int BCTest::ReadOidRanges(const OidRanges_t& v)
{

	blockCacheClient bc(BRP);
	int32_t readBlocks=0;
	int32_t missBlocks=0;
	//int ret;
	for(uint32_t i=0; i<v.ranges.size(); i++)
	{
		FileBuffer fb(-1, -1);
		const InlineLBIDRange r={v.ranges[i].start, v.ranges[i].size};
		for(int j=r.start; readBlocks<fCacheSz && j<r.start+r.size; j++)
		{
			//ret=0;
			//ret=bc.read(j, 0, fb);
			FileBuffer* ptr =bc.getBlockPtr(j, 0);
			if (ptr)
				readBlocks++;	
			else
				missBlocks++;	
		}

		//cout << " -- Read range idx: " << i << " hits: " << readBlocks << " miss: " << missBlocks << " hwm: " << v.hwm << endl;
	}

	return readBlocks;

} // ReadRange

// add one block to block cache
//
void BCTest::LoadLbid(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
	blockCacheClient bc(BRP);
	bool b;
	bc.check(lbid, ver, false, b);
} // LoadLbid

// get one block out of block cache
//
void BCTest::ReadOidLbids(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
	uint8_t d[8192]={'\0'};
	blockCacheClient bc(BRP);
	bc.read(lbid, ver, d);
} // ReadLbid


struct loadThr
{
	loadThr::loadThr(BCTest& bc, int reps=1) : fBC(bc), fReps(reps) {}

	void operator()()
	{
		uint32_t loadedBlocks=0;
		uint32_t oidBlocks;
		uint32_t i=0;	
		uint32_t rc=0;

		clock_gettime(CLOCK_REALTIME, &tm1);
		for(uint32_t j=0; j<fReps; j++)	
			for(i=0; /*loadedBlocks<cacheSize &&*/ i<fBC.OidRangesList.size(); i++)
			{
				oidBlocks=0;
				rc = fBC.LoadOid(fBC.OidRangesList[i], oidBlocks);
			/**
			cout << "."
				<< "-- " << i << " " << fBC.OidRangesList[i].oid
				<< " h: " << fBC.OidRangesList[i].hwm
				<< "/" << oidBlocks
				<< endl;
			**/
				loadedBlocks+=oidBlocks;
			}

		clock_gettime(CLOCK_REALTIME, &tm2);
		double tm3;
		timespec_sub(tm1, tm2, tm3);		
		lastRangeListIdx=i;

		cout << "loadtest ld: " << loadedBlocks
			<< " sz: " 		<< fBC.fCacheSz
			//<< " last: "	<< lastRangeListIdx
			<< " tm: " << right << setw(10) << fixed << tm3
			<< endl;

	} // operator()

	BCTest& fBC;
	uint32_t fReps;
	struct timespec tm1;
	struct timespec tm2;

};

struct readThr
{

	readThr::readThr(BCTest& bc, int reps=1) : fBC(bc), fReps(reps) {}

	void operator()()
	{
		uint32_t rc=0;
		uint32_t loadedBlocks=0;
		for (uint32_t k=0; k<fReps; k++) {
			for(uint32_t i=0; i<fBC.OidRangesList.size() ; i++)
			{
				rc = fBC.ReadOidRanges(fBC.OidRangesList[i]);
				cout << ".";
				//cout << "-- ReadTest " << OidRangesList[i].oid << " h: " << OidRangesList[i].hwm << "/" << rc << endl;
				loadedBlocks+=rc;
				rc=0;
			}
		}
		cout << "loadtest " << loadedBlocks << " " << rc << endl<<endl;

	} // operator()


	BCTest& fBC;
	uint32_t fReps;

};

void usage()
{
	cout << "testbc <cacheSz/1024> <reader threads> <read ahead> <client threads> <reps>" << endl;
}

//
int main(int argc, char* argv[]) {

	int cacheSz=128; // K number of blocks
	int thr=1;
	int ra=1024;
	int clients=1;
	int reps=1;

	if (argc > 1 && atoi(argv[1]) > 0)
		cacheSz=atoi(argv[1])*1024;

	if (argc > 2 && atoi(argv[2]) > 0)
		thr=atoi(argv[2]);

	if (argc > 3 && atoi(argv[3]) > 0)
		ra=atoi(argv[3]);

	if (argc > 4 && atoi(argv[4]) > 0)
		clients=atoi(argv[4]);

	if (argc > 5 && atoi(argv[5]) > 0)
		reps=atoi(argv[5]);

	BCTest bc(cacheSz, thr, ra);

	cout <<
		"Cache Size: " << cacheSz <<
		" read Threads: " << thr <<
		" read Ahead: " << ra <<
		" clients: " << clients <<
		" repetitions: " << reps <<
		" max Blocks: " << bc.maxBlocksAvailable <<
		endl;

// loader test
	struct loadThr loader1(bc, reps);
	vector<boost::thread*> v;

	for (int i=0; i<clients;i++) {
		boost::thread* th1 = new boost::thread(loader1);
		v.push_back(th1);
	}

	for (int i=0; i<clients;i++) {
		boost::thread* th1 = v[i];
		th1->join();
		delete th1;
	}

	v.clear();

// reader test


	return 0;

} // end main
