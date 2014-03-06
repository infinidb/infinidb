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
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>, (C) 2007
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <vector>
#include <string>
#include <iostream>
#include <sys/time.h>
#include <unistd.h>

#include "blockcacheclient.h"
#include "stats.h"
#include "brm.h"
using namespace BRM;
using namespace dbbc;
using namespace std;

Stats* gPMStatsPtr=NULL;
bool gPMProfOn=false;
uint32_t gSession=0;

int fLoops=1;
int thr_cnt=1;
uint64_t bfoundTot=0;
uint64_t bnfoundTot=0;
uint64_t rfoundTot=0;
uint64_t rnfoundTot=0;
uint64_t rangeOpCountTot=0;
uint64_t blockOpCountTot=0;
uint64_t noOpCountTot=0;

struct thr_wait_struct {
	int predicate;
	pthread_mutex_t fMutex;
	pthread_cond_t fCond;
	vector<LBIDRange_v> range_thr;
};
typedef thr_wait_struct thr_wait_t;

const int32_t cacheSize=175000;
BlockRequestProcessor BRP(cacheSize, 4, 16);
BRM::VER_t ver=0xFFFF;
u_int64_t totBlocks=0;

void* thr_client(void* clientArgs) {
	blockCacheClient bc(BRP);
	uint64_t bfound=0;
	uint64_t bnfound=0;
	uint64_t rfound=0;
	uint64_t rnfound=0;
	uint64_t rangeOpCount=0;
	uint64_t blockOpCount=0;
	uint64_t noOpCount=0;
	thr_wait_t* clientWait = (thr_wait_t*)clientArgs;
	struct timeval tv, tv2;
	uint32_t randstate=0;
	randstate = static_cast<uint32_t>(tv.tv_usec);
	pthread_mutex_lock(&clientWait->fMutex);
	clientWait->predicate++;
	pthread_mutex_unlock(&clientWait->fMutex);
	vector<LBIDRange_v>& range_thr = clientWait->range_thr;

	gettimeofday(&tv, NULL);
	int j=0;
	int s=0;
	uint8_t fb[8192]={0};
	LBIDRange_v& r=range_thr[0];
	for(int idx=0; idx<fLoops; idx++) {
		for (int jdx=0; jdx<range_thr.size(); jdx++) {
			uint32_t lbid=0;
			bool b;
			int ret=0;
			r = range_thr[jdx];
			for(int l=0; l<r.size(); l++) {
				for(int m=r[l].start;m<r[l].start+r[l].size; m++) {
					//ret=bc.getBlock(m, ver, &fb, false, b);
					ret=bc.read(m, ver, &fb);
					if (ret) {
						bfound++;
					} else {
						bnfound++;
					}
				}
			}
		}
	}

	gettimeofday(&tv2, NULL);
	time_t tm=time(0);
	char t[50];
	ctime_r(&tm, t);
	t[strlen(t)-1]=0;
	uint32_t elTime=tv2.tv_sec-tv.tv_sec;
	uint64_t avgTot=0;
	uint64_t rangeAvg=0;
	uint64_t blkAvg=0;

	if (elTime>0) {
		avgTot=(bfound+rfound)/elTime;
		rangeAvg=(rfound)/elTime;
		blkAvg=(bfound)/elTime;
	} else {
		avgTot=bfound+rfound;
		rangeAvg=rfound;
		blkAvg=bfound;
	}

	cout << "thr(" << pthread_self() << ") tm " << t << " " << (tv2.tv_sec-tv.tv_sec) << endl <<
		"\tBlk: c " << blockOpCount << " pass " << bfound << " fail " << bnfound <<
		" Blks/Sec Blk " << blkAvg << endl << endl;

	pthread_mutex_lock(&clientWait->fMutex);
	bfoundTot+=bfound;
	bnfoundTot+=bnfound;
	rfoundTot+=rfound;
	rnfoundTot+=rnfound;
	rangeOpCountTot+=rangeOpCount;
	blockOpCountTot+=blockOpCount;
	noOpCountTot+=noOpCount;
	clientWait->predicate--;
	pthread_cond_signal(&clientWait->fCond);
	pthread_mutex_unlock(&clientWait->fMutex);

	return NULL;
	
} // end thr_client

void LoadRange(const LBIDRange_v& v, uint32_t& loadCount)
{
	blockCacheClient bc(BRP);

	uint32_t rCount=0;
	for (uint32_t i =0; i<v.size() ; i++)
	{
		const InlineLBIDRange r={v[i].start, v[i].size};
		if (r.size<=1024) {
			bc.check(r, ver, rCount );
			loadCount+=rCount;
		}
		rCount=0;

	}

}

void ReadRange(const LBIDRange_v& v)
{
	blockCacheClient bc(BRP);
	int found=0;
	int notfound=0;
	int ret=0;
	for(uint32_t i=0; i<v.size(); i++)
	{
		const InlineLBIDRange r={v[i].start, v[i].size};
		FileBuffer fb(-1, -1);
		for(int j=r.start; j<r.start+r.size; j++)
		{
			if (r.size > 1024)
				continue;
			ret=bc.read(j, ver, fb);
			if (ret)
				found++;
			else
				notfound++;
			ret=0;
		}
		
		totBlocks+=found;	
		totBlocks+=notfound;	
		found=0;
		notfound=0;
	}

}

void  LoadLbid(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
	blockCacheClient bc(BRP);
	bool b;
	bc.check(lbid, ver, false, b);
}

void ReadLbid(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
	static int found=0, notfound=0;
	uint8_t d[8192];
	blockCacheClient bc(BRP);
	//FileBuffer fb(-1, -1);
	//bc.read(lbid, ver, fb);
	int ret = bc.read(lbid, ver, d);
	if (ret)
		found++;
	else
		notfound++;

	if ((found+notfound)%10000==0)
			cout << "found " << found << " notfound " << notfound << endl;
}

//
int main(int argc, char *argv[]) {

	if (argc>=2) thr_cnt=atoi(argv[1]);
	if (argc>=3) fLoops=atoi(argv[2]);
	if (thr_cnt<=0) thr_cnt=1;
	if (thr_cnt>1024) thr_cnt=1024;
	if (fLoops<=0) fLoops=1;

	LBIDRange_v r;
	vector<LBIDRange_v> ranges;
	DBRM dbrm;
	uint32_t hwm, lowfbo, highfbo, fbo, extentSize, lowlbid;
	struct timeval tv, tv2;

	cout << "Starting " << endl;
	extentSize = dbrm.getExtentSize();
	BRM::OID_t oid=3000;
	uint32_t totalExt=0;
	do {
		int ret = dbrm.lookup(oid, r);
		if (ret==0 && r.size() > 0) {
			lowlbid = (r[0].start/extentSize) * extentSize;
			dbrm.lookup(r[0].start, ver, false, oid, fbo);  // need the oid
			dbrm.getHWM(oid, hwm);
			lowfbo = fbo - (r[0].start - lowlbid);
			highfbo = lowfbo + extentSize;
			r[0].start=lowlbid;
			if (hwm < highfbo)
				r[0].size = hwm - lowfbo + 1;
			else
				r[0].size = extentSize;
			for (uint32_t idx=0; idx<r.size(); idx++)
				totalExt+=r[idx].size;
			ranges.push_back(r);
		}
		oid++;
	}
	while ( (r.size() > 0 || oid < 900000) );

	cout << ranges.size() << " ranges found" << endl;

	gettimeofday(&tv, NULL);
	uint32_t blksLoaded=0;
	int rangesLoaded=0;
	for (uint32_t i =0; i<ranges.size() && blksLoaded < cacheSize; i++)
	{
		LoadRange(ranges[i], blksLoaded);
		rangesLoaded++;
	}
	cout << endl;

	gettimeofday(&tv2, NULL);
	cout << "Loaded: " <<  blksLoaded << " blks " << rangesLoaded << " ranges sec: " << tv2.tv_sec - tv.tv_sec <<endl;

	while (ranges.size() > rangesLoaded) ranges.pop_back();

#ifdef BLAH
	for (uint32_t i =0; i<ranges; i++)
		ReadRange(ranges[i]);

	for (uint32_t i =0; i<ranges.size(); i++)
	{
		LBIDRange_v rv=ranges[i];
		for(uint32_t j=0; j < rv.size(); j++)
		{
			const InlineLBIDRange l = {rv[j].start, rv[j].size};
			for(uint32_t k=l.start; k<l.start+l.size; k++)
			{
				LoadLbid(k, ver);
				ReadLbid(k, ver);
			}
		}
	}
#endif

	pthread_t thr_id[thr_cnt];
	thr_wait_t thr_wait={0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, ranges};
	
	//start threads running
	cout << "Starting driver threads" << endl;
	gettimeofday(&tv, NULL);
	memset(thr_id, 0, thr_cnt*(sizeof(pthread_t)));
	for(int i=0; i<thr_cnt; i++) {
		pthread_create(&thr_id[i], NULL, thr_client, &thr_wait);
	}

	// waiting until all threads have indicated completion
	pthread_mutex_lock(&thr_wait.fMutex);
	while (thr_wait.predicate>0) {
		pthread_cond_wait(&thr_wait.fCond, &thr_wait.fMutex);
	}
	pthread_mutex_unlock(&thr_wait.fMutex);

	// join threads back to main
	for(int i=0; i<thr_cnt; i++) {
		pthread_join(thr_id[i], NULL);
	}

	gettimeofday(&tv2, NULL);
	time_t tm=time(0);
	char t[50];
	ctime_r(&tm, t);
	t[strlen(t)-1]=0;
	uint32_t elTime=tv2.tv_sec-tv.tv_sec;
	uint64_t total = bfoundTot + rfoundTot;	
	uint64_t avgTot=0;
	uint64_t rangeAvg=0;
	uint64_t blkAvg=0;

	if (elTime>0) {
		avgTot=(bfoundTot+rfoundTot)/elTime;
		rangeAvg=(rfoundTot)/elTime;
		blkAvg=(bfoundTot)/elTime;
	} else {
		avgTot=bfoundTot+rfoundTot;
		rangeAvg=rfoundTot;
		blkAvg=bfoundTot;
	}
	
	cout << "Summary tm " << t << " " << (tv2.tv_sec-tv.tv_sec) << endl <<
		"\tBlk: c " << blockOpCountTot << " pass " << bfoundTot << " fail " << bnfoundTot << 
		//"\tRng: c "<< rangeOpCountTot << " pass " << rfoundTot << " fail " << rnfoundTot << endl <<
		//"\tNoOp: c " << noOpCountTot << " Total " << total << endl <<
		//"\tblks/sec Blk " << blkAvg << " Rng " << rangeAvg << " Tot " << avgTot << " Thr " << avgTot/thr_cnt <<  endl << endl;
		" Blks/Sec Blk " << blkAvg << " Thr " << avgTot/thr_cnt << endl << endl;
			

	return 0;
	
} // end main
