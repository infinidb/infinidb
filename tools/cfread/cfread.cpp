//

#include <iostream>
#include <iomanip>
#include <boost/scoped_array.hpp>
#include <errno.h>
#define NDEBUG
#include <cassert>
#include "writeengine.h"

using namespace std;

void timespec_sub(const struct timespec &tv1,
				const struct timespec &tv2,
				struct timespec &diff) 
{
		if (tv2.tv_nsec < tv1.tv_nsec) {
			diff.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
			diff.tv_nsec = tv1.tv_nsec - tv2.tv_nsec;
		}
		else {
			diff.tv_sec = tv2.tv_sec - tv1.tv_sec;
			diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
		}
}

// TODO:
// add getopt for CLAs
// add threads for reading a file
// add threads for reading multiple files

// main()
//
int main(int argc, char **argv)
{
	uint64_t acc=0;
	uint64_t readBlocks=0; // read size ib blocks
	uint64_t readSize=0; // read size ib bytes
	uint64_t readBufferSz=0;
	const uint64_t blockSize=8192;
	char* alignedbuff=0;
 	boost::scoped_array<char> realbuff;	
	const unsigned pageSize = 4096; //getpagesize();
	WriteEngine::FileOp fFileOp;
	BRM::OID_t oid;
	char fname[256];
	struct timespec tm;
	struct timespec tm2;
	struct timespec tm3;
	struct timespec starttm;
	struct timespec endtm;
	struct timespec tottm;
	bool odirect=true;
	int fd = 0;
	char response='Y';

	if (argc <= 1) {
		cerr << "usage: testread <oid> <buffer size in blocks>" << endl;
		return -1;
	}

	oid=atoi(argv[1]);
	if (oid <=0)
		exit(-1);

	if (argc >=2) {
		readBlocks = atoi(argv[2]);
		if (readBlocks <= 0)
			readBlocks = 8;
	}

	if (argc >=4) {
		odirect=false;
	}

	readSize=readBlocks*blockSize;
	readBufferSz=readSize+pageSize;

	realbuff.reset(new char[readBufferSz]);
	if (realbuff.get() == 0) {
		cerr << "thr_popper: Can't allocate space for a whole extent in memory" << endl;
		return 0;
	}

	if (fFileOp.getFileName(oid, fname) != WriteEngine::NO_ERROR) {
		fname[0]=0;
		throw std::runtime_error("fileOp.getFileName failed");
	}
	else {
		cout << "Reading oid: " << oid << " od: " << odirect << " file: " << fname << endl;
	}

#if __LP64__
	alignedbuff=(char*)((((ptrdiff_t)realbuff.get() >> 12) << 12) + pageSize);
#else
	alignedbuff=(char*)(((((ptrdiff_t)realbuff.get() >> 12) << 12) & 0xffffffff) + pageSize);
#endif
	idbassert(((ptrdiff_t)alignedbuff - (ptrdiff_t)realbuff.get()) < (ptrdiff_t)pageSize);
	idbassert(((ptrdiff_t)alignedbuff % pageSize) == 0);

	if (odirect)
		fd=open(fname, O_RDONLY|O_DIRECT|O_LARGEFILE|O_NOATIME);
	else
		fd=open(fname, O_RDONLY|O_LARGEFILE|O_NOATIME);

	if (fd<0) {
		cerr << "Open failed" << endl;
		perror("open");
		throw runtime_error("Error opening file");
	}

	while (toupper(response) != 'N') {
	uint64_t i=1;
	uint64_t rCnt=0;
	clock_gettime(CLOCK_REALTIME, &starttm);
	while (i!=0) {
		//clock_gettime(CLOCK_REALTIME, &tm);
		i = pread(fd, alignedbuff, readSize, acc);
		//clock_gettime(CLOCK_REALTIME, &tm2);
		idbassert(i==0||i==readSize);
		idbassert(i%pageSize==0);
		idbassert(acc%pageSize==0);
		if (i < 0 && errno == EINTR) {
			timespec_sub(tm, tm2, tm3);
			cout << "* "
				<< i << " "
				<< right << setw(2) << setfill(' ') << tm3.tv_sec << "."
				<< right << setw(9) << setfill('0') << tm3.tv_nsec
				<< endl;
			continue;
		}
		else if (i < 0) {
			timespec_sub(tm, tm2, tm3);
			cout << "* i: "
				<< i << " sz: " << readSize << " acc: " << acc
				<< right << setw(2) << setfill(' ') << tm3.tv_sec << " "
				<< right << tm3.tv_nsec
				<< endl;
			perror("pread");
			//make loop exit
			i=0;
		}

		acc += i;
		if (i>0)
			rCnt++;

		//timespec_sub(tm, tm2, tm3);
		//cout
		//	<< i << " "
		//	<< right << setw(2) << setfill(' ') << tm3.tv_sec << " "
		//	<< right << tm3.tv_nsec
		//	<< endl;

	} // while(acc...

	clock_gettime(CLOCK_REALTIME, &endtm);
	timespec_sub(starttm, endtm, tottm);

	cout << "Total reads: " << rCnt
		<< " sz: " << acc/(1024*1024) << "MB"
		<< " tm: " << tottm.tv_sec << "secs "
		<< tottm.tv_nsec << "ns"
		<< endl;

	cout << "Repeat the last scan[Y,N]?" << endl;
	cin >> response;
	acc=0;
	
	} // while response...

	close(fd);
	return 0;

} //main

