//

#include <iostream>
#include <iomanip>
#include <getopt.h>
#include <errno.h>
#include <boost/thread.hpp>
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
// add threads for reading a file
// add threads for reading multiple files

struct readThr
{
	public:
	readThr(const int oid, const int rSize)
	{
		fblockSize=8192;
		fpageSize=getpagesize();
		freadBlocks=rSize;
		freadSize=rSize*fblockSize; // read size ib bytes
		freadBufferSz=freadSize+fpageSize;
		falignedbuff=0;
		foid=oid;
		facc=0;
		memset(fname,0, sizeof(fname));
		memset((char*)&ftm, 0, sizeof(ftm));
		memset((char*)&ftm2, 0, sizeof(ftm2));
		memset((char*)&ftm3, 0, sizeof(ftm3));
		memset((char*)&fstarttm, 0, sizeof(fstarttm));
		memset((char*)&fendtm, 0, sizeof(fendtm));
		memset((char*)&ftottm, 0, sizeof(ftottm));
		fodirect=true;
		fd = 0;
		cout << "o: " << foid << " r: " << freadBlocks << " b: " << freadBufferSz << endl;
	}

	void operator() ()
	{

		WriteEngine::FileOp fFileOp;
		char frealbuff[freadBufferSz];
		memset(frealbuff, 0, freadBufferSz);
		if (frealbuff==0) {
			cerr << "thr_popper: Can't allocate space for a whole extent in memory" << endl;
			return;
		}

		if (fFileOp.getFileName(foid, fname) != WriteEngine::NO_ERROR) {
			fname[0]=0;
			throw std::runtime_error("fileOp.getFileName failed");
		}
		else {
			cout << "Reading oid: " << foid << " od: " << fodirect << " file: " << fname << endl;
		}

#if __LP64__
		falignedbuff=(char*)((((ptrdiff_t)frealbuff >> 12) << 12) + fpageSize);
#else
		falignedbuff=(char*)(((((ptrdiff_t)frealbuff >> 12) << 12) & 0xffffffff) + fpageSize);
#endif
		idbassert(((ptrdiff_t)falignedbuff - (ptrdiff_t)frealbuff) < (ptrdiff_t)fpageSize);
		idbassert(((ptrdiff_t)falignedbuff % fpageSize) == 0);

		if (fodirect)
			fd=open(fname, O_RDONLY|O_DIRECT|O_LARGEFILE|O_NOATIME);
		else
			fd=open(fname, O_RDONLY|O_LARGEFILE|O_NOATIME);

		if (fd<0) {
			cerr << "Open failed" << endl;
			perror("open");
			throw runtime_error("Error opening file");
		}

		uint64_t i=1;
		uint64_t rCnt=0;

		clock_gettime(CLOCK_REALTIME, &fstarttm);
		while (i>0) {
			clock_gettime(CLOCK_REALTIME, &ftm);
			i = pread(fd, falignedbuff, freadSize, facc);
			clock_gettime(CLOCK_REALTIME, &ftm2);

			idbassert(i==0||i==freadSize);
			idbassert(i%fpageSize==0);
			idbassert(facc%fpageSize==0);

			if (i < 0 && errno == EINTR)
			{
				timespec_sub(ftm, ftm2, ftm3);
				cout << "* "
					<< i << " "
					<< right << setw(2) << setfill(' ') << ftm3.tv_sec << "."
					<< right << setw(9) << setfill('0') << ftm3.tv_nsec
					<< endl;
				continue;
			}
			else if (i < 0)
			{
				timespec_sub(ftm, ftm2, ftm3);
				cout << "* i: "
					<< i << " sz: " << freadSize << " acc: " << facc
					<< right << setw(2) << setfill(' ') << ftm3.tv_sec << " "
					<< right << ftm3.tv_nsec
					<< endl;
				perror("pread");
			}

			facc += i;
			if (i>0)
				rCnt++;
			/**			
			timespec_sub(ftm, ftm2, ftm3);
			cout
				<< rCnt << " " << facc/(1024*1024)
				<< right << setw(2) << setfill(' ') << ftm3.tv_sec << "."
				<< right << ftm3.tv_nsec << " i: " << i/(1024*1024)
				<< endl;
			**/

		} // while(acc...

		clock_gettime(CLOCK_REALTIME, &fendtm);
		timespec_sub(fstarttm, fendtm, ftottm);

		cout << "Total reads: " << rCnt
			<< " sz: " << facc/(1024*1024) << "MB"
			<< " tm: " << ftottm.tv_sec << "secs "
			<< ftottm.tv_nsec << "ns"
			<< endl;

		facc=0;
		close(fd);
	} // operator()

	public:
	uint64_t facc;
	uint64_t freadBlocks; // read size ib blocks
	uint64_t freadSize; // read size ib bytes
	uint64_t freadBufferSz;
	uint64_t fblockSize;
	char* falignedbuff;
	unsigned fpageSize ;
	BRM::OID_t foid;
	char fname[256];
	struct timespec ftm;
	struct timespec ftm2;
	struct timespec ftm3;
	struct timespec fstarttm;
	struct timespec fendtm;
	struct timespec ftottm;
	bool fodirect;
	int fd;

}; // struct readThr

//
void usage()
{
	cerr << "usage: mtread -o <oid> -s <read size in blocks>" << endl;
}

// main()
//
int main(int argc, char **argv)
{
	int ch=0;
	int readBlocks=0;
	BRM::OID_t oid=0;
	std::vector<BRM::OID_t> oidList;

	enum CLA_ENUM {
		OID=(int)0,
		READSIZE
	};

	//longopt struct
	//struct option {
    //const char *name;
    //int has_arg;
    //int *flag;
    //int val;
	//};

	static struct
	option long_options[] =
	{
		//{const char *name, 	int has_arg, 	int *flag,	int val},
		{"oid", 		required_argument, 		NULL, OID},
		{"rsize", 		required_argument, 		NULL, READSIZE},
		{0, 					0, 				0, 			0}
	};

	if (argc <= 1) {
		return -1;
	}

	// process command line arguments
	while( (ch = getopt_long_only(argc, argv, "o:s:", long_options, NULL)) != -1 )
	{
		//pid_t pidId = getpid();
		switch (ch) {

			case OID:
			case 'o':
				oid=atoi(optarg);
				oidList.push_back(oid);
				cout << "oid: " << optarg << endl;
				break;

			case READSIZE:
			case 's':
				readBlocks = atoi(optarg);
				cout << "read size: " << optarg << endl;
				if (readBlocks <= 0)
					readBlocks = 1;
				break;

			case '?':
			default:
				cout << "optarg " << optarg << endl;
				usage();
				break;

		} // switch

	} // while...


	uint32_t idx=0;
	std::vector<boost::thread*> thrList;

	while(idx < oidList.size()) {
		struct readThr rdr(oidList[idx++], readBlocks);
		boost::thread* thr = new boost::thread(rdr);
		thrList.push_back(thr);
	}

	idx=0;
	while(idx<thrList.size()) {
		boost::thread* thr = thrList[idx++];
		thr->join();
		delete thr;
	}

	thrList.clear();

} //main

