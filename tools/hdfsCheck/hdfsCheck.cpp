/*
 * foptest.cc
 *
 *  Created on: Jul 17, 2012
 *      Author: rtw
 */

#include <unistd.h>
#include <stdio.h>
#ifndef _MSC_VER
#include <stdio_ext.h>
#endif
#include <stdlib.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <syslog.h>

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>

#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "IDBPolicy.h"
using namespace idbdatafile;
using namespace std;

size_t BLK_SIZE=2048;
const size_t MAX_BLK_SIZE=1048576;
size_t MAX_BLOCK=1024;

int timeval_subtract (struct timeval *result, struct timeval *x, struct timeval *y)
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }
  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;}

void usage()
{
	cerr << "usage: hdfsCheck [-b:n:t:r:] <hdfs-plugin>" << endl;
	cerr << "       Required Arguments" << endl;
	cerr << "         <hdfs-plugin> - path to the HDFS plugin to be loaded" << endl;
	cerr << "       Options" << endl;
	cerr << "         -b <BLK_SIZE> block size in bytes" << endl;
	cerr << "         -n <num blocks> number of blocks to write/read" << endl;
	cerr << "         -t <timeout> how long to retry (in secs.) until we give up (default=30)" << endl;
	cerr << "         -r <dbroots> number of dbroot directories to check (default=0)" << endl;
}

struct foptest_opts
{
	string pluginFile;
	size_t blockSize;
	int    numBlocks;
	int    timeout;
	int    numDbRoots;

	foptest_opts() :
		pluginFile(""),
		blockSize(BLK_SIZE),
		numBlocks(64),
		timeout(30),
		numDbRoots(0)
		{;}
};

class TestRunner
{
public:
	TestRunner(int id, const foptest_opts& opts);
	~TestRunner();

	bool runTest( IDBDataFile::Types filetype, unsigned open_opts );

	const foptest_opts& runOpts() const { return m_opts; }

private:
	bool fillDefault();
	bool writeBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag);
	bool writeBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count);
	bool readBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag);
	bool readBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count);

	bool readTest( IDBDataFile::Types filetype );
	bool openByModeStrTest();
	bool writeTest( IDBDataFile::Types filetype );
	bool rdwrTest( IDBDataFile::Types filetype );
	bool hdfsRdwrExhaustTest();
	bool openWF( bool reopen=false );
	bool openRF();

	void reset();

	enum LogLevel {
		INFO,
		ERROR
	};
	void logMsg( LogLevel level, const string& msg );

	string		 m_fname;
	foptest_opts m_opts;
	unsigned char m_defbuf[MAX_BLK_SIZE];
	IDBDataFile*  m_file;
	unsigned     m_open_opts;
	int          m_id;
};

TestRunner::TestRunner(int id, const foptest_opts& opts) :
		m_opts(opts),
		m_file(NULL),
		m_open_opts(0),
		m_id(id)
{
	for( unsigned i = 0; i < BLK_SIZE; i++ )
	{
		m_defbuf[i] = 0xfe;
	}
}

void TestRunner::reset()
{
	delete m_file;
	m_file = 0;
}

struct TestCleanup
{
	TestCleanup( IDBDataFile::Types type, const string& dir, const string& file ) :
		m_fs( IDBFileSystem::getFs( type ) ),
		m_dir(dir),
		mm_file(file) {}

	~TestCleanup()
	{
		if( m_fs.exists( mm_file.c_str() ) )
			m_fs.remove(mm_file.c_str());
		assert( !m_fs.exists( mm_file.c_str() ) );
		if( m_fs.exists( m_dir.c_str() ) )
			m_fs.remove(m_dir.c_str());
		assert( !m_fs.exists( m_dir.c_str() ) );
	}

	IDBFileSystem& m_fs;
	string	m_dir;
	string	mm_file;
};

bool TestRunner::runTest( IDBDataFile::Types filetype, unsigned open_opts )
{
	m_open_opts = open_opts;
	ostringstream infostr;
	infostr << "Running test for file type "
			<< ( filetype == IDBDataFile::UNBUFFERED ? "Unbuffered" :
					( filetype == IDBDataFile::BUFFERED ? "Buffered" : "HDFS" ) );
	logMsg( INFO, infostr.str() );

	IDBFileSystem& fs = IDBFileSystem::getFs( filetype );

	// build the file name we are going to use
	ostringstream oss;
	// embed pid so that this is a new directory path
	oss << "/tmp/idbdf-dir-" << getpid() << "-" << m_id;
	string dir = oss.str();
	m_fname = dir + "/foobar";

	// instantiate this here so that we always clean up files we created no matter
	// where we exit the function from
	boost::scoped_ptr<TestCleanup> cleanup( new TestCleanup( filetype, dir, m_fname ) );

	logMsg( INFO, "Running writeTest" );
	bool returnval = true;
	returnval = writeTest( filetype );

	if( !returnval )
	{
		return false;
	}

	// going to check the size two different ways - once through the file and
	// once through the file system.
	unsigned fsize = m_file->size();
	if( fsize != m_opts.numBlocks * BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "bad file size from file " << fsize << " != " << m_opts.numBlocks * BLK_SIZE << "!";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// this deletes and closes the file - required to get accurate file size with
	// buffered IO and in HDFS from the file system
	reset();
	fsize = fs.size(m_fname.c_str());
	if( fsize != m_opts.numBlocks * BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "bad file size from fs " << fsize << " != " << m_opts.numBlocks * BLK_SIZE << "!";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	if( returnval )
	{
		logMsg( INFO, "Running readTest" );
		returnval = readTest( filetype );
	}

	if( returnval )
	{
		logMsg( INFO, "Running rdwrTest" );
		returnval = rdwrTest( filetype );
	}

	if( returnval && filetype == IDBDataFile::HDFS )
	{
		logMsg( INFO, "Running hdfsRdwrExhaustTest" );
		returnval = hdfsRdwrExhaustTest();
	}

	if( m_opts.numDbRoots > 0 )
	{
		logMsg( INFO, "Checking dbroots" );
		for( int i = 0; i < m_opts.numDbRoots; ++i )
		{
			ostringstream dbroot;
			dbroot << "/usr/local/Calpont/data" << i+1;
			if( !fs.exists(dbroot.str().c_str()) )
			{
				ostringstream msg;
				msg << "Could not locate dbroot directory " << dbroot.str();
				logMsg( ERROR, msg.str() );
				returnval = false;
			}
		}
	}

	list<string> dircontents;
	assert( fs.listDirectory( dir.c_str(), dircontents ) == 0 );
	ostringstream ldstr;
	ldstr << "Listed directory " << dir << ":";
	list<string>::iterator iend = dircontents.end();
	for( list<string>::iterator i = dircontents.begin(); i != iend; ++i )
	{
		ldstr << (*i) << ",";
	}
	logMsg( INFO, ldstr.str() );
	assert( dircontents.size() == 1 );

	// now check a bogus path and make sure it returns -1
	assert( fs.listDirectory( "/this-is-a-bogus-directory", dircontents ) == -1 );
	assert( fs.remove( "/this-is-a-bogus-directory" ) == 0 );
	assert( !fs.isDir( "/this-is-a-bogus-directory" ));
	assert( !fs.isDir( m_fname.c_str() ));

	if( returnval )
	{
		logMsg( INFO, "All tests passed!" );
	}

	reset();
	return returnval;
}

bool TestRunner::readTest( IDBDataFile::Types filetype )
{
	reset();
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts);
	if( !m_file )
	{
		ostringstream errstr;
		errstr << "Unable to open " << m_fname << " for reading";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// check the mtime method.
	time_t now = time(0);
	time_t mtime = m_file->mtime();
	assert( (now-mtime) <= 3 );

    struct timeval starttime,endtime,timediff;
	gettimeofday(&starttime,0x0);

	for( int i = 0; i < m_opts.numBlocks; ++i)
	{
		if( !readBlock(i, m_defbuf, i) )
			return false;
	}

    gettimeofday(&endtime,0x0);
	timeval_subtract(&timediff,&endtime,&starttime);
	float secs = timediff.tv_sec + (timediff.tv_usec * 0.000001);

	ostringstream infostr;
	infostr << "Read " << m_opts.numBlocks * BLK_SIZE << " bytes in " << secs << " secs, ";
	infostr << "Throughput = " << setprecision(3) << ((m_opts.numBlocks * BLK_SIZE) / 1000000.0) / secs << "MB/sec";
	logMsg( INFO, infostr.str() );

	return true;
}

bool TestRunner::openByModeStrTest()
{
	// in this test we want to check the alternate open modes available for buffered i/o
	// this test is only run if we are doing buffered I/O and expects it is run after the
	// write test to guarantee the file is there
	reset();
	m_file = IDBDataFile::open(IDBDataFile::BUFFERED, m_fname.c_str(), "r+b", m_open_opts);
	if( !m_file )
	{
		ostringstream errstr;
		errstr << "Unable to open " << m_fname << " for read/write";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// keep this fairly simple - read a block then write a block

	size_t readct = m_file->read(m_defbuf,BLK_SIZE);
	if( readct != BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "Only read " << readct << " bytes, expected 4";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if( m_defbuf[0] != (unsigned char) 0 )
	{
		ostringstream errstr;
		errstr << "Data error - expected " << 0 << ", read " << (int) m_defbuf[0];
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// we should be at block 1
	long filepos = m_file->tell();
	if( filepos != long(BLK_SIZE) )
	{
		ostringstream errstr;
		errstr << "File position not at correct block, " << filepos << " != " << BLK_SIZE;
		logMsg( ERROR, errstr.str() );
		return false;
	}

	m_defbuf[0] = 1;
	size_t bytes_written = m_file->write(m_defbuf, BLK_SIZE);
	if( bytes_written != BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "Only wrote " << bytes_written << " bytes, expected 4";
		logMsg( ERROR, errstr.str() );
		return false;
	}

    return true;
}

bool TestRunner::writeTest( IDBDataFile::Types filetype )
{
	reset();
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "w", m_open_opts);
	if( !m_file )
	{
		ostringstream errstr;
		errstr << "Unable to open " << m_fname << " for writing";
		logMsg( ERROR, errstr.str() );
		return false;
	}

    struct timeval starttime,endtime,timediff;
	gettimeofday(&starttime,0x0);

	for( int i = 0; i < m_opts.numBlocks; ++i)
	{
		if( !writeBlock(i, m_defbuf, i) )
			return false;
	}

	gettimeofday(&endtime,0x0);
	timeval_subtract(&timediff,&endtime,&starttime);
	float secs = timediff.tv_sec + (timediff.tv_usec * 0.000001);

	ostringstream infostr;
	infostr << "Wrote " << m_opts.numBlocks * BLK_SIZE << " bytes in " << secs << " secs, ";
	infostr << "Throughput = " << setprecision(3) << ((m_opts.numBlocks * BLK_SIZE) / 1000000.0) / secs << "MB/sec";
	logMsg( INFO, infostr.str() );

	return true;
}

bool TestRunner::rdwrTest( IDBDataFile::Types filetype )
{
	reset();
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "r+", m_open_opts);
	if( !m_file )
	{
		ostringstream errstr;
		errstr << "Unable to open " << m_fname << " for reading";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	struct drand48_data d48data;
    srand48_r(0xdeadbeef, &d48data);

    // we will write to 5 random blocks and then come back and
    // verify the contents and undo them
    int blocks_to_touch = min( 5, m_opts.numBlocks);
    vector<int> touched;
    for( int i = 0; i < blocks_to_touch; ++i )
    {
    	long int blk_num;
    	// we need to make sure all the blocks we touch are unique or
    	// the pattern used by this test won't work
    	bool found = false;
    	while(!found)
    	{
    		lrand48_r( &d48data, &blk_num);
    		blk_num = blk_num % m_opts.numBlocks;
    		vector<int>::iterator pos = find( touched.begin(), touched.end(), blk_num );
    		if( pos == touched.end())
    			found = true;
    	}

    	if( m_file->seek(blk_num * BLK_SIZE, SEEK_SET) )
    	{
    		ostringstream errstr;
    		errstr << "failed to seek block " << blk_num;
    		logMsg( ERROR, errstr.str() );
    		return false;
    	}

    	unsigned char writeval = 0xb0;
    	size_t writect = m_file->write(&writeval,1);
       	if( writect != 1 )
    	{
    		ostringstream errstr;
    		errstr << "Only wrote " << writect << " bytes, expected 1";
    		logMsg( ERROR, errstr.str() );
    		return false;
    	}

       	touched.push_back(blk_num);
    }

    m_file->flush();

    for( int i = 0; i < (int) touched.size(); ++i )
    {
    	unsigned char readbuf;
    	size_t readct = m_file->pread(&readbuf,touched[i] * BLK_SIZE,1);
    	if( readct != 1 || readbuf != (unsigned char) 0xb0 )
    	{
    		ostringstream errstr;
    		errstr << "Error reading expected value, readct=" << readct << " bytes, value" << (int) readbuf;
    		logMsg( ERROR, errstr.str() );
    		return false;
    	}

    	readbuf = touched[i];

    	if( m_file->seek(-1, SEEK_CUR) )
    	{
    		ostringstream errstr;
    		errstr << "failed to seek -1";
    		logMsg( ERROR, errstr.str() );
    		return false;
    	}

    	size_t writect = m_file->write(&readbuf,1);
       	if( writect != 1 )
    	{
    		ostringstream errstr;
    		errstr << "Only wrote " << writect << " bytes, expected 1";
    		logMsg( ERROR, errstr.str() );
    		return false;
    	}
   }

    return true;
}

bool TestRunner::hdfsRdwrExhaustTest()
{
	// this is going to be a self-contained test that attempts to test
	// all logic inherent in HdfsRdwr

	// choose a new filename that is specific to our thread
	ostringstream oss;
	// embed pid so that this is a new directory path
	oss << "/tmp/hdfsrdwr-" << getpid() << "-" << m_id;
	string newpath = oss.str();

	// open a file with arbitrarily small buffer
	IDBDataFile* file = IDBDataFile::open(IDBDataFile::HDFS, newpath.c_str(), "r+", 0, 8);
	assert( file );

	// check various empty file conditions
	assert( file->size() == 0 );
	assert( file->tell() == 0 );
	assert( file->seek(-1, SEEK_CUR) == -1);
	assert( file->seek(0, SEEK_SET) == 0);
	unsigned char buf[4];
	assert( file->read(buf, 4) == 0);

	// write some data
	buf[0] = 0xde; buf[1] = 0xad; buf[2] = 0xbe; buf[3] = 0xef;
	assert( file->write(buf, 4) == 4);
	assert( file->size() == 4 );
	assert( file->tell() == 4 );
	assert( file->truncate(-1) == -1 );

	// now make file empty again
	assert( file->truncate(0) == 0 );
	assert( file->size() == 0 );
	assert( file->seek(0, SEEK_SET) == 0);
	assert( file->tell() == 0 );
	assert( file->read(buf, 4) == 0);

	// write data again, this time exactly up to allocated size
	assert( file->write(buf, 4) == 4);
	assert( file->write(buf, 4) == 4);
	assert( file->size() == 8 );
	assert( file->tell() == 8 );

	// truncate back to 4
	assert( file->truncate(4) == 0 );
	assert( file->size() == 4 );
	assert( file->seek(4, SEEK_SET) == 0);
	assert( file->tell() == 4 );

	// now trigger a buffer reallocation
	assert( file->write(buf, 4) == 4);
	assert( file->write(buf, 4) == 4);
	assert( file->size() == 12 );

	// now delete and close.
	delete file;

	// check the file size through the file system
	IDBFileSystem& fs = IDBFileSystem::getFs( IDBDataFile::HDFS );
	assert( fs.size( newpath.c_str() ) == 12);

	// open again - the file is bigger than the default buffer so it triggers alternate
	// logic in the constructor
	file = IDBDataFile::open(IDBDataFile::HDFS, newpath.c_str(), "r+", 0, 8);
	assert( file );
	assert( file->size() == 12);
	unsigned char newbuf[4];
	assert( file->pread(newbuf, 4, 4) == 4);
	assert( newbuf[0] == 0xde && newbuf[1] == 0xad && newbuf[2] == 0xbe &&newbuf[3] == 0xef);
	delete file;

	fs.remove(newpath.c_str());

	return true;
}

TestRunner::~TestRunner()
{
}

bool TestRunner::fillDefault()
{
	return true;
}

bool TestRunner::writeBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag)
{
	buf[0] = tag;
	size_t rc = m_file->write(buf, BLK_SIZE);

	if (rc != BLK_SIZE)
	{
		ostringstream errstr;
		errstr << "writeBlock failed for block " << blocknum << ", wrote " << rc << " bytes, expecting " << BLK_SIZE;
		logMsg( ERROR, errstr.str() );
		return false;
	}

	return true;
}

bool TestRunner::writeBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count)
{
	for (unsigned i = 0; i < count; ++i)
	{
		writeBlock(blocknum+i, buf, tag+i);
	}
	// should actually aggregate return values but using messages for now
	return true;
}

bool TestRunner::readBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag)
{
	size_t rc = m_file->pread(buf,blocknum*BLK_SIZE,BLK_SIZE);
	//cout << "DEBUG: read " << rc << " bytes at offset " << blocknum * BLK_SIZE << endl;

	if (rc != BLK_SIZE)
	{
		ostringstream errstr;
		errstr << "readBlock failed for block " << blocknum << ", read " << rc << " bytes, expecting " << BLK_SIZE;
		logMsg( ERROR, errstr.str() );
		return false;
	}
	else if (tag != buf[0])
	{
		ostringstream errstr;
		errstr << "read tag 0x" << setw(2) << hex << setfill('0') << (int) buf[0] << " at block " << dec
				<< blocknum << ", expected 0x" << (int) tag;
		logMsg( ERROR, errstr.str() );
		return false;
	}

	return true;
}

bool TestRunner::readBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count)
{
	for (unsigned i = 0; i < count; ++i)
	{
		readBlock(blocknum+i, buf, (unsigned char)(tag+i));
	}
	// should actually aggregate return values but using messages for now
	return true;
}

void TestRunner::logMsg( LogLevel level, const string& msg )
{
	cout << "Test-" << m_id << ":" << ((level == INFO) ? "INFO:" : "ERROR:") << msg << endl;
}

void logSuccess()
{
	::openlog("hdfsCheck", 0|LOG_PID, LOG_LOCAL1);
	::syslog(LOG_INFO, "All HDFS tests passed!");
	::closelog();
}

void logFailure(const char* msg)
{
	ostringstream oss;
    oss << "hdfsCheck failed: " << msg;
	::openlog("hdfsCheck", 0|LOG_PID, LOG_LOCAL1);
	::syslog(LOG_CRIT, oss.str().c_str());
	::closelog();
}

int main(int argc, char** argv)
{
	foptest_opts opts;

	int c;
    while ((c = getopt (argc, argv, "b:n:t:r:")) != -1)
    	switch (c)
        {
    		case 'b':
    			opts.blockSize = atoi(optarg);
    			if( opts.blockSize > MAX_BLK_SIZE )
    			{
    				opts.blockSize = MAX_BLK_SIZE;
    				cout << "WARNING: block size exceeds max, using " << opts.blockSize << endl;
    			}
    			else
    			{
    				cout << "INFO: using BLK_SIZE " << opts.blockSize << endl;
    			}
    			break;
    		case 'n':
    			opts.numBlocks = atoi(optarg);
    			cout << "INFO: will operate on " << opts.numBlocks << " blocks" << endl;
    			break;
    		case 't':
    			opts.timeout = atoi(optarg);
    			cout << "INFO: setting timeout to " << opts.timeout << " secs." << endl;
    			break;
    		case 'r':
    			opts.numDbRoots = atoi(optarg);
    			cout << "INFO: Will check " << opts.numDbRoots << " dbroot directories" << endl;
    			break;
        	default:
        		usage();
				logFailure("bad command-line option");
        		return 1;
        }

	if( optind != argc - 1 )
	{
		usage();
		logFailure("missing hdfs-plugin argument");
		return 1;
	}
	opts.pluginFile = argv[optind];

	try
	{
		// init the library with logging enabled
		std::string hdfsRdwrScratch = "/tmp/rdwr_scratch"; 
    	IDBPolicy::init( true, true, hdfsRdwrScratch, 2147483648 );
	
		if( !IDBPolicy::installPlugin(opts.pluginFile) )
		{
			cerr << "ERROR: unable to install HDFS plugin " << opts.pluginFile << endl;
			return -1;
		}
	}
	catch (...)
	{
		logFailure("failure to initialize IDBPolicy and/or load HDFS plugin");
		return -1;
	}

    struct timeval starttime,endtime,timediff;
	gettimeofday(&starttime,0x0);

	bool done = false;

	while ( !done )
	{
		// check to see if timeout has expired
	    TestRunner trun(0, opts);
		try
		{
			if( trun.runTest( IDBDataFile::HDFS, 0 ) )
			{
				done = true;
				cout << "INFO: All HDFS checks passed!" << endl;
			}
		}
		catch (...)
		{
			; // treat an exception thrown as a failure
		}
	    if( !done )
	    {
		    gettimeofday(&endtime,0x0);
			timeval_subtract(&timediff,&endtime,&starttime);
			float secs = timediff.tv_sec + (timediff.tv_usec * 0.000001);
			if( secs < opts.timeout )
			{
				// wait 5 seconds before we try again
		    	sleep(5);
			}
			else
			{
	    		cerr << "ERROR: timeout out after unsuccessful HDFS tests" << endl;
				logFailure("One or more HDFS tests failed");
	    		return -1;
			}
	    }
	}

	// only way we get here is if everything passes
	logSuccess();
	return 0;
}
