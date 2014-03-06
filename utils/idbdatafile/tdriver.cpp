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

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "IDBPolicy.h"

using namespace std;
using namespace idbdatafile;

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
	cerr << "usage: fophdfs [adpsrh:b:t:cvuen:l:m:] <fname>" << endl;
	cerr << "         -d use O_DIRECT" << endl;
	cerr << "         -p flush before read" << endl;
	cerr << "         -s sync on write" << endl;
	cerr << "         -r reopen file for read" << endl;
	cerr << "         -b <BLK_SIZE> block size in bytes" << endl;
	cerr << "         -d <milliseconds> delay before read" << endl;
	cerr << "         -c close file after each write" << endl;
	cerr << "         -v use setvbuf to disable buffering" << endl;
	cerr << "         -n <num blocks> number of blocks to write/read" << endl;
	cerr << "         -t <num threads> number of concurrent test threads" << endl;
	cerr << "         -l <filename> do an HDFS large read-write file test" << endl;
	cerr << "         -h <plugin-file> load an HDFS plugin (and run HDFS tests)" << endl;
	cerr << "         -m set max size for HDFS mem buffer, default is unlimited" << endl;
}

struct foptest_opts
{
	bool usedirect;
	bool preflush;
	bool synconwrite;
	bool reopen;
	bool closeonwrite;
	bool usevbuf;
	int numthreads;
	int numblocks;
	IDBDataFile::Types filetype;
	string largeFile;
	string pluginFile;
	bool useHdfs;
	unsigned hdfsMaxMem;

	foptest_opts() :
		usedirect(false),
		preflush(false),
		synconwrite(false),
		reopen(false),
		closeonwrite(false),
		usevbuf(false),
		numthreads(1),
		numblocks(64),
		filetype(IDBDataFile::UNBUFFERED),
		largeFile(""),
		pluginFile(""),
		useHdfs(false),
		hdfsMaxMem(0)
		{;}
};

class TestRunner
{
public:
	TestRunner(int id, const foptest_opts& opts);
	~TestRunner();

	bool runTest( IDBDataFile::Types filetype, unsigned open_opts );

	bool fillDefault();
	bool doBlock(unsigned int blocknum, unsigned char tag, unsigned int count);
	bool writeBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag);
	bool writeBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count);
	bool readBlock(unsigned int blocknum, unsigned char* buf, unsigned char tag);
	bool readBlocks(unsigned int blocknum, unsigned char* buf, unsigned char tag, unsigned int count);

	const foptest_opts& runOpts() const { return m_opts; }

	enum LogLevel {
		INFO,
		ERROR
	};
	void logMsg( LogLevel level, const string& msg, bool bold = false );

private:
	bool readTest( IDBDataFile::Types filetype );
	friend void thread_func2( TestRunner& trun );
	bool randomReadTest();
	bool openByModeStrTest();
	bool writeTest( IDBDataFile::Types filetype );
	bool truncateTest( IDBDataFile::Types filetype );
	bool renameTest( IDBDataFile::Types filetype );
	bool copyTest( IDBDataFile::Types filetype );
	bool rdwrTest( IDBDataFile::Types filetype );
	bool hdfsRdwrExhaustTest();
	bool concurrencyTest( IDBDataFile::Types filetype );
	bool tellTest( IDBDataFile::Types filetype );
	bool openWF( bool reopen=false );
	bool openRF();
	bool flushTest(IDBDataFile::Types);
	bool seekTest(IDBDataFile::Types);
	bool listDirTest(IDBDataFile::Types, const string&);

	void reset();

	string        m_fname;
	foptest_opts  m_opts;
	unsigned char m_defbuf[MAX_BLK_SIZE];
	IDBDataFile*  m_file;
	unsigned      m_open_opts;
	int           m_id;
	static boost::mutex m_guard;
};

boost::mutex TestRunner::m_guard;

void thread_func2( TestRunner& trun )
{
	trun.randomReadTest();
}

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
		assert( m_fs.exists( mm_file.c_str() ) );
		m_fs.remove(mm_file.c_str());
		assert( !m_fs.exists( mm_file.c_str() ) );
		assert( m_fs.exists( m_dir.c_str() ) );
		assert( m_fs.isDir( m_dir.c_str() ));
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
	string typeString = ( filetype == IDBDataFile::UNBUFFERED ? "Unbuffered" :
						( filetype == IDBDataFile::BUFFERED ? "Buffered" : "HDFS" ) );
	infostr << "Running test for file type " << typeString;
	logMsg( INFO, infostr.str() );

	if (filetype != IDBDataFile::UNBUFFERED && filetype != IDBDataFile::BUFFERED)
	{
	    if( !IDBPolicy::installPlugin(m_opts.pluginFile) )
	    {
			cout << "ERROR: unable to install HDFS plugin!" << endl;
			return -1;
	    }
	}

	IDBFileSystem& fs = IDBFileSystem::getFs( filetype );

	// build the file name we are going to use
	ostringstream oss;
	// embed pid so that this is a new directory path
	oss << "/tmp/idbdf-dir-" << getpid() << "-" << m_id << "/Calpont/data";
	// we need to make sure this directory doesn't already exist
	// todo-this only works non-HDFS
	string cmd = "rm -rf " + oss.str();
	system(cmd.c_str());
	string dir = oss.str();
	m_fname = dir + "/foobar";

	// instantiate this here so that we always clean up files we created no matter
	// where we exit the function from
	boost::scoped_ptr<TestCleanup> cleanup( new TestCleanup( filetype, dir, m_fname ) );

	// HDFS will automatically create parent directories when opening a file so these
	// tests around mkdir are irrelevant.
	if( filetype != IDBDataFile::HDFS )
	{
		// we expect this to fail because the parent directory should not exist
		reset();
		m_file = IDBDataFile::open(filetype, m_fname.c_str(), "w", m_open_opts);
		if( m_file )
		{
			ostringstream errstr;
			errstr << "open for writing of path " << m_fname << " should not succeed!";
			logMsg( ERROR, errstr.str() );
			return false;
		}

		// now create the path
		if (fs.mkdir(dir.c_str()))
		{
			ostringstream errstr;
			errstr << "open mkdir of " << dir << " failed!";
			logMsg( ERROR, errstr.str() );
			return false;
		}
	}

	bool returnval = true;

	if( returnval )
	{
		returnval = writeTest( filetype );
	}

	// going to check the size two different ways - once through the file and
	// once through the file system.
	unsigned fsize = m_file->size();
	if( fsize != m_opts.numblocks * BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "bad file size from file " << fsize << " != " << m_opts.numblocks * BLK_SIZE << "!";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// this deletes and closes the file - required to get accurate file size with
	// buffered IO and in HDFS from the file system
	reset();
	fsize = fs.size(m_fname.c_str());
	if( fsize != m_opts.numblocks * BLK_SIZE )
	{
		ostringstream errstr;
		errstr << "bad file size from fs " << fsize << " != " << m_opts.numblocks * BLK_SIZE << "!";
		logMsg( ERROR, errstr.str() );
		return false;
	}

    if( returnval )
    {
        returnval = tellTest( filetype );
    }

	if( returnval )
	{
		returnval = readTest( filetype );
	}

	if( returnval && (filetype == IDBDataFile::UNBUFFERED || filetype == IDBDataFile::HDFS ))
	{
		returnval = concurrencyTest( filetype );
	}

	if( returnval )
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

		if( m_opts.numthreads > 1)
		{
			boost::thread_group thread_group;
			for( int i = 0; i < m_opts.numthreads; ++i )
			{
				thread_group.create_thread(boost::bind(thread_func2, boost::ref(*this)));
			}
			thread_group.join_all();
		}

		returnval = randomReadTest();
	}

	if( returnval )
	{
		returnval = rdwrTest( filetype );
	}

	if( returnval && filetype == IDBDataFile::BUFFERED )
	{
		returnval = openByModeStrTest();
	}

	if( returnval )
	{
		returnval = truncateTest( filetype );
	}

	if( returnval )
	{
		returnval = renameTest( filetype );
	}

	if( returnval )
	{
		returnval = copyTest( filetype );
	}

	if( returnval && filetype == IDBDataFile::HDFS )
	{
		returnval = hdfsRdwrExhaustTest();
	}

	if( returnval )
	{
		returnval = flushTest( filetype );
	}

	if( returnval )
	{
		returnval = seekTest( filetype );
	}

	if( returnval )
	{
		returnval = listDirTest( filetype, dir );
	}

	if( returnval )
		logMsg( INFO, typeString + " tests passed!", true );

	reset();
	return returnval;
}

bool TestRunner::readTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "readTest" );

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

	for( int i = 0; i < m_opts.numblocks; ++i)
	{
		if( !readBlock(i, m_defbuf, i) )
			return false;
	}

    gettimeofday(&endtime,0x0);
	timeval_subtract(&timediff,&endtime,&starttime);
	float secs = timediff.tv_sec + (timediff.tv_usec * 0.000001);

	ostringstream infostr;
	infostr << "Read " << m_opts.numblocks * BLK_SIZE << " bytes in " << secs << " secs, ";
	infostr << "Throughput = " << setprecision(3) << ((m_opts.numblocks * BLK_SIZE) / 1000000.0) / secs << "MB/sec";
	logMsg( INFO, infostr.str() );

	return true;
}

bool TestRunner::randomReadTest()
{
	logMsg( INFO, "randomReadTest" );

	struct drand48_data d48data;
    srand48_r(pthread_self(), &d48data);

    for( int i = 0; i < 10; ++i )
    {
    	long int blk_num;
    	lrand48_r( &d48data, &blk_num);
    	blk_num = blk_num % m_opts.numblocks;

    	unsigned char readbuf[4];
    	assert( m_file->pread( readbuf, blk_num * BLK_SIZE, 4 ) == 4 );
    	assert( readbuf[0] == (unsigned char) blk_num );
    }

    return true;
}

bool TestRunner::openByModeStrTest()
{
	logMsg( INFO, "openByModeStrTest" );

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

	ssize_t readct = m_file->read(m_defbuf,BLK_SIZE);
	if( (size_t) readct != BLK_SIZE )
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
	ssize_t bytes_written = m_file->write(m_defbuf, BLK_SIZE);
	if( (size_t) bytes_written != BLK_SIZE )
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
	logMsg( INFO, "writeTest" );

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

	for( int i = 0; i < m_opts.numblocks; ++i)
	{
		if( !writeBlock(i, m_defbuf, i) )
			return false;
	}

	gettimeofday(&endtime,0x0);
	timeval_subtract(&timediff,&endtime,&starttime);
	float secs = timediff.tv_sec + (timediff.tv_usec * 0.000001);

	ostringstream infostr;
	infostr << "Wrote " << m_opts.numblocks * BLK_SIZE << " bytes in " << secs << " secs, ";
	infostr << "Throughput = " << setprecision(3) << ((m_opts.numblocks * BLK_SIZE) / 1000000.0) / secs << "MB/sec";
	logMsg( INFO, infostr.str() );

	return true;
}

bool TestRunner::truncateTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "truncateTest" );

	reset();
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "a", m_open_opts);
	if( !m_file )
	{
		ostringstream errstr;
		errstr << "Unable to open " << m_fname << " for writing";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// choose a random block to truncate at
	struct drand48_data d48data;
    srand48_r(0xdeadbeef, &d48data);

    long int blk_num;
    lrand48_r( &d48data, &blk_num);
    blk_num = blk_num % m_opts.numblocks;
    // always leave at least one block
    if( blk_num == 0 )
    	blk_num = 1;

    int rc = m_file->truncate(blk_num * BLK_SIZE);
    if( (filetype != IDBDataFile::HDFS) && rc)
    {
		logMsg( ERROR, "truncate failed!" );
		return false;
    }
    else if ( (filetype == IDBDataFile::HDFS) && !rc )
    {
		logMsg( ERROR, "truncate is supposed to fail for HDFS files!" );
		return false;
    }
    else if( (filetype == IDBDataFile::HDFS) )
    {
    	// this is the "success" case for HDFS we didn't expect to truncate so reset blk_num
    	blk_num = m_opts.numblocks;
    }

    off64_t fsize = m_file->size();
    if( fsize != (off64_t) (blk_num * BLK_SIZE))
    {
		ostringstream errstr;
		errstr << "wrong file size after truncate, " << fsize << " != " << blk_num*BLK_SIZE;
		logMsg( ERROR, errstr.str() );
		return false;
    }

    return true;
}

bool TestRunner::renameTest( IDBDataFile::Types type )
{
	logMsg( INFO, "renameTest" );

	// assume this test is run after the write test so that the file m_fname exists
	reset();
	IDBFileSystem& fs = IDBFileSystem::getFs( type );

    // get the size before we move for compare purposes.
	off64_t fsize_orig = fs.size( m_fname.c_str() );

	// choose a path in a different directory that we know already exists
	// and make it specific to our thread...
	ostringstream oss;
	// embed pid so that this is a new directory path
	oss << "/tmp/renametest-" << getpid() << "-" << m_id;
	string newpath = oss.str();

	// rename it
	int rc = fs.rename( m_fname.c_str(), newpath.c_str() );
    if( rc != 0 )
    {
		ostringstream errstr;
		errstr << "rename failed, " << strerror( errno );
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // now check if oldpath exists using size method
	off64_t fsize = fs.size( m_fname.c_str() );
    if( fsize != -1 )
    {
		ostringstream errstr;
		errstr << "old file still exists, size = " << fsize;
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // now check if newpath exists using size method
	fsize = fs.size( newpath.c_str() );
    if( fsize != fsize_orig )
    {
		ostringstream errstr;
		errstr << "new file and old file sizes differ, " << fsize << "!=" << fsize_orig;
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // everything looks good - put it back
	// rename it
	rc = fs.rename( newpath.c_str(), m_fname.c_str() );
    if( rc != 0 )
    {
		ostringstream errstr;
		errstr << "final rename failed, " << strerror( errno );
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // now a negative test case.  Try to rename a file that does not exist
    assert( fs.rename( "a-bogus-file-name", newpath.c_str() ) == -1 );

    return true;
}

bool TestRunner::copyTest( IDBDataFile::Types type )
{
	// assume this test is run after the write test so that the file m_fname exists
	reset();
	IDBFileSystem& fs = IDBFileSystem::getFs( type );

    // get the size before we copy for compare purposes.
	off64_t fsize_orig = fs.size( m_fname.c_str() );

	// choose a path in a different directory that we know already exists
	// and make it specific to our thread...
	ostringstream oss;
	// embed pid so that this is a new directory path
	oss << "/tmp/copytest-" << getpid() << "-" << m_id;
	string newpath = oss.str();

	// copy it
	int rc = fs.copyFile( m_fname.c_str(), newpath.c_str() );
    if( rc != 0 )
    {
		ostringstream errstr;
		errstr << "copy failed, " << strerror( errno );
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // now check if newpath exists using size method
	off64_t fsize = fs.size( newpath.c_str() );
    if( fsize != fsize_orig )
    {
		ostringstream errstr;
		errstr << "new file and old file sizes differ, " << fsize << "!=" << fsize_orig;
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // everything looks good - delete the copy
	rc = fs.remove( newpath.c_str() );
    if( rc != 0 )
    {
		ostringstream errstr;
		errstr << "file delete failed (after copy), " << strerror( errno );
		logMsg( ERROR, errstr.str() );
		return false;
    }

    // now a negative test case.  Try to copy a file that does not exist
    assert( fs.copyFile( "a-bogus-file-name", newpath.c_str() ) == -1 );

	return true;
}

bool TestRunner::rdwrTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "rdwrTest" );

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
    int blocks_to_touch = min( 5, m_opts.numblocks);
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
    		blk_num = blk_num % m_opts.numblocks;
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
    	ssize_t writect = m_file->write(&writeval,1);
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
		ssize_t readct = m_file->pread(&readbuf,touched[i] * BLK_SIZE,1);
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

		ssize_t writect = m_file->write(&readbuf,1);
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
	logMsg( INFO, "hdfsRdwrExhaustTest" );

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

	if( m_opts.largeFile.length() )
	{
		file = IDBDataFile::open(IDBDataFile::HDFS, m_opts.largeFile.c_str(), "r+", 0);
		assert( file );
	}
	return true;
}

bool TestRunner::concurrencyTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "concurrencyTest" );

	reset();

	// ok - scenario is a reader opens the file then a reader/writer opens
	// the same file and updates something, then the reader tries to read
	// again.
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts);
	assert(m_file);

	// read a few blocks
	assert( readBlock(0, m_defbuf, 0) );
	assert( readBlock(1, m_defbuf, 1) );

	// open the same file for read/write
	IDBDataFile* rdwrFile = IDBDataFile::open(filetype, m_fname.c_str(), "r+", m_open_opts);
	assert(rdwrFile);

	// read a block, write a block
	assert( rdwrFile->pread(m_defbuf, 0, BLK_SIZE) == (ssize_t) BLK_SIZE );
	assert( m_defbuf[0] == 0 );
	m_defbuf[0] = 95;
	assert( rdwrFile->seek(0, 0) == 0 );
	assert( rdwrFile->write(m_defbuf, BLK_SIZE) );

	// close file
	delete rdwrFile;

	// this 5 seconds is important in the HDFS case because it gives HDFS
	// time to age off (or whatever) the blocks for the original file that
	// have now been rewritten.  The value was determined experimentally -
	// 3 secs works fine most times but not all.  If HDFS hasn't aged out
	// the blocks then the read will return the old data
	if( filetype == IDBDataFile::HDFS )
		sleep(10);

	// go back to the reader and make sure he got the new value, then close
	assert( readBlock(0, m_defbuf, 95) );
	delete m_file;

	// now put block 0 back the way it was
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "r+", m_open_opts);
	assert(m_file);

	assert( writeBlock(0, m_defbuf, 0) );

	return true;
}

bool TestRunner::tellTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "tellTest" );

    reset();

    // scenario: reader opens file, seeks somewhere and tells where it is.
    m_file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts);
    assert(m_file);

    // read a few blocks
    assert( readBlock(0, m_defbuf, 0) );
    assert( readBlock(1, m_defbuf, 1) );

    if( m_file->seek(BLK_SIZE, SEEK_SET) )
    {
        ostringstream errstr;
        errstr << "tellTest: failed to seek block";
        logMsg( ERROR, errstr.str() );
        return false;
    }

    off64_t filepos = m_file->tell();
    if( filepos != off64_t(BLK_SIZE) )
    {
        ostringstream errstr;
        errstr << "tellTest: File position not at correct block, " << filepos << " != " << BLK_SIZE;
        logMsg( ERROR, errstr.str() );
        return false;
    }

    return true;
}

bool TestRunner::flushTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "flushTest" );

    reset();

	string scratch = "/tmp/rdwr_scratch" + m_fname;  // scratch file name if exists
	boost::filesystem::remove(scratch);
	IDBPolicy::remove(m_fname.c_str());

	// scenario: writer opens the file, writes 8 bytes, flush;
	//           reader opens the file, reads 8 bytes, verifys the data, then closes file;
	//           writer writes 8M bytes (force to buffered file) if -m option used correctly;
	//           reader opens the file, verifys the file size and content, then closes the file;
	//           writer closes the file.
	ostringstream errstr;
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "w+", m_open_opts, /*default:4*/ 1);
	if (!m_file)
	{
		errstr << "flushTest: open " << m_fname.c_str() << " for write failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// write 8 "1" through mem buff
	const char w1[] = "11111111";
	ssize_t bytes = 0;
	if ((bytes = m_file->write(w1, 8)) != 8)
	{
		errstr << "flushTest: write count = 8, return = " << bytes;
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// for HDFS, force writing out to disk.
	m_file->flush();
	if (!IDBPolicy::exists(m_fname.c_str()))
	{
		errstr << "flushTest: " << m_fname.c_str() << " does not exist";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if (filetype == IDBDataFile::HDFS && boost::filesystem::exists(scratch))
	{
		errstr << "flushTest: " << scratch << " exists after 1st write";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// read from file in "r" mode
	IDBDataFile* file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts, 1);
	if (!file)
	{
		errstr << "flushTest: 1st open " << m_fname.c_str() << " to read failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	char r1[9] = {0};
	ssize_t n1 = file->pread(r1, 0, 8);
	if (n1 != 8 || strncmp(r1, w1, 8) != 0)
	{
		errstr << "flushTest: read " << n1 << " != 8 OR " << r1 << "!= 11111111";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	delete file;
	file = NULL;

	// write 8M "2", switched to file buffer if max size for mem buffer is small.
	//char w2[] = {[0 ... 8*1024*1024] = '2'};
	ssize_t m9 = 9*1024*1024;  // must be large than EXTENTSIZE = 8390656 to swith to file buffer
	boost::scoped_array<char> w2(new char[m9]);
	memset(w2.get(), '2', m9);
	m_file->write(w2.get(), m9);
	m_file->flush();

	// check file size
	if (IDBPolicy::size(m_fname.c_str()) != 8 + m9)
	{
		errstr << "flushTest: size of " << m_fname.c_str() << " is "
				<< IDBPolicy::size(m_fname.c_str()) << ", expecting " << (8 + m9);
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if (filetype == IDBDataFile::HDFS &&
		!boost::filesystem::exists(scratch) &&
		m_opts.hdfsMaxMem < m9)
	{
		errstr << "flushTest: " << scratch << " does not exist after 2nd write";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// 2nd read
	file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts, 1);
	if (!file)
	{
		errstr << "flushTest: 2nd open " << m_fname.c_str() << " to read failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	//char r2[9*1024*1024 + 8 + 1] = {0};
	boost::scoped_array<char> r2(new char[9*1024*1024 + 8 + 1]);
	memset(r2.get(), 0, m9 + 9);
	ssize_t n2 = file->pread(r2.get(), 0, m9 + 8);
	if (n2 != (m9+8) || strncmp(r2.get(), w1, 8) != 0 || memcmp(r2.get()+8, w2.get(), m9) != 0)
	{
		errstr << "flushTest: 2nd read " << m_fname.c_str() << " failed" << endl
				<< "   return value: " << n2 << " bytes -- " << r2;  // need hex dump?
		logMsg( ERROR, errstr.str() );
		return false;
	}

	delete file;
	file = NULL;

	delete m_file;
	m_file = NULL;

	return true;
}

bool TestRunner::seekTest( IDBDataFile::Types filetype )
{
	logMsg( INFO, "seekTest" );

    reset();

	// scenario: writer opens the file with w+, writes 8 bytes, seek to 4 from 0, write 4 bytes
	//           reader opens the file with r, verify size and contents,
	//           writer seeks 4 bytes beyond EOF, write 4 bytes, and close the file,
	//           reader rewinds, verify size and contents, and close the file.
	ostringstream errstr;
	m_file = IDBDataFile::open(filetype, m_fname.c_str(), "w+", m_open_opts);
	if (!m_file)
	{
		errstr << "seekTest: open " << m_fname.c_str() << " for write failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// write 8 "1" through mem buff
	const char w1[] = "11111111";
	ssize_t bytes = 0;
	if ((bytes = m_file->write(w1, 8)) != 8)
	{
		errstr << "seekTest: write1 count = 8, return = " << bytes;
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if (m_file->seek(4, SEEK_SET) != 0)
	{
		errstr << "seekTest: seek(4, SEEK_SET) failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	const char w2[] = "2222";
	if ((bytes = m_file->write(w2, 4)) != 4)
	{
		errstr << "seekTest: write2 count = 4, return = " << bytes;
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// for HDFS, force writing out to disk.
	m_file->flush();

	// read from file in "r" mode
	IDBDataFile* file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts);
	if (!file)
	{
		errstr << "seekTest: 1st open " << m_fname.c_str() << " to read failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	char r1[9] = {0};
	ssize_t n1 = file->pread(r1, 0, 8);
	if (IDBPolicy::size(m_fname.c_str()) != 8)
	{
		errstr << "seekTest: size of " << m_fname.c_str() << " is "
				<< IDBPolicy::size(m_fname.c_str()) << ", expecting 8";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if (n1 != 8 || strncmp(r1, w1, 4) != 0 || strncmp(r1+4, w2, 4) != 0)
	{
		errstr << "seekTest: read " << n1 << " != 8 OR " << r1 << "!= 11112222";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// now seek beyond the eof, and write 4 bytes.
	const char w3[] = "3333";
	if (m_file->seek(4, SEEK_END) != 0)
	{
		errstr << "seekTest: seek(4, SEEK_END) failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if ((bytes = m_file->write(w3, 4)) != 4)
	{
		errstr << "seekTest: write3 count = 4, return = " << bytes;
		logMsg( ERROR, errstr.str() );
		return false;
	}
	m_file->flush();

	delete m_file;
	m_file = NULL;

	// check file size
	if (IDBPolicy::size(m_fname.c_str()) != 16)
	{
		errstr << "seekTest: size of " << m_fname.c_str() << " is "
				<< IDBPolicy::size(m_fname.c_str()) << ", expecting 16";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// 2nd read
	file = IDBDataFile::open(filetype, m_fname.c_str(), "r", m_open_opts);
	if (!file)
	{
		errstr << "seekTest: 2nd open " << m_fname.c_str() << " to read failed";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	char r2[17] = {0};
	const char pd[4] = {0};  // padding
	ssize_t n2 = file->pread(r2, 0, 16);
	if (n2 != 16 ||
		strncmp(r2, w1, 4) != 0 || memcmp(r2+4, w2, 4) != 0 ||
		strncmp(r2+8, pd, 4) != 0 || memcmp(r2+12, w3, 4) != 0)
	{
		errstr << "seekTest: 2nd read " << m_fname.c_str() << " failed" << endl
				<< "   return value: " << n2 << " bytes -- " << r2;  // need hex dump?
		logMsg( ERROR, errstr.str() );
		return false;
	}

	delete file;
	file = NULL;

	return true;
}

bool TestRunner::listDirTest( IDBDataFile::Types filetype, const string& dir )
{
	logMsg( INFO, "listDirTest" );

	IDBFileSystem& fs = IDBFileSystem::getFs( filetype );
	ostringstream errstr;
	string fname2 = m_fname + "2";
	string fname3 = m_fname + "3";

	IDBDataFile* file2 = IDBDataFile::open(filetype, fname2.c_str(), "w", m_open_opts);
	if (file2)
		delete file2;

	IDBDataFile* file3 = IDBDataFile::open(filetype, fname3.c_str(), "w", m_open_opts);
	if (file3)
		delete file3;

	list<string> dircontents;
	if (fs.listDirectory( dir.c_str(), dircontents ) != 0)
	{
		errstr << "Error calling listDirectory";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	ostringstream ldstr;
	ldstr << "Listed directory " << dir << ":";
	list<string>::iterator iend = dircontents.end();
	bool foobarFound  = false;
	bool foobar2Found = false;
	bool foobar3Found = false;
	for( list<string>::iterator i = dircontents.begin(); i != iend; ++i )
	{
		ldstr << (*i) << ",";
		if ((*i) == "foobar")
			foobarFound = true;
		else if ((*i) == "foobar2")
			foobar2Found = true;
		else if ((*i) == "foobar3")
			foobar3Found = true;
	}
	logMsg( INFO, ldstr.str() );

	if (dircontents.size() != 3)
	{
		errstr << "listDirectory not returning 3 file names";
		logMsg( ERROR, errstr.str() );
		return false;
	}
	if ((!foobarFound || !foobar2Found || !foobar3Found))
	{
		errstr << "listDirectory returning incorrect file names";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	// now check a bogus path and make sure it returns -1
	if (fs.listDirectory( "/this-is-a-bogus-directory", dircontents ) != -1)
	{
		errstr << "listDirectory not failing a call for a bogus directory";
		logMsg( ERROR, errstr.str() );
		return false;
	}

	assert( fs.remove( "/this-is-a-bogus-directory" ) == 0 );
	assert( !fs.isDir( "/this-is-a-bogus-directory" ));
	assert( !fs.isDir( m_fname.c_str() ));

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
	ssize_t rc = m_file->write(buf, BLK_SIZE);

	if ((size_t) rc != BLK_SIZE)
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
	ssize_t rc = m_file->pread(buf,blocknum*BLK_SIZE,BLK_SIZE);
	//cout << "DEBUG: read " << rc << " bytes at offset " << blocknum * BLK_SIZE << endl;

	if ((size_t) rc != BLK_SIZE)
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

bool TestRunner::doBlock(unsigned int blocknum, unsigned char tag, unsigned int count)
{
	writeBlocks(blocknum,m_defbuf,tag,count);
	m_file->flush();
	if( m_opts.closeonwrite )
	{
		delete m_file;
		// will have to cache type somewhere later
		m_file = IDBDataFile::open( IDBDataFile::HDFS, m_fname.c_str(), "a", m_open_opts );
		if( !m_file )
			return false;
	}

	if( m_opts.reopen )
	{
		delete m_file;
		// will have to cache type somewhere later
		m_file = IDBDataFile::open( IDBDataFile::HDFS, m_fname.c_str(), "r", m_open_opts );
		if( !m_file )
			return false;
	}

	unsigned char buf[BLK_SIZE];
	return readBlocks(blocknum,buf,tag,count);
}

void TestRunner::logMsg( LogLevel level, const string& msg, bool bold)
{
	boost::mutex::scoped_lock lock( m_guard );
	if (bold) cout << "\033[0;1m";
	cout << "Test-" << m_id << ":" << ((level == INFO) ? "INFO:" : "ERROR:") << msg << endl;
	if (bold) cout << "\033[0;39m";
}

void thread_func( TestRunner& trun )
{
	bool ret = true;
	// todo-add some capability to use USE_ODIRECT or not
	ret = ret && trun.runTest( IDBDataFile::UNBUFFERED, 0 );
	// todo-add some capability to use USE_VBUF or not
	ret = ret && trun.runTest( IDBDataFile::BUFFERED, 0 );
	if( trun.runOpts().useHdfs )
		ret = ret && trun.runTest( IDBDataFile::HDFS, 0 );


	trun.logMsg( TestRunner::INFO, string(ret?"A":"NOT a") + "ll tests passed!\n", true );
}

int main(int argc, char** argv)
{
	foptest_opts opts;

	int c;
    while ((c = getopt (argc, argv, "adpsrh:b:t:cvuen:l:m:")) != -1)
    	switch (c)
        {
    		case 'b':
    			BLK_SIZE = atoi(optarg);
    			if( BLK_SIZE > MAX_BLK_SIZE )
    			{
    				BLK_SIZE = MAX_BLK_SIZE;
    				cout << "WARNING: block size exceeds max, using " << BLK_SIZE << endl;
    			}
    			else
    			{
    				cout << "INFO: using BLK_SIZE " << BLK_SIZE << endl;
    			}
    			break;
    		case 'c':
    			opts.closeonwrite = true;
    			cout << "INFO: will close on write" << endl;
    			break;
        	case 'd':
        		opts.usedirect = 1;
    			cout << "INFO: will open with O_DIRECT" << endl;
        		break;
        	case 'p':
        		opts.preflush = 1;
    			cout << "INFO: will use read pre-flush" << endl;
        		break;
        	case 's':
        		opts.synconwrite = 1;
    			cout << "INFO: will fsync after write" << endl;
        		break;
        	case 'r':
        		opts.reopen = 1;
    			cout << "INFO: will reopen before each read" << endl;
        		break;
    		case 't':
    			opts.numthreads = atoi(optarg);
    			cout << "INFO: will start " << opts.numthreads << " test threads" << endl;
    			break;
    		case 'v':
    			opts.usevbuf = true;
    			cout << "INFO: will use setvbuf to disable buffering" << endl;
    			break;
    		case 'n':
    			opts.numblocks = atoi(optarg);
    			cout << "INFO: will operate on " << opts.numblocks << " blocks" << endl;
    			break;
    		case 'l':
    			opts.largeFile = optarg;
    			cout << "INFO: will run a large HDFS RDWR test on " << opts.largeFile << endl;
    			break;
    		case 'h':
    			opts.pluginFile = optarg;
    			cout << "INFO: will load HDFS plugin " << optarg << endl;
    			break;
    		case 'm':
    			opts.hdfsMaxMem = atoi(optarg);
    			if (opts.hdfsMaxMem)
    				cout << "INFO: will set hdfsRdwrBufferMaxSize to " << opts.hdfsMaxMem << endl;
    			else
    				cout << "INFO: will set hdfsRdwrBufferMaxSize to unlimited" << endl;
    			break;
        	default:
        		usage();
        		return 1;
        }

    // init the library with logging enabled
    IDBPolicy::init( true, true, "/tmp/rdwr_scratch", opts.hdfsMaxMem);

	if( opts.pluginFile.length() )
		opts.useHdfs = true;

    //vector<TestRunner>
    boost::thread_group thread_group;
    for( int i = 0; i < opts.numthreads; ++i )
    {
        thread_group.create_thread(boost::bind(thread_func, boost::ref(* (new TestRunner(i, opts)))));
    }
    thread_group.join_all();
}
