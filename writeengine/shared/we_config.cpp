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

/*******************************************************************************
* $Id: we_config.cpp 2873 2011-02-08 14:35:57Z rdempsey $
*
*******************************************************************************/
/** @file */

#include <string>
#include <boost/thread.hpp>
#include <vector>
#include <sstream>
using namespace std;

#include "configcpp.h"
#include "messagequeue.h"
#define WRITEENGINECONFIG_DLLEXPORT
#include "we_config.h"
#undef WRITEENGINECONFIG_DLLEXPORT
using namespace config;
using namespace messageqcpp;

#include "cacheutils.h"

namespace WriteEngine
{
	const int      DEFAULT_WAIT_PERIOD                = 10;
	const unsigned DEFAULT_FILES_PER_COLUMN_PARTITION = 32;
	const unsigned DEFAULT_EXTENTS_PER_SEGMENT_FILE   =  4;
	const unsigned DEFAULT_BULK_PROCESS_PRIORITY      = -1;
	const unsigned DEFAULT_MAX_FILESYSTEM_DISK_USAGE  = 98; // allow 98% full
	const unsigned DEFAULT_COMPRESSED_PADDING_BLKS    =  1;

	int              Config::m_dbRootCount = -1;
	Config::strvec_t Config::m_dbRoot;
	string           Config::m_bulkRoot;
	boost::mutex     Config::m_dbRoot_lk;
	boost::mutex     Config::m_bulkRoot_lk;
	int      Config::m_WaitPeriod              = DEFAULT_WAIT_PERIOD;
	unsigned Config::m_FilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
	unsigned Config::m_ExtentsPerSegmentFile   = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
	int      Config::m_BulkProcessPriority     = DEFAULT_BULK_PROCESS_PRIORITY;
	string   Config::m_BulkRollbackDir;
    unsigned Config::m_MaxFileSystemDiskUsage  = DEFAULT_MAX_FILESYSTEM_DISK_USAGE;
	bool     Config::m_UseLocalCachedValues    = false;
    unsigned Config::m_NumCompressedPadBlks    = DEFAULT_COMPRESSED_PADDING_BLKS;

//Don't call this method without holding the m_dbRoot_lk mutex!
/*******************************************************************************
 * DESCRIPTION:
 *    Initialize DBRootCount and list of DBRoots
 * PARAMETERS:
 *    none
 * RETURN:
 *    none
 ******************************************************************************/
void Config::doDBRootInit()
{
	config::Config* cf = config::Config::makeConfig();

	m_dbRootCount =
		cf->fromText( cf->getConfig("SystemConfig","DBRootCount") );

	for (int idx=1; idx<=m_dbRootCount; idx++)
	{
		ostringstream oss;
		oss << "DBRoot" << idx;
		m_dbRoot.push_back( cf->getConfig("SystemConfig", oss.str()) );
	}
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root count
 * PARAMETERS:
 *    none
 * RETURN:
 *    Number of DBRoot paths to be used for database files
 ******************************************************************************/
size_t Config::DBRootCount()
{
	{
		boost::mutex::scoped_lock lk(m_dbRoot_lk);
		if (m_dbRootCount < 0)
			doDBRootInit();
	}

	return m_dbRootCount;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root 
 * PARAMETERS:
 *    idx - index of DBRootn to fetch (0 fetches DBRoot1, etc.)
 * RETURN:
 *    DBRoot path for specified index
 ******************************************************************************/
const char* Config::getDBRoot(unsigned idx)
{ 
	//If we haven't initialized m_dbRoot, do that now
	{
		boost::mutex::scoped_lock lk(m_dbRoot_lk);
		if (m_dbRoot.size() == 0)
			doDBRootInit();
	}

	if (idx >= m_dbRoot.size()) return 0;

	return m_dbRoot[idx].c_str(); 
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get bulk root 
 * PARAMETERS:
 *    none 
 * RETURN:
 *    NO_ERROR if success, other otherwise
 ******************************************************************************/
const char*  Config::getBulkRoot() 
{  
	boost::mutex::scoped_lock lk(m_bulkRoot_lk);
	if( m_bulkRoot.length() == 0 ) {
		m_bulkRoot = config::Config::makeConfig()->getConfig(
			"WriteEngine", "BulkRoot" );
	if( m_bulkRoot.length() == 0 )
#ifdef _MSC_VER
		m_bulkRoot = "C:\\Calpont\\bulk";
#else
		m_bulkRoot = "/usr/local/Calpont/data/bulk";
#endif
	}

	return m_bulkRoot.c_str();
}

#ifdef SHARED_NOTHING_DEMO_2
void Config::getSharedNothingRoot(char *ret)
{
	string root;
	boost::mutex::scoped_lock lk(m_bulkRoot_lk);

	root = config::Config::makeConfig()->getConfig(
		"WriteEngine", "SharedNothingRoot");
	strncpy(ret, root.c_str(), FILE_NAME_SIZE);
}
#endif

/*******************************************************************************
 * DESCRIPTION:
 *    send message to all of the primporc to flush 
 * PARAMETERS:
 *    none 
 * RETURN:
 *    NO_ERROR if success, other otherwise
 ******************************************************************************/
const int Config::sendMsgPM()
{
	int rc = cacheutils::flushPrimProcCache();
	return (rc == 0 ? NO_ERROR : -1);
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get wait period used between tries to get a transaction id.
 * PARAMETERS:
 *    none
 * RETURN:
 *    Wait Period in seconds
 ******************************************************************************/
int Config::getWaitPeriod()
{ 
	if (m_UseLocalCachedValues)
	{
		return m_WaitPeriod;
	}

	int waitPeriod = DEFAULT_WAIT_PERIOD;
	string waitPeriodStr = config::Config::makeConfig()->getConfig(
		"SystemConfig", "WaitPeriod");
	if ( waitPeriodStr.length() != 0 )
		waitPeriod = static_cast<int>(config::Config::fromText(
			waitPeriodStr));
	return waitPeriod; 
}
	
/*******************************************************************************
 * DESCRIPTION:
 *    Get number of segment files per column partition.
 * PARAMETERS:
 *    none
 * RETURN:
 *    Number of files
 ******************************************************************************/
unsigned Config::getFilesPerColumnPartition()
{
	if (m_UseLocalCachedValues)
	{
		return m_FilesPerColumnPartition;
	}

   	unsigned filesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;

	config::Config* cf = config::Config::makeConfig();
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if ( fpc.length() != 0 )		
		filesPerColumnPartition = cf->uFromText(fpc);		

	return filesPerColumnPartition;  	
}
   
/*******************************************************************************
 * DESCRIPTION:
 *    Get number of extents per column segment file
 * PARAMETERS:
 *    none
 * RETURN:
 *    Number of extents
 ******************************************************************************/
unsigned Config::getExtentsPerSegmentFile()
{
	if (m_UseLocalCachedValues)
	{
		return m_ExtentsPerSegmentFile;
	}

   	unsigned extentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
   	config::Config* cf = config::Config::makeConfig();
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if ( epsf.length() != 0 )
		extentsPerSegmentFile = cf->uFromText(epsf);

	return extentsPerSegmentFile;
 }  
   
/*******************************************************************************
 * DESCRIPTION:
 *    Get process priority for cpimport process.
 *    Config file priority is in range 40..1 (highest..lowest)
 *    Return value is in range (-20..19)
 *
 *    This range of values 40..1, and -20..19, matches the
 *    convention used by PrimProc and ExeMgr, so that we are
 *    consistent with those processes.  Likewise we employ
 *    the same default priority of -1.
 * PARAMETERS:
 *    none
 * RETURN:
 *    cpimport process priority
 ******************************************************************************/
int Config::getBulkProcessPriority()
{
	if (m_UseLocalCachedValues)
	{
		return m_BulkProcessPriority;
	}

  	int bulkProcessPriority = DEFAULT_BULK_PROCESS_PRIORITY;
	config::Config* cf = config::Config::makeConfig();
	string prior = cf->getConfig("WriteEngine", "Priority");
	if ( prior.length() != 0 )
	{
		int initialBPP = cf->fromText(prior);

		// config file priority is 40..1 (highest..lowest) 
		// vert config file value to setpriority(2) value(-20..19, -1 is the default)
		if (initialBPP > 0)
			bulkProcessPriority = 20 - initialBPP;
		else if (initialBPP < 0)
			bulkProcessPriority = 19;

		if (bulkProcessPriority < -20)
			bulkProcessPriority = -20;
	}

	return bulkProcessPriority;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get bulk rollback directory path.
 * PARAMETERS:
 *    none 
 ******************************************************************************/
const char*  Config::getBulkRollbackDir()
{  
	if (m_UseLocalCachedValues)
	{
		return m_BulkRollbackDir.c_str();
	}

	m_BulkRollbackDir = config::Config::makeConfig()->getConfig(
		"WriteEngine", "BulkRollbackDir" );
	if (m_BulkRollbackDir.length() == 0)
	{
		m_BulkRollbackDir.assign( getBulkRoot() );
		m_BulkRollbackDir += "/rollback";
	}

	return m_BulkRollbackDir.c_str();
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get Max percentage of allowable file system disk usage for each DBRoot
 * PARAMETERS:
 *    none 
 ******************************************************************************/
unsigned Config::getMaxFileSystemDiskUsage()
{
	if (m_UseLocalCachedValues)
	{
		return m_MaxFileSystemDiskUsage;
	}

	unsigned maxFileSystemDiskUsage = DEFAULT_MAX_FILESYSTEM_DISK_USAGE;

	config::Config* cf = config::Config::makeConfig();
	string usg = cf->getConfig("WriteEngine", "MaxFileSystemDiskUsagePct");
	if ( usg.length() != 0 )
		maxFileSystemDiskUsage = cf->uFromText(usg);
	if (maxFileSystemDiskUsage > 100)
		maxFileSystemDiskUsage = DEFAULT_MAX_FILESYSTEM_DISK_USAGE;

	return maxFileSystemDiskUsage;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get number of blocks to use in padding each compressed chunk (only
 *    applies to compressed columns).
 * PARAMETERS:
 *    none 
 ******************************************************************************/
unsigned Config::getNumCompressedPadBlks()
{
	if (m_UseLocalCachedValues)
	{
		return m_NumCompressedPadBlks;
	}

	unsigned numCompressedPadBlks = DEFAULT_COMPRESSED_PADDING_BLKS;

	config::Config* cf = config::Config::makeConfig();
	string ncpb = cf->getConfig("WriteEngine", "CompressedPaddingBlocks");
	if ( ncpb.length() != 0 )
		numCompressedPadBlks = cf->uFromText(ncpb);

	return numCompressedPadBlks;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Loads config parms into local cache.
 *    Call should be made to initConfigCache() at the start
 *    of the program to initialize local cache, before calling
 *    accessors.
 * PARAMETERS:
 *    none
 ******************************************************************************/
void Config::initConfigCache()
{
	// ignore return value from DBRootCount(), but we still
	// call it to force a call to doDBRootInit()
	Config::DBRootCount();

	m_WaitPeriod              = Config::getWaitPeriod();
	m_FilesPerColumnPartition = Config::getFilesPerColumnPartition();
	m_ExtentsPerSegmentFile   = Config::getExtentsPerSegmentFile();
	m_BulkProcessPriority     = Config::getBulkProcessPriority();
	m_BulkRollbackDir         = Config::getBulkRollbackDir();
	m_MaxFileSystemDiskUsage  = Config::getMaxFileSystemDiskUsage();
	m_NumCompressedPadBlks    = Config::getNumCompressedPadBlks();

	m_UseLocalCachedValues    = true;
}
   
} //end of namespace

