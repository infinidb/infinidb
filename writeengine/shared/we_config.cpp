/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*******************************************************************************
* $Id: we_config.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
*
*******************************************************************************/
/** @file */

#include <string>
#include <boost/thread.hpp>
#include <vector>
#include <sstream>
#include <algorithm>
using namespace std;

#include "configcpp.h"
#include "liboamcpp.h"
#include "installdir.h"
#define WRITEENGINECONFIG_DLLEXPORT
#include "we_config.h"
#undef WRITEENGINECONFIG_DLLEXPORT
using namespace config;

#include "IDBPolicy.h"
using namespace idbdatafile;

#include <boost/algorithm/string.hpp>

namespace WriteEngine
{
    const int      DEFAULT_WAIT_PERIOD                = 10;
    const unsigned DEFAULT_FILES_PER_COLUMN_PARTITION =  4;
    const unsigned DEFAULT_EXTENTS_PER_SEGMENT_FILE   =  2;
    const int      DEFAULT_BULK_PROCESS_PRIORITY      = -1;
    const unsigned DEFAULT_MAX_FILESYSTEM_DISK_USAGE  = 98; // allow 98% full
    const unsigned DEFAULT_COMPRESSED_PADDING_BLKS    =  1;
    const int      DEFAULT_LOCAL_MODULE_ID            = 1;
    const bool     DEFAULT_PARENT_OAM                 = true;
    const char*    DEFAULT_LOCAL_MODULE_TYPE          = "pm";

    int              Config::m_dbRootCount = 0;
    Config::strvec_t Config::m_dbRootPath;
    Config::intstrmap_t Config::m_dbRootPathMap;
    Config::uint16vec_t Config::m_dbRootId;
    string           Config::m_bulkRoot;

    unsigned long    Config::fDBRootChangeCount = 0;
    time_t           Config::fCacheTime = 0;
    boost::mutex     Config::fCacheLock;
#ifdef SHARED_NOTHING_DEMO_2
    boost::mutex     Config::m_bulkRoot_lk;
#endif
    int      Config::m_WaitPeriod              = DEFAULT_WAIT_PERIOD;
    unsigned Config::m_FilesPerColumnPartition =
        DEFAULT_FILES_PER_COLUMN_PARTITION;
    unsigned Config::m_ExtentsPerSegmentFile   =
        DEFAULT_EXTENTS_PER_SEGMENT_FILE;
    int      Config::m_BulkProcessPriority     = DEFAULT_BULK_PROCESS_PRIORITY;
    string   Config::m_BulkRollbackDir;
    unsigned Config::m_MaxFileSystemDiskUsage  =
        DEFAULT_MAX_FILESYSTEM_DISK_USAGE;
    unsigned Config::m_NumCompressedPadBlks    =DEFAULT_COMPRESSED_PADDING_BLKS;
    bool     Config::m_ParentOAMModuleFlag     = DEFAULT_PARENT_OAM;
    string   Config::m_LocalModuleType;
    int      Config::m_LocalModuleID           = DEFAULT_LOCAL_MODULE_ID;
    string   Config::m_VersionBufferDir;

/*******************************************************************************
 * DESCRIPTION:
 *    Loads config parms into local cache.
 *    Call can be made to initConfigCache() at the start
 *    of the program to initialize local cache, before calling accessors.
 * PARAMETERS:
 *    none
 ******************************************************************************/
void Config::initConfigCache()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );
}

/*******************************************************************************
 * DESCRIPTION:
 *    Reload local cache using contents of Calpont.xml file.
 * PARAMETERS:
 *    none
 ******************************************************************************/
void Config::checkReload( )
{
    bool bFirstLoad = false;

    if (fCacheTime == 0)
        bFirstLoad = true;

    config::Config* cf = config::Config::makeConfig();

    // Immediately return if Calpont.xml timestamp has not changed
    if (cf->getCurrentMTime() == fCacheTime)
        return;

    //std::cout << "RELOADING cache..." << std::endl;

    //--------------------------------------------------------------------------
    // Initialize bulk root directory
    //--------------------------------------------------------------------------
    m_bulkRoot = cf->getConfig("WriteEngine", "BulkRoot");
    if ( m_bulkRoot.length() == 0 )
    {
        m_bulkRoot = startup::StartUp::installDir();
#ifndef _MSC_VER
		m_bulkRoot += "/data";
#endif
		m_bulkRoot += "/bulk";
    }

    // Get latest Calpont.xml timestamp after first access forced a reload
    fCacheTime = cf ->getLastMTime();

    //--------------------------------------------------------------------------
    // Initialize time interval (in seconds) between retries
    //--------------------------------------------------------------------------
    m_WaitPeriod = DEFAULT_WAIT_PERIOD;
    string waitPeriodStr = cf->getConfig("SystemConfig", "WaitPeriod");
    if ( waitPeriodStr.length() != 0 )
        m_WaitPeriod = static_cast<int>(config::Config::fromText(
            waitPeriodStr));

    //--------------------------------------------------------------------------
    // Initialize files per column partition
    //--------------------------------------------------------------------------
    m_FilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
    string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
    if ( fpc.length() != 0 )
        m_FilesPerColumnPartition = cf->uFromText(fpc);

    //--------------------------------------------------------------------------
    // Initialize extents per segment file
    //--------------------------------------------------------------------------
    m_ExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
    string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
    if ( epsf.length() != 0 )
        m_ExtentsPerSegmentFile = cf->uFromText(epsf);

    //--------------------------------------------------------------------------
    // Initialize bulk load process priority
    //--------------------------------------------------------------------------
    m_BulkProcessPriority = DEFAULT_BULK_PROCESS_PRIORITY;
    string prior = cf->getConfig("WriteEngine", "Priority");
    if ( prior.length() != 0 )
    {
        int initialBPP = cf->fromText(prior);

        // config file priority is 40..1 (highest..lowest) 
        // convert config file value to setpriority(2) value(-20..19, -1 is the
        // default)
        if (initialBPP > 0)
            m_BulkProcessPriority = 20 - initialBPP;
        else if (initialBPP < 0)
            m_BulkProcessPriority = 19;

        if (m_BulkProcessPriority < -20)
            m_BulkProcessPriority = -20;
    }

    //--------------------------------------------------------------------------
    // Initialize bulk rollback directory
    // Note this uses m_bulkRoot, so this init section must be after the section
    // that sets m_bulkRoot.
    //--------------------------------------------------------------------------
    m_BulkRollbackDir = cf->getConfig("WriteEngine", "BulkRollbackDir");
    if (m_BulkRollbackDir.length() == 0)
    {
        m_BulkRollbackDir.assign( m_bulkRoot );
        m_BulkRollbackDir += "/rollback";
    }

    //--------------------------------------------------------------------------
    // Initialize max disk usage
    //--------------------------------------------------------------------------
    m_MaxFileSystemDiskUsage = DEFAULT_MAX_FILESYSTEM_DISK_USAGE;
    string usg = cf->getConfig("WriteEngine", "MaxFileSystemDiskUsagePct");
    if ( usg.length() != 0 )
        m_MaxFileSystemDiskUsage = cf->uFromText(usg);
    if (m_MaxFileSystemDiskUsage > 100)
        m_MaxFileSystemDiskUsage = DEFAULT_MAX_FILESYSTEM_DISK_USAGE;

    //--------------------------------------------------------------------------
    // Number of compressed padding blocks
    //--------------------------------------------------------------------------
    m_NumCompressedPadBlks = DEFAULT_COMPRESSED_PADDING_BLKS;
    string ncpb = cf->getConfig("WriteEngine", "CompressedPaddingBlocks");
    if ( ncpb.length() != 0 )
        m_NumCompressedPadBlks = cf->uFromText(ncpb);

#if 0  // common code, moved to IDBPolicy
    //--------------------------------------------------------------------------
    // IDBDataFile logging
    //--------------------------------------------------------------------------
    bool idblog = false;
    string idblogstr = cf->getConfig("SystemConfig", "DataFileLog");
    if ( idblogstr.length() != 0 )
    {
    	boost::to_upper(idblogstr);
    	idblog = ( idblogstr == "ON" );
    }

    //--------------------------------------------------------------------------
    // Optional File System Plugin - if a HDFS type plugin is loaded
    // then the system will use HDFS for all IDB data files
    //--------------------------------------------------------------------------
    string fsplugin = cf->getConfig("SystemConfig", "DataFilePlugin");
    if ( fsplugin.length() != 0 )
    {
        IDBPolicy::installPlugin(fsplugin);
    }

    //--------------------------------------------------------------------------
    // HDFS file buffering
    //--------------------------------------------------------------------------
    // Maximum amount of memory to use for hdfs buffering.
    bool bUseRdwrMemBuffer = true;  // If true, use in-memory buffering, else use file buffering
    int64_t hdfsRdwrBufferMaxSize = 0;
    string strBufferMaxSize = cf->getConfig("SystemConfig", "hdfsRdwrBufferMaxSize");
    if (strBufferMaxSize.length() == 0)
    {
        // Default is use membuf with no maximum size.
        bUseRdwrMemBuffer = true;
    }
    else
    {
        hdfsRdwrBufferMaxSize = static_cast<int64_t>(cf->uFromText(strBufferMaxSize));
        if ( hdfsRdwrBufferMaxSize == 0 )
        {
            // If we're given a size of 0, turn off membuffering.
            bUseRdwrMemBuffer = false;
        }
    }

    // Directory in which to place file buffer temporary files.
    string hdfsRdwrScratch = cf->getConfig("SystemConfig", "hdfsRdwrScratch");
    if ( hdfsRdwrScratch.length() == 0 )
    {
        hdfsRdwrScratch = "/tmp/hdfsscratch";
    }

	IDBPolicy::init( idblog, bUseRdwrMemBuffer, hdfsRdwrScratch, hdfsRdwrBufferMaxSize );
#endif

	IDBPolicy::configIDBPolicy();

    //--------------------------------------------------------------------------
    // Initialize Parent OAM Module flag
    // Initialize Module Type
    // Initialize Local Module ID
    //--------------------------------------------------------------------------
    oam::Oam oam;
    oam::oamModuleInfo_t t;
    try {
        t = oam.getModuleInfo();
        m_ParentOAMModuleFlag = boost::get<4>(t);
        m_LocalModuleType     = boost::get<1>(t);
        m_LocalModuleID       = boost::get<2>(t);
    }
    catch (exception&) {
        m_ParentOAMModuleFlag = DEFAULT_PARENT_OAM;
        m_LocalModuleType.assign( DEFAULT_LOCAL_MODULE_TYPE );
        m_LocalModuleID       = DEFAULT_LOCAL_MODULE_ID;
    }

    //--------------------------------------------------------------------------
    // Initialize Version Buffer
    //--------------------------------------------------------------------------
    m_VersionBufferDir = cf->getConfig("SystemConfig", "DBRMRoot");
    if ( m_VersionBufferDir.length() == 0 )
    {
#ifdef _MSC_VER
        m_VersionBufferDir = startup::StartUp::installDir() + "\\version";
#else
        m_VersionBufferDir =
            startup::StartUp::installDir() + "/data1/systemFiles/dbrm/BRM_saves";
#endif
    }

    //--------------------------------------------------------------------------
    // Initialize m_dbRootCount, m_dbRootPath, m_dbRootPathMap, m_dbRootId.
    // Note this uses m_localModuleType and m_LocalModuleID, so this init
    // section must be after the section(s) that set m_localModuleType and
    // m_LocalModuleID.
    //--------------------------------------------------------------------------
    uint16vec_t dbRootIdPrevious( m_dbRootId );     // save current settings
    strvec_t    dbRootPathPrevious( m_dbRootPath ); // save current setttings

    m_dbRootPath.clear();
    m_dbRootPathMap.clear();
    m_dbRootId.clear();

    if (m_LocalModuleType == "pm")
    {
        oam::DBRootConfigList oamRootList;
        try {
            oam.getPmDbrootConfig( m_LocalModuleID, oamRootList );

            std::sort( oamRootList.begin(), oamRootList.end() );

            m_dbRootCount = oamRootList.size();

            for (unsigned int idx=0; idx<oamRootList.size(); idx++)
            {
                ostringstream oss;
                oss << "DBRoot" << oamRootList[idx];
                std::string DbRootPath = 
                    cf->getConfig("SystemConfig", oss.str());
                m_dbRootPath.push_back( DbRootPath );
                m_dbRootPathMap[ oamRootList[idx] ] = DbRootPath;
                m_dbRootId.push_back( oamRootList[idx] );
            }
        }
        catch (exception&) {
            m_dbRootCount = 0;
        }
    }
    else
    {
        m_dbRootCount = 0;
    }

    // Update counter used to track changes to local PM DBRoot list
    if (!bFirstLoad)
    {
        if ((dbRootIdPrevious   != m_dbRootId) ||
            (dbRootPathPrevious != m_dbRootPath))
        {
            fDBRootChangeCount++;
        }
    }

//  for (unsigned int n=0; n<m_dbRootPath.size(); n++)
//  {
//      std::cout << "dbrootpath: " << n << ". " << m_dbRootPath[n] <<std::endl;
//  }
//  for (unsigned int n=0; n<m_dbRootId.size(); n++)
//  {
//      std::cout << "dbrootId: " << n << ". " << m_dbRootId[n] << std::endl;
//  }
//  for (Config::intstrmap_t::iterator k=m_dbRootPathMap.begin();
//      k!=m_dbRootPathMap.end(); ++k)
//  {
//      std::cout << "dbrootmap: " << k->first << "," << k->second << std::endl;
//  }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root count for local PM
 * PARAMETERS:
 *    none
 * RETURN:
 *    Number of DBRoot paths to be used for database files
 ******************************************************************************/
size_t Config::DBRootCount()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_dbRootCount;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root 
 * PARAMETERS:
 *    idx - Index of the DBRootn entry to fetch (0 fetches DBRoot[0],etc.)
 * RETURN:
 *    DBRoot path for specified index
 ******************************************************************************/
std::string Config::getDBRootByIdx(unsigned idx)
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    if (idx >= m_dbRootPath.size())
    {
        std::string emptyResult;
        return emptyResult;
    }

    return m_dbRootPath[idx];
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root path list for the local PM
 * PARAMETERS:
 *    dbRootPathList - return list of DBRoot paths
 * RETURN:
 *    none
 ******************************************************************************/
void Config::getDBRootPathList( std::vector<std::string>& dbRootPathList )
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    dbRootPathList.clear();
    dbRootPathList = m_dbRootPath;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get db root 
 * PARAMETERS:
 *    num - DBRootN entry to fetch (1 fetches DBRoot1, etc.)
 * RETURN:
 *    DBRoot path for specified index
 ******************************************************************************/
std::string Config::getDBRootByNum(unsigned num)
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    Config::intstrmap_t::const_iterator iter = m_dbRootPathMap.find( num );
    if (iter == m_dbRootPathMap.end())
    {
        std::string emptyResult;
        return emptyResult;
    }

    return iter->second; 
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get list of applicable DBRoot ids for this job.
 * PARAMETERS:
 *    N/A
 * RETURN:
 *    The list of DBRoot ids
 ******************************************************************************/
void Config::getRootIdList( std::vector<u_int16_t>& rootIds )
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    rootIds = m_dbRootId;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get bulk root 
 * PARAMETERS:
 *    none 
 * RETURN:
 *    NO_ERROR if success, other otherwise
 ******************************************************************************/
std::string Config::getBulkRoot() 
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_bulkRoot;
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
 *    Get wait period used between tries to get a transaction id.
 * PARAMETERS:
 *    none
 * RETURN:
 *    Wait Period in seconds
 ******************************************************************************/
int Config::getWaitPeriod()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_WaitPeriod;
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
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_FilesPerColumnPartition;
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
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_ExtentsPerSegmentFile;
}  

/*******************************************************************************
 * DESCRIPTION:
 *    Get process priority for cpimport.bin process.
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
 *    cpimport.bin process priority
 ******************************************************************************/
int Config::getBulkProcessPriority()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_BulkProcessPriority;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get bulk rollback directory path.
 * PARAMETERS:
 *    none 
 ******************************************************************************/
std::string Config::getBulkRollbackDir()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_BulkRollbackDir;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get Max percentage of allowable file system disk usage for each DBRoot
 * PARAMETERS:
 *    none 
 ******************************************************************************/
unsigned Config::getMaxFileSystemDiskUsage()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_MaxFileSystemDiskUsage;
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
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_NumCompressedPadBlks;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get Parent OAM Module flag; are we running on active parent OAM node.
 * PARAMETERS:
 *    none
 ******************************************************************************/
bool Config::getParentOAMModuleFlag()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_ParentOAMModuleFlag;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get Module type (ex: "pm")
 * PARAMETERS:
 *    none
 ******************************************************************************/
std::string Config::getLocalModuleType()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_LocalModuleType;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get Module ID number (ex: 1)
 * PARAMETERS:
 *    none
 ******************************************************************************/
u_int16_t Config::getLocalModuleID()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_LocalModuleID;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get version buffer root 
 * PARAMETERS:
 *    none 
 * RETURN:
 *    NO_ERROR if success, other otherwise
 ******************************************************************************/
std::string Config::getVBRoot() 
{
    boost::mutex::scoped_lock lk(fCacheLock);
    checkReload( );

    return m_VersionBufferDir;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Has the DBRoot list for the local PM changed since the last time this
 *    function was called.
 * PARAMETERS:
 *    none
 * RETURN:
 *    returns TRUE if local DBRoot list has changed since the last call.
 ******************************************************************************/
bool Config::hasLocalDBRootListChanged()
{
    boost::mutex::scoped_lock lk(fCacheLock);
    if (fDBRootChangeCount > 0)
    {
        fDBRootChangeCount = 0;
        return true;
    }

    return false;
}


} //end of namespace
