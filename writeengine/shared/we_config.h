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
* $Id: we_config.h 4726 2013-08-07 03:38:36Z bwilkinson $
*
*******************************************************************************/
/** @file */

#ifndef WE_CONFIG_H_
#define WE_CONFIG_H_

#include <string>
#include <boost/thread.hpp>
#include <vector>
#include <map>

#include "we_obj.h"

//#define SHARED_NOTHING_DEMO_2

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class Config */
class Config 
{
public:
    /**
     * @brief Constructor
     */
    Config() {}

    /**
     * @brief Default Destructor
     */
    ~Config() {}

    /**
     * @brief Get DB root (for local PM)
     * @param idx Index of the DBRootn entry to fetch (0 fetches DBRoot[0],etc.)
     */
    EXPORT static std::string getDBRootByIdx(unsigned idx);

    /**
     * @brief Get complete DBRoot path list for the local PM
     * @param dbRootPathList vector of DBRoot paths
     */
    EXPORT static void getDBRootPathList(
        std::vector<std::string>& dbRootPathList );

    /**
     * @brief Get DB root (for local PM)
     * @param num DBRootN entry to fetch (1 fetches DBRoot1, etc.)
     */
    EXPORT static std::string getDBRootByNum(unsigned num);

    /**
     * @brief Get list of applicable DBRoot ids for this job.
     */
    EXPORT static void getRootIdList( std::vector<uint16_t>& dbRootIds );

#ifdef SHARED_NOTHING_DEMO_2
    EXPORT static void getSharedNothingRoot(char *);  // pass in an char[FILE_NAME_SIZE]
#endif

    /**
     * @brief Bulkload DB root
     */
    EXPORT static std::string getBulkRoot();

    /**
     * @brief DBRoot count for local PM
     */
    EXPORT static size_t DBRootCount();

    /**
     * @brief Wait Period
     */
    EXPORT static int getWaitPeriod();
   
    /**
     * @brief FilesPerColumnPartition
     */
    EXPORT static unsigned getFilesPerColumnPartition();

    /**
     * @brief ExtentsPerSegmentFile
     */
    EXPORT static unsigned getExtentsPerSegmentFile();

    /**
     * @brief Process Priority for cpimport.bin
     * Return value is in range -20..19 (highest...lowest, 0=normal)
     */
    EXPORT static int getBulkProcessPriority();

    /**
     * @brief Directory carrying Bulk Rollback meta data files
     */
    EXPORT static std::string getBulkRollbackDir();

    /**
     * @brief Max percentage of allowable file system disk usage for each DBRoot
     */
    EXPORT static unsigned getMaxFileSystemDiskUsage();

    /**
     * @brief Number of Blocks to pad each compressed chunk.
     */
    EXPORT static unsigned getNumCompressedPadBlks();

    /**
     * @brief Parent OAM Module flag (is this the parent OAM node, ex: pm1)
     */
    EXPORT static bool getParentOAMModuleFlag();

    /**
     * @brief Local Module Type (ex: "pm")
     */
    EXPORT static std::string getLocalModuleType();

    /**
     * @brief Local Module ID   (ex: 1   )
     */
    EXPORT static uint16_t getLocalModuleID();

    /**
     * @brief Version Buffer root
     */
    EXPORT static std::string getVBRoot();
   
    /**
     * @brief Cache the config parameters locally
     * Initialize Config cache.  Cache will be updated as needed.
     */
    EXPORT static void initConfigCache();

    /**
     * @brief Has Local PM DBRoot info changed since last time this function
     * was called.  Can be used to monitor changes to DBRoot info.
     */
    EXPORT static bool hasLocalDBRootListChanged();

private:
    typedef std::vector<std::string>  strvec_t;
    typedef std::map<int,std::string> intstrmap_t;
    typedef std::vector<uint16_t>    uint16vec_t;

    static void         checkReload();

    static int          m_dbRootCount;           // num DBRoots for local PM
    static strvec_t     m_dbRootPath;            // root paths for open files
    static intstrmap_t  m_dbRootPathMap;         // map of root id to root paths
    static uint16vec_t  m_dbRootId;              // list of root ids
    static std::string  m_bulkRoot;              // root path for bulk operation
    static unsigned long fDBRootChangeCount;     // track recent DBRoot changes
    static time_t       fCacheTime;              // timestamp associated w/cache
    static boost::mutex fCacheLock;              // mutex for m_dbRoot sync
#ifdef SHARED_NOTHING_DEMO_2
    static boost::mutex m_bulkRoot_lk;           // mutex for m_bulkRoot sync
#endif
    static int          m_WaitPeriod;            // secs to wait for transaction
    static unsigned     m_FilesPerColumnPartition;//# seg files per partition
    static unsigned     m_ExtentsPerSegmentFile; // # extents per segment file
    static int          m_BulkProcessPriority;   // cpimport.bin proc priority
    static std::string  m_BulkRollbackDir;       // bulk rollback meta data dir
    static unsigned     m_MaxFileSystemDiskUsage;// max file system % disk usage
    static unsigned     m_NumCompressedPadBlks;  // num blks to pad comp chunks
    static bool         m_ParentOAMModuleFlag;   // are we running on parent PM
    static std::string  m_LocalModuleType;       // local node type (ex: "pm")
    static int          m_LocalModuleID;         // local node id   (ex: 1   )
    static std::string  m_VersionBufferDir;      // Version buffer directory 
};

} //end of namespace

#undef EXPORT

#endif // WE_CONFIG_H_
