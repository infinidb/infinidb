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
* $Id: we_config.h 2873 2011-02-08 14:35:57Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef WE_CONFIG_H_
#define WE_CONFIG_H_

#include <string>
#include <boost/thread.hpp>
#include <vector>

#include "we_obj.h"

//#define SHARED_NOTHING_DEMO_2

#if defined(_MSC_VER) && defined(WRITEENGINECONFIG_DLLEXPORT)
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
	 * @brief Get DB root
	 * @param idx Index of the DBRootn entry to fetch (0 fetches DBRoot1, etc.)
	 */
	EXPORT static const char*   getDBRoot(unsigned idx);
#ifdef SHARED_NOTHING_DEMO_2
	EXPORT static void getSharedNothingRoot(char *);  // pass in an char[FILE_NAME_SIZE]
#endif

	/**
	 * @brief Bulkload DB root
	 */
	EXPORT static const char*   getBulkRoot();
	EXPORT static const int     sendMsgPM();

	/**
	 * @brief DBRoot count
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
	 * @brief Process Priority for cpimport
	 * Return value is in range -20..19 (highest...lowest, 0=normal)
	 */
	EXPORT static int getBulkProcessPriority();

	/**
	 * @brief Directory carrying Bulk Rollback meta data files
	 */
	EXPORT static const char* getBulkRollbackDir();

	/**
	 * @brief Max percentage of allowable file system disk usage for each DBRoot
	 */
	EXPORT static unsigned getMaxFileSystemDiskUsage();

	/**
	 * @brief Number of Blocks to pad each compressed chunk.
	 */
	EXPORT static unsigned getNumCompressedPadBlks();
   
	/**
	 * @brief Cache the config parameters locally
	 * If there are any parms that we want to support their dynamically
	 * changing in the Calpont.xml file, then they should not be cached
	 * locally.  Any such parms that might change "on the fly" should be
	 * reacquired from the config::Config class each time.
	 * Currently, all the config parms collected by WriteEngine::Config
 	 * are assumed to be static, and can be cached locally, by calling
	 * initConfigCache at the start of the program.
	 */
	EXPORT static void initConfigCache();
   
private:
	typedef std::vector<std::string> strvec_t;

	static void doDBRootInit();

	static int          m_dbRootCount;           // number of DBRoot paths
	static strvec_t     m_dbRoot;                // root paths for open files
	static std::string  m_bulkRoot;              // root path for bulk operation
	static boost::mutex m_dbRoot_lk;             // mutex for m_dbRoot sync
	static boost::mutex m_bulkRoot_lk;           // mutex for m_bulkRoot sync
	static int          m_WaitPeriod;            // secs to wait for transaction
	static unsigned     m_FilesPerColumnPartition;//# seg files per partition
	static unsigned     m_ExtentsPerSegmentFile; // # extents per segment file
	static int          m_BulkProcessPriority;   // cpimport process priority
	static std::string  m_BulkRollbackDir;       // bulk rollback meta data dir
	static unsigned     m_MaxFileSystemDiskUsage;// max file system % disk usage
	static bool         m_UseLocalCachedValues;  // are config parms cached
	static unsigned     m_NumCompressedPadBlks;  // num blks to pad comp chunks
};

} //end of namespace

#undef EXPORT

#endif // WE_CONFIG_H_
