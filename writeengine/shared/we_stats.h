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
* $Id: we_stats.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_STATS_H_
#define _WE_STATS_H_
#include <we_obj.h>
#ifdef PROFILE
#include <boost/thread/mutex.hpp>
#include "stopwatch.h"
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
struct IoStats
{
    long     blockRead;
    long     blockWrite;
};

/** Class Stats */
class Stats
{
public:
    /**
     * @brief Constructor
     */
    Stats() {}

    /**
     * @brief Default Destructor
     */
    ~Stats() {}

    /**
     * @brief I/O
     */
    static long    getIoBlockRead() { return m_ioStats.blockRead; }
    static long    getIoBlockWrite() { return m_ioStats.blockWrite; }

    static void    incIoBlockRead( const int blockNum = 1 );
    static void    incIoBlockWrite( const int blockNum = 1 );

    static bool    getUseStats() { return m_bUseStats; }
    static void    setUseStats( const bool flag ) { m_bUseStats = flag; }

    static IoStats m_ioStats;                          // IO

#ifdef PROFILE
    // Prefined event labels
    #define WE_STATS_ALLOC_DCT_EXTENT             "AllocDctExtent"
    #define WE_STATS_COMPACT_VARBINARY            "CompactingVarBinary"
    #define WE_STATS_COMPLETING_PARSE             "CompletingParse"
    #define WE_STATS_COMPLETING_READ              "CompletingRead"
    #define WE_STATS_COMPRESS_COL_INIT_ABBREV_EXT "CmpColInitAbbrevExtent"
    #define WE_STATS_COMPRESS_COL_INIT_BUF        "CmpColInitBuf"
    #define WE_STATS_COMPRESS_COL_COMPRESS        "CmpColCompress"
    #define WE_STATS_COMPRESS_COL_FINISH_EXTENT   "CmpColFinishExtent"
    #define WE_STATS_COMPRESS_DCT_INIT_BUF        "CmpDctInitBuf"
    #define WE_STATS_COMPRESS_DCT_COMPRESS        "CmpDctCompress"
    #define WE_STATS_COMPRESS_DCT_SEEKO_CHUNK     "CmpDctSeekOutputChunk"
    #define WE_STATS_COMPRESS_DCT_WRITE_CHUNK     "CmpDctWriteChunk"
    #define WE_STATS_COMPRESS_DCT_SEEKO_HDR       "CmpDctSeekOutputHdr"
    #define WE_STATS_COMPRESS_DCT_WRITE_HDR       "CmpDctWriteHdr"
    #define WE_STATS_COMPRESS_DCT_BACKUP_CHUNK    "CmpDctBackupChunk"
    #define WE_STATS_CREATE_COL_EXTENT            "CreateColExtent"
    #define WE_STATS_CREATE_DCT_EXTENT            "CreateDctExtent"
    #define WE_STATS_EXPAND_COL_EXTENT            "ExpandColExtent"
    #define WE_STATS_EXPAND_DCT_EXTENT            "ExpandDctExtent"
    #define WE_STATS_FLUSH_PRIMPROC_BLOCKS        "FlushPrimProcBlocks"
    #define WE_STATS_INIT_COL_EXTENT              "InitColExtent"
    #define WE_STATS_INIT_DCT_EXTENT              "InitDctExtent"
    #define WE_STATS_OPEN_DCT_FILE                "OpenDctFile"
    #define WE_STATS_PARSE_COL                    "ParseCol"
    #define WE_STATS_PARSE_DCT                    "ParseDct"
    #define WE_STATS_PARSE_DCT_SEEK_EXTENT_BLK    "ParseDctSeekExtentBlk"
    #define WE_STATS_READ_INTO_BUF                "ReadIntoBuf"
    #define WE_STATS_RESIZE_OUT_BUF               "ResizeOutBuf"
    #define WE_STATS_WAIT_FOR_INTERMEDIATE_FLUSH  "WaitForIntermediateFlush"
    #define WE_STATS_WAIT_FOR_READ_BUF            "WaitForReadBuf"
    #define WE_STATS_WAIT_TO_COMPLETE_PARSE       "WaitCompleteParse"
    #define WE_STATS_WAIT_TO_COMPLETE_READ        "WaitCompleteRead"
    #define WE_STATS_WAIT_TO_CREATE_COL_EXTENT    "WaitCreateColExtent"
    #define WE_STATS_WAIT_TO_CREATE_DCT_EXTENT    "WaitCreateDctExtent"
    #define WE_STATS_WAIT_TO_EXPAND_COL_EXTENT    "WaitExpandColExtent"
    #define WE_STATS_WAIT_TO_EXPAND_DCT_EXTENT    "WaitExpandDctExtent"
    #define WE_STATS_WAIT_TO_PARSE_DCT            "WaitParseDct"
    #define WE_STATS_WAIT_TO_RELEASE_OUT_BUF      "WaitReleaseOutBuf"
    #define WE_STATS_WAIT_TO_RESERVE_OUT_BUF      "WaitReserveOutBuf"
    #define WE_STATS_WAIT_TO_RESIZE_OUT_BUF       "WaitResizeOutBuf"
    #define WE_STATS_WAIT_TO_SELECT_COL           "WaitSelectCol"
    #define WE_STATS_WAIT_TO_SELECT_TBL           "WaitSelectTbl"
    #define WE_STATS_WRITE_COL                    "WriteCol"
    #define WE_STATS_WRITE_DCT                    "WriteDct"

    // Functions used to support performance profiling
    static void    enableProfiling(int nReadThreads, int nParseThreads);
    static void    registerReadProfThread ( );
    static void    registerParseProfThread( );
    static void    startReadEvent ( const std::string& eventString )
                   { readEvent ( eventString, true  ); }
    static void    stopReadEvent  ( const std::string& eventString )
                   { readEvent ( eventString, false ); }
    static void    startParseEvent( const std::string& eventString )
                   { parseEvent( eventString, true  ); }
    static void    stopParseEvent ( const std::string& eventString )
                   { parseEvent( eventString, false ); }
    static void    printProfilingResults( );
#endif

private:
    static bool    m_bUseStats;                        // Use statistic flag

#ifdef PROFILE
    // Data members and functions used to support performance profiling
    static bool    fProfiling;                         // Is profiling enabled

    // Protect concurrent addition of Readers
    static boost::mutex fRegisterReaderMutex;

    // Protect concurrent addition of Parsers
    static boost::mutex fRegisterParseMutex;

    // Read threads to be profiled
    static std::vector<pthread_t> fReadProfThreads;

    // Parse threads to be profiled
    static std::vector<pthread_t> fParseProfThreads;

    // Track/profile Read events
    static std::vector<logging::StopWatch> fReadStopWatch;

    // Track/profile Parse events
    static std::vector<logging::StopWatch> fParseStopWatch;

    static void    readEvent  ( const std::string& eventString, bool start );
    static void    parseEvent ( const std::string& eventString, bool start );
#endif
};


} //end of namespace
#endif // _WE_STATIS_H_
