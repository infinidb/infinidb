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

//  $Id: we_define.h 4726 2013-08-07 03:38:36Z bwilkinson $

/** @file */

#undef NO_ERROR

#ifndef _WE_DEFINE_H_
#define _WE_DEFINE_H_
#include <string>
#include <map>
#include <stdint.h>

#if defined(_MSC_VER) && defined(WRITEENGINEERRORCODES_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
    const short MAX_COLUMN_BOUNDARY     = 8;        // Max bytes for one column
    const int   MAX_SIGNATURE_SIZE      = 8000;     // Max len of dict sig val
    const int   MAX_FIELD_SIZE          = 1000;     // Max len non-dict fld val
    const int   MAX_DB_DIR_LEVEL        = 6;        // Max lvl of db dir struct
    const int   MAX_DB_DIR_NAME_SIZE    = 20;       // Max len of db dir size
    const short ROW_PER_BYTE            = 8;        // Rows/byte in bitmap file
    const int   BYTE_PER_BLOCK          = 8192;     // Num bytes per data block
    const int   BYTE_PER_SUBBLOCK       = 256;      // Num bytes per sub block
    const int   ENTRY_PER_SUBBLOCK      = 32;       // Num entries per sub block
    const int   INITIAL_EXTENT_ROWS_TO_DISK = 256 * 1024;
                            // Num rows reserved to disk for 'initial' extent
    const int   FILE_NAME_SIZE          = 200;      // Max size of file name
    const long long MAX_ALLOW_ERROR_COUNT = 100000; //Max allowable error count

    //--------------------------------------------------------------------------
    // Dictionary related constants
    //--------------------------------------------------------------------------
    const uint16_t   DCTNRY_END_HEADER  = 0xffff ;  // end of header
    const uint64_t   NOT_USED_PTR       = 0x0 ;     // not continuous ptr
    const int   HDR_UNIT_SIZE           = 2;        // hdr unit size
    const int   NEXT_PTR_BYTES          = 8;        // const ptr size
    const int   MAX_OP_COUNT            = 1024;     // op max size
    const int   DCTNRY_HEADER_SIZE      = 14;       // header total size
    const int   MAX_STRING_CACHE_SIZE   = 1000;
    // End of Dictionary related constants

    const int   COLPOSPAIR_NULL_TOKEN_OFFSET= -1;   // offset value denoting a null token
    const uint32_t   BULK_SYSCAT_SESSION_ID = 0;    // SessionID for syscat queries

    const char  COL_TYPE_DICT           = 'D';      // Dictionary type

    const uint64_t INVALID_LBID         = 0xFFFFFFFFFULL;   // 2**36 - 1

    const unsigned int SUBSYSTEM_ID_DDLPROC = 15;
    const unsigned int SUBSYSTEM_ID_DMLPROC = 20;
    const unsigned int SUBSYSTEM_ID_WE      = 19;
    const unsigned int SUBSYSTEM_ID_WE_SRV  = 32;
    const unsigned int SUBSYSTEM_ID_WE_SPLIT= 33;
    const unsigned int SUBSYSTEM_ID_WE_BULK = 34;

    //--------------------------------------------------------------------------
    // Default definitions
    //--------------------------------------------------------------------------
    const int   DEFAULT_CACHE_BLOCK     = 256;      // Max num of cache blocks
    const int   DEFAULT_CHK_INTERVAL    = 3;        // Checkpoint in seconds
    const int   DEFAULT_CACHE_PCT_FREE  = 25;       // Min % of free cache
    const int   DEFAULT_BUFSIZ          = 1*1024*1024;  // setvbuf buffer size
    const int   DEFAULT_COLSIZ          = 8;        // col size for hdfs rdwr buf

    const int   BLK_INIT                = 0;
    const int   BLK_READ                = 1;
    const int   BLK_WRITE               = 2;

    //--------------------------------------------------------------------------
    // Return code definitions
    //--------------------------------------------------------------------------
    const int   NO_ERROR                = 0;        // No error
    const int   NOT_FOUND               = -1;       // Not found
    const int   INVALID_NUM             = -1;       // Invalid number

    //--------------------------------------------------------------------------
    // Error code definition
    //--------------------------------------------------------------------------
    const int   ERR_CODEBASE            = 1000; // Generic error codes
    const int   ERR_FILEBASE            = 1050; // File-related error codes
    const int   ERR_XMLBASE             = 1150; // XML job file error codes
    const int   ERR_TBLLOCKBASE         = 1200; // Table-lock error codes
    const int   ERR_WRAPPERBASE         = 1250; // DDL/DML API related errors
    const int   ERR_INDEXBASE           = 1300; // Index-related error codes
    const int   ERR_FMGRBASE            = 1350; // Freemgr errors
    const int   ERR_DCTNRYBASE          = 1400; // Dictionary errors
    const int   ERR_BULKBASE            = 1450; // Bulk specific errors
    const int   ERR_BRMBASE             = 1500; // BRM errors
    const int   ERR_DMBASE              = 1550; // Disk manager errors
    const int   ERR_CACHEBASE           = 1600; // Cche management errors
    const int   ERR_COMPBASE            = 1650; // Compression errors
    const int   ERR_AUTOINCBASE         = 1700; // Auto-increment errors
    const int   ERR_BLKCACHEBASE        = 1750; // Block cache flush errors
    const int   ERR_METABKUPBASE        = 1800; // Backup bulk meta file errors

    //--------------------------------------------------------------------------
    // Generic error
    //--------------------------------------------------------------------------
    const int   ERR_UNKNOWN             = ERR_CODEBASE + 1; // Generic error
    const int   ERR_INVALID_PARAM       = ERR_CODEBASE + 2; // Invalid parms
    const int   ERR_STRUCT_EMPTY        = ERR_CODEBASE + 3; // Struct is empty
    const int   ERR_VALUE_OUTOFRANGE    = ERR_CODEBASE + 4; // Val out of range
    const int   ERR_PARSING             = ERR_CODEBASE + 5; // Parsing error
    const int   ERR_NO_MEM              = ERR_CODEBASE + 6; // Mem alloc error
    const int   ERR_DML_LOG_NAME        = ERR_CODEBASE + 7; // DML log filename error
    const int   ERR_OPEN_DML_LOG        = ERR_CODEBASE + 8; // Open DML log file error
    const int   ERR_HDFS_BACKUP         = ERR_CODEBASE + 9; // HDFS backup error

    //--------------------------------------------------------------------------
    // File level error
    //--------------------------------------------------------------------------
    const int   ERR_FILE_CREATE         = ERR_FILEBASE + 1; // File creation error, mostly because file has already existed
    const int   ERR_FILE_OPEN           = ERR_FILEBASE + 2; // Can not open the file, mostly because file not found
    const int   ERR_FILE_DELETE         = ERR_FILEBASE + 3; // Can not delete the file, common reason is file not exist
    const int   ERR_FILE_EXIST          = ERR_FILEBASE + 4; // File alreay exists
    const int   ERR_FILE_NOT_EXIST      = ERR_FILEBASE + 5; // File not exists
    const int   ERR_FILE_NULL           = ERR_FILEBASE + 6; // File is empty
    const int   ERR_FILE_WRITE          = ERR_FILEBASE + 7; // Error writing to a DB file
    const int   ERR_FILE_READ           = ERR_FILEBASE + 8; // Error reading from a DB file
    const int   ERR_FILE_SEEK           = ERR_FILEBASE + 9; // Error in positioning file handle
    const int   ERR_FILE_READ_IMPORT    = ERR_FILEBASE + 10;// Error reading import source file
    const int   ERR_DIR_CREATE          = ERR_FILEBASE + 11;// Error in creating directory
    const int   ERR_FILE_NEW_EXTENT_FBO = ERR_FILEBASE + 12;// New extent fbo too large
    const int   ERR_FILE_FBO_NEG        = ERR_FILEBASE + 13;// File FBO is negative
    const int   ERR_FILE_TRUNCATE       = ERR_FILEBASE + 14;// Error truncating file
    const int   ERR_FILE_DISK_SPACE     = ERR_FILEBASE + 15;// Out of space on file system
    const int   ERR_FILE_STAT           = ERR_FILEBASE + 16;// Error getting stats on file
    const int   ERR_VB_FILE_NOT_EXIST   = ERR_FILEBASE + 17;// Version buffer file not exists
    const int   ERR_FILE_FLUSH          = ERR_FILEBASE + 18;// Error flushing file
    const int   ERR_FILE_GLOBBING       = ERR_FILEBASE + 19;// Error globbing a file name

    //--------------------------------------------------------------------------
    // XML level error
    //--------------------------------------------------------------------------
    const int   ERR_XML_FILE            = ERR_XMLBASE  + 1; // File error, probably because file does not exist
    const int   ERR_XML_ROOT_ELEM       = ERR_XMLBASE  + 2; // Root element err
    const int   ERR_XML_EMPTY           = ERR_XMLBASE  + 3; // Empty XML file
    const int   ERR_XML_PARSE           = ERR_XMLBASE  + 4; // Parsing error

    //--------------------------------------------------------------------------
    // table lock level error
    //--------------------------------------------------------------------------
    const int   ERR_TBLLOCK_LOCK_NOT_FOUND = ERR_TBLLOCKBASE + 1; // table has no lock
    const int   ERR_TBLLOCK_GET_LOCK       = ERR_TBLLOCKBASE + 2; // error acquiring a table lock
    const int   ERR_TBLLOCK_GET_LOCK_LOCKED= ERR_TBLLOCKBASE + 3; // table currently locked
    const int   ERR_TBLLOCK_RELEASE_LOCK   = ERR_TBLLOCKBASE + 4; // error releasing a table lock   
    const int   ERR_TBLLOCK_CHANGE_STATE   = ERR_TBLLOCKBASE + 5; // error changing state of lock
    const int   ERR_TBLLOCK_GET_INFO       = ERR_TBLLOCKBASE + 6; // error getting info about a lock
    const int   ERR_TBLLOCK_LOCKID_CONFLICT= ERR_TBLLOCKBASE + 7; // lockID for different table than expected

    //--------------------------------------------------------------------------
    // DDL/DML Interface level error
    //--------------------------------------------------------------------------
    const int   ERR_STRUCT_VALUE_NOT_MATCH = ERR_WRAPPERBASE + 1; // The number of struct not match with the number of value set
    const int   ERR_ROWID_VALUE_NOT_MATCH  = ERR_WRAPPERBASE + 2; // The number of rowid not match with the number of values
	const int   ERR_TBL_SYSCAT_ERROR       = ERR_WRAPPERBASE + 3; /** @brief Syscatalog query error   */

    //--------------------------------------------------------------------------
    // index error
    //--------------------------------------------------------------------------
    const int   ERR_IDX_TREE_MOVE_ENTRY     = ERR_INDEXBASE + 1; // The error in move part of tree to a new subblock
    const int   ERR_IDX_TREE_INVALID_TYPE   = ERR_INDEXBASE + 2; // Invalid tree entry type
    const int   ERR_IDX_TREE_BITTEST_VAL    = ERR_INDEXBASE + 3; // Wrong bit test value in the entry
    const int   ERR_IDX_TREE_INVALID_LEVEL  = ERR_INDEXBASE + 4; // Invalid testbit treel level
    const int   ERR_IDX_TREE_INVALID_GRP    = ERR_INDEXBASE + 5; // Invalid group type
    const int   ERR_IDX_TREE_LISTPTR_CHANGE = ERR_INDEXBASE + 6; // List pointer change
    //index list error
    const int   ERR_IDX_LIST_INVALID_ADDHDR = ERR_INDEXBASE + 7; // Create indexlist header error
    const int   ERR_IDX_LIST_INVALID_UPDATE = ERR_INDEXBASE + 8; // Update Index List error
    const int   ERR_IDX_LIST_INVALID_DELETE = ERR_INDEXBASE + 9; // Delete rowid in indexlist err*/ 
    const int   ERR_IDX_LIST_INVALID_KEY    = ERR_INDEXBASE + 10;// Invalid Key passed
    const int   ERR_IDX_LIST_GET_RID_ARRARY = ERR_INDEXBASE + 11;// RID array
    const int   ERR_IDX_LIST_WRONG_KEY      = ERR_INDEXBASE + 12;// not matched Key passed
    const int   ERR_IDX_LIST_HDR_EMPTY      = ERR_INDEXBASE + 13;// Delete rowid in indexlist err 
    const int   ERR_IDX_LIST_GET_SEGMT      = ERR_INDEXBASE + 14;// Get Segment
    const int   ERR_IDX_LIST_WRONG_LBID_WRITE=ERR_INDEXBASE + 15;
    const int   ERR_IDX_LIST_UPDATE_SUB     = ERR_INDEXBASE + 16;
    const int   ERR_IDX_LIST_UPDATE_NARRAY  = ERR_INDEXBASE + 17;
    const int   ERR_IDX_LIST_LAST_FBO_NEG   = ERR_INDEXBASE + 18;
    const int   ERR_IDX_LIST_INIT_NEW_BLKS  = ERR_INDEXBASE + 19;
    const int   ERR_IDX_LIST_INIT_LINK_BLKS = ERR_INDEXBASE + 20;
    const int   ERR_IDX_LIST_UPDATE_COUNT   = ERR_INDEXBASE + 21;
    const int   ERR_IDX_LIST_SET_NEXT_LBID  = ERR_INDEXBASE + 22;
    const int   ERR_IDX_LIST_INVALID_LBID   = ERR_INDEXBASE + 23;
    const int   ERR_IDX_LIST_INVALID_BLK_READ=ERR_INDEXBASE + 24;
    const int   ERR_IDX_LIST_UPDATE_HDR_COUNT=ERR_INDEXBASE + 25;
    const int   ERR_IDX_LIST_WRONG_BLK      = ERR_INDEXBASE + 26;
    const int   ERR_IDX_LIST_WRONG_TYPE     = ERR_INDEXBASE + 27;
    const int   ERR_IDX_LIST_GET_COUNT      = ERR_INDEXBASE + 28;
    const int   ERR_IDX_LIST_GET_NEXT       = ERR_INDEXBASE + 29;
    const int   ERR_IDX_LIST_GET_PARENT     = ERR_INDEXBASE + 30;
    const int   ERR_IDX_LIST_GET_SUB_BLK    = ERR_INDEXBASE + 31;
    const int   ERR_IDX_LIST_INVALID_UP_HDR = ERR_INDEXBASE + 32;// Update Index List error
    const int   ERR_IDX_LIST_INVALID_ADD_LIST=ERR_INDEXBASE + 33;// Update Index List error
    const int   ERR_IDX_LIST_INVALID_UP     = ERR_INDEXBASE + 34;// Update Index List error

    //--------------------------------------------------------------------------
    // freemgr error
    //--------------------------------------------------------------------------
    const int   ERR_FM_ASSIGN_ERR   = ERR_FMGRBASE + 1; // General assignment error
    const int   ERR_FM_RELEASE_ERR  = ERR_FMGRBASE + 2; // General release error
    const int   ERR_FM_BAD_FBO      = ERR_FMGRBASE + 3; // File Block Offset err
    const int   ERR_FM_BAD_TYPE     = ERR_FMGRBASE + 4; // type must be pointer or list
    const int   ERR_FM_NO_SPACE     = ERR_FMGRBASE + 5; // No blocks available
    const int   ERR_FM_EXTEND       = ERR_FMGRBASE + 6; // Error extending file
   
    //--------------------------------------------------------------------------
    // Dictionary error
    //--------------------------------------------------------------------------
    const int   ERR_DICT_NO_SPACE_INSERT= ERR_DCTNRYBASE+ 1; // ins no space
    const int   ERR_DICT_SIZE_GT_8000   = ERR_DCTNRYBASE+ 2; // ins size >8000
    const int   ERR_DICT_NO_OP_DELETE   = ERR_DCTNRYBASE+ 3; // del no op
    const int   ERR_DICT_NO_OFFSET_DELETE=ERR_DCTNRYBASE+ 4; // del bad offset
    const int   ERR_DICT_INVALID_HDR    = ERR_DCTNRYBASE+ 5; // Delete Hdr
    const int   ERR_DICT_ZERO_LEN       = ERR_DCTNRYBASE+ 6; // Delete zero len
    const int   ERR_DICT_TOKEN_NOT_FOUND= ERR_DCTNRYBASE+ 7; // token not found
    const int   ERR_DICT_FILE_NOT_FOUND = ERR_DCTNRYBASE+ 8; // dict file not found
    const int   ERR_DICT_BAD_TOKEN_LBID = ERR_DCTNRYBASE+ 9; // bad token lbid
    const int   ERR_DICT_BAD_TOKEN_OP   = ERR_DCTNRYBASE+ 10;// token op is bad

    //--------------------------------------------------------------------------
    // Bulk error
    //--------------------------------------------------------------------------
    const int   ERR_BULK_MAX_ERR_NUM        = ERR_BULKBASE + 1; // Maximum number of error rows reached
    const int   ERR_BULK_DATA_COL_NUM       = ERR_BULKBASE + 2; // The total number of data column not match with column definitions
    const int   ERR_BULK_SEND_MSG_ERR       = ERR_BULKBASE + 3; // send msg to primproc to flush cache
    const int   ERR_BULK_MISSING_EXTENT_ENTRY=ERR_BULKBASE + 4; // Missing Extent Entry when trying to save LBID info
    const int   ERR_BULK_MISSING_EXTENT_ROW = ERR_BULKBASE + 5; // Missing Extent Row when trying to save LBID info
    const int   ERR_BULK_ROW_FILL_BUFFER    = ERR_BULKBASE + 6; // Single row fills read buffer
    const int   ERR_BULK_DBROOT_CHANGE      = ERR_BULKBASE + 7; // Local DBRoot settings changed during an import
    const int   ERR_BULK_ROLLBACK_MISS_ROOT = ERR_BULKBASE + 8; // Mode3 automatic rollback skipped with missing DBRoot
    const int   ERR_BULK_ROLLBACK_SEG_LIST  = ERR_BULKBASE + 9; // Error building segment file list in a directory
    const int   ERR_BULK_BINARY_PARTIAL_REC = ERR_BULKBASE + 10;// Binary input did not end on fixed length record boundary
    const int   ERR_BULK_BINARY_IGNORE_FLD  = ERR_BULKBASE + 11;// <IgnoreField> tag not supported for binary import

    //--------------------------------------------------------------------------
    // BRM error
    //--------------------------------------------------------------------------
    const int   ERR_BRM_LOOKUP_LBID     = ERR_BRMBASE + 1; // Lookup LBID error
    const int   ERR_BRM_LOOKUP_FBO      = ERR_BRMBASE + 2; // Lookup FBO error
    const int   ERR_BRM_ALLOC_EXTEND    = ERR_BRMBASE + 3; // Allocate extent error
    const int   ERR_BRM_COMMIT          = ERR_BRMBASE + 4; // Commit error
    const int   ERR_BRM_ROLLBACK        = ERR_BRMBASE + 5; // Rollback error
    const int   ERR_BRM_GET_UNCOMM_LBID = ERR_BRMBASE + 6; // Get uncommitted lbid list error
    const int   ERR_BRM_DEL_OID         = ERR_BRMBASE + 7; // Delete oid error
    const int   ERR_BRM_BEGIN_COPY      = ERR_BRMBASE + 8; // Begin copy error
    const int   ERR_BRM_END_COPY        = ERR_BRMBASE + 9; // End copy error
    const int   ERR_BRM_GET_HWM         = ERR_BRMBASE + 10;// Get hwm error
    const int   ERR_BRM_SET_HWM         = ERR_BRMBASE + 11;// Set hwm error
    const int   ERR_BRM_WR_VB_ENTRY     = ERR_BRMBASE + 12;// Write VB entry error
    const int   ERR_BRM_VB_COPY_READ    = ERR_BRMBASE + 13;// VB copy read error
    const int   ERR_BRM_VB_COPY_SEEK_DB = ERR_BRMBASE + 14;// VB copy seek error to DB file
    const int   ERR_BRM_VB_COPY_SEEK_VB = ERR_BRMBASE + 15;// VB copy seek error to VB file
    const int   ERR_BRM_VB_COPY_WRITE   = ERR_BRMBASE + 16;// VB copy write
    const int   ERR_BRM_DEAD_LOCK       = ERR_BRMBASE + 17;// DEAD lock error
    const int   ERR_BRM_MARK_INVALID    = ERR_BRMBASE + 18;// Mark extent invalid error from casual paritioning
    const int   ERR_BRM_SAVE_STATE      = ERR_BRMBASE + 19;// Save state error
    const int   ERR_BRM_GET_START_EXTENT= ERR_BRMBASE + 20;// Get starting Extent error
    const int   ERR_BRM_VB_OVERFLOW     = ERR_BRMBASE + 21;// Version buffer overflow
    const int   ERR_BRM_READ_ONLY       = ERR_BRMBASE + 22;// BRM is in READ-ONLY state
    const int   ERR_BRM_GET_READ_WRITE  = ERR_BRMBASE + 23;// error getting BRM READ/WRITE state
    const int   ERR_BRM_BULK_RB_COLUMN  = ERR_BRMBASE + 24;// error during column bulk rollback
    const int   ERR_BRM_BULK_RB_DCTNRY  = ERR_BRMBASE + 25;// error during dctnry bulk rollback
    const int   ERR_BRM_DELETE_EXTENT_COLUMN= ERR_BRMBASE + 26;// error during delete column extents
    const int   ERR_BRM_DELETE_EXTENT_DCTNRY= ERR_BRMBASE + 27;// error during delete dictionary extents
    const int   ERR_BRM_TAKE_SNAPSHOT   = ERR_BRMBASE + 28;// Taking snapshot of BRM state
    const int   ERR_BRM_LOOKUP_START_LBID=ERR_BRMBASE + 29;// Lookup starting LBID error
    const int   ERR_BRM_BULK_UPDATE     = ERR_BRMBASE + 30;// Error with bulk update of HWM and CP
    const int   ERR_BRM_GET_EXT_STATE   = ERR_BRMBASE + 31;// Error getting extent state
    const int   ERR_EXTENTMAP_LOOKUP    = ERR_BRMBASE + 32;// Lookup extent map error
    const int   ERR_BRM_LOOKUP_VERSION  = ERR_BRMBASE + 33;// Lookup version error
    const int   ERR_BRM_LOOKUP_LBID_RANGES  = ERR_BRMBASE + 34;// Lookup LBID Ranges error
    const int   ERR_BRM_HWMS_NOT_EQUAL  = ERR_BRMBASE + 35;// HWMs of same col width not equal
    const int   ERR_BRM_HWMS_OUT_OF_SYNC= ERR_BRMBASE + 36;// HWMs for dif col width not in sync
    const int   ERR_BRM_DBROOT_HWMS     = ERR_BRMBASE + 37;// Error getting HWMs for each DBRoot
    const int   ERR_BRM_NETWORK         = ERR_BRMBASE + 38;// Network error when calling BRM functions
    const int   ERR_BRM_READONLY        = ERR_BRMBASE + 39;// DBRM is readonly 
    const int   ERR_INVALID_VBOID       = ERR_BRMBASE + 40;// returned if the given vboid is invalid
    const int   ERR_BRM_SET_EXTENTS_CP  = ERR_BRMBASE + 41;// Error setting extents min/max
    const int   ERR_BRM_SHUTDOWN        = ERR_BRMBASE + 42;// BRM is set to shutdown
    const int   ERR_BRM_GET_SHUTDOWN    = ERR_BRMBASE + 43;// error getting BRM Shutdown flag
    const int   ERR_BRM_SUSPEND         = ERR_BRMBASE + 44;// BRM is set to Suspend writes
    const int   ERR_BRM_GET_SUSPEND     = ERR_BRMBASE + 45;// error getting BRM Suspend flag
    const int   ERR_BRM_BAD_STRIPE_CNT  = ERR_BRMBASE + 46;// Incorrect num of cols allocated in stripe

    //--------------------------------------------------------------------------
    // DM error
    //--------------------------------------------------------------------------
    const int   ERR_DM_CONVERT_OID      = ERR_DMBASE + 1; // Conversion error

    //--------------------------------------------------------------------------
    // Cache error 
    //--------------------------------------------------------------------------
    const int   ERR_CACHE_KEY_EXIST     = ERR_CACHEBASE + 1; // Cache key exist
    const int   ERR_CACHE_KEY_NOT_EXIST = ERR_CACHEBASE + 2; // Cache key not exist
    const int   ERR_NULL_BLOCK          = ERR_CACHEBASE + 3; // Block is NULL
    const int   ERR_FREE_LIST_EMPTY     = ERR_CACHEBASE + 4; // Empty Free list

    //--------------------------------------------------------------------------
    // Compression error 
    //--------------------------------------------------------------------------
    const int   ERR_COMP_COMPRESS       = ERR_COMPBASE + 1; // Error compressing data
    const int   ERR_COMP_UNCOMPRESS     = ERR_COMPBASE + 2; // Error uncompressing data
    const int   ERR_COMP_PARSE_HDRS     = ERR_COMPBASE + 3; // Error parsing compression headers
    const int   ERR_COMP_VERIFY_HDRS    = ERR_COMPBASE + 4; // Error verifying compression headers
    const int   ERR_COMP_PAD_DATA       = ERR_COMPBASE + 5; // Pad compressed data failed
    const int   ERR_COMP_READ_BLOCK     = ERR_COMPBASE + 6; // Failed to read a block
    const int   ERR_COMP_SAVE_BLOCK     = ERR_COMPBASE + 7; // Failed to save a block
    const int   ERR_COMP_WRONG_PTR      = ERR_COMPBASE + 8; // Pointer in header is wrong
    const int   ERR_COMP_FILE_NOT_FOUND = ERR_COMPBASE + 9; // File not found in map
    const int   ERR_COMP_CHUNK_NOT_FOUND= ERR_COMPBASE + 10;// Chunk not found in map
    const int   ERR_COMP_UNAVAIL_TYPE   = ERR_COMPBASE + 11;// Unavailable compression type
    const int   ERR_COMP_REMOVE_FILE    = ERR_COMPBASE + 12;// Failed to remove a file
    const int   ERR_COMP_RENAME_FILE    = ERR_COMPBASE + 13;// Failed to rename a file
    const int   ERR_COMP_OPEN_FILE      = ERR_COMPBASE + 14;// Failed to open a compressed data file
    const int   ERR_COMP_SET_OFFSET     = ERR_COMPBASE + 15;// Failed to set offset in a compressed data file
    const int   ERR_COMP_READ_FILE      = ERR_COMPBASE + 16;// Failed to read from a compressed data file
    const int   ERR_COMP_WRITE_FILE     = ERR_COMPBASE + 17;// Failed to write to a compresssed data file
    const int   ERR_COMP_CLOSE_FILE     = ERR_COMPBASE + 18;// Failed to close a compressed data file
    const int   ERR_COMP_TRUNCATE_ZERO  = ERR_COMPBASE + 19;// Invalid attempt to truncate file to 0 bytes

    //--------------------------------------------------------------------------
    // Auto-increment error
    //--------------------------------------------------------------------------
    const int   ERR_AUTOINC_GEN_EXCEED_MAX  = ERR_AUTOINCBASE + 1; // Generated autoinc value exceeds max auto increment value/
    const int   ERR_AUTOINC_USER_OUT_OF_RANGE=ERR_AUTOINCBASE + 2; // User specified autoinc value is out of range
    const int   ERR_AUTOINC_TABLE_NAME  = ERR_AUTOINCBASE + 3; // Invalid schema/tablename for auto increment
    const int   ERR_AUTOINC_INIT1       = ERR_AUTOINCBASE + 4; // Error initializing auto increment (known exception)
    const int   ERR_AUTOINC_INIT2       = ERR_AUTOINCBASE + 5; // Error initializing auto increment (unknown exception)
    const int   ERR_AUTOINC_RID         = ERR_AUTOINCBASE + 6; // Error initializing auto increment (unknown exception)
    const int   ERR_AUTOINC_START_SEQ   = ERR_AUTOINCBASE + 7; // Error setting up an auto-increment sequence
    const int   ERR_AUTOINC_GET_RANGE   = ERR_AUTOINCBASE + 8; // Error reserving an auto-increment range
    const int   ERR_AUTOINC_GET_LOCK    = ERR_AUTOINCBASE + 9; // Error getting a lock to update auto-inc next value
    const int   ERR_AUTOINC_REL_LOCK    = ERR_AUTOINCBASE +10; // Error releasing lock to update auto-inc next value
    const int   ERR_AUTOINC_UPDATE      = ERR_AUTOINCBASE +11; // Error updating nextValue in system catalog

    //--------------------------------------------------------------------------
    // Block cache flush error
    //--------------------------------------------------------------------------
    const int   ERR_BLKCACHE_FLUSH_LIST = ERR_BLKCACHEBASE + 1; // Error flushing list of blocks to PrimProc

    //--------------------------------------------------------------------------
    // Bulk backup metadata file and corresponding HWM compressed chunk files
    //--------------------------------------------------------------------------
    const int   ERR_METADATABKUP_FILE_RENAME         = ERR_METABKUPBASE + 1; // Error renaming meta file */
    const int   ERR_METADATABKUP_COMP_PARSE_HDRS     = ERR_METABKUPBASE + 2; // Error parsing compression headers */
    const int   ERR_METADATABKUP_COMP_VERIFY_HDRS    = ERR_METABKUPBASE + 3; // Error verifying compression headers */
    const int   ERR_METADATABKUP_COMP_CHUNK_NOT_FOUND= ERR_METABKUPBASE + 4; // Chunk not found in file */
    const int   ERR_METADATABKUP_COMP_OPEN_BULK_BKUP = ERR_METABKUPBASE + 5; // Error opening backup chunk file */
    const int   ERR_METADATABKUP_COMP_WRITE_BULK_BKUP= ERR_METABKUPBASE + 6; // Error writing to backup chunk file */
    const int   ERR_METADATABKUP_COMP_READ_BULK_BKUP = ERR_METABKUPBASE + 7; // Error reading from backup chunk file */
    const int   ERR_METADATABKUP_COMP_RENAME         = ERR_METABKUPBASE + 8; // Error renaming chunk file */

//------------------------------------------------------------------------------
// Class used to convert an error code to a corresponding error message string
//------------------------------------------------------------------------------
struct WErrorCodes
{
    EXPORT WErrorCodes();
    EXPORT std::string errorString(int code);
private:
    typedef std::map<int, std::string> CodeMap;
    CodeMap fErrorCodes;
};

} //end of namespace

#undef EXPORT

#endif // _WE_DEFINE_H_
