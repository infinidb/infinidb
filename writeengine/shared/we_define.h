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

//  $Id: we_define.h 4282 2012-10-29 16:31:57Z chao $

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
   const short MAX_COLUMN_BOUNDARY     = 8;                 /** @brief Maximum bytes for one column */
   const int   MAX_SIGNATURE_SIZE      = 8000;              /** @brief Maximum size of a dictionary signature value */
   const int   MAX_FIELD_SIZE          = 1000;              /** @brief Maximum length of non-dictionary field value */
   const int   MAX_SEM_NUM             = 16;                /** @brief Maximum number of semaphores in a set  */
   const int   MAX_DB_DIR_LEVEL        = 6;                 /** @brief The maximum level of db directory structure */
   const int   MAX_DB_DIR_NAME_SIZE    = 20;                /** @brief The maximum length of db directory size */
   const short ROW_PER_BYTE            = 8;                 /** @brief Rows per byte in bitmap file */
   const int   BYTE_PER_BLOCK          = 8192;              /** @brief Total number of bytes per data block */
   const int   BYTE_PER_SUBBLOCK       = 256;               /** @brief Total number of bytes per sub block */
   const int   SUBBLOCK_PER_BLOCK      = 32;                /** @brief Total number of sub block within a block */
   const int   ENTRY_PER_SUBBLOCK      = 32;                /** @brief Total number of entries per sub block */
   const int   INITIAL_EXTENT_ROWS_TO_DISK = 256 * 1024;    /** @brief No. rows reserved to disk for 'initial' extent */
   const int   FILE_NAME_SIZE          = 200;               /** @brief The maximum size of file name */
   const uint64_t   MAX_FBO_SIZE       = 68719476735ULL;    /** @brief 2 **36 -1 */
    const long long MAX_ALLOW_ERROR_COUNT = 100000; //Max allowable error count

   const uint16_t   DCTNRY_END_HEADER  = 0xffff ;           /** @brief dictionary related end of header */
   const uint64_t   NOT_USED_PTR       = 0x0 ;              /** @brief dictionary related not continuos ptr */
   const int   HDR_UNIT_SIZE           = 2;                 /** @brief dictionary related hdr unit size*/
   const int   NEXT_PTR_BYTES          = 8;                 /** @brief dictionary related cont ptr size */
   const int   MAX_OP_COUNT            = 1024;              /** @brief dictionary related op max size */
   const int   DCTNRY_HEADER_SIZE      = 14;                /** @brief dictionary header total size*/
   const int   MAX_STRING_CACHE_SIZE   = 1000;
   const int   COLPOSPAIR_NULL_TOKEN_OFFSET= -1;            /** @brief offset value denoting a null token*/
   const uint32_t   BULK_SYSCAT_SESSION_ID = 0;             /** @brief SessionID for syscat queries */

   const char  COL_TYPE_DICT           = 'D';               /** @brief Dictionary type */

   const uint64_t INVALID_LBID         = 0xFFFFFFFFFULL;

   const unsigned int SUBSYSTEM_ID_DDLPROC = 15;
   const unsigned int SUBSYSTEM_ID_DMLPROC = 20;
   const unsigned int SUBSYSTEM_ID_WE      = 19;

   /*****************************************************
   * Default definitions
   ******************************************************/
   const int   DEFAULT_CACHE_BLOCK     = 256;               /** @brief Default value of maximum number of cache block buffer  */
   const int   DEFAULT_CHK_INTERVAL    = 3;                 /** @brief Default value of checkpoint in seconds  */
   const int   DEFAULT_CACHE_PCT_FREE  = 25;                /** @brief Default minimum percentage of free cache */
   const int   DEFAULT_BUFSIZ          = 1*1024*1024;       /** @brief Default setvbuf buffer size */

   const int   BLK_INIT                = 0;
   const int   BLK_READ                = 1;
   const int   BLK_WRITE               = 2;

   /*****************************************************
   * Return code definitions
   ******************************************************/
   const int   NO_ERROR                = 0;                 /** @brief No error */
   const int   NOT_FOUND               = -1;                /** @brief Not found */
   const int   INVALID_NUM             = -1;                /** @brief Invalid number */

   /*****************************************************
   * Error code definition
   ******************************************************/
   const int   ERR_CODEBASE            = 1000;  // Generic error codes
   const int   ERR_FILEBASE            = 1050;  // File-related error codes
   const int   ERR_MEMBASE             = 1100;  // Memory-related error codes
   const int   ERR_XMLBASE             = 1150;  // XML job file error codes
   const int   ERR_TABLEBASE           = 1200;  // Table-related error codes
   const int   ERR_WRAPPERBASE         = 1250;  // DDL/DML API related errors
   const int   ERR_INDEXBASE           = 1300;  // Index-related error codes
   const int   ERR_FMGRBASE            = 1350;  // Freemgr related errors
   const int   ERR_DCTNRYBASE          = 1400;  // Dictionary related errors
   const int   ERR_BULKBASE            = 1450;  // Bulk specific errors
   const int   ERR_BRMBASE             = 1500;  // BRM related errors
   const int   ERR_DMBASE              = 1550;  // Disk manager related errors
   const int   ERR_CACHEBASE           = 1600;  // Cche management errors
   const int   ERR_COMPBASE            = 1650;  // Compression related errors
   const int   ERR_AUTOINCBASE         = 1700;  // Auto-increment related errors
   const int   ERR_BLKCACHEBASE        = 1750;  // Block cache flush related errors
   const int   ERR_METABKUPBASE        = 1800; // Backup bulk meta file errors

   // Generic error
   const int   ERR_UNKNOWN             = ERR_CODEBASE + 1;  /** @brief Generic catch all error */
   const int   ERR_INVALID_PARAM       = ERR_CODEBASE + 2;  /** @brief Invalid parameters */
   const int   ERR_STRUCT_EMPTY        = ERR_CODEBASE + 3;  /** @brief Structure is empty*/
   const int   ERR_SIZE_NOT_MATCH      = ERR_CODEBASE + 4;  /** @brief Size is not match */
   const int   ERR_VALUE_OUTOFRANGE    = ERR_CODEBASE + 5;  /** @brief Value is out of range */
   const int   ERR_LOCK_FAIL           = ERR_CODEBASE + 6;  /** @brief Lock fail */
   const int   ERR_UNLOCK_FAIL         = ERR_CODEBASE + 7;  /** @brief Unlock fail */
   const int   ERR_PARSING             = ERR_CODEBASE + 8;  /** @brief Parsing error */
   const int   ERR_NOT_NULL            = ERR_CODEBASE + 9; /** @brief Not null error */

   // Memory level error
   const int   ERR_MAX_SEM             = ERR_MEMBASE + 1;   /** @brief Maximum semaphores reachs */
   const int   ERR_SEM_EXIST           = ERR_MEMBASE + 2;   /** @brief Semaphore exist */
   const int   ERR_SEM_NOT_EXIST       = ERR_MEMBASE + 3;   /** @brief Semaphore not exist */
   const int   ERR_NO_SEM_RESOURCE     = ERR_MEMBASE + 4;   /** @brief Semaphore resource exhausted */
   const int   ERR_NO_SEM_LOCK         = ERR_MEMBASE + 5;   /** @brief No semaphore lock */
   const int   ERR_NO_MEM              = ERR_MEMBASE + 6;  /** @brief Failed to allocated  memory */

   // File level error
   const int   ERR_FILE_CREATE         = ERR_FILEBASE + 1;  /** @brief File creation error, mostly because file has already existed */
   const int   ERR_FILE_OPEN           = ERR_FILEBASE + 2;  /** @brief Can not open the file, mostly because file not found */
   const int   ERR_FILE_DELETE         = ERR_FILEBASE + 3;  /** @brief Can not delete the file, common reason is file not exist */
   const int   ERR_FILE_EXIST          = ERR_FILEBASE + 4;  /** @brief File alreay exists */
   const int   ERR_FILE_NOT_EXIST      = ERR_FILEBASE + 5;  /** @brief File not exists */
   const int   ERR_FILE_NULL           = ERR_FILEBASE + 6;  /** @brief File is empty */
   const int   ERR_FILE_WRITE          = ERR_FILEBASE + 10; /** @brief Have error in writing to a file */
   const int   ERR_FILE_READ           = ERR_FILEBASE + 11; /** @brief Have error in reading from a file */
   const int   ERR_FILE_SEEK           = ERR_FILEBASE + 12; /** @brief Error in positioning file handle */
   const int   ERR_FILE_NEED_EXTEND    = ERR_FILEBASE + 13; /** @brief File needs to be extended */
   const int   ERR_DIR_CREATE          = ERR_FILEBASE + 14; /** @brief Error in creating directory */
   const int   ERR_DIR_REMOVE          = ERR_FILEBASE + 15; /** @brief Error in removing directory */
   const int   ERR_FILE_FBO_TOO_BIG    = ERR_FILEBASE + 16; /** @brief File FBO is more than 2**36 */
   const int   ERR_FILE_FBO_NEG        = ERR_FILEBASE + 17; /** @brief File FBO is negative */
   const int   ERR_FILE_TRUNCATE       = ERR_FILEBASE + 18; /** @brief Error truncating file */
   const int   ERR_FILE_DISK_SPACE     = ERR_FILEBASE + 19; /** @brief Out of space on file system */
   const int   ERR_FILE_STAT           = ERR_FILEBASE + 20; /** @brief Error getting stats on file */
   const int   ERR_VB_FILE_NOT_EXIST   = ERR_FILEBASE + 21; /** @brief Version buffer file not exists */
   const int   ERR_FILE_FLUSH          = ERR_FILEBASE + 22; /** @brief Error flushing file */

   // XML level error
   const int   ERR_XML_FILE            = ERR_XMLBASE  + 1;  /** @brief File file error, mostly because file not exist */
   const int   ERR_XML_ROOT_ELEM       = ERR_XMLBASE  + 2;  /** @brief Root element error  */
   const int   ERR_XML_EMPTY           = ERR_XMLBASE  + 3;  /** @brief Empty XML file  */
   const int   ERR_XML_PARSE           = ERR_XMLBASE  + 4;  /** @brief Parsing error */

   // table level error
   const int   ERR_TBL_ROW_NOT_FOUND                 = ERR_TABLEBASE + 1; /** @brief Row not found*/
   const int   ERR_TBL_TABLE_HAS_VALID_CPIMPORT_LOCK = ERR_TABLEBASE + 2; /** @brief table locked by cpimport */
   const int   ERR_TBL_TABLE_HAS_VALID_DML_DDL_LOCK  = ERR_TABLEBASE + 3; /** @brief table locked by DML/DDL  */
   const int   ERR_TBL_TABLE_LOCK_NOT_FOUND          = ERR_TABLEBASE + 4; /** @brief table has no lock        */
   const int   ERR_TBL_SYSCAT_ERROR            		 = ERR_TABLEBASE + 5; /** @brief Syscatalog query error   */

   // DDL/DML Interface level error
   const int   ERR_COL_SIZE_NOT_MATCH     = ERR_WRAPPERBASE + 1;  /** @brief The number of column definitions and values are not match*/
   const int   ERR_STRUCT_VALUE_NOT_MATCH = ERR_WRAPPERBASE + 2;  /** @brief The number of struct not match with the number of value set */
   const int   ERR_ROWID_VALUE_NOT_MATCH  = ERR_WRAPPERBASE + 3;  /** @brief The number of rowid not match with the number of values */
   const int   ERR_INVALID_CHAR_LEN       = ERR_WRAPPERBASE + 3;  /** @brief The len of a char tuple exceeds 8 bytes */
   const int   ERR_INVALID_DATETIME       = ERR_WRAPPERBASE + 4;  /** @brief Invalid Date Time, the error can be used by bulk load */

   // index error
   const int   ERR_IDX_TREE_MOVE_ENTRY    = ERR_INDEXBASE + 1;    /** @brief The error in move part of tree to a new subblock*/
   const int   ERR_IDX_TREE_INVALID_TYPE  = ERR_INDEXBASE + 2;    /** @brief Invalid tree entry type */
   const int   ERR_IDX_TREE_BITTEST_VAL   = ERR_INDEXBASE + 3;    /** @brief Wrong bit test value in the entry */
   const int   ERR_IDX_TREE_INVALID_LEVEL = ERR_INDEXBASE + 4;    /** @brief Invalid testbit treel level */
   const int   ERR_IDX_TREE_INVALID_GRP   = ERR_INDEXBASE + 5;    /** @brief Invalid group type  */
   const int   ERR_IDX_TREE_LISTPTR_CHANGE= ERR_INDEXBASE + 6;    /** @brief List pointer change */
   //index list error
   const int   ERR_IDX_LIST_INVALID_ADDHDR   = ERR_INDEXBASE + 10;/** @brief Create indexlist header error*/
   const int   ERR_IDX_LIST_INVALID_UPDATE   = ERR_INDEXBASE + 11;/** @brief Update Index List error  */
   const int   ERR_IDX_LIST_INVALID_DELETE   = ERR_INDEXBASE + 12;/** @brief Delete rowid in indexlist err*/ 
   const int   ERR_IDX_LIST_INVALID_KEY      = ERR_INDEXBASE + 13;/** @brief Invalid Key passed */
   const int   ERR_IDX_LIST_GET_RID_ARRARY   = ERR_INDEXBASE + 14; /**@brief RID array */
   const int   ERR_IDX_LIST_WRONG_KEY        = ERR_INDEXBASE + 15; /** @brief not matched Key passed */
   const int   ERR_IDX_LIST_HDR_EMPTY        = ERR_INDEXBASE + 16;/** @brief Delete rowid in indexlist err*/ 
   const int   ERR_IDX_LIST_GET_SEGMT        = ERR_INDEXBASE + 17;/** @brief Get Segment */
   const int   ERR_IDX_LIST_WRONG_LBID_WRITE = ERR_INDEXBASE + 18;/** @brief Get Segment */
   const int   ERR_IDX_LIST_UPDATE_SUB       = ERR_INDEXBASE + 19;/** @brief Get Segment */
   const int   ERR_IDX_LIST_UPDATE_NARRAY    = ERR_INDEXBASE + 20;/** @brief Get Segment */
   const int   ERR_IDX_LIST_LAST_FBO_NEG     = ERR_INDEXBASE + 21;/** @brief Get Segment */
   const int   ERR_IDX_LIST_INIT_NEW_BLKS    = ERR_INDEXBASE + 22;/** @brief Get Segment */
   const int   ERR_IDX_LIST_INIT_LINK_BLKS   = ERR_INDEXBASE + 23;/** @brief Get Segment */
   const int   ERR_IDX_LIST_UPDATE_COUNT     = ERR_INDEXBASE + 24;/** @brief Get Segment */
   const int   ERR_IDX_LIST_SET_NEXT_LBID    = ERR_INDEXBASE + 25;/** @brief Get Segment */
   const int   ERR_IDX_LIST_INVALID_LBID     = ERR_INDEXBASE + 26;/** @brief Get Segment */
   const int   ERR_IDX_LIST_INVALID_BLK_READ = ERR_INDEXBASE + 27;/** @brief Get Segment */
   const int   ERR_IDX_LIST_UPDATE_HDR_COUNT = ERR_INDEXBASE + 28;/** @brief Get Segment */
   const int   ERR_IDX_LIST_WRONG_BLK        = ERR_INDEXBASE + 29;/** @brief Get Segment */
   const int   ERR_IDX_LIST_WRONG_TYPE       = ERR_INDEXBASE + 30;/** @brief Get Segment */
   const int   ERR_IDX_LIST_GET_COUNT        = ERR_INDEXBASE + 31;/** @brief Get Segment */
   const int   ERR_IDX_LIST_GET_NEXT         = ERR_INDEXBASE + 32;/** @brief Get Segment */
   const int   ERR_IDX_LIST_GET_PARENT       = ERR_INDEXBASE + 33;
   const int   ERR_IDX_LIST_GET_SUB_BLK      = ERR_INDEXBASE + 34;
   const int   ERR_IDX_LIST_INVALID_UP_HDR   = ERR_INDEXBASE + 35;/** @brief Update Index List error  */
   const int   ERR_IDX_LIST_INVALID_ADD_LIST = ERR_INDEXBASE + 36;/** @brief Update Index List error  */
   const int   ERR_IDX_LIST_INVALID_UP       = ERR_INDEXBASE + 37;/** @brief Update Index List error  */

   //freemgr error
   const int   ERR_FM_ASSIGN_ERR      = ERR_FMGRBASE + 1;   /** @brief General assignment error*/
   const int   ERR_FM_RELEASE_ERR     = ERR_FMGRBASE + 2;   /** @brief General release error*/
   const int   ERR_FM_BAD_FBO         = ERR_FMGRBASE + 3;   /** @brief File Block Offset error*/
   const int   ERR_FM_BAD_TYPE        = ERR_FMGRBASE + 4;   /** @brief type must be pointer or list */
   const int   ERR_FM_NO_SB_SPACE     = ERR_FMGRBASE + 5;   /** @brief No subblocks available */
   const int   ERR_FM_NO_SPACE        = ERR_FMGRBASE + 6;   /** @brief No blocks available*/
   const int   ERR_FM_EXTEND          = ERR_FMGRBASE + 7;   /** @brief Error while extending file*/
   
   // Dictionary error
   const int   ERR_DICT_INVALID_INSERT  = ERR_DCTNRYBASE+ 1;/**@brief insert*/
   const int   ERR_DICT_INVALID_DELETE  = ERR_DCTNRYBASE+ 2;/**@brief Delete */ 
   const int   ERR_DICT_NO_SPACE_INSERT = ERR_DCTNRYBASE+ 3;/**@brief ins no space*/
   const int   ERR_DICT_SIZE_GT_8000    = ERR_DCTNRYBASE+ 4;/**@brief ins size >8000*/
   const int   ERR_DICT_NO_OP_DELETE    = ERR_DCTNRYBASE+ 5;/**@brief del no op*/
   const int   ERR_DICT_NO_FBO_DELETE   = ERR_DCTNRYBASE+ 6;/**@brief Del no fbo*/
   const int   ERR_DICT_NO_OFFSET_DELETE= ERR_DCTNRYBASE+ 7;/**@brief Delete bad offset*/
   const int   ERR_DICT_INVALID_HDR     = ERR_DCTNRYBASE+ 8;/**@brief Delete Hdr */
   const int   ERR_DICT_ZERO_LEN        = ERR_DCTNRYBASE+ 9;/**@brief Delete zero len*/
   const int   ERR_DICT_INVALID_INIT_HDR= ERR_DCTNRYBASE+ 10;/**@brief init header*/
   const int   ERR_DICT_TOKEN_NOT_FOUND = ERR_DCTNRYBASE+ 11;/**@brief token not found*/ 
   const int   ERR_DICT_FILE_NOT_FOUND  = ERR_DCTNRYBASE+ 12;/**@brief dict file not found*/
   const int   ERR_DICT_SIGVALUE_LT_8B  = ERR_DCTNRYBASE+ 13;/**@brief dict file not found*/
   const int   ERR_DICT_DROP_WRONG_OID  = ERR_DCTNRYBASE+ 14;/**@brief dict file not found*/
   const int   ERR_DICT_BAD_TOKEN_ARRAY = ERR_DCTNRYBASE+ 15;/**@brief token array bad token*/
   const int   ERR_DICT_BAD_TOKEN_LBID  = ERR_DCTNRYBASE+ 16;/**@brief token lbid is bad*/
   const int   ERR_DICT_BAD_TOKEN_OP    = ERR_DCTNRYBASE+ 17;/**@brief token op is bad*/


   // Bulk error
   const int   ERR_BULK_MAX_ERR_NUM     = ERR_BULKBASE + 1; /** @brief Maximum number of error rows reached */
   const int   ERR_BULK_DATA_COL_NUM    = ERR_BULKBASE + 2; /** @brief The total number of data column not match with column definitions */
   const int   ERR_BULK_SEND_MSG_ERR    = ERR_BULKBASE + 3; /** @brief send msg to primproc to flush cache */
   const int   ERR_BULK_MISSING_EXTENT_ENTRY = ERR_BULKBASE + 4; /** @brief Missing Extent Entry when trying to save LBID info */
   const int   ERR_BULK_MISSING_EXTENT_ROW   = ERR_BULKBASE + 5; /** @brief Missing Extent Row when trying to save LBID info */
   const int   ERR_BULK_ROW_FILL_BUFFER = ERR_BULKBASE + 6; /** @brief Single row fills read buffer */

   // BRM error
   const int   ERR_BRM_LOOKUP_LBID      = ERR_BRMBASE + 1;  /** @brief Lookup LBID error */
   const int   ERR_BRM_LOOKUP_FBO       = ERR_BRMBASE + 2;  /** @brief Lookup FBO error */
   const int   ERR_BRM_ALLOC_EXTEND     = ERR_BRMBASE + 3;  /** @brief Allocate extent error */
   const int   ERR_BRM_COMMIT           = ERR_BRMBASE + 4;  /** @brief Commit error */
   const int   ERR_BRM_ROLLBACK         = ERR_BRMBASE + 5;  /** @brief Rollback error */
   const int   ERR_BRM_GET_UNCOMM_LBID  = ERR_BRMBASE + 6;  /** @brief Get uncommitted lbid list error */
   const int   ERR_BRM_DEL_OID          = ERR_BRMBASE + 7;  /** @brief Delete oid error */
   const int   ERR_BRM_BEGIN_COPY       = ERR_BRMBASE + 8;  /** @brief Begin copy error */
   const int   ERR_BRM_END_COPY         = ERR_BRMBASE + 9;  /** @brief End copy error */
   const int   ERR_BRM_GET_HWM          = ERR_BRMBASE + 10; /** @brief Get hwm error */
   const int   ERR_BRM_SET_HWM          = ERR_BRMBASE + 11; /** @brief Set hwm error */
   const int   ERR_BRM_WR_VB_ENTRY      = ERR_BRMBASE + 12; /** @brief Write VB entry error */
   const int   ERR_BRM_VB_COPY_READ     = ERR_BRMBASE + 13; /** @brief VB copy read error */
   const int   ERR_BRM_VB_COPY_SEEK_DB  = ERR_BRMBASE + 14; /** @brief VB copy seek error to DB file */
   const int   ERR_BRM_VB_COPY_SEEK_VB  = ERR_BRMBASE + 15; /** @brief VB copy seek error to VB file */
   const int   ERR_BRM_VB_COPY_WRITE    = ERR_BRMBASE + 16; /** @brief VB copy write */
   const int   ERR_BRM_DEAD_LOCK        = ERR_BRMBASE + 17; /** @brief DEAD lock error */
   const int   ERR_BRM_MARK_INVALID     = ERR_BRMBASE + 18; /** @brief Mark extent invalid error from casual paritioning */
   const int   ERR_BRM_SAVE_STATE       = ERR_BRMBASE + 19; /** @brief Save state error */
   const int   ERR_BRM_GET_START_EXTENT = ERR_BRMBASE + 20; /** @brief Get starting Extent error */
   const int   ERR_BRM_VB_OVERFLOW      = ERR_BRMBASE + 21; /** @brief Version buffer overflow */
   const int   ERR_BRM_READ_ONLY        = ERR_BRMBASE + 22; /** @brief BRM is in READ-ONLY state */
   const int   ERR_BRM_GET_READ_WRITE   = ERR_BRMBASE + 23; /** @brief error getting BRM READ/WRITE state */
   const int   ERR_BRM_GET_TABLE_LOCK   = ERR_BRMBASE + 24; /** @brief error getting table lock */
   const int   ERR_BRM_SET_TABLE_LOCK   = ERR_BRMBASE + 25; /** @brief error setting table lock */   
   const int   ERR_BRM_BULK_RB_COLUMN   = ERR_BRMBASE + 26; /** @brief error during column bulk rollback */
   const int   ERR_BRM_BULK_RB_DCTNRY   = ERR_BRMBASE + 27; /** @brief error during dctnry bulk rollback */
   const int   ERR_BRM_DELETE_EXTENT_COLUMN = ERR_BRMBASE + 28; /** @brief error during delete column extents */
   const int   ERR_BRM_DELETE_EXTENT_DCTNRY = ERR_BRMBASE + 29; /** @brief error during delete dictionary extents */
   const int   ERR_BRM_TAKE_SNAPSHOT    = ERR_BRMBASE + 30; /** @brief Taking snapshot of BRM state */
   const int   ERR_BRM_LOOKUP_START_LBID= ERR_BRMBASE + 31; /** @brief Lookup starting LBID error */
   const int   ERR_BRM_BULK_UPDATE      = ERR_BRMBASE + 32;// Error with bulk update of HWM and CP
   const int   ERR_BRM_FLUSH_INODE_CACHE= ERR_BRMBASE + 33; /** @brief Error flushing inode cache */
   const int   ERR_EXTENTMAP_LOOKUP       = ERR_BRMBASE + 34;  /** @brief Lookup extent map error */
   const int   ERR_BRM_LOOKUP_VERSION     = ERR_BRMBASE + 35;  /** @brief Lookup version error */
   const int   ERR_BRM_LOOKUP_LBID_RANGES = ERR_BRMBASE + 36;  /** @brief Lookup LBID Ranges error */
   const int   ERR_BRM_HWMS_NOT_EQUAL     = ERR_BRMBASE + 37;  /** @brief HWMs of same col width not equal */
   const int   ERR_BRM_HWMS_OUT_OF_SYNC   = ERR_BRMBASE + 38;  /** @brief HWMs for dif col width not in sync */
   const int   ERR_BRM_NETWORK         = ERR_BRMBASE + 39;// Network error when calling BRM functions

   // DM error
   const int   ERR_DM_CONVERT_OID       = ERR_DMBASE + 1;   /** @brief Conversion error */

   // Cache error 
   const int   ERR_CACHE_KEY_EXIST      = ERR_CACHEBASE + 1;/** @brief Cache key exist */
   const int   ERR_CACHE_KEY_NOT_EXIST  = ERR_CACHEBASE + 2;/** @brief Cache key not exist */
   const int   ERR_NULL_BLOCK           = ERR_CACHEBASE + 3;/** @brief Block is NULL */
   const int   ERR_FREE_LIST_EMPTY      = ERR_CACHEBASE + 4;/** @brief Free list is empty */

   // Compression error 
   const int   ERR_COMP_COMPRESS        = ERR_COMPBASE + 1; /** @brief Error compressing data */
   const int   ERR_COMP_UNCOMPRESS      = ERR_COMPBASE + 2; /** @brief Error uncompressing data */
   const int   ERR_COMP_PARSE_HDRS      = ERR_COMPBASE + 3; /** @brief Error parsing compression headers */
   const int   ERR_COMP_VERIFY_HDRS     = ERR_COMPBASE + 4; /** @brief Error verifying compression headers */
   const int   ERR_COMP_PAD_DATA        = ERR_COMPBASE + 5; /** @brief Pad compressed data failed */
   const int   ERR_COMP_READ_BLOCK      = ERR_COMPBASE + 6; /** @brief Failed to read a block */
   const int   ERR_COMP_SAVE_BLOCK      = ERR_COMPBASE + 7; /** @brief Failed to save a block */
   const int   ERR_COMP_WRONG_PTR       = ERR_COMPBASE + 8; /** @brief Pointer in header is wrong */
   const int   ERR_COMP_FILE_NOT_FOUND  = ERR_COMPBASE + 9; /** @brief File not found in map */
   const int   ERR_COMP_CHUNK_NOT_FOUND = ERR_COMPBASE + 10; /** @brief Chunk not found in map */
   const int   ERR_COMP_UNAVAIL_TYPE    = ERR_COMPBASE + 11; /** @brief Unavailable compression type */
   const int   ERR_COMP_REMOVE_FILE     = ERR_COMPBASE + 12; /** @brief Failed to remove a file */
   const int   ERR_COMP_RENAME_FILE     = ERR_COMPBASE + 13; /** @brief Failed to rename a file */
   const int   ERR_COMP_OPEN_FILE       = ERR_COMPBASE + 14; /** @brief Failed to open a compressed data file */
   const int   ERR_COMP_SET_OFFSET      = ERR_COMPBASE + 15; /** @brief Failed to set offset in a compressed data file */
   const int   ERR_COMP_READ_FILE       = ERR_COMPBASE + 16; /** @brief Failed to read from a compressed data file */
   const int   ERR_COMP_WRITE_FILE      = ERR_COMPBASE + 17; /** @brief Failed to write to a compresssed data file */
   const int   ERR_COMP_CLOSE_FILE      = ERR_COMPBASE + 18; /** @brief Failed to close a compressed data file */
   const int   ERR_COMP_TRUNCATE_ZERO   = ERR_COMPBASE + 19; /** @brief Invalid attempt to truncate file to 0 bytes */

   // Auto-increment error
   const int   ERR_AUTOINC_GEN_EXCEED_MAX    = ERR_AUTOINCBASE + 1; /** @brief Generated autoinc value exceeds max auto increment value */
   const int   ERR_AUTOINC_USER_OUT_OF_RANGE = ERR_AUTOINCBASE + 2; /** @brief User specified autoinc value is out of range */
   const int   ERR_AUTOINC_TABLE_NAME   = ERR_AUTOINCBASE + 3; /** @brief Invalid schema/tablename for auto increment */
   const int   ERR_AUTOINC_INIT1        = ERR_AUTOINCBASE + 4; /** @brief Error initializing auto increment (known exception) */
   const int   ERR_AUTOINC_INIT2        = ERR_AUTOINCBASE + 5; /** @brief Error initializing auto increment (unknown exception) */
   const int   ERR_AUTOINC_RID          = ERR_AUTOINCBASE + 6; /** @brief Error initializing auto increment (unknown exception) */

   // Block cache flush error
   const int   ERR_BLKCACHE_FLUSH_LIST  = ERR_BLKCACHEBASE +1; /** @brief Error flushing list of blocks to PrimProc */

   // Bulk backup metadata file and corresponding HWM compressed chunk files
   const int   ERR_METADATABKUP_FILE_RENAME         = ERR_METABKUPBASE + 1; /** @brief Error renaming meta file */
   const int   ERR_METADATABKUP_COMP_PARSE_HDRS     = ERR_METABKUPBASE + 2; /** @brief Error parsing compression headers */
   const int   ERR_METADATABKUP_COMP_VERIFY_HDRS    = ERR_METABKUPBASE + 3; /** @brief Error verifying compression headers */
   const int   ERR_METADATABKUP_COMP_CHUNK_NOT_FOUND= ERR_METABKUPBASE + 4; /** @brief Chunk not found in file */
   const int   ERR_METADATABKUP_COMP_OPEN_BULK_BKUP = ERR_METABKUPBASE + 5; /** @brief Error opening backup chunk file */
   const int   ERR_METADATABKUP_COMP_WRITE_BULK_BKUP= ERR_METABKUPBASE + 6; /** @brief Error writing to backup chunk file */
   const int   ERR_METADATABKUP_COMP_READ_BULK_BKUP = ERR_METABKUPBASE + 7; /** @brief Error reading from backup chunk file */
   const int   ERR_METADATABKUP_COMP_RENAME         = ERR_METABKUPBASE + 8; /** @brief Error renaming chunk file */

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
