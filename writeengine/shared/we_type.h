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

//  $Id: we_type.h 2972 2011-05-17 20:50:24Z dcathey $

/** @file */

#undef EXPORT
#undef DELETE
#undef NO_ERROR

#ifndef _WE_TYPE_H_
#define _WE_TYPE_H_
#include <sys/types.h>

//#include <fstream>
//#include <iostream>
#include <string>
#include <vector>
#include <boost/any.hpp>
#include <cstring>
#include <stdexcept>

#include "we_define.h"
#include "we_typeext.h"
/** Namespace WriteEngine */
namespace WriteEngine
{
   /************************************************************************
    * Type definitions
    ************************************************************************/
   typedef u_int16_t          i16;                    /** @brief 2 byte value */
   typedef u_int32_t          i32;                    /** @brief 4 byte value */

   typedef i32                UID;                    /** @brief User ID */
   typedef i32                SID;                    /** @brief Session ID */
   typedef i32                OID;                    /** @brief Object ID */
   typedef i32                FID;                    /** @brief File ID */
   typedef i64                RID;                    /** @brief Row ID */
   typedef i32                TransID;                /** @brief Transaction ID (will delete) */
   typedef i32                TxnID;                  /** @brief Transaction ID (New)*/
   typedef i32                HWM;                    /** @brief high water mark */


   /************************************************************************
    * Type enumerations
    ************************************************************************/
   enum DebugLevel {                                  /** @brief Debug level type enumeration */
      DEBUG_0                 = 0,                    /** @brief No debug info print out */
      DEBUG_1                 = 1,                    /** @brief Summary level debug info print out */
      DEBUG_2                 = 2,                    /** @brief A little detail debug info print out */
      DEBUG_3                 = 3,                    /** @brief Detail debug info print out */
   };

   // INFO2 only goes to log file unless '-i' cmd line arg is specified,
   // in which case the msg will also get logged to the console.
   // All other messages always get logged to the log file and the console.
   enum MsgLevel {                                    /** @brief Message level */
      MSGLVL_INFO1            = 0,                    /** @brief Basic Information level */
      MSGLVL_INFO2            = 1,                    /** @brief More Information level */
      MSGLVL_WARNING          = 2,                    /** @brief Warning level */
      MSGLVL_ERROR            = 3,                    /** @brief Error level */
      MSGLVL_CRITICAL         = 4,                    /** @brief Critical level */
   };

   enum OpType {                                      /** @brief Operation type */
      NOOP                    = 0,                    /** @brief No oper */
      INSERT                  = 1,                    /** @brief Insert */
      UPDATE                  = 2,                    /** @brief Update */
      DELETE                  = 4,                    /** @brief Delete */
      QUERY                   = 8,                    /** @brief Query */
   };

   enum ColType {                                     /** @brief Column type enumeration*/
//      WR_BIT                  = 1,                  /** @brief Bit */
      WR_BYTE                 = 2,                    /** @brief Byte */
      WR_SHORT                = 3,                    /** @brief Short */
      WR_INT                  = 4,                    /** @brief Int */
//      WR_LONG                 = 5,                  /** @brief Long */
      WR_LONGLONG             = 6,                    /** @brief Long long*/
      WR_FLOAT                = 7,                    /** @brief Float */
      WR_DOUBLE               = 8,                    /** @brief Double */
      WR_CHAR                 = 9,                    /** @brief Char */
      WR_TOKEN                = 10,                   /** @brief Token */
      WR_BLOB                 = 11,                   /** @brief BLOB */
      WR_VARBINARY            = 12,                   /** @brief VARBINARY */
   };

   // Describes relation of field to column for a bulk load
   enum BulkFldColRel { BULK_FLDCOL_COLUMN_FIELD,  // map input field to db col
                        BULK_FLDCOL_COLUMN_DEFAULT,// import def val to db col
                        BULK_FLDCOL_IGNORE_FIELD };// ignore fld in import file

   /**
   * the set of Calpont column data type names; MUST match ColDataType in
   * we_typeext.h.
   */
   const char  ColDataTypeStr[NUM_OF_COL_DATA_TYPE][20] = {
               "bit",
               "tinyint",
               "char",
               "smallint",
               "decimal",
               "medint",
               "integer",
               "float",
               "date",
               "bigint",
               "double",
               "datetime",
               "varchar",
               "varbinary",
               "clob",
               "blob" };

   enum FuncType { FUNC_WRITE_ENGINE, FUNC_INDEX, FUNC_DICTIONARY };

   enum CacheListType { FREE_LIST, LRU_LIST, WRITE_LIST }; /** @brief List type */

   /************************************************************************
    * struct data block structure
    ************************************************************************/
   struct DataBlock                                   /** @brief Data block structure definition */
   {
      long           no;                              /** @brief block number */
      i64            lbid;                            /** @brief lbid */
      bool           dirty;                           /** @brief block dirty flag */
      int            state;                           /** @brief initialized 0, read 1 , modified 2 */
      unsigned char  data[BYTE_PER_BLOCK];            /** @brief data buffer */
      DataBlock()    { dirty = false; memset( data, 0, BYTE_PER_BLOCK ); } /** @brief constructor */
   };

   struct DataSubBlock                                /** @brief Data subblock structure definition */
   {
      long           no;                              /** @brief sub block number */
      bool           dirty;                           /** @brief block dirty flag */
      unsigned char  data[BYTE_PER_SUBBLOCK];         /** @brief data buffer */
      DataSubBlock() { dirty = false; memset( data, 0, BYTE_PER_SUBBLOCK ); } /** @brief constructor */
   };


   /************************************************************************
    * @brief file structure. Default copy constructor, assignment oper, etc
    * are in play here, as they are not overridden.  Beware that if copies
    * of a File object are made, only one user should be closing the pFile.
    * oid and fid replicate one another.  oid mostly used by index, cache,
    * and dictionary.  fid mostly used by colop and bulk.
    ************************************************************************/
   struct File                                        /** @brief File structure definition */
   {
      OID            oid;                             /** @brief Oid */
      FID            fid;                             /** @brief File id */
      HWM            hwm;                             /** @brief High water mark */
      FILE*          pFile;                           /** @brief File handle */
      u_int32_t      fPartition;                      /** @brief Partition for pFile */
      u_int16_t      fSegment;                        /** @brief Segment for pFile */
      u_int16_t      fDbRoot;                         /** @brief DbRoot for pFile */
      std::string    fSegFileName;                    /** @brief Current seg file path */
      File()         { clear(); } /** @brief constructor */
      void clear()   { pFile = NULL; oid = fid = hwm = 0;
                       fPartition = fSegment = fDbRoot = 0;
                       fSegFileName.clear(); }
   };

   /************************************************************************
    * @brief session structure
    ************************************************************************/
   struct Session                                     /** @brief Session structure definition */
   {
      SID            sid;                             /** @brief Sid */
      TxnID          txnid;                           /** @brief Transaction id */
      Session()      { clear(); }                     /** @brief constructor */
      void clear()   { sid = 0; txnid = 0; }
   };

   /************************************************************************
    * @brief Internal communication block structure
    ************************************************************************/
   struct CommBlock                                   /** @brief Communication Block structure definition */
   {
      Session        session;                         /** @brief Session structure */
      File           file;                            /** @brief File structure */
      void clear()   { file.clear(); session.clear(); }
   };

   /************************************************************************
    * @brief column structure used to pass data in/out of we_colop functions
    ************************************************************************/
   typedef struct column_struct                       /** @brief Column structure definition */
   {
      int            colNo;                           /** @brief column number */
      int            colWidth;                        /** @brief column width */
      ColType        colType;                         /** @brief column type (internal use)*/
      ColDataType    colDataType;                     /** @brief column data type (from interface)*/
      File           dataFile;                        /** @brief column data file */
      int            compressionType;                 /** @brief column compression type */
   } Column;

   /************************************************************************
    * @brief dictionary related structures (Token struct is defined in
    * we_typeext.h to facilitate its use in dbcon and utils/dataconvert).
    ************************************************************************/
   typedef struct offset_                              /** @brief Offset structure definition */
   {
     int hdrLoc;                                      /** @brief offset postion in header*/
     i16 offset;                                      /** @brief offset in block */
   } Offset;    

   /************************************************************************
    * @brief interfaces with DDL/DML
    ************************************************************************/
   typedef struct colTuple_struct                     /** @brief Column Tuple definition */
   {
      boost::any     data;                            /** @brief column value */
   } ColTuple;

   typedef std::vector<ColTuple>  ColTupleList;       /** @brief column value list */

   struct ColStruct                                   /** @brief Column Interface Structure definition */
   {
      OID            dataOid;                         /** @brief column data file object id */
      int            colWidth;                        /** @brief column width */
      bool           tokenFlag;                       /** @brief column token flag, must be set to true if it is a token column */
      ColDataType    colDataType;                     /** @brief column data type (for interface)*/
      ColType        colType;                         /** @brief column type (internal use for write engine)*/
      u_int32_t      fColPartition;                   /** @brief Partition for column file */
      u_int16_t      fColSegment;                     /** @brief Segment for column file */
      u_int16_t      fColDbRoot;                      /** @brief DBRoot for column file */
      int            fCompressionType;                /** @brief Compression tpye for column file */
      ColStruct() : dataOid(0), colWidth(0),          /** @brief constructor */
                    tokenFlag(false), colDataType(INT), colType(WR_INT),
                    fColPartition(0), fColSegment(0), fColDbRoot(0), fCompressionType(0) { }
   };

   typedef std::vector<ColStruct>      ColStructList; /** @brief column struct list */
   typedef std::vector<ColTupleList>   ColValueList;  /** @brief column value list */
   typedef std::vector<RID>            RIDList;       /** @brief RID list */
   
   typedef std::vector<std::string> dictStr;
   typedef std::vector<dictStr> DictStrList;


   // index
   typedef struct idxTuple_struct                     /** @brief Index Tuple definition */
   {
      boost::any     data;                            /** @brief index key or value */
   } IdxTuple;

   typedef std::vector<IdxTuple>  IdxTupleList;       /** @brief index value list */

   struct  IdxStruct                                 /** @brief Index Interface Structure definition */
   {
      OID            treeOid;                         /** @brief index tree file object id */
      OID            listOid;                         /** @brief index list file object id */
      int            idxWidth;                        /** @brief index width */
      bool           tokenFlag;                       /** @brief index token flag */
      ColDataType    idxDataType;                     /** @brief index data type (for interface)*/
      ColType        idxType;                         /** @brief index type (internal use for write engine)*/
      bool           multiColFlag;                    /** @brief multiple column index indicator */
      IdxStruct()    { clear(); }         /** @brief constructor */
      void clear()   { treeOid = listOid = idxWidth = 0; tokenFlag = multiColFlag = false; idxDataType = INT; idxType = WR_INT;}
   };

   typedef std::vector<IdxStruct>      IdxStructList; /** @brief index struct list */
   typedef std::vector<IdxTupleList>   IdxValueList;  /** @brief index value key list */

   // dictionary
   struct DctnryStruct                                /** @brief Dictionary Interface Structure definition */
   {
      OID            dctnryOid;                       /** @brief dictionary signature file */
      OID            columnOid;                       /** @brief corresponding column file */
      int            colWidth;                        /** @brief string width for the dictionary column*/
      u_int32_t      fColPartition;                   /** @brief Partition for column file */
      u_int16_t      fColSegment;                     /** @brief Segment for column file */
      u_int16_t      fColDbRoot;                      /** @brief DBRoot for column file */
      int            fCompressionType;                /** @brief Compression tpye for column file */
      DctnryStruct() : dctnryOid(0), columnOid(0),    /** @brief constructor */
                       colWidth(0),
                       fColPartition(0), fColSegment(0),
                       fColDbRoot(0), fCompressionType(0) { }
   };

   struct DctnryTuple                                 /** @brief Dictionary Tuple definition */
   {
      unsigned char   sigValue[MAX_SIGNATURE_SIZE];   /** @brief dictionary signature value*/
      int             sigSize;                        /** @brief dictionary signature size */
      Token           token;                          /** @brief dictionary token */
      bool            isNull;
      DctnryTuple()   { }
      ~DctnryTuple()  { }
   };

   typedef std::vector<DctnryTuple> DctColTupleList;
   typedef std::vector<DctnryStruct>  DctnryStructList; /** @brief column struct list */
   typedef std::vector<DctColTupleList>   DctnryValueList;  /** @brief column value list */
   /************************************************************************
    * @brief bulk job
    ************************************************************************/
   struct JobColumn                                   /** @brief Job Column Structure definition */
   {
      std::string    colName;                         /** @brief column name */
      OID            mapOid;                          /** @brief column OID */
      ColDataType    dataType;                        /** @brief column data type */
      ColType        weType;                          /** @brief column write engine data type */
      std::string    typeName;                        /** @brief column data type name */
      i64            emptyVal;                        /** @brief column default empty value */
      int            width;                           /** @brief column width; for a dictionary column, this is "eventually" the token width */
      int            definedWidth;                    /** @brief column width as defined in the table, used for non-dictionary strings */
      int            dctnryWidth;                     /** @brief dictionary width */
      int            precision;                       /** @brief column precision of decimal */
      int            scale;                           /** @brief column scale of decimal */
      bool           notNull;                         /** @brief column not null */
      BulkFldColRel  fFldColRelation;                 /** @brief type of field/col relation*/
      char           colType;                         /** @brief column type, blank is regular, D is dictionary */
      int            compressionType;                 /** @brief column compression type */
      bool           autoIncFlag;                     /** @brief column auto increment flag */
      DctnryStruct   dctnry;                          /** @brief column dictionary structure */
      long long      fMinIntSat;                      /** @brief For integer type, the min saturation value */
      long long      fMaxIntSat;                      /** @brief For integer type, the max saturation value */
      JobColumn() : mapOid(0), dataType(INT), weType(WR_INT),
                    typeName("integer"), emptyVal(0),
                    width(0), definedWidth(0), dctnryWidth(0),
                    precision(0), scale(0), notNull(false),
                    fFldColRelation(BULK_FLDCOL_COLUMN_FIELD), colType(' '),
                    compressionType(0),autoIncFlag(false),
                    fMinIntSat(0), fMaxIntSat(0) { }
   };

   typedef std::vector<JobColumn>  JobColList;        /** @brief column value list */

   struct JobFieldRef                   // references field/column in JobTable
   {
      BulkFldColRel  fFldColType;       // type of field or column
      unsigned       fArrayIndex;       // index into colList or fIgnoredFields
                                        //   in JobTable based on fFldColType.
      JobFieldRef( ) : fFldColType(BULK_FLDCOL_COLUMN_FIELD), fArrayIndex(0) { }
      JobFieldRef( BulkFldColRel fldColType, unsigned idx ) :
                     fFldColType( fldColType ), fArrayIndex( idx ) { }
   };
   typedef std::vector<JobFieldRef>  JobFieldRefList;

   struct JobTable                                    /** @brief Job Table Structure definition */
   {
      std::string    tblName;                         /** @brief table name */
      OID            mapOid;                          /** @brief table OID */
      std::string    loadFileName;                    /** @brief table load file name */
      i64            maxErrNum;                       /** @brief max number of error rows before abort */
      JobColList     colList;                         /** @brief list of columns to be loaded; followed by default columns to be loaded */
      JobColList     fIgnoredFields;                  /** @brief list of fields in input file to be ignored */
      JobFieldRefList fFldRefs;                       /** @brief Combined list of refs to entries in colList and fIgnoredFields */
      JobTable() : mapOid(0), maxErrNum(0) { }
   } ;

   typedef std::vector<JobTable>  JobTableList;       /** @brief table list */

   struct Job                                         /** @brief Job Structure definition */
   {
      int            id;                              /** @brief job id */
      std::string    schema;                          /** @brief database name */
      std::string    name;                            /** @brief job name */
      std::string    desc;                            /** @brief job description */
      std::string    userName;                        /** @brief user name */
      JobTableList   jobTableList;                    /** @brief job table list */
      
      std::string    createDate;                      /** @brief job create date */
      std::string    createTime;                      /** @brief job create time */

      char           fDelimiter;
      char           fEnclosedByChar;
      char           fEscapeChar;
      int            numberOfReadBuffers;
      unsigned       readBufferSize;
      unsigned       writeBufferSize;
      Job() : id(0), fDelimiter('|'), fEnclosedByChar('\0'), fEscapeChar('\0'),
          numberOfReadBuffers(0), readBufferSize(0), writeBufferSize(0) { }
   };

   /************************************************************************
    * @brief Cache memory
    ************************************************************************/
   struct CacheBlock                                  /** @brief Cache block strucutre */
   {
      i64            fbo;                             /** @brief file fbo */
      i64            lbid;                            /** @brief lbid */
      bool           dirty;                           /** @brief dirty flag */
      int            hitCount;                        /** @brief hit count */
      unsigned char* data;                            /** @brief block buffer */
      CacheBlock()   { data = NULL; clear(); }        /** @brief constructor */
      void clear()   { fbo = lbid = hitCount = 0; dirty = false; if( data ) memset( data, 0, BYTE_PER_BLOCK); }   /** @brief clear, NOTE: buf must be free by caller first */
      void init()    { data = (unsigned char*)malloc(BYTE_PER_BLOCK); }
      void freeMem() { if( data ) free( data ); }
   };

   struct BlockBuffer                                 /** @brief Block buffer */
   {
      CommBlock      cb;                              /** @brief Communication block structure */
      CacheBlock     block;                           /** @brief Cache block strucutre */
      CacheListType  listType;                        /** @brief List number, 0 - free, 1 - LRU, 2 - write */
      BlockBuffer()  { clear(); }                     /** @brief constructor */
      void init()    { block.init(); }
      void freeMem() { block.freeMem(); }
      void clear()   { cb.clear(); block.clear(); listType = FREE_LIST; }
   };

   struct CacheControl                                /** @brief Cache control structure */
   {
      int            totalBlock;                      /** @brief The toal number of allocated blocks */
      int            pctFree;                         /** @brief The percentage of free blocks when some blocks must be aged out */
      int            checkInterval;                   /** @brief A check point interval in seconds */
      CacheControl() { totalBlock = pctFree = checkInterval; } /** @brief constructor */
   };

   struct SortTuple                                   /** @brief Sort tuple structure */
   {
      i64            key;                             /** @brief The key value, todo: change to a more generic type later */
      // @bug 458 : fix for bug 458 - Change rid in SortTuple struct from i32 to i64
      i64            rid;                             /** @brief RID */
      SortTuple()    { key = rid = 0; }
      SortTuple& operator= ( const SortTuple& val ) { 
         if( this == &val ) return *this;             // handle self assignment
         key = val.key; rid = val.rid;
         return *this;
      }
   };

   /************************************************************************
    * @brief Bulk parse meta data describing data in a read buffer.
    * An offset of COLPOSPAIR_NULL_TOKEN_OFFSET represents a null token.
    ************************************************************************/
   struct ColPosPair            /** @brief Column position pair structure */
   {
      int               start;  /** @brief start position */
      int               offset; /** @brief length of token*/
   };

   /************************************************************************
    * @brief Exceptions
    * SecondaryShutdown used to terminate a thread when it sees that the
    * JobStatus flag has been set to EXIT_FAILURE (by another thread).
    ************************************************************************/
   class SecondaryShutdownExcept : public std::runtime_error
   {
      public:
         SecondaryShutdownExcept(const std::string& msg) :
            std::runtime_error(msg) { }
   };

} //end of namespace

#endif // _WE_TYPE_H_
