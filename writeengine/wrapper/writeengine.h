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

//  $Id: writeengine.h 4376 2012-12-03 23:14:04Z xlou $


/** @file */

#ifndef _WRITE_ENGINE_H_
#define _WRITE_ENGINE_H_
#include <stdio.h>
#include <string>

// the header file for fd
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// end

#include <boost/lexical_cast.hpp>

#include "we_brm.h"
#include "we_colop.h"
#include "we_dctnry.h"
#include "we_index.h"
#define IO_BUFF_SIZE 81920

#if defined(_MSC_VER) && defined(WRITEENGINEWRAPPER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

//... Total compression operation: un_compresssed, compressed
const int UN_COMPRESSED_OP  = 0;
const int COMPRESSED_OP     = 1;
const int TOTAL_COMPRESS_OP = 2;

//...Forward class declarations
class Log;

/** Class WriteEngineWrapper */
class WriteEngineWrapper : public WEObj
{
public:
   /**
    * @brief Constructor
    */
   EXPORT WriteEngineWrapper();

   /**
    * @brief Default Destructor
    */
   EXPORT ~WriteEngineWrapper();

   /************************************************************************
    * Interface definitions
    ************************************************************************/
   /**
    * @brief Performs static/global initialization for BRMWrapper.
    * Should be called once from the main thread.
    */
   EXPORT static void init(unsigned subSystemID);

   /**
    * @brief Build a index from an oid file (NOTE: this is write engine internal used function, just for test purpose and not for generic use
    */
   int buildIndex(const OID& colOid, const OID& treeOid, const OID& listOid,
                         ColDataType colDataType, int width, int hwm,
                         bool resetFile, i64& totalRows, int maxRow = IDX_DEFAULT_READ_ROW)
   { return -1; }

   /**
    * @brief Build a index from a file
    */
   int buildIndex(const std::string& sourceFileName, const OID& treeOid, const OID& listOid,
                         ColDataType colDataType, int width, int hwm, bool resetFile,
                         i64& totalRows, const std::string& indexName, Log* pLogger,
                         int maxRow = IDX_DEFAULT_READ_ROW)
   { return -1; }

   int buildIndexRec(const TxnID& txnid, const IdxStructList& idxStructList,
                            const IdxValueList& idxValueList, const RIDList& ridList) { return -1; }

   /**
    * @brief Close a index file
    */
   void closeIndex() { }

   /**
    * @brief Close a dictionary
    */
   int closeDctnry(const TxnID& txnid, int i) { return m_dctnry[op(i)]->closeDctnry(); }

   /**
    * @brief Commit transaction
    */
   int commit(const TxnID& txnid) { return BRMWrapper::getInstance()->commit(txnid); }

   /**
    * @brief Convert interface value list to internal value array
    */
   EXPORT void convertValArray(size_t totalRow, const ColType colType,
                               ColTupleList& curTupleList, void* valArray,
                               bool bFromList = true) ;

   /**
    * @brief Create a column, include object ids for column data and bitmap files
    * @param dataOid column datafile object id
    * @param dataType column data type
    * @param dataWidth column width
    * @param dbRoot DBRoot under which file is to be located (1-based)
    * @param partition Starting partition number for segment file path (0-based).
    * @param compressionType compression type
    */
   EXPORT int createColumn(const TxnID& txnid, const OID& dataOid,
                           ColDataType dataType, int dataWidth,
                           uint16_t dbRoot, uint32_t partition=0, int compressionType = 0);

   //BUG931
   /**
    * @brief Fill a new column with default value using row-ids from a reference column
    *
    * @param txnid Transaction id
    * @param dataOid OID of the new column
    * @param dataType Data-type of the new column
    * @param dataWidth Width of the new column
    * @param defaultVal Default value to be filled in the new column
    * @param refColOID OID of the reference column
    * @param refColDataType Data-type of the referecne column
    * @param refColWidth Width of the reference column
    */
   EXPORT int fillColumn(const TxnID& txnid, const OID& dataOid, ColDataType dataType,
                         int dataWidth, ColTuple defaultVal,
                         const OID& refColOID, ColDataType refColDataType,
                         int refColWidth, int refCompressionType, uint16_t dbRoot, bool isNULL, int compressionType,
                         const std::string& defaultValStr, long long & nextVal, const OID& dictOid = 0, bool autoincrement = false);

   /**
    * @brief Create a index related files, include object ids for index tree and list files

    * @param treeOid index tree file object id
    * @param listOid index list file object id
    */
   int createIndex(const TxnID& txnid, const OID& treeOid, const OID& listOid)
   { int rc = -1; flushVMCache(); return rc;}

   /**
    * @brief Create dictionary
    * @param dctnryOid dictionary signature file object id
    * @param partition Starting partition number for segment file path (0-based).
    * @param segment segment number
    * @param compressionType compression type
    */
   EXPORT int createDctnry(const TxnID& txnid, const OID& dctnryOid,
                          int colWidth, uint16_t dbRoot,
                          uint32_t partiotion=0, uint16_t segment=0, int compressionType = 0);

   /**
    * @brief Delete a list of rows from a table
    * @param colStructList column struct list
    * @param colOldValueList column old values list (return value)
    * @param rowIdList row id list
    */
    EXPORT int deleteRow(const TxnID& txnid, std::vector<ColStructList>& colExtentsStruct,
                         std::vector<void *>& colOldValueList, std::vector<RIDList>& ridLists);

   /**
    * @brief Delete rid/values from an index
    * @param idxStructList index structure list
    * @param idxValueList index value list
    * @param ridList row id list
    */
   int deleteIndexRec(const TxnID& txnid, IdxStructList& idxStructList, IdxValueList& idxValueList,
                      RIDList& ridList)
   { return -1; }

   /**
    * @brief delete a dictionary signature and its token
    * @param dctnryStruct dictionary structure
    * @param dctnryTuple dictionary tuple
    */
   //ITER17_Obsolete
   // int deleteToken(const TxnID& txnid, Token& token); // Files need already open
   // int deleteToken(const TxnID& txnid, DctnryStruct& dctnryStruct, Token& token);

   /**
    * @brief Drop a column, include object ids for column data file
    * @param dataOid column datafile object id
    */
   int dropColumn(const TxnID& txnid, const OID dataOid)
   { return m_colOp[0]->dropColumn((FID) dataOid); }

   /**
    * @brief Drop files
    * @param dataOids column and dictionary datafile object id
    */
   int dropFiles(const TxnID& txnid, const std::vector<int32_t> & dataOids)
   { return m_colOp[0]->dropFiles(dataOids); }

    /**
    * @brief Delete files for one partition
    * @param dataOids column and dictionary datafile object id
    */
   int deletePartition(const std::vector<int32_t> & dataOids, uint32_t partition)
   { return m_colOp[0]->dropPartition(dataOids, partition); }

   int deleteOIDsFromExtentMap (const TxnID& txnid,  const std::vector<int32_t>& dataOids)
   { return m_colOp[0]->deleteOIDsFromExtentMap(dataOids); }

   /**
    * @brief Create a index related files, include object ids for index tree and list files
    * @param treeOid index tree file object id
    * @param listOid index list file object id
    */
   int dropIndex(const TxnID& txnid, const OID& treeOid, const OID& listOid)
   { return -1; }

   /**
    * @brief Drop a dictionary
    * @param dctnryOid dictionary signature file object id
    * @param treeOid dictionary tree file object id
    * @param listOid index list file object id
    */
   int dropDctnry(const TxnID& txnid, const OID& dctnryOid, const OID& treeOid, const OID& listOid)
   { return m_dctnry[0]->dropDctnry(dctnryOid); }

   /**
    * @brief Flush VM write cache
    * @param None
    */
   EXPORT void flushVMCache() const;

   /**
    * @brief Insert values into a column
    * @param colStructList column structure list
    * @param colValueList column value list
    * @param dicStringListt dictionary values list
    */
    EXPORT int insertColumnRec(const TxnID& txnid,
                               ColStructList& colStructList,
                               ColValueList& colValueList,
                               DctnryStructList& dctnryStructList,
                               DictStrList& dictStrList,
                               bool insertSelect = false);

   /**
    * @brief Open dictionary
    * @param dctnryOid dictionary signature file object id
    * @param treeOid dictionary tree file object id
    * @param listOid index list file object id
    */
   int openDctnry(const TxnID& txnid, DctnryStruct dctnryStruct)
   {
      int compress_op = op(dctnryStruct.fCompressionType);
      m_dctnry[compress_op]->setTransId(txnid);
      return m_dctnry[compress_op]->openDctnry(
                                dctnryStruct.dctnryOid,
//                                dctnryStruct.treeOid,
//                                dctnryStruct.listOid,
                                dctnryStruct.fColDbRoot,
                                dctnryStruct.fColPartition,
                                dctnryStruct.fColSegment);
   }

   /**
    * @brief Rollback transaction
    */
   EXPORT int rollbackTran(const TxnID& txnid, int sessionId);  

   /**
    * @brief Set the root directory for column data files
    * @param path data directory
    */
   void setChunkManager(ChunkManager * cm)
   { 
         m_colOp[COMPRESSED_OP]->chunkManager(cm);
         m_dctnry[COMPRESSED_OP]->chunkManager(cm);
   }
   
   /**
    * @brief Tokenize a dictionary signature into a token
    * @param dctnryStruct dictionary structure
    * @param dctnryTuple dictionary tuple
    */
   EXPORT int tokenize(const TxnID& txnid, DctnryTuple&, int compType ); // Files need open first
   EXPORT int tokenize(const TxnID& txnid, DctnryStruct& dctnryStruct, DctnryTuple& dctnryTuple);

   /**
    * @brief Update values into a column (New one)
    * @param colStructList column structure list
    * @param colValueList column value list
    * @param colOldValueList column old values list (return value)
    * @param ridList row id list
    */
    EXPORT int updateColumnRec(const TxnID& txnid,
                               std::vector<ColStructList>& colExtentsStruct,
                               ColValueList& colValueList,
                               std::vector<void *>& colOldValueList,
                               std::vector<RIDList>& ridLists,
                               std::vector<DctnryStructList>& dctnryExtentsStruct,
                               DctnryValueList& dctnryValueList);

  /**
    * @brief Update values into columns
    * @param colStructList column structure list
    * @param colValueList column value list
    * @param ridList row id list
    */

    EXPORT int updateColumnRecs(const TxnID& txnid,
                                std::vector<ColStruct>& colStructList,
                                ColValueList& colValueList,
                                const RIDList & ridLists);

   /**
    * @brief Update values into an index
    * @param idxStructList index structure list
    * @param idxValueList index value list
    * @param ridList row id list
    */
   int updateIndexRec(const TxnID& txnid, IdxStructList& idxStructList,
                      IdxValueList& idxValueList, RIDList& ridList)
   { return -1; }


   /**
    * @brief Update values into an multicolumn index
    * @param idxStructList index structure list
    * @param idxValueList index value list
    * @param ridList row id list
    */
   int updateMultiColIndexRec(const TxnID& txnid, IdxStructList& idxStructList,
                              IdxValueList& idxValueList, RIDList& ridList)
   { return -1; }

   int deleteMultiColIndexRec(const TxnID& txnid, IdxStructList& idxStructList,
                              IdxValueList& idxValueList, RIDList& ridList)
   { return -1; }


   /**
    * @brief rollback the specified table and clear the table lock
    * @param tableOid
    * @param tableName name of table associated with tableOid
    * @param applName application that is driving this bulk rollback
    */
   EXPORT int clearLockOnly(const OID& tableOid);
   EXPORT int bulkRollback(const OID& tableOid,
                           const std::string& tableName,
                           const std::string& applName, bool rollbackOnly,
                           bool debugConsole, std::string& errorMsg);
						   
						   
	/**
    * @brief update SYSCOLUMN next value
    * @param oidValueMap
    */
	
   typedef std::map<OID, long long> AutoIncrementValue_t;
   EXPORT int updateNextValue(const AutoIncrementValue_t & oidValueMap, const uint32_t sessionID = 0);					   
	/**
    * @brief write active datafiles to disk
    *
    */
    EXPORT int flushDataFiles(int rc, std::map<FID,FID> & columnOids);

   /************************************************************************
    * Future implementations
    ************************************************************************/
   /**
    * @brief Begin transaction
    */
   // todo: add implementation when we work on version control
   // int beginTran(const TransID transOid) { return NO_ERROR; }

   /**
    * @brief End transaction
    */
   // todo: add implementation when we work on version control
   // int endTran(const TransID transOid) { return NO_ERROR; }


   /************************************************************************
    * Internal use definitions
    ************************************************************************/
private:
   /**
    * @brief Check whether the passing parameters are valid
    */
   int checkValid(const TxnID& txnid, const ColStructList& colStructList, const ColValueList& colValueList, const RIDList& ridList) const;

   int checkIndexValid(const TxnID& txnid, const IdxStructList& idxStructList, const IdxValueList& idxValueList, const RIDList& ridList) const { return -1; }

   /**
    * @brief Convert interface column type to a internal column type
    */
   // void convertColType(void* curStruct, const FuncType curType = FUNC_WRITE_ENGINE) const;

   /**
    * @brief Convert interface value list to internal value array
    */
   void convertIndexValArray(ColType colType, IdxTupleList& curTupleList, void* valArray)
   { for (IdxTupleList::size_type i = 0; i < curTupleList.size(); i++)
         (void)0;
   }

   void convertValue(const ColType colType, void* valArray, size_t pos, boost::any& data, bool fromList = true);

   /**
    * @brief Convert column value to its internal representation
    *
    * @param colType Column data-type
    * @param value Memory pointer for storing output value. Should be pre-allocated
    * @param data Column data
    */
   void convertValue(const ColType colType, void* value, boost::any& data);

   /**
    * @brief Print input value from DDL/DML processors
    */
   void printInputValue(const ColStructList& colStructList, const ColValueList& colValueList, const RIDList& ridList) const;

   /**
    * @brief Internal process index operations
    */
   int processIndexRec(const TxnID& txnid, const IdxStructList& idxStructList,
                       const IdxValueList& idxValueList, const RIDList& ridList,
                       bool updateFlag = true) { return -1; }

   /**
    * @brief Internal process multicolumn index operations
    */
   int processMultiColIndexRec(const TxnID& txnid, const IdxStructList& idxStructList,
                               const IdxValueList& idxValueList, const RIDList& ridList,
                               bool updateFlag = true) { return -1; }

   /**
    * @brief Process version buffer
    */
   int processVersionBuffer(FILE* pFile, const TxnID& txnid, const ColStruct& colStruct,
                            int width, int totalRow, const RID* rowIdArray,
                            std::vector<BRM::LBIDRange> &  rangeList);

   /**
    * @brief Process version buffers for update and delete @Bug 1886,2870
    */
   int processVersionBuffers(FILE* pFile, const TxnID& txnid, const ColStruct& colStruct,
                             int width, int totalRow, const RIDList& ridList,
                             std::vector<BRM::LBIDRange> &  rangeList);


   void setTransId(const TxnID& txnid)
   {
      for (int i = 0; i < TOTAL_COMPRESS_OP; i++)
      {
         m_colOp[i]->setTransId(txnid);
         m_dctnry[i]->setTransId(txnid);
      }
   }

   void setDebugLevel(const DebugLevel level)
   {
      WEObj::setDebugLevel(level);
      for (int i = 0; i < TOTAL_COMPRESS_OP; i++)
      {
         m_colOp[i]->setDebugLevel(level);
         m_dctnry[i]->setDebugLevel(level);
      }
   }  // todo: cleanup

   int buildIndex(IdxLoadTuple*  colBuf, int& colBufSize, const OID& treeOid, const OID& listOid,
                  const ColDataType colDataType, int width, int maxRow) { return -1; }

   /**
    * @brief Common methods to write values to a column
    */
    int writeColumnRec(const TxnID& txnid, const ColStructList& colStructList,
                       const ColValueList& colValueList, ColValueList& colOldValueList,
                       RID* rowIdArray, const ColStructList& newColStructList,
                       const ColValueList& newColValueList);

    //@Bug 1886,2870 pass the address of ridList vector
    int writeColumnRec(const TxnID& txnid, const ColStructList& colStructList,
                       const ColValueList& colValueList, std::vector<void *>& colOldValueList,
                       const RIDList& ridList, bool convertStructFlag = true,
                       ColTupleList::size_type nRows = 0);

    //For update column from column to use
    int writeColumnRecords(const TxnID& txnid, std::vector<ColStruct>& colStructList,
                           ColValueList& colValueList, const RIDList & ridLists, bool versioning = true);

    /**
    * @brief util method to convert rowid to a column file
    *
    */
    int convertRidToColumn(RID& rid, uint16_t& dbRoot, uint32_t& partition, uint16_t& segment,
                           const RID filesPerColumnPartition, const RID  extentsPerSegmentFile,
                           const RID extentRows, uint16_t startDBRoot, unsigned dbrootCnt);

    int op(int compressionType) { return (compressionType > 0 ? COMPRESSED_OP : UN_COMPRESSED_OP); }


    ColumnOp*      m_colOp[TOTAL_COMPRESS_OP];          // column operations
    Dctnry*        m_dctnry[TOTAL_COMPRESS_OP];         // dictionary operations
    OpType         m_opType;                            // operation type
    DebugLevel     m_debugLevel;                        // debug level
};

} //end of namespace

#undef EXPORT

#endif // _WRITE_ENGINE_H_
