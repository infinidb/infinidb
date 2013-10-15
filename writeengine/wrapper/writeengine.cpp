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

// $Id: writeengine.cpp 4737 2013-08-14 20:45:46Z bwilkinson $

/** @writeengine.cpp
 *   A wrapper class for the write engine to write information to files
 */
#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
using namespace std;

#include "joblisttypes.h"

#define WRITEENGINEWRAPPER_DLLEXPORT
#include "writeengine.h"
#undef WRITEENGINEWRAPPER_DLLEXPORT

#include "we_convertor.h"
#include "we_log.h"
#include "we_simplesyslog.h"
#include "we_config.h"
#include "we_bulkrollbackmgr.h"
#include "brm.h"
#include "stopwatch.h"
#include "we_colop.h"
#include "we_dctnry.h"
#include "we_type.h"

#include "we_colopcompress.h"
#include "we_dctnrycompress.h"
#include "cacheutils.h"
#include "calpontsystemcatalog.h"
#include "we_simplesyslog.h"
using namespace cacheutils;
using namespace logging;
using namespace BRM;
using namespace execplan;
#include "IDBDataFile.h"
#include "IDBPolicy.h"
#include "MonitorProcMem.h"
using namespace idbdatafile;

#ifdef _MSC_VER
#define isnan _isnan
#endif

namespace WriteEngine
//#define PROFILE 1

{
StopWatch timer;

/**@brief WriteEngineWrapper Constructor
*/
WriteEngineWrapper::WriteEngineWrapper() :  m_opType(NOOP)
{
   m_colOp[UN_COMPRESSED_OP] = new ColumnOpCompress0;
   m_colOp[COMPRESSED_OP]    = new ColumnOpCompress1;

   m_dctnry[UN_COMPRESSED_OP] = new DctnryCompress0;
   m_dctnry[COMPRESSED_OP]    = new DctnryCompress1;
}

WriteEngineWrapper::WriteEngineWrapper(const WriteEngineWrapper& rhs) :  m_opType(rhs.m_opType)
{
   m_colOp[UN_COMPRESSED_OP] = new ColumnOpCompress0;
   m_colOp[COMPRESSED_OP]    = new ColumnOpCompress1;

   m_dctnry[UN_COMPRESSED_OP] = new DctnryCompress0;
   m_dctnry[COMPRESSED_OP]    = new DctnryCompress1;
}

/**@brief WriteEngineWrapper Constructor
*/
WriteEngineWrapper::~WriteEngineWrapper()
{
	delete m_colOp[UN_COMPRESSED_OP];
	delete m_colOp[COMPRESSED_OP];
	delete m_dctnry[UN_COMPRESSED_OP];
	delete m_dctnry[COMPRESSED_OP];
}

/**@brief Perform upfront initialization
*/
/* static */ void WriteEngineWrapper::init(unsigned subSystemID)
{
    SimpleSysLog::instance()->setLoggingID(logging::LoggingID(subSystemID));
    Config::initConfigCache(); 
    BRMWrapper::getInstance();

    // Bug 5415 Add HDFS MemBuffer vs. FileBuffer decision logic.
    config::Config* cf = config::Config::makeConfig();
    //--------------------------------------------------------------------------
    // Memory overload protection. This setting will cause the process to die should 
    // it, by itself, consume maxPct of total memory. Monitored in MonitorProcMem.
    // Only used at the express direction of Field Support.
    //--------------------------------------------------------------------------
    int maxPct = 0; //disable by default
    string strMaxPct = cf->getConfig("WriteEngine", "MaxPct");
    if ( strMaxPct.length() != 0 )
        maxPct = cf->uFromText(strMaxPct);

    //--------------------------------------------------------------------------
    // MemoryCheckPercent. This controls at what percent of total memory be consumed
    // by all processes before we switch from HdfsRdwrMemBuffer to HdfsRdwrFileBuffer.
    // This is only used in Hdfs installations.
    //--------------------------------------------------------------------------
    int checkPct = 95;
    string strCheckPct = cf->getConfig("SystemConfig", "MemoryCheckPercent");
    if ( strCheckPct.length() != 0 )
        checkPct = cf->uFromText(strCheckPct);

    //--------------------------------------------------------------------------
    // If we're either HDFS, or maxPct is turned on, start the monitor thread.
    // Otherwise, we don't need it, so don't waste the resources.
    //--------------------------------------------------------------------------
    if (maxPct > 0 || IDBPolicy::useHdfs())
    {
        new boost::thread(utils::MonitorProcMem(maxPct, checkPct, subSystemID));
    }
}

/*@brief checkValid --Check input parameters are valid
 */
/***********************************************************
 * DESCRIPTION:
 *    Check input parameters are valid
 * PARAMETERS:
 *    colStructList - column struct list
 *    colValueList - column value list
 *    ridList - rowid list
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in the checking process
 ***********************************************************/
int WriteEngineWrapper::checkValid(const TxnID& txnid, const ColStructList& colStructList, const ColValueList& colValueList, const RIDList& ridList) const
{
   ColTupleList   curTupleList;
   ColStructList::size_type structListSize;
   ColValueList::size_type  valListSize;
   ColTupleList::size_type  totalRow;

   if (colStructList.size() == 0)
      return ERR_STRUCT_EMPTY;

   structListSize = colStructList.size() ;
   valListSize = colValueList.size();
//      if (colStructList.size() !=  colValueList.size())
   if (structListSize != valListSize)
      return ERR_STRUCT_VALUE_NOT_MATCH;

   for (ColValueList::size_type i = 0; i < valListSize; i++) {

      curTupleList = static_cast<ColTupleList>(colValueList[i]);
      totalRow = curTupleList.size();

      if (ridList.size() > 0) {
         if (totalRow != ridList.size())
            return ERR_ROWID_VALUE_NOT_MATCH;
      }

   } // end of for (int i = 0;

   return NO_ERROR;
}

/*@convertValArray -  Convert interface values to internal values
 */
/***********************************************************
 * DESCRIPTION:
 *    Convert interface values to internal values
 * PARAMETERS:
 *    colStructList - column struct list
 *    colValueList - column value list
 * RETURN:
 *    none
 *    valArray - output value array
 *    nullArray - output null flag array
 ***********************************************************/
void WriteEngineWrapper::convertValArray(const size_t totalRow, const ColType colType, ColTupleList& curTupleList, void* valArray, bool bFromList)
{
   ColTuple    curTuple;
   ColTupleList::size_type i;

   if (bFromList)
      for (i = 0; i < curTupleList.size(); i++) {
         curTuple = curTupleList[i];
         convertValue(colType, valArray, i, curTuple.data);
      } // end of for (int i = 0
   else
      for (i = 0; i < totalRow; i++) {
         convertValue(colType, valArray, i, curTuple.data, false);
         curTupleList.push_back(curTuple);
      }
}

/*
 * @brief Convert column value to its internal representation
 */
void WriteEngineWrapper::convertValue(const ColType colType, void* value, boost::any& data)
{
   string curStr;
   int size;
   switch (colType)
   {
      case WriteEngine::WR_INT :    if (data.type() == typeid(int))
                                    {
                                       int val = boost::any_cast<int>(data); 
                                       size = sizeof(int);
                                       memcpy(value, &val, size);
                                    }
                                    else
                                    {
                                       uint32_t val = boost::any_cast<uint32_t>(data); 
                                       size = sizeof(uint32_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;
      case WriteEngine::WR_UINT :   {
                                       uint32_t val = boost::any_cast<uint32_t>(data); 
                                       size = sizeof(uint32_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_VARBINARY : // treat same as char for now
      case WriteEngine::WR_CHAR :
      case WriteEngine::WR_BLOB :
                                    curStr = boost::any_cast<string>(data);
                                    if ((int) curStr.length() > MAX_COLUMN_BOUNDARY)
                                       curStr = curStr.substr(0, MAX_COLUMN_BOUNDARY);
                                       memcpy(value, curStr.c_str(), curStr.length());
                                    break;

      case WriteEngine::WR_FLOAT:  {
                                       float val = boost::any_cast<float>(data);
//N.B.There is a bug in boost::any or in gcc where, if you store a nan, you will get back a nan,
// but not necessarily the same bits that you put in. This only seems to be for float (double seems
// to work).
                                       if (isnan(val))
                                       {
                                          uint32_t ti = joblist::FLOATNULL;
                                          float* tfp = (float*)&ti;
                                          val = *tfp;
                                       }
                                       size = sizeof(float);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_DOUBLE:  {
                                       double val = boost::any_cast<double>(data);
                                       size = sizeof(double);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_SHORT:   {
                                       short val = boost::any_cast<short>(data);
                                       size = sizeof(short);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_USHORT:  {
                                       uint16_t val = boost::any_cast<uint16_t>(data);
                                       size = sizeof(uint16_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_BYTE:    {
                                       char val = boost::any_cast<char>(data);
                                       size = sizeof(char);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_UBYTE:   {
                                       uint8_t val = boost::any_cast<uint8_t>(data);
                                       size = sizeof(uint8_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_LONGLONG:
                                    if (data.type() == typeid(long long))
                                    {
                                       long long val = boost::any_cast<long long>(data);
                                       size = sizeof(long long);
                                       memcpy(value, &val, size);
                                    }
                                    else
                                    {
                                       uint64_t val = boost::any_cast<uint64_t>(data);
                                       size = sizeof(uint64_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_ULONGLONG:
                                    {
                                       uint64_t val = boost::any_cast<uint64_t>(data);
                                       size = sizeof(uint64_t);
                                       memcpy(value, &val, size);
                                    }
                                    break;

      case WriteEngine::WR_TOKEN:   {
                                       Token val = boost::any_cast<Token>(data);
                                       size = sizeof(Token);
                                       memcpy(value, &val, size);
                                    }
                                    break;

   } // end of switch (colType)
}  /*@convertValue -  The base for converting values */

/***********************************************************
 * DESCRIPTION:
 *    The base for converting values
 * PARAMETERS:
 *    colType - data type
 *    pos - array position
 *    data - value
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::convertValue(const ColType colType, void* valArray, const size_t pos, boost::any& data, bool fromList)
{
   string curStr;
//      ColTuple    curTuple;

   if (fromList) {
      switch (colType)
      {
         case WriteEngine::WR_INT :    if (data.type() == typeid(long))
                                          ((int*)valArray)[pos] = static_cast<int>(boost::any_cast<long>(data));
                                       else if (data.type() == typeid(int))
                                          ((int*)valArray)[pos] = boost::any_cast<int>(data);
                                       else
                                          ((int*)valArray)[pos] = boost::any_cast<uint32_t>(data);
                                       break;
         case WriteEngine::WR_UINT :   ((uint32_t*)valArray)[pos] = boost::any_cast<uint32_t>(data);
                                       break;
         case WriteEngine::WR_VARBINARY : // treat same as char for now
         case WriteEngine::WR_CHAR :
         case WriteEngine::WR_BLOB :   curStr = boost::any_cast<string>(data);
                                       if ((int) curStr.length() > MAX_COLUMN_BOUNDARY)
                                          curStr = curStr.substr(0, MAX_COLUMN_BOUNDARY);
                                       memcpy((char*)valArray + pos * MAX_COLUMN_BOUNDARY, curStr.c_str(), curStr.length());
                                       break;

//            case WriteEngine::WR_LONG :   ((long*)valArray)[pos] = boost::any_cast<long>(curTuple.data);
//                                          break;
         case WriteEngine::WR_FLOAT:    ((float*)valArray)[pos] = boost::any_cast<float>(data);
                                           if (isnan(((float*)valArray)[pos]))
                                           {
                                              uint32_t ti = joblist::FLOATNULL;
                                              float* tfp = (float*)&ti;
                                              ((float*)valArray)[pos] = *tfp;
                                           }
                                       break;
         case WriteEngine::WR_DOUBLE:  ((double*)valArray)[pos] = boost::any_cast<double>(data);
                                       break;
         case WriteEngine::WR_SHORT:   ((short*)valArray)[pos] = boost::any_cast<short>(data);
                                       break;
         case WriteEngine::WR_USHORT:  ((uint16_t*)valArray)[pos] = boost::any_cast<uint16_t>(data);
                                       break;
//            case WriteEngine::WR_BIT:     ((bool*)valArray)[pos] = boost::any_cast<bool>(data);
//                                          break;
         case WriteEngine::WR_BYTE:    ((char*)valArray)[pos] = boost::any_cast<char>(data);
                                       break;
         case WriteEngine::WR_UBYTE:   ((uint8_t*)valArray)[pos] = boost::any_cast<uint8_t>(data);
                                       break;
         case WriteEngine::WR_LONGLONG:
                                       if (data.type() == typeid(long long))
                                          ((long long*)valArray)[pos] = boost::any_cast<long long>(data);
                                       else
                                          ((long long*)valArray)[pos] = boost::any_cast<uint64_t>(data);
                                       break;
         case WriteEngine::WR_ULONGLONG:
                                       ((uint64_t*)valArray)[pos] = boost::any_cast<uint64_t>(data);
                                       break;
         case WriteEngine::WR_TOKEN:   ((Token*)valArray)[pos] = boost::any_cast<Token>(data);
                                       break;
      } // end of switch (colType)
   }
   else {
      switch (colType)
      {
         case WriteEngine::WR_INT :    data = ((int*)valArray)[pos];
                                       break;
         case WriteEngine::WR_UINT :   data = ((uint64_t*)valArray)[pos];
                                       break;
         case WriteEngine::WR_VARBINARY : // treat same as char for now
         case WriteEngine::WR_CHAR :
         case WriteEngine::WR_BLOB :   char tmp[10];
                                       memcpy(tmp, (char*)valArray + pos*8, 8);
                                       curStr = tmp;
                                       data = curStr;
                                       break;

//            case WriteEngine::WR_LONG :   ((long*)valArray)[pos] = boost::any_cast<long>(curTuple.data);
//                                          break;
         case WriteEngine::WR_FLOAT:   data = ((float*)valArray)[pos];
                                       break;
         case WriteEngine::WR_DOUBLE:  data = ((double*)valArray)[pos];
                                       break;
         case WriteEngine::WR_SHORT:   data = ((short*)valArray)[pos];
                                       break;
         case WriteEngine::WR_USHORT:  data = ((uint16_t*)valArray)[pos];
                                       break;
//            case WriteEngine::WR_BIT:     data = ((bool*)valArray)[pos];
//                                          break;
         case WriteEngine::WR_BYTE:    data = ((char*)valArray)[pos];
                                       break;
         case WriteEngine::WR_UBYTE:   data = ((uint8_t*)valArray)[pos];
                                       break;
         case WriteEngine::WR_LONGLONG:data = ((long long*)valArray)[pos];
                                       break;
         case WriteEngine::WR_ULONGLONG:
                                       data = ((uint64_t*)valArray)[pos];
                                       break;
         case WriteEngine::WR_TOKEN:   data = ((Token*)valArray)[pos];
                                       break;
      } // end of switch (colType)
   } // end of if
}

/*@createColumn -  Create column files, including data and bitmap files
 */
/***********************************************************
 * DESCRIPTION:
 *    Create column files, including data and bitmap files
 * PARAMETERS:
 *    dataOid - column data file id
 *    bitmapOid - column bitmap file id
 *    colWidth - column width
 *    dbRoot   - DBRoot where file is to be located
 *    partition - Starting partition number for segment file path
 *     compressionType - compression type
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if something wrong in creating the file
 ***********************************************************/
int WriteEngineWrapper::createColumn(
   const TxnID& txnid,
   const OID& dataOid,
   const CalpontSystemCatalog::ColDataType dataType,
   int dataWidth,
   uint16_t dbRoot,
   uint32_t partition,
   int compressionType)
{
   int      rc;
   Column   curCol;

   int compress_op = op(compressionType);
   m_colOp[compress_op]->initColumn(curCol);
   rc = m_colOp[compress_op]->createColumn(curCol, 0, dataWidth, dataType,
      WriteEngine::WR_CHAR, (FID)dataOid, dbRoot, partition);

   // This is optional, however, it's recommended to do so to free heap
   // memory if assigned in the future
   m_colOp[compress_op]->clearColumn(curCol);
   std::map<FID,FID> oids;

   if (rc == NO_ERROR)
      rc = flushDataFiles(NO_ERROR, txnid, oids);

   if (rc != NO_ERROR)
   {
      return rc;
   }

   RETURN_ON_ERROR(BRMWrapper::getInstance()->setLocalHWM(dataOid,partition , 0,0));
   // @bug 281 : fix for bug 281 - Add flush VM cache to clear all write buffer
   //flushVMCache();
   return rc;
}

//BUG931
/**
 * @brief Fill column with default values
 */
int WriteEngineWrapper::fillColumn(const TxnID& txnid, const OID& dataOid,
                                   const CalpontSystemCatalog::ColDataType dataType, int dataWidth,
                                   ColTuple defaultVal, const OID& refColOID,
                                   const CalpontSystemCatalog::ColDataType refColDataType,
                                   int refColWidth, int refCompressionType,
                                   bool isNULL, int compressionType,
                                   const string& defaultValStr, 
								   const OID& dictOid, bool autoincrement)
{
   int      rc = NO_ERROR;
   Column   newCol;
   Column   refCol;
   ColType  newColType;
   ColType  refColType;
   boost::scoped_array<char> defVal(new char[MAX_COLUMN_BOUNDARY]);
   ColumnOp* colOpNewCol = m_colOp[op(compressionType)];
   ColumnOp* refColOp = m_colOp[op(refCompressionType)];
   Dctnry*   dctnry  = m_dctnry[op(compressionType)]; 
   colOpNewCol->initColumn(newCol);
   refColOp->initColumn(refCol);
   //boost::shared_ptr<Dctnry> dctnry;
  // boost::shared_ptr<ColumnOp> refColOp;
  // refColOp.reset(colOpRefCol);
  // dctnry.reset(dctOp);
   uint16_t dbRoot = 1;	//not to be used
   //Convert HWM of the reference column for the new column
   //Bug 1703,1705
   bool isToken = false;
   if (((dataType == CalpontSystemCatalog::VARCHAR) && (dataWidth > 7)) ||
      ((dataType == CalpontSystemCatalog::CHAR) && (dataWidth > 8)) || (dataType == CalpontSystemCatalog::VARBINARY) )
   {
      isToken = true;
   }
   Convertor::convertColType(dataType, newColType, isToken);

   if (((refColDataType == CalpontSystemCatalog::VARCHAR) && (refColWidth > 7)) ||
      ((refColDataType == CalpontSystemCatalog::CHAR) && (refColWidth > 8)) || (refColDataType == CalpontSystemCatalog::VARBINARY))
   {
      isToken = true;
   }

   Convertor::convertColType(refColDataType, refColType, isToken);
   refColOp->setColParam(refCol, 0, refColOp->getCorrectRowWidth(refColDataType, refColWidth),
                      refColDataType, refColType, (FID)refColOID, refCompressionType, dbRoot);
   colOpNewCol->setColParam(newCol, 0, colOpNewCol->getCorrectRowWidth(dataType, dataWidth),
                      dataType, newColType, (FID)dataOid, compressionType, dbRoot);

   int size = sizeof(Token);
   if (newColType == WriteEngine::WR_TOKEN) 
   {
      if (isNULL)
      {
         Token nullToken;
         memcpy(defVal.get(), &nullToken, size);
      }
	  //Tokenization is done when we create dictionary file
   }
   else
      convertValue(newColType, defVal.get(), defaultVal.data);

   if (rc == NO_ERROR)
      rc = colOpNewCol->fillColumn(txnid, newCol, refCol, defVal.get(), dctnry, refColOp, dictOid, dataWidth, defaultValStr, autoincrement);

//   colOpNewCol->clearColumn(newCol);
//   colOpRefCol->clearColumn(refCol);
  
//   free(defVal);

// flushing files is in colOp->fillColumn()
// if (rc == NO_ERROR)
// rc = flushDataFiles();

   return rc;
}

 int WriteEngineWrapper::deleteRow(const TxnID& txnid, vector<ColStructList>& colExtentsStruct, vector<void *>& colOldValueList, 
									vector<RIDList>& ridLists, const int32_t tableOid)
{
   ColTuple         curTuple;
   ColStruct        curColStruct;
   DctnryStruct     dctnryStruct;
   ColValueList     colValueList;
   ColTupleList     curTupleList;
   DctnryStructList dctnryStructList;
   DctnryValueList  dctnryValueList;
   ColStructList    colStructList;
   uint64_t         emptyVal;
   int              rc;
   string           tmpStr("");
   vector<DctnryStructList> dctnryExtentsStruct;
   if (colExtentsStruct.size() == 0 || ridLists.size() == 0)
      return ERR_STRUCT_EMPTY;

   // set transaction id
   setTransId(txnid);
   unsigned numExtents = colExtentsStruct.size();
   for (unsigned extent = 0; extent < numExtents; extent++)
   {
     colStructList = colExtentsStruct[extent];
     for (ColStructList::size_type i = 0; i < colStructList.size(); i++)
     {
      curTupleList.clear();
      curColStruct = colStructList[i];
      emptyVal = m_colOp[op(curColStruct.fCompressionType)]->
                     getEmptyRowValue(curColStruct.colDataType, curColStruct.colWidth);

      curTuple.data = emptyVal;
      //for (RIDList::size_type j = 0; j < ridLists[extent].size(); j++)
     //    curTupleList.push_back(curTuple);
      curTupleList.push_back(curTuple);
      colValueList.push_back(curTupleList);

      dctnryStruct.dctnryOid = 0;
      dctnryStruct.fColPartition = curColStruct.fColPartition;
      dctnryStruct.fColSegment = curColStruct.fColSegment;
      dctnryStruct.fColDbRoot = curColStruct.fColDbRoot;
      dctnryStruct.columnOid = colStructList[i].dataOid;
      dctnryStructList.push_back(dctnryStruct);

      DctnryTuple dctnryTuple;
      DctColTupleList dctColTuples;
      memcpy(dctnryTuple.sigValue, tmpStr.c_str(), tmpStr.length());
      dctnryTuple.sigSize = tmpStr.length();
      dctnryTuple.isNull = true;
      dctColTuples.push_back (dctnryTuple);
      dctnryValueList.push_back (dctColTuples);
     }
     dctnryExtentsStruct.push_back(dctnryStructList);
   }
   // unfortunately I don't have a better way to instruct without passing too many parameters
   m_opType = DELETE;
   rc = updateColumnRec(txnid, colExtentsStruct, colValueList, colOldValueList, ridLists, dctnryExtentsStruct, dctnryValueList, tableOid);
   m_opType = NOOP;

   return rc;
}


 /*@flushVMCache - Flush VM cache
 */
/***********************************************************
 * DESCRIPTION:
 *    Flush sytem VM cache
 * PARAMETERS:
 *    none
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::flushVMCache() const
{
//      int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
//      write(fd, "3", 1);
//      close(fd);

}

 /*@insertColumnRecs -  Insert value(s) into a column
 */
/***********************************************************
 * DESCRIPTION:
 *    Insert values into  columns (batchinsert)
 * PARAMETERS:
 *    colStructList - column struct list
 *    colValueList - column value list
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/

int WriteEngineWrapper::insertColumnRecs(const TxnID& txnid,
                                        ColStructList& colStructList,
                                        ColValueList& colValueList,
                                        DctnryStructList& dctnryStructList,
                                        DictStrList& dictStrList,
                                        std::vector<boost::shared_ptr<DBRootExtentTracker> > & dbRootExtentTrackers,
										RBMetaWriter* fRBMetaWriter,
										bool bFirstExtentOnThisPM,
										bool insertSelect, 
										bool isAutoCommitOn,
										OID tableOid,
										bool isFirstBatchPm)
{
   int            rc;
   RID*           rowIdArray = NULL;
   ColTupleList   curTupleList;
   Column         curCol;
   ColStruct      curColStruct;
   ColValueList   colOldValueList;
   ColValueList   colNewValueList;
   ColStructList  newColStructList;
   DctnryStructList newDctnryStructList;
   HWM            hwm = 0;
   HWM            oldHwm = 0;
   HWM    		  newHwm = 0;
   ColTupleList::size_type totalRow;
   ColStructList::size_type totalColumns;
   uint64_t rowsLeft = 0;
   bool newExtent = false;
   RIDList ridList;
   ColumnOp* colOp = NULL;

   unsigned i=0;
#ifdef PROFILE
 StopWatch timer;
#endif
   // debug information for testing
   if (isDebug(DEBUG_2)) {
      printf("\nIn wrapper insert\n");
      printInputValue(colStructList, colValueList, ridList);
   }
   // end

   //Convert data type and column width to write engine specific
   for (i = 0; i < colStructList.size(); i++)
      Convertor::convertColType(&colStructList[i]);

  // rc = checkValid(txnid, colStructList, colValueList, ridList);
  // if (rc != NO_ERROR)
   //   return rc;

	setTransId(txnid);
	uint16_t  dbRoot, segmentNum;
	uint32_t partitionNum;	
	string    segFile;
    bool newFile;
	TableMetaData* tableMetaData= TableMetaData::makeTableMetaData(tableOid);
	//populate colStructList with file information
	IDBDataFile* pFile = NULL;
	std::vector<DBRootExtentInfo> extentInfo;
	int currentDBrootIdx = 0;
	std::vector<BRM::CreateStripeColumnExtentsArgOut> extents;

	//--------------------------------------------------------------------------
	// For first batch on this PM:
	//   o get starting extent from ExtentTracker, and allocate extent if needed
	//   o construct colStructList and dctnryStructList accordingly
	//   o save extent information in tableMetaData for future use
	// If not first batch on this PM:
	//   o construct colStructList and dctnryStructList from tableMetaData
	//--------------------------------------------------------------------------
	if (isFirstBatchPm)
	{
		currentDBrootIdx = dbRootExtentTrackers[0]->getCurrentDBRootIdx();
		extentInfo = dbRootExtentTrackers[0]->getDBRootExtentList();
		dbRoot = extentInfo[currentDBrootIdx].fDbRoot;
		partitionNum = extentInfo[currentDBrootIdx].fPartition;

		//----------------------------------------------------------------------
		// check whether this extent is the first on this PM
		//----------------------------------------------------------------------
		if (bFirstExtentOnThisPM)
		{
			//cout << "bFirstExtentOnThisPM is " << bFirstExtentOnThisPM << endl;
			std::vector<BRM::CreateStripeColumnExtentsArgIn> cols;
			BRM::CreateStripeColumnExtentsArgIn createStripeColumnExtentsArgIn;
			for (i=0; i < colStructList.size(); i++)
			{
				createStripeColumnExtentsArgIn.oid = colStructList[i].dataOid;
				createStripeColumnExtentsArgIn.width = colStructList[i].colWidth;
                createStripeColumnExtentsArgIn.colDataType = colStructList[i].colDataType;
				cols.push_back(createStripeColumnExtentsArgIn);
			}
			rc = BRMWrapper::getInstance()->allocateStripeColExtents(cols, dbRoot, partitionNum, segmentNum, extents);
			if (rc != NO_ERROR)
				return rc;
			//Create column files
			BRM::CPInfoList_t cpinfoList;
			BRM::CPInfo cpInfo;
            if (isUnsigned(colStructList[i].colDataType))
            {
                cpInfo.max = 0;
                cpInfo.min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
            }
            else
            {
                cpInfo.max = numeric_limits<int64_t>::min();
                cpInfo.min = numeric_limits<int64_t>::max();
            }
			cpInfo.seqNum = -1;	
			for ( i=0; i < extents.size(); i++)
			{
				colOp = m_colOp[op(colStructList[i].fCompressionType)];
				colOp->initColumn(curCol);
				colOp->setColParam(curCol, 0, colStructList[i].colWidth, colStructList[i].colDataType,
				colStructList[i].colType, colStructList[i].dataOid, colStructList[i].fCompressionType,
					dbRoot, partitionNum, segmentNum);
				rc = colOp->extendColumn(curCol, false, extents[i].startBlkOffset, extents[i].startLbid, extents[i].allocSize, dbRoot,  
					partitionNum, segmentNum, segFile, pFile, newFile);
				if (rc != NO_ERROR)
					return rc;
					
				//mark the extents to invalid
				cpInfo.firstLbid = extents[i].startLbid;
				cpinfoList.push_back(cpInfo);
				colStructList[i].fColPartition = partitionNum;
				colStructList[i].fColSegment = segmentNum;
				colStructList[i].fColDbRoot = dbRoot;
				dctnryStructList[i].fColPartition = partitionNum;
				dctnryStructList[i].fColSegment = segmentNum;
				dctnryStructList[i].fColDbRoot = dbRoot;
			}

			//mark the extents to invalid
			rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);
			if (rc != NO_ERROR)
				return rc;
			//create corresponding dictionary files
			for (i=0; i < dctnryStructList.size(); i++)
			{
				if (dctnryStructList[i].dctnryOid > 0)
				{
					rc = createDctnry(txnid, dctnryStructList[i].dctnryOid, dctnryStructList[i].colWidth, dbRoot, partitionNum,
                                 segmentNum, dctnryStructList[i].fCompressionType);
					if ( rc != NO_ERROR)
						return rc;
				}	
			}	
		}    // if ( bFirstExtentOnThisPM)
		else // if (!bFirstExtentOnThisPM)
		{
			std::vector<DBRootExtentInfo> tmpExtentInfo;
			for (i=0; i < dbRootExtentTrackers.size(); i++)
			{
				tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
				colStructList[i].fColPartition =  tmpExtentInfo[currentDBrootIdx].fPartition;
				colStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
				colStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
				//cout << "Load from dbrootExtenttracker oid:dbroot:part:seg = " <<colStructList[i].dataOid<<":"
				//<<colStructList[i].fColDbRoot<<":"<<colStructList[i].fColPartition<<":"<<colStructList[i].fColSegment<<endl;
				dctnryStructList[i].fColPartition = tmpExtentInfo[currentDBrootIdx].fPartition;
				dctnryStructList[i].fColSegment = tmpExtentInfo[currentDBrootIdx].fSegment;
				dctnryStructList[i].fColDbRoot = tmpExtentInfo[currentDBrootIdx].fDbRoot;
			}
		}	
		
		//----------------------------------------------------------------------
		// Save the extents info in tableMetaData 
		//----------------------------------------------------------------------
		for (i=0; i < colStructList.size(); i++)
		{		
			ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
			ColExtsInfo::iterator it = aColExtsInfo.begin();
			while (it != aColExtsInfo.end())
			{
				if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) && (it->segNum == colStructList[i].fColSegment))
					break;
				it++;
			}

			if (it == aColExtsInfo.end()) //add this one to the list
			{
				ColExtInfo aExt;
				aExt.dbRoot = colStructList[i].fColDbRoot;
				aExt.partNum = colStructList[i].fColPartition;
				aExt.segNum = colStructList[i].fColSegment;
				aExt.compType = colStructList[i].fCompressionType;
				aExt.isDict = false;
				if (bFirstExtentOnThisPM)
				{
					aExt.hwm = extents[i].startBlkOffset;
					aExt.isNewExt = true;
				//cout << "adding a ext to metadata" << endl;
				}
				else
				{
					std::vector<DBRootExtentInfo> tmpExtentInfo;
					tmpExtentInfo = dbRootExtentTrackers[i]->getDBRootExtentList();
					aExt.isNewExt = false;
					aExt.hwm = tmpExtentInfo[currentDBrootIdx].fLocalHwm;
					//cout << "oid " << colStructList[i].dataOid << " gets hwm " << aExt.hwm << endl;
				}
				aExt.current = true;
				aColExtsInfo.push_back(aExt);
				//cout << "get from extentinfo oid:hwm = " << colStructList[i].dataOid << ":" << aExt.hwm << endl;
			}

			tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
		}
		
		for (i=0; i < dctnryStructList.size(); i++)
		{	
			if (dctnryStructList[i].dctnryOid > 0)
			{
				ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
				ColExtsInfo::iterator it = aColExtsInfo.begin();
				while (it != aColExtsInfo.end())
				{
					if ((it->dbRoot == dctnryStructList[i].fColDbRoot) && (it->partNum == dctnryStructList[i].fColPartition) && (it->segNum == dctnryStructList[i].fColSegment))
						break;
					it++;
				}

				if (it == aColExtsInfo.end()) //add this one to the list
				{
					ColExtInfo aExt;
					aExt.dbRoot = dctnryStructList[i].fColDbRoot;
					aExt.partNum = dctnryStructList[i].fColPartition;
					aExt.segNum = dctnryStructList[i].fColSegment;
					aExt.compType = dctnryStructList[i].fCompressionType;
					aExt.isDict = true;	
					aColExtsInfo.push_back(aExt);
				}
				tableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
			}
		}
		
	} // if (isFirstBatchPm)
	else //get the extent info from tableMetaData
	{
		ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[0].dataOid);
		ColExtsInfo::iterator it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if (it->current) 
				break;
			it++;
		}
		if (it == aColExtsInfo.end())
			return 1;
			
		for (i=0; i < colStructList.size(); i++)
		{
			colStructList[i].fColPartition = it->partNum;
			colStructList[i].fColSegment = it->segNum;
			colStructList[i].fColDbRoot = it->dbRoot;
			dctnryStructList[i].fColPartition = it->partNum;
			dctnryStructList[i].fColSegment = it->segNum;
			dctnryStructList[i].fColDbRoot = it->dbRoot;
		}
	}

   curTupleList = static_cast<ColTupleList>(colValueList[0]);
   totalRow = curTupleList.size();
   totalColumns = colStructList.size();
   rowIdArray = new RID[totalRow];
   // use scoped_array to ensure ptr deletion regardless of where we return
   boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
   memset(rowIdArray, 0, (sizeof(RID)*totalRow));

	//--------------------------------------------------------------------------
	// allocate row id(s)
	//--------------------------------------------------------------------------
   curColStruct = colStructList[0];
   colOp = m_colOp[op(curColStruct.fCompressionType)];

   colOp->initColumn(curCol);

   //Get the correct segment, partition, column file
   vector<ExtentInfo> colExtentInfo; //Save those empty extents in case of failure to rollback
   vector<ExtentInfo> dictExtentInfo; //Save those empty extents in case of failure to rollback
   vector<ExtentInfo> fileInfo;
   dbRoot = curColStruct.fColDbRoot;
   //use the first column to calculate row id 
   ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[0].dataOid);
   ColExtsInfo::iterator it = aColExtsInfo.begin();
	while (it != aColExtsInfo.end())
	{
		if ((it->dbRoot == colStructList[0].fColDbRoot) && (it->partNum == colStructList[0].fColPartition) && (it->segNum == colStructList[0].fColSegment) && it->current )
			break;
		it++;
	}
	if (it != aColExtsInfo.end()) 
	{
		hwm = it->hwm;
		//cout << "Got from colextinfo hwm for oid " << colStructList[0].dataOid << " is " << hwm << " and seg is " << colStructList[0].fColSegment << endl;
	}
   
   oldHwm = hwm; //Save this info for rollback
   //need to pass real dbRoot, partition, and segment to setColParam
   colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType,
       curColStruct.colType, curColStruct.dataOid, curColStruct.fCompressionType,
       curColStruct.fColDbRoot, curColStruct.fColPartition, curColStruct.fColSegment);

   rc = colOp->openColumnFile(curCol, segFile);
   if (rc != NO_ERROR) {
      return rc;
   }

   //get hwm first
   // @bug 286 : fix for bug 286 - correct the typo in getHWM
   //RETURN_ON_ERROR(BRMWrapper::getInstance()->getHWM(curColStruct.dataOid, hwm));

   Column newCol;

#ifdef PROFILE
timer.start("allocRowId");
#endif
	newColStructList = colStructList;
	newDctnryStructList = dctnryStructList;
    bool bUseStartExtent = true;
	if (idbdatafile::IDBPolicy::useHdfs())
		insertSelect = true;
		
   rc = colOp->allocRowId(txnid, bUseStartExtent,
	   curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent, rowsLeft, newHwm, newFile, 
	   newColStructList, newDctnryStructList, dbRootExtentTrackers, insertSelect, true, tableOid, isFirstBatchPm);
   
   //cout << "after allocrowid, total row = " <<totalRow << " newExtent is " << newExtent << endl; 
  // cout << "column oid " << curColStruct.dataOid << " has hwm:newHwm = " << hwm <<":" << newHwm<< endl;
   if (rc != NO_ERROR) //Clean up is already done
	  return rc;
	
#ifdef PROFILE
timer.stop("allocRowId");
#endif
	//--------------------------------------------------------------------------
	// Expand initial abbreviated extent if any RID in 1st extent is > 256K.
	// if totalRow == rowsLeft, then not adding rows to 1st extent, so skip it.
	//--------------------------------------------------------------------------
    if ((curCol.dataFile.fPartition == 0) &&
       (curCol.dataFile.fSegment   == 0) &&
       ((totalRow-rowsLeft) > 0) &&
       (rowIdArray[totalRow-rowsLeft-1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
    {
       for (unsigned k=1; k<colStructList.size(); k++)
       {
           Column expandCol;
           colOp = m_colOp[op(colStructList[k].fCompressionType)];
           colOp->setColParam(expandCol, 0,
               colStructList[k].colWidth,
               colStructList[k].colDataType,
               colStructList[k].colType,
               colStructList[k].dataOid,
               colStructList[k].fCompressionType,
               colStructList[k].fColDbRoot,
               colStructList[k].fColPartition,
               colStructList[k].fColSegment);
           rc = colOp->openColumnFile(expandCol, segFile);
           if (rc == NO_ERROR)
           {
               if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
               {
                   rc = colOp->expandAbbrevExtent(expandCol);
               }
           }
           if (rc != NO_ERROR)
           {
				return rc;
           }
           colOp->clearColumn(expandCol); // closes the file
       }
    }
	
	//--------------------------------------------------------------------------
	// Tokenize data if needed
	//--------------------------------------------------------------------------
   BRMWrapper::setUseVb( true );
   dictStr::iterator dctStr_iter;
   ColTupleList::iterator col_iter;
   for (i = 0; i < colStructList.size(); i++)
   {
      if (colStructList[i].tokenFlag)
      {
         dctStr_iter = dictStrList[i].begin();
         col_iter = colValueList[i].begin();
         Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];
         rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid,
                     dctnryStructList[i].fColDbRoot, dctnryStructList[i].fColPartition,
                     dctnryStructList[i].fColSegment);
         if (rc !=NO_ERROR)
		 {
			cout << "Error opening dctnry file " << dctnryStructList[i].dctnryOid<< endl;
             return rc;
		}

         for (uint32_t     rows = 0; rows < (totalRow - rowsLeft); rows++)
         {
             if (dctStr_iter->length() == 0)
               {
                   Token nullToken;
                 col_iter->data = nullToken;
               }
             else
               {
#ifdef PROFILE
timer.start("tokenize");
#endif
                 DctnryTuple dctTuple;
                 memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
                 dctTuple.sigSize = dctStr_iter->length();
                 dctTuple.isNull = false;
                   rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);
                 if (rc != NO_ERROR)
                 {
                     dctnry->closeDctnry();
                     return rc;
                 }
#ifdef PROFILE
timer.stop("tokenize");
#endif
                   col_iter->data = dctTuple.token;
               }
               dctStr_iter++;
               col_iter++;

         }
         //close dictionary files
         rc = dctnry->closeDctnry();
         if (rc != NO_ERROR)
             return rc;

         if (newExtent)
         {
			//@Bug 4854 back up hwm chunk for the file to be modified
			if (fRBMetaWriter)
				fRBMetaWriter->backupDctnryHWMChunk(newDctnryStructList[i].dctnryOid, newDctnryStructList[i].fColDbRoot, newDctnryStructList[i].fColPartition, newDctnryStructList[i].fColSegment);
             rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid,
             //            dctnryStructList[i].treeOid, dctnryStructList[i].listOid,
                           newDctnryStructList[i].fColDbRoot, newDctnryStructList[i].fColPartition,
                           newDctnryStructList[i].fColSegment);
             if (rc !=NO_ERROR)
                 return rc;

             for (uint32_t     rows = 0; rows < rowsLeft; rows++)
             {
             if (dctStr_iter->length() == 0)
               {
                   Token nullToken;
                 col_iter->data = nullToken;
               }
             else
               {
#ifdef PROFILE
timer.start("tokenize");
#endif
                 DctnryTuple dctTuple;
                 memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
                 dctTuple.sigSize = dctStr_iter->length();
                 dctTuple.isNull = false;
                 rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);
                 if (rc != NO_ERROR)
                 {
                     dctnry->closeDctnry();
                     return rc;
                 }
#ifdef PROFILE
timer.stop("tokenize");
#endif
                     col_iter->data = dctTuple.token;
                 }
                 dctStr_iter++;
                 col_iter++;
             }
             //close dictionary files
             rc = dctnry->closeDctnry();
             if (rc != NO_ERROR)
                 return rc;
         }
      }
   }
	BRMWrapper::setUseVb( true );

	//--------------------------------------------------------------------------
	// Update column info structure @Bug 1862 set hwm, and
	// Prepare ValueList for new extent (if applicable)
	//--------------------------------------------------------------------------
   //@Bug 2205 Check whether all rows go to the new extent
   RID lastRid = 0;
   RID lastRidNew = 0;
   if (totalRow-rowsLeft > 0)
   {
     lastRid = rowIdArray[totalRow-rowsLeft-1];
     lastRidNew = rowIdArray[totalRow-1];
   }
   else
   {
     lastRid = 0;
     lastRidNew = rowIdArray[totalRow-1];
   }
   //cout << "rowid allocated is "  << lastRid << endl;
   //if a new extent is created, all the columns in this table should have their own new extent
   //First column already processed

   //@Bug 1701. Close the file
   m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
   //cout << "Saving hwm info for new ext batch" << endl;
   //Update hwm to set them in the end
    bool succFlag = false;
    unsigned colWidth = 0;
    int      curFbo = 0, curBio;
	for (i=0; i < totalColumns; i++)
    {
		//shoud be obtained from saved hwm
		aColExtsInfo = tableMetaData->getColExtsInfo(colStructList[i].dataOid);
		it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if ((it->dbRoot == colStructList[i].fColDbRoot) && (it->partNum == colStructList[i].fColPartition) 
				&& (it->segNum == colStructList[i].fColSegment) && it->current)
				break;
			it++;
		}
		if (it != aColExtsInfo.end()) //update hwm info
		{
			oldHwm = it->hwm;
		}
		 
         // save hwm for the old extent
         colWidth = colStructList[i].colWidth;
         succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
		//cout << "insertcolumnrec   oid:rid:fbo:oldhwm = " << colStructList[i].dataOid << ":" << lastRid << ":" << curFbo << ":" << oldHwm << endl;
         if (succFlag)
         {
            if ((HWM)curFbo >= oldHwm)
			{
				it->hwm = (HWM)curFbo;
			}
			//@Bug 4947. set current to false for old extent.
			if (newExtent)
			{
					it->current = false;
			}		
			
			//cout << "updated old ext info for oid " << colStructList[i].dataOid << " dbroot:part:seg:hwm:current = " 
	  //<< it->dbRoot<<":"<<it->partNum<<":"<<it->segNum<<":"<<it->hwm<<":"<< it->current<< " and newExtent is " << newExtent << endl;
         }
         else
            return ERR_INVALID_PARAM;
			
		//update hwm for the new extent
	  if (newExtent)
	  {
		it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if ((it->dbRoot == newColStructList[i].fColDbRoot) && (it->partNum == newColStructList[i].fColPartition) 
				&& (it->segNum == newColStructList[i].fColSegment) && it->current)
				break;
			it++;
		}
		succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
         if (succFlag)
         {
			if (it != aColExtsInfo.end())
			{
				it->hwm = (HWM)curFbo;
				//cout << "setting hwm to " << (int)curFbo <<" for seg " <<it->segNum << endl;
				it->current = true;
			}
         }
		 else
            return ERR_INVALID_PARAM;
	  }	
	  tableMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
    }
		
	//--------------------------------------------------------------------------
	//Prepare the valuelist for the new extent
	//--------------------------------------------------------------------------
      ColTupleList colTupleList;
      ColTupleList newColTupleList;
      ColTupleList firstPartTupleList;
      for (unsigned i=0; i < totalColumns; i++)
      {
         colTupleList = static_cast<ColTupleList>(colValueList[i]);
         for (uint64_t j=rowsLeft; j > 0; j--)
         {
            newColTupleList.push_back(colTupleList[totalRow-j]);
         }
         colNewValueList.push_back(newColTupleList);
         newColTupleList.clear();
         //upate the oldvalue list for the old extent
         for (uint64_t j=0; j < (totalRow-rowsLeft); j++)
         {
            firstPartTupleList.push_back(colTupleList[j]);
         }
         colOldValueList.push_back(firstPartTupleList);
         firstPartTupleList.clear();
      }

   // end of allocate row id

#ifdef PROFILE
timer.start("writeColumnRec");
#endif
//cout << "Writing column record" << endl;

	if (rc == NO_ERROR)
	{
		//----------------------------------------------------------------------
		//Mark extents invalid
		//----------------------------------------------------------------------
		vector<BRM::LBID_t> lbids;
        vector<CalpontSystemCatalog::ColDataType> colDataTypes;
		bool successFlag = true;
		unsigned width = 0;
		int         curFbo = 0, curBio, lastFbo = -1;
		
		if (isFirstBatchPm && (totalRow == rowsLeft))
		{}
		else {
		for (unsigned i = 0; i < colStructList.size(); i++)
		{
			colOp = m_colOp[op(colStructList[i].fCompressionType)];
			width = colStructList[i].colWidth;
			successFlag = colOp->calculateRowId(lastRid , BYTE_PER_BLOCK/width, width, curFbo, curBio);
			if (successFlag) {
				if (curFbo != lastFbo) {
					RETURN_ON_ERROR(AddLBIDtoList(txnid,
												  lbids,
                                                  colDataTypes,
                                                  colStructList[i],
												  curFbo));
				}
			}
		}
		}
  
		if (lbids.size() > 0)
				rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);

		//----------------------------------------------------------------------
		// Write row(s) to database file(s)
		//----------------------------------------------------------------------
		rc = writeColumnRec(txnid, colStructList, colOldValueList, rowIdArray, newColStructList, colNewValueList, tableOid);
	}
   return rc;
}

int WriteEngineWrapper::insertColumnRec_SYS(const TxnID& txnid,
                                        ColStructList& colStructList,
                                        ColValueList& colValueList,
                                        DctnryStructList& dctnryStructList,
                                        DictStrList& dictStrList,
										const int32_t tableOid)
{
   int            rc;
   RID*           rowIdArray = NULL;
   ColTupleList   curTupleList;
   Column         curCol;
   ColStruct      curColStruct;
   ColValueList   colOldValueList;
   ColValueList   colNewValueList;
   ColStructList  newColStructList;
   DctnryStructList newDctnryStructList;
   HWM            hwm = 0;
   HWM            newHwm = 0;
   HWM            oldHwm = 0;
   ColTupleList::size_type totalRow;
   ColStructList::size_type totalColumns;
   uint64_t rowsLeft = 0;
   bool newExtent = false;
   RIDList ridList;
   ColumnOp* colOp = NULL;
   uint i = 0;
#ifdef PROFILE
 StopWatch timer;
#endif
   // debug information for testing
   if (isDebug(DEBUG_2)) {
      printf("\nIn wrapper insert\n");
      printInputValue(colStructList, colValueList, ridList);
   }
   // end

   //Convert data type and column width to write engine specific
   for (i = 0; i < colStructList.size(); i++)
      Convertor::convertColType(&colStructList[i]);

   rc = checkValid(txnid, colStructList, colValueList, ridList);
   if (rc != NO_ERROR)
      return rc;

   setTransId(txnid);

   curTupleList = static_cast<ColTupleList>(colValueList[0]);
   totalRow = curTupleList.size();
   totalColumns = colStructList.size();
   rowIdArray = new RID[totalRow];
   // use scoped_array to ensure ptr deletion regardless of where we return
   boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
   memset(rowIdArray, 0, (sizeof(RID)*totalRow));

   // allocate row id(s)
   curColStruct = colStructList[0];
   colOp = m_colOp[op(curColStruct.fCompressionType)];

   colOp->initColumn(curCol);

   //Get the correct segment, partition, column file
   uint16_t dbRoot, segmentNum;
   uint32_t partitionNum;
   vector<ExtentInfo> colExtentInfo; //Save those empty extents in case of failure to rollback
   vector<ExtentInfo> dictExtentInfo; //Save those empty extents in case of failure to rollback
   vector<ExtentInfo> fileInfo;
   ExtentInfo info;
   //Don't search for empty space, always append to the end. May need to revisit this part
   dbRoot = curColStruct.fColDbRoot;
   int  extState;
   bool extFound;
   RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(
      curColStruct.dataOid, dbRoot, partitionNum, segmentNum, hwm,
      extState, extFound));
	  
   for (i = 0; i < colStructList.size(); i++)
	{
		colStructList[i].fColPartition = partitionNum;
		colStructList[i].fColSegment = segmentNum;
		colStructList[i].fColDbRoot = dbRoot;
	}
   oldHwm = hwm; //Save this info for rollback
   //need to pass real dbRoot, partition, and segment to setColParam
   colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType,
       curColStruct.colType, curColStruct.dataOid, curColStruct.fCompressionType,
       dbRoot, partitionNum, segmentNum);

   string segFile;
   rc = colOp->openColumnFile(curCol, segFile);
   if (rc != NO_ERROR) {
      return rc;
   }

   //get hwm first
   // @bug 286 : fix for bug 286 - correct the typo in getHWM
   //RETURN_ON_ERROR(BRMWrapper::getInstance()->getHWM(curColStruct.dataOid, hwm));

   //...Note that we are casting totalRow to int to be in sync with
   //...allocRowId().  So we are assuming that totalRow
   //...(curTupleList.size()) will fit into an int.  We arleady made
   //...that assumption earlier in this method when we used totalRow
   //...in the call to calloc() to allocate rowIdArray.
   Column newCol;
   bool newFile;

#ifdef PROFILE
timer.start("allocRowId");
#endif

	newColStructList = colStructList;
	newDctnryStructList = dctnryStructList;
	std::vector<boost::shared_ptr<DBRootExtentTracker> >   dbRootExtentTrackers;
	bool bUseStartExtent = true;
	rc = colOp->allocRowId(txnid, bUseStartExtent,
      curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent, rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList, 
	  dbRootExtentTrackers, false, false, 0);
	
	if ((rc == ERR_FILE_DISK_SPACE) && newExtent) 
	{
		for (i = 0; i < newColStructList.size(); i++)
		{
				info.oid = newColStructList[i].dataOid;
				info.partitionNum = newColStructList[i].fColPartition;
				info.segmentNum = newColStructList[i].fColSegment;
				info.dbRoot = newColStructList[i].fColDbRoot;
				if (newFile)
					fileInfo.push_back (info);
					
				colExtentInfo.push_back (info);
		}
		int rc1 = BRMWrapper::getInstance()->deleteEmptyColExtents(colExtentInfo);
		if ((rc1 == 0) &&  newFile)
        {
           rc1 = colOp->deleteFile(fileInfo[0].oid, fileInfo[0].dbRoot, fileInfo[0].partitionNum, fileInfo[0].segmentNum);
		   if ( rc1 != NO_ERROR)
				return rc;
		   FileOp fileOp;
		   for (i = 0; i < newDctnryStructList.size(); i++)
		   {
				if (newDctnryStructList[i].dctnryOid > 0)
				{    
					info.oid = newDctnryStructList[i].dctnryOid;
					info.partitionNum = newDctnryStructList[i].fColPartition;
					info.segmentNum = newDctnryStructList[i].fColSegment;
					info.dbRoot = newDctnryStructList[i].fColDbRoot;
					info.newFile = true;
					fileInfo.push_back (info);
					dictExtentInfo.push_back (info);
				}
			}
			if (dictExtentInfo.size() > 0)
			{
				rc1 = BRMWrapper::getInstance()->deleteEmptyDictStoreExtents(dictExtentInfo);
				if ( rc1 != NO_ERROR)
					return rc;
				for (unsigned j = 0; j < fileInfo.size(); j++)
				{
					rc1 = fileOp.deleteFile(fileInfo[j].oid, fileInfo[j].dbRoot,
                                             fileInfo[j].partitionNum, fileInfo[j].segmentNum);
				}
			}
        }
   }

   TableMetaData* aTableMetaData = TableMetaData::makeTableMetaData(tableOid);
   //..Expand initial abbreviated extent if any RID in 1st extent is > 256K
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
   if ((partitionNum == 0) &&
       (segmentNum   == 0) &&
       ((totalRow-rowsLeft) > 0) &&
       (rowIdArray[totalRow-rowsLeft-1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
   {
       for (unsigned k=1; k<colStructList.size(); k++)
       {
           Column expandCol;
           colOp = m_colOp[op(colStructList[k].fCompressionType)];
           colOp->setColParam(expandCol, 0,
               colStructList[k].colWidth,
               colStructList[k].colDataType,
               colStructList[k].colType,
               colStructList[k].dataOid,
               colStructList[k].fCompressionType,
               dbRoot,
               partitionNum,
               segmentNum);
           rc = colOp->openColumnFile(expandCol, segFile);
           if (rc == NO_ERROR)
           {
               if (colOp->abbreviatedExtent(expandCol.dataFile.pFile, colStructList[k].colWidth))
               {
                   rc = colOp->expandAbbrevExtent(expandCol);
               }
           }
           if (rc != NO_ERROR)
           {
               if (newExtent)
               {
                   //Remove the empty extent added to the first column
                   int rc1 = BRMWrapper::getInstance()->
                                 deleteEmptyColExtents(colExtentInfo);
                   if ((rc1 == 0) && newFile)
                   {
                       rc1 = colOp->deleteFile(fileInfo[0].oid,
                                                fileInfo[0].dbRoot,
                                                fileInfo[0].partitionNum,
                                                fileInfo[0].segmentNum);
                   }
               }
               colOp->clearColumn(expandCol); // closes the file
               return rc;
           }
           colOp->clearColumn(expandCol); // closes the file
       }
   }

   //Tokenize data if needed
   dictStr::iterator dctStr_iter;
   ColTupleList::iterator col_iter;
   for (i = 0; i < colStructList.size(); i++)
   {
      if (colStructList[i].tokenFlag)
      {
         dctStr_iter = dictStrList[i].begin();
         col_iter = colValueList[i].begin();
         Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];

         dctnryStructList[i].fColPartition = partitionNum;
         dctnryStructList[i].fColSegment = segmentNum;
         dctnryStructList[i].fColDbRoot = dbRoot;
         rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid,
         //          dctnryStructList[i].treeOid, dctnryStructList[i].listOid,
                     dctnryStructList[i].fColDbRoot, dctnryStructList[i].fColPartition,
                     dctnryStructList[i].fColSegment);
         if (rc !=NO_ERROR)
             return rc;

		ColExtsInfo aColExtsInfo = aTableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
		ColExtsInfo::iterator it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if ((it->dbRoot == dctnryStructList[i].fColDbRoot) && (it->partNum == dctnryStructList[i].fColPartition) && (it->segNum == dctnryStructList[i].fColSegment))
				break;
			it++;
		}

		if (it == aColExtsInfo.end()) //add this one to the list
		{
			ColExtInfo aExt;
			aExt.dbRoot =dctnryStructList[i].fColDbRoot;
			aExt.partNum = dctnryStructList[i].fColPartition;
			aExt.segNum = dctnryStructList[i].fColSegment;
			aExt.compType = dctnryStructList[i].fCompressionType;	
			aExt.isDict = true;			
			aColExtsInfo.push_back(aExt);
			aTableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
		}
         for (uint32_t     rows = 0; rows < (totalRow - rowsLeft); rows++)
         {
             if (dctStr_iter->length() == 0)
               {
                   Token nullToken;
                 col_iter->data = nullToken;
               }
             else
               {
#ifdef PROFILE
timer.start("tokenize");
#endif
                 DctnryTuple dctTuple;
                 memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
                 dctTuple.sigSize = dctStr_iter->length();
                 dctTuple.isNull = false;
                   rc = tokenize(txnid, dctTuple, dctnryStructList[i].fCompressionType);
                 if (rc != NO_ERROR)
                 {
                     dctnry->closeDctnry();
                     return rc;
                 }
#ifdef PROFILE
timer.stop("tokenize");
#endif
                   col_iter->data = dctTuple.token;
               }
               dctStr_iter++;
               col_iter++;

         }
         //close dictionary files
         rc = dctnry->closeDctnry();
         if (rc != NO_ERROR)
             return rc;

         if (newExtent)
         {
             rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid,
             //            dctnryStructList[i].treeOid, dctnryStructList[i].listOid,
                           newDctnryStructList[i].fColDbRoot, newDctnryStructList[i].fColPartition,
                           newDctnryStructList[i].fColSegment);
             if (rc !=NO_ERROR)
                 return rc;

			aColExtsInfo = aTableMetaData->getColExtsInfo(newDctnryStructList[i].dctnryOid);
			it = aColExtsInfo.begin();
			while (it != aColExtsInfo.end())
			{
				if ((it->dbRoot == newDctnryStructList[i].fColDbRoot) && (it->partNum == newDctnryStructList[i].fColPartition) && (it->segNum == newDctnryStructList[i].fColSegment))
					break;
				it++;
			}

			if (it == aColExtsInfo.end()) //add this one to the list
			{
				ColExtInfo aExt;
				aExt.dbRoot =newDctnryStructList[i].fColDbRoot;
				aExt.partNum = newDctnryStructList[i].fColPartition;
				aExt.segNum = newDctnryStructList[i].fColSegment;
				aExt.compType = newDctnryStructList[i].fCompressionType;	
				aExt.isDict = true;			
				aColExtsInfo.push_back(aExt);
				aTableMetaData->setColExtsInfo(newDctnryStructList[i].dctnryOid, aColExtsInfo);
			}	 
			
             for (uint32_t     rows = 0; rows < rowsLeft; rows++)
             {
             if (dctStr_iter->length() == 0)
               {
                   Token nullToken;
                 col_iter->data = nullToken;
               }
             else
               {
#ifdef PROFILE
timer.start("tokenize");
#endif
                 DctnryTuple dctTuple;
                 memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
                 dctTuple.sigSize = dctStr_iter->length();
                 dctTuple.isNull = false;
                 rc = tokenize(txnid, dctTuple, newDctnryStructList[i].fCompressionType);
                 if (rc != NO_ERROR)
                 {
                     dctnry->closeDctnry();
                     return rc;
                 }
#ifdef PROFILE
timer.stop("tokenize");
#endif
                     col_iter->data = dctTuple.token;
                 }
                 dctStr_iter++;
                 col_iter++;
             }
             //close dictionary files
             rc = dctnry->closeDctnry();
             if (rc != NO_ERROR)
                 return rc;
         }
      }
   }


   //Update column info structure @Bug 1862 set hwm
   //@Bug 2205 Check whether all rows go to the new extent
   RID lastRid = 0;
   RID lastRidNew = 0;
   if (totalRow-rowsLeft > 0)
   {
     lastRid = rowIdArray[totalRow-rowsLeft-1];
     lastRidNew = rowIdArray[totalRow-1];
   }
   else
   {
     lastRid = 0;
     lastRidNew = rowIdArray[totalRow-1];
   }
   //cout << "rowid allocated is "  << lastRid << endl;
   //if a new extent is created, all the columns in this table should have their own new extent

   //@Bug 1701. Close the file
   m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
   std::vector<BulkSetHWMArg> hwmVecNewext;
   std::vector<BulkSetHWMArg> hwmVecOldext;
   if (newExtent) //Save all hwms to set them later.
   {
	  BulkSetHWMArg aHwmEntryNew;
	  BulkSetHWMArg aHwmEntryOld;
      bool succFlag = false;
      unsigned colWidth = 0;
      int  extState;
      bool extFound;
      int      curFbo = 0, curBio;
      for (i=0; i < totalColumns; i++)
      {
		 Column         curColLocal;
		 colOp->initColumn(curColLocal);
		 
         colOp = m_colOp[op(newColStructList[i].fCompressionType)];
         colOp->setColParam(curColLocal, 0,
            newColStructList[i].colWidth, newColStructList[i].colDataType,
            newColStructList[i].colType, newColStructList[i].dataOid,
            newColStructList[i].fCompressionType, dbRoot, partitionNum, segmentNum);

         rc = BRMWrapper::getInstance()->getLastHWM_DBroot(
            curColLocal.dataFile.fid, dbRoot, partitionNum, segmentNum, oldHwm,
            extState, extFound);

         info.oid = curColLocal.dataFile.fid;
         info.partitionNum = partitionNum;
         info.segmentNum = segmentNum;
         info.dbRoot = dbRoot;
         info.hwm = oldHwm;
         colExtentInfo.push_back(info);
         // @Bug 2714 need to set hwm for the old extent
         colWidth = colStructList[i].colWidth;
         succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
         //cout << "insertcolumnrec   oid:rid:fbo:hwm = " << colStructList[i].dataOid << ":" << lastRid << ":" << curFbo << ":" << hwm << endl;
         if (succFlag)
         {
            if ((HWM)curFbo > oldHwm)
			{
				aHwmEntryOld.oid = colStructList[i].dataOid;
				aHwmEntryOld.partNum = partitionNum;
				aHwmEntryOld.segNum = segmentNum;
				aHwmEntryOld.hwm = curFbo;
				hwmVecOldext.push_back(aHwmEntryOld);
			}
         }
         else
            return ERR_INVALID_PARAM;

         colWidth = newColStructList[i].colWidth;
         succFlag = colOp->calculateRowId(lastRidNew, BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
         if (succFlag)
         {
			aHwmEntryNew.oid = newColStructList[i].dataOid;
			aHwmEntryNew.partNum = newColStructList[i].fColPartition;
			aHwmEntryNew.segNum = newColStructList[i].fColSegment;
			aHwmEntryNew.hwm = curFbo;
			hwmVecNewext.push_back(aHwmEntryNew); 
         }
		 m_colOp[op(curColLocal.compressionType)]->clearColumn(curColLocal);
      }

      //Prepare the valuelist for the new extent
      ColTupleList colTupleList;
      ColTupleList newColTupleList;
      ColTupleList firstPartTupleList;
      for (unsigned i=0; i < totalColumns; i++)
      {
         colTupleList = static_cast<ColTupleList>(colValueList[i]);
         for (uint64_t j=rowsLeft; j > 0; j--)
         {
            newColTupleList.push_back(colTupleList[totalRow-j]);
         }
         colNewValueList.push_back(newColTupleList);
         newColTupleList.clear();
         //upate the oldvalue list for the old extent
         for (uint64_t j=0; j < (totalRow-rowsLeft); j++)
         {
            firstPartTupleList.push_back(colTupleList[j]);
         }
         colOldValueList.push_back(firstPartTupleList);
         firstPartTupleList.clear();
      }
   }

//Mark extents invalid
   vector<BRM::LBID_t> lbids;
   vector<CalpontSystemCatalog::ColDataType> colDataTypes;
   bool successFlag = true;
   unsigned width = 0;
   BRM::LBID_t lbid;
   int         curFbo = 0, curBio, lastFbo = -1;
   if (totalRow-rowsLeft > 0)
   {
      for (unsigned i = 0; i < colStructList.size(); i++)
      {
         colOp = m_colOp[op(colStructList[i].fCompressionType)];
         width = colStructList[i].colWidth;
         successFlag = colOp->calculateRowId(lastRid , BYTE_PER_BLOCK/width, width, curFbo, curBio);
         if (successFlag) {
            if (curFbo != lastFbo) {
               RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(
                   colStructList[i].dataOid, colStructList[i].fColPartition,
                   colStructList[i].fColSegment, curFbo, lbid));
               lbids.push_back((BRM::LBID_t)lbid);
               colDataTypes.push_back(colStructList[i].colDataType);
            }
         }
      }
   }
   lastRid = rowIdArray[totalRow-1];
   for (unsigned i = 0; i < newColStructList.size(); i++)
   {
      colOp = m_colOp[op(newColStructList[i].fCompressionType)];
      width = newColStructList[i].colWidth;
      successFlag = colOp->calculateRowId(lastRid , BYTE_PER_BLOCK/width, width, curFbo, curBio);
      if (successFlag) 
      {
         if (curFbo != lastFbo) 
         {
             RETURN_ON_ERROR(AddLBIDtoList(txnid,
                                           lbids,
                                           colDataTypes,
                                           newColStructList[i],
                                           curFbo));
         }
      }
   }
   //cout << "lbids size = " << lbids.size()<< endl;
   if (lbids.size() > 0)
       rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);

   if (rc == NO_ERROR)
   {
      if (newExtent)
      {
         rc = writeColumnRec(txnid, colStructList, colOldValueList, rowIdArray, newColStructList, colNewValueList, tableOid);
      }
      else
      {
         rc = writeColumnRec(txnid, colStructList, colValueList, rowIdArray, newColStructList, colNewValueList, tableOid);
      }
   }
#ifdef PROFILE
timer.stop("writeColumnRec");
#endif
//   for (ColTupleList::size_type  i = 0; i < totalRow; i++)
//      ridList.push_back((RID) rowIdArray[i]);

  // if (rc == NO_ERROR)
   //   rc = flushDataFiles(NO_ERROR);

	if ( !newExtent )
	{
		//flushVMCache();
	  bool succFlag = false;
      unsigned colWidth = 0;
      int  extState;
      bool extFound;
      int curFbo = 0, curBio;
	  std::vector<BulkSetHWMArg> hwmVec;
      for (unsigned i=0; i < totalColumns; i++)
      {
         //colOp = m_colOp[op(colStructList[i].fCompressionType)];
		 //Set all columns hwm together
		 BulkSetHWMArg aHwmEntry;
         RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(colStructList[i].dataOid, dbRoot, partitionNum, segmentNum, hwm,
            extState, extFound));
         colWidth = colStructList[i].colWidth;
         succFlag = colOp->calculateRowId(lastRid, BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
         //cout << "insertcolumnrec   oid:rid:fbo:hwm = " << colStructList[i].dataOid << ":" << lastRid << ":" << curFbo << ":" << hwm << endl;
         if (succFlag)
         {
            if ((HWM)curFbo > hwm)
			{
				aHwmEntry.oid = colStructList[i].dataOid;
				aHwmEntry.partNum = partitionNum;
				aHwmEntry.segNum = segmentNum;
				aHwmEntry.hwm = curFbo;
				hwmVec.push_back(aHwmEntry); 
			}
         }
         else
            return ERR_INVALID_PARAM;
       }
	   if (hwmVec.size() > 0 ) 
	   {
			std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
			RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP( hwmVec, mergeCPDataArgs));		
	   }
	}
	if (newExtent)
	{
#ifdef PROFILE
timer.start("flushVMCache");
#endif
		std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
		RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP( hwmVecNewext, mergeCPDataArgs));
		RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP( hwmVecOldext, mergeCPDataArgs));
      //flushVMCache();
#ifdef PROFILE
timer.stop("flushVMCache");
#endif
   }

#ifdef PROFILE
timer.finish();
#endif
   return rc;
}

int WriteEngineWrapper::insertColumnRec_Single(const TxnID& txnid,
                                        ColStructList& colStructList,
                                        ColValueList& colValueList,
                                        DctnryStructList& dctnryStructList,
                                        DictStrList& dictStrList,
										const int32_t tableOid)
{
	int            rc;
	RID*           rowIdArray = NULL;
	ColTupleList   curTupleList;
	Column         curCol;
	ColStruct      curColStruct;
	ColValueList   colOldValueList;
	ColValueList   colNewValueList;
	ColStructList  newColStructList;
	DctnryStructList newDctnryStructList;
	HWM            hwm = 0;
	HWM            newHwm = 0;
	HWM            oldHwm = 0;
	ColTupleList::size_type totalRow;
	ColStructList::size_type totalColumns;
	uint64_t rowsLeft = 0;
	bool newExtent = false;
	RIDList ridList;
	ColumnOp* colOp = NULL;
	uint i = 0;

#ifdef PROFILE
StopWatch timer;
#endif
	// debug information for testing
	if (isDebug(DEBUG_2)) {
		printf("\nIn wrapper insert\n");
		printInputValue(colStructList, colValueList, ridList);
	}
	// end

	//Convert data type and column width to write engine specific
	for (i = 0; i < colStructList.size(); i++)
		Convertor::convertColType(&colStructList[i]);

	rc = checkValid(txnid, colStructList, colValueList, ridList);
	if (rc != NO_ERROR)
		return rc;

	setTransId(txnid);

	curTupleList = static_cast<ColTupleList>(colValueList[0]);
	totalRow = curTupleList.size();
	totalColumns = colStructList.size();
	rowIdArray = new RID[totalRow];
	// use scoped_array to ensure ptr deletion regardless of where we return
	boost::scoped_array<RID> rowIdArrayPtr(rowIdArray);
	memset(rowIdArray, 0, (sizeof(RID)*totalRow));

	//--------------------------------------------------------------------------
	// allocate row id(s)
	//--------------------------------------------------------------------------
	curColStruct = colStructList[0];
	colOp = m_colOp[op(curColStruct.fCompressionType)];

	colOp->initColumn(curCol);

	//Get the correct segment, partition, column file
	uint16_t dbRoot;
	uint16_t segmentNum   = 0;
	uint32_t partitionNum = 0;
	//Don't search for empty space, always append to the end. May revisit later
	dbRoot = curColStruct.fColDbRoot;
	int  extState;
	bool bStartExtFound;
	bool bUseStartExtent = false;
	RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(
		curColStruct.dataOid, dbRoot, partitionNum, segmentNum, hwm,
		extState, bStartExtFound));
	if ((bStartExtFound) && (extState == BRM::EXTENTAVAILABLE))
		bUseStartExtent = true;
	
	for (i = 0; i < colStructList.size(); i++)
	{
		colStructList[i].fColPartition = partitionNum;
		colStructList[i].fColSegment   = segmentNum;
		colStructList[i].fColDbRoot    = dbRoot;
	}
	for (i = 0; i < dctnryStructList.size(); i++)
	{
		dctnryStructList[i].fColPartition = partitionNum;
		dctnryStructList[i].fColSegment   = segmentNum;
		dctnryStructList[i].fColDbRoot    = dbRoot;
	}

	oldHwm = hwm; //Save this info for rollback
	//need to pass real dbRoot, partition, and segment to setColParam
	colOp->setColParam(curCol, 0, curColStruct.colWidth, curColStruct.colDataType,
		curColStruct.colType, curColStruct.dataOid, curColStruct.fCompressionType,
		dbRoot, partitionNum, segmentNum);

	string segFile;
	if (bUseStartExtent)
	{
		rc = colOp->openColumnFile(curCol, segFile);
		if (rc != NO_ERROR) {
			return rc;
		}
	}

	bool newFile;

#ifdef PROFILE
timer.start("allocRowId");
#endif
	newColStructList = colStructList;
	newDctnryStructList = dctnryStructList;
	std::vector<boost::shared_ptr<DBRootExtentTracker> >  dbRootExtentTrackers;
	rc = colOp->allocRowId(txnid, bUseStartExtent,
		curCol, (uint64_t)totalRow, rowIdArray, hwm, newExtent,
		rowsLeft, newHwm, newFile, newColStructList, newDctnryStructList, 
		dbRootExtentTrackers, false, false, 0);
	 
	//--------------------------------------------------------------------------
	// Handle case where we ran out of disk space allocating a new extent.
	// Rollback extentmap and delete any db files that were created.
	//--------------------------------------------------------------------------
	if (rc != NO_ERROR)
	{
		if ((rc == ERR_FILE_DISK_SPACE) && newExtent) 
		{
			vector<ExtentInfo> colExtentInfo;
			vector<ExtentInfo> dictExtentInfo;
			vector<ExtentInfo> fileInfo;
			ExtentInfo info;

			for (i = 0; i < newColStructList.size(); i++)
			{
				info.oid          = newColStructList[i].dataOid;
				info.partitionNum = newColStructList[i].fColPartition;
				info.segmentNum   = newColStructList[i].fColSegment;
				info.dbRoot       = newColStructList[i].fColDbRoot;
				if (newFile)
					fileInfo.push_back (info);
					
				colExtentInfo.push_back (info);
			}

			int rc1 = BRMWrapper::getInstance()->deleteEmptyColExtents(colExtentInfo);

			// Only rollback dictionary extents "if" store file is new
			if ((rc1 == 0) && newFile)
        	{
				for (unsigned int j = 0; j < fileInfo.size(); j++)
				{
					// ignore return code and delete what we can
					rc1 = colOp->deleteFile(fileInfo[j].oid,
						fileInfo[j].dbRoot,
						fileInfo[j].partitionNum,
						fileInfo[j].segmentNum);
				}
				fileInfo.clear();

				for (i = 0; i < newDctnryStructList.size(); i++)
				{
					if (newDctnryStructList[i].dctnryOid > 0)
					{
						info.oid          = newDctnryStructList[i].dctnryOid;
						info.partitionNum = newDctnryStructList[i].fColPartition;
						info.segmentNum   = newDctnryStructList[i].fColSegment;
						info.dbRoot       = newDctnryStructList[i].fColDbRoot;
						info.newFile      = true;
						fileInfo.push_back (info);
						dictExtentInfo.push_back (info);
					}
				}

				if (dictExtentInfo.size() > 0)
				{
					FileOp fileOp;
					rc1 = BRMWrapper::getInstance()->deleteEmptyDictStoreExtents(dictExtentInfo);
					if ( rc1 != NO_ERROR)
						return rc;
					for (unsigned j = 0; j < fileInfo.size(); j++)
					{
						rc1 = fileOp.deleteFile(fileInfo[j].oid,
							fileInfo[j].dbRoot,
							fileInfo[j].partitionNum,
							fileInfo[j].segmentNum);
					}
				}
			}
		} // disk space error allocating new extent

		return rc;
	}     // rc != NO_ERROR from call to allocRowID()
   
#ifdef PROFILE
timer.stop("allocRowId");
#endif

	TableMetaData* aTableMetaData= TableMetaData::makeTableMetaData(tableOid);
	//--------------------------------------------------------------------------
	// Expand initial abbreviated extent if any RID in 1st extent is > 256K.
	// if totalRow == rowsLeft, then not adding rows to 1st extent, so skip it.
	//--------------------------------------------------------------------------
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
	if ((colStructList[0].fColPartition == 0) &&
		(colStructList[0].fColSegment   == 0) &&
		((totalRow-rowsLeft) > 0) &&
		(rowIdArray[totalRow-rowsLeft-1] >= (RID)INITIAL_EXTENT_ROWS_TO_DISK))
	{
		for (unsigned k=1; k<colStructList.size(); k++)
		{
			Column expandCol;
			colOp = m_colOp[op(colStructList[k].fCompressionType)];
			colOp->setColParam(expandCol, 0,
				colStructList[k].colWidth,
				colStructList[k].colDataType,
				colStructList[k].colType,
				colStructList[k].dataOid,
				colStructList[k].fCompressionType,
				colStructList[k].fColDbRoot,
				colStructList[k].fColPartition,
				colStructList[k].fColSegment);
			rc = colOp->openColumnFile(expandCol, segFile);
			if (rc == NO_ERROR)
			{
				if (colOp->abbreviatedExtent(
					expandCol.dataFile.pFile, colStructList[k].colWidth))
				{
					rc = colOp->expandAbbrevExtent(expandCol);
				}
			}

			colOp->clearColumn(expandCol); // closes the file
			if (rc != NO_ERROR)
			{
				return rc;
			}
		} // loop through columns
	}     // if starting extent needs to be expanded
	
	//--------------------------------------------------------------------------
	// Tokenize data if needed
	//--------------------------------------------------------------------------
	dictStr::iterator dctStr_iter;
	ColTupleList::iterator col_iter;
	for (unsigned i = 0; i < colStructList.size(); i++)
	{
		if (colStructList[i].tokenFlag)
		{
			dctStr_iter = dictStrList[i].begin();
			col_iter = colValueList[i].begin();
			Dctnry* dctnry = m_dctnry[op(dctnryStructList[i].fCompressionType)];

			ColExtsInfo aColExtsInfo = aTableMetaData->getColExtsInfo(dctnryStructList[i].dctnryOid);
			ColExtsInfo::iterator it = aColExtsInfo.begin();
			if (bUseStartExtent)
			{
				rc = dctnry->openDctnry(dctnryStructList[i].dctnryOid,
					dctnryStructList[i].fColDbRoot,
					dctnryStructList[i].fColPartition,
					dctnryStructList[i].fColSegment);
				if (rc !=NO_ERROR)
					return rc;

				while (it != aColExtsInfo.end())
				{
					if ((it->dbRoot == dctnryStructList[i].fColDbRoot) && (it->partNum == dctnryStructList[i].fColPartition) && (it->segNum == dctnryStructList[i].fColSegment))
						break;
					it++;
				}

				if (it == aColExtsInfo.end()) //add this one to the list
				{
					ColExtInfo aExt;
					aExt.dbRoot =dctnryStructList[i].fColDbRoot;
					aExt.partNum = dctnryStructList[i].fColPartition;
					aExt.segNum = dctnryStructList[i].fColSegment;
					aExt.compType = dctnryStructList[i].fCompressionType;	
					aExt.isDict = true;			
					aColExtsInfo.push_back(aExt);
					aTableMetaData->setColExtsInfo(dctnryStructList[i].dctnryOid, aColExtsInfo);
				}
				
				
				for (uint32_t rows = 0; rows < (totalRow - rowsLeft); rows++)
				{
					if (dctStr_iter->length() == 0)
					{
						Token nullToken;
						col_iter->data = nullToken;
					}
					else
					{
#ifdef PROFILE
timer.start("tokenize");
#endif
						DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
						dctTuple.sigSize = dctStr_iter->length();
						dctTuple.isNull = false;
						rc = tokenize(txnid,
							dctTuple,
							dctnryStructList[i].fCompressionType);
						if (rc != NO_ERROR)
						{
							dctnry->closeDctnry();
							return rc;
						}
#ifdef PROFILE
timer.stop("tokenize");
#endif
						col_iter->data = dctTuple.token;
					}
					dctStr_iter++;
					col_iter++;
				}

				//close dictionary files
				rc = dctnry->closeDctnry();
				if (rc != NO_ERROR)
					return rc;
			} // tokenize dictionary rows in 1st extent

			if (newExtent)
			{
				rc = dctnry->openDctnry(newDctnryStructList[i].dctnryOid,
					newDctnryStructList[i].fColDbRoot,
					newDctnryStructList[i].fColPartition,
					newDctnryStructList[i].fColSegment);
				if (rc !=NO_ERROR)
					return rc;

				aColExtsInfo = aTableMetaData->getColExtsInfo(newDctnryStructList[i].dctnryOid);
				it = aColExtsInfo.begin();
				while (it != aColExtsInfo.end())
				{
					if ((it->dbRoot == newDctnryStructList[i].fColDbRoot) && (it->partNum == newDctnryStructList[i].fColPartition) && (it->segNum == newDctnryStructList[i].fColSegment))
						break;
					it++;
				}

				if (it == aColExtsInfo.end()) //add this one to the list
				{
					ColExtInfo aExt;
					aExt.dbRoot =newDctnryStructList[i].fColDbRoot;
					aExt.partNum = newDctnryStructList[i].fColPartition;
					aExt.segNum = newDctnryStructList[i].fColSegment;
					aExt.compType = newDctnryStructList[i].fCompressionType;	
					aExt.isDict = true;			
					aColExtsInfo.push_back(aExt);
					aTableMetaData->setColExtsInfo(newDctnryStructList[i].dctnryOid, aColExtsInfo);
				}
				
				for (uint32_t rows = 0; rows < rowsLeft; rows++)
				{
					if (dctStr_iter->length() == 0)
					{
						Token nullToken;
						col_iter->data = nullToken;
					}
					else
					{
#ifdef PROFILE
timer.start("tokenize");
#endif
						DctnryTuple dctTuple;
						memcpy(dctTuple.sigValue, dctStr_iter->c_str(), dctStr_iter->length());
						dctTuple.sigSize = dctStr_iter->length();
						dctTuple.isNull = false;
						rc = tokenize(txnid,
							dctTuple,
							newDctnryStructList[i].fCompressionType);
						if (rc != NO_ERROR)
						{
							dctnry->closeDctnry();
							return rc;
						}
#ifdef PROFILE
timer.stop("tokenize");
#endif
						col_iter->data = dctTuple.token;
					}
					dctStr_iter++;
					col_iter++;
				}

				//close dictionary files
				rc = dctnry->closeDctnry();
				if (rc != NO_ERROR)
					return rc;
			} // tokenize dictionary rows in second extent
		}     // tokenize dictionary columns
	}         // loop through columns to see which ones need tokenizing

	//----------------------------------------------------------------------
	// Update column info structure @Bug 1862 set hwm, and
	// Prepare ValueList for new extent (if applicable)
	//----------------------------------------------------------------------
	//@Bug 2205 Check whether all rows go to the new extent
	RID lastRid = 0;
	RID lastRidNew = 0;
	if (totalRow-rowsLeft > 0)
	{
		lastRid = rowIdArray[totalRow-rowsLeft-1];
		lastRidNew = rowIdArray[totalRow-1];
	}
	else
	{
		lastRid = 0;
		lastRidNew = rowIdArray[totalRow-1];
	}
	//cout << "rowid allocated is "  << lastRid << endl;
	//if a new extent is created, all the columns in this table should
	//have their own new extent

	//@Bug 1701. Close the file
	if (bUseStartExtent)
	{
		m_colOp[op(curCol.compressionType)]->clearColumn(curCol);
	}

	std::vector<BulkSetHWMArg> hwmVecNewext;
	std::vector<BulkSetHWMArg> hwmVecOldext;
	if (newExtent) //Save all hwms to set them later.
	{
		BulkSetHWMArg aHwmEntryNew;
		BulkSetHWMArg aHwmEntryOld;
      
		bool succFlag = false;
		unsigned colWidth = 0;
		int curFbo = 0, curBio;

		for (i=0; i < totalColumns; i++)
		{
			colOp = m_colOp[op(newColStructList[i].fCompressionType)];

			// @Bug 2714 need to set hwm for the old extent
			colWidth = colStructList[i].colWidth;
			succFlag = colOp->calculateRowId(lastRid,
				BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
			//cout << "insertcolumnrec   oid:rid:fbo:hwm = " <<
			//colStructList[i].dataOid << ":" << lastRid << ":" <<
			//curFbo << ":" << hwm << endl;
			if (succFlag)
			{
				if ((HWM)curFbo > oldHwm)
				{
					aHwmEntryOld.oid     = colStructList[i].dataOid;
					aHwmEntryOld.partNum = colStructList[i].fColPartition;
					aHwmEntryOld.segNum  = colStructList[i].fColSegment;
					aHwmEntryOld.hwm     = curFbo;
					hwmVecOldext.push_back(aHwmEntryOld);
				}
			}
			else
				return ERR_INVALID_PARAM;

			colWidth = newColStructList[i].colWidth;
			succFlag = colOp->calculateRowId(lastRidNew,
				BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
			if (succFlag)
			{
				aHwmEntryNew.oid     = newColStructList[i].dataOid;
				aHwmEntryNew.partNum = newColStructList[i].fColPartition;
				aHwmEntryNew.segNum  = newColStructList[i].fColSegment;
				aHwmEntryNew.hwm     = curFbo;
				hwmVecNewext.push_back(aHwmEntryNew); 
			}
		}

		//----------------------------------------------------------------------
		// Prepare the valuelist for the new extent
		//----------------------------------------------------------------------
		ColTupleList colTupleList;
		ColTupleList newColTupleList;
		ColTupleList firstPartTupleList;
		for (unsigned i=0; i < totalColumns; i++)
		{
			colTupleList = static_cast<ColTupleList>(colValueList[i]);
			for (uint64_t j=rowsLeft; j > 0; j--)
			{
				newColTupleList.push_back(colTupleList[totalRow-j]);
			}
			colNewValueList.push_back(newColTupleList);

			newColTupleList.clear();
			//upate the oldvalue list for the old extent
			for (uint64_t j=0; j < (totalRow-rowsLeft); j++)
			{
				firstPartTupleList.push_back(colTupleList[j]);
			}
			colOldValueList.push_back(firstPartTupleList);
			firstPartTupleList.clear();
		}
	}

	//--------------------------------------------------------------------------
	//Mark extents invalid
	//--------------------------------------------------------------------------
	vector<BRM::LBID_t> lbids;
	vector<CalpontSystemCatalog::ColDataType> colDataTypes;
	bool successFlag = true;
	unsigned width = 0;
	//BRM::LBID_t lbid;
	int curFbo = 0, curBio, lastFbo = -1;
/*	if (totalRow-rowsLeft > 0)
	{
		for (unsigned i = 0; i < colStructList.size(); i++)
		{
			colOp = m_colOp[op(colStructList[i].fCompressionType)];
			width = colStructList[i].colWidth;
			successFlag = colOp->calculateRowId(lastRid ,
				BYTE_PER_BLOCK/width, width, curFbo, curBio);
			if (successFlag) {
				if (curFbo != lastFbo) {
					RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(
						colStructList[i].dataOid,
						colStructList[i].fColPartition,
						colStructList[i].fColSegment, curFbo, lbid));
					lbids.push_back((BRM::LBID_t)lbid);
					colDataTypes.push_back(colStructList[i].colDataType);
				}
			}
		}
	}
*/
	lastRid = rowIdArray[totalRow-1];
	for (unsigned i = 0; i < colStructList.size(); i++)
	{
		colOp = m_colOp[op(colStructList[i].fCompressionType)];
		width = colStructList[i].colWidth;
		successFlag = colOp->calculateRowId(lastRid ,
			BYTE_PER_BLOCK/width, width, curFbo, curBio);
		if (successFlag) 
		{
			if (curFbo != lastFbo) 
			{
				colDataTypes.push_back(colStructList[i].colDataType);
				RETURN_ON_ERROR(AddLBIDtoList(txnid,
					lbids,
					colDataTypes,
					colStructList[i],
					curFbo));
			}
		}
	}

	//cout << "lbids size = " << lbids.size()<< endl;
	if (lbids.size() > 0)
		rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);

	//--------------------------------------------------------------------------
	// Write row(s) to database file(s)
	//--------------------------------------------------------------------------
#ifdef PROFILE
timer.start("writeColumnRec");
#endif
	if (rc == NO_ERROR)
	{
		if (newExtent)
		{
			rc = writeColumnRec(txnid, colStructList, colOldValueList,
				rowIdArray, newColStructList, colNewValueList, tableOid);
		}
		else
		{
			rc = writeColumnRec(txnid, colStructList, colValueList,
				rowIdArray, newColStructList, colNewValueList, tableOid);
		}
	}
#ifdef PROFILE
timer.stop("writeColumnRec");
#endif
//  for (ColTupleList::size_type  i = 0; i < totalRow; i++)
//      ridList.push_back((RID) rowIdArray[i]);

//  if (rc == NO_ERROR)
//  rc = flushDataFiles(NO_ERROR);

	//--------------------------------------------------------------------------
	// Update BRM
	//--------------------------------------------------------------------------
	if ( !newExtent )
	{
		//flushVMCache();
		bool succFlag = false;
		unsigned colWidth = 0;
		int  extState;
		bool extFound;
		int curFbo = 0, curBio;
		std::vector<BulkSetHWMArg> hwmVec;
		for (unsigned i=0; i < totalColumns; i++)
		{
			//colOp = m_colOp[op(colStructList[i].fCompressionType)];
			//Set all columns hwm together
			BulkSetHWMArg aHwmEntry;
			RETURN_ON_ERROR(BRMWrapper::getInstance()->getLastHWM_DBroot(
				colStructList[i].dataOid,
				colStructList[i].fColDbRoot,
				colStructList[i].fColPartition,
				colStructList[i].fColSegment,
				hwm,
				extState, extFound));
			colWidth = colStructList[i].colWidth;
			succFlag = colOp->calculateRowId(lastRid,
				BYTE_PER_BLOCK/colWidth, colWidth, curFbo, curBio);
			//cout << "insertcolumnrec   oid:rid:fbo:hwm = " <<
			//colStructList[i].dataOid << ":" << lastRid << ":" <<
			//curFbo << ":" << hwm << endl;
			if (succFlag)
			{
				if ((HWM)curFbo > hwm)
				{
					aHwmEntry.oid     = colStructList[i].dataOid;
					aHwmEntry.partNum = colStructList[i].fColPartition;
					aHwmEntry.segNum  = colStructList[i].fColSegment;
					aHwmEntry.hwm     = curFbo;
					hwmVec.push_back(aHwmEntry); 
				}
			}
			else
				return ERR_INVALID_PARAM;
		}

		std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
		RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(
			hwmVec, mergeCPDataArgs));
	}
	else // if (newExtent)
	{
#ifdef PROFILE
timer.start("flushVMCache");
#endif
		std::vector<BRM::CPInfoMerge> mergeCPDataArgs;
		if (hwmVecNewext.size() > 0)
			RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(
				hwmVecNewext, mergeCPDataArgs));
		if (hwmVecOldext.size() > 0)
			RETURN_ON_ERROR(BRMWrapper::getInstance()->bulkSetHWMAndCP(
				hwmVecOldext, mergeCPDataArgs));
		//flushVMCache();
#ifdef PROFILE
timer.stop("flushVMCache");
#endif
	}

#ifdef PROFILE
timer.finish();
#endif
	//flush PrimProc FD cache
	ColsExtsInfoMap colsExtsInfoMap = aTableMetaData->getColsExtsInfoMap();
	ColsExtsInfoMap::iterator it = colsExtsInfoMap.begin();
	ColExtsInfo::iterator aIt;
	std::vector<BRM::FileInfo> files;
	BRM::FileInfo aFile;
	while (it != colsExtsInfoMap.end())
	{
		aIt = (it->second).begin();
		aFile.oid = it->first;
		//cout << "OID:" << aArg.oid; 
		while (aIt != (it->second).end())
		{
			aFile.partitionNum = aIt->partNum;
			aFile.dbRoot =aIt->dbRoot;
			aFile.segmentNum = aIt->segNum;
			aFile.compType = aIt->compType;
			files.push_back(aFile);
			//cout <<"Added to files oid:dbroot:part:seg:compType = " << aFile.oid<<":"<<aFile.dbRoot<<":"<<aFile.partitionNum<<":"<<aFile.segmentNum
			//<<":"<<aFile.compType <<endl;
			aIt++;
		}
		it++;
	}
	if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size() > 0))
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
	TableMetaData::removeTableMetaData(tableOid);	
	return rc;
}

/*@brief printInputValue - Print input value
*/
/***********************************************************
 * DESCRIPTION:
 *    Print input value
 * PARAMETERS:
 *    tableOid - table object id
 *    colStructList - column struct list
 *    colValueList - column value list
 *    ridList - RID list
 * RETURN:
 *    none
 ***********************************************************/
void WriteEngineWrapper::printInputValue(const ColStructList& colStructList,
                                         const ColValueList& colValueList,
                                         const RIDList& ridList) const
{
   ColTupleList   curTupleList;
   ColStruct      curColStruct;
   ColTuple       curTuple;
   string         curStr;
   ColStructList::size_type i;
   ColTupleList::size_type  j;

   printf("\n=========================\n");
//      printf("\nTable OID : %d \n", tableOid);

   printf("\nTotal RIDs: %zu\n", ridList.size());
   for (i = 0; i < ridList.size(); i++)
       cout<<"RID["<<i<<"] : "<<ridList[i]<<"\n";
   printf("\nTotal Columns: %zu\n", colStructList.size());


   for (i = 0; i < colStructList.size(); i++) {
      curColStruct = colStructList[i];
      curTupleList = colValueList[i];

      printf("\nColumn[%zu]", i);
      printf("\nData file OID : %d \t", curColStruct.dataOid);
      printf("\tWidth : %d \t Type: %d", curColStruct.colWidth, curColStruct.colDataType);
      printf("\nTotal values : %zu \n", curTupleList.size());

      for (j = 0; j < curTupleList.size(); j++) {
         curTuple = curTupleList[j];

         try {
            if (curTuple.data.type() == typeid(int))
               curStr = boost::lexical_cast<string>(boost::any_cast<int>(curTuple.data));
            else
            if (curTuple.data.type() == typeid(float))
               curStr = boost::lexical_cast<string>(boost::any_cast<float>(curTuple.data));
            else
            if (curTuple.data.type() == typeid(long long))
               curStr = boost::lexical_cast<string>(boost::any_cast<long long>(curTuple.data));
            else
            if (curTuple.data.type() == typeid(double))
               curStr = boost::lexical_cast<string>(boost::any_cast<double>(curTuple.data));
//               else
//               if (curTuple.data.type() == typeid(bool))
//                  curStr = boost::lexical_cast<string>(boost::any_cast<bool>(curTuple.data));
            else
            if (curTuple.data.type() == typeid(short))
               curStr = boost::lexical_cast<string>(boost::any_cast<short>(curTuple.data));
            else
            if (curTuple.data.type() == typeid(char))
               curStr = boost::lexical_cast<string>(boost::any_cast<char>(curTuple.data));
            else
               curStr = boost::any_cast<string>(curTuple.data);
         }
         catch(...)
         {
         }

         if (isDebug(DEBUG_3))
            printf("Value[%zu] : %s\n", j, curStr.c_str());
      }

   }
   printf("\n=========================\n");
}

/***********************************************************
 * DESCRIPTION:
 *    Process version buffer before any write operation
 * PARAMETERS:
 *    txnid - transaction id
 *    oid - column oid
 *    totalRow - total number of rows
 *    rowIdArray - rowid array
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::processVersionBuffer(IDBDataFile* pFile, const TxnID& txnid,
                                             const ColStruct& colStruct, int width,
                                             int totalRow, const RID* rowIdArray, vector<LBIDRange> &  rangeList)
{
   if (idbdatafile::IDBPolicy::useHdfs())
      return 0;

   RID         curRowId;
   int         rc = NO_ERROR;
   int         curFbo = 0, curBio, lastFbo = -1;
   bool        successFlag;
   BRM::LBID_t lbid;
   BRM::VER_t  verId = (BRM::VER_t) txnid;
   vector<uint32_t> fboList;
   LBIDRange   range;
   ColumnOp* colOp = m_colOp[op(colStruct.fCompressionType)];

   for (int i = 0; i < totalRow; i++) {
      curRowId = rowIdArray[i];
      //cout << "processVersionBuffer got rid " << curRowId << endl;
      successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK/width, width, curFbo, curBio);
      if (successFlag) {
         if (curFbo != lastFbo) {
            //cout << "processVersionBuffer is processing lbid  " << lbid << endl;
            RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(
               colStruct.dataOid, colStruct.fColPartition, colStruct.fColSegment, curFbo, lbid));
             //cout << "processVersionBuffer is processing lbid  " << lbid << endl;
             fboList.push_back((uint32_t)curFbo);
             range.start = lbid;
             range.size = 1;
             rangeList.push_back(range);
         }
         lastFbo = curFbo;
      }
   }
   std::vector<VBRange> freeList;
   rc = BRMWrapper::getInstance()->
                        writeVB(pFile, verId, colStruct.dataOid,fboList, rangeList, colOp, freeList, colStruct.fColDbRoot);

   return rc;
}

int WriteEngineWrapper::processVersionBuffers(IDBDataFile* pFile, const TxnID& txnid,
                                              const ColStruct& colStruct, int width,
                                              int totalRow, const RIDList& ridList,
                                              vector<LBIDRange> &   rangeList)
{
   if (idbdatafile::IDBPolicy::useHdfs())
      return 0;

   RID         curRowId;
   int         rc = NO_ERROR;
   int         curFbo = 0, curBio, lastFbo = -1;
   bool        successFlag;
   BRM::LBID_t lbid;
   BRM::VER_t  verId = (BRM::VER_t) txnid;
   LBIDRange   range;
   vector<uint32_t> fboList;
   //vector<LBIDRange>   rangeList;
   ColumnOp* colOp = m_colOp[op(colStruct.fCompressionType)];
   for (int i = 0; i < totalRow; i++) {
      curRowId = ridList[i];
      //cout << "processVersionBuffer got rid " << curRowId << endl;
      successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK/width, width, curFbo, curBio);
      if (successFlag) {
         if (curFbo != lastFbo) {
            //cout << "processVersionBuffer is processing lbid  " << lbid << endl;
            RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(
               colStruct.dataOid, colStruct.fColPartition, colStruct.fColSegment, curFbo, lbid));
             //cout << "processVersionBuffer is processing lbid  " << lbid << endl;
             fboList.push_back((uint32_t)curFbo);
             range.start = lbid;
             range.size = 1;
             rangeList.push_back(range);
         }
         lastFbo = curFbo;
      }
   }

//cout << "calling writeVB with blocks " << rangeList.size() << endl;
	std::vector<VBRange> freeList;
   rc = BRMWrapper::getInstance()->
                        writeVB(pFile, verId, colStruct.dataOid, fboList, rangeList, colOp, freeList, colStruct.fColDbRoot);

   return rc;
}

int WriteEngineWrapper::processBeginVBCopy(const TxnID& txnid, const vector<ColStruct>& colStructList, const RIDList& ridList,  
			std::vector<VBRange>& freeList, vector<vector<uint32_t> >& fboLists, vector<vector<LBIDRange> >& rangeLists,
			vector<LBIDRange>&   rangeListTot)
{
   if (idbdatafile::IDBPolicy::useHdfs())
      return 0;

   RID         curRowId;
   int         rc = NO_ERROR;
   int         curFbo = 0, curBio, lastFbo = -1;
   bool        successFlag;
   BRM::LBID_t lbid;
   LBIDRange   range;
  
 //StopWatch timer;
// timer.start("calculation");
   for (uint j=0; j < colStructList.size(); j++)
   {
	vector<uint32_t> fboList;
   vector<LBIDRange>    rangeList;
   lastFbo = -1;
	ColumnOp* colOp = m_colOp[op(colStructList[j].fCompressionType)];
	for (uint i = 0; i < ridList.size(); i++) {
      curRowId = ridList[i];
      //cout << "processVersionBuffer got rid " << curRowId << endl;
      successFlag = colOp->calculateRowId(curRowId, BYTE_PER_BLOCK/colStructList[j].colWidth, colStructList[j].colWidth, curFbo, curBio);
      if (successFlag) {
         if (curFbo != lastFbo) {
            //cout << "processVersionBuffer is processing lbid  " << lbid << endl;
            RETURN_ON_ERROR(BRMWrapper::getInstance()->getBrmInfo(
               colStructList[j].dataOid, colStructList[j].fColPartition, colStructList[j].fColSegment, curFbo, lbid));
             //cout << "beginVBCopy is processing lbid:transaction  " << lbid <<":"<<txnid<< endl;
             fboList.push_back((uint32_t)curFbo);
             range.start = lbid;
             range.size = 1;
             rangeList.push_back(range);
         }
         lastFbo = curFbo;
      }
	}

	BRMWrapper::getInstance()->pruneLBIDList(txnid, &rangeList, &fboList);
	rangeLists.push_back(rangeList);
	
	fboLists.push_back(fboList);
	rangeListTot.insert(rangeListTot.end(), rangeList.begin(), rangeList.end());
   }

for (uint i=0; i < rangeListTot.size(); i++)
{
	cout << "lbid:txnid = " << rangeListTot[i].start <<":"<< txnid << endl;
} 
	if (rangeListTot.size() > 0)
		rc = BRMWrapper::getInstance()->getDbrmObject()->beginVBCopy(txnid, colStructList[0].fColDbRoot, rangeListTot, freeList);
//timer.stop("beginVBCopy");
//timer.finish();
   return rc;
}

	/**
    * @brief Process versioning for batch insert - only version the hwm block.
    */
#if 0
 int WriteEngineWrapper::processBatchVersions(const TxnID& txnid, std::vector<Column> columns, std::vector<BRM::LBIDRange> &  rangeList)
 {
	int rc = 0;
	std::vector<DbFileOp*> fileOps;
	//open the column files
	for ( unsigned i = 0; i < columns.size(); i++)
	{
		ColumnOp* colOp = m_colOp[op(columns[i].compressionType)];
		Column curCol;
		// set params
		colOp->initColumn(curCol);
		ColType colType;
		Convertor::convertColType(columns[i].colDataType, colType); 
		colOp->setColParam(curCol, 0, columns[i].colWidth,
        columns[i].colDataType, colType, columns[i].dataFile.oid,
        columns[i].compressionType,
        columns[i].dataFile.fDbRoot, columns[i].dataFile.fPartition, columns[i].dataFile.fSegment);	
		string segFile;		
		rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);
        if (rc != NO_ERROR)
			break;
		columns[i].dataFile.pFile = curCol.dataFile.pFile;
		fileOps.push_back(colOp);
	}
	 
	if ( rc == 0)
	{
		BRM::VER_t  verId = (BRM::VER_t) txnid;
		rc = BRMWrapper::getInstance()->writeBatchVBs(verId, columns, rangeList, fileOps);
	}
	
	//close files
	for ( unsigned i = 0; i < columns.size(); i++)
	{
		ColumnOp* colOp = dynamic_cast<ColumnOp*> (fileOps[i]);
		Column curCol;
		// set params
		colOp->initColumn(curCol);
		ColType colType;
		Convertor::convertColType(columns[i].colDataType, colType); 
		colOp->setColParam(curCol, 0, columns[i].colWidth,
        columns[i].colDataType, colType, columns[i].dataFile.oid,
        columns[i].compressionType,
        columns[i].dataFile.fDbRoot, columns[i].dataFile.fPartition, columns[i].dataFile.fSegment);	
		curCol.dataFile.pFile = columns[i].dataFile.pFile;
		colOp->clearColumn(curCol);
	}
	return rc;
 }
 #endif
 void WriteEngineWrapper::writeVBEnd(const TxnID& txnid, std::vector<BRM::LBIDRange> &  rangeList)
 {
	if (idbdatafile::IDBPolicy::useHdfs())
		return;

	BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
 }

 int WriteEngineWrapper::updateColumnRec(const TxnID& txnid,
                                      vector<ColStructList>& colExtentsStruct,
                                      ColValueList& colValueList,
                                      vector<void *>& colOldValueList,
                                      vector<RIDList>& ridLists,
                                      vector<DctnryStructList>& dctnryExtentsStruct,
                                      DctnryValueList& dctnryValueList,
									  const int32_t tableOid)
{
   int            rc = 0;
   //RID*           rowIdArray = NULL;
   //RIDList::size_type i;
   unsigned numExtents = colExtentsStruct.size();
  // ColValueList tmpColValueList;
   RIDList::const_iterator ridsIter;
   ColStructList colStructList;
   DctnryStructList dctnryStructList;
   ColumnOp* colOp = NULL;
   
   for (unsigned extent = 0; extent < numExtents; extent++)
   {
      ridsIter = ridLists[extent].begin();

      //rowIdArray = (RID*)calloc(sizeof(RID), ridLists[extent].size());

      colStructList = colExtentsStruct[extent];
      dctnryStructList = dctnryExtentsStruct[extent];
      if (m_opType != DELETE)
      {

/*            ColTuple colTuple;
         ColTupleList colTupleList;
         for (i=0; i < colValueList.size(); i++)
         {
             colTupleList = colValueList[i];
             colTuple = colTupleList[0];
             for (unsigned i = 1; i < ridLists[extent].size(); i++)
             {
                 colTupleList.push_back(colTuple);
             }
             tmpColValueList.push_back(colTupleList);
         }
*/
         //Tokenize data if needed
         vector<Token> tokenList;

         DctColTupleList::iterator dctCol_iter;
         ColTupleList::iterator col_iter;
         for (unsigned i = 0; i < colStructList.size(); i++)
         {
            if (colStructList[i].tokenFlag)
            {
               // only need to tokenize once
               dctCol_iter = dctnryValueList[i].begin();
               //col_iter = colValueList[i].begin();
               Token token;
               if (!dctCol_iter->isNull)
               {
                  RETURN_ON_ERROR(tokenize(
                     txnid, dctnryStructList[i], *dctCol_iter));
                  token = dctCol_iter->token;

#ifdef PROFILE
//timer.stop("tokenize");
#endif
               }
			   else
			   {
					//if (dctnryStructList[i].dctnryOid == 2001)
					//	std::cout << " got null token for string " << dctCol_iter->sigValue <<std::endl;
			   }
			   //if (dctnryStructList[i].dctnryOid == 2001)
				//std::cout << " got token for string " << dctCol_iter->sigValue << " op:fbo = " << token.op <<":"<<token.fbo << std::endl;
               tokenList.push_back(token);
            }
         }

         int dicPos = 0;
         for (unsigned i = 0; i < colStructList.size(); i++)
         {
            if (colStructList[i].tokenFlag)
            {
               // only need to tokenize once
               col_iter = colValueList[i].begin();
               while (col_iter != colValueList[i].end())
               {
                  col_iter->data = tokenList[dicPos];
                  col_iter++;
               }
               dicPos++;
            }
         }
      }
      RIDList::iterator rid_iter;
/*    i = 0;
      while (rid_iter != ridLists[extent].end())
      {
         rowIdArray[i] = *rid_iter;
         rid_iter++;
         i++;
      }
*/
      //Mark extents invalid
	//if (colStructList[0].dataOid < 3000) {
      vector<BRM::LBID_t> lbids;
      vector<CalpontSystemCatalog::ColDataType> colDataTypes;
      bool successFlag = true;
      unsigned width = 0;
      int      curFbo = 0, curBio, lastFbo = -1;
      rid_iter = ridLists[extent].begin();
      RID aRid = *rid_iter;
      for (unsigned j = 0; j< colStructList.size(); j++)
      {
         colOp = m_colOp[op(colStructList[j].fCompressionType)];
         if (colStructList[j].tokenFlag)
             continue;

         width = colOp->getCorrectRowWidth(colStructList[j].colDataType, colStructList[j].colWidth);
         successFlag = colOp->calculateRowId(aRid , BYTE_PER_BLOCK/width, width, curFbo, curBio);
         if (successFlag) 
         {
            if (curFbo != lastFbo)
            {
                RETURN_ON_ERROR(AddLBIDtoList(txnid,
                                              lbids,
                                              colDataTypes,
                                              colStructList[j],
                                              curFbo));
            }
         }
      }
      //cout << "lbids size = " << lbids.size()<< endl;
//#ifdef PROFILE
//timer.start("markExtentsInvalid");
//#endif
      if (lbids.size() > 0)
         rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);
	//}
	
	  if ( m_opType != DELETE)
		m_opType = UPDATE;
      rc = writeColumnRec(txnid, colStructList, colValueList, colOldValueList,
                          ridLists[extent], tableOid, true, ridLists[extent].size());

//    if (rowIdArray)
//       free(rowIdArray);
	
	m_opType = NOOP;
	if (rc != NO_ERROR)
		break;
   }

   return rc;
}

int WriteEngineWrapper::updateColumnRecs(const TxnID& txnid,
                                         vector<ColStruct>& colExtentsStruct,
                                         ColValueList& colValueList,
                                         const RIDList& ridLists,
										 const int32_t tableOid)
{
    //Mark extents invalid
    //int rc = 0;
    //if (colExtentsStruct[0].dataOid < 3000) 
    //{
    vector<BRM::LBID_t> lbids;
    vector<CalpontSystemCatalog::ColDataType> colDataTypes;
    ColumnOp* colOp = NULL;
    bool successFlag = true;
    unsigned width = 0;\
    int curFbo = 0, curBio, lastFbo = -1; 
    RID aRid = ridLists[0];
    int rc = 0;

    for (unsigned j = 0; j< colExtentsStruct.size(); j++)
    {
        colOp = m_colOp[op(colExtentsStruct[j].fCompressionType)];
        if (colExtentsStruct[j].tokenFlag)
            continue;

        width = colOp->getCorrectRowWidth(colExtentsStruct[j].colDataType, colExtentsStruct[j].colWidth);
        successFlag = colOp->calculateRowId(aRid , BYTE_PER_BLOCK/width, width, curFbo, curBio);
        if (successFlag)
        {
            if (curFbo != lastFbo)
            {
                RETURN_ON_ERROR(AddLBIDtoList(txnid,
                                              lbids,
                                              colDataTypes,
                                              colExtentsStruct[j],
                                              curFbo));
            }
        }
    }

    if (lbids.size() > 0)
    {
//        cout << "BRMWrapper::getInstance()->markExtentsInvalid(lbids); " << lbids.size() << " lbids" << endl;
        rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);
    }
    //}	 
	if ( m_opType != DELETE)
		m_opType = UPDATE;
    rc = writeColumnRecords (txnid, colExtentsStruct, colValueList, ridLists, tableOid);
	m_opType = NOOP;
    return rc;
}

int WriteEngineWrapper::writeColumnRecords(const TxnID& txnid,
                                           vector<ColStruct>& colStructList,
                                           ColValueList& colValueList,
                                           const RIDList& ridLists, const int32_t tableOid, bool versioning)
{
   bool           bExcp;
   int            rc = 0;
   void*          valArray = NULL;
   Column         curCol;
   ColStruct      curColStruct;
   ColTupleList   curTupleList;
   ColStructList::size_type  totalColumn;
   ColStructList::size_type  i;
   ColTupleList::size_type   totalRow;
   setTransId(txnid);
   totalColumn = colStructList.size();
   totalRow = ridLists.size();

   TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
   for (i = 0; i < totalColumn; i++)
   {
      valArray = NULL;
      curColStruct = colStructList[i];
      curTupleList = colValueList[i];
      ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

      Convertor::convertColType(&curColStruct);

      // set params
      colOp->initColumn(curCol);

      colOp->setColParam(curCol, 0, curColStruct.colWidth,
         curColStruct.colDataType, curColStruct.colType, curColStruct.dataOid,
         curColStruct.fCompressionType,
         curColStruct.fColDbRoot, curColStruct.fColPartition, curColStruct.fColSegment);
		 
	  ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
	  ColExtsInfo::iterator it = aColExtsInfo.begin();
	  while (it != aColExtsInfo.end())
	  {
		if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) && (it->segNum == curColStruct.fColSegment))
			break;
		it++;
	  }

	  if (it == aColExtsInfo.end()) //add this one to the list
	  {
		ColExtInfo aExt;
		aExt.dbRoot =curColStruct.fColDbRoot;
		aExt.partNum = curColStruct.fColPartition;
		aExt.segNum = curColStruct.fColSegment;
		aExt.compType = curColStruct.fCompressionType;	
		aExt.isDict = false;		
		aColExtsInfo.push_back(aExt);
		aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
	  }
	
      string segFile;
      rc = colOp->openColumnFile(curCol, segFile);
      if (rc != NO_ERROR)
         break;
	  vector<LBIDRange>   rangeList;
	  if (versioning) {
			rc = processVersionBuffers(curCol.dataFile.pFile, txnid, curColStruct,
                                 curColStruct.colWidth, totalRow, ridLists, rangeList);
	  }

      if (rc != NO_ERROR) {
		if (curColStruct.fCompressionType == 0)
		{
			curCol.dataFile.pFile->flush();
		}
    	BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
        break;
      }

      switch (curColStruct.colType)
      {
         case WriteEngine::WR_INT:
            valArray = (int*) calloc(sizeof(int), totalRow);
            break;
         case WriteEngine::WR_UINT:
            valArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow);
            break;
         case WriteEngine::WR_VARBINARY : // treat same as char for now
         case WriteEngine::WR_CHAR:
         case WriteEngine::WR_BLOB:
            valArray = (char*) calloc(sizeof(char), totalRow * MAX_COLUMN_BOUNDARY);
            break;
         case WriteEngine::WR_FLOAT:
            valArray = (float*) calloc(sizeof(float), totalRow);
            break;
         case WriteEngine::WR_DOUBLE:
            valArray = (double*) calloc(sizeof(double), totalRow);
            break;
         case WriteEngine::WR_BYTE:
            valArray = (char*) calloc(sizeof(char), totalRow);
            break;
         case WriteEngine::WR_UBYTE:
            valArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow);
            break;
         case WriteEngine::WR_SHORT:
            valArray = (short*) calloc(sizeof(short), totalRow);
            break;
         case WriteEngine::WR_USHORT:
            valArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow);
            break;
         case WriteEngine::WR_LONGLONG:
            valArray = (long long*) calloc(sizeof(long long), totalRow);
            break;
         case WriteEngine::WR_ULONGLONG:
            valArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow);
            break;
         case WriteEngine::WR_TOKEN:
            valArray = (Token*) calloc(sizeof(Token), totalRow);
            break;
      }

      // convert values to valArray
      bExcp = false;
      try {
         convertValArray(totalRow, curColStruct.colType, curTupleList, valArray);
      }
      catch(...) {
         bExcp = true;
      }
      if (bExcp) {
    	 BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
         return ERR_PARSING;
      }
#ifdef PROFILE
timer.start("writeRow ");
#endif
      rc = colOp->writeRowsValues(curCol, totalRow, ridLists, valArray);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
      colOp->clearColumn(curCol);
	  if (curColStruct.fCompressionType == 0)
	  {
		std::vector<BRM::FileInfo> files;
		BRM::FileInfo aFile;			
		aFile.partitionNum = curColStruct.fColPartition;
		aFile.dbRoot =curColStruct.fColDbRoot;;
		aFile.segmentNum = curColStruct.fColSegment;
		aFile.compType = curColStruct.fCompressionType;
		files.push_back(aFile);
		if (idbdatafile::IDBPolicy::useHdfs())
			cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
	  }
	  BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
      if (valArray != NULL)
         free(valArray);

      // check error
      if (rc != NO_ERROR)
         break;
   }

   return rc;
}

/*@brief writeColumnRec - Write values to a column
*/
/***********************************************************
 * DESCRIPTION:
 *    Write values to a column
 * PARAMETERS:
 *    tableOid - table object id
 *    colStructList - column struct list
 *    colValueList - column value list
 *    colNewStructList - the new extent struct list
 *    colNewValueList - column value list for the new extent
 *    rowIdArray -  row id list
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::writeColumnRec(const TxnID& txnid,
                                       const ColStructList& colStructList,
                                       const ColValueList& colValueList,
                                       RID* rowIdArray,
                                       const ColStructList& newColStructList,
                                       const ColValueList& newColValueList,
									   const int32_t tableOid,
									   bool versioning)
{
   bool           bExcp;
   int            rc = 0;
   void*          valArray;
   void*          oldValArray = NULL;
   string         segFile;
   Column         curCol;
   ColStruct      curColStruct;
   ColTupleList   curTupleList, oldTupleList;
   ColStructList::size_type  totalColumn;
   ColStructList::size_type  i;
   ColTupleList::size_type   totalRow1, totalRow2;

   setTransId(txnid);

   totalColumn = colStructList.size();
#ifdef PROFILE
StopWatch timer;
#endif
   if (newColValueList.size() > 0)
   {
       curTupleList = static_cast<ColTupleList>(colValueList[0]);
       totalRow1 = curTupleList.size();
       curTupleList = static_cast<ColTupleList>(newColValueList[0]);
       totalRow2 = curTupleList.size();
   }
   else
   {
       curTupleList = static_cast<ColTupleList>(colValueList[0]);
       totalRow1 = curTupleList.size();
       totalRow2 = 0;
   }

	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
   for (i = 0; i < totalColumn; i++) {
      if (totalRow2 > 0)
      {
         RID * secondPart = rowIdArray + totalRow1;
         //@Bug 2205 Check if all rows go to the new extent
         if (totalRow1 > 0)
         {
            //Write the first batch
            valArray = NULL;
            RID * firstPart = rowIdArray;
            curColStruct = colStructList[i];
            curTupleList = colValueList[i];
            ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

            // set params
            colOp->initColumn(curCol);
            // need to pass real dbRoot, partition, and segment to setColParam
            colOp->setColParam(curCol, 0, curColStruct.colWidth,
            curColStruct.colDataType, curColStruct.colType, curColStruct.dataOid,
            curColStruct.fCompressionType, curColStruct.fColDbRoot,
            curColStruct.fColPartition, curColStruct.fColSegment);
			
			ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
			ColExtsInfo::iterator it = aColExtsInfo.begin();
			while (it != aColExtsInfo.end())
			{
				if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) && (it->segNum == curColStruct.fColSegment))
					break;
				it++;
			}

			if (it == aColExtsInfo.end()) //add this one to the list
			{
				ColExtInfo aExt;
				aExt.dbRoot =curColStruct.fColDbRoot;
				aExt.partNum = curColStruct.fColPartition;
				aExt.segNum = curColStruct.fColSegment;
				aExt.compType = curColStruct.fCompressionType;		
				aColExtsInfo.push_back(aExt);
				aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
			}

            rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);
            if (rc != NO_ERROR)
               break;

            // handling versioning
			vector<LBIDRange>   rangeList;
			if (versioning)
			{
					rc = processVersionBuffer(curCol.dataFile.pFile, txnid, curColStruct,
                                      curColStruct.colWidth, totalRow1, firstPart, rangeList);
				if (rc != NO_ERROR) {
					if (curColStruct.fCompressionType == 0)
					{
						curCol.dataFile.pFile->flush();
					}
	
					BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
					break;
				}
            }

            //totalRow1 -= totalRow2;
            // have to init the size here
            // nullArray = (bool*) malloc(sizeof(bool) * totalRow);
            switch (curColStruct.colType)
            {
               case WriteEngine::WR_INT:
                  valArray = (int*) calloc(sizeof(int), totalRow1);
                  oldValArray = (int*) calloc(sizeof(int), totalRow1);
                  break;
               case WriteEngine::WR_UINT:
                  valArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow1);
                  oldValArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow1);
                  break;
               case WriteEngine::WR_VARBINARY : // treat same as char for now
               case WriteEngine::WR_CHAR:
               case WriteEngine::WR_BLOB:
                  valArray = (char*) calloc(sizeof(char), totalRow1 * MAX_COLUMN_BOUNDARY);
                  oldValArray = (char*) calloc(sizeof(char), totalRow1 * MAX_COLUMN_BOUNDARY);
                  break;
//             case WriteEngine::WR_LONG:
//                valArray = (long*) calloc(sizeof(long), totalRow1);
//                break;
               case WriteEngine::WR_FLOAT:
                  valArray = (float*) calloc(sizeof(float), totalRow1);
                  oldValArray = (float*) calloc(sizeof(float), totalRow1);
                  break;
               case WriteEngine::WR_DOUBLE:
                  valArray = (double*) calloc(sizeof(double), totalRow1);
                  oldValArray = (double*) calloc(sizeof(double), totalRow1);
                  break;
//             case WriteEngine::WR_BIT:
//                valArray = (bool*) calloc(sizeof(bool), totalRow1);
//                break;
               case WriteEngine::WR_BYTE:
                  valArray = (char*) calloc(sizeof(char), totalRow1);
                  oldValArray = (char*) calloc(sizeof(char), totalRow1);
                  break;
               case WriteEngine::WR_UBYTE:
                  valArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow1);
                  oldValArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow1);
                  break;
               case WriteEngine::WR_SHORT:
                  valArray = (short*) calloc(sizeof(short), totalRow1);
                  oldValArray = (short*) calloc(sizeof(short), totalRow1);
                  break;
               case WriteEngine::WR_USHORT:
                  valArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow1);
                  oldValArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow1);
                  break;
               case WriteEngine::WR_LONGLONG:
                  valArray = (long long*) calloc(sizeof(long long), totalRow1);
                  oldValArray = (long long*) calloc(sizeof(long long), totalRow1);
                  break;
               case WriteEngine::WR_ULONGLONG:
                  valArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow1);
                  oldValArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow1);
                  break;
               case WriteEngine::WR_TOKEN:
                  valArray = (Token*) calloc(sizeof(Token), totalRow1);
                  oldValArray = (Token*) calloc(sizeof(Token), totalRow1);
                  break;
            }

            // convert values to valArray
            if (m_opType != DELETE) {
               bExcp = false;
               try {
                  convertValArray(totalRow1, curColStruct.colType, curTupleList, valArray);
               }
               catch(...) {
                  bExcp = true;
               }
               if (bExcp) {
				  if (versioning)
						BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
                  return ERR_PARSING;
               }
#ifdef PROFILE
timer.start("writeRow ");
#endif
               rc = colOp->writeRow(curCol, totalRow1, firstPart, valArray, oldValArray);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
            }
            else
            {
#ifdef PROFILE
timer.start("writeRow ");
#endif
               rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray, true);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
            }

            // clean
            curTupleList.clear();

            colOp->clearColumn(curCol);
			
			if (versioning)
				BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
				
            if (valArray != NULL)
               free(valArray);

            if (oldValArray != NULL)
               free(oldValArray);

            // check error
            if (rc != NO_ERROR)
               break;
         }
         //Process the second batch
         valArray = NULL;

         curColStruct = newColStructList[i];
         curTupleList = newColValueList[i];
         ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

         // set params
         colOp->initColumn(curCol);
         colOp->setColParam(curCol, 0, curColStruct.colWidth,
            curColStruct.colDataType, curColStruct.colType, curColStruct.dataOid,
            curColStruct.fCompressionType, curColStruct.fColDbRoot,
            curColStruct.fColPartition, curColStruct.fColSegment);

		ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
		ColExtsInfo::iterator it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) && (it->segNum == curColStruct.fColSegment))
				break;
			it++;
		}

		if (it == aColExtsInfo.end()) //add this one to the list
		{
			ColExtInfo aExt;
			aExt.dbRoot =curColStruct.fColDbRoot;
			aExt.partNum = curColStruct.fColPartition;
			aExt.segNum = curColStruct.fColSegment;
			aExt.compType = curColStruct.fCompressionType;		
			aColExtsInfo.push_back(aExt);
			aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
		}
         rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);
         if (rc != NO_ERROR)
             break;

         // handling versioning
		 vector<LBIDRange>   rangeList;
		 if (versioning)
		 {
			rc = processVersionBuffer(curCol.dataFile.pFile, txnid, curColStruct,
                                   curColStruct.colWidth, totalRow2, secondPart, rangeList);
			if (rc != NO_ERROR) {
				if (curColStruct.fCompressionType == 0)
				{
					curCol.dataFile.pFile->flush();
				}
				BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
				break;
			}
		 }

         //totalRow1 -= totalRow2;
         // have to init the size here
//       nullArray = (bool*) malloc(sizeof(bool) * totalRow);
         switch (curColStruct.colType)
         {
            case WriteEngine::WR_INT:
               valArray = (int*) calloc(sizeof(int), totalRow2);
               oldValArray = (int*) calloc(sizeof(int), totalRow2);
               break;
            case WriteEngine::WR_UINT:
               valArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow2);
               oldValArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow2);
               break;
            case WriteEngine::WR_VARBINARY : // treat same as char for now
            case WriteEngine::WR_CHAR:
            case WriteEngine::WR_BLOB:
               valArray = (char*) calloc(sizeof(char), totalRow2 * MAX_COLUMN_BOUNDARY);
               oldValArray = (char*) calloc(sizeof(char), totalRow2 * MAX_COLUMN_BOUNDARY);
               break;
//          case WriteEngine::WR_LONG:
//             valArray = (long*) calloc(sizeof(long), totalRow);
//             break;
            case WriteEngine::WR_FLOAT:
               valArray = (float*) calloc(sizeof(float), totalRow2);
               oldValArray = (float*) calloc(sizeof(float), totalRow2);
               break;
            case WriteEngine::WR_DOUBLE:
               valArray = (double*) calloc(sizeof(double), totalRow2);
               oldValArray = (double*) calloc(sizeof(double), totalRow2);
               break;
//          case WriteEngine::WR_BIT:
//             valArray = (bool*) calloc(sizeof(bool), totalRow);
//             break;
            case WriteEngine::WR_BYTE:
               valArray = (char*) calloc(sizeof(char), totalRow2);
               oldValArray = (char*) calloc(sizeof(char), totalRow2);
               break;
            case WriteEngine::WR_UBYTE:
               valArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow2);
               oldValArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow2);
               break;
            case WriteEngine::WR_SHORT:
               valArray = (short*) calloc(sizeof(short), totalRow2);
               oldValArray = (short*) calloc(sizeof(short), totalRow2);
               break;
            case WriteEngine::WR_USHORT:
               valArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow2);
               oldValArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow2);
               break;
            case WriteEngine::WR_LONGLONG:
               valArray = (long long*) calloc(sizeof(long long), totalRow2);
               oldValArray = (long long*) calloc(sizeof(long long), totalRow2);
               break;
            case WriteEngine::WR_ULONGLONG:
               valArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow2);
               oldValArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow2);
               break;
            case WriteEngine::WR_TOKEN:
               valArray = (Token*) calloc(sizeof(Token), totalRow2);
               oldValArray = (Token*) calloc(sizeof(Token), totalRow2);
               break;
         }

         // convert values to valArray
         if (m_opType != DELETE) {
            bExcp = false;
            try {
               convertValArray(totalRow2, curColStruct.colType, curTupleList, valArray);
            }
            catch(...) {
               bExcp = true;
            }
            if (bExcp) {
			   if (versioning)
					BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
               return ERR_PARSING;
            }
#ifdef PROFILE
timer.start("writeRow ");
#endif
            rc = colOp->writeRow(curCol, totalRow2, secondPart, valArray, oldValArray);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
         }
         else
         {
#ifdef PROFILE
timer.start("writeRow ");
#endif
            rc = colOp->writeRow(curCol, totalRow2, rowIdArray, valArray, oldValArray, true);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
         }

         // clean
         curTupleList.clear();

         colOp->clearColumn(curCol);
		 if (versioning)
			BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
			
         if (valArray != NULL)
            free(valArray);

         if (oldValArray != NULL)
            free(oldValArray);

         // check error
         if (rc != NO_ERROR)
            break;
      }
      else
      {
         valArray = NULL;

         curColStruct = colStructList[i];
         curTupleList = colValueList[i];
         ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];

         // set params
         colOp->initColumn(curCol);
         colOp->setColParam(curCol, 0, curColStruct.colWidth,
            curColStruct.colDataType, curColStruct.colType, curColStruct.dataOid,
            curColStruct.fCompressionType, curColStruct.fColDbRoot,
            curColStruct.fColPartition, curColStruct.fColSegment);

         rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);
		  //cout << " Opened file oid " << curCol.dataFile.pFile << endl;
         if (rc != NO_ERROR)
            break;

		ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
			ColExtsInfo::iterator it = aColExtsInfo.begin();
			while (it != aColExtsInfo.end())
			{
				if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) && (it->segNum == curColStruct.fColSegment))
					break;
				it++;
			}

			if (it == aColExtsInfo.end()) //add this one to the list
			{
				ColExtInfo aExt;
				aExt.dbRoot =curColStruct.fColDbRoot;
				aExt.partNum = curColStruct.fColPartition;
				aExt.segNum = curColStruct.fColSegment;
				aExt.compType = curColStruct.fCompressionType;		
				aColExtsInfo.push_back(aExt);
				aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
			}
         // handling versioning
		 vector<LBIDRange>   rangeList;
		 if (versioning)
		 {
			rc = processVersionBuffer(curCol.dataFile.pFile, txnid, curColStruct,
                                   curColStruct.colWidth, totalRow1, rowIdArray, rangeList);
				if (rc != NO_ERROR) {
					if (curColStruct.fCompressionType == 0)
					{
						curCol.dataFile.pFile->flush();
					}
					BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
				break;
			}
		}

         // have to init the size here
//       nullArray = (bool*) malloc(sizeof(bool) * totalRow);
         switch (curColStruct.colType)
         {
            case WriteEngine::WR_INT:
               valArray = (int*) calloc(sizeof(int), totalRow1);
               oldValArray = (int*) calloc(sizeof(int), totalRow1);
               break;
            case WriteEngine::WR_UINT:
               valArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow1);
               oldValArray = (uint32_t*) calloc(sizeof(uint32_t), totalRow1);
               break;
            case WriteEngine::WR_VARBINARY : // treat same as char for now
            case WriteEngine::WR_CHAR:
            case WriteEngine::WR_BLOB:
               valArray = (char*) calloc(sizeof(char), totalRow1 * MAX_COLUMN_BOUNDARY);
               oldValArray = (char*) calloc(sizeof(char), totalRow1 * MAX_COLUMN_BOUNDARY);
               break;
//          case WriteEngine::WR_LONG:
//             valArray = (long*) calloc(sizeof(long), totalRow1);
//             break;
            case WriteEngine::WR_FLOAT:
               valArray = (float*) calloc(sizeof(float), totalRow1);
               oldValArray = (float*) calloc(sizeof(float), totalRow1);
               break;
            case WriteEngine::WR_DOUBLE:
               valArray = (double*) calloc(sizeof(double), totalRow1);
               oldValArray = (double*) calloc(sizeof(double), totalRow1);
               break;
//          case WriteEngine::WR_BIT:
//             valArray = (bool*) calloc(sizeof(bool), totalRow1);
//             break;
            case WriteEngine::WR_BYTE:
               valArray = (char*) calloc(sizeof(char), totalRow1);
               oldValArray = (char*) calloc(sizeof(char), totalRow1);
               break;
            case WriteEngine::WR_UBYTE:
               valArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow1);
               oldValArray = (uint8_t*) calloc(sizeof(uint8_t), totalRow1);
               break;
            case WriteEngine::WR_SHORT:
               valArray = (short*) calloc(sizeof(short), totalRow1);
               oldValArray = (short*) calloc(sizeof(short), totalRow1);
               break;
            case WriteEngine::WR_USHORT:
               valArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow1);
               oldValArray = (uint16_t*) calloc(sizeof(uint16_t), totalRow1);
               break;
            case WriteEngine::WR_LONGLONG:
               valArray = (long long*) calloc(sizeof(long long), totalRow1);
               oldValArray = (long long*) calloc(sizeof(long long), totalRow1);
               break;
            case WriteEngine::WR_ULONGLONG:
               valArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow1);
               oldValArray = (uint64_t*) calloc(sizeof(uint64_t), totalRow1);
               break;
            case WriteEngine::WR_TOKEN:
               valArray = (Token*) calloc(sizeof(Token), totalRow1);
               oldValArray = (Token*) calloc(sizeof(Token), totalRow1);
               break;
         }

         // convert values to valArray
         if (m_opType != DELETE) {
            bExcp = false;
            try {
              convertValArray(totalRow1, curColStruct.colType, curTupleList, valArray);
            }
            catch(...) {
               bExcp = true;
            }
            if (bExcp) {
				if (versioning)
					BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
                return ERR_PARSING;
            }
#ifdef PROFILE
timer.start("writeRow ");
#endif
            rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
         }
         else
         {
#ifdef PROFILE
timer.start("writeRow ");
#endif
         rc = colOp->writeRow(curCol, totalRow1, rowIdArray, valArray, oldValArray, true);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
         }

         // clean
         curTupleList.clear();
		//cout << " close file oid " << curCol.dataFile.pFile << endl;
         colOp->clearColumn(curCol);
		 
		 if (versioning)
			BRMWrapper::getInstance()->writeVBEnd(txnid, rangeList);
         if (valArray != NULL)
            free(valArray);

         if (oldValArray != NULL)
            free(oldValArray);

         // check error
         if (rc != NO_ERROR)
            break;
      }
   } // end of for (i = 0

#ifdef PROFILE
timer.finish();
#endif
   return rc;
}

int WriteEngineWrapper::writeColumnRec(const TxnID& txnid,
                                       const ColStructList& colStructList,
                                       const ColValueList& colValueList,
                                       vector<void *>& colOldValueList,
                                       const RIDList& ridList,
									   const int32_t tableOid,
                                       bool convertStructFlag,
                                       ColTupleList::size_type nRows)
{
   bool           bExcp;
   int            rc = 0;
   void*          valArray = NULL;
   Column         curCol;
   ColStruct      curColStruct;
   ColTupleList   curTupleList, oldTupleList;
   ColStructList::size_type  totalColumn;
   ColStructList::size_type  i;
   ColTupleList::size_type   totalRow;

   setTransId(txnid);
   colOldValueList.clear();
   totalColumn = colStructList.size();
   totalRow = nRows;

#ifdef PROFILE
StopWatch timer;
#endif

    vector<LBIDRange>   rangeListTot;
	std::vector<VBRange> freeList;
	vector<vector<uint32_t> > fboLists;
	vector<vector<LBIDRange> > rangeLists;
		
	rc = processBeginVBCopy(txnid, colStructList, ridList, freeList, fboLists, rangeLists, rangeListTot);
	if (rc != NO_ERROR) 
	{
		if (rangeListTot.size() > 0)
			BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);
		return rc;
	}
	
	VBRange aRange;
	uint blocksProcessedThisOid = 0;
	uint blocksProcessed = 0;
	std::vector<BRM::FileInfo> files;
	TableMetaData* aTbaleMetaData = TableMetaData::makeTableMetaData(tableOid);
   for (i = 0; i < totalColumn; i++)
   {
      valArray = NULL;
      curColStruct = colStructList[i];
      curTupleList = colValueList[i]; //same value for all rows
      ColumnOp* colOp = m_colOp[op(curColStruct.fCompressionType)];
      // convert column data type
      if (convertStructFlag)
         Convertor::convertColType(&curColStruct);

      // set params
      colOp->initColumn(curCol);
      colOp->setColParam(curCol, 0, curColStruct.colWidth,
         curColStruct.colDataType, curColStruct.colType, curColStruct.dataOid,
         curColStruct.fCompressionType, curColStruct.fColDbRoot,
         curColStruct.fColPartition, curColStruct.fColSegment);
		
	  
		ColExtsInfo aColExtsInfo = aTbaleMetaData->getColExtsInfo(curColStruct.dataOid);
		ColExtsInfo::iterator it = aColExtsInfo.begin();
		while (it != aColExtsInfo.end())
		{
			if ((it->dbRoot == curColStruct.fColDbRoot) && (it->partNum == curColStruct.fColPartition) && (it->segNum == curColStruct.fColSegment))
				break;
			it++;
		}

		if (it == aColExtsInfo.end()) //add this one to the list
		{
			ColExtInfo aExt;
			aExt.dbRoot =curColStruct.fColDbRoot;
			aExt.partNum = curColStruct.fColPartition;
			aExt.segNum = curColStruct.fColSegment;
			aExt.compType = curColStruct.fCompressionType;		
			aColExtsInfo.push_back(aExt);
			aTbaleMetaData->setColExtsInfo(colStructList[i].dataOid, aColExtsInfo);
		}
	 
      string segFile;
      rc = colOp->openColumnFile(curCol, segFile, IO_BUFF_SIZE);
      if (rc != NO_ERROR)
         break;
	  if (curColStruct.fCompressionType == 0)
	  {
		BRM::FileInfo aFile;	
		aFile.oid = curColStruct.dataOid;		
		aFile.partitionNum = curColStruct.fColPartition;
		aFile.dbRoot =curColStruct.fColDbRoot;;
		aFile.segmentNum = curColStruct.fColSegment;
		aFile.compType = curColStruct.fCompressionType;
		files.push_back(aFile); 
	  }
      // handling versioning
      //cout << " pass to processVersionBuffer rid " << rowIdArray[0] << endl;
      //cout << "dataOid:fColPartition = " << curColStruct.dataOid << ":" << curColStruct.fColPartition << endl;
//timer.start("processVersionBuffers");
	  //vector<LBIDRange>   rangeList;
     // rc = processVersionBuffers(curCol.dataFile.pFile, txnid, curColStruct, curColStruct.colWidth, totalRow, ridList, rangeList);
	 std::vector<VBRange> curFreeList;
	 uint blockUsed = 0;
	 if (!idbdatafile::IDBPolicy::useHdfs()) {
	if (rangeListTot.size() > 0) {	
		if (freeList[0].size >= (blocksProcessed + rangeLists[i].size()))
		{
		aRange.vbOID = freeList[0].vbOID;
		aRange.vbFBO = freeList[0].vbFBO + blocksProcessed;
		aRange.size = rangeLists[i].size();
		curFreeList.push_back(aRange);
		//cout << "range size = " << aRange.size <<" and blocksProcessed = " << blocksProcessed<< endl;
		}
		else
		{
		aRange.vbOID = freeList[0].vbOID;
		aRange.vbFBO = freeList[0].vbFBO + blocksProcessed;
		aRange.size = freeList[0].size - blocksProcessed;
		blockUsed = aRange.size;
		curFreeList.push_back(aRange);
		if (freeList.size() > 1)
		{
			aRange.vbOID = freeList[1].vbOID;
			aRange.vbFBO = freeList[1].vbFBO + blocksProcessedThisOid;
			aRange.size = rangeLists[i].size()-blockUsed;
			curFreeList.push_back(aRange);
			blocksProcessedThisOid += aRange.size;
		}
		else
		{
			rc = 1;
			break;
		}
		//cout << "curFreeList size = " << curFreeList.size() << endl;
		
		}
	 
		blocksProcessed += rangeLists[i].size();

        //timer.start("Delete:writeVB");
        rc = BRMWrapper::getInstance()->
                       writeVB(curCol.dataFile.pFile, (BRM::VER_t)txnid,
                       curColStruct.dataOid, fboLists[i], rangeLists[i],
                       colOp, curFreeList, curColStruct.fColDbRoot, true);
	    }
	}
	 //timer.stop("Delete:writeVB");	
//timer.stop("processVersionBuffers");
      // cout << " rc for processVersionBuffer is " << rc << endl;
      if (rc != NO_ERROR) {
    	  if (curColStruct.fCompressionType == 0)
    	  {
    		  curCol.dataFile.pFile->flush();
    	  }
		 if (rangeListTot.size() > 0)
			BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);
         break;
      }

      switch (curColStruct.colType)
      {
         case WriteEngine::WR_INT:
            valArray = (int*) calloc(sizeof(int), 1);
            break;
         case WriteEngine::WR_UINT:
            valArray = (uint32_t*) calloc(sizeof(uint32_t), 1);
            break;
         case WriteEngine::WR_VARBINARY : // treat same as char for now
         case WriteEngine::WR_CHAR:
         case WriteEngine::WR_BLOB:
            valArray = (char*) calloc(sizeof(char), 1 * MAX_COLUMN_BOUNDARY);
            break;
         case WriteEngine::WR_FLOAT:
            valArray = (float*) calloc(sizeof(float), 1);
            break;
         case WriteEngine::WR_DOUBLE:
            valArray = (double*) calloc(sizeof(double), 1);
            break;
         case WriteEngine::WR_BYTE:
            valArray = (char*) calloc(sizeof(char), 1);
            break;
         case WriteEngine::WR_UBYTE:
            valArray = (uint8_t*) calloc(sizeof(uint8_t), 1);
            break;
         case WriteEngine::WR_SHORT:
            valArray = (short*) calloc(sizeof(short), 1);
            break;
         case WriteEngine::WR_USHORT:
            valArray = (uint16_t*) calloc(sizeof(uint16_t), 1);
            break;
         case WriteEngine::WR_LONGLONG:
            valArray = (long long*) calloc(sizeof(long long), 1);
            break;
         case WriteEngine::WR_ULONGLONG:
            valArray = (uint64_t*) calloc(sizeof(uint64_t), 1);
            break;
         case WriteEngine::WR_TOKEN:
            valArray = (Token*) calloc(sizeof(Token), 1);
            break;
      }

      // convert values to valArray
      if (m_opType != DELETE) {
         bExcp = false;
         ColTuple    curTuple;
         curTuple = curTupleList[0];

         try {
            convertValue(curColStruct.colType, valArray, curTuple.data);
         }
         catch(...) {
             bExcp = true;
         }
         if (bExcp) {
			if (rangeListTot.size() > 0)
				BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);
            return ERR_PARSING;
         }
#ifdef PROFILE
timer.start("writeRow ");
#endif
         rc = colOp->writeRows(curCol, totalRow, ridList, valArray);
#ifdef PROFILE
timer.stop("writeRow ");
#endif
      }
      else
      {
#ifdef PROFILE
timer.start("writeRows ");
#endif
         rc = colOp->writeRows(curCol, totalRow, ridList, valArray, 0, true);
#ifdef PROFILE
timer.stop("writeRows ");
#endif
      }

 //     colOldValueList.push_back(oldValArray);
 //timer.start("Delete:closefile");	
      colOp->clearColumn(curCol);
//timer.stop("Delete:closefile");	  
      if (valArray != NULL)
         free(valArray);

      // check error
      if (rc != NO_ERROR)
         break;

   } // end of for (i = 0)
// timer.start("Delete:purgePrimProcFdCache");	
if ((idbdatafile::IDBPolicy::useHdfs()) && (files.size() > 0))
	cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID());
//if (idbdatafile::IDBPolicy::useHdfs())
//			cacheutils::dropPrimProcFdCache();
//timer.stop("Delete:purgePrimProcFdCache");
if (rangeListTot.size() > 0)	
	BRMWrapper::getInstance()->writeVBEnd(txnid, rangeListTot);
//timer.stop("Delete:writecolrec");	
//#ifdef PROFILE
//timer.finish();
//#endif
   return rc;
}

/*@brief tokenize - return a token for a given signature and size
*/
/***********************************************************
 * DESCRIPTION:
 *  return a token for a given signature and size
 *  If it is not in the dictionary, the signature
 *  will be added to the dictionary and the index tree
 *  If it is already in the dictionary, then
 *  the token will be returned
 *  This function does not open and close files.
 *  users need to use openDctnry and CloseDctnry
 * PARAMETERS:
 *  DctnryTuple& dctnryTuple - holds the sigValue, sigSize and token
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::tokenize(const TxnID& txnid, DctnryTuple& dctnryTuple, int ct)
{
  int cop = op(ct);
  m_dctnry[cop]->setTransId(txnid);
  //cout << "Tokenizing dctnryTuple.sigValue " << dctnryTuple.sigValue << endl;
  return m_dctnry[cop]->updateDctnry(dctnryTuple.sigValue, dctnryTuple.sigSize, dctnryTuple.token);
}

/*@brief tokenize - return a token for a given signature and size
 *                          accept OIDs as input
*/
/***********************************************************
 * DESCRIPTION:
 *  Token for a given signature and size
 *  If it is not in the dictionary, the signature
 *  will be added to the dictionary and the index tree
 *  If it is already in the dictionary, then
 *  the token will be returned
 * PARAMETERS:
 *  DctnryTuple& dctnryTuple - holds the sigValue, sigSize and token
 *  DctnryStruct dctnryStruct- contain the 3 OID for dictionary,
 *                             tree and list.
 * RETURN:
 *    NO_ERROR if success
 *    others if something wrong in inserting the value
 ***********************************************************/
int WriteEngineWrapper::tokenize(const TxnID& txnid,
                                 DctnryStruct& dctnryStruct,
                                 DctnryTuple& dctnryTuple)
{
  //find the corresponding column segment file the token is going to be inserted.

  Dctnry* dctnry = m_dctnry[op(dctnryStruct.fCompressionType)];
  int rc = dctnry->openDctnry(dctnryStruct.dctnryOid,
  //                          dctnryStruct.treeOid, dctnryStruct.listOid,
                              dctnryStruct.fColDbRoot, dctnryStruct.fColPartition,
                              dctnryStruct.fColSegment);
  if (rc !=NO_ERROR)
    return rc;

  rc = tokenize(txnid, dctnryTuple, dctnryStruct.fCompressionType);
  int rc2 = dctnry->closeDctnry(); // close file, even if tokenize() fails
  if ((rc == NO_ERROR) && (rc2 != NO_ERROR))
    rc = rc2;
  return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Create column files, including data and bitmap files
 * PARAMETERS:
 *    dataOid - column data file id
 *    bitmapOid - column bitmap file id
 *    colWidth - column width
 *    dbRoot   - DBRoot where file is to be located
 *    partition - Starting partition number for segment file path
 *     segment - segment number
 *     compressionType - compression type
 * RETURN:
 *    NO_ERROR if success
 *    ERR_FILE_EXIST if file exists
 *    ERR_FILE_CREATE if something wrong in creating the file
 ***********************************************************/
int WriteEngineWrapper::createDctnry(const TxnID& txnid,
                                    const OID& dctnryOid,
                                    int colWidth,
                                    uint16_t dbRoot,
                                    uint32_t partiotion,
                                    uint16_t segment,
                                    int compressionType)
{
    BRM::LBID_t startLbid;
    return m_dctnry[op(compressionType)]->
           createDctnry( dctnryOid, colWidth, dbRoot, partiotion, segment, startLbid);
}

int WriteEngineWrapper::convertRidToColumn (RID& rid, uint16_t& dbRoot, uint32_t& partition,
                                            uint16_t& segment, RID filesPerColumnPartition,
                                            RID  extentsPerSegmentFile, RID extentRows,
                                            uint16_t startDBRoot, unsigned dbrootCnt)
{
    int rc = 0;
    partition = rid / (filesPerColumnPartition * extentsPerSegmentFile * extentRows);

    segment =(((rid % (filesPerColumnPartition * extentsPerSegmentFile * extentRows)) / extentRows)) % filesPerColumnPartition;

    dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;

    //Calculate the relative rid for this segment file
    RID relRidInPartition = rid - ((RID)partition * (RID)filesPerColumnPartition * (RID)extentsPerSegmentFile * (RID)extentRows);
    assert (relRidInPartition <= (RID)filesPerColumnPartition * (RID)extentsPerSegmentFile * (RID)extentRows);
    uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
    unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
    RID relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
    rid = relRidInThisExtent +  numExtentsInThisSegPart * extentRows;
    return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Clears table lock for the specified table lock ID.
 * PARAMETERS:
 *    lockID - table lock to be released
 *    errMsg - if error occurs, this is the return error message
 * RETURN:
 *    NO_ERROR if operation is successful
 ***********************************************************/
int WriteEngineWrapper::clearTableLockOnly(
    uint64_t     lockID,
    std::string& errMsg)
{
    bool bReleased;
    
    int rc = BRMWrapper::getInstance()->releaseTableLock( lockID,
        bReleased, errMsg);

    return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Rolls back the state of the extentmap and database files for the
 *    specified table OID, using the metadata previously saved to disk.
 *    Also clears the table lock for the specified table OID.
 * PARAMETERS:
 *    tableOid - table OID to be rolled back
 *    lockID   - table lock corresponding to tableOid
 *    tableName - table name associated with tableOid
 *    applName - application that is driving this bulk rollback
 *    debugConsole - enable debug logging to the console
 *    errorMsg - error message explaining any rollback failure
 * RETURN:
 *    NO_ERROR if rollback completed succesfully
 ***********************************************************/
int WriteEngineWrapper::bulkRollback(OID   tableOid,
                                     uint64_t lockID,
                                     const std::string& tableName,
                                     const std::string& applName,
                                     bool debugConsole, string& errorMsg)
{
    errorMsg.clear();

    BulkRollbackMgr rollbackMgr(tableOid, lockID, tableName, applName);
    if (debugConsole)
        rollbackMgr.setDebugConsole(true);

    // We used to pass "false" to not keep (delete) the metafiles at the end of
    // the rollback.  But after the transition to sharedNothing, we pass "true"
    // to initially keep these files.  The metafiles are deleted later, only
    // after all the distributed bulk rollbacks are successfully completed.
    int rc = rollbackMgr.rollback( true );
    if (rc != NO_ERROR)
        errorMsg = rollbackMgr.getErrorMsg();

    // Ignore the return code for now; more important to base rc on the
    // success or failure of the previous work
    BRMWrapper::getInstance()->takeSnapshot();

    return rc;
}

int WriteEngineWrapper::rollbackCommon(const TxnID& txnid, int sessionId)
{ 
	//Remove the unwanted tmp files and recover compressed chunks.
	string prefix;

    // BUG 4312
    RemoveTxnFromLBIDMap(txnid);

    config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		cerr << "Need a valid DBRMRoot entry in Calpont configuation file";
		return -1;
	}
	
	uint64_t pos =  prefix.find_last_of ("/") ;
	std::string aDMLLogFileName;
	if (pos != string::npos)
	{
		aDMLLogFileName = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
        logging::Message::Args args;
        args.add("RollbackTran cannot find the dbrm directory for the DML log file");
        SimpleSysLog::instance()->logMsg(args, logging::LOG_TYPE_CRITICAL, logging::M0007);	
		return -1;

	}
	std::ostringstream oss;
	oss << txnid << "_" << Config::getLocalModuleID();
	aDMLLogFileName += "DMLLog_" + oss.str();

	if (IDBPolicy::exists(aDMLLogFileName.c_str()))
	{
		// TODO-for now the DML log file will always be in a local
		// filesystem since IDBDataFile doesn't have any support for
		// a cpp iostream interface.  need to decide if this is ok.
		boost::scoped_ptr<IDBDataFile> aDMLLogFile(IDBDataFile::open(
											IDBPolicy::getType(aDMLLogFileName.c_str(),
											IDBPolicy::WRITEENG),
											aDMLLogFileName.c_str(), "r", 0));

		if (aDMLLogFile) //need recover
		{
			ssize_t fileSize = aDMLLogFile->size();
			boost::scoped_array<char> buf(new char[fileSize]);
			if (aDMLLogFile->read(buf.get(), fileSize) != fileSize)
				return ERR_FILE_READ;

			std::istringstream strstream(string(buf.get(), fileSize));
			std::string backUpFileType;
			std::string filename;
			int64_t size;
			int64_t offset;
			while (strstream >> backUpFileType >> filename >> size >> offset)
			{
				//cout << "Found: " <<  backUpFileType << " name " << filename << "size: " << size << " offset: " << offset << endl;
				std::ostringstream oss;
				oss << "RollbackTran found " <<  backUpFileType << " name " << filename << " size: " << size << " offset: " << offset;
				logging::Message::Args args;
				args.add(oss.str());
				SimpleSysLog::instance()->logMsg(args, logging::LOG_TYPE_INFO, logging::M0007);	
				if (backUpFileType.compare("rlc") == 0)
				{
					//remove the rlc file
					filename += ".rlc";
					//cout << " File removed: " << filename << endl;
					IDBPolicy::remove(filename.c_str());
					logging::Message::Args args1;
					args1.add(filename);
					args1.add(" is removed.");
					SimpleSysLog::instance()->logMsg(args1, logging::LOG_TYPE_INFO, logging::M0007);
				}
				else if (backUpFileType.compare("tmp") == 0)
				{
					int rc = NO_ERROR;
					string orig(filename + ".orig");
					// restore the orig file
   					if (IDBPolicy::exists(orig.c_str()))
					{
						// not likely both cdf and tmp exist
						if (IDBPolicy::exists(filename.c_str()) &&
							IDBPolicy::remove(filename.c_str()) != 0)
							rc = ERR_COMP_REMOVE_FILE;

						if (rc==NO_ERROR && IDBPolicy::rename(orig.c_str(), filename.c_str())!=0)
							rc = ERR_COMP_RENAME_FILE;
					}

   					// remove the tmp file
					string tmp(filename + ".tmp");
					if (rc == NO_ERROR && IDBPolicy::exists(tmp.c_str()) &&
						IDBPolicy::remove(tmp.c_str()) != 0)
						rc = ERR_COMP_REMOVE_FILE;

   					// remove the chunk shifting helper
					string rlc(filename + ".rlc");
					if (rc == NO_ERROR && IDBPolicy::exists(rlc.c_str()) &&
						IDBPolicy::remove(rlc.c_str()) != 0)
						rc = ERR_COMP_REMOVE_FILE;

					logging::Message::Args args1;
					args1.add(filename);
					if (rc == NO_ERROR)
					{
						args1.add(" is restored.");
						SimpleSysLog::instance()->logMsg(args1,
														logging::LOG_TYPE_INFO, logging::M0007);
					}
					else
					{
						args1.add(" may not restored: ");
						args1.add(rc);
						SimpleSysLog::instance()->logMsg(args1,
														logging::LOG_TYPE_CRITICAL, logging::M0007);

						return rc;
					}
				}
				else
				{
					//copy back to the data file
					std::string backFileName(filename);
					if (backUpFileType.compare("chk") == 0 )
						backFileName += ".chk";
					else
						backFileName += ".hdr";
					//cout << "Rollback found file " << backFileName << endl;	
					IDBDataFile* sourceFile = IDBDataFile::open(
									IDBPolicy::getType(backFileName.c_str(), IDBPolicy::WRITEENG),
									backFileName.c_str(), "r", 0);
					IDBDataFile* targetFile = IDBDataFile::open(
									IDBPolicy::getType(filename.c_str(), IDBPolicy::WRITEENG),
									filename.c_str(), "r+", 0);

					size_t byteRead;
					unsigned char* readBuf = new unsigned char[size];
					boost::scoped_array<unsigned char> readBufPtr( readBuf );
					if( sourceFile != NULL ) {
						int rc = sourceFile->seek( 0, 0 );
						if (rc)
							return ERR_FILE_SEEK;
						byteRead = sourceFile->read( readBuf, size );
						if( (int) byteRead != size )
						{
							logging::Message::Args args6;
							args6.add("Rollback cannot read backup file ");
							args6.add(backFileName);
							SimpleSysLog::instance()->logMsg(args6, logging::LOG_TYPE_ERROR, logging::M0007);	
							return ERR_FILE_READ;
						}
					}
					else
					{
						logging::Message::Args args5;
						args5.add("Rollback cannot open backup file ");
						args5.add(backFileName);					
						SimpleSysLog::instance()->logMsg(args5, logging::LOG_TYPE_ERROR, logging::M0007);	
						return ERR_FILE_NULL;
					}
					size_t byteWrite;

					if( targetFile != NULL ) {
						int rc = targetFile->seek( offset, 0 );
						if (rc)
							return ERR_FILE_SEEK;
						byteWrite = targetFile->write( readBuf, size );
						if( (int) byteWrite != size )
						{
							logging::Message::Args args3;
							args3.add("Rollback cannot copy to file ");
							args3.add(filename);
							args3.add( "from file ");
							args3.add(backFileName);
							SimpleSysLog::instance()->logMsg(args3, logging::LOG_TYPE_ERROR, logging::M0007);	
							
							return ERR_FILE_WRITE;
						}
					}
					else
					{
						logging::Message::Args args4;
						args4.add("Rollback cannot open target file ");
						args4.add(filename);					
						SimpleSysLog::instance()->logMsg(args4, logging::LOG_TYPE_ERROR, logging::M0007);	
						return ERR_FILE_NULL;
					}
						
					//cout << "Rollback copied to file " << filename << " from file " << backFileName << endl;
					
					delete targetFile;
					delete sourceFile;
					IDBPolicy::remove( backFileName.c_str() );
					logging::Message::Args arg1;
					arg1.add("Rollback copied to file ");
					arg1.add(filename);
					arg1.add( "from file ");
					arg1.add(backFileName);
					SimpleSysLog::instance()->logMsg(arg1, logging::LOG_TYPE_INFO, logging::M0007);	
				}
			}
		}

		IDBPolicy::remove(aDMLLogFileName.c_str());
	}

	return 0;

}

int WriteEngineWrapper::rollbackTran(const TxnID& txnid, int sessionId)
{
	if( rollbackCommon( txnid, sessionId ) != 0 )
		return -1;

	return BRMWrapper::getInstance()->rollBack(txnid, sessionId);
}

int WriteEngineWrapper::rollbackBlocks(const TxnID& txnid, int sessionId)
{ 
	if( rollbackCommon( txnid, sessionId ) != 0 )
		return -1;

	return BRMWrapper::getInstance()->rollBackBlocks(txnid, sessionId);
}

int WriteEngineWrapper::rollbackVersion(const TxnID& txnid, int sessionId)
{ 
    // BUG 4312
    RemoveTxnFromLBIDMap(txnid);

    return BRMWrapper::getInstance()->rollBackVersion(txnid, sessionId); 
}

int WriteEngineWrapper::updateNextValue(const TxnID txnId, const OID& columnoid, const uint64_t nextVal, const uint32_t sessionID, const uint16_t dbRoot)
{
	int rc = NO_ERROR;
	CalpontSystemCatalog* systemCatalogPtr;
	RIDList ridList;
	ColValueList colValueList;
	WriteEngine::ColTupleList colTuples;
	ColStructList colStructList;
	WriteEngine::ColStruct colStruct;
	colStruct.dataOid = OID_SYSCOLUMN_NEXTVALUE;
	colStruct.colWidth = 8;
	colStruct.tokenFlag = false;
	colStruct.colDataType =  CalpontSystemCatalog::UBIGINT;
	colStruct.fColDbRoot = dbRoot;
	if (idbdatafile::IDBPolicy::useHdfs())	
		colStruct.fCompressionType = 2;
		
	colStructList.push_back(colStruct);
	ColTuple colTuple;
	systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);
	CalpontSystemCatalog::ROPair ropair;
	
	try {
		ropair = systemCatalogPtr->nextAutoIncrRid(columnoid);
	}
	catch (...)
	{
		rc = ERR_AUTOINC_RID;
	}
	if (rc != NO_ERROR)
		return rc;
		
	ridList.push_back(ropair.rid);
	colTuple.data = nextVal;
	colTuples.push_back(colTuple);		
	colValueList.push_back(colTuples);
	//TxnID txnid;
	rc = writeColumnRecords(txnId, colStructList, colValueList, ridList, SYSCOLUMN_BASE, false);
	if (rc != NO_ERROR)
		return rc;
		
	//flush PrimProc cache
	vector<LBID_t> blockList;
	BRM::LBIDRange_v lbidRanges;
	rc = BRMWrapper::getInstance()->lookupLbidRanges(OID_SYSCOLUMN_NEXTVALUE,
                                                     lbidRanges);
	if (rc != NO_ERROR)
		return rc;
		
	LBIDRange_v::iterator it;
	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
	{
		for (LBID_t  lbid = it->start; lbid < (it->start + it->size); lbid++)
		{
			blockList.push_back(lbid);
		}
	}
	//Bug 5459 Flush FD cache
	std::vector<BRM::FileInfo> files;
	BRM::FileInfo aFile;	
	aFile.oid = colStruct.dataOid;		
	aFile.partitionNum = colStruct.fColPartition;
	aFile.dbRoot =colStruct.fColDbRoot;;
	aFile.segmentNum = colStruct.fColSegment;
	aFile.compType = colStruct.fCompressionType;
	files.push_back(aFile);
	if (idbdatafile::IDBPolicy::useHdfs())
		cacheutils::purgePrimProcFdCache(files, Config::getLocalModuleID()); 
	rc = cacheutils::flushPrimProcAllverBlocks (blockList);
	if (rc != 0)
		rc = ERR_BLKCACHE_FLUSH_LIST; // translate to WE error
	
	return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Flush compressed files in chunk manager
 * PARAMETERS:
 *    none
 * RETURN:
 *    none
 ***********************************************************/
int WriteEngineWrapper::flushDataFiles(int rc, const TxnID txnId, std::map<FID,FID> & columnOids)
{
   RemoveTxnFromLBIDMap(txnId);

   for (int i = 0; i < TOTAL_COMPRESS_OP; i++)
   {
      int rc1 = m_colOp[i]->flushFile(rc, columnOids);
      int rc2 = m_dctnry[i]->flushFile(rc, columnOids);
      if (rc == NO_ERROR)
      {
         rc = (rc1 != NO_ERROR) ? rc1 : rc2;
      }
   }

   return rc;
}

/***********************************************************
 * DESCRIPTION:
 *    Add an lbid to a list of lbids for sending to markExtentsInvalid.
 *    However, rather than storing each lbid, store only unique first
 *    lbids. This is an optimization to prevent invalidating the same
 *    extents over and over.
 * PARAMETERS:
 *    txnid - the lbid list is per txn. We use this to keep transactions
 *            seperated.
 *    lbids - the current list of lbids. We add to this list
 *            if the discovered lbid is in a new extent.
 *   These next are needed for dbrm to get the lbid
 *    oid       -the table oid.
 *    colPartition - the table column partition
 *    segment   - table segment
 *    fbo       - file block offset
 * RETURN: 0 => OK. -1 => error
 ***********************************************************/
int WriteEngineWrapper::AddLBIDtoList(const TxnID     txnid,
                                      std::vector<BRM::LBID_t>& lbids,
                                      std::vector<CalpontSystemCatalog::ColDataType>& colDataTypes,
                                      const ColStruct& colStruct,
                                      const int       fbo)
{
    int rtn = 0;

    BRM::LBID_t     startingLBID;
    SP_TxnLBIDRec_t spTxnLBIDRec;
    std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter;

    // Find the set of extent starting LBIDs for this transaction. If not found, then create it.
    mapIter = m_txnLBIDMap.find(txnid);
    if (mapIter == m_txnLBIDMap.end())
    {
        // This is a new transaction.
        SP_TxnLBIDRec_t  sptemp(new TxnLBIDRec);
        spTxnLBIDRec = sptemp;
        m_txnLBIDMap[txnid] = spTxnLBIDRec;
//        cout << "New transaction entry " << txnid << " transaction count " << m_txnLBIDMap.size() << endl;
    }
    else
    {
        spTxnLBIDRec = (*mapIter).second;
    }
    
    // Get the extent starting lbid given all these values (startingLBID is an out parameter).
    rtn = BRMWrapper::getInstance()->getStartLbid(colStruct.dataOid, colStruct.fColPartition, 
                                                  colStruct.fColSegment, fbo, startingLBID);
    if (rtn != 0)
        return -1;

    if (spTxnLBIDRec->m_LBIDMap.find(startingLBID) == spTxnLBIDRec->m_LBIDMap.end())
    {
        // Not found in the map. This must be a new extent. Add it to the list.
//        cout << "Adding lbid " << startingLBID << " to txn " << txnid << endl;
        spTxnLBIDRec->AddLBID(startingLBID);
        lbids.push_back((BRM::LBID_t)startingLBID);
        colDataTypes.push_back(colStruct.colDataType);
    }
    else
    {
        ++spTxnLBIDRec->m_squashedLbids;
    }

    // If the starting LBID list has grown to more than 2000, truncate.
    // This is the purpose of the seqnum. If spTxnLBIDRec->m_lastSeqnum
    // is divisible by 1000 and size() > 1000, get rid of everything older
    // than the last 1000 entries. This is to save memory in large
    // transactions. We assume older extents are unlikely to be hit again.
    if (spTxnLBIDRec->m_lastSeqnum % 1000 == 0
     && spTxnLBIDRec->m_LBIDMap.size() > 1000)
    {
//        cout << "Trimming the LBID list for " << txnid << ". LBID count is " << spTxnLBIDRec->m_LBIDMap.size() << endl;
        uint32_t firstDrop = spTxnLBIDRec->m_lastSeqnum - 1000;
        std::tr1::unordered_map<BRM::LBID_t, uint32_t>::iterator iter;
        for (iter = spTxnLBIDRec->m_LBIDMap.begin(); iter != spTxnLBIDRec->m_LBIDMap.end();)
        {
            if ((*iter).second < firstDrop)
            {
                 iter = spTxnLBIDRec->m_LBIDMap.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
//        cout << "LBID count is now" << spTxnLBIDRec->m_LBIDMap.size() << endl;
    }

    return rtn;
}


/***********************************************************
 * DESCRIPTION:
 *    Remove a transaction LBID list from the LBID map
 *    Called when a transaction ends, either commit or rollback
 * PARAMETERS:
 *    txnid - the transaction to remove.
 * RETURN:
 *    0 => success or not found, -1 => error
 ***********************************************************/
int WriteEngineWrapper::RemoveTxnFromLBIDMap(const TxnID txnid)
{
    int rtn = 0;
    std::tr1::unordered_map<TxnID, SP_TxnLBIDRec_t>::iterator mapIter;

    // Find the set of extent starting LBIDs for this transaction. If not found, then create it.
    try
    {
        mapIter = m_txnLBIDMap.find(txnid);
        if (mapIter != m_txnLBIDMap.end())
        {
            SP_TxnLBIDRec_t spTxnLBIDRec = (*mapIter).second;
            // Debug
//            cout << "Remove transaction entry " << txnid << " transaction count " << m_txnLBIDMap.size() << endl;
//            cout << "    count = " << spTxnLBIDRec->m_LBIDMap.size() << 
//                    ", lastSeqnum = " <<  spTxnLBIDRec->m_lastSeqnum << 
//                    ", squashed lbids = " << spTxnLBIDRec->m_squashedLbids << endl;
            m_txnLBIDMap.erase(txnid);   // spTxnLBIDRec is auto-destroyed
        }
    }
    catch(...)
    {
        rtn = -1;
    }
    return rtn;
}


} //end of namespace
// vim:ts=4 sw=4:

