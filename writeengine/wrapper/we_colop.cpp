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

//  $Id: we_colop.cpp 4733 2013-08-12 22:38:38Z chao $

/** @file */

#include <stdio.h>
#include <string.h>
#include <vector>
#include <map>
#include <boost/scoped_ptr.hpp>

using namespace std;

#define WRITEENGINECOLUMNOP_DLLEXPORT
#include "we_colop.h"
#undef WRITEENGINECOLUMNOP_DLLEXPORT
#include "we_log.h"
#include "we_dbfileop.h"
#include "we_dctnrycompress.h"
#include "we_colopcompress.h"
#include "idbcompress.h"
#include "writeengine.h"

using namespace execplan;

namespace WriteEngine
{
	struct RefcolInfo
	{
	   		int localHwm;
	   		unsigned numExtents;	   	
	};

   /**
    * Constructor
    */
   ColumnOp::ColumnOp()
   {
      //memset(m_workBlock.data, 0, BYTE_PER_BLOCK);
   }
   ColumnOp::ColumnOp(Log* logger)
   {
      setDebugLevel(logger->getDebugLevel());
      setLogger    (logger);
   }

   /**
    * Default Destructor
    */
   ColumnOp::~ColumnOp()
   {}

   /***********************************************************
    * DESCRIPTION:
    *    Allocate Row ID
    * PARAMETERS:
    *    tableFid - the file id for table bitmap file
    *    totalRow - the total number of rows need to be allocated
    * RETURN:
    *    NO_ERROR if success
    *    rowIdArray - allocation of the row id left here
    ***********************************************************/
   int ColumnOp::allocRowId(const TxnID& txnid, Column& column, uint64_t totalRow, RID* rowIdArray, HWM& hwm, bool& newExtent, uint64_t& rowsLeft, HWM& newHwm, 
			bool& newFile, ColStructList& newColStructList, DctnryStructList& newDctnryStructList, std::vector<DBRootExtentTracker*> & dbRootExtentTrackers, 
			bool insertSelect, bool isBatchInsert, OID tableOid)
   {
   	//MultiFiles per OID: always append the rows to the end for now.
	  // See if the current HWM block might be in an abbreviated extent that
	  // needs to be expanded, if we end up adding enough rows.
	  bool bCheckAbbrevExtent      = false;
	  uint64_t  numBlksPerInitialExtent = INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK * column.colWidth;
// DMC-SHARED_NOTHING_NOTE: Is it safe to assume only part0 seg0 is abbreviated?
	  if ((column.dataFile.fPartition == 0) &&
          (column.dataFile.fSegment   == 0) &&
	      ((hwm+1) <= numBlksPerInitialExtent))
              bCheckAbbrevExtent = abbreviatedExtent(column.dataFile.pFile, column.colWidth);

	  //The current existed rows upto hwm
	  int counter = 0;
	  uint64_t totalRowPerBlock = BYTE_PER_BLOCK/column.colWidth;
	  uint64_t currentRows = totalRowPerBlock * hwm;
	  uint64_t extentRows = BRMWrapper::getInstance()->getExtentRows();
	  uint64_t numExtentsFilled = currentRows / extentRows;
	  uint64_t rowsAvailable = extentRows - (numExtentsFilled * extentRows);
	  rowsLeft = totalRow < rowsAvailable ? 0 : totalRow - rowsAvailable;
	  newExtent = false;
	  uint j = 0, i=0, rowsallocated = 0;
	  int rc = 0;
	  newFile = false;
	  Column newCol;
	  unsigned char  buf[BYTE_PER_BLOCK];
		
		// ZZ. For insert select, skip the hwm block and start inserting from the next block
		// to avoid self insert issue.
		//For batch insert: if not first batch, use the saved last rid to start adding rows.
		
		if (!insertSelect)
		{
		  //..Search the HWM block for empty rows
		  rc = readBlock(column.dataFile.pFile, buf, hwm);
		  if ( rc != NO_ERROR)
			return rc;
		  for(j = 0; j < totalRowPerBlock; j++) {
	         if (isEmptyRow(buf, j, column)) {
	            rowIdArray[counter] = getRowId(hwm, column.colWidth, j);
	            rowsallocated++;
	            counter++;
	         if (rowsallocated >= totalRow)
	         	break;	  		                
		  	}
		 }
		}
	  
	if (rowsallocated < totalRow)
	{
	  	 //..Search remaining blks in current extent (after HWM) for empty rows
	  	 //Need go to next block
		 //need check whether this block is the last block for this extent
		 while (((totalRowPerBlock * (hwm+1)) % extentRows) > 0)
		 {
			hwm++;

			// Expand abbreviated initial extent on disk if needed.
			if (bCheckAbbrevExtent) {
				if ((hwm+1) > numBlksPerInitialExtent) {
					RETURN_ON_ERROR(expandAbbrevExtent(column));
					bCheckAbbrevExtent = false;
				}
			}
			
			RETURN_ON_ERROR(readBlock(column.dataFile.pFile, buf, hwm));
			for(j = 0; j < totalRowPerBlock; j++) {
				if (isEmptyRow(buf, j, column)) {
					rowIdArray[counter] = getRowId(hwm, column.colWidth, j);
					rowsallocated++;
					counter++;
					if (rowsallocated >= totalRow)
					break;	  		                
				}
			}
			if (rowsallocated >= totalRow)
				break;
		 }
/*		 RETURN_ON_ERROR(BRMWrapper::getInstance()->setLocalHWM_int(column.dataFile.fid,
                                                                column.dataFile.fPartition,
                                                                column.dataFile.fSegment,
                                                                hwm)); */

	    //Check if a new extent is needed  		
        if (rowsallocated < totalRow)
		 {
			 //Create another extent
			 u_int16_t  dbRoot;
			 u_int32_t  partition = 0;
			 u_int16_t  segment;
			 FILE*      pFile = NULL;
			 std::string segFile;
			 rowsLeft = 0;
			 int		allocSize = 0;
			 newExtent = true;
			 if ((column.dataFile.fid < 3000) || (!isBatchInsert)) //systables or single insert
			 {
				dbRoot = column.dataFile.fDbRoot;
			 }
			 else
			 {
				//Find out where the rest rows go
				BRM::LBID_t startLbid;
				//need to put in a loop until newExtent is true
				newExtent = dbRootExtentTrackers[0]->nextSegFile(dbRoot, partition, segment, newHwm, startLbid);
				TableMetaData* tableMetaData= TableMetaData::makeTableMetaData(tableOid);
				while (!newExtent)
				{
					/*partially filled extent encountered due to user moved dbroot. Set hwm to the end of the extent.
						If compressed,fill the rest eith empty values.
					*/	
					unsigned int BLKS_PER_EXTENT = 0;
					unsigned int nBlks = 0;
					unsigned int nRem = 0;
					FileOp fileOp;	
					long long fileSizeBytes = 0;
					
					for (i=0; i < dbRootExtentTrackers.size(); i++)
					{
						if (i != 0)
							dbRootExtentTrackers[i]->nextSegFile(dbRoot, partition, segment, newHwm, startLbid);
						
						// Round up HWM to the end of the current extent
						BLKS_PER_EXTENT =(BRMWrapper::getInstance()->getExtentRows() * newColStructList[i].colWidth)/BYTE_PER_BLOCK;
						nBlks = newHwm + 1;
						nRem  = nBlks % BLKS_PER_EXTENT;
						if (nRem > 0)
							newHwm = nBlks - nRem + BLKS_PER_EXTENT - 1;
						else
							newHwm = nBlks - 1;
						//save it to set in the end
						ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(newColStructList[i].dataOid);
						ColExtInfo aExt;
						aExt.dbRoot =dbRoot;
						aExt.partNum = partition;
						aExt.segNum = segment;
						aExt.hwm = newHwm;
						aExt.isNewExt = false;
						aExt.current = false;
						aColExtsInfo.push_back(aExt);
						if (newColStructList[i].fCompressionType > 0)
						{
							uint64_t emptyVal = getEmptyRowValue(newColStructList[i].colDataType, newColStructList[i].colWidth);
							string errorInfo;
							rc = fileOp.fillCompColumnExtentEmptyChunks(newColStructList[i].dataOid, newColStructList[i].colWidth, 
								emptyVal, dbRoot, partition, segment, newHwm, segFile, errorInfo);
							if (rc != NO_ERROR)
								return rc;
						}
						//@Bug 4758. Check whether this is a abbreviated extent
						else if (newColStructList[i].fCompressionType == 0)
						{
							rc = fileOp.getFileSize3(newColStructList[i].dataOid, dbRoot, partition, segment, fileSizeBytes);
							if (rc != NO_ERROR)
								return rc;
						
							if (fileSizeBytes == (long long)  INITIAL_EXTENT_ROWS_TO_DISK * newColStructList[i].colWidth)
							{
								 FILE* pFile = fileOp.openFile( newColStructList[i].dataOid, dbRoot, partition, segment, segFile );
								 if ( !pFile )
								 {
									rc = ERR_FILE_OPEN;
									return rc;
								 }
								 uint64_t emptyVal = getEmptyRowValue(newColStructList[i].colDataType, newColStructList[i].colWidth);
								 rc = fileOp.expandAbbrevColumnExtent( pFile, dbRoot, emptyVal, newColStructList[i].colWidth);
								 //set hwm for this extent.
								 fileOp.closeFile(pFile);
								 if (rc != NO_ERROR)
									return rc;	
							}
						}
						tableMetaData->setColExtsInfo(newColStructList[i].dataOid, aColExtsInfo);
					}
					newExtent = dbRootExtentTrackers[0]->nextSegFile(dbRoot, partition, segment, newHwm, startLbid);	
				}
			 }
			 
			 std::vector<BRM::CreateStripeColumnExtentsArgOut> extents;
			 if (newExtent)
			 {
				//extend all columns together
				std::vector<BRM::CreateStripeColumnExtentsArgIn> cols;
				BRM::CreateStripeColumnExtentsArgIn createStripeColumnExtentsArgIn;
				for (i=0; i < newColStructList.size(); i++)
				{
					createStripeColumnExtentsArgIn.oid = newColStructList[i].dataOid;
					createStripeColumnExtentsArgIn.width = newColStructList[i].colWidth;
                    createStripeColumnExtentsArgIn.colDataType = newColStructList[i].colDataType;
					cols.push_back(createStripeColumnExtentsArgIn);
				}
				
				rc = BRMWrapper::getInstance()->allocateStripeColExtents(cols, dbRoot, partition, segment, extents);
				newHwm = extents[0].startBlkOffset;
				if (rc != NO_ERROR)
					return rc;
					
				//Create column files
				vector<BRM::LBID_t> lbids;
                vector<CalpontSystemCatalog::ColDataType> colDataTypes;
				//BRM::CPInfoList_t cpinfoList;
				//BRM::CPInfo cpInfo;
				//cpInfo.max = numeric_limits<int64_t>::min();
				//cpInfo.min = numeric_limits<int64_t>::max();
				//cpInfo.seqNum = -1;	
				for ( i=0; i < extents.size(); i++)
				{
					setColParam(newCol, 0, newColStructList[i].colWidth, newColStructList[i].colDataType, newColStructList[i].colType, 
						newColStructList[i].dataOid, newColStructList[i].fCompressionType, dbRoot, partition, segment);
					rc = extendColumn(newCol, false, false, extents[i].startBlkOffset, extents[i].startLbid, extents[i].allocSize, 
						dbRoot, partition, segment, segFile, pFile, newFile);
					if (rc != NO_ERROR)
						return rc;
					
					//cpInfo.firstLbid = extents[i].startLbid;
					//cpinfoList.push_back(cpInfo);
					newColStructList[i].fColPartition = partition;
					newColStructList[i].fColSegment = segment;
					newColStructList[i].fColDbRoot = dbRoot;
					newDctnryStructList[i].fColPartition = partition;
					newDctnryStructList[i].fColSegment = segment;
					newDctnryStructList[i].fColDbRoot = dbRoot;
					lbids.push_back(extents[i].startLbid);
                    colDataTypes.push_back(newColStructList[i].colDataType);
				}
				
				//mark the extents to updating
//rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);
				rc = BRMWrapper::getInstance()->markExtentsInvalid(lbids, colDataTypes);
				if (rc != NO_ERROR)
					return rc;
				//create corresponding dictionary files
				if (newFile )
				{
					boost::scoped_ptr<WriteEngineWrapper> we (new WriteEngineWrapper());
					for (i=0; i < newDctnryStructList.size(); i++)
					{
						if (newDctnryStructList[i].dctnryOid > 0)
						{
							rc = we->createDctnry(txnid, newDctnryStructList[i].dctnryOid, newDctnryStructList[i].colWidth, dbRoot, partition,
                                 segment, newDctnryStructList[i].fCompressionType);
							if ( rc != NO_ERROR)
								return rc;
						}	
					}
				}
			 }
			 
			 //save the extent info for batch insert
			if (isBatchInsert && newExtent)
			{
			  TableMetaData* tableMetaData= TableMetaData::makeTableMetaData(tableOid);	
			  for (i=0; i < newColStructList.size(); i++)
			  {		
				ColExtsInfo aColExtsInfo = tableMetaData->getColExtsInfo(newColStructList[i].dataOid);
				ColExtsInfo::iterator it = aColExtsInfo.begin();
				while (it != aColExtsInfo.end())
				{
					if ((it->dbRoot == newColStructList[i].fColDbRoot) && (it->partNum == newColStructList[i].fColPartition) && (it->segNum == newColStructList[i].fColSegment))
						break;
					it++;
				}
				ColExtInfo aExt;
				aExt.dbRoot = newColStructList[i].fColDbRoot;
				aExt.partNum = newColStructList[i].fColPartition;
				aExt.segNum = newColStructList[i].fColSegment;
				aExt.hwm = extents[i].startBlkOffset;
				aExt.isNewExt = true;
				aExt.current = true;
				aColExtsInfo.push_back(aExt);
				tableMetaData->setColExtsInfo(newColStructList[i].dataOid, aColExtsInfo);
			  }	 		
			}
			
			 setColParam(newCol, 0, column.colWidth,column.colDataType, column.colType, 
					 column.dataFile.fid, column.compressionType, dbRoot, partition, segment);
			 rc = openColumnFile(newCol, segFile);
			 if (rc != NO_ERROR)
				 return rc;
			//@Bug 3164 update compressed extent
			updateColumnExtent(newCol.dataFile.pFile, allocSize);
	         //..Search first block of new extent for empty rows
			rc = readBlock(newCol.dataFile.pFile, buf, newHwm);
			if ( rc != NO_ERROR)
					return rc;
			for(j = 0; j < totalRowPerBlock; j++) {
				if (isEmptyRow(buf, j, column)) {
					rowIdArray[counter] = getRowId(newHwm, column.colWidth, j);
					rowsallocated++;
					rowsLeft++;
					counter++;
					if (rowsallocated >= totalRow) {
						break;	
					}
				}
			}

			if (rowsallocated < totalRow)
			{
				//..Search remaining blks in new extent for empty rows
				newHwm++;
				while (((totalRowPerBlock * newHwm) % extentRows) > 0)
				{
					rc = readBlock(newCol.dataFile.pFile, buf, newHwm);
					if ( rc != NO_ERROR)
						return rc;
					
					for(j = 0; j < totalRowPerBlock; j++) {
					 if (isEmptyRow(buf, j, column)) {
						 rowIdArray[counter] = getRowId(newHwm, column.colWidth, j);
						 rowsallocated++;
						 rowsLeft++;
						 counter++;
						 if (rowsallocated >= totalRow) {
							 break;	
						}
					 }
					}
					if ((rowsallocated < totalRow)) {
						newHwm++;
					}
					else
						break;
				} 
			}
		}
	  }
	  
	  if (rowsallocated < totalRow) {
		return 1;
	  }
	  if (!newExtent)
		rowsLeft = 0;
	  	

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Clear a column
    * PARAMETERS:
    *    column - column
    * RETURN:
    *    none
    ***********************************************************/
   void ColumnOp::clearColumn(Column& column) const
   {
	  if (column.dataFile.pFile)
		  fflush(column.dataFile.pFile);

      setColParam(column);
      closeColumnFile(column);
   }

   /***********************************************************
    * DESCRIPTION:
    *    Close column's file
    * PARAMETERS:
    *    column - column
    * RETURN:
    *    none
    ***********************************************************/
   void ColumnOp::closeColumnFile(Column& column) const
   {
      if (column.dataFile.pFile != NULL)
         closeFile(column.dataFile.pFile);

      column.dataFile.pFile = NULL;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Create a column and its' related files
    * PARAMETERS:
    *    column - column
    *    colNo - column number
    *    colWidth - column width
    *    colType - column type
    *    dataFid - the file id for column data file
    *    dbRoot  - DBRoot where file is to be located
    *    partition-Starting partition number for segment file path
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_EXIST if file exists
    *    ERR_FILE_CREATE if something wrong in creating the file
    ***********************************************************/
   int ColumnOp::createColumn(Column& column,
		int colNo,
		int colWidth,
		CalpontSystemCatalog::ColDataType colDataType,
		ColType colType,
		FID dataFid,
		uint16_t dbRoot,
        uint32_t partition)
   {
      int rc, newWidth, allocSize;
      uint64_t emptyVal = 0;
      int compressionType = column.compressionType;
      setColParam(column, colNo, colWidth, colDataType, colType);
      emptyVal = getEmptyRowValue(colDataType, colWidth);
      newWidth = getCorrectRowWidth(colDataType, colWidth);
      column.dataFile.fid = dataFid;
      column.dataFile.fDbRoot    = dbRoot;
      column.dataFile.fPartition = partition;
      column.dataFile.fSegment   = 0;
	  column.compressionType = compressionType;
      rc = createFile(column.dataFile.fid, allocSize, dbRoot, partition, colDataType, emptyVal, newWidth);
      if (rc != NO_ERROR)
         return rc;

      return NO_ERROR;
   }

   /* BUG931
    * @brief Fills up a column with null/default values in all non-empty rows as the reference column. The reference column
    * would typically be another column of the same table.
    *
    * @param
    *
    * @return
    */
int ColumnOp::fillColumn(const TxnID& txnid, Column& column, Column& refCol, void * defaultVal, const OID dictOid, 
						const int dictColWidth, const string defaultValStr, bool autoincrement)
{
    unsigned char refColBuf[BYTE_PER_BLOCK]; //Refernce column buffer
    unsigned char colBuf[BYTE_PER_BLOCK];
    bool dirty = false;
    HWM colHwm = 0;
    RID maxRowId = 0;
	int size = sizeof(Token);
    uint64_t emptyVal;
    uint64_t refEmptyVal;

    long long startColFbo = 0;
    long long startRefColFbo = 0;

    int        refBufOffset = 0;
    int        colBufOffset = 0;
	unsigned   currentnumExtent = 0; 
	uint64_t   nexValNeeded = 0;
	uint64_t   nextVal;
    uint32_t   partition;
    uint16_t   segment;
    HWM        lastRefHwm;
    int        rc = 0;
	std::string segFile, errorMsg;
	BRM::LBID_t    startLbid;
	bool        newFile = true;
	int		 allocSize = 0;
	boost::scoped_ptr<Dctnry> dctnry;
    if (m_compressionType == 0)
      dctnry.reset(new DctnryCompress0);
    else
      dctnry.reset(new DctnryCompress1);
	  
	boost::scoped_ptr<ColumnOp> refColOp;
    if (refCol.compressionType != 0)
        refColOp.reset(new ColumnOpCompress1);
    else
        refColOp.reset(new ColumnOpCompress0);
	//get dbroots from config
	Config config;
	config.initConfigCache();
	std::vector<u_int16_t> rootList;
	config.getRootIdList( rootList );
	unsigned extentRows = BRMWrapper::getInstance()->getExtentRows();
	emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
    refEmptyVal = getEmptyRowValue(refCol.colDataType, refCol.colWidth);
	//find the dbroots which have rows for refrence column
	unsigned int i=0, k=0;
	
	for (i=0; i < rootList.size(); i++)
	{
		std::vector<struct BRM::EMEntry> refEntries;
		rc = BRMWrapper::getInstance()->getExtents_dbroot(refCol.dataFile.fid, refEntries, rootList[i]);
		std::vector<struct BRM::EMEntry>::const_iterator iter = refEntries.begin();
		while ( iter != refEntries.end() )
		{
			//fill in for the new column for each extent in the reference column
			//organize the extents into a file
			std::vector<struct BRM::EMEntry> fileExtents;
			fileExtents.push_back(refEntries[0]);
			//cout << "Get extent for ref oid:dbroot:part:seg = " << refCol.dataFile.fid<<":"<<rootList[0]<<":"<<refEntries[0].partitionNum
			//<<":"<<refEntries[0].segmentNum<<endl;
			for (k = 1; k < refEntries.size(); k++)
			{
				if ((refEntries[0].partitionNum == refEntries[k].partitionNum) && (refEntries[0].segmentNum == refEntries[k].segmentNum)) //already the same dbroot
				{
					fileExtents.push_back(refEntries[k]);
				}
			}
			
			//Process this file
			lastRefHwm = fileExtents[0].HWM;
			for (k = 1; k < fileExtents.size(); k++)
			{
				//Find the hwm of this file
				if (fileExtents[k].HWM > lastRefHwm)
					lastRefHwm = fileExtents[k].HWM;
			}
			
			//create extents for the new column
			// If we are processing the first extent in the first segment
            // file, we check to see if we have enough rows (256K) to re-
            // quire just create the initial abbrev extent for the new column.
			std::vector<struct BRM::EMEntry> newEntries;
            if (( refEntries.size() == 1) && (refEntries[0].partitionNum == 0) && (refEntries[0].segmentNum == 0))
			{
				//@Bug3565 use ref colwidth to calculate.
				unsigned int numBlksForFirstExtent =
                     (INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK) * refCol.colWidth;
                if ((lastRefHwm+1) < numBlksForFirstExtent)
                {
                    rc = createColumn(column, 0, column.colWidth, column.colDataType,
							WriteEngine::WR_CHAR, column.dataFile.fid, rootList[i], 0);
					if (rc != NO_ERROR)
						return rc;
					//cout << "createColumn for oid " << column.dataFile.fid << endl;
					BRM::EMEntry aEntry;
					aEntry.partitionNum = partition = 0;
					aEntry.segmentNum = segment = 0;
					aEntry.dbRoot = rootList[i];
					newEntries.push_back(aEntry);
					if (dictOid >3000)  //Create dictionary file if needed
					{
						rc = dctnry->createDctnry(dictOid, dictColWidth,
							rootList[i], partition, segment, startLbid, newFile);
						if (rc != NO_ERROR)
							return rc;
						//tokenize default value if needed
						if (defaultValStr.length() > 0)
						{
							DctnryStruct dctnryStruct;
							dctnryStruct.dctnryOid = dictOid;
							dctnryStruct.columnOid = column.dataFile.fid;
							dctnryStruct.fColPartition = partition;
							dctnryStruct.fColSegment = segment;
							dctnryStruct.fColDbRoot = rootList[i];
							dctnryStruct.colWidth = dictColWidth;
							dctnryStruct.fCompressionType = column.compressionType;
							DctnryTuple dctnryTuple;
							memcpy(dctnryTuple.sigValue, defaultValStr.c_str(), defaultValStr.length());
							dctnryTuple.sigSize = defaultValStr.length();
							WriteEngineWrapper wrapper;
							rc = wrapper.tokenize(txnid, dctnryStruct, dctnryTuple);
							if (rc != NO_ERROR)
								return rc;
							memcpy(defaultVal, &dctnryTuple.token, size); 
						}
					}
                }
            }
			   
			if (newEntries.size() == 0)
			{		
				for (k = 0; k < fileExtents.size(); k++)
				{
					uint16_t dbroot = rootList[i];
					partition = fileExtents[k].partitionNum;
					segment = fileExtents[k].segmentNum;
					if ( k == 0)
					{
						rc =  addExtent(column, dbroot, partition, segment,
                         segFile, startLbid, newFile, allocSize) ;
						if (rc != NO_ERROR)
							return rc; //Clean up will be done throgh DDLProc
						
						//cout << "extendColumn for oid " << column.dataFile.fid << endl;
						BRM::EMEntry aEntry;
						aEntry.partitionNum = partition;
						aEntry.segmentNum = segment;
						aEntry.dbRoot = rootList[i];
						newEntries.push_back(aEntry);
						if ((dictOid >3000) && newFile) //Create dictionary file if needed
						{
							rc = dctnry->createDctnry(dictOid, dictColWidth,
								rootList[i], partition, segment, startLbid, newFile);
							if (rc != NO_ERROR)
								return rc;
							//tokenize default value if needed
							if (defaultValStr.length() > 0)
							{
								DctnryStruct dctnryStruct;
								dctnryStruct.dctnryOid = dictOid;
								dctnryStruct.columnOid = column.dataFile.fid;
								dctnryStruct.fColPartition = partition;
								dctnryStruct.fColSegment = segment;
								dctnryStruct.fColDbRoot = rootList[i];
								dctnryStruct.colWidth = dictColWidth;
								dctnryStruct.fCompressionType = column.compressionType;
								DctnryTuple dctnryTuple;
								memcpy(dctnryTuple.sigValue, defaultValStr.c_str(), defaultValStr.length());
								WriteEngineWrapper wrapper;
								dctnryTuple.sigSize = defaultValStr.length();
								rc = wrapper.tokenize(txnid, dctnryStruct, dctnryTuple);
								if (rc != NO_ERROR)
									return rc;
								memcpy(defaultVal, &dctnryTuple.token, size); 
							}	
						}
					}
					else //just add a extent to the file
					{
						rc = addExtent(column, dbroot, partition, segment,
                         segFile, startLbid, newFile, allocSize) ;
						if (rc != NO_ERROR)
							return rc; //Clean up will be done throgh DDLProc
					}
				}
			}
			//Fill the new file with values
			//Open new column file and reference column file
			column.dataFile.fDbRoot = rootList[i];
            column.dataFile.fPartition = newEntries[0].partitionNum;
            column.dataFile.fSegment = newEntries[0].segmentNum;
            RETURN_ON_ERROR(openColumnFile(column, segFile));
			//cout << "Processing new col file " << segFile << endl;
			refCol.dataFile.fDbRoot = rootList[i];
			refCol.dataFile.fPartition = newEntries[0].partitionNum;
            refCol.dataFile.fSegment = newEntries[0].segmentNum;
            std::string segFileRef;
            RETURN_ON_ERROR(refColOp->openColumnFile(refCol, segFileRef));
			//cout << "Processing ref file " << segFileRef << " and hwm is " << lastRefHwm << endl;
			RETURN_ON_ERROR(refColOp->readBlock(refCol.dataFile.pFile, refColBuf, lastRefHwm));
			
            refBufOffset = BYTE_PER_BLOCK - refCol.colWidth;
            maxRowId = (lastRefHwm * BYTE_PER_BLOCK)/refCol.colWidth; //Local maxRowId
			
            while(refBufOffset > 0)
            {
				if (memcmp(&refColBuf[refBufOffset], &refEmptyVal, refCol.colWidth) != 0)
                {
                     maxRowId = maxRowId + (refBufOffset/refCol.colWidth);
                        break;
                }
                refBufOffset -= refCol.colWidth;
            }

            //Compute local hwm for the new column
            colHwm = (maxRowId * column.colWidth) / BYTE_PER_BLOCK;
			//cout << " new col hwm is " << colHwm << endl;
			currentnumExtent = (colHwm+1) * (BYTE_PER_BLOCK /column.colWidth) / extentRows;
			startRefColFbo = 0;
			startColFbo = 0;
            //startRefColFbo = currentnumExtent * extentRows * refCol.colWidth / BYTE_PER_BLOCK;
            //startColFbo = currentnumExtent * extentRows * column.colWidth / BYTE_PER_BLOCK;
            //Initizliaing to BYTE_PER_BLOCK to force read the first time
			refBufOffset = BYTE_PER_BLOCK;
            colBufOffset = BYTE_PER_BLOCK;
            dirty = false;
			BRM::CPInfo cpInfo;
			if (autoincrement)
			{
				uint64_t nextValStart = 0;
				while(startRefColFbo <= lastRefHwm || startColFbo <= colHwm)
				{
					//nexValNeeded = 0;
					//cout << "current startRefColFbo:startColFbo:refBufOffset:colBufOffset = " << startRefColFbo <<":"<< startColFbo <<":"<<refBufOffset<<":"<<colBufOffset<< endl;
					if ((refBufOffset + refCol.colWidth) > BYTE_PER_BLOCK)
					{
						//If current reference column block is fully processed get to the next one
						//cout << "reading from ref " << endl;
						RETURN_ON_ERROR(refColOp->readBlock(refCol.dataFile.pFile, refColBuf, startRefColFbo));
						startRefColFbo++;
						refBufOffset = 0;
						nexValNeeded = 0;
					}
					if ((colBufOffset + column.colWidth) > BYTE_PER_BLOCK)
					{
						//Current block of the new colum is full. Write it if dirty and then get the next block
						if (dirty)
						{
							//cout << " writing to new col " << endl;
							RETURN_ON_ERROR(saveBlock(column.dataFile.pFile, colBuf, startColFbo-1));
							dirty = false;
						}
						//cout << "reading from new col " << endl;
						RETURN_ON_ERROR(readBlock(column.dataFile.pFile, colBuf, startColFbo));
                        startColFbo++;
                        colBufOffset = 0;
					}
					if (nexValNeeded == 0)
					{
						int tmpBufOffset = 0;
						while((tmpBufOffset + refCol.colWidth) <= BYTE_PER_BLOCK)
                           
						{
							if (memcmp(refColBuf + tmpBufOffset, &refEmptyVal, refCol.colWidth) != 0) //Find the number of nextVal needed.
							{
								nexValNeeded++;
							//memcpy(colBuf + colBufOffset, defaultVal, column.colWidth);
							//dirty = true;
							}
						
							tmpBufOffset += refCol.colWidth;
						}
			
						//reserve the next value, should have a AI sequence in controller from DDLProc
						if (nexValNeeded > 0)
						{
							rc = BRMWrapper::getInstance()->getAutoIncrementRange(column.dataFile.fid, nexValNeeded, nextVal, errorMsg);
							if (rc != NO_ERROR)
								return rc;
						}
						nextValStart = nextVal;
					}
					//write the values to column
					
					
					//colBufOffset = 0; @Bug 5436. Need to handle the new column width is different from reference column
					while(((refBufOffset + refCol.colWidth) <= BYTE_PER_BLOCK) &&
                           ((colBufOffset + column.colWidth) <= BYTE_PER_BLOCK))
					{
						if (memcmp(refColBuf + refBufOffset, &refEmptyVal, refCol.colWidth) != 0) //Find the number of nextVal needed.
						{
							memcpy(defaultVal, &nextVal, 8);
							nextVal++;
							memcpy(colBuf + colBufOffset, defaultVal, column.colWidth);
							dirty = true;
						}
						
						refBufOffset += refCol.colWidth;
						colBufOffset += column.colWidth;
					}
				}			
				
				cpInfo.max = nextValStart + nexValNeeded -1;
				cpInfo.min = nextValStart;
				cpInfo.seqNum = 0;		
				
			}
			else
			{
				while(startRefColFbo <= lastRefHwm || startColFbo <= colHwm)
				{
					if ((refBufOffset + refCol.colWidth) > BYTE_PER_BLOCK)
					{
                    //If current reference column block is fully processed get to the next one
                    RETURN_ON_ERROR(refColOp->readBlock(refCol.dataFile.pFile, refColBuf, startRefColFbo));
                    startRefColFbo++;
                    refBufOffset = 0;
					}
					if ((colBufOffset + column.colWidth) > BYTE_PER_BLOCK)
					{
						//Current block of the new colum is full. Write it if dirty and then get the next block
						if (dirty)
						{
							RETURN_ON_ERROR(saveBlock(column.dataFile.pFile, colBuf, startColFbo-1));
							dirty = false;
						}
						RETURN_ON_ERROR(readBlock(column.dataFile.pFile, colBuf, startColFbo));
                        startColFbo++;
                        colBufOffset = 0;
					}

					while(((refBufOffset + refCol.colWidth) <= BYTE_PER_BLOCK) &&
                           ((colBufOffset + column.colWidth) <= BYTE_PER_BLOCK))
					{
						if (memcmp(refColBuf + refBufOffset, &refEmptyVal, refCol.colWidth) != 0)
						{
                           /*if (autoincrement)
						   {
								memcpy(defaultVal, &nextVal, 8);
								nextVal++;
						   } */
							memcpy(colBuf + colBufOffset, defaultVal, column.colWidth);
							dirty = true;
						}
						else if (column.compressionType !=0) //@Bug 3866, fill the empty row value for compressed chunk
						{
							memcpy(colBuf + colBufOffset, &emptyVal, column.colWidth);
							dirty = true;
						}
						refBufOffset += refCol.colWidth;
						colBufOffset += column.colWidth;
					}
				}
                if (isUnsigned(column.colDataType))
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
            }
            
			if (dirty)
            {
                RETURN_ON_ERROR(saveBlock(column.dataFile.pFile, colBuf, startColFbo - 1));
                dirty = false;
            }

            closeColumnFile(column);
            refColOp->closeColumnFile(refCol);
			std::map<FID,FID> oids;
            rc = flushFile(rc, oids);
			
			//Mark extents invalid first
			BRM::LBID_t    startLbid;
			rc = BRMWrapper::getInstance()->getStartLbid(column.dataFile.fid, column.dataFile.fPartition, column.dataFile.fSegment, colHwm, startLbid);
			if (autoincrement) //@Bug 4074. Mark it invalid first to set later
			{
				BRM::CPInfo cpInfo1;
                if (isUnsigned(column.colDataType))
                {
                    cpInfo1.max = 0;
                    cpInfo1.min = static_cast<int64_t>(numeric_limits<int64_t>::max());
                }
                else
                {
                    cpInfo1.max = numeric_limits<int64_t>::min();
                    cpInfo1.min = numeric_limits<int64_t>::max();
                }
				cpInfo1.seqNum = -1;
				cpInfo1.firstLbid = startLbid;
				BRM::CPInfoList_t cpinfoList1;
				cpinfoList1.push_back(cpInfo1);
				rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList1);
				if ( rc != NO_ERROR)
					return rc;
			}
			
			BRM::CPInfoList_t cpinfoList;
			cpInfo.firstLbid = startLbid;
			cpinfoList.push_back(cpInfo);
			//cout << "calling setExtentsMaxMin for startLbid = " << startLbid << endl;
			rc = BRMWrapper::getInstance()->setExtentsMaxMin(cpinfoList);
			if ( rc != NO_ERROR)
				return rc;
				
			//cout << "calling setLocalHWM for oid:hwm = " << column.dataFile.fid <<":"<<colHwm << endl;
            rc = BRMWrapper::getInstance()->setLocalHWM((OID)column.dataFile.fid,column.dataFile.fPartition,
				column.dataFile.fSegment,colHwm);	 
			if ( rc != NO_ERROR)
				return rc;
			//erase the entries from this dbroot.
			std::vector<struct BRM::EMEntry> refEntriesTrimed;
			for (uint m=0; m<refEntries.size(); m++)
			{
				if ((refEntries[0].partitionNum != refEntries[m].partitionNum) || (refEntries[0].segmentNum != refEntries[m].segmentNum))
					refEntriesTrimed.push_back(refEntries[m]);
			}
			refEntriesTrimed.swap(refEntries);
			iter = refEntries.begin();
		}
	}
	
    return rc;
}

   /***********************************************************
    * DESCRIPTION:
    *    Create a table file
    * PARAMETERS:
    *    tableFid - the file id for table bitmap file
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_EXIST if file exists
    *    ERR_FILE_CREATE if something wrong in creating the file
    ***********************************************************/
   int ColumnOp::createTable(/*const FID tableFid*/) const
   {
//      return createFile(tableFid, DEFAULT_TOTAL_BLOCK );
      return NO_ERROR;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Drop  column related files
    * PARAMETERS:
    *    dataFid - the file id for column data file
    *    bitmapFid - the file id for column bitmap file
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_NOT_EXIST if file not exist
    ***********************************************************/
   int ColumnOp::dropColumn(const FID dataFid) 
   {
      return deleteFile(dataFid);
   }

 
   /***********************************************************
    * DESCRIPTION:
    *    Drop  column and dictionary related files
    * PARAMETERS:
    *    dataFids - the file oids for column and dictionary data file
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_NOT_EXIST if file not exist
    ***********************************************************/
   int ColumnOp::dropFiles(const std::vector<int32_t>& dataFids)
   {
		return deleteFiles(dataFids);
   }

	int ColumnOp::dropPartitions(const std::vector<OID>& dataFids, 
	                             const std::vector<BRM::PartitionInfo>& partitions)
	{
		return deletePartitions(dataFids, partitions);
	}

   int ColumnOp::deleteOIDsFromExtentMap(const std::vector<int32_t>& dataFids)
   {
		int rc = 0;
		rc = BRMWrapper::getInstance()->deleteOIDsFromExtentMap(dataFids);
		return rc;
   }
   
/**************************************************************
 * DESCRIPTION:
 *    Add an extent to the specified column OID and DBRoot.
 *    Partition and segment number (and HWM) of the segment file containing
 *    the new extent are returned.
 * PARAMETERS:
 *    column    - input column attributes like OID and column width.
 *    leaveFileOpen - indicates whether db file is to be left open upon return
 *    firstFileOnPM - If first file on a PM, then first empty chunk is
 *        written out (if compressed), to give us a startup file on this PM,
 *        much like MySQL "create table" creates the very "first" file on a
 *        selected PM.
 *    hwm       - The HWM (or fbo) of the column segment file where the
 *                new extent begins.
 *    startLbid - The starting LBID for the new extent.
 *    allocSize - Number of blocks in new extent.
 *    dbRoot    - The DBRoot of the file with the new extent.
 *    partition - The partition num of the file with the new extent.
 *    segment   - The segment number of the file with the new extent.
 *    segFile   - Name of the segment file to which the extent is added.
 *    pFile     - FILE ptr to the file where the extent is added.
 *    newFile   - Indicates if extent is added to new or existing file.
 *    hdrs      - Contents of headers if file is compressed.
 * RETURN:
 *    NO_ERROR if success
 *    other number if fail
 **************************************************************/
int ColumnOp::extendColumn(
    const Column& column,
    bool         leaveFileOpen,
    bool         firstFileOnPM,
    HWM          hwm,
    BRM::LBID_t  startLbid,
    int          allocSize,
    uint16_t     dbRoot,
    uint32_t     partition,
    uint16_t     segment,
    std::string& segFile,
    FILE*&       pFile,
    bool&        newFile,
    char*        hdrs)
{
    uint64_t emptyVal = 0;

    emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
    int rc = extendFile(column.dataFile.fid,
                        emptyVal,
                        column.colWidth,
                        hwm,
                        startLbid,
                        allocSize,
                        dbRoot,
                        partition,
                        segment,
                        segFile,
                        pFile,
                        newFile,
                        hdrs);
    if (rc != NO_ERROR)
    {
        if ((!leaveFileOpen) && (pFile))
            closeFile( pFile );
        return rc;
    }

    // If application code specifies this is the first file for this PM, then
    // we write out an empty chunk to initialize the "startup" file on each PM.
    if (firstFileOnPM)
    {
        // DMC_TEMP
        // If firstFileOnPM is true, then newFile should have been set to true
        // by extendFile().  We might add validation for this at some point.
        if ((newFile) && (m_compressionType))
        {
            int nRows = compress::IDBCompressInterface::UNCOMPRESSED_INBUF_LEN /
                column.colWidth;
            if (hdrs)
            {
                rc = writeInitialCompColumnChunk( pFile,
                    allocSize, nRows, emptyVal, column.colWidth, hdrs );
            }
            else
            {
                char localHdrs[compress::IDBCompressInterface::HDR_BUF_LEN*2];
                rc = writeInitialCompColumnChunk( pFile,
                    allocSize, nRows, emptyVal, column.colWidth, localHdrs );
            }
        }

        // Only close file for DML/DDL; leave file open for bulkload
        if (!leaveFileOpen)
            closeFile( pFile );
    }
    else
    {
        // Only close file for DML/DDL; leave file open for bulkload
        if (!leaveFileOpen)
            closeFile( pFile );
    }

    return rc;
}

int ColumnOp::addExtent(
    const Column& column,
    uint16_t    dbRoot,
    uint32_t    partition,
    uint16_t    segment,
    std::string& segFile,
    BRM::LBID_t& startLbid,
    bool&        newFile,
    int&         allocSize,
    char*        hdrs)
{
    uint64_t emptyVal = 0;

    emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
    int rc = addExtentExactFile(column.dataFile.fid,
                        emptyVal,
                        column.colWidth,
                        allocSize,
                        dbRoot,
                        partition,
                        segment,
                        column.colDataType,
                        segFile,
                        startLbid,
                        newFile,
                        hdrs);
    return rc;
}

   /***********************************************************
    * DESCRIPTION:
    *    Expand the current abbreviated extent in the column file to a full
    *    extent.
    * PARAMETERS:
    *    column    - input column attributes like OID and column width.
    * RETURN:
    *    NO_ERROR if success
    *    other number if fail
    ***********************************************************/
   int ColumnOp::expandAbbrevExtent(const Column& column)
   {
      uint64_t emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
	  int rc = expandAbbrevColumnExtent(column.dataFile.pFile,
                                         column.dataFile.fDbRoot,
                                         emptyVal,
                                         column.colWidth);

	  return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Get column data type
    * PARAMETERS:
    *    name - type name
    * RETURN:
    *    true if success, false otherwise
    ***********************************************************/
   bool ColumnOp::getColDataType(const char* name, CalpontSystemCatalog::ColDataType& colDataType) const
   {
      bool bFound = false;
      for(int i = 0; i < CalpontSystemCatalog::NUM_OF_COL_DATA_TYPE; i++)
         if (strcmp(name, ColDataTypeStr[i]) == 0) {
            colDataType = static_cast<CalpontSystemCatalog::ColDataType>(i);
            bFound = true;
            break;
         }

      return bFound;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Initialize a column
    * PARAMETERS:
    *    column - current column
    * RETURN:
    *    none
    ***********************************************************/
   void ColumnOp::initColumn(Column& column) const
   {
      setColParam(column);
      column.dataFile.pFile = NULL;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Check whether the row is empty
    * PARAMETERS:
    *    buf - data buffer
    *    offset - buffer offset
    *    column - current column
    * RETURN:
    *    true if success, false otherwise
    ***********************************************************/
   bool ColumnOp::isEmptyRow(unsigned char* buf, int offset, const Column& column)
   {
      bool emptyFlag = true;
      uint64_t  curVal, emptyVal;

      memcpy(&curVal, buf + offset*column.colWidth, column.colWidth);
      emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
      if (/*curVal != emptyVal*/memcmp(&curVal, &emptyVal, column.colWidth))
         emptyFlag = false;

      return emptyFlag;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Check whether column parameters are valid
    * PARAMETERS:
    *    column - current column
    * RETURN:
    *    true if success, false otherwise
    ***********************************************************/
   bool ColumnOp::isValid(Column& column) const
   {
      return /*column.colNo > 0 && */ column.colWidth > 0 ;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Open all column related files
    * PARAMETERS:
    *    column - column (includes the file id for column data file,
    *             as well as the DBRoot, partition, and segment number)
    *    segFile- is set to the name of the column segment file
    *             that is opened.
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_READ if something wrong in reading the file
    ***********************************************************/
   int ColumnOp::openColumnFile(Column& column,
      std::string& segFile,
      int ioBuffSize) const
   {
      if (!isValid(column))
         return ERR_INVALID_PARAM;

      // open column data file
      column.dataFile.pFile = openFile(column,
         column.dataFile.fDbRoot,
         column.dataFile.fPartition,
         column.dataFile.fSegment,
         column.dataFile.fSegFileName,
         "r+b", ioBuffSize);
      segFile = column.dataFile.fSegFileName;
      if (column.dataFile.pFile == NULL)
	  {
		ostringstream oss;
		oss << "oid: " << column.dataFile.fid << " with path " << segFile;
		logging::Message::Args args;
		logging::Message message(1);
		args.add("Error opening file ");
		args.add(oss.str());
		args.add("");
		args.add("");
		message.format(args);
		logging::LoggingID lid(21);
        logging::MessageLog ml(lid);

        ml.logErrorMessage( message );
         return ERR_FILE_OPEN;
		 
	}

      // open column bitmap file
/*      column.bitmapFile.pFile = openFile(column.bitmapFile.fid);
      if (column.bitmapFile.pFile == NULL) {
         closeFile(column.dataFile.pFile );         // clear previous one
         column.dataFile.pFile = NULL;
         return ERR_FILE_OPEN;
      }
*/
      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Open all table related files
    * PARAMETERS:
    *    table - table structure
    * RETURN:
    *    NO_ERROR if success
    *    ERR_FILE_READ if something wrong in reading the file
    ***********************************************************/
/*   int ColumnOp::openTableFile() const
   {
      // open table bitmap file
      return NO_ERROR;
   }
*/

   /***********************************************************
    * DESCRIPTION:
    *    Set column parameters
    * PARAMETERS:
    *    column - current column
    *    colNo - column no
    *    colWidth - column width
    * RETURN:
    *    none
    ***********************************************************/
   void ColumnOp::setColParam(Column& column,
      int       colNo,
      int       colWidth,
      CalpontSystemCatalog::ColDataType colDataType,
      ColType   colType,
      FID       dataFid,
      int       compressionType,
      u_int16_t dbRoot,
      u_int32_t partition,
      u_int16_t segment) const
   {
      column.colNo = colNo;
      column.colWidth = colWidth;
      column.colType = colType;
      column.colDataType = colDataType;

      column.dataFile.fid = dataFid;
      column.dataFile.fDbRoot    = dbRoot;
      column.dataFile.fPartition = partition;
      column.dataFile.fSegment   = segment;

      column.compressionType = compressionType;
   }


   /***********************************************************
    * DESCRIPTION:
    *    Write row(s)
    * PARAMETERS:
    *    curCol - column information
    *    totalRow - the total number of rows need to be inserted
    *    rowIdArray - the array of row id, for performance purpose, I am assuming the rowIdArray is sorted
    *    valArray - the array of row values
    *    oldValArray - the array of old value
    * RETURN:
    *    NO_ERROR if success, other number otherwise
    ***********************************************************/
   int ColumnOp::writeRow(Column& curCol, uint64_t totalRow, const RID* rowIdArray, const void* valArray, const void* oldValArray, bool bDelete )
   {
      uint64_t i = 0, curRowId;
      int      dataFbo, dataBio, curDataFbo = -1;
      unsigned char  dataBuf[BYTE_PER_BLOCK]; 
      bool     bExit = false, bDataDirty = false; 
      void*    pVal = 0;
//      void*    pOldVal;
      char     charTmpBuf[8];
      uint64_t  emptyVal;
	  int rc = NO_ERROR;
	  
      while(!bExit) {
         curRowId = rowIdArray[i];

         calculateRowId(curRowId, BYTE_PER_BLOCK/curCol.colWidth, curCol.colWidth, dataFbo, dataBio);
         // load another data block if necessary
         if (curDataFbo != dataFbo) {
            if (bDataDirty) {
               rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			   if ( rc != NO_ERROR)
					return rc;
               bDataDirty = false;
            }

            curDataFbo = dataFbo;
            rc = readBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			if ( rc != NO_ERROR)
				return rc;
            bDataDirty = true;
         }

         // This is a awkward way to convert void* and get ith element, I just don't have a good solution for that
         // How about pVal = valArray + i*curCol.colWidth?
        switch (curCol.colType)
        {
//               case WriteEngine::WR_LONG :   pVal = &((long *) valArray)[i]; break;
            case WriteEngine::WR_FLOAT :
                if (!bDelete) pVal = &((float *) valArray)[i];
                //pOldVal = &((float *) oldValArray)[i];
                break;
            case WriteEngine::WR_DOUBLE : 
                if (!bDelete) pVal = &((double *) valArray)[i];
                //pOldVal = &((double *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   
                if (!bDelete)
                {
                    memcpy(charTmpBuf, (char*)valArray + i*8, 8);
                    pVal = charTmpBuf;
                }
                //pOldVal = (char*)oldValArray + i*8;
                break;
//            case WriteEngine::WR_BIT :    pVal = &((bool *) valArray)[i]; break;
            case WriteEngine::WR_SHORT :  
                if (!bDelete) pVal = &((short *) valArray)[i];
                //pOldVal = &((short *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_BYTE :   
                if (!bDelete) pVal = &((char *) valArray)[i];
                //pOldVal = &((char *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_LONGLONG:
                if (!bDelete) pVal = &((long long *) valArray)[i];
                //pOldVal = &((long long *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_TOKEN:   
                if (!bDelete) pVal = &((Token *) valArray)[i];
                //pOldVal = &((Token *) oldValArray)[i];
                break;
            case WriteEngine::WR_INT :
                if (!bDelete) pVal = &((int *) valArray)[i];
                //pOldVal = &((int *) oldValArray)[i];
                break;
            case WriteEngine::WR_USHORT:  
                if (!bDelete) pVal = &((uint16_t *) valArray)[i];
                //pOldVal = &((uint16_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_UBYTE :  
                if (!bDelete) pVal = &((uint8_t *) valArray)[i];
                //pOldVal = &((uint8_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_UINT :  
                if (!bDelete) pVal = &((uint32_t *) valArray)[i];
                //pOldVal = &((uint8_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_ULONGLONG:
                if (!bDelete) pVal = &((uint64_t *) valArray)[i];
                //pOldVal = &((uint64_t *) oldValArray)[i]; 
                break;
            default  :
                if (!bDelete) pVal = &((int *) valArray)[i];
                //pOldVal = &((int *) oldValArray)[i];
                break;
        }
         
         // This is the stuff to retrieve old value
         //memcpy(pOldVal, dataBuf + dataBio, curCol.colWidth);

         if (bDelete) {
            emptyVal = getEmptyRowValue(curCol.colDataType, curCol.colWidth);
            pVal = &emptyVal;
         }

         // This is the write stuff
         writeBufValue(dataBuf + dataBio, pVal, curCol.colWidth);

         i++;
         if (i >= totalRow)
            bExit = true;
      }

      // take care of the cleanup
      if (bDataDirty && curDataFbo >= 0)
         rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);

      return rc;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Write rows
    * PARAMETERS:
    *    curCol - column information
    *    totalRow - the total number of rows need to be inserted
    *    ridList - the vector of row id
    *    valArray - the array of one row value
    *    oldValArray - the array of old value
    * RETURN:
    *    NO_ERROR if success, other number otherwise
    ***********************************************************/
   int ColumnOp::writeRows(Column& curCol, uint64_t totalRow, const RIDList& ridList, const void* valArray, const void* oldValArray, bool bDelete )
   {
      uint64_t i = 0, curRowId;
      int      dataFbo, dataBio, curDataFbo = -1;
      unsigned char  dataBuf[BYTE_PER_BLOCK]; 
      bool     bExit = false, bDataDirty = false; 
      void*    pVal = 0;
      //void*    pOldVal;
      char     charTmpBuf[8];
      uint64_t  emptyVal;
	  int rc = NO_ERROR;
	 
      while(!bExit) {
         curRowId = ridList[i];

         calculateRowId(curRowId, BYTE_PER_BLOCK/curCol.colWidth, curCol.colWidth, dataFbo, dataBio);
         // load another data block if necessary
         if (curDataFbo != dataFbo) {
            if (bDataDirty) {
               rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			   if ( rc != NO_ERROR)
					return rc;
               fflush(curCol.dataFile.pFile);
               bDataDirty = false;
            }

            curDataFbo = dataFbo;
			//@Bug 4849. need to check error code to prevent disk error			
            rc = readBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			if ( rc != NO_ERROR)
				return rc;
            bDataDirty = true;
         }

         // This is a awkward way to convert void* and get ith element, I just don't have a good solution for that
         // How about pVal = valArray? You're always getting the 0'th element here anyways.
        switch (curCol.colType)
        {
//               case WriteEngine::WR_LONG :   pVal = &((long *) valArray)[i]; break;
            case WriteEngine::WR_FLOAT :  
                if (!bDelete) pVal = &((float *) valArray)[0];
                //pOldVal = &((float *) oldValArray)[i];
                break;
            case WriteEngine::WR_DOUBLE : 
                if (!bDelete) pVal = &((double *) valArray)[0];
                //pOldVal = &((double *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   
                if (!bDelete)
                {
                    memcpy(charTmpBuf, (char*)valArray, 8);
                    pVal = charTmpBuf;
                }
                //pOldVal = (char*)oldValArray + i*8;
                break;
//          case WriteEngine::WR_BIT :    pVal = &((bool *) valArray)[i]; break;
            case WriteEngine::WR_SHORT :  
                if (!bDelete) pVal = &((short *) valArray)[0];
                //pOldVal = &((short *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_BYTE :   
                if (!bDelete) pVal = &((char *) valArray)[0];
                //pOldVal = &((char *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_LONGLONG:
                if (!bDelete) pVal = &((long long *) valArray)[0];
                //pOldVal = &((long long *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_TOKEN:    if (!bDelete) pVal = &((Token *) valArray)[0];
                //pOldVal = &((Token *) oldValArray)[i];
                break;
            case WriteEngine::WR_INT :
                if (!bDelete) pVal = &((int *) valArray)[0];
                //pOldVal = &((int *) oldValArray)[i];
                break;
            case WriteEngine::WR_USHORT :  
                if (!bDelete) pVal = &((uint16_t *) valArray)[0];
                //pOldVal = &((uint16_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_UBYTE :   
                if (!bDelete) pVal = &((uint8_t *) valArray)[0];
                //pOldVal = &((uint8_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_ULONGLONG:
                if (!bDelete) pVal = &((uint64_t *) valArray)[0];
                //pOldVal = &((uint64_t *) oldValArray)[i]; 
                break;
            case WriteEngine::WR_UINT :
                if (!bDelete) pVal = &((uint32_t *) valArray)[0];
                //pOldVal = &((uint32_t *) oldValArray)[i];
                break;
            default  :                    
                if (!bDelete) pVal = &((int *) valArray)[0];
                //pOldVal = &((int *) oldValArray)[i];
                break;
        }
         
         // This is the stuff to retrieve old value
         //memcpy(pOldVal, dataBuf + dataBio, curCol.colWidth);

         if (bDelete) {
            emptyVal = getEmptyRowValue(curCol.colDataType, curCol.colWidth);
            pVal = &emptyVal;
         }

         // This is the write stuff
         writeBufValue(dataBuf + dataBio, pVal, curCol.colWidth);

         i++;
         if (i >= totalRow)
            bExit = true;
      }

      // take care of the cleanup
      if (bDataDirty && curDataFbo >= 0)
	  {
		//@Bug 4849. need to check error code to prevent disk error
        rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);	
	  }
      fflush(curCol.dataFile.pFile);

      return rc;
   }

  /***********************************************************
    * DESCRIPTION:
    *    Write rows
    * PARAMETERS:
    *    curCol - column information
    *    totalRow - the total number of rows need to be inserted
    *    ridList - the vector of row id
    *    valArray - the array of one row value
    *    oldValArray - the array of old value
    * RETURN:
    *    NO_ERROR if success, other number otherwise
    ***********************************************************/
   int ColumnOp::writeRowsValues(Column& curCol, uint64_t totalRow, const RIDList& ridList, const void* valArray )
   {
      uint64_t i = 0, curRowId;
      int      dataFbo, dataBio, curDataFbo = -1;
      unsigned char  dataBuf[BYTE_PER_BLOCK]; 
      bool     bExit = false, bDataDirty = false; 
      void*    pVal = 0;
      //void*    pOldVal;
      char     charTmpBuf[8];
	  int rc = NO_ERROR;
      while(!bExit) {
         curRowId = ridList[i];

         calculateRowId(curRowId, BYTE_PER_BLOCK/curCol.colWidth, curCol.colWidth, dataFbo, dataBio);
         // load another data block if necessary
         if (curDataFbo != dataFbo) {
            if (bDataDirty) {
               rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			   if ( rc != NO_ERROR)
					return rc;
               bDataDirty = false;
            }

            curDataFbo = dataFbo;
            rc = readBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
			if ( rc != NO_ERROR)
				return rc;
            bDataDirty = true;
         }

         // This is a awkward way to convert void* and get ith element, I just don't have a good solution for that
        switch (curCol.colType)
        {
            case WriteEngine::WR_FLOAT :  
                pVal = &((float *) valArray)[i]; 
                break;
            case WriteEngine::WR_DOUBLE : 
                pVal = &((double *) valArray)[i]; 
                break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   
                {
                    memcpy(charTmpBuf, (char*)valArray+i*8, 8);
                    pVal = charTmpBuf;
                }
                break;
            case WriteEngine::WR_SHORT :  
                pVal = &((short *) valArray)[i]; 
                break;
            case WriteEngine::WR_BYTE :   
                pVal = &((char *) valArray)[i];  
                break;
            case WriteEngine::WR_LONGLONG:
                pVal = &((long long *) valArray)[i]; 
                break;
            case WriteEngine::WR_TOKEN:   
                pVal = &((Token *) valArray)[i];
                break;
            case WriteEngine::WR_INT :
                pVal = &((int *) valArray)[i]; 
                break;
            case WriteEngine::WR_USHORT :  
                pVal = &((uint16_t *) valArray)[i]; 
                break;
            case WriteEngine::WR_UBYTE :   
                pVal = &((uint8_t *) valArray)[i];  
                break;
            case WriteEngine::WR_ULONGLONG:
                pVal = &((uint64_t *) valArray)[i]; 
                break;
            case WriteEngine::WR_UINT :
                pVal = &((uint32_t *) valArray)[i]; 
                break;
            default  :                    
                pVal = &((int *) valArray)[i]; 
                break;
        }         

         // This is the write stuff
         writeBufValue(dataBuf + dataBio, pVal, curCol.colWidth);

         i++;
         if (i >= totalRow)
            bExit = true;
      }

      // take care of the cleanup
      if (bDataDirty && curDataFbo >= 0)
	  {
         rc = saveBlock(curCol.dataFile.pFile, dataBuf, curDataFbo);
	  }

      return rc;
   }


} //end of namespace

