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

//  $Id: we_colop.cpp 4282 2012-10-29 16:31:57Z chao $

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
   int ColumnOp::allocRowId(Column& column, uint64_t totalRow, RID* rowIdArray, int& hwm, bool& newExtent, Column& newCol, uint64_t& rowsLeft, HWM& newHwm, bool& newFile, bool insertSelect)
   {
   	//MultiFiles per OID: always append the rows to the end for now.
 /*  	
      int      totalBlock, i, j, counter = 0, totalRowPerBlock;
      bool     bExit = false; //, bDirty = false;
      unsigned char  buf[BYTE_PER_BLOCK];

      totalBlock = blocksInFile(column.dataFile.pFile);

      for(i = 0; i < totalBlock && !bExit; i++) {
         readBlock(column.dataFile.pFile, buf, i);

         totalRowPerBlock = BYTE_PER_BLOCK/column.colWidth;
         for(j = 0; j < totalRowPerBlock && !bExit; j++) 
            if (isEmptyRow(buf, j, column)) {
               rowIdArray[counter] = getRowId(i, column.colWidth, j);

               counter++;
               if (counter >= totalRow) {
                  if (i > hwm)
                     hwm = i;

                  bExit = true;
               }
            }
      }
*/
	  // See if the current HWM block might be in an abbreviated extent that
	  // needs to be expanded, if we end up adding enough rows.
	  bool bCheckAbbrevExtent      = false;
	  int  numBlksPerInitialExtent = INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK * column.colWidth;
	  if ((column.dataFile.fPartition == 0) &&
          (column.dataFile.fSegment   == 0) &&
	      ((hwm+1) <= numBlksPerInitialExtent))
              bCheckAbbrevExtent = abbreviatedExtent(column.dataFile.pFile, column.colWidth);

	  //The current existed rows upto hwm
	  int counter = 0;
	  uint64_t totalRowPerBlock = BYTE_PER_BLOCK/column.colWidth;
	  uint64_t currentRows = totalRowPerBlock * hwm;
	  unsigned extentRows = BRMWrapper::getInstance()->getExtentRows();
	  int numExtentsFilled = currentRows / extentRows;
	  uint64_t rowsAvailable = extentRows - (numExtentsFilled * extentRows);
	  rowsLeft = totalRow < rowsAvailable ? 0 : totalRow - rowsAvailable;
	  newExtent = false;
	  uint j = 0, rowsallocated = 0;
	  int rc = 0;
	  newFile = false;
	  unsigned char  buf[BYTE_PER_BLOCK];
		
		// ZZ. For insert select, skip the hwm block and start inserting from the next block
		// to avoid self insert issue.
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
             BRM::LBID_t startLbid;
			 RETURN_ON_ERROR(extendColumn (
				column, false, pFile, dbRoot, partition, segment,
                segFile, newHwm, startLbid, newFile, allocSize));
	 
			 newExtent = true;
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
			//RETURN_ON_ERROR(BRMWrapper::getInstance()->setLocalHWM_HWMt(column.dataFile.fid, partition, segment, newHwm)); 
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
		ColDataType colDataType,
		ColType colType,
		FID dataFid,
		uint16_t dbRoot,
        uint32_t partition)
   {
      int rc, newWidth, allocSize;
      i64 emptyVal = 0;
     
      setColParam(column, colNo, colWidth, colDataType, colType);
      emptyVal = getEmptyRowValue(colDataType, colWidth);
      newWidth = getCorrectRowWidth(colDataType, colWidth);
      column.dataFile.fid = dataFid;
      column.dataFile.fDbRoot    = dbRoot;
      column.dataFile.fPartition = partition;
      column.dataFile.fSegment   = 0;
      rc = createFile(column.dataFile.fid, allocSize, dbRoot, partition, emptyVal, newWidth);
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
   int ColumnOp::fillColumn(Column& column, Column& refCol, void * defaultVal, long long & nextVal, const OID dictOid, const int dictColWidth)
   {
      unsigned char refColBuf[BYTE_PER_BLOCK]; //Refernce column buffer
      unsigned char colBuf[BYTE_PER_BLOCK];
      bool dirty = false;
      int refHwm = 0;
      int colHwm = 0;
      RID maxRowId = 0;

      i64 emptyVal;
      i64 refEmptyVal;

      long long startColFbo = 0;
      long long startRefColFbo = 0;
      long long endColFbo = 0;
      long long endRefColFbo = 0;

      int refBufOffset = 0;
      int colBufOffset = 0;
	  bool autoincrement = false;
	  if (nextVal > 0)
		autoincrement = true;
		
      //Get the last partition and the last segment number and the last dbRoot number
      // from the reference column oid
      u_int16_t  lastDBRoot;
      u_int32_t  lastPartition;
      u_int16_t  totalSegments = 0;
      u_int16_t  lastSegment;
      int        lastHwm;
      int        rc = 0;
      uint16_t   colStartDBRoot = column.dataFile.fDbRoot;
      uint16_t   currentDBRoot;
      u_int32_t  startPartition;
      u_int16_t  lastSegmentThisPart;
	  std::vector<struct BRM::EMEntry> entries;
	  vector<struct BRM::EMEntry>::iterator iter;
	  typedef std::tr1::unordered_map<u_int32_t, u_int32_t> PartitionMap_t;
	  PartitionMap_t partitionMap;
      rc = BRMWrapper::getInstance()->getStartExtent(refCol.dataFile.fid, colStartDBRoot, startPartition, true);
	  rc = BRMWrapper::getInstance()->getExtents(refCol.dataFile.fid, entries, false,false,true);
	  //@Bug 3657. For autoincrement column, mark the first extent invalid
	 
	  BRM::LBID_t    startLbid;
	  rc = BRMWrapper::getInstance()->getStartLbid(column.dataFile.fid, startPartition, 0, 0, startLbid);
	  boost::scoped_ptr<BRM::DBRM> dbrmp(new BRM::DBRM());
	  dbrmp->setExtentMaxMin(startLbid, numeric_limits<int64_t>::min()+1, numeric_limits<int64_t>::max()-1, -1);
			
	  
	  if (entries.size() > 0) //Find out which partition is dropped. 
	  {
			iter = entries.begin();
			for (;iter != entries.end(); ++iter)
			{
				partitionMap[iter->partitionNum] = iter->partitionNum;
			}
	  }

      unsigned dbrootCnt = Config::DBRootCount();
      unsigned extentRows = BRMWrapper::getInstance()->getExtentRows();

      rc = BRMWrapper::getInstance()->getLastLocalHWM_int(refCol.dataFile.fid, lastDBRoot, lastPartition, lastSegment, lastHwm);

      std::vector<RefcolInfo>   refColInfo;
      unsigned                  currentnumExtent = 0;
      boost::scoped_ptr<Dctnry> dctnry;
      if (m_compressionType == 0)
         dctnry.reset(new DctnryCompress0);
      else
         dctnry.reset(new DctnryCompress1);
      emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
      refEmptyVal = getEmptyRowValue(refCol.colDataType, refCol.colWidth);
      lastSegmentThisPart = lastSegment;
	  PartitionMap_t::const_iterator it;
      for (u_int32_t i = startPartition; i <= lastPartition; i++)
      {
		 it = partitionMap.find (i);
		 if (it == partitionMap.end())
			continue;
		 //extend the column first
		if (i > startPartition) //First one is created already
		{
			u_int16_t   dbRoot1;
			u_int32_t   partition1 = i;
			u_int16_t   segment1;
			FILE*       pFile1 = NULL;
			std::string segFile1;
			HWM         hwm;
			bool        newFile;
			BRM::LBID_t startLbid;
			int		 allocSize = 0;
			RETURN_ON_ERROR(extendColumn(column, false, pFile1,
                                         dbRoot1, partition1, segment1,
                                         segFile1, hwm, startLbid, newFile, allocSize)) ;

			//@Bug 2636, 3257 Check if need to create store file
			if ((dictOid > 0) && newFile )
			{
				BRM::LBID_t startLbid;
				RETURN_ON_ERROR(dctnry->createDctnry(dictOid, dictColWidth,
                                                      dbRoot1, partition1, segment1, startLbid, newFile));
			}
		}
         refColInfo.clear();
         u_int16_t numExtentsFilled = Config::getExtentsPerSegmentFile();
         if (i == lastPartition) //@Bug 2582 Need to know whether all segements in this partition has been filled at least one extent.
         {
            uint64_t totalRowPerBlock = BYTE_PER_BLOCK/refCol.colWidth;
            uint64_t currentRows = totalRowPerBlock * (lastHwm+1); //hwm starts from 0

            numExtentsFilled = (currentRows / extentRows);
            if (currentRows%extentRows > 0)
               numExtentsFilled++;

            if (numExtentsFilled > 1)
            {
               totalSegments = Config::getFilesPerColumnPartition();
               lastSegmentThisPart = totalSegments;
            }
            else
            {
               totalSegments = lastSegment + 1; //Segment start from 0
               lastSegmentThisPart = totalSegments;
            }
         }
         else
         {
            totalSegments = Config::getFilesPerColumnPartition();
            //the loop counter starts from 0
            lastSegmentThisPart = totalSegments;
         }
         //   std::cout << "numExtentsFilled:   totalSegments:lastSegmentThisPart = " << numExtentsFilled<<":"<< totalSegments<<":"<<lastSegmentThisPart<<std::endl;
         for (u_int16_t stripe = 0; stripe < numExtentsFilled; stripe++)
         {
            if ((stripe == (numExtentsFilled-1)) && (i == lastPartition))
				lastSegmentThisPart = lastSegment+1;

            for (u_int16_t j=0; j < totalSegments; j++)
            {
               //   std::cout << "now j:lastSegmentThisPart:stripe " << j <<":" << lastSegmentThisPart <<":" << stripe<< std::endl;
               if ((stripe == (numExtentsFilled-1)) && (j > lastSegmentThisPart))
                  break;

               boost::scoped_ptr<ColumnOp> refColOp;
               if (refCol.compressionType != 0)
                  refColOp.reset(new ColumnOpCompress1);
               else
                  refColOp.reset(new ColumnOpCompress0);

               //get correct dbroot
               currentDBRoot = ((colStartDBRoot-1 + j) % dbrootCnt) + 1;
               std::string segFile;
               column.dataFile.fDbRoot = refCol.dataFile.fDbRoot = currentDBRoot;
               column.dataFile.fPartition = refCol.dataFile.fPartition = i;
               column.dataFile.fSegment = refCol.dataFile.fSegment = j;
               //std::cout << "openning file i:j:fid:dbroot = " << i<<":"<<j<<":"<<column.dataFile.fid<<":"<<currentDBRoot<<std::endl;
               RETURN_ON_ERROR(openColumnFile(column, segFile));

               //std::cout << " filling partition:segment:stripe = " << i <<":" << j <<":" << stripe << std::endl;
               std::string segFileRef;
               RETURN_ON_ERROR(refColOp->openColumnFile(refCol, segFileRef));

               //Computing the highest row-id in the reference column
               //1. maxRowId is initialized to the row-id corresponding to last row in (hwm-1) block
               //2. The hwm block is read and highest row-id is incremented to the last non-empty value in the hwm block

               rc = BRMWrapper::getInstance()->getLocalHWM_int((OID)refCol.dataFile.fid,i, j, refHwm);
               RefcolInfo refcolinfo;
               if (refHwm > 0) {
                  //maxRowId = (refHwm * BYTE_PER_BLOCK)/refCol.colWidth;
                  refcolinfo.localHwm = refHwm;
                  refcolinfo.numExtents = ((refHwm + 1) * (BYTE_PER_BLOCK /refCol.colWidth) / extentRows);
                  refColInfo.push_back(refcolinfo);
               }
               else
               {
                  //@Bug 1762.
                  refcolinfo.numExtents = 0;
               }

               rc = BRMWrapper::getInstance()->getLocalHWM_int((OID)column.dataFile.fid,i, j, colHwm);
               currentnumExtent = (colHwm+1) * (BYTE_PER_BLOCK /column.colWidth) / extentRows;
               //std::cout <<"here" << std::endl;

               // If we are processing the first extent in the first segment
               // file, we check to see if we have enough rows (256K) to re-
               // quire expanding the initial abbrev extent for the new column.
               if ((stripe == 0) &&
                   (i      == 0) &&
                   (j      == 0))
               {
			   //@Bug3565 use ref colwidth to calculate.
                  int numBlksForFirstExtent =
                     (INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK) * refCol.colWidth;
                  if ((refHwm+1) > numBlksForFirstExtent)
                  {
                     column.dataFile.fDbRoot = currentDBRoot;
                     RETURN_ON_ERROR(expandAbbrevExtent(column));
                  }
               }

               if (currentnumExtent < refcolinfo.numExtents)
               {
                  //Fill this extent with default or null value
                  startRefColFbo = currentnumExtent * extentRows * refCol.colWidth / BYTE_PER_BLOCK;
                  startColFbo = currentnumExtent * extentRows * column.colWidth / BYTE_PER_BLOCK;
                  endRefColFbo = (currentnumExtent+1) * extentRows * refCol.colWidth / BYTE_PER_BLOCK;
                  endColFbo = (currentnumExtent+1) * extentRows * column.colWidth / BYTE_PER_BLOCK;
                  //Initizliaing to BYTE_PER_BLOCK to force read the first time
                  refBufOffset = BYTE_PER_BLOCK;
                  colBufOffset = BYTE_PER_BLOCK;
                  dirty = false;
                  while(startRefColFbo < endRefColFbo || startColFbo < endColFbo)
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
                           RETURN_ON_ERROR(saveBlock(column.dataFile.pFile, colBuf, startColFbo - 1));
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
                           //printf("setting dirty to ture");
						   if (autoincrement)
						   {
								memcpy(defaultVal, &nextVal, 8);
								nextVal++;
						   }
						   
						   memcpy(colBuf + colBufOffset, defaultVal, column.colWidth);
                           dirty = true;

                        }
						else if (column.compressionType !=0)
						{
							memcpy(colBuf + colBufOffset, &emptyVal, column.colWidth);
							dirty = true;
						}
						
                        refBufOffset += refCol.colWidth;
                        colBufOffset += column.colWidth;
                     }
                  }
                  //Compute hwm for the new column
                  if (colHwm == 0)
                     colHwm += ((extentRows * column.colWidth) / BYTE_PER_BLOCK)-1;
                  else
                     colHwm += ((extentRows * column.colWidth) / BYTE_PER_BLOCK);

                  //std::cout << "Before hwm" << std::endl;
                  rc = BRMWrapper::getInstance()->setLocalHWM_int((OID)column.dataFile.fid,i,j,colHwm);
                  //std::cout << "After hwm" << std::endl;
                  if ((i != lastPartition) || (j !=  lastSegment) || stripe != (numExtentsFilled-1))
                  {
                     //extend the column
                     //std::cout<<"extending column" <<std::endl;
					 if ((stripe == (numExtentsFilled-1)) && (j == lastSegmentThisPart-1)) { }
					 else {
						u_int16_t   dbRoot1;
						u_int32_t   partition1 = i;
						u_int16_t   segment1;
						FILE*       pFile1 = NULL;
						std::string segFile1;
						HWM         hwm;
						bool        newFile;
						BRM::LBID_t startLbid;
						int		 allocSize = 0;
						RETURN_ON_ERROR(extendColumn(column, false, pFile1,
                                                  dbRoot1, partition1, segment1,
                                                  segFile1, hwm, startLbid, newFile, allocSize)) ;

						//@Bug 2636, 3257 Check if need to create store file
						if ((dictOid > 0) && newFile )
						{
							BRM::LBID_t startLbid;
							RETURN_ON_ERROR(dctnry->createDctnry(dictOid, dictColWidth,
                                                             dbRoot1, partition1, segment1, startLbid, newFile));
						}
					 }
                  }
               }
               else
               {
                  //std::cout <<"in else" << std::endl;
                  //Fill until last row.
                  RETURN_ON_ERROR(refColOp->readBlock(refCol.dataFile.pFile, refColBuf, refHwm));
                  refBufOffset = BYTE_PER_BLOCK - refCol.colWidth;
                  maxRowId = (refHwm * BYTE_PER_BLOCK)/refCol.colWidth; //Local maxRowId
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
                  startRefColFbo = currentnumExtent * extentRows * refCol.colWidth / BYTE_PER_BLOCK;
                  startColFbo = currentnumExtent * extentRows * column.colWidth / BYTE_PER_BLOCK;
                  //Initizliaing to BYTE_PER_BLOCK to force read the first time
                  refBufOffset = BYTE_PER_BLOCK;
                  colBufOffset = BYTE_PER_BLOCK;
                  dirty = false;
                  while(startRefColFbo <= refHwm || startColFbo <= colHwm)
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
                           if (autoincrement)
						   {
								memcpy(defaultVal, &nextVal, 8);
								nextVal++;
						   }
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
                  rc = BRMWrapper::getInstance()->setLocalHWM_int((OID)column.dataFile.fid,i,j,colHwm);

                  //not extendColumn if it is last partition and last segment file
                  if ((i != lastPartition) || (j !=  lastSegment) || (stripe != numExtentsFilled-1))
                  {
					 if ( lastSegmentThisPart != j ) {
						//extend the column
						u_int16_t   dbRoot1;
						u_int32_t   partition1 = i;
						u_int16_t   segment1;
						FILE*       pFile1 = NULL;
						std::string segFile1;
						HWM         hwm;
						bool        newFile;
						BRM::LBID_t startLbid;
						int		   allocSize = 0;
						RETURN_ON_ERROR(extendColumn(column, false, pFile1,
                                                  dbRoot1, partition1, segment1,
                                                  segFile1, hwm, startLbid, newFile, allocSize));
						//@Bug 2636,3257 Check if need to create store file
						if ((dictOid > 0) && newFile )
						{
							BRM::LBID_t startLbid;
							RETURN_ON_ERROR(dctnry->createDctnry(dictOid, dictColWidth,
                                                             dbRoot1, partition1, segment1, startLbid, newFile));
						}
					}
                  }
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
            }
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

   int ColumnOp::dropPartition(const std::vector<int32_t>& dataFids, const uint32_t partition)
   {
		return deletePartition(dataFids, partition);
   }
   
   int ColumnOp::deleteOIDsFromExtentMap(const std::vector<int32_t>& dataFids)
   {
		int rc = 0;
		rc = BRMWrapper::getInstance()->deleteOIDsFromExtentMap(dataFids);
		return rc;
   }
   
   /***********************************************************
    * DESCRIPTION:
    *    Add an extent to the specified column OID.  The specific DBRoot,
    *    partition, and segment number (and HWM) of the segment file containing
    *    the new extent are returned.
    * PARAMETERS:
    *    column    - input column attributes like OID and column width.
    *    isbulkload- indicates if this is a bulkload job or not.
    *    pFile     - FILE ptr to the file where the extent is added.
    *    dbRoot    - The DBRoot of the file with the new extent.
    *    partition - The partition num of the file with the new extent.
    *    segment   - The segment number of the file with the new extent.
    *    segFile   - Name of the segment file to which the extent is added.
    *    hwm       - The HWM (or fbo) of the column segment file where the
    *                new extent begins.
    *    startLbid - The starting LBID for the new extent.
    *    newFile   - Indicates if extent is added to new or existing file.
    *    hdrs      - Contents of headers if file is compressed.
    * RETURN:
    *    NO_ERROR if success
    *    other number if fail
    ***********************************************************/
   int ColumnOp::extendColumn(
      const Column& column,
      bool         isbulkload,
      FILE*&       pFile,
      uint16_t&    dbRoot,
      uint32_t&    partition,
      uint16_t&    segment,
      std::string& segFile,
      HWM&         hwm,
      BRM::LBID_t& startLbid,
	  bool&        newFile,
	  int&		   allocSize,
      char*        hdrs)
   {
      i64 emptyVal = 0;
      //int allocSize;

      emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
	  int rc = extendFile(column.dataFile.fid,
                         emptyVal,
                         column.colWidth,
                         isbulkload,
                         allocSize,
                         pFile,
                         dbRoot,
                         partition,
                         segment,
                         segFile,
                         hwm,
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
      i64 emptyVal = getEmptyRowValue(column.colDataType, column.colWidth);
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
   bool ColumnOp::getColDataType(const char* name, ColDataType& colDataType) const
   {
      bool bFound = false;
      for(int i = 0; i < NUM_OF_COL_DATA_TYPE; i++)
         if (!strcmp(name, ColDataTypeStr[i])) {
            colDataType = (ColDataType) i ;
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
      i64  curVal, emptyVal;

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
         return ERR_FILE_OPEN;

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
      ColDataType colDataType,
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
      void*    pOldVal;
      char     charTmpBuf[8];
      i64      emptyVal;
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
         switch(curCol.colType)
         {
//               case WriteEngine::WR_LONG :   pVal = &((long *) valArray)[i]; break;
            case WriteEngine::WR_FLOAT :  if (!bDelete) pVal = &((float *) valArray)[i]; 
                                          pOldVal = &((float *) oldValArray)[i];
                                          break;
            case WriteEngine::WR_DOUBLE : if (!bDelete) pVal = &((double *) valArray)[i]; 
                                          pOldVal = &((double *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   if (!bDelete) 
										  {
												memcpy(charTmpBuf, (char*)valArray + i*8, 8);
                                          		pVal = charTmpBuf;
										  }
                                          pOldVal = (char*)oldValArray + i*8;
                                          break;
//            case WriteEngine::WR_BIT :    pVal = &((bool *) valArray)[i]; break;
            case WriteEngine::WR_SHORT :  if (!bDelete) pVal = &((short *) valArray)[i]; 
                                          pOldVal = &((short *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_BYTE :   if (!bDelete) pVal = &((char *) valArray)[i]; 
                                          pOldVal = &((char *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_LONGLONG:if (!bDelete) pVal = &((long long *) valArray)[i]; 
                                          pOldVal = &((long long *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_TOKEN:   if (!bDelete) pVal = &((Token *) valArray)[i];
                                          pOldVal = &((Token *) oldValArray)[i];
                                          break;
            case WriteEngine::WR_INT :
            default  :                    if (!bDelete) pVal = &((int *) valArray)[i]; 
                                          pOldVal = &((int *) oldValArray)[i];
                                          break;
         }
         
         // This is the stuff to retrieve old value
         memcpy(pOldVal, dataBuf + dataBio, curCol.colWidth);

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
      i64      emptyVal;
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
         switch(curCol.colType)
         {
//               case WriteEngine::WR_LONG :   pVal = &((long *) valArray)[i]; break;
            case WriteEngine::WR_FLOAT :  if (!bDelete) pVal = &((float *) valArray)[0]; 
                                          //pOldVal = &((float *) oldValArray)[i];
                                          break;
            case WriteEngine::WR_DOUBLE : if (!bDelete) pVal = &((double *) valArray)[0]; 
                                          //pOldVal = &((double *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   if (!bDelete) 
										  {
												memcpy(charTmpBuf, (char*)valArray, 8);
                                          		pVal = charTmpBuf;
										  }
                                          //pOldVal = (char*)oldValArray + i*8;
                                          break;
//            case WriteEngine::WR_BIT :    pVal = &((bool *) valArray)[i]; break;
            case WriteEngine::WR_SHORT :  if (!bDelete) pVal = &((short *) valArray)[0]; 
                                          //pOldVal = &((short *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_BYTE :   if (!bDelete) pVal = &((char *) valArray)[0]; 
                                          //pOldVal = &((char *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_LONGLONG:if (!bDelete) pVal = &((long long *) valArray)[0]; 
                                          //pOldVal = &((long long *) oldValArray)[i]; 
                                          break;
            case WriteEngine::WR_TOKEN:   if (!bDelete) pVal = &((Token *) valArray)[0];
                                          //pOldVal = &((Token *) oldValArray)[i];
                                          break;
            case WriteEngine::WR_INT :
            default  :                    if (!bDelete) pVal = &((int *) valArray)[0]; 
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
         switch(curCol.colType)
         {
//               case WriteEngine::WR_LONG :   pVal = &((long *) valArray)[i]; break;
            case WriteEngine::WR_FLOAT :  pVal = &((float *) valArray)[i]; 
                                          break;
            case WriteEngine::WR_DOUBLE : pVal = &((double *) valArray)[i]; 
                                          break;
            case WriteEngine::WR_VARBINARY :   // treat same as char for now
            case WriteEngine::WR_CHAR :   {
												memcpy(charTmpBuf, (char*)valArray+i*8, 8);
                                          		pVal = charTmpBuf;
										  }
                                          break;
            case WriteEngine::WR_SHORT :  pVal = &((short *) valArray)[i]; 
                                          break;
            case WriteEngine::WR_BYTE :   pVal = &((char *) valArray)[i];  
                                          break;
            case WriteEngine::WR_LONGLONG:pVal = &((long long *) valArray)[i]; 
                                          break;
            case WriteEngine::WR_TOKEN:   pVal = &((Token *) valArray)[i];
                                          break;
            case WriteEngine::WR_INT :
            default  :                    pVal = &((int *) valArray)[i]; 
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

