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

//  $Id: we_colop.h 4450 2013-01-21 14:13:24Z rdempsey $


/** @file */

#ifndef _WE_COLOP_H_
#define _WE_COLOP_H_

#include <stdlib.h>

#include "we_dbfileop.h"
#include "brmtypes.h"
#include "we_dbrootextenttracker.h"
#include "we_tablemetadata.h"

#if defined(_MSC_VER) && defined(WRITEENGINECOLUMNOP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
   class Log;

//const int   COL_DATATYPE_NUM = 15;
//const char  ColDataTypeStr[COL_DATATYPE_NUM][20] = { "bit", "tinyint", "char", "smallint", "decimal", "medint", "integer", "float", "date", "bigint",
//            "double", "datetime", "varchar", "clob", "blob" };

/** Class ColumnOp */
class ColumnOp : public DbFileOp
{
public:
   /**
    * @brief Constructor
    */
   EXPORT ColumnOp();
   EXPORT ColumnOp(Log* logger);

   /**
    * @brief Default Destructor
    */
   EXPORT virtual ~ColumnOp();

   EXPORT virtual int allocRowId(const TxnID& txnid,
								Column& column,
                                uint64_t totalRow,
                                RID* rowIdArray,
                                HWM& hwm,
                                bool& newExtent,
                                uint64_t& rowsLeft,
                                HWM& newHwm,
                                bool& newFile,
								ColStructList& newColStructList,
                                DctnryStructList& newDctnryStructList,
								std::vector<DBRootExtentTracker*> & dbRootExtentTrackers,
                                bool insertSelect = false,
								bool isBatchInsert = false,
								OID tableOid = 0);

   /**
    * @brief Create column file(s)
    */
   EXPORT virtual int createColumn(Column& column,
                                  int colNo,
                                  int colWidth,
                                  execplan::CalpontSystemCatalog::ColDataType colDataType,
                                  ColType colType,
                                  FID dataFid,
                                  uint16_t dbRoot,
                                  uint32_t partition);

   /**
    * @brief Fills up a column with null/default values in all non-empty rows as the reference column. The reference column
    * would typically be another column of the same table.
    *
    * @param column The new colum
    * @param refCol The reference column for identifying valid rows
    * @param defaultVal The default value of the new column
    * @param dictOid The dictionary store OID for a dictionary column
    * @param dictColWidth The dictionary string width for a dictionary column
    */
   //BUG931
   EXPORT virtual int fillColumn(const TxnID& txnid,
								Column& column,
                                Column& refCol,
                                void* defaultVal,
                                const OID dictOid = 0,
								const int dictColWidth = 0,
								const std::string defaultValStr = "", 
								bool autoincrement = false);

   /**
    * @brief Create a table file
    */
   EXPORT virtual int createTable() const;

   /**
    * @brief Drop column file(s)
    */
   EXPORT virtual int dropColumn(const FID dataFid);

    /**
    * @brief Drop column and dictionary file(s)
    */
   EXPORT virtual int dropFiles(const std::vector<int32_t>& dataFids);

   /**
    * @brief Delete file(s) for the given partition
    */
   EXPORT virtual int dropPartitions(const std::vector<OID>& dataFids, 
                                     const std::vector<BRM::PartitionInfo>& partitions);


   EXPORT virtual int deleteOIDsFromExtentMap(const std::vector<int32_t>& dataFids);
   /**
    * @brief Drop a table file
    */
   int dropTable() const { return NO_ERROR; }

   /**
    * @brief Expand the abbreviated extent to a full extent for this column.
    */
   EXPORT virtual int expandAbbrevExtent(const Column& column);

   /**
    * @brief Add an extent to the specified column OID and DBRoot.
    * When this function returns, the file position will be located at the
    * end of the file.  If the applicable column segment file does not exist,
    * extendColumn() will create the new segment file.
    * The extent must already exist in the extentmap prior to calling this fctn.
    *
    * @param column Column struct with input column attributes.
    * @param leaveFileOpen Leave the db file open when leaving this function
    * @param firstFileOnPM If first file on a PM, then first empty chunk is
    *        written out (if compressed), to give us a startup file on this PM,
    *        much like MySQL "create table" creates the very "first" file on a
    *        selected PM.
    * @param hwm The fbo of the column segment file where the new extent begins
    * @param startLbid The starting LBID for the new extent.
    * @param allocSize Number of blocks to be written for an extent
    * @param dbRoot The DBRoot of the file with the new extent.
    * @param partition Partition num of the file with the new extent.
    * @param segment The segment number of the file with the new extent.
    * @param segFile (out) Name of segment file to which the extent is added.
    * @param pFile (out) FILE ptr to the file where the extent is added.
    * @param newFile (out) Indicates if extent was added to new or existing file
    * @param hdrs (out) Contents of headers if file is compressed.
    * @return returns NO_ERROR if success.
    */
   EXPORT int extendColumn(const Column& column,
                           bool          leaveFileOpen,
                           bool          firstFileOnPM,
                           HWM           hwm,
                           BRM::LBID_t   startLbid,
                           int           allocSize,
                           uint16_t      dbRoot,
                           uint32_t      partition,
                           uint16_t      segment,
                           std::string&  segFile,
                           FILE*&        pFile,
                           bool&         newFile,
                           char*         hdrs = NULL);

	/**
    * @brief Add an extent to the OID specified in the column argument.
    * When this function returns, the file position will be located at the
    * end of the file. 
    *
    * @param column Column struct with input column attributes. 
    * @param dbRoot (in) The DBRoot of the file with the new extent.
    * @param partition (in) The partition num of the file with the new extent.
    * @param segment (in) The segment number of the file with the new extent.
    * @param segFile (out) Name of segment file to which the extent is added.
    * @param startLbid (out) The starting LBID for the new extent.
    * @param newFile (out) Indicates if extent was added to new or existing file
    * @param hdsr (out) Contents of headers if file is compressed.
	* @param allocSize (out) number of blocks to be written for an extent
    * @return returns NO_ERROR if success.
    */
   EXPORT int addExtent(const Column& column,
                           uint16_t     dbRoot,
                           uint32_t     partition,
                           uint16_t     segment,
                           std::string&  segFile,
                           BRM::LBID_t&  startLbid,
                           bool&         newFile,
                           int&          allocSize,
                           char*         hdrs = NULL);

   /**
    * @brief Get columne data type
    */
   EXPORT virtual bool getColDataType(const char* name, execplan::CalpontSystemCatalog::ColDataType& colDataType) const;

   /**
    * @brief Initialize the column
    */
   EXPORT virtual void initColumn(Column& column) const;

   /**
    * @brief Check whether it is an empty row
    */
   EXPORT virtual bool isEmptyRow(unsigned char* buf, int offset, const Column& column);

   /**
    * @brief Check whether it is a valid column
    */
   EXPORT virtual bool isValid(Column& column) const;

   /**
    * @brief Open column file, segFile is set to the name of the column
    *        segment file that is opened.
    */
   EXPORT virtual int openColumnFile(Column& column,
                              std::string& segFile,
                              int ioBuffSize=DEFAULT_BUFSIZ) const;

   /**
    * @brief Open table file
    */
   int      openTableFile() const { return NO_ERROR; }

   /**
    * @brief Delete a file
    */
   EXPORT virtual void setColParam(Column& column, int colNo = 0,
                                  int colWidth = 0,
                                  execplan::CalpontSystemCatalog::ColDataType colDataType = execplan::CalpontSystemCatalog::INT,
                                  ColType colType = WR_INT,
                                  FID dataFid = 0,
                                  int comppre = 0,
                                  u_int16_t dbRoot = 0,
                                  u_int32_t partition = 0,
                                  u_int16_t segment = 0) const;

   /**
    * @brief Write row(s)
    */
   EXPORT virtual int writeRow(Column& curCol,
                              uint64_t totalRow,
                              const RID* rowIdArray,
                              const void* valArray,
                              const void* oldValArray,
                              bool bDelete = false);

   /**
    * @brief Write row(s) for update and delete  @Bug 1886,2870
    */
   EXPORT virtual int writeRows(Column& curCol,
                               uint64_t totalRow,
                               const RIDList& ridList,
                               const void* valArray,
                               const void* oldValArray=0,
                               bool bDelete = false);

   /**
    * @brief Write row(s) for update and delete  @Bug 1886,2870
    */
   EXPORT virtual int writeRowsValues(Column& curCol,
                              uint64_t totalRow,
                              const RIDList& ridList,
                              const void* valArray);

   /**
    * @brief Test if the pFile is an abbreviated extent.
    */
   virtual bool abbreviatedExtent(FILE* pFile, int colWidth) const = 0;

   /**
    * @brief Caculate the number of blocks in file.
    */
   virtual int blocksInFile(FILE* pFile) const = 0;

   /**
    * @brief Clear a column
    */
   EXPORT void clearColumn(Column& column) const;

   /**
    * @brief open a data file of column
    */
   virtual FILE*  openFile(const Column& column, uint16_t dbRoot, uint32_t partition,
      uint16_t segment, std::string& segFile, const char* mode = "r+b",
      int ioBuffSize = DEFAULT_BUFSIZ) const = 0;


   /**
    * @brief backup blocks to version buffer
    */
   int writeVB(FILE* pSource, const OID sourceOid, FILE* pTarget, const OID targetOid,
              const std::vector<uint32_t>& fboList, const BRM::VBRange& freeList,
              size_t& nBlocksProcessed, const size_t fboCurrentOffset);

   /**
    * @brief restore blocks from version buffer
    */
   int copyVB(FILE* pSource, const BRM::VER_t txnD, const OID oid, std::vector<uint32_t>& fboList,
               std::vector<BRM::LBIDRange>& rangeList);
protected:

   /**
    * @brief close column file
    */
   EXPORT virtual void closeColumnFile(Column& column) const;

   /**
    * @brief populate readBuf with data in block #lbid
    */
   virtual int readBlock(FILE* pFile, unsigned char* readBuf, const uint64_t fbo) = 0;

   /**
    * @brief output writeBuf to pFile starting at position fbo
    */
   virtual int saveBlock(FILE* pFile, const unsigned char* writeBuf, const uint64_t fbo) = 0;

private:
};

} //end of namespace

#undef EXPORT

#endif // _WE_COLOP_H_
