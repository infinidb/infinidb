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

/*********************************************************************
 *   $Id: we_bulkloadbuffer.h 4489 2013-01-30 18:47:53Z dcathey $
 *
 ********************************************************************/
#ifndef _WE_BULKLOADBUFFER_H
#define _WE_BULKLOADBUFFER_H

#include "we_type.h"
#include "limits"
#include "string"
#include "vector"
#include "boost/thread/mutex.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "we_columninfo.h"
#include "calpontsystemcatalog.h"

namespace WriteEngine 
{
class Log;

// Used to collect stats about a BulkLoadBuffer buffer that is being parsed
class BLBufferStats
{
  public:
    int64_t minBufferVal;
    int64_t maxBufferVal;
    int64_t satCount;
    BLBufferStats(ColDataType colDataType) : satCount(0)
    {
        if (isUnsigned(colDataType))
        {
            minBufferVal = static_cast<int64_t>(MAX_UBIGINT);
            maxBufferVal = static_cast<int64_t>(MIN_UBIGINT);
        }
        else
        {
            minBufferVal = MAX_BIGINT;
            maxBufferVal = MIN_BIGINT;
        }
    }
};

class BulkLoadBuffer
{
private:    

    //--------------------------------------------------------------------------
    // Private Data Members
    //--------------------------------------------------------------------------

    char *fData;                        // Buffer with data read from tbl file
    char *fDataParser;                  // for temporary use by parser 

    char *fOverflowBuf;                 // Overflow data held for next buffer
    unsigned fOverflowSize;             // Current size of fOverflowBuf

    // Information about the locker and status for each column in this buffer.
    // Note that TableInfo::fSyncUpdatesTI mutex is used to synchronize
    // access to fColumnLocks and fParseComplete from both read and parse
    // threads.  Table-scope lock (fSyncUpdatesTI) is used instead of
    // buffer-scope lock (fSyncUpdatesBLB), to keep state for all buffers
    // static while we are scanning fColumnLocks for the buffers in a table
    std::vector<LockInfo> fColumnLocks;

    // Note that TableInfo::fSyncUpdatesTI mutex (not fStatusBLB) is used
    // to synchronize getting/setting fStatusBLB between threads.
    Status fStatusBLB;                  // Status of buffer

    // TableInfo::fSyncUpdatesTI mutex should be locked when accessing
    // this data member (see fColumnLocks discussion).
    unsigned fParseComplete;            // Num of columns that are parseComplete

    unsigned fTotalRows;                // Max rows this buffer can now hold;
                                        //   size of fTokens array
    std::vector< std::pair<RID,std::string> > fRowStatus;//Status of bad rows
    std::vector<std::string> fErrRows; // Rejected rows to write to .bad file

    uint32_t fTotalReadRows;                // Total valid rows read into buffer;
                                        //   this count excludes rejected rows
    uint32_t fTotalReadRowsParser;          // for temporary use by parser

    uint32_t fTotalReadRowsForLog;          // Total rows read into this buffer
                                        //   including invalid rows

    RID fStartRow;                      // Starting row id for rows in buffer,
                                        //   relative to start of job.
                                        //   Rejected rows are excluded.
    RID fStartRowParser;                // for temporary use by parser

    RID fStartRowForLogging;            // Starting row id for rows in buffer,
                                        //   relative to start of current input
                                        //   file.  All rows are counted.
    RID fStartRowForLoggingParser;      // for temporary use by parser

    uint32_t fAutoIncGenCount;              // How many auto-increment values are
                                        //   to be generated for current buffer
    uint32_t fAutoIncGenCountParser;        // for temporary use by parser

    uint64_t fAutoIncNextValue;         // Next auto-increment value assign to
                                        //   a row in this buffer
    unsigned fNumberOfColumns;          // Number of ColumnInfo objs in table

    ColPosPair **fTokens;               // Vector of start and offsets for the
                                        //   column values read from tbl files
    ColPosPair **fTokensParser;         // for temporary use by parser

    char fColDelim;                     // Character to delimit columns in a row
    unsigned fBufferSize;               // Size of input read buffer (fData)
    unsigned fReadSize;                 // Number of bytes in read buffer(fData)
    boost::mutex fSyncUpdatesBLB;       // Mutex to synchronize updates
    Log* fLog;                          // Logger object
    bool fNullStringMode;               // Indicates if "NULL" string is to be
                                        //   treated as a NULL value or not
    char fEnclosedByChar;               // Optional char to enclose col values
    char fEscapeChar;                   // Used to escape enclosed character
    int fBufferId;                      // Id for this read buffer
    std::string fTableName;             // Table assigned to this read buffer
    JobFieldRefList fFieldList;         // Complete list of cols and flds
    unsigned int fNumFieldsInFile;      // Number of fields in input file
                                        //   (including fields to be ignored)
    unsigned int fNumColsInFile;        // Number of flds in input file targeted
                                        //   for db cols (omits default cols)
    bool fbTruncationAsError;           // Treat string truncation as error
    ImportDataMode fImportDataMode;     // Import data in text or binary mode
    unsigned int fFixedBinaryRecLen;    // Fixed rec len used in binary mode

    //--------------------------------------------------------------------------
    // Private Functions
    //--------------------------------------------------------------------------

    /** @brief Copy constructor
     */
    BulkLoadBuffer(const BulkLoadBuffer &buffer);

    /** @brief Assignment operator
     */
    BulkLoadBuffer & operator =(const BulkLoadBuffer & buffer);

    /** @brief Convert the buffer data depending upon the data type
     */
    void convert(char *field, int fieldLength,
                 bool nullFlag, unsigned char *output,
                 const JobColumn & column,
                 BLBufferStats& bufStats);

    /** @brief Copy the overflow data
     */
    void copyOverflow(const BulkLoadBuffer & buffer);

    /** @brief Parse a Read buffer for a nonDictionary column
     */
    int parseCol(ColumnInfo &columnInfo);

    /** @brief Parse a Read buffer for a nonDictionary column
     */
    void parseColLogMinMax(std::ostringstream& oss,
                           ColDataType         colDataType,
                           int64_t             minBufferVal,
                           int64_t             maxBufferVal) const;

    /** @brief Parse a Read buffer for a Dictionary column
     */
    int parseDict(ColumnInfo &columnInfo);

    /** @brief Parse a Dictionary Read buffer into a ColumnBufferSection.
     * 
     * Parses the Read buffer into a section up to the point at which
     * the buffer crosses an extent boundary.
     *
     * @param columnInfo    Column being parsed
     * @oaram tokenPos      Position of rows to be parsed, in fTokens.
     * @param startRow      Row id of first row in buffer to be parsed.
     *        Row id is relative to all the rows in this import.
     * @param totalReadRows Number of buffer rows ready to be parsed
     * @param nRowsParsed   Number of buffer rows that were parsed
     */
    int parseDictSection(ColumnInfo &columnInfo, int tokenPos,
                         RID startRow, uint32_t totalReadRows,
                         uint32_t& nRowsParsed);

    /** @brief Expand the size of the fTokens array
     */
    void resizeTokenArray();

    /** @brief tokenize the buffer contents and fill up the token array.
     */
    void tokenize(const boost::ptr_vector<ColumnInfo>& columnsInfo,
                  unsigned int allowedErrCntThisCall);

    /** @brief Binary tokenization of the buffer, and fill up the token array.
     */
    int tokenizeBinary(const boost::ptr_vector<ColumnInfo>& columnsInfo,
                  unsigned int allowedErrCntThisCall,
                  bool bEndOfData);

    /** @brief Determine if specified value is NULL or not.
     */
    bool isBinaryFieldNull(void* val, WriteEngine::ColType ct,
                  execplan::CalpontSystemCatalog::ColDataType dt);

public:

    /** @brief Constructor
     * @param noOfCol Number of columns
     * @param bufferSize Buffer size
     * @param logger The Log object used for logging
     * @param bufferId Id assigned to this buffer
     * @param tableName Name of table associated with this read buffer
     * @param jobFieldRefList Complete list of cols/flds listed in Job XML file
     */
    BulkLoadBuffer(unsigned noOfCols,
                   unsigned bufferSize, Log* logger,
                   int bufferId, const std::string& tableName,
                   const JobFieldRefList& jobFieldRefList);

    /** @brief Destructor
    */
    ~BulkLoadBuffer();

    /** @brief Resets the values of the members (excluding column locks)
     */
    void reset();

    /** @brief Resets the column locks.
     * TableInfo::fSyncUpdatesTI mutex should be locked when calling this 
     * function (see fColumnLocks discussion).
     */
    void resetColumnLocks();

    /** @brief Get the buffer status
     */
    Status getStatusBLB() const {return fStatusBLB;}

    /** @brief Set the buffer status
     */
    void setStatusBLB(const Status & status){fStatusBLB = status;}

    /** @brief Try to lock a column for the buffer
     * TableInfo::fSyncUpdatesTI mutex should be locked when calling this 
     * function (see fColumnLocks discussion).
     */
    bool tryAndLockColumn(const int & columnId, const int & id);

    /** @brief Read the table data into the buffer
     */
    int fillFromFile(const BulkLoadBuffer& overFlowBufIn,
        FILE * handle, RID & totalRows, RID & correctTotalRows,
        const boost::ptr_vector<ColumnInfo>& columnsInfo,
        unsigned int allowedErrCntThisCall); 

    /** @brief Get the overflow size
     */
    int getOverFlowSize()  const {return fOverflowSize;}

    /** @brief Parse the buffer data
     */
    int parse(ColumnInfo &columnInfo);

    /** @brief Set the delimiter used to delimit the columns within a row
     */
    void setColDelimiter(const char & delim){fColDelim = delim;}

    /** @brief Set mode to treat "NULL" string as NULL value or not.
     */
    void setNullStringMode( bool bMode ) { fNullStringMode = bMode; }

    /** @brief Set character optionally used to enclose input column values.
     */
    void setEnclosedByChar( char enChar ) { fEnclosedByChar = enChar; }

    /** @brief Set escape char to use in conjunction with enclosed by char.
     */
    void setEscapeChar    ( char esChar ) { fEscapeChar  = esChar; }

    /** @brief Get the column status
     *  TableInfo::fSyncUpdatesTI mutex should be locked when calling this 
     *  function (see fColumnLocks discussion).
     */
    Status getColumnStatus(const int & columnId) const
    { return fColumnLocks[columnId].status; }

    /** @brief Set the column status
     *  TableInfo::fSyncUpdatesTI mutex should be locked when calling this 
     *  function (see fColumnLocks discussion).
     *  @returns TRUE if all columns in the buffer are complete.
     */
    bool setColumnStatus(const int &columnId, const Status & status);

    /** @brief Get the error row status's
     */
    const std::vector< std::pair<RID,std::string> >& getErrorRows() const
    {return fRowStatus;}

    /** @brief Get the error rows
     */
    const std::vector<std::string>& getExactErrorRows() const
    {return fErrRows;}
       
    void clearErrRows() {fRowStatus.clear();fErrRows.clear();}

    /** @brief Get the column locker.
     *  TableInfo::fSyncUpdatesTI mutex should be locked when calling this 
     *  function (see fColumnLocks discussion).
     */
    const int getColumnLocker(const int & columnId) const
    { return fColumnLocks[columnId].locker; }

    /** @brief set truncation as error for this import.
     */
    void setTruncationAsError(bool bTruncationAsError) 
    { fbTruncationAsError = bTruncationAsError; }

    /** @brief retrieve the tuncation as error setting for this
     *  import. When set, this causes char and varchar strings
     *  that are longer than the column definition to be treated
     *  as errors instead of warnings.
     */
    bool getTruncationAsError() const
    { return fbTruncationAsError; }

    /** @brief Set text vs binary import mode along with corresponding fixed
     *         record length that is used if the binary mode is set to TRUE.
     */
    void setImportDataMode( ImportDataMode importMode,
        unsigned int fixedBinaryRecLen )
    { fImportDataMode    = importMode;
      fFixedBinaryRecLen = fixedBinaryRecLen; }
};

}
#endif
