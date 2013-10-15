/* Copyright (C) 2013 Calpont Corp.

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
 *   $Id: we_bulkloadbuffer.cpp 4661 2013-06-04 12:59:50Z dcathey $
 *
 ********************************************************************/

#include <sys/time.h>
#include <sstream>
#include <string>
#include <stdint.h>
#include <cerrno>
#include <cstring>
#include <cstdlib> // includes <alloca.h> on linux
#include <cmath>
#include <ctype.h>
#include <cfloat>
#include "we_bulkloadbuffer.h"
#include "we_brm.h"
#include "we_convertor.h"
#include "we_log.h"
#include "brmtypes.h"
#include "dataconvert.h"
#include "exceptclasses.h"

#include "joblisttypes.h"

using namespace std;
using namespace boost;
using namespace execplan;

#if defined(_MSC_VER) && !defined(isnan)
#define isnan _isnan
namespace
{
    inline int __signbitf(float __x)
    {
        union { float __f; int __i; } __u;
        __u.__f = __x;
        return __u.__i < 0;
    }
    inline int __signbit(double __x)
    {
        union { double __d; int __i[2]; } __u;
        __u.__d = __x;
        return __u.__i[1] < 0;
    }
}
#define signbit(x) (sizeof(x) == sizeof(float) ? __signbitf(x) : __signbit(x))
#endif

namespace
{
    const std::string INPUT_ERROR_WRONG_NO_COLUMNS =
                      "Data contains wrong number of columns";
    const std::string INPUT_ERROR_TOO_LONG =
                      "Data in wrong format; exceeds max field length; ";
    const std::string INPUT_ERROR_NULL_CONSTRAINT =
                      "Data violates NOT NULL constraint with no default";
    const std::string INPUT_ERROR_ODD_VARBINARY_LENGTH =
                      "VarBinary column is incomplete; odd number of bytes; ";
    const std::string INPUT_ERROR_STRING_TOO_LONG =
                      "Character data exceeds max field length; ";
    const char        NULL_CHAR            = 'N';   
    const char*       NULL_VALUE_STRING    = "NULL";
    const char        NULL_AUTO_INC_0      = '0';
    const unsigned long long NULL_AUTO_INC_0_BINARY = 0;
    const char        NEWLINE_CHAR         = '\n';

    // Enumeration states related to parsing a column value
    enum FieldParsingState
    {
        FLD_PARSE_LEADING_CHAR_STATE  = 1, // parsing leading character
        FLD_PARSE_ENCLOSED_STATE      = 2, // parsing an enclosed column value
        FLD_PARSE_TRAILING_CHAR_STATE = 3, // parsing bytes after an
                                           //   enclosed column value
        FLD_PARSE_NORMAL_STATE        = 4  // parsing non-enclosed column value
    };

//------------------------------------------------------------------------------
// Expand pRowData to size "newArrayCapacity", preserving contents, and
// deleting old pointer
//------------------------------------------------------------------------------
inline void resizeRowDataArray( char** pRowData,
    unsigned int dataLength, 
    unsigned int newArrayCapacity)
{
    char* tmpRaw = new char[newArrayCapacity];
    memcpy(tmpRaw, *pRowData, dataLength);
    delete []*pRowData;
    *pRowData = tmpRaw;
}

}

//#define DEBUG_TOKEN_PARSING 1

namespace WriteEngine 
{

//------------------------------------------------------------------------------
// BulkLoadBuffer constructor
//------------------------------------------------------------------------------
BulkLoadBuffer::BulkLoadBuffer(
    unsigned numberOfCols, unsigned bufferSize, Log* logger,
    int bufferId, const std::string& tableName,
    const JobFieldRefList& jobFieldRefList ) :
      fOverflowSize(0), fParseComplete(0), fTotalRows(0), fStartRow(0),
        fStartRowForLogging(0), fAutoIncGenCount(0),
        fAutoIncNextValue(0),
        fReadSize(0), fLog(logger), fNullStringMode(false),
        fEnclosedByChar('\0'), fEscapeChar('\\'),
        fBufferId(bufferId), fTableName(tableName),
        fbTruncationAsError(false), fImportDataMode(IMPORT_DATA_TEXT),
        fFixedBinaryRecLen(0)
{
    fData            = new char[bufferSize];
    fOverflowBuf     = NULL;
    fStatusBLB       = WriteEngine::NEW;
    fNumberOfColumns = numberOfCols;
    fBufferSize      = bufferSize;

    fColumnLocks.clear();

    fTokens = 0;

    fRowStatus.clear();
    fErrRows.clear();

    struct LockInfo info;
    info.locker = -1;
    info.status = WriteEngine::NEW;

    fColumnLocks.resize(numberOfCols);
    fColumnLocks.assign(fNumberOfColumns, info);

    fTotalReadRowsParser      = 0;
    fStartRowParser           = 0;
    fDataParser               = 0;
    fTokensParser             = 0;
    fStartRowForLoggingParser = 0;
    fAutoIncGenCountParser    = 0;
    fNumFieldsInFile          = 0;
    fNumColsInFile            = 0;

    // Count the total number of fields in the input file (fNumFieldsInFile)
    // and the number of db columns that will be loaded from those fields
    // (fNumColsInFile).  Keep in mind that fNumColsInFile may be less than
    // fNumFieldsInFile, because there may be fields we are to ignore, and/or
    // some db columns may get default loaded without a corresponding field
    // in the input file.
    fFieldList.resize( jobFieldRefList.size() );
    for (unsigned k=0; k<jobFieldRefList.size(); k++)
    {
        fFieldList[k] = jobFieldRefList[k];
        switch (jobFieldRefList[k].fFldColType)
        {
            case BULK_FLDCOL_COLUMN_FIELD:
            {
                fNumColsInFile++;
                fNumFieldsInFile++;
                break;
            }
            case BULK_FLDCOL_IGNORE_FIELD:
            {
                fNumFieldsInFile++;
                break;
            }
            case BULK_FLDCOL_COLUMN_DEFAULT:
            default:
            {
                break;
            }
        }
    }
}

//------------------------------------------------------------------------------
// BulkLoadBuffer destructor
//------------------------------------------------------------------------------
BulkLoadBuffer::~BulkLoadBuffer()
{
    if(fData != NULL)
        delete [] fData;
    if(fOverflowBuf != NULL)
        delete [] fOverflowBuf;
    fColumnLocks.clear();

    if(fTokens != NULL)
    {
       for(unsigned int i=0; i<fTotalRows; ++i)
       {
           delete [] fTokens[i];
       }
       delete [] fTokens;

    }
    fRowStatus.clear();
    fErrRows.clear();
}

//------------------------------------------------------------------------------
// Resets state of buffer.
//------------------------------------------------------------------------------
void BulkLoadBuffer::reset()
{
    fStartRow = fTotalReadRows = fTotalReadRowsForLog = 0;
    fAutoIncGenCount = 0;
}

//------------------------------------------------------------------------------
// Resets state of buffer's column locks.
//------------------------------------------------------------------------------
void BulkLoadBuffer::resetColumnLocks()
{   
    fParseComplete = 0;
        
    struct LockInfo info;
    fColumnLocks.assign(fNumberOfColumns, info);
}

//------------------------------------------------------------------------------
// Copy overflow leftover from previous buffer into the start of "this" buffer
//------------------------------------------------------------------------------
void BulkLoadBuffer::copyOverflow(const BulkLoadBuffer & buffer)
{
    if(fOverflowBuf != NULL)
    {
        delete [] fOverflowBuf;
        fOverflowBuf = NULL;
    }

    fOverflowSize = buffer.fOverflowSize;
    if(fOverflowSize != 0)
    {
        fOverflowBuf = new char[buffer.fOverflowSize];
        memcpy( fOverflowBuf, buffer.fOverflowBuf, buffer.fOverflowSize );
    }
}

//------------------------------------------------------------------------------
// Parse/convert the given "field" value based on the specified length and type.
// field        (in)     - the input field value to be parsed
// fieldLength  (in)     - number of bytes of data in "field"
// nullFlag     (in)     - indicates if NULL value is to be assigned to "output"
//                         rather than parsing the data in "field"
// output       (out)    - the parsed value taken from "field"
// column       (in)     - column information for the column we are parsing
// bufStats:
// minBufferVal (in/out) - ongoing min value for the Read buffer we are parsing
// maxBufferVal (in/out) - ongoing max value for the Read buffer we are parsing
// satCount     (in/out) - ongoing saturation row count for buffer being parsed
//------------------------------------------------------------------------------
void BulkLoadBuffer::convert(char *field, int fieldLength,
     bool nullFlag, unsigned char *output,const JobColumn &column,
     BLBufferStats& bufStats)
{
    char        biVal;
    int         iVal;
    float       fVal;
    double      dVal;
    short       siVal;
    void*       pVal;
    int32_t     iDate;
    char        charTmpBuf[MAX_COLUMN_BOUNDARY+1] = {0};
    long long   llVal=0, llDate=0;
    uint64_t    tmp64;
    uint32_t    tmp32;
    uint8_t     ubiVal;
    uint16_t    usiVal;
    uint32_t    uiVal;
    uint64_t    ullVal;
      
    int width = column.width;
      
    //--------------------------------------------------------------------------
    // Parse based on column data type
    //--------------------------------------------------------------------------
    switch( column.weType )
    {
        //----------------------------------------------------------------------
        // FLOAT
        //----------------------------------------------------------------------
        case WriteEngine::WR_FLOAT :  
        {
            if (nullFlag)
            {
                if (column.fWithDefault)
                {
                    fVal  = column.fDefaultDbl;
                    pVal  = &fVal;
                }
                else 
                {
                    tmp32 = joblist::FLOATNULL;
                    pVal  = &tmp32;
                }
            }
            else
            {
                float minFltSat = column.fMinDblSat;
                float maxFltSat = column.fMaxDblSat;

                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    memcpy(&fVal,field,sizeof(fVal));
                    if ( isnan(fVal) )
                    {
                        if (signbit(fVal))
                            fVal = minFltSat;
                        else
                            fVal = maxFltSat;
                        bufStats.satCount++;
                    }
                    else
                    {
                        if ( fVal > maxFltSat )
                        {
                            fVal = maxFltSat;
                            bufStats.satCount++;
                        }
                        else if ( fVal < minFltSat )
                        {
                            fVal = minFltSat;
                            bufStats.satCount++;
                        }
                    }
                }
                else
                {
                    errno = 0;
#ifdef _MSC_VER
                    fVal = (float)strtod( field, 0 );
#else
                    fVal = strtof( field, 0 );
#endif
                    if (errno == ERANGE)
                    {
#ifdef _MSC_VER
                        if ( abs(fVal) == HUGE_VAL )
#else
                        if ( abs(fVal) == HUGE_VALF )
#endif
                        {
                            if ( fVal > 0 )
                                fVal = maxFltSat;
                            else
                                fVal = minFltSat;
                            bufStats.satCount++;
                        }
                        else
                            fVal = 0;
                    }
                    else
                    {
                        if ( fVal > maxFltSat )
                        {
                            fVal = maxFltSat;
                            bufStats.satCount++;
                        }
                        else if ( fVal < minFltSat )
                        {
                            fVal = minFltSat;
                            bufStats.satCount++;
                        }
                    }
                }

                pVal = &fVal;
            }
            break;
        }

        //----------------------------------------------------------------------
        // DOUBLE
        //----------------------------------------------------------------------
        case WriteEngine::WR_DOUBLE : 
        {
            if (nullFlag)
            {
                if (column.fWithDefault)
                {
                    dVal  = column.fDefaultDbl;
                    pVal  = &dVal;
                }
                else
                {
                    tmp64 = joblist::DOUBLENULL;
                    pVal  = &tmp64;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    memcpy(&dVal,field,sizeof(dVal));
                    if ( isnan(dVal) )
                    {
                        if (signbit(dVal))
                            dVal = column.fMinDblSat;
                        else
                            dVal = column.fMaxDblSat;
                        bufStats.satCount++;
                    }
                    else
                    {
                        if ( dVal > column.fMaxDblSat )
                        {
                            dVal = column.fMaxDblSat;
                            bufStats.satCount++;
                        }
                        else if ( dVal < column.fMinDblSat )
                        {
                            dVal = column.fMinDblSat;
                            bufStats.satCount++;
                        }
                    }
                }
                else
                {
                    errno = 0;
                    dVal = strtod(field, 0);
                    if (errno == ERANGE)
                    {
#ifdef _MSC_VER
                        if ( abs(dVal) == HUGE_VAL )
#else
                        if ( abs(dVal) == HUGE_VALL )
#endif
                        {
                            if ( dVal > 0 )
                                dVal = column.fMaxDblSat;
                            else
                                dVal = column.fMinDblSat;
                            bufStats.satCount++;
                        }
                        else
                            dVal = 0;
                    }
                    else
                    {
                        if ( dVal > column.fMaxDblSat )
                        {
                            dVal = column.fMaxDblSat;
                            bufStats.satCount++;
                        }
                        else if ( dVal < column.fMinDblSat )
                        {
                            dVal = column.fMinDblSat;
                            bufStats.satCount++;
                        }
                    }
                }

                pVal = &dVal;
            }
            break;
        }

        //----------------------------------------------------------------------
        // CHARACTER
        //----------------------------------------------------------------------
        case WriteEngine::WR_CHAR :   
        {
            if (nullFlag)
            {
                if (column.fWithDefault)
                {
                    int defLen          = column.fDefaultChr.size();
                    const char* defData = column.fDefaultChr.c_str();
                    if (defLen > column.definedWidth)
                        memcpy( charTmpBuf, defData, column.definedWidth );
                    else
                        memcpy( charTmpBuf, defData, defLen );
                    // fall through to update saturation and min/max
                }
                else
                {
                    idbassert(width<=8);

                    for(int i=0; i< width-1; i++)
                    {
                        charTmpBuf[i]='\377';
                    }
                    charTmpBuf[width-1]='\376';
 
                    pVal = charTmpBuf;
                    break;
                }
            }
            else
            {
                // truncate string if it is too long
                // @Bug 3040.  Use definedWidth for the data truncation to keep
                // from storing characters beyond the column's defined width.
                // It contains the column definition width rather than the bytes
                // on disk (e.g. 5 for a varchar(5) instead of 8).
                if (fieldLength > column.definedWidth)
                {
                    memcpy( charTmpBuf, field, column.definedWidth );
                    bufStats.satCount++;
                }
                else
                    memcpy( charTmpBuf, field, fieldLength );
            }

            // Swap byte order before comparing character string
            int64_t binChar = static_cast<int64_t>( uint64ToStr(
                    *(reinterpret_cast<uint64_t*>(charTmpBuf)) ) );

            // Update min/max range
            if (binChar < bufStats.minBufferVal)
                bufStats.minBufferVal = binChar;
            if (binChar > bufStats.maxBufferVal)
                bufStats.maxBufferVal = binChar;

            pVal = charTmpBuf;
            break;
        }

        //----------------------------------------------------------------------
        // SHORT INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_SHORT :
        {
            long long origVal;  
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        origVal = column.fDefaultInt;
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        siVal = joblist::SMALLINTNULL;
                        pVal  = &siVal;
                        break;
                    }
                }
                else
                {
                    origVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    short int siVal2;
                    memcpy(&siVal2,field,sizeof(siVal2));
                    origVal = siVal2;
                }
                else
                {
                    if( (column.dataType == CalpontSystemCatalog::DECIMAL ) ||
                        (column.dataType == CalpontSystemCatalog::UDECIMAL) )
                    {
                        // errno is initialized and set in convertDecimalString
                        origVal = Convertor::convertDecimalString(
                            field, fieldLength, column.scale );
                    }
                    else
                    {
                        errno   = 0;
                        origVal = strtol( field, 0, 10 );
                    }
                    if (errno == ERANGE)
                        bSatVal = true;
                }
            }

            // Saturate the value
            if ( origVal < column.fMinIntSat )
            {
                origVal = column.fMinIntSat;
                bSatVal = true;
            }
            else if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
            {
                origVal = static_cast<int64_t>(column.fMaxIntSat);
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            if (origVal < bufStats.minBufferVal)
                bufStats.minBufferVal = origVal;
            if (origVal > bufStats.maxBufferVal)
                bufStats.maxBufferVal = origVal;

            siVal = origVal;
            pVal = &siVal;

            break;
        }

        //----------------------------------------------------------------------
        // UNSIGNED SHORT INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_USHORT :
        {
            int64_t origVal=0;
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        origVal = static_cast<int64_t>(column.fDefaultUInt);
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        usiVal = joblist::USMALLINTNULL;
                        pVal  = &usiVal;
                        break;
                    }
                }
                else
                {
                    origVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    unsigned short int siVal2;
                    memcpy(&siVal2,field,sizeof(siVal2));
                    origVal = siVal2;
                }
                else
                {
                    errno   = 0;
                    origVal = strtoll(field, 0, 10);
                    if (errno == ERANGE)
                        bSatVal = true;
                }
            }

            // Saturate the value (saturates any negative value to 0)
            if ( origVal < column.fMinIntSat )
            {
                origVal = column.fMinIntSat;
                bSatVal = true;
            }
            else
            if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
            {
                origVal = static_cast<int64_t>(column.fMaxIntSat);
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            uint64_t uVal = origVal;
            if (uVal < static_cast<uint64_t>(bufStats.minBufferVal))
                bufStats.minBufferVal = origVal;
            if (uVal > static_cast<uint64_t>(bufStats.maxBufferVal))
                bufStats.maxBufferVal = origVal;

            usiVal = origVal;
            pVal = &usiVal;

            break;
        }

        //----------------------------------------------------------------------
        // TINY INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_BYTE :   
        {
            long long origVal;  
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        origVal = column.fDefaultInt;
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        biVal = joblist::TINYINTNULL;
                        pVal = &biVal;
                        break;
                    }
                }
                else
                {
                    origVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    char biVal2;
                    memcpy(&biVal2,field,sizeof(biVal2));
                    origVal = biVal2;
                }
                else
                {
                    if( (column.dataType == CalpontSystemCatalog::DECIMAL ) ||
                        (column.dataType == CalpontSystemCatalog::UDECIMAL))
                    {
                        // errno is initialized and set in convertDecimalString
                        origVal = Convertor::convertDecimalString(
                            field, fieldLength, column.scale );
                    }
                    else
                    {
                        errno   = 0;
                        origVal = strtol( field, 0, 10 );
                    }
                    if (errno == ERANGE)
                        bSatVal = true;
                }
            }

            // Saturate the value
            if ( origVal < column.fMinIntSat )
            {
                origVal = column.fMinIntSat;
                bSatVal = true;
            }
            else if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
            {
                origVal = static_cast<int64_t>(column.fMaxIntSat);
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            if (origVal < bufStats.minBufferVal)
                bufStats.minBufferVal = origVal;
            if (origVal > bufStats.maxBufferVal)
                bufStats.maxBufferVal = origVal;

            biVal = origVal;
            pVal = &biVal;

            break;
        }

        //----------------------------------------------------------------------
        // UNSIGNED TINY INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_UBYTE :   
        {
            int64_t origVal=0;
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        origVal = static_cast<int64_t>(column.fDefaultUInt);
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        ubiVal = joblist::UTINYINTNULL;
                        pVal = &ubiVal;
                        break;
                    }
                }
                else
                {
                    origVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    uint8_t biVal2;
                    memcpy(&biVal2,field,sizeof(biVal2));
                    origVal = biVal2;
                }
                else
                {
                    errno   = 0;
                    origVal = strtoll(field, 0, 10);
                    if (errno == ERANGE)
                        bSatVal = true;
                }
            }

            // Saturate the value (saturates any negative value to 0)
            if ( origVal < column.fMinIntSat )
            {
                origVal = column.fMinIntSat;
                bSatVal = true;
            }
            else
            if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
            {
                origVal = static_cast<int64_t>(column.fMaxIntSat);
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            uint64_t uVal = origVal;
            if (uVal < static_cast<uint64_t>(bufStats.minBufferVal))
                bufStats.minBufferVal = origVal;
            if (uVal > static_cast<uint64_t>(bufStats.maxBufferVal))
                bufStats.maxBufferVal = origVal;

            ubiVal = origVal;
            pVal = &ubiVal;

            break;
        }

        //----------------------------------------------------------------------
        // BIG INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_LONGLONG:
        {
            bool bSatVal = false;

            if( column.dataType != CalpontSystemCatalog::DATETIME )
            {
                if (nullFlag)
                {
                    if (!column.autoIncFlag)
                    {
                        if (column.fWithDefault)
                        {
                            llVal = column.fDefaultInt;
                            // fall through to update saturation and min/max
                        }
                        else
                        {
                            llVal = joblist::BIGINTNULL;
                            pVal  = &llVal;
                            break;
                        }
                    }
                    else
                    {
                        llVal = fAutoIncNextValue++;
                    }
                }
                else
                {
                    if (fImportDataMode != IMPORT_DATA_TEXT)
                    {
                        memcpy(&llVal,field,sizeof(llVal));
                    }
                    else
                    {
                        if( (column.dataType == CalpontSystemCatalog::DECIMAL)||
                            (column.dataType == CalpontSystemCatalog::UDECIMAL))
                        {
                        // errno is initialized and set in convertDecimalString
                            llVal = Convertor::convertDecimalString(
                                field, fieldLength, column.scale );
                        }
                        else
                        {
                            errno = 0;
                            llVal = strtoll( field, 0, 10 );
                        }
                    }
                    if (errno == ERANGE)
                        bSatVal = true;
                }

                // Saturate the value
                if ( llVal < column.fMinIntSat )
                {
                    llVal   = column.fMinIntSat;
                    bSatVal = true;
                }
                else if ( llVal > static_cast<int64_t>(column.fMaxIntSat) )
                {   // llVal can be > fMaxIntSat if this is a decimal column
                    llVal   = static_cast<int64_t>(column.fMaxIntSat);
                    bSatVal = true;
                }
                if (bSatVal)
                    bufStats.satCount++;

                // Update min/max range
                if (llVal < bufStats.minBufferVal)
                    bufStats.minBufferVal = llVal;
                if (llVal > bufStats.maxBufferVal)
                    bufStats.maxBufferVal = llVal;

                pVal = &llVal;
            }
            else
            {  // datetime conversion
                int rc = 0;
                if (nullFlag)
                {
                    if (column.fWithDefault)
                    {
                        llDate = column.fDefaultInt;
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        llDate = joblist::DATETIMENULL;
                        pVal = &llDate;
                        break;
                    }
                }
                else
                {
                    if (fImportDataMode != IMPORT_DATA_TEXT)
                    {
                        memcpy(&llDate,field,sizeof(llDate));
                        if (!dataconvert::DataConvert::isColumnDateTimeValid(
                            llDate))
                            rc = -1;
                    }
                    else
                    {
                        llDate =dataconvert::DataConvert::convertColumnDatetime(
                            field, dataconvert::CALPONTDATETIME_ENUM,
                            rc, fieldLength );
                    }
                }

                if (rc == 0) {
                    if (llDate < bufStats.minBufferVal)
                        bufStats.minBufferVal = llDate;
                    if (llDate > bufStats.maxBufferVal)
                        bufStats.maxBufferVal = llDate;
                }
                else {
                    // @bug 3375: reset invalid date/time to NULL,
                    //            and track as a saturated value.
                    llDate = joblist::DATETIMENULL;
                    bufStats.satCount++;
                }

                pVal = &llDate;
            }
            break;
        }

        //----------------------------------------------------------------------
        // UNSIGNED BIG INT
        //----------------------------------------------------------------------
        case WriteEngine::WR_ULONGLONG:
        {
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        ullVal = column.fDefaultUInt;
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        ullVal = joblist::UBIGINTNULL;
                        pVal  = &ullVal;
                        break;
                    }
                }
                else
                {
                    ullVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    memcpy(&ullVal,field,sizeof(ullVal));
                }
                else
                {
                    // Check for negative. strtoull doesn't do this for us.
                    // I considered using boost::trim_left here, but part of the
                    // exercise is to minimize cpu cycles, so I do it the old
                    // fashioned way.  isspace() uses more cycles than direct
                    // compare to ' ', '\t', etc.  but the payoff is that it
                    // works with Locale, so it ought to work well with utf-8
                    // input.
                    int idx1;
                    for (idx1=0; idx1<fieldLength; idx1++)
                    {
                        if (!isspace(field[idx1]))
                            break;
                    }
                    if ((idx1 < fieldLength) && (field[idx1] == '-'))
                    {
                        ullVal = static_cast<uint64_t>(column.fMinIntSat);
                        bSatVal = true;
                    }
                    else
                    {
                        errno = 0;
                        ullVal = strtoull(field, 0, 10);
                        if (errno == ERANGE)
                            bSatVal = true;
                    }
                }
            }

            // Saturate the value
            if ( ullVal > column.fMaxIntSat )
            {
                ullVal = column.fMaxIntSat;
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            if (ullVal < static_cast<uint64_t>(bufStats.minBufferVal))
                bufStats.minBufferVal = static_cast<int64_t>(ullVal);
            if (ullVal > static_cast<uint64_t>(bufStats.maxBufferVal))
                bufStats.maxBufferVal = static_cast<int64_t>(ullVal);

            pVal = &ullVal;
            break;
        }

        //----------------------------------------------------------------------
        // UNSIGNED INTEGER
        //----------------------------------------------------------------------
        case WriteEngine::WR_UINT :
        {
            int64_t origVal;
            bool bSatVal = false;

            if (nullFlag)
            {
                if (!column.autoIncFlag)
                {
                    if (column.fWithDefault)
                    {
                        origVal = static_cast<int64_t>(column.fDefaultUInt);
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        uiVal = joblist::UINTNULL;
                        pVal = &uiVal;
                        break;
                    }
                }
                else
                {
                    origVal = fAutoIncNextValue++;
                }
            }
            else
            {
                if (fImportDataMode != IMPORT_DATA_TEXT)
                {
                    unsigned int iVal2;
                    memcpy(&iVal2,field,sizeof(iVal2));
                    origVal = iVal2;
                }
                else
                {
                    errno   = 0;
                    origVal = strtoll(field, 0, 10);
                    if (errno == ERANGE)
                        bSatVal = true;
                }
            }

            // Saturate the value (saturates any negative value to 0)
            if ( origVal < column.fMinIntSat)
            {
                origVal = column.fMinIntSat;
                bSatVal = true;
            }
            else
            if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
            {
                origVal = static_cast<int64_t>(column.fMaxIntSat);
                bSatVal = true;
            }
            if (bSatVal)
                bufStats.satCount++;

            // Update min/max range
            uint64_t uVal = origVal;
            if (uVal < static_cast<uint64_t>(bufStats.minBufferVal))
                bufStats.minBufferVal = origVal;
            if (uVal > static_cast<uint64_t>(bufStats.maxBufferVal))
                bufStats.maxBufferVal = origVal;

            uiVal = origVal;
            pVal = &uiVal;
            break;
        }
        //----------------------------------------------------------------------
        // INTEGER
        //----------------------------------------------------------------------
        case WriteEngine::WR_INT :
        default  :
        {
            if( column.dataType != CalpontSystemCatalog::DATE ) 
            {
                long long origVal;
                bool bSatVal = false;

                if (nullFlag)
                {
                    if (!column.autoIncFlag)
                    {
                        if (column.fWithDefault)
                        {
                            origVal = column.fDefaultInt;
                            // fall through to update saturation and min/max
                        }
                        else
                        {
                            iVal = joblist::INTNULL;
                            pVal = &iVal;
                            break;
                        }
                    }
                    else
                    {
                        origVal = fAutoIncNextValue++;
                    }
                }
                else
                {
                    if (fImportDataMode != IMPORT_DATA_TEXT)
                    {
                        int iVal2;
                        memcpy(&iVal2,field,sizeof(iVal2));
                        origVal = iVal2;
                    }
                    else
                    {
                        if( (column.dataType == CalpontSystemCatalog::DECIMAL)||
                            (column.dataType == CalpontSystemCatalog::UDECIMAL))
                        {
                        // errno is initialized and set in convertDecimalString
                            origVal = Convertor::convertDecimalString(
                                field, fieldLength, column.scale );
                        }
                        else
                        {
                            errno   = 0;
                            origVal = strtol( field, 0, 10 );
                        }
                        if (errno == ERANGE)
                            bSatVal = true;
                    }
                }

                // Saturate the value
                if ( origVal < column.fMinIntSat )
                {
                    origVal = column.fMinIntSat;
                    bSatVal = true;
                }
                else if ( origVal > static_cast<int64_t>(column.fMaxIntSat) )
                {
                    origVal = static_cast<int64_t>(column.fMaxIntSat);
                    bSatVal = true;
                }
                if (bSatVal)
                    bufStats.satCount++;

                // Update min/max range
                if (origVal < bufStats.minBufferVal)
                    bufStats.minBufferVal = origVal;
                if (origVal > bufStats.maxBufferVal)
                    bufStats.maxBufferVal = origVal;

                iVal = (int)origVal;
                pVal = &iVal;
            }
            else 
            {  // date conversion
                int rc = 0;
                if (nullFlag)
                {
                    if (column.fWithDefault)
                    {
                        iDate = column.fDefaultInt;
                        // fall through to update saturation and min/max
                    }
                    else
                    {
                        iDate = joblist::DATENULL;
                        pVal = &iDate;
                        break;
                    }
                }
                else
                {
                    if (fImportDataMode != IMPORT_DATA_TEXT)
                    {
                        memcpy(&iDate,field,sizeof(iDate));
                        if (!dataconvert::DataConvert::isColumnDateValid(iDate))
                            rc = -1;
                    }
                    else
                    {
                        iDate = dataconvert::DataConvert::convertColumnDate(
                            field, dataconvert::CALPONTDATE_ENUM,
                            rc, fieldLength );
                    }
                }

                if (rc == 0) {
                    if (iDate < bufStats.minBufferVal)
                        bufStats.minBufferVal = iDate;
                    if (iDate > bufStats.maxBufferVal)
                        bufStats.maxBufferVal = iDate;
                }
                else {
                    // @bug 3375: reset invalid date to NULL,
                    //            and track as a saturated value.
                    iDate = joblist::DATENULL;
                    bufStats.satCount++;
                }

                pVal = &iDate;
            }
            break;
        }
    }

    memcpy(output, pVal, width);
}

//------------------------------------------------------------------------------
// Parse the contents of the Read buffer based on whether it is a dictionary
// column or not.
//------------------------------------------------------------------------------
int  BulkLoadBuffer::parse(ColumnInfo &columnInfo)
{
    int rc = NO_ERROR;

    // Rather than locking fSyncUpdatesBLB for the entire life of parse(),
    // we only briefly lock, and force a synchronization with the relevant
    // class variables from reader threads (by copying to Parser specific
    // variables).  It should be okay to reference a copy of these variables
    // as no other thread should be changing them while we are in parse().
    {
        boost::mutex::scoped_lock lock(fSyncUpdatesBLB);
        fTotalReadRowsParser      = fTotalReadRows;
        fStartRowParser           = fStartRow;
        fDataParser               = fData;
        fTokensParser             = fTokens;
        fStartRowForLoggingParser = fStartRowForLogging;
        fAutoIncGenCountParser    = fAutoIncGenCount;
    }

    //Bug806 - If buffer is empty then return early.
    if ( fTotalReadRowsParser == 0 )
        return rc;

    // If this is the first batch of rows, create the starting DB file
    // if this PM did not have a DB file (delayed file creation).
    RETURN_ON_ERROR( columnInfo.createDelayedFileIfNeeded(fTableName) );

    if(columnInfo.column.colType == COL_TYPE_DICT)
        rc = parseDict(columnInfo);
    else
        rc = parseCol(columnInfo);

    return rc;
}

//------------------------------------------------------------------------------
// Parse nonDictionary column Read buffer.  Parsed row values are added to
// fColBufferMgr, which stores them into an output buffer before writing them
// out to the applicable column segment file.
//------------------------------------------------------------------------------
int  BulkLoadBuffer::parseCol(ColumnInfo &columnInfo)
{
    int     rc = NO_ERROR;

    // Parse the data and fill up a buffer; which is written to output file
    uint nRowsParsed;

    if (fLog->isDebug( DEBUG_2 ))
    {
        ostringstream oss;
        oss << "ColResSecIn:  OID-" << columnInfo.column.mapOid <<
            "; StartRID/Rows: " << fStartRowParser << " " <<
            fTotalReadRowsParser;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    ColumnBufferSection *section = 0;
    RID lastInputRowInExtent;
    RETURN_ON_ERROR( columnInfo.fColBufferMgr->reserveSection(
        fStartRowParser, fTotalReadRowsParser, nRowsParsed,
        &section, lastInputRowInExtent ) );

    if (nRowsParsed > 0)
    {
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_PARSE_COL);
#endif
        // Reserve auto-increment numbers we need to generate
        if ((columnInfo.column.autoIncFlag) &&
            (fAutoIncGenCountParser > 0))
        {
            rc = columnInfo.reserveAutoIncNums( fAutoIncGenCountParser,
                                           fAutoIncNextValue );
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                ostringstream oss;
                oss << "parseCol: error generating auto-increment values "
                    "for table-" << fTableName <<
                    ", column-" << columnInfo.column.colName <<
                    "; OID-" << columnInfo.column.mapOid <<
                    "; " << ec.errorString(rc);
                fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
                return rc;
            }
        }

        // create a buffer for the size of the rows being written.
        unsigned char *buf = new unsigned char[fTotalReadRowsParser *
                                              columnInfo.column.width];
        char  *field = new char[MAX_FIELD_SIZE+1];

        // Initialize min/max buffer values.  We initialize to a sufficient
        // range to force the first value to automatically update the range.
        // If we are managing char data, minBufferVal and maxBufferVal are
        // maintained in reverse byte order to facilitate string comparisons
        BLBufferStats bufStats(columnInfo.column.dataType);
        bool    updateCPInfoPendingFlag = false;

        int  tokenLength   = 0;
        bool tokenNullFlag = false;
        for(uint i=0; i<fTotalReadRowsParser; ++i)
        {
            char *p = fDataParser + fTokensParser[i][columnInfo.id].start;

            if ( fTokensParser[i][columnInfo.id].offset > 0)
            {
                memcpy( field, p, fTokensParser[i][columnInfo.id].offset );
                field[fTokensParser[i][columnInfo.id].offset] = '\0';
                tokenLength   = fTokensParser[i][columnInfo.id].offset;
                tokenNullFlag = false;
            }
            else
            {
                field[0]      = '\0';
                tokenLength   = 0;
                tokenNullFlag = true;
            }

            // convert the data into appropriate format.
            convert(field, tokenLength, tokenNullFlag,
                buf + i * columnInfo.column.width,
                columnInfo.column, bufStats);
            updateCPInfoPendingFlag = true;

            // Update CP min/max if this is last row in this extent
            if ( (fStartRowParser + i) == lastInputRowInExtent )
            {
                columnInfo.updateCPInfo( lastInputRowInExtent,
                                         bufStats.minBufferVal,
                                         bufStats.maxBufferVal,
                                         columnInfo.column.dataType );

                if (fLog->isDebug( DEBUG_2 ))
                {
                    ostringstream oss;
                    oss << "ColRelSecOut: OID-" << columnInfo.column.mapOid
                        << "; StartRID/Rows1: " << section->startRowId()
                        << " " << i+1
                        << "; lastExtentRow: "  << lastInputRowInExtent;
                    parseColLogMinMax( oss,
                                       columnInfo.column.dataType,
                                       bufStats.minBufferVal,
                                       bufStats.maxBufferVal );

                    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
                }
 
                lastInputRowInExtent += columnInfo.rowsPerExtent();
                if (isUnsigned(columnInfo.column.dataType))
                {
                    bufStats.minBufferVal = static_cast<int64_t>(MAX_UBIGINT);
                    bufStats.maxBufferVal = static_cast<int64_t>(MIN_UBIGINT);
                    updateCPInfoPendingFlag = false;
                }
                else
                {
                    bufStats.minBufferVal = MAX_BIGINT;
                    bufStats.maxBufferVal = MIN_BIGINT;
                    updateCPInfoPendingFlag = false;
                }
            }
        }

        if (updateCPInfoPendingFlag)
        {
            columnInfo.updateCPInfo( lastInputRowInExtent,
                                     bufStats.minBufferVal,
                                     bufStats.maxBufferVal,
                                     columnInfo.column.dataType );
        }

        if (bufStats.satCount) // @bug 3504: increment row saturation count
        {
            // If we don't want to allow saturated values for auto inc columns.
            // then this is where we handle it.  Too late to reject a single
            // row from the parsing thread, so we abort the job.
            //if (columnInfo.column.autoIncFlag)
            //{
            //    rc = ERR_AUTOINC_USER_OUT_OF_RANGE;
            //    WErrorCodes ec;
            //    ostringstream oss;
            //    oss << "parseCol: error with auto-increment values "
            //        "for table-" << fTableName <<
            //        ", column-" << columnInfo.column.colName <<
            //        "; OID-" << columnInfo.column.mapOid <<
            //        "; " << ec.errorString(rc);
            //    fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            //    return rc;
            //}
            columnInfo.incSaturatedCnt( bufStats.satCount );
        }

        delete [] field;
        section->write(buf, fTotalReadRowsParser);
        delete [] buf;
#ifdef PROFILE
        Stats::stopParseEvent(WE_STATS_PARSE_COL);
#endif

        if (fLog->isDebug( DEBUG_2 ))
        {
            ostringstream oss;
            RID rid1 = section->startRowId();
            RID rid2 = section->endRowId();
            oss << "ColRelSecOut: OID-" << columnInfo.column.mapOid     <<
                "; StartRID/Rows2: "    << rid1 << " " << (rid2-rid1)+1 <<
                "; startOffset: "       << section->getStartOffset()    <<
                "; lastExtentRow: "     << lastInputRowInExtent;
            parseColLogMinMax( oss,
                               columnInfo.column.dataType,
                               bufStats.minBufferVal,
                               bufStats.maxBufferVal );

            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        RETURN_ON_ERROR(columnInfo.fColBufferMgr->releaseSection(section));
    }

    return rc;
}

//------------------------------------------------------------------------------
// Log the specified min/max buffer values to the log file.  This is straight
// forward for numeric types, but for character data, we have to reverse the
// order of min/max values, because they are maintained in reverse order to
// facilitate the comparison of character strings in an int64_t variable.
//------------------------------------------------------------------------------
void BulkLoadBuffer::parseColLogMinMax(
    ostringstream&             oss,
    ColDataType                colDataType,
    int64_t                    minBufferVal,
    int64_t                    maxBufferVal ) const
{
    if (isCharType(colDataType))
    {
        // Swap/restore byte order before printing character string
        int64_t minVal = static_cast<int64_t>( uint64ToStr(
            static_cast<uint64_t>(minBufferVal) ) );
        int64_t maxVal = static_cast<int64_t>( uint64ToStr(
            static_cast<uint64_t>(maxBufferVal) ) );
        char minValStr[sizeof(int64_t) + 1];
        char maxValStr[sizeof(int64_t) + 1];
        memcpy(minValStr, &minVal, sizeof(int64_t));
        memcpy(maxValStr, &maxVal, sizeof(int64_t));
        minValStr[sizeof(int64_t)] = '\0';
        maxValStr[sizeof(int64_t)] = '\0';
        oss << "; minVal: " << minVal << "; (" << minValStr << ")"
            << "; maxVal: " << maxVal << "; (" << maxValStr << ")";
    }
    else 
    if (isUnsigned(colDataType))
    {
        oss << "; minVal: " << static_cast<uint64_t>(minBufferVal) <<
               "; maxVal: " << static_cast<uint64_t>(maxBufferVal);
    }
    else
    {
        oss << "; minVal: " << minBufferVal <<
               "; maxVal: " << maxBufferVal;
    }
}

//------------------------------------------------------------------------------
// Parse Dictionary column Read buffer.  Parsed row values are added to
// fColBufferMgr, which stores them into an output buffer before writing them
// out to the applicable column segment (token) file.  This gets a little sticky
// here if the amount of data (in the column file) crosses an extent boundary.
// In this case, we have to split up the tokens into 2 column segment files
// and of course split up the corresponding strings into 2 different dictionary
// store files as well.
//------------------------------------------------------------------------------
int  BulkLoadBuffer::parseDict(ColumnInfo &columnInfo)
{
    int rc = NO_ERROR;

    uint nRowsParsed1;
    rc = parseDictSection( columnInfo,
                           0,
                           fStartRowParser,
                           fTotalReadRowsParser,
                           nRowsParsed1 );
    if (rc != NO_ERROR)
    {
        WErrorCodes ec;
        ostringstream oss;
        oss << "parseDict: error parsing section1: "  <<
            " OID-" << columnInfo.curCol.dataFile.fid <<
            "; " << ec.errorString(rc);
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    //..If fTotalReadRows != nRowsParsed1 then reserveInSection() had to
    //  split up our input buffer tokens because they spanned 2 extents.
    //  After exiting reserveSection() above, we no longer have a mutex
    //  lock on the sections in the internal buffer, so you might think
    //  this could cause a race condition with more rows being added to
    //  the buffer by other parsing threads, while we are busy wrapping
    //  up the first extent and creating the second.  But since reserve-
    //  Section() only took some of the rows from the Read buffer, any
    //  other threads should be blocked waiting for us to add the remain-
    //  ing rows from "this" Read buffer into a new ColumnBufferSection.
    //  The following condition wait in reserveSection() should be keeping
    //  things stable:
    //      while((fMaxRowId + 1) != startRowId) {
    //        //Making sure that allocation are made in order
    //        fOutOfSequence.wait(lock);
    //      }

    if (fTotalReadRowsParser != nRowsParsed1)
    {
        if (fLog->isDebug( DEBUG_1 ))
        {
            ostringstream oss;
            oss << "parseDict breaking up bufsec for OID-" <<
                columnInfo.curCol.dataFile.fid <<
                "; file-" << columnInfo.curCol.dataFile.fSegFileName <<
                "; totalInRows-" << fTotalReadRowsParser <<
                "; rowsFlushedToEndExtent-" << nRowsParsed1;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        //..Flush the rows in the buffer that fill up the current extent
        rc = columnInfo.fColBufferMgr->intermediateFlush();
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "parseDict: error flushing column: "   <<
                " OID-" << columnInfo.curCol.dataFile.fid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        //..See if we just finished filling in the last extent for this seg-
        //  ment token file, in which case we can truncate the corresponding
        //  dictionary store segment file. (this only affects compressed data).
        uint16_t root = columnInfo.curCol.dataFile.fDbRoot;
        uint32_t pNum = columnInfo.curCol.dataFile.fPartition;
        uint16_t sNum = columnInfo.curCol.dataFile.fSegment;
        bool bFileComplete = columnInfo.isFileComplete();

        //..Close the current segment file, and add an extent to the next
        //  segment file in the rotation sequence.  newSegmentFile is a
        //  FILE* that points to the newly opened segment file.
        rc = columnInfo.fColBufferMgr->extendTokenColumn( );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "parseDict: error extending column: "  <<
                " OID-" << columnInfo.curCol.dataFile.fid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        //..Close current dictionary store file and open the dictionary
        //  store file that will match the newly opened column segment file.
        rc = columnInfo.closeDctnryStore(false);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "parseDict: error closing store file: "<<
                " OID-" << columnInfo.column.dctnry.dctnryOid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        rc = columnInfo.openDctnryStore( false );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "parseDict: error opening store file: "<<
                " OID-" << columnInfo.column.dctnry.dctnryOid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            // Ignore return code from closing file; already in error state
            columnInfo.closeDctnryStore(true); // clean up loose ends
            return rc;
        }

        //..Now we can add the remaining rows in the current Read buffer to
        //  to the output buffer destined for the next extent we just added.
        uint nRowsParsed2;
        rc = parseDictSection( columnInfo,
                               nRowsParsed1,
                              (fStartRowParser+nRowsParsed1),
                              (fTotalReadRowsParser-nRowsParsed1),
                               nRowsParsed2 );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            ostringstream oss;
            oss << "parseDict: error parsing section2: "  <<
                " OID-" << columnInfo.curCol.dataFile.fid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }

        //..We went ahead and completed all the necessary parsing to free up
        //  the buffer we were working on, so that any blocked threads can
        //  continue.  In the mean time, this thread can now go back and
        //  truncate the dctnry store file we just completed, if applicable.
        if (bFileComplete)
        {
            rc = columnInfo.truncateDctnryStore(
                columnInfo.column.dctnry.dctnryOid, root, pNum, sNum);
            if (rc != NO_ERROR)
                 return rc;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Parses all or part of a Dictionary Read buffer into a ColumnBufferSection,
// depending on whether the buffer crosses an extent boundary or not.  If it
// crosses such a boundary, then parseDictSection() will only parse the buffer
// up to the end of the current extent.  A second call to parseDictSection()
// should be made to parse the remainder of the buffer into the second extent.
//------------------------------------------------------------------------------
int  BulkLoadBuffer::parseDictSection(ColumnInfo& columnInfo,
                                      int         tokenPos,
                                      RID         startRow,
                                      uint        totalReadRows,
                                      uint&       nRowsParsed)
{
    int rc = NO_ERROR;

    if (fLog->isDebug( DEBUG_2 ))
    {
        ostringstream oss;
        oss << "DctResSecIn:  OID-" << columnInfo.column.mapOid <<
            "; StartRID/Rows: " << startRow << " " << totalReadRows;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    ColumnBufferSection *section = 0;
    RID lastInputRowInExtent = 0;
    RETURN_ON_ERROR( columnInfo.fColBufferMgr->reserveSection(
            startRow, totalReadRows, nRowsParsed,
            &section, lastInputRowInExtent ) );

    if (nRowsParsed > 0)
    {
        char* tokenBuf    = new char[nRowsParsed * 8];

        // Pass fDataParser data and fTokensParser meta data to dictionary
        // to be parsed and tokenized, with tokens returned in tokenBuf.
        rc = columnInfo.updateDctnryStore( fDataParser,
            &fTokensParser[tokenPos],
            nRowsParsed,
            tokenBuf ) ;

        if(rc == NO_ERROR)
        {
#if 0
            int64_t* tokenVals = reinterpret_cast<int64_t*>(tokenBuf);
            for (unsigned int j=0; j<nRowsParsed; j++)
            {
                if (tokenVals[j] == 0)
                {
                    ostringstream oss;
                    oss << "Warning: 0 token being stored for OID-" <<
                        columnInfo.curCol.dataFile.fid << "; file-" <<
                        columnInfo.curCol.dataFile.fSegFileName <<
                        "; input row number-"<<fStartRowForLoggingParser+j;
                    fLog->logMsg( oss.str(), MSGLVL_INFO1 );
                }
            }
#endif
            section->write(tokenBuf, nRowsParsed);
            delete [] tokenBuf;

            if (fLog->isDebug( DEBUG_2 ))
            {
                ostringstream oss;
                RID rid1 = section->startRowId();
                RID rid2 = section->endRowId();
                oss << "DctRelSecOut: OID-" << columnInfo.column.mapOid   <<
                    "; StartRID/Rows: "   << rid1 << " " << (rid2-rid1)+1 <<
                    "; startOffset: "     << section->getStartOffset();
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }

            RETURN_ON_ERROR(
                columnInfo.fColBufferMgr->releaseSection(section) );
        }
        else
        {
            delete [] tokenBuf;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Read the next set of rows from the input import file (for the specified
// table), into "this" BulkLoadBuffer.
// totalReadRows (input/output) - total row count from tokenize() (per file)
// correctTotalRows (input/output) - total valid row count from tokenize()
//   (cumulative)
//------------------------------------------------------------------------------
int  BulkLoadBuffer::fillFromFile(
    const BulkLoadBuffer& overFlowBufIn,
    FILE * handle, RID & totalReadRows, RID & correctTotalRows,
    const boost::ptr_vector<ColumnInfo>& columnsInfo,
    unsigned int allowedErrCntThisCall )
{
    boost::mutex::scoped_lock lock(fSyncUpdatesBLB);
    reset();
    copyOverflow( overFlowBufIn );
    size_t readSize = 0;

    // Copy the overflow data from the last buffer, that did not get written
    if(fOverflowSize != 0)
    {
        memcpy( fData, fOverflowBuf, fOverflowSize );
        if(fOverflowBuf != NULL)
        {
            delete [] fOverflowBuf;
            fOverflowBuf = NULL;
        }
    }
        
    readSize = fread( fData + fOverflowSize, 
               1, fBufferSize - fOverflowSize, handle );
    if ( ferror(handle) )
    {
        return ERR_FILE_READ_IMPORT;
    }

    bool bEndOfData = false;
    if (feof(handle))
        bEndOfData = true;
    if ( bEndOfData && // @bug 3516: Add '\n' if missing from last record
       (fImportDataMode == IMPORT_DATA_TEXT) ) // Only applies to ascii mode
    {
        if ( (fOverflowSize > 0) | (readSize > 0) )
        {
            if ( fData[ fOverflowSize + readSize - 1 ] != '\n' )
            {
                // Should be safe to add byte to fData w/o risk of overflowing,
                // since we hit EOF.  That should mean fread() did not read all
                // the bytes we requested, meaning we have room to add a byte.
                fData[ fOverflowSize + readSize ] = '\n';
                readSize++;
            }
        }
    }

    // Lazy allocation of fToken memory as needed
    if (fTokens == 0)
    {
        resizeTokenArray();
    }

    if ((readSize > 0) || (fOverflowSize > 0))
    {
        if(fOverflowBuf != NULL)
        {
            delete [] fOverflowBuf;
            fOverflowBuf = NULL;
        }

        fReadSize = readSize + fOverflowSize;
        fStartRow = correctTotalRows;
        fStartRowForLogging = totalReadRows;

        if (fImportDataMode == IMPORT_DATA_TEXT)
        {
            tokenize( columnsInfo, allowedErrCntThisCall );
        }
        else
        {
            int rc = tokenizeBinary( columnsInfo, allowedErrCntThisCall,
                bEndOfData );
            if (rc != NO_ERROR)
                return rc;
        }

        // If we read a full buffer without hitting any new lines, then
        // terminate import because row size is greater than read buffer size.
        if ((fTotalReadRowsForLog == 0) && (fReadSize == fBufferSize))
        {
            return ERR_BULK_ROW_FILL_BUFFER;
        }

        totalReadRows    += fTotalReadRowsForLog;
        correctTotalRows += fTotalReadRows;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Parse the rows of data in "fData", saving the meta information that describes
// the parsed data, in fTokens.  If the number of read parsing errors for a
// given call to tokenize() should exceed the value of "allowedErrCntThisCall",
// then tokenize() will stop reading data and exit.
//
// We parse the data using the following state machine-like table.
// Enclosed by character ("), escaped by character (\), and field delimiter
// (|) can all be overridden; but we show default values in the state table.
//
//                    Character(s) found and action taken
//         
//   Current          \" or    "          |        \n       other
//   State            ""                                    character
//   -----------------------------------------------------------------------
//   LEADING_CHAR   | n/a      ENCLOSED   endFld   endFld   NORMAL
//   TRAILING_CHAR  | n/a      n/a        endFld   endFld   ignore
//   ENCLOSED       | convert  TRAIL      n/a      n/a      n/a
//   NORMAL         | n/a      n/a        endFld   endFld   n/a
//   -----------------------------------------------------------------------
//
//   n/a      - not applicable; no check is made for this specific character in
//              this state
//   ENCLOSED - transition to ENCLOSED state
//   TRAIL    - transition to TRAILING_CHAR state
//   NORMAL   - transition to NORMAL state
//   convert  - convert an escaped double quote (\" or "") to a single double
//              quote ("), and strip out the other character
//
// The initial parsing state for each column is LEADING_CHAR or NORMAL,
// depending on whether the user has enabled the "enclosed by" feature.
//------------------------------------------------------------------------------
void BulkLoadBuffer::tokenize(
    const boost::ptr_vector<ColumnInfo>& columnsInfo,
    unsigned int allowedErrCntThisCall )
{
    unsigned offset=0;      // length of field
    unsigned curCol=0;      // dest db column counter within a row
    unsigned curFld=0;      // src input field counter within a row
    unsigned curRowNum=0;   // "total" number of rows read during this call
    unsigned curRowNum1=0;  // number of "valid" rows inserted into fTokens
    char* p;                // iterates thru each byte in the input buffer
    char  c;                // value of byte at address "p".
    char* lastRowHead = 0;  // start of latest row being processed
    bool bValidRow = true;  // track whether current row is valid
    bool bRowGenAutoInc=false;//track whether row uses generated auto-inc
    std::string validationErrMsg;//validation error msg (if any) for current row
    unsigned errorCount             = 0;
    const char FIELD_DELIM_CHAR     = fColDelim;
    const char STRING_ENCLOSED_CHAR = fEnclosedByChar;
    const char ESCAPE_CHAR          = fEscapeChar;

    // Variables used to store raw data read for a row; needed if we strip out
    // enclosed char(s) and later have to print original data in a *.bad file
    char* pRawDataRow = 0;
    unsigned rawDataRowCapacity = 0;
    unsigned rawDataRowLength   = 0;
    const unsigned MIN_RAW_DATA_CAP = 1024;

    // Enable "enclosed by" checking if user specified an "enclosed by" char
    FieldParsingState initialState = FLD_PARSE_NORMAL_STATE;
    if (STRING_ENCLOSED_CHAR != '\0')
    {
        initialState = FLD_PARSE_LEADING_CHAR_STATE;
    }
    FieldParsingState fieldState = initialState;
    bool bNewLine    = false;        // Tracks new line
    unsigned start   = 0;            // Where next field starts in fData
    unsigned idxFrom = 0;            // idxFrom and idxTo are used to strip out
    unsigned idxTo   = 0;            //   escape characters in \" and ""

    // Initialize which field values are enclosed
    unsigned int enclosedFieldFlag = 0;
#ifdef DEBUG_TOKEN_PARSING
    unsigned int enclosedFieldFlags[fNumberOfColumns];
    memset (enclosedFieldFlags, 0, sizeof(unsigned)*fNumberOfColumns);
#endif

    p = lastRowHead = fData;
    const char* pEndOfData = p + fReadSize; //@bug3810 set an end-of-data marker

    //--------------------------------------------------------------------------
    // Loop through all the bytes in the read buffer in order to construct
    // the meta data stored in fTokens.
    //--------------------------------------------------------------------------
    while( p < pEndOfData )
    {
        c = *p;

        // If we have stripped "enclosed" characters, then save raw data
        if (rawDataRowLength > 0)
        {
            if (rawDataRowLength == rawDataRowCapacity) // resize array if full
            {
                rawDataRowCapacity = rawDataRowCapacity * 2;
                resizeRowDataArray( &pRawDataRow,
                    rawDataRowLength, rawDataRowCapacity );
            }

            pRawDataRow[rawDataRowLength] = c;
            rawDataRowLength++;
        }

        //----------------------------------------------------------------------
        // Branch based on current parsing state for this field.
        // Note that we fall out of switch/case and do more processing if we
        // have hit end of column or line; else we "continue" directly to end
        // of loop to process the next byte.
        //----------------------------------------------------------------------
        switch (fieldState)
        {
            //------------------------------------------------------------------
            // FLD_PARSE_NORMAL_STATE
            // Field not enclosed in a string delimiter such as a double quote
            //------------------------------------------------------------------
            case FLD_PARSE_NORMAL_STATE:
            {
                if ((c == FIELD_DELIM_CHAR) || (c == NEWLINE_CHAR))
                {
                    start = p - fData - offset;

                    if (c == NEWLINE_CHAR)
                        bNewLine = true;
                }
                else
                {
                    offset++;
                    p++;
                    continue; // process next byte
                }

                break;
            }

            // If state is something other than FLD_PARSE_NORMAL_STATE, then
            // there is extra processing to allow for fields that may be en-
            // closed within a string delimiter (such as a double quote)

            //----------------------------------------------------------------
            // FLD_PARSE_LEADING_CHAR_STATE
            //----------------------------------------------------------------
            case FLD_PARSE_LEADING_CHAR_STATE:
            {
                bool bNewColumn = false;
                if (c == STRING_ENCLOSED_CHAR)
                {
                    fieldState = FLD_PARSE_ENCLOSED_STATE;
                    idxFrom    = p - fData + 1;
                    idxTo      = idxFrom;
                    start      = idxTo;
                    offset     = 0;
                    enclosedFieldFlag = 1;
                }

                else if ((c == FIELD_DELIM_CHAR) || (c == NEWLINE_CHAR))
                {
                    bNewColumn = true;
                    start      = p - fData;
                    offset     = 0;

                    if ( c == NEWLINE_CHAR )
                        bNewLine = true;
                }

                else
                {
                    fieldState = FLD_PARSE_NORMAL_STATE;
                    start      = p - fData;
                    offset     = 1;
                }

                if (!bNewColumn)
                {
                    p++;
                    continue; // process next byte
                }

                break;
            }

            //------------------------------------------------------------------
            // FLD_PARSE_ENCLOSED_STATE
            //------------------------------------------------------------------
            case FLD_PARSE_ENCLOSED_STATE:
            {
                if ( (p+1 < pEndOfData) &&
                    (((c == ESCAPE_CHAR) &&
                       ( *(p+1) == STRING_ENCLOSED_CHAR)) ||
                     ((c == STRING_ENCLOSED_CHAR ) &&
                       ( *(p+1) == STRING_ENCLOSED_CHAR)) ||
                     ((c == ESCAPE_CHAR) &&
                       ( *(p+1) == ESCAPE_CHAR))) )
                {
                    // Create/save original data before stripping out bytes
                    if (rawDataRowLength == 0)
                    {
                        rawDataRowLength = (p+1) - lastRowHead + 1;
                        rawDataRowCapacity = rawDataRowLength*2;
                        if (rawDataRowCapacity < MIN_RAW_DATA_CAP)
                            rawDataRowCapacity = MIN_RAW_DATA_CAP;
                        pRawDataRow = new char[rawDataRowCapacity];
                        memcpy(pRawDataRow,
                               lastRowHead,
                               rawDataRowLength);
                    }
                    else
                    {
                        if (rawDataRowLength == rawDataRowCapacity)
                        {   // resize array if full
                            rawDataRowCapacity = rawDataRowCapacity * 2;
                            resizeRowDataArray( &pRawDataRow,
                                rawDataRowLength, rawDataRowCapacity );
                        }

                        pRawDataRow[rawDataRowLength] = *(p+1);
                        rawDataRowLength++;
                    }

                    fData[ idxTo ] = *(p+1);
                    idxFrom += 2;
                    idxTo++;
                    offset++;
                    p++;
                }

                else if (c == STRING_ENCLOSED_CHAR)
                {
                    fieldState = FLD_PARSE_TRAILING_CHAR_STATE;
                }

                else
                {
                    if (idxTo != idxFrom)
                      fData[ idxTo ] = fData[ idxFrom ];
                    idxFrom++;
                    idxTo++;
                    offset++;
                }

                p++;
                continue; // process next byte
            }

            //------------------------------------------------------------------
            // FLD_PARSE_TRAILING_CHAR_STATE
            // Ignore any trailing chars till we reach field or line delimiter.
            //------------------------------------------------------------------
            case FLD_PARSE_TRAILING_CHAR_STATE:
            default:
            {
                if ((c == FIELD_DELIM_CHAR) || (c == NEWLINE_CHAR))
                {
                    if (c == NEWLINE_CHAR)
                        bNewLine = true;
                }
                else
                {
                    p++;
                    continue; // process next byte
                }

                break;
            }
        } // end of switch on fieldState

        //----------------------------------------------------------------------
        // Finished reading the bytes in the next source field.
        // See if source field is to be included (or ignored)
        //----------------------------------------------------------------------
        if ((curFld < fNumFieldsInFile) &&
            (fFieldList[curFld].fFldColType == BULK_FLDCOL_COLUMN_FIELD))
        {
            //------------------------------------------------------------------
            // Process destination column or end of row if source field is to
            // be included as part of output to database.
            //------------------------------------------------------------------
            if (curCol < fNumColsInFile)
            {
                fTokens[curRowNum1][curCol].start  = start;
                fTokens[curRowNum1][curCol].offset = offset;
#ifdef DEBUG_TOKEN_PARSING
                enclosedFieldFlags[curCol] = enclosedFieldFlag;
#endif

                // Would like to refactor this validation logic into a separate
                // inline function, but code may be too long for compiler
                // to inline.  And factoring out into a non-inline function
                // slows down the read thread by 10%.  So left code here.
                const JobColumn& jobCol = columnsInfo[curCol].column;
                if (offset)
                {
                    switch (fTokens[curRowNum1][curCol].offset)
                    {
                        // Special auto-increment case; treat '0' as null value
                        case 1:
                        {
                            if ((jobCol.autoIncFlag) &&
                                (*(fData+fTokens[curRowNum1][curCol].start) == 
                                NULL_AUTO_INC_0))
                            {
                                fTokens[curRowNum1][curCol].offset =
                                    COLPOSPAIR_NULL_TOKEN_OFFSET;
                                bRowGenAutoInc = true;
                            }
                            else if (jobCol.dataType ==
                                     CalpontSystemCatalog::VARBINARY)
                            {
                                bValidRow = false;

                                ostringstream ossErrMsg;
                                ossErrMsg << INPUT_ERROR_ODD_VARBINARY_LENGTH <<
                                    "field " << (curFld+1) <<
                                    " has " << offset << " bytes";
                                validationErrMsg = ossErrMsg.str();
                            }
                            break;
                        }

                        case 2:
                        {
                            if((*(fData+fTokens[curRowNum1][curCol].start)  == 
                                ESCAPE_CHAR) &&
                              (*(fData+fTokens[curRowNum1][curCol].start+1) ==
                                NULL_CHAR))
                            {
                                fTokens[curRowNum1][curCol].offset =
                                    COLPOSPAIR_NULL_TOKEN_OFFSET;
                                if (jobCol.autoIncFlag)
                                    bRowGenAutoInc = true;
                            }
                            break;
                        }

                        // If enclosedFlag set, then treat "NULL" as data and
                        // not as a null value
                        case 4:
                        {
                            if ((fNullStringMode) &&
                                (!enclosedFieldFlag))
                            {
                                if((*(fData+fTokens[curRowNum1][curCol].start)==
                                    NULL_VALUE_STRING[0]) &&
                                (*(fData+fTokens[curRowNum1][curCol].start+1) ==
                                    NULL_VALUE_STRING[1]) &&
                                (*(fData+fTokens[curRowNum1][curCol].start+2) ==
                                    NULL_VALUE_STRING[2]) &&
                                (*(fData+fTokens[curRowNum1][curCol].start+3) ==
                                    NULL_VALUE_STRING[3]))
                                {
                                    fTokens[curRowNum1][curCol].offset =
                                        COLPOSPAIR_NULL_TOKEN_OFFSET;
                                    if (jobCol.autoIncFlag)
                                        bRowGenAutoInc = true;
                                }
                            }
                            break;
                        }
                                
                        default:
                        {
                            if ((jobCol.dataType ==
                                 CalpontSystemCatalog::VARBINARY) &&
                                ((offset & 1) == 1) &&
                                (bValidRow))
                            {
                                bValidRow = false;

                                ostringstream ossErrMsg;
                                ossErrMsg << INPUT_ERROR_ODD_VARBINARY_LENGTH <<
                                    "field " << (curFld+1) <<
                                    " has " << offset << " bytes";
                                validationErrMsg = ossErrMsg.str();
                            }

                            // @bug 4037: When cmd line option set, treat char
                            // and varchar fields that are too long as errors
                            else if (getTruncationAsError())
                            {
                                if ((jobCol.dataType ==
                                     CalpontSystemCatalog::VARCHAR ||
                                     jobCol.dataType ==
                                     CalpontSystemCatalog::CHAR)   
                                 && (fTokens[curRowNum1][curCol].offset >
                                     jobCol.definedWidth))
                                {
                                    bValidRow = false;
                                    ostringstream ossErrMsg;
                                    ossErrMsg << INPUT_ERROR_STRING_TOO_LONG <<
                                        "field " << (curFld+1) <<
                                        " longer than " << jobCol.definedWidth<<
                                        " bytes";
                                    validationErrMsg = ossErrMsg.str();
                                }
                            }
                             
                            // @bug 3478: Truncate instead of rejecting dctnry
                            // strings>8000. Only reject numeric cols>1000 bytes
                            else if ((fTokens[curRowNum1][curCol].offset >
                                MAX_FIELD_SIZE) &&
                                (jobCol.colType != COL_TYPE_DICT) &&
                                (bValidRow))
                            {
                                bValidRow = false;

                                ostringstream ossErrMsg;
                                ossErrMsg << INPUT_ERROR_TOO_LONG <<
                                    "field " << (curFld+1) <<
                                    " longer than " << MAX_FIELD_SIZE  <<
                                    " bytes";
                                validationErrMsg = ossErrMsg.str();
                            }
                            break;
                        }
                    } // end of switch on offset
                } // end of "if (offset)"
                else
                {
                    fTokens[curRowNum1][curCol].offset =
                        COLPOSPAIR_NULL_TOKEN_OFFSET;
                    if (jobCol.autoIncFlag)
                        bRowGenAutoInc = true;
                }

                // Validate a NotNull column is supplied a value or a default
                if ((jobCol.fNotNull) &&
                    (fTokens[curRowNum1][curCol].offset ==
                      COLPOSPAIR_NULL_TOKEN_OFFSET) &&
                    (!jobCol.fWithDefault) &&
                    (bValidRow))
                {
                    bValidRow = false;
  
                    ostringstream ossErrMsg;
                    ossErrMsg << INPUT_ERROR_NULL_CONSTRAINT <<
                        "; field " << (curFld+1);
                    validationErrMsg = ossErrMsg.str();
                }
            } // end if curCol < fNumberOfColumns

            curCol++;
        }
        curFld++;

        //----------------------------------------------------------------------
        // End-of-row processing
        //----------------------------------------------------------------------
        if (bNewLine)
        {
            // Debug: Dump next row that may or may not be accepted as
            // valid.  Not a typo, that we print "curRowNum" as the row
            // number, but we use curRowNum1 as the index into fTokens.
#ifdef DEBUG_TOKEN_PARSING
            std::cout << "Row " << curRowNum+1 << ". fTokens: " <<
                "(start,offset,enclosed)" << std::endl;
            unsigned kColCount = fNumColsInFile;
            if (curCol < kColCount)
                kColCount = curCol;
            for (unsigned int k=0; k<kColCount; k++)
            {
                std::cout << "  (" << fTokens[curRowNum1][k].start <<
                             ","   << fTokens[curRowNum1][k].offset<<
                             ","   << enclosedFieldFlags[k] << ") ";
                if (fTokens[curRowNum1][k].offset !=
                    COLPOSPAIR_NULL_TOKEN_OFFSET)
                {
                    std::string outField(fData +
                        fTokens[curRowNum1][k].start,
                        fTokens[curRowNum1][k].offset );
                    std::cout << "  " << outField << std::endl;
                }
                else
                {
                    std::cout << "  <NULL>" << std::endl;
                }
            }
#endif

            curRowNum++; // increment total number of rows read
            int rowLength = p - lastRowHead + 1;

            // @bug 3146: Allow optional trailing value at end of input file
            // Don't count last column if no data after last delimiter,
            // and we don't need that last column.
            if ((offset == 0) && (curFld == (fNumFieldsInFile+1)))
            {
                curFld--;
            }

            if ((curFld != fNumFieldsInFile) &&
                (bValidRow))
            {
                bValidRow = false;

                ostringstream ossErrMsg;
                ossErrMsg << INPUT_ERROR_WRONG_NO_COLUMNS <<
                    "; num fields expected-" << fNumFieldsInFile <<
                    "; num fields found-"    << curFld;
                validationErrMsg = ossErrMsg.str();
            }

            if (bValidRow)
            {
                // Initialize fTokens for <DefaultColumn> tags not in input file
                if (fNumColsInFile < fNumberOfColumns)
                {
                    for (unsigned int n=fNumColsInFile; n<fNumberOfColumns; n++)
                    {
                        fTokens[curRowNum1][n].start  = 0;
                        fTokens[curRowNum1][n].offset =
                            COLPOSPAIR_NULL_TOKEN_OFFSET;
                        if (columnsInfo[n].column.autoIncFlag)
                            bRowGenAutoInc = true;
                    }
                }
                curRowNum1++; // increment valid row count
                if (bRowGenAutoInc)
                    fAutoIncGenCount++; // update number of generated auto-incs
            }
            else
            {
                // Store validation error message to be logged
                if (rawDataRowLength == 0)
                {
                    string tmp(lastRowHead,rowLength);
                    fErrRows.push_back( tmp );
                }
                else
                {
                    string tmp(pRawDataRow, rawDataRowLength);
                    fErrRows.push_back( tmp );
                }

                fRowStatus.push_back(std::pair<RID,std::string>(
                    fStartRowForLogging + curRowNum,
                    validationErrMsg));

                errorCount++;

                // Quit if we exceed max allowable errors for this call.
                // We set lastRowHead = p, so that the code that follows this
                // loop won't try to save any data in fOverflowBuf.
                if (errorCount > allowedErrCntThisCall)
                {
                    lastRowHead = p + 1;
                    p++;
                    break;
                }
            }

            curCol      = 0;
            curFld      = 0;
            lastRowHead = p + 1;
            rawDataRowLength = 0;

            // Resize fTokens array if we are about to fill it up
            if ( curRowNum1 >= fTotalRows )
            {
                resizeTokenArray();
            }

            bNewLine  = false;
            bValidRow = true;
            bRowGenAutoInc = false;
#ifdef DEBUG_TOKEN_PARSING
            if (initialState == FLD_PARSE_LEADING_CHAR_STATE)
                memset (enclosedFieldFlags,0,sizeof(unsigned)*fNumberOfColumns);
#endif
        } // end of (bNewLine)

        offset     = 0;
        fieldState = initialState;
        enclosedFieldFlag = 0;

        p++;
    } // end of (p < pEndOfData) loop to step thru the read buffer

    // Save any leftover data that we did not yet parse, into fOverflowBuf
    if( p > lastRowHead )
    {
        fOverflowSize  = p - lastRowHead;
        fOverflowBuf   = new char[fOverflowSize];

        // If we stripped out any chars, be sure to preserve the original data
        if (rawDataRowLength == 0)
            memcpy( fOverflowBuf, lastRowHead, fOverflowSize );
        else
            memcpy( fOverflowBuf, pRawDataRow, fOverflowSize );
    }
    else
    {
        fOverflowSize = 0;
        fOverflowBuf  = NULL;
    }

    fTotalReadRows       = curRowNum1; // number of valid rows read
    fTotalReadRowsForLog = curRowNum;  // total number of rows read

    if (pRawDataRow)
        delete []pRawDataRow;
}

//------------------------------------------------------------------------------
// Resize the fTokens array used to store meta data about the input read buffer.
// Used for initial allocation as well.
//------------------------------------------------------------------------------
void BulkLoadBuffer::resizeTokenArray()
{
    unsigned tmpTotalRows = 0;
    if (!fTokens)
    {
        tmpTotalRows = fBufferSize / 100;

        // Estimate the number of rows we can store in
        // one buffer by getting length of first record
        for (unsigned int k=0; k<(fBufferSize-fOverflowSize); k++)
        {
            if (fData[k] == NEWLINE_CHAR)
            {
                tmpTotalRows = fBufferSize / (k+1);
                break;
            }
        }
    }
    else
    {
        tmpTotalRows = (unsigned int)(fTotalRows * 1.25);

        // @bug 3478: Make sure token array is expanded.
        // If rows are loooong, then fTotalRows may be small (< 4), in which
        // a 1.25 factor won't increase the row count.  So this check is here
        // to make sure we increase the row count in this case.
        if (tmpTotalRows <= fTotalRows)
            tmpTotalRows = fTotalRows * 2;
    }

    if (fLog->isDebug( DEBUG_1 ))
    {
        std::string allocLabel("Re-Allocating");
        if (!fTokens)
            allocLabel = "Allocating";
        ostringstream oss;
        oss << "Table: "     << fTableName <<
            "; ReadBuffer: " << fBufferId  <<
            "; " << allocLabel << " ColValue metadata of size " <<
            sizeof(ColPosPair) << " for " << tmpTotalRows <<
            " rows and " << fNumberOfColumns << " columns ";
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    ColPosPair **tmp;
    tmp = new ColPosPair *[tmpTotalRows];
    if (fTokens)
    {
        memcpy(tmp, fTokens, sizeof(ColPosPair*) * fTotalRows);
        delete [] fTokens;
    }
    fTokens = tmp;

    // Allocate a ColPosPair array for each new row
    for(unsigned i=fTotalRows; i < tmpTotalRows ; ++i)
        fTokens[i] = new ColPosPair[fNumberOfColumns];

    fTotalRows = tmpTotalRows;
}

//@bug 5027: Add tokenizeBinary() and isBinaryFieldNull() for binary imports
//------------------------------------------------------------------------------
// Alternatve version of tokenize() uesd to import fixed length records in
// binary mode.
// Parse the rows of data in "fData", saving the meta information that describes
// the parsed data, in fTokens.  If the number of read parsing errors for a
// given call to tokenize() should exceed the value of "allowedErrCntThisCall",
// then tokenize() will stop reading data and exit.
//------------------------------------------------------------------------------
int BulkLoadBuffer::tokenizeBinary(
    const boost::ptr_vector<ColumnInfo>& columnsInfo,
    unsigned int allowedErrCntThisCall,
    bool bEndOfData )
{
    unsigned curCol=0;      // dest db column counter within a row
    unsigned curRowNum=0;   // "total" number of rows read during this call
    unsigned curRowNum1=0;  // number of "valid" rows inserted into fTokens
    char* p;                // iterates thru each field in the input buffer
    char* lastRowHead = 0;  // start of latest row being processed
    bool bValidRow = true;  // track whether current row is valid
    bool bRowGenAutoInc=false;//track whether row uses generated auto-inc
    std::string validationErrMsg;//validation error msg (if any) for current row
    unsigned errorCount = 0;
    int rc = NO_ERROR;

    p = lastRowHead = fData;

    ldiv_t rowcnt = ldiv(fReadSize,fFixedBinaryRecLen);

    //--------------------------------------------------------------------------
    // Loop through all the bytes in the read buffer in order to construct
    // the meta data stored in fTokens.
    //--------------------------------------------------------------------------
    for (long kRow=0; kRow<rowcnt.quot; kRow++)
    {
        //----------------------------------------------------------------------
        // Manage all the fields in a row
        //----------------------------------------------------------------------
        for (unsigned int curFld=0; curFld<fNumFieldsInFile; curFld++)
        {
            if (fFieldList[curFld].fFldColType == BULK_FLDCOL_COLUMN_FIELD)
            {
                const JobColumn& jobCol = columnsInfo[curCol].column;

                if (curCol < fNumColsInFile)
                {
                    fTokens[curRowNum1][curCol].start  = p - fData;
                    fTokens[curRowNum1][curCol].offset = jobCol.definedWidth;

                    // Special auto-increment case; treat 0 as null value
                    if (jobCol.autoIncFlag)
                    {
                        if (memcmp(p,&NULL_AUTO_INC_0_BINARY,
                            jobCol.definedWidth) == 0)
                        {
                            fTokens[curRowNum1][curCol].offset =
                                COLPOSPAIR_NULL_TOKEN_OFFSET;
                            bRowGenAutoInc = true;
                        }
                    }

                    switch (jobCol.weType)
                    {
                        case WR_CHAR:
                        {
                            // Detect empty string for CHAR and VARCHAR
                            if (*p == '\0')
                                fTokens[curRowNum1][curCol].offset =
                                    COLPOSPAIR_NULL_TOKEN_OFFSET;
                            break;
                        }
                        case WR_VARBINARY:
                        {
                            // Detect empty VARBINARY field
                            int kk;
                            for (kk=0; kk<jobCol.definedWidth; kk++)
                            {
                                if (p[kk] != '\0')
                                    break;
                            }
                            if (kk >= jobCol.definedWidth)
                                fTokens[curRowNum1][curCol].offset =
                                    COLPOSPAIR_NULL_TOKEN_OFFSET;
                            break;
                        }
                        default:
                        {
                            // In BinaryAcceptNULL mode, check for NULL value
                            if ((fTokens[curRowNum1][curCol].offset !=
                                COLPOSPAIR_NULL_TOKEN_OFFSET) &&
                                (fImportDataMode==IMPORT_DATA_BIN_ACCEPT_NULL))
                            {
                                if (isBinaryFieldNull(p,
                                    jobCol.weType,
                                    jobCol.dataType))
                                {
                                    fTokens[curRowNum1][curCol].offset =
                                        COLPOSPAIR_NULL_TOKEN_OFFSET;
                                    if (jobCol.autoIncFlag)
                                        bRowGenAutoInc = true;
                                }
                            }
                            break;
                        }
                    } // end of switch (jobCol.weType)

                    // Validate NotNull column is supplied a value or a default
                    if ((jobCol.fNotNull) &&
                        (fTokens[curRowNum1][curCol].offset ==
                         COLPOSPAIR_NULL_TOKEN_OFFSET) &&
                        (!jobCol.fWithDefault) &&
                        (bValidRow))
                    {
                        bValidRow = false;
  
                        ostringstream ossErrMsg;
                        ossErrMsg << INPUT_ERROR_NULL_CONSTRAINT <<
                            "; field " << (curFld+1);
                        validationErrMsg = ossErrMsg.str();
                    }
                } // end "if (curCol < fNumColsInFile)"

                p += jobCol.definedWidth;
                curCol++;
            }
            else
            {
                // This is where we would handle <IgnoreField> fields
                // if they were supported in Binary Import mode
                //p += ?
            }
        } // end of loop through fields in a row

        //----------------------------------------------------------------------
        // End-of-row processing
        //----------------------------------------------------------------------

        curRowNum++; // increment total number of rows read

        if (bValidRow)
        {
            // Initialize fTokens for <DefaultColumn> tags not in input file
            if (fNumColsInFile < fNumberOfColumns)
            {
                for (unsigned int n=fNumColsInFile; n<fNumberOfColumns; n++)
                {
                    fTokens[curRowNum1][n].start  = 0;
                    fTokens[curRowNum1][n].offset =
                        COLPOSPAIR_NULL_TOKEN_OFFSET;
                    if (columnsInfo[n].column.autoIncFlag)
                        bRowGenAutoInc = true;
                }
            }
            curRowNum1++; // increment valid row count
            if (bRowGenAutoInc)
                fAutoIncGenCount++; // update number of generated auto-incs
        }
        else
        {
            // Store validation error message to be logged
            string tmp(lastRowHead,fFixedBinaryRecLen);
            fErrRows.push_back( tmp );

            fRowStatus.push_back(std::pair<RID,std::string>(
                fStartRowForLogging + curRowNum,
                validationErrMsg));

            errorCount++;

            // Quit if we exceed max allowable errors for this call
            if (errorCount > allowedErrCntThisCall)
                break;
        }

        curCol      = 0;
        lastRowHead+= fFixedBinaryRecLen;

        // Resize fTokens array if we are about to fill it up
        if ( curRowNum1 >= fTotalRows )
        {
            resizeTokenArray();
        }

        bValidRow      = true;
        bRowGenAutoInc = false;
    } // end of loop through the rows in the read buffer

    // Save any leftover data that we did not yet parse, into fOverflowBuf
    if (rowcnt.rem > 0)
    {
        if (bEndOfData)
        {
            rc = ERR_BULK_BINARY_PARTIAL_REC;
            ostringstream oss;
            oss << "Incomplete record (" << rowcnt.rem << " bytes) at end "
                "of import data; expected fixed length records of length " <<
                fFixedBinaryRecLen << " bytes";
            fLog->logMsg(oss.str(), rc, MSGLVL_ERROR);
        }
        else
        {
            fOverflowSize  = rowcnt.rem;
            fOverflowBuf   = new char[fOverflowSize];

            memcpy( fOverflowBuf, (fData+fReadSize-rowcnt.rem),
                fOverflowSize );
        }
    }
    else
    {
        fOverflowSize = 0;
        fOverflowBuf  = NULL;
    }

    fTotalReadRows       = curRowNum1; // number of valid rows read
    fTotalReadRowsForLog = curRowNum;  // total number of rows read

    return rc;
}

//------------------------------------------------------------------------------
// Compare the numeric value (val) against the relevant NULL value, based on
// column type (ct and dt), to see whether the specified value is NULL.
//------------------------------------------------------------------------------
bool BulkLoadBuffer::isBinaryFieldNull(void* val,
    WriteEngine::ColType ct,
    execplan::CalpontSystemCatalog::ColDataType dt)
{
    bool isNullFlag = false;
    switch (ct)
    {
        case WriteEngine::WR_BYTE:
        {
            if ((*(uint8_t*)val)  == joblist::TINYINTNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_SHORT:
        {
            if ((*(uint16_t*)val) == joblist::SMALLINTNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_INT:
        {
            if (dt == execplan::CalpontSystemCatalog::DATE)
            {
                if ((*(uint32_t*)val) == joblist::DATENULL)
                    isNullFlag = true;
            }
            else
            {
                if ((*(uint32_t*)val) == joblist::INTNULL)
                    isNullFlag = true;
            }
            break;
        }

        case WriteEngine::WR_LONGLONG:
        {
            if (dt == execplan::CalpontSystemCatalog::DATETIME)
            {
                if ((*(uint64_t*)val) == joblist::DATETIMENULL)
                    isNullFlag = true;
            }
            else
            {
                if ((*(uint64_t*)val) == joblist::BIGINTNULL)
                    isNullFlag = true;
            }
            break;
        }

        case WriteEngine::WR_FLOAT:
        {
            if ((*(uint32_t*)val) == joblist::FLOATNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_DOUBLE:
        {
            if ((*(uint64_t*)val) == joblist::DOUBLENULL)
                isNullFlag = true;
            break;
        }

        // Detect empty string for CHAR and VARCHAR
        case WriteEngine::WR_CHAR:
        {
            // not applicable
            break;
        }

        // Detect empty VARBINARY field
        case WriteEngine::WR_VARBINARY:
        {
            // not applicable
            break;
        }

        case WriteEngine::WR_UBYTE:
        {
            if ((*(uint8_t*)val)  == joblist::UTINYINTNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_USHORT:
        {
            if ((*(uint16_t*)val) == joblist::USMALLINTNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_UINT:
        {
            if ((*(uint32_t*)val) == joblist::UINTNULL)
                isNullFlag = true;
            break;
        }

        case WriteEngine::WR_ULONGLONG:
        {
            if ((*(uint64_t*)val) == joblist::UBIGINTNULL)
                isNullFlag = true;
            break;
        }

        default:
        {
            break;
        }
    }

    return isNullFlag;
}

//------------------------------------------------------------------------------
// Sets the column status.
// returns TRUE if all columns in the buffer are complete.
//
// Note that fSyncUpdatesTI mutex is used to synchronize usage of fColumnLocks
// and fParseComplete from both read and parse threads.
//
// setColumnStatus() and tryAndLockColumn() formerly used fSyncUpdatesBLB mutex.
// But this seemed inconsistent because resetColumnLocks(), getColumnStatus(),
// and getColumnLocker() were not using this mutex.  In researching the idea of
// adding fSyncUpdatesBLB locks to these functions, I determined, that all the
// calls to the following functions were protected by a fSyncUpdatesTI mutex:
//   setColumnStatus()
//   tryAndLockColumn()
//   resetColumnLocks()
//   getColumnStatus()
//   getColumnLocker()
// So I added this note and removed the extraneous fSyncUpdatesBLB lock from
// setColumnStatus() and tryAndLockColumn().  (dmc-07/19/2009)
//------------------------------------------------------------------------------
bool BulkLoadBuffer::setColumnStatus(const int &columnId,
                                     const Status & status)
{
    fColumnLocks[columnId].status = status;

    if(status == WriteEngine::PARSE_COMPLETE)
        fParseComplete++;

    if (fParseComplete == fNumberOfColumns)
        return true;

    return false;
}

//------------------------------------------------------------------------------
// Note that fSyncUpdatesTI mutex is used to synchronize usage of fColumnLocks
// and fParseComplete from both read and parse threads.
//------------------------------------------------------------------------------
bool BulkLoadBuffer::tryAndLockColumn(const int & columnId, const int & id)
{
    if((fColumnLocks[columnId].status != WriteEngine::PARSE_COMPLETE) &&
       (fColumnLocks[columnId].locker == -1))
    {
        fColumnLocks[columnId].locker = id;
        return true;
    }

    return false;
}

}
