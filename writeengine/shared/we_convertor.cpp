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

/*******************************************************************************
* $Id: we_convertor.cpp 4726 2013-08-07 03:38:36Z bwilkinson $
*
*******************************************************************************/
/** @file */

#include <unistd.h>
#include <limits>
#include <cstring>
#ifdef _MSC_VER
#include <cstdio>
#endif
#include "config.h"

#define WRITEENGINECONVERTOR_DLLEXPORT
#include "we_convertor.h"
#undef WRITEENGINECONVERTOR_DLLEXPORT

using namespace std;
using namespace execplan;
namespace
{
    const char DATE_TIME_FORMAT[] = "%04d-%02d-%02d %02d:%02d:%02d";

/*******************************************************************************
 * DESCRIPTION:
 *    Takes an 8-bit value and converts it into a directory name.
 * PARAMETERS:
 *    pBuffer(output) - a pointer to the output buffer
 *    blen   (input)  - the length of the output buffer (in bytes)
 *    val    (input)  - value to be used in the formatted name
 * RETURN:
 *    the number of characters printed in the output buffer (not including
 *    the null terminator).  -1 is returned if the input buffer pointer is NULL.
 ******************************************************************************/
int _doDir(char* pBuffer, int blen, unsigned int val)
{
    int rc;

    if (!pBuffer)
    {
        rc = -1;
    } else {
        rc = snprintf(pBuffer, blen, "%03u.dir", val);
            pBuffer[blen-1] = (char)0;
    }

    return rc;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Takes an 8-bit value and converts it into a file name.
 * PARAMETERS:
 *    pBuffer(output) - a pointer to the output buffer
 *    blen   (input)  - the length of the output buffer (in bytes)
 *    val    (input)  - value to be used in the formatted name
 * RETURN:
 *    the number of characters printed in the output buffer (not including
 *    the null terminator).  -1 is returned if the input buffer pointer is NULL.
 ******************************************************************************/
int _doFile(char* pBuffer, int blen, unsigned char val)
{
    int rc;

    if (!pBuffer)
    {
        rc = -1;
    } else {
        rc = snprintf(pBuffer, blen, "FILE%03d.cdf", val);
            pBuffer[blen-1] = (char)0;
    }

    return rc;
}
}

namespace WriteEngine
{

struct Convertor::dmFilePathArgs_t
{
    char* pDirA;    // < OUT -- DirA's buffer
    char* pDirB;    // < OUT -- DirB's buffer
    char* pDirC;    // < OUT -- DirC's buffer
    char* pDirD;    // < OUT -- DirD's buffer
    char* pDirE;    // < OUT -- DirE's buffer
    char* pFName;   // < OUT -- Filename buffer
    int     ALen;   // < IN -- Size in bytes of DirA's Buffer.
    int     BLen;   // < IN -- Size in bytes of DirB's Buffer.
    int     CLen;   // < IN -- Size in bytes of DirC's Buffer.
    int     DLen;   // < IN -- Size in bytes of DirD's Buffer.
    int     ELen;   // < IN -- Size in bytes of DirE's Buffer.
    int     FNLen;  // < IN -- Size in bytes of Filename's Buffer.
    int     Arc;    // < OUT -- result code for formatting DirA.
    int     Brc;    // < OUT -- result code for formatting DirB.
    int     Crc;    // < OUT -- result code for formatting DirC.
    int     Drc;    // < OUT -- result code for formatting DirD.
    int     Erc;    // < OUT -- result code for formatting DirE.
    int     FNrc;   // < OUT -- result code for formatting Filename.
};

/*******************************************************************************
 * DESCRIPTION:
 *    Get time string
 * PARAMETERS:
 *    none
 * RETURN:
 *    time string
 ******************************************************************************/
/* static */
const std::string Convertor::getTimeStr() 
{
    char     buf[sizeof(DATE_TIME_FORMAT)+10] = {0};
    time_t   curTime = time(NULL);
    struct tm pTime;
    localtime_r(&curTime, &pTime);    
    string   timeStr;

    snprintf(buf, sizeof(buf), DATE_TIME_FORMAT, pTime.tm_year + 1900,
    					pTime.tm_mon + 1, pTime.tm_mday,
    					pTime.tm_hour, pTime.tm_min, pTime.tm_sec);

    timeStr = buf;

    return timeStr;      
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert int value to string
 * PARAMETERS:
 *    val - value
 * RETURN:
 *    string
 ******************************************************************************/
/* static */
const std::string Convertor::int2Str(int val)
{
    char buf[12];
    string myStr;

    snprintf(buf, sizeof(buf), "%d", val);
    myStr = buf;

    return myStr;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert a numeric string to a decimal long long, given the specified
 *    scale.  errno should be checked upon return from this function to see
 *    if it is set to ERANGE, meaning the value was out of range.
 * PARAMETERS:
 *    field       - string to be interpreted
 *    fieldLength - length of "field"
 *    scale       - decimal scale value to be used to parse "field"
 * RETURN:
 *    converted long long for specified "field"
 ******************************************************************************/
/* static */
long long Convertor::convertDecimalString(
    const char* field,
    int         fieldLength,
    int         scale )
{
    long long llVal = 0;

    int nDigitsBeforeDecPt = 0;
    int nDigitsAfterDecPt  = 0;
    long long roundUp      = 0; //@bug 3405 round off decimal column values

    // Determine the number of digits before and after the decimal point
    char* posDecPt = (char*)memchr(field, '.', fieldLength);
    if (posDecPt)
    {
        nDigitsBeforeDecPt = posDecPt - field;
        nDigitsAfterDecPt  = fieldLength - nDigitsBeforeDecPt - 1;

        //@bug 3405 round off decimal column values
        // We look at the scale+1 digit to see if we need to round up.
        if (nDigitsAfterDecPt > scale)
        {
            char roundOffDigit = *(posDecPt + 1 + scale);
            if ( (roundOffDigit > '4') &&
                 (roundOffDigit <='9') ) // round up
            {
                roundUp = 1;

                // We can't just use the sign of llVal to determine whether to
                // add +1 or -1, because if we read in -0.005 with scale 2, we
                // end up parsing "-0.00", which yields 0; meaning we lose the
                // sign.  So better (though maybe slower) to look for any lead-
                // ing negative sign in the input string.
                for (int k=0; k<fieldLength; k++)
                {
                    if (!isspace(field[k]))
                    {
                        if (field[k] == '-')
                            roundUp = -1;
                        break;
                    }
                }
            }
        }
    }
    else
    {
        nDigitsBeforeDecPt = fieldLength;
        nDigitsAfterDecPt  = 0;
    }

   // Strip out the decimal point by stringing together
   // the digits before and after the decimal point.
   char* data = (char*)alloca(nDigitsBeforeDecPt + scale + 1);
   memcpy(data, field, nDigitsBeforeDecPt);
   if (nDigitsAfterDecPt)
   {
        if (scale > nDigitsAfterDecPt)
            memcpy(data  + nDigitsBeforeDecPt,
                   field + nDigitsBeforeDecPt + 1,
                   nDigitsAfterDecPt);
        else // (scale <= nDigitsAfterDecPt)
            memcpy(data  + nDigitsBeforeDecPt,
                   field + nDigitsBeforeDecPt + 1,
                   scale);
    }

    // Add on any necessary zero padding at the end
    if (scale > nDigitsAfterDecPt)
    {
        memset(data + nDigitsBeforeDecPt + nDigitsAfterDecPt,
               '0',
               scale - nDigitsAfterDecPt);
    }

    data[nDigitsBeforeDecPt + scale] = '\0';

    // Convert our constructed decimal string back to a long long
    //@bug 1814 Force strtoll to use base 10
    errno = 0;
    llVal = strtoll(data, 0, 10);

    //@bug 3405 round off decimal values
    if ((roundUp) && (errno == 0))
        llVal += roundUp;

    return llVal;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert an oid to a filename (with partition and segment number
 *    in the filepath.
 * PARAMETERS:
 *    fid - fid
 *    fullFileName - file name
 *    dbDirName - components of fullFileName
 *    partition - partition number to be in used in filepath
 *    segment   - segment number to be used in filename
 * RETURN:
 *    NO_ERROR if success, other if fail
 ******************************************************************************/
/* static */
int Convertor::oid2FileName(FID fid,
    char* fullFileName,
    char dbDirName[][MAX_DB_DIR_NAME_SIZE],
    uint32_t partition,
    uint16_t segment)
{
    dmFilePathArgs_t  args;
    int               rc;

    char aBuff[MAX_DB_DIR_NAME_SIZE];
    char bBuff[MAX_DB_DIR_NAME_SIZE];
    char cBuff[MAX_DB_DIR_NAME_SIZE];
    char dBuff[MAX_DB_DIR_NAME_SIZE];
    char eBuff[MAX_DB_DIR_NAME_SIZE];
    char fnBuff[MAX_DB_DIR_NAME_SIZE];

    args.pDirA = aBuff;
    args.pDirB = bBuff;
    args.pDirC = cBuff;
    args.pDirD = dBuff;
    args.pDirE = eBuff;
    args.pFName = fnBuff;

    args.ALen = sizeof(aBuff);
    args.BLen = sizeof(bBuff);
    args.CLen = sizeof(cBuff);
    args.DLen = sizeof(dBuff);
    args.ELen = sizeof(eBuff);
    args.FNLen = sizeof(fnBuff);

    args.Arc = 0;
    args.Brc = 0;
    args.Crc = 0;
    args.Drc = 0;
    args.Erc = 0;
    args.FNrc = 0;

    RETURN_ON_WE_ERROR(
       (rc = dmOid2FPath(fid, partition, segment, &args)),
          ERR_DM_CONVERT_OID);
    sprintf(fullFileName, "%s/%s/%s/%s/%s/%s", args.pDirA,
       args.pDirB, args.pDirC, args.pDirD, args.pDirE, args.pFName);

    strcpy(dbDirName[0], args.pDirA);
    strcpy(dbDirName[1], args.pDirB);
    strcpy(dbDirName[2], args.pDirC);
    strcpy(dbDirName[3], args.pDirD);
    strcpy(dbDirName[4], args.pDirE);
    strcpy(dbDirName[5], args.pFName);
//  std::cout << "OID: " << fid <<
//       " mapping to file: " << fullFileName <<std::endl;

    return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Map specified errno to the associated error message string.
 * PARAMETERS:
 *    errNum  - errno to be converted
 *    errString-(output) error message string associated with errNum
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::mapErrnoToString(int errNum, std::string& errString)
{
    char errnoMsgBuf[1024];
#if STRERROR_R_CHAR_P
    char* errnoMsg = strerror_r(errNum, errnoMsgBuf, sizeof(errnoMsgBuf));
    if (errnoMsg)
        errString = errnoMsg;
    else
        errString.clear();
#else
    int   errnoMsg = strerror_r(errNum, errnoMsgBuf, sizeof(errnoMsgBuf));
    if (errnoMsg == 0)
        errString = errnoMsgBuf;
    else
        errString.clear();
#endif
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert specified ColDataType to internal storage type (ColType).
 * PARAMETERS:
 *    dataType     - Interface data-type
 *    internalType - Internal data-type used for storing
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertColType(CalpontSystemCatalog::ColDataType dataType,
    ColType& internalType, bool isToken)
{
    if (isToken)
    {
        internalType = WriteEngine::WR_TOKEN;
        return;
    }

    switch(dataType) {
        // Map BIT and TINYINT to WR_BYTE
        case CalpontSystemCatalog::BIT :
        case CalpontSystemCatalog::TINYINT :
            internalType = WriteEngine::WR_BYTE; break;

        // Map SMALLINT to WR_SHORT
        case CalpontSystemCatalog::SMALLINT :
            internalType = WriteEngine::WR_SHORT; break;

        // Map MEDINT, INT, and DATE to WR_INT
        case CalpontSystemCatalog::MEDINT :
        case CalpontSystemCatalog::INT :
        case CalpontSystemCatalog::DATE :
            internalType = WriteEngine::WR_INT; break;

        // Map FLOAT and UFLOAT to WR_FLOAT
        case CalpontSystemCatalog::FLOAT :
        case CalpontSystemCatalog::UFLOAT:
            internalType = WriteEngine::WR_FLOAT; break;

        // Map BIGINT and DATETIME to WR_LONGLONG
        case CalpontSystemCatalog::BIGINT :
        case CalpontSystemCatalog::DATETIME :
            internalType = WriteEngine::WR_LONGLONG; break;

        // Map DOUBLE and UDOUBLE to WR_DOUBLE
        case CalpontSystemCatalog::DOUBLE :
        case CalpontSystemCatalog::UDOUBLE:
            internalType = WriteEngine::WR_DOUBLE; break;

        // Map BLOB to WR_BLOB
        case CalpontSystemCatalog::BLOB :
            internalType = WriteEngine::WR_BLOB; break;

        // Map VARBINARY to WR_VARBINARY
        case CalpontSystemCatalog::VARBINARY:
            internalType = WriteEngine::WR_VARBINARY; break;

        // Map DECIMAL to applicable WR_CHAR
        // We can't map them to their proper int size, since in this version
        // of convertColType(), we don't know what that is. Hopefully
        // this is never used for DECIMAL. 
        case CalpontSystemCatalog::DECIMAL :
        case CalpontSystemCatalog::UDECIMAL:

        // Map CHAR, VARCHAR, and CLOB to WR_CHAR
        case CalpontSystemCatalog::CHAR :
        case CalpontSystemCatalog::VARCHAR :
        case CalpontSystemCatalog::CLOB :
            internalType = WriteEngine::WR_CHAR; break;

        // Map UTINYINT to WR_UBYTE
        case CalpontSystemCatalog::UTINYINT:
            internalType = WriteEngine::WR_UBYTE; break;

        // Map USMALLINT to WR_USHORT
        case CalpontSystemCatalog::USMALLINT:
            internalType = WriteEngine::WR_USHORT; break;

        // Map UMEDINT and UINT to WR_UINT
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            internalType = WriteEngine::WR_UINT; break;

        // Map UBIGINT to WR_ULONGLONG
        case CalpontSystemCatalog::UBIGINT:
            internalType = WriteEngine::WR_ULONGLONG; break;

        default:
            internalType = WriteEngine::WR_CHAR; break;
    }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert specified internal storage type (ColType) to ColDataType. Since
 *    there is a one to many relationship, we choose the most general.
 * PARAMETERS:
 *    internalType - Internal data-type used for storing
 *    dataType     - Interface data-type
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertWEColType(ColType internalType, 
    CalpontSystemCatalog::ColDataType& dataType)
{
    switch(internalType) 
    {
        // Map BIT and TINYINT to WR_BYTE
        case WriteEngine::WR_BYTE :
            dataType = CalpontSystemCatalog::TINYINT; break;

        // Map SMALLINT to WR_SHORT
        case WriteEngine::WR_SHORT :
            dataType = CalpontSystemCatalog::SMALLINT; break;

        // Map MEDINT, INT, and DATE to WR_INT
        case WriteEngine::WR_INT :
            dataType = CalpontSystemCatalog::INT; break;

        // Map FLOAT and UFLOAT to WR_FLOAT
        case WriteEngine::WR_FLOAT:
            dataType = CalpontSystemCatalog::FLOAT; break;

        // Map BIGINT and DATETIME to WR_LONGLONG
        case WriteEngine::WR_LONGLONG :
            dataType = CalpontSystemCatalog::BIGINT; break;

        // Map DOUBLE and UDOUBLE to WR_DOUBLE
        case WriteEngine::WR_DOUBLE :
            dataType = CalpontSystemCatalog::DOUBLE; break;

        // Map BLOB to WR_BLOB
        case WriteEngine::WR_BLOB :
            dataType = CalpontSystemCatalog::BLOB; break;

        // Map VARBINARY to WR_VARBINARY
        case WriteEngine::WR_VARBINARY:
            dataType = CalpontSystemCatalog::VARBINARY; break;

        // Map CHAR, VARCHAR, and CLOB to WR_CHAR
        case WriteEngine::WR_CHAR :
            dataType = CalpontSystemCatalog::CHAR; break;

        // Map UTINYINT to WR_UBYTE
        case WriteEngine::WR_UBYTE:
            dataType = CalpontSystemCatalog::UTINYINT; break;

        // Map USMALLINT to WR_USHORT
        case WriteEngine::WR_USHORT:
            dataType = CalpontSystemCatalog::USMALLINT; break;

        // Map UMEDINT and UINT to WR_UINT
        case WriteEngine::WR_UINT:
            dataType = CalpontSystemCatalog::UINT; break;

        // Map UBIGINT to WR_ULONGLONG
        case WriteEngine::WR_ULONGLONG:
            dataType = CalpontSystemCatalog::UBIGINT; break;

        default:
            dataType = CalpontSystemCatalog::CHAR; break;
    }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Convert curStruct from an interface-type struct to an internal-type
 *    struct.  curStruct is handled as a ColStruct.
 * PARAMETERS:
 *    curStruct - column struct to be initialized
 * RETURN:
 *    none
 ******************************************************************************/
/* static */
void Convertor::convertColType(ColStruct* curStruct)
{
    CalpontSystemCatalog::ColDataType dataType     // This will be updated later,
                              = CalpontSystemCatalog::CHAR; // CHAR used only for initialization.
    ColType*    internalType = NULL;
    bool        bTokenFlag = false;
    int*        width = NULL;

    dataType     = curStruct->colDataType;
    internalType = &(curStruct->colType);
    bTokenFlag   = curStruct->tokenFlag;
    width        = &(curStruct->colWidth);

    switch(dataType) {
        // Map BIT and TINYINT to WR_BYTE
        case CalpontSystemCatalog::BIT :
        case CalpontSystemCatalog::TINYINT :
            *internalType = WriteEngine::WR_BYTE; break;

        // Map SMALLINT to WR_SHORT
        case CalpontSystemCatalog::SMALLINT :
            *internalType = WriteEngine::WR_SHORT; break;

        // Map MEDINT, INT, and DATE to WR_INT
        case CalpontSystemCatalog::MEDINT :
        case CalpontSystemCatalog::INT :
        case CalpontSystemCatalog::DATE :
            *internalType = WriteEngine::WR_INT; break;

        // Map FLOAT and UFLOAT to WR_FLOAT
        case CalpontSystemCatalog::FLOAT :
        case CalpontSystemCatalog::UFLOAT :
            *internalType = WriteEngine::WR_FLOAT; break;

        // Map BIGINT and DATETIME to WR_LONGLONG
        case CalpontSystemCatalog::BIGINT :
        case CalpontSystemCatalog::DATETIME :
            *internalType = WriteEngine::WR_LONGLONG; break;

        // Map DOUBLE and UDOUBLE to WR_DOUBLE
        case CalpontSystemCatalog::DOUBLE :
        case CalpontSystemCatalog::UDOUBLE :
            *internalType = WriteEngine::WR_DOUBLE; break;

        // Map DECIMAL to applicable integer type
        case CalpontSystemCatalog::DECIMAL :
        case CalpontSystemCatalog::UDECIMAL :
        {
            switch (*width)
            {
                case 1 : *internalType = WriteEngine::WR_BYTE; break;
                case 2 : *internalType = WriteEngine::WR_SHORT; break;
                case 4 : *internalType = WriteEngine::WR_INT; break;
                default: *internalType = WriteEngine::WR_LONGLONG; break;
            }
            break;
        }

        // Map BLOB to WR_BLOB
        case CalpontSystemCatalog::BLOB :
            *internalType = WriteEngine::WR_BLOB; break;

        // Map VARBINARY to WR_VARBINARY
        case CalpontSystemCatalog::VARBINARY:
            *internalType = WriteEngine::WR_VARBINARY; break;

        // Map CHAR, VARCHAR, and CLOB to WR_CHAR
        case CalpontSystemCatalog::CHAR :
        case CalpontSystemCatalog::VARCHAR :
        case CalpontSystemCatalog::CLOB :
            *internalType = WriteEngine::WR_CHAR; break;

        // Map UTINYINT to WR_UBYTE
        case CalpontSystemCatalog::UTINYINT:
            *internalType = WriteEngine::WR_UBYTE; break;

        // Map USMALLINT to WR_USHORT
        case CalpontSystemCatalog::USMALLINT:
            *internalType = WriteEngine::WR_USHORT; break;

        // Map UMEDINT and UINT to WR_UINT
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            *internalType = WriteEngine::WR_UINT; break;

        // Map UBIGINT to WR_ULONGLONG
        case CalpontSystemCatalog::UBIGINT:
            *internalType = WriteEngine::WR_ULONGLONG; break;

        default:
            *internalType = WriteEngine::WR_CHAR; break;
    }

    if (bTokenFlag)        // token overwrite any other types
        *internalType = WriteEngine::WR_TOKEN;

    // check whether width is in sync with the requirement
    *width = getCorrectRowWidth(dataType, *width);

    // This is the patch for the decimal thing, override
//  if (dataType == CalpontSystemCatalog::DECIMAL)
//  {
//      *internalType = *width <= 4 ?
//                      WriteEngine::WR_INT : WriteEngine::WR_LONGLONG;
//  }
}

/*******************************************************************************
 * DESCRIPTION:
 *    Get the correct width for a row
 * PARAMETERS:
 *    dataType - data type
 *    width - data width in byte
 * RETURN:
 *    emptyVal - the value of empty row
 ******************************************************************************/
/* static */
int Convertor::getCorrectRowWidth(CalpontSystemCatalog::ColDataType dataType, int width)
{
    int offset, newWidth = 4;

    switch(dataType) {
        case CalpontSystemCatalog::TINYINT: 
        case CalpontSystemCatalog::UTINYINT: 
            newWidth = 1; break;
        case CalpontSystemCatalog::SMALLINT: 
        case CalpontSystemCatalog::USMALLINT: 
            newWidth = 2; break;
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            newWidth = 4; break;
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::UBIGINT:
            newWidth = 8; break;
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            newWidth = 4; break;
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            newWidth = 8; break;
        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
            if (width == 1)
                newWidth = 1;
            else if (width == 2)
                newWidth = 2;
            else if (width <= 4)
                newWidth = 4;
            else
                newWidth = 8;
            break;
        case CalpontSystemCatalog::DATE: 
            newWidth = 4; break;
        case CalpontSystemCatalog::DATETIME:
            newWidth = 8; break;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::VARBINARY: // treat same as varchar for now
        default:
            offset = (dataType == CalpontSystemCatalog::VARCHAR)? -1 : 0;
            newWidth = 1;
            if (width == (2 + offset))
                newWidth = 2;
            else if (width >= (3 + offset) && width <= (4 + offset))
                newWidth = 4;
            else if (width >= (5 + offset))
                newWidth = 8;
            break;
    }

    return newWidth;
}

/*******************************************************************************
 * DESCRIPTION:
 * Converts an OID into a group if directories and a filename.
 *
 * This function takes a 32-bit Object ID (OID).  If DLen is 0, then the
 * OID is converted into 3 hierarchical directory names and a filename.
 * If DLen is >0 then the OID is converted into 5 hierarchical directory
 * names and a filename (using the partition and segment as additional
 * input into the filepath.  The actual location
 * of the file is <DBRoot>/<DirA>/<DirB>/<DirC>/<FName>, or
 * <DBRoot>/<DirA>/<DirB>/<DirC>/<DirD>/<part#>/<segFName>.  The <DBRoot>
 * entry must be pre-pended by the calling application after calling
 * this function.  The value for <DBRoot> is stored in the Calpont.xml
 * configuration file.
 *
 * PARAMETERS:
 *    oid       INPUT -- The Object Id.
 *    partition INPUT -- partition to be included in filepath.
 *    segment   INPUT -- segment to be included in filepath.
 *    dmFilePathArgs* INPUT/OUTPUT -- Points to a buffer structure
 *
 * RETURN:
 *    return 0 if everything went OK.  -1 if an error occured.  Two
 *    kinds of errors are possible:
 *
 *        - a null pointer was passed in
 *        - truncation occured.
 *
 *    If a null buffer pointer is passed in, a return code
 *    of -1 will be returned FOR THAT BUFFER.
 *
 *    Truncation can occur if the buffer length specified in
 *    dmFilePathArgs is too small.
 *
 *    If a buffer's return code is not zero, the appropriate
 *    return code in dmfilePathArgs can be examined.  If a
 *    buffer's return code is be less than zero, the
 *    corresponding buffer pointer was NULL.  If it is greater
 *    or equal to the buffer's length argument, length is too small
 ******************************************************************************/
/*static*/
int Convertor::dmOid2FPath(uint32_t oid, uint32_t partition, uint32_t segment,
    dmFilePathArgs_t* pArgs)
{
    pArgs->Arc = _doDir(
        pArgs->pDirA,
        pArgs->ALen,
        (unsigned int)oid>>24);

    pArgs->Brc = _doDir(
        pArgs->pDirB,
        pArgs->BLen,
        (unsigned int)(oid&0x00ff0000)>>16);

    pArgs->Crc = _doDir(
        pArgs->pDirC,
        pArgs->CLen,
        (unsigned int)(oid&0x0000ff00)>>8);

    // include partition and seg num in the file path if they are present
    if (pArgs->DLen > 0)
    {
        pArgs->Drc = _doDir(
            pArgs->pDirD,
            pArgs->DLen,
            (unsigned int)(oid&0x000000ff));

        pArgs->Erc = _doDir(
            pArgs->pDirE,
            pArgs->ELen,
            partition);

        pArgs->FNrc = _doFile(
            pArgs->pFName,
            pArgs->FNLen,
            segment);

        if ( (pArgs->Drc < 0) ||
             (pArgs->Erc < 0) )
            return -1;

        if ( (pArgs->Drc >= pArgs->ALen) ||
             (pArgs->Erc >= pArgs->ALen) )
            return -1;
    }
    else
    {
        pArgs->FNrc = _doFile(
            pArgs->pFName,
            pArgs->FNLen,
            (unsigned int)(oid&0x000000ff));
    }

    if ( (pArgs->Arc < 0) ||
         (pArgs->Brc < 0) ||
         (pArgs->Crc < 0) ||
         (pArgs->FNrc < 0) )
        return -1;

    if ( (pArgs->Arc >= pArgs->ALen) ||
         (pArgs->Brc >= pArgs->BLen) ||
         (pArgs->Crc >= pArgs->CLen) ||
         (pArgs->FNrc >= pArgs->FNLen) )
        return -1;
    else
        return 0;
}

} //end of namespace

