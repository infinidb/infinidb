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

/*******************************************************************************
* $Id: we_convertor.cpp 2972 2011-05-17 20:50:24Z dcathey $
*
*******************************************************************************/
/** @file */

#include <limits>
#include <cstring>
#include "config.h"

#define WRITEENGINECONVERTOR_DLLEXPORT
#include "we_convertor.h"
#undef WRITEENGINECONVERTOR_DLLEXPORT

using namespace std;

namespace WriteEngine
{

/***********************************************************
 * DESCRIPTION:
 *    Get time string
 * PARAMETERS:
 *    none
 * RETURN:
 *    time string
 ***********************************************************/
const string Convertor::getTimeStr() 
{
    char     buf[sizeof(DATE_TIME_FORMAT)+10] = {0};
    time_t   curTime = time(NULL);
    struct tm pTime;
    localtime_r(&curTime, &pTime);    
    string   timeStr;

    sprintf(buf, DATE_TIME_FORMAT, pTime.tm_year + 1900, pTime.tm_mon + 1, 
             pTime.tm_mday, pTime.tm_hour, pTime.tm_min, pTime.tm_sec);

    timeStr = buf;

    return timeStr;      
}

/***********************************************************
 * DESCRIPTION:
 *    Convert float value to string
 * PARAMETERS:
 *    val - value
 * RETURN:
 *    string
 ***********************************************************/
const string Convertor::float2Str(const float val) 
{
    char buf[10];
    string myStr;

    sprintf(buf, "%f", val);
    myStr = buf;

    return myStr;
}

/***********************************************************
 * DESCRIPTION:
 *    Convert int value to string
 * PARAMETERS:
 *    val - value
 * RETURN:
 *    string
 ***********************************************************/
const string Convertor::int2Str(const int val)
{
    char buf[12];
    string myStr;

    sprintf(buf, "%d", val);
    myStr = buf;

    return myStr;
}

/***********************************************************
 * DESCRIPTION:
 *    Convert unsigned 64 bit int value to a string
 * PARAMETERS:
 *    val - value
 * RETURN:
 *    string
 ***********************************************************/
const string Convertor::i64ToStr(const i64 val)
{
    char buf[24];
    string myStr;

#if __LP64__
    sprintf(buf, "%lu", val);
#elif __GLIBC_HAVE_LONG_LONG
    sprintf(buf, "%llu", val);
#endif

    myStr = buf;

    return myStr;
}

/***********************************************************
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
 ***********************************************************/
const int Convertor::oid2FileName(const FID fid,
    char* fullFileName,
    char dbDirName[][MAX_DB_DIR_NAME_SIZE],
    const uint32_t partition,
    const uint16_t segment)
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
       (rc = dmOid2FPath((UINT32) fid, partition, segment, &args)),
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

/***********************************************************
 * DESCRIPTION:
 *    Map specified errno to the associated error message string.
 * PARAMETERS:
 *    errNum  - errno to be converted
 *    errString-(output) error message string associated with errNum
 * RETURN:
 *    none
 ***********************************************************/
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

/***********************************************************
 * DESCRIPTION:
 *    Convert interface column type to a internal column type
 * PARAMETERS:
 *    dataType     - Interface data-type
 *    internalType - Internal data-type used for storing
 * RETURN:
 *    none
 ***********************************************************/
/* static */
void Convertor::convertColType(ColDataType dataType,
    ColType& internalType, bool isToken)
{
    if (isToken)
    {
        internalType = WriteEngine::WR_TOKEN;
        return;
    }

    switch(dataType) {
        // Map BIT and TINYINT to WR_BYTE
        case WriteEngine::BIT :
        case WriteEngine::TINYINT :
            internalType = WriteEngine::WR_BYTE; break;

        // Map SMALLINT to WR_SHORT
        case WriteEngine::SMALLINT :
            internalType = WriteEngine::WR_SHORT; break;

        // Map MEDINT, INT, and DATE to WR_INT
        case WriteEngine::MEDINT :
        case WriteEngine::INT :
        case WriteEngine::DATE :
            internalType = WriteEngine::WR_INT; break;

        // Map FLOAT to WR_FLOAT
        case WriteEngine::FLOAT :
            internalType = WriteEngine::WR_FLOAT; break;

        // Map BIGINT and DATETIME to WR_LONGLONG
        case WriteEngine::BIGINT :
        case WriteEngine::DATETIME :
            internalType = WriteEngine::WR_LONGLONG; break;

        // Map DOUBLE to WR_DOUBLE
        case WriteEngine::DOUBLE :
            internalType = WriteEngine::WR_DOUBLE; break;

        // Map BLOB to WR_BLOB
        case WriteEngine::BLOB :
            internalType = WriteEngine::WR_BLOB; break;

        // Map VARBINARY to WR_VARBINARY
        case WriteEngine::VARBINARY:
            internalType = WriteEngine::WR_VARBINARY; break;

        // Map CHAR, VARCHAR, and CLOB to WR_CHAR
        case WriteEngine::DECIMAL :
        case WriteEngine::CHAR :
        case WriteEngine::VARCHAR :
        case WriteEngine::CLOB :
        default:
            internalType = WriteEngine::WR_CHAR; break;
    }
}

/***********************************************************
 * DESCRIPTION:
 *    Convert interface column data type to internal data type
 * PARAMETERS:
 *    colStruct - column struct
 * RETURN:
 *    none
 ***********************************************************/
/* static */
void Convertor::convertColType(void* curStruct, const FuncType curType)
{
    ColDataType dataType     // This will be updated later,
        = WriteEngine::CHAR; // CHAR used only for initialization.
    ColType*    internalType = NULL;
    bool        bTokenFlag = false;
    int*        width = NULL;

    if (curType == FUNC_WRITE_ENGINE) {
        dataType = ((ColStruct*) curStruct)->colDataType;
        internalType = &((ColStruct*) curStruct)->colType;
        bTokenFlag = ((ColStruct*) curStruct)->tokenFlag;
        width = &((ColStruct*) curStruct)->colWidth;
    }
    else if (curType == FUNC_INDEX) {
        dataType = ((IdxStruct*) curStruct)->idxDataType;
        internalType = &((IdxStruct*) curStruct)->idxType;
        bTokenFlag = ((IdxStruct*) curStruct)->tokenFlag;
        width = &((IdxStruct*) curStruct)->idxWidth;
    }
    else
    {
        *width = 0;
    }

    switch(dataType) {
        // Map BIT and TINYINT to WR_BYTE
        case WriteEngine::BIT :
        case WriteEngine::TINYINT :
            *internalType = WriteEngine::WR_BYTE; break;

        // Map SMALLINT to WR_SHORT
        case WriteEngine::SMALLINT :
            *internalType = WriteEngine::WR_SHORT; break;

        // Map MEDINT, INT, and DATE to WR_INT
        case WriteEngine::MEDINT :
        case WriteEngine::INT :
        case WriteEngine::DATE :
            *internalType = WriteEngine::WR_INT; break;

        // Map FLOAT to WR_FLOAT
        case WriteEngine::FLOAT :
            *internalType = WriteEngine::WR_FLOAT; break;

        // Map BIGINT and DATETIME to WR_LONGLONG
        case WriteEngine::BIGINT :
        case WriteEngine::DATETIME :
            *internalType = WriteEngine::WR_LONGLONG; break;

        // Map DOUBLE to WR_DOUBLE
        case WriteEngine::DOUBLE :
            *internalType = WriteEngine::WR_DOUBLE; break;

        // Map DECIMAL to applicable integer type
        case WriteEngine::DECIMAL :
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
        case WriteEngine::BLOB :
            *internalType = WriteEngine::WR_BLOB; break;

        // Map VARBINARY to WR_VARBINARY
        case WriteEngine::VARBINARY:
            *internalType = WriteEngine::WR_VARBINARY; break;

        // Map CHAR, VARCHAR, and CLOB to WR_CHAR
        case WriteEngine::CHAR :
        case WriteEngine::VARCHAR :
        case WriteEngine::CLOB :
        default:
            *internalType = WriteEngine::WR_CHAR; break;
    }

    if (bTokenFlag)        // token overwrite any other types
        *internalType = WriteEngine::WR_TOKEN;

    // check whether width is in sync with the requirement
    if (curType == FUNC_WRITE_ENGINE)
        *width = getCorrectRowWidth(dataType, *width);

    // This is the patch for the decimal thing, override
//  if (dataType == WriteEngine::DECIMAL)
//  {
//      *internalType = *width <= 4 ?
//                      WriteEngine::WR_INT : WriteEngine::WR_LONGLONG;
//  }
}

/***********************************************************
 * DESCRIPTION:
 *    Get the correct width for a row
 * PARAMETERS:
 *    dataType - data type
 *    width - data width in byte
 * RETURN:
 *    emptyVal - the value of empty row
 ***********************************************************/
/* static */
int Convertor::getCorrectRowWidth(const ColDataType dataType, const int width)
{
    int offset, newWidth = 4;

    offset = (dataType == VARCHAR)? -1 : 0;
    switch(dataType) {
        case TINYINT : newWidth = 1; break;
        case SMALLINT: newWidth = 2; break;
        case MEDINT :
        case INT :
                       newWidth = 4; break;
        case BIGINT :  newWidth = 8; break;
        case FLOAT :   newWidth = 4; break;
        case DOUBLE :  newWidth = 8; break;
        case DECIMAL :
            if (width == 1)
                newWidth = 1;
            else if (width == 2)
                newWidth = 2;
            else if (width <= 4)
                newWidth = 4;
            else
                newWidth = 8;
            break;
        case DATE : newWidth = 4; break;
        case DATETIME : newWidth = 8; break;

        case CHAR :
        case VARCHAR :
        case VARBINARY : // treat same as varchar for now
        default:
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

} //end of namespace

