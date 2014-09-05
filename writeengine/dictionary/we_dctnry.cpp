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
* $Id: we_dctnry.cpp 4503 2013-02-01 18:53:48Z dcathey $
*
*******************************************************************************/
/** @we_dctnry.cpp
 *  When a signature is given, the value will be stored in dictionary and
 *  a token will be issued. Given a token, the signature in the dictionary
 *  can be deleted.
 *  The whole file contains only one class Dctnry
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <sstream>
#ifndef _MSC_VER
#include <inttypes.h>
#endif
#include <iostream>
using namespace std;

#include "bytestream.h"
#include "brmtypes.h"
#include "extentmap.h"    // for DICT_COL_WIDTH
#include "we_stats.h"
#include "we_log.h"
#define WRITEENGINEDCTNRY_DLLEXPORT
#include "we_dctnry.h"
#undef WRITEENGINEDCTNRY_DLLEXPORT
using namespace messageqcpp;
using namespace WriteEngine;
using namespace BRM;

namespace
{
    // These used to be member variables, hence the "m_" prefix.  But they are
    // all constants, so I removed them as member variables.  May change the
    // variable name later (to remove the m_ prefix) as time allows.
    const uint16_t m_endHeader = DCTNRY_END_HEADER; // end of header flag (0xffff)
    const uint16_t m_offSetZero= BYTE_PER_BLOCK;    // value for 0 offset (8192)
    const int m_lastOffSet= BYTE_PER_BLOCK;    // end of last offset
    const int m_totalHdrBytes =                // # bytes in header
          HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE + HDR_UNIT_SIZE;
    const int m_bigSpace  =                    // free space in an empty block
          BYTE_PER_BLOCK - (m_totalHdrBytes + HDR_UNIT_SIZE);

    const int START_HDR1  =                    // start loc of 2nd offset (HDR1)
          HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE;
    const int PSEUDO_COL_WIDTH = DICT_COL_WIDTH; // used to convert row count to block count

}

namespace WriteEngine
{
    // We will make this a constant for now.  If we ever decide to make
    // INITIAL_EXTENT_ROWS_TO_DISK configurable, we will need to move this
    // statement, and use Config class to get INITIAL_EXTENT_ROWS_TO_DISK.
    int NUM_BLOCKS_PER_INITIAL_EXTENT =
        ((INITIAL_EXTENT_ROWS_TO_DISK/BYTE_PER_BLOCK) *  PSEUDO_COL_WIDTH);

/*******************************************************************************
 * Description:
 * Dctnry constructor
 ******************************************************************************/
Dctnry::Dctnry() :
    m_nextPtr(NOT_USED_PTR),
    m_partition(0),
    m_segment(0),
    m_dbRoot(1),
    m_numBlocks(0),
    m_lastFbo(0),
    m_hwm(0),
    m_newStartOffset(0),
    m_freeSpace(0),
    m_curOp(0),
    m_colWidth(0),
    m_importDataMode(IMPORT_DATA_TEXT)
{
    memset( m_dctnryHeader, 0, sizeof(m_dctnryHeader));
    memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
    m_curBlock.lbid = INVALID_LBID;
    //add all initial header sizes for an empty block
    m_freeSpace   = BYTE_PER_BLOCK - m_totalHdrBytes ;

    memcpy(m_dctnryHeader2,                 &m_freeSpace, HDR_UNIT_SIZE);
    memcpy(m_dctnryHeader2+ HDR_UNIT_SIZE,  &m_nextPtr,NEXT_PTR_BYTES);
    memcpy(m_dctnryHeader2+ HDR_UNIT_SIZE+NEXT_PTR_BYTES,
                                            &m_offSetZero, HDR_UNIT_SIZE);
    memcpy(m_dctnryHeader2+ HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE,
                                            &m_endHeader,  HDR_UNIT_SIZE);
    m_curFbo  = INVALID_NUM;
    m_curLbid = INVALID_LBID;
    memset(m_sigArray, 0 , MAX_STRING_CACHE_SIZE*sizeof(Signature));
    m_arraySize =0;

    clear();//files
}

/*******************************************************************************
 * Description:
 * Dctnry destructor
 ******************************************************************************/
Dctnry::~Dctnry()
{
    //clear string cache here!
    freeStringCache( );
}

/*******************************************************************************
 * Description:
 * Free memory consumed by dictionary string cache
 ******************************************************************************/
void Dctnry::freeStringCache( )
{
    for (int i=0; i<m_arraySize; i++)
    {
        delete [] m_sigArray[i].signature;
        m_sigArray[i].signature = 0;
    }
    memset(m_sigArray, 0 , MAX_STRING_CACHE_SIZE*sizeof(Signature));
    m_arraySize = 0;
}

/*******************************************************************************
 * Description:
 * Create a dictionary file and initialize the header
 *
 * PARAMETERS:
 *    none
 *
 * RETURN:
 *    success    - successfully write the header to block
 *    failure    - it did not  write the header to block
 ******************************************************************************/
int  Dctnry::init()
{
    //cout <<"Init called! m_dctnryOID ="  << m_dctnryOID << endl;
    m_lastFbo =0;
    m_hwm = 0;
    m_newStartOffset =0;
    m_freeSpace = 0;
    m_curOp=0;
    memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
    m_curBlock.lbid = INVALID_LBID;
    memset(m_sigArray, 0 , MAX_STRING_CACHE_SIZE*sizeof(Signature));
    m_arraySize =0;

    return NO_ERROR;
}

/*******************************************************************************
 * Description:
 * Create a dictionary file and initialize the header, or can be used to
 * just add an extent to an already open dictionary store file.
 *
 * PARAMETERS:
 *    input
 *        dctnryOID - dictionary OID
 *        colWidth  - dictionary string width (not the token width)
 *        dbRoot    - DBRoot where file is located
 *        partition - partition number associated with the file
 *        segment   - segment number associated with the file
 *        startLbid - (out) starting LBID of the newly allocated extent
 *        flag      - "true" indicates we are adding the first block and the
 *                    file needs to be created with an abbreviated extent.
 *                    "false" indicates we just want to add an extent to
 *                    an existing file, and the file has already been opened.
 *
 * RETURN:
 *    success    - successfully created file and/or extent
 *    failure    - failed to create file and/or extent
 ******************************************************************************/
int  Dctnry::createDctnry( const OID& dctnryOID, int colWidth,
    const uint16_t dbRoot, const uint32_t partition, const uint16_t segment,
    LBID_t& startLbid, bool flag)
{
    int   allocSize = 0;
    char  fileName[FILE_NAME_SIZE];
    int   rc;
    std::map<FID,FID> oids;

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_ALLOC_DCT_EXTENT);
#endif
    if (flag)
    {
        m_dctnryOID   = dctnryOID;
        m_partition   = partition;
        m_segment     = segment;
        m_dbRoot      = dbRoot;
        RETURN_ON_ERROR( ( rc = oid2FileName( m_dctnryOID, fileName, true,
            m_dbRoot, m_partition, m_segment ) ) );
        m_segFileName = fileName;

        // if obsolete file exists, "w+b" will truncate and write over
        m_dFile = createDctnryFile(fileName, colWidth, "w+b", DEFAULT_BUFSIZ);
    }
    else
    {
        RETURN_ON_ERROR( setFileOffset(m_dFile, 0, SEEK_END) );
    }

    rc = BRMWrapper::getInstance()->allocateDictStoreExtent(
        (const OID)m_dctnryOID, m_dbRoot, m_partition, m_segment,
        startLbid, allocSize);
    if (rc != NO_ERROR)
    {
        if (flag)
        {
            closeDctnryFile(false, oids);
        }
        return rc;
    }

    // We allocate a full extent from BRM, but only write an abbreviated 256K
    // rows to disk for 1st extent in each store file, to conserve disk usage.
    int totalSize = allocSize;
    if (flag)
    {
        totalSize = NUM_BLOCKS_PER_INITIAL_EXTENT;
    }

    if ( !isDiskSpaceAvail(Config::getDBRootByNum(m_dbRoot), totalSize) )
    {
        if (flag)
        {
            closeDctnryFile(false, oids);
        }
        return ERR_FILE_DISK_SPACE;
    }

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_ALLOC_DCT_EXTENT);
#endif
    if( m_dFile != NULL ) {
        rc = FileOp::initDctnryExtent( m_dFile,
                                       m_dbRoot,
                                       totalSize,
                                       m_dctnryHeader2,
                                       m_totalHdrBytes,
                                       false );
        if (rc != NO_ERROR)
        {
            if (flag)
            {
                closeDctnryFile(false, oids);
            }
            return rc;
        }
    }
    else
        return ERR_FILE_CREATE;
    if (flag)
    {
        closeDctnryFile(true, oids);
        m_numBlocks = totalSize;
        m_hwm = 0;
        rc = BRMWrapper::getInstance()->setLocalHWM(
            m_dctnryOID, m_partition, m_segment, m_hwm);
    }
    else
    {
        m_numBlocks = m_numBlocks + totalSize;
    }

    return rc;
}

/*******************************************************************************
 * Description:
 * This function should be called to expand an abbreviated dictionary extent
 * into a full extent on disk.
 *
 * PARAMETERS:
 *    none
 *
 * RETURN:
 *    success    - successfully expanded extent
 *    failure    - failed to expand extent
 ******************************************************************************/
int  Dctnry::expandDctnryExtent()
{
    RETURN_ON_NULL( m_dFile, ERR_FILE_SEEK );

#ifdef _MSC_VER
    __int64 oldOffset = _ftelli64(m_dFile);
#else
    off_t oldOffset = ftello( m_dFile ); //save current offset
#endif
    RETURN_ON_ERROR( setFileOffset(m_dFile, 0, SEEK_END) );

    // Based on extent size, see how many blocks to add to fill the extent
    int blksToAdd = ( ((int)BRMWrapper::getInstance()->getExtentRows() -
        INITIAL_EXTENT_ROWS_TO_DISK)/BYTE_PER_BLOCK ) *  PSEUDO_COL_WIDTH;

    if ( !isDiskSpaceAvail(Config::getDBRootByNum(m_dbRoot), blksToAdd) )
    {
        return ERR_FILE_DISK_SPACE;
    }

    int rc = FileOp::initDctnryExtent( m_dFile,
                                   m_dbRoot,
                                   blksToAdd,
                                   m_dctnryHeader2,
                                   m_totalHdrBytes,
                                   true );
    if (rc != NO_ERROR)
        return rc;


    // Restore offset back to where we were before expanding the extent
    RETURN_ON_ERROR( setFileOffset(m_dFile, oldOffset, SEEK_SET) );

    // Update block count to reflect disk space added by expanding the extent.
    m_numBlocks = m_numBlocks + blksToAdd;

    return rc;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Close dictionary files
 *
 * PARAMETERS:
 *    none
 *
 * RETURN:
 *    none
 ******************************************************************************/
int Dctnry::closeDctnry()
{
    if ( !m_dFile )
        return NO_ERROR;

    int rc;
    CommBlock cb;
    cb.file.oid = m_dctnryOID;
    cb.file.pFile = m_dFile;
    std::map<FID,FID> oids;
    if (m_curBlock.state==BLK_WRITE)
    {
        rc = writeDBFile(cb, &m_curBlock, m_curBlock.lbid);
        if (rc != NO_ERROR)
        {
            closeDctnryFile(false, oids);
            return rc;
        }
        memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
        // m_curBlock.state== BLK_INIT;
    }

    // dmc-error handling (should detect/report error in closing file)
    closeDctnryFile(true, oids);

    m_hwm = (HWM)m_lastFbo;
    idbassert(m_dctnryOID>=0);
    rc = BRMWrapper::getInstance()->setLocalHWM(
        m_dctnryOID, m_partition, m_segment, m_hwm);
    if (rc != NO_ERROR)
        return rc;

    //cout <<"Init called! m_dctnryOID ="  << m_dctnryOID << endl;
    freeStringCache( );

    return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 *    Close dictionary file without flushing block buffer or updating
 *    BRM with HWM.
 *
 * PARAMETERS:
 *    none
 *
 * RETURN:
 *    none
 ******************************************************************************/
int Dctnry::closeDctnryOnly( )
{
    if ( !m_dFile )
        return NO_ERROR;

    // dmc-error handling (should detect/report error in closing file)
    std::map<FID,FID> oids;
    closeDctnryFile(false, oids);

    freeStringCache( );

    return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 *    drop/delete dictionary file
 *
 * PARAMETERS:
 *    dctnryOID -- file number to drop
 *
 * RETURN:
 *    none
 ******************************************************************************/
int  Dctnry::dropDctnry( const OID& dctnryOID)
{
    m_dctnryOID = dctnryOID;
    if (m_dFile)
    {
        RETURN_ON_ERROR( closeDctnry() );
    }
    return deleteFile( dctnryOID);
}

/*******************************************************************************
 * DESCRIPTION:
 *    open dictionary file
 *
 * PARAMETERS:
 *    dctnryOID-- for open dictionary file
 *    dbRoot   -- DBRoot for dictionary store segment file
 *    partition-- partition for dictionary store segment file
 *    segment  -- segment for dictionary store segment file
 *
 * RETURN:
 *    successful- NO_ERROR
 *    Fail      - Error Code
 ******************************************************************************/
int Dctnry::openDctnry(const OID& dctnryOID,
    const uint16_t dbRoot,
    const uint32_t partition,
    const uint16_t segment)
{
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_OPEN_DCT_FILE);
#endif
    int rc = NO_ERROR;
    m_dctnryOID = dctnryOID;
    m_dbRoot    = dbRoot;
    m_partition = partition;
    m_segment   = segment;

    m_dFile = openDctnryFile();
    if( m_dFile == NULL )
    {
        ostringstream oss;
        oss << "oid:partition:segment " <<
            dctnryOID <<":"<<partition<<":"<<segment;
        logging::Message::Args args;
        logging::Message message(1);
        args.add("Error opening dictionary file ");
        args.add(oss.str());
        args.add("");
        args.add("");
        message.format(args);
        logging::LoggingID lid(21);
        logging::MessageLog ml(lid);

        ml.logErrorMessage( message );
        return ERR_FILE_OPEN;
    }

    m_numBlocks = numOfBlocksInFile();
    std::map<FID,FID> oids;

    //Initialize other misc member variables
    init();

    rc=BRMWrapper::getInstance()->getLocalHWM(dctnryOID,
        m_partition, m_segment, m_hwm);
    if (rc!=NO_ERROR)
    {
        closeDctnryFile(false, oids);
        return rc;
    }
    m_lastFbo = (int)m_hwm;

    memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
    m_curFbo = m_lastFbo;
    rc = BRMWrapper::getInstance()->getBrmInfo( m_dctnryOID,
                                                m_partition, m_segment,
                                                m_curFbo,    m_curLbid);
    if (rc!=NO_ERROR)
    {
        closeDctnryFile(false, oids);
        return rc;
    }

    CommBlock cb;
    cb.file.oid = m_dctnryOID;
    cb.file.pFile = m_dFile;
#ifdef PROFILE
    // We omit the call to readDBFile from OPEN_DCT_FILE stats, because com-
    // pressed files have separate stats that readDBFile() will capture thru
    // ChunkManager::fetchChunkFromFile().
    Stats::stopParseEvent(WE_STATS_OPEN_DCT_FILE);
#endif
    rc=readDBFile(cb, m_curBlock.data, m_curLbid);
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_OPEN_DCT_FILE);
#endif
    if (rc!=NO_ERROR)
    {
        closeDctnryFile(false, oids);
        return rc;
    }

    // Position file to the start of the current block;
    // Determine file byte offset based on the current block offset (m_curFbo)
    long long byteOffset = ((long long)m_curFbo) * (long)BYTE_PER_BLOCK;
    rc = setFileOffset(m_dFile, byteOffset);
    if (rc!=NO_ERROR)
    {
        closeDctnryFile(false, oids);
        return rc;
    }

    m_curBlock.lbid = m_curLbid;
    m_curBlock.state= BLK_READ;
    int opCnt       = 0;
    // Get new free space (m_freeSpace) from header too! Here!!!!!!!!!!!!!!!
    getBlockOpCount( m_curBlock, opCnt);
    m_curOp = opCnt;

    // "If" this store file contains no more than 1 block, then we preload
    // the string cache used to recognize duplicates during row insertion.
    if (m_hwm == 0)
    {
        preLoadStringCache( m_curBlock );
    }
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_OPEN_DCT_FILE);
#endif

    return rc;
}

/*******************************************************************************
 * Description:
 * Determine if the specified signature is present in the string cache.
 *
 * PARAMETERS:
 *    input
 *       sig - signature to search for
 *
 * RETURN:
 *    true  - if signature if found
 *    false - if signature is not found
 ******************************************************************************/
bool Dctnry::getTokenFromArray(Signature& sig)
{
    for (int i=0; i<(int)m_arraySize ; i++ )
    {
        if (sig.size == m_sigArray[i].size)
        {
            if (!memcmp(sig.signature, m_sigArray[i].signature, sig.size))
            {
                sig.token = m_sigArray[i].token;
                return true;
            }//endif sig compare
        }//endif size compare
    }

    return false;
}

/*******************************************************************************
 * Description:
 * Used by bulk import to insert a signature into m_curBlock, and update
 * the m_curBlock header accordingly.
 *
 * PARAMETERS:
 *    input
 *       sig   - signature to be inserted
 *    output
 *       token - token that was assigned to the inserted signature
 *
 * RETURN:
 *    none
 ******************************************************************************/
void Dctnry::insertDctnry2(Signature& sig)
{
    insertDctnryHdr(m_curBlock.data,
                    sig.size);
    insertSgnture(m_curBlock.data, sig.size, (unsigned char*)sig.signature);

    sig.token.fbo = m_curLbid;
    sig.token.op  = m_curOp;
    sig.token.spare = 0U;
}

/*******************************************************************************
 * Description:
 * Used by bulk import to insert collection of strings into this store file.
 * Function assumes that the file is already positioned to the current block.
 *
 * PARAMETERS:
 *    input
 *       buf - character buffer containing input strings
 *       pos - meta data describing data in "buf"
 *       totalRow - number of rows in "buf"
 *       col - column of strings to be parsed from "buf"
 *    output
 *       tokenBuf  - tokens assigned to inserted strings
 *
 * RETURN:
 *    success    - successfully write the header to block
 *    failure    - it did not  write the header to block
 ******************************************************************************/
int Dctnry::insertDctnry(const char* buf,
                         ColPosPair ** pos,
                         const int totalRow, const int col,
                         char* tokenBuf,
                         long long& truncCount)
{
#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_PARSE_DCT);
#endif
    int startPos     = 0;
    int totalUseSize = 0;

    int outOffset    = 0;
    const char* pIn;
    char* pOut       = tokenBuf;
    Signature curSig;
    bool found       = false;
    bool next        = false;
    CommBlock cb;
    cb.file.oid      = m_dctnryOID;
    cb.file.pFile    = m_dFile;
    WriteEngine::Token nullToken;

    //...Loop through all the rows for the specified column
    while(startPos < totalRow)
    {
        found = false;
        memset(&curSig, 0, sizeof(curSig));
        curSig.size = pos[startPos][col].offset;

        // Strip trailing null bytes '\0' (by adjusting curSig.size) if import-
        // ing in binary mode.  If entire string is binary zeros, then we treat
        // as a NULL value.
        if (m_importDataMode != IMPORT_DATA_TEXT)
        {
            if ((curSig.size > 0) &&
                (curSig.size != COLPOSPAIR_NULL_TOKEN_OFFSET))
            {
                char* fld = (char*)buf + pos[startPos][col].start;
                int kk = curSig.size-1;
                for (; kk>=0; kk--)
                {
                    if (fld[kk] != '\0')
                        break;
                }
                curSig.size = kk + 1;
            }
        }

        // Read thread should validate against max size so that the entire row
        // can be rejected up front.  Once we get here in the parsing thread,
        // it is too late to reject the row.  However, as a precaution, we
        // still check against max size & set to null token if needed.
        if ((curSig.size == 0) ||
            (curSig.size == COLPOSPAIR_NULL_TOKEN_OFFSET) ||
            (curSig.size >  MAX_SIGNATURE_SIZE))
        {
            if (m_defVal.length() > 0) // use default string if available
            {
                pIn = m_defVal.c_str();
                curSig.signature = (unsigned char*)pIn;
                curSig.size      = m_defVal.length();
            }
            else
            {
                memcpy( pOut + outOffset, &nullToken, 8 );
                outOffset += 8;
                startPos++;
                continue;
            }
        }
        else
        {
            pIn = (char*)buf + pos[startPos][col].start;
            curSig.signature =(unsigned char*)pIn;
        }

        // @Bug 2565: Truncate any strings longer than schema's column width
        if (curSig.size > m_colWidth)
        {
            curSig.size = m_colWidth;
            ++truncCount;
        }

        //...Search for the string in our string cache
        if (m_arraySize < MAX_STRING_CACHE_SIZE)
        {
            //Stats::startParseEvent("getTokenFromArray");
            found = getTokenFromArray(curSig);
            if(found)
            {
                memcpy( pOut + outOffset, &curSig.token, 8 );
                outOffset += 8;
                startPos++;
                //Stats::stopParseEvent("getTokenFromArray");
                continue;
            }
            //Stats::stopParseEvent("getTokenFromArray");
        }
        totalUseSize = HDR_UNIT_SIZE + curSig.size;

        //...String not found in cache, so proceed.
        //   If room is available in current block then insert into block.
        // @bug 3960: Add MAX_OP_COUNT check to handle case after bulk rollback
        if( (totalUseSize <= m_freeSpace) &&
            (m_curOp      < (MAX_OP_COUNT-1)) ) {
            insertDctnry2(curSig); //m_freeSpace updated!
            m_curBlock.state = BLK_WRITE;
            memcpy( pOut + outOffset, &curSig.token, 8 );
            outOffset += 8;
            startPos++;
            found = true;

            //...If we have reached limit for the number of strings allowed in
            //   a block, then we write the current block so that we can start
            //   another block.
            if (m_curOp>= MAX_OP_COUNT -1)
            {
#ifdef PROFILE
                Stats::stopParseEvent(WE_STATS_PARSE_DCT);
#endif
                RETURN_ON_ERROR(writeDBFileNoVBCache(cb,&m_curBlock,m_curFbo));
                m_curBlock.state = BLK_READ;
                next = true;
            }

            //...Add string to cache, if we have not exceeded cache limit
            if (m_arraySize < MAX_STRING_CACHE_SIZE)
            {
                addToStringCache( curSig );
            }
        }
        else //...No room for this string in current block, so we write
             //   out the current block, so we can start another block
        {
#ifdef PROFILE
            Stats::stopParseEvent(WE_STATS_PARSE_DCT);
#endif
            RETURN_ON_ERROR( writeDBFileNoVBCache(cb, &m_curBlock, m_curFbo) );
            m_curBlock.state = BLK_READ;
            next = true;
            found = false;
        }//if m_freeSpace

        //..."next" flag is used to indicate that we need to advance to the
        //   next block in the store file.
        if (next)
        {
            memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
            memcpy( m_curBlock.data, &m_dctnryHeader2, m_totalHdrBytes);
            m_freeSpace = BYTE_PER_BLOCK - m_totalHdrBytes;
            m_curBlock.state = BLK_WRITE;
            m_curOp =0;
            next = false;
            m_lastFbo++;
            m_curFbo = m_lastFbo;

            //...Expand current extent if it is an abbreviated initial extent
            if ((m_curFbo    == m_numBlocks) &&
                (m_numBlocks == NUM_BLOCKS_PER_INITIAL_EXTENT))
            {
                RETURN_ON_ERROR( expandDctnryExtent() );
            }

            //...Allocate a new extent if we have reached the last block in the
            //   current extent.
            if (m_curFbo == m_numBlocks)
            {//last block
                LBID_t startLbid;

                // Add an extent.
                RETURN_ON_ERROR( createDctnry(m_dctnryOID,
                                 m_colWidth,
                                 m_dbRoot,
                                 m_partition,
                                 m_segment,
                                 startLbid,
                                 false) );

                if (m_logger)
                {
                    std::ostringstream oss;
                    oss << "Add dictionary extent OID-" << m_dctnryOID <<
                           "; DBRoot-" << m_dbRoot    <<
                           "; part-"   << m_partition <<
                           "; seg-"    << m_segment   <<
                           "; hwm-"    << m_curFbo    <<
                           "; LBID-"   << startLbid   <<
                           "; file-"   << m_segFileName;
                    m_logger->logMsg( oss.str(), MSGLVL_INFO2 );
                }
                m_curLbid = startLbid;
#ifdef PROFILE
                Stats::startParseEvent(WE_STATS_PARSE_DCT_SEEK_EXTENT_BLK);
#endif
                // now seek back to the curFbo, after adding an extent
                long long byteOffset = m_curFbo;
                byteOffset *= BYTE_PER_BLOCK;
                RETURN_ON_ERROR( setFileOffset(m_dFile, byteOffset) );
#ifdef PROFILE
                Stats::stopParseEvent(WE_STATS_PARSE_DCT_SEEK_EXTENT_BLK);
#endif
            }
            else
            {
                // LBIDs are numbered collectively and consecutively within an
                // extent, so within an extent we can derive the LBID by simply
                // incrementing it rather than having to go back to BRM to look
                // up the LBID for each FBO.
                m_curLbid++;
            }
#ifdef PROFILE
            Stats::startParseEvent(WE_STATS_PARSE_DCT);
#endif
            m_curBlock.lbid = m_curLbid;

            //..."found" flag indicates whether the string was already found
            //   "or" added to the end of the previous block.  If false, then
            //   we need to add the string to the new block.
            if (!found)
            {
                insertDctnry2(curSig); //m_freeSpace updated!
                m_curBlock.state = BLK_WRITE;
                memcpy( pOut + outOffset, &curSig.token, 8 );
                outOffset += 8;
                startPos++;

                //...Add string to cache, if we have not exceeded cache limit
                if (m_arraySize < MAX_STRING_CACHE_SIZE)
                {
                    addToStringCache( curSig );
                }
            }
        }//if next
    }//end while
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_PARSE_DCT);
#endif
    //Done
    // If any data leftover and not written by subsequent call to
    // insertDctnry(), then it will be written by closeDctnry().

    return NO_ERROR;
}

/*******************************************************************************
 * DESCRIPTION:
 * Used by DML to insert a single string into this store file.
 * (1) Insert a signature value into the block
 * (2) The header information inserted at front
 * (3) The signature inserted from back
 * (4) Total minimum header size-- free space 2bytes, next pointer 8 bytes
 *     zero offset 2 bytes, end of header 2 bytes, total 14 bytes
 *     plus 2 bytes for new values' starting offset value storage
 *     total 14 bytes
 * (5) Values size <=8176 =(8192-16) will not be split into two blocks
 * (6) For smaller value <=8176, it has to fit into one block or
 *     unsuccessfully to insert
 * (7) For large value > 8176,
 *      smaller space first then take up a whole block
 *      or a whole block first then some left over space in another
 *     block
 * (8) limit to 8000 byte for this release size
 *
 * PARAMETERS:
 *    input dFile
 *        -- File handle
 *    Input  sgnature_size
 *        -- how many bytes the signature occupies
 *    Input  sgnature_value
 *        -- the value of the signature
 *    output token
 *        -- token structure carrying the assigned fbo and op
 *
 * RETURN:
 *    success    - successfully insert the signature
 *    failure    - it did not   insert the signature
 ******************************************************************************/
int Dctnry::insertDctnry(const int& sgnature_size,
                         const unsigned char* sgnature_value,
                         Token& token)
{
    int rc = 0;
    int i;
    bool found = false;
    unsigned char* value = NULL;
    int size;
    if (sgnature_size > MAX_SIGNATURE_SIZE)
    {
        return ERR_DICT_SIZE_GT_8000;
    }
    if (sgnature_size == 0)
    {
        WriteEngine::Token nullToken;
        memcpy( &token, &nullToken, 8 );
        return NO_ERROR;
    }

    CommBlock cb;
    cb.file.oid = m_dctnryOID;
    cb.file.pFile = m_dFile;

    size = sgnature_size;
    value = (unsigned char*)sgnature_value;

    for (i = m_lastFbo; i < m_numBlocks; i++)
    {
        // @bug 3960: Add MAX_OP_COUNT check to handle case after bulk rollback
        if( (m_freeSpace>= (size + HDR_UNIT_SIZE)) &&
            (m_curOp    <  (MAX_OP_COUNT-1)) )
        { // found the perfect block; signature size fit in this block
            found = true;
            insertDctnryHdr(m_curBlock.data, size);
            insertSgnture(m_curBlock.data, size, value);
            m_curBlock.state = BLK_WRITE;

            token.fbo = m_curLbid;
            token.op  = m_curOp;
            token.spare = 0;
            m_lastFbo = i;
            m_curFbo = m_lastFbo;
            if (m_curOp < (MAX_OP_COUNT-1))
                return NO_ERROR;
        }//end Found

        //@bug 3832. check error code
        RETURN_ON_ERROR( writeDBFile(cb, &m_curBlock, m_curLbid) );
        memset( m_curBlock.data, 0, sizeof(m_curBlock.data));
        memcpy( m_curBlock.data, &m_dctnryHeader2, m_totalHdrBytes);
        m_freeSpace = BYTE_PER_BLOCK - m_totalHdrBytes;
        m_curBlock.state = BLK_WRITE;
        m_curOp =0;
        m_lastFbo++;
        m_curFbo = m_lastFbo;

        //...Expand current extent if it is an abbreviated initial extent
        if ((m_curFbo    == m_numBlocks) &&
            (m_numBlocks == NUM_BLOCKS_PER_INITIAL_EXTENT))
        {
            RETURN_ON_ERROR( expandDctnryExtent() );
        }

        //...Allocate a new extent if we have reached the last block in the
        //   current extent.
        if (m_curFbo == m_numBlocks)
        {//last block
            //for roll back the extent to use
            //Save those empty extents in case of failure to rollback
            std::vector<ExtentInfo> dictExtentInfo;
            ExtentInfo info;
            info.oid = m_dctnryOID;
            info.partitionNum = m_partition;
            info.segmentNum = m_segment;
            info.dbRoot = m_dbRoot;
            info.hwm = m_hwm;
            info.newFile = false;
            dictExtentInfo.push_back (info);
            LBID_t startLbid;
            // Add an extent.
            rc =  createDctnry(m_dctnryOID,
                               0,             // dummy column width
                               m_dbRoot,
                               m_partition,
                               m_segment,
                               startLbid,
                               false) ;
            if ( rc != NO_ERROR )
            {
                //roll back the extent             
                BRMWrapper::getInstance()->deleteEmptyDictStoreExtents(
                    dictExtentInfo);
                return rc;
            }
        }
        RETURN_ON_ERROR( BRMWrapper::getInstance()->getBrmInfo(m_dctnryOID,
                                                    m_partition, m_segment,
                                                    m_curFbo,    m_curLbid) );
        m_curBlock.lbid = m_curLbid;
    }//end for loop for all of the blocks

    return ERR_DICT_NO_SPACE_INSERT;
}

/*******************************************************************************
 * Description
 * Update the block header (and data members like m_freeSpace,
 * m_newStartOffset, etc), to reflect the insertion of string of size "size"
 *
 * PARAMETERS:
 *    input
 *        blockBuf
 *        --the block buffer
 *    input
 *        size
 *        --Size of the signature value
 *
 * RETURN:
 *    none
 ******************************************************************************/
void Dctnry::insertDctnryHdr( unsigned char* blockBuf,
                              const int& size)
{
    int endHdrLoc      = START_HDR1 + (m_curOp+1)*HDR_UNIT_SIZE;
    int nextOffsetLoc  = START_HDR1 +  m_curOp*HDR_UNIT_SIZE;
    int lastOffsetLoc  = START_HDR1 + (m_curOp-1)*HDR_UNIT_SIZE ;

    m_freeSpace -= (size + HDR_UNIT_SIZE);
    memcpy(&blockBuf[endHdrLoc],&m_endHeader,HDR_UNIT_SIZE);
    uint16_t lastOffset =*(uint16_t*)&blockBuf[lastOffsetLoc];
    uint16_t nextOffset = lastOffset- size;

    memcpy(&blockBuf[0], &m_freeSpace, HDR_UNIT_SIZE);
    memcpy(&blockBuf[nextOffsetLoc], &nextOffset, HDR_UNIT_SIZE);
    m_newStartOffset = nextOffset;
    m_curOp++;
}

/*******************************************************************************
 * DESCRIPTION:
 * Insert the specified string into the block buffer.
 *
 * PARAMETERS:
 *    Input blockBuf
 *        --block buffer
 *    Input size
 *        -- size of the signature value
 *    Input value
 *        -- value of the signature
 *
 * RETURN:
 *    none
 ******************************************************************************/
void Dctnry::insertSgnture(unsigned char* blockBuf,
                           const int& size, unsigned char*value)
{
    //m_newStartLoc is calculated from the header insertion code
    memcpy(&blockBuf[m_newStartOffset], value, size);
}

/*******************************************************************************
 * Description:
 * get the op count for a block
 * input
 *      DataBlock& fileBlock -- the file block
 * output
 *      op_count - total op count
 ******************************************************************************/
void  Dctnry::getBlockOpCount( const DataBlock &fileBlock, int & op_count)
{
    ByteStream bs;
    ByteStream::byte inbuf[BYTE_PER_BLOCK];
    memcpy(inbuf, fileBlock.data , BYTE_PER_BLOCK);
    bs.load(inbuf, BYTE_PER_BLOCK);

    ByteStream::doublebyte offset;
    ByteStream::doublebyte dbyte;
    bs >> m_freeSpace;
    bs >> dbyte;
    bs >> dbyte;
    bs >> dbyte;
    bs >> dbyte;
    bs >> dbyte;
    idbassert(dbyte == BYTE_PER_BLOCK);
    bs >> offset;

    while (offset < 0xffff)
    {
        op_count++;
        bs >> offset;
    }
}

/*******************************************************************************
 * Description:
 * Loads the string cache from the specified DataBlock, which should be
 * the first block in the applicable dictionary store file.
 * input
 *      DataBlock& fileBlock -- the file block
 ******************************************************************************/
void  Dctnry::preLoadStringCache( const DataBlock& fileBlock )
{
    int hdrOffsetBeg = HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE;
    int hdrOffsetEnd = HDR_UNIT_SIZE + NEXT_PTR_BYTES;
    uint16_t offBeg = 0;
    uint16_t offEnd = 0;
    memcpy( &offBeg, &fileBlock.data[hdrOffsetBeg], HDR_UNIT_SIZE );
    memcpy( &offEnd, &fileBlock.data[hdrOffsetEnd], HDR_UNIT_SIZE );

    int op = 1; // ordinal position of the string within the block
    Signature aSig;
    memset( &aSig, 0, sizeof(Signature));

    while ((offBeg != DCTNRY_END_HEADER) &&
           (op     <= MAX_STRING_CACHE_SIZE))
    {
        unsigned int len = offEnd - offBeg;
        aSig.size        = len;
        aSig.signature   = new unsigned char[len];
        memcpy(aSig.signature, &fileBlock.data[offBeg], len);
        aSig.token.op    = op;
        aSig.token.fbo   = m_curLbid;
        m_sigArray[op-1] = aSig;

        offEnd           = offBeg;
        hdrOffsetBeg    += HDR_UNIT_SIZE;
        memcpy( &offBeg, &fileBlock.data[hdrOffsetBeg], HDR_UNIT_SIZE );
        op++;
    }
    m_arraySize = op - 1;

    //std::cout << "Preloading strings..." << std::endl;
    //char strSig[1000];
    //uint64_t tokenVal;
    //for (int i=0; i<m_arraySize; i++)
    //{
    //  memcpy(strSig, m_sigArray[i].signature, m_sigArray[i].size );
    //  memcpy(&tokenVal, &m_sigArray[i].token, sizeof(uint64_t));
    //  strSig[m_sigArray[i].size] = '\0';
    //  std::cout << "op-"      << m_sigArray[i].token.op  <<
    //               "; fbo-"   << m_sigArray[i].token.fbo <<
    //               "; sig-"   << strSig   <<
    //               "; token-" << tokenVal << std::endl;
    //}
}

/*******************************************************************************
 * Description:
 * Add the specified signature (string) to the string cache.
 * input
 *      newSig -- Signature string to be added to the string cache.
 ******************************************************************************/
void  Dctnry::addToStringCache( const Signature& newSig )
{
    Signature asig;
    memset(&asig, 0, sizeof(Signature));
    asig.signature = new unsigned char[newSig.size];
    memcpy(asig.signature, newSig.signature, newSig.size );
    asig.size      = newSig.size;
    asig.token     = newSig.token;
    m_sigArray[m_arraySize]=asig;
    m_arraySize++;
}

/*******************************************************************************
 * Description:
 * get the location of the end of header
 * input
 *      dFile - file handle
 *      lbid  - block of interest
 * output
 *      endOp - ordinal position of the end of header for "lbid"
 *
 * return value
 *        Success -- found and deleted
 *        Fail    -- ERR_DICT_INVALID_DELETE
 ******************************************************************************/
int  Dctnry::getEndOp(FILE* dFile, int lbid, int &endOp)
{
    DataBlock fileBlock;
    Offset newOffset;
    int rc;
    CommBlock cb;
    cb.file.oid = m_dctnryOID;
    cb.file.pFile = dFile;
    memset( fileBlock.data, 0, sizeof(fileBlock.data));
    m_dFile = dFile;
    rc=readSubBlockEntry( cb, &fileBlock, lbid, 0, 0,
                          HDR_UNIT_SIZE  +
                          NEXT_PTR_BYTES +
                          HDR_UNIT_SIZE  +
                          HDR_UNIT_SIZE,
                          &m_dctnryHeader);

    memcpy(&m_freeSpace, &fileBlock.data[0],HDR_UNIT_SIZE);
    memcpy(&m_nextPtr, &fileBlock.data[HDR_UNIT_SIZE],NEXT_PTR_BYTES);

    newOffset.hdrLoc = HDR_UNIT_SIZE + NEXT_PTR_BYTES + HDR_UNIT_SIZE ;
    memcpy(&newOffset.offset,&fileBlock.data[newOffset.hdrLoc],HDR_UNIT_SIZE);
    endOp = 1; //should be zero counting the end of header then
    while ( newOffset.offset !=DCTNRY_END_HEADER)
    {
        newOffset.hdrLoc += HDR_UNIT_SIZE;
        memcpy(&newOffset.offset,&fileBlock.data[newOffset.hdrLoc],
               HDR_UNIT_SIZE);
        endOp++;
    }
    return rc;
}

/*******************************************************************************
 * Add a signature value to the dictionary store.
 * Function first checks to see if the signature is already
 * in our string cache, and returns the corresponding token
 * if it is found in the cache.
 ******************************************************************************/
int  Dctnry::updateDctnry(unsigned char* sigValue, int& sigSize,
                          Token& token)
{
    int rc = NO_ERROR;
    Signature sig;
    sig.signature = sigValue;
    sig.size = sigSize;

    // Look for string in cache
    if (m_arraySize < MAX_STRING_CACHE_SIZE)
    {
        bool found = false;
        found = getTokenFromArray(sig);
        if (found)
        {
            token = sig.token;
            return NO_ERROR;
        }
    }

    //Insert into Dictionary
    rc = insertDctnry(sigSize, sigValue, token);

    //Add the new signature and token into cache
    if (m_arraySize < MAX_STRING_CACHE_SIZE)
    {
        Signature sig;
        sig.size = sigSize;
        sig.signature = new unsigned char[sigSize];
        memcpy (sig.signature, sigValue, sigSize);
        sig.token = token;
        m_sigArray[m_arraySize]=sig;
        m_arraySize++;
    }

    return rc;
}

/*******************************************************************************
 * open dictionary file
 ******************************************************************************/
FILE* Dctnry::createDctnryFile(
    const char *name, int, const char *mode, int ioBuffSize)
{
    return openFile(name, mode, ioBuffSize);
}

/*******************************************************************************
 * open dictionary file
 ******************************************************************************/
FILE* Dctnry::openDctnryFile()
{
    return openFile(
        m_dctnryOID, m_dbRoot, m_partition, m_segment, m_segFileName);
}

/*******************************************************************************
 * close dictionary file
 ******************************************************************************/
void Dctnry::closeDctnryFile(bool doFlush, std::map<FID,FID> & oids)
{
    closeFile(m_dFile);
    m_dFile = NULL;
}

int Dctnry::numOfBlocksInFile()
{
    long long fileSizeBytes = 0;
    getFileSize2(m_dFile,fileSizeBytes); //dmc-error handling (ignoring rc)
    return fileSizeBytes/BYTE_PER_BLOCK;
}

void Dctnry::copyDctnryHeader(void* buf)
{
    memcpy(buf, m_dctnryHeader2, m_totalHdrBytes);
}

} //end of namespace
