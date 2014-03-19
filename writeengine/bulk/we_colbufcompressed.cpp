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

/*****************************************************************************
 * $Id: we_colbufcompressed.cpp 4737 2013-08-14 20:45:46Z bwilkinson $
 *
 ****************************************************************************/

/** @file
 * Implementation of the ColumnBufferCompressed class
 *
 */

#include "we_colbufcompressed.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <sstream>

#include <boost/scoped_array.hpp>

#include "we_define.h"
#include "we_config.h"
#include "we_convertor.h"
#include "we_columninfo.h"
#include "we_fileop.h"
#include "we_log.h"
#include "we_stats.h"
#include "IDBDataFile.h"
using namespace idbdatafile;

#include "idbcompress.h"
using namespace compress;

namespace WriteEngine {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ColumnBufferCompressed::ColumnBufferCompressed( ColumnInfo* pColInfo,
    Log* logger) :
        ColumnBuffer(pColInfo, logger),
        fToBeCompressedBuffer(0),
        fToBeCompressedCapacity(0),
        fNumBytes(0),
        fCompressor(0),
        fPreLoadHWMChunk(true),
        fFlushedStartHwmChunk(false)
{
    fUserPaddingBytes = Config::getNumCompressedPadBlks() * BYTE_PER_BLOCK;
    fCompressor = new compress::IDBCompressInterface( fUserPaddingBytes );
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ColumnBufferCompressed::~ColumnBufferCompressed()
{
    if (fToBeCompressedBuffer)
        delete []fToBeCompressedBuffer;
    fToBeCompressedBuffer   = 0;
    fToBeCompressedCapacity = 0;
    fNumBytes               = 0;
    delete fCompressor;
}

//------------------------------------------------------------------------------
// Reset "this" ColumnBufferCompressed object to read a different file, by
// resetting the FILE*, starting HWM, and the chunk pointers.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::setDbFile(IDBDataFile* f, HWM startHwm, const char* hdrs)
{
    fFile        = f;
    fStartingHwm = startHwm;

    IDBCompressInterface compressor;
    if (compressor.getPtrList(hdrs, fChunkPtrs) != 0)
    {
        return ERR_COMP_PARSE_HDRS;
    }

    // If we have any orphaned chunk pointers (ex: left over after a DML
    // rollback), that fall after the HWM, then drop those trailing ptrs.
    unsigned int chunkIndex             = 0;
    unsigned int blockOffsetWithinChunk = 0;
    fCompressor->locateBlock(fStartingHwm,chunkIndex,blockOffsetWithinChunk);
    if ((chunkIndex+1) < fChunkPtrs.size())
    {
        fChunkPtrs.resize(chunkIndex+1);
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Reinitialize to-be-compressed column buffer (to empty chunk) prior to
// importing the first chunk of the next extent.  Returns startFileOffset
// which indicates file offset (in bytes) where next extent will be starting.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::resetToBeCompressedColBuf(
    long long& startFileOffset )
{
    // Don't load chunk, once we go to next extent
    fPreLoadHWMChunk = false;

    // Lazy creation of to-be-compressed buffer
    if (!fToBeCompressedBuffer)
    {
        fToBeCompressedBuffer =
            new unsigned char[IDBCompressInterface::UNCOMPRESSED_INBUF_LEN];
    }
    BlockOp::setEmptyBuf( fToBeCompressedBuffer,
                          IDBCompressInterface::UNCOMPRESSED_INBUF_LEN,
                          fColInfo->column.emptyVal,
                          fColInfo->column.width );

    if (fLog->isDebug( DEBUG_2 )) {
        std::ostringstream oss;
        oss << "Initializing empty chunk for next extent: OID-" <<
            fColInfo->curCol.dataFile.fid <<
            "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
            "; part-"   << fColInfo->curCol.dataFile.fPartition <<
            "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
            "; hwm-"    << fStartingHwm;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    fToBeCompressedCapacity = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;

    // Set file offset past end of last chunk
    startFileOffset = IDBCompressInterface::HDR_BUF_LEN*2;
    if (fChunkPtrs.size() > 0)
        startFileOffset = fChunkPtrs[ fChunkPtrs.size()-1 ].first +
                          fChunkPtrs[ fChunkPtrs.size()-1 ].second;

    // Positition ourselves to start of empty to-be-compressed buffer
    fNumBytes       = 0;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Intercept data being copied from the raw-data output buffer to the output
// file, and instead buffer up the data to be compressed in 4M chunks before
// writing it out.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::writeToFile(int startOffset, int writeSize)
{
    if (writeSize == 0) // skip unnecessary write, if 0 bytes given
        return NO_ERROR;

    // If we are starting a new file, we need to reinit the buffer and
    // find out what our file offset should be set to.
    if (!fToBeCompressedCapacity)
    {
#ifdef PROFILE
        Stats::startParseEvent(WE_STATS_COMPRESS_COL_INIT_BUF);
#endif
        long long startFileOffset;
        int rc = initToBeCompressedBuffer( startFileOffset );
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFile: error initializing to-be-compressed buffer "
                "for OID " << fColInfo->curCol.dataFile.fid <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return rc;
        }

        rc = fColInfo->colOp->setFileOffset(fFile, startFileOffset, SEEK_SET);
        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "writeToFile: error init compressed file offset for " <<
                "OID " << fColInfo->curCol.dataFile.fid <<
                "; " << startFileOffset <<
                "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return rc;
        }
#ifdef PROFILE
        Stats::stopParseEvent(WE_STATS_COMPRESS_COL_INIT_BUF);
#endif
    }

    unsigned char* bufOffset = fToBeCompressedBuffer + fNumBytes;

    // Expand the compression buffer size if working with an abbrev extent, and
    // the bytes we are about to add will overflow the abbreviated extent.
    if((fToBeCompressedCapacity<IDBCompressInterface::UNCOMPRESSED_INBUF_LEN) &&
       ((fNumBytes + writeSize) > fToBeCompressedCapacity) )
    {
        std::ostringstream oss;
        oss << "Expanding abbrev to-be-compressed buffer for: OID-" <<
            fColInfo->curCol.dataFile.fid <<
            "; DBRoot-"   << fColInfo->curCol.dataFile.fDbRoot    <<
            "; part-"     << fColInfo->curCol.dataFile.fPartition <<
            "; seg-"      << fColInfo->curCol.dataFile.fSegment;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        fToBeCompressedCapacity = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
    }

    if ((fNumBytes + writeSize) <= fToBeCompressedCapacity)
    {
        if (fLog->isDebug( DEBUG_2 )) {
            std::ostringstream oss;
            oss << "Buffering data to-be-compressed for: OID-" <<
                fColInfo->curCol.dataFile.fid <<
                "; DBRoot-"   << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"     << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"      << fColInfo->curCol.dataFile.fSegment   <<
                "; addBytes-" << writeSize <<
                "; totBytes-" << (fNumBytes+writeSize);
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        memcpy(bufOffset, (fBuffer + startOffset), writeSize);
        fNumBytes += writeSize;
    }
    else // Not enough room to add all the data to the to-be-compressed buffer
    {
        int startOffsetX = startOffset;
        int writeSizeX   = writeSize;

        // The number of bytes (in fBuffer) to be written, could be larger than
        // our to-be-compressed buffer, so we require a loop to potentially
        // iterate thru all the bytes to be compresssed and written from fBuffer
        while (writeSizeX > 0)
        {
            idbassert( (fNumBytes <= fToBeCompressedCapacity) ); // DMC-temp debug

            size_t writeSizeOut = 0;
            if ((fNumBytes + writeSizeX) > fToBeCompressedCapacity)
            {
                writeSizeOut = fToBeCompressedCapacity - fNumBytes;

                if (fLog->isDebug( DEBUG_2 )) {
                    std::ostringstream oss;
                    oss << "Buffering data (full) to-be-compressed for: OID-" <<
                        fColInfo->curCol.dataFile.fid <<
                        "; DBRoot-"   << fColInfo->curCol.dataFile.fDbRoot    <<
                        "; part-"     << fColInfo->curCol.dataFile.fPartition <<
                        "; seg-"      << fColInfo->curCol.dataFile.fSegment   <<
                        "; addBytes-" << writeSizeOut                         <<
                        "; totBytes-" << (fNumBytes + writeSizeOut);
                    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
                }

                if (writeSizeOut > 0)
                {
                    memcpy(bufOffset, (fBuffer + startOffsetX), writeSizeOut);
                    fNumBytes += writeSizeOut;
                }

                //char resp;
                //std::cout << "dbg: before writeToFile->compressAndFlush" <<
                //    std::endl;
                //std::cin  >> resp;
                int rc = compressAndFlush( false );
                //std::cout << "dbg: after writeToFile->compressAndFlush" <<
                //    std::endl;
                //std::cin  >> resp;
                if (rc != NO_ERROR)
                {
                    WErrorCodes ec;
                    std::ostringstream oss;
                    oss << "writeToFile: error compressing and writing chunk "
                        "for OID " << fColInfo->curCol.dataFile.fid <<
                    "; "   << ec.errorString(rc);
                    fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

                    return rc;
                }

                // Start over again loading a new to-be-compressed buffer
                BlockOp::setEmptyBuf( fToBeCompressedBuffer,
                    IDBCompressInterface::UNCOMPRESSED_INBUF_LEN,
                    fColInfo->column.emptyVal,
                    fColInfo->column.width );

                fToBeCompressedCapacity =
                    IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
                bufOffset = fToBeCompressedBuffer;

                fNumBytes = 0;
            }
            else
            {
                writeSizeOut = writeSizeX;

                if (fLog->isDebug( DEBUG_2 )) {
                    std::ostringstream oss;
                    oss << "Buffering data (new) to-be-compressed for: OID-"  <<
                        fColInfo->curCol.dataFile.fid <<
                        "; DBRoot-"   << fColInfo->curCol.dataFile.fDbRoot    <<
                        "; part-"     << fColInfo->curCol.dataFile.fPartition <<
                        "; seg-"      << fColInfo->curCol.dataFile.fSegment   <<
                        "; addBytes-" << writeSizeOut                         <<
                        "; totBytes-" << (fNumBytes + writeSizeOut);
                    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
                }

                memcpy(bufOffset, (fBuffer + startOffsetX), writeSizeOut);
                fNumBytes += writeSizeOut;
            }

            startOffsetX += writeSizeOut;
            writeSizeX   -= writeSizeOut;
        } // end of while loop
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Compress and write out the data in the to-be-compressed buffer.
// Also may write out the compression header.
//
// bFinishingFile indicates whether we are finished working with this file,
// either because we are completing an extent or because we have reached the
// end of the input data.  In either case, if bFinishingFile is true, then
// in addition to flushing the current working chunk to disk, this function
// will also write out the updated compression header to match the data.
//
// This function will also write out the compression header if we are writing
// out the first (starting HWM) chunk for this import.  We do this to keep the
// compression header in sync with the data, in case PrimProc is trying to read
// the db file.  It is not necessary to immediately update the header for the
// remaining chunks as they are written out, because PrimProc will not be try-
// ing to access those chunk until we update the extentmap HWM at the end of
// this import.  It's only the starting HWM chunk that may cause a problem and
// requires the immediate rewriting of the header, because we are modifying
// that chunk and adding rows to it.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::compressAndFlush( bool bFinishingFile )
{
    const int OUTPUT_BUFFER_SIZE = IDBCompressInterface::maxCompressedSize(fToBeCompressedCapacity) +
        fUserPaddingBytes;
    unsigned char* compressedOutBuf = new unsigned char[ OUTPUT_BUFFER_SIZE ];
    boost::scoped_array<unsigned char> compressedOutBufPtr(compressedOutBuf);
    unsigned int   outputLen = OUTPUT_BUFFER_SIZE;

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_COMPRESS_COL_COMPRESS);
#endif

    int rc = fCompressor->compressBlock(
        reinterpret_cast<char*>(fToBeCompressedBuffer),
        fToBeCompressedCapacity,
        compressedOutBuf,
        outputLen );
    if (rc != 0)
    {
        return ERR_COMP_COMPRESS;
    }

    // Round up the compressed chunk size
    rc = fCompressor->padCompressedChunks( compressedOutBuf,
        outputLen, OUTPUT_BUFFER_SIZE );
    if (rc != 0)
    {
        return ERR_COMP_PAD_DATA;
    }
    
#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_COL_COMPRESS);
    Stats::startParseEvent(WE_STATS_WRITE_COL);
#endif

    off64_t   fileOffset = fFile->tell();
    size_t nitems =  fFile->write(compressedOutBuf, outputLen) / outputLen;
    if (nitems != 1)
        return ERR_FILE_WRITE;
    CompChunkPtr compChunk(
        (uint64_t)fileOffset, (uint64_t)outputLen);
    fChunkPtrs.push_back( compChunk );

    if (fLog->isDebug( DEBUG_2 )) {
        std::ostringstream oss;
        oss << "Writing compressed data for: OID-" <<
            fColInfo->curCol.dataFile.fid <<
            "; DBRoot-"    << fColInfo->curCol.dataFile.fDbRoot    <<
            "; part-"      << fColInfo->curCol.dataFile.fPartition <<
            "; seg-"       << fColInfo->curCol.dataFile.fSegment   <<
            "; bytes-"     << outputLen <<
            "; fileOffset-"<< fileOffset;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    // We write out the compression headers if we are finished with this file
    // (either because we are through with the extent or the data), or because
    // this is the first HWM chunk that we may be modifying.
    // See the description that precedes this function for more details.
    if ( bFinishingFile || !fFlushedStartHwmChunk )
    {
        fileOffset = fFile->tell();
        RETURN_ON_ERROR( saveCompressionHeaders() );

        // If we just updated the chunk header for the starting HWM chunk,
        // then we flush our output, to synchronize with compressed chunks,
        if ( !fFlushedStartHwmChunk )
        {
            //char resp;
            //std::cout << "dbg: before fflush of hdrs" << std::endl;
            //std::cin  >> resp;
        	if (fFile->flush() != 0)
                return ERR_FILE_FLUSH;
            //std::cout << "dbg: after fflush of hdrs" << std::endl;
            //std::cin  >> resp;
            fFlushedStartHwmChunk = true;
        }

        // After seeking to the top of the file to write the headers,
        // we restore the file offset to continue adding more chunks,
        // if we are not through with this file.
        if ( !bFinishingFile )
        {
            RETURN_ON_ERROR( fColInfo->colOp->setFileOffset(
                             fFile, fileOffset, SEEK_SET) );
        }
    }

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_WRITE_COL);
#endif

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Final flushing of data and headers prior to closing the file.
// File is also truncated if applicable.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::finishFile(bool bTruncFile)
{
    // If capacity is 0, we never got far enough to read in the HWM chunk for
    // the current column segment file, so no need to update the file contents.
    // But we do continue in case we need to truncate the file before exiting.
    // This could happen if our initial block skipping finished an extent.
    if (fToBeCompressedCapacity > 0)
    {
        //char resp;
        //std::cout << "dbg: before finishFile->compressAndFlush" << std::endl;
        //std::cin  >> resp;
        // Write out any data still waiting to be compressed
        RETURN_ON_ERROR( compressAndFlush( true ) );
        //std::cout << "dbg: after finishFile->compressAndFlush" << std::endl;
        //std::cin  >> resp;
    }

#ifdef PROFILE
    Stats::startParseEvent(WE_STATS_COMPRESS_COL_FINISH_EXTENT);
#endif

    // Truncate file (if applicable) based on offset and size of last chunk
    if (bTruncFile && (fChunkPtrs.size() > 0))
    {
        long long truncateFileSize = fChunkPtrs[fChunkPtrs.size()-1].first +
            fChunkPtrs[fChunkPtrs.size()-1].second;

        // @bug5769 Don't initialize extents or truncate db files on HDFS
        if (idbdatafile::IDBPolicy::useHdfs())
        {
            std::ostringstream oss1;
            oss1 << "Finished writing column file"
                ": OID-"    << fColInfo->curCol.dataFile.fid        <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; size-"   << truncateFileSize;
            fLog->logMsg( oss1.str(), MSGLVL_INFO2 );
        }
        else
        {
            std::ostringstream oss1;
            oss1 << "Truncating column file"
                ": OID-"    << fColInfo->curCol.dataFile.fid        <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; size-"   << truncateFileSize;
            fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

            int rc = NO_ERROR;
            if (truncateFileSize > 0)
                rc = fColInfo->colOp->truncateFile( fFile, truncateFileSize );
            else
                rc = ERR_COMP_TRUNCATE_ZERO;//@bug3913-Catch truncate to 0 bytes
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss2;
                oss2 << "finishFile: error truncating file for "        <<
                    "OID "      << fColInfo->curCol.dataFile.fid        <<
                    "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                    "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                    "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                    "; size-"   << truncateFileSize                     <<
                    "; "        << ec.errorString(rc);
                fLog->logMsg( oss2.str(), rc, MSGLVL_ERROR );

                return rc;
            }
        }
    }

    // Nothing more to do if we are not updating the file contents.
    if (fToBeCompressedCapacity == 0)
    {
#ifdef PROFILE
        Stats::stopParseEvent(WE_STATS_COMPRESS_COL_FINISH_EXTENT);
#endif
        return NO_ERROR;
    }

    fToBeCompressedCapacity = 0;
    fNumBytes               = 0;
    fChunkPtrs.clear();

#ifdef PROFILE
    Stats::stopParseEvent(WE_STATS_COMPRESS_COL_FINISH_EXTENT);
#endif

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Write out the updated compression headers.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::saveCompressionHeaders( )
{
    // Construct the header records
    char hdrBuf[IDBCompressInterface::HDR_BUF_LEN*2];
    fCompressor->initHdr( hdrBuf, fColInfo->column.compressionType );
    fCompressor->setBlockCount(hdrBuf,
        (fColInfo->getFileSize()/BYTE_PER_BLOCK) );

    std::vector<uint64_t> ptrs;
    for (unsigned i=0; i<fChunkPtrs.size(); i++)
    {
        ptrs.push_back( fChunkPtrs[i].first );
    }
    unsigned lastIdx = fChunkPtrs.size() - 1;
    ptrs.push_back( fChunkPtrs[lastIdx].first + fChunkPtrs[lastIdx].second );
    fCompressor->storePtrs( ptrs, hdrBuf );

    // Write out the header records
    //char resp;
    //std::cout << "dbg: before writeHeaders" << std::endl;
    //std::cin  >> resp;
    RETURN_ON_ERROR( fColInfo->colOp->writeHeaders(fFile, hdrBuf) );
    //std::cout << "dbg: after writeHeaders" << std::endl;
    //std::cin  >> resp;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Allocates to-be-compressed buffer if it has not already been allocated.
// Initializes to-be-compressed buffer with the contents of the chunk containing
// the fStartingHwm block, as long as that chunk is in the pointer list.
// If the chunk is not in the list, then we must be adding a new chunk, in
// which case we just initialize an empty chunk.
// Returns startFileOffset which indicates file offset (in bytes) where the
// next chunk will be starting.
//------------------------------------------------------------------------------
int ColumnBufferCompressed::initToBeCompressedBuffer(long long& startFileOffset)
{
    bool bNewBuffer = false;

    // Lazy initialization of to-be-compressed buffer
    if (!fToBeCompressedBuffer)
    {
        fToBeCompressedBuffer =
            new unsigned char[IDBCompressInterface::UNCOMPRESSED_INBUF_LEN];
        BlockOp::setEmptyBuf( fToBeCompressedBuffer,
                              IDBCompressInterface::UNCOMPRESSED_INBUF_LEN,
                              fColInfo->column.emptyVal,
                              fColInfo->column.width );
        bNewBuffer = true;
    }

    // Find the chunk containing the starting HWM, as long as our initial
    // block skipping has not caused us to exit the HWM chunk; in which
    // case we start a new empty chunk.
    unsigned int chunkIndex             = 0;
    unsigned int blockOffsetWithinChunk = 0;
    bool         bSkipStartingBlks      = false;
    if (fPreLoadHWMChunk)
    {
        if (fChunkPtrs.size() > 0)
        {
            fCompressor->locateBlock(fStartingHwm,
                chunkIndex, blockOffsetWithinChunk);
            if (chunkIndex < fChunkPtrs.size())
                startFileOffset  = fChunkPtrs[chunkIndex].first;
            else
                fPreLoadHWMChunk = false;
        }
        // If we are at the start of the job, fPreLoadHWMChunk will be true,
        // to preload the old HWM chunk.  But if we have no chunk ptrs, then
        // we are starting on an empty PM.  In this case, we skip starting
        // blks if fStartingHwm has been set.
        else
        {
            fPreLoadHWMChunk  = false;
            bSkipStartingBlks = true;
        }
    }

    // Preload (read and uncompress) the chunk for the starting HWM extent only
    if (fPreLoadHWMChunk)
    {
        fPreLoadHWMChunk = false; // only preload HWM chunk in the first extent

        std::ostringstream oss;
        oss << "Reading HWM chunk for: OID-" <<
            fColInfo->curCol.dataFile.fid <<
            "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
            "; part-"   << fColInfo->curCol.dataFile.fPartition <<
            "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
            "; hwm-"    << fStartingHwm <<
            "; chunk#-" << chunkIndex   <<
            "; blkInChunk-" << blockOffsetWithinChunk;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        // Read the chunk
        RETURN_ON_ERROR( fColInfo->colOp->setFileOffset(
            fFile, startFileOffset, SEEK_SET) );

        char* compressedOutBuf = new char[ fChunkPtrs[chunkIndex].second ];
        boost::scoped_array<char> compressedOutBufPtr(compressedOutBuf);
        size_t itemsRead = fFile->read(compressedOutBuf, fChunkPtrs[chunkIndex].second) / fChunkPtrs[chunkIndex].second;
        if (itemsRead != 1)
        {
            std::ostringstream oss;
            oss << "Error reading HWM chunk for: " <<
                "OID-" << fColInfo->curCol.dataFile.fid <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; hwm-"    << fStartingHwm;
            fLog->logMsg( oss.str(), ERR_COMP_READ_BLOCK, MSGLVL_ERROR );

            return ERR_COMP_READ_BLOCK;
        }

        // Uncompress the chunk into our 4MB buffer
        unsigned int outLen = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;
        int rc = fCompressor->uncompressBlock(
            compressedOutBuf,
            fChunkPtrs[chunkIndex].second,
            fToBeCompressedBuffer,
            outLen);
        if (rc)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error uncompressing HWM chunk for: " <<
                "OID-" << fColInfo->curCol.dataFile.fid <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; hwm-"    << fStartingHwm <<
                "; "        << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

            return ERR_COMP_UNCOMPRESS;
        }

        fToBeCompressedCapacity = outLen;

        // Positition ourselves to start adding data to the HWM block
        fNumBytes = blockOffsetWithinChunk * BYTE_PER_BLOCK;

        // We are going to add data to, and thus re-add, the last chunk; so we
        // drop it from our list.
        fChunkPtrs.resize( fChunkPtrs.size()-1 );
    }
    else // We have left the HWM chunk; just position file offset,
         // without reading anything
    {
        // If it's not a new buffer, we need to initialize, since we won't be
        // reading in anything to overlay what's in the to-be-compressed buffer.
        if (!bNewBuffer)
        {
            BlockOp::setEmptyBuf( fToBeCompressedBuffer,
                                  IDBCompressInterface::UNCOMPRESSED_INBUF_LEN,
                                  fColInfo->column.emptyVal,
                                  fColInfo->column.width );
        }

        if (fLog->isDebug( DEBUG_2 )) {
            std::ostringstream oss;
            oss << "Initializing new empty chunk: OID-" <<
                fColInfo->curCol.dataFile.fid <<
                "; DBRoot-" << fColInfo->curCol.dataFile.fDbRoot    <<
                "; part-"   << fColInfo->curCol.dataFile.fPartition <<
                "; seg-"    << fColInfo->curCol.dataFile.fSegment   <<
                "; hwm-"    << fStartingHwm;
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        fToBeCompressedCapacity = IDBCompressInterface::UNCOMPRESSED_INBUF_LEN;

        // Set file offset to start after last current chunk
        startFileOffset     = IDBCompressInterface::HDR_BUF_LEN*2;
        if (fChunkPtrs.size() > 0)
            startFileOffset = fChunkPtrs[ fChunkPtrs.size()-1 ].first +
                              fChunkPtrs[ fChunkPtrs.size()-1 ].second;

        // Position ourselves to start of empty to-be-compressed buffer.
        // If we are starting the first extent on a PM, we may employ blk
        // skipping at start of import; adjust fNumBytes accordingly.
        // (see ColumnInfo::createDelayedFileIfNeeded() for discussion)
        if (bSkipStartingBlks)
            fNumBytes = fStartingHwm * BYTE_PER_BLOCK;
        else
            fNumBytes = 0;
    }

    return NO_ERROR;
}
 
}
