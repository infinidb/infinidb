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

//  $Id: we_dctnry.h 4726 2013-08-07 03:38:36Z bwilkinson $

/** @we_dctnry.h
 *  Defines the Dctnry class
 *  When a signature is given, the value will be stored in dictionary and
 *  a token will be issued. Given a token, the signature in the dictionary
 *  can be deleted
 */

#ifndef _WE_DCTNRY_H_
#define _WE_DCTNRY_H_

#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

#include "we_dbfileop.h"
#include "we_type.h"
#include "we_brm.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
//---------------------------------------------------------------------------
// Structure used to store signatures in string cache
//---------------------------------------------------------------------------
typedef struct Signature
{
    int size;
    unsigned char* signature;
    Token token;
} Signature;

/**
 * @brief Class to interface with dictionary store files.
 */
class Dctnry : public DbFileOp
{
    //--------------------------------------------------------------------------
    // Public members
    //--------------------------------------------------------------------------
public:
    /**
     * @brief Dctnry Constructor
     */
    EXPORT Dctnry();

    /**
     * @brief Dctnry Destructor
     */
    EXPORT virtual ~Dctnry();

    /**
     * @brief Close the dictionary file handle.
     */
    EXPORT int   closeDctnry(bool realClose = true);

    /**
     * @brief Close the dictionary file handle without flushing the current blk
     * buffer or updating HWM to BRM.
     */
    EXPORT int   closeDctnryOnly();

    /**
     * @brief Create a dictionary extent
     * 
     * If 'flag' is true, a new file is created with an abbreviated extent.
     * If 'flag' is false, then function adds a full exent to an already open
     * file, basically assuming that the file already has 1 or more extents.
     *
     * @param dctnryOID - dictionary file OID
     * @param colWidth  - dictionary string width (not the token width)
     * @param dbRoot    - DBRoot for store file
     * @param partition - partition number for store file
     * @param segment   - column segment number for store file
     * @param flag      - indicates whether extent is added to new file (true)
     * @param startLbid - starting LBID for the newly allocated extent
     */
    EXPORT int   createDctnry(const OID& dctnryOID,
                              int colWidth,
                              const uint16_t dbRoot,
                              const uint32_t partition,
                              const uint16_t segment,
                              BRM::LBID_t&   startLbid,
                              bool flag=true);

    /**
     * @brief Drop dictionary store
     *
     * @param dctnryOID- OID of dictionary store file to be deleted
     */
    EXPORT int   dropDctnry( const OID& dctnryOID);

    /**
     * @brief Accessors
     */
    const std::string& getFileName() const { return m_segFileName; }
    HWM                getHWM()           const { return m_hwm; }
    EXPORT bool        getTokenFromArray(Signature& sig);
    EXPORT uint64_t     getCurLbid(){return m_curLbid;}

    /**
     * @brief Insert a signature value to a file block and return token/pointer.
     * (for DDL/DML use)
     *
     * @param sgnature_size  - size of signature to be inserted
     * @param sgnature_value - signature to be inserted
     * @param token          - (output) token associated with inserted signature
     */
    EXPORT int   insertDctnry(const int& sgnature_size,
                      const unsigned char* sgnature_value, Token& token);

    /**
     * @brief Insert a signature value to a file block and return token/pointer
     * (for Bulk use)
     *
     * @param buf       - bulk buffer containing strings to be parsed
     * @param pos       - list of offsets into buf
     * @param totalRow  - total number of rows in buf
     * @param col       - the column to be parsed from buf
     * @param tokenBuf  - (output) list of tokens for the parsed strings
     */
    EXPORT int   insertDctnry(const char* buf,
                      ColPosPair ** pos,
                      const int totalRow, const int col,
                      char* tokenBuf,
                      long long& truncCount);

    /**
     * @brief Update dictionary store with tokenized strings (for DDL/DML use)
     *
     * @param sigValue  - signature value
     * @param sigSize   - signature size
     * @param token     - (output) token that was added
     */
    EXPORT int   updateDctnry(unsigned char* sigValue,
                              int& sigSize,
                              Token& token);

    /**
     * @brief open dictionary store
     *
     * @param dctnryOID - dictionary file OID
     * @param dbRoot    - DBRoot for store file
     * @param partition - partition number for store file
     * @param segment   - column segment number for store file
     * @param useTmpSuffix - for Bulk HDFS usage: use or not use *.tmp file suffix
     */
    EXPORT int   openDctnry(const OID& dctnryOID, const uint16_t dbRoot,
                    const uint32_t partition, const uint16_t segment,
                    const bool useTmpSuffix);

    /**
     * @brief copy the dictionary header to buffer
     */
    void         copyDctnryHeader(void* buf);

    /**
     * @brief Set logger that can be used for logging (primarily by bulk load)
     */
    void         setLogger(Log* logger) { m_logger = logger; }

    /**
     * @brief Set dictionary column width for this column
     */
    void         setColWidth(int colWidth) { m_colWidth = colWidth; }

    /**
     * @brief Set dictionary default for this column
     */
    void         setDefault(const std::string& defVal) { m_defVal = defVal; }

    void         setImportDataMode( ImportDataMode importMode )
                 { m_importDataMode = importMode; }
                 
    virtual int checkFixLastDictChunk() {return NO_ERROR;}

//------------------------------------------------------------------------------
// Protected members
//------------------------------------------------------------------------------
protected:

    //
    // Add the specified signature (string) to the string cache
    //
    void         addToStringCache( const Signature& newSig );

    //
    // Clear the dictionary store.
    //
    void         clear() { m_dFile = NULL; m_dctnryOID =(OID)INVALID_NUM; }

    // Expand an abbreviated extent on disk.
    int          expandDctnryExtent();

    // Free memory consumed by strings in the string cache
    void         freeStringCache();

    //
    // Functions to read data:
    //   getBlockOpCount - get the ordinal position (OP) count from the header
    //   getEndOp        - read OP of the end of header for specified fbo
    //
    void  getBlockOpCount(const DataBlock& fileBlock, int & op_count);
    int   getEndOp       (IDBDataFile* dFile, int fbo, int &op);

    //
    // Initialization
    //
    int          init();

    //
    // Support functions for inserting values into dictionary.
    // insertDctnryHdr inserts the new value info into the header.
    // insertSgnture   inserts the new value into the block.
    //
    void         insertDctnry2(Signature& sig);
    void         insertDctnryHdr( unsigned char* blockBuf, const int& size);
    void         insertSgnture(unsigned char* blockBuf,
                               const int& size, unsigned char*value);

    //
    // Preloads the strings from the specified DataBlock.  Currently
    // used to preload the first block, of a store file having only 1 block.
    //
    void         preLoadStringCache( const DataBlock& fileBlock );

    // methods to be overriden by compression classes
    // (width argument in createDctnryFile() is string width, not token width)
    virtual IDBDataFile* createDctnryFile(const char *name, int width,
                                   const char *mode, int ioBuffSize);
    virtual IDBDataFile* openDctnryFile(bool useTmpSuffix);
    virtual void  closeDctnryFile(bool doFlush, std::map<FID,FID> & oids);
    virtual int   numOfBlocksInFile();

    Signature    m_sigArray[MAX_STRING_CACHE_SIZE]; // string cache
    int          m_arraySize;                       // num strings in m_sigArray

    // m_dctnryHeader  used for hdr when readSubBlockEntry is used to read a blk
    // m_dctnryHeader2 contains filled in template used to initialize new blocks
    unsigned char m_dctnryHeader[DCTNRY_HEADER_SIZE];  // first 14 bytes of hdr
    unsigned char m_dctnryHeader2[DCTNRY_HEADER_SIZE]; // first 14 bytes of hdr

    uint64_t     m_nextPtr;          // next pointer

    //relate to different Dictionary file
    FID          m_dctnryOID;        // OID for the dctnry file
    IDBDataFile* m_dFile;            // dictionary file
    uint32_t     m_partition;        // partition associated with OID
    uint16_t     m_segment;          // segment associated with OID
    uint16_t     m_dbRoot;           // DBRoot associated with OID
    std::string  m_segFileName;      // current column segment file
    int          m_numBlocks;        // num "raw" uncompressed blocks in file
    int          m_lastFbo;
    HWM          m_hwm;
    //Need to be initialized for different Dictionary file
    int          m_newStartOffset;   // start offset
    uint16_t     m_freeSpace;        // free space (bytes) within current block
    int          m_curOp;            // current ordinal pointer within m_curFbo
    int          m_curFbo;           // current "raw" (uncompressed) FBO
    BRM::LBID_t  m_curLbid;          // LBID associated with m_curFbo
    DataBlock    m_curBlock;         // current "raw" (uncompressed) data block
    Log*         m_logger;           // logger, mainly for bulk load
    int          m_colWidth;         // width of this dictionary column
    std::string  m_defVal;           // optional default string value
    ImportDataMode m_importDataMode; // Import data in text or binary mode

};//end of class

} //end of namespace

#undef EXPORT

#endif // _WE_DCTNRY_H_
