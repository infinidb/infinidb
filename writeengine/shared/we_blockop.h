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
* $Id: we_blockop.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_BLOCKOP_H_
#define _WE_BLOCKOP_H_

#include <we_obj.h>

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class BlockOp */
class BlockOp : public WEObj
{
public:
   /**
    * @brief Constructor
    */
    EXPORT BlockOp();

   /**
    * @brief Default Destructor
    */
    EXPORT ~BlockOp();
 

   /**
    * @brief Calculate the location of Row ID
    */
    EXPORT bool     calculateRowId( RID rowId,
                                    const int epb,
                                    const int width,
                                    int&  fbo,
                                    int&  bio ) const;

   /**
    * @brief Calculate the location of Row ID
    */
    void            clearBlock(     DataBlock* block )
                    { memset(block->data, 0, sizeof(block->data));
                      block->no = -1;
                      block->dirty = false; }

   /**
    * @brief Get bit value after shift
    */
    uint64_t        getBitValue(    uint64_t val,
                                    int shiftBit,
                                    uint64_t mask ) const
                    { return ( val >> shiftBit ) & mask ; }

   /**
    * @brief Get correct row width
    */
    EXPORT int      getCorrectRowWidth( const execplan::CalpontSystemCatalog::ColDataType colDataType,
                                    const int width ) const;

   /**
    * @brief Get an empty row value
    */
    EXPORT uint64_t getEmptyRowValue(const execplan::CalpontSystemCatalog::ColDataType colDataType,
                                    const int width ) const;

   /**
    * @brief Calculate row id
    */
    EXPORT RID      getRowId(       const long fbo,
                                    const int width,
                                    const int rowPos ) const;

   /**
    * @brief Get buffer value
    */
    EXPORT void     readBufValue(   const unsigned char* buf,
                                    void* val, const short width ) const;

   /**
    * @brief Reset a buffer
    */
    EXPORT void     resetBuf(       unsigned char* buf,
                                    const int bufSize ) const;

   /**
    * @brief Fill buffer with empty values
    */
    EXPORT void static setEmptyBuf( unsigned char* buf,
                                    const int bufSize,
                                    uint64_t emptyVal, const int width );

   /**
    * @brief Set a value in a buffer
    */
    EXPORT void     writeBufValue(  unsigned char* buf,
                                    void* val,
                                    const size_t width,
                                    const bool clear = false ) const;
};

} //end of namespace

#undef EXPORT

#endif // _WE_BLOCKOP_H_
