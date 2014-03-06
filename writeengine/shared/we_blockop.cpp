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
* $Id: we_blockop.cpp 4500 2013-01-31 20:25:42Z dhall $
*
*******************************************************************************/
/** @file */

#include <stdio.h>
#include <string.h>

#include "joblisttypes.h"

#include "we_blockop.h"

#include "we_config.h"
#include "we_brm.h"
#include "we_convertor.h"

using namespace execplan;

namespace WriteEngine
{

/**
 * Constructor
 */
BlockOp::BlockOp()
{}

/**
 * Default Destructor
 */
BlockOp::~BlockOp()
{}

/***********************************************************
 * DESCRIPTION:
 *    Calculate the location of Row ID
 * PARAMETERS:
 *    rowId - row id
 *    epb - entry per block
 *    width - width per column record
 * RETURN:
 *    fbo - File block offset
 *    bio - Block internal offset
 *    true if success, false otherwise
 ***********************************************************/
bool BlockOp::calculateRowId(
    RID rowId, const int epb, const int width, int& fbo, int& bio ) const
{
    if( std::numeric_limits<WriteEngine::RID>::max() == rowId )
        return false;
         
	fbo = (int)( rowId/epb );
	bio = ( rowId & ( epb - 1 )) * width;

    return true;
}

/***********************************************************
 * DESCRIPTION:
 *    Get the value that represents empty row
 * PARAMETERS:
 *    colDataType - data type
 *    width - data width in byte
 * RETURN:
 *    emptyVal - the value of empty row
 ***********************************************************/
uint64_t BlockOp::getEmptyRowValue(
    const CalpontSystemCatalog::ColDataType colDataType, const int width ) const
{
    uint64_t emptyVal = 0;
    int offset = 0;

    switch( colDataType ) {
        case CalpontSystemCatalog::TINYINT : emptyVal = joblist::TINYINTEMPTYROW; break;
        case CalpontSystemCatalog::SMALLINT: emptyVal = joblist::SMALLINTEMPTYROW; break;
        case CalpontSystemCatalog::MEDINT :  
        case CalpontSystemCatalog::INT :     emptyVal = joblist::INTEMPTYROW; break;
        case CalpontSystemCatalog::BIGINT :  emptyVal = joblist::BIGINTEMPTYROW; break;
        case CalpontSystemCatalog::FLOAT :
        case CalpontSystemCatalog::UFLOAT :   emptyVal = joblist::FLOATEMPTYROW; break;
        case CalpontSystemCatalog::DOUBLE :
        case CalpontSystemCatalog::UDOUBLE :  emptyVal = joblist::DOUBLEEMPTYROW; break;
        case CalpontSystemCatalog::DECIMAL : 
        case CalpontSystemCatalog::UDECIMAL : 
/*          if( width <= 4 )
                emptyVal = joblist::SMALLINTEMPTYROW;
            else
                if( width <= 9 )
                    emptyVal = 0x80000001;
                else
                if( width <= 18 )
                    emptyVal = 0x8000000000000001LL; 
                else
                    emptyVal = 0xFFFFFFFFFFFFFFFFLL; 
*/ 
                // @bug 194 use the correct logic in handling empty value for decimal
				if (width <= 1)
					emptyVal = joblist::TINYINTEMPTYROW;
                else if( width <= 2 )
                    emptyVal = joblist::SMALLINTEMPTYROW;
                else if( width <= 4 )
                    emptyVal = joblist::INTEMPTYROW;
                else
                    emptyVal = joblist::BIGINTEMPTYROW;
                break;
        case CalpontSystemCatalog::UTINYINT : emptyVal = joblist::UTINYINTEMPTYROW; break;
        case CalpontSystemCatalog::USMALLINT: emptyVal = joblist::USMALLINTEMPTYROW; break;
        case CalpontSystemCatalog::UMEDINT :  
        case CalpontSystemCatalog::UINT :     emptyVal = joblist::UINTEMPTYROW; break;
        case CalpontSystemCatalog::UBIGINT :  emptyVal = joblist::UBIGINTEMPTYROW; break;

        case CalpontSystemCatalog::CHAR : 
        case CalpontSystemCatalog::VARCHAR : 
        case CalpontSystemCatalog::DATE :
        case CalpontSystemCatalog::DATETIME :
        default:
            offset = ( colDataType == CalpontSystemCatalog::VARCHAR )? -1 : 0;
            emptyVal = joblist::CHAR1EMPTYROW;
            if( width == (2 + offset) )
                emptyVal = joblist::CHAR2EMPTYROW; 
            else
                if( width >= (3 + offset) && width <= ( 4 + offset ) )
                    emptyVal = joblist::CHAR4EMPTYROW; 
                else
                if( width >= (5 + offset)  )
                    emptyVal = joblist::CHAR8EMPTYROW;
                break;
    }

    return emptyVal;
}

/***********************************************************
 * DESCRIPTION:
 *    Get the correct width for a row
 * PARAMETERS:
 *    colDataType - data type
 *    width - data width in byte
 * RETURN:
 *    emptyVal - the value of empty row
 ***********************************************************/
int BlockOp::getCorrectRowWidth(
    const CalpontSystemCatalog::ColDataType colDataType, const int width ) const
{
    return Convertor::getCorrectRowWidth(colDataType, width);
}

/***********************************************************
 * DESCRIPTION:
 *    Get row id
 * PARAMETERS:
 *    fbo - file block offset
 *    bio - block internal offset
 *    bbo - byte internal offset
 * RETURN:
 *    row id
 ***********************************************************/
RID BlockOp::getRowId(
    const long fbo, const int width, const int rowPos
    /*const int bio, const int bbo*/ ) const
{
//      return fbo*BYTE_PER_BLOCK*ROW_PER_BYTE + bio*ROW_PER_BYTE + bbo;
    return (BYTE_PER_BLOCK/width) * fbo + rowPos;
}

/***********************************************************
 * DESCRIPTION:
 *    Get buffer value
 * PARAMETERS:
 *    buf - buffer
 *    width - data width in byte
 * RETURN:
 *    val - buffer value
 ***********************************************************/
void BlockOp::readBufValue(
    const unsigned char* buf, void* val, const short width ) const
{
    memcpy( val, buf, width );
}

/***********************************************************
 * DESCRIPTION:
 *    Reset a buffer
 * PARAMETERS:
 *    buf - buffer
 *    bufSize - buffer size
 * RETURN:
 *    none
 ***********************************************************/
void BlockOp::resetBuf(  unsigned char* buf, const int bufSize ) const
{
    memset( buf, 0, bufSize );
}

/***********************************************************
 * DESCRIPTION:
 *    Fill buffer with empty values
 * PARAMETERS:
 *    buf - buffer
 *    bufSize - buffer size
 *    emptyVal - empty value
 *    width - type width
 * RETURN:
 *    none
 ***********************************************************/
/* static */
void BlockOp::setEmptyBuf(
    unsigned char* buf, const int bufSize, uint64_t emptyVal, const int width )
{
    const int ARRAY_COUNT     = 128;
    const int NBYTES_IN_ARRAY = width * ARRAY_COUNT;
    //unsigned char emptyValArray[NBYTES_IN_ARRAY];
	unsigned char* emptyValArray = (unsigned char*)alloca(NBYTES_IN_ARRAY);

    // Optimize buffer initialization by constructing and copying in an array
    // instead of individual values.  This reduces the number of calls to
    // memcpy().
    for (int j=0; j<ARRAY_COUNT; j++)
    {
        memcpy(emptyValArray+(j*width), &emptyVal, width);
    }

    int countFull128 = (bufSize/width) / ARRAY_COUNT;
    int countRemain  = (bufSize/width) % ARRAY_COUNT;

    // Copy in the 128 element array into "buf" as many times as needed
    if (countFull128 > 0)
    {
        for( int i = 0; i < countFull128; i++ )
            memcpy( buf + (i * (NBYTES_IN_ARRAY)),
                  emptyValArray,
                  NBYTES_IN_ARRAY );
    }

    // Initialize the remainder of "buf" that is leftover
    if (countRemain > 0)
    {
        memcpy( buf + (countFull128 * NBYTES_IN_ARRAY),
                  emptyValArray,
                  width*countRemain );
    }
}

/***********************************************************
 * DESCRIPTION:
 *    Set a value in a buffer
 * PARAMETERS:
 *    buf - buffer
 *    val - buffer value
 *    width - data width in byte
 * RETURN:
 *    none
 ***********************************************************/
void BlockOp::writeBufValue(
    unsigned char* buf, void* val, const size_t width, const bool clear ) const
{
    if( clear )
        memset( buf, 0, width );

    memcpy( buf, val, width );
}

} //end of namespace

