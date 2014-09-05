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

/******************************************************************************************
* $Id: we_freemgr.h 33 2006-07-19 17:09:27Z mthomas $
*
******************************************************************************************/
/** @file */

#ifndef _WE_FREEMGR_H_
#define _WE_FREEMGR_H_

#include <stdlib.h>

#include <we_dbfileop.h>
#include <we_index.h>

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class FreeMgr */
    class FreeMgr : public DbFileOp
{
public:
   /**
    * @brief Constructor
    */
   FreeMgr();

   /**
    * @brief Default Destructor
    */
   ~FreeMgr();

   /**
    * @brief init free chains in a new index file
    */
   const int      init( CommBlock &cb, const int freemgrType );
   
   /**
    * @brief Init free chains in a new file or an existing file
    * The start block is an FBO or an LBID depending on whether we are using BRM
   **/
   const int      init( CommBlock &cb, DataBlock* blockZero, const int freemgrType , const IdxTreeGroupType chainType, const int startBlock, const int numberBlocks );

   /**
    * @brief find a free segment and return ptr
    */
   const int      assignSegment( CommBlock &cb, DataBlock* blockZero, const int freemgr_type, const IdxTreeGroupType segmentType,  IdxEmptyListEntry* assignPtr ) ;

   /**
    * @brief put a free segment back into chain
    */
   const int      releaseSegment( CommBlock &cb, DataBlock* blockZero, const int freemgr_type, const IdxTreeGroupType segmentType,  IdxEmptyListEntry* assignPtr ) ;
    
   /**
    * @brief Map an FBO to LBID
     */
   const uint64_t  mapLBID( CommBlock &cb, const uint64_t fbo, int &rc );
   
   /**
    * extendFreespace - ran out of space in one of the chains? 
    * Add blocks via calls to BRM extent mgr and fseek the file to the end
    **/
   const int extendFreespace( CommBlock &cb, DataBlock* blockZero, const int freemgr_type );


//private:
    
    /**
     * @brief Create sub block zero for use by free manager
     */
    const int initBlockzero( DataBlock* blockZero);
    
    /**
     * @brief Handle release of sub-blocks separately - sometimes a sub-block contains list entries and sometimes it is a list entry
     */
   
    const int releaseSubblock( CommBlock &cb, WriteEngine::DataBlock*, int, WriteEngine::IdxEmptyListEntry*) ;
    
    /**
     * @brief Handle assignment of sub-blocks separately - sometimes a sub-block contains list entries and sometimes it is a list entry
     */
   
    const int assignSubblock( CommBlock &cb, WriteEngine::DataBlock*, int, WriteEngine::IdxEmptyListEntry*);
    
    /**
     * Blank out the entries in the structure
     **/
    const void  nullPtr( WriteEngine::IdxEmptyListEntry* assignPtr ) const;
    
    const void printMemSubBlock( DataBlock* curBlock, const int sbid );

    inline const int calcPtrOffset( const int position ) const
    { return 1+position; }
    
    inline const int calcStatOffset( const int position ) const
    { return 8+position; }
    
    int initType;// decide which algorithm to use to init the chains
    int allowExtend; // allow file to be extended
};

} //end of namespace
#endif // _WE_FREEMGR_H_
