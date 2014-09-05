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
* $Id: we_freemgr.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
******************************************************************************************/
/** @file */

#include <stdio.h>
#include <string.h>
#include <iostream>
#include "we_freemgr.h"
using namespace std;

namespace WriteEngine
{
   /**
    * Constructor
    */
   FreeMgr::FreeMgr()
   {
      //memset( m_workBlock.data, 0, BYTE_PER_BLOCK );
       
       
       // use 0 for old style, 1 for new without block list and 2 for new with block list
       initType = 2;
//        setDebugLevel( DEBUG_1 );
       allowExtend = 1;
   }

   /**
    * Default Destructor
    */
   FreeMgr::~FreeMgr()
   {}

   
   /**
    * 
    * blank out all stats, set types and groups of null pointers in block zero
    */
   const int FreeMgr::initBlockzero( DataBlock* blockZero)
   {
       IdxEmptyListEntry Pointer; 
       uint64_t blankVal = 0;
       
       memset(blockZero->data, 0, sizeof(blockZero->data));
        
       nullPtr( &Pointer );
       Pointer.type = EMPTY_LIST;

       Pointer.group = ENTRY_BLK;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_BLK), 8, &Pointer); 
       
       Pointer.group = ENTRY_32;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_32), 8, &Pointer); 
            
       Pointer.group = ENTRY_16;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_16), 8, &Pointer); 
            
       Pointer.group = ENTRY_8;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_8), 8, &Pointer); 
            
       Pointer.group = ENTRY_4;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_4), 8, &Pointer); 
            
       Pointer.group = ENTRY_2;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_2), 8, &Pointer); 
            
       Pointer.group = ENTRY_1;
       setSubBlockEntry( blockZero->data, 0, calcPtrOffset(ENTRY_1+ENTRY_1), 8, &Pointer); 

       //all stat locations are contiguous and after the pointers in block zero
       
       for (int idx = calcStatOffset( ENTRY_1 ); idx < calcStatOffset( ENTRY_32 )+1; idx++)
       {
           setSubBlockEntry( blockZero->data, 0, idx, 8, &blankVal); 
       }

           
       return NO_ERROR;
   }
   
   const int FreeMgr::init(CommBlock &cb,  DataBlock* blockZero, int freemgrType, IdxTreeGroupType chainType, int startBlock, int numberBlocks)
   {
     
       /**
        * Use this to allocate a group of blocks or sub blocks to a free list
        * startBlock is fbo NOT lbid
        * This allows us to use a simple for loop to traverese the full range of blocks
        *
        **/
       
       int blkIdx, sbIdx;
       int rc;
       IdxEmptyListEntry emptyEntry;  // populate the chains with pointer to empty entries
       FILE* indexFile;
               
       indexFile = cb.file.pFile;

       nullPtr(&emptyEntry);
       emptyEntry.type = EMPTY_PTR;

       if( isDebug( DEBUG_3 )) { printf("\nNew style Init v2, file: %u startBlock: %i num: %i \n", cb.file.oid, (unsigned int)startBlock, (unsigned int)numberBlocks); }
       int lastBlock = numberBlocks + startBlock;

       if (chainType == ENTRY_BLK)
       {
           emptyEntry.group = ENTRY_BLK;

           for ( blkIdx =  lastBlock ; blkIdx > startBlock  ; blkIdx-- )
           {
//                if( isDebug( DEBUG_1 )) { printf("Initing block %u\n", blkIdx-1);}
               emptyEntry.fbo = mapLBID( cb, blkIdx-1, rc );// map fbo to lbid before storage
               if (rc != NO_ERROR ){ printf("DBG: Init 1: Error resolving LBID for OID: %u FBO: %u\n", cb.file.oid, blkIdx-1 ); return rc; }
               if( isDebug( DEBUG_3 )) { cout<<"DBG: Working on block "<<emptyEntry.fbo<<"\n"; }
               
               emptyEntry.sbid = 0;
               emptyEntry.type = EMPTY_PTR;
               rc = releaseSegment( cb, blockZero, freemgrType, chainType, &emptyEntry );
               if (rc != NO_ERROR){ printf("Error releasing sb\n"); return rc; }
           }

       } else if (chainType == ENTRY_32 ){

           emptyEntry.group = ENTRY_32;

           for ( blkIdx = lastBlock ; blkIdx > startBlock ; blkIdx-- )
           {
//                if( isDebug( DEBUG_1 )) { printf("Initing block %u\n", blkIdx-1);}
               emptyEntry.fbo = mapLBID( cb, blkIdx-1, rc );// map fbo to lbid before storage
               if (rc != NO_ERROR ){ printf("DBG: Init 2: Error resolving LBID for OID: %u FBO: %u\n", cb.file.oid, blkIdx-1 ); return rc; }
               if( isDebug( DEBUG_3 )) { cout<<"DBG: Working on block "<<emptyEntry.fbo<<"\n"; }

               for (sbIdx=BYTE_PER_BLOCK/BYTE_PER_SUBBLOCK-1; sbIdx >-1; sbIdx--)  
               {
                   if( isDebug( DEBUG_3 )) { printf("DBG: Working on subblock %u\n", sbIdx); }
                   emptyEntry.sbid = sbIdx;
                   emptyEntry.type = EMPTY_PTR;
                   rc = releaseSubblock( cb, blockZero, freemgrType, &emptyEntry );
                   if (rc != NO_ERROR){ printf("Error releasing sb\n"); return rc; }
               }
           }

       } else {
           if ( isDebug( DEBUG_1 )){ printf("DBG: Invalid segment size %i (should be ENTRY_32 or ENTRY_BLK)\n", chainType); }
           return ERR_INVALID_PARAM;
       }
        // now write sb0 back to disk
       uint64_t lbid = mapLBID( cb, 0, rc);
       if (rc != NO_ERROR ){ return rc; }
       rc = writeDBFile( cb, blockZero, lbid );
       if (rc != NO_ERROR)
       {
           return rc; 
       }

       return NO_ERROR;
   }
   
   
      /***********************************************************
   * DESCRIPTION:
   *    Initializes free space lists in an new index file
   * PARAMETERS:
   *    indexFile - file pointer for new index file
   *    freemgr_type - type of free mgr to init
   * RETURN:
   *    NO_ERROR if success
   *
      ***********************************************************/

   
   const int FreeMgr::init( CommBlock &cb, int freemgrType) 
   {
     
       /**
        * Tree has 6 chains, (1,2,4,8,16,32 entries) starting at entry 1 in sb0 of block0
        * List has 3 chains (4,32,1024 entries) starting at first entry of file
        *
        * Starting at Block 1, add each subblock after sb0 to the free list in sb0
        * then go back and build the smaller chains on demand. This way we make sure all SBs
		* are in a chain somewhere.. but this is not best behaviour when allocating blocks
        *
       **/

        int         rc;    // return code from file ops
        DataBlock   blockZero;
        DataBlock   workBlock;
        int         sbIdx;
        uint64_t     blkIdx;
        IdxEmptyListEntry emptyEntry;  // populate the chains with pointer to empty entries
        IdxEmptyListEntry nextPointer; // pointer at end of sub-block of pointers to emptyEntries
        uint64_t     numBlocks; 
        FILE*       indexFile;

        indexFile = cb.file.pFile;

        if (!indexFile) // Make sure that we have non-null filehandle
        {
//             printf("DBG: File handle is null\n");
            return ERR_INVALID_PARAM;
        }
        if (freemgrType!=TREE && freemgrType!=LIST){
//             printf("DBG: Bad type in freeMgr Init\n");
            return ERR_INVALID_PARAM;
        }

        numBlocks = getFileSize( indexFile )/BYTE_PER_BLOCK ;
        
//         printf ("DBG: File size: %lu Total blocks: %llu (%u)\n", getFileSize( indexFile ), numBlocks, BYTE_PER_BLOCK);
//         printf ("DBG: Adding sub-blocks: %llu \n",  numBlocks * 32);

		// Clear the list of pointers in sb0
        initBlockzero( &blockZero );

        // initialize the non varying fields
        nullPtr(&emptyEntry);
        emptyEntry.type = EMPTY_PTR;
        emptyEntry.group = ENTRY_32;

        // nextPointer identifies next sub-block with empty pointers
        // initially there is no next sub-block so zero it out
        nullPtr(&nextPointer);
        nextPointer.type = EMPTY_LIST;
        nextPointer.group = ENTRY_32;
        nextPointer.entry = ENTRY_PER_SUBBLOCK-1;  // last entry on the list.. think of the list as a stack

        if (initType == 0){
            if( isDebug( DEBUG_3 )) { printf("\nOld style init\n"); }
            
        for ( blkIdx = numBlocks-1; blkIdx > 0; blkIdx-- )
        {
            emptyEntry.fbo  = blkIdx;// map fbo to lbid before storage
//             emptyEntry.fbo  = mapLBID( cb, blkIdx, rc );// map fbo to lbid before storage
//             if (rc != NO_ERROR ){ printf("DBG: Error resolving LBID for OID: %u FBO: %llu\n", cb.file.oid, blkIdx-1 ); return rc; }

            if( isDebug( DEBUG_3 )) { cout<<"DBG: Working on block "<<emptyEntry.fbo<<"\n"; }
            memset(workBlock.data, 0, sizeof(workBlock.data));

            /** 
             * each block after zeroth block uses sb0 to store entry list
             * first entry (#0) is the llp to additional sblks with entries
             * entries 1-31 are pointers
             **/
                			
			// sb0 is used for initial map, so start at sb1
            for (sbIdx=1; sbIdx < BYTE_PER_BLOCK/BYTE_PER_SUBBLOCK; sbIdx++)  
            {
                if( isDebug( DEBUG_3 )) { printf("DBG: Working on subblock %u\n", sbIdx); }
                emptyEntry.sbid = sbIdx;
                emptyEntry.type = EMPTY_PTR;
				// store pointer in sb0 -  replace this with a releaseSubblock call
                setSubBlockEntry( workBlock.data, 0, sbIdx, 8, &emptyEntry);  
                if( isDebug( DEBUG_2 )) {
                    cout<<"DBG: Init sb fbo "<<emptyEntry.fbo<<" sbid "<<emptyEntry.sbid<<" entry "<<emptyEntry.entry<<"\n";
                }
            }
			// having stored all pointers, store linkptr
            setSubBlockEntry( workBlock.data, 0, 0, 8, &nextPointer);  
//             nextPointer.fbo = mapLBID( cb, blkIdx, rc ); // remember this block ID
            nextPointer.fbo = blkIdx; // remember this block ID
//             if (rc != NO_ERROR ){ printf("DBG: Error resolving LBID for OID: %u FBO: %llu\n", cb.file.oid, blkIdx-1 ); return rc; }

            rc = writeDBFile( cb, workBlock.data, emptyEntry.fbo );
            if (rc != NO_ERROR){ return rc; }

        }

		// chain for segments of sub block size
        setSubBlockEntry( blockZero.data, 0, calcPtrOffset(ENTRY_32), 8, &nextPointer); 
        
        /**
         * the next algorithm uses the release sub-block method and does not layout the map on particular boundaries.
         **/
        
        } else if (initType  == 1) { // new style - use release 
            if( isDebug( DEBUG_3 )) { printf("\nNew style\n"); }
            
            for ( blkIdx = numBlocks-1; blkIdx > 0; blkIdx-- )
            {
                
                emptyEntry.fbo  = blkIdx;// map fbo to lbid before storage

//                 emptyEntry.fbo  = mapLBID( cb, blkIdx, rc );// map fbo to lbid before storage
//                 if (rc != NO_ERROR ){ printf("DBG: Error resolving LBID for OID: %u FBO: %llu\n", cb.file.oid, blkIdx-1 ); return rc; }

                if( isDebug( DEBUG_3 )) { cout<<"DBG: Working on block "<<emptyEntry.fbo<<"\n"; }
                memset(workBlock.data, 0, sizeof(workBlock.data));
            
                for (sbIdx=BYTE_PER_BLOCK/BYTE_PER_SUBBLOCK-1; sbIdx >-1; sbIdx--)  
                {
                    if( isDebug( DEBUG_3 )) { printf("DBG: Working on subblock %u\n", sbIdx); }
                    emptyEntry.sbid = sbIdx;
                    emptyEntry.type = EMPTY_PTR;
                // store pointer in sb0 -  replace this with a releaseSubblock call
                rc = releaseSubblock( cb, &blockZero, freemgrType, &emptyEntry );
                if (rc != NO_ERROR){ printf("Error releasing sb\n"); return rc; }
                }
            }

            /**
             * the next algorithm uses the release sub-block method and does not layout the map on particular boundaries.
             * It also allows pieces to be allocated
            **/
        

        } else  if (initType  == 2){
                /** The following calls to init accept FBO not LBID..
                 * This makes it easier to work on a range of blocks
                 **/
                // use the first block of the new range for sub-block chain
                // and the rest for block chain
            rc = init( cb, &blockZero,  freemgrType,  ENTRY_32,  1, 50);
            if ( rc != NO_ERROR ) { return rc; }
            rc = init( cb, &blockZero,  freemgrType,  ENTRY_BLK, 51, numBlocks-52);
            if ( rc != NO_ERROR ) { return rc; }

        } 
        // now write sb0 back to disk
        if ( isDebug( DEBUG_2 ))
        { 
            printf("Writing SB0 back to disk\n"); 
            printMemSubBlock( &blockZero, 0 );
        }
        
        uint64_t lbid = mapLBID( cb, 0, rc);
        if (rc != NO_ERROR ){ return rc; }

        rc = writeDBFile( cb, blockZero.data, lbid );
        if (rc != NO_ERROR)
        { 
            return rc; 
        }
       
        return NO_ERROR;

   }

   /**
    * DESCRIPTION:
    *    Assign segment- public method used to get a free segment for use
    * PARAMETERS:
    *   indexFile - file pointer 
    *   blockZero - in memory copy of blockZero
    *   freemgr_type - either TREE or LIST
    *   segmentType - group type
    *   assignPtr - the assigned ptr
    *
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    **/

   const int FreeMgr::assignSegment( CommBlock &cb, DataBlock* blockZero, const int freemgr_type, const IdxTreeGroupType segmentType, IdxEmptyListEntry* assignPtr )
   {
       int          rc;    // return code from file ops
       DataBlock    workBlock;
       int          listOffset; // entry in block zero of head pointer
       IdxEmptyListEntry emptyEntry;
       IdxEmptyListEntry emptyPtr;
       IdxEmptyListEntry emptyMap;
       uint64_t          numBlocks;
       IdxEmptyListEntry newSb;
       FILE* indexFile;
               
       indexFile = cb.file.pFile;

       if( isDebug( DEBUG_3 )) { 
           printf("DBG: Assign ENTRY_%i segment in %s\n", 1<<(unsigned int)segmentType,(freemgr_type==LIST)?"LIST":"TREE");
       }
       
/* Check all input parameters are ok   */
       if (!blockZero){
           if( isDebug( DEBUG_1 )) { printf ("DBG: Bad pointer: blockZero is zero\n"); }
           return ERR_INVALID_PARAM;
       }
       
       if (!assignPtr)
       {
           if( isDebug( DEBUG_1 )) {  printf ("DBG: Bad pointer: assignPtr is zero\n"); }
           return ERR_INVALID_PARAM;
       }
       if (freemgr_type != TREE && freemgr_type!= LIST)
       {
           if( isDebug( DEBUG_0 )) { printf ("DBG: assignSegment: Must be TREE or LIST\n");}
           return ERR_INVALID_PARAM;
       }
           
       numBlocks = getFileSize( indexFile )/BYTE_PER_BLOCK ;
       //find the start of the chain
       if (segmentType == ENTRY_32)
       {
           rc = assignSubblock( cb, blockZero, freemgr_type, assignPtr);
           if (rc != NO_ERROR)
           { 
               if( isDebug( DEBUG_1 )) { printf("DBG: Error assigning sb (rc=%i)\n", rc); }
           }
           return rc;
       }
               
       listOffset = calcPtrOffset( segmentType );
       if (freemgr_type==LIST && !(segmentType== ENTRY_4 || segmentType== ENTRY_BLK))
       {
           if( isDebug( DEBUG_1 )) { printf("DBG: Assign segment size illegal %i\n", (unsigned int)segmentType); }
           return ERR_INVALID_PARAM; // should not have got here so quit with error
       }
       
       // read Empty Map in sb0
       getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
       if( isDebug( DEBUG_2 )) { 
           cout<<"DBG: Empty map ENTRY_"<<(1<<segmentType)<<"  was fbo "<<emptyPtr.fbo<<" sbid "<<(unsigned int)emptyPtr.sbid<<" entry "<<(unsigned int)emptyPtr.entry<<"\n";
       }

/*       if (emptyPtr.fbo > numBlocks)
       {
           printf("DBG: Weirdness in assignSegment.. emptyPtr.fbo > numBlocks\n");
           return ERR_FM_BAD_FBO;
       }*/
       
       // check to see if queue has been built
       // if not then assign a container block, add LLP in entry 0 pointing to nothing
       if (emptyPtr.fbo == 0){
           if( isDebug( DEBUG_3 )) { printf("DBG: Need to add sb to chain and entries to sb\n"); }
           
           // cannot assign more space to block list from a smaller list.. need to extend the file
           if ( segmentType == ENTRY_BLK ) {
               if ( isDebug( DEBUG_1 )){ printf("Out of space in BLOCK list, quitting\n"); }
               rc = extendFreespace( cb, blockZero, freemgr_type );
               if (rc != NO_ERROR){ 
                   if (isDebug( DEBUG_3 )){ printf("DBG: Error extending file\n"); }
                   return rc; 
               }
           }
           
           rc = assignSubblock( cb, blockZero, freemgr_type, &emptyPtr);
           if (rc != NO_ERROR){ printf("DBG: Error extending chain\n"); 
               return rc; 
           }
           rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
           if (rc != NO_ERROR){ /*printf("DBG: Error reading newly allocated sb\n");*/ 
               return rc; 
           }
           // update map to point to new bucket
           setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr); 

           nullPtr(&emptyEntry);
           emptyEntry.type = EMPTY_LIST;
           emptyEntry.group = segmentType;

           setSubBlockEntry( workBlock.data, emptyPtr.sbid, 0, 8, &emptyEntry);  // store head ptr

           rc = writeDBFile( cb, workBlock.data, emptyPtr.fbo );
           if (rc != NO_ERROR){ return rc; }
           if( isDebug( DEBUG_2 )) { 
		cout<<"DBG: Added fbo "<<emptyPtr.fbo<<" sbid "<<(unsigned int)emptyPtr.sbid<<" as bucket to chain ENTRY_"<<(1<<segmentType)<<"in"<<((freemgr_type==LIST)?"LIST":"TREE")<<"\n";
           }
       }

       // follow the chain to the head container 
       rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
       if (rc != NO_ERROR)
       { 
           if( isDebug( DEBUG_1 )) { cout<<"DBG: Error reading block ("<<emptyPtr.fbo<<") during segmentAssign: rc is "<<rc; }
           return rc; 
       }
       
       getSubBlockEntry( &workBlock, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry );
           
       if ((emptyEntry.type != EMPTY_LIST) && (emptyEntry.type != EMPTY_PTR))
       {
           if( isDebug( DEBUG_0 )) { 
               printf("WTF: Bad entry in ENTRY_%i chain - type is %i (expected %i or %i)\n",  1<<segmentType, (unsigned int)emptyEntry.type, EMPTY_PTR, EMPTY_LIST );
               printMemSubBlock( &workBlock, emptyPtr.sbid );
           }
           if( isDebug( DEBUG_2 )) { 
               cout<<"DBG: fbo "<<emptyEntry.fbo<<" sbid "<<(unsigned int)emptyEntry.sbid<<" entry "<< (unsigned int)emptyEntry.entry<<" chain ENTRY_"<<(1<<segmentType)<<"in"<<((freemgr_type==LIST)?"LIST":"TREE")<<"\n";
           }

           return ERR_FM_BAD_TYPE;
       }
       
       if ((emptyEntry.fbo == 0) && (emptyEntry.type == EMPTY_PTR))
       {
           if ( isDebug( DEBUG_0 )) {printf("DBG: Bad entry in %i list - found EMPTY_PTR but indicates block 0\n",  1<<segmentType );}
           return ERR_FM_BAD_TYPE;
       }

       if ((emptyEntry.fbo == 0) && (emptyEntry.type == EMPTY_LIST)) 
       {
           /**
            * if at the end of the rainbow, in a bucket with no entries, fill the bucket
            * Allocate a sub block and split it into parts
           **/
           
           // cannot assign more space to block list from a smaller list.. need to extend the file
           if ( segmentType == ENTRY_BLK ) {
               if ( isDebug( DEBUG_1 )){ printf("\nNeed to extend block\n");}
               if ( isDebug( DEBUG_1 )){ printf("Out of space in BLOCK list\n"); }
               rc = extendFreespace( cb, blockZero, freemgr_type );
               if (rc != NO_ERROR){ 
                   if (isDebug( DEBUG_3 )){ printf("DBG: Error extending file\n"); }
                   return rc; 
               }
               getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
               rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
               getSubBlockEntry( &workBlock, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry );

           } else {
           
           rc = assignSubblock( cb, blockZero, freemgr_type, &newSb);
           if (rc != NO_ERROR){ 
//                printf("DBG: Error extending chain\n"); 
               return rc; 
           }
           if (newSb.entry != 0){
               printf("WTF: Entry should be 0 after assign from sb list, instead is %i", (unsigned int)newSb.entry);
               return ERR_FM_ASSIGN_ERR;
           }
           if (isDebug(DEBUG_2)){
               cout<<"DBG: added fbo "<<newSb.fbo<<" sbid "<<(unsigned int)newSb.sbid<<" entry "<<(unsigned) newSb.entry<<" to "<<segmentType<<" list - need to split "<<(1<<(ENTRY_32-segmentType))<<"times\n";
           }
           
           newSb.entry = 0;
           newSb.group = segmentType;
           newSb.type = EMPTY_PTR;
           emptyEntry = newSb;

           int idx, inc;
           inc = 1<<segmentType;
           for (idx=0; idx < ENTRY_PER_SUBBLOCK - inc; idx+= inc){
               if( isDebug( DEBUG_3 )) { printf ("DBG: split..%i-%i\n", idx, idx+inc-1 );}
               newSb.entry = idx;
               releaseSegment( cb, blockZero, freemgr_type, segmentType,  &newSb );
           }
           emptyEntry.entry = idx;
           if( isDebug( DEBUG_3 )) 
           { 
               printf ("DBG: split and return..%i-%i\n", idx, idx+inc-1);
           }
           if( isDebug( DEBUG_2 )) 
           { 
             cout<<"DBG: Assigned fbo "<< emptyEntry.fbo<<" sbid "<<(unsigned int)emptyEntry.sbid<<" entry "<<(unsigned int)emptyEntry.entry<<" to chain ENTRY_"<<(1<<segmentType)<<"\n";
           }
           memcpy(assignPtr, &emptyEntry, 8);
           
           uint64_t count;
           getSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
           count--;
           setSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );

           return NO_ERROR;
       }
       }
       /**
        * got here because we didn't need to populate a chain and did not fall into any traps
        * so either we are at the end of bucket or we have a valid entry
        **/
       if (emptyEntry.type == EMPTY_LIST) // reached end of this segment (should release it for re-use)
           {
               /**
                * release bucket
                **/
               if( isDebug( DEBUG_2 )) { 
                   cout<<"DBG: Need to release sb fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" from chain ENTRY_"<<(1<<segmentType)<<" in "<<((freemgr_type==LIST)?"LIST":"TREE")<<"\n";
               }
               // when we stored the ptr in the empty map, we tweaked group, must change it back
               emptyPtr.type = EMPTY_PTR;
               emptyPtr.group = ENTRY_32;
               rc = releaseSubblock( cb, blockZero, freemgr_type, &emptyPtr );
               if (rc != NO_ERROR)
               { 
                   printf("Error releasing sb\n");
                   return rc; 
               }
               emptyPtr = emptyEntry;
               rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );

               if (rc != NO_ERROR){ 
                   printf("DBG: Error following chain\n");
                   return rc; 
               }
               getSubBlockEntry( &workBlock, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry );

           } 

       if (emptyEntry.type == EMPTY_PTR)
       {
       
           emptyPtr.entry--; 
           blockZero->dirty = 1;
           setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
           
           if( isDebug( DEBUG_3 )) { 
               printf("DBG: Empty entry is now %u\n",(unsigned int)emptyPtr.entry);
           }
           if( isDebug( DEBUG_2 )) 
           { 
               cout<<"DBG: Assigned fbo "<<emptyEntry.fbo<<" sbid "<<emptyEntry.sbid<<" entry "<<emptyEntry.entry<<" from chain ENTRY_"<<(1<<segmentType)<<"\n";
               cout<<"DBG: Empty map ENTRY_"<<(1<<segmentType)<<" now fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
           }
           memcpy(assignPtr, &emptyEntry, 8);
         
           // -- workblock may have changed on disk since it was read.. making the writeDBfile dangerous without a readDBfile first
          nullPtr(&emptyMap); // zero out the entry
          readDBFile( cb, &workBlock, emptyPtr.fbo );
          setSubBlockEntry( workBlock.data, emptyPtr.sbid, emptyPtr.entry+1, 8, &emptyMap );
          rc = writeDBFile( cb, &workBlock, emptyPtr.fbo );
           // --
           
           uint64_t count;
           getSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
           count--;
           setSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );

           return NO_ERROR;
       }
       
       return ERR_FM_ASSIGN_ERR;
   }

   /**
    * DESCRIPTION:
    *    Assign sub-block - private method used to manage sub-blocks in list
    * PARAMETERS:
    *   indexFile - file pointer 
    *   blockZero - in memory copy of blockZero
    *   freemgr_type - either TREE or LIST
    *   assignPtr - the assigned ptr
    *
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    **/

   const int FreeMgr::assignSubblock( CommBlock &cb, DataBlock* blockZero, const int freemgrType,  IdxEmptyListEntry* assignPtr ) 
   {
       int          rc;    // return code from file ops
       DataBlock    workBlock, tempBlock;
       int          listOffset; // entry in block zero of head pointer
       IdxEmptyListEntry emptyEntry;
       IdxEmptyListEntry emptyPtr, emptyMap;
       IdxEmptyListEntry newBlock;
       uint64_t      numBlocks;
       FILE* indexFile;
               
       indexFile = cb.file.pFile;

       /**
        * Separated subblock assignment out from general segment assignment
        * Reduces the hoops to jump through
        **/
       
       numBlocks = getFileSize( indexFile )/BYTE_PER_BLOCK ;
       if( isDebug( DEBUG_3 )) { printf("DBG: Assign subblock \n"); }
       
       //find the start of the chain
       listOffset = calcPtrOffset( ENTRY_32 );
       
       getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
       if( isDebug( DEBUG_2 ))
       {
           cout<<"DBG: EM (start assign) sb fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
       }

       if (emptyPtr.type != EMPTY_LIST ){
           return ERR_FM_BAD_TYPE;
       }   
//        if (emptyPtr.fbo > numBlocks)
//        {
//            if( isDebug( DEBUG_1 )) { printf("DBG: Weirdness in assignSubblock.. emptyPtr.fbo > numBlocks\n"); }
//            return ERR_FM_BAD_FBO;
//        }

       // follow the chain to the empty entry
       rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
       if (rc != NO_ERROR)
       { 
//            printf("DBG: RC Weirdness: rc is %i", rc); 
           return rc; 
       }
           
       getSubBlockEntry( &workBlock, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry );
       if( isDebug( DEBUG_2 )) 
       { 
           cout<<"DBG: Next avail sb fbo "<<emptyEntry.fbo<<" sbid "<<" entry "<<emptyEntry.entry<<"\n";
       }

           if (emptyEntry.fbo == 0) // then nowhere to go.. exit
           {
               //if( isDebug( DEBUG_1 )) { printf("DBG: No space in subblock list\n"); }
               if( isDebug( DEBUG_2 )) { 
                   cout<<"DBG: fbo "<<emptyEntry.fbo<<" sbid "<<emptyEntry.sbid<<" entry "<<emptyEntry.entry<<"\n";
               }
               
               //-- try and assign from BLOCK list 
//                printf("Go ask for a block\n");
               rc = assignSegment( cb, blockZero, freemgrType, ENTRY_BLK, &newBlock);

//                rc = extendFreespace( indexFile, blockZero, freemgrType );

               if (rc != NO_ERROR){ 
                   if( isDebug( DEBUG_1 )) { printf("DBG: Could not get block from block list\n"); }
                   return rc;
               }
               // got a block so now split it
               newBlock.entry = 0;
               newBlock.group = ENTRY_32;
               newBlock.type = EMPTY_PTR;
               emptyEntry = newBlock;

               //-- assign almost all sub blocks - keep last one
               int sbIdx;
               for (sbIdx=BYTE_PER_BLOCK/BYTE_PER_SUBBLOCK-1; sbIdx >0; sbIdx--)  
               {
                   if( isDebug( DEBUG_3 )) { cout<<"DBG: Working on fbo "<<newBlock.fbo<<" sbid "<<sbIdx<<"\n"; }
                   emptyEntry.sbid = sbIdx;
                   emptyEntry.type = EMPTY_PTR;
                   rc = releaseSubblock( cb, blockZero, freemgrType, &emptyEntry );
                   if (rc != NO_ERROR)
                   { 
                       printf("DBG: Error releasing sb\n"); 
                       return rc; 
                   }
               }
               emptyEntry.sbid=0;
           }

           if ((emptyEntry.type != EMPTY_LIST) && (emptyEntry.type != EMPTY_PTR))
           {
               if( isDebug( DEBUG_0 )) 
               { 
                   printf("WTF: Bad entry in in subblock list- type is %i (expected %i or %i)\n",  (unsigned int)emptyEntry.type, EMPTY_PTR, EMPTY_LIST); 
               }
               if( isDebug( DEBUG_2 )) 
               {
                   cout<<"DBG: fbo "<<emptyEntry.fbo<<" sbid "<<emptyEntry.sbid<<" entry "<<emptyEntry.entry<<"\n"; 
               }
               return ERR_FM_BAD_TYPE;
           }
           
           if (emptyEntry.type == EMPTY_PTR)
           {
               // this is what we expect normally
               emptyPtr.entry--; // only decrement if we didn't just drain a bucket 
               setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
               memcpy(assignPtr, &emptyEntry, 8);

           } else if (emptyEntry.type == EMPTY_LIST && emptyPtr.entry == 0 )
           {
               if (emptyPtr.entry >0) printf("\nWTF!! %i\n", (unsigned int) emptyPtr.entry);
                // reached end of this bucket (should release it for re-use)
               // this is not the typical case..
               if( isDebug( DEBUG_3 )) { 
                   cout<<"DBG: Drained bucket sb fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
               }
               //blank the llp
               rc = readDBFile( cb, tempBlock.data, emptyPtr.fbo );
               if (rc != NO_ERROR) { 
                   if (isDebug( DEBUG_1 )){ cout<<"DBG: File error during releaseSubblock, fbo/lbid: "<<emptyPtr.fbo<<"\n"; }
                   return rc; 
               }
               
               nullPtr(&emptyMap); // zero out the entry

               setSubBlockEntry( tempBlock.data, emptyPtr.sbid, 0, 8, &emptyMap );
               rc = writeDBFile( cb, &tempBlock, emptyPtr.fbo );
               if (rc != NO_ERROR){ return rc; }

               memcpy(assignPtr, &emptyPtr, 8);
               assignPtr->type = EMPTY_PTR;
               
               if( isDebug( DEBUG_2 )) { 
                cout<<"DBG: Change head pointer to fbo "<<emptyEntry.fbo<<" sbid "<<emptyEntry.sbid<<" entry "<<emptyEntry.entry<<"\n";
               }
               setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyEntry );
           } else 
           {
               printf("DBG: Weirdness - not list and not ptr\n"); 
           }
//            printf("DBG: Assigned sb fbo %llu sbid %u entry %u\n", assignPtr->fbo, (unsigned int)assignPtr->sbid, (unsigned int)assignPtr->entry);
           
           getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyMap );
           if(isDebug(DEBUG_3)){ 
               cout<<"DBG: EM (sb assign 1) sb fbo "<<emptyMap.fbo<<" sbid "<<emptyMap.sbid<<" entry "<<emptyMap.entry<<"\n";
           }

           blockZero->dirty = 1;
           
           uint64_t count;
           getSubBlockEntry( blockZero, 0, calcStatOffset(ENTRY_32) , 8, &count );
           count--;
           setSubBlockEntry( blockZero, 0, calcStatOffset(ENTRY_32) , 8, &count );

           
           // -- workblock may have changed on disk since it was read.. making the writeDBfile dangerous without a readDBfile first
           nullPtr(&emptyMap); // zero out the entry
           readDBFile( cb, &workBlock, emptyPtr.fbo );
           setSubBlockEntry( workBlock.data, emptyPtr.sbid, emptyPtr.entry+1, 8, &emptyMap );
           rc = writeDBFile( cb, &workBlock, emptyPtr.fbo );
           // --
//            if( isDebug( DEBUG_3 )) { printf("DBG: Assign subblock -- all done\n"); }

           return NO_ERROR;

   }


   /**
    * DESCRIPTION:
    *    Release a segment back to the list
    * PARAMETERS:
    *   indexFile - file pointer 
    *   blockZero - in memory copy of blockZero
    *   freemgr_type - either TREE or LIST
    *   segmentType - group type
    *   assignPtr - the assigned ptr
    *
    * RETURN:
    *    NO_ERROR if success
    *    error no if fail
    **/

   
   const int FreeMgr::releaseSegment( CommBlock &cb, DataBlock* blockZero, const int freemgr_type, const IdxTreeGroupType segmentType,  IdxEmptyListEntry* assignPtr )
   {
       int         rc;    // return code from file ops
       DataBlock   workBlock;
       DataBlock   extraBlock;
       int          listOffset; // entry in block zero of head pointer
       IdxEmptyListEntry emptyPtr;
       IdxEmptyListEntry newSb; 
       uint64_t     numBlocks;
       FILE*       indexFile;
               
       indexFile = cb.file.pFile;

       if (!assignPtr){
//            printf ("DBG: Bad pointer: assignPtr is zero\n");
           return ERR_INVALID_PARAM;
       }

       if (!blockZero){
//            printf ("DBG: Bad pointer: blockZero is zero\n");
           return ERR_INVALID_PARAM;
       }
       if( isDebug( DEBUG_3 )) { printf("DBG: release ENTRY_%i segment \n", 1<<(unsigned int)segmentType); }
       if( isDebug( DEBUG_2 )) 
       { 
           cout<<"DBG: releasing fbo "<<assignPtr->fbo<<" sbid "<<assignPtr->sbid<<" entry "<<assignPtr->entry<<" from "<<((freemgr_type==LIST)?"LIST":"TREE")<<" chain ENTRY_"<<(1<<segmentType)<<" (type is "<<((assignPtr->type==EMPTY_PTR)?"EMPTY_PTR":"Not EMPTY_PTR")<<")\n";
       }
       
       numBlocks = getFileSize( indexFile )/BYTE_PER_BLOCK ;

       /*       if (assignPtr->fbo > numBlocks)
       {
           if( isDebug( DEBUG_1 )) { printf("DBG: Weirdness in releaseSegment.. assignPtr.fbo > numBlocks (%llu %llu)\n", assignPtr->fbo, numBlocks );};
           return ERR_FM_BAD_FBO;
       }*/
       
       if (assignPtr->type != EMPTY_PTR)
       {
//            printf("DBG: Weirdness in releaseSegment.. tried to return a pointer with type %i (expected %i)\n", (unsigned int)assignPtr->type, EMPTY_PTR );
           assignPtr->type = EMPTY_PTR;
           //            return ERR_FM_BAD_TYPE; // do not exit
       }
       if ( assignPtr->group != (uint64_t)segmentType )
       {
//            printf("DBG: Weirdness in releaseSegment.. tried to return a pointer from group %i to group %i\n", (unsigned int)assignPtr->group, segmentType );
           return ERR_FM_RELEASE_ERR;
       }
       
       //find the start of the chain
       if (segmentType == ENTRY_32)
       {
           rc = releaseSubblock( cb, blockZero, freemgr_type, assignPtr);
           if (rc != NO_ERROR)
           { 
//                    printf("DBG: Error releasing sb\n");
           }
           return rc;
       }
               
       listOffset = calcPtrOffset( segmentType );
       if (freemgr_type==LIST && !(segmentType== ENTRY_4 || segmentType== ENTRY_BLK))
       {
           printf("DBG: Assign segment size illegal %i\n", (unsigned int)segmentType);
           return ERR_INVALID_PARAM; // should not have got here so quit with error
       }

       getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );

/*       if (emptyPtr.fbo > numBlocks)
       {
       if( isDebug( DEBUG_1 )) { printf("DBG: Weirdness in releaseSegment.. emptyPtr.fbo > numBlocks (%llu %llu)\n", emptyPtr.fbo, numBlocks );}
           return ERR_FM_BAD_FBO;
       }*/
       
       //sub block is full or chain never started
       if (emptyPtr.entry == ENTRY_PER_SUBBLOCK-1 || emptyPtr.fbo==0)
       { 
           if( isDebug( DEBUG_3 ))
           { 
               printf("DBG: No room in chain %i - need to add a sub-block\n", (unsigned int)segmentType);
           }
           if( isDebug( DEBUG_2 )) 
           { 
               cout<<"DBG: Empty ptr fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
           }
            //ask for a new sb to extend chain
               rc = assignSubblock( cb, blockZero, freemgr_type, &newSb);
               if (rc != NO_ERROR) { printf("DBG: Error extending chain\n");  return rc; }
               if( isDebug( DEBUG_2 )) 
               { 
                   cout<<"DBG: release segment, new SB is fbo "<<newSb.fbo<<" sbid "<<newSb.sbid<<" entry "<<newSb.entry<<"\n";
               }
               rc = readDBFile( cb, extraBlock.data, newSb.fbo );
               if (rc != NO_ERROR)
               { 
                   if (isDebug( DEBUG_1 )){printf("DBG: File error during releaseSegment (3)\n");}
                   return rc; 
               }
               
               emptyPtr.type = EMPTY_LIST; // writing into the LLP field so set type accordingly
               setSubBlockEntry( extraBlock.data, newSb.sbid, 0, 8, &emptyPtr );
               setSubBlockEntry( extraBlock.data, newSb.sbid, 1, 8, assignPtr );
               rc = writeDBFile( cb, &extraBlock, newSb.fbo );
               if (rc != NO_ERROR)
               { 
                   if (isDebug( DEBUG_1 )){printf("DBG: File error during releaseSegment (4)\n");}
                   return rc; 
               }
               
               newSb.entry = 1;
               newSb.type = EMPTY_LIST;
               newSb.group = segmentType;

               setSubBlockEntry( blockZero, 0, listOffset, 8, &newSb );
               blockZero->dirty = 1;

               uint64_t count;
               getSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
               count++;
               setSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
               

               return NO_ERROR ;
            
       }
       else 
       { // 
           emptyPtr.entry++;
           rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
           if (rc != NO_ERROR){ 
               if (isDebug( DEBUG_1 )){ printf("DBG: File error during releaseSegment\n"); }
               return rc; 
           }

           if( isDebug( DEBUG_2 )) { 
               cout<<"DBG: Empty map ENTRY_"<<( 1<<segmentType)<<" is fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
           }
           setSubBlockEntry( workBlock.data, emptyPtr.sbid, emptyPtr.entry, 8, assignPtr );
           rc = writeDBFile( cb, workBlock.data, emptyPtr.fbo );
           if (rc != NO_ERROR){ return rc; }
           
           emptyPtr.type = EMPTY_LIST;
           emptyPtr.group = segmentType;

           setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
           blockZero->dirty = 1;//sub block is full or chain never started

       }

       uint64_t count;
       getSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
       count++;
       setSubBlockEntry( blockZero, 0, calcStatOffset(segmentType) , 8, &count );
               

       return NO_ERROR;
   }


   const int FreeMgr::releaseSubblock( CommBlock &cb, DataBlock* blockZero, const int freemgr_type,  IdxEmptyListEntry* assignPtr ) 
   {
       int         rc;    // return code from file ops
       DataBlock   workBlock;
       DataBlock   extraBlock;
       int          listOffset; // entry in block zero of head pointer
       IdxEmptyListEntry emptyPtr, emptyMap ;
       uint64_t     numBlocks;
       FILE*       indexFile;
               
       indexFile = cb.file.pFile;
       /**
        * Release sub-block - handle de-allocation of only sub-blocks
        * This makes the assign/release code for smaller segments simpler and 
        * separates the tasks of handling the list containers and the list contents
        * When called, we look at the bucket indicated as head of chain and if it full
        * or not present (no room left in chain) then insert the returned SB as a bucket and 
        * move the head pointer
        **/
       
       
       //if( isDebug( DEBUG_1 )) { printf("DBG: releaseSubblock\n"); }
       if( isDebug( DEBUG_2 )) { 
           cout<<"DBG: releasing sb fbo "<<assignPtr->fbo<<" sbid "<<assignPtr->sbid<<" entry "<<assignPtr->entry<<" from "<<((freemgr_type==LIST)?"LIST":"TREE")<<" (type is "<<((assignPtr->type==EMPTY_PTR)?"EMPTY_PTR":"Not EMPTY_PTR")<<")\n";
       }
       if (!assignPtr){
//            printf ("DBG: Bad pointer: assignPtr is zero\n");
           return ERR_INVALID_PARAM;
       }

       if (!blockZero){
           printf ("DBG: Bad pointer: pointer for blockZero is zero\n");
           return ERR_INVALID_PARAM;
       }

       numBlocks = getFileSize( indexFile )/BYTE_PER_BLOCK ;
/*       if (assignPtr->fbo > numBlocks)
       {
       if( isDebug( DEBUG_1 )) { printf("DBG: Weirdness in releaseSubblock.. assignPtr.fbo > numBlocks (%llu %llu)\n", assignPtr->fbo, numBlocks );}
           return ERR_FM_BAD_FBO;
       }*/
       if (assignPtr->type != EMPTY_PTR)
       {
           printf("DBG: Weirdness in releaseSubblock.. tried to return a pointer with type %i (expected %i)\n", (unsigned int)assignPtr->type, EMPTY_PTR );
           return ERR_FM_BAD_TYPE;
       }
       if ( assignPtr->group != ENTRY_32 )
       {
           printf("DBG: Weirdness in releaseSubblock.. tried to return a pointer from group %i to subblock group\n", (unsigned int)assignPtr->group );
           return ERR_INVALID_PARAM;
       }           
       //find the start of the chain
       listOffset = calcPtrOffset( ENTRY_32 );

       getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
       if( isDebug( DEBUG_2 )) { 
          cout<<"DBG: EM (sb release 1) sb fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
       }

       //sub block is full or chain empty
       if (emptyPtr.entry == ENTRY_PER_SUBBLOCK-1 || emptyPtr.fbo==0)
       { 
       // change type from EMPTY_PTR to EMPTY_LIST 
           assignPtr->type = EMPTY_LIST;
           
           //if( isDebug( DEBUG_1 )) { printf("DBG: No room in subblock chain - need to add a sub-block\n");  }
           if( isDebug( DEBUG_2 )) { 
                   cout<<"DBG: Change head pointer to fbo "<<assignPtr->fbo<<" sbid "<<assignPtr->sbid<<" entry "<<assignPtr->entry<<"\n";
           }
           
           // change head pointer to released segment
           setSubBlockEntry( blockZero, 0, listOffset, 8, assignPtr );
           blockZero->dirty = 1;
           
           // read in released segment to set llp of new block to point to current head of chain
           rc = readDBFile( cb, extraBlock.data, assignPtr->fbo );
           if (rc != NO_ERROR){ 
               if (isDebug( DEBUG_1 )){
                   cout<<"DBG: File error during releaseSegment (2), rc: "<<rc<<" fbo/lbid: "<<assignPtr->fbo<<"\n"; 
               }
               return rc;
           }
           
           if( isDebug( DEBUG_2 )) { 
               cout<<"DBG: Set LLP for fbo "<<assignPtr->fbo<<" sbid "<<assignPtr->sbid<<" entry "<<assignPtr->entry<<"to fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
           }
           if( isDebug( DEBUG_3)){
               printf("Before\n");
               printMemSubBlock( &extraBlock, assignPtr->sbid );
           }
           
           emptyPtr.type = EMPTY_LIST;
           //memset( extraBlock.data, 0, BYTE_PER_SUBBLOCK);
           setSubBlockEntry( extraBlock.data, assignPtr->sbid, 0, 8, &emptyPtr );
           rc = writeDBFile( cb, &extraBlock, assignPtr->fbo );
           if (rc != NO_ERROR){ return rc; }
           
           if( isDebug( DEBUG_2 )) { 
               getSubBlockEntry( blockZero, 0, listOffset, 8, &emptyMap );
               cout<<"DBG: EM (sb release 2) sb fbo "<<emptyMap.fbo<<" sbid "<<emptyMap.sbid<<" entry "<<emptyMap.entry<<"\n";
           }
           if( isDebug( DEBUG_3)){
               printf("After\n");
               printMemSubBlock( &extraBlock, assignPtr->sbid );
           }
           
       }
       else 
       { // 
           emptyPtr.entry++;
           rc = readDBFile( cb, workBlock.data, emptyPtr.fbo );
           if (rc != NO_ERROR) { 
               if (isDebug( DEBUG_1 ))
               {
                   printf("DBG: File error during releaseSubblock\n"); 
               }
               return rc;
           }
           
           if( isDebug( DEBUG_3)){
               printf("Before\n");
               printMemSubBlock( &workBlock, emptyPtr.sbid );
           }

           setSubBlockEntry( workBlock.data, emptyPtr.sbid, emptyPtr.entry, 8, assignPtr );
           rc = writeDBFile( cb, &workBlock, emptyPtr.fbo );
           if (rc != NO_ERROR){ return rc; }

           if( isDebug( DEBUG_2 )) { 
               cout<<"DBG: setting emptyPtr sb fbo "<<emptyPtr.fbo<<" sbid "<<emptyPtr.sbid<<" entry "<<emptyPtr.entry<<"\n";
           }
           if( isDebug( DEBUG_3)){
               printf("After\n");
               printMemSubBlock( &workBlock, emptyPtr.sbid );
           }
           
           emptyPtr.type = EMPTY_LIST;
           emptyPtr.group = ENTRY_32;

           setSubBlockEntry( blockZero, 0, listOffset, 8, &emptyPtr );
           blockZero->dirty = 1;

       }
       uint64_t count;
       
       getSubBlockEntry( blockZero, 0, calcStatOffset(ENTRY_32) , 8, &count );
       count++;
       setSubBlockEntry( blockZero, 0, calcStatOffset(ENTRY_32) , 8, &count );
               

       return NO_ERROR;
   }

   const void FreeMgr::nullPtr(WriteEngine::IdxEmptyListEntry* assignPtr ) const
   { 
       
       
       assignPtr->fbo = 0; 
       assignPtr->sbid = 0; 
       assignPtr->entry = 0; 
       assignPtr->type = 0; 
       assignPtr->group = 0; 
       assignPtr->spare = 0;
       assignPtr->spare2 = 0;
   }

   /**
    * extendFreeSpace - Having reached the end of the available freespace or exhausted the block/subblock chain
    * add extra space by extending the file and calling the Init function for the new space
    * 
    * @param indexFile 
    * @param blockZero 
    * @param freemgrType 
    * @return 
    */
   const int FreeMgr::extendFreespace( CommBlock &cb, DataBlock* blockZero, int freemgrType )
   {
       DataBlock   workBlock;
       int rc = NO_ERROR; // result code from file ops
       FILE* indexFile;
       int allocSize = 0;
        
       indexFile = cb.file.pFile;
       if( isDebug( DEBUG_1 )){ printf ("Extending File\n");}
       int numBlocks = 1024; // default number - should ask BRM for extentsize
       int currSize = getFileSize( indexFile )/BYTE_PER_BLOCK ;

       if (!allowExtend){ 
           if( isDebug( DEBUG_1 )){ printf("DBG: Extension denied\n");}
           return ERR_FM_EXTEND;
       }
       if( isDebug( DEBUG_1 )){ printf("DBG: Extending free space in file\n"); }
       if( isDebug( DEBUG_1 )){ printf("DBG: File is currently: %i blocks   Asking for: %i blocks\n", currSize, numBlocks); }
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
       rc = extendFile( indexFile, cb.file.oid, numBlocks, allocSize, 0, 8, false );
#endif
       if (rc != NO_ERROR) { return rc; }
       
       if( isDebug( DEBUG_1 )){ printf("DBG: Extended file by %i blocks\n", allocSize); }

       
       rc = init( cb, blockZero, freemgrType, ENTRY_32, currSize, 50 );
       if ( rc != NO_ERROR ) { return rc; }
       rc = init( cb, blockZero, freemgrType, ENTRY_BLK, currSize +50, allocSize-50 );
       if ( rc != NO_ERROR ) { return rc; }

       
       return NO_ERROR;
   }
   
   /**
    * Quick utility function to map LBID if BRM in use, return original FBO otherwise
    * @param cb 
    * @param fbo 
    * @return 
    */
   const uint64_t FreeMgr::mapLBID( CommBlock &cb, const uint64_t fbo, int &rc )
   {
       uint64_t lbid = 0;

       rc = NO_ERROR;
#ifdef BROKEN_BY_MULTIPLE_FILES_PER_OID
       rc = BRMWrapper::getInstance()->getBrmInfo( cb.file.oid, fbo, lbid );
#endif
       if (rc != NO_ERROR)
       {
          if (isDebug( DEBUG_0 ))
          {
             cout<<"BRM Failure looking up lbid for oid: "<<cb.file.oid<<" fbo: "<<fbo<<", returning unchanged value\n";
          }
          return fbo;
       } else {
          return lbid;
       }
   }
   
   
   const void FreeMgr::printMemSubBlock( DataBlock* curBlock, const int sbid )
   {
       int off;
//      DataBlock         curBlock;
       unsigned char*    curPos;
       IdxEmptyListEntry   curEntry, testZero;

       nullPtr( &testZero );
//      readDBFile( m_pTreeFile, &curBlock, fbo );
       curPos = curBlock->data + BYTE_PER_SUBBLOCK * sbid;
       printf( "\n======================== sbid: %i", sbid );
       for( int i = 0; i < ENTRY_PER_SUBBLOCK; i++ ) {
           memcpy( &curEntry, curPos, MAX_COLUMN_BOUNDARY );
           off = memcmp( &testZero, &curEntry, MAX_COLUMN_BOUNDARY );
//         if( /*bNoZero &&*/ off == 0 )
//            continue;
           printf( "\n Entry %2d : ", i );
           for( int j = 0; j < MAX_COLUMN_BOUNDARY; j++ )
               printf( " %2X", *(curPos + j) );

           cout<<" fbo="<<curEntry.fbo<<" sbid="<<curEntry.sbid<<" entry="<<curEntry.entry<<" group="<<curEntry.group<<" type="<<curEntry.type;

           curPos += MAX_COLUMN_BOUNDARY;
       }
       printf( "\n" );
   }

} //end of namespace

