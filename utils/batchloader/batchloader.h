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

/*
* $Id: we_dbrootextenttracker.h 3672 2012-03-26 12:31:27Z rdempsey $
*/

/** @file we_dbrootextenttracker.h
 * Contains classes to track the order of placement (rotation) of extents as
 * they are filled in and/or added to the DBRoots for the local PM.
 */

#ifndef BATCHLOADER_H_
#define BATCHLOADER_H_

#include <vector>
#include "oamcache.h"
#include "brmtypes.h"
#include "calpontsystemcatalog.h"

namespace batchloader
{
    
//------------------------------------------------------------------------------
/** @brief Class to find the PM id to send a batch of row
 */
//------------------------------------------------------------------------------
class BatchLoader
{
public:

    /** @brief BatchLoader constructor
     * @param tableOid table OID of interest.
     * @param sessionId cpimport use 0 as session id.
     * @param PMs vector Collection of PM ids.
     */
    BatchLoader ( uint32_t tableOid, execplan::CalpontSystemCatalog::SCN sessionId, std::vector<uint32_t>& PMs);

    /**
     * 	@brief select the Next PM where batch data to be distributed.
     * 	return the PM where next batch to be send.
     * 	if an error occurs, 0 will be returned
     */
    uint32_t selectNextPM();

    /**
	 * 	@brief Move to previous Sequence in the array.
	 * 	This can be used when we find that we cannot use the PM right now
	 * 	and want to use it later. for example the queue is full of the
	 * 	current PM and we want to get the same PM Id when we call selectNextPM()
	 * 	next time also.
	 */
    void reverseSequence();

	/*
     * @brief After calling selectFirstPM(), if we need to keep continuing to
     * the next PM in the list, we need to call this. If we just want to start
     * distributing from dbroot 1 onwards, no need to call this function.
     */
    void prepareForSecondPM();
	
	struct RootExtentsBlocks
    {
        /** @brief the dbroot
         */
        uint32_t DBRoot;
        /** @brief the number of extents
         */
        uint64_t numExtents;
		 /** @brief the number of blocks in the last partition
         */
		uint64_t numBlocks;
    };
	
	struct PMRootInfo
	{
		/** @brief the module id
         */
        uint32_t PMId;
		/** @brief the dbroot info
		*/
		std::vector<RootExtentsBlocks> rootInfo;
	};

private:
    /** @brief Select the first PM to send the first batch of rows.
     * @param startFromNextPM - if true, don't use the PMId. Instead use the next one in the sorted dmList.
     * @param PMId - The PM id to send the first batch of rows if startFromNextPM is false.
     * @return Returns 0 if success, else returns error code.
     */
    void selectFirstPM ( uint32_t& PMId);

   /** @brief build the batch distribution sequence in a vector
     * return void
     */
    void buildBatchDistSeqVector();

    /** @brief build the batch distribution sequence in a vector
      * return void
      */
    void buildBatchDistSeqVector(uint32_t StartPm);

    typedef std::vector<uint32_t> BlIntVec;
	BlIntVec fPMs;
	BlIntVec fDbRoots;
	BlIntVec fPmDistSeq;
	uint32_t fNextIdx;
	uint32_t fFirstPm;
	execplan::CalpontSystemCatalog::SCN fSessionId;
	uint32_t fTableOid;
	oam::OamCache::PMDbrootsMap_t fPmDbrootMap;
	oam::OamCache::dbRootPMMap_t fDbrootPMmap;
};

} //end of namespace

#undef EXPORT

#endif // WE_DBROOTEXTENTTRACKER_H_
