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
* $Id: we_columninfocompressed.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/

/** @file
 *  Contains main class used to manage compressed column information.
 */

#ifndef _WE_COLUMNINFOCOMPRESSED_H
#define _WE_COLUMNINFOCOMPRESSED_H

#include "we_columninfo.h"
#include "we_fileop.h"

#include "idbcompress.h"

#include <vector>

namespace WriteEngine
{
class RBMetaWriter;

/** @brief Maintains information about a DB column.
 */
class ColumnInfoCompressed : public ColumnInfo
{
  public:

    /** @brief Constructor.
     */
    ColumnInfoCompressed(Log*   logger,
               int              id,
               const JobColumn& column,
               DBRootExtentTracker* pDBRootExtTrk,
               TableInfo*		pTableInfo);
               //RBMetaWriter*    rbMetaWriter);

    /** @brief Destructor
     */
    virtual ~ColumnInfoCompressed();

    /** @brief Close the current Column file.
     *  @param bCompletedExtent are we completing an extent
     *  @param bAbort indicates if job is aborting and file should be
     *  closed without doing extra work: flushing buffer, etc.
     */
    virtual int closeColumnFile(bool bCompletingExtent, bool bAbort);

    /** @brief Truncate specified dictionary file.  Only applies if compressed.
     * @param dctnryOid Dictionary store OID
     * @param root DBRoot of relevant dictionary store segment file.
     * @param pNum Partition number of relevant dictionary store segment file.
     * @param sNum Segment number of relevant dictionary store segment file.
     */
    virtual int truncateDctnryStore(OID dctnryOid,
        uint16_t root, uint32_t pNum, uint16_t sNum) const;

  private:

    virtual int resetFileOffsetsNewExtent(const char* hdr);

    // Prepare initial compressed column seg file for importing of data.
    // oldHWM - Current HWM prior to initial block skipping.  This is only
    //     used for abbreviated extents, to detect when block skipping has
    //     caused us to require a full expanded extent.
    // newHWM - Starting point for adding data after initial blockskipping
    virtual int setupInitialColumnFile( HWM oldHWM, HWM newHWM );

    virtual int saveDctnryStoreHWMChunk(bool& needBackup);
    virtual int extendColumnOldExtent(
        uint16_t dbRootNext,
        uint32_t partitionNext,
        uint16_t segmentNext,
        HWM      hwmNext );

    RBMetaWriter* fRBMetaWriter;
    FileOp        fTruncateDctnryFileOp; // Used to truncate dctnry store file

};

}

#endif
