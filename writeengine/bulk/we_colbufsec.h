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


/******************************************************************************
 * $Id: we_colbufsec.h 4450 2013-01-21 14:13:24Z rdempsey $
 *
 *****************************************************************************/

/** @file
 * class ColumnBufferSection
 */
#ifndef WRITEENGINE_COLBUFSEC_H
#define WRITEENGINE_COLBUFSEC_H

#include "we_type.h"
#include "we_colbuf.h"

namespace WriteEngine {

/* @brief Status codes for the ColumnBufferSection
 */
enum {
    INIT_COMPLETE,
    WRITE_COMPLETE,
};

/**
 * @brief ColumnBufferSection class represent a section of the ColumnBuffer and
 * provides functionality for writing to the ColumnBuffer.
 */
class ColumnBufferSection {

  public:
    /* @brief Constructor
     *
     * @param cb Ptr to this section's corresponding CoulmnBuffer
     * @param startRowId Starting row-id of the column
     * @param endRowId Ending row-id of the column
     * @param Width Width of the underlying column
     * @param startOffset 
     */
    ColumnBufferSection(ColumnBuffer* const cb, RID startRowId,
        RID endRowId, int colWidth, int startOffset);

    /* @brief Default destructor
     */
    ~ColumnBufferSection();

    /* @brief Updates the status of the section
     *
     * @param Status value
     */
    void setStatus(int status);

    /* @brief Write data into the column buffer section
     *
     * @param data pointer to the data
     * @param nRows Number of rows to be written, starting from data pointer
     */
    void write(const void * const data, int nRows);

    /* @brief Returns the current status of the section
     */
    int getStatus() const;

    /* @brief Returns the start offset of this section
     */
    int getStartOffset() const;

    /* @brief Returns the size of this section
     */
    int getSectionSize() const;

    /* @brief Returns the ending row-id of this section
     */
    RID endRowId()   const { return fEndRowId;   }

    /* @brief Returns the starting row-id of this section
     */
    RID startRowId() const { return fStartRowId; }

  private:

    const ColumnBuffer* const fCBuf; //ColumnBuffer associated with this section
    RID fStartRowId;        // Starting row-id for this section
    RID fEndRowId;          // Ending row-id for this section
    int fColWidth;          // Width of column (in bytes)
    int fBufStartOffset;    // Starting offset of this section in ColumnBuffer
    RID fCurrRowId;         // row-id which will be written next
    int fStatus;            // Status of this section
};

}

#endif /*WRITEENGINE_COLBUFSEC_H*/

