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
 * $Id: we_brmreporter.h 3615 2012-03-09 16:41:14Z dcathey $
 *
 *****************************************************************************/

#include <fstream>
#include <string>
#include <vector>

#include "brmtypes.h"

/** @file
 * class BRMReporter
 */

#ifndef _WE_BRMREPORTER_H_
#define _WE_BRMREPORTER_H_

namespace WriteEngine {
class Log;

/** @brief Accumulate update info that is later forwarded to BRM.
 *
 * This class accumulates BRM data (HWM and Casual Partition).  At the end
 * of the job, the BRMReporter object sends the information directly to BRM.
 */
class BRMReporter
{
  public:

    /** @brief Constructor
     *  @param logger Object used for logging
     *  @param tableName Name of relevant DB table
     */
    BRMReporter(Log* logger, const std::string& tableName);

    /** @brief Destructor
     */
    ~BRMReporter( );

    /** @brief Add a CPInfoMerge entry to the output list
     *  @param cpEntry Casual Partition information to be added to output list
     */
    void addToCPInfo(const BRM::CPInfoMerge& cpEntry);

    /** @brief Add a BulkSetHWMArg entry to the output list
     *  @param hwmEntry HWM information to be be added to output list
     */
    void addToHWMInfo(const BRM::BulkSetHWMArg& hwmEntry);

    /** @brief Send HWM and Casual Partition Data to BRM
     */
    int sendBRMInfo( );

  private:  

    // Disable copy constructor and assignment operator by declaring and
    // not defining.
    BRMReporter(const BRMReporter&);
    BRMReporter& operator=(const BRMReporter&);

    Log*              fLog;         // Logger
    std::string       fTableName;   // Name of db table we are saving info for
    BRM::CPInfoMergeList_t          fCPInfo;  // Collection of CP info to send
    std::vector<BRM::BulkSetHWMArg> fHWMInfo; // Collection of HWM info to send
};

} // end of namespace

#endif // _WE_BRMREPORTER_H_
