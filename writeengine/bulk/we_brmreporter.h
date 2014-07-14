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
 * $Id: we_brmreporter.h 4731 2013-08-09 22:37:44Z wweeks $
 *
 *****************************************************************************/

#include <fstream>
#include <string>
#include <vector>
#include <utility>
#include "boost/tuple/tuple.hpp"

#include "brmtypes.h"
#include "calpontsystemcatalog.h"
using namespace execplan;
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
 * of the job, the BRMReporter object either saves the data to a report file,
 * or the information is sent directly to BRM.
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

    /** @brief Add a Column FileInfo entry to the output list
     *  @param fileEntry file information to be be added to output list
     */
    void addToFileInfo(const BRM::FileInfo& fileEntry);

    /** @brief Add a Dictionary FileInfo entry to the output list
     *  @param fileEntry file information to be be added to output list
     */
    void addToDctnryFileInfo(const BRM::FileInfo& fileEntry);

    /** @brief Add a ErrMsg entry to the output list
     *  @param Critical Error Message
     */
    void addToErrMsgEntry(const std::string& errCritMsg);

    /* @brief Save Critical error messages to the BRM report file
     * @param rptFileName Name of file to save info, else info is dropped
     */
    void sendErrMsgToFile ( const std::string& rptFileName );

    /** @brief Send HWM and Casual Partition Data to the applicable destination
     *  @param rptFileName Name of file to save info, else info is sent to BRM.
     *  @param errFiles    List of *.err filenames to record in report file
     *  @param badFiles    List of *.bad filenames to record in report file
     */
    int sendBRMInfo( const std::string& rptFileName,
                     const std::vector<std::string>& errFiles,
                     const std::vector<std::string>& badFiles );

    /** @brief Report summary totals
     *  @param totalReadRows Total number of rows read
     *  @param totalInsertedRows Total number of rows inserted
     *  @param satCounts Number of out-of-range values for each column,
     *                   Vector is vector of column oid, count pairs.
     */
    void reportTotals(uint64_t totalReadRows,
                      uint64_t totalInsertedRows,
               const std::vector<boost::tuple<CalpontSystemCatalog::ColDataType,
                                 uint64_t,uint64_t> >& satCounts);

    /** @brief Generate report for job that exceeds error limit
     *  @param rptFileName Name of file to save info, else info is dropped
     *  @param errFiles    List of *.err filenames to record in report file
     *  @param badFiles    List of *.bad filenames to record in report file
     */
    void rptMaxErrJob(const std::string& rptFileName,
                     const std::vector<std::string>& errFiles,
                     const std::vector<std::string>& badFiles );

  private:  

    // Disable copy constructor and assignment operator by declaring and
    // not defining.
    BRMReporter(const BRMReporter&);
    BRMReporter& operator=(const BRMReporter&);

    int  sendHWMandCPToBRM( );      // send HWM and CP updates to BRM
    void sendHWMToFile( );          // save HWM updates to a report file
    void sendCPToFile ( );          // save CP  updates to a report file
    int  openRptFile  ( );          // open BRM Report file
    void closeRptFile ( );          // close BRM Report file

    Log*              fLog;         // Logger
    std::string       fTableName;   // Name of db table we are saving info for
    BRM::CPInfoMergeList_t          fCPInfo;  // Collection of CP info to send
    std::vector<BRM::BulkSetHWMArg> fHWMInfo; // Collection of HWM info to send
    std::vector<std::string>        fCritErrMsgs; //Collection of CRIT ERRs
    std::string       fRptFileName; // Name of BRM report file
    std::ofstream     fRptFile;     // BRM report file that is generated
    std::vector<BRM::FileInfo> fFileInfo;  // Column files to flush from FDcache
    std::vector<BRM::FileInfo> fDctnryFileInfo;//Dct files to flush from FDcache
};

} // end of namespace

#endif // _WE_BRMREPORTER_H_
