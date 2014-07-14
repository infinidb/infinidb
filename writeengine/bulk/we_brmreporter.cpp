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

/*****************************************************************************
 * $Id: we_brmreporter.cpp 4731 2013-08-09 22:37:44Z wweeks $
 *
 ****************************************************************************/

/** @file
 * Implementation of the BRMReporter class
 */

#include "we_brmreporter.h"

#include <cerrno>
#include <set>

#include "we_brm.h"
#include "we_convertor.h"
#include "we_log.h"
#include "cacheutils.h"
#include "IDBPolicy.h"

namespace WriteEngine {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
BRMReporter::BRMReporter(Log* logger, const std::string& tableName) :
    fLog( logger ),
    fTableName(tableName )
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BRMReporter::~BRMReporter( )
{
}

//------------------------------------------------------------------------------
// Add a casual partition update to "this" BRMReporter's collection
//------------------------------------------------------------------------------
void BRMReporter::addToCPInfo(const BRM::CPInfoMerge& cpEntry)
{
    fCPInfo.push_back( cpEntry );
}

//------------------------------------------------------------------------------
// Add an HWM update to "this" BRMReporter's collection
//------------------------------------------------------------------------------
void BRMReporter::addToHWMInfo(const BRM::BulkSetHWMArg& hwmEntry)
{
    fHWMInfo.push_back( hwmEntry );
}

//------------------------------------------------------------------------------
// Add Column File infomation to "this" BRMReporter's collection
//------------------------------------------------------------------------------
void BRMReporter::addToFileInfo(const BRM::FileInfo& fileEntry)
{
    fFileInfo.push_back( fileEntry );
}

//------------------------------------------------------------------------------
// Add Dictionary File infomation to "this" BRMReporter's collection
//------------------------------------------------------------------------------
void BRMReporter::addToDctnryFileInfo(const BRM::FileInfo& fileEntry)
{
    fDctnryFileInfo.push_back( fileEntry );
}

//------------------------------------------------------------------------------
// Add Critical Error Message
//------------------------------------------------------------------------------
void BRMReporter::addToErrMsgEntry(const std::string& errCritMsg)
{
    fCritErrMsgs.push_back(errCritMsg);
}

//------------------------------------------------------------------------------
// Send ErrMsg to BRM Rpt File
//------------------------------------------------------------------------------
void BRMReporter::sendErrMsgToFile(const std::string& rptFileName)
{
    if((!rptFileName.empty())&&(fRptFileName.empty()))
        fRptFileName=rptFileName;
    if ((!fRptFileName.empty())&&(fCritErrMsgs.size()))
    {
        fRptFile.open( fRptFileName.c_str(), std::ios_base::app);
        if ( fRptFile.good() )
        {
            for (unsigned int i=0; i<fCritErrMsgs.size(); i++)
            {
                fRptFile << "MERR: " << fCritErrMsgs[i] << std::endl;
                //std::cout <<"**********" << fCritErrMsgs[i] << std::endl;
            }
        }
    }
}

//------------------------------------------------------------------------------
// Send collection information (Casual Partition and HWM) to applicable
// destination.  If file name given, then data is saved to the file, else
// the data is sent directly to BRM.
//
// On HDFS system, this function also notifies PrimProc to flush certain file
// descriptors (for columns and dictionary store), and blocks (for dictionary
// store).  Any DB file changes should have been "confirmed" prior to calling
// sendBRMInfo().  Once PrimProc cache is flushed, we can send the BRM updates.
//------------------------------------------------------------------------------
int BRMReporter::sendBRMInfo(const std::string& rptFileName,
    const std::vector<std::string>& errFiles,
    const std::vector<std::string>& badFiles)
{
    int rc = NO_ERROR;

    // For HDFS, we need to flush PrimProc cache since we modify HDFS files
    // by rewriting the files.
    if (idbdatafile::IDBPolicy::useHdfs())
    {
        std::vector<BRM::FileInfo> allFileInfo;

        if ( fFileInfo.size() > 0 )
        {
            for (unsigned k=0; k<fFileInfo.size(); k++)
            {
                allFileInfo.push_back( fFileInfo[k] );
            }
        }

        std::vector<BRM::OID_t> oidsToFlush;
        std::set<BRM::OID_t>    oidSet;
        if (fDctnryFileInfo.size() > 0)
        {
            for (unsigned k=0; k<fDctnryFileInfo.size(); k++)
            {
                allFileInfo.push_back( fDctnryFileInfo[k] );
                oidSet.insert( fDctnryFileInfo[k].oid );
            }

            // Store dictionary oids in std::set first, to eliminate duplicates
            if (oidSet.size() > 0)
            {
                for (std::set<BRM::OID_t>::const_iterator iter=oidSet.begin();
                    iter != oidSet.end();
                    ++iter)
                {
                    oidsToFlush.push_back( *iter );
                }
            }
        }

        // Flush PrimProc FD cache
        if (allFileInfo.size() > 0)
        {
            cacheutils::purgePrimProcFdCache(allFileInfo,
                Config::getLocalModuleID());
        }

        // Flush PrimProc block cache
        if (oidsToFlush.size() > 0)
            cacheutils::flushOIDsFromCache(oidsToFlush);
    }

    // After flushing cache (for HDFS), now we can update BRM
    if (rptFileName.empty())
    {
        // Set Casual Partition (CP) info for BRM for this column.  Be sure to
        // do this before we set the HWM.  Updating HWM 1st could cause a race
        // condition resulting in a query being based on temporary outdated CP
        // info.

        rc = sendHWMandCPToBRM( );

        // If HWM error occurs, we fail the job.
        if (rc != NO_ERROR)
        {
            return rc;
        }
    }
    else
    {
        fRptFileName = rptFileName;

        rc = openRptFile( );
        if (rc != NO_ERROR)
        {
            return rc;
        }

        sendCPToFile ( );
        sendHWMToFile( );

        // Log the list of *.err and *.bad files
        for (unsigned k=0; k<errFiles.size(); k++)
        {
            fRptFile << "ERR: " << errFiles[k] << std::endl;
        }

        for (unsigned k=0; k<badFiles.size(); k++)
        {
            fRptFile << "BAD: " << badFiles[k] << std::endl;
        }

    }

    return rc;
}

//------------------------------------------------------------------------------
// Send HWM and CP update information to BRM
//------------------------------------------------------------------------------
int BRMReporter::sendHWMandCPToBRM( )
{
    int rc = NO_ERROR;

    if (fHWMInfo.size() > 0)
    {
        std::ostringstream oss;
        oss << "Committing " << fHWMInfo.size() << " HWM update(s) for table "<<
            fTableName << " to BRM";
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    if (fCPInfo.size() > 0)
    {
        std::ostringstream oss;
        oss << "Committing " << fCPInfo.size() << " CP update(s) for table " <<
            fTableName << " to BRM";
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );
    }

    if ((fHWMInfo.size() > 0) || (fCPInfo.size() > 0))
    {
        rc = BRMWrapper::getInstance()->bulkSetHWMAndCP( fHWMInfo, fCPInfo );

        if (rc != NO_ERROR)
        {
            WErrorCodes ec;
            std::ostringstream oss;
            oss << "Error updating BRM with HWM and CP data for table " <<
                fTableName << "; " << ec.errorString(rc);
            fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
            return rc;
        }
    }

    return rc;
}

//------------------------------------------------------------------------------
// Save HWM update information to a file
//------------------------------------------------------------------------------
void BRMReporter::sendHWMToFile( )
{
    if (fHWMInfo.size() > 0)
    {
        std::ostringstream oss;
        oss << "Writing " << fHWMInfo.size() << " HWM update(s) for table " <<
            fTableName << " to report file " << fRptFileName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        for (unsigned int i=0; i<fHWMInfo.size(); i++)
        {
            fRptFile << "HWM: " << fHWMInfo[i].oid     << ' ' <<
                                   fHWMInfo[i].partNum << ' ' <<
                                   fHWMInfo[i].segNum  << ' ' <<
                                   fHWMInfo[i].hwm     << std::endl;
        }
    }
}

//------------------------------------------------------------------------------
// Send Casual Partition update information to BRM
//------------------------------------------------------------------------------
void BRMReporter::sendCPToFile( )
{
    if (fCPInfo.size() > 0)
    {
        std::ostringstream oss;
        oss << "Writing " << fCPInfo.size() << " CP updates for table " <<
            fTableName << " to report file " << fRptFileName;
        fLog->logMsg( oss.str(), MSGLVL_INFO2 );

        for (unsigned int i=0; i<fCPInfo.size(); i++)
        {
            fRptFile << "CP: " << fCPInfo[i].startLbid << ' ' <<
                                  fCPInfo[i].max       << ' ' <<
                                  fCPInfo[i].min       << ' ' <<
                                  fCPInfo[i].seqNum    << ' ' <<
                                  fCPInfo[i].type      << ' ' <<
                                  fCPInfo[i].newExtent << std::endl;
        }
    }
}

//------------------------------------------------------------------------------
// Report Summary totals; only applicable if we are generating a report file.
//------------------------------------------------------------------------------
void BRMReporter::reportTotals(
    uint64_t totalReadRows,
    uint64_t totalInsertedRows,
    const std::vector<boost::tuple<CalpontSystemCatalog::ColDataType, uint64_t, uint64_t> >& satCounts)
{
    if (fRptFile.is_open())
    {
        fRptFile << "ROWS: " << totalReadRows << ' ' <<
                                totalInsertedRows << std::endl;
        for (unsigned k=0; k<satCounts.size(); k++)
        {
            if (boost::get<0>(satCounts[k]) > 0)
                fRptFile << "DATA: " << k << ' ' << boost::get<0>(satCounts[k]) << ' ' <<
                    boost::get<1>(satCounts[k]) << ' ' << boost::get<2>(satCounts[k]) << std::endl;
        }

        closeRptFile();
    }
}

//------------------------------------------------------------------------------
// Generate report file indicating that user's import exceeded allowable error
// limit.
//------------------------------------------------------------------------------
void BRMReporter::rptMaxErrJob(const std::string& rptFileName,
    const std::vector<std::string>& errFiles,
    const std::vector<std::string>& badFiles )
{
    // We only write out information if we are generating a report file.
    if (!rptFileName.empty())
    {
        fRptFileName = rptFileName;

        int rc = openRptFile();

        // No need to return bad return code; we are already in a job that
        // is aborting.  openRptFile() at least logged the error.
        if (rc != NO_ERROR)
        {
            return;
        }

        // Log the list of *.err and *.bad files
        for (unsigned k=0; k<errFiles.size(); k++)
        {
            fRptFile << "ERR: " << errFiles[k] << std::endl;
        }

        for (unsigned k=0; k<badFiles.size(); k++)
        {
            fRptFile << "BAD: " << badFiles[k] << std::endl;
        }

        closeRptFile();
    }
}

//------------------------------------------------------------------------------
// Open BRM report file
//------------------------------------------------------------------------------
int BRMReporter::openRptFile( )
{
    fRptFile.open( fRptFileName.c_str() );
    if ( fRptFile.fail() )
    {
        int errRc = errno;
        std::ostringstream oss;
        std::string eMsg;
        Convertor::mapErrnoToString(errRc, eMsg);
        oss << "Error opening BRM report file " << fRptFileName << "; " << eMsg;
        int rc = ERR_FILE_OPEN;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    fRptFile << "#CP:   startLBID max min seqnum type newExtent" << std::endl;
    fRptFile << "#HWM:  oid partition segment hwm"               << std::endl;
    fRptFile << "#ROWS: numRowsRead numRowsInserted"             << std::endl;
    fRptFile << "#DATA: columNum columnType columnOid numOutOfRangeValues"   << std::endl;
    fRptFile << "#ERR:  error message file"                      << std::endl;
    fRptFile << "#BAD:  bad data file, with rejected rows"       << std::endl;
    fRptFile << "#MERR: critical error messages in cpimport.bin" << std::endl;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close BRM report file
//------------------------------------------------------------------------------
void BRMReporter::closeRptFile( )
{
    fRptFile.close();
}

}
