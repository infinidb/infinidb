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
 * $Id: we_brmreporter.cpp 3615 2012-03-09 16:41:14Z dcathey $
 *
 ****************************************************************************/

/** @file
 * Implementation of the BRMReporter class
 */

#include "we_brmreporter.h"

#include <cerrno>

#include "we_brm.h"
#include "we_log.h"

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
// Send collection information (Casual Partition and HWM) to BRM.
//------------------------------------------------------------------------------
int BRMReporter::sendBRMInfo( )
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

}
