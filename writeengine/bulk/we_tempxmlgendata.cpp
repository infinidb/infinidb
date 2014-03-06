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
* $Id: we_tempxmlgendata.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#include "we_tempxmlgendata.h"

#include <iostream>

#include "calpontsystemcatalog.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
// TempXMLGenData constructor
//------------------------------------------------------------------------------
TempXMLGenData::TempXMLGenData(const std::string& jobId,
                         const std::string& schema,
                         const std::string& table)
{
    fParms[ JOBID] = jobId;                     // add or override default value
    fSchema = schema;
    execplan::CalpontSystemCatalog::TableName tbl(schema, table);
    fTables.push_back( tbl );
}

//------------------------------------------------------------------------------
// TempXMLGenData destructor
//------------------------------------------------------------------------------
/* virtual */
TempXMLGenData::~TempXMLGenData( )
{ }

//------------------------------------------------------------------------------
// TempXMLGenData print function.
//------------------------------------------------------------------------------
/* virtual */
void TempXMLGenData::print(std::ostream& os) const
{
    os << "Generating runtime job xml file for: schema-" << fSchema;
    if (fTables.size() > 0)
        os << ": table-" << fTables[0];
}

} // end of WriteEngine namespace
