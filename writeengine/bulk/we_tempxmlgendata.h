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
* $Id: we_tempxmlgendata.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_TEMPXMLGENDATA_H_
#define _WE_TEMPXMLGENDATA_H_

#include <string>
#include <iosfwd>

#include "we_xmlgendata.h"

namespace WriteEngine
{

/** @brief Class used by cpimport.bin to store data that is saved into a
 *  temporary runtime Job XML file.
 */
class TempXMLGenData : public XMLGenData
{
  public:
    TempXMLGenData(const std::string& jobId,
                const std::string& schema,
                const std::string& table );

    virtual ~TempXMLGenData( );

    virtual void print(std::ostream& os) const;

  private:
    TempXMLGenData(const TempXMLGenData&);           //disable default copy ctor
    TempXMLGenData& operator=(const TempXMLGenData&);//disable def assignment
};

} // end of WriteEngine namespace

#endif // _WE_TEMPXMLGENDATA_H_
