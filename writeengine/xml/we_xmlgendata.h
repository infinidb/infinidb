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

/***********************************************************************
 *   $Id: we_xmlgendata.h 3720 2012-04-04 18:18:49Z rdempsey $
 *
 ***********************************************************************/
/** @file */
#ifndef _WE_XMLGENDATA_H_
#define _WE_XMLGENDATA_H_

#include <iosfwd>
#include <string>
#include <vector>
#include <map>
#include "calpontsystemcatalog.h"

#if defined(_MSC_VER) && defined(WRITEENGINEXMLGENDATA_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

/** @brief Base class for storing input data used to generate a Job XML file.
 *
 *  This class represents common code refactored out of inputmgr.h, under
 *  tools/dbloadxml.  It was moved to writeengine/xml so that this common
 *  code could be used by both colxml and cpimport.bin.
 */
class XMLGenData
{
  public:
    typedef std::vector<execplan::CalpontSystemCatalog::TableName> TableList;
    typedef std::map<std::string, std::string> ParmList;
    typedef std::vector<std::string> LoadNames;

    // Valid parms that can be stored and retrieved from XMLGenData
    EXPORT const static std::string DELIMITER;
    EXPORT const static std::string DESCRIPTION;
#if defined(_MSC_VER) && !defined(WRITEENGINEXMLGENDATA_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT const static std::string ENCLOSED_BY_CHAR;
#if defined(_MSC_VER) && !defined(WRITEENGINEXMLGENDATA_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT const static std::string ESCAPE_CHAR;
#if defined(_MSC_VER) && !defined(WRITEENGINEXMLGENDATA_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT const static std::string JOBID;
    EXPORT const static std::string MAXERROR;
    EXPORT const static std::string NAME;
    EXPORT const static std::string PATH;
#if defined(_MSC_VER) && !defined(WRITEENGINEXMLGENDATA_DLLEXPORT)
    __declspec(dllimport)
#endif
    EXPORT const static std::string RPT_DEBUG;
    EXPORT const static std::string USER;
    EXPORT const static std::string NO_OF_READ_BUFFER;
    EXPORT const static std::string READ_BUFFER_CAPACITY;
    EXPORT const static std::string WRITE_BUFFER_SIZE;
    EXPORT const static std::string EXT;

    /** @brief XMLGenData constructor
     */
    EXPORT XMLGenData();

    /** @brief XMLGenData destructor
     */
    EXPORT virtual ~XMLGenData();

    /** @brief Print contents of this object to the specified stream.
     */
    EXPORT virtual void print(std::ostream& os) const;

    EXPORT std::string getParm(const std::string& key) const;
    const TableList&   getTables()    const { return fTables;    }
    const std::string& getSchema()    const { return fSchema;    }
    const LoadNames&   getLoadNames() const { return fLoadNames; }

  protected:
    TableList   fTables;
    ParmList    fParms;
    std::string fSchema;
    LoadNames   fLoadNames;

  private:
    XMLGenData(const XMLGenData&);             // disable default copy ctor
    XMLGenData& operator=(const XMLGenData&);  // disable default assignment
};

}

#undef EXPORT

#endif
