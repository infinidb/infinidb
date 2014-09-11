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
* $Id: we_xmlgendata.cpp 2968 2011-05-13 19:01:26Z rdempsey $
*
*******************************************************************************/

#define WRITEENGINEXMLGENDATA_DLLEXPORT
#include "we_xmlgendata.h"
#undef WRITEENGINEXMLGENDATA_DLLEXPORT

#include <iostream>
#include <boost/filesystem/path.hpp>
#include "we_config.h"

namespace
{
    const std::string JOBDIR("job");
}

namespace WriteEngine
{
    /* static */ const std::string XMLGenData::DELIMITER           ("-d");
    /* static */ const std::string XMLGenData::DESCRIPTION         ("-s");
#ifdef _MSC_VER
   __declspec(dllexport)
#endif
    /* static */ const std::string XMLGenData::ENCLOSED_BY_CHAR    ("-E");
#ifdef _MSC_VER
   __declspec(dllexport)
#endif
    /* static */ const std::string XMLGenData::ESCAPE_CHAR         ("-C");
#ifdef _MSC_VER
   __declspec(dllexport)
#endif
    /* static */ const std::string XMLGenData::JOBID               ("-j");
    /* static */ const std::string XMLGenData::MAXERROR            ("-e");
    /* static */ const std::string XMLGenData::NAME                ("-n");
    /* static */ const std::string XMLGenData::PATH                ("-p");
#ifdef _MSC_VER
   __declspec(dllexport)
#endif
    /* static */ const std::string XMLGenData::RPT_DEBUG           ("-b");
    /* static */ const std::string XMLGenData::USER                ("-u");
    /* static */ const std::string XMLGenData::NO_OF_READ_BUFFER   ("-r");
    /* static */ const std::string XMLGenData::READ_BUFFER_CAPACITY("-c");
    /* static */ const std::string XMLGenData::WRITE_BUFFER_SIZE   ("-w");
    /* static */ const std::string XMLGenData::EXT                 ("-x");

//------------------------------------------------------------------------------
// XMLGenData constructor
// Omit inserting JOBID; derived class is required to insert
//------------------------------------------------------------------------------
XMLGenData::XMLGenData( )
{
    fParms.insert(ParmList::value_type(DELIMITER,std::string("|")));
    fParms.insert(ParmList::value_type(DESCRIPTION,std::string()));
    fParms.insert(ParmList::value_type(ENCLOSED_BY_CHAR,std::string("")));
    fParms.insert(ParmList::value_type(ESCAPE_CHAR,std::string("\\")));
    fParms.insert(ParmList::value_type(JOBID,std::string("299")));
    fParms.insert(ParmList::value_type(MAXERROR,std::string("10")));
    fParms.insert(ParmList::value_type(NAME,std::string()));
#ifdef _MSC_VER
	std::string br;
	br = Config::getBulkRoot();
	boost::filesystem::path p(br);
#else
    boost::filesystem::path p( std::string(Config::getBulkRoot()) );
#endif
    p /= JOBDIR;
    fParms.insert(ParmList::value_type(PATH, p.string()));

    fParms.insert(ParmList::value_type(RPT_DEBUG,std::string("0")));
    fParms.insert(ParmList::value_type(USER,std::string()));
    fParms.insert(ParmList::value_type(NO_OF_READ_BUFFER,std::string("5")));
    fParms.insert(ParmList::value_type(READ_BUFFER_CAPACITY,
        std::string("1048576")));
    fParms.insert(ParmList::value_type(WRITE_BUFFER_SIZE,
        std::string("10485760")));
    fParms.insert(ParmList::value_type(EXT,std::string("tbl")));
}

//------------------------------------------------------------------------------
// XMLGenData destructor
//------------------------------------------------------------------------------
/* virtual */
XMLGenData::~XMLGenData( )
{
}

//------------------------------------------------------------------------------
// Return value for the specified parm.
//------------------------------------------------------------------------------
std::string XMLGenData::getParm(const std::string& key) const
{
    ParmList::const_iterator p = fParms.find(key);
    if (fParms.end() != p)
        return p->second;
    else
        return "";
}

//------------------------------------------------------------------------------
// print contents to specified stream
//------------------------------------------------------------------------------
/* virtual */
void XMLGenData::print(std::ostream& /* os */) const
{
}

}  //namespace WriteEngine
