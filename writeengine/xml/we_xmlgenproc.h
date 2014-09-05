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
 *   $Id: we_xmlgenproc.h 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ***********************************************************************/
/** @file */
#ifndef WE_XMLGENPROC_H
#define WE_XMLGENPROC_H

#include <libxml/parser.h>
#include <libxml/xmlwriter.h>
#include <string>
#include <vector>

#include "calpontsystemcatalog.h"
#include "we_log.h"
#include "we_xmlgendata.h"

#if defined(_MSC_VER) && defined(WRITEENGINEXMLGENPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{
struct SysCatColumn
{
    execplan::CalpontSystemCatalog::OID          oid;
    execplan::CalpontSystemCatalog::ColType      colType;
    execplan::CalpontSystemCatalog::TableColName tableColName;
};
typedef std::vector<SysCatColumn> SysCatColumnList;

class XMLGenData;

/** @brief Generates job_xxx.xml file (for cpimport.bin) using XMLGenData as
 *  input.  Can also be used to generate an XML-formatted report.
 *
 *  This class was moved from XMLProc, formerly in tools/dbloadxml.  It was
 *  moved to writeengine/xml so that both colxml and cpimport.bin could use it.
 */
class XMLGenProc
{
public:

    /** @brief XMLGenProc constructor
     *
     * @param mgr The input data used to generate a Job XML file.
     * @param bUseXmlLogFile Log info/errors to Job XML log file.
     * @param bSysCatRpt Generating SysCat report (true) or XML file (false)
     */
    EXPORT XMLGenProc(XMLGenData* mgr, bool bUseXmlLogFile, bool bSysCatRpt);
    EXPORT ~XMLGenProc();

    /** @brief start constructing XML file document.
     */
    EXPORT void  startXMLFile( );
    
     /** @brief Creates table tag for the specified table.
     *
     * @param table Name of table for which the table tag is to be generated.
     */ 
    EXPORT void  makeTableData(
        const execplan::CalpontSystemCatalog::TableName& table);

    /** @brief Creates column tags for the specified table.
     *
     * @param table Name of table for which the column tags are to be generated.
     * @return true means column tags created; else false is returned
     */  
    EXPORT bool  makeColumnData(
        const execplan::CalpontSystemCatalog::TableName& table);
    
    /** @brief Generate Job XML file name
     */   
    EXPORT std::string genJobXMLFileName( ) const;

    /** @brief Write xml file document to the destination Job XML file.
     *
     * @param xmlFileName Name of XML file to be generated.
     */   
    EXPORT void  writeXMLFile( const std::string& xmlFileName );

    /** @brief log a message.
     *
     * @param msg The message to be logged to the error log file.
     */
    EXPORT void logErrorMessage(const std::string& msg);
    std::string errorString() { return fErrorString; }

    /** @brief set debug level
     */
    void setDebugLevel( int dbg ) { fDebugLevel = dbg; }

protected:

private:
    XMLGenProc(const XMLGenProc&);             // disable default copy ctor
    XMLGenProc& operator=(const XMLGenProc&);  // disable default assignment
    void getColumnsForTable(const std::string& schema,
        const std::string& table, SysCatColumnList& colList);
    void sortColumnsByPosition(SysCatColumnList &columns);
 
    Log               fLog;
    xmlDocPtr         fDoc;
    xmlTextWriterPtr  fWriter;
    std::string       fErrorString;
    int               fDebugLevel;
    XMLGenData*       fInputMgr;    // Input data used to generate Job XML file
    bool              fSysCatRpt;   // True colxml output or a syscat report
    bool              fUseXmlLogFile;//Log info/errors to Job XML log file
};

}                                                 // namespace WriteEngine

#undef EXPORT

#endif                                            
