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
 *   $Id: we_xmlgenproc.cpp 3806 2012-05-01 12:23:02Z dcathey $
 *
 ******************************************************************************/
#define WRITEENGINEXMLGENPROC_DLLEXPORT
#include "we_xmlgenproc.h"
#undef WRITEENGINEXMLGENPROC_DLLEXPORT

#include <sstream>
#include <unistd.h>

#include "we_config.h"
#include "we_xmltag.h"
#include "we_xmlgendata.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem/path.hpp>
using namespace execplan;

namespace
{
    const char*  DICT_TYPE("D");
    const char*  ENCODING("UTF-8");
    const char*  JOBNAME("Job_");
    const char*  LOGNAME("Jobxml_");
    const std::string LOGDIR("/log/");
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// XMLGen constructor
//------------------------------------------------------------------------------
XMLGenProc::XMLGenProc(XMLGenData* mgr, bool bUseXmlLogFile, bool bSysCatRpt) :
    fLog(),
    fDoc(),
    fWriter(),
    fErrorString("XMLGen encountered exception, abnormal exit "
                 "and file not created.\nCheck error log at:\t"),
    fDebugLevel(0),
    fInputMgr(mgr),
    fSysCatRpt(bSysCatRpt),
    fUseXmlLogFile(bUseXmlLogFile)
{
    std::string logFile(Config::getBulkRoot() + std::string(LOGDIR) + LOGNAME +
                   fInputMgr->getParm(XMLGenData::JOBID) + ".log" );
    std::string errFile(Config::getBulkRoot() + std::string(LOGDIR) + LOGNAME +
                   fInputMgr->getParm(XMLGenData::JOBID) + ".err" );
    fErrorString.append(errFile + "\n");
    if (fUseXmlLogFile)
    {
        fLog.setLogFileName( logFile.c_str(), errFile.c_str() );
        std::ostringstream ss;
        fInputMgr->print( ss );
        fLog.logMsg(ss.str(), MSGLVL_INFO1 );
    }
}

//------------------------------------------------------------------------------
// XMLGen destructor
//------------------------------------------------------------------------------
XMLGenProc::~XMLGenProc()
{
    xmlFreeDoc(fDoc);
}

//------------------------------------------------------------------------------
// startXMLFile
// Creates xmlDocPtr (fDoc) that we will use in generating our XML file.
// Create XML header section.
//------------------------------------------------------------------------------
void XMLGenProc::startXMLFile( )
{
    fWriter = xmlNewTextWriterDoc(&fDoc, 0);
    if (fWriter == NULL) {
        throw std::runtime_error("Error creating the xml fWriter: "
            "bad return from xmlNewTextWriter");
    }

    /* Start the fDocument with the xml default for the version,
     * encoding UTF-8 and the default for the standalone
     * declaration. */
    int rc = xmlTextWriterStartDocument(fWriter, NULL, ENCODING, NULL);
    if (rc < 0) {
        throw std::runtime_error("Error at xmlTextWriterStartfDocument: "
            "bad return from xmlTextWriterStartDocument");
    }

    if (!fSysCatRpt) // skip non-syscat tags if we are writing a syscat dump
    {
        xmlTextWriterStartElement(fWriter, BAD_CAST xmlTagTable[TAG_BULK_JOB]);
        xmlTextWriterWriteFormatElement(fWriter, BAD_CAST xmlTagTable[TAG_ID],
            "%d", atoi(fInputMgr->getParm(XMLGenData::JOBID).c_str()));
        xmlTextWriterWriteElement(fWriter, BAD_CAST xmlTagTable[TAG_NAME],
            BAD_CAST fInputMgr->getParm(XMLGenData::NAME).c_str() );
        xmlTextWriterWriteElement(fWriter, BAD_CAST xmlTagTable[TAG_DESC],
            BAD_CAST fInputMgr->getParm(XMLGenData::DESCRIPTION).c_str() );

        std::string now(boost::posix_time::to_iso_string(
            boost::posix_time::second_clock::local_time()));
        xmlTextWriterWriteElement(fWriter,
            BAD_CAST xmlTagTable[TAG_CREATE_DATE],
            BAD_CAST now.substr(0, 8).c_str() );
        xmlTextWriterWriteElement(fWriter,
            BAD_CAST xmlTagTable[TAG_CREATE_TIME],
            BAD_CAST now.substr(9, 4).c_str() );
        xmlTextWriterWriteElement(fWriter, BAD_CAST xmlTagTable[TAG_USER],
            BAD_CAST fInputMgr->getParm(XMLGenData::USER).c_str() );
        xmlTextWriterWriteElement(fWriter, BAD_CAST xmlTagTable[TAG_DELIMITER],
            BAD_CAST fInputMgr->getParm(XMLGenData::DELIMITER).c_str() );

        // Only include enclosedBy and escape chars if enclosedBy was specified
        std::string enclosedByChar = fInputMgr->getParm(
            XMLGenData::ENCLOSED_BY_CHAR);
        if (enclosedByChar.length() > 0)
        {
            xmlTextWriterWriteElement(fWriter,
                BAD_CAST xmlTagTable[TAG_ENCLOSED_BY_CHAR],
                BAD_CAST fInputMgr->getParm(
                    XMLGenData::ENCLOSED_BY_CHAR).c_str() );
            xmlTextWriterWriteElement(fWriter,
                BAD_CAST xmlTagTable[TAG_ESCAPE_CHAR],
                BAD_CAST fInputMgr->getParm(XMLGenData::ESCAPE_CHAR).c_str() );
        }

        // Added new tags for configurable parameters
        xmlTextWriterStartElement(fWriter,
            BAD_CAST xmlTagTable[TAG_READ_BUFFERS]);
        xmlTextWriterWriteFormatAttribute(fWriter,
            BAD_CAST xmlTagTable[TAG_NO_OF_READ_BUFFERS], "%d",
            atoi(fInputMgr->getParm(XMLGenData::NO_OF_READ_BUFFER).c_str()));
        xmlTextWriterWriteFormatAttribute(fWriter,
            BAD_CAST xmlTagTable[TAG_READ_BUFFER_SIZE],  "%d",
            atoi(fInputMgr->getParm(XMLGenData::READ_BUFFER_CAPACITY).c_str()));
        xmlTextWriterEndElement(fWriter);
        xmlTextWriterWriteFormatElement(fWriter,
            BAD_CAST xmlTagTable[TAG_WRITE_BUFFER_SIZE], "%d",
            atoi( fInputMgr->getParm(XMLGenData::WRITE_BUFFER_SIZE).c_str()));
        // End of additions
    }

    xmlTextWriterStartElement(fWriter, BAD_CAST xmlTagTable[TAG_SCHEMA]);
    xmlTextWriterWriteAttribute(fWriter, BAD_CAST xmlTagTable[TAG_NAME],
        BAD_CAST fInputMgr->getSchema().c_str() );
}

//------------------------------------------------------------------------------
// makeTableData
// Create XML tag for a table.
//------------------------------------------------------------------------------
void XMLGenProc::makeTableData(const CalpontSystemCatalog::TableName& table)
{
    static unsigned kount;

    xmlTextWriterStartElement(fWriter, BAD_CAST xmlTagTable[TAG_TABLE]);
    std::string tmp(table.schema + "." + table.table);
    xmlTextWriterWriteAttribute(fWriter,
        BAD_CAST xmlTagTable[TAG_TBL_NAME], BAD_CAST tmp.c_str() );

    if (fSysCatRpt) // Write full schema information for syscat rpt
    {
        try
        {
            CalpontSystemCatalog* cat =
                CalpontSystemCatalog::makeCalpontSystemCatalog(
                BULK_SYSCAT_SESSION_ID);
            cat->identity(CalpontSystemCatalog::EC);
            xmlTextWriterWriteFormatAttribute(fWriter,
                BAD_CAST xmlTagTable[TAG_TBL_OID], "%d",
                cat->tableRID(table).objnum);
        }
        catch (std::exception& ex)
        {
            std::ostringstream oss;
            oss << "Error getting OID for table " <<
                table.schema << '.' << table.table << ": " << ex.what();
            throw std::runtime_error( oss.str() );
        }
        catch (...)
        {
            std::ostringstream oss;
            oss << "Unknown error getting OID for table " <<
                table.schema << '.' << table.table;
            throw std::runtime_error( oss.str() );
        }
    }

    if (!fSysCatRpt) // skip non-syscat tags if we are writing a syscat dump
    {
        const XMLGenData::LoadNames& loadNames = fInputMgr->getLoadNames();
        if ( loadNames.size() > kount )
        {
            tmp = loadNames[kount];
        }
        else 
        {
            tmp = (table.table + "." + fInputMgr->getParm(
                XMLGenData::EXT).c_str());
        }
        xmlTextWriterWriteAttribute(fWriter,
            BAD_CAST xmlTagTable[TAG_LOAD_NAME], BAD_CAST tmp.c_str() );
        xmlTextWriterWriteFormatAttribute(fWriter,
            BAD_CAST xmlTagTable[TAG_MAX_ERR_ROW], "%d",
            atoi( fInputMgr->getParm(XMLGenData::MAXERROR).c_str()) );
    }

    kount++;
}

//------------------------------------------------------------------------------
// sortColumnsByPosition
// Sort list of columns by column position.
//------------------------------------------------------------------------------
void XMLGenProc::sortColumnsByPosition(SysCatColumnList &columns)
{
    std::map<int,SysCatColumn> tempCols;

    SysCatColumnList::const_iterator cend = columns.end();
    for (SysCatColumnList::const_iterator col = columns.begin();
        col != cend; ++col)
    {
        tempCols[col->colType.colPosition] = *col ;
    }

    columns.clear();

    std::map<int,SysCatColumn>::iterator pos;
    for (pos = tempCols.begin(); pos != tempCols.end(); ++pos) 
    {
        columns.push_back(pos->second);
    }
    
    tempCols.clear();
}

//------------------------------------------------------------------------------
// makeColumnData
// Create XML tag for the columns in a table.
//------------------------------------------------------------------------------
bool XMLGenProc::makeColumnData(const CalpontSystemCatalog::TableName& table)
{
    SysCatColumnList columns;
    getColumnsForTable(table.schema,table.table, columns);
    sortColumnsByPosition(columns);
    if (columns.empty())
    {
        if (fUseXmlLogFile)
        {
            fLog.logMsg("No columns for " + table.table +
                        ", or table does not exist", MSGLVL_ERROR );
        }
        return false;
    }

    SysCatColumnList::const_iterator cend = columns.end();
    for (SysCatColumnList::const_iterator col = columns.begin();
        col != cend; ++col)
    {
        xmlTextWriterStartElement(fWriter, BAD_CAST xmlTagTable[TAG_COLUMN]);
        xmlTextWriterWriteAttribute(fWriter,
            BAD_CAST xmlTagTable[TAG_COL_NAME],
            BAD_CAST col->tableColName.column.c_str());

        if (fSysCatRpt) // Write full schema information for syscat rpt
        {
            xmlTextWriterWriteFormatAttribute(fWriter,
                BAD_CAST xmlTagTable[TAG_COL_OID], "%d", col->oid);
            xmlTextWriterWriteAttribute(fWriter,
                BAD_CAST xmlTagTable[TAG_DATA_TYPE],
                BAD_CAST ColDataTypeStr[
                    col->colType.colDataType]);
            if (col->colType.compressionType !=
                CalpontSystemCatalog::NO_COMPRESSION)
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_COMPRESS_TYPE], "%d",
                    col->colType.compressionType);

            // Old logic went by scale > 0; New logic checks for "decimal" type
            if ( (0 < col->colType.scale ) ||
                (col->colType.colDataType == CalpontSystemCatalog::DECIMAL) )
            {
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_PRECISION], "%d",
                    col->colType.precision);
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_SCALE], "%d", col->colType.scale);
            }
            xmlTextWriterWriteFormatAttribute(fWriter,
                BAD_CAST xmlTagTable[TAG_WIDTH], "%d", col->colType.colWidth);
 
#ifndef SKIP_AUTOI
            if (col->colType.autoincrement)
            {
                int autoInc = 1;
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_AUTOINCREMENT_FLAG], "%d",autoInc);
            }
#endif

            //need dictionary and decimal stuff
            if (col->colType.ddn.dictOID > 0)
            {
                xmlTextWriterWriteAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_COL_TYPE], BAD_CAST DICT_TYPE );
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_DVAL_OID], "%d",
                    col->colType.ddn.dictOID );
            }

            // Include NotNull and Default value
            const std::string col_defaultValue(col->colType.defaultValue);

            if (col->colType.constraintType ==
                execplan::CalpontSystemCatalog::NOTNULL_CONSTRAINT)
            {
                int notNull = 1;
                xmlTextWriterWriteFormatAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_NOT_NULL], "%d", notNull);
                if (!col_defaultValue.empty())
                {
                    xmlTextWriterWriteAttribute(fWriter,
                        BAD_CAST xmlTagTable[TAG_DEFAULT_VALUE],
                        BAD_CAST col_defaultValue.c_str());
                }
            }
            else if (col->colType.constraintType ==
                execplan::CalpontSystemCatalog::DEFAULT_CONSTRAINT)
            {
                xmlTextWriterWriteAttribute(fWriter,
                    BAD_CAST xmlTagTable[TAG_DEFAULT_VALUE],
                    BAD_CAST col_defaultValue.c_str());
            }
        } // end of "if fSysCatRpt"

        xmlTextWriterEndElement(fWriter);
    }
    xmlTextWriterEndElement(fWriter); //table 
    return true;
}

//------------------------------------------------------------------------------
// getColumnsForTable
// Access the system catalog in order to get the OID, column type, and column
// name for all the columns in the specified schema and table.
//
// This function is modeled after the function by the same name in DDLPackage-
// Processor.  XMLGen used to use that DDLPackageProcessor function, but I
// decided to implement the functionality in XMLGen in order to eliminate the
// dependency on ddlpackageprocessor.
//------------------------------------------------------------------------------
void XMLGenProc::getColumnsForTable(
    const std::string& schema,
    const std::string& table,
    SysCatColumnList&  colList)
{
    CalpontSystemCatalog::TableName tableName;
    tableName.schema = schema;
    tableName.table = table;

    try
    {
        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog(
            BULK_SYSCAT_SESSION_ID);
        systemCatalogPtr->identity(CalpontSystemCatalog::EC);

        const CalpontSystemCatalog::RIDList ridList =
            systemCatalogPtr->columnRIDs(tableName, true);

        CalpontSystemCatalog::RIDList::const_iterator rid_iterator =
            ridList.begin();
        while (rid_iterator != ridList.end())
        {
            CalpontSystemCatalog::ROPair roPair = *rid_iterator;

            SysCatColumn column;
            column.oid = roPair.objnum;
            column.colType = systemCatalogPtr->colType(column.oid);
            column.tableColName = systemCatalogPtr->colName(column.oid);

            colList.push_back(column);

            ++rid_iterator;
        }
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Error reading columns for table " <<
            schema << '.' << table << ": " << ex.what();
        throw std::runtime_error( oss.str() );
    }
    catch (...)
    {
        std::ostringstream oss;
        oss << "Unknown error reading columns for table " <<
            schema << '.' << table;
        throw std::runtime_error( oss.str() );
    }
}

//------------------------------------------------------------------------------
// Generate Job XML File Name
//------------------------------------------------------------------------------
std::string XMLGenProc::genJobXMLFileName( ) const
{
    std::string xmlFileName;
    boost::filesystem::path p(std::string(
        fInputMgr->getParm(XMLGenData::PATH)));

    //Append the jobname, jobid & file extension
    std::string fileName( JOBNAME );
    fileName += fInputMgr->getParm(XMLGenData::JOBID);
    fileName += ".xml";
    p /= fileName;

    //If the filespec doesn't begin with a '/' (i.e. it's not an absolute path),
    // attempt to make it absolute so that we can log the full pathname.
#ifdef _MSC_VER
    //We won't worry about being so fancy in Windows, just print a relative
    // path if so given
    xmlFileName = p.string();
#else
    if (!p.has_root_path())
    {
        char cwdPath[4096];
        getcwd(cwdPath, sizeof(cwdPath));
        boost::filesystem::path p2(cwdPath);
        p2 /= p;
        xmlFileName = p2.string();
    }
    else
    {
        xmlFileName = p.string();
    }
#endif

	return xmlFileName;
}

//------------------------------------------------------------------------------
// writeXMLFile
//------------------------------------------------------------------------------
void XMLGenProc::writeXMLFile( const std::string& xmlFileName )
{
    xmlTextWriterEndDocument(fWriter);
    xmlFreeTextWriter(fWriter);

    xmlSaveFormatFile(xmlFileName.c_str(), fDoc, 1);
}

//------------------------------------------------------------------------------
// logErrorMessage
//------------------------------------------------------------------------------
void XMLGenProc::logErrorMessage(const std::string& msg) 
{ 
    if (fUseXmlLogFile)
    {
        fLog.logMsg( msg , MSGLVL_ERROR );
    }
}

} // namespace bulkloadxml
