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
* $Id: we_xmljob.cpp 4579 2013-03-19 23:16:54Z dhall $
*
*******************************************************************************/
/** @file */

#define WRITEENGINEXMLJOB_DLLEXPORT
#include "we_xmljob.h"
#undef WRITEENGINEXMLJOB_DLLEXPORT

#include <limits>
#include <sstream>
#include <unistd.h>
#include <stdexcept>
#include <cstdlib>
#include <set>
#include "we_config.h"
#include "we_log.h"
#include "we_convertor.h"
#include "dataconvert.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

using namespace std;
using namespace execplan;

namespace WriteEngine
{
// Maximum saturation value for DECIMAL types based on precision
const long long infinidb_precision[19] = {
0,
9,
99,
999,
9999,
99999,
999999,
9999999,
99999999,
999999999,
9999999999LL,
99999999999LL,
999999999999LL,
9999999999999LL,
99999999999999LL,
999999999999999LL,
9999999999999999LL,
99999999999999999LL,
999999999999999999LL
};

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XMLJob::XMLJob( ) : fDebugLevel( DEBUG_0 ),
    fDeleteTempFile(false),
    fValidateColList(true)
{
}

//------------------------------------------------------------------------------
// Default Destructor
// Delete temporary Job XML file if applicable.
//------------------------------------------------------------------------------
XMLJob::~XMLJob()
{
    if ((fDeleteTempFile) && (!fJobFileName.empty()))
    {
        unlink( fJobFileName.c_str() );
    }
}

//------------------------------------------------------------------------------
// Load a job xml file
// fileName - name of file to load
// bTempFile - are we loading a temporary file (that destructor should delete)
// bValidateColumnList - validate that all db columns have an XML tag
// returns NO_ERROR if success; other if fail
//------------------------------------------------------------------------------
int XMLJob::loadJobXmlFile( const string& fileName,
    bool bTempFile,
    bool bValidateColumnList,
    string& errMsg )
{
    int rc;

    fDeleteTempFile = bTempFile;
    fJobFileName    = fileName;
    fValidateColList= bValidateColumnList;

    try
    {
        rc = parseDoc( fileName.c_str() );
        if (rc != NO_ERROR)
            return rc;
    }
    catch (exception& ex)
    {
        errMsg = ex.what();
        return ERR_XML_PARSE;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Print contents of fJob to the specified logger object.
// logger - Log object to use in logging
//------------------------------------------------------------------------------
void XMLJob::printJobInfo( Log& logger ) const
{
    const Job& job = fJob;

    ostringstream oss1;
    oss1 << "Job " << job.id << " input\n";
    oss1 << "===============================================" << endl;
    oss1 << "Name : " << job.name << endl;
    oss1 << "Desc : " << job.desc << endl;
    oss1 << "User : " << job.userName << endl;
    oss1 << "Delim: " << job.fDelimiter << endl;
    oss1 << "Enclosed By : ";
    if (job.fEnclosedByChar)
        oss1 << job.fEnclosedByChar << endl;
    else
        oss1 << "n/a" << endl;
    oss1 << "Escape Char : ";
    if (job.fEscapeChar)
        oss1 << job.fEscapeChar << endl;
    else
        oss1 << "n/a" << endl;
    oss1 << "Read Buffers:     " << job.numberOfReadBuffers << endl;
    oss1 << "Read Buffer Size: " << job.readBufferSize << endl;
    oss1 << "setvbuf Size: " << job.writeBufferSize << endl;
    oss1 << "Create Date : " << job.createDate << endl;
    oss1 << "Create Time : " << job.createTime << endl;
    oss1 << "Schema Name : " << job.schema << endl;

    oss1 << "Num Tables  : " << job.jobTableList.size() << endl;
    logger.logMsg( oss1.str(), MSGLVL_INFO2 );

    for( unsigned int i = 0; i < job.jobTableList.size(); i++ ) {
        const JobTable& jobTable = job.jobTableList[i];
        ostringstream oss2;
        oss2 << "\n-------------------------------------------------" << endl;
        oss2 << "\tTable Name      : " << jobTable.tblName << endl;
        oss2 << "\tTable OID       : " << jobTable.mapOid << endl;
        oss2 << "\tTable Load Name : " << jobTable.loadFileName <<
                endl;
        oss2 << "\tMax Err Num     : " << jobTable.maxErrNum << endl;
         
        const JobColList& colList = jobTable.colList;

        oss2 << "\tNum of Columns  : " << colList.size() << endl;
        logger.logMsg( oss2.str(), MSGLVL_INFO2 );

        // Note that we don't print JobColumn.dataType because it is not carried
        // in the XML file.  dataType is assigned/used internally by bulkload.
        for( unsigned int j = 0; j < jobTable.fFldRefs.size(); j++ ) {
            unsigned idx            = jobTable.fFldRefs[j].fArrayIndex;
            BulkFldColRel fldColType= jobTable.fFldRefs[j].fFldColType;
            const JobColumn& jobCol = ((fldColType == BULK_FLDCOL_IGNORE_FIELD)?
                jobTable.fIgnoredFields[idx] :
                jobTable.colList[idx] );
            ostringstream oss3;
            oss3 << "\n\t****************************************" << endl;
            if (fldColType == BULK_FLDCOL_COLUMN_DEFAULT)
                oss3 << "\t\tDefaultColumn Name: " << jobCol.colName << endl;
            else
                oss3 << "\t\tColumn Name       : " << jobCol.colName << endl;
            oss3 << "\t\tColumn OID        : " << jobCol.mapOid << endl;
            oss3 << "\t\tColumn type name  : " << jobCol.typeName << endl;
            oss3 << "\t\tColumn width      : " << jobCol.width << endl;
            oss3 << "\t\tColumn Not Null   : " << jobCol.fNotNull << endl;
            oss3 << "\t\tColumn WithDefault: " << jobCol.fWithDefault << endl;
            oss3 << "\t\tColumn type       : " << jobCol.colType << endl;
            oss3 << "\t\tColumn comp type  : " << jobCol.compressionType <<endl;

#ifndef SKIP_AUTOI
            oss3 << "\t\tColumn autoInc    : " << jobCol.autoIncFlag << endl;
#endif

            if( jobCol.typeName == ColDataTypeStr[CalpontSystemCatalog::DECIMAL] ) {
                oss3 << "\t\tColumn Precision  : " << jobCol.precision << endl;
                oss3 << "\t\tColumn Scale      : " << jobCol.scale << endl;
            }
            if( jobCol.typeName == ColDataTypeStr[CalpontSystemCatalog::UDECIMAL] ) {
                oss3 << "\t\tColumn Precision  : " << jobCol.precision << endl;
                oss3 << "\t\tColumn Scale      : " << jobCol.scale << endl;
            }

            if( jobCol.colType == 'D' ) {
                oss3 << "\t\tDictionary Oid    : " <<
                        jobCol.dctnry.dctnryOid << endl;
            }
            logger.logMsg( oss3.str(), MSGLVL_INFO2 );
        } // end of loop through columns in a table
    } // end of loop through tables
}

//------------------------------------------------------------------------------
// Print brief contents of specified Job to specified logger object.
// logger - Log object to use in logging
//------------------------------------------------------------------------------
void XMLJob::printJobInfoBrief( Log& logger ) const
{
    const Job& job = fJob;

    ostringstream oss1;
    oss1 << "XMLJobFile: Delim(" << job.fDelimiter << "); EnclosedBy(";
    if (job.fEnclosedByChar)
        oss1 << job.fEnclosedByChar;
    else
        oss1 << "n/a";
    oss1 << "); EscapeChar(";
    if (job.fEscapeChar)
        oss1 << job.fEscapeChar;
    else
        oss1 << "n/a";
    oss1 << "); ReadBufs("    << job.numberOfReadBuffers <<
            "); ReadBufSize(" << job.readBufferSize      <<
            "); setvbufSize(" << job.writeBufferSize     << ')';
    logger.logMsg( oss1.str(), MSGLVL_INFO2 );

    for( unsigned int i = 0; i < job.jobTableList.size(); i++ ) {
        const JobTable& jobTable = job.jobTableList[i];
        ostringstream oss2;
        oss2 << "  Table(" << jobTable.tblName <<
                "); OID("  << jobTable.mapOid  << ')' <<
                "; MaxErrNum(" << jobTable.maxErrNum << ')';
        logger.logMsg( oss2.str(), MSGLVL_INFO2 );

        for( unsigned int j = 0; j < jobTable.fFldRefs.size(); j++ ) {
            unsigned idx            = jobTable.fFldRefs[j].fArrayIndex;
            BulkFldColRel fldColType= jobTable.fFldRefs[j].fFldColType;
            const JobColumn& jobCol = ((fldColType == BULK_FLDCOL_IGNORE_FIELD)?
                jobTable.fIgnoredFields[idx] :
                jobTable.colList[idx]);
            ostringstream oss3;
            if (fldColType == BULK_FLDCOL_COLUMN_DEFAULT)
                oss3 << "    DefaultColumn(" << jobCol.colName;
            else
                oss3 << "    Column("        << jobCol.colName;
            oss3 << "); OID("     << jobCol.mapOid   <<
                    "); Type("    << jobCol.typeName <<
                    "); Width("   << jobCol.width    <<
                    "); Comp("    << jobCol.compressionType;
            if( jobCol.colType == 'D' )
                oss3 << "); DctnryOid(" << jobCol.dctnry.dctnryOid;
            oss3 << ')';
#ifndef SKIP_AUTOI
            if (jobCol.autoIncFlag)
                oss3 << "; autoInc";
#endif
            if (jobCol.fNotNull)
                oss3 << "; NotNull";
            if (jobCol.fWithDefault)
                oss3 << "; WithDefault";
            logger.logMsg( oss3.str(), MSGLVL_INFO2 );
        }
    } // end of for( int i
}

//------------------------------------------------------------------------------
// Process a node
// pNode - current node
// returns TRUE if success, FALSE otherwise
//------------------------------------------------------------------------------
bool XMLJob::processNode( xmlNode* pNode )
{
    if( isTag( pNode, TAG_BULK_JOB ))
    {
        // no work for the BulkJob tag
    }
    else if( isTag( pNode, TAG_CREATE_DATE ))
        setJobData( pNode, TAG_CREATE_DATE, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_CREATE_TIME ))
        setJobData( pNode, TAG_CREATE_TIME, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_COLUMN ))
        setJobData( pNode, TAG_COLUMN, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_DEFAULT_COLUMN ))
        setJobData( pNode, TAG_DEFAULT_COLUMN, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_DESC ))
        setJobData( pNode, TAG_DESC, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_ID ))
        setJobData( pNode, TAG_ID, true, TYPE_INT );
    else if( isTag( pNode, TAG_IGNORE_FIELD ))
        setJobData( pNode, TAG_IGNORE_FIELD, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_NAME ))
        setJobData( pNode, TAG_NAME, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_PATH ))
        setJobData( pNode, TAG_PATH, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_TABLE ))
        setJobData( pNode, TAG_TABLE, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_TYPE ))
        setJobData( pNode, TAG_TYPE, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_USER ))
        setJobData( pNode, TAG_USER, true, TYPE_CHAR );
    else if( isTag( pNode, TAG_SCHEMA))
        setJobData( pNode, TAG_SCHEMA, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_READ_BUFFERS))
        setJobData( pNode, TAG_READ_BUFFERS, false, TYPE_EMPTY );
    else if( isTag( pNode, TAG_WRITE_BUFFER_SIZE))
        setJobData( pNode, TAG_WRITE_BUFFER_SIZE, true, TYPE_INT);
    else if( isTag( pNode, TAG_DELIMITER))
        setJobData( pNode, TAG_DELIMITER, true, TYPE_CHAR);
    else if( isTag( pNode, TAG_ENCLOSED_BY_CHAR))
        setJobData( pNode, TAG_ENCLOSED_BY_CHAR, true, TYPE_CHAR);
    else if( isTag( pNode, TAG_ESCAPE_CHAR))
        setJobData( pNode, TAG_ESCAPE_CHAR, true, TYPE_CHAR);
    else
    {
        ostringstream oss;
        oss << "Unrecognized TAG in Job XML file: <" << pNode->name << ">";
        throw runtime_error( oss.str() );
    }

    if (XMLOp::processNode( pNode ))
    {
        if( isTag( pNode, TAG_TABLE ))
        {
            postProcessTableNode();
        }
    }
    else
    {
        return false;
    }

    return true;
}

//------------------------------------------------------------------------------
// Generic setter
// pNode - current node
// tag - xml tag
// bExpectContent - should node content be present to process
// tagType - data type
//------------------------------------------------------------------------------
void XMLJob::setJobData( xmlNode* pNode,
                         const xmlTag tag,
                         bool  bExpectContent,
                         XML_DTYPE tagType ) 
{
    int         intVal = 0;
    long long   llVal = 0;
    std::string bufString;
    bool        bSuccess = false;

    if (bExpectContent)
    {
        if( tagType == TYPE_INT ) 
            bSuccess = getNodeContent( pNode, &intVal, TYPE_INT );
        else // longlong
            if( tagType == TYPE_LONGLONG ) 
                bSuccess = getNodeContent( pNode, &llVal, TYPE_LONGLONG );
        else // char
            if( tagType == TYPE_CHAR ) 
                bSuccess = getNodeContentStr( pNode, bufString );
        if (!bSuccess)
            return;
    }

    // process tag content and attributes
    switch( tag ) {
        case  TAG_READ_BUFFERS:
            setReadBuffers( pNode );
            break;
        case  TAG_COLUMN:
            setJobDataColumn( pNode, false );
            break;
        case  TAG_CREATE_DATE:
            fJob.createDate = bufString;
            break;
        case  TAG_CREATE_TIME:
            fJob.createTime = bufString;
            break;
        case  TAG_DEFAULT_COLUMN:
            setJobDataColumn( pNode, true );
            break;
        case  TAG_DESC:
            fJob.desc = bufString;
            break;
        case  TAG_ID:
            fJob.id = intVal;
            break;
        case  TAG_IGNORE_FIELD:
            setJobDataIgnoreField( );
            break;
        case  TAG_NAME:
            fJob.name = bufString;
            break;
        case  TAG_PATH:
            // no action necessary, but keep for backwards compatability
            break;
        case  TAG_TABLE:
            setJobDataTable( pNode );
            break;
        case  TAG_TYPE:
            // no action necessary, but keep for backwards compatability
            break;
        case  TAG_USER:
            fJob.userName = bufString;
            break;
        case  TAG_SCHEMA:
            setSchema( pNode );
            break;
        case TAG_WRITE_BUFFER_SIZE:
            fJob.writeBufferSize  = intVal;
            break;
        case TAG_DELIMITER:
        {
            const char* buf = bufString.c_str();
            if ((!strcmp(buf,"\\t")) ||
                (!strcmp(buf,"'\\t'")))
            {
                fJob.fDelimiter = '\t';
            }
            else
            {
                fJob.fDelimiter = bufString[0];
            }
            break;
        }
        case TAG_ENCLOSED_BY_CHAR:
        {
            fJob.fEnclosedByChar = bufString[0];
            break;
        }
        case TAG_ESCAPE_CHAR:
        {
            fJob.fEscapeChar = bufString[0];
            break;
        }

        default: break;
    }
}

//------------------------------------------------------------------------------
// Set table information parms.
// pNode - current node
//------------------------------------------------------------------------------
void XMLJob::setJobDataTable( xmlNode* pNode ) 
{
    int         intVal;
    std::string bufString;
    JobTable    curTable;

    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_ORIG_NAME], bufString ) )
        curTable.tblName = bufString;
    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_TBL_NAME], bufString ) )
        curTable.tblName = bufString;
    if (curTable.tblName.empty())
    {
        throw runtime_error(
            "Required table name attribute (tblName) missing from Table tag");
    }

    if( getNodeAttribute( pNode, xmlTagTable[TAG_TBL_OID], &intVal, TYPE_INT ) )
        curTable.mapOid = intVal;

    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_LOAD_NAME], bufString ) )
        curTable.loadFileName = bufString;

    if( getNodeAttribute( pNode, xmlTagTable[TAG_MAX_ERR_ROW], &intVal,
        TYPE_INT))
        curTable.maxErrNum = intVal;

    fJob.jobTableList.push_back( curTable );
}

//------------------------------------------------------------------------------
// Set column information parms.
// pNode - current node
// bDefaultCol - is this a <DefaultColumn> tag
//
// Note on Supported Tags: (Bug 2828)
// Note that the "notnull" and "defaultValue" attribute tags are not recognized
// by this function because by the time we added support for these tags, we had
// changed to only store the table and column names in the XML file.  Much of
// the functionality in setJobDataColumn() is only present to provide backwards
// compatability for an old Job XML file that a user might still be using.
//
// Any other new tags probably don't need adding to setJobDataColumn() either,
// for the same reason.
//------------------------------------------------------------------------------
void XMLJob::setJobDataColumn( xmlNode* pNode, bool bDefaultCol ) 
{
    int         intVal;
    std::string bufString;
    JobColumn   curColumn;

    if( fJob.jobTableList.size() == 0 )
        return;

    int tableNo = fJob.jobTableList.size() - 1;
    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_ORIG_NAME], bufString ) )
        curColumn.colName = bufString;
    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_COL_NAME], bufString ) )
        curColumn.colName = bufString;
    if (curColumn.colName.empty())
    {
        ostringstream oss;
        oss << "Required column name attribute (colName) missing from "
               "Column tag for table " <<
               fJob.jobTableList[tableNo].tblName;
        throw runtime_error( oss.str() );
    }

    if( getNodeAttribute( pNode, xmlTagTable[TAG_COL_OID], &intVal, TYPE_INT ) )
        curColumn.mapOid = intVal;

    if( getNodeAttribute( pNode, xmlTagTable[TAG_WIDTH], &intVal, TYPE_INT ) ) {
        curColumn.width = intVal;
        curColumn.definedWidth = intVal; //@Bug 3040 
    }

    if( getNodeAttribute( pNode, xmlTagTable[TAG_PRECISION], &intVal, TYPE_INT))
        curColumn.precision = intVal;

    if( getNodeAttribute( pNode, xmlTagTable[TAG_SCALE], &intVal, TYPE_INT ) )
        curColumn.scale = intVal;

    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_DATA_TYPE], bufString ) )
        curColumn.typeName = bufString;

    if( getNodeAttribute( pNode, xmlTagTable[TAG_COMPRESS_TYPE], &intVal,
        TYPE_INT))
    {
        curColumn.compressionType = intVal;
        curColumn.dctnry.fCompressionType = intVal;
    }

    if( getNodeAttribute( pNode, xmlTagTable[TAG_AUTOINCREMENT_FLAG],
        &intVal, TYPE_INT))
    {
        if (intVal)
            curColumn.autoIncFlag = true;
        else
            curColumn.autoIncFlag = false;
    }

#ifdef SKIP_AUTOI
    curColumn.autoIncFlag = false;
#endif

    if( getNodeAttributeStr( pNode, xmlTagTable[TAG_COL_TYPE], bufString ) ) {
        const char* buf = bufString.c_str();
        if( !strcmp( buf, "D" ) ) {
            curColumn.colType = 'D';

            // @Bug 2565: Retain dictionary width to use in truncating strings,
            // since BulkLoad eventually stores column token width in 'width'.
            curColumn.dctnryWidth = curColumn.width;

            if( getNodeAttribute( pNode,
                                  xmlTagTable[TAG_DVAL_OID],
                                  &intVal,
                                  TYPE_INT ) )
                curColumn.dctnry.dctnryOid = intVal;
        }
    }

    // This is a workaround that DBBuilder can not pass decimal type to XML file
    if( ( curColumn.typeName == ColDataTypeStr[CalpontSystemCatalog::INT] ||
          curColumn.typeName == ColDataTypeStr[CalpontSystemCatalog::BIGINT] ||
          curColumn.typeName == ColDataTypeStr[CalpontSystemCatalog::SMALLINT]||
          curColumn.typeName == ColDataTypeStr[CalpontSystemCatalog::TINYINT])&&
          curColumn.scale > 0 ) 
        curColumn.typeName = ColDataTypeStr[CalpontSystemCatalog::DECIMAL];
    // end of workaround

    // Initialize the saturation limits for this column
    initSatLimits( curColumn );

    // Save default columns in separate list, so that we can intentionally
    // add/keep them at the "end" of colList later, after all other columns.
    if (bDefaultCol) // temporarily save in separate list
    {
        curColumn.fFldColRelation = BULK_FLDCOL_COLUMN_DEFAULT;
        fDefaultColumns.push_back ( curColumn );
    }
    else
    {
        // Add to list of db columns to be loaded
        curColumn.fFldColRelation = BULK_FLDCOL_COLUMN_FIELD;
        fJob.jobTableList[tableNo].colList.push_back ( curColumn );

        // Add to combined field list of columns and ignored fields
        JobFieldRef fieldRef( BULK_FLDCOL_COLUMN_FIELD,
                              fJob.jobTableList[tableNo].colList.size()-1 );
        fJob.jobTableList[tableNo].fFldRefs.push_back( fieldRef  );
    }
}

//------------------------------------------------------------------------------
// Set column information parms for an input field that is to be ignored
//------------------------------------------------------------------------------
void XMLJob::setJobDataIgnoreField( )
{
    JobColumn curColumn;

    int tableNo = fJob.jobTableList.size() - 1;
    ostringstream oss;
    oss << "IgnoreField" << fJob.jobTableList[tableNo].fFldRefs.size()+1;
    curColumn.colName     = oss.str();

    // Add to list of ignored fields
    curColumn.fFldColRelation = BULK_FLDCOL_IGNORE_FIELD;
    fJob.jobTableList[tableNo].fIgnoredFields.push_back( curColumn );

    // Add to combined field list of columns and ignored fields
    JobFieldRef fieldRef( BULK_FLDCOL_IGNORE_FIELD,
                          fJob.jobTableList[tableNo].fIgnoredFields.size()-1 );
    fJob.jobTableList[tableNo].fFldRefs.push_back      ( fieldRef  );
}

//------------------------------------------------------------------------------
// Initialize the saturation limits for the specified column.
//------------------------------------------------------------------------------
void XMLJob::initSatLimits( JobColumn& curColumn ) const
{
    // If one of the integer types, we set the min/max saturation value.
    // For DECIMAL columns this will vary with the precision.
    if      ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::INT] ) {
        curColumn.fMinIntSat = MIN_INT;
        curColumn.fMaxIntSat = MAX_INT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UINT] ) {
        curColumn.fMinIntSat = MIN_UINT;
        curColumn.fMaxIntSat = MAX_UINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::BIGINT] ) {
        curColumn.fMinIntSat = MIN_BIGINT;
        curColumn.fMaxIntSat = MAX_BIGINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UBIGINT] ) {
        curColumn.fMinIntSat = MIN_UBIGINT;
        curColumn.fMaxIntSat = MAX_UBIGINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::SMALLINT] ) {
        curColumn.fMinIntSat = MIN_SMALLINT;
        curColumn.fMaxIntSat = MAX_SMALLINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::USMALLINT] ) {
        curColumn.fMinIntSat = MIN_USMALLINT;
        curColumn.fMaxIntSat = MAX_USMALLINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::TINYINT] ) {
        curColumn.fMinIntSat = MIN_TINYINT;
        curColumn.fMaxIntSat = MAX_TINYINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UTINYINT] ) {
        curColumn.fMinIntSat = MIN_UTINYINT;
        curColumn.fMaxIntSat = MAX_UTINYINT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::DECIMAL] ) {
        curColumn.fMinIntSat = -infinidb_precision[curColumn.precision];
        curColumn.fMaxIntSat = infinidb_precision[curColumn.precision];
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UDECIMAL] ) {
        curColumn.fMinIntSat = 0;
        curColumn.fMaxIntSat = infinidb_precision[curColumn.precision];
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::FLOAT] ) {
        curColumn.fMinDblSat = MIN_FLOAT;
        curColumn.fMaxDblSat = MAX_FLOAT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UFLOAT] ) {
        curColumn.fMinDblSat = 0.0;
        curColumn.fMaxDblSat = MAX_FLOAT;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::DOUBLE] ) {
        curColumn.fMinDblSat = MIN_DOUBLE;
        curColumn.fMaxDblSat = MAX_DOUBLE;
    }
    else if ( curColumn.typeName ==
              ColDataTypeStr[CalpontSystemCatalog::UDOUBLE] ) {
        curColumn.fMinDblSat = 0.0;
        curColumn.fMaxDblSat = MAX_DOUBLE;
    }
}

//------------------------------------------------------------------------------
// Set Read Buffers attributes
// pNode - current node
//------------------------------------------------------------------------------
void XMLJob::setReadBuffers( xmlNode* pNode ) 
{
    int intVal = 0;

    if(getNodeAttribute(pNode,
                        xmlTagTable[TAG_NO_OF_READ_BUFFERS],
                        &intVal,
                        TYPE_INT ))
        fJob.numberOfReadBuffers = intVal;

    if(getNodeAttribute(pNode,
                        xmlTagTable[TAG_READ_BUFFER_SIZE],
                        &intVal,
                        TYPE_INT ))
        fJob.readBufferSize = intVal;
}

//------------------------------------------------------------------------------
// Set Schema attributes
// pNode - current node
//------------------------------------------------------------------------------
void XMLJob::setSchema( xmlNode* pNode ) 
{
    std::string bufString;

    if( getNodeAttributeStr( pNode,
                          xmlTagTable[TAG_SCHEMA_NAME],
                          bufString ) )
        fJob.schema = bufString;
}

//------------------------------------------------------------------------------
// Transfer any/all <DefaultColumn> columns from temporary fDefaultColumns, to
// the end of the column/field lists.
// It is assumed that we are working with the last table in jobTableList.
// Then get additional information from system catalog to finish populating
// our Job structs with all the table and column attributes we need.
//------------------------------------------------------------------------------
void XMLJob::postProcessTableNode()
{
    bool bValidateNoDefColWithoutDefValue = false;
    if (fDefaultColumns.size() > 0)
    {
        bValidateNoDefColWithoutDefValue = true;
        int tableNo = fJob.jobTableList.size() - 1;

        for (unsigned k=0; k<fDefaultColumns.size(); k++)
        {
            // Add to list of db columns to be loaded
            fJob.jobTableList[tableNo].colList.push_back( fDefaultColumns[k] );

            // Add to combined list of columns and ignored fields
            JobFieldRef fieldRef( BULK_FLDCOL_COLUMN_DEFAULT,
                                  fJob.jobTableList[tableNo].colList.size()-1 );
            fJob.jobTableList[tableNo].fFldRefs.push_back( fieldRef );
        }
        fDefaultColumns.clear();
    }

    // Supplement xml file contents with information from syscat
    execplan::CalpontSystemCatalog::RIDList colRidList;
    fillInXMLDataAsLoaded( colRidList );

    // After getting all the system catalog information...
    // Validate that if there are any <DefaultColumn> tags for a NotNull
    // column, that the column is defined as NotNull With Default.
    if (bValidateNoDefColWithoutDefValue)
    {
        int tableNo = fJob.jobTableList.size() - 1;

        for (unsigned int iCol=0;
            iCol<fJob.jobTableList[tableNo].colList.size(); iCol++)
        {
            JobColumn& col = fJob.jobTableList[tableNo].colList[iCol];

            if (col.fFldColRelation == BULK_FLDCOL_COLUMN_DEFAULT)
            {
                if ( (col.fNotNull) && (!col.fWithDefault) )
                {
                    std::ostringstream oss;
                    oss << "Column " << col.colName << " in table " <<
                        fJob.jobTableList[tableNo].tblName << " is NotNull "
                        "w/o default; cannot be used with <DefaultColumn>";
                    throw std::runtime_error( oss.str() );
                }
            }
        }
    }

    // Make sure all Columns in the DB are counted for with <Column> or
    // <DefaultColumn> tags (unless validate is disabled)
    if (fValidateColList)
        validateAllColumnsHaveTags( colRidList );
}

//------------------------------------------------------------------------------
// Use the table and column names from the last <Table> just loaded, to
// collect the remaining information from the system catalog, in order to
// populate the JobColumn structure.
//------------------------------------------------------------------------------
void XMLJob::fillInXMLDataAsLoaded(
    execplan::CalpontSystemCatalog::RIDList& colRidList)
{
    boost::shared_ptr<execplan::CalpontSystemCatalog> cat =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
        BULK_SYSCAT_SESSION_ID);
    cat->identity(execplan::CalpontSystemCatalog::EC);

    // Get the table and column attributes for the last <Table> processed
    unsigned int iTbl = fJob.jobTableList.size()-1;
    JobTable& tbl = fJob.jobTableList[iTbl];

    std::string tblName;
    string::size_type startName = tbl.tblName.rfind('.');
    if (startName == string::npos)
        tblName.assign( tbl.tblName );
    else
        tblName.assign( tbl.tblName.substr(startName+1) );

    execplan::CalpontSystemCatalog::TableName table(
        fJob.schema, tblName );
    if (fJob.jobTableList[iTbl].mapOid == 0)
    {
        execplan::CalpontSystemCatalog::OID tblOid =
            cat->tableRID(table).objnum;
        tbl.mapOid = tblOid;
    }

    // This call is made to improve performance.
    // The call forces all the column information for this table to be
    // cached at one time, instead of doing it piece-meal through repeated
    // calls to lookupOID().
    colRidList = cat->columnRIDs(table, true);

    // Loop through the columns to get the column attributes
    for (unsigned int iCol=0;
        iCol<fJob.jobTableList[iTbl].colList.size(); iCol++)
    {
        JobColumn& col = fJob.jobTableList[iTbl].colList[iCol];

        if (col.mapOid == 0)
        {
            execplan::CalpontSystemCatalog::TableColName column;
            column.schema = fJob.schema;
            column.table  = tblName;
            column.column = col.colName;
            execplan::CalpontSystemCatalog::OID colOid =
                cat->lookupOID( column );
            if (colOid < 0)
            {
                ostringstream oss;
                oss << "Column OID lookup failed for: " << column;
                throw runtime_error( oss.str() );
            }
            col.mapOid    = colOid;

            execplan::CalpontSystemCatalog::ColType colType =
                cat->colType( col.mapOid );
                
            col.width                   = colType.colWidth;
            col.definedWidth            = colType.colWidth;
            if ((colType.scale > 0) ||
                (colType.colDataType ==
                 execplan::CalpontSystemCatalog::DECIMAL) ||
                (colType.colDataType ==
                 execplan::CalpontSystemCatalog::UDECIMAL))
            {
                col.precision           = colType.precision;
                col.scale               = colType.scale;
            }
            col.typeName                = ColDataTypeStr[colType.colDataType];
            col.compressionType         = colType.compressionType;
            col.dctnry.fCompressionType = colType.compressionType;
            if (colType.autoincrement)
                col.autoIncFlag         = true;
            else
                col.autoIncFlag         = false;
#ifdef SKIP_AUTOI
            col.autoIncFlag             = false;
#endif

            // Initialize NotNull and Default Value (based on data type)
            fillInXMLDataNotNullDefault( tbl.tblName, colType, col );
            
            if (colType.ddn.dictOID > 0)
            {
                col.colType             = 'D';
                col.dctnryWidth         = colType.colWidth;
                col.dctnry.dctnryOid    = colType.ddn.dictOID;
            }

            // @bug3801: For backwards compatability, we treat
            // integer types with nonzero 0 scale as decimal if scale > 0
            if( ((col.typeName ==
                  ColDataTypeStr[CalpontSystemCatalog::INT])      ||
                 (col.typeName ==
                  ColDataTypeStr[CalpontSystemCatalog::BIGINT])   ||
                 (col.typeName ==
                  ColDataTypeStr[CalpontSystemCatalog::SMALLINT]) ||
                 (col.typeName ==
                  ColDataTypeStr[CalpontSystemCatalog::TINYINT])) &&
                 (col.scale > 0) )
            {
                col.typeName = ColDataTypeStr[CalpontSystemCatalog::DECIMAL];
            }

            // Initialize the saturation limits for this column
            initSatLimits( col );
        }
    } // end of loop through columns
}

//------------------------------------------------------------------------------
// Using information from the system catalog (in colType), fill in the
// applicable NotNull Default values into the specified JobColumn.
//------------------------------------------------------------------------------
void XMLJob::fillInXMLDataNotNullDefault(
    const std::string& fullTblName,
    execplan::CalpontSystemCatalog::ColType& colType,
    JobColumn& col )
{
    const std::string col_defaultValue(colType.defaultValue);

    if (colType.constraintType ==
        execplan::CalpontSystemCatalog::NOTNULL_CONSTRAINT)
    {
        col.fNotNull            = true;
        if (!col_defaultValue.empty())
            col.fWithDefault    = true;
    }
    else if (colType.constraintType ==
        execplan::CalpontSystemCatalog::DEFAULT_CONSTRAINT)
    {
        col.fWithDefault        = true;
    }

    if (col.fWithDefault)
    {
        bool bDefaultConvertError = false;

        // Convert Default Value.
        // We go ahead and report basic format conversion error;
        // but we don't do complete validation (like checking to see
        // if the default is too large for the given integer type),
        // because we assume DDL is fully validating the default value.
        switch (colType.colDataType)
        {
            case execplan::CalpontSystemCatalog::BIT:
            case execplan::CalpontSystemCatalog::TINYINT:
            case execplan::CalpontSystemCatalog::SMALLINT:
            case execplan::CalpontSystemCatalog::MEDINT:
            case execplan::CalpontSystemCatalog::INT:
            case execplan::CalpontSystemCatalog::BIGINT:
            {
                errno = 0;
                col.fDefaultInt = strtoll(col_defaultValue.c_str(),0,10);
                if (errno == ERANGE)
                    bDefaultConvertError = true;
                break;
            }

            case execplan::CalpontSystemCatalog::UTINYINT:
            case execplan::CalpontSystemCatalog::USMALLINT:
            case execplan::CalpontSystemCatalog::UMEDINT:
            case execplan::CalpontSystemCatalog::UINT:
            case execplan::CalpontSystemCatalog::UBIGINT:
            {
                errno = 0;
                col.fDefaultUInt = strtoull(col_defaultValue.c_str(),0,10);
                if (errno == ERANGE)
                    bDefaultConvertError = true;
                break;
            }

            case execplan::CalpontSystemCatalog::DECIMAL:
            case execplan::CalpontSystemCatalog::UDECIMAL:
            {
                col.fDefaultInt = Convertor::convertDecimalString(
                    col_defaultValue.c_str(),
                    col_defaultValue.length(),
                    colType.scale);           
                if (errno == ERANGE)
                    bDefaultConvertError = true;
                break;
            }

            case execplan::CalpontSystemCatalog::DATE:
            {
                int convertStatus;
                int32_t dt =
                    dataconvert::DataConvert::convertColumnDate(
                    col_defaultValue.c_str(),
                    dataconvert::CALPONTDATE_ENUM, convertStatus,
                    col_defaultValue.length() );
                if (convertStatus != 0)
                    bDefaultConvertError = true;
                col.fDefaultInt = dt;
                break;
            }

            case execplan::CalpontSystemCatalog::DATETIME:
            {
                int convertStatus;
                int64_t dt =
                    dataconvert::DataConvert::convertColumnDatetime(
                    col_defaultValue.c_str(),
                    dataconvert::CALPONTDATETIME_ENUM, convertStatus,
                    col_defaultValue.length() );
                if (convertStatus != 0)
                    bDefaultConvertError = true;
                col.fDefaultInt = dt;
                break;
            }

            case execplan::CalpontSystemCatalog::FLOAT:
            case execplan::CalpontSystemCatalog::DOUBLE:
            case execplan::CalpontSystemCatalog::UFLOAT:
            case execplan::CalpontSystemCatalog::UDOUBLE:
            {
                errno = 0;
                col.fDefaultDbl = strtod(col_defaultValue.c_str(),0);
                if (errno == ERANGE)
                    bDefaultConvertError = true;
                break;
            }

            default:
            {
                col.fDefaultChr = col_defaultValue;
                break;
            }
        }

        if (bDefaultConvertError)
        {
            std::ostringstream oss;
            oss << "Column " << col.colName << " in table " << fullTblName <<
                " has an invalid default value in system catalog.";
            throw std::runtime_error( oss.str() );
        }
    }
}

//------------------------------------------------------------------------------
// Use the table and column names from the last <Table> just loaded, to
// validate that all the columns have a <Column> or <DefaultColumn> tag
// present in the job XML file.
//------------------------------------------------------------------------------
void XMLJob::validateAllColumnsHaveTags(
    const execplan::CalpontSystemCatalog::RIDList& colRidList) const
{
    // Validate column list for the last <Table> processed
    unsigned int iTbl = fJob.jobTableList.size()-1;
    const JobTable& tbl = fJob.jobTableList[iTbl];

    std::string tblName;
    string::size_type startName = tbl.tblName.rfind('.');
    if (startName == string::npos)
        tblName.assign( tbl.tblName );
    else
        tblName.assign( tbl.tblName.substr(startName+1) );

    try
    {
        // Loop through column tags, saving col OIDs to a std::set for lookups
        std::set<execplan::CalpontSystemCatalog::OID> colOIDList;
        typedef std::set<execplan::CalpontSystemCatalog::OID>::iterator SetIter;
        std::pair<SetIter,bool> retVal;
        for (unsigned int iCol=0;
            iCol<fJob.jobTableList[iTbl].colList.size(); iCol++)
        {
            const JobColumn& col = fJob.jobTableList[iTbl].colList[iCol];
            retVal = colOIDList.insert( col.mapOid );
            if (!retVal.second)
            {
                boost::shared_ptr<execplan::CalpontSystemCatalog> cat =
                    execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
                    BULK_SYSCAT_SESSION_ID);
                cat->identity(execplan::CalpontSystemCatalog::EC);

                execplan::CalpontSystemCatalog::TableColName dbColName =
                    cat->colName( col.mapOid );
                std::ostringstream oss;
                oss << "Column " << dbColName.column << " referenced in Job XML"
                    " file more than once.";
                throw std::runtime_error( oss.str() );
            }
        }

        SetIter pos;

        // Loop thru cols in system catalog and verify that each one has a tag
        execplan::CalpontSystemCatalog::RIDList::const_iterator rid_iterator =
            colRidList.begin();
        while (rid_iterator != colRidList.end())
        {
            pos = colOIDList.find( rid_iterator->objnum );
            if (pos != colOIDList.end())
            {
                colOIDList.erase( pos ); // through with this column, so delete
            }
            else
            {
                boost::shared_ptr<execplan::CalpontSystemCatalog> cat =
                    execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
                    BULK_SYSCAT_SESSION_ID);
                cat->identity(execplan::CalpontSystemCatalog::EC);

                execplan::CalpontSystemCatalog::TableColName dbColName =
                    cat->colName( rid_iterator->objnum );
                std::ostringstream oss;
                oss << "No tag present in Job XML file for DB column: " <<
                    dbColName.column;
                throw std::runtime_error( oss.str() );
            }

            ++rid_iterator;
        }
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Error validating column list for table " <<
            fJob.schema << '.' << tblName << "; " << ex.what();
        throw std::runtime_error( oss.str() );
    }
    catch (...)
    {
        std::ostringstream oss;
        oss << "Unknown Error validating column list for table " <<
            fJob.schema << '.' << tblName;
        throw std::runtime_error( oss.str() );
    }
}

//------------------------------------------------------------------------------
// Generate a permanent or temporary Job XML file name path.
// sXMLJobDir Command line override for complete Job directory path
// jobDIr     Job subdirectory under default <BulkRoot> path
// jobId      Job ID
// bTempFile  Are we creating a temporary Job Xml File
// schmaName  If temp file, this is schema name to use
// tableName  If temp file, this is the table name to use
// xmlDirPath The complete Job XML file path that is constructed
// errMsg     Relevant error message if return value is not NO_ERROR.
//------------------------------------------------------------------------------
/* static */
int XMLJob::genJobXMLFileName(
    const string& sXMLJobDir,
    const string& jobDir,
    const string& jobId,
    bool          bTempFile,
    const string& schemaName,
    const string& tableName,
    boost::filesystem::path& xmlFilePath,
    string& errMsg,
    std::string&	   tableOIDStr )
{
    // get full file directory path for XML job description file
    if (sXMLJobDir.empty())
    {
        xmlFilePath  = Config::getBulkRoot();
        xmlFilePath /= jobDir;
    }
    else
    {
        xmlFilePath = sXMLJobDir;

        //If filespec doesn't begin with a '/' (i.e. it's not an absolute path),
        // attempt to make it absolute so that we can log the full pathname.
        if (!xmlFilePath.has_root_path())
        {
#ifdef _MSC_VER
            // nothing else to do
#else
            char cwdPath[4096];
            getcwd(cwdPath, sizeof(cwdPath));
            string trailingPath(xmlFilePath.string());
            xmlFilePath  = cwdPath;
            xmlFilePath /= trailingPath;
#endif
        }
    }

    // Append the file name to the directory path
    string jobFileName;
    if (bTempFile)
    {
        // Create tmp directory if does not exist
        RETURN_ON_ERROR( createTempJobDir( xmlFilePath.string(), errMsg ) );
		jobFileName +=tableOIDStr;
        //jobFileName += schemaName;
       // jobFileName += '_';
       // jobFileName += tableName;
        jobFileName += "_D";

        string now(boost::posix_time::to_iso_string(
            boost::posix_time::second_clock::local_time()));
        jobFileName += now.substr(0, 8);
        jobFileName += "_T";
        jobFileName += now.substr(9, 6);
        jobFileName += '_';
    }

    jobFileName += "Job_";
    jobFileName += jobId;
    jobFileName += ".xml";

    xmlFilePath /= jobFileName;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Create directory for temporary XML job description files.
// OAM restart should delete any/all files in this directory.
//------------------------------------------------------------------------------
/* static */
int XMLJob::createTempJobDir( const string& xmlFilePath,
                              string& errMsg )
{
    boost::filesystem::path pathDir(xmlFilePath);

    // create temp directory for XML job file if it does not exist
    try
    {
        if ( !boost::filesystem::exists( xmlFilePath ) )
        {
            string boostErrString;
            try
            {
                boost::filesystem::create_directories(pathDir);
            }
            catch (exception& ex)
            {
                // ignore exception for now; we may have just had a
                // race condition where 2 jobs were creating dirs.
                boostErrString = ex.what();
            }

            if ( !boost::filesystem::exists( xmlFilePath ) )
            {
                ostringstream oss;
                oss << "Error creating XML temp job file directory(1) " <<
                    xmlFilePath << "; " << boostErrString;
                errMsg = oss.str();

                return ERR_DIR_CREATE;
            }
        }
    }
    catch (exception& ex)
    {
        ostringstream oss;
        oss << "Error creating XML temp job file directory(2) " <<
            xmlFilePath << "; " << ex.what();
        errMsg = oss.str();

        return ERR_DIR_CREATE;
    }

    if (!boost::filesystem::is_directory(pathDir) )
    {
        ostringstream oss;
        oss << "Error creating XML temp job file directory " <<
            xmlFilePath << "; path already exists as non-directory" << endl;
        errMsg = oss.str();

        return ERR_DIR_CREATE;
    }

    return NO_ERROR;
}

} //end of namespace

