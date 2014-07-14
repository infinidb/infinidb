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
* $Id: we_xmljob.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_XMLJOB_H_
#define _WE_XMLJOB_H_

#include <iostream>
#include <string>
#include <time.h>
#include <boost/filesystem/path.hpp>

#include "we_xmlop.h"
#include "calpontsystemcatalog.h"

#if defined(_MSC_VER) && defined(xxxWRITEENGINEXMLJOB_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
class Log;
struct JobColumn;

/** @brief Reads Job XML file and loads into a "Job" object for use by
 *  cpimport.bin.
 *
 */
class XMLJob : public XMLOp
{

public:
    /**
     * @brief Constructor
     */
    EXPORT XMLJob();

    /**
     * @brief Default Destructor
     */
    EXPORT ~XMLJob();

    /**
     * @brief Utility to generate a full path name for a Job XML file name
     * @param sXMLJobDir Command line override for complete Job directory path
     * @param jobDIr     Job subdirectory under default <BulkRoot> path
     * @param jobId      Job ID
     * @param bTempFile  Are we creating a temporary Job XML File name
     * @param schmaName  If temp file, this is schema name to use
     * @param tableName  If temp file, this is the table name to use
     * @param xmlDirPath The complete Job XML file path that is constructed
     * @param errMsg     Error message if return value is not NO_ERROR
     */
    EXPORT static int genJobXMLFileName(
        const std::string& sXMLJobDir,
        const std::string& jobDir,
        const std::string& jobId,
        bool               bTempFile,
        const std::string& schemaName,
        const std::string& tableName,
        boost::filesystem::path& xmlDirPath,
        std::string&       errMsg,
        std::string&	   tableOIDStr );

    /**
     * @brief Get job structure
     */
    const Job&     getJob()  const { return fJob; }

    /**
     * @brief Load job information
     * @param fileName   Name of Job XML file to be read
     * @param bTempFile  Are we creating a temporary Job XML File that will be
     *                   deleted by the destructor
     * @param bValidateColumnList Validate that all columns have an XML tag
     * @param errMsg     Error message if return value is not NO_ERROR
     */
    EXPORT int     loadJobXmlFile( const std::string& fileName,
                                   bool bTempFile,
                                   bool bValidateColumnList,
                                   std::string& errMsg );

    /**
     * @brief Print job related information
     * @param logger Log object that is to receive the print output
     */
    EXPORT void    printJobInfo(Log& logger) const;

    /**
     * @brief Print abbreviated job related information
     * @param logger Log object that is to receive the print output
     */
    EXPORT void    printJobInfoBrief(Log& logger) const;

    /**
     * @brief Process node 
     * @param pParentNode Node to be parsed from XML
     */
    EXPORT bool    processNode( xmlNode* pParentNode );

private:
    void           setJobData( xmlNode* pNode,
                               const xmlTag tag,
                               bool  bExpectContent,
                               XML_DTYPE tagType );
    void           setJobDataColumn( xmlNode* pNode, bool bDefaultCol );
    void           setJobDataIgnoreField ( );
    void           setJobDataTable ( xmlNode* pNode );
    void           setReadBuffers  ( xmlNode* pNode );
    void           setSchema       ( xmlNode* pNode );
    void           initSatLimits( JobColumn& column ) const;
    void           fillInXMLDataAsLoaded(
                     execplan::CalpontSystemCatalog::RIDList& colRidList);
    void           fillInXMLDataNotNullDefault(
                     const std::string& fullTblName,
                     execplan::CalpontSystemCatalog::ColType& colType,
                     JobColumn& col );
    void           validateAllColumnsHaveTags( const
                     execplan::CalpontSystemCatalog::RIDList& colRidList) const;
    void           postProcessTableNode( );
    static int     createTempJobDir( const std::string& xmlFilePath,
                                     std::string& errMsg );

    Job            fJob;                     // current job xml 

    DebugLevel     fDebugLevel;              // internal use debug level
    bool           fDeleteTempFile;          // delete tmp jobfile in destructor
    std::string    fJobFileName;             // job file name
    JobColList     fDefaultColumns;          // temporary list of default cols
                                             //   for table node being processed
    bool           fValidateColList;         // Validate all cols have XML tag
};

} //end of namespace

#undef EXPORT

#endif // _WE_XMLJOB_H_
