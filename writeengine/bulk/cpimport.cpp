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
* $Id: cpimport.cpp 4702 2013-07-08 20:06:14Z bpaul $
*
*******************************************************************************/

#include <iostream>
#include <sstream>
#include <fstream>
#include <clocale>

#include <sys/types.h>
#include <unistd.h>
#include <csignal>
#include <cstring>
#include <string>
#include <cerrno>
#include <cstdlib>
#include <sys/time.h>
#ifndef _MSC_VER
#include <sys/resource.h>
#else
#include <cstdio>
#endif
#include <boost/filesystem/path.hpp>
#include "idberrorinfo.h"
#include "we_simplesyslog.h"
#include "we_bulkload.h"
#include "we_bulkstatus.h"
#include "we_config.h"
#include "we_xmljob.h"
#include "we_xmlgenproc.h"
#include "we_tempxmlgendata.h"
#include "liboamcpp.h"
#include "utils_utf8.h"

using namespace std;
using namespace WriteEngine;
using namespace execplan;

namespace
{
    char* pgmName = 0;
    const std::string IMPORT_PATH_CWD  (".");
    bool  bDebug  = false;

    //@bug 4643: cpimport job ended during setup w/o any err msg.
    //           Added a try/catch with logging to main() in case 
    //           the process was dying with an uncaught exception.
    enum TASK
    {
        TASK_CMD_LINE_PARSING     = 1,
        TASK_INIT_CONFIG_CACHE    = 2,
        TASK_BRM_STATE_READY      = 3,
        TASK_BRM_STATE_READ_WRITE = 4,
        TASK_SHUTDOWN_PENDING     = 5,
        TASK_SUSPEND_PENDING      = 6,
        TASK_ESTABLISH_JOBFILE    = 7,
        TASK_LOAD_JOBFILE         = 8,
        TASK_PROCESS_DATA         = 9
    };
    const char* taskLabels[] =
    {
        "",
        "parsing command line options",
        "initializing config cache",
        "checking BRM Ready state",
        "checking BRM Read/Write state",
        "checking for pending shutdown",
        "checking for pending suspend",
        "establishing job file",
        "loading job file",
        "processing data"
    };
}

//------------------------------------------------------------------------------
// Print command line usage
//------------------------------------------------------------------------------
void printUsage()
{
    cerr << endl << "Simple usage using positional parameters "
      "(no XML job file):" << endl <<
      "    cpimport.bin dbName tblName [loadFile] [-j jobID] " << endl <<
      "    [-h] [-r readers] [-w parsers] [-s c] [-f path] " << endl <<
      "    [-b readBufs] [-c readBufSize] [-e maxErrs] [-B libBufSize]" <<endl<<
      "    [-n NullOption] [-E encloseChar] [-C escapeChar] [-S]" << 
      "[-d debugLevel] [-i]" << endl;

    cerr << endl << "Traditional usage without positional parameters "
      "(XML job file required):" << endl <<
      "    cpimport.bin -j jobID " << endl <<
      "    [-h] [-r readers] [-w parsers] [-s c] [-f path]" << endl <<
      "    [-b readBufs] [-c readBufSize] [-e maxErrs] [-B libBufSize]" <<endl<<
      "    [-n NullOption] [-E encloseChar] [-C escapeChar] [-S]" <<
      "[-d debugLevel] [-i]" << endl <<
      "    [-p path] [-l loadFile]" << endl << endl;

    cerr << "    Positional parameters:" << endl <<
        "        dbName    Name of database to load" << endl <<
        "        tblName   Name of table to load"   << endl <<
        "        loadFile  Optional input file name in current directory, " <<
        "unless a fully" << endl <<
        "                  qualified name is given.  If not given, " << 
        "input read from stdin." << endl << endl;

    cerr << "    Options:" << endl <<
        "        -b Number of read buffers" << endl <<
        "        -c Application read buffer size (in bytes)" << endl <<
        "        -d Print different level (1-3) debug message " << endl <<
        "        -e Maximum number of allowable errors per table" << endl <<
        "        -f Data file directory path; " << endl <<
        "           In simple usage:" << endl <<
        "             Default is current working directory." << endl <<
        "             -f option only applies if loadFile is specified." <<endl<<
        "           In traditional usage: " << endl <<
        "             Default is <BulkRoot>/data/import." << endl <<
        "             'STDIN' (all caps) redirects input from stdin." << endl <<
        "        -h Print this message" << endl <<
        "        -i Print extended info to console, else this info only goes "
        "to log file." << endl <<
        "        -j Job id.  In simple usage, default is the table OID."<<endl<<
        "        -l Name of input file to be loaded, relative to -f path,"
        << endl <<
        "           unless a fully qualified input file name is given."<<endl<<
        "        -n NullOption (0-treat the string NULL as data (default);" <<
        endl <<
        "                       1-treat the string NULL as a NULL value)" <<
        endl <<
        "        -p Path for XML job description file" << endl <<
        "        -r Number of readers" << endl <<
        "        -s 'c' is the delimiter between column values" << endl <<
        "        -w Number of parsers" << endl <<
        "        -B I/O library read buffer size (in bytes)" << endl <<
        "        -E Enclosed by character if field values are enclosed"<<endl<<
        "        -C Escape character used in conjunction with 'enclosed by'" <<
        endl <<
        "           character (default is '\\')" << endl << 
        "        -S Treat string truncations as errors" << endl << endl;

    cerr << "    Example1:" << endl <<
        "        cpimport.bin -j 1234" << endl <<
        "    Example2: Some column values are enclosed within double quotes." <<
        endl <<
        "        cpimport.bin -j 3000 -E '\"'" << endl <<
        "    Example3: Import a nation table without a Job XML file"<< endl <<
        "        cpimport.bin -j 301 tpch nation nation.tbl" << endl;

    exit( EXIT_SUCCESS );
}

//------------------------------------------------------------------------------
// Signal handler to catch SIGTERM signal to terminate the process
//------------------------------------------------------------------------------
void handleSigTerm(int i)
{
    std::cout << "Received SIGTERM to terminate the process..." << std::endl;
    BulkStatus::setJobStatus( EXIT_FAILURE );
}

//------------------------------------------------------------------------------
// Signal handler to catch Control-C signal to terminate the process
//------------------------------------------------------------------------------
void handleControlC(int i)
{
    std::cout << "Received Control-C to terminate the process..." << std::endl;
    BulkStatus::setJobStatus( EXIT_FAILURE );
}

//------------------------------------------------------------------------------
// If error occurs during startup, this function is called to log the specified
// message and terminate the process.
//------------------------------------------------------------------------------
void startupError( const std::string& errMsg, bool showHint )
{
    // Log to console
    cerr << errMsg << endl;
    if (showHint)
    {
        std::ostringstream oss;
        oss  << "Try '" << pgmName << " -h' for more information.";
        cerr << oss.str() << endl;
    }

    // Log to syslog
    logging::Message::Args errMsgArgs;
    errMsgArgs.add(errMsg);
    SimpleSysLog::instance()->logMsg(
        errMsgArgs, 
        logging::LOG_TYPE_ERROR,
        logging::M0087);

    std::string jobIdStr("0");
    logging::Message::Args endMsgArgs;
    endMsgArgs.add(jobIdStr);
    endMsgArgs.add("FAILED");
    SimpleSysLog::instance()->logMsg(
        endMsgArgs,
        logging::LOG_TYPE_INFO,
        logging::M0082);

    exit( EXIT_FAILURE );
}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------
void setupSignalHandlers()
{
#ifdef _MSC_VER
    //FIXME
#else
    struct sigaction ign;

    // Ignore SIGPIPE signal
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ign, 0);

	//Ignore SIGHUP signals
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigaction(SIGHUP, &ign, 0);

    // @bug 4344 enable Control-C by disabling this section of code
    // Ignore SIGINT (Control-C) signal
    //memset(&ign, 0, sizeof(ign));
    //ign.sa_handler = SIG_IGN;
    //sigaction(SIGINT, &ign, 0);

    // @bug 4344 enable Control-C by adding this section of code
    // catch Control-C signal to terminate the program
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = handleControlC;
    sigaction(SIGINT, &act, 0);

    // catch SIGTERM signal to terminate the program
    memset(&act, 0, sizeof(act));
    act.sa_handler = handleSigTerm;
    sigaction(SIGTERM, &act, 0);
#endif
}

//------------------------------------------------------------------------------
// Parse the command line arguments
//------------------------------------------------------------------------------
void parseCmdLineArgs(
    int          argc,
    char**       argv,
    BulkLoad&    curJob,
    std::string& sJobIdStr,
    std::string& sXMLJobDir,
    std::string& sModuleIDandPID,
    bool&        bLogInfo2ToConsole,
    std::string& xmlGenSchema,
    std::string& xmlGenTable,
    bool&        bValidateColumnList )
{
    std::string importPath;
    std::string rptFileName;
    int         option;
    bool        bImportFileArg   = false;
    BulkModeType bulkMode = BULK_MODE_LOCAL;

    while( (option=getopt(
        argc,argv,"b:c:d:e:f:hij:kl:m:n:p:r:w:s:B:C:DE:P:R:X:S")) != EOF )
    {
        switch(option)
        {
            case 'b':                                // -b: no. of read buffers
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -b is invalid or out of range."), true );
                }

                int noOfReadBuffers = lValue;
                curJob.setReadBufferCount( noOfReadBuffers );
                break;
            }

            case 'c':                                // -c: read buffer size
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -c is invalid or out of range."), true );
                }

                int readBufferSize = lValue;
                curJob.setReadBufferSize( readBufferSize );
                break;
            }

            case 'd':                                // -d: debug level
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -d is invalid or out of range."), true );
                }

                int debugLevel = lValue;
                if( debugLevel > 0 && debugLevel <= 3 ) 
                {
                    bDebug = true;
                    curJob.setAllDebug( (DebugLevel) debugLevel );
                    cout << "\nDebug level is set to " << debugLevel << endl;
                }
                break;
            }

            case 'e':                                // -e: max allowed errors
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 0) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -e is invalid or out of range."), true );
                }

                int maxErrors = lValue;
                curJob.setMaxErrorCount( maxErrors );
                break;
            }

            case 'f':                                // -f: import path
            {
                importPath = optarg;
                std::string setAltErrMsg;
                if (curJob.setAlternateImportDir(importPath,
                    setAltErrMsg) != NO_ERROR)
                    startupError( setAltErrMsg, false );
                break;
            }

            case 'h':                                // -h: help
            {
                printUsage();
            }

            case 'i':                                // -i: log info to console
            {
                bLogInfo2ToConsole = true;
                break;
            }

            case 'j':                                // -j: jobID
            {
                sJobIdStr = optarg;
                break;
            }

            case 'k':                  // -k: hidden option to keep (not delete)
            {                          //     bulk rollback meta-data files
                curJob.setKeepRbMetaFiles( true );
                break;
            }

            case 'l':                                // -l: import load file(s)
            {
                bImportFileArg = true;
                curJob.addToCmdLineImportFileList( std::string(optarg) );
                break;
            }

            case 'm':                                // -m: bulk load mode
            {
                bulkMode = (BulkModeType)atoi( optarg );
                if ((bulkMode != BULK_MODE_REMOTE_SINGLE_SRC)   &&
                    (bulkMode != BULK_MODE_REMOTE_MULTIPLE_SRC) &&
                    (bulkMode != BULK_MODE_LOCAL))
                {
                    startupError ( std::string(
                        "Invalid bulk mode; can be 1,2, or 3"), true );
                }
                break;
            }

            case 'n':                                // -n: treat "NULL" as null
            {
                int nullStringMode = atoi( optarg );
                if ((nullStringMode != 0) &&
                    (nullStringMode != 1))
                {
                    startupError ( std::string(
                        "Invalid NULL option; value can be 0 or 1"), true );
                }
                if (nullStringMode)
                    curJob.setNullStringMode(true);
                else
                    curJob.setNullStringMode(false);
                break;
            }

            case 'p':                                // -p: Job XML path
            {
                sXMLJobDir = optarg;
                break;
            }

            case 'r':                                // -r: num read threads
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -r is invalid or out of range."), true );
                }

                int numOfReaders = lValue;
#if !defined(__LP64__) && !defined(_MSC_VER)
                if (numOfReaders > 1)
                {
                    cerr << "Note: resetting number of read threads to maximum"
                        << endl;
                    numOfReaders = 1;
                }
#endif
                curJob.setNoOfReadThreads(numOfReaders);
                cout << "number of read threads : " << numOfReaders << endl;
                break;
            }

            case 's':                                // -s: column delimiter
            {
                char delim;
                if (!strcmp(optarg,"\\t"))
                {
                    delim = '\t';
                    cout << "Column delimiter : " << "\\t" << endl;
                }
                else
                {
                    delim = optarg[0];
                    if (delim == '\t') // special case to print a <TAB>
                        cout << "Column delimiter : '\\t'" << endl;
                    else
                        cout << "Column delimiter : " << delim << endl;
                }
                curJob.setColDelimiter( delim );
                break;
            }

            case 'w':                                // -w: num parse threads
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -w is invalid or out of range."), true );
                }

                int numOfParser = lValue;
#if !defined(__LP64__) && !defined(_MSC_VER)
                if (numOfParser > 3)
                {
                    cerr << "Note: resetting number of parse threads to maximum"
                         << endl;
                    numOfParser = 3;
                }
#endif
                curJob.setNoOfParseThreads( numOfParser );
                cout << "number of parse threads : " << numOfParser << endl;
                break;
            }

            case 'B':                                // -B: setvbuf read size
            {
                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if ((errno != 0) ||
                    (lValue < 1) || (lValue > INT_MAX))
                {
                    startupError ( std::string(
                        "Option -B is invalid or out of range."), true );
                }

                int vbufReadSize = lValue;
                curJob.setVbufReadSize( vbufReadSize );
                break;
            }

            case 'C':                                // -C: enclosed escape char
            {
                curJob.setEscapeChar( optarg[0] );
                cout << "Escape Character  : " << optarg[0] << endl;
                break;
            }

            case 'E':                                // -E: enclosed by char
            {
                curJob.setEnclosedByChar( optarg[0] );
                cout << "Enclosed by Character : " << optarg[0] << endl;
                break;
            }

            case 'P':                                // -P: Calling moduleid
            {                                        //     and PID
                sModuleIDandPID = optarg;
                break;
            }

            case 'R':                                // -R: distributed mode
            {                                        //     report file
                rptFileName = optarg;
                break;
            }

            case 'S':                                // -S: Char & VarChar data
            {                                        //     greater than col def
                curJob.setTruncationAsError(true);   //     are reported as err
                break;
            }

            case 'X':                                // Hidden extra options
            {
                if (!strcmp(optarg,"AllowMissingColumn"))
                    bValidateColumnList = false;
                break;
            }

            default :
            {
                ostringstream oss;
                oss << "Unrecognized command line option (" << option << ")";
                startupError( oss.str(), true );
            }
        }
    }

    // Inconsistent to specify -f STDIN with -l importFile
    if ((bImportFileArg) && (importPath == "STDIN"))
    {
        startupError( std::string(
            "-f STDIN is invalid with -l importFile."), true );
    }

    // If distributed mode, make sure report filename is specified and that we
    // can create the file using the specified path.
    if ((bulkMode == BULK_MODE_REMOTE_SINGLE_SRC) ||
        (bulkMode == BULK_MODE_REMOTE_MULTIPLE_SRC))
    {
        if (rptFileName.empty())
        {
            startupError( std::string(
                "Bulk modes 1 and 2 require -R rptFileName."), true );
        }
        else
        {
            std::ofstream rptFile( rptFileName.c_str() );
            if ( rptFile.fail() )
            {
                std::ostringstream oss;
                oss << "Unable to open report file " << rptFileName;
                startupError( oss.str(), false );
            }
            rptFile.close();
        }
        curJob.setBulkLoadMode( bulkMode, rptFileName );
    }

    // Get positional arguments, User can provide:
    // 1. no positional parameters
    // 2. Two positional parameters (schema and table names)
    // 3. Three positional parameters (schema, table, and import file name)
    if (optind < argc)                         // see if db schema name is given
    {
        xmlGenSchema = argv[optind];                             // 1st pos parm
        optind++;

        if (optind < argc)                         // see if table name is given
        {
            // Validate invalid options in conjunction with 2-3 positional
            // parameter mode, which means we are using temp Job XML file.
            if (bImportFileArg)
            {
                startupError( std::string(
                    "-l importFile is invalid with positional parameters"),
                    true );
            }

            if (!sXMLJobDir.empty())
            {
                startupError( std::string(
                    "-p path is invalid with positional parameters."), true );
            }

            if (importPath == "STDIN")
            {
                startupError( std::string(
                    "-f STDIN is invalid with positional parameters."), true );
            }

            xmlGenTable = argv[optind];                          // 2nd pos parm
            optind++;

            if (optind < argc)                // see if input file name is given
            {                                                    // 3rd pos parm
                curJob.addToCmdLineImportFileList( std::string(argv[optind]) );

                // Default to CWD if loadfile name given w/o -f path
                if (importPath.empty())
                {
                    std::string setAltErrMsg;
                    if (curJob.setAlternateImportDir(
                        std::string("."), setAltErrMsg) != NO_ERROR)
                        startupError( setAltErrMsg, false );
                }
            }
            else
            {
                // Invalid to specify -f if no load file name given
                if (!importPath.empty())
                {
                    startupError( std::string(
                      "-f requires 3rd positional parameter (load file name)."),
                        true );
                }

                // Default to STDIN if no import file name given
                std::string setAltErrMsg;
                if (curJob.setAlternateImportDir(
                    std::string("STDIN"), setAltErrMsg) != NO_ERROR)
                    startupError( setAltErrMsg, false );
            }
        }
        else
        {
            startupError( std::string(
                "No table name specified with schema."), true );
        }
    }
    else
    {
        // JobID is a required parameter with no positional parm mode,
        // because we need the jobid to identify the input job xml file.
        if (sJobIdStr.empty()) 
        {
            startupError( std::string("No JobID specified."), true );
        }
    }
}

//------------------------------------------------------------------------------
// Print the path of the input load file(s), and the name of the job xml file.
//------------------------------------------------------------------------------
void printInputSource(
    const std::string& alternateImportDir,
    const std::string& jobDescFile)
{
    if (alternateImportDir.size() > 0)
    {
        if (alternateImportDir == IMPORT_PATH_CWD)
        {
            char cwdBuf[4096];
            ::getcwd(cwdBuf,sizeof(cwdBuf));
            cout << "Input file(s) will be read from : " << cwdBuf << endl;
        }
        else
        {
            cout << "Input file(s) will be read from : " <<
                alternateImportDir << endl;
        }
    }
    else
    {
        cout << "Input file(s) will be read from Bulkload root directory : " <<
            Config::getBulkRoot() << endl;
    }

    cout << "Job description file : "    << jobDescFile << endl;
}

//------------------------------------------------------------------------------
// Construct temporary Job XML file if user provided schema, job, and
// optional load filename.
// tempJobDir   - directory used to store temporary job xml file
// sJobIdStr    - job id (-j) specified by user
// xmlGenSchema - db schema name specified by user (1st positional parm)
// xmlGenTable  - db table name specified by user  (2nd positional parm)
// alternateImportDir - alternate directory for input data files
// sFileName(out)-filename path for temporary job xml file that is created
//------------------------------------------------------------------------------
void constructTempXmlFile(
    const std::string&       tempJobDir,
    const std::string&       sJobIdStr,
    const std::string&       xmlGenSchema,
    const std::string&       xmlGenTable,
    const std::string&       alternateImportDir,
    boost::filesystem::path& sFileName)
{
    // Construct the job description file name
    std::string xmlErrMsg;
    int rc = XMLJob::genJobXMLFileName( std::string(),
                                        tempJobDir,
                                        sJobIdStr,
                                        true, // using temp job xml file
                                        xmlGenSchema,
                                        xmlGenTable,
                                        sFileName,
                                        xmlErrMsg );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "cpimport.bin error creating temporary Job XML file name: " <<
            xmlErrMsg;
        startupError( oss.str(), false );
    }
    printInputSource( alternateImportDir, sFileName.string() );

    TempXMLGenData genData( sJobIdStr, xmlGenSchema, xmlGenTable );
    XMLGenProc genProc( &genData,
                        false,   // don't log to Jobxml_nnn.log
                        false ); // generate XML file (not a syscat report)
    try
    {
        genProc.startXMLFile( );
        execplan::CalpontSystemCatalog::TableName tbl(
            xmlGenSchema, xmlGenTable );
        genProc.makeTableData( tbl );
        if ( !genProc.makeColumnData( tbl ) )
        {
            std::ostringstream oss;
            oss << "No columns for " << xmlGenSchema << '.' << xmlGenTable;
            startupError( oss.str(), false );
        }
    }
    catch (runtime_error& ex)
    {
        std::ostringstream oss;
        oss << "cpimport.bin runtime exception constructing temporary "
               "Job XML file: " << ex.what();
        startupError( oss.str(), false );
    }
    catch (exception& ex)
    {
        std::ostringstream oss;
        oss << "cpimport.bin exception constructing temporary "
                "Job XML file: " << ex.what();
        startupError( oss.str(), false );
    }
    catch (...)
    {
        startupError( std::string( "cpimport.bin "
            "unknown exception constructing temporary Job XML file"),
            false );
    }

    genProc.writeXMLFile( sFileName.string() );
}

//------------------------------------------------------------------------------
// Get TableOID string for the specified db and table name.
//------------------------------------------------------------------------------
void getTableOID(const std::string& xmlGenSchema,
                 const std::string& xmlGenTable,
                 std::string& tableOIDStr)
{
    OID tableOID = 0;

    execplan::CalpontSystemCatalog::TableName tbl(
        xmlGenSchema, xmlGenTable );
    try
    {
        boost::shared_ptr<CalpontSystemCatalog> cat =
            CalpontSystemCatalog::makeCalpontSystemCatalog(
            BULK_SYSCAT_SESSION_ID);
        cat->identity(CalpontSystemCatalog::EC);
        tableOID = cat->tableRID(tbl).objnum;
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Unable to set default JobID; " <<
            "Error getting OID for table " <<
            tbl.schema << '.' << tbl.table << ": " << ex.what();
        startupError( oss.str(), false );
    }
    catch (...)
    {
        std::ostringstream oss;
        oss << "Unable to set default JobID; " <<
            "Unknown error getting OID for table " <<
            tbl.schema << '.' << tbl.table;
        startupError( oss.str(), false );
    }

    std::ostringstream oss;
    oss << tableOID;
    tableOIDStr = oss.str();
}

//------------------------------------------------------------------------------
// Verify we are running from a PM node.
//------------------------------------------------------------------------------
void verifyNode()
{
    std::string localModuleType = Config::getLocalModuleType();

    // Validate running on a PM
    if (localModuleType != "pm")
    {
        startupError( std::string( "Exiting, "
            "cpimport.bin can only be run on a PM node"),
            true );
    }
}

//------------------------------------------------------------------------------
// Log initiate message
//------------------------------------------------------------------------------
void logInitiateMsg( const char* initText )
{
    logging::Message::Args initMsgArgs;
    initMsgArgs.add( initText );
    SimpleSysLog::instance()->logMsg(
        initMsgArgs,
        logging::LOG_TYPE_INFO,
        logging::M0086);
}

//------------------------------------------------------------------------------
// Main entry point into the cpimport.bin program
//------------------------------------------------------------------------------
int main(int argc, char **argv)
{
#ifdef _MSC_VER
    _setmaxstdio(2048);
#else
    setuid( 0 ); // set effective ID to root; ignore return status
#endif
    setupSignalHandlers();

    // Set up LOCALE - BUG 5362
    std::string systemLang("C");
	systemLang = funcexp::utf8::idb_setlocale();

    // Initialize singleton instance of syslogging
    if (argc > 0)
        pgmName = argv[0];
    logging::IDBErrorInfo::instance();
    SimpleSysLog::instance();

    // Log job initiation unless user is asking for help
    std::ostringstream ossArgList;
    bool bHelpFlag = false;
    for (int m=1; m<argc; m++)
    {
        if (strcmp(argv[m],"-h") == 0)
        {
            bHelpFlag = true;
            break;
        }
        if (!strcmp(argv[m],"\t")) // special case to print a <TAB>
            ossArgList << "'\\t'" << ' ';   
        else
            ossArgList << argv[m] << ' ';   
    }
    if (!bHelpFlag)
    {
        logInitiateMsg( ossArgList.str().c_str() );
    }

    BulkLoad   curJob;
    string     sJobIdStr;
    string     sXMLJobDir;
    string     sModuleIDandPID;
    bool       bLogInfo2ToConsole = false;
    bool       bValidateColumnList= true;
    bool       bRollback          = false;
    bool       bForce             = false;
    int        rc                 = NO_ERROR;
    std::string exceptionMsg;
    TASK       task;                // track tasks being performed

    try
    {
    //--------------------------------------------------------------------------
    // Parse the command line arguments
    //--------------------------------------------------------------------------
    task = TASK_CMD_LINE_PARSING;
    string xmlGenSchema;
    string xmlGenTable;
    parseCmdLineArgs( argc, argv,
        curJob, sJobIdStr, sXMLJobDir, sModuleIDandPID, bLogInfo2ToConsole,
        xmlGenSchema, xmlGenTable, bValidateColumnList );

    //--------------------------------------------------------------------------
    // Save basename portion of program path from argv[0]
    //--------------------------------------------------------------------------
    string base;
    string::size_type startBase = string(argv[0]).rfind('/');
    if (startBase == string::npos)
        base.assign( argv[0] );
    else
        base.assign( argv[0]+startBase+1 );
    curJob.setProcessName( base );
    if (bDebug)
        logInitiateMsg( "Command line arguments parsed" );

    //--------------------------------------------------------------------------
    // Init singleton classes (other than syslogging that we already setup)
    //--------------------------------------------------------------------------
    task = TASK_INIT_CONFIG_CACHE;

    // Initialize cache used to store configuration parms from Calpont.xml
    Config::initConfigCache();

    // initialize singleton BRM Wrapper.  Also init ExtentRows (in dbrm) from
    // main thread, since ExtentMap::getExtentRows is not thread safe.
    BRMWrapper::getInstance()->getInstance()->getExtentRows();

    //--------------------------------------------------------------------------
    // Validate running on valid node
    //--------------------------------------------------------------------------
    verifyNode( );

    //--------------------------------------------------------------------------
    // Set scheduling priority for this cpimport.bin process
    //--------------------------------------------------------------------------
#ifdef _MSC_VER
    //FIXME
#else
    setpriority( PRIO_PROCESS, 0, Config::getBulkProcessPriority() );
#endif
    if (bDebug)
        logInitiateMsg( "Config cache initialized" );

    //--------------------------------------------------------------------------
    // Make sure DMLProc startup has completed before running a cpimport.bin job
    //--------------------------------------------------------------------------
    task = TASK_BRM_STATE_READY;
    if (!BRMWrapper::getInstance()->isSystemReady())
    {
        startupError( std::string(
            "System is not ready.  Verify that InfiniDB is up and ready "
            "before running cpimport."), false );
    }
    if (bDebug)
        logInitiateMsg( "BRM state verified: state is Ready" );

    //--------------------------------------------------------------------------
    // Verify that the state of BRM is read/write
    //--------------------------------------------------------------------------
    task = TASK_BRM_STATE_READ_WRITE;
    int brmReadWriteStatus = BRMWrapper::getInstance()->isReadWrite();
    if (brmReadWriteStatus != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << ec.errorString(brmReadWriteStatus) <<
               "  cpimport.bin is terminating.";
        startupError( oss.str(), false );
    }
    if (bDebug)
        logInitiateMsg( "BRM state is Read/Write" );

    //--------------------------------------------------------------------------
    // Make sure we're not about to shutdown
    //--------------------------------------------------------------------------
    task = TASK_SHUTDOWN_PENDING;
    int brmShutdownPending = BRMWrapper::getInstance()->isShutdownPending(
        bRollback, bForce);
    if (brmShutdownPending != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << ec.errorString(brmShutdownPending) <<
               "  cpimport.bin is terminating.";
        startupError( oss.str(), false );
    }
    if (bDebug)
        logInitiateMsg( "Verified no shutdown operation is pending" );

    //--------------------------------------------------------------------------
    // Make sure we're not write suspended
    //--------------------------------------------------------------------------
    task = TASK_SUSPEND_PENDING;
    int brmSuspendPending = BRMWrapper::getInstance()->isSuspendPending();
    if (brmSuspendPending != NO_ERROR)
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << ec.errorString(brmSuspendPending) <<
               "  cpimport.bin is terminating.";
        startupError( oss.str(), false );
    }
    if (bDebug)
        logInitiateMsg( "Verified no suspend operation is pending" );

    //--------------------------------------------------------------------------
    // Set some flags
    //--------------------------------------------------------------------------
    task = TASK_ESTABLISH_JOBFILE;
    BRMWrapper::setUseVb( false );
    Cache::setUseCache  ( false );

    //--------------------------------------------------------------------------
    // Construct temporary Job XML file if user provided schema, job, and
    // optional load filename.
    //--------------------------------------------------------------------------
    boost::filesystem::path sFileName;
    bool bUseTempJobFile = false;

    cout << std::endl; // print blank line before we start

    // Start tracking time to create/load jobfile;
    // The elapsed time for this step is logged at the end of loadJobInfo()
    curJob.startTimer();
    if (!xmlGenSchema.empty()) // create temporary job file name
    {
        // If JobID is not provided, then default to the table OID
        if (sJobIdStr.empty())
        {
            std::string tableOIDStr;
            getTableOID(xmlGenSchema,
                        xmlGenTable,
                        tableOIDStr);

            cout << "Using table OID " << tableOIDStr <<
                " as the default JOB ID" << std::endl;
            sJobIdStr = tableOIDStr;
        }

        // No need to validate column list in job XML file for user errors,
        // if cpimport.bin just generated the job XML file on-the-fly.
        bValidateColumnList = false;

        bUseTempJobFile     = true;
        constructTempXmlFile(curJob.getTempJobDir(),
                             sJobIdStr,
                             xmlGenSchema,
                             xmlGenTable,
                             curJob.getAlternateImportDir(),
                             sFileName);
    }
    else                       // create user's persistent job file name
    {
        // Construct the job description file name
        std::string xmlErrMsg;
        rc = XMLJob::genJobXMLFileName( sXMLJobDir,
                                        curJob.getJobDir(),
                                        sJobIdStr,
                                        bUseTempJobFile,
                                        std::string(),
                                        std::string(),
                                        sFileName,
                                        xmlErrMsg );
        if (rc != NO_ERROR)
        {
            std::ostringstream oss;
            oss << "cpimport.bin error creating Job XML file name: " <<
                xmlErrMsg;
            startupError( oss.str(), false );
        }
        printInputSource( curJob.getAlternateImportDir(), sFileName.string() );
    }
    if (bDebug)
        logInitiateMsg( "Job xml file is established" );

    //--------------------------------------------------------------------------
    // This is the real business
    //--------------------------------------------------------------------------
    task = TASK_LOAD_JOBFILE;
    rc = curJob.loadJobInfo( sFileName.string(), bUseTempJobFile,
        systemLang, argc, argv, bLogInfo2ToConsole, bValidateColumnList );
    if( rc != NO_ERROR )
    {
        WErrorCodes ec;
        std::ostringstream oss;
        oss << "Error in loading job information; " <<
            ec.errorString(rc) << "; cpimport.bin is terminating.";
        startupError( oss.str(), false );
    }
    if (bDebug)
        logInitiateMsg( "Job xml file is loaded" );
    task = TASK_PROCESS_DATA;

    // Log start of job to INFO log
    logging::Message::Args startMsgArgs;
    startMsgArgs.add(sJobIdStr);
    startMsgArgs.add(curJob.getSchema());
    SimpleSysLog::instance()->logMsg(
        startMsgArgs,
        logging::LOG_TYPE_INFO,
        logging::M0081);

    curJob.printJob();

    rc = curJob.processJob( );
    if( rc != NO_ERROR )
        cerr << endl << "Error in loading job data" << endl;
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Uncaught exception caught in cpimport.bin main() while " <<
            taskLabels[ task ] << "; " <<
            ex.what();
        exceptionMsg = oss.str();
        if (task != TASK_PROCESS_DATA)
        {
            startupError( exceptionMsg, false );
        }
        rc = ERR_UNKNOWN;
    }

    //--------------------------------------------------------------------------
    // Log end of job to INFO log
    //--------------------------------------------------------------------------
    logging::Message::Args endMsgArgs;
    endMsgArgs.add(sJobIdStr);
    if (rc != NO_ERROR)
    {
        std::string failMsg("FAILED");
        if (exceptionMsg.length() > 0)
        {
            failMsg += "; ";
            failMsg += exceptionMsg;
        }
        endMsgArgs.add(failMsg.c_str());
    }
    else
    {
        endMsgArgs.add("SUCCESS");
    }
    SimpleSysLog::instance()->logMsg(
        endMsgArgs,
        logging::LOG_TYPE_INFO,
        logging::M0082);
    
    if (rc != NO_ERROR)
        return ( EXIT_FAILURE );
    else
        return ( EXIT_SUCCESS );
}
