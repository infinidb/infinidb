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

#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>

#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <exception>
#include <stdexcept>
#include <cerrno>
using namespace std;

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "liboamcpp.h"
using namespace oam;

#include "we_cmdargs.h"

#include "installdir.h"

namespace WriteEngine
{

//----------------------------------------------------------------------
//----------------------------------------------------------------------
WECmdArgs::WECmdArgs(int argc, char** argv) :
	fMultiTableCount(0),
	fJobLogOnly(false),
	fHelp(false),
	fMode(1),
	fArgMode(-1),
	fQuiteMode(true),
	fConsoleLog(false),
	fVerbose(0),
	fBatchQty(10000),
	fNoOfReadThrds(0),
	fDebugLvl(0),
	fMaxErrors(-1),
	fReadBufSize(0),
	fIOReadBufSize(0),
	fSetBufSize(0),
	fColDelim('|'),
	fEnclosedChar(0),
	fEscChar(0),
	fNoOfWriteThrds(0),
	fNullStrMode(false),
	fImportDataMode(IMPORT_DATA_TEXT),
	fCpiInvoke(false),
	fBlockMode3(false),
	fbTruncationAsError(false),
	fUUID(boost::uuids::nil_generator()())
{

    try
    {
		appTestFunction();
	    parseCmdLineArgs(argc, argv);
    }
    catch(std::exception& exp)
    {
		std::string exceptMsg( exp.what() );
		exceptMsg += "\nTry 'cpimport -h' for more information.";
        throw(runtime_error( exceptMsg ));
    }
}

//----------------------------------------------------------------------

void WECmdArgs::appTestFunction()
{

	// testing begins
	//std::string aJobFile("/home/bpaul/Calpont/data/bulk/job/Job_1234.xml");
	//std::string aJobFile("/usr/local/Calpont/data/bulk/job/Job_1234.xml");
	//setSchemaAndTableFromJobFile(aJobFile);
	//setEnclByAndEscCharFromJobFile(aJobFile);
	//exit(1);

	//testing ends
	return;
}

//----------------------------------------------------------------------
std::string WECmdArgs::getCpImportCmdLine()
{
	std::ostringstream aSS;
	std::string aCmdLine;


	aSS << fPrgmName;	//prgm name as arg[0]

	if(fHelp)
	{
		aSS << " -h ";
		aCmdLine = aSS.str();
		return aCmdLine;
	}

	//checkJobIdCase();			// check if JobID


	if((fPmFile.length()>0)&&(0==getMode()))
		aSS << " -l " << fPmFile;
	//BUG 4379 if -m is not given while prep args, default is m=1 but
	//on single node -m will set it to 3, after prep args.
	//if((fPmFilePath.length()>0)&&(1!=getMode()))
	//	aSS << " -f " << fPmFilePath;
	if((fPmFilePath.length()>0)&&(1!=getMode()))
	{
		if(fPmFilePath == "STDIN")	//if stdin, must pass it
			aSS << " -f " << fPmFilePath;
		else if(2==getArgMode())	//Mode 2 we need to pass the -f option
			aSS << " -f " << fPmFilePath;
		else if(3==getArgMode()) 	//-m given, -f built in already.
			aSS << " -f " << fPmFilePath;
		else if (0==fLocFile.length()) //No filename given, from job file
			aSS << " -f " << fPmFilePath;
	}

	if(fJobId.length()>0)
		aSS << " -j " << fJobId;
	if(fNoOfReadThrds>0)
		aSS << " -r " << fNoOfReadThrds;
	if(fNoOfWriteThrds>0)
		aSS << " -w " << fNoOfWriteThrds;
	if(fMaxErrors>=0)
		aSS << " -e " << fMaxErrors;
	// BUG 5088
	if(fDebugLvl>0)
		aSS << " -d " << fDebugLvl;
	if(fSetBufSize>0)
		aSS << " -B " << fSetBufSize;
	if(fColDelim != '|')
	{
		if(fColDelim == '\t')
			aSS << " -s " << "\\t"; //- working with user '\t'
			// NONE of the following will work.
			//aSS << " -s " << "\t"; //aSS << " -s " << "\"\\t\""; //aSS << " -s " << "'\\t'";
		else
			aSS << " -s " << fColDelim;
	}
	if(fEnclosedChar != 0)
		aSS << " -E " << fEnclosedChar;
	if(fEscChar != 0)
		aSS << " -C " << fEscChar;
	if(fNullStrMode)
		aSS << " -n " << '1';
	if(fImportDataMode != IMPORT_DATA_TEXT)
		aSS << " -I " << fImportDataMode;
	//if(fConfig.length()>0)
	//	aSS << " -c " << fConfig;
	if(fReadBufSize>0)
	{
		cout << "setting fReadBufSize = " << fReadBufSize;
		aSS << " -c " << fReadBufSize;
	}

	if(fIOReadBufSize>0)
		aSS << " -b " << fIOReadBufSize;


	if((fJobPath.length()>0)&&(fMode==3))
		aSS << " -p " << fJobPath;


	if(fConsoleLog)
		aSS << " -i ";
	if((fMode == 1)||(fMode == 2))
	{
		aSS << " -R " << getBrmRptFileName();
		aSS << " -m " << fMode;
	}

	aSS << " -P " << getModuleID();

	if(fbTruncationAsError)
		aSS << " -S ";

	if((fJobId.length()>0)&&(fMode==1)&&(!fJobLogOnly))
	{
		// if JobPath provided, make it w.r.t WES
		aSS << " -p " << fTmpFileDir;
		aSS << " -fSTDIN";
	}
	else if((fJobId.length()>0)&&(fMode==2)&&(!fJobLogOnly))
	{
		// if JobPath provided, make it w.r.t WES
		aSS << " -p " << fTmpFileDir;
		if(fPmFile.length()>0)
			aSS << " -l " << fPmFile;
		if(fPmFilePath.length()>0)
			aSS << " -f " << fPmFilePath;
	}
	else	// do not provide schema & table with JobId
	{

    if (!fUUID.is_nil())
	    aSS << " -u" << boost::uuids::to_string(fUUID);

    if(fSchema.length()>0)
        aSS << " " << fSchema;
    //else if((fMode != 0)||(fMode==3))	//TODO make it mode3 + jobID
    else if(fJobId.length()>0)
    { }// may or may not contain Schema.
    //else if((fMode == 1)||(fMode==2))	//TODO make it mode3 + jobID
    else if(fMode != 0)
        throw (runtime_error("Schema not available"));
    if(fTable.length()>0)
        aSS << " " << fTable;
    else if(fJobId.length()>0)
    {} //may or may not contain Table.
    else if(fMode != 0)
    	throw (runtime_error("Tablename not available"));
    //else if((fMode != 0)||(fMode==3))	//TODO make it mode3 + jobID
    //else if((fMode == 1)||(fMode == 2))	//TODO make it mode3 + jobID
    //    throw (runtime_error("Tablename not available"));
    if((fPmFile.length()>0) && (2 == getMode()))
    {
    	//if(fPmFilePath.length()>0)
    	//	aSS << " " << fPmFilePath;
    	aSS << " " << fPmFile;
    }
    else if (2 == getMode())
    	throw (runtime_error("loadFile [-l ] not available"));

	}

    if((fLocFile.length()>0) && (fLocFile != "STDIN") && (3 == getMode()))
    {
    	//Bug 4342 multi-files mode 3 support
    	//convert all the spaces into 'commas'
    	if(fLocFile.find_first_of(' ') == string::npos)
    		aSS << " " << fLocFile;
    	else
    	{
    		std::string aLocFiles = replaceCharInStr(fLocFile, ' ', ',');
    		aSS << " " << aLocFiles;
    	}
    }

    try
    {
    	aCmdLine = aSS.str();
    }
    catch(exception&)
    {
        throw runtime_error("getcpImportCmdLine failed");
    }

	return aCmdLine;
}


//----------------------------------------------------------------------

//BUG 4205 (part FIX) - need to implement more into it
bool WECmdArgs::checkForCornerCases()
{
	//BUG 4210
	this->checkJobIdCase();		//Need to do this before we go further


	if(fMode == 0)
	{
		if(!fJobId.empty())
		{
			//cout << "Invalid option mode 0 with a Job File" << endl;
			throw(runtime_error("Mode 0 with a Job file option is not valid!!"
				"\nTry 'cpimport -h' for more information."));
		}
		else if(!fJobPath.empty())
		{
			cout << "Invalid option mode 0 with a Job Path" << endl;
			throw(runtime_error("Mismatched options"
				"\nTry 'cpimport -h' for more information."));
		}
		else if(!fSchema.empty())
		{
			cout << "Invalid option in mode 0 with a schema name" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if(!fTable.empty())
		{
			cout << "Invalid option in mode 0 with a table name" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if((!fPmFilePath.empty())&&(fPmFilePath!="STDIN"))
		{
			cout << "Invalid option -f in Mode 0 with value other than STDIN" << endl;
			throw(runtime_error("Mismatched options."));
		}

		if(fSetBufSize)
		{
			cout << "Invalid option -B with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fIOReadBufSize)
		{
			cout << "Invalid option -b with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fMaxErrors>=0)
		{
			cout << "Invalid option -e with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fConsoleLog)
		{
			cout << "Invalid option -i with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fReadBufSize)
		{
			cout << "Invalid option -c with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fNoOfReadThrds)
		{
			cout << "Invalid option -r with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}
		else if (fNoOfWriteThrds)
		{
			cout << "Invalid option -w with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}

		if (fImportDataMode != IMPORT_DATA_TEXT)
		{
			cout << "Invalid option -I with Mode 0" << endl;
			throw(runtime_error("Mismatched options."));
		}

	}

	if(fMode == 1)
	{
		if(!fJobId.empty())
		{
			if((!fPmFilePath.empty())&&(fPmFilePath == "STDIN"))
			{
				// do not do anything.. this is good.
			}
		}
		// Mode 1, if Input Path is existing and input file is not existing
		// it is an error, bce it assumes all the files in directory.
		// In mode 2, we are passing info to cpimport.bin, which will take care
		// of it, as in Mode 3.
		else if((!fPmFilePath.empty())&&(fPmFile.empty()))
		{
			// assumed since position param is missing
			if((fLocFile == "STDIN")&&(fPmFilePath != "STDIN"))
			{
				cout << "Invalid options in Mode 1 : option -l " << endl;
				cout << " or input file position parameter needed" << endl;
				//cout << "\tOption (-j) should follow with option -l option or "
				//		"an input file position parameter" << endl;
				throw(runtime_error("In Mode 1 Error occurred!! "
							"\nTry 'cpimport -h' for more information."));
			}
		}
	}

	if(fMode == 2)
	{
		if(fPmFile.empty())
			throw(runtime_error("Mode 2 require PM based filename [-l]"
				"\nTry 'cpimport -h' for more information."));
		if((fPmFilePath.empty())&&(fPmFile.at(0)!='/'))
			throw(runtime_error("Mode 2 require remote file opts -f and -l or "\
					"a fully qualified path for the remote file."
				"\nTry 'cpimport -h' for more information."));
	}

	if(fMode == 3)
	{
		if(fPmVec.size())
		{
			cout << "Invalid option -P with Mode 3" << endl;
			throw(runtime_error("Mismatched options."));
		}

	}

	return true;
}

//----------------------------------------------------------------------

bool WECmdArgs::str2PmList(std::string& PmList, VecInts& V)
{
    const int BUFLEN=512;
	char aBuff[BUFLEN];


    int aLen = PmList.length();
    if(aLen>0)
    {
        strncpy(aBuff, PmList.c_str(), BUFLEN);
        aBuff[BUFLEN-1]=0;
    }
    else
        return false;

    char* pTok = strtok(aBuff, ",");
    while(pTok != NULL)
    {
        int aPmId = 0;
        try
        {
            aPmId = atoi(pTok);
            V.push_back(aPmId);
        }
        catch(exception& ex)
        {
        	std::stringstream aErr;
        	aErr << "Wrong PM id format : " << ex.what();
            //cout << "Wrong PM id format : "<< ex.what() << endl;
            throw(runtime_error(aErr.str()));
        }
        pTok = strtok(NULL, ",");
    }

    return true;
}

//----------------------------------------------------------------------

void WECmdArgs::usage()
{
	cout << "Simple usage using positional parameters (no XML job file):\n";
	cout << "\tcpimport dbName tblName [loadFile] [-h] [-m mode]\n";
	cout << "\t\t [-f path] [-d debugLevel] [-c readbufSize] [-b readBufs] \n";
	cout << "\t\t [-r readers] [-j JobID] [-e maxErrs] [-B libBufSize] [-w parsers]\n";
	cout << "\t\t [-s c] [-E enclosedChar] [-C escapeChar] [-n NullOption]\n";
	cout << "\t\t [-q batchQty] [-p jobPath] [-P list of PMs] [-S] [-i] [-v verbose]\n";
	cout << "\t\t [-I binaryOpt]\n";


	cout << "Traditional usage without positional parameters (XML job file required):\n";
	cout << "\tcpimport -j jobID\n";
	cout << "\t\t [-h] [-m mode] [-r readers] [-w parsers] [-s c] [-f path]\n";
	cout << "\t\t [-b readBufs] [-p path] [-c readBufSize] [-e maxErrs] [-B libBufSize]\n";
	cout << "\t\t [-n NullOption] [-E encloseChar] [-C escapeChar] [-i] [-v verbose]\n";
	cout << "\t\t [-d debugLevel] [-q batchQty] [-l loadFile] [-P list of PMs] [-S]\n";
	cout << "\t\t [-I binaryOpt]\n";

	cout << "\n\nPositional parameters:\n";
	cout << "\tdbName     Name of the database to load\n";
	cout << "\ttblName    Name of table to load\n";
	cout << "\tloadFile   Optional input file name in current directory,\n";
	cout << "\t\t\tunless a fully qualified name is given.\n";
	cout << "\t\t\tIf not given, input read from STDIN.\n";

	cout << "\n\nOptions:\n"
			<<"\t-b\tNumber of read buffers\n"
			<<"\t-c\tApplication read buffer size(in bytes)\n"
			<<"\t-d\tPrint different level(1-3) debug message\n"
			<<"\t-e\tMax number of allowable error per table per PM\n"
			<<"\t-f\tData file directory path.\n"
			<<"\t\t\tDefault is current working directory.\n"
			<<"\t\t\tIn Mode 1, -f represents the local input file path.\n"
			<<"\t\t\tIn Mode 2, -f represents the PM based input file path.\n"
			<<"\t\t\tIn Mode 3, -f represents the local input file path.\n"
			<<"\t-l\tName of import file to be loaded, relative to -f path,\n"
			<<"\t-h\tPrint this message.\n"
			<<"\t-q\tBatch Quantity, Number of rows distributed per batch in Mode 1\n"
			<<"\t-i\tPrint extended info to console in Mode 3.\n"
			<<"\t-j\tJob ID. In simple usage, default is the table OID.\n"
			<<"\t\t\tunless a fully qualified input file name is given.\n"
			<<"\t-n\tNullOption (0-treat the string NULL as data (default);\n"
			<<"\t\t\t1-treat the string NULL as a NULL value)\n"
			<<"\t-p\tPath for XML job description file.\n"
			<<"\t-r\tNumber of readers.\n"
			<<"\t-s\t'c' is the delimiter between column values.\n"
			<<"\t-B\tI/O library read buffer size (in bytes)\n"
			<<"\t-w\tNumber of parsers.\n"
			<<"\t-E\tEnclosed by character if field values are enclosed.\n"
			<<"\t-C\tEscape character used in conjunction with 'enclosed by'\n"
			<<"\t\t\tcharacter, or as part of NULL escape sequence ('\\N');\n"
			<<"\t\t\tdefault is '\\'\n"
			<<"\t-I\tImport binary data; how to treat NULL values:\n"
			<<"\t\t\t1 - import NULL values\n"
			<<"\t\t\t2 - saturate NULL values\n"
			<<"\t-P\tList of PMs ex: -P 1,2,3. Default is all PMs.\n"
			<<"\t-S\tTreat string truncations as errors.\n"
			<<"\t-m\tmode\n"
			<<"\t\t\t1 - rows will be loaded in a distributed manner across PMs.\n"
			<<"\t\t\t2 - PM based input files loaded onto their respective PM.\n"
			<<"\t\t\t3 - input files will be loaded on the local PM.\n";

	cout << "\nExample1: Traditional usage\n"
			<<"\tcpimport -j 1234";
	cout << "\nExample2: Some column values are enclosed within double quotes.\n"
	     	<<"\tcpimport -j 3000 -E '\"'";
	cout << "\nExample3: Import a nation table without a Job XML file\n"
			<<"\tcpimport -j 301 tpch nation nation.tbl";
	cout << "\nExample4: Import a nation table to all PMs in Mode 1\n"
			<<"\tcpimport -m 1 tpch nation nation.tbl";
	cout << "\nExample4: Import a nation table to only PM1 and PM2 in Mode 1\n"
			<<"\tcpimport -m 1 -P 1,2 tpch nation nation.tbl";
	cout << "\nExample5: Import nation.tbl from PMs to nation table in Mode 2\n"
	     	<<"\tcpimport -m 2 tpch nation -l nation.tbl";
	cout << "\nExample6: Import nation.tbl in mode 3\n"
			<<"\tcpimport -m 3 tpch nation nation.tbl\n\n";


	exit(1);
}


//-----------------------------------------------------------------------------

void WECmdArgs::parseCmdLineArgs(int argc, char** argv)
{
	int aCh;
	std::string importPath;
	bool aJobType = false;


	if (argc > 0)
		fPrgmName = "cpimport.bin"; //argv[0] is splitter but we need cpimport

	//Just for testing cpimport invoking in UM
	//if(argc>0)
	//	fPrgmName = "/home/bpaul/genii/export/bin/cpimport";

	while ((aCh = getopt(argc, argv,
		"d:j:w:s:v:l:r:b:e:B:f:q:ihm:E:C:P:I:n:p:c:S"))
			!= EOF)
	{
		switch (aCh)
		{
		case 'm':
		{
			//fMode = atoi(optarg);
			fArgMode = atoi(optarg);
			//cout << "Mode level set to " << fMode << endl;
			if ((fArgMode > -1) && (fArgMode <= 3)){}
			else
				throw runtime_error("Wrong Mode level");
			break;
		}
		case 'B':
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -B is invalid or out of range");
			fSetBufSize = lValue;
			break;
		}
		case 'b':
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -b is invalid or out of range");
			fIOReadBufSize = lValue;
			break;
		}
		case 'e':
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 0) || (lValue > INT_MAX))
				throw runtime_error("Option -e is invalid or out of range");
			fMaxErrors = lValue;
			break;
		}
		case 'i':
		{
			fConsoleLog = true;
			break;
		}
		case 'c':
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -c is invalid or out of range");
			fReadBufSize = lValue;
			break;
		}
		case 'j': // -j: jobID
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 0) || (lValue > INT_MAX))
				throw runtime_error("Option -j is invalid or out of range");
			fJobId = optarg;
			fOrigJobId = fJobId;	// in case if we need to split it.
			if(0==fJobId.length()) throw runtime_error("Wrong JobID Value");
			aJobType = true;
			break;
		}
		case 'v': // verbose
		{
			string aVerbLen = optarg;
			fVerbose = aVerbLen.length();
			fDebugLvl = fVerbose;
			break;
		}
		case 'd': // -d debug
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -d is invalid or out of range");
			fDebugLvl = lValue;
			if (fDebugLvl > 0 && fDebugLvl <= 3)
			{
				cout << "\nDebug level set to " << fDebugLvl << endl;
			}
			else
			{
				throw runtime_error("Wrong Debug level");
			}
			break;
		}
		case 'r': // -r: num read threads
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -r is invalid or out of range");
			fNoOfReadThrds = lValue;
			break;
		}
		case 'w': // -w: num parse threads
		{
			errno = 0;
			long lValue = strtol(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > INT_MAX))
				throw runtime_error("Option -w is invalid or out of range");
			fNoOfWriteThrds = lValue;
			break;
		}
		case 's': // -s: column delimiter
		{
			if (!strcmp(optarg, "\\t"))
			{
				fColDelim = '\t';
				if(fDebugLvl) cout << "Column delimiter : " << "\\t" << endl;
			}
			else
			{
				fColDelim = optarg[0];
				if(fDebugLvl) cout << "Column delimiter : " << fColDelim << endl;
			}
			break;
		}
		case 'l': // -l: if JobId (-j), it can be input file
		{
			fPmFile = optarg;
			if(0==fPmFile.length()) throw runtime_error("Wrong local filename");
			break;
		}
		case 'f': // -f: import file path
		{
			fPmFilePath = optarg;
			break;
		}
		case 'n': // -n: treat "NULL" as null
		{ // default is 0, ie it is equal to not giving this option
			int nullStringMode = atoi(optarg);
			if ((nullStringMode != 0) && (nullStringMode != 1))
			{
				throw(runtime_error(
						"Invalid NULL option; value can be 0 or 1"));
			}
			if (nullStringMode)
				fNullStrMode = true;
			else
				fNullStrMode = false; // This is default
			break;
		}
		case 'P': // -p: list of PM's
		{
			try
			{
				std::string aPmList = optarg;
				if (!str2PmList(aPmList, fPmVec))
					throw(runtime_error("PM list is wrong"));
			} catch (runtime_error& ex)
			{
				throw(ex);
			}
			break;
		}
		case 'p':
		{
			fJobPath = optarg;
			break;
		}
		case 'E': // -E: enclosed by char
		{
			fEnclosedChar = optarg[0];
			//cout << "Enclosed by Character : " << optarg[0] << endl;
			break;
		}
		case 'C': // -C: enclosed escape char
		{
			fEscChar = optarg[0];
			//cout << "Escape Character  : " << optarg[0] << endl;
			break;
		}
		case 'h': // -h: help
		{
			//usage(); // will exit(1) here
			fHelp = true;
			break;
		}
		case 'I': // -I: binary mode (null handling)
		{ // default is text mode, unless -I option is specified
			int binaryMode = atoi(optarg);
			if (binaryMode == 1)
			{
				fImportDataMode = IMPORT_DATA_BIN_ACCEPT_NULL;
			}
			else if (binaryMode == 2)
			{
				fImportDataMode = IMPORT_DATA_BIN_SAT_NULL;
			}
			else
			{
				throw(runtime_error(
					"Invalid Binary mode; value can be 1 or 2"));
			}
			break;
		}
		case 'S': // -S: Treat string truncations as errors
		{
			setTruncationAsError(true);
			//cout << "TruncationAsError  : true" << endl;
			break;
		}
		case 'q': // -q: batch quantity - default value is 10000
		{
			errno = 0;
			long long lValue = strtoll(optarg, 0, 10);
			if ((errno != 0) || (lValue < 1) || (lValue > UINT_MAX))
				throw runtime_error("Option -q is invalid or out of range");
			fBatchQty = lValue;
			if(fBatchQty<10000) fBatchQty = 10000;
			else if(fBatchQty>100000) fBatchQty = 10000;
			break;
		}
		default:
		{
			std::string aErr = "Unknown command line option " + aCh;
			//cout << "Unknown command line option " << aCh << endl;
			throw(runtime_error(aErr));
		}
		}
	}

	if(fHelp) usage();	//BUG 4210

	if(fArgMode != -1) fMode = fArgMode;	//BUG 4210

	std::string bulkRootPath = getBulkRootDir();

	if (aJobType)
	{
		if(0==fArgMode) throw runtime_error("Incompatible mode and option types");
		if (optind < argc)
		{
			fSchema = argv[optind]; // 1st pos parm
			optind++;
			if (optind < argc)
			{
				fTable = argv[optind]; // 2nd pos parm
				optind++;
			}
			else
			{ // if schema is there, table name should be there
				throw runtime_error("No table name specified with schema.");
			}

			if (optind < argc) // see if input file name is given
			{ // 3rd pos parm
				fLocFile = argv[optind];
				if((fLocFile.at(0)!= '/')&&(fLocFile != "STDIN"))
				{
					std::string aTmp = fLocFile;
					// BUG 4379 -f given? use that
					if((!fPmFilePath.empty()) && (fMode==1))
						fLocFile = fPmFilePath + "/"+aTmp;
					else if (fPmFilePath.empty())
						fLocFile = bulkRootPath + "/data/import/"+ aTmp;
				}
			}
			else
			{
				if(!fPmFile.empty())
					fLocFile = fPmFile;
				//BUG 4186
				//else  // else take it from the jobxml file
				//	fLocFile = "STDIN";
				//Historically cpimport works with jobfile as
				// -l <fileName> && -f <filePath>   or
				// -fSTDIN as the stdin, it will override colxml loadfile entry
				// if -fSTDIN is not provided get i/p file from jobfile
				else if((!fPmFilePath.empty())&&(fPmFilePath == "STDIN"))
					fLocFile = "STDIN";
				// else take it from the jobxml file
			}

			if((fSchema.length()>0)&&(fTable.length()>0)&&(fLocFile.length()>0))
				fJobLogOnly = true;
		}
		else
		{
			if(!fPmFile.empty())
			{
				fLocFile = fPmFile;
				if(!fPmFilePath.empty())
				{
					if(fPmFilePath == "STDIN")
					{
						throw runtime_error("Conflicting options -l and -fSTDIN");
					}
					else
					{
						std::string aTmp = fLocFile;
						if((!fPmFilePath.empty())&& (fMode==1)) //BUG 4379 -f given? use that
							fLocFile = fPmFilePath + "/"+aTmp;
						else if(!fPmFilePath.empty())
							fLocFile = bulkRootPath + "/data/import/"+aTmp;
					}

				}
				if((fLocFile.at(0)!= '/')&&(fLocFile != "STDIN")&&(fPmFilePath.empty()))
				{
					std::string aTmp = fLocFile;
					fLocFile = bulkRootPath + "/data/import/"+ aTmp;
				}
			}
			//BUG 4186
			//else
			//	fLocFile = "STDIN";
			//Historically cpimport works with jobfile as
			// -l <fileName> && -f <filePath>   or
			// -fSTDIN as the stdin, it will override colxml loadfile entry
			// if -fSTDIN is not provided get i/p file from jobfile
			else if((!fPmFilePath.empty())&&(fPmFilePath == "STDIN"))
				fLocFile = "STDIN";
			// else take it from the jobxml file
		}

	}
	// Get positional arguments, User can provide:
	// 1. no positional parameters	- Mode 0 & stdin
	// 2. Two positional parameters (schema and table names) - Mode 1/2, stdin
	// 3. Three positional parameters (schema, table, and import file name)
	else if (optind < argc) // see if db schema name is given
	{
		if (fArgMode == 0)
		{
			//added the code as per BUG 4245
			if(!fPmFilePath.empty())
			{
				fLocFile = fPmFilePath;
				if(fLocFile != "STDIN")
					throw(runtime_error("ERROR: In Mode 0, -f option can only have value STDIN"));
			}
			else
			{
				fLocFile = argv[optind];
				optind++;
			}
			if(optind < argc) //dest filename provided
			{
				fPmFile = argv[optind];
				if(fPmFile.at(0)!='/')
				{
					std::string aTmp = fPmFile;
					fPmFile = bulkRootPath +"/data/import/"+aTmp;
				}
			}
			else // no dest filename
			{
				if(fLocFile == "STDIN")
					throw(runtime_error("ERROR: Destination file name required!!"));
				if(fLocFile.at(0)=='/')	//local FQ-filename,parse out filename
					fPmFile = getFileNameFromPath(fLocFile);
				else
					fPmFile = fLocFile;
				if(fPmFile.at(0)!='/')	//should be true all the time
				{
					std::string aTmp = fPmFile;
					fPmFile = bulkRootPath +"/data/import/"+aTmp;
				}
			}

			/* commented out for BUG 4245
			if(fPmFilePath.empty())
				fLocFile = argv[optind];
			else
				fLocFile = fPmFilePath +"/"+ argv[optind];

			if (fPmFile.empty()) //BUG 4200
			{
				//if(fLocFile.at(0)== '/')
				//	fPmFile = fLocFile;
				//else
				if(fLocFile.at(0)!='/')
					fPmFile = bulkRootPath + "/data/import/"+ fLocFile;
			}
			else
			{
				if(fPmFile.at(0)!='/')
				{
					std::string aTmp = fPmFile;
					fPmFile = bulkRootPath + "/data/import/"+aTmp;
				}
			}
			*/
		}
		else
			fSchema = argv[optind]; // 1st pos parm
		optind++;

		if (optind < argc) // see if table name is given
		{
			fTable = argv[optind]; // 2nd pos parm
			optind++;

			if (optind < argc) // see if input file name is given
			{ // 3rd pos parm
				fLocFile = argv[optind];

				//BUG 4379 if -f option given we need to use that path,
				//over riding bug 4231. look at the code below
				//BUG 4231 - This bug over writes 4199 and commenting out changes
				//BUG 4199
				//Path not provided, not fully qualified, Look in import dir
				//if((fLocFile.at(0)!= '/')&&(fLocFile != "STDIN"))
				//{
				//	std::string aTmp = fLocFile;
				//	fLocFile = bulkRootPath + "/data/import/"+ aTmp;
				//}
				//BUG 4379 if -f option given we need to use that path
				if((fLocFile.at(0)!= '/')&&(fLocFile != "STDIN"))
				{
					std::string aTmp = fLocFile;
					//if -f given? use that otherwise just go ahead with CWD
					if((!fPmFilePath.empty())&& (fMode==1))
						fLocFile = fPmFilePath + "/"+aTmp;
					// TODO - if -f option is given and a list of files are
					// are provided, we need to be able to import all that.
				}


			}
			else
			{
				if (fPmFile.length() > 0)
				{
					// BUG 4210
					//if (fPmFilePath.length() > 0)
					//{
					//	fLocFile = fPmFilePath +"/"+ fPmFile;
					//}
					//else
					if(fPmFilePath.empty())
					{
						//NOTE - un-commenting with an if statement for Mode 2
						//BUG 4231 makes it comment out the below changes,
						//This will not change even though directly, to be
						//on safer side, we should take out this too.
						//check path fully qualified? then set as data import
						if(2 == fArgMode)
						{
							//BUG 4342
							if(fPmFile.at(0)!= '/')
							{
								std::string aTmp = fPmFile;
								fPmFile = PrepMode2ListOfFiles(aTmp);
							}
							else
							{
								if(fPmFile.find_first_of(' ') != string::npos)
								{
								    std::string aPmFiles = replaceCharInStr(fPmFile, ' ', ',');
								    fPmFile = aPmFiles;
								}
							}
						}
						fLocFile = fPmFile;
					}
				}
				else
				{
					fLocFile = "STDIN";
				}
				//cout << "LocFile set as stdin" << endl;
			}
		}
		else
		{
			// If Mode is not 0 and table name is a required argument
			if (fArgMode != 0)
				throw(runtime_error("No table name specified with schema."));
		}

	}
	else
	{
		// for testing we are allowing data from stdin even with Mode 0
		// that is without LocFileName
		if (0 == fArgMode)
		{
			fLocFile = "STDIN";	//cout << "LocFile set as stdin" << endl;
		}
		else
		{
			// If Mode 0, LocFileName is reqd and otherwies Schema is required
			throw(runtime_error("No schema or local filename specified."));
		}
	}

}

std::string WECmdArgs::getJobFileName()
{
	std::ostringstream aSS;
	string aJobIdFileName;
	if (fJobId.length() > 0)
	{
		if (fJobPath.length() > 0)
			aSS << fJobPath;
		else
		{
			fJobPath = config::Config::makeConfig()->getConfig("WriteEngine",
					"BulkRoot") + "/Job";
			aSS << fJobPath;
		}
		aSS << "/Job_" << fJobId << ".xml";
		aJobIdFileName = aSS.str();
	}
	return aJobIdFileName;
}

bool WECmdArgs::getPmStatus(int Id)
{
	// if no PMID's provided on cmdline, return true;
	if(0==fPmVec.size()) return true;

	VecInts::iterator aIt = fPmVec.begin();
	while (aIt != fPmVec.end())
	{
		if (*aIt == static_cast<unsigned int>(Id))
			return true;
		++aIt;
	}
	return false;
}


//------------------------------------------------------------------------------
// It is a recursive call.
std::string WECmdArgs::getBrmRptFileName()
{
	if(!fBrmRptFile.empty())
		return fBrmRptFile;

	string brmRptFileName = getTmpFileDir();
	if(!brmRptFileName.empty())
	{
		fTmpFileDir = brmRptFileName;
		char aBuff[64];
		time_t aTime;
		struct tm pTm;
		time(&aTime);
		localtime_r(&aTime, &pTm);

		// BUG 4424
		//			M   D   H   M   S
		snprintf(aBuff, sizeof(aBuff), "/BrmRpt%02d%02d%02d%02d%02d%d.rpt",
				pTm.tm_mon, pTm.tm_mday, pTm.tm_hour,
				pTm.tm_min, pTm.tm_sec, getpid());
		brmRptFileName += aBuff;
	}
	else
	{
		//cout << "ERROR: Could not find TempFileDir in Calpont.xml" << endl;
		throw(runtime_error("Could not find TempFileDir in Calpont.xml"));
	}
	setBrmRptFileName(brmRptFileName);

	return brmRptFileName;

}
//------------------------------------------------------------------------------

void WECmdArgs::addJobFilesToVector(std::string& JobName)
{
	//if((!fSchema.empty())&&(!fTable.empty())&&(!fLocFile.empty())) return;

	WEXmlgetter aXmlGetter(JobName);
	vector<string> aSections;
	aSections.push_back("BulkJob");
	aSections.push_back("Schema");
	aSections.push_back("Table");

	//BUG 4163
	typedef std::vector<string> TableVec;
	TableVec aTableVec;
	aXmlGetter.getConfig(aSections[1], aSections[2], aTableVec);
	setMultiTableCount(aTableVec.size());

	if(getMultiTableCount()>1)
	{
		splitConfigFilePerTable(JobName, aTableVec.size());
	}
	else
	{
		fVecJobFiles.push_back(JobName);
	}

}

//------------------------------------------------------------------------------
// Set the schema, table, and loadfile name from the xml job file.
// If running in binary mode, we also get the list of columns for the table,
// so that we can determine the exact fixed record length of the incoming data.
//------------------------------------------------------------------------------
void WECmdArgs::setSchemaAndTableFromJobFile(std::string& JobName)
{
	if (((fVecJobFiles.size()==1)&&(!fSchema.empty())&&
			(!fTable.empty())&&(!fLocFile.empty()))  &&
		(fImportDataMode == IMPORT_DATA_TEXT)) return;

	WEXmlgetter aXmlGetter(JobName);
	vector<string> aSections;
	aSections.push_back("BulkJob");
	aSections.push_back("Schema");
	aSections.push_back("Table");

	// Reset the fSchema, fTable, and FLocFile
	if ((fVecJobFiles.size() > 1) ||
		(fSchema.empty()) || (fTable.empty()) || (fLocFile.empty()))
	{
		std::string aSchemaTable;
		std::string aInputFile;

		aSchemaTable = aXmlGetter.getAttribute(aSections, "tblName");
		if(getDebugLvl()>1) cout << "schema.table = " << aSchemaTable << endl;
		aInputFile = aXmlGetter.getAttribute(aSections, "loadName");
		if(getDebugLvl()>1) cout << "xml::InputFile = " << aInputFile << endl;

		if(aSchemaTable.length()>0)
		{
			char aSchema[64];
			char aTable[64];
			int aRet = aSchemaTable.find('.');
			if(aRet>0)
			{
				int aLen = aSchemaTable.copy(aSchema,aRet);
				if(getDebugLvl()>1) cout << "Schema: " << aSchema << endl;
				aSchema[aLen] = 0;
				if(fSchema.empty()) fSchema = aSchema;
				aLen = aSchemaTable.copy(aTable, aSchemaTable.length(),aRet+1 );
				aTable[aLen]=0;
				if(getDebugLvl()>1) cout << "Table: " << aTable << endl;
				fTable = aTable;
			}
			else
				throw runtime_error(
					"JobFile ERROR: Can't get Schema and Table Name");
			}
		else
		{
			throw runtime_error(
				"JobFile ERROR: Can't get Schema and Table Name");
		}

		if((fLocFile.empty())&&(!aInputFile.empty()))
		{
			string bulkRootPath = config::Config::makeConfig()->getConfig(
									"WriteEngine", "BulkRoot");
			if(aInputFile.at(0) == '/')
				fLocFile = aInputFile;
			else if ((!fPmFilePath.empty())&& (fMode==1))
				fLocFile = fPmFilePath + "/" + aInputFile;
			else if((!bulkRootPath.empty())&&(fPmFilePath.empty()))
				fLocFile = bulkRootPath + "/data/import/"+ aInputFile;
			else
				fLocFile = aInputFile;
			if(fArgMode==2) fPmFile = fLocFile;
		}

		if(getDebugLvl()>1) cout << "schema = " << fSchema << endl;
		if(getDebugLvl()>1) cout << "TableName = " << fTable << endl;
		if(getDebugLvl()>1) cout << "Input File = " << fLocFile << endl;
	}

	// Reset the list of columns we will be importing from the input data
	fColFldsFromJobFile.clear();
	if (fImportDataMode != IMPORT_DATA_TEXT)
	{
		aSections.push_back("Column");
		aXmlGetter.getAttributeListForAllChildren(
			aSections, "colName", fColFldsFromJobFile);
	}
}

//------------------------------------------------------------------------------
void WECmdArgs::checkJobIdCase()
{
	if((fJobId.empty())||(fJobLogOnly)||(fMode==3)||(fMode==0)) return;
	if(fJobPath.empty())
	{
		string bulkRootPath = config::Config::makeConfig()->getConfig(
												"WriteEngine", "BulkRoot");
		//cout << "checkJobIdCase::BulkRoot: " << bulkRootPath << endl;

		if(!bulkRootPath.empty())
			fJobPath = bulkRootPath + "/job";
		else
			throw runtime_error("Config Error: BulkRoot not found in Calpont.xml");
	}
	char aBuff[256];
	if(!fJobPath.empty())
		snprintf(aBuff,sizeof(aBuff),"%s/Job_%s.xml",fJobPath.c_str(),
														fJobId.c_str());
	else	// for time being
		snprintf(aBuff, sizeof(aBuff),"%s/data/bulk/job/Job_%s.xml",
				startup::StartUp::installDir().c_str(), fJobId.c_str());

	std::string aJobFileName(aBuff);

	//cout << "checkJobIdCase::aJobFileName: " << aJobFileName << endl;


	//BUG 4171
	addJobFilesToVector(aJobFileName);

	aJobFileName =  fVecJobFiles[0];
	setSchemaAndTableFromJobFile(aJobFileName);
	setEnclByAndEscCharFromJobFile(aJobFileName);

}

//------------------------------------------------------------------------------

std::string WECmdArgs::getTmpFileDir()
{
	if(!fTmpFileDir.empty()) return fTmpFileDir;

	fTmpFileDir = config::Config::makeConfig()->getConfig("SystemConfig",
																"TempFileDir");
	if(fTmpFileDir.empty())
		throw( runtime_error("Config ERROR: TmpFileDir not found!!"));
	else
		return fTmpFileDir;
}

//------------------------------------------------------------------------------

std::string WECmdArgs::getBulkRootDir()
{
	if(!fBulkRoot.empty()) return fBulkRoot;

	fBulkRoot = config::Config::makeConfig()->getConfig("WriteEngine",
																"BulkRoot");
	if(fBulkRoot.empty())
		throw( runtime_error("Config ERROR: <BulkRoot> not found!!"));
	else
		return fBulkRoot;
}

//------------------------------------------------------------------------------

unsigned int WECmdArgs::getBatchQuantity()
{
	return (fBatchQty>=10000)?fBatchQty:10000;	//default Batch Qty is 10000
}

//------------------------------------------------------------------------------

void WECmdArgs::setEnclByAndEscCharFromJobFile(std::string& JobName)
{
	if((fEnclosedChar == 0))	// check anything in Jobxml file
	{
		WEXmlgetter aXmlGetter(JobName);
		vector<string> aSections;
		aSections.push_back("BulkJob");
		aSections.push_back("EnclosedByChar");

		try
		{
			//std::string aTable = aXmlGetter.getConfig(aSection, aElement);
			std::string aEnclosedBy = aXmlGetter.getValue(aSections);
			if(getDebugLvl()>1)cout << "aEncloseBy = " << aEnclosedBy << endl;
			if(!aEnclosedBy.empty())
			{
				fEnclosedChar = aEnclosedBy.at(0);
			}
		}
		catch(std::runtime_error&)
		{
			// do not do anything
		}
	}

	if(fEscChar == 0)	// check anything in Jobxml file
	{
		WEXmlgetter aXmlGetter(JobName);
		vector<string> aSections;
		aSections.push_back("BulkJob");
		aSections.push_back("EscapeChar");

		try
		{
			//std::string aTable = aXmlGetter.getConfig(aSection, aElement);
			std::string aEscChar = aXmlGetter.getValue(aSections);
			if(getDebugLvl()>1) cout << "aEscapeChar = " << aEscChar << endl;
			if(!aEscChar.empty())
			{
				fEscChar = aEscChar.at(0);
			}
		}
		catch(std::runtime_error&)
		{
			// do not do anything
		}
	}

}

//------------------------------------------------------------------------------
std::string WECmdArgs::getFileNameFromPath(const std::string& Path) const
{
	char aBuff[64];
	int iDx = Path.find_last_of('/');
	iDx++;		// compensate for the forward slash
	int aCx = Path.size() - iDx;
	Path.copy(aBuff, aCx, iDx);
	aBuff[aCx] = 0;
	return aBuff;
}

//------------------------------------------------------------------------------
std::string WECmdArgs::getModuleID()
{
	oam::Oam oam;
	oam::oamModuleInfo_t sModInfo;
	std::string sModuleID;
	char szModuleIDandPID[64];
	int nModuleNumber;

	try 
	{
		sModInfo = oam.getModuleInfo();
		sModuleID = boost::get < 1 > (sModInfo);
		nModuleNumber = boost::get < 2 > (sModInfo);
		snprintf(szModuleIDandPID,sizeof(szModuleIDandPID),"%s%d-%d",
						sModuleID.c_str(), nModuleNumber, getpid());
		sModuleID = szModuleIDandPID;
	}
	catch (exception&) 
	{
		sModuleID = "unknown";
	}

	return sModuleID;
}

//------------------------------------------------------------------------------


void WECmdArgs::splitConfigFilePerTable(std::string& ConfigName, int tblCount)
{
	std::string aOpenTag = "<Table ";
	std::string aCloseTag = "</Table>";
	std::string aCloseSchemaTag = "</Schema>";

	std::vector<std::ofstream*> aVecFiles;
	//std::vector<std::string> aVecConfigs;
	for(int aIdx=1; aIdx<=tblCount; aIdx++)
	{
		char aConfName[128];
		snprintf(aConfName, sizeof(aConfName), "%s_%d.xml", ConfigName.c_str(), aIdx);
		//aVecConfigs.push_back(aConfName);
		fVecJobFiles.push_back(aConfName);
		std::ofstream* pCopy = new std::ofstream;
		//pCopy->open(aConfName, std::ios_base::app);
		pCopy->open(aConfName);
		aVecFiles.push_back(pCopy);
	}


	std::ifstream aMaster;
	aMaster.open(ConfigName.c_str());

	if(aMaster.is_open())
	{
		char aBuff[256];
		int aTblNo = 0;
		size_t aStrPos = std::string::npos;
		bool aOpenFound = false;
		bool aCloseFound = false;

		while(!aMaster.eof())
		{
			aMaster.getline(aBuff, sizeof(aBuff)-1);
			unsigned int aLen = aMaster.gcount();
			if((aLen < (sizeof(aBuff)-2)) && (aLen>0))
			{
				aBuff[aLen-1] = '\n';
				aBuff[aLen]=0;
				string aData = aBuff;
				//cout << "Data Read " << aBuff;

				if(!aOpenFound)
				{
					aStrPos = aData.find(aOpenTag);
					if(aStrPos != std::string::npos)
					{
						aOpenFound = true;
						aTblNo++;
						write2ConfigFiles(aVecFiles, aBuff, aTblNo);
					}
					else
					{
						if((!aOpenFound) && (aCloseFound))
						{
							aStrPos = aData.find(aCloseSchemaTag);
							if(aStrPos != std::string::npos)
							{
								aOpenFound = false;
								aCloseFound = false;
								aTblNo = 0;
							}
						}
						write2ConfigFiles(aVecFiles, aBuff, aTblNo);
					}
				}
				else
				{
					aStrPos = aData.find(aCloseTag);
					if(aStrPos != std::string::npos)
					{
						aOpenFound = false;
						aCloseFound = true;
						write2ConfigFiles(aVecFiles, aBuff, aTblNo);
					}
					else
					{
						write2ConfigFiles(aVecFiles, aBuff, aTblNo);
					}
				}
			}
		}//while Master.eof
	}
	else
	{
		throw runtime_error("Could not open Job Config file");
	}


	for(unsigned int Idx=0; Idx < aVecFiles.size(); Idx++)
	{
		aVecFiles[Idx]->close();
		delete aVecFiles[Idx];
	}

	aVecFiles.clear();

}

//------------------------------------------------------------------------------

void WECmdArgs::write2ConfigFiles(std::vector<std::ofstream*>& Files,
														char*pBuff, int FileIdx)
{

	if(FileIdx == 0)
	{
		std::vector<std::ofstream*>::iterator aIt = Files.begin();
		while(aIt != Files.end())
		{
			std::ofstream* pCopy = (*aIt);
			pCopy->write(pBuff, strlen(pBuff));
			++aIt;
		}
	}
	else
	{
		Files[FileIdx-1]->write(pBuff, strlen(pBuff));
	}
}

//------------------------------------------------------------------------------

void WECmdArgs::updateWithJobFile(int Idx)
{
	setLocFile("");	// resetting the from the previous import	
	std::string aJobFileName =  fVecJobFiles[Idx];
	setSchemaAndTableFromJobFile(aJobFileName);
	setEnclByAndEscCharFromJobFile(aJobFileName);
	setJobFileName(aJobFileName);

	std::ostringstream aSS;
	aSS << fOrigJobId << ".xml_"<<(Idx+1);
	fJobId = aSS.str();
}


//------------------------------------------------------------------------------

std::string WECmdArgs::replaceCharInStr(const std::string& Str,char C,char R)
{
	std::stringstream aSs;

	size_t start=0, end=0;
	end = Str.find_first_of(C);
	do
	{
		if(end != string::npos)
		{
			aSs << Str.substr(start, end-start) << R;
			start = end + 1;
		}
		else
		{
			aSs << Str.substr(start, end-start);
			break;
		}
		end = Str.find_first_of(C,start);
	}
	while(start != end);

	return aSs.str();
}

//------------------------------------------------------------------------------
// Introduced to handle Bug 4342 with Mode 2

std::string WECmdArgs::PrepMode2ListOfFiles(std::string& FileName)
{
	VecArgs aInfileList;
	std::string bulkRootPath = getBulkRootDir();
	//cout << "Inside PrepMode2ListOfFiles("<< FileName << ")" << endl;
	std::string aFileName = FileName;

	istringstream iss(aFileName);
	size_t start = 0, end = 0;
	const char* sep = " ,|";

	end = aFileName.find_first_of(sep);
	do
	{
		if(end != string::npos)
		{
			std::string aFile = aFileName.substr(start, end-start);
			if(getDebugLvl()>1)
				cout << "File: " << aFileName.substr(start, end-start) << endl;
			start = end + 1;
			aInfileList.push_back(aFile);
		}
		else
		{
			std::string aFile = aFileName.substr(start, end-start);
			if(getDebugLvl()>1)
				cout << "Next Input File " << aFileName.substr(start, end-start) << endl;
			aInfileList.push_back(aFile);
			break;
		}
		end = aFileName.find_first_of(sep, start);
	}
	while(start != end);

	std::ostringstream aSS;
	int aVecSize = aInfileList.size();
	int aVecIdx = 0;
	// Take file list one by one and append it to one string
	while(aVecIdx < aVecSize)
	{
		std::string aNextFile = aInfileList[aVecIdx];
		aVecIdx++;
		//aInfileList.pop_front();
		if(aNextFile.at(0) != '/')
		{
			aSS << bulkRootPath << "/data/import/"+aNextFile;
		}
		else
		{
			aSS <<aNextFile;
		}
		if(aVecIdx < aVecSize) aSS <<",";
	}

	//cout << "File list are = " << aSS.str() << endl;

	return aSS.str();
}

//------------------------------------------------------------------------------
// Get set of column names in the "current" table being processed from the
// Job xml file.
//------------------------------------------------------------------------------
void WECmdArgs::getColumnList( std::set<std::string>& columnList ) const
{
	columnList.clear();
	for (unsigned k=0; k<fColFldsFromJobFile.size(); k++)
	{
		columnList.insert( fColFldsFromJobFile[k] );
	}
}



} /* namespace WriteEngine */
