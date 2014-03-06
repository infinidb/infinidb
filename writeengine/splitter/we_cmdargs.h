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
* $Id$ 
*
*******************************************************************************/
#ifndef WE_CMDARGS_H_
#define WE_CMDARGS_H_


#include "we_xmlgetter.h"
#include "we_type.h"
#include <set>

namespace WriteEngine
{

class WECmdArgs
{
    public:
        WECmdArgs(int argc, char** argv);
        virtual ~WECmdArgs(){}
    public:
        void appTestFunction();
        void parseCmdLineArgs(int argc, char** argv);
        std::string getCpImportCmdLine();
        void setSchemaAndTableFromJobFile(std::string& JobName);
        void setEnclByAndEscCharFromJobFile(std::string& JobName);
        void usage();
        void usageMode3();
        bool checkForCornerCases();

        void addJobFilesToVector(std::string& JobName);
    	void splitConfigFilePerTable(std::string& ConfigName, int tblCount);
    	void write2ConfigFiles(std::vector<std::ofstream*>& Files,
    												char*pBuff, int FileIdx);
    	void updateWithJobFile(int Idx);

    public:
        std::string getJobFileName();
		std::string getBrmRptFileName();
		std::string getTmpFileDir();
		std::string getBulkRootDir();
		unsigned int getBatchQuantity();
		void checkJobIdCase();
		std::string getFileNameFromPath(const std::string& Path) const;
		std::string getModuleID();
		std::string replaceCharInStr(const std::string& Str,char C,char R);

		std::string getJobId() const { return fJobId; }
		std::string getOrigJobId() const { return fOrigJobId; }
		std::string getLocFile() const { return fLocFile; }
		int getMode() const { return fMode;	}
		int getArgMode() const {	return fArgMode; }
		bool isHelpMode() { return fHelp; }
		int getDebugLvl() {	return fDebugLvl; }
		char getEnclChar() { return fEnclosedChar; }
		char getEscChar() { return fEscChar;	}
		char getDelimChar() { return fColDelim; }
		ImportDataMode getImportDataMode() const { return fImportDataMode; }
		bool getConsoleLog() { return fConsoleLog; }

		bool isCpimportInvokeMode(){return (fBlockMode3)? false : fCpiInvoke;}
		bool isQuiteMode() const {	return fQuiteMode;	}
		void setJobId(std::string fJobId) {	this->fJobId = fJobId;	}
		void setOrigJobId(std::string fOrigJobId){ this->fOrigJobId = fJobId; }
		void setLocFile(std::string fLocFile) { this->fLocFile = fLocFile; }
		void setMode(int fMode) { this->fMode = fMode; }
		void setArgMode(int ArgMode) { this->fArgMode = ArgMode; }
		void setPmFile(std::string fPmFile)	{ this->fPmFile = fPmFile; }
		void setQuiteMode(bool fQuiteMode) { this->fQuiteMode = fQuiteMode;	}
		void setVerbose(int fVerbose) { this->fVerbose = fVerbose; }
		void setJobFileName(std::string& JobFileName){fJobFile = JobFileName;}
		void setBrmRptFileName(std::string& BrmFileName)
		{ this->fBrmRptFile = BrmFileName; }
		void setCpiInvoke(bool CpiInvoke=true) { this->fCpiInvoke = CpiInvoke; }
		void setBlockMode3(bool Block) { this->fBlockMode3 = Block; }
		void setTruncationAsError(bool bTruncationAsError)
		{ fbTruncationAsError = bTruncationAsError;	}

    private:	// variables for SplitterApp
        typedef std::vector<std::string> VecArgs;
        VecArgs fVecArgs;
        const int MAXARG;
        typedef std::vector<unsigned int> VecInts;
        VecInts fPmVec;

        VecArgs fVecJobFiles;		//JobFiles splitter from master JobFile
        int fMultiTableCount;		//MultiTable count
        VecArgs fColFldsFromJobFile;//List of columns from any job file, that
                                    // represent fields in the import data

    public:
		bool getPmStatus(int Id);
        bool str2PmList(std::string& PmList, VecInts& V);
        int getPmVecSize(){ return fPmVec.size(); }
        void add2PmVec(int PmId){ fPmVec.push_back(PmId); }

		WECmdArgs::VecInts& getPmVec() { return fPmVec;	}
		std::string getPmFile() const {	return fPmFile;	}
		int getVerbose() const { return fVerbose; }
		std::string getTableName() const { return fTable; }
		std::string getSchemaName() const { return fSchema; }
		bool getTruncationAsError() const {return fbTruncationAsError; }

		int getMultiTableCount() const { return fMultiTableCount; }
		void setMultiTableCount(int Count) { fMultiTableCount = Count; }

		std::string PrepMode2ListOfFiles(std::string& FileName); // Bug 4342
		void getColumnList( std::set<std::string>& columnList ) const;

    private:	// variables for SplitterApp
        std::string fJobId;				// JobID
        std::string fOrigJobId;	// Original JobID, in case we have to split it
        bool fHelp;				// Help mode
        int fMode;				// splitter Mode
        int fArgMode;			// Argument mode, dep. on this fMode is decided.
        bool fQuiteMode;		// in quite mode or not
        bool fConsoleLog;		// Log everything to console - w.r.t cpimport
        int fVerbose;			// how many v's
        std::string fPmFile;	// FileName at PM
        std::string fPmFilePath;// Path of input file in PM
        std::string fLocFile;	// Local file name
        std::string fBrmRptFile;// BRM report file
        std::string fJobPath;	// Path to Job File
        std::string fTmpFileDir;// Temp file directory.
        std::string fBulkRoot;	// Bulk Root path
        std::string fJobFile;	// Job File Name

        unsigned int fBatchQty;	// No. of batch Qty.
        int fNoOfReadThrds;		// No. of read buffers
        //std::string fConfig;	// config filename
        int fDebugLvl;			// Debug level
        int fMaxErrors;			// Max allowable errors
        int fReadBufSize;		// Read buffer size
        int fIOReadBufSize;		// I/O read buffer size
        int fSetBufSize;		// Buff size w/setvbuf
        char fColDelim;			// column delimiter
        char fEnclosedChar;		// enclosed by char
        char fEscChar;			// esc char
        int fNoOfWriteThrds;	// No. of write threads
        bool fNullStrMode;		// set null string mode - treat null as null
        ImportDataMode fImportDataMode; // Importing text or binary data		
        std::string fPrgmName;	// argv[0]
        std::string fSchema;	// Schema name - positional parmater
        std::string fTable;		// Table name - table name parameter

        bool fCpiInvoke;		// invoke cpimport in mode 3
        bool fBlockMode3;		// Do not allow Mode 3
        bool fbTruncationAsError; // Treat string truncation as error
};
//----------------------------------------------------------------------


}

#endif /* WE_CMDARGS_H_ */
