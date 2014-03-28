/* Copyright (C) 2013 Calpont Corp.

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

/*************************
*
* $Id: replaytxnlog.cpp 2678 2007-06-14 03:41:50Z wweeks $
*
*************************/
#include <iostream>
#include <string>
#include <sstream>
#include <ctime>
#include <fstream>
#include <ctime>
#include <stdexcept>
#include <cstdlib>
using namespace std;

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/algorithm/string/case_conv.hpp>
namespace fs = boost::filesystem;

#include "replaytxnlog.h"
#include "sessionmanager.h"
#include "liboamcpp.h"
#include "messagelog.h"
#include "messageids.h"

// Testing definitions.  These should never be commented out in checked in code.
#define OAM_AVAILABLE		// Comment out to skip the OAM calls to get log file locations and turn on/off logging.
#define LAST_TXN_AVAILABLE	// Comment out to use a test start txnId.
#define CALDB_AVAILABLE      	// Comment out to use against non-Calpont db (commits instead of calcommits).

ReplayTxnLog::ReplayTxnLog(const string& user, const string& password, const string& stopDateStr, 
			   const bool& ignoreBulkLoad, const bool& reportMode) : 
	fUser(user), fPassword(password), fStopDateStr(stopDateStr), fIgnoreBulkLoad(ignoreBulkLoad)
{
	// Initialize fCurrentYearStr.
	// WWW - Check for a better way to do this.
	time_t ttNow;
	time(&ttNow);
	struct tm tmNow;
	localtime_r(&ttNow, &tmNow);
	int year = tmNow.tm_year + 1900;
	ostringstream oss;
	oss << setw(4) << year;
	fCurrentYearStr = oss.str();

	// Set the program start time.
	time(&fProgramStarted);

	fDDLCount = 0;
	fCommitCount = 0; 
	fRollbackCount = 0;
	fLastSchemaOwner = "";
};

/***************************************************************************
*
* Function:  validateInput
*
* Purpose:   Validates the user id, password, and stop date string.  Prompts
*	     for user id, password, and stop date if they were not passed in 
*            with the constructor.  Attempts to start a database session using 
*            the user and password.  Throws a runtime_error if the session
*	     can not be started.
*
****************************************************************************/
void ReplayTxnLog::validateInput() {
	
	bool validStopDate = false;

	// Prompt for a user if not provided.
/*
	while(fUser.length() == 0) {
		cout << "Enter User: ";
		getline(cin, fUser);
	}

	// Prompt including commits and rollbacks.for a password if not provided.
	// WWW - Make the password masked with *s.
	while(fPassword.length() == 0 && !fReportMode) {
		cout << "Enter Password: ";
		getline(cin, fPassword);
	}

	if(!fReportMode) {
		// @bug 2417.  Took out the convert user id to uppercase.  Leftover from a prior db front end.
		// boost::algorithm::to_upper(fUser);
		fSession.startSession(fUser, fPassword, fOracleSID);
	}

	// Validate the stop date.  Default to 'Now' if a stop date wasn't passed.
	while(!validStopDate) {
*/
		if(fStopDateStr == "Now" || fStopDateStr.length() == 0) 
		{
			validStopDate = true;
			time(&fStopDate);
		}
		else if(fStopDateStr.length() == 17) 
		{
			if((fStopDateStr[2] != '/') || (fStopDateStr[5] != '/') || 
			   (fStopDateStr[8] != '@') || (fStopDateStr[11] != ':') || 
			   (fStopDateStr[14] != ':')) 
			{
				validStopDate = false;
			}
			else {
				tm lts;

				// Build date structure.
				lts.tm_isdst = -1;  // -1 for unknown whether date is during Daylight Saving Time.
				lts.tm_mon = atoi(fStopDateStr.substr(0, 2).c_str()) - 1;    // Month is 0..11
				lts.tm_mday = atoi(fStopDateStr.substr(3, 2).c_str());       // Day
				lts.tm_year = atoi(fStopDateStr.substr(6, 2).c_str()) + 100; // Year since 1900
				lts.tm_hour = atoi(fStopDateStr.substr(9, 2).c_str());       // Hour
				lts.tm_min = atoi(fStopDateStr.substr(12, 2).c_str());	     // Minute
				lts.tm_sec = atoi(fStopDateStr.substr(15, 2).c_str());	     // Second

				// Convert to a time_t object.
				fStopDate = mktime(&lts);

				validStopDate = true;
			}
		}
		if (!validStopDate) 
		{
			if(fStopDateStr.length() > 0) 
			{
/*
				cout << endl;
				cout << "'" << fStopDateStr << "' is not a valid date and time." << endl;
				cout << endl;
*/
				throw runtime_error(fStopDateStr + " is not a valid date and time.");

			}
			cout << "Enter Stop Date and Time as 'mm/dd/yy@hh:mm:ss' or 'Now': ";
			getline(cin, fStopDateStr);
		}
//	}
}

/***************************************************************************
*
* Function:  split
*
* Purpose:   Split a string on a given character into a vector of strings.
*	     The vector is passed by reference and cleared each time.
*	     The number of strings split out is returned.
*
****************************************************************************/
int ReplayTxnLog::split(vector<string>& v, const string& str, const char& c)
{
	v.clear();
	string::const_iterator s = str.begin();
	while (true) {
		string::const_iterator begin = s;
	
		while (*s != c && s != str.end()) { ++s; }
	
		v.push_back(string(begin, s));
	
		if (s == str.end()) {
		break;
		}
	
		if (++s == str.end()) {
		v.push_back("");
		break;
		}
	}
	return v.size();
}

/***************************************************************************
*
* Function:  convertLogDateStr
*
* Purpose:   Converts a string formatted as "Apr 30 16:10:49" to a time_t.
*	     The date is passed in as three string parameters, such as
*	     "Apr", "30", "16:10:49".
*	     
* Note:	     There is no year included in the syslog time stamp.  This 
*	     function returns the date in the current year if the date is 
*	     at or before the current date and time.  If the date on a 
*	     after current date / time, it returns the date in the prior year.
*
****************************************************************************/
time_t ReplayTxnLog::convertLogDateStr(const string& sMth, const string& sDay, const string& sTime) {
	time_t tt;
	tm ltm = {0,0,0,0,0,0,0,0,-1};  // -1 is tm_dst unknown.  

	// Convert the log time to a time_t using the current year.
	string dt = fCurrentYearStr + "-" + sMth + "-" + sDay + "-" + sTime;
	strptime(dt.c_str(), "%Y-%b-%d-%H:%M:%S", &ltm);
	tt = mktime(&ltm);

	// If the log time is in the future, change it to the prior year.
	if(tt > fProgramStarted) {
		ltm.tm_year--;
		tt = mktime(&ltm);
	}
	return tt;
}

/***************************************************************************
*
	* Function:  getNextCalpontLogEntry
*
* Purpose:   Reads the passed filestream until a Calpont log entry is found.
*	     The log entry is assigned in the pass by reference argument.
*	     A bool is returned indicating whether a Calpont entry was found.
*
****************************************************************************/
bool ReplayTxnLog::getNextCalpontLogEntry(ifstream& dataFile, LogEntry& logEntry) {
	bool found = false;
	string text;
	string::size_type pos;
	vector<string> v;
	vector<string> vFirstSection;
	/*
	Sample log entries:
	Mar 11 01:54:04 srvswdev3 Calpont[29720]: 04.372658 |5|1|0| C 00 CAL0018: DDL|WALT|CREATE TABLE TEST (XXX CHAR(8));
	Mar 11 02:40:22 srvswdev3 Calpont[29720]: 04.373627 |7|2|0| C 00 CAL0017: DML|WALT|INSERT INTO TEST (XXX) values ('TEST');
	Mar 11 02:40:24 srvswdev3 Calpont[29720]: 04.373429 |5|1|0| C 00 CAL0019: CMD|COMMIT;
	*/

	while(!found && getline(dataFile, text)) {
		split(v, text, '|');
		if(v.size() >= 5) {

			// Read the log entry type.
			if(v[4].find("CAL0017") != string::npos) {
				logEntry.type = DML;
			}
			else if(v[4].find("CAL0019") != string::npos) {
				if(v[5].find("COMMIT") != string::npos) {
					logEntry.type = XCOMMIT; // WWW - COMMIT caused compiler error, 
				}
				else if(v[5].find("ROLLBACK") != string::npos) {
					logEntry.type = ROLLBACK;
				}
			}
			else if(v[4].find("CAL0018") != string::npos) {
				logEntry.type = DDL;
			}
			else if(v[4].find("CAL0008") != string::npos) {
				logEntry.type = BULKLOAD;
				logEntry.fSchemaOwner = v[5]; // steal schema owner for job number
                                logEntry.fSqlStatement = v[6];

			}
			else if(v[4].find("CAL0022") != string::npos) {
				if(!fIgnoreBulkLoad) {
					cout << endl;
					cout << "Encountered the following bulk load log entry." << endl;
					cout << text << endl;
					cout << "Press Enter to continue." << endl;
					getline(cin, text);
					cout << endl;
					continue;
				}
			}
			else {
				continue;
			}

			// Get the versionID.
			logEntry.fVersionID = atoi(v[2].c_str());

			// Get the current schema and sql for DML or DDL.
			if(logEntry.type == DML or logEntry.type == DDL) {
				logEntry.fSchemaOwner = v[5];

				// Parse out the sql statement.
				if(v.size() == 7) {
					logEntry.fSqlStatement = v[6];
				}
				// Sql contains one or more "|" (separator) characters.
				else {	
					pos = text.find(v[6]);
					logEntry.fSqlStatement = text.substr(pos, text.size() - pos);
				}
			}

			// Parse out the date and time. 
			split(vFirstSection, v[0], ' ');  
			if(vFirstSection.size() < 3) {
				continue; 
			}
			else {

				// Quick hack for extra spaces showing up in the date such as:
				// Mar  7 21:28:36 srvperf6 DDLProc[7964]: 36.179609 |1|86|0| C 15 CAL0018: DDL ||CREATE TABLE Y (K NUMERIC)ENGINE=INFINIDB;
				// This can be stripped out when the log is no longer using syslog.
				if(vFirstSection[1].size() == 0)
					logEntry.fLogTime = convertLogDateStr(vFirstSection[0], vFirstSection[2], vFirstSection[3]);
				else	
					logEntry.fLogTime = convertLogDateStr(vFirstSection[0], vFirstSection[1], vFirstSection[2]);
			}
			found = true;
		}
	}
	return found;
}


/***************************************************************************
*
* Function:  getLogFileNames
*
* Purpose:   Returns the list of files that contain the transactions that will
*	     be replayed.  
*
* NOTE:      Sorting the files by name must put the files in
*	     date sorted order or this will not work.
*
****************************************************************************/
list<string> ReplayTxnLog::getLogFileNames()
{
	oam::Oam oam;
	string::size_type pos;
	string fullLogFileName;
	string dir;
	string archiveDir;
	string logFileName;

	// Get the log file name, directory, and archive file directory.
/*
	#ifdef OAM_AVAILABLE
		oam.getLogFile("pm1", "data", fullLogFileName);
	#else
*/
		fullLogFileName = "/var/log/Calpont/data/data_mods.log";
/*
	#endif
*/
	pos = fullLogFileName.rfind("/");
	if(pos == string::npos) {
		throw runtime_error("Unable to parse log file path - " + fullLogFileName);
	}
	dir = fullLogFileName.substr(0, pos);
	logFileName = fullLogFileName.substr(pos + 1, fullLogFileName.size() - pos + 1);
	archiveDir = dir + "/archive";
	
	// Read the archive directory and get a list of archived log file names.
	list<string> dbFileNames;
	string fileName;
	fs::path sourceDir(archiveDir); 
	fs::directory_iterator iter(sourceDir);
	fs::directory_iterator end_iter;
	while (iter != end_iter)
	{
		fs::path source = *iter;
		if (!fs::is_directory(source) )	{
#if BOOST_VERSION >= 105200
			fileName = source.filename().c_str();
#else
			fileName = iter->leaf();
#endif
			if(fileName.find(logFileName, 0) == 0) { 
				fileName = archiveDir + "/" + fileName;
				dbFileNames.push_back(fileName);
			}
		}
		++iter;
	}

	// Sort the list by file name.  The files are named such that sorting them by name will be the
	// same as sorting them by the order they were created.
	dbFileNames.sort();

	// Make sure the current log file exists and add it to the end of the list.
	if(!fs::exists(fullLogFileName)) {
		throw runtime_error("Current log file not found:  " + fullLogFileName);
	}
	else {
		dbFileNames.push_back(fullLogFileName);
	}
	
	// WWW - Warn the user and prompt to exit if there are missing file names.

	// If there is more than one log file, Read through the first transaction id in each file until 
	// we find the one that begins with an id greater than the one we are starting with.  Once we've 
	// found that, we know we can start with the previous file.
	list<string>::iterator curFile;
	list<string>::iterator emptyFile;
	vector<string> v;
	if(dbFileNames.size() > 1) {
		curFile = dbFileNames.begin();
		curFile++;
		bool done = false;
		bool validLogEntry = false;
		LogEntry logEntry;

		while(!done && curFile != dbFileNames.end()) {
			fileName = *curFile;
			ifstream dataFile(fileName.c_str());
			validLogEntry = getNextCalpontLogEntry(dataFile, logEntry);
			dataFile.close();

			if(!validLogEntry) { // Empty file
				emptyFile = curFile;
				curFile++;
				dbFileNames.erase(emptyFile); // Zap the empty file from the list.
			}
			else if (logEntry.fVersionID > fStartVersionID) {
				done = true;
			}
			else {
				
				// This file starts with an id less than our start id, so the prior file
				// contains only transactions from before the backup was restored.  Throw 
				// the prior file away and keep looking.
				dbFileNames.pop_front();
				curFile++;
			}
		}
	}
	

	return dbFileNames;
}

/***************************************************************************
*
* Function:  processStatement
*
* Purpose:   Receives a versionID and a statement. 
*
*	     For DML statements, the statement is added to the transaction if 
*	     it's a subsequent statement in an active transaction.  If it's for a 
*	     new transaction, the transactioni is first added to the vector of 
*	     active transactions, then the statement is added to the transaction's 
*	     list of statements.
*
*	     For Rollbacks, the transaction is zapped from the vector of active
*	     transactions.  No database rollback is done because the statements
*	     within the transaction never hit the database.
*
*	     For Commits, the transaction's statements are issued and committed
*	     to the database, then the transaction is removed from the vector.
*
*	     For DDL statements, the statement is executed immediately.  Each
*	     DDL statement is it's own transaction.
****************************************************************************/
void ReplayTxnLog::processStatement(const LogEntry& logEntry) {

	vector<ActiveTxn>::iterator curTxn;
	bool found = false;

	// First, see if transaction already active.
	curTxn = fActiveTxns.begin();
	while(!found && (curTxn != fActiveTxns.end())) {
		if(curTxn->fVersionID == logEntry.fVersionID) {
			found = true;
		}
		else {
			curTxn++;
		}
	}


	// If it's a commit or DDL statement, issue each statement for the transaction, then 
	// process the DLL statement or issue the commit.
	if(logEntry.type == XCOMMIT || logEntry.type == DDL) {

		replaytxnlog::DBSession dbSession;

		// Track the first committed txn and last committed txn.  DDL statements cause
		// an auto commit, so consider those to be commits.
		if(fLogEntryFirstCommit.fLogTime == 0) {
			fLogEntryFirstCommit = logEntry;
		}
		fLogEntryLastCommit = logEntry;

		// Increment commit count or ddl count.
		if(logEntry.type == XCOMMIT) {
			fCommitCount++;
		} 
		else {
			fDDLCount++;
		}
		cout << "-- Transaction " << logEntry.fVersionID << " committed " <<
				ctime(&logEntry.fLogTime) ;
		dbSession.startSession(logEntry.fSchemaOwner);

		if(found) {

			// Loop through the statements and execute each one.
			vector<LogEntry>::iterator curStatement;
			for(curStatement = curTxn->fStatements.begin(); 
					    curStatement != curTxn->fStatements.end(); curStatement++) 
			{
				dbSession.issueStatement(curStatement->fSqlStatement);
			}

			// Commit.
			curTxn->fStatements.clear();
			if(logEntry.type == XCOMMIT) {
				dbSession.commit();
			}

			// Clear the transaction from the vector.
			fActiveTxns.erase(curTxn);
		}
		
		// If it's DDL, issue the statement.
		if(logEntry.type == DDL) {
			dbSession.issueStatement(logEntry.fSqlStatement);
		}
		dbSession.endSession();
	}

	else if(logEntry.type == BULKLOAD) {
		cout << "-- Import completed " << ctime(&logEntry.fLogTime);
		cout << "-- " << logEntry.fSqlStatement << endl;
		cout << "-- " << logEntry.fSchemaOwner << endl << endl;
	}

	// If it's a rollback, clear the transaction from the vector.  No need to do any database work.
	else if(logEntry.type == ROLLBACK) {
		fRollbackCount++;
		if(found) {

			// Throw away the transaction.
			curTxn->fStatements.clear();
			fActiveTxns.erase(curTxn);
		}
	}

	// If it's a DML statement, add it to the vector of statements for the transaction.
	else if(logEntry.type == DML) {
		if(found) {
			curTxn->fStatements.push_back(logEntry);
		}
		else {
			ActiveTxn newTxn;
			newTxn.fVersionID = logEntry.fVersionID;
			newTxn.fStatements.push_back(logEntry);
			fActiveTxns.push_back(newTxn);
		}
	}

}

/***************************************************************************
*
* Function:  displaySummaryAndConfirm
*
* Purpose:   Displays a message with the date and time of the first transaction
*	     to be replayed and the date and time of the last transaction.
****************************************************************************/
bool ReplayTxnLog::displaySummaryAndConfirm(const LogEntry& logEntry) {
	string replyYN;

/*	
	// Show summary.
	cout << endl;
	cout << "--" << endl;
	cout << "-- Summary" << endl;
	cout << "--" << endl;
	cout << "-- First Transaction   :  " << logEntry.fVersionID << endl;
	cout << "-- Start Date and Time :  " << ctime(&logEntry.fLogTime);
	cout << "-- Stop Date and Time  :  " << ctime(&fStopDate) << endl;
	cout << "-- Transactions that were committed before the stop date and time will be reported." << endl;
	cout << "--" << endl;
	
	cout << endl;

	// Prompt to continue.
	do {
		cout << "Would you like to continue (Y/N)?";
		getline(cin, replyYN);
	} 
	while ((replyYN != "Y") && (replyYN != "N"));
	cout << endl;
	if(replyYN == "Y") {
		return true;
	}
	return false;
*/
	return true;
}

/***************************************************************************
*
* Function:  displayProgress
*
* Purpose:   Outputs progress message.
****************************************************************************/
void ReplayTxnLog::displayProgress(const string& fileName) {
	// WWW - Check into having this stay on the same line like dbBuilder.
/*
	if(fLogEntryLastCommit.fLogTime > 0) {
		cout << "Last transaction committed " << fLogEntryLastCommit.fVersionID << " from " <<
			ctime(&fLogEntryLastCommit.fLogTime); 
	}
	cout << endl;
	cout << "Now processing file " << fileName << endl;
*/
}

/***************************************************************************
*
* Function:  replayTransactions
*
* Purpose:   Reads the qualifying files and processes the transactions 
*	     starting with the first one with an id greater than the last
*	     id handed out before the backup and ending with the last
*	     transaction committed before the stop date.
****************************************************************************/
void ReplayTxnLog::replayTransactions(list<string>& fileNames) {
	list<string>::iterator curFile;
	vector<string> v;
	curFile = fileNames.begin();
	LogEntry logEntry;
	string statement;
	bool done = false;
	bool confirmed = true;
	oam::Oam oam;
	
	curFile = fileNames.begin();

//	cout << "Searching files for first transaction to process." << endl << endl;

	while (!done && curFile != fileNames.end()) {
		ifstream dataFile(curFile->c_str());
		if(confirmed) {
			displayProgress(*curFile);
		}
		while(!done && getNextCalpontLogEntry(dataFile, logEntry)) {
			if(logEntry.fLogTime >= fStopDate) {
				done = true;
			}
			else if(logEntry.fVersionID > fStartVersionID) {
				if(confirmed) {
					processStatement(logEntry);
				}
/*
				else if(displaySummaryAndConfirm(logEntry)) {
					confirmed = true;
					if(!fReportMode) {

						// Log start message.
						ostringstream oss;
						oss << "First transaction ID to be processed is " << logEntry.fVersionID << ".";
						string msg = oss.str();
						logging::logEventToDataLog(logging::M0019, msg);
				
						// Turn off DML/DDL logging to keep from the statements from 
						// being logged again.
						#ifdef OAM_AVAILABLE
						cout << endl << "Turning off DML/DDL logging." << endl << endl;
						oam.updateLog(oam::MANDISABLEDSTATE, "system", "data"); 
						#endif
					}
					displayProgress(*curFile);
					processStatement(logEntry);
				}
				else {
					done = true;
				}
*/
			}
		}
		dataFile.close();
		curFile++;
	}	

	// Turn DML/DDL logging back on.
/*
	if(!fReportMode && confirmed) {
		#ifdef OAM_AVAILABLE
		cout << endl << "Turning DML/DDL logging back on." << endl << endl;
		oam.updateLog(oam::ENABLEDSTATE, "system", "data"); 
		#endif

		// Log complete message.
		ostringstream oss;
		oss << "Last transaction ID processed was " << fLogEntryLastCommit.fVersionID << ".";
		string msg = oss.str();
		logging::logEventToDataLog(logging::M0020, msg);
	}
*/
}

/***************************************************************************
*
* Function:  checkDefinitions
*
* Purpose:   Reminder to uncomment required #defines.
****************************************************************************/
void ReplayTxnLog::checkDefinitions() {
	#ifndef OAM_AVAILABLE
	cout << "Warning!!!  Uncomment #define OAM_AVAILABLE before checking in." << endl;
	#endif
	#ifndef LAST_TXN_AVAILABLE
	cout << "Warning!!!  Uncomment #define LAST_TXN_AVAILABLE before checking in."  << endl;
	#endif
	#ifndef CALDB_AVAILABLE
	cout << "Warning!!!  Uncomment #define CALDB_AVAILABLE before checking in." << endl;
	#endif
}

/***************************************************************************
*
* Function:  displayFinalSummary
*
* Purpose:   Shows totals at end of program.
****************************************************************************/
void ReplayTxnLog::displayFinalSummary() {
	cout << endl;
	if(fCommitCount == 0 && fRollbackCount == 0 && fDDLCount == 0) {
		cout << "No transactions were processed." << endl;
	}
	else {
/*
		cout << "---------------------------------------------------------------------------------------" << endl;
		cout << "-- Summary of transaction reported." << endl;
		cout << "--" << endl;
		cout << "-- Commits        :  " << fCommitCount << endl;
//		cout << "-- Rollbacks      :  " << fRollbackCount << endl;
		cout << "-- DDL Statements :  " << fDDLCount << endl;
		cout << "--" << endl;
		cout << "-- First Commit :  " << "Transaction " << fLogEntryFirstCommit.fVersionID << " originally " <<
			"committed " << ctime(&fLogEntryFirstCommit.fLogTime);
		cout << "-- Last Commit  :  " << "Transaction " << fLogEntryLastCommit.fVersionID << " originally " <<
			"committed " << ctime(&fLogEntryLastCommit.fLogTime) ;
		int openCount = fActiveTxns.size();
		if(openCount > 0) {
			cout << "-- " << endl;
			cout << "-- There were " << openCount << " transactions that were open at the stop time." << endl;
		}
		cout << "---------------------------------------------------------------------------------------" << endl;
		cout << endl;
*/
	}
}

/***************************************************************************
*
* Function:  process
*
* Purpose:   The main function.  Receives and validates the user's input and 
*	     and processes the transactions contained in the log files.
****************************************************************************/
void ReplayTxnLog::process() {
	list<string> dbFileNames;

	// Output a warning to the developer if #defines are commented out.
	checkDefinitions();

	// Validate the user id, password, and stop date.
	validateInput(); // Starts the session 

	// Get the last transaction id.
	execplan::SessionManager mgr;

	// Get the last version from issued before the backup.
	#ifdef LAST_TXN_AVAILABLE
		fStartVersionID = mgr.verID().currentScn;
	#else
		fStartVersionID = 0;
	#endif

	cout << "-- The current database version ID is " << fStartVersionID << "." << endl << endl;

	// Get the log file names.
	dbFileNames = getLogFileNames();

	// Replay the transactions.
	replayTransactions(dbFileNames);

	// Display summary of transactions processed.
	displayFinalSummary();

	// Warn the developer again if #defines are commented out.
	checkDefinitions();

//	cout << "Program completed successfully." << endl;
}

namespace replaytxnlog
{

DBSession::DBSession() {
        fConnectedUser = "";
}

// Begins an Oracle session.
void DBSession::startSession(const std::string& connectAsUser)
{
        cout << "use " << connectAsUser << ";" << endl;
        fConnectedUser = connectAsUser;
}

void DBSession::issueStatement(const std::string& stmt) {

        try {

                cout << stmt << endl;

        } catch(std::runtime_error e) {
                throw std::runtime_error("Error executing Statement:  " + stmt + "\n" + e.what());
        }
}


void DBSession::endSession() {
	cout << endl;
}

void DBSession::commit() {
        cout << "commit;" << endl << endl;
}

void DBSession::rollback() {
        cout << "rollback;" << endl << endl;
}

std::string DBSession::getConnectedUser() {
        return fConnectedUser;
}

} //namespace replaytxnlog

