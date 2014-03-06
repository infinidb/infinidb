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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/
// WWW - Check on header.

/** @file 
 * class ReplayTxnLog interface
 */
 
#include <string>
#include <list>
#include <ctime>

#include "calpontsystemcatalog.h"

/** @brief Replays remaining transactions after a backup is restored.
 * 
 *
 * This class replays transactions after a full backup has been restored.  The class re-issues statements 
 * from the DML/DDL log files allowing the database to be a state from a point in time after the backup occurred.
 *
 * The user provides an Oracle user id and password as well as a stop date and time.  Transactions that were committed
 * after the time of the backup through before the stop date and time will be replayed in the order that they were 
 * committed.
 *
 * This program only works under the following conditions:
 * 1) A backup was restored including the Calpont database files, the file containing the last txnId, and the 
 *    corresponding Oracle files.
 * 2) The DML/DDL log files include the log entry for the backup and all of the transactions from the backup time
 *    through the stop date and time provided by the user.
 * 3) All of the transaction ids being replayed have higher numbers than the transaction id from the backup.
 *    The procedure for resetting the txnId had not been defined when this utility was written.  We need to take
 *    this utility into account when defining the procedure. 
 * 
 */

class ReplayTxnLog {
	public:
		
		/** @brief Constructor
		 *  user -     		The database user id.  User will be prompted if passed blank.  Program will
		 *			abort if invalid user / password combination.
		 *
		 *  password - 		The database user's password.  User will be prompted if passed blank.
		 *
 		 *  stopDate - 		The stopdate formatted as "mm/dd/yy@hh:mm:ss" or "Now".  User will be 
		 *			prompted if passed blank or the format is incorrect.
		 *
		 *  ignoreBulkLoad -	True if bulk load log entries are to be ignored.  If false, the user
		 *                      will be prompted with each bulk load entry.  Bulk loads are not included
		 *			in the log, so the user would have to know that the bulk load will not
		 *			cause any sql errors because of dependent data if bulk load entries 
		 *			are skipped.  Otherwise, the user would have to re-apply bulk loads 
		 *			each time this utility pauses.
		 */
		ReplayTxnLog(const std::string& user, const std::string& password, const std::string& stopDate, 
			     const bool& ignoreBulkLoad, const bool& reportMode);
		
		/** @brief processes the transaction replay.
		*/
		void process();
	private:
		/* Passed in constructor. */
		std::string fUser;
		std::string fPassword;
		std::string fStopDateStr;
		bool fIgnoreBulkLoad;

		/* Other private members. */
		execplan::CalpontSystemCatalog::SCN fStartVersionID;
		execplan::CalpontSystemCatalog::SCN fDDLCount; 
		execplan::CalpontSystemCatalog::SCN fCommitCount;
		execplan::CalpontSystemCatalog::SCN fRollbackCount;
		time_t fProgramStarted;
		time_t fStopDate; 
		std::string fCurrentYearStr;
		std::string fLastSchemaOwner;
		enum LogEntryType {DML, DDL, ROLLBACK, XCOMMIT, BULKLOAD, 
				   BACKUP_START, BACKUP_END, REPLAY_TXN_START, REPLAY_TXN_END}; // WWW - COMMIT reserved?

		struct LogEntry {
			execplan::CalpontSystemCatalog::SCN fVersionID;
			std::string fSqlStatement;
			time_t fLogTime;
			std::string fSchemaOwner;
			LogEntryType type;
			LogEntry(): fVersionID(0), fSqlStatement(""), fLogTime(0), fSchemaOwner("") {}
		};
		LogEntry fLogEntryFirstCommit;
		LogEntry fLogEntryLastCommit;
		struct ActiveTxn {
			execplan::CalpontSystemCatalog::SCN fVersionID;
			std::vector<LogEntry> fStatements;
		};	
		std::vector <ActiveTxn> fActiveTxns;

		/* Private functions */
		void validateInput();
		std::list<std::string> getLogFileNames();
		void replayTransactions(std::list<std::string>& files);
		void processStatement(const LogEntry& logEntry);
		int split(vector<std::string>& v, const std::string& str, const char& c);
		bool getNextCalpontLogEntry(ifstream& dataFile, LogEntry& logEntry);
		bool displaySummaryAndConfirm(const LogEntry& logEntry);
		void displayProgress(const std::string& fileName);
		void displayFinalSummary();
		time_t convertLogDateStr(const std::string& mth, const std::string& day, const std::string& time);
		void checkDefinitions();

		ReplayTxnLog(const ReplayTxnLog& rhs); 			// no copies
		ReplayTxnLog& operator=(const ReplayTxnLog& rhs); 	// no assignments

};

namespace replaytxnlog
{

/** @brief wraps an Oracle session
  * This class provides an Oracle session interface allowing a session to be started, DML statements
  * to be executed, and closing the session.
  */
class DBSession {

        public:

                /** @brief starts the session.
                */
                void startSession(const std::string& connectAsUser);

                /** @brief issues a DML or DDL statement.
                */
                void issueStatement(const std::string& stmt);

                /** @brief issues a commit.
                */
                void commit();

                /** @brief issues a rollback.
                */
                void rollback();

                /** @brief ends the session.
                */
                void endSession();


                /** @brief returns the connected user or empty string if a session is not active.
                */
                std::string getConnectedUser();

                /** @brief constructor
                */
                DBSession();

        private:

                DBSession(const DBSession& rhs); // private ctor
                DBSession& operator=(const DBSession& rhs);
                std::string fConnectedUser;
};


} //namespace replaytxnlog 

