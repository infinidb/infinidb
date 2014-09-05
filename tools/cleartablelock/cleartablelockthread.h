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
* $Id: cleartablelockthread.h 2101 2013-01-21 14:12:52Z rdempsey $
*
*******************************************************************************/
#ifndef ClearTableLockThread_H__
#define ClearTableLockThread_H__

#include <string>

#include "dbrm.h"
#include "messagequeue.h"

#include "boost/thread/mutex.hpp"

//------------------------------------------------------------------------------
/** @brief Tracks return status for a bulkload rollback request to a PM server
 */
class ClearTableLockStatus
{
public:
	/** @brief ClearTableLockStatus constructor
	 *  @param moduleID PM module ID relevant to this status object
	 */
	explicit ClearTableLockStatus(int moduleID) :
		fModuleID(moduleID), fReturnStatus(0)               { }

	/** @brief Accessor to return status
	 */
	int  retStatus()            const { return fReturnStatus; }

	/** @brief Accessor to return message
	 */
	const std::string& retMsg() const { return fReturnMsg;    }

	/** @brief Accessor to PM module ID
	 */
	int  moduleID()             const { return fModuleID;     }

	/** @brief Mutator used to set the return status
	 *  @param stat Status to be saved
	 */
	void retStatus(int stat)          { fReturnStatus = stat; }

	/** @brief Mutator used to set the return message
	 *  @param msg Status message to be saved
	 */
	void retMsg   (const std::string& msg) { fReturnMsg = msg;}

private:
	int         fModuleID;     // PM module ID associated with this request
	int         fReturnStatus; // Return status from the PM
	std::string fReturnMsg;    // Return message from the PM
};

//------------------------------------------------------------------------------
/** @brief Issues bulkload rollback request to a PM server and gets the response
 */
class ClearTableLockThread
{
public:
	enum CLRTBLLOCK_MSGTYPE {
		CLRTBLLOCK_MSGTYPE_ROLLBACK = 1,
		CLRTBLLOCK_MSGTYPE_CLEANUP  = 2
	};

	/** @brief ClearTableLockThread constructor
	 *  @param brm     Handle to DBRM
	 *  @param clt     MessageQueueClient used to communicate with PM
	 *  @param tInfo   Initial table lock information
	 *  @param tblName Name of table referenced by tInfo
	 *  @param msgType Message to process
	 *  @param pStatus Status object used to track this bulkload rollback req
	 */
	ClearTableLockThread(
		BRM::DBRM*                brm,
		messageqcpp::MessageQueueClient* clt,
		const BRM::TableLockInfo& tInfo,
		const std::string&        tblName,
		CLRTBLLOCK_MSGTYPE        msgType,
		ClearTableLockStatus*     pStatus);

	/** @brief Entry point for thread execution
	 */
	void operator() ();

private:
	void executeRollback   ( );
	void executeFileCleanup( );
	void setStatus(int status, const std::string& msg)
	{
		fStatus->retStatus( status );
		fStatus->retMsg   ( msg    );
	}

	BRM::TableLockInfo    fTableLockInfo; // Initial table lock information
	BRM::DBRM*            fBrm;           // Handle to DBRM
	messageqcpp::MessageQueueClient* fClt;// Msg queue client to send/rcv msgs
	std::string           fTblName;       // Name of relevant table
	CLRTBLLOCK_MSGTYPE    fMsgType;       // Msg type to process
	ClearTableLockStatus* fStatus;        // Status object used to track request
	static boost::mutex   fStdOutLock;    // Synchronize logging to stdout
};

#endif
