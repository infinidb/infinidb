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

/*****************************************************************************
 * $Id: cleartablelockthread.cpp 2101 2013-01-21 14:12:52Z rdempsey $
 *
 ****************************************************************************/
#include "cleartablelockthread.h"

#include "bytestream.h"
#include "we_messages.h"

#include <stdexcept>
#include <sstream>

/*static*/ boost::mutex ClearTableLockThread::fStdOutLock;

//------------------------------------------------------------------------------
// ClearTableLockThread constructor
//------------------------------------------------------------------------------
ClearTableLockThread::ClearTableLockThread(
	BRM::DBRM*                brm,
	messageqcpp::MessageQueueClient* clt,
	const BRM::TableLockInfo& tInfo,
	const std::string&        tblName,
	CLRTBLLOCK_MSGTYPE        msgType, 
	ClearTableLockStatus*     pStatus) :
		fTableLockInfo(tInfo),
		fBrm(brm),
		fClt(clt),
		fTblName(tblName),
		fMsgType(msgType),    
		fStatus(pStatus)
{
}

//------------------------------------------------------------------------------
// Main entry point for a ClearTableLockThread object, used to forward a
// cleartablelock tool command to the WriteEngineServer specified to the ctor.
//------------------------------------------------------------------------------
void ClearTableLockThread::operator() ()
{
	try {
		if (fMsgType == CLRTBLLOCK_MSGTYPE_ROLLBACK)
			executeRollback( );
		else if (fMsgType == CLRTBLLOCK_MSGTYPE_CLEANUP)
			executeFileCleanup( );
	}
	catch (std::exception& ex)
	{
		setStatus( 101, ex.what() );
	}
	catch (...)
	{
		std::string errMsg("Unknown exception.");
		setStatus( 102, errMsg );
	}
}

//------------------------------------------------------------------------------
// Process bulk rollback portion of cleartablelock command.
//------------------------------------------------------------------------------
void ClearTableLockThread::executeRollback( )
{
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	messageqcpp::ByteStream bsOut;

	const std::string APPLNAME("cleartablelock command");

	//--------------------------------------------------------------------------
	// Send rollback msg to the writeengine server connected to fClt.
	//--------------------------------------------------------------------------
	{
		boost::mutex::scoped_lock lk(fStdOutLock);
		std::cout << "Sending rollback request to PM" <<
			fStatus->moduleID() << "..." << std::endl;
//		std::cout << "cleartablelock rollback: tableLock-" <<fTableLockInfo.id<<
//			": oid-"  << fTableLockInfo.tableOID <<
//			"; name-" << fTblName                <<
//			"; app-"  << APPLNAME                << std::endl;
	}

	bsOut << (messageqcpp::ByteStream::byte)
		WriteEngine::WE_CLT_SRV_CLEAR_TABLE_LOCK;
	bsOut << fTableLockInfo.id;
	bsOut << fTableLockInfo.tableOID;
	bsOut << fTblName;
	bsOut << APPLNAME;
	fClt->write(bsOut);

	// Wait for the response, and check for any errors
	std::string rollbackErrMsg;
	bsIn.reset(new messageqcpp::ByteStream());
	bsIn = fClt->read();
	if (bsIn->length() == 0)
	{
		std::string errMsg("Network error, PM rollback");
		setStatus( 103, errMsg );

		boost::mutex::scoped_lock lk(fStdOutLock);
		std::cout << "No response from PM" << fStatus->moduleID() << std::endl;
		return;
	}
	else
	{
		messageqcpp::ByteStream::byte rc;
		*bsIn >> rc;
		*bsIn >> rollbackErrMsg;

		{
			boost::mutex::scoped_lock lk(fStdOutLock);
			if (rc == 0)
				std::cout << "Successful rollback response from PM" <<
					fStatus->moduleID() << std::endl;
			else
				std::cout << "Unsuccessful rollback response from PM" <<
					fStatus->moduleID() << "; " << rollbackErrMsg << std::endl;
//			std::cout << "cleartablelock rollback response; rc-" << (int)rc <<
//				"; retMsg: <" << rollbackErrMsg << '>' << std::endl;
		}

		if (rc != 0)
		{
			std::string errMsg("PM rollback error: ");
			errMsg +=  rollbackErrMsg;
			setStatus( 104, errMsg );
			return;
		}

	}
}

//------------------------------------------------------------------------------
// Process file cleanup portion of cleartablelock command.
//------------------------------------------------------------------------------
void ClearTableLockThread::executeFileCleanup( )
{
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	messageqcpp::ByteStream bsOut;

	//--------------------------------------------------------------------------
	// Send cleanup msg (to delete rb files) to the we server connected to fClt.
	//--------------------------------------------------------------------------
	{
		boost::mutex::scoped_lock lk(fStdOutLock);
		std::cout << "Sending cleanup request to PM" <<
			fStatus->moduleID() << "..." << std::endl;
//		std::cout  << "cleartablelock cleanup: " <<
//			"oid-" << fTableLockInfo.tableOID    << std::endl;
	}

	bsOut << (messageqcpp::ByteStream::byte)
		WriteEngine::WE_CLT_SRV_CLEAR_TABLE_LOCK_CLEANUP;
	bsOut << fTableLockInfo.tableOID;
	fClt->write(bsOut);

	// Wait for the response, and check for any errors
	std::string fileDeleteErrMsg;
	bsIn.reset(new messageqcpp::ByteStream());
	bsIn = fClt->read();
	if (bsIn->length() == 0)
	{
		std::string errMsg("Network error; PM rollback cleanup");
		setStatus( 105, errMsg );

		boost::mutex::scoped_lock lk(fStdOutLock);
		std::cout << "No response from PM" << fStatus->moduleID() << std::endl;
		return;
	}
	else
	{
		messageqcpp::ByteStream::byte rc;
		*bsIn >> rc;
		*bsIn >> fileDeleteErrMsg;

		{
			boost::mutex::scoped_lock lk(fStdOutLock);
			if (rc == 0)
				std::cout << "Successful cleanup response from PM" <<
					fStatus->moduleID() << std::endl;
			else
				std::cout << "Unsuccessful cleanup response from PM" <<
					fStatus->moduleID() << "; " << fileDeleteErrMsg <<std::endl;
//			std::cout << "cleartablelock cleanup response; rc-" << (int)rc <<
//				"; retMsg: <" << fileDeleteErrMsg << '>' << std::endl;
		}

		if (rc != 0)
		{
			std::string errMsg("PM rollback cleanup error: ");
			errMsg +=  fileDeleteErrMsg;
			setStatus( 106, errMsg );
			return;
		}
	}
}
