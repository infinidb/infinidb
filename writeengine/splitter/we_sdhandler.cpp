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

/*
 * we_sdhandler.cpp
 *
 *  Created on: Oct 17, 2011
 *      Author: bpaul
 */

#include <ctime>
#include <fstream>
#include <istream>
using namespace std;

#include "we_messages.h"

#include <sys/time.h>

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;

//-----

#include "snmpmanager.h"
using namespace snmpmanager;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "batchloader.h"
using namespace batchloader;

//-----

#include "we_sdhandler.h"

#include "we_splitterapp.h"
#include "we_respreadthread.h"
#include "we_filereadthread.h"
#include "we_brmupdater.h"
#include "we_tablelockgrabber.h"
#include "we_simplesyslog.h"

namespace WriteEngine
{
//------------------------------------------------------------------------------
//
/** @brief Add PM to the list
 *  @param	- PmId
 */

void WEPmList::addPm2List(int PmId) {
	mutex::scoped_lock aLock(fListMutex);
	fPmList.push_back(PmId);
	aLock.unlock();
}

//------------------------------------------------------------------------------

void WEPmList::addPriorityPm2List(int PmId) {
	mutex::scoped_lock aLock(fListMutex);
	fPmList.push_front(PmId);
	aLock.unlock();
}
//------------------------------------------------------------------------------

int WEPmList::getNextPm() {
	mutex::scoped_lock aLock(fListMutex);
	int aPmId = 0;
	if (!fPmList.empty()) {
		aPmId = fPmList.front();
		fPmList.pop_front();
	}
	return aPmId;
}
//------------------------------------------------------------------------------

// incase a shutdown or roll back is called we need to clear the data
// so that the sendingthreads will not keep sending data.
void WEPmList::clearPmList() {
	mutex::scoped_lock aLock(fListMutex);
	if (!fPmList.empty())
		fPmList.clear();
	aLock.unlock();
}

//------------------------------------------------------------------------------

bool WEPmList::check4Pm(int PmId) {
	mutex::scoped_lock aLock(fListMutex);
	WePmList::iterator aIt = fPmList.begin();
	bool aFound = false;
	while (aIt != fPmList.end()) {
		if ((*aIt) == PmId) {
			aFound = true;
			fPmList.erase(aIt);
			break;
		}
		++aIt;
	}
	return aFound;
}

//------------------------------------------------------------------------------
//	WESDHandler Definitions
//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

WESDHandler::WESDHandler(WESplitterApp& Ref) :
		fRef(Ref),
		fLog(),
		fQId(101), // 101 - took it from air
		fRm(),
		fOam(),
		fModuleTypeConfig(),
		fDebugLvl(0),
		fPmCount(0),
		fTableLock(0),
		fTableOId(0),
		fFixedBinaryRecLen(0),
		fRespMutex(),
		fRespCond(),
		fSendMutex(),
		fRespList(),
		fpRespThread(0),
		fDataFeedList(),
		fFileReadThread(*this),
		fDisconnectFailure(false),
		fForcedFailure(false),
		fAllCpiStarted(false),
		fFirstDataSent(false),
		fFirstPmToSend(0),
		fSelectOtherPm(false),
		fContinue(true),
		fWeSplClients(MAX_PMS),
		fBrmRptVec(),
		fpBatchLoader(0)
{
}
//------------------------------------------------------------------------------

WESDHandler::~WESDHandler()
{
	try
	{
		for (int aCnt = 1; aCnt <= fPmCount; aCnt++)
		{
			if (fWeSplClients[aCnt] != 0)
			{
				delete fWeSplClients[aCnt];
				fWeSplClients[aCnt] = 0;
			}
		}

		delete fpRespThread;
		fpRespThread = 0;
		delete fpBatchLoader;
		fpBatchLoader = 0;
	}
	catch (...)
	{
		std::string aStr = "Handled an error in ~WESDHandler";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(aStr);
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR,
				logging::M0000);
		cout << aStr << endl;
	}

}

//------------------------------------------------------------------------------

void WESDHandler::reset()
{
	fTableLock = 0;
	fTableOId = 0;
	fFixedBinaryRecLen = 0;
	//fpRespThread = 0;
	//fFileReadThread(*this);
	fForcedFailure = false;
	fAllCpiStarted = false;
	fFirstDataSent = false;
	fFirstPmToSend = 0;
	fSelectOtherPm = false;
	fContinue = true;
	//fWeSplClients();
	//fpBatchLoader = 0;
	fImportRslt.reset();
	fLog.closeLog();

	try
	{
		for (int aCnt = 1; aCnt <= fPmCount; aCnt++)
		{
			if (fWeSplClients[aCnt] != 0)
			{
				delete fWeSplClients[aCnt];
				fWeSplClients[aCnt] = 0;
			}
		}

		delete fpRespThread;
		fpRespThread = 0;
		delete fpBatchLoader;
		fpBatchLoader = 0;
	}
	catch (...)
	{
		std::string aStr = "Handled an error in ~WESDHandler";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(aStr);
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR,
				logging::M0000);
		cout << aStr << endl;
	}


}
//------------------------------------------------------------------------------
//BP 10/24/2011 14:22
//------------------------------------------------------------------------------
void WESDHandler::send2Pm(ByteStream& Bs, unsigned int PmId) {
	//mutex::scoped_lock aLock(fSendMutex);

	if (PmId == 0) // send it to everyone
			{
		for (int aIdx = 1; aIdx <= getPmCount(); ++aIdx) {
			if (fWeSplClients[aIdx] != 0) {
				mutex::scoped_lock aLock(fWeSplClients[aIdx]->fWriteMutex);
				fWeSplClients[aIdx]->write(Bs);
				aLock.unlock();
			}
		}
	} else {
		mutex::scoped_lock aLock(fWeSplClients[PmId]->fWriteMutex);
		fWeSplClients[PmId]->write(Bs);
		aLock.unlock();
	}

	//aLock.unlock();
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void WESDHandler::send2Pm(messageqcpp::SBS& Sbs, unsigned int PmId) {

	//mutex::scoped_lock aLock(fSendMutex);

	if (PmId == 0) // send it to everyone
			{
		for (int aIdx = 1; aIdx <= getPmCount(); ++aIdx) {
			if (fWeSplClients[aIdx] != 0) {
				mutex::scoped_lock aLock(fWeSplClients[aIdx]->fSentQMutex);
				fWeSplClients[aIdx]->add2SendQueue(Sbs);
				aLock.unlock();

			}
		}
	} else {
		mutex::scoped_lock aLock(fWeSplClients[PmId]->fSentQMutex);
		fWeSplClients[PmId]->add2SendQueue(Sbs);
		aLock.unlock();
	}

	//aLock.unlock();

}

//------------------------------------------------------------------------------
//

void WESDHandler::sendEODMsg()
{
	// BUG 5035 Sending multiple EOD so that to avoid 'silly window syndrome'
	for(int idx=0; idx<3; idx++)
	{
		messageqcpp::SBS aSbs(new messageqcpp::ByteStream);
		*aSbs << (ByteStream::byte) WE_CLT_SRV_EOD;
		send2Pm(aSbs);
	}

	{
		std::stringstream aStrStr;
		aStrStr << "Send EOD message to All PMs";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(aStrStr.str());
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
	}
}

//------------------------------------------------------------------------------
//  check for the messages from all WE servers
//------------------------------------------------------------------------------
void WESDHandler::checkForRespMsgs() {
	ByteStream aBs;
	ByteStream::byte aPmId;
	ByteStream::byte aMsgId;
	//boost::shared_ptr<messageqcpp::ByteStream> aSbs;
	messageqcpp::SBS aSbs;
	while (isContinue())
	{
		mutex::scoped_lock aLock(fRespMutex);

		//NOTE - if isContinue is not checked thread will hang on shutdown
		// 		by locking again on fRespList.empty()
		while ((fRespList.empty()) && (isContinue()))
			fRespCond.wait(aLock);

		//if(!isContinue()) {	aLock.unlock();	break;	} testing for rare hanging

		//cout <<"wait signaled checkForRespMsgs" << endl;
		while (!fRespList.empty()) {
			//mutex::scoped_lock aLock (fRespMutex);
			aSbs = fRespList.front();
			fRespList.pop_front();
			//aLock.unlock();
			*aSbs >> aMsgId;
			*aSbs >> aPmId;
			//Debugging
			//cout << "aMsgid = " << static_cast<short>(aMsgId) << endl;
			switch (aMsgId) {
			case WE_CLT_SRV_KEEPALIVE:
				onKeepAliveMessage(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_ACK:
				onAckResponse(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_DATARQST:
				onDataRqstResponse(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_EOD:
				onEodResponse(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_STARTCPI:
				onStartCpiResponse(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_CPIPASS:
				onCpimportPass(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_CPIFAIL:
				onCpimportFail(static_cast<int>(aPmId));
				break;
			case WE_CLT_SRV_BRMRPT:
				onBrmReport(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_ROLLBACK:
				onRollbackResult(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_CLEANUP:
				onCleanupResult(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_DBRCNT:
				onDBRootCount(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_ERRLOG:
				onErrorFile(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_BADLOG:
				onBadFile(static_cast<int>(aPmId), aSbs);
				break;
			case WE_CLT_SRV_IMPFILEERROR:
				onImpFileError(static_cast<int>(aPmId));
				break;
			default:
				break;
			} // switch
			aSbs.reset();
		} // while not empty()

		aLock.unlock();
		// yield here so that other threads get slice
	} //while

}
//------------------------------------------------------------------------------

void WESDHandler::add2RespQueue(const messageqcpp::SBS& Sbs) {
	mutex::scoped_lock aLock(fRespMutex);
	fRespList.push_back(Sbs);
	aLock.unlock();
	//cout <<"Notifing from add2RespQueue" << endl;
	fRespCond.notify_one();
}

//------------------------------------------------------------------------------
void WESDHandler::setup()
{
	std::stringstream aPid;
	bool bRollback;
	bool bForce;
	aPid << getpid();
	std::string aTimeStamp = getTime2Str();
	std::string aLogName;
	std::string aErrLogName;
	if(fRef.fCmdArgs.isJobLogOnly())
        aLogName = fRef.fCmdArgs.getBulkRootDir()+"/log/"+
							"cpimport_Job_"+fRef.fCmdArgs.getJobId()+".log";
	else
        aLogName = fRef.fCmdArgs.getBulkRootDir()+"/log/"+
							"cpimport_"+aTimeStamp+"_"+aPid.str()+".log";


	if(getDebugLvl()>1) cout <<"LogName : " << aLogName << endl;

	if(fRef.fCmdArgs.isJobLogOnly())
        aErrLogName = fRef.fCmdArgs.getBulkRootDir()+"/log/"+
							"cpimport_Job_"+fRef.fCmdArgs.getJobId()+".err";
    else
        aErrLogName = fRef.fCmdArgs.getBulkRootDir()+"/log/"+
							"cpimport_"+aTimeStamp+"_"+aPid.str()+".err";


	if(getDebugLvl()>1) cout <<"ErrLogName : " << aErrLogName << endl;
	// consoleFlag false will only output only MSGLOG_LVL1 to console
	// and MSGLOG_LVL2 to log file without writing to console.
	//fLog.setLogFileName(aLogName.c_str(), aErrLogName.c_str(), false);
	fLog.setLogFileName(aLogName.c_str(), aErrLogName.c_str(), getConsoleLog());

	// In mode 0 and Mode 1, we need to check for local file availability
	if ((0==fRef.fCmdArgs.getMode()) || (1==fRef.fCmdArgs.getMode()))
	{
		if(!check4InputFile(fRef.getLocFile()))
		{
			throw(runtime_error("Could not open Input file "+ fRef.getLocFile()));
		}
	}

	fImportRslt.startTimer();

	if (fPmCount == 0) // Should have already set in cpimport invoke check
	{
		throw(runtime_error("Configuration Error. PM Count = 0"));
	}

	if (fRef.fCmdArgs.getPmVecSize() == 0) //No Pms listed in Cmd line
	{
		//BUG 4668 - Added this code to find the PM's realtime from OAM
		//fOam.getSystemConfig("pm", fModuleTypeConfig); //commented out since-
		// - we are calling that function in check4CpiInvokeMode()
		oam::DeviceNetworkList::iterator pt =
				fModuleTypeConfig.ModuleNetworkList.begin();
		for (; pt != fModuleTypeConfig.ModuleNetworkList.end(); pt++)
		{
			int moduleID = atoi(
					(*pt).DeviceName.substr(oam::MAX_MODULE_TYPE_SIZE,
							oam::MAX_MODULE_ID_SIZE).c_str());
			if (getDebugLvl() > 1)
				cout << "Adding PmId - " << moduleID << endl;
			fRef.fCmdArgs.add2PmVec(moduleID);
		}
		/*
		 for (int PmId = 1; ((PmId <= fPmCount) && (PmId < MAX_PMS)); ++PmId)
		 {
		 if(getDebugLvl()>1) cout<<"Adding PmId - "<<PmId <<endl;
		 fRef.fCmdArgs.add2PmVec(PmId);
		 }
		 */
	}

// getModuleStatus will take too long. Also to test in development
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
	{
		vector<unsigned int>& aVec = fRef.fCmdArgs.getPmVec();
		for (unsigned int PmId = 1; (PmId <= static_cast<unsigned int>(fPmCount)); ++PmId)
		{
			int opState = 0;
			bool aDegraded = false;
			ostringstream aOss;
			aOss << "pm" << PmId;
			std::string aModName = aOss.str();
			if(getDebugLvl())
				cout << "getModuleStatus() ModName = " << aModName << endl;
			try
			{
				fOam.getModuleStatus(aModName, opState, aDegraded);
				if (getDebugLvl())
					cout << "ModName = " << aModName <<" opState = "<< opState << endl;
			}
			catch(std::exception& ex)
			{
				ostringstream oss;
				oss << "Exception on getModuleStatus on module ";
				oss <<	aModName;
				oss <<  ":  ";
				oss <<  ex.what();
				//fLog.logMsg( oss.str(), MSGLVL_ERROR );
				setContinue(false);
				throw runtime_error( oss.str() );
			}

			if(opState != oam::ACTIVE )// BUG 4668
			{
				aVec.erase(std::remove(aVec.begin(),aVec.end(),PmId),aVec.end());
			}
		}
	}
#endif


	int rtn = fDbrm.getSystemReady();
	if (rtn < 1)
	{
		ostringstream oss;
		oss << "System is not ready (" << rtn << ").  Verify that InfiniDB is up and ready ";
		//fLog.logMsg( oss.str(), MSGLVL_ERROR );
		setContinue(false);
		throw runtime_error( oss.str() );
	}
	else if (BRM::ERR_OK != fDbrm.isReadWrite())
	{
		ostringstream oss;
		oss << "Error: System in ReadOnly state.";
		//fLog.logMsg(oss.str(), MSGLVL_ERROR);
		setContinue(false);
		throw runtime_error(oss.str());
	}
	else if (fDbrm.getSystemShutdownPending(bRollback, bForce) > 0)
	{
		ostringstream oss;
		oss << "System is being shutdown. Can't start a new import";
		setContinue(false);
		throw runtime_error(oss.str());
	}
	else if (fDbrm.getSystemSuspendPending(bRollback) > 0 || fDbrm.getSystemSuspended())
	{
		ostringstream oss;
		oss << "System is in write disabled state. Can't start a new import";
		setContinue(false);
		throw runtime_error(oss.str());
	}

	if ((fRef.fCmdArgs.getMode() == 1) || (fRef.fCmdArgs.getMode() == 2))
	{
		fTableOId = 0;
		fFixedBinaryRecLen = 0;
		try
		{
			int32_t tblOid = getTableOID(fRef.fCmdArgs.getSchemaName(),
					fRef.fCmdArgs.getTableName());
			fTableOId = tblOid;
			if (getDebugLvl())
				cout << "Table OID = " << fTableOId << endl;

			if (fRef.fCmdArgs.getImportDataMode() != IMPORT_DATA_TEXT)
			{
				fFixedBinaryRecLen = calcTableRecLen(
					fRef.fCmdArgs.getSchemaName(),
					fRef.fCmdArgs.getTableName());
			}
		}
		catch (std::exception& ex)
		{
			std::string aDetails = fRef.fCmdArgs.getSchemaName() + "."
					+ fRef.fCmdArgs.getTableName() + " ERROR : ";
			std::string aStr = aDetails + ex.what();
			logging::Message::Args errMsgArgs;
			//BUG 4152
			errMsgArgs.add(fRef.fCmdArgs.getSchemaName());
			errMsgArgs.add(fRef.fCmdArgs.getTableName());
			errMsgArgs.add(ex.what());
			fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0097);
			fLog.logMsg( aStr, MSGLVL_ERROR );
			setContinue(false);
			if (fTableOId == 0) // error getting table OID
				throw runtime_error(
					"Please make sure both schema and table exists!!");
			else                // error getting fixed binary record length
				throw runtime_error( ex.what() );
		}

		int aWaitIntvl = 10;	// In seconds
		try
		{
			string aWaitPeriod = config::Config::makeConfig()->getConfig(
														"SystemConfig",
														"WaitPeriod");
			if(!aWaitPeriod.empty()) aWaitIntvl = atoi(aWaitPeriod.c_str());
			if(getDebugLvl()) cout<<"aWaitPeriod = "<<aWaitPeriod<< endl;
		}
		catch(std::exception&)
		{
			aWaitIntvl = 10;
		}

		std::vector<unsigned int> aPmVec = fRef.fCmdArgs.getPmVec();
		WETableLockGrabber aTLG(*this);
		string errMsg;
		for (int aIdx = 0; aIdx < (aWaitIntvl*10); aIdx++)
		{
			try
			{
				fTableLock = aTLG.grabTableLock(aPmVec, fTableOId);
				if (getDebugLvl() > 1)
					cout << "Table Lock = " << fTableLock << endl;
			}
			catch (std::exception& ex)
			{
				errMsg = ex.what();
			}
			if (fTableLock != 0) break;
			usleep(100000);
		}

		if (fTableLock == 0)
		{
	        ostringstream oss;
	        oss << "Failed to acquire Table Lock of ";
	        oss << fRef.fCmdArgs.getSchemaName() << ".";
			oss << fRef.fCmdArgs.getTableName()<< "; " << errMsg;
	        //fLog.logMsg( oss.str(), MSGLVL_ERROR );
			setContinue(false);
			throw runtime_error(oss.str());
		}

		if(0 != fTableOId)
		{
			try
			{
				if(getDebugLvl()>1)
				{
					for(unsigned int idx=0;idx<aPmVec.size();idx++)
						cout <<"PmId = "<< aPmVec[idx] << std::endl;
				}
				fpBatchLoader = new BatchLoader(fTableOId, 0, aPmVec);
				//int aRet=fpBatchLoader->selectFirstPM(fFirstPmToSend, fSelectOtherPm);
				//if (aRet != 0) throw runtime_error("BatchLoader error.. exiting");
			}
			catch(std::exception& ex)
			{
				releaseTableLocks();
		        ostringstream oss;
		        oss << ex.what() << " ... import exiting";
		        //fLog.logMsg( oss.str(), MSGLVL_ERROR );
				setContinue(false);
				throw runtime_error(oss.str());
			}
		}
	}

	for (int PmId = 1; ((PmId <= fPmCount) && (PmId < MAX_PMS)); ++PmId)
	{
		try
		{
			if (fRef.getPmStatus(PmId))
			{
				fWeSplClients.at(PmId) = new WESplClient(*this, PmId);
				if (fWeSplClients[PmId] != NULL)
				{
					fWeSplClients[PmId]->setup();
					if (2 == fRef.fCmdArgs.getMode())
						fWeSplClients[PmId]->setRdSecTo(1); //Set Rd T/O to 1 sec
				}
				else
				{
					std::string aStr;
					aStr = "Encountered NULL WESplClient : " + PmId;
					cout << aStr << endl;
					fLog.logMsg( aStr, MSGLVL_ERROR );
					throw WESdHandlerException(aStr);
				}
			}

		}
		catch (const std::exception& ex)
		{
			std::string aStr = ex.what();
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(aStr);
			fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
			releaseTableLocks();	//BUG 4295 - release table lock as connection fails
			//cout << aStr << endl;
			fLog.logMsg( aStr, MSGLVL_ERROR );
			throw runtime_error("Error in connection setup.");
		}
	}

	// to initiate adding data to the SendQueue, that many BS
	// BUG 5031 - Initial Data request count set to 100. This will be the max
	// Q size on the WES side. Here after a batch send for every data rqst
	for (int aIdx = 0; aIdx < MAX_WES_QSIZE; aIdx++) {
		for (int PmId = 1; PmId <= fPmCount; PmId++) {
			if (fWeSplClients[PmId] != 0)
				fWeSplClients[PmId]->incDataRqstCount();
			//fDataFeedList.addPm2List(PmId);
		}
	}

	fpRespThread = new boost::thread(WERespReadThread(*this));

	try
	{
		// start the File Read thread
		if ((fRef.fCmdArgs.getMode() == 0) || (fRef.fCmdArgs.getMode() == 1))
		{
			if(fRef.getLocFile() == "STDIN")
			{
				ostringstream oss;
				oss << "Reading input from STDIN to import into table ";
				oss << fRef.fCmdArgs.getSchemaName() << ".";
				oss << fRef.fCmdArgs.getTableName() << "...";
				
				if ( fRef.fCmdArgs.getConsoleOutput())
					fLog.logMsg( oss.str(), MSGLVL_INFO1 );
			}
			if(getDebugLvl())
				cout << "BatchQuantity="<<fRef.fCmdArgs.getBatchQuantity()<<endl;
			fFileReadThread.setBatchQty(fRef.fCmdArgs.getBatchQuantity());
			fFileReadThread.setup(fRef.getLocFile());
		}
	}
	catch(std::exception& ex)
	{
		releaseTableLocks();	//BUG 4295
		throw runtime_error(ex.what());
	}

	//Output "Running distributed import (Mode{x}) on [all/<pmlist>] PMs"
	{
		ostringstream oss;
		oss << "Running distributed import (mode ";
		oss << fRef.fCmdArgs.getMode() <<") on ";
		if(fRef.fCmdArgs.getPmVecSize() == fPmCount)
			oss << "all PMs...";
		else
		{
			oss << " PM ";
			std::vector<unsigned int> aPmVec = fRef.fCmdArgs.getPmVec();
			unsigned int aIdx = 0;
			while(aIdx < aPmVec.size())
			{
				oss << aPmVec[aIdx];
				aIdx++;
				if(aIdx != aPmVec.size()) oss <<",";
			}
			oss << " ...";
		}
		if ( fRef.fCmdArgs.getConsoleOutput())
			fLog.logMsg( oss.str(), MSGLVL_INFO1 );
	}



}

//-----------------------------------------------------------------------------
bool WESDHandler::updateCPAndHWMInBRM() {
	if (getDebugLvl())
		cout << "Inside updateCPAndHWMInBRM()" << endl;
	WEBrmUpdater aBrmUpdater(*this);
	bool aRslt = aBrmUpdater.updateCasualPartitionAndHighWaterMarkInBRM();
	return aRslt;
}

//-----------------------------------------------------------------------------
void WESDHandler::cancelOutstandingCpimports()
{
	std::string aStr = "Canceling outstanding cpimports";
	if(getDebugLvl()) cout << aStr << endl;
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStr, MSGLVL_INFO1 );

	fFileReadThread.shutdown();
	bool aSetFail = false;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			if ((!fWeSplClients[aCnt]->isCpiFailed())
					&& (!fWeSplClients[aCnt]->isCpiPassed()))
			{
				if(getDebugLvl())
					cout << "Canceling Cpimport in " << aCnt << endl;
				// clear the sendQ
				fWeSplClients[aCnt]->clearSendQueue();
				messageqcpp::ByteStream aBs;
				aBs << (ByteStream::byte) WE_CLT_SRV_EOD;
				if (fWeSplClients[aCnt]->isConnected()) {
					mutex::scoped_lock aLock(fWeSplClients[aCnt]->fWriteMutex);
					fWeSplClients[aCnt]->write(aBs);
					aLock.unlock();
				} else {
					fWeSplClients[aCnt]->setCpiFailed(true); //Setting as FAILED
				}
			}
			// if it is passed already set it as canceled so that we can rollback
			else if (fWeSplClients[aCnt]->isCpiPassed()) {
				fWeSplClients[aCnt]->setCpiPassed(false);
				fWeSplClients[aCnt]->setCpiFailed(true); //Setting as FAILED
				aSetFail = true;
			}
		}
	}

	// setting Manual Failed caused a Rollback. Warrented if it the last PM
	if (aSetFail) {
		if (checkAllCpiFailStatus())
			doRollback();
	}

	if (getDebugLvl())
		cout << "Canceled all outstanding cpimports!!" << endl;
}

//-----------------------------------------------------------------------------

bool WESDHandler::checkForRollbackAndCleanup() {
	bool aRetStatus = true;

	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			// If Anyone of the client is
			if ((!fWeSplClients[aCnt]->isCpiFailed()) && //NOT Failed
					(!fWeSplClients[aCnt]->isCpiPassed()) && //NOT Passed
					(fWeSplClients[aCnt]->isConnected())) //NOT Disconnected
					{
				aRetStatus = false; // then its not time to Rollback/Cleanup
				break;
			}
		}
	}

	return aRetStatus;
}

//-----------------------------------------------------------------------------

bool WESDHandler::checkForCpiFailStatus() {
	bool aRetStatus = false;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			// If Anyone of the client is
			if (fWeSplClients[aCnt]->isCpiFailed()) // Failed
			{
				aRetStatus = true; // encounterd a Failure
				break;
			}
		}
	}
	//if no CPI failed but a Forced Failure simulated, return Status as true
	if((!aRetStatus)&&(fForcedFailure)) aRetStatus = true;
	return aRetStatus;
}

//-----------------------------------------------------------------------------

void WESDHandler::checkForConnections()
{
	time_t aNow = time(0);
	for (int PmId = 1; PmId <= fPmCount; ++PmId)
	{
		if (fWeSplClients[PmId] != 0)
		{
			if (aNow - fWeSplClients[PmId]->getLastInTime() > 180)
			{
				std::string aStr;
				aStr = "Heartbeats missed - Non Responsive PM" + PmId;
				fLog.logMsg( aStr, MSGLVL_ERROR );
				fWeSplClients[PmId]->onDisconnect();
				exit(1); //Otherwise; have to wait till write() comes out
			}
		}
	}
}

//-----------------------------------------------------------------------------

void WESDHandler::sendHeartbeats()
{
    messageqcpp::SBS aSbs(new messageqcpp::ByteStream);
    *aSbs << (ByteStream::byte) WE_CLT_SRV_KEEPALIVE;
    send2Pm(aSbs);
}

//-----------------------------------------------------------------------------

void WESDHandler::shutdown()
{
	if(fDisconnectFailure)
	{
		onDisconnectFailure();
	}

	if(fRef.fSigHup)
	{
		onHandlingSigHup();
	}
	if(fRef.fSignaled)
	{
		onHandlingSignal();
	}


	fDataFeedList.clearPmList();

	fFileReadThread.shutdown();
	if (fFileReadThread.getFpThread())
		fFileReadThread.getFpThread()->join();

	if (getDebugLvl()) cout << "cleaning up Client threads " << endl;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++)
	{
		if (fWeSplClients[aCnt] != 0)
		{
			fWeSplClients[aCnt]->setContinue(false);
			fWeSplClients[aCnt]->setConnected(false);
			if(fWeSplClients[aCnt]->getFpThread() != NULL)
				fWeSplClients[aCnt]->getFpThread()->join();
			if(getDebugLvl()) fWeSplClients[aCnt]->printStats();
		}
	}


	mutex::scoped_lock aLock(fRespMutex);
	this->setContinue(false);
	usleep(100000);		// so that response thread get updated.
	fRespCond.notify_all();
	aLock.unlock();

	if(fpRespThread) fpRespThread->join();

	fLog.logMsg("Shutdown of all child threads Finished!!", MSGLVL_INFO2);
}

//------------------------------------------------------------------------------

void WESDHandler::onStartCpiResponse(int PmId)
{
	if (getDebugLvl())
		cout << "On Start CPI response arrived " << PmId << endl;
	messageqcpp::ByteStream aBs;
	aBs << (ByteStream::byte) WE_CLT_SRV_STARTCPI;
	send2Pm(aBs, PmId);
	fWeSplClients[PmId]->setCpiStarted(true);
}

//------------------------------------------------------------------------------
void WESDHandler::onDataRqstResponse(int PmId) {
	// may be we should add this pmid to the queue
	//cout << "Received an DataRqst from "<< PmId << endl; - for debug
	//BUG 5031 - Don't allow to accumulate RqstCnt uncontrollably
	// since that will end up sending too many messages to WES.
	// - Walt don't want this checking since we are going FIXED Q size
	int aCnt = fWeSplClients[PmId]->getDataRqstCount();
	if(aCnt < MAX_WES_QSIZE) // 100
	{
		if (getDebugLvl()>2)
			cout<<"DataReqst ["<<PmId << "] = "<<
				fWeSplClients[PmId]->getDataRqstCount()<< endl;
		fWeSplClients[PmId]->incDataRqstCount();
	}

	//fDataFeedList.addPm2List(PmId);
}

//------------------------------------------------------------------------------

void WESDHandler::onAckResponse(int PmId) {
	// may be we should add this pmid to the queue
	if (getDebugLvl())
		cout << "Received an ACK from " << PmId << endl;
	//fDataFeedList.addPm2List(PmId);
}
//------------------------------------------------------------------------------
void WESDHandler::onNakResponse(int PmId) {
	if (getDebugLvl())
		cout << "Received a NAK from " << PmId << endl;
}
//------------------------------------------------------------------------------
// Eod response means we are do not send anymore data to this PM
// Increase the read timeout for this PM thread so that it won't
// consume too much CPU
void WESDHandler::onEodResponse(int PmId) {
	if (getDebugLvl())
		cout << "Received a EOD from " << PmId << endl;
	fWeSplClients[PmId]->setRdSecTo(1); //Set Rd T/O to 1 sec

	if (fRef.fCmdArgs.getMode() == 0)
	{
		// This is when one PM fail on Mode 0
		if (checkForCpiFailStatus()) // someone else failed,
		{ // so set this as failed for rollback.
			if (getDebugLvl())
				cout << "Setting CPI failed on "<< PmId << endl;
			fWeSplClients[PmId]->setCpiPassed(false);
			fWeSplClients[PmId]->setCpiFailed(true);
			if (getDebugLvl())
				cout << "Calling onSigInterrupt from onEodResponse() "<< endl;
			if (checkAllCpiFailStatus()) fRef.onSigInterrupt(1);
		}
		else
		{
			if (getDebugLvl())
				cout << "Calling onCpimportPass() from onEodResponse() "<< endl;
			onCpimportPass(PmId); // set dummy Cpimport Pass
			if (checkAllCpiPassStatus()) // Mode 0 won't don't have BRM report
			{
				fImportRslt.stopTimer();
			    ostringstream oss1;
			    //Bulk load completed, total run time :    2.98625 seconds
			    oss1 << "Load file distribution completed, total run time : ";
			    oss1 << fImportRslt.getTotalRunTime()<<" seconds" << endl;
			    if ( fRef.fCmdArgs.getConsoleOutput())
					fLog.logMsg( oss1.str(), MSGLVL_INFO1 );
				fRef.onSigInterrupt(0); // 1 for the sake of it
			}
		}
	}
	else if (fRef.fCmdArgs.getMode() == 1)
	{
		// Only time when we get a EOD on Mode 1 is when there is failure
		// in one of the PMs. Stop sending data and Send EOD to all PMS.
		// Make sure fileread thread is not working anymore.
		if(fFileReadThread.isContinue())	//check already stopped running
		{
			sendEODMsg();
			fFileReadThread.shutdown();
		}
	}

}

//------------------------------------------------------------------------------
//  BUG 4244
//	ERROR because
//	either an File already exist or the directory doesn't exists

void WESDHandler::onImpFileError(int PmId)
{
	if (fRef.fCmdArgs.getMode() != 0)
	{
		cout << "ERROR : we should not be here. Mode Non-Zero" << endl;
	}
	std::stringstream aStrStr;
	aStrStr << "Target file Error from PM" << PmId
			<< " - File already exists or path doesn't exist";
	char aDefCon[16], aRedCol[16];
	snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
	snprintf(aRedCol, sizeof(aRedCol), "\033[0;31m");
	cout << aRedCol << aStrStr.str() << aDefCon << endl;
	fLog.logMsg(aStrStr.str(), MSGLVL_ERROR);

	if (!fWeSplClients[PmId]->isCpiFailed())
		fWeSplClients[PmId]->setCpiFailed(true);
	else
		return; // this failure already reported so get out

	// If IMPFILE open Failed, then stop sending data to other PMs too
	if (checkAllCpiFailStatus())
	{	// If all are failed we will get out; no rollback for mode 0
		fRef.onSigInterrupt(1);		// 1 for the sake of it
	}
	else
	{	// Stop sending data to all PMs. Also send EOF to all PMs
		cancelOutstandingCpimports();
	}

}

//------------------------------------------------------------------------------
void WESDHandler::onPmErrorResponse(int PmId) {
	if (getDebugLvl())
		cout << "Received a NAK from " << PmId << endl;
}
//------------------------------------------------------------------------------
void WESDHandler::onKeepAliveMessage(int PmId) {
	if (getDebugLvl())
		cout << "Received a Keep Alive from " << PmId << endl;
}
//------------------------------------------------------------------------------
void WESDHandler::onCpimportPass(int PmId) {
	std::stringstream aStrStr;
	aStrStr << "Received a Cpimport Pass from PM" << PmId;
	logging::Message::Args errMsgArgs;
	//BUG 4152
	errMsgArgs.add(PmId);
	fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0098);
	if (getDebugLvl())
		cout << aStrStr.str() << endl;
	fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );

	fWeSplClients[PmId]->setCpiPassed(true);

	// Every CPI passed, BRM report will be send to us b4 this Msg.
	if (checkForCpiFailStatus()) // someone else failed,
	{ // so set this as failed for rollback.
		fWeSplClients[PmId]->setCpiPassed(false);
		fWeSplClients[PmId]->setCpiFailed(true);
		if (checkAllCpiFailStatus())
			doRollback();
	}

}

//------------------------------------------------------------------------------
void WESDHandler::onCpimportFail(int PmId, bool SigHandle)
{
	std::stringstream aStrStr;
	char aDefCon[16], aRedCol[16];
	snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
	snprintf(aRedCol, sizeof(aRedCol), "\033[0;31m");
	aStrStr <<aRedCol<<"Received a Cpimport Failure from PM"<<PmId <<aDefCon;
	logging::Message::Args errMsgArgs;
	errMsgArgs.add(aStrStr.str());

	if(0 != PmId)
	{
		fRef.fpSysLog->logMsg(errMsgArgs,logging::LOG_TYPE_ERROR,logging::M0000);
		//cout << aRedCol << aStrStr.str() << aDefCon << endl;
		fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );
		fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
		std::stringstream aStrStr2;
		aStrStr2 << "Please verify error log files in PM"<< PmId;
		if ( fRef.fCmdArgs.getConsoleOutput())
			fLog.logMsg( aStrStr2.str(), MSGLVL_INFO1 );

		if (!fWeSplClients[PmId]->isCpiFailed())
			fWeSplClients[PmId]->setCpiFailed(true);
		else
			return; // this failure already reported so get out
	}

	if((SigHandle) && (0==PmId)) { fForcedFailure = true; }

//	if (!fWeSplClients[PmId]->isCpiFailed())
//		fWeSplClients[PmId]->setCpiFailed(true);
//	else
//		return; // this failure already reported so get out

	// If Any CPI Failed, then stop other CPIMPORTS too
	// cancelOutstandingCpimports() - called below
	// TODO - later do a total rollback and release locks.
	if (checkAllCpiFailStatus())
	{
		doRollback();
		//fRef.onSigInterrupt(1);		// 1 for the sake of it
	}
	else
	{
		// Stop sending data to all PMs. Also send EOF to all PMs
		// so that all cpimports will finish bulk upload
		cancelOutstandingCpimports();
	}

}
//------------------------------------------------------------------------------

void WESDHandler::onBrmReport(int PmId, messageqcpp::SBS& Sbs) {
	std::stringstream aStrStr;
	aStrStr << "Received a BRM-Report from " << PmId;
	fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );
	if (getDebugLvl())
		cout << aStrStr.str() << endl;

	fWeSplClients[PmId]->setBrmRptRcvd(true);

	std::string aStr;
	int64_t aTotRows = 0;
	int64_t aInsRows = 0;
	int aColNum = 0;
	CalpontSystemCatalog::ColDataType aColType = CalpontSystemCatalog::INT;
	int aOorVal = 0;
	std::string aBadFileName;
	std::string aErrFileName;
	std::string aColName;

    if (getDebugLvl()>2)
        cout <<"BRM Report length : "<< (*Sbs).length() << endl;

	while ((*Sbs).length() > 0)
	{
		(*Sbs) >> aStr;

        if (getDebugLvl() > 2)
            cout <<"BRM Report value : "<< aStr << endl;

		bool aRet = WEBrmUpdater::prepareRowsInsertedInfo(aStr, aTotRows, aInsRows);
		if (aRet)
		{
			setRowsUploadInfo(PmId, aTotRows, aInsRows);
			fImportRslt.updateRowsProcessed(aTotRows);
			fImportRslt.updateRowsInserted(aInsRows);
		}
		aRet = WEBrmUpdater::prepareColumnOutOfRangeInfo(aStr, aColNum, aColType,
														 aColName, aOorVal);
		if (aRet)
		{
			add2ColOutOfRangeInfo(PmId, aColNum, aColType, aColName, aOorVal);
			fImportRslt.updateColOutOfRangeInfo(aColNum, aColType, aColName, aOorVal);
		}
		aRet = WEBrmUpdater::prepareBadDataFileInfo(aStr, aBadFileName);
		if (aRet)
		{
			setBadFileName(PmId, aBadFileName);
			// BUG 4324  - Mode 2 bad/err files left in PM(s).
			if(1==fRef.fCmdArgs.getMode()) getBadLog(PmId, aBadFileName);
			else if(2==fRef.fCmdArgs.getMode())
			{
				std::stringstream aOss;
				aOss << "Bad File : " << aBadFileName << " @ PM"<< PmId;
				if ( fRef.fCmdArgs.getConsoleOutput())
					fLog.logMsg( aOss.str(), MSGLVL_INFO1 );
			}
		}
		aRet = WEBrmUpdater::prepareErrorFileInfo(aStr, aErrFileName);
		if (aRet)
		{
			setErrorFileName(PmId, aErrFileName);
			// BUG 4324  - Mode 2 bad/err files left in PM(s).
			if(1==fRef.fCmdArgs.getMode()) getErrorLog(PmId, aErrFileName);
			else if(2==fRef.fCmdArgs.getMode())
			{
				std::stringstream aOss;
				aOss << "Err File : " << aErrFileName << " @ PM"<< PmId;
				if ( fRef.fCmdArgs.getConsoleOutput())
					fLog.logMsg( aOss.str(), MSGLVL_INFO1 );
			}
		}

		aRet = check4CriticalErrMsgs(aStr);	// check 4 Critical msg from .bin
		if(aRet)
		{
			std::stringstream aOss;
			aOss << "PM"<< PmId << " : " << aStr;
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(aOss.str());
			if (!fRef.fCmdArgs.getConsoleOutput())
			{
				ostringstream oss;
				oss << "/tmp/" <<fTableOId << ".txt";
				ofstream dmlFile(oss.str().c_str(), std::ofstream::app);
				if (dmlFile.is_open())
				{
					dmlFile << aOss.str();
					dmlFile << endl;
				}
				dmlFile.close();	
			}	
			fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
			if (getDebugLvl()) cout << aOss.str() << endl;
			fLog.logMsg( aOss.str(), MSGLVL_ERROR );
		}
		else	// do not add Crit Msgs to BRMRpt vector
			fBrmRptVec.push_back(aStr);
	}

	// Even when CPI fail, we get BRMRpt to get the Err/Bad file.
	if (checkForCpiFailStatus())
		return; // if a PM failed, don't update BRM.

	//cout << "Checking clients for BRM Reports" << endl;
	//TODO we should update BRM with the report we got.
	if (check4AllBrmReports()) {
		bool aRslt = updateCPAndHWMInBRM();
		if (aRslt) {
			std::stringstream aStrStr;
			aStrStr << "BRM updated successfully ";
			if (getDebugLvl()) cout << aStrStr.str() << endl;
			fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );

			if (fTableLock != 0) {

				WETableLockGrabber aTLG(*this);
				bool aRet = aTLG.changeTableLockState(fTableLock);
				if (aRet)
				{
					if (getDebugLvl())
						cout << "\tSuccessfully changed TableLock State" << endl;
					doCleanup(true);
				}
				else
				{
					std::stringstream aStrStr;
					aStrStr << "Failed to change TableLock state to cleanup";
					fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
					if (getDebugLvl()) cout << aStrStr.str() << endl;
				}

			}

		}
		else
		{
			std::stringstream aStrStr;
			aStrStr	<< "\tBRM update Failed : Need to Manually release the table locks";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(aStrStr.str());
			fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
			if (getDebugLvl()) cout << aStrStr.str() << endl;
			fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
			fRef.onSigInterrupt(1); //BUG 4701 //failure in BRM update & process, ie. 1
		}

	}
	else
	{
		if (getDebugLvl())	cout << "Still Brm Reports to come in!!" << endl;
	}

}

//------------------------------------------------------------------------------

bool WESDHandler::check4CriticalErrMsgs(std::string& Entry)
{
	bool aFound=false;
	if ((!Entry.empty()) && (Entry.at(0) == 'M'))
	{
		aFound = true;
		// start from after "MERR: "
		std::string aTmp(Entry.begin()+6, Entry.end());
		Entry = aTmp;
	}
	return aFound;
}

//------------------------------------------------------------------------------
void WESDHandler::onErrorFile(int PmId, messageqcpp::SBS& Sbs) {
	std::stringstream aStrStr;
	aStrStr << "Received ErrReport from " << PmId;
	logging::Message::Args errMsgArgs;
	// BUG 4152
	errMsgArgs.add(PmId);
	fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0099);
	if (getDebugLvl()) cout << aStrStr.str() << endl;
	fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );

	//TODO - Open the ERROR log file and append to it
	ofstream aErrFile;
	std::string aErrFileName;
	std::string aTmpFileName;
	std::string aData;
	(*Sbs) >> aTmpFileName;

	// BUG 4324  - Mode 1 bad/err files stored in datafile loc or CWD
    size_t aPos = aTmpFileName.rfind('/');
    if(aPos != std::string::npos)
    {
    	std::string aFile = aTmpFileName.substr(aPos+1); //+1 to pass '/'

    	std::string aInFile = fRef.getLocFile(); // input file
    	if(aInFile != "/dev/stdin")
    	{
    		size_t aPos2 = aInFile.rfind('/');
    		if(aPos2 != std::string::npos)
    		{
    			std::string aStr = aInFile.substr(0, aPos2+1);
    			//std::cout << "Point 1 " << aStr << std::endl;
    			std::stringstream aStrStr1;
    			aStrStr1 << aStr << aFile;
    			aTmpFileName = aStrStr1.str();
    			//std::cout << "Point 2 " << aTmpFileName << std::endl;
    		}
    		else
    			aTmpFileName = aFile;
    	}
    	else
        	aTmpFileName = aFile;

    	if (getDebugLvl())
    		std::cout << "Prep ErrFile " << aTmpFileName << std::endl;
    }

	aStrStr.str(std::string());
	aStrStr << aTmpFileName << "_" << PmId;
	aErrFileName = aStrStr.str(); //PmId+"_"+aTmpFileName;
	if (getDebugLvl())
		cout << "Error File Name: " << aErrFileName << endl;
	if (getDebugLvl())
		cout << "Error Data: " << endl;
	try
	{
		aErrFile.open(aErrFileName.c_str());
		while ((*Sbs).length() > 0)
		{
			(*Sbs) >> aData;
			if (getDebugLvl()>1) cout << aData << endl;
			aErrFile << aData;
			aErrFile << endl;
		}
		aErrFile.close();
		setErrorFileName(PmId, aErrFileName);
		aStrStr.str(std::string());
		aStrStr << "Row numbers with error reasons are listed in file : "<< aErrFileName;
		if ( fRef.fCmdArgs.getConsoleOutput())
			fLog.logMsg( aStrStr.str(), MSGLVL_INFO1 );

	} catch (std::exception&) {
		cout << "Error in opening the ERROR file!!" << aErrFileName << endl;
		cout << "Error in opening the ERROR file!!" << aTmpFileName << endl;
		cout << "Check for ErrorFile " << aTmpFileName << "in Pm " << PmId
				<< endl;
	}

}

//------------------------------------------------------------------------------
// Process a bulk load *.bad file containing the rejected rows from a PM.
//------------------------------------------------------------------------------
void WESDHandler::onBadFile(int PmId, messageqcpp::SBS& Sbs) {
	std::stringstream aStrStr;
	aStrStr << "Received BadData Report from " << PmId;
	logging::Message::Args errMsgArgs;
	//BUG 4152
	errMsgArgs.add(PmId);
	fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0100);
	if (getDebugLvl()) cout << aStrStr.str() << endl;
	fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );

	//TODO - Open the ERROR log file and append to it
	ofstream aBadFile;
	std::string aBadFileName;
	std::string aTmpFileName;
	std::string aData;
	(*Sbs) >> aTmpFileName;

	// BUG 4324  - Mode 1 bad/err files stored in datafile loc or CWD
    size_t aPos = aTmpFileName.rfind('/');
    if(aPos != std::string::npos)
    {
    	std::string aFile = aTmpFileName.substr(aPos+1); //+1 to pass '/'

    	std::string aInFile = fRef.getLocFile(); // input file
    	if(aInFile != "/dev/stdin")
    	{
    		size_t aPos2 = aInFile.rfind('/');
    		if(aPos2 != std::string::npos)
    		{
    			std::string aStr = aInFile.substr(0, aPos2+1);
    			//std::cout << "Point 1 " << aStr << std::endl;
    			std::stringstream aStrStr1;
    			aStrStr1 << aStr << aFile;
    			aTmpFileName = aStrStr1.str();
    			//std::cout << "Point 2 " << aTmpFileName << std::endl;
    		}
    		else
    			aTmpFileName = aFile;
    	}
    	else
        	aTmpFileName = aFile;

    	if (getDebugLvl()>1)
    		std::cout << "Prep BadFile " << aTmpFileName << std::endl;
    }

	aStrStr.str(std::string());
	aStrStr << aTmpFileName << "_" << PmId;
	aBadFileName = aStrStr.str(); //PmId+"_"+aTmpFileName;
	if (getDebugLvl()>1)
		cout << "Bad File Name: " << aBadFileName << endl;
	if ( (getDebugLvl()>1) &&
		(fRef.fCmdArgs.getImportDataMode() == IMPORT_DATA_TEXT) )
		cout << "Bad Data: " << endl;
	std::string task;
	try
	{
		task = "opening";
		aBadFile.open(aBadFileName.c_str());

		task = "copying rejected rows to";
		while ((*Sbs).length() > 0)
		{
			(*Sbs) >> aData;
			if ( (getDebugLvl()>1) &&
				(fRef.fCmdArgs.getImportDataMode() == IMPORT_DATA_TEXT) )
			{
				cout << aData;
			}
			aBadFile.write( aData.c_str(), aData.length() );
		}
		aBadFile.close();
		setBadFileName(PmId, aBadFileName);

		aStrStr.str(std::string());
		aStrStr << "Exact error rows are listed in file : "<< aBadFileName;
		if ( fRef.fCmdArgs.getConsoleOutput())
			fLog.logMsg( aStrStr.str(), MSGLVL_INFO1 );
	}
	catch (std::exception& ex)
	{
		cout << "Error in " << task << " the bad file " <<
			aBadFileName << "; " << ex.what() << endl;
		cout << "Check for Bad File " << aTmpFileName << " on Pm " <<PmId<<endl;
	}
}

//------------------------------------------------------------------------------

void WESDHandler::getErrorLog(int PmId, const std::string& ErrFileName) {
	if (getDebugLvl())
		cout << "Requesting Error Log" << endl;
	//TODO code appropriately for the message
	messageqcpp::ByteStream aBs;
	aBs << (ByteStream::byte) WE_CLT_SRV_ERRLOG;
	aBs << ErrFileName;
	send2Pm(aBs, PmId);
	//fWeSplClients[PmId]->setErrLogRqst(true);

}

//------------------------------------------------------------------------------

void WESDHandler::getBadLog(int PmId, const std::string& BadFileName) {
	if (getDebugLvl())
		cout << "Requesting Bad Log" << endl;
	//TODO code appropriately for the message
	messageqcpp::ByteStream aBs;
	aBs << (ByteStream::byte) WE_CLT_SRV_BADLOG;
	aBs << BadFileName;
	send2Pm(aBs, PmId);
	//fWeSplClients[PmId]->setBadLogRqst(true);

}

//------------------------------------------------------------------------------

void WESDHandler::onRollbackResult(int PmId, messageqcpp::SBS& Sbs) {
	ByteStream::byte aRslt = 0;

	(*Sbs) >> aRslt;

	if (getDebugLvl())
		cout << "Rollback rslt arrived PmId = " << PmId << " Rslt = "
				<< (int) aRslt << endl;

	if (aRslt)
		fWeSplClients[PmId]->setRollbackRslt(1);
	else {
		fWeSplClients[PmId]->setRollbackRslt(-1);
		std::stringstream aStrStr;
		aStrStr << "Rollback Failed on PM : " << PmId;
		logging::Message::Args errMsgArgs;
		//BUG 4152
		errMsgArgs.add(PmId);
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR,
				logging::M0101);
		fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
		if (getDebugLvl())
			cout << aStrStr.str() << endl;
	}

	int aStatus = check4RollbackRslts();
	if (aStatus == -1) {
		std::stringstream aStrStr;
		aStrStr << "Rollback Failed on one or more PMs";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(aStrStr.str());
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR,
				logging::M0000);
		fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
		if (getDebugLvl())
			cout << aStrStr.str() << endl;

		//fRef.onSigInterrupt(1);		// 1 for the sake of it
		if (check4AllRollbackStatus()) {
			fRef.onSigInterrupt(1); // process altogether is a failure
		}
	} else if (aStatus == 1) {
		std::stringstream aStrStr;
		aStrStr << "Rollback succeed on all PMs";
		fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );
		if (getDebugLvl()) cout << aStrStr.str() << endl;

		// false flag sent to doCleanup says to not delete HDFS temp db files,
		// because the bulk rollback will have already deleted them.  We still
		// call doCleanup for other file cleanup (like deleting meta file).
		doCleanup(false);
	}

}

//------------------------------------------------------------------------------

void WESDHandler::onCleanupResult(int PmId, messageqcpp::SBS& Sbs) {
	ByteStream::byte aRslt = 0;
	(*Sbs) >> aRslt;

	if (getDebugLvl())
		cout << "Cleanup rslt arrived PmId = " << PmId << " Rslt = "
				<< (int) aRslt << endl;

	if (aRslt)
		fWeSplClients[PmId]->setCleanupRslt(1);
	else {
		fWeSplClients[PmId]->setCleanupRslt(-1);
		std::stringstream aStrStr;
		aStrStr << "ERROR: Cleanup Failed on PM : " << PmId;
		logging::Message::Args errMsgArgs;
		//BUG 4152
		errMsgArgs.add(PmId);
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0102);
		if (getDebugLvl()) cout << aStrStr.str() << endl;
		fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
	}

	int aStatus = check4CleanupRslts();
	if (aStatus == -1) {
		std::stringstream aStrStr;
		aStrStr << "Cleanup Failed on one or more PMs";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(aStrStr.str());
		fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
		fLog.logMsg( aStrStr.str(), MSGLVL_ERROR );
		if (getDebugLvl()) cout << aStrStr.str() << endl;

		//fRef.onSigInterrupt(1);		// 1 for the sake of it -
		// We need to wait till all the results comes back.
		if (check4AllCleanupStatus()) {
			fRef.onSigInterrupt(1); // failure in cleanup & process, ie. 1
		}
	} else if (aStatus == 1) {
		releaseTableLocks();

		std::stringstream aStrStr;
		aStrStr << "Cleanup succeed on all PMs";
		fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );
		if (getDebugLvl())
			cout << aStrStr.str() << endl;

		if (checkAllCpiPassStatus()) //Cleanup and entire process success.
		{
		    ostringstream oss;
		    //For table walt.abc: 1000 rows processed and 1000 rows inserted.
		    oss << "For table ";
		    oss << fRef.fCmdArgs.getSchemaName() << ".";
			oss << fRef.fCmdArgs.getTableName() << ": ";
			oss << fImportRslt.fRowsPro << " rows processed and ";
			oss << fImportRslt.fRowsIns << " rows inserted.";
			
			if ( fRef.fCmdArgs.getConsoleOutput())
				fLog.logMsg( oss.str(), MSGLVL_INFO1 );

			// BUG 4399 Print out WARN messages for out of range counts
			WEColOorVec::iterator aIt = fImportRslt.fColOorVec.begin();
			ofstream dmlFile;
			if (!fRef.fCmdArgs.getConsoleOutput()) //for DML to use file /tmp/
			{
				ostringstream oss;
				oss << "/tmp/" <<fTableOId << ".txt";
				dmlFile.open(oss.str().c_str());
			}			
			
			while(aIt != fImportRslt.fColOorVec.end())
			{
				if ((*aIt).fNoOfOORs > 0)
				{
					ostringstream ossSatCnt;
					ossSatCnt << "Column " << (*aIt).fColName << "; Number of ";
					switch ((*aIt).fColType)
					{
					case CalpontSystemCatalog::DATE:
						ossSatCnt << "invalid dates replaced with null: ";
						break;
					case CalpontSystemCatalog::DATETIME:
						ossSatCnt << "invalid date/times replaced with null: ";
						break;
					case CalpontSystemCatalog::CHAR:
						ossSatCnt << "character strings truncated: ";
						break;
					case CalpontSystemCatalog::VARCHAR:
						ossSatCnt << "varchar strings truncated: ";
						break;
					default:
						ossSatCnt << "rows inserted with saturated values: ";
						break;
					}
					ossSatCnt << (*aIt).fNoOfOORs;
					fLog.logMsg(ossSatCnt.str(), MSGLVL_WARNING);
					if (!fRef.fCmdArgs.getConsoleOutput()) //for DML to use
					{
						if (dmlFile.is_open())
						{
							dmlFile << (*aIt).fNoOfOORs;
							dmlFile << endl;
						}
					}
				}
				aIt++;
			}
			dmlFile.close();
			fImportRslt.stopTimer();
		    ostringstream oss1;
		    //Bulk load completed, total run time :    2.98625 seconds
		    oss1 << "Bulk load completed, total run time : ";
		    oss1 << fImportRslt.getTotalRunTime()<<" seconds";
		    
		    if ( fRef.fCmdArgs.getConsoleOutput())
				fLog.logMsg( oss1.str(), MSGLVL_INFO1 );

			fRef.onSigInterrupt(0); // 0 for entire success
		}
		else
		{
			ostringstream oss;
			oss << "Table "<<fRef.fCmdArgs.getSchemaName()<<".";
			oss << fRef.fCmdArgs.getTableName() << ": (OID-";
			oss << this->getTableOID() << ") was NOT successfully loaded.";
			if ( fRef.fCmdArgs.getConsoleOutput())
				fLog.logMsg( oss.str(), MSGLVL_INFO1 );

			fImportRslt.stopTimer();
			ostringstream oss1;
			//Bulk load completed, total run time :    2.98625 seconds
			oss1 << "Bulk load completed, total run time : ";
			oss1 << fImportRslt.getTotalRunTime()<<" seconds";
			if ( fRef.fCmdArgs.getConsoleOutput())
				fLog.logMsg( oss1.str(), MSGLVL_INFO1 );
			// Even though cleanup is success, entire process is failure
			fRef.onSigInterrupt(1); // therefore 1
		}
	}

}

//------------------------------------------------------------------------------

void WESDHandler::onDBRootCount(int PmId, messageqcpp::SBS& Sbs) {
	ByteStream::byte aDbrCnt = 0;
	(*Sbs) >> aDbrCnt;

	if (getDebugLvl())
		cout << "No of DBRoots in PM" << PmId << " = " << (int) aDbrCnt << endl;

	if (aDbrCnt > 0) {
		fWeSplClients[PmId]->setDbRootCnt(static_cast<unsigned int>(aDbrCnt));
		fWeSplClients[PmId]->resetDbRootVar();
	}

}

//------------------------------------------------------------------------------

void WESDHandler::doRollback()
{

	std::string aAppName = "cpimport";
	messageqcpp::ByteStream aBs;
	aBs << (ByteStream::byte) WE_CLT_SRV_ROLLBACK;
	aBs << (ByteStream::octbyte) fTableLock;
	aBs << (ByteStream::quadbyte) fTableOId;
	aBs << fRef.fCmdArgs.getTableName();
	aBs << aAppName;
	mutex::scoped_lock aLock(fSendMutex);
	send2Pm(aBs);
	aLock.unlock();

}

//------------------------------------------------------------------------------

void WESDHandler::doCleanup(bool deleteHdfsTempDbFiles) {
	if (getDebugLvl())
		cout << "A cleanup is called!!" << endl;

	messageqcpp::ByteStream aBs;
	aBs << (ByteStream::byte) WE_CLT_SRV_CLEANUP;
	aBs << (ByteStream::quadbyte) fTableOId;
	aBs << (ByteStream::byte) deleteHdfsTempDbFiles;
	mutex::scoped_lock aLock(fSendMutex);
	send2Pm(aBs);
	aLock.unlock();
}

//------------------------------------------------------------------------------

bool WESDHandler::releaseTableLocks() {
	if (fTableLock != 0) {
		WETableLockGrabber aTLG(*this);

		// BUG 4398. Move the call to takeSnapshot() from cpimport.bin to here
		fDbrm.takeSnapshot();

		bool aRet = aTLG.releaseTableLock(fTableLock);
		if (aRet) {
			std::stringstream aStrStr;
			aStrStr << "Released Table Lock";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(aStrStr.str());
			fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO,
					logging::M0000);
			if (getDebugLvl())
				cout << aStrStr.str() << endl;
			fLog.logMsg( aStrStr.str(), MSGLVL_INFO2 );

			return true;
		}
	}
	return false;
}

//------------------------------------------------------------------------------

int WESDHandler::check4RollbackRslts() {
	int aStatus = 1;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			int aRslt = fWeSplClients[aCnt]->getRollbackRslt();
			if (aRslt == -1) {
				//cout << "Rollback Failed in PM - " << aCnt << endl;
				aStatus = -1;
				break;
			} else if (aRslt == 0) // not all results available yet
					{
				aStatus = 0;
				break;
			}
		}
	}

	return aStatus;
}

//------------------------------------------------------------------------------

int WESDHandler::check4CleanupRslts() {
	int aStatus = 1;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			int aRslt = fWeSplClients[aCnt]->getCleanupRslt();
			if (aRslt == -1) {
				//cout << "Cleanup Failed in PM - " << aCnt << endl;
				aStatus = -1;
				break;
			} else if (aRslt == 0) // not all results available yet
					{
				aStatus = 0;
				break;
			}
		}
	}
	return aStatus;

}

//------------------------------------------------------------------------------

bool WESDHandler::check4AllRollbackStatus() {
	bool aStatus = true;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			int aRslt = fWeSplClients[aCnt]->getRollbackRslt();
			if (aRslt == 0) // not all results available yet; either -1/1
			{
				aStatus = false;
				break;
			}
		}
	}
	return aStatus;
}

//------------------------------------------------------------------------------

bool WESDHandler::check4AllCleanupStatus() {
	bool aStatus = true;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			int aRslt = fWeSplClients[aCnt]->getCleanupRslt();
			if (aRslt == 0) // not all results available yet; either -1/1
					{
				aStatus = false;
				break;
			}
		}
	}
	return aStatus;
}

//------------------------------------------------------------------------------

bool WESDHandler::checkAllCpiPassStatus() {
	bool aStatus = true;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			if (!fWeSplClients[aCnt]->isCpiPassed()) {
				if (getDebugLvl())
					cout << "CPI Pass status still False in " << aCnt << endl;
				aStatus = false;
				break;
			}
		}
	}
	return aStatus;
}
//
//------------------------------------------------------------------------------

bool WESDHandler::checkAllCpiFailStatus() {
	bool aStatus = true;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			if (!fWeSplClients[aCnt]->isCpiFailed())
			{
				if(getDebugLvl())
					cout << "CPI Fail status still False in " << aCnt << endl;
				aStatus = false;
				break;
			}
		}
	}
	return aStatus;
}

//-----------------------------------------------------------------------------
bool WESDHandler::check4AllBrmReports() {
	for (int PmId = 1; PmId <= fPmCount; ++PmId) {
		if (fWeSplClients[PmId] != NULL) {
			if (!fWeSplClients[PmId]->isBrmRptRcvd()) {
				if (getDebugLvl())
					cout << "BRMReport from " << PmId << " still not received"
							<< endl;
				return false;
			}
		}
	}
	return true;
}


//------------------------------------------------------------------------------
int WESDHandler::getNextPm2Feed() {
	//int aLdPm = leastDataSendPm();
	int aLdPm = getNextDbrPm2Send();

	if (fWeSplClients[aLdPm] != 0) {
		// Balancing the total bytes sent and Q Size
		int aSz = fWeSplClients[aLdPm]->getSendQSize();
		if (aSz > MAX_QSIZE)
		{	//cout << "Queue Size = " << aSz << endl;
			if(fpBatchLoader) fpBatchLoader->reverseSequence();
			aLdPm = 0; //Filled Q
		} // Check enough DataRqst to send Data

		if (getDebugLvl() > 2) cout << "NextPm2Feed " << aLdPm << endl;
		// will work only specific to mode 0 (fpBatchLoader==0)
		if ((aLdPm > 0)&&(!fpBatchLoader)) fWeSplClients[aLdPm]->decDbRootVar();
	}
	else if(aLdPm != 0)
	{
		cout << "Next PMid Error : PmId = "<<aLdPm <<endl;
		aLdPm = 0;
	}

	return aLdPm;
}
//------------------------------------------------------------------------------

int WESDHandler::getNextDbrPm2Send() {
	unsigned int aDbrVar = 0;
	unsigned int aPmId = 0;

	if ((!fAllCpiStarted) && (1 == fRef.fCmdArgs.getMode()))
	{
		check4AllCpiStarts();
		return aPmId; // Not all Cpi started.
	}

	//NOTE : Implementing BatchLoader, which will be used to select the
	//			FIRST PM to send data and subsequent PM's to send data
	if(fpBatchLoader)//for mode 1 and 2 only, since mode 0 don't have tableOID
	{
		if((!fFirstDataSent)&&(1 == fRef.fCmdArgs.getMode()))
		{

			try
			{
				aPmId = fFirstPmToSend;
				fFirstDataSent=true;
				if(fSelectOtherPm) aPmId = 0; //Select the other method
				if(getDebugLvl()) cout << "1st PM for data = "<<aPmId << endl;
				fpBatchLoader->prepareForSecondPM();
			}
			catch(std::exception& ex)
			{
				fLog.logMsg(ex.what(), MSGLVL_ERROR );
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(ex.what());
				fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
			}

		}
		else
		{
			aPmId = fpBatchLoader->selectNextPM();
		}
		if (getDebugLvl()>2) cout << "Next PM to get data = "<< aPmId << endl;
		return aPmId;
	}

	//--------- The part below is for mode 0, which don't have table relation

	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {

		if (fWeSplClients[aCnt] != 0) {
			if (aPmId == 0) // init
			{
				aDbrVar = fWeSplClients[aCnt]->getDbRootVar();
				aPmId = aCnt;
			}
			else if (fWeSplClients[aCnt]->getDbRootVar() > aDbrVar)
			{
				aPmId = aCnt;
				aDbrVar = fWeSplClients[aCnt]->getDbRootVar();
			}
		}

	}

	if (aDbrVar == 0)
	{
		for (int aCnt = 1; aCnt <= fPmCount; aCnt++)
		{
			if (fWeSplClients[aCnt] != 0)
			{
				fWeSplClients[aCnt]->resetDbRootVar();
			}
		}
		aPmId = 0;
	}

	if (getDebugLvl() > 2)
		cout << "DbrPM2Send [" << aPmId << "] = " << aDbrVar << endl;

	return aPmId;

}


//------------------------------------------------------------------------------

int WESDHandler::leastDataSendPm() {
	unsigned int aTx = 0;
	int aPmId = 0;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			if (aPmId == 0) // init
					{
				aTx = fWeSplClients[aCnt]->getBytesTx();
				aPmId = aCnt;
			} else if (fWeSplClients[aCnt]->getBytesTx() < aTx) {
				aPmId = aCnt;
				aTx = fWeSplClients[aCnt]->getBytesTx();
			}
		}
	}
	return aPmId;
}

//------------------------------------------------------------------------------

int WESDHandler::getTableOID(std::string Schema, std::string Table) {
	execplan::CalpontSystemCatalog::ROPair roPair;
	CalpontSystemCatalog::TableName tableName(Schema, Table);
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
			CalpontSystemCatalog::makeCalpontSystemCatalog();
	roPair = systemCatalogPtr->tableRID(tableName);

	return roPair.objnum;
}

//------------------------------------------------------------------------------
// Get the expected import binary fixed record length for the specified table.
//------------------------------------------------------------------------------
unsigned int WESDHandler::calcTableRecLen(
	const std::string& schema, const std::string table) {
	unsigned int recLen = 0;

	CalpontSystemCatalog::TableName tableName(schema, table);
	boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
		CalpontSystemCatalog::makeCalpontSystemCatalog();
	CalpontSystemCatalog::RIDList colRidList =
		systemCatalogPtr->columnRIDs(tableName, true);
	CalpontSystemCatalog::RIDList::const_iterator rid_iterator =
		colRidList.begin();

	std::set<std::string> colListInJobFile;
	fRef.fCmdArgs.getColumnList( colListInJobFile );
	std::set<std::string>::const_iterator setIter;

	// Add up the column widths to get the total record length
	while (rid_iterator != colRidList.end())
	{
		CalpontSystemCatalog::ROPair roPair = *rid_iterator;
		CalpontSystemCatalog::OID       oid = roPair.objnum;
		CalpontSystemCatalog::ColType   colType;

		colType = systemCatalogPtr->colType(oid);

		// If we have a list of column names taken from an XML job file,
		// then we filter against that list
		if (colListInJobFile.size() > 0)
		{
			CalpontSystemCatalog::TableColName colName;
			colName = systemCatalogPtr->colName(oid);

			setIter = colListInJobFile.find( colName.column );
			if (setIter != colListInJobFile.end())
			{
				recLen += colType.colWidth;
			}
		}
		else
		{
			recLen += colType.colWidth;
		}

		++rid_iterator;
	}

	if(getDebugLvl())
		cout << "Binary record length for " << schema << '.' << table <<
			" is: " << recLen << endl;

	return recLen;
}

//------------------------------------------------------------------------------

void WESDHandler::check4CpiInvokeMode() {
	try {
		oam::oamModuleInfo_t aModInfo = fOam.getModuleInfo();
		string aModuleType = boost::get < 1 > (aModInfo);
		if (getDebugLvl())
			cout << "ModuleType " << aModuleType << endl;
		int aInstallType = boost::get < 5 > (aModInfo);
		if (getDebugLvl())
			cout << "InstallType " << aInstallType << endl;

		fOam.getSystemConfig("pm", fModuleTypeConfig);
		fPmCount = fModuleTypeConfig.ModuleCount;

		if (getDebugLvl())
			cout << "PM Count " << fPmCount << endl;

		//oam::INSTALL_COMBINE_* 2,3,4  ie UM+PM in 1 Machine
		if ((aInstallType > 1) && (fPmCount == 1)) {
			//Mode arg was NOT provided; set to to default Mode 3
			if (fRef.fCmdArgs.getArgMode() == -1) {
				fRef.fCmdArgs.setMode(3);
				fRef.fCmdArgs.setCpiInvoke();
			} else if (fRef.fCmdArgs.getArgMode() == 3) //BUG 4210
					{
				fRef.fCmdArgs.setCpiInvoke();
			} else if (fRef.fCmdArgs.getArgMode() == 0) {
				throw runtime_error("Mode 0 allowed only in Multi-Nodes.");
			}

		} else if ((aInstallType == oam::INSTALL_NORMAL)
				&& (aModuleType == "um")) {
			//BUG 4165
			if (fRef.fCmdArgs.getMode() == 3) {
				throw runtime_error("Mode 3 imports can only be run on a PM.");
			}
			else if (fRef.fCmdArgs.getMode() == -1) //default mode //BUG 4210
					{
				fRef.fCmdArgs.setMode(1);
			}
		} else if (aModuleType == "pm") //BUG 4210
				{ //BUG 4210
			if ((fPmCount >= 1) && (fRef.fCmdArgs.getMode() == 3)) {
				fRef.fCmdArgs.setCpiInvoke();
			}
			// Single node default without argument option m
			else if ((fPmCount == 1) && (fRef.fCmdArgs.getArgMode() == -1)) {
				fRef.fCmdArgs.setMode(3);
				fRef.fCmdArgs.setCpiInvoke();
			} // Multi-node default without argument option m
			else if ((fPmCount > 1) && (fRef.fCmdArgs.getArgMode() == -1)) {
				fRef.fCmdArgs.setMode(1);
			}
		}

	} catch (runtime_error& exp) {
		throw runtime_error(exp.what());
	} catch (...) {
		std::string aStr =
				"oam.getModuleInfo/getSystemConfig error : WESDHandler check4CpiInvoke()";
		throw runtime_error(aStr);
	}

	check4PmArguments();

}

//------------------------------------------------------------------------------
bool WESDHandler::check4PmArguments()
{
	if(fRef.fCmdArgs.getPmVecSize()>0)
	{
		std::vector<unsigned int> aPmVec = fRef.fCmdArgs.getPmVec();
		int aSize = aPmVec.size();
		for (int aIdx=0; aIdx< aSize; aIdx++ )
		{
			if((fPmCount < static_cast<int>(aPmVec[aIdx]))||(0 == aPmVec[aIdx]))
			{
				std::stringstream aStrStr;
				aStrStr << "Invalid argument PMid "	<< aPmVec[aIdx] << endl;
				throw runtime_error(aStrStr.str());
			}
		}

	}
	return true;
}


//------------------------------------------------------------------------------
bool WESDHandler::check4AllCpiStarts() {
	bool aStarted = true;
	for (int aCnt = 1; aCnt <= fPmCount; aCnt++) {
		if (fWeSplClients[aCnt] != 0) {
			if (!fWeSplClients[aCnt]->isCpiStarted())
				aStarted = false;
		}
	}
	if (aStarted)
		fAllCpiStarted = aStarted;
	return aStarted;
}

//------------------------------------------------------------------------------

void WESDHandler::exportJobFile(std::string &JobId, std::string &JobFileName)
{

	if (getDebugLvl())
		cout << "JobFile Name is " << JobFileName << endl;
	ifstream aInFile;
	aInFile.open(JobFileName.c_str());
	if ((aInFile.is_open()) && (!aInFile.eof()))
	{
		std::stringstream aSs;
		aSs << fRef.fCmdArgs.getTmpFileDir();
		aSs <<"/Job_";
		aSs << JobId;
		aSs << ".xml";

		messageqcpp::ByteStream aBs;
		aBs << (ByteStream::byte) WE_CLT_SRV_JOBID;
		aBs << aSs.str();
		send2Pm(aBs);

		if (getDebugLvl())
			cout << "exportJobFile::Send RmtFileName " << aSs.str() << endl;

		// Read everything to a String
		std::string aData((std::istreambuf_iterator<char>(aInFile)),
		std::istreambuf_iterator<char>());
		if (getDebugLvl())
			cout << "Sending XML FileData " << aData << endl;
		aBs.restart();
		aBs << (ByteStream::byte) WE_CLT_SRV_JOBDATA;
		aBs << aData;
		send2Pm(aBs);

	} else {
		throw runtime_error("unable to open Job File");
	}

}

//------------------------------------------------------------------------------
bool WESDHandler::getConsoleLog()
{
	return fRef.fCmdArgs.getConsoleLog();
}
//------------------------------------------------------------------------------

char WESDHandler::getEnclChar()
{
	return fRef.fCmdArgs.getEnclChar();
}

//------------------------------------------------------------------------------

char WESDHandler::getEscChar()
{
	return fRef.fCmdArgs.getEscChar();
}

//------------------------------------------------------------------------------

char WESDHandler::getDelimChar()
{
	return fRef.fCmdArgs.getDelimChar();
}

//------------------------------------------------------------------------------

std::string WESDHandler::getTableName() const
{
	return fRef.fCmdArgs.getTableName();
}
//------------------------------------------------------------------------------

std::string WESDHandler::getSchemaName() const
{
	return fRef.fCmdArgs.getSchemaName();
}

ImportDataMode WESDHandler::getImportDataMode() const
{
	return fRef.fCmdArgs.getImportDataMode();
}

//------------------------------------------------------------------------------

void WESDHandler::sysLog(const logging::Message::Args& msgArgs,
		logging::LOG_TYPE logType, logging::Message::MessageID msgId)
{
	fRef.fpSysLog->logMsg(msgArgs, logType, msgId);
}

//------------------------------------------------------------------------------

std::string WESDHandler::getTime2Str() const
{
	char aBuff[64];
	time_t aTime;
	struct tm pTm;
	time(&aTime);
	localtime_r(&aTime, &pTm);

	//				  M   D   H   M   S
	snprintf(aBuff, sizeof(aBuff), "%02d%02d%02d%02d%02d",
			pTm.tm_mon+1, pTm.tm_mday, pTm.tm_hour,
			pTm.tm_min, pTm.tm_sec);

	return aBuff;
}

//------------------------------------------------------------------------------

bool WESDHandler::check4InputFile(std::string InFileName)
{
	bool aRet = false;
	if((0==InFileName.compare("STDIN"))||(0==InFileName.compare("stdin")))
	{
		fFileReadThread.add2InputDataFileList(InFileName);
		return true;
	}
	else
	{
		//BUG 4342 - Need to support "list of infiles"
		fFileReadThread.chkForListOfFiles(InFileName);
		std::string aFileName = fFileReadThread.getNextInputDataFile();
		std::ifstream aFile(aFileName.c_str());
		aRet = (aFile.good())?true:false;
		// add back to list, which we pop_front for checking the file.
		if(aRet) fFileReadThread.add2InputDataFileList(aFileName);
	}
	return aRet;
}

//------------------------------------------------------------------------------

void WESDHandler::onHandlingSignal()
{
	std::stringstream aStrStr;
	char aDefCon[16], aRedCol[16];
	snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
	snprintf(aRedCol, sizeof(aRedCol), "\033[0;31m");
	aStrStr <<aRedCol<<"Received signal to terminate the process."<<aDefCon;
	
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr.str(), MSGLVL_INFO1 );

	logging::Message::Args errMsgArgs;
	errMsgArgs.add(aStrStr.str());
	fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);

	std::stringstream aStrStr1;
	aStrStr1 <<"Handling signal ......";
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr1.str(), MSGLVL_INFO1 );

	fRef.fSignaled = false;
	bool aTblLockReleased = false;
	bool aRollbackSuccess = false;
	onCpimportFail(0, true);
	usleep(2000000*fPmCount);

	//BUG 4649  - Some systems taking too long to finish the process.
	//				So we have to wait some more time.
	std::stringstream aStrStr2;
	aStrStr2 << "Rolling back ..........";
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr2.str(), MSGLVL_INFO1 );
	for(int aIdx=0; aIdx<60; aIdx++)
	{
		int aStatus = check4RollbackRslts();
		if(1==aStatus)
		{
			if (getDebugLvl()) cout << "Rollback Successful... " << endl;
			aRollbackSuccess = true;
			break;
		}
		else if(-1 == aStatus)
		{
			if (getDebugLvl()) cout << "Rollback Failed... " << endl;
			break;
		}
		usleep(2000000*fPmCount);
	}

    //Bug 5774 - if rollback failed, leave the tablelock
	if(!aRollbackSuccess)
	{    
		std::stringstream aStrStr2a;
		aStrStr2a << "Rollback Failed; Leaving Tablelock ... "; 
		if ( fRef.fCmdArgs.getConsoleOutput())
			fLog.logMsg( aStrStr2a.str(), MSGLVL_INFO1 );
		return;
	}    

	std::stringstream aStrStr3;
	aStrStr3 << "Cleaning up ..........";
	
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr3.str(), MSGLVL_INFO1 );
	for(int aIdx=0; aIdx<60; aIdx++)
	{
		int aStatus = check4CleanupRslts();
		if(aStatus == 1)
		{
			if (getDebugLvl()) cout << "Cleanup Successful... " << endl;
			releaseTableLocks();
			aTblLockReleased = true;
			break;
		}
		usleep(2000000*fPmCount);
	}

	if((!aTblLockReleased) && (aRollbackSuccess))
	{
		releaseTableLocks();
	}
}

//------------------------------------------------------------------------------


void WESDHandler::onHandlingSigHup()
{
	std::stringstream aStrStr;
	char aDefCon[16], aRedCol[16];
	snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
	snprintf(aRedCol, sizeof(aRedCol), "\033[0;31m");
	aStrStr <<aRedCol<<"Interrupt received .... Program exiting."<<aDefCon;
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr.str(), MSGLVL_INFO1 );

	logging::Message::Args errMsgArgs;
	errMsgArgs.add(aStrStr.str());
	fRef.fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);


	fRef.fSigHup = false;
	//onCpimportFail(0, true);  - cancelOutstandingCpimports will hang on send()
	//cancelOutstandingCpimports();
	exit(1);	// Hard exit on SIGHUP signal

}

//------------------------------------------------------------------------------

void WESDHandler::onDisconnectFailure()
{
	string aStr("Trying to reconnect and rollback");
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStr, MSGLVL_INFO1 );

	for (int aSec = 0; aSec < 15; aSec++ )
	{
		bool aDisconnect = false;
		usleep(1000000);
		for (int PmId = 1; PmId <= fPmCount; ++PmId)
		{
			if (fWeSplClients[PmId] != 0)
			{
				if (!fWeSplClients[PmId]->isConnected())
				{
					aDisconnect = true;
					try
					{
						fWeSplClients[PmId]->setup();
					}
					catch(runtime_error&)
					{
						cout << "Unable to connect to PM" <<
								PmId <<"; Trying again..."<< endl;
					}
				}
			}
		}
		if(!aDisconnect) break;
	}

	doRollback();

	bool aTblLockReleased = false;
	bool aRollbackSuccess = true;
	//BUG 4649  - Some systems taking too long to finish the process.
	//				So we have to wait some more time.
	std::stringstream aStrStr2;
	aStrStr2 << "Rolling back ..........";
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr2.str(), MSGLVL_INFO1 );
	for(int aIdx=0; aIdx<10; aIdx++)
	{
		int aStatus = check4RollbackRslts();
		if(1==aStatus)
		{
			if (getDebugLvl()) cout << "Rollback Successful... " << endl;
			break;
		}
		else if(-1 == aStatus)
		{
			if (getDebugLvl()) cout << "Rollback Failed... " << endl;
			aRollbackSuccess = false;
			break;
		}
		usleep(2000000*fPmCount);
	}

	std::stringstream aStrStr3;
	aStrStr3 << "Cleaning up ..........";
	if ( fRef.fCmdArgs.getConsoleOutput())
		fLog.logMsg( aStrStr3.str(), MSGLVL_INFO1 );
	for(int aIdx=0; aIdx<10; aIdx++)
	{
		int aStatus = check4CleanupRslts();
		if(aStatus == 1)
		{
			if (getDebugLvl()) cout << "Cleanup Successful... " << endl;
			if(aRollbackSuccess)
			{
				releaseTableLocks();
				aTblLockReleased = true;
			}
			break;
		}
		usleep(2000000*fPmCount);
	}

	if((!aTblLockReleased) && (aRollbackSuccess))
	{
		releaseTableLocks();
	}

}

//------------------------------------------------------------------------------

void WESDHandler::setDisconnectFailure(bool Flag)
{
	if(fFileReadThread.isContinue())    //check already stopped running
	{
		sendEODMsg();
		fFileReadThread.shutdown();
	}
	fDisconnectFailure = Flag;
	fRef.onSigInterrupt(1);     // process altogether is a failure
}

//------------------------------------------------------------------------------

} /* namespace WriteEngine */

