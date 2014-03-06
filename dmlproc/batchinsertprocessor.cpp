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

// $Id: batchinsertprocessor.h 525 2010-01-19 23:18:05Z xlou $
//
/** @file */

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;
using namespace std;
#include "liboamcpp.h"
using namespace oam;

#include "batchinsertprocessor.h"
using namespace dmlprocessor;

#include "we_messages.h"
using namespace WriteEngine;

#include "dmlpackageprocessor.h"
using namespace batchloader;

#include "calpontsystemcatalog.h"
using namespace execplan;
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "dbrm.h"
using namespace BRM;
using namespace messageqcpp;
boost::mutex mute;
boost::condition_variable cond;
boost::mutex fLock;

namespace dmlprocessor 
{

BatchInsertProc::BatchInsertProc(bool isAutocommitOn, uint32_t tableOid, execplan::CalpontSystemCatalog::SCN txnId, BRM::DBRM* aDbrm) :
fTxnid(txnId), fIsAutocommitOn(isAutocommitOn), fTableOid(tableOid), fDbrm(aDbrm)
{
	fErrorCode = 0;
	fErrMsg = "";
	fLastpkg = false;
	fUniqueId = fDbrm->getUnique64(); 
	fWEClient = new WriteEngine::WEClients(WriteEngine::WEClients::DMLPROC);
	fWEClient->addQueue(fUniqueId);
	fOamcache = OamCache::makeOamCache();
	std::vector<int> moduleIds = fOamcache->getModuleIds();
	//cout << "moduleIds size is " << moduleIds.size() << endl;
	for (unsigned i=0; i < moduleIds.size(); i++)
	{	
		fPMs.push_back((uint32_t)moduleIds[i]);
		//cout << "got module id " << (uint32_t)moduleIds[i] << endl;
		//cout << "fPMs[0] is " << fPMs[0] << endl;
	}
	fBatchLoader = new BatchLoader(fTableOid, fTxnid, fPMs);
	for (unsigned i=0; i<fPMs.size(); i++)
	{
		fPmState[fPMs[i]] = true;
	}
	//cout << "Constructor: There are " << (int)fPMs.size() << " pms" << endl;
	fInsertPkgQueue.reset(new pkg_type);
}

BatchInsertProc::~BatchInsertProc()
{	
	fWEClient->removeQueue(fUniqueId);
	delete fWEClient;
}

uint64_t BatchInsertProc::grabTableLock(int32_t sessionId)
{
	uint32_t  processID = ::getpid();
	int32_t txnId = fTxnid;
	std::string  processName("DMLProc batchinsert");
	int32_t tmpSessionId = sessionId;
	uint32_t tableOid = fTableOid;
	int i = 0;		
	try {
	//cout << "In grabTableLock, fPMs[0] is "<< fPMs[0] << endl;
		fTableLockid = fDbrm->getTableLock(fPMs, tableOid, &processName, &processID, &tmpSessionId, &txnId, BRM::LOADING );
	}
	catch (std::exception&)
	{
		setError(dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR , IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
		return 0;
	}
		
	if ( fTableLockid  == 0 )
	{
		int waitPeriod = 10;
		int sleepTime = 100; // sleep 100 milliseconds between checks
		int numTries = 10;  // try 10 times per second
		string waitPeriodStr = config::Config::makeConfig()->getConfig("SystemConfig", "WaitPeriod");
		if ( waitPeriodStr.length() != 0 )
			waitPeriod = static_cast<int>(config::Config::fromText(waitPeriodStr));
								
		numTries = 	waitPeriod * 10;
		struct timespec rm_ts;

		rm_ts.tv_sec = sleepTime/1000;
		rm_ts.tv_nsec = sleepTime%1000 *1000000;

		for (; i < numTries; i++)
		{
#ifdef _MSC_VER
			Sleep(rm_ts.tv_sec * 1000);
#else
			struct timespec abs_ts;
			do
			{
				abs_ts.tv_sec = rm_ts.tv_sec;
				abs_ts.tv_nsec = rm_ts.tv_nsec;
			}
			while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif
			try 
			{
				processID = ::getpid();
				txnId = fTxnid;
				tmpSessionId = sessionId;
				processName = "DMLProc batchinsert";
				fTableLockid = fDbrm->getTableLock(fPMs, tableOid, &processName, &processID, &tmpSessionId, &txnId, BRM::LOADING );
			}
			catch (std::exception&)
			{
				setError(dmlpackageprocessor::DMLPackageProcessor::INSERT_ERROR , IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
				return 0;
			}

			if (fTableLockid > 0)
				return fTableLockid;
		}

		if (i >= numTries) //error out
		{
			logging::Message::Args args;
			args.add(processName);
			args.add((uint64_t)processID);
			args.add(tmpSessionId);
			setError(dmlpackageprocessor::DMLPackageProcessor::TABLE_LOCK_ERROR , IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED,args));
			return 0;										
		}
	}
	return fTableLockid;
}

BatchInsertProc::SP_PKG  BatchInsertProc::getInsertQueue()
{	
	mutex::scoped_lock lk(fLock);
	return fInsertPkgQueue;
}

void BatchInsertProc::setLastPkg (bool lastPkg)
{
	fLastpkg = lastPkg;
}
void BatchInsertProc::addPkg(messageqcpp::ByteStream& insertBs)
{
	mutex::scoped_lock lk(fLock);
	fInsertPkgQueue->push(insertBs);
	
}

messageqcpp::ByteStream BatchInsertProc::getPkg()
{
	messageqcpp::ByteStream bs;
	mutex::scoped_lock lk(fLock);
	bs = fInsertPkgQueue->front();
	fInsertPkgQueue->pop();
	return bs;
}

void BatchInsertProc::buildPkg(messageqcpp::ByteStream& bs)
{
	bs.reset();
	bs << (ByteStream::byte) WE_SVR_BATCH_INSERT;
	bs << fUniqueId;
	bs << (uint32_t) fTxnid;
	bs << fCurrentPMid; //to keep track of PMs
	bs += getPkg();
}

void BatchInsertProc::buildLastPkg(messageqcpp::ByteStream& bs)
{
	bs.reset();
	bs << (ByteStream::byte) WE_SVR_BATCH_INSERT_END;
	bs << fUniqueId;
	bs << (ByteStream::quadbyte) fTxnid;
	bs << (ByteStream::byte)fIsAutocommitOn;
	bs << fTableOid;
	bs << (ByteStream::byte) fErrorCode;
}

void BatchInsertProc::sendFirstBatch()
{
	uint32_t firstPmId = 0;
	int rc = 0;
	try {
		firstPmId = fBatchLoader->selectNextPM();
	}
	catch (std::exception& ex)
	{
		rc = 1;
		setError(rc, ex.what());
		return;
	}
	
	if (firstPmId == 0)
	{
		rc = 1;
		setError(rc, "Request cannot be sent to PM 0.");
		return;
	}

	fCurrentPMid = firstPmId;
	messageqcpp::ByteStream bs;
	buildPkg(bs);
	
	fWEClient->write(bs, fCurrentPMid);
	//cout << "in sendFirstBatch: batchinsertprocessor sent pkg to pmnum  " << fCurrentPMid << endl;
	fPmState[fCurrentPMid] = false;
}

void BatchInsertProc::sendNextBatch()
{
	int rc = 0;
	try {
		fCurrentPMid = fBatchLoader->selectNextPM();
	}
	catch (std::exception& ex)
	{
		rc = 1;
		setError(rc, ex.what());
		return;
	}
	messageqcpp::ByteStream bs;
	buildPkg(bs);
	string errorMsg;
	//cout << "in sendNextBatch: fCurrentPMid changed to " << fCurrentPMid << " this = " << this << endl;
	if (fPmState[fCurrentPMid]) {
		//cout << "current pm state for pm is true for pm " << fCurrentPMid << " this = " << this<< endl;	
		fWEClient->write(bs, fCurrentPMid);
		//cout << "batchinsertprocessor sent pkg to pmnum  " << fCurrentPMid << endl;
		fPmState[fCurrentPMid] = false;
		//cout << "set pm state to false for pm " << fCurrentPMid <<  " this = " << this << endl;
	}
	else {
		//cout << "current pm state for pm is false for pm " << fCurrentPMid<< endl;
		while (1) 
		{
		//cout << "Read from WES for pm id " << (uint32_t) tmp32 << endl;
		bsIn.reset(new ByteStream());	
		fWEClient->read(fUniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			errorMsg = "Lost connection to WES.";
			setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);				
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			*bsIn >> errorMsg;
			*bsIn >> tmp32;
			//cout << "got response from WES for pm id " << (uint32_t) tmp32 << endl;
			if (rc != 0) {
				//cout << "Batch insert got error code:errormsg = " << (uint32_t)tmp8<<":"<<errorMsg<<endl;
				setError(rc, errorMsg);
				fPmState[tmp32] = true;
				break;
			}
		}
		fPmState[tmp32] = true;
		//cout << "set pm state to true for pm " << (uint32_t) tmp32 << " and current PM id is " << fCurrentPMid<< " this = " << this << endl;
		if (tmp32 == fCurrentPMid)
			break;
		}
		//cout << "before batchinsertprocessor sent pkg to pm " << fCurrentPMid  << endl;	
		fWEClient->write(bs, fCurrentPMid);
		//cout << "batchinsertprocessor sent pkg to pm " << fCurrentPMid  << endl;
		fPmState[fCurrentPMid] = false;
		//cout << "set pm state to false for pm " << fCurrentPMid <<  " this = " << this << endl;
	}

}

void BatchInsertProc::sendlastBatch()
{
	messageqcpp::ByteStream bs;
	buildLastPkg(bs);
	try {
		fWEClient->write_to_all(bs);
		//cout << "sent the last pkg" << endl;
	}
	catch (std::exception& ex)
	{
		ostringstream oss;
		oss << "Exception on communicating to WES ";
		oss << ex.what();
		setError(1, oss.str());
		//cout << oss.str() << endl;
	}
}

void BatchInsertProc::receiveOutstandingMsg()
{
	//check how many message we need to receive
	uint32_t messagesNotReceived = 0;
	int rc = 0;
	for (unsigned i=0; i<fPMs.size(); i++)
	{
		if (!fPmState[fPMs[i]])
			messagesNotReceived++;
	}
	//cout << "receiveOutstandingMsg: Need to receive " << messagesNotReceived <<  " messages. this = " << this << endl;
	if ((messagesNotReceived > 0) && (messagesNotReceived <= fWEClient->getPmCount()))
	{
		string errorMsg;
		uint32_t msgReceived = 0;

		while (1)
		{
			if (msgReceived == messagesNotReceived)
				break;
				
			bsIn.reset(new ByteStream());
			try {
				fWEClient->read(fUniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					rc = 1;
					setError(rc, errorMsg);
				}			
				else {
					*bsIn >> tmp8;
					*bsIn >> errorMsg;
					*bsIn >> tmp32;
					
					fPmState[tmp32] = true;
					msgReceived++;
					if ( tmp8 !=0 )
						setError(tmp8, errorMsg);
				}
			}
			catch (runtime_error& ex) //write error
			{
				errorMsg = ex.what();
				setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
				break;
			}
			catch (...)
			{
				errorMsg = "Lost connection to WES.";
				setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
				break;
			}
		}
	}

}

void BatchInsertProc::receiveAllMsg()
{
	uint32_t msgRevd = 0;
	int rc = 0;
	string errorMsg;
	try {
		while (1)
		{
			if (msgRevd == fWEClient->getPmCount())
				break;
							
			//cout << "Read last from WES bytestream" << endl;
			fWEClient->read(fUniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				errorMsg = "Lost connection to WES.";
				setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
				msgRevd++;
				if (!fIsAutocommitOn)
				{
					BulkSetHWMArgs setHWMArgs;
					fHwmArgsAllPms.push_back(setHWMArgs);
				}
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				*bsIn >> errorMsg;
				msgRevd++;
				if (!fIsAutocommitOn) //collect Hwm
				{		
					BulkSetHWMArgs setHWMArgs;				
					deserializeInlineVector(*(bsIn.get()), setHWMArgs);
					fHwmArgsAllPms.push_back(setHWMArgs);
					
				}
				if (rc == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
				{
					setError(rc, errorMsg);
				}
				else if (rc != 0) {
				//cout << "Batch insert lastpkg got error code:errormsg = " << (uint32_t)tmp8<<":"<<errorMsg<<endl;
					setError(rc, errorMsg);
				}
			}
		}
	}
	catch (std::exception& ex)
	{
		ostringstream oss;
		oss << "Exception on communicating to WES ";
		oss << ex.what();
		rc = 1;
		setError(rc, oss.str());
	}
	if (!fIsAutocommitOn && (fHwmArgsAllPms.size() == fWEClient->getPmCount())) 
		setHwm();

}

void BatchInsertProc::collectHwm()
{
	BulkSetHWMArgs setHWMArgs;
	//cout << "received from WES bytestream length = " <<  bsIn->length() << endl;
	deserializeInlineVector(*(bsIn.get()), setHWMArgs);
	//cout << "get hwm info from WES size " << setHWMArgs.size() << endl;
	fHwmArgsAllPms.push_back(setHWMArgs);
}

void BatchInsertProc::setHwm()
{
	std::vector<BRM::BulkSetHWMArg> allHwm;
	BulkSetHWMArgs::const_iterator itor;
	//cout << "total hwmArgsAllPms size " << hwmArgsAllPms.size() << endl;
	for (unsigned i=0; i  < fWEClient->getPmCount(); i++)
	{
		itor = fHwmArgsAllPms[i].begin();
		while (itor != fHwmArgsAllPms[i].end())
		{
			allHwm.push_back(*itor);
			//cout << "received hwm info: " <<  itor->oid << ":" << itor->hwm << endl;
			itor++;
		}
	}
	
	if (allHwm.size() > 0)
	{
		//cout << "setting hwm allHwm size " << allHwm.size() << endl;
		int rc = fDbrm->bulkSetHWM(allHwm, 0);
		if ( rc !=0 )
		{
			string errorMsg;
			BRM::errString(rc, errorMsg);
			setError(rc, errorMsg);
		}
	}
}

void BatchInsertProc::setError(int errorCode, std::string errMsg)
{
	mutex::scoped_lock lk(fLock);
	fErrorCode = errorCode;
	fErrMsg = errMsg;
}
void BatchInsertProc::getError(int & errorCode, std::string & errMsg)
{
	mutex::scoped_lock lk(fLock);
	errorCode = fErrorCode;
	errMsg = fErrMsg;
}
}
// vim:ts=4 sw=4:
