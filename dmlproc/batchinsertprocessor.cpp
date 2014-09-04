/*
  Copyright (C) 2009-2012 Calpont Corporation.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

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

#include "we_clients.h"
#include "we_messages.h"
using namespace WriteEngine;

#include "dmlpackageprocessor.h"

#include "dbrm.h"
using namespace BRM;

#include "batchloader.h"
using namespace batchloader;

#include "calpontsystemcatalog.h"
using namespace execplan;

using namespace messageqcpp;
boost::mutex mute;
boost::condition_variable cond;
boost::mutex fLock;
namespace {
using namespace dmlprocessor;
class BatchInsertThread
{
public:
	BatchInsertThread(execplan::CalpontSystemCatalog::SCN txnid, uint32_t tableOid) : fTxnid(txnid), fTableOid(tableOid) {}

	void operator()()
	{
		fWEClient = WriteEngine::WEClients::instance(WriteEngine::WEClients::DMLPROC);
		fBatchInsertProc = BatchInsertProc::makeBatchInsertProc(fTxnid, fTableOid);
		OamCache * oamcache = OamCache::makeOamCache();
		std::vector<int> moduleIds = oamcache->getModuleIds();
		//@Bug 4495 check PM status first
		oam::Oam oam;
		int rc = 0;
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
		for (unsigned i=0; i < moduleIds.size(); i++)
		{	
			int opState = 0;
			bool aDegraded = false;
			ostringstream aOss;
			aOss << "pm" << moduleIds[i];
			std::string aModName = aOss.str();
			try
			{
				oam.getModuleStatus(aModName, opState, aDegraded);
			}
			catch(std::exception& ex)
			{
				ostringstream oss;
				oss << "Exception on getModuleStatus on module ";
				oss <<	aModName;
				oss <<  ":  ";
				oss <<  ex.what();
				rc = 1;
				fBatchInsertProc->setError(rc, oss.str());
			}

			if(opState == oam::ACTIVE )
			{
				fPMs.push_back((uint)moduleIds[i]);
			}
		}
#else
		for (unsigned i=0; i < moduleIds.size(); i++)
		{
			fPMs.push_back((uint)moduleIds[i]);
		}
#endif		
		//Look up extent map to decide which dbroot to start
		DBRM dbrm;
		boost::scoped_ptr<BatchLoader> aBatchloader( new BatchLoader(fTableOid, fTxnid, fPMs) );
		uint firstPmId = 0;
		bool startFromNextPM = false;
		string errorMsg;
		try {
			rc = aBatchloader->selectFirstPM (firstPmId, startFromNextPM);
		}
		catch (std::exception& ex)
		{
			rc = 1;
			fBatchInsertProc->setError(rc, ex.what());
			return;
		}
		if ( rc ==0 )
		{
			if (startFromNextPM)
			{
				aBatchloader->prepareForSecondPM();
				firstPmId = aBatchloader->selectNextPM();
			}
			if (firstPmId == 0)
			{
				aBatchloader->prepareForSecondPM();	
				firstPmId = aBatchloader->selectNextPM();
			}
		}
		
		if (firstPmId == 0)
		{
			rc = 1;
			fBatchInsertProc->setError(rc, "Request cannot be sent to PM 0.");
			return;
		}
		aBatchloader->prepareForSecondPM();	
		uint64_t uniqueId = dbrm.getUnique64(); 
		fWEClient->addQueue(uniqueId);

		
		ByteStream bs1;
		bs1 << (ByteStream::byte) WE_SVR_BATCH_INSERT;
		bs1 << uniqueId;
		bs1 << (ByteStream::quadbyte) fTxnid;
		ByteStream bs2;
		fLastPkg = false;
		bool isAutoCommitOn = true;
		uint32_t tableOid;
		boost::shared_ptr<messageqcpp::ByteStream> bsIn;
		bsIn.reset(new ByteStream());
		ByteStream::byte tmp8;
		ByteStream::quadbyte tmp32;
		std::map<unsigned, bool> pmState;
		
		for (unsigned i=0; i<fPMs.size(); i++)
		{
			pmState[fPMs[i]] = true;
		}
		
		typedef std::vector<BRM::BulkSetHWMArg> BulkSetHWMArgs;
		std::vector<BulkSetHWMArgs> hwmArgsAllPms;
		fCurrentPMid = firstPmId;
		try {
		while (1)
		{
			boost::unique_lock<boost::mutex> lock(mute);
			
			while ((fBatchInsertProc->getInsertQueue()->size()) == 0)
			{
				//cout << "batchinsertprocessor is waiting for work ... " << endl;
				cond.wait(lock);
			}
			//send pkg to pm
			
			//cout << "Before loop, current queue size = " << fBatchInsertProc->getInsertQueue()->size() << endl;
			unsigned int quesize = fBatchInsertProc->getInsertQueue()->size();
			for (unsigned i=0; i < quesize; i++)
			{
				//find the dbroot and the residing pm to sent the package to
				//cout << "Inside loop, current queue size = " << fBatchInsertProc->getInsertQueue()->size() << " and i is " << i << endl;
				bs2.reset();
				bs2 = bs1;
				bs2 << (ByteStream::quadbyte) fCurrentPMid; //to keep track of PMs
				bs2 += fBatchInsertProc->getPkg(fLastPkg, isAutoCommitOn, tableOid);
				//cout << " getPkg fLastPkg is " << fLastPkg << endl;
				//send to all dbroots
				if (pmState[fCurrentPMid]) {
					//cout << "current pm state for pm is true for pm " << fDbrootPm[fCurrentDbroot]<< endl;	
					fWEClient->write(bs2, fCurrentPMid);
					//cout << "batchinsertprocessor sent pkg to pmnum  " << fCurrentPMid << endl;
					pmState[fCurrentPMid] = false;
					fCurrentPMid = aBatchloader->selectNextPM();
					//cout << "fCurrentPMid changed to " << fCurrentPMid << endl;
					//cout << "set pm state to false for pm " << fDbrootPm[fCurrentDbroot] << endl;
				}
				else {
					//cout << "current pm state for pm is false for pm " << fCurrentPMid<< endl;
					while (1) 
					{
						//cout << "Read from WES for pm id " << (uint) tmp32 << endl;
						fWEClient->read(uniqueId, bsIn);
						if ( bsIn->length() == 0 ) //read error
						{
							fBatchInsertProc->setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
							//fWEClient->removeQueue(uniqueId);
						break;
						}			
						else {
							*bsIn >> tmp8;
							rc = tmp8;
							*bsIn >> errorMsg;
							*bsIn >> tmp32;
							//cout << "got response from WES for pm id " << (uint) tmp32 << endl;
							if (rc != 0) {
								//cout << "Batch insert got error code:errormsg = " << (uint)tmp8<<":"<<errorMsg<<endl;
								fBatchInsertProc->setError(rc, errorMsg);
								pmState[tmp32] = true;
								while(!fBatchInsertProc->getInsertQueue()->empty()) fBatchInsertProc->getInsertQueue()->pop();								
								cond.notify_one();
								//fWEClient->removeQueue(uniqueId);
								break;
							}
						}
						pmState[tmp32] = true;
						//cout << "set pm state to true for pm " << (uint) tmp32 << " and current PM id is " << fCurrentPMid << endl;
						if (tmp32 == fCurrentPMid)
							break;
					}
					if (rc != 0)
						break;
					//cout << "before batchinsertprocessor sent pkg to pm " << fCurrentPMid  << endl;	
					fWEClient->write(bs2, fCurrentPMid);
					//cout << "batchinsertprocessor sent pkg to pm " << fCurrentPMid  << endl;
					pmState[fCurrentPMid] = false;
					fCurrentPMid = aBatchloader->selectNextPM();
					//cout << "fCurrentPMid changed to " << fCurrentPMid << endl;
				}		
				//send to all PMs to set hwm if this is the last pkg
				//cout << "fLastPkg is now " << fLastPkg << endl;
				if (fLastPkg && (fBatchInsertProc->getInsertQueue()->size() == 0) )
				{
					//receive message first
					//check whether need to wait for message
					uint meesageNotReceved = 0;
					for (unsigned i=0; i<fPMs.size(); i++)
					{					
//cout << "pmState of pm i:state = " << fPMs[i] << ":"<< pmState[fPMs[i]]<< endl;						
						if (!pmState[fPMs[i]])
							meesageNotReceved++;						
					}
					//cout << " message not received " << meesageNotReceved << endl;
					if (meesageNotReceved > 0) {
						uint messageRcvd = 0;
						while (1)
						{
							if (meesageNotReceved == messageRcvd)
								break;
								
							fWEClient->read(uniqueId, bsIn);
							if ( bsIn->length() == 0 ) //read error
							{
								fBatchInsertProc->setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
								//fWEClient->removeQueue(uniqueId);
								messageRcvd++;
							}			
							else {
								*bsIn >> tmp8;
								rc = tmp8;
								*bsIn >> errorMsg;
								*bsIn >> tmp32;
								messageRcvd++;
								//cout << "got response from WES for pm id " << (uint) tmp32 << endl;
							if (rc == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
							{
								fBatchInsertProc->setError(rc, errorMsg);
							}
							else if (rc != 0) {
									//cout << "Batch insert got error code:errormsg = " << rc<<":"<<errorMsg<<endl;
									fBatchInsertProc->setError(rc, errorMsg);
									pmState[tmp32] = true;
									//cout << "set pm state to true for pm " << (uint) tmp32  << endl;
									//fWEClient->removeQueue(uniqueId);
									break;
								}
								pmState[tmp32] = true;
								//cout << "set pm state to true for pm " << (uint) tmp32  << endl;
							}
						}
					}
					//cout << "out of for loop and rc is " << rc<<endl;
					if (rc != 0)
					{
						//cout << "break" << endl;
						break;
					} 
					bs1.reset();
					bs1 << (ByteStream::byte) WE_SVR_BATCH_INSERT_END;
					bs1 << uniqueId;
					bs1 << (ByteStream::quadbyte) fTxnid;
					bs1 << (ByteStream::byte)isAutoCommitOn;
					bs1 << tableOid;
					bs1 << (ByteStream::byte) rc;
					//cout << "sending the last pkg" << endl;
					fWEClient->write_to_all(bs1);
					//cout << "sent the last pkg" << endl;
					uint msgRevd = 0;
					while (1)
					{
						if (msgRevd == fWEClient->getPmCount())
							break;
							
						//cout << "Read last from WES bytestream" << endl;
						fWEClient->read(uniqueId, bsIn);
						if ( bsIn->length() == 0 ) //read error
						{
							fBatchInsertProc->setError(dmlpackageprocessor::DMLPackageProcessor::NETWORK_ERROR, errorMsg);
							//fWEClient->removeQueue(uniqueId);
						break;
						}			
						else {
							*bsIn >> tmp8;
							rc = tmp8;
							*bsIn >> errorMsg;
							msgRevd++;
							if (!isAutoCommitOn) //collect Hwm
							{
								BulkSetHWMArgs setHWMArgs;
								//cout << "received from WES bytestream length = " <<  bsIn->length() << endl;
								deserializeInlineVector(*(bsIn.get()), setHWMArgs);
								//cout << "get hwm info from WES size " << setHWMArgs.size() << endl;
								hwmArgsAllPms.push_back(setHWMArgs);
							}
							if (rc == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING)
							{
								fBatchInsertProc->setError(rc, errorMsg);
							}
							else if (rc != 0) {
								//cout << "Batch insert lastpkg got error code:errormsg = " << (uint)tmp8<<":"<<errorMsg<<endl;
								fBatchInsertProc->setError(rc, errorMsg);
								//fWEClient->removeQueue(uniqueId);
								break;
							}
						}
					}
					//cout << " received response for last batch" << endl;
					if (!isAutoCommitOn)
					{
						//set hwm
						std::vector<BRM::BulkSetHWMArg> allHwm;
						BulkSetHWMArgs::const_iterator itor;
						//cout << "total hwmArgsAllPms size " << hwmArgsAllPms.size() << endl;
						for (unsigned i=0; i  < fWEClient->getPmCount(); i++)
						{
							itor = hwmArgsAllPms[i].begin();
							while (itor != hwmArgsAllPms[i].end())
							{
								allHwm.push_back(*itor);
								//cout << "received hwm info: " <<  itor->oid << ":" << itor->hwm << endl;
								itor++;
							}
						}
	
						if (allHwm.size() > 0)
						{
							//cout << "setting hwm allHwm size " << allHwm.size() << endl;
							scoped_ptr<DBRM> dbrmp(new DBRM());
							rc = dbrmp->bulkSetHWM(allHwm, 0);
						}
					}
					break;
					
				}
			}
				
			//@Bug 4419. need to recive all messages before returning to connector in error
			if (rc != 0)
			{
				//receive message first
				//check whether need to wait for message
				//cout << "error occured." << endl;
				uint messagesNotReceived = 0;
				for (unsigned i=0; i<fPMs.size(); i++)
				{
					if (!pmState[fPMs[i]])
						messagesNotReceived++;
				}
			
				uint msgReceived = 0;
				if (messagesNotReceived > 0)
				{
					//cout << "Batchinsertprocessor erroring out and need to receive messages " << messagesNotReceived << endl;
					while (1)
					{
						if (msgReceived == messagesNotReceived)
							break;
				
						fWEClient->read(uniqueId, bsIn);
						if ( bsIn->length() == 0 ) //read error
						{
							msgReceived++;
						}			
						else {
							*bsIn >> tmp8;
							*bsIn >> errorMsg;
							*bsIn >> tmp32;
							msgReceived++;
						}
					}
				}
				//cout << "Batchinsertprocessor erroring out and received all messages " << endl;
				bs1.reset();
				bs1 << (ByteStream::byte) WE_SVR_BATCH_INSERT_END;
				bs1 << uniqueId;
				bs1 << (ByteStream::quadbyte) fTxnid;
				bs1 << (ByteStream::byte)isAutoCommitOn;
				bs1 << tableOid;
				bs1 << (ByteStream::byte) rc;
					//cout << "sending the last pkg" << endl;
				fWEClient->write_to_all(bs1);
					//cout << "sent the last pkg" << endl;
				msgReceived = 0;
				while (1)
				{
					if (msgReceived == fWEClient->getPmCount())
						break;
							
					fWEClient->read(uniqueId, bsIn);
					if ( bsIn->length() == 0 ) //read error
					{
						msgReceived++;
					}			
					else {
						*bsIn >> tmp8;
						*bsIn >> errorMsg;
						msgReceived++;
						if (!isAutoCommitOn) //collect Hwm
						{
							BulkSetHWMArgs setHWMArgs;
								//cout << "received from WES bytestream length = " <<  bsIn->length() << endl;
							deserializeInlineVector(*(bsIn.get()), setHWMArgs);
								//cout << "get hwm info from WES size " << setHWMArgs.size() << endl;
							hwmArgsAllPms.push_back(setHWMArgs);
						}
					}
				}
				//cout << "received all response before error out " << endl;
				if (!isAutoCommitOn)
				{
					//set hwm
					std::vector<BRM::BulkSetHWMArg> allHwm;
					BulkSetHWMArgs::const_iterator itor;
						//cout << "total hwmArgsAllPms size " << hwmArgsAllPms.size() << endl;
					for (unsigned i=0; i  < fWEClient->getPmCount(); i++)
					{
						itor = hwmArgsAllPms[i].begin();
						while (itor != hwmArgsAllPms[i].end())
						{
							allHwm.push_back(*itor);
								//cout << "received hwm info: " <<  itor->oid << ":" << itor->hwm << endl;
							itor++;
						}
					}
	
					if (allHwm.size() > 0)
					{
							//cout << "setting hwm allHwm size " << allHwm.size() << endl;
						scoped_ptr<DBRM> dbrmp(new DBRM());
						rc = dbrmp->bulkSetHWM(allHwm, 0);
					}
				}
				break;
			}
			//@Bug 4530. Need check queue size to prevent prematurally terminate the process.
			if (fLastPkg && (fBatchInsertProc->getInsertQueue()->size() == 0) )
				break;
			//Notify receive pkg
			//cout << "notifying dmlprocessor" << endl;
			cond.notify_one();		
		}	
		}
		catch (std::exception& ex)
		{
			ostringstream oss;
			oss << "Exception on communicating to WES ";
			oss << ex.what();
			rc = 1;
			fBatchInsertProc->setError(rc, oss.str());
			cout << oss.str() << endl;
		}
		fWEClient->removeQueue(uniqueId);
	}


private:
	
	std::vector<uint> fPMs;
	WriteEngine::WEClients* fWEClient;
	std::vector<uint> fDbRoots;
	std::map<uint, uint > fDbrootPm; //  key - dbroot. 
	BatchInsertProc* fBatchInsertProc;
	uint fStartingDbroot;
	uint fCurrentPMid;
	execplan::CalpontSystemCatalog::SCN fTxnid;
	bool fLastPkg;
	uint32_t fTableOid;
};
}

namespace dmlprocessor 
{
/*static*/
BatchInsertProc* BatchInsertProc::fInstance;
uint BatchInsertProc::fNumDBRoots;
uint32_t BatchInsertProc::fTableOid;
boost::thread* BatchInsertProc::fProcessorThread;
execplan::CalpontSystemCatalog::SCN BatchInsertProc::fTxnid;
int BatchInsertProc::fErrorCode;
std::string BatchInsertProc::fErrMsg; 
BatchInsertProc * BatchInsertProc::makeBatchInsertProc(execplan::CalpontSystemCatalog::SCN txnid, uint32_t tableOid)
{
   if (fInstance)
		return fInstance;
   
    fInstance = new BatchInsertProc();	
	joblist::ResourceManager rm;
	fNumDBRoots = rm.getDBRootCount();
	fTxnid = txnid;
	fTableOid = tableOid;
	fErrorCode = 0;
	fErrMsg = "";
	BatchInsertThread batchInsertThread(fTxnid, fTableOid);
	fProcessorThread = new boost::thread(batchInsertThread);
	return fInstance;
}

void BatchInsertProc::removeBatchInsertProc(int & rc, string & errMsg)
{
	fProcessorThread->join();
	delete fProcessorThread;
	fProcessorThread = NULL;
	rc = fErrorCode;
	//cout << "fErrorCode is " << fErrorCode << endl;
	errMsg = fErrMsg;
	
	if (fInstance)
		delete fInstance;
	fInstance = 0;
}

BatchInsertProc::BatchInsertProc():fInsertPkgQueue(new pkg_type)
{
	
}

BatchInsertProc::~BatchInsertProc()
{
}


BatchInsertProc::SP_PKG  BatchInsertProc::getInsertQueue()
{	
	mutex::scoped_lock lk(fLock);
	return fInsertPkgQueue;
}

uint BatchInsertProc::getNumDBRoots()
{
	return fNumDBRoots;
}

void BatchInsertProc::addPkg(messageqcpp::ByteStream& insertBs, bool lastpkg, bool isAutocommitOn, uint32_t tableOid)
{
	mutex::scoped_lock lk(fLock);
	fInsertPkgQueue->push(insertBs);
	fLastpkg = lastpkg;
	//cout << "set fLastpkg to " << fLastpkg << endl;
	fIsAutocommitOn = isAutocommitOn;
}

messageqcpp::ByteStream BatchInsertProc::getPkg(bool & lastPkg, bool & isAutoCommitOn, uint32_t & tableOid)
{
	messageqcpp::ByteStream bs;
	mutex::scoped_lock lk(fLock);
	bs = fInsertPkgQueue->front();
	fInsertPkgQueue->pop();
	lastPkg = fLastpkg;
	//cout << "get fLastpkg is " << fLastpkg << endl;
	isAutoCommitOn = fIsAutocommitOn;
	tableOid = fTableOid;
	return bs;
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
