/*

   Copyright (C) 2009-2013 Calpont Corporation.

   Use of and access to the Calpont InfiniDB Community software is subject to the
   terms and conditions of the Calpont Open Source License Agreement. Use of and
   access to the Calpont InfiniDB Enterprise software is subject to the terms and
   conditions of the Calpont End User License Agreement.

   This program is distributed in the hope that it will be useful, and unless
   otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
   the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   Please refer to the Calpont Open Source License Agreement and the Calpont End
   User License Agreement for more details.

   You should have received a copy of either the Calpont Open Source License
   Agreement or the Calpont End User License Agreement along with this program; if
   not, it is your responsibility to review the terms and conditions of the proper
   Calpont license agreement by visiting http://www.calpont.com for the Calpont
   InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
   the Calpont InfiniDB Community Calpont Open Source License Agreement.

   Calpont may make changes to these license agreements from time to time. When
   these changes are made, Calpont will make a new copy of the Calpont End User
   License Agreement available at http://www.calpont.com and a new copy of the
   Calpont Open Source License Agreement available at http:///www.infinidb.org.
   You understand and agree that if you use the Program after the date on which
   the license agreement authorizing your use has changed, Calpont will treat your
   use as acceptance of the updated License.

*/

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * we_splclient.cpp
 *
 *  Created on: Oct 20, 2011
 *      Author: bpaul
 */

#include <iosfwd>
#ifndef _MSC_VER
#include <arpa/inet.h>
#else
#include <intrin.h>
#endif
#if __FreeBSD__
#include <sys/socket.h>
#endif


#include <cstdio>
#include <iostream>
#include <string>
using namespace std;

//using namespace std;

#include "errorids.h"
#include "exceptclasses.h"
#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "liboamcpp.h"
using namespace oam;

#include "snmpmanager.h"
using namespace snmpmanager;

#include "we_sdhandler.h"
#include "we_splclient.h"

//------------------------------------------------------------------------------
/*
void  writeToLog(const char* file, int line, const string& msg, LOG_TYPE logto = LOG_TYPE_INFO)
{
       LoggingID lid(05);
       MessageLog ml(lid);
       Message::Args args;
       Message m(0);
       args.add(file);
       args.add("@");
       args.add(line);
       args.add(msg);
       m.format(args);
	switch (logto)
	{
       	case LOG_TYPE_DEBUG:	ml.logDebugMessage(m); break;
       	case LOG_TYPE_INFO: 	ml.logInfoMessage(m); break;
       	case LOG_TYPE_WARNING:	ml.logWarningMessage(m); break;
       	case LOG_TYPE_ERROR:	ml.logWarningMessage(m); break;
       	case LOG_TYPE_CRITICAL:	ml.logCriticalMessage(m); break;
	}
}
*/
#ifdef _MSC_VER
mutex inet_ntoa_mutex;
#endif


namespace WriteEngine
{

//------------------------------------------------------------------------------
//BP 10/24/2011 14:25
//------------------------------------------------------------------------------
void WESplClientRunner::operator()()
{
	fOwner.sendAndRecv();
}

//------------------------------------------------------------------------------
//BP 10/24/2011 14:25
//------------------------------------------------------------------------------


WESplClient::WESplClient(WESDHandler& Sdh, int PmId):
	    fContinue(true),
	    fConnected(false),
	    fPmId(PmId),
	    fDbrCnt(0),
	    fDbrVar(0),
	    fDataRqstCnt(0),
	    fRdSecTo(0),
	    fRowTx(0),
	    fBytesTx(0),
	    fBytesRcv(0),
	    fLastInTime(0),
	    fStartTime(time(0)),
	    fSend(true),
	    fCpiStarted(false),
	    fCpiPassed(false),
	    fCpiFailed(false),
	    fBrmRptRcvd(false),
	    fRollbackRslt(0),
	    fCleanupRslt(0),
	    fServer(),
	    fClnt(),
	    fpThread(0),
	    fOwner(Sdh)
{
	// TODO ctor
}
//------------------------------------------------------------------------------
WESplClient::~WESplClient()
{
	delete fpThread;
	fpThread=0;
}
//------------------------------------------------------------------------------
void WESplClient::setup()
{
	// do the setup stuffs here
	if(fOwner.getDebugLvl())
		cout << "setting connection to moduleid " << getPmId() << endl;

	char buff[32];
	snprintf(buff, sizeof(buff), "pm%u_WriteEngineServer", getPmId());
	fServer = buff;

	fClnt.reset(new MessageQueueClient(fServer));

	if(fOwner.getDebugLvl())
		cout << "WEServer : " << fServer << " " << fClnt->addr2String() <<endl;

	try
	{
		if (fClnt->connect())
		{
			onConnect();
			startClientThread();
		}
		else
		{
			throw runtime_error("Connection refused");
		}
	} catch (std::exception& ex)
	{
		cerr << "Could not connect to " << fServer << ": " << ex.what() << endl;
	} catch (...)
	{
		cerr << "Could not connect to " << fServer  << endl;
		throw runtime_error("Problem in connecting to PM");
	}


}
//------------------------------------------------------------------------------
void WESplClient::startClientThread()
{
    this->fpThread = new boost::thread(WESplClientRunner(*this));
}
//------------------------------------------------------------------------------
void WESplClient::sendAndRecv()
{
	while(fContinue)
	{
		try
		{
		// Send messages if out queue has something
		send();
		// Recv messages if there is something in socket or timeout
		recv();
		}
		catch (runtime_error&)
		{
			//setCpiFailed(true); - done in onDisconnect() BUG
			setConnected(false);
			if(fOwner.getDebugLvl())
				cout <<"Disconnect from PM - " << getPmId() << endl;
			onDisconnect();
		}
	}

	if(this->fCpiFailed)
	{
		// NOTE : commented out to avoid sending rollback twice.
		//fOwner.onCpimportFail(this->getPmId());
		char aDefCon[16], aRedCol[16];
		snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
		snprintf(aRedCol, sizeof(aRedCol), "\033[0;31m");
		if(fOwner.getDebugLvl())
			cout << aRedCol << "Bulk load FAILED on PM "
							<< getPmId()<< aDefCon << endl;
	}
	else if(this->fCpiPassed)
	{
		//if(fOwner.getDebugLvl())
		//BUG 4195
		char aDefCon[16], aGreenCol[16];
		snprintf(aDefCon, sizeof(aDefCon), "\033[0m");
		snprintf(aGreenCol, sizeof(aGreenCol), "\033[0;32m");
		if(fOwner.getDebugLvl())
			cout << aGreenCol << "Bulk load Finished Successfully on PM "
							  << getPmId()<< aDefCon << endl;
	}
	else if(!this->fCpiStarted)
	{
		if(fOwner.getDebugLvl())
			cout << "Cpimport Failed to Start!!!"<< this->getPmId() << endl;
	}
}
//------------------------------------------------------------------------------
void WESplClient::send()
{
	//TODO - may have to move the aLock here.
	//if ((!fSendQueue.empty())&&(getSendFlag()))

	//if (!fSendQueue.empty())
	// Check enough DataRqst to send Data


	if ((!fSendQueue.empty())&&(getDataRqstCount()>0))
	{
		if(fOwner.getDebugLvl()>2)
			cout << "DataRqstCnt [" << getPmId() << "] = "
									<< getDataRqstCount() << endl;
		//setSendFlag(false);
		mutex::scoped_lock aLock(fSentQMutex);
		messageqcpp::SBS aSbs = fSendQueue.front();
		fSendQueue.pop();
		aLock.unlock();
		int aLen = (*aSbs).length();
		if (aLen > 0)
		{
			mutex::scoped_lock aLock(fWriteMutex);
			setBytesTx(getBytesTx() + aLen);
			fClnt->write(aSbs);
			aLock.unlock();
		}

		decDataRqstCount();
		//decDbRootVar();
	}
	//setSendFlag(fOwner.check4Ack(fPmId));

}
//------------------------------------------------------------------------------
void WESplClient::recv()
{
	messageqcpp::SBS aSbs;
    struct timespec rm_ts;
    rm_ts.tv_sec = fRdSecTo;			//0 when data sending otherwise 1- second
    rm_ts.tv_nsec = 20000000;			// 20 milliSec
    bool isTimeOut = false;

    try
    {
    	aSbs = fClnt->read(&rm_ts, &isTimeOut);
    }
    catch (std::exception& ex)
    {
    	setConnected(false);
    	cout << ex.what() <<endl;
    	cout << "fClnt read error on " << getPmId() << endl;
    	throw runtime_error("fClnt read error");
    }
	// - aSbs->length()>0 add to the sdh.fWesMsgQueue
	int aLen = aSbs->length();

	if(aLen > 0)
	{
		//setLastInTime(time(0));	BUG 4195
		setLastInTime(time(0));  //added back for BUG 4535
		setBytesRcv( getBytesRcv()+ aLen);
		fOwner.add2RespQueue(aSbs);
	}
	else if ((aLen <= 0) && (!isTimeOut))	//disconnect
	{
		cout <<"Disconnect from PM - " << getPmId() << " IP " << endl;
		onDisconnect();
	}

}
//------------------------------------------------------------------------------
void WESplClient::write(const messageqcpp::ByteStream& Msg)
{
	//mutex::scoped_lock aLock(fWriteMutex);
	setBytesTx(getBytesTx() + Msg.length());
	fClnt->write(Msg);
	//aLock.unlock();
}
//------------------------------------------------------------------------------
void WESplClient::read(messageqcpp::SBS& Sbs)
{
	// read from the WEServerMsgQueue
	// if Key is needed give that constant here
}
//------------------------------------------------------------------------------
//TODO - We may need to make it much more efficient by incorporating file read
void WESplClient::add2SendQueue(const messageqcpp::SBS& Sbs)
{
	//mutex::scoped_lock aLock(fSentQMutex);
	this->fSendQueue.push(Sbs);
	//aLock.unlock();
}


void WESplClient::clearSendQueue()
{
	mutex::scoped_lock aLock(fSentQMutex);
	while(!fSendQueue.empty())
		fSendQueue.pop();
	aLock.unlock();
}

int WESplClient::getSendQSize()
{
	int aQSize=0;
	mutex::scoped_lock aLock(fSentQMutex);
	aQSize = fSendQueue.size();
	aLock.unlock();
	return aQSize;
}


//------------------------------------------------------------------------------
void WESplClient::printStats()
{
	if(fOwner.getDebugLvl())
	{
		cout <<"\tPMid      \t"<<getPmId()<<endl;
		cout <<"\tTx Rows   \t"<<getRowTx()<<endl;
		//if(fOwner.getDebugLvl())
		cout <<"\tTx Bytes  \t"<<getBytesTx()<<endl;
		//if(fOwner.getDebugLvl())
		cout <<"\tRcv Bytes \t"<<getBytesRcv()<<endl;
		cout <<"\tInserted/Read Rows   "<<fRowsUploadInfo.fRowsInserted<<"/"
										<<fRowsUploadInfo.fRowsRead<< endl;
		if(fColOorVec.size()>0)
			cout <<"\tCol Id\tColName\t\t\tout-of-range count" <<endl;
		WEColOorVec::iterator aIt = fColOorVec.begin();
		while(aIt != fColOorVec.end())
		{
			cout <<"\t"<<(*aIt).fColNum <<"\t"<<(*aIt).fColName <<"\t\t" << (*aIt).fNoOfOORs <<endl;
			aIt++;
		}
		if(!fBadDataFile.empty())cout<<"\tBad Data Filename    "<<fBadDataFile<<endl;
		if(!fErrInfoFile.empty())cout<<"\tError Filename       "<<fErrInfoFile<<endl;
		cout <<"\t("<<getLastInTime()-getStartTime()<<"sec)"<<endl;
		cout <<"\t"<<endl;
	}
}
//------------------------------------------------------------------------------

void WESplClient::onConnect()
{
	//TODO
	// update all the flags on Connect.
	// alert data can be send now
	// do not allow to connect back again.

	setConnected(true);
	ByteStream bsWrite;
	bsWrite << (ByteStream::byte) WE_CLT_SRV_KEEPALIVE;
	this->write(bsWrite);				// send the keep init keep alive

	// need to send Alarm
	fIpAddress = fClnt->addr2String();

}
//------------------------------------------------------------------------------
void WESplClient::onDisconnect()
{
	//TODO
	// - set fContinue to false  - set the thread free
	setContinue(false);
	setConnected(false);
	setRollbackRslt(-1);
	setCleanupRslt(-1);

	if((!fCpiPassed)&&(!fCpiFailed))	//a hard disconnection
		fOwner.onCpimportFail(fPmId);

	// update all the flags of disconnect.
	// alert on roll back
	// do not allow to connect back again.

	try
	{
	// send alarm
	SNMPManager alarmMgr;
	//std::string alarmItem = sin_addr2String(fClnt->serv_addr().sin_addr);
	std::string alarmItem = fClnt->addr2String();
	alarmItem.append(" WriteEngineServer");
	alarmMgr.sendAlarmReport(alarmItem.c_str(), oam::CONN_FAILURE, SET);
	}
	catch(...)
	{
		// just ignore it for time being.
	}
}

//------------------------------------------------------------------------------

void WESplClient::setRowsUploadInfo(int RowsRead, int RowsInserted)
{
	fRowsUploadInfo.fRowsRead = RowsRead;
	fRowsUploadInfo.fRowsInserted = RowsInserted;
}


//------------------------------------------------------------------------------

void WESplClient::add2ColOutOfRangeInfo(int ColNum, 
                                        CalpontSystemCatalog::ColDataType ColType, 
                                        std::string&  ColName, int NoOfOors)
{
	WEColOORInfo aColOorInfo;
	aColOorInfo.fColNum = ColNum;
	aColOorInfo.fColType = ColType;
	aColOorInfo.fColName = ColName;
	aColOorInfo.fNoOfOORs = NoOfOors;
	fColOorVec.push_back(aColOorInfo);
}

//------------------------------------------------------------------------------

void WESplClient::setBadDataFile(const std::string& BadDataFile)
{
	fBadDataFile = BadDataFile;
}

//------------------------------------------------------------------------------


void WESplClient::setErrInfoFile(const std::string& ErrInfoFile)
{
	fErrInfoFile = ErrInfoFile;
}

//------------------------------------------------------------------------------


} /* namespace WriteEngine */
