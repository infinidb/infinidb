/*

   Copyright (C) 2009-2012 Calpont Corporation.

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
 * we_cpifeederthread.cpp
 *
 *  Created on: Mar 29, 2012
 *      Author: bpaul
 */


#include <iostream>
#include <string>
#include <queue>
using namespace std;

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "we_dataloader.h"

#include "we_cpifeederthread.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
//------------------------------------------------------------------------------

void WECpiFeederRunner::operator()()
{
	fOwner.feedData2Cpi();
	cout << "Finished running Feeder Thread!!" << endl;
}


//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
WECpiFeederThread::WECpiFeederThread(WEDataLoader& Ref):
		fOwner(Ref),
		fpThread(0),
		fContinue(true),
		fStopped(true)
{
	cout << "Inside WECpiFeederThread constructor"<< endl;
}

//------------------------------------------------------------------------------

WECpiFeederThread::~WECpiFeederThread()
{
	if (fpThread)
	{
		delete fpThread;
	}
	fpThread=0;
}
//------------------------------------------------------------------------------
void WECpiFeederThread::startFeederThread()
{
	fStopped = false;
	cout << "Starting Feeder Thread!!" << endl;
    fpThread = new boost::thread(WECpiFeederRunner(*this));
}
//------------------------------------------------------------------------------

void WECpiFeederThread::add2MsgQueue(ByteStream& Ibs)
{

	//TODO creating copy is NOT good; later read from socket using a SBS
	messageqcpp::SBS aSbs(new messageqcpp::ByteStream(Ibs));
	Ibs.reset();	//forcefully clearing it
	mutex::scoped_lock aLock(fMsgQMutex);
	//cout << "pushing to the MsgQueue" << endl;
	fMsgQueue.push(aSbs);
	fFeederCond.notify_one();	// as per preference of Damon
	aLock.unlock();

}

//------------------------------------------------------------------------------

void WECpiFeederThread::feedData2Cpi()
{

	while(isContinue())
	{

		mutex::scoped_lock aLock(fMsgQMutex);
		//TODO create a wait signal here
		//when continue is false we need to move and break out of loop
		if(fMsgQueue.empty())
		{
			//cout << "Going to Lock MsgQMutex!!" << endl;
			//fFeederCond.wait(aLock);
			bool aTimeout = fFeederCond.timed_wait(aLock, boost::posix_time::milliseconds(3000));
			if(!isContinue()) { aLock.unlock(); break; }
			//mutex::scoped_lock aCondLock(fContMutex);
			//if(!fContinue) break;
			//aCondLock.unlock();
			// to avoid spurious wake ups.
			if((fMsgQueue.empty())||(!aTimeout)) {	aLock.unlock();	continue; }
		}

		//fPushing = true;	//make it false only when Q empty
		//cout << "Poping from the MsgQueue" << endl;
		messageqcpp::SBS aSbs = fMsgQueue.front();
		fMsgQueue.pop();

		aLock.unlock();

		try
		{
			fOwner.pushData2Cpimport((*aSbs));
			//cout << "Finished PUSHING data " << endl;
		}
		catch(runtime_error& e)
		{
			//cout << "Caught exception : " << e.what() << endl;
			//break;
		}

		aSbs.reset();	//forcefully clearing it
		// We start sending data request from here ONLY
		//if(getQueueSize()< WEDataLoader::MAX_QSIZE) fOwner.sendDataRequest();
		//fOwner.sendDataRequest();
		//fPushing = false;
		//usleep(1000);
		if(getQueueSize() == WEDataLoader::MAX_QSIZE) fOwner.sendDataRequest();
	}

	cout << "CpiFeedThread Stopped!! " << endl;
	fStopped = true;



	/*
	while(fContinue)
	{

		while(!fMsgQueue.empty())
		{

			fPushing = true;	//make it false only when Q empty
			mutex::scoped_lock aLock(fMsgQMutex);
			//when continue is false we need to move and break out of loop
			//while((fMsgQueue.empty())&&(fContinue)) fFeederCond.wait(aLock);
			//if(!fContinue) break;
			//cout << "Poping from the MsgQueue" << endl;
			messageqcpp::SBS aSbs = fMsgQueue.front();
			fMsgQueue.pop();
			aLock.unlock();

			fOwner.pushData2Cpimport((*aSbs));
			//cout << "Finished PUSHING data " << endl;
		}
		fPushing = false;
		usleep(1000);
	}
	cout << "CpiFeedThread Stopped!! " << endl;
	fStopped = true;
	*/



}

//------------------------------------------------------------------------------

bool WECpiFeederThread::isMsgQueueEmpty()
{
	bool aRet = false;
	mutex::scoped_lock aLock(fMsgQMutex);
	aRet = fMsgQueue.empty();
	aLock.unlock();
	return aRet;
}

//------------------------------------------------------------------------------

void WECpiFeederThread::stopThread()
{
	mutex::scoped_lock aCondLock(fContMutex);
	fContinue = false;
	aCondLock.unlock();
	fFeederCond.notify_all();
	cout << "Notified all" << endl;
}

//------------------------------------------------------------------------------

bool WECpiFeederThread::isContinue()
{
	mutex::scoped_lock aCondLock(fContMutex);
	return fContinue;
}

//------------------------------------------------------------------------------



} /* namespace WriteEngine */
