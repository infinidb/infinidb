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
		if(fMsgQueue.empty())
		{
			bool aTimedOut = fFeederCond.timed_wait(aLock, boost::posix_time::milliseconds(3000));
			if(!isContinue()) { aLock.unlock(); break; }
			// to handle spurious wake ups and timeout wake ups
			if((fMsgQueue.empty())||(!aTimedOut)) {	aLock.unlock();	continue; }
		}

		messageqcpp::SBS aSbs = fMsgQueue.front();
		fMsgQueue.pop();

		aLock.unlock();

		try
		{
			fOwner.pushData2Cpimport((*aSbs));
			//cout << "Finished PUSHING data " << endl;
		}
		catch(runtime_error&)
		{
			//cout << "Caught exception : " << e.what() << endl;
			//break;
		}

		aSbs.reset();	//forcefully clearing it
		// We start sending data request from here ONLY
		if(getQueueSize() == WEDataLoader::MAX_QSIZE) fOwner.sendDataRequest();
	}

	cout << "CpiFeedThread Stopped!! " << endl;
	fStopped = true;

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
