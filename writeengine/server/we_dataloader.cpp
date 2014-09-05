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
 * 	we_dataloader.cpp
 *
 *  Created on: Oct 4, 2011
 *      Author: Boby Paul: bpaul@calpont.com
 */


#include <cstdlib>
#include <csignal>
#include <cstring>
#include <cerrno>

#include <unistd.h>			//pipe() && fork()
#if defined(__linux__)
#include <wait.h>			//wait()
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/stat.h>   	// For stat().
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "bytestream.h"
#include "rwlock_local.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <map>
using namespace std;


#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
using namespace boost;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "we_messages.h"
#include "we_brmrprtparser.h"
#include "we_cleartablelockcmd.h"
#include "we_dataloader.h"
#include "we_readthread.h"
#include "config.h" // Used to pickup STRERROR_R_CHAR_P definition

#include "installdir.h"

namespace WriteEngine {

//bool WEDataLoader::fTearDownCpimport=false; // @bug 4267

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	WEDataLoader::Constructor
 *
 **/
WEDataLoader::WEDataLoader(SplitterReadThread& Srt ):fRef(Srt),
	fMode(-1),
	fDataDumpFile(),
    fTxBytes(0),
    fRxBytes(0),
	fPmId(0),
	fCh_pid(0),
	fThis_pid(0),
	fP_pid(0),
	fpCfThread(0),
	fTearDownCpimport(false), // @bug 4267
	fWaitPidRc(0),            // @bug 4267
	fWaitPidStatus(0),         // @bug 4267
	fForceKill(false),
	fPipeErr(false),
	fpSysLog(0)
{
 	Config weConfig;
  	u_int16_t localModuleId = weConfig.getLocalModuleID();
  	fPmId = static_cast<char>(localModuleId); 

  	srand ( time(NULL) );				// initialize random seed
  	int aObjId = rand() % 10000 + 1;		// generate a random number

  	setObjId(aObjId);

  	setupSignalHandlers();
}
//-----------------------------------------------------------------------------
/**
 *
 * @brief 	WEDataLoader::Destructor
 *
 **/

WEDataLoader::~WEDataLoader() {

	try {
		if(fDataDumpFile.is_open()) fDataDumpFile.close();
		cout <<"\tRx Bytes "<< getRxBytes() << endl;
		cout <<"\tTX Bytes "<< getTxBytes() << endl;

		cout <<"\tChild PID "<< getChPid() << endl;
		if (getChPid())
		{
			if(2 == getMode())  //@bug 5012
			{
				kill(getChPid(), SIGINT);
				teardownCpimport(fTearDownCpimport);
			}
			else
			{
				teardownCpimport(false); // @bug 4267
			}
		}

	}
	catch (std::exception&) // @bug 4164: exception causing thread to exit
	{
		cout << "Error tearing down cpimport in WEDataLoader destructor"<<endl;
	}

	//cout << "Leaving WEDataLoader destructor" << endl;
}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------

void WEDataLoader::setupSignalHandlers()
{
#ifndef _MSC_VER
    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, WEDataLoader::onSigChild);
#endif
}
//------------------------------------------------------------------------------
// handles on signal Terminate
//------------------------------------------------------------------------------
void WEDataLoader::onSigChild(int aInt)
{
	std::string aStr = "Received SIGCHLD of terminated process..";
    cout << aStr << endl;
    // fTearDownCpimport = true; // @bug 4267

    // commented out for non-static variables
	//ostringstream oss;
	//oss << getObjId() <<" : " <<aStr;
	//logging::Message::Args errMsgArgs;
	//errMsgArgs.add(oss.str());
	//fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);

}


//-----------------------------------------------------------------------------
/**
 *
 * @brief 	WEDataLoader::update
 *
 **/

bool WEDataLoader::update(Subject* pSub)
{
	return true;
}
//-----------------------------------------------------------------------------
/**
 *
 * @brief setup cpimport as a seperate process
 *
 **/

bool WEDataLoader::setupCpimport() // fork the cpimport
{
	pid_t aChPid;

	if(pipe(fFIFO)== -1)
	{
		perror("pipe");
		throw runtime_error("Error in creating pipe!!");
	}

	setPid(getpid());
	setPPid(getppid());

	aChPid = fork();

	if(aChPid == -1)	//an error caused
	{
		perror("fork");	// fork failed
		throw runtime_error("Error in forking cpimport!!");
	}
	else if(aChPid == 0)// we are in child
	{
		int aStartFD = 3;
		int aEndFD = fFIFO[1]+256;
		close(fFIFO[1]);	//close the WRITER of CHILD

		cout << "Child Process Info: PID = "<< getpid()
			 <<" (fFIFO[0], fFIFO[1]) = ("<< fFIFO[0] <<","<<fFIFO[1]<<")"
			 <<" (StartFD, EndFD) = ("<< aStartFD <<","<<aEndFD<<")"<<endl;

		std::vector<char*> Cmds;
		//str2Argv(fCmdLineStr, Cmds);	// to avoid out-of-scope problem
		std::string aCmdLine = fCmdLineStr;
		std::istringstream ss(aCmdLine);
		std::string arg;
		std::vector<std::string> v2;
		while (ss >> arg)
		{
			v2.push_back(arg);
			Cmds.push_back(const_cast<char*>(v2.back().c_str()));
		}

		Cmds.push_back(0); //null terminate
		//updatePrgmPath(Cmds);

		//NOTE: for debugging
		int aSize = Cmds.size();
		for(int aIdx = 0; aIdx<aSize; ++aIdx)
		{
			cout << "Args " << Cmds[aIdx] << endl;
		}
		cout.flush();

		close(0);			//close stdin for the child
		dup2(fFIFO[0], 0);	//make stdin be the reading end of the pipe

		//BUG 4410 : hacky solution so that CHLD process get EOF on close of pipe
		for(int i=aStartFD;i< aEndFD;i++) close(i);

		int aRet = execv(Cmds[0], &Cmds[0]);	//NOTE - works with full Path
		//int aRet = execvp(Cmds[0], &Cmds[0]);	//NOTE - works if $PATH has cpimport

		cout << "Return status of cpimport is " << aRet <<endl;
		cout.flush();
		close(fFIFO[0]);	// will trigger an EOF on stdin
		ostringstream oss;
		oss << getObjId() <<" :execv error: cpimport.bin invocation failed; Check file and try invoking locally.";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
		//fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		if(aRet == -1) exit(-1);
	}
	else	// parent
	{
		setChPid(aChPid);	// This is the child PID
		cout << "Child PID is " << this->getChPid() << endl;
		close(fFIFO[0]);	//close the READER of PARENT
		// now we can send all the data thru FIFO[1], writer of PARENT
	}

	if(aChPid == 0)
		cout << "******** Child finished its work ********" << endl;

	return true;
}
//-----------------------------------------------------------------------------
/**
 *
 * @brief close all file handles opened for cpimport
 * @brief wait for the cpimport process to finish work
 *
 **/

void WEDataLoader::teardownCpimport(bool useStoredWaitPidStatus) // @bug 4267
{
	fTearDownCpimport = false;		//Reset it
	//cout << "Tearing down Cpimport" << endl;
    int aStatus;

    //cout << "checking fpCfThread value" << endl;
	if(fpCfThread)
	{
	    //cout << "checking fpCfThread has a valid value" << endl;

		//wait until we are done with the queued messages
		while((!fpCfThread->isMsgQueueEmpty())&&(!fpCfThread->isStopped()))
		{
			//cout << "DEBUG : MsgQueue not empty" << endl;
			//cannot be too low, since there is a lock in isMsgQueueEmpty()
			usleep(2000000);
		}

//		while(fpCfThread->isPushing())
//		{
//			cout << "DEBUG : still pushing" << endl;
//			usleep(100000);
//		}

		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Message Queue is empty; Stopping CF Thread";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
		}

		fpCfThread->stopThread();
		while(!fpCfThread->isStopped())
		{
			cout << "DEBUG : still not stopped" << endl;
			usleep(100000);
		}
		delete fpCfThread;
		fpCfThread = 0;
	}


	closeWritePipe();
	pid_t aPid;
	// @bug 4267 begin: call waitpid() to get job status or use stored job status
	// aPid = waitpid(getChPid(), &aStatus, 0); // wait until cpimport finishs
	if (useStoredWaitPidStatus)
	{
		aPid    = fWaitPidRc;
		aStatus = fWaitPidStatus;
	}
	else
	{
		//aPid = waitpid(getChPid(), &aStatus, 0); // wait until cpimport finishs
		aPid = waitpid(getChPid(), &aStatus, WNOHANG); // wait until cpimport finishs
		int aIdx=0;
		while((aPid == 0)&&(aIdx < 25*MAX_QSIZE)) //Do not loop infinitly
		{
			usleep(2000000);
			aPid = waitpid(getChPid(), &aStatus, WNOHANG);
			cout << "Inside tearDown waitpid rc["<<aIdx<<"] = "<< aPid << endl;
			++aIdx;
		}
	}
	// @bug 4267 end						 // BP - added -1 as per DMC comment below
	if ((aPid == getChPid())|| (aPid == -1)) // @bug 4267 (DMC-shouldn't we check for aPid of -1?)
	{
		setChPid(0);
		if ((WIFEXITED(aStatus)) && (WEXITSTATUS(aStatus) == 0))
		{
			cout << "\tCpimport exit on success" << endl;
			ostringstream oss;
			oss << getObjId() <<" : cpimport exit on success";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);

			onCpimportSuccess();
		}
		else
		{
			if(!fForceKill)
			{
				cout << "\tCpimport exit on failure" << endl;
				ostringstream oss;
				oss << getObjId() <<" : cpimport exit on failure";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				onCpimportFailure();
			}
			else
			{
				cout << "\tCpimport exit on Force Kill!!" << endl;
				ostringstream oss;
				oss << getObjId() <<" : cpimport exit on Force kill!!";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				onCpimportSuccess();
			}
		}
	}
}
//-----------------------------------------------------------------------------
/**
 * @brief 	Push the data to cpimport from the incoming ByteStream
 * @param	Incoming ByteStream
 *
 */
//
void WEDataLoader::pushData2Cpimport(ByteStream& Ibs)
{
	if(Ibs.length()>0)
	{
		int aLen = Ibs.length();
		char* pStart = reinterpret_cast<char*>(Ibs.buf());
		char* pEnd = pStart+aLen;
		char* pPtr = pStart;
		while(pPtr < pEnd)
		{
			//		if(pEnd > (pPtr + MAX_LEN))
			//		{
			//			int aRet = write(fFIFO[1], pPtr, MAX_LEN);
			//			if(aRet == -1) throw runtime_error("Pipe write error");
			//			//write(fFIFO[1], Ibs.buf(), Ibs.length());
			//			pPtr += MAX_LEN;
			//		}
			//		else
			//		{
			//			int aStrLen = pEnd - pPtr;
			//			int aRet = write(fFIFO[1], pPtr, aStrLen);
			//			if(aRet == -1) throw runtime_error("Pipe write error");
			//			pPtr += aStrLen;
			//		}

			try
			{
				int aRet = write(fFIFO[1], pPtr, pEnd-pPtr);
				if(aRet < 0)
				{
					if(!fPipeErr)
					{
					int e = errno;
					fPipeErr = true;
					std::string aStr = "pushing data : PIPE error .........";

					char errMsgBuf[160];
#if STRERROR_R_CHAR_P
					const char* pErrMsg = strerror_r(
						e, errMsgBuf, sizeof(errMsgBuf));
					if (pErrMsg)
						aStr += pErrMsg;
#else
					int errMsgRc = strerror_r(e, errMsgBuf, sizeof(errMsgBuf));
					if (errMsgRc == 0)
						aStr += errMsgBuf;
#endif
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(aStr);
					fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
					}
					throw runtime_error("Pipe Error - cpimport.bin exited already!!");
				}
				pPtr += aRet;
			}
			catch(...)
			{
				//std::string aStr = "pushing data PIPE error .........";
				//logging::Message::Args errMsgArgs;
				//errMsgArgs.add(aStr);
				//fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
				throw runtime_error("Pipe Error - cpimport.bin exited already!!");
			}
		}

	}
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Close the pipe through which data was written to cpimport
 * @brief 	This will also signal a EOF to the reading pipe.
 *
 **/

void  WEDataLoader::closeWritePipe()
{
	cout << "Going to close call = "<<fFIFO[1] << endl;
	//NOTE this will flush the file buffer and close it.
	int aRet = close(fFIFO[1]);		// will trigger EOD
	cout << "----- closed both pipes -------- aRet = "<< aRet << endl;
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Tokenize a string into char** argv format and store in a vector
 * @brief 	we pass the V as arguments to exec cpimport
 * @param 	CmdLine is the string form of arguments demlimited by space
 * @param 	V vector which contains each element of argv
 *
 **/

void WEDataLoader::str2Argv(std::string CmdLine, std::vector<char*>& V)
{
	std::istringstream ss(CmdLine);
	std::string arg;
	std::vector<std::string> v2;
	while (ss >> arg)
	{
		v2.push_back(arg);
		V.push_back(const_cast<char*>(v2.back().c_str()));
	}

	V.push_back(0); //null terminate
}


//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/
std::string WEDataLoader::getCalpontHome()
{
	string calpontDir = config::Config::makeConfig()->getConfig(
			"SystemConfig", "CalpontHome");
	if(0 == calpontDir.length())
	{
		calpontDir = startup::StartUp::installDir() + "/bin";
	}
	else
	{
		calpontDir += "/bin";
	}

	return calpontDir;
}


//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/
std::string WEDataLoader::getPrgmPath(std::string& PrgmName)
{
	std::string cpimportPath = getCalpontHome();
	cpimportPath += "/";
	cpimportPath += PrgmName;
	return cpimportPath;
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/

void WEDataLoader::updateCmdLineWithPath(string& CmdLine)
{
	std::istringstream iSs(CmdLine);
	std::ostringstream oSs;
	std::string aArg;
	int aCount=0;
	while(iSs >> aArg)
	{
		if(0 == aCount)
		{
			string aPrgmPath = getPrgmPath(aArg);
			oSs << aPrgmPath;
		}
		else
		{
			oSs << " ";
			oSs << aArg;

		}
		++aCount;
	}

	CmdLine = oSs.str();
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Event to trigger when cpimport is successful.
 *
 **/

void WEDataLoader::onCpimportSuccess()
{

	ByteStream obs;

	cout <<"Sending BRMRPT" << endl;
	obs << (ByteStream::byte)WE_CLT_SRV_BRMRPT;
	obs << (ByteStream::byte)fPmId;     // PM id
	// for testing
	//std::string fRptFileName("ReportFile.txt");
	BrmReportParser aBrmRptParser;
	bool aRet = aBrmRptParser.serialize(fBrmRptFileName, obs);
	if(aRet)
	{
		mutex::scoped_lock aLock(fClntMsgMutex);
		//aBrmRptParser.unserialize(obs);   - was for testing
		updateTxBytes(obs.length());
		try
		{
			fRef.fIos.write(obs);
		}
		catch(...)
		{
			cout <<"Broken Pipe .." << endl;
			if(fpSysLog)
			{
				ostringstream oss;
				oss << getObjId() <<" : Broken Pipe : socket write failed ";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
			}
		}
		aLock.unlock();
		cout <<"Finished Sending BRMRPT" << endl;
	}
	else
	{
		cout << "Failed to serialize BRMRpt "<< endl;
	}

	if(remove(fBrmRptFileName.c_str()) != 0)
		cout <<"Failed to delete BRMRpt File "<< fBrmRptFileName << endl;
	//usleep(1000000);	//sleep 1 second.

	obs.reset();
	obs << (ByteStream::byte)WE_CLT_SRV_CPIPASS;
	obs << (ByteStream::byte)fPmId;     // PM id
	mutex::scoped_lock aLock(fClntMsgMutex);
    updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}

	}
	aLock.unlock();

	cout <<"Sent CPIPASS info" << endl;
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : onCpimportSuccess BrmReport Send";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Event to trigger if a cpimport failure occurs.
 *
 **/
void WEDataLoader::onCpimportFailure()
{

	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_CPIFAIL;
	obs << (ByteStream::byte)fPmId;     // PM id
	mutex::scoped_lock aLock(fClntMsgMutex);
    updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();

	//Even if we failed, we have failure info in BRMRPT
	obs.reset();
	obs << (ByteStream::byte)WE_CLT_SRV_BRMRPT;
	obs << (ByteStream::byte)fPmId;     // PM id
	BrmReportParser aBrmRptParser;
	bool aRet = aBrmRptParser.serialize(fBrmRptFileName, obs);
	if(aRet)
	{
		mutex::scoped_lock aLock(fClntMsgMutex);
		updateTxBytes(obs.length());
		try
		{
			fRef.fIos.write(obs);
		}
		catch(...)
		{
			cout <<"Broken Pipe .." << endl;
			if(fpSysLog)
			{
				ostringstream oss;
				oss << getObjId() <<" : Broken Pipe : socket write failed ";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
			}
		}
		aLock.unlock();
		cout <<"Finished Sending BRMRPT" << endl;
	}

	if(remove(fBrmRptFileName.c_str()) != 0)
		cout <<"Failed to delete BRMRpt File "<< fBrmRptFileName << endl;

	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : onCpimportFailure BrmReport Send";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}
}

//-----------------------------------------------------------------------------
/**
 * @brief 	Event when a KEEPALIVE arrives.
 * @param	Incoming ByteStream, not used currently
 */
void WEDataLoader::onReceiveKeepAlive(ByteStream& Ibs)
{
	// Do what we have to do with the message
	if(!fpSysLog)
	{
		fpSysLog = SimpleSysLog::instance();
		fpSysLog->setLoggingID(logging::LoggingID(SUBSYSTEM_ID_WE_SRV));
	}

	/*
	// TODO comment out when we done with debug
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : Received KEEPALIVE";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
	}
	*/
    //cout << "Received KEEPALIVE" << endl;
	// NOTE only seldom a KEEPALIVE receives,
    // 		so nothing wrong in responding with a KEEPALIVE.
	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_KEEPALIVE;
	obs << (ByteStream::byte)fPmId;     // PM id
	mutex::scoped_lock aLock(fClntMsgMutex);
    updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() << ": Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();

	// @bug 4267 begin
    int   aStatus;
	pid_t aPid;
	if (getChPid() > 0)
	{
		aPid = waitpid(getChPid(), &aStatus, WNOHANG); // wait until cpimport finishs
		if(aPid != 0)
		{
			cout << "waitpid(" << getChPid() << "): rc-" << aPid <<
					"; status-" << aStatus << "; exited-" <<
					(WIFEXITED(aStatus)) << endl;
		}
		if ((aPid == getChPid()) || (aPid == -1))
		{
			fTearDownCpimport = true;
			fWaitPidRc        = aPid;
			fWaitPidStatus    = aStatus;
		}
	}
	// @bug 4267 end

	if(fTearDownCpimport)
	{
		//fTearDownCpimport = false;		//Reset it // commented out to use the flag in EOD
		cout << "Cpimport terminated " << endl;
		if(0 == getMode()) onReceiveEod(Ibs);
		else if(1 == getMode())	//mode 1 has to drive from UM
		{
			ByteStream obs;
			obs << (ByteStream::byte)WE_CLT_SRV_EOD;
			obs << (ByteStream::byte)fPmId;     // PM id
			mutex::scoped_lock aLock(fClntMsgMutex);
		    updateTxBytes(obs.length());
			try
			{
				fRef.fIos.write(obs);
			}
			catch(...)
			{
				cout <<"Broken Pipe .." << endl;
				if(fpSysLog)
				{
					ostringstream oss;
					oss << getObjId() <<": Broken Pipe : socket write failed ";
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(oss.str());
					fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				}
			}
			aLock.unlock();
		}
		else if(2==getMode())
		{
			//if(getChPid()) teardownCpimport(true); // @bug 4267
			if(getChPid()) teardownCpimport(fTearDownCpimport); // @bug 4267 BP
		}
	}
	else
	{
		//if(1 == getMode())
		//	if(fpCfThread) cout << "Queue Size = " << fpCfThread->getQueueSize() << endl;
	}

}
//-----------------------------------------------------------------------------
/**
 * @brief 	trigger when a DATA arrives.
 * @param	Incoming ByteStream which contains data
 */

void WEDataLoader::onReceiveData(ByteStream& Ibs)
{

	if((0 == getMode())&& (fDataDumpFile.is_open()))
	{
		// Will write to the output file.
		fDataDumpFile << Ibs;
		sendDataRequest();
	}
	else if( 1 == getMode())
	{
		// commented out since we are going to use seperate thread
		//pushData2Cpimport(Ibs);

		if(fpCfThread)
		{
			fpCfThread->add2MsgQueue(Ibs);
			//sendDataRequest();	// Need to control Queue Size
			// Bug 5031 : Will only send 1 rqst for a batch to cpimport.bin
			//if(fpCfThread->getQueueSize()<MIN_QSIZE) sendDataRequest();
			//if(fpCfThread->getQueueSize()<MAX_QSIZE) sendDataRequest();

			int aQsz = (fpCfThread)?fpCfThread->getQueueSize():0;
			// Bug 5031 : If Q size goes above 100 (2*250);
			if(aQsz < MAX_QSIZE) sendDataRequest();
			if(aQsz > 1.5*MAX_QSIZE) // > 2*250
			{
				cout <<"WARNING : Data Queuing up : QSize = "<< aQsz <<endl;
				if(fpSysLog)
				{
					ostringstream oss;
					oss << getObjId() <<"WARNING : Data Queuing up : QSize = "<< aQsz;
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(oss.str());
					fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				}
			}
		}

	}
	else if( 2 == getMode())
	{
		cout << "onReceiveData : In Mode 2 NO data suppose to receive"<< endl;
	}


//	ByteStream obs;
//	obs << (ByteStream::byte)WE_CLT_SRV_DATARQST;
//	obs << (ByteStream::byte)fPmId;     // PM id
//  updateTxBytes(obs.length());
//	fRef.fIos.write(obs);
}

//-----------------------------------------------------------------------------
/**
 * @brief 	trigger when a EOD arrives.
 * @param	Incoming ByteStream; not relevent for now
 */
void WEDataLoader::onReceiveEod(ByteStream& Ibs)
{
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : onReceiveEOD : child ID = " << getChPid();
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

	cout << "Received EOD " << endl;
	if(0 == getMode())
	{
		fDataDumpFile.close();
	}
	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_EOD;
	obs << (ByteStream::byte)fPmId;     // PM id
	mutex::scoped_lock aLock(fClntMsgMutex);
    updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();

	//if(( 1 == getMode())||( 2 == getMode()))
	if(1 == getMode()) 	//BUG 4370 - seperated mode 1 & 2
	{
		//if(getChPid()) teardownCpimport(false); // @bug 4267
		if(getChPid()) teardownCpimport(fTearDownCpimport); // @bug 4267 //BP changed to send the correct flag
	}
	else if (2 == getMode())	//BUG 4370
	{
		if(getChPid())
		{
			kill(getChPid(), SIGINT);	//BUG 4370
			fForceKill = true;
			teardownCpimport(fTearDownCpimport);	//BUG 4370
		}
	}


}
//-----------------------------------------------------------------------------
/**
 * @brief 	Event on Command Received. It should contain sub commands
 * @param	Incoming ByteStream, will have sub commands
 */
void WEDataLoader::onReceiveCmd(ByteStream& bs) {
	//TODO - can be cpimport cmd or server cmd, for now write to a file
    ByteStream::byte aCmdId;
	//(*bs) >> aCmdId;
    bs >> aCmdId;

	// switch
	switch(aCmdId)
	{
	default:
		//cout << "Cmd received .. check where did it come from " << endl;
		break;
	}

}
//-----------------------------------------------------------------------------
/**
 * @brief 	The mode in which WES running.
 * @param	Incoming ByteStream, not relevent
 */
void WEDataLoader::onReceiveMode(ByteStream& Ibs)
{
	// Assigning it here since WEDataLoader constructor is called multiple times
	// while coping in readthread class.
	if(!fpSysLog)
	{
		fpSysLog = SimpleSysLog::instance();
		fpSysLog->setLoggingID(logging::LoggingID(SUBSYSTEM_ID_WE_SRV));
	}

	Ibs >> (ByteStream::quadbyte&)fMode;
	cout << "Setting fMode = " << fMode << endl;

	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : onReceiveMode() Setting fMode = " << fMode;
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

	char aName[64];
	snprintf(aName, sizeof(aName), "ModuleDBRootCount%d-3",fPmId);

	string aStrDbRootCnt = config::Config::makeConfig()->getConfig(
	             	 	 	 	 	 "SystemModuleConfig", aName);
	cout << "DbRootCnt = " << aStrDbRootCnt << endl;
	ByteStream::byte aDbCnt = (ByteStream::byte)atoi(aStrDbRootCnt.c_str());

	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : onReceiveMode() DbRoot Count = " + aStrDbRootCnt;
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}


	//Send No. of DBRoots to Client
	ByteStream aObs;
	aObs << (ByteStream::byte)WE_CLT_SRV_DBRCNT;
	aObs << (ByteStream::byte)fPmId;
	aObs << (ByteStream::byte)aDbCnt;
	mutex::scoped_lock aLock(fClntMsgMutex);
    updateTxBytes(aObs.length());
	try
	{
		fRef.fIos.write(aObs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}

	}
	aLock.unlock();

}
//-----------------------------------------------------------------------------
/**
 * @brief 	Acknowledgment. Not relevant at this point of time.
 * @brief	Can make use to update the BRM
 * @param	Incoming ByteStream, not relevant for now
 */

void WEDataLoader::onReceiveAck(ByteStream& Ibs)
{
	// All is good
	// update the status
}
//-----------------------------------------------------------------------------
/**
 * @brief 	NAK. A Failure, Rollback should be initiated.
 * @brief	Can make use to update the BRM
 * @param	Incoming ByteStream, not relevant for now
 */
void WEDataLoader::onReceiveNak(ByteStream& Ibs)
{
	// TODO - handle the problem
}
//-----------------------------------------------------------------------------
/**
 * @brief 	ERROR. A Failure, Rollback should be initiated.
 * @brief	Can make use to update the BRM
 * @param	Incoming ByteStream, not relevant for now
 */
void WEDataLoader::onReceiveError(ByteStream& Ibs)
{
	// TODO - handle the failure situation.
}
//------------------------------------------------------------------------------
//  onReceiveCmdLineArgs - do what ever need to do with command line args
//------------------------------------------------------------------------------
/**
 * @brief 	Command line args received.
 * @brief	Can make use to update the BRM
 * @param	Incoming ByteStream, not relevant for now
 */

void WEDataLoader::onReceiveCmdLineArgs(ByteStream& Ibs)
{
	Ibs >> fCmdLineStr;
	cout << "CMD LINE ARGS came in " << fCmdLineStr << endl;
	updateCmdLineWithPath(fCmdLineStr);
	cout << "Updated CmdLine : " << fCmdLineStr << endl;

	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : CMD LINE ARGS came in " << fCmdLineStr;
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

	ByteStream obs;
	//TODO - Need to check all clear for starting CPI
	if(0 != getMode())
	{
		obs << (ByteStream::byte)WE_CLT_SRV_STARTCPI;
	}
	else
	{
		obs << (ByteStream::byte)WE_CLT_SRV_DATARQST;
	}
	obs << (ByteStream::byte) fPmId; // PM id
	mutex::scoped_lock aLock(fClntMsgMutex);
	updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();

}

//-----------------------------------------------------------------------------

void WEDataLoader::onReceiveStartCpimport()
{
	cout << "Start Cpimport command reached!!" << endl;
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : Start Cpimport command reached!!";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

	try
	{
		setupCpimport();
		if(1 == getMode())	// create a thread to handle the data feeding part
		{
			fpCfThread = new WECpiFeederThread(*this);
			fpCfThread->startFeederThread();
		}
	}
	catch(...)
	{
		// send an CPI FAIL command back to splitter
	}
	if(1 == getMode())	// In mode 2/0 we do not rqst data.
	{
		sendDataRequest();
	}
	// We need to respond to KEEP ALIVES
	//else if(2 == getMode())	// Now we wait till cpimport comes back
	//{
	//	if(getChPid())
	//		teardownCpimport();
	//}


}

//-----------------------------------------------------------------------------

void WEDataLoader::onReceiveBrmRptFileName(ByteStream& Ibs)
{
	Ibs >> fBrmRptFileName;
	cout << "Brm Rpt Filename Arrived "<< fBrmRptFileName << endl;

	//BUG 4645
	string::size_type idx = fBrmRptFileName.find_last_of('/');
	if (idx > 0 && idx < string::npos)
	{
		string dirname(fBrmRptFileName, 0, idx);
		struct stat st;
		if (stat(dirname.c_str(), &st) != 0)
		{
			cout << "Creating directory : " << dirname <<endl;
			boost::filesystem::create_directories(dirname.c_str());
		}
		/*
		#ifdef _MSC_VER
			mkdir(dirname.c_str());
		#else
			mkdir(dirname.c_str(), 0777);
			boost::filesystem::create_directories("/tmp/boby/test");
		#endif
		*/
	}

	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : Brm Rpt Filename Arrived "<< fBrmRptFileName;
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

}

//-----------------------------------------------------------------------------

void WEDataLoader::onReceiveCleanup(ByteStream& Ibs)
{
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : OnReceiveCleanup arrived";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}

	std::string aErrMsg;

	WE_ClearTableLockCmd aClrTblLockCmd("DataLoader");
	int aRet = aClrTblLockCmd.processCleanup(Ibs, aErrMsg);

	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_CLEANUP;
	obs << (ByteStream::byte) fPmId; 	// PM id
	if(aRet == 0)
		obs << (ByteStream::byte)1;		// cleanup success
	else
		obs << (ByteStream::byte)0;		// cleanup failed
	mutex::scoped_lock aLock(fClntMsgMutex);
	updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}

	}
	aLock.unlock();

}


//-----------------------------------------------------------------------------



void WEDataLoader::onReceiveRollback(ByteStream& Ibs)
{
	if(fpSysLog)
	{
		ostringstream oss;
		oss << getObjId() <<" : OnReceiveRollback arrived";
		logging::Message::Args errMsgArgs;
		errMsgArgs.add(oss.str());
		fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_DEBUG, logging::M0000);
	}


	std::string aErrMsg;

	WE_ClearTableLockCmd aClrTblLockCmd("DataLoader");
	int aRet = aClrTblLockCmd.processRollback(Ibs, aErrMsg);

	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_ROLLBACK;
	obs << (ByteStream::byte) fPmId; // PM id
	if(aRet == 0)
		obs << (ByteStream::byte)1;		// Rollback success
	else
		obs << (ByteStream::byte)0;		// Rollback failed
	mutex::scoped_lock aLock(fClntMsgMutex);
	updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() << " : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();

}
//-----------------------------------------------------------------------------


void WEDataLoader::onReceiveImportFileName(ByteStream& Ibs)
{
	bool aGoodFile = true;
	std::string aFileName;
	Ibs >> aFileName;

	//BUG 4245 : Need to check the file or path exists
	{
		std::fstream aFin;
		aFin.open(aFileName.c_str(), std::ios::in);
		if(aFin.is_open())	// File exist, send an ERROR immediately
		{// file exists
			ByteStream obs;
			obs << (ByteStream::byte)WE_CLT_SRV_IMPFILEERROR;
			obs << (ByteStream::byte)fPmId;
			updateTxBytes(obs.length());
			mutex::scoped_lock aLock(fClntMsgMutex);
			try
			{
				fRef.fIos.write(obs);
			}
			catch(...)
			{
				cout <<"Broken Pipe .." << endl;
				if(fpSysLog)
				{
					ostringstream oss;
					oss << getObjId() <<" : Broken Pipe : socket write failed ";
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(oss.str());
					fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				}

			}
			aGoodFile = false;
			aLock.unlock();
		}
		aFin.close();
	}

	if(aGoodFile)
	{
		fDataDumpFile.open(aFileName.c_str(),std::ios::app);
		//BUG 4245 : If file dir is not existing, we need to fail this import
		if(!fDataDumpFile.good())
		{
			ByteStream obs;
			obs << (ByteStream::byte)WE_CLT_SRV_IMPFILEERROR;
			obs << (ByteStream::byte)fPmId;     // PM id
			mutex::scoped_lock aLock(fClntMsgMutex);
			updateTxBytes(obs.length());
			try
			{
				fRef.fIos.write(obs);
			}
			catch(...)
			{
				cout <<"Broken Pipe .." << endl;
				if(fpSysLog)
				{
					ostringstream oss;
					oss << getObjId() <<" : Broken Pipe : socket write failed ";
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(oss.str());
					fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
				}
			}
			aLock.unlock();
		}
	}

}
//------------------------------------------------------------------------------

void WEDataLoader::onReceiveJobId(ByteStream& Ibs)
{
	std::string aJobFileName;
	Ibs >> aJobFileName;

	cout << "Incoming JobFileName : " << aJobFileName << endl;

	//BUG 4645
	string::size_type idx = aJobFileName.find_last_of('/');
	if (idx > 0 && idx < string::npos)
	{
		string dirname(aJobFileName, 0, idx);
		struct stat st;

		if (stat(dirname.c_str(), &st) != 0)
		{
			cout << "Creating directory : " << dirname << endl;
			boost::filesystem::create_directories(dirname.c_str());
		}
		/*
		#ifdef _MSC_VER
			mkdir(dirname.c_str());
		#else
			mkdir(dirname.c_str(), 0777);
			boost::filesystem::create_directories("/tmp/boby/test");
		#endif
		*/
	}

	fJobFile.open(aJobFileName.c_str());

}

//------------------------------------------------------------------------------

void WEDataLoader::onReceiveJobData(ByteStream& Ibs)
{
	// Will write to the output file.
	std::string aData;
	Ibs >> aData;
	fJobFile << aData;
	fJobFile.close();
}

//------------------------------------------------------------------------------

void WEDataLoader::onReceiveErrFileRqst(ByteStream& Ibs)
{
	std::string aErrFileName;
	Ibs >> aErrFileName;
	cout << "Error Filename Arrived "<< aErrFileName << endl;

	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_ERRLOG;
	obs << (ByteStream::byte) fPmId; // PM id
	obs << aErrFileName;
	BrmReportParser aErrFileParser;
	bool aRet = aErrFileParser.serialize(aErrFileName, obs);
	if(aRet)
	{
		mutex::scoped_lock aLock(fClntMsgMutex);
		updateTxBytes(obs.length());
		try
		{
			fRef.fIos.write(obs);
		}
		catch(...)
		{
			cout <<"Broken Pipe .." << endl;
			if(fpSysLog)
			{
				ostringstream oss;
				oss << getObjId() <<" : Broken Pipe : socket write failed ";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
			}
		}
		aLock.unlock();
	}
	// delete the temp files
	if(remove(aErrFileName.c_str()) != 0 )
		cout << "Failed in removing Error file: " << aErrFileName << endl;
}



//------------------------------------------------------------------------------
// Process the receipt of a msg containing the contents of a *.bad file.
//------------------------------------------------------------------------------
void WEDataLoader::onReceiveBadFileRqst(ByteStream& Ibs)
{
	std::string aBadFileName;
	Ibs >> aBadFileName;
	cout << "Error Filename Arrived "<< aBadFileName << endl;

	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_BADLOG;
	obs << (ByteStream::byte) fPmId; // PM id
	obs << aBadFileName;
	BrmReportParser aBadFileParser;
	bool aRet = aBadFileParser.serializeBlocks(aBadFileName, obs);
	if(aRet)
	{
		mutex::scoped_lock aLock(fClntMsgMutex);
		updateTxBytes(obs.length());
		try
		{
			fRef.fIos.write(obs);
		}
		catch(...)
		{
			cout <<"Broken Pipe .." << endl;
			if(fpSysLog)
			{
				ostringstream oss;
				oss << getObjId() <<" : Broken Pipe : socket write failed ";
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(oss.str());
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
			}
		}
		aLock.unlock();
	}

	// delete the temp files
	if( remove(aBadFileName.c_str()) != 0)
		cout << "Failed in removing Error file: " << aBadFileName << endl;
}


//------------------------------------------------------------------------------

void WEDataLoader::sendDataRequest()
{
	int aQsz = (fpCfThread)?fpCfThread->getQueueSize():0;
	//if(aQsz>MIN_QSIZE)
	// Bug 5031 : If Q size goes above 100 (2*50); there is some thing wrong
	// will put a warning in info log. Controlled in Cpimport init data rqst cnt
	if(aQsz> MAX_QSIZE) // >250
	{
		cout <<"WARNING : Data Queuing up : QSize = "<< aQsz <<endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<"WARNING : Data Queuing up : QSize = "<< aQsz;
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}

	mutex::scoped_lock aLock(fClntMsgMutex);
	ByteStream obs;
	obs << (ByteStream::byte)WE_CLT_SRV_DATARQST;
	obs << (ByteStream::byte)fPmId;     // PM id
	updateTxBytes(obs.length());
	try
	{
		fRef.fIos.write(obs);
	}
	catch(...)
	{
		cout <<"Broken Pipe .." << endl;
		if(fpSysLog)
		{
			ostringstream oss;
			oss << getObjId() <<" : Broken Pipe : socket write failed ";
			logging::Message::Args errMsgArgs;
			errMsgArgs.add(oss.str());
			fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);
		}
	}
	aLock.unlock();
}


//------------------------------------------------------------------------------
void WEDataLoader::serialize(messageqcpp::ByteStream& b) const {
	//TODO to be changed. left it here to understand how to implement
	/*
	b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN;
	ReturnedColumn::serialize(b); // parent class serialize
	b << (u_int32_t) fOid;
	b << fData;
	b << static_cast<const ByteStream::doublebyte>(fReturnAll);
	b << (u_int32_t) fSequence;
	*/
}

//-----------------------------------------------------------------------------

void WEDataLoader::unserialize(messageqcpp::ByteStream& b)
{
	//TODO to be changed. left it here to understand how to implement
	/*
	ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN);
	ReturnedColumn::unserialize(b); // parent class unserialize
	b >> (u_int32_t&) fOid;
	b >> fData;
	b >> reinterpret_cast<ByteStream::doublebyte&>(fReturnAll);
	b >> (u_int32_t&) fSequence;
	*/
}

//-----------------------------------------------------------------------------

}   // namespace WriteEngine
