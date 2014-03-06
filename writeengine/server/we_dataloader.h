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
 * 	we_dataloader.h
 *
 *  Created on: Oct 4, 2011
 *      Author: Boby Paul : bpaul@calpont.com
 *
 */



#ifndef WE_DATALOADER_H_
#define WE_DATALOADER_H_

#include "rwlock_local.h"
#include "resourcemanager.h"

#include "we_simplesyslog.h"

#include "we_observer.h"
//#include "we_readthread.h"

#include "we_cpifeederthread.h"

namespace WriteEngine {

class SplitterReadThread;

// This class will go with the read thread & client socket
// It will manage communication between WEClient & cpimport

class WEDataLoader : public Observer
{
public:
	explicit WEDataLoader(SplitterReadThread& pSrt );
	virtual ~WEDataLoader();

	virtual bool update(Subject* pSub);

public:
	bool setupCpimport();		// fork the cpimport
	void teardownCpimport(bool useStoredWaitPidStatus); // @bug 4267
	void pushData2Cpimport(ByteStream& Ibs);	// push data to cpimport from the queue
	void closeWritePipe();
	void str2Argv(std::string CmdLine, std::vector<char*>& V);
	std::string getCalpontHome();
	std::string getPrgmPath(std::string& PrgmName);
	void updateCmdLineWithPath(string& CmdLine);

    
public:
	void onReceiveKeepAlive(ByteStream& Ibs);
	void onReceiveData(ByteStream& Ibs);
	void onReceiveEod(ByteStream& Ibs);		//end of data
	void onReceiveMode(ByteStream& Ibs);
	//void onReceiveCmd(messageqcpp::SBS bs);// {(ByteStream& Ibs);
	void onReceiveCmd(ByteStream& bs);// {(ByteStream& Ibs);
	void onReceiveAck(ByteStream& Ibs);
	void onReceiveNak(ByteStream& Ibs);
	void onReceiveError(ByteStream& Ibs);
   
	void onReceiveJobId(ByteStream& Ibs);
	void onReceiveJobData(ByteStream& Ibs);
    void onReceiveImportFileName(ByteStream& Ibs);
    void onReceiveCmdLineArgs(ByteStream& Ibs);
    void onReceiveStartCpimport();
    void onReceiveBrmRptFileName(ByteStream& Ibs);
    void onReceiveCleanup(ByteStream& Ibs);
    void onReceiveRollback(ByteStream& Ibs);

    void onReceiveErrFileRqst(ByteStream& Ibs);
    void onReceiveBadFileRqst(ByteStream& Ibs);

    void onCpimportSuccess();
    void onCpimportFailure();

    void sendDataRequest();
    void sendCpimportFailureNotice();

	void serialize(messageqcpp::ByteStream& b) const;
	void unserialize(messageqcpp::ByteStream& b);


    // setup the signal handlers for the main app
    void setupSignalHandlers();
    static void onSigChild(int aInt);

public:
	void setMode(int Mode){ fMode = Mode; }
    void updateTxBytes(unsigned int Tx){ fTxBytes += Tx; }
    void updateRxBytes(unsigned int Rx){ fRxBytes += Rx; }
    void setChPid(pid_t pid){ fCh_pid = pid; }
    void setPid(pid_t pid){ fThis_pid = pid; }
    void setPPid(pid_t pid){ fP_pid = pid; }
    void setCmdLineStr(std::string& Str){ fCmdLineStr = Str; }
    void setObjId(int ObjId){ fObjId = ObjId; }

    unsigned int getTxBytes(){ return fTxBytes; }
    unsigned int getRxBytes(){ return fRxBytes; }

    int getObjId(){ return fObjId; }
    int getMode(){ return fMode; }
    pid_t getChPid(){ return fCh_pid; }
    pid_t getPid(){ return fThis_pid; }
    pid_t getPPid(){ return fP_pid; }
    std::string getCmdLineStr(){ return fCmdLineStr; }

private:
	SplitterReadThread& fRef;

	int fMode;
	ofstream fDataDumpFile;
	ofstream fJobFile;
    unsigned int fTxBytes;
    unsigned int fRxBytes;
	char fPmId;
	int fObjId;					// Object Identifier for logging

	// CpImport related Member variables
	int fFIFO[2];			//I/O Pipes
	pid_t fCh_pid;
	pid_t fThis_pid;
	pid_t fP_pid;
	bool fCpIStarted;
	std::string fCmdLineStr;
	std::string fBrmRptFileName;

	//CPI Feeder Thread
	WECpiFeederThread* fpCfThread;

	boost::mutex fClntMsgMutex;		//mutex in sending messages to client.

    //static bool fTearDownCpimport; // @bug 4267
    bool fTearDownCpimport; // @bug 4267
	pid_t fWaitPidRc;       // @bug 4267
	int   fWaitPidStatus;   // @bug 4267

	bool fForceKill;
	bool fPipeErr;			// Err Flag to restrict err msgs logging.

private:
	// more enums follow
	enum CmdId { BULKFILENAME };
public:
	enum { MIN_QSIZE=25, MAX_QSIZE=250};

public:
    SimpleSysLog* fpSysLog;

};

}

#endif /* WE_DATALOADER_H_ */
