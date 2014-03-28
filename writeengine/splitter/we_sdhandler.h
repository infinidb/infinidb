/* Copyright (C) 2013 Calpont Corp.

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
 * we_spltrdatahandler.h
 *
 *  Created on: Oct 17, 2011
 *      Author: bpaul
 */

#ifndef WE_SPLITTERDATAHANDLER_H_
#define WE_SPLITTERDATAHANDLER_H_


#include "liboamcpp.h"
#include "resourcemanager.h"
#include "threadsafequeue.h"
#include "dbrm.h"
#include "batchloader.h"
#include "we_log.h"
#include "we_type.h"

#include "we_filereadthread.h"
#include "we_splclient.h"


namespace WriteEngine
{

class WESplitterApp;        //forward declaration
class WESplClient;
class WEFileReadThread;

//------------------------------------------------------------------------------
//	a stl list to keep Next PM to send data
//------------------------------------------------------------------------------

class WEPmList
{
public:
	WEPmList():fPmList(),fListMutex(){}
	virtual ~WEPmList(){ fPmList.clear(); }

	void addPm2List(int PmId);
	void addPriorityPm2List(int PmId);
	int getNextPm();
	void clearPmList();
	bool check4Pm(int PmId);

private:
	typedef std::list<int> WePmList;	// List to add in front
	WePmList fPmList;
	boost::mutex fListMutex;	//mutex controls add/remove

};


//------------------------------------------------------------------------------
class WESDHandler
{

public:
	WESDHandler(WESplitterApp& Ref);
	WESDHandler(const WESDHandler& rhs);
	virtual ~WESDHandler();

    void setup();
    void shutdown();
    void reset();
    void send2Pm(messageqcpp::SBS& Sbs, unsigned int PmId=0);
    void send2Pm(messageqcpp::ByteStream & Bs, unsigned int PmId=0);
    void sendEODMsg();
    void checkForRespMsgs();
    void add2RespQueue(const messageqcpp::SBS & Sbs);
    void exportJobFile(std::string &JobId, std::string &JobFileName);
    int  leastDataSendPm();
    bool check4AllBrmReports();
    bool updateCPAndHWMInBRM();
    void cancelOutstandingCpimports();
    void doRollback();
    void doCleanup(bool deleteHdfsTempDbFiles);
    void getErrorLog(int PmId, const std::string& ErrFileName);
    void getBadLog(int PmId, const std::string& BadFileName);
    int check4RollbackRslts();
    bool check4AllRollbackStatus();
    int check4CleanupRslts();
    bool check4AllCleanupStatus();
    bool check4AllCpiStarts();
    bool releaseTableLocks();
    void check4CpiInvokeMode();
    bool check4PmArguments();
    bool check4InputFile(std::string InFileName);
    bool check4CriticalErrMsgs(std::string& Entry);

    void onStartCpiResponse(int PmId);
    void onDataRqstResponse(int PmId);
    void onAckResponse(int PmId);
    void onNakResponse(int PmId);
    void onEodResponse(int Pmid);
	void onPmErrorResponse(int PmId);
	void onKeepAliveMessage(int PmId);
	void onCpimportPass(int PmId);
	void onCpimportFail(int PmId, bool SigHandle=false);
	void onImpFileError(int PmId);
	void onBrmReport(int PmId, messageqcpp::SBS& Sbs);
	void onErrorFile(int PmId, messageqcpp::SBS& Sbs);
	void onBadFile(int PmId, messageqcpp::SBS& Sbs);
	void onRollbackResult(int PmId, messageqcpp::SBS& Sbs);
	void onCleanupResult(int PmId, messageqcpp::SBS& Sbs);
	void onDBRootCount(int PmId, messageqcpp::SBS& Sbs);
	void onHandlingSignal();
	void onHandlingSigHup();
	void onDisconnectFailure();

    int getNextPm2Feed();
    int getNextDbrPm2Send();
    int getTableOID(std::string Schema, std::string Table);
    std::string getTime2Str() const;

    bool checkAllCpiPassStatus();
    bool checkAllCpiFailStatus();
    bool checkForRollbackAndCleanup();
    bool checkForCpiFailStatus();

    void checkForConnections();
    void sendHeartbeats();
    std::string getTableName() const;
    std::string getSchemaName() const;
    char getEnclChar();
    char getEscChar();
    char getDelimChar();
	bool getConsoleLog();
    ImportDataMode getImportDataMode() const;
    void sysLog(const logging::Message::Args& msgArgs,
    		logging::LOG_TYPE logType, logging::Message::MessageID msgId);


    boost::thread* getFpRespThread() const { return fpRespThread;  }
    unsigned int getQId() const { return fQId; }
    void setFpRespThread(boost::thread *pRespThread)
    {	fpRespThread = pRespThread; }
    void setQId(unsigned int QId) { fQId = QId; }
    bool isContinue() const { return fContinue; }
    void setContinue(bool Continue) { 	fContinue = Continue; }
    int getPmCount() const { return fPmCount; }
    void setPmCount(int PmCount) {	fPmCount = PmCount; }
    int getNextPm2Send() {	return fDataFeedList.getNextPm(); }
    bool check4Ack(unsigned int PmId) {	return fDataFeedList.check4Pm(PmId); }
    int getTableOID()   { return fTableOId; }
    void setDebugLvl(int DebugLvl) { fDebugLvl = DebugLvl; }
    int getDebugLvl() {	return fDebugLvl; }
    unsigned int getTableRecLen() const { return fFixedBinaryRecLen; }
    void updateRowTx(unsigned int RowCnt, int CIdx)
    {   fWeSplClients[CIdx]->updateRowTx(RowCnt);    }
    void resetRowTx(){ for (int aCnt = 1; aCnt <= fPmCount; aCnt++)
    	{if (fWeSplClients[aCnt] != 0) {fWeSplClients[aCnt]->resetRowTx(); } } }
    void setRowsUploadInfo(int PmId, int RowsRead, int RowsInserted)
    { fWeSplClients[PmId]->setRowsUploadInfo(RowsRead, RowsInserted); }
    void add2ColOutOfRangeInfo(int PmId, int ColNum, 
                               CalpontSystemCatalog::ColDataType ColType,
                               std::string&  ColName, int NoOfOors)
    { fWeSplClients[PmId]->add2ColOutOfRangeInfo(ColNum, ColType, ColName, NoOfOors); }
    void setErrorFileName(int PmId, const std::string& ErrFileName)
    { fWeSplClients[PmId]->setErrInfoFile(ErrFileName); }
    void setBadFileName(int PmId, const std::string& BadFileName)
    {  	fWeSplClients[PmId]->setBadDataFile(BadFileName);    }

	void setDisconnectFailure(bool Flag);
	bool getDisconnectFailure(){ return fDisconnectFailure; }

public:	// for multi-table support
    WESplitterApp& fRef;
    Log fLog;                     // logger

private:
    unsigned int fQId;
    joblist::ResourceManager fRm;
    oam::Oam fOam;
    oam::ModuleTypeConfig fModuleTypeConfig;
    int fDebugLvl;
    int fPmCount;

    int64_t fTableLock;
    int32_t fTableOId;
    uint32_t fFixedBinaryRecLen;

    boost::mutex fRespMutex;
    boost::condition fRespCond;

    boost::mutex fSendMutex;

    // It could be a queue too. Stores all the responses from PMs
    typedef std::list<messageqcpp::SBS> WESRespList;
    WESRespList fRespList;
    // Other member variables
    boost::thread *fpRespThread;

    WEPmList fDataFeedList;
    WEFileReadThread fFileReadThread;

	bool fDisconnectFailure;	//Failure due to disconnect from PM
    bool fForcedFailure;
    bool fAllCpiStarted;
    bool fFirstDataSent;
    unsigned int fFirstPmToSend;
    bool fSelectOtherPm;	// Don't send first data to First PM
    bool fContinue;
    // set of PM specific vector entries
    typedef std::vector<WESplClient*> WESplClients;
    WESplClients fWeSplClients;
    enum { MAX_PMS = 512, MAX_QSIZE=10, MAX_WES_QSIZE=100};

    typedef std::vector<std::string> StrVec;
    StrVec fBrmRptVec;

    BRM::DBRM fDbrm;

    batchloader::BatchLoader* fpBatchLoader;

    unsigned int calcTableRecLen(const std::string& schema,
                                 const std::string table);

    class WEImportRslt
    {
    public:
    	WEImportRslt():fRowsPro(0),fRowsIns(0),fStartTime(),fEndTime(),fTotTime(0){}
    	~WEImportRslt(){}
    public:
    	void reset(){fRowsPro=0; fRowsIns=0; fTotTime=0; fColOorVec.clear();}
    	void updateRowsProcessed(int Rows){ fRowsPro+=Rows; }
    	void updateRowsInserted(int Rows){ fRowsIns+=Rows; }
		void updateColOutOfRangeInfo(int aColNum, CalpontSystemCatalog::ColDataType aColType, 
                                     std::string aColName, int aNoOfOors)
		{
			WEColOorVec::iterator aIt = fColOorVec.begin();
			while(aIt != fColOorVec.end())
			{
				if ((*aIt).fColNum == aColNum)
				{
					(*aIt).fNoOfOORs += aNoOfOors;
					break;
				}
				aIt++;
			}
			if (aIt == fColOorVec.end())
			{
				// First time for aColNum to have out of range count
				WEColOORInfo aColOorInfo;
				aColOorInfo.fColNum = aColNum;
				aColOorInfo.fColType = aColType;
				aColOorInfo.fColName = aColName;
				aColOorInfo.fNoOfOORs = aNoOfOors;
				fColOorVec.push_back(aColOorInfo);
			}
#if 0
			try
			{
				fColOorVec.at(aColNum).fNoOfOORs += aNoOfOors;
			}
			catch (out_of_range& e)
			{
				// First time for aColNum to have out of range count
				WEColOORInfo aColOorInfo;
				aColOorInfo.fColNum = aColNum;
				aColOorInfo.fColName = aColName;
				aColOorInfo.fNoOfOORs = aNoOfOors;
				fColOorVec[aColNum] = aColOorInfo;
			}
#endif
		}
    	void startTimer(){ gettimeofday( &fStartTime, 0 ); }
    	void stopTimer(){ gettimeofday( &fEndTime, 0 ); }
    	float getTotalRunTime()
    	{
    		//fTotTime = (fEndTime>0)?(fEndTime-fStartTime):0;
    		fTotTime = (fEndTime.tv_sec   + (fEndTime.tv_usec   / 1000000.0)) -
    		             (fStartTime.tv_sec + (fStartTime.tv_usec / 1000000.0));
    		return fTotTime;
    	}

    public:
    	int fRowsPro;	//Rows processed
    	int fRowsIns;	//Rows inserted
    	timeval fStartTime;	//StartTime
    	timeval fEndTime;	//EndTime
    	float fTotTime;	//TotalTime
		// A vector containing a list of rows and counts of Out of Range values
		WEColOorVec fColOorVec;	
    };
    WEImportRslt fImportRslt;

    friend class WESplClient;
    friend class WEBrmUpdater;
    friend class WESplitterApp;
    friend class WEFileReadThread;
	friend class WETableLockGrabber;

};
//------------------------------------------------------------------------------

} /* namespace WriteEngine */

#endif /* WE_SPLITTERDATAHANDLER_H_ */
