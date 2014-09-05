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
 * we_splclient.h
 *
 *  Created on: Oct 20, 2011
 *      Author: bpaul
 */

#ifndef WE_SPLCLIENT_H_
#define WE_SPLCLIENT_H_

#include "threadsafequeue.h"
#include "resourcemanager.h"

#include "we_messages.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

namespace WriteEngine
{

class WESplClient;			//forward decleration

// Structure for holding the Out of Range data from the BRMReport
// This class is also used by we_sdhandler to hold the agregate info.
class WEColOORInfo		// Column Out-Of-Range Info
{
public:
    WEColOORInfo():fColNum(0),fColType(CalpontSystemCatalog::INT), fNoOfOORs(0){}
    ~WEColOORInfo(){}
public:
    int fColNum;
    CalpontSystemCatalog::ColDataType fColType;
    std::string fColName;
    int fNoOfOORs;
};
typedef std::vector<WEColOORInfo> WEColOorVec;

//------------------------------------------------------------------------------
class WESdHandlerException: public std::exception
{
public:
    std::string fWhat;
    WESdHandlerException(std::string& What) throw() { fWhat = What; }
    virtual ~WESdHandlerException() throw() {}
    virtual const char* what() const throw()
    {
        return fWhat.c_str();
    }
};
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------


class WESplClientRunner
{
public:
	WESplClientRunner(WESplClient& Sc): fOwner(Sc){ /* ctor */ }
	virtual ~WESplClientRunner(){/* dtor */}
	void operator()();

public:
	WESplClient& fOwner;
};

//------------------------------------------------------------------------------

class WESplClient
{
public:
	WESplClient(WESDHandler& Sdh, int PmId);
	virtual ~WESplClient();

	void setup();
	void startClientThread();
	void sendAndRecv();
	void send();
	void recv();
	void write(const messageqcpp::ByteStream& Msg);
	void read(messageqcpp::SBS& Sbs);
	void add2SendQueue(const messageqcpp::SBS& Sbs);
	void clearSendQueue();
	int getSendQSize();

	void printStats();
	void onConnect();
	void onDisconnect();

    unsigned int getRowTx() const {	return fRowTx; }
    uint32_t getBytesRcv() const { 	return fBytesRcv;   }
    uint32_t getBytesTx()
    {
    	boost::mutex::scoped_lock aLock(fTxMutex);
    	return fBytesTx;
    }
    boost::thread* getFpThread() const  { return fpThread; }
    time_t getLastInTime()
    {
    	boost::mutex::scoped_lock aLock(fLastInMutex);
    	return(fLastInTime>0)?fLastInTime:fStartTime; //BUG 4309
    }
    time_t getStartTime() const  { return fStartTime; }
    time_t getElapsedTime() { return (getLastInTime() - getStartTime()); }
    bool isCpiStarted() const { return fCpiStarted; }
    bool isCpiPassed() const { return fCpiPassed; }
    bool isCpiFailed() const {	return fCpiFailed;  }
    bool isBrmRptRcvd() const {	return fBrmRptRcvd;  }
    int getRollbackRslt() const  {	return fRollbackRslt; }
    int getCleanupRslt() const  { return fCleanupRslt; }
    bool getSendFlag() const { 	return fSend; }
    unsigned int getPmId() const  {	return fPmId; }
    unsigned int getDbRootCnt() const {	return fDbrCnt;  }
    unsigned int getDbRootVar()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	return fDbrVar;
    }
    int getDataRqstCount()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	return fDataRqstCnt;
    }
    long getRdSecTo() const { return fRdSecTo; }
    bool isConnected() const { return fConnected; }
    bool isContinue() const { return fContinue; }
    const std::string& getServer() const { return fServer; }
    const std::string& getIpAddress() const { return fIpAddress;  }
    void setBytesRcv(uint32_t BytesRcv) { fBytesRcv = BytesRcv; }
    void setBytesTx(uint32_t BytesTx)
    {
    	boost::mutex::scoped_lock aLock(fTxMutex);
    	BytesTx = BytesTx;
    	aLock.unlock();
    }
    void updateBytesTx(uint32_t fBytes)
    {
    	boost::mutex::scoped_lock aLock(fTxMutex);
    	fBytesTx += fBytes;
    	aLock.unlock();
    }
    void setConnected(bool Connected) {	fConnected = Connected; }
    void setContinue(bool Continue) { fContinue = Continue; }
    void setFpThread(boost::thread* pThread) { fpThread = pThread; }
    void setLastInTime(time_t LastInTime) {	fLastInTime = LastInTime; }
    void setStartTime(time_t StartTime)
    {
    	boost::mutex::scoped_lock aLock(fLastInMutex);
    	fStartTime = StartTime;
    	aLock.lock();
    }
    void setSendFlag(bool Send) { fSend = Send; }
    void setCpiStarted(bool Start)  { fCpiStarted = Start; }
    void setCpiPassed(bool Pass)
    {
    	setLastInTime(time(0));
    	fCpiPassed = Pass;
    }
    void setCpiFailed(bool Fail)
    {
    	setLastInTime(time(0));
    	fCpiFailed = Fail;
    	fRowsUploadInfo.fRowsRead = 0;
    	fRowsUploadInfo.fRowsInserted = 0;
    }
    void setBrmRptRcvd(bool Rcvd) { fBrmRptRcvd = Rcvd; }
    void setRollbackRslt(int Rslt) { fRollbackRslt = Rslt; }
    void setCleanupRslt(int Rslt) {	fCleanupRslt = Rslt; }
    void setPmId(unsigned int PmId) { fPmId = PmId; }
    void setDbRootCnt(unsigned int DbrCnt) { fDbrCnt = DbrCnt; }
    void resetDbRootVar()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	fDbrVar = fDbrCnt;
    	aLock.unlock();
    }
    void decDbRootVar()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	if(fDbrVar>0) --fDbrVar;
    	aLock.unlock();
    }
    void setRdSecTo(long RdSecTo)
    {
    	fRdSecTo = RdSecTo;
    }
    void setDataRqstCount(int DataRqstCnt)
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	fDataRqstCnt = DataRqstCnt;
    	aLock.unlock();
    }
    void decDataRqstCount()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	if(fDataRqstCnt>0) --fDataRqstCnt;
    	aLock.unlock();
    }
    void incDataRqstCount()
    {
    	boost::mutex::scoped_lock aLock(fDataRqstMutex);
    	++fDataRqstCnt;
    	aLock.unlock();
    }
    void setServer(const std::string& Server) {	fServer = Server; }
    void setIpAddress(const std::string& IpAddr) { fIpAddress = IpAddr; }
    void updateRowTx(unsigned int aCnt) { fRowTx += aCnt; }
    void resetRowTx() { fRowTx = 0; }

private:
    bool fContinue;
    bool fConnected;
    unsigned int fPmId;
    unsigned int fDbrCnt;
    unsigned int fDbrVar;		// Var to keep track next PM to send.
    int fDataRqstCnt;			// Data request count
    long fRdSecTo;				// read timeout sec
    unsigned int fRowTx;		// No. Of Rows Transmitted
    uint32_t fBytesTx;
    uint32_t fBytesRcv;
    time_t fLastInTime;
    time_t fStartTime;
    bool fSend;
    bool fCpiStarted;
    bool fCpiPassed;
    bool fCpiFailed;
    bool fBrmRptRcvd;
    int fRollbackRslt;
    int fCleanupRslt;

    boost::mutex fTxMutex;		//mutex for TxBytes
    boost::mutex fDataRqstMutex;
    boost::mutex fWriteMutex;
    boost::mutex fSentQMutex;
    boost::mutex fLastInMutex;
    typedef std::queue<messageqcpp::SBS> WESendQueue;
    WESendQueue fSendQueue;

    std::string fServer;
    std::string fIpAddress;
    boost::shared_ptr<messageqcpp::MessageQueueClient> fClnt;
    boost::thread *fpThread;
    WESDHandler& fOwner;

    class WERowsUploadInfo
    {
    public:
    	WERowsUploadInfo():fRowsRead(0),fRowsInserted(0){}
    	~WERowsUploadInfo(){}
    public:
    	int fRowsRead;
    	int fRowsInserted;
    };
    WERowsUploadInfo fRowsUploadInfo;
    WEColOorVec fColOorVec;
    std::string fBadDataFile;
    std::string fErrInfoFile;

    void setRowsUploadInfo(int RowsRead, int RowsInserted);
    void add2ColOutOfRangeInfo(int ColNum, 
                               CalpontSystemCatalog::ColDataType ColType, 
                               std::string&  ColName, int NoOfOors);
    void setBadDataFile(const std::string& BadDataFile);
    void setErrInfoFile(const std::string& ErrInfoFile);

    friend class WESDHandler;

};
//------------------------------------------------------------------------------

} /* namespace WriteEngine */
#endif /* WE_SPLCLIENT_H_ */
