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

//  $Id: primitivestep.h 9144 2012-12-11 22:42:40Z pleblanc $


/** @file */

#ifndef JOBLIST_PRIMITIVESTEP_H
#define JOBLIST_PRIMITIVESTEP_H

#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <utility>
#include <cassert>
#include <sys/time.h>
#include <set>
#include <map>
#include <stdexcept>
#include <sstream>
#ifndef _MSC_VER
#include <tr1/memory>
#else
#include <memory>
#endif

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "calpontsystemcatalog.h"
#include "calpontselectexecutionplan.h"
#include "brm.h"
#include "parsetree.h"
#include "simplefilter.h"

#include "jobstep.h"
#include "primitivemsg.h"
#include "elementtype.h"
#include "distributedenginecomm.h"
#include "jl_logger.h"
#include "lbidlist.h"
#include "joblisttypes.h"
#include "timestamp.h"
#include "timeset.h"
#include "resourcemanager.h"
#include "joiner.h"
#include "tuplejoiner.h"
#include "rowgroup.h"
#include "rowaggregation.h"
#include "funcexpwrapper.h"

namespace joblist
{

/* Forward decl's to support the batch primitive classes */
struct JobInfo;
class CommandJL;
class ColumnCommandJL;
class DictStepJL;
class BatchPrimitiveProcessorJL;
class pColStep;
class pColScanStep;
class PassThruStep;


typedef boost::shared_ptr<LBIDList> SP_LBIDList;
typedef std::vector<execplan::CalpontSystemCatalog::OID> OIDVector;
typedef std::vector<std::pair<execplan::CalpontSystemCatalog::OID, int> > OIDIntVector;


enum PrimitiveStepType {
			SCAN,
			COLSTEP,
			DICTIONARYSCAN,
			DICTIONARY,
			PASSTHRU,
			AGGRFILTERSTEP
		};


/** @brief class PrimitiveMsg
 *
 */
class PrimitiveMsg
{
public:
    /** @brief virtual void Send method
     */
    virtual void send();
    /** @brief virtual void Receive method
     */
    virtual void receive();
    /** @brief virtual void BuildPrimitiveMessage method
     */
    virtual void buildPrimitiveMessage(ISMPACKETCOMMAND cmd, void* filterValues, void* ridArray);
    virtual void sendPrimitiveMessages();
    virtual void receivePrimitiveMessages();

    PrimitiveMsg() { }

    virtual ~PrimitiveMsg() { }

	uint16_t planFlagsToPrimFlags(uint32_t planFlags);

  private:
};


class pColScanStep;
class pColStep : public JobStep, public PrimitiveMsg
{

	typedef std::pair<int64_t, int64_t> element_t;	

public:
    /** @brief pColStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
	 * @param flushInterval The interval in msgs at which the sending side should 
	 * wait for the receiveing side to catch up.  0 (default) means never.
     */
    pColStep(const JobStepAssociation& in, 
		const JobStepAssociation& out, 
		DistributedEngineComm* ec,
		execplan::CalpontSystemCatalog *syscat,
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		const execplan::CalpontSystemCatalog::ColType& ct,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t verId,
		uint16_t stepId,
		uint32_t statementId,
		ResourceManager& rm,
		uint32_t flushInterval = 0,
		bool isEM = false);
    
    pColStep(const pColScanStep& rhs);

    pColStep(const PassThruStep& rhs);

	virtual ~pColStep();

    /** @brief Starts processing.  Set at least the RID list before calling.
	 * 
	 * Starts processing.  Set at least the RID list before calling this.
     */
    virtual void run();
	/** @brief Sync's the caller with the end of execution.
	 *
	 * Does nothing.  Returns when this instance is finished.
	 */
    virtual void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    
	virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

	virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual const std::string toString() const;
	
	/** @brief Set the step ID for this JobStep.
	 * 
	 * Set the step ID for this JobStep.
	 */
	virtual void stepId(uint16_t stepId) { fStepId = stepId; }
	
	virtual bool isDictCol() const { return fIsDict; };
	bool isExeMgr() const { return isEM; }

    /** @brief Set config parameters for this JobStep.
     *
     * Set the config parameters this JobStep.
     */
    void initializeConfigParms();
	
	/** @brief The main loop for the send-side thread 
	 * 
	 * The main loop for the primitive-issuing thread.  Don't call it directly.
	 */
    void sendPrimitiveMessages();

	/** @brief The main loop for the recv-side thread
	 *
	 * The main loop for the receive-side thread.  Don't call it directly.
	 */
    void receivePrimitiveMessages();

	/** @brief Add a filter.  Use this interface when the column stores anything but 4-byte floats.
 	 * 
	 * Add a filter.  Use this interface when the column stores anything but 4-byte floats.
 	 */
	void addFilter(int8_t COP, int64_t value, uint8_t roundFlag = 0);
	void addFilter(int8_t COP, float value);

	/** @brief Sets the DataList to get RID values from.
	 * 
	 * Sets the DataList to get RID values from.  Filtering by RID distinguishes
	 * this class from pColScan.  Use pColScan if the every RID should be considered; it's
	 * faster at that.
	 */
	void setRidList(DataList<ElementType> *rids);
	
	/** @brief Sets the String DataList to get RID values from.
	 * 
	 * Sets the string DataList to get RID values from.  Filtering by RID distinguishes
	 * this class from pColScan.  Use pColScan if the every RID should be considered; it's
	 * faster at that.
	 */
	void setStrRidList(DataList<StringElementType> *strDl);

	/** @brief Set the binary operator for the filter predicate (BOP_AND or BOP_OR).
	 * 
	 * Set the binary operator for the filter predicate (BOP_AND or BOP_OR).
	 */
	void setBOP(int8_t BOP);

	/** @brief Set the output type.
	 * 
	 * Set the output type (1 = RID, 2 = Token, 3 = Both).
	 */
	void setOutputType(int8_t OutputType);

	/** @brief Set the swallowRows flag.
	 * 
	 * 
	 * If true, no rows will be inserted to the output datalists.
	 */
	void setSwallowRows(const bool swallowRows);

	/** @brief Get the swallowRows flag.
	 * 
	 * 
	 * If true, no rows will be inserted to the output datalists.
	 */
	bool getSwallowRows() const { return fSwallowRows; }

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	
/** @brief Set the DistributedEngineComm object this instance should use
 *
 * Set the DistributedEngineComm object this instance should use
 */
//	void dec(DistributedEngineComm* dec) {
//		if (fDec) fDec->removeQueue(uniqueID);
//		fDec = dec; 
//		if (fDec) fDec->addQueue(uniqueID);
//	 }

//	DistributedEngineComm* dec() const { return fDec; }
	uint32_t sessionId() const { return fSessionId; }
	uint32_t txnId() const { return fTxnId; }
	uint32_t verId() const { return fVerId; }
	uint16_t stepId() const { return fStepId; }
	uint32_t statementId() const { return fStatementId; }
	uint32_t filterCount() const { return fFilterCount; }
	messageqcpp::ByteStream filterString() const { return fFilterString; }
	int8_t BOP() const { return fBOP; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	void appendFilter(const messageqcpp::ByteStream& filter, unsigned count);
	uint flushInterval() const { return fFlushInterval; }
	bool getFeederFlag() const { return isFilterFeeder; }
	
	void setFeederFlag (bool filterFeeder) { isFilterFeeder = filterFeeder; }
	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           () const { return fNumBlksSkipped; }
	ResourceManager& resourceManager() const { return fRm; }

	std::string udfName() const { return fUdfName; };
	void udfName(const std::string& name) { fUdfName = name; }
	SP_LBIDList getlbidList() const { return lbidList;}

	void addFilter(const execplan::Filter* f);
	void appendFilter(const std::vector<const execplan::Filter*>& fs);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }

protected:
	void addFilters();

private:

    /** @brief constructor for completeness
     */
    explicit pColStep();

    /** @brief StartPrimitiveThread
     *  Utility function to start worker thread that sends primitive messages
     */
    void startPrimitiveThread();
    /** @brief StartAggregationThread
     *  Utility function to start worker thread that receives result aggregation from primitive servers
     */
    void startAggregationThread();
	uint64_t getLBID(uint64_t rid, bool& scan);
	uint64_t getFBO(uint64_t lbid);

	ResourceManager& fRm;
    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
	execplan::CalpontSystemCatalog *sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::ColType fColType;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
	uint16_t fStepId;
	uint32_t fStatementId;
	uint32_t fFilterCount;
	int8_t fBOP;
	int8_t fOutputType;
	uint16_t realWidth;
	DataList_t* ridList;
	StrDataList* strRidList;
	messageqcpp::ByteStream fFilterString;
	std::vector<struct BRM::EMEntry> extents;
	uint extentSize, divShift, modMask, ridsPerBlock, rpbShift, blockSizeShift, numExtents;
	uint64_t rpbMask;	
	uint64_t msgsSent, msgsRecvd;
	bool finishedSending, recvWaiting, fIsDict;
	bool isEM;
	int64_t ridCount;
	uint fFlushInterval;

	// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is TRACE_FLAGS::TRACE_NO_ROWS4.  
	// 	      Running with this one will swallow rows at projection.
	bool fSwallowRows;	
	u_int32_t fProjectBlockReqLimit;     // max number of rids to send in a scan
                                     // request to primproc
    u_int32_t fProjectBlockReqThreshold; // min level of rids backlog before
                                     // consumer will tell producer to send
                                     // more rids scan requests to primproc

    volatile bool fStopSending;
    bool isFilterFeeder;
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fNumBlksSkipped;//total number of block scans skipped due to CP
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
	
	BRM::DBRM dbrm;

	boost::shared_ptr<boost::thread> cThread;  //consumer thread
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::mutex mutex;
	boost::condition condvar;
	boost::condition flushed;
	SPJL fLogger;
	SP_LBIDList lbidList;
	std::vector<int> scanFlags; // use to keep track of which extents to eliminate from this step
	uint32_t uniqueID;	
	std::string fUdfName;

	//@bug 2634
    //@bug 3128 change ParseTree* to vector<Filter*>
	std::vector<const execplan::Filter*> fFilters;

	friend class pColScanStep;
	friend class PassThruStep;
	friend class ColumnCommandJL;
	friend class RTSCommandJL;
	friend class BatchPrimitiveStep;
	friend class TupleBPS;
};

/** @brief the pColScan Step
 * 
 *  The most common step which requires no input RID list, but may have value filters applied
 * 
 *  The input association will always be null here so that we can go as soon as the Run function is called
 * 
 *  The StartPrimitiveThread will spawn a new worker thread that will
 *  a) take any input filters and apply them to a primitive message to be sent
 *  b) walk the block resolution manager via an LBID list for the oid
 *  c) send messages to the primitive server as quickly as possible
 */

class pColScanStep : public JobStep, public PrimitiveMsg
{
public:
    /** @brief pColScanStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    pColScanStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	DistributedEngineComm* dec,
	execplan::CalpontSystemCatalog* syscat,
	execplan::CalpontSystemCatalog::OID oid,
	execplan::CalpontSystemCatalog::OID tableOid,
	const execplan::CalpontSystemCatalog::ColType& ct,
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t verId,
	uint16_t stepId,
	uint32_t statementId,
	ResourceManager& rm);

    pColScanStep(const pColStep& rhs);
	~pColScanStep();

    /** @brief Starts processing.
	 * 
	 * Starts processing.
     */
    virtual void run();

	/** @brief Sync's the caller with the end of execution.
	 *
	 * Does nothing.  Returns when this instance is finished.
	 */
    virtual void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }
    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }
    virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual bool isDictCol() const { return fIsDict; };

	/** @brief The main loop for the send-side thread 
	 * 
	 * The main loop for the primitive-issuing thread.  Don't call it directly.
	 */
    void sendPrimitiveMessages();

	/** @brief The main loop for the recv-side thread
	 *
	 * The main loop for the receive-side thread.  Don't call it directly.
	 */
    void receivePrimitiveMessages(uint64_t i = 0);

	/** @brief Add a filter when the column is a 4-byte float type 
	 * 
	 * Add a filter when the column is a 4-byte float type
	 */
	void addFilter(int8_t COP, float value);

	/** @brief Add a filter when the column is anything but a 4-byte float type.
 	 *
	 * Add a filter when the column is anything but a 4-byte float type, including
	 * 8-byte doubles.
	 */
	void addFilter(int8_t COP, int64_t value, uint8_t roundFlag = 0);

	/** @brief Set the binary operator for the filter predicates
 	 *
	 * Set the binary operator for the filter predicates (BOP_AND or BOP_OR).
	 * It is initialized to OR.
	 */
	void setBOP(int8_t BOP);	// AND or OR
	int8_t BOP() const { return fBOP; }

	bool getFeederFlag() const { return isFilterFeeder; }
	
	void setFeederFlag (bool filterFeeder) { isFilterFeeder = filterFeeder; }
	/** @brief Get the string of the filter predicates
 	 *
	 * Get the filter string constructed from the predicates
	 */
    messageqcpp::ByteStream filterString() const { return fFilterString; }

	void setSingleThread(bool b);
	bool getSingleThread() { return fSingleThread; }
	
	/** @brief Set the output type.
	 * 
	 * Set the output type (1 = RID, 2 = Token, 3 = Both).pColScan
	 */
	void setOutputType(int8_t OutputType);
//	DistributedEngineComm* dec() const { return fDec; }
	uint32_t verId() const { return fVerId; }
	uint32_t filterCount() const { return fFilterCount; }
	/** @brief Set the DistributedEngineComm object this instance should use
	 *
	 * Set the DistributedEngineComm object this instance should use
	 */
//	void dec(DistributedEngineComm* dec) {
//		if (fDec) fDec->removeQueue(uniqueID);
//		fDec = dec;
//		if (fDec) fDec->addQueue(uniqueID);
//	}

	/** @brief Set the step ID of this JobStep
	 *
	 * Set the step ID of this JobStep.  XXXPAT: todo: Need to update the DEC as well 
	 * I assume.
	 */
	virtual void stepId(uint16_t stepId) { fStepId = stepId; }
	virtual uint16_t stepId() const { return fStepId; }

    virtual uint32_t sessionId()   const { return fSessionId; }
    virtual uint32_t txnId()       const { return fTxnId; }
    virtual uint32_t statementId() const { return fStatementId; }

	virtual const std::string toString() const;

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	ResourceManager& resourceManager() const { return fRm; }

	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return recvCount;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}
    uint getRidsPerBlock() const {return ridsPerBlock;}

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           () const { return fNumBlksSkipped; }

	std::string udfName() const { return fUdfName; };
	void udfName(const std::string& name) { fUdfName = name; }

	SP_LBIDList getlbidList() const { return lbidList;}

	void addFilter(const execplan::Filter* f);
	void appendFilter(const std::vector<const execplan::Filter*>& fs);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }
	const std::vector<const execplan::Filter*>& getFilters() const { return fFilters; }

protected:
	void addFilters();

private:
    //defaults okay?
    //pColScanStep(const pColScanStep& rhs);
    //pColScanStep& operator=(const pColScanStep& rhs);

    typedef boost::shared_ptr<boost::thread> SPTHD;
    void startPrimitiveThread();
    void startAggregationThread();
    void initializeConfigParms();
    void sendAPrimitiveMessage (
		ISMPacketHeader& ism,
		BRM::LBID_t msgLbidStart,
		u_int32_t msgLbidCount);
	uint64_t getFBO(uint64_t lbid);
	bool isEmptyVal(const u_int8_t *val8) const;

	ResourceManager& fRm;
    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
    uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
    uint16_t fStepId;
	uint32_t fStatementId;
    ColByScanRangeRequestHeader fMsgHeader;
    SPTHD fConsumerThread;
    /// number of threads on the receive side
	uint fNumThreads;
	
    SPTHD * fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::ColType fColType;
	int8_t fBOP;
	int8_t fOutputType;
    uint32_t sentCount;
	uint32_t recvCount; 
	BRM::LBIDRange_v lbidRanges;
	uint32_t hwm;
	BRM::DBRM dbrm;
	SP_LBIDList lbidList;

	boost::mutex mutex;
	boost::mutex dlMutex;
	boost::mutex cpMutex;
	boost::condition condvar;
	boost::condition condvarWakeupProducer;
	bool finishedSending, sendWaiting, rDoNothing, fIsDict;
	uint recvWaiting, recvExited;
	uint64_t ridsReturned;

	std::vector<struct BRM::EMEntry> extents;
	uint extentSize, divShift, ridsPerBlock, rpbShift, numExtents;
	SPJL fLogger;
// 	config::Config *fConfig;

	u_int32_t fScanLbidReqLimit;     // max number of LBIDs to send in a scan
                                     // request to primproc
	u_int32_t fScanLbidReqThreshold; // min level of scan LBID backlog before
                                     // consumer will tell producer to send
                                     // more LBID scan requests to primproc

	bool fStopSending;
	bool fSingleThread;
	bool isFilterFeeder;
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fNumBlksSkipped;//total number of block scans skipped due to CP
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
    uint32_t fMsgsToPm;     // total number of messages sent to PMs
	uint32_t uniqueID;
	std::string fUdfName;

	//@bug 2634
    //@bug 3128 change ParseTree* to vector<Filter*>
	std::vector<const execplan::Filter*> fFilters;

	friend class ColumnCommandJL;
	friend class BatchPrimitiveProcessorJL;
	friend class BucketReuseStep;
	friend class BatchPrimitiveStep;
	friend class TupleBPS;
};


#if 0
class pIdxStep : public JobStep
{
public:
    /** @brief pIdxStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    pIdxStep(JobStepAssociation *in, JobStepAssociation *out, DistributedEngineComm *ec);
    /** @brief virtual void Run method
     */
    virtual void run();
private:
    pIdxStep();
    void startPrimitveThread();
    void startAggregationThread();

protected:
    DistributedEngineComm* fDec;
    JobStepAssociation* fInputJobStepAssociation;
    JobStepAssociation* fOutputJobStepAssociation;
};
#endif

/** @brief class pDictionaryStep
 *
 */
class pDictionaryStep : public JobStep, public PrimitiveMsg
{

public:
    /** @brief pDictionaryStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    
    pDictionaryStep(const JobStepAssociation& in, 
		const JobStepAssociation& out, 
		DistributedEngineComm* ec,
		execplan::CalpontSystemCatalog *syscat,
		execplan::CalpontSystemCatalog::OID oid,
		int ct,
		execplan::CalpontSystemCatalog::OID tabelOid,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t verId,
		uint16_t stepId,
		uint32_t statementId,
		ResourceManager& rm,
		uint32_t interval=0);

	virtual ~pDictionaryStep();

    /** @brief virtual void Run method
     */
    virtual void run();
	virtual void join();
	//void setOutList(StringDataList* rids);
	void setInputList(DataList_t* rids);
	void setBOP(int8_t b);
	void sendPrimitiveMessages();
	void receivePrimitiveMessages();

    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    
	virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

	virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual const std::string toString() const;

	void stepId(uint16_t stepId) { fStepId = stepId; }
    uint16_t stepId() const { return fStepId; }

    uint32_t sessionId()   const { return fSessionId; }
    uint32_t txnId()       const { return fTxnId; }
    uint32_t statementId() const { return fStatementId; }
//    DistributedEngineComm* dec() const { return fDec; }
	uint32_t verId() const { return fVerId; }
	execplan::CalpontSystemCatalog::ColType& colType() { return fColType; }
	execplan::CalpontSystemCatalog::ColType colType() const { return fColType; }

/** @brief Set the DistributedEngineComm object this instance should use
 *
 * Set the DistributedEngineComm object this instance should use
 */
//	void dec(DistributedEngineComm* dec) {
//		if (fDec) fDec->removeQueue(uniqueID);
//		fDec = dec; 
//		if (fDec) fDec->addQueue(uniqueID);
//	 }

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}
	void addFilter(int8_t COP, const std::string& value);
	uint32_t filterCount() const { return fFilterCount; }
	messageqcpp::ByteStream filterString() const { return fFilterString; }

	// @bug3321, add filters into pDictionary
	void appendFilter(const messageqcpp::ByteStream& filter, unsigned count);
	void addFilter(const execplan::Filter* f);
	void appendFilter(const std::vector<const execplan::Filter*>& fs);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }
	int8_t BOP() const { return fBOP; }

private:
    pDictionaryStep();
    void startPrimitiveThread();
    void startAggregationThread();

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
	execplan::CalpontSystemCatalog* sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
	uint32_t fStepId;
	uint32_t fStatementId;
	uint32_t fBOP;
	uint32_t msgsSent;
	uint32_t msgsRecvd;
	uint32_t finishedSending;
	uint32_t recvWaiting;
	int64_t ridCount;
	execplan::CalpontSystemCatalog::ColType fColType;
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::shared_ptr<boost::thread> cThread;  //producer thread

	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;

	DataList_t* requestList;
	//StringDataList* stringList;
	boost::mutex mutex;
	boost::condition condvar;
	uint32_t fInterval;
	SPJL fLogger;
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
	uint32_t uniqueID;
	ResourceManager& fRm;

    //@bug 3128 change ParseTree* to vector<Filter*>
	std::vector<const execplan::Filter*> fFilters;
	
	bool hasEqualityFilter;
	int8_t tmpCOP;
	std::vector<std::string> eqFilter;

	friend class DictStepJL;
	friend class RTSCommandJL;
	friend class BucketReuseStep;
	friend class BatchPrimitiveStep;
	friend class TupleBPS;
};

/** @brief class pDictionaryScan
 *
 */
class pDictionaryScan : public JobStep, public PrimitiveMsg
{

//typedef StringElementType DictionaryStepElement_t;
//typedef StringDataList DictionaryDataList;

public:

    /** @brief pDictionaryScan constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    
    pDictionaryScan(const JobStepAssociation& in, 
		const JobStepAssociation& out, 
		DistributedEngineComm* ec,
		execplan::CalpontSystemCatalog *syscat,
		execplan::CalpontSystemCatalog::OID oid,
		int ct,
		execplan::CalpontSystemCatalog::OID tableOid,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t verId,
		uint16_t step,
		uint32_t statementId,
		ResourceManager& rm);

    ~pDictionaryScan();

    /** @brief virtual void Run method
     */
    virtual void run();
	virtual void join();
	//void setOutList(StringDataList* rids);
	void setInputList(DataList_t* rids);
	void setBOP(int8_t b);
	void sendPrimitiveMessages();
	void receivePrimitiveMessages();
	void setSingleThread();
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    
	virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

	virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual const std::string toString() const;

	virtual void stepId(uint16_t step) { fStepId = step; }

    virtual uint16_t stepId() const { return fStepId; }

    virtual uint32_t sessionId()   const { return fSessionId; }
    virtual uint32_t txnId()       const { return fTxnId; }
    virtual uint32_t statementId() const { return fStatementId; }

	void setRidList(DataList<ElementType> *rids);

	/** @brief Add a filter.  Use this interface when the column stores anything but 4-byte floats.
 	 * 
	 * Add a filter.  Use this interface when the column stores anything but 4-byte floats.
 	 */
	void addFilter(int8_t COP, const std::string& value);  // all but FLOATS can use this interface

/** @brief Set the DistributedEngineComm object this instance should use
 *
 * Set the DistributedEngineComm object this instance should use
 */
	void dec(DistributedEngineComm* dec) {
		if (fDec) fDec->removeQueue(uniqueID);
		fDec = dec; 
		if (fDec) fDec->addQueue(uniqueID); 
	}

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	void logger(const SPJL& logger) { fLogger = logger; }

	uint64_t phyIOCount    () const { return fPhysicalIO; }
	uint64_t cacheIOCount  () const { return fCacheIO;    }
	uint64_t msgsRcvdCount () const { return msgsRecvd;   }
	uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	uint64_t msgBytesOut   () const { return fMsgBytesOut;}

	BPSOutputType getOutputType() const { return fOutType; }
	void getOutputType(BPSOutputType ot) { fOutType = ot; }
	void setOutputRowGroup(const rowgroup::RowGroup& rg) { fOutputRowGroup = rg; }
	const rowgroup::RowGroup& getOutputRowGroup() const { return fOutputRowGroup; }

	// @bug3321, add interface for combining filters.
	int8_t BOP() const { return fBOP; }
	void addFilter(const execplan::Filter* f);
	void appendFilter(const std::vector<const execplan::Filter*>& fs);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }
	messageqcpp::ByteStream filterString() const { return fFilterString; }
	uint32_t filterCount() const { return fFilterCount; }
	void appendFilter(const messageqcpp::ByteStream& filter, unsigned count);

	virtual void abort();

protected:
	void sendError(uint16_t error);

private:
    pDictionaryScan();
    void startPrimitiveThread();
    void startAggregationThread();
    void initializeConfigParms();
    void sendAPrimitiveMessage(
		messageqcpp::ByteStream& primMsg,
		BRM::LBID_t msgLbidStart,
		uint32_t msgLbidCount, uint16_t dbroot);
	void formatMiniStats();

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
	execplan::CalpontSystemCatalog* sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	BRM::HWM_t fDictBlkCount;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
	uint32_t fStepId;
	uint32_t fStatementId;
	uint32_t fFilterCount;
	uint32_t fBOP;
	uint32_t fCOP1;
	uint32_t fCOP2;
	uint32_t msgsSent;
	uint32_t msgsRecvd;
	uint32_t finishedSending;
	uint32_t recvWaiting;
	uint32_t sendWaiting;
	int64_t  ridCount;
	u_int32_t fLogicalBlocksPerScan;
	DataList<ElementType> *ridList;
	messageqcpp::ByteStream fFilterString;
	execplan::CalpontSystemCatalog::ColType colType;
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::shared_ptr<boost::thread> cThread;  //producer thread
	DataList_t* requestList;
	//StringDataList* stringList;
	boost::mutex mutex;
	boost::condition condvar;
	boost::condition condvarWakeupProducer;
	BRM::LBIDRange_v fDictlbids;
	std::vector<struct BRM::EMEntry> extents;
	uint64_t extentSize;
	uint64_t divShift;
	uint64_t numExtents;
	SPJL fLogger;
	u_int32_t fScanLbidReqLimit;     // max number of LBIDs to send in a scan
                                     // request to primproc
	u_int32_t fScanLbidReqThreshold; // min level of scan LBID backlog before
                                     // consumer will tell producer to send
	bool fStopSending;
	bool fSingleThread;
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
    uint32_t fMsgsToPm;     // total number of messages sent to PMs
	uint32_t uniqueID;
	ResourceManager& fRm;
	BPSOutputType fOutType;
	rowgroup::RowGroup fOutputRowGroup;
	uint64_t fRidResults;

	//@bug 2634
    //@bug 3128 change ParseTree* to vector<Filter*>
	std::vector<const execplan::Filter*> fFilters;

	bool isEquality;
	std::vector<std::string> equalityFilter;
	void serializeEqualityFilter();
	void destroyEqualityFilter();
};

class BatchPrimitive : public JobStep, public PrimitiveMsg, public DECEventListener
{
public:

	virtual bool getFeederFlag() const = 0;
	virtual execplan::CalpontSystemCatalog::OID getLastOid() const = 0;
	virtual uint getStepCount () const = 0;
	virtual void setBPP(JobStep* jobStep) = 0;
	virtual void setFirstStepType(PrimitiveStepType firstStepType) = 0;
	virtual void setIsProjectionOnly() = 0;
	virtual void setLastOid(execplan::CalpontSystemCatalog::OID colOid) = 0;
	virtual void setOutputType(BPSOutputType outputType) = 0;
	virtual void setProjectBPP(JobStep* jobStep1, JobStep* jobStep2) = 0;
	virtual void setStepCount() = 0;
	virtual void setSwallowRows(const bool swallowRows) = 0;
	virtual void setBppStep() = 0;
	virtual void dec(DistributedEngineComm* dec) = 0;
	virtual const OIDVector& getProjectOids() const = 0;
	virtual uint64_t blksSkipped() const = 0;
	virtual bool wasStepRun() const = 0;
	virtual BPSOutputType getOutputType() const = 0;
	virtual uint64_t getRows() const = 0;
	virtual void useJoiner(boost::shared_ptr<joiner::Joiner>) = 0;
	virtual void setJobInfo(const JobInfo* jobInfo) = 0;
	virtual void setOutputRowGroup(const rowgroup::RowGroup& rg) = 0;
	virtual const rowgroup::RowGroup& getOutputRowGroup() const = 0;
	virtual void addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>& fe) = 0;
	virtual void setFE1Input(const rowgroup::RowGroup& feInput) = 0;
	virtual void setFcnExpGroup3(const std::vector<execplan::SRCP>& fe) = 0;
	virtual void setFE23Output(const rowgroup::RowGroup& rg) = 0;


};


/** @brief class TupleBPS
 *
 */
class TupleBPS : public BatchPrimitive, public TupleDeliveryStep
{
public:
	TupleBPS(const pColStep& rhs);
	TupleBPS(const pColScanStep& rhs);
	TupleBPS(const pDictionaryStep& rhs);
	TupleBPS(const pDictionaryScan& rhs);
	TupleBPS(const PassThruStep& rhs);
	virtual ~TupleBPS();

    /** @brief Starts processing.
	 * 
	 * Starts processing.
     */
    virtual void run();
	/** @brief Sync's the caller with the end of execution.
	 *
	 * Does nothing.  Returns when this instance is finished.
	 */
    virtual void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }
    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }
    virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual void abort();
	void abort_nolock();

	/** @brief The main loop for the send-side thread 
	 * 
	 * The main loop for the primitive-issuing thread.  Don't call it directly.
	 */
    void sendPrimitiveMessages();

	/** @brief The main loop for the recv-side thread
	 *
	 * The main loop for the receive-side thread.  Don't call it directly.
	 */
    void receiveMultiPrimitiveMessages(uint threadID);

/** @brief Add a filter when the column is anything but a 4-byte float type.
 *
 * Add a filter when the column is anything but a 4-byte float type, including
 * 8-byte doubles.
 */
	void setBPP(JobStep* jobStep);
	void setProjectBPP(JobStep* jobStep1, JobStep* jobStep2);
	bool scanit(uint64_t rid);
	void storeCasualPartitionInfo(const bool estimateRowCounts);
	bool getFeederFlag() const { return isFilterFeeder; }
	void setFeederFlag (bool filterFeeder) { isFilterFeeder = filterFeeder; }
	void setSwallowRows(const bool swallowRows) {fSwallowRows = swallowRows; }
	bool getSwallowRows() const { return fSwallowRows; }
	void setIsDelivery(bool b) { isDelivery = b; }

	/* Base class interface fcn that can go away */
	void setOutputType(BPSOutputType) { } //Can't change the ot of a TupleBPS
	BPSOutputType getOutputType() const { return ROW_GROUP; }
	void setBppStep() { }
	void setIsProjectionOnly() { }

	uint64_t getRows() const { return rowsReturned; }
	void setFirstStepType(PrimitiveStepType firstStepType) { ffirstStepType = firstStepType;}
	PrimitiveStepType getPrimitiveStepType () { return ffirstStepType; }
	void setStepCount() { fStepCount++; }
	uint getStepCount () const { return fStepCount; }
	void setLastOid(execplan::CalpontSystemCatalog::OID colOid) { fLastOid = colOid; }
	execplan::CalpontSystemCatalog::OID getLastOid() const { return fLastOid; }

/** @brief Set the DistributedEngineComm object this instance should use
 *
 * Set the DistributedEngineComm object this instance should use
 */
	void dec(DistributedEngineComm* dec);

/** @brief Set the step ID of this JobStep
 *
 * Set the step ID of this JobStep.  XXXPAT: todo: Need to update the DEC as well 
 * I assume.
 */
	virtual void stepId(uint16_t stepId);
	virtual uint16_t stepId() const { return fStepId; }

    virtual uint32_t sessionId()   const { return fSessionId; }
    virtual uint32_t txnId()       const { return fTxnId; }
    virtual uint32_t statementId() const { return fStatementId; }

	virtual const std::string toString() const;

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	const OIDVector& getProjectOids() const { return projectOids; }
	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}
	virtual uint64_t blockTouched  () const { return fBlockTouched;}
	uint nextBand(messageqcpp::ByteStream &bs);

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           () const { return fNumBlksSkipped; }

	uint32_t getUniqueID() { return uniqueID; }
	void useJoiner(boost::shared_ptr<joiner::Joiner>);
	void useJoiner(boost::shared_ptr<joiner::TupleJoiner>);
	void useJoiners(const std::vector<boost::shared_ptr<joiner::TupleJoiner> > &);
	bool wasStepRun() const { return fRunExecuted; }

	// DEC event listener interface
	void newPMOnline(uint connectionNumber);

	void setInputRowGroup(const rowgroup::RowGroup &rg);
	void setOutputRowGroup(const rowgroup::RowGroup &rg);
	const rowgroup::RowGroup & getOutputRowGroup() const;

	void setAggregateStep(const rowgroup::SP_ROWAGG_PM_t& agg, const rowgroup::RowGroup &rg);

	/* This is called by TupleHashJoin only */
	void setJoinedResultRG(const rowgroup::RowGroup &rg);

	/* OR hacks */
	void setBOP(uint8_t op);  // BOP_AND or BOP_OR
	uint8_t getBOP() { return bop; }

	void setJobInfo(const JobInfo* jobInfo);

    // @bug 2123.  Added getEstimatedRowCount function.   
	/* @brief estimates the number of rows that will be returned for use in determining the 
	*  large side table for hashjoins.
	*/
    uint64_t getEstimatedRowCount();

	/* Functions & Expressions support.
		Group 1 is for single-table filters only at the moment.  Group 1 objects
		are registered by JLF on the TBPS object directly because there is no join
		involved.

		Group 2 is for cross-table filters only and should be registered on the
		join instance by the JLF.  When the query starts running, the join object
		decides whether the Group 2 instance should run on the PM and UM, then
		registers it with the TBPS.

		Group 3 is for selected columns whether or not its calculation is single-
		table or cross-table.  If it's single-table and there's no join instance,
		JLF should register that object with the TBPS for that table.  If it's
		cross-table, then JLF should register it with the join step.
	*/
	void addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>& fe);
	void setFE1Input(const rowgroup::RowGroup& feInput);

	/* for use by the THJS only... */
	void setFcnExpGroup2(const boost::shared_ptr<funcexp::FuncExpWrapper>& fe2,
	  const rowgroup::RowGroup &output, bool runFE2onPM);

	/* Functions & Expressions in select and groupby clause.
	JLF should use these only if there isn't a join.  If there is, call the
	equivalent fcn on THJS instead */
	void setFcnExpGroup3(const std::vector<execplan::SRCP>& fe);
	void setFE23Output(const rowgroup::RowGroup& rg);
	bool hasFcnExpGroup3() { return (fe2 != NULL); }

	// rowgroup to connector
	const rowgroup::RowGroup& getDeliveredRowGroup() const;

	/* Interface for adding add'l predicates for casual partitioning.
	 * This fcn checks for any intersection between the values in vals
	 * and the range of a given extent.  If there is no intersection, that extent
	 * won't be processed.  For every extent in OID, it effectively calculates
	 * ((vals[0] >= min && vals[0] <= max) || (vals[1] >= min && vals[1] <= max)) ... 
	 * && (previous calculation for that extent).
	 * Note that it is an adder not a setter.  For an extent to be scanned, all calls
	 * must have a non-empty intersection.
	 */
	void addCPPredicates(uint32_t OID, const std::vector<int64_t> &vals, bool isRange);
	
    /* semijoin adds */
	void setJoinFERG(const rowgroup::RowGroup &rg);

	/* To cover over the race between creating extents in each column.  Mitigates
	 * bug 3607.*/
	bool goodExtentCount();
	void reloadExtentLists();
	void initExtentMarkers();   // need a better name for this

protected:
	void sendError(uint16_t status);
	void processError(const std::string& ex, uint16_t err, const std::string& src);

private:
	void formatMiniStats();

    typedef boost::shared_ptr<boost::thread> SPTHD;
	typedef boost::shared_array<SPTHD> SATHD;
    void startPrimitiveThread();
    void startAggregationThreads();
    void initializeConfigParms();
	uint64_t getFBO(uint64_t lbid);
	void checkDupOutputColumns(const rowgroup::RowGroup &rg);
	void dupOutputColumns(rowgroup::RowGroup&);
	void dupOutputColumns(boost::shared_array<uint8_t>&, rowgroup::RowGroup&);
	void rgDataToDl(boost::shared_array<uint8_t>&, rowgroup::RowGroup&, RowGroupDL*);
	void rgDataVecToDl(std::vector<boost::shared_array<uint8_t> >&, rowgroup::RowGroup&, RowGroupDL*);

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
    boost::shared_ptr<BatchPrimitiveProcessorJL> fBPP;
	uint rowCount;
    uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
    uint16_t fStepId;
	uint32_t fStatementId;
	uint16_t fNumSteps;
	int fColWidth;
	uint fStepCount;
    bool fCPEvaluated;  // @bug 2123
	uint64_t fEstimatedRows; // @bug 2123
    /// number of threads on the receive side
	uint fNumThreads;
	PrimitiveStepType ffirstStepType;
	bool isFilterFeeder;
    SATHD fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;
	execplan::CalpontSystemCatalog::ColType fColType;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::OID fLastOid;
	BRM::LBIDRange_v lbidRanges;
	boost::scoped_array<int32_t> hwm;
	std::vector<int32_t> lastExtent;
	std::vector<BRM::LBID_t> lastScannedLBID;
	BRM::DBRM dbrm;
    SP_LBIDList lbidList;
	uint64_t ridsRequested;
	volatile uint64_t msgsSent, msgsRecvd;
	volatile bool finishedSending;
	bool firstRead, sendWaiting;
	uint32_t recvWaiting;
	uint recvExited;
	uint64_t ridsReturned;
	uint64_t rowsReturned;
	std::map<execplan::CalpontSystemCatalog::OID, std::tr1::unordered_map<int64_t, struct BRM::EMEntry> > extentsMap;
	std::vector<BRM::EMEntry> scannedExtents;
	OIDVector projectOids;
	uint extentSize, divShift, rpbShift, numExtents, modMask;
	SPJL fLogger;
// 	config::Config *fConfig;
	u_int32_t fRequestSize; // the number of logical extents per batch of requests sent to PrimProc.
	u_int32_t fProcessorThreadsPerScan; // The number of messages sent per logical extent. 
	bool fSwallowRows;	
    u_int32_t fMaxOutstandingRequests; // The number of logical extents have not processed by PrimProc
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fNumBlksSkipped;//total number of block scans skipped due to CP
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
	uint64_t fBlockTouched; // total blocks touched
    uint32_t fMsgBppsToPm;  // total number of BPP messages sent to PMs
    uint32_t fExtentRows;  // config value of ExtentMap/ExtentRows
    boost::shared_ptr<boost::thread> cThread;  //consumer thread
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::mutex mutex;
	boost::mutex dlMutex;
	boost::mutex cpMutex;
	boost::condition condvarWakeupProducer, condvar;
	bool isDelivery;
	
	std::vector<bool> scanFlags; // use to keep track of which extents to eliminate from this step
	bool BPPIsAllocated;
	uint32_t uniqueID;
	ResourceManager& fRm;

	/* HashJoin support */

	void serializeJoiner();
	void serializeJoiner(uint connectionNumber);

	void generateJoinResultSet(const std::vector<std::vector<uint8_t *> > &joinerOutput,
	  rowgroup::Row &baseRow, const std::vector<boost::shared_array<int> > &mappings,
	  const uint depth, rowgroup::RowGroup &outputRG, boost::shared_array<uint8_t> &rgData,
	  std::vector<boost::shared_array<uint8_t> > *outputData,
	  const boost::scoped_array<rowgroup::Row> &smallRows, rowgroup::Row &joinedRow);

	std::vector<boost::shared_ptr<joiner::TupleJoiner> > tjoiners;
	bool doJoin, hasPMJoin, hasUMJoin;
	std::vector<rowgroup::RowGroup> joinerMatchesRGs;   // parses the small-side matches from joiner

	uint smallSideCount;
	int  smallOuterJoiner;

	bool fRunExecuted; // was the run method executed for this step
	rowgroup::RowGroup inputRowGroup;   // for parsing the data read from the datalist
	rowgroup::RowGroup primRowGroup;    // for parsing the data received from the PM
	rowgroup::RowGroup outputRowGroup;  // if there's a join, these are the joined
										// result, otherwise it's = to primRowGroup
	// aggregation support
	rowgroup::SP_ROWAGG_PM_t fAggregatorPm;
	rowgroup::RowGroup       fAggRowGroupPm;

	// OR hacks
	uint8_t bop; 		// BOP_AND or BOP_OR

	// temporary hack to make sure JobList only calls run and join once
	boost::mutex jlLock;
	bool runRan, joinRan;

	// bug 1965, trace duplicat columns in delivery list <dest, src>
	std::vector<std::pair<uint, uint> > dupColumns;

	/* Functions & Expressions vars */
	boost::shared_ptr<funcexp::FuncExpWrapper> fe1, fe2;
	rowgroup::RowGroup fe1Input, fe2Output;
	boost::shared_array<int> fe2Mapping;
	bool runFEonPM;

	/* for UM F & E 2 processing */
	boost::scoped_array<uint8_t> fe2Data;
	rowgroup::Row fe2InRow, fe2OutRow;
	
	void processFE2(rowgroup::RowGroup &input, rowgroup::RowGroup &output,
	  rowgroup::Row &inRow, rowgroup::Row &outRow,
	  std::vector<boost::shared_array<uint8_t> > *rgData,
	  funcexp::FuncExpWrapper* localFE2);
	void processFE2_oneRG(rowgroup::RowGroup &input, rowgroup::RowGroup &output,
	  rowgroup::Row &inRow, rowgroup::Row &outRow,
	  funcexp::FuncExpWrapper* localFE2);
	  
	/* Runtime Casual Partitioning adjustments.  The CP code is needlessly complicated;
	 * to avoid making it worse, decided to designate 'scanFlags' as the static
	 * component and this new array as the runtime component.  The final CP decision
	 * is scanFlags & runtimeCP.
	 */
	std::vector<bool> runtimeCPFlags;
	
	/* semijoin vars */
	rowgroup::RowGroup joinFERG;

	/* shared nothing support */
	struct Job {
		Job(uint d, uint n, uint b, boost::shared_ptr<messageqcpp::ByteStream> &bs) :
			dbroot(d), connectionNum(n), expectedResponses(b), msg(bs) { }
		uint dbroot;
		uint connectionNum;
		uint expectedResponses;
		boost::shared_ptr<messageqcpp::ByteStream> msg;
	};

	void prepCasualPartitioning();
	void makeJobs(std::vector<Job> *jobs);
	void interleaveJobs(std::vector<Job> *jobs) const;
	void sendJobs(const std::vector<Job> &jobs);
	uint numDBRoots;

};

/** @brief class FilterStep
 *
 */
class FilterStep : public JobStep
{
public:

    FilterStep(uint32_t sessionId,
		uint32_t txnId,
		uint32_t statementId,
		execplan::CalpontSystemCatalog::ColType colType);
    ~FilterStep();

    /** @brief virtual void Run method
     */
    void run();
    void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }
    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }
    void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

    void stepId(uint16_t stepId) { fStepId = stepId; }
    virtual uint16_t stepId() const { return fStepId; }

    virtual uint32_t sessionId()   const { return fSessionId; }
    virtual uint32_t txnId()       const { return fTxnId; }
    virtual uint32_t statementId() const { return fStatementId; }

    const std::string toString() const;

    execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID; }
    void tableOid(execplan::CalpontSystemCatalog::OID tableOid) { fTableOID = tableOid; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	void logger(const SPJL& logger) { fLogger = logger; }
	void setBOP(int8_t b);
	int8_t BOP() const { return fBOP; }
    friend struct FSRunner;	

	void addFilter(const execplan::Filter* f);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }

protected:
//	void unblockDataLists(FifoDataList* fifo, StringFifoDataList* strFifo, StrDataList* strResult, DataList_t* result);

private:

    //This i/f is not meaningful in this step
    execplan::CalpontSystemCatalog::OID oid() const { return 0; }
    void doFilter();    // @bug 686

// 	config::Config *fConfig;

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	uint32_t fSessionId;
	uint32_t fTxnId;
    uint16_t fStepId;
	uint32_t fStatementId;
    execplan::CalpontSystemCatalog::OID fTableOID;
	execplan::CalpontSystemCatalog::ColType fColType;
    SPJL fLogger;
    int8_t fBOP;
    boost::shared_ptr<boost::thread> runner;    // @bug 686

	// @bug 687 Add data and friend declarations for concurrent filter steps.
	std::vector<ElementType> fSortedA; // used to internally sort input data
	std::vector<ElementType> fSortedB;
//	FifoDataList* fFAp;                // Used to internally pass data to
//	FifoDataList* fFBp;                // FilterOperation thread.
	uint64_t  resultCount;

	std::vector<const execplan::Filter*> fFilters;
};

/** @brief class PassThruStep
 *
 */
class PassThruStep : public JobStep, public PrimitiveMsg
{

	typedef std::pair<int64_t, int64_t> element_t;	

public:
    /** @brief PassThruStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
	 * wait for the receiveing side to catch up.  0 (default) means never.
     */
    PassThruStep(const JobStepAssociation& in, 
		const JobStepAssociation& out, 
		DistributedEngineComm* dec,
		execplan::CalpontSystemCatalog::ColType colType,
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t verId,
		uint16_t stepId,
		uint32_t statementId,
		bool isEM,
		ResourceManager& rm);

	PassThruStep(const pColStep& rhs, bool isEM);

	virtual ~PassThruStep();

    /** @brief Starts processing.  Set at least the RID list before calling.
	 * 
	 * Starts processing.  Set at least the RID list before calling this.
     */
    virtual void run();

	/** @brief Sync's the caller with the end of execution.
	 *
	 * Does nothing.  Returns when this instance is finished.
	 */
    virtual void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    
	virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

	virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	virtual const std::string toString() const;
	
	/** @brief Set the step ID for this JobStep.
	 * 
	 * Set the step ID for this JobStep.
	 */
	virtual void stepId(uint16_t stepId) { fStepId = stepId; }
	
	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	
	DistributedEngineComm* dec() const { return fDec; }
	void dec(DistributedEngineComm* dec) { fDec = dec; }
	uint32_t sessionId() const { return fSessionId; }
	uint32_t txnId() const { return fTxnId; }
	uint32_t verId() const { return fVerId; }
	uint16_t stepId() const { return fStepId; }
	uint32_t statementId() const { return fStatementId; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	uint8_t getColWidth() const { return colWidth; }
	bool isDictCol() const { return isDictColumn; }
	bool isExeMgr() const { return isEM; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	ResourceManager& resourceManager() const { return fRm; }

protected:

private:

    /** @brief constructor for completeness
     */
    explicit PassThruStep();

	uint64_t getLBID(uint64_t rid, bool& scan);
	uint64_t getFBO(uint64_t lbid);

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	execplan::CalpontSystemCatalog *catalog;
	DistributedEngineComm *fDec;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
	uint16_t fStepId;
	uint32_t fStatementId;
	uint8_t colWidth;
	uint16_t realWidth;
	execplan::CalpontSystemCatalog::ColType fColType; 
	bool isDictColumn;
	bool isEM;

	boost::thread* fPTThd;

	// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is TRACE_FLAGS::TRACE_NO_ROWS4.  
	// 	      Running with this one will swallow rows at projection.
	bool fSwallowRows;
	SPJL fLogger;
	ResourceManager& fRm;
	friend class PassThruCommandJL;
	friend class RTSCommandJL;
	friend class BatchPrimitiveStep;
	friend class TupleBPS;
};


}

#endif  // JOBLIST_PRIMITIVESTEP_H
// vim:ts=4 sw=4:


