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

//  $Id: jobstep.h 9142 2012-12-11 22:18:06Z pleblanc $


/** @file */

#ifndef JOBLIST_JOBSTEP_H_
#define JOBLIST_JOBSTEP_H_

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

#include "primitivemsg.h"
#include "elementtype.h"
#include "distributedenginecomm.h"
#include "tableband.h"
#include "jl_logger.h"
#include "lbidlist.h"
#include "joblisttypes.h"
#include "filteroperation.h"
#include "profiling.h"
#include "timestamp.h"
#include "timeset.h"
#include "aggregator.h"
#include "resourcemanager.h"
#include "joiner.h"
#include "tuplejoiner.h"
#include "rowgroup.h"
#include "rowaggregation.h"
#include "funcexpwrapper.h"

#ifdef PROFILE
extern void timespec_sub(const struct timespec &tv1, const struct timespec &tv2,
	struct timespec &diff);
#endif

/* branch prediction macros for gcc.  Is there a better place for them? */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#ifndef __builtin_expect
#define __builtin_expect(x, expected_value) (x)
#endif
#endif

#ifndef LIKELY
#define LIKELY(x)   __builtin_expect((x),1)
#define UNLIKELY(x) __builtin_expect((x),0)
#endif

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

namespace joblist
{

/** @brief enum JobStepType
 *
 */
enum JobStepType
{
    PCOL_STEP,
    PTOKENBYSCAN_STEP,
    PGETSIGNATURE_STEP,
    UNKNOWN_STEP,
};

/** @brief enum OutputType
 *
 */
enum OutputType
{
    UNKNOWN,
    RID,
    TOKEN,
    BOTH,
};

enum FirstStepType {
			SCAN,
			COLSTEP,
			DICTIONARYSCAN,
			DICTIONARY,
			PASSTHRU,
			AGGRFILTERSTEP
		};
		
enum BPSInputType {
			DATALIST,
			FIFODATALIST,
			STRINGDATALIST,
			STRINGFIFODATALIST
					// Need a definition or Tuples
		};			
const int MAX_BUFFER_SIZE = (int)(1024*100);
//const uint32_t defaultFlushInterval = 0x2000;
//const uint32_t defaultFifoSize = 0x80;

typedef uint64_t ridtype_t;
class DeliveryStep;

struct JobInfo;

/** @brief class JobStepAssociation mediator class to connect/control JobSteps and DataLists
 * 
 * Class JobStepAssociation controls JobSteps and DataLists
 */
typedef boost::shared_ptr<uint16_t> Suint16_t;
typedef boost::shared_ptr<LBIDList> SP_LBIDList;

typedef std::vector<execplan::CalpontSystemCatalog::OID> OIDVector;
typedef std::vector<std::pair<execplan::CalpontSystemCatalog::OID, int> > OIDIntVector;

struct ErrorInfo {
	ErrorInfo() : errCode(0) { }
	uint32_t errCode;
	std::string errMsg;
	// for backward compat
	ErrorInfo(uint16_t v) : errCode(v) { }
	ErrorInfo & operator=(uint16_t v) { errCode = v; errMsg.clear(); return *this; }
};

typedef boost::shared_ptr<ErrorInfo> SErrorInfo;

class JobStepAssociation
{
public:
    JobStepAssociation(): fToSort(0)  { }
    JobStepAssociation(SErrorInfo s): fToSort(0), errInfo(s)  { }
    virtual ~JobStepAssociation() {}

	void inAdd(const AnyDataListSPtr& spdl) __attribute__((deprecated)) { fInDataList.push_back(spdl); }
    void outAdd(const AnyDataListSPtr& spdl) { fOutDataList.push_back(spdl); }
    void outAdd(const AnyDataListSPtr& spdl, size_t pos) {
		if (pos > fOutDataList.size()) throw std::logic_error("Insert position is beyond end.");
		fOutDataList.insert(fOutDataList.begin()+pos, spdl); }
    void outAdd(const DataListVec& spdlVec, size_t pos) {
		if (pos > fOutDataList.size()) throw std::logic_error("Insert position is beyond end.");
		fOutDataList.insert(fOutDataList.begin()+pos, spdlVec.begin(), spdlVec.end()); }
    size_t inSize() const __attribute__((deprecated)) { return fInDataList.size(); }
    size_t outSize() const { return fOutDataList.size(); }
    const AnyDataListSPtr& inAt(size_t i) const __attribute__((deprecated)) { return fInDataList.at(i); }
    const AnyDataListSPtr& outAt(size_t i) const { return fOutDataList.at(i); }
    AnyDataListSPtr& outAt(size_t i) { return fOutDataList.at(i); }
    uint8_t toSort() const { return fToSort; }
    void  toSort(uint8_t toSort)  { fToSort = toSort; }
    uint32_t status() const { return errInfo->errCode; }
    void  status(uint32_t s)  { errInfo->errCode = s; }
	std::string errorMessage() { return errInfo->errMsg; }
	void errorMessage(const std::string &s) { errInfo->errMsg = s; }
    const SErrorInfo& statusPtr() const { return errInfo; }
    void statusPtr(SErrorInfo sp) { errInfo = sp; }

private:
    DataListVec fInDataList;
    DataListVec fOutDataList;
    uint8_t fToSort;

	SErrorInfo errInfo;
//    Suint16_t fStatus;

};

typedef boost::shared_ptr<JobStepAssociation> SJSA;
typedef boost::shared_ptr<JobStepAssociation> JobStepAssociationSPtr; 

/* Forward decl's to support the batch primitive classes */
class CommandJL;
class ColumnCommandJL;
class DictStepJL;
class BatchPrimitiveProcessorJL;
class pColStep;
class pColScanStep;
class PassThruStep;

/** @brief class JobStep abstract class describing a query execution step
 * 
 * Class JobStep is an abstract class that describes a query execution step
 */
struct JobStep
{
    /** constructor
     */
    JobStep() : fToSort(0), fTraceFlags(0), fCardinality(0), fError(0),
		fDelayedRunFlag(false), fWaitToRunStepCnt(0), die(false), fTupleId(-1) { }
    /** destructor
     */
    virtual ~JobStep() { /*pthread_mutex_destroy(&mutex);*/ }
    /** @brief virtual void Run method
     */
    virtual void run() = 0;
	virtual void abort();
    /** @brief virtual void join method
     */
    virtual void join() = 0;
    /** @brief virtual JobStepAssociation * inputAssociation method
     */
    virtual const JobStepAssociation& inputAssociation() const = 0;
    virtual void inputAssociation(const JobStepAssociation& inputAssociation) = 0;
    /** @brief virtual JobStepAssociation * outputAssociation method
     */
    virtual const JobStepAssociation& outputAssociation() const = 0;
    virtual void outputAssociation(const JobStepAssociation& outputAssociation) = 0;

    virtual const std::string toString() const = 0;
    virtual void stepId(uint16_t stepId) = 0;
    virtual uint16_t stepId() const = 0;
    virtual uint32_t sessionId()   const = 0;
    virtual uint32_t txnId()       const = 0;
    virtual uint32_t statementId() const = 0;
    virtual bool isDictCol() const { return 0; }
    virtual execplan::CalpontSystemCatalog::OID oid() const { return 0; }
    virtual execplan::CalpontSystemCatalog::OID tableOid() const { return 0; }
    virtual void logger(const SPJL& logger) = 0;
	// @bug 598 Added alias for self-join
    virtual std::string alias() const { return fAlias; }
    virtual void  alias(const std::string& alias)  { fAlias = alias; }
	// @bug 3401 & 3402, view support
    virtual std::string view() const { return fView; }
    virtual void  view(const std::string& vw)  { fView= vw; }
	// @bug 3438, stats with column name
    virtual std::string name() const { return fName; }
    virtual void  name(const std::string& nm)  { fName= nm; }
	// @bug 3398, add columns' unique tuple ID to job step
	virtual uint64_t tupleId() const  { return fTupleId; }
	virtual void tupleId(uint64_t id) { fTupleId = id; }
	//util function: convert eight byte to string
	void intToStr( DataList_t& inList, StringBucketDataList& outList );
    virtual uint8_t toSort() const { return fToSort; }
    virtual void  toSort(uint8_t toSort)  { fToSort = toSort; }

	//...Final I/O blk count, msg rcv count, etc for this job step. These
	//...methods do not use a mutex lock to acquire values, because it is
	//...assumed they are called after all processing is complete.
	virtual uint64_t phyIOCount    ( ) const { return 0; }
	virtual uint64_t cacheIOCount  ( ) const { return 0; }
	virtual uint64_t msgsRcvdCount ( ) const { return 0; }
	virtual uint64_t msgBytesIn    ( ) const { return 0; }
	virtual uint64_t msgBytesOut   ( ) const { return 0; }
	virtual uint64_t blockTouched  ( ) const { return 0;}
	virtual uint64_t cardinality   ( ) const { return fCardinality; }
	virtual void cardinality ( const uint64_t cardinality ) { fCardinality = cardinality; }

	// functions to delay/control jobstep execution; decWaitToRunStepCnt() per-
	// forms atomic decrement op because it is accessed by multiple threads.
	bool     delayedRun() const        { return fDelayedRunFlag; }
	int      waitToRunStepCnt()        { return fWaitToRunStepCnt; }
	void     incWaitToRunStepCnt()     {
				fDelayedRunFlag = true;
				++fWaitToRunStepCnt; }
	int      decWaitToRunStepCnt()     {
#ifndef _MSC_VER
		return __sync_fetch_and_add(&fWaitToRunStepCnt, -1);
#else
		return InterlockedDecrement(&fWaitToRunStepCnt) + 1;
#endif
	}
	void resetDelayedRun() { fDelayedRunFlag = false; fWaitToRunStepCnt = 0; }

	void logEnd(const char* s)
	{
		mutex.lock(); //pthread_mutex_lock(&mutex);
		std::cout << s <<std::endl;
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
	}
	void syslogStartStep(uint32_t subSystem,
		const std::string& stepName) const;
	void syslogEndStep  (uint32_t subSystem,
		uint64_t blockedDLInput,
		uint64_t blockedDLOutput,
		uint64_t msgBytesInput =0,
		uint64_t msgBytesOutput=0 )   const;
	void syslogReadBlockCounts (uint32_t subSystem,
		uint64_t physicalReadCount,
		uint64_t cacheReadCount,
		uint64_t casualPartBlocks )   const;
	void syslogProcessingTimes (uint32_t subSystem,
        const struct timeval&   firstReadTime,
        const struct timeval&   lastReadTime,
        const struct timeval&   firstWriteTime,
        const struct timeval&   lastWriteTime) const;
	void setTrace(bool trace) __attribute__((deprecated));
	inline bool traceOn() const { return fTraceFlags & execplan::CalpontSelectExecutionPlan::TRACE_LOG; }
	void setTraceFlags(uint32_t flags) { fTraceFlags = flags; }
	JSTimeStamp dlTimes;

	const std::string& extendedInfo() const { return fExtendedInfo; }
	const std::string& miniInfo() const { return fMiniInfo; }

protected:
    std::string fAlias;
    std::string fView;
	std::string fName;
    uint8_t fToSort;
	uint32_t fTraceFlags;
	ProfileData fProfileData;
	uint64_t fCardinality;
	bool fError;
	bool fDelayedRunFlag;
#ifdef _MSC_VER
	volatile long fWaitToRunStepCnt;
#else
	_Atomic_word fWaitToRunStepCnt;
#endif
	volatile bool die;
	boost::mutex* fStatsMutexPtr;
	std::string fExtendedInfo;
	std::string fMiniInfo;

	uint64_t fTupleId;

private:
	static boost::mutex mutex;
	friend class CommandJL;
};

std::ostream& operator<<(std::ostream& os, const JobStep* rhs);

typedef boost::shared_ptr<JobStep> SJSTEP;

class TupleJobStep
{
public:
	TupleJobStep() {}
	virtual ~TupleJobStep() {}
	virtual void  setOutputRowGroup(const rowgroup::RowGroup&) = 0;
	virtual void  setFcnExpGroup3(const std::vector<execplan::SRCP>&) {}
	virtual void  setFE23Output(const rowgroup::RowGroup&) {}
	virtual const rowgroup::RowGroup& getOutputRowGroup() const = 0;
};

class TupleDeliveryStep : public TupleJobStep
{
public:
	virtual ~TupleDeliveryStep() { }
	virtual uint nextBand(messageqcpp::ByteStream &bs) = 0;
	virtual const rowgroup::RowGroup& getDeliveredRowGroup() const = 0;
	virtual void setIsDelivery(bool b) = 0;
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

// moved to here from joblistfactory.cpp
/* This is used as a delimiter between 2 sides of an OR statement in the query 
	vector */
class OrDelimiter : public JobStep 
{
	public:
		OrDelimiter() : JobStep() { }
		OrDelimiter(const OrDelimiter &e) : JobStep(e) { }
		virtual ~OrDelimiter() { }
		void run() { }
		void join() { }
		const JobStepAssociation &inputAssociation() const { return inJSA; }
		void inputAssociation(const JobStepAssociation &in) { throw std::logic_error("connecting an OrDelimiter inputAssociation is wrong"); }
		const JobStepAssociation &outputAssociation() const { return outJSA; }
		void outputAssociation(const JobStepAssociation &out) { throw std::logic_error("connecting an OrDelimiter outputAssociation is wrong"); }
		JobStepAssociation& outputAssociation() { return outJSA; }
		const std::string toString() const { return "OrDelimiter"; }
		void stepId(uint16_t sId) { stepID = sId; }
		uint16_t stepId() const { return stepID; }
		uint32_t sessionId()   const { return 0; }
		uint32_t txnId()       const { return 0; }
		uint32_t statementId() const { return 0; }
		void logger(const SPJL& logger) { }
	private: 
		uint16_t stepID;
		JobStepAssociation inJSA, outJSA;
};

// This class is to mark the beginning of left-hand side branch of a "OR" operation
class OrDelimiterLhs : public OrDelimiter 
{
	public:
		const std::string toString() const { return "OrDelimiterLhsBegin"; }
};

// This class is to mark the beginning of right-hand side branch of a "OR" operation
class OrDelimiterRhs : public OrDelimiter 
{
	public:
		const std::string toString() const { return "OrDelimiterRhsBegin"; }
};

/** @brief class pColStep
 *
 */
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
	 * Set the output type( 1 = RID, 2 = Token, 3 = Both ).
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
	void dec(DistributedEngineComm* dec) {
		if (fDec) fDec->removeQueue(uniqueID);
		fDec = dec; 
		if (fDec) fDec->addQueue(uniqueID);
	 }

	DistributedEngineComm* dec() const { return fDec; }
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
	
	void setFeederFlag ( bool filterFeeder ) { isFilterFeeder = filterFeeder; }
	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           ( ) const { return fNumBlksSkipped; }
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
	execplan::CalpontSystemCatalog::ColType fColType;
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
#ifdef PROFILE
	timespec ts1, ts2;
#endif

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
	
	void setFeederFlag ( bool filterFeeder ) { isFilterFeeder = filterFeeder; }
	/** @brief Get the string of the filter predicates
 	 *
	 * Get the filter string constructed from the predicates
	 */
    messageqcpp::ByteStream filterString() const { return fFilterString; }

	void setSingleThread( bool b);
	bool getSingleThread() { return fSingleThread; }
	
	/** @brief Set the output type.
	 * 
	 * Set the output type( 1 = RID, 2 = Token, 3 = Both ).pColScan
	 */
	void setOutputType(int8_t OutputType);
	DistributedEngineComm* dec() const { return fDec; }
	uint32_t verId() const { return fVerId; }
	uint32_t filterCount() const { return fFilterCount; }
	/** @brief Set the DistributedEngineComm object this instance should use
	 *
	 * Set the DistributedEngineComm object this instance should use
	 */
	void dec(DistributedEngineComm* dec) {
		if (fDec) fDec->removeQueue(uniqueID);
		fDec = dec;
		if (fDec) fDec->addQueue(uniqueID);
	}

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

	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return recvCount;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}
    uint getRidsPerBlock() const {return ridsPerBlock;}

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           ( ) const { return fNumBlksSkipped; }

	std::string udfName() const { return fUdfName; };
	void udfName(const std::string& name) { fUdfName = name; }

	SP_LBIDList getlbidList() const { return lbidList;}

	void addFilter(const execplan::Filter* f);
	void appendFilter(const std::vector<const execplan::Filter*>& fs);
	std::vector<const execplan::Filter*>& getFilters() { return fFilters; }

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
	execplan::CalpontSystemCatalog::ColType fColType;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
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
#ifdef PROFILE
	timespec ts1, ts2;
#endif
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

/** @brief class pColAggregateStep
 *
 */
class pColAggregateStep : public JobStep, public PrimitiveMsg
{
public:
    /** @brief pColAggregateStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    pColAggregateStep(JobStepAssociation *in, JobStepAssociation *out, DistributedEngineComm *ec);
    /** @brief virtual void Run method
     */
    virtual void run();
private:
    pColAggregateStep();
    void startPrimitveThread();
    void startAggregationThread();

    DistributedEngineComm* fDec;
    JobStepAssociation* fInputJobStepAssociation;
    JobStepAssociation* fOutputJobStepAssociation;
};

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
    DistributedEngineComm* dec() const { return fDec; }
	uint32_t verId() const { return fVerId; }
	execplan::CalpontSystemCatalog::ColType& colType() { return fColType; }
	execplan::CalpontSystemCatalog::ColType colType() const { return fColType; }

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
	const SPJL& logger() const { return fLogger; }
	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}
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

	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}

	virtual BPSOutputType getOutputType() const { return fOutType; }
	virtual void getOutputType(BPSOutputType ot) { fOutType = ot; }
	virtual void setOutputRowGroup(const rowgroup::RowGroup& rg) { fOutputRowGroup = rg; }
	virtual const rowgroup::RowGroup& getOutputRowGroup() const { return fOutputRowGroup; }

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
	virtual void setFirstStepType(FirstStepType firstStepType) = 0;
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
	virtual const execplan::CalpontSystemCatalog::TableName tableName() const = 0;
	virtual void useJoiner(boost::shared_ptr<joiner::Joiner>) = 0;
	virtual void setJobInfo(const JobInfo* jobInfo) = 0;

};

/** @brief class BatchPrimitiveStep
 *
 */
class BatchPrimitiveStep : public BatchPrimitive
{
public:
	
	
	/** @brief BatchPrimitiveStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
  BatchPrimitiveStep(ResourceManager& rm) : fRm(rm), fUpdatedEnd(false) {}
    BatchPrimitiveStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	DistributedEngineComm* dec,
	ResourceManager& rm);
	
	BatchPrimitiveStep(const pColStep& rhs);
	BatchPrimitiveStep(const pColScanStep& rhs);
	BatchPrimitiveStep(const pDictionaryStep& rhs);
	BatchPrimitiveStep(const pDictionaryScan& rhs);
	BatchPrimitiveStep(const PassThruStep& rhs);
	virtual ~BatchPrimitiveStep();

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
	void setBPP( JobStep* jobStep );
	void setProjectBPP( JobStep* jobStep1, JobStep* jobStep2 );
	void setRidList(DataList<ElementType> *rids);
	void setStrRidList(DataList<StringElementType> *strDl);
	void addFilter(int8_t COP, int64_t value, uint8_t roundFlag = 0);
	void addFilters();
	uint64_t getLBID(uint64_t rid, bool& scan);
	bool scanit(uint64_t rid);
	void storeCasualPartitionInfo();
	bool getFeederFlag() const { return isFilterFeeder; }
	void setFeederFlag ( bool filterFeeder ) { isFilterFeeder = filterFeeder; }
	void setSwallowRows(const bool swallowRows) { fSwallowRows = swallowRows; }
	bool getSwallowRows() const { return fSwallowRows; }

/** @brief Set the binary operator for the filter predicates
 *
 * Set the binary operator for the filter predicates (BOP_AND or BOP_OR).
 * It is initialized to OR.
 */
	void setBOP(int8_t BOP);	// AND or OR
	
/** @brief Set the output type.
	 * 
	 * Set the output type( 1 = RID, 2 = Token, 3 = Both ).
	 */
	void setOutputType(BPSOutputType outputType);
	BPSOutputType getOutputType() const { return fOutputType;}
	uint64_t getRows() const { return rowsReturned; }
	void setFirstStepType ( FirstStepType firstStepType) { ffirstStepType = firstStepType;}
	FirstStepType getFirstStepType () { return ffirstStepType; }
	void setBppStep();
	int getIterator();
	bool getNextElement(const int it);
	uint64_t getAbsRid( uint64_t i);
	uint64_t getRowCount();
	void addElementToBPP(uint64_t i);
	void setStepCount( ) { fStepCount++; }
	uint getStepCount () const { return fStepCount; }
	void setIsProjectionOnly() { fIsProjectionOnly = true; }
	void setLastOid( execplan::CalpontSystemCatalog::OID colOid ) { fLastOid = colOid; }
	execplan::CalpontSystemCatalog::OID getLastOid() const { return fLastOid; }
    //void setFirstStepType( FirstStepType step ) { ffirstStepType = step; }
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
	const execplan::CalpontSystemCatalog::TableName tableName() const { return fTableName; }
	const OIDVector& getProjectOids() const { return projectOids; }
	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}
	virtual uint64_t blockTouched  ( ) const { return fBlockTouched;}
	const TableBand nextBand();
	uint nextBand(messageqcpp::ByteStream &bs);

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           ( ) const { return fNumBlksSkipped; }

	uint32_t getUniqueID() { return uniqueID; }
	void endDatalists();
	void useJoiner(boost::shared_ptr<joiner::Joiner>);


	bool wasStepRun() const { return fRunExecuted; }

	// DEC event listener interface
	void newPMOnline(uint connectionNumber);

	void setJobInfo(const JobInfo* jobInfo);

protected:
	void sendError(uint16_t status);
	void processError(const std::string& ex, uint16_t err, const std::string& src);
private:	
	
    typedef boost::shared_ptr<boost::thread> SPTHD;
	typedef boost::shared_array<SPTHD> SATHD;
    void startPrimitiveThread();
    void startAggregationThread();
    void startAggregationThreads();
    void initializeConfigParms();
	uint64_t getFBO(uint64_t lbid);

    void setEndOfInput(FifoDataList *fifo, StringFifoDataList *strFifo, StrDataList* strDlp, TupleBucketDataList *tbdl, ZDL<ElementType> *zdl, DataList_t* dlp);
    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
    boost::shared_ptr<BatchPrimitiveProcessorJL> fBPP;
    FifoDataList *fifo;
    StringFifoDataList* strFifo;
	uint rowCount;
    uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
    uint16_t fStepId;
	uint32_t fStatementId;
	uint16_t fNumSteps;
	int fColWidth;
	uint fStepCount;
	execplan::CalpontSystemCatalog::TableName fTableName;
    /// number of threads on the receive side
	uint fNumThreads;
	FirstStepType ffirstStepType;
	bool fIsProjectionOnly;
	bool isFilterFeeder;
    SATHD fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;
	execplan::CalpontSystemCatalog::ColType fColType;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::OID fLastOid;
	int8_t fBOP;
	BRM::LBIDRange_v lbidRanges;
	uint32_t hwm;
	BRM::DBRM dbrm;
    SP_LBIDList lbidList;
	uint64_t ridsRequested;
	//Variables for convience
	DataList_t* ridList;
	StrDataList* strRidList;
	ElementType e;
	UintRowGroup rw;
	StringElementType strE;
	StringRowGroup strRw;
	BPSOutputType fOutputType;
	BPSInputType fInputType;
	volatile uint64_t msgsSent, msgsRecvd;
	volatile bool finishedSending;
	bool firstRead, sendWaiting;
	uint32_t recvWaiting;
	uint recvExited;
	uint64_t ridsReturned;
	uint64_t rowsReturned;
	std::vector<struct BRM::EMEntry> extents;
	OIDVector projectOids;
	uint extentSize, divShift, rpbShift, numExtents, modMask;
#ifdef PROFILE
	timespec ts1, ts2;
#endif
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
    boost::shared_ptr<boost::thread> cThread;  //consumer thread
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::mutex mutex;
	boost::mutex dlMutex;
	boost::mutex cpMutex;
	boost::condition condvarWakeupProducer, condvar;
	
	std::vector<int> scanFlags; // use to keep track of which extents to eliminate from this step
	bool BPPIsAllocated;
	uint32_t uniqueID;
	ResourceManager& fRm;

	/* HashJoin support */
	boost::shared_ptr<joiner::Joiner> joiner;
	bool doJoin;
	void serializeJoiner();
	void serializeJoiner(uint connectionNumber);

	bool fRunExecuted; // was the run method executed for this step
	bool fUpdatedEnd;   // error handling
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
	void setBPP( JobStep* jobStep );
	void setProjectBPP( JobStep* jobStep1, JobStep* jobStep2 );
	bool scanit(uint64_t rid);
	void storeCasualPartitionInfo(const bool estimateRowCounts);
	bool getFeederFlag() const { return isFilterFeeder; }
	void setFeederFlag ( bool filterFeeder ) { isFilterFeeder = filterFeeder; }
	void setSwallowRows(const bool swallowRows) {fSwallowRows = swallowRows; }
	bool getSwallowRows() const { return fSwallowRows; }
	void setIsDelivery(bool b) { isDelivery = b; }

	/* Base class interface fcn that can go away */
	void setOutputType(BPSOutputType) { } //Can't change the ot of a TupleBPS
	BPSOutputType getOutputType() const { return ROW_GROUP; }
	void setBppStep() { }
	void setIsProjectionOnly() { }

	uint64_t getRows() const { return rowsReturned; }
	void setFirstStepType ( FirstStepType firstStepType) { ffirstStepType = firstStepType;}
	FirstStepType getFirstStepType () { return ffirstStepType; }
	void setStepCount( ) { fStepCount++; }
	uint getStepCount () const { return fStepCount; }
	void setLastOid( execplan::CalpontSystemCatalog::OID colOid ) { fLastOid = colOid; }
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
	const execplan::CalpontSystemCatalog::TableName tableName() const { return fTableName; }
	const OIDVector& getProjectOids() const { return projectOids; }
	virtual uint64_t phyIOCount    ( ) const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  ( ) const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount ( ) const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    ( ) const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   ( ) const { return fMsgBytesOut;}
	virtual uint64_t blockTouched  ( ) const { return fBlockTouched;}
	uint nextBand(messageqcpp::ByteStream &bs);

	//...Currently only supported by pColStep and pColScanStep, so didn't bother
	//...to define abstract method in base class, but if start adding to other
	//...classes, then should consider adding pure virtual method to JobStep.
	uint64_t blksSkipped           ( ) const { return fNumBlksSkipped; }

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
	void initExtentMarkers();

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
	execplan::CalpontSystemCatalog::TableName fTableName;
    /// number of threads on the receive side
	uint fNumThreads;
	FirstStepType ffirstStepType;
	bool isFilterFeeder;
    SATHD fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;
	execplan::CalpontSystemCatalog::ColType fColType;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::OID fLastOid;
	BRM::LBIDRange_v lbidRanges;
	uint32_t hwm;
	BRM::LBID_t lastScannedLBID;
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
#ifdef PROFILE
	timespec ts1, ts2;
#endif
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

};

/** @brief class DeliveryStep
 *
 */
class DeliveryStep : public JobStep
{
public:
    /** @brief DeliveryStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    DeliveryStep(const JobStepAssociation& inputJobStepAssociation,
				 const JobStepAssociation& outputJobStepAssociation,
		         execplan::CalpontSystemCatalog::TableName tableName,
				 execplan::CalpontSystemCatalog* syscat,
				 uint32_t sessionId,
				 uint32_t txnId,
				 uint32_t statementId,
				 uint32_t flushInterval);

	/* Don't use this constructor except for a unit test or benchmark */
    DeliveryStep(const JobStepAssociation& inputJobStepAssociation,
				 const JobStepAssociation& outputJobStepAssociation,
				 uint colWidth);

    // This default constructor "apparently" assumes the default assign-
    // ment operator will subsequently be invoked to assign values to the
    // data members, as little data member initialization is performed
    // in this constructor.
    DeliveryStep() : initialized(false)
	{
		fExtendedInfo = "DS: Not available.";
	}
   ~DeliveryStep();
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

    const TableBand nextBand();
	uint nextBand(messageqcpp::ByteStream &bs);

    execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID; }

	void logger(const SPJL& logger) { fLogger = logger; }
	const execplan::CalpontSystemCatalog::TableName tableName() const { return fTableName; }

private:

	/*	Patrick's impl */
	struct Demuxer
	{
    	DeliveryStep *ds;
		uint index;
		uint type; 
		bool generateBS;

		// type = 0 -> ElementType, 1 -> StringElementType
	    Demuxer(DeliveryStep *DS, uint i, uint t, bool bs) : ds(DS), index(i), type(t),
			generateBS(bs)
   	 	{}

    	void operator()()
    	{
			if (generateBS)
				if (type == 0)
					ds->fillBSFromNumericalColumn(index);
				else if (type == 1)
					ds->fillBSFromStringColumn(index);
				else
					std::cerr << "Bad type passed to Demuxer" << std::endl;
			else 
				if (type == 0)
					ds->readNumericalColumn(index);
				else if (type == 1)
					ds->readStringColumn(index);
				else
					std::cerr << "Bad type passed to Demuxer" << std::endl;
		}
	};
	
	void initialize(bool bs);
	void fillByteStream(messageqcpp::ByteStream &bs);
	void fillBSFromNumericalColumn(uint index);
	void fillBSFromStringColumn(uint index);
	void readNumericalColumn(uint index);
	void readStringColumn(uint index);

	boost::shared_array<uint> iterators;
	boost::shared_array<uint> colWidths;
	std::vector<boost::thread *> threads;
	boost::mutex mutex;
	boost::condition nextBandReady;
	boost::condition allDone;
	uint doneCounter;
	bool die, initialized;
	uint64_t bandCount;
	uint columnCount;
	uint returnColumnCount;
	execplan::CalpontSystemCatalog *catalog;

	// when the generateBS methods are called, this is filled in
	// instead of the TableBand/TableColumn structures.
	// They are in column order in this array.
	boost::shared_array<messageqcpp::ByteStream> columnData;

	// filled in by the first column after every band
	uint rowCount;

    //This i/f is not meaningful in this step
    execplan::CalpontSystemCatalog::OID oid() const { return fTableOID; }

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	uint32_t fSessionId;
	uint32_t fTxnId;
    uint16_t fStepId;
	uint32_t fStatementId;
	TableBand fTableBand;
    TableBand fEmptyTableBand;
    execplan::CalpontSystemCatalog::OID fTableOID;
    int fState;

	uint32_t fFlushInterval;
    SPJL fLogger;
    int64_t fRowsDelivered;
    execplan::CalpontSystemCatalog::TableName fTableName;

	friend class BatchPrimitiveProcessorJL;
	friend class Joblist;
};


const std::string insertTime("insert time: ");
const std::string mergeTime("merge time: ");

class ReduceStep : public JobStep
{
public:
	ReduceStep(const JobStepAssociation &in, 
		const JobStepAssociation &out,
		execplan::CalpontSystemCatalog::OID tableOID,
		uint32_t sessionID,
		uint32_t txnID,
		uint32_t verID,
		uint16_t stepID,
		uint32_t statementID);
	~ReduceStep();

	const JobStepAssociation& inputAssociation() const;
	void inputAssociation(const JobStepAssociation& inputAssociation);
	const JobStepAssociation& outputAssociation() const;
	void outputAssociation(const JobStepAssociation& outputAssociation);

	void run();
	void join();

	const std::string toString() const;
	void stepId(uint16_t);
	uint16_t stepId() const;
	uint32_t sessionId()   const;
	uint32_t txnId()       const;
	uint32_t statementId() const;

   	execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID; }
    void tableOid(execplan::CalpontSystemCatalog::OID tableOid) { fTableOID = tableOid; }
// @bug 598 for self-join
    std::string alias1() const { return fAlias1; }
    void alias1(const std::string& alias) { fAlias = fAlias1 = alias; }

   	std::string alias2() const { return fAlias2; }
   	void alias2(const std::string& alias) { fAlias2 = alias; }

    std::string view1() const { return fView1; }
    void view1(const std::string& vw) { fView = fView1 = vw; }

   	std::string view2() const { return fView2; }
   	void view2(const std::string& vw) { fView2 = vw; }

	void doReduction();

	void logger(const SPJL& logger) { fLogger = logger; }

protected:
	template<typename element1_t, typename element2_t>
	void reduceByOrderedFifo(FIFO<element1_t> *, FIFO<element2_t> *);
	template<typename element1_t, typename element2_t>
	void reduceByZdl(ZDL<element1_t> *, ZDL<element2_t> *);
	template<typename element1_t, typename element2_t>
	void reduceByMixedDl(FIFO<element1_t> *, ZDL<element2_t> *);
	void unblockDatalists(uint16_t status);
private:
	explicit ReduceStep();
	ReduceStep(const ReduceStep &);

	JobStepAssociation inJSA;  // [0] is the "input" DL, [1] is the "driver" DL
	JobStepAssociation outJSA;
   	execplan::CalpontSystemCatalog::OID fTableOID;
	uint32_t sessionID;
	uint32_t txnID;
	uint32_t verID;
	uint16_t stepID;
	uint32_t statementID;
	boost::shared_ptr<boost::thread> runner;
	uint64_t inputSize, outputSize;
	SPJL fLogger;
// @bug 598 for self-join
	std::string fAlias1;
	std::string fAlias2;
	std::string fView1;
	std::string fView2;
	TimeSet fTimeSet;
};

/** @brief class 
 *
 */
class LargeHashJoin : public JobStep
{
public:

    LargeHashJoin(JoinType joinType,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t statementId,
		ResourceManager& rm);

    ~LargeHashJoin();

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

    execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID2; }
    execplan::CalpontSystemCatalog::OID tableOid1() const { return fTableOID1; }
    void tableOid1(execplan::CalpontSystemCatalog::OID tableOid1) { fTableOID1 = tableOid1; }
    execplan::CalpontSystemCatalog::OID tableOid2() const { return fTableOID2; }
    void tableOid2(execplan::CalpontSystemCatalog::OID tableOid2) { fTableOID2 = tableOid2; }
	void logger(const SPJL& logger) { fLogger = logger; }
// @bug 598 for self-join
    std::string alias1() const { return fAlias1; }
    void alias1(const std::string& alias) { fAlias1 = alias; }
    std::string alias2() const { return fAlias2; }
    void alias2(const std::string& alias) { fAlias = fAlias2 = alias; }

    std::string view1() const { return fView1; }
    void view1(const std::string& vw) { fView1 = vw; }
    std::string view2() const { return fView2; }
    void view2(const std::string& vw) { fView = fView2 = vw; }

    std::map<execplan::CalpontSystemCatalog::OID, uint64_t>& tbCardMap() { return fTbCardMap; }
    friend struct HJRunner;

protected:

    //This i/f is not meaningful in this step
    execplan::CalpontSystemCatalog::OID oid() const { return 0; }
    void unblockDatalists(uint16_t status);
    void errorLogging(const std::string& msg) const;
// 	config::Config *fConfig;

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	uint32_t fSessionId;
	uint32_t fTxnId;
    uint16_t fStepId;
	uint32_t fStatementId;
    execplan::CalpontSystemCatalog::OID fTableOID1;
    execplan::CalpontSystemCatalog::OID fTableOID2;
    SPJL fLogger;
    JoinType fJoinType;
    boost::shared_ptr<boost::thread> runner;    // @bug 686
    std::map<execplan::CalpontSystemCatalog::OID, uint64_t> fTbCardMap;
    ResourceManager& fRm;


 private:        
    void doHashJoin();
    // @bug 598 for self-join
    std::string fAlias1;
    std::string fAlias2;
	std::string fView1;
	std::string fView2;
};

/** @brief class StringHashJoinStep
 *
 */
class StringHashJoinStep : public LargeHashJoin
{
public:

    StringHashJoinStep(JoinType joinType,
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t statementId,
		ResourceManager& rm);

    ~StringHashJoinStep();

    /** @brief virtual void Run method
     */
    void run();
    const std::string toString() const;
    friend struct StringHJRunner;
private:
    void doStringHashJoin();
};

class PNLJoin : public JobStep
{
public:
	/** Constructor
	 *
	 * @param in out[0] should be the value list with the OID set to the column to scan, out[1] is an optional list used for RIDs
	 * @param out out[0] is the result of the pCol defined by in's contents, out[1] is an optional list containing the entries of in.out[1] that are in pCol results.
	 */
	PNLJoin(const JobStepAssociation &in, 
		const JobStepAssociation &out,
		DistributedEngineComm* dec,
		execplan::CalpontSystemCatalog* syscat,
		uint32_t sessionID,
		uint32_t txnId,
		uint16_t stepID,
		uint32_t statementID,
		ResourceManager& rm);
	~PNLJoin();

	const JobStepAssociation& inputAssociation() const;
	void inputAssociation(const JobStepAssociation& inputAssociation);
	const JobStepAssociation& outputAssociation() const;
	void outputAssociation(const JobStepAssociation& outputAssociation);

	void run();
	void join();

	const std::string toString() const;
	void stepId(uint16_t);
	uint16_t stepId() const;
	uint32_t sessionId()   const;
	uint32_t txnId()       const;
	uint32_t statementId() const;
	void logger(const SPJL& logger) { fLogger = logger; }

	friend struct PNLJRunner;
private:
	PNLJoin();
	PNLJoin(const PNLJoin &);

	void operator=(const PNLJoin &);

	void doPNLJoin();
	void doPNLJoin_reduce();

	JobStepAssociation in;
	JobStepAssociation out;
	uint32_t sessionID;
	uint32_t txnID;
	uint16_t stepID;
	uint32_t statementID;
	DistributedEngineComm* dec;
	execplan::CalpontSystemCatalog* syscat;
	boost::shared_ptr<boost::thread> runner;
	ResourceManager& fRm;

	/* ~500MB per BucketDL */
	static const int BUCKETS = 10;
	static const int PERBUCKET = 3200000;

	SPJL fLogger;
};


class UnionStep : public JobStep
{
public:
	UnionStep(const JobStepAssociation &in, 
		const JobStepAssociation &out,
		execplan::CalpontSystemCatalog::OID tableOID,
		uint32_t sessionID,
		uint32_t txnId,
		uint32_t verId,
		uint16_t stepID,
		uint32_t statementID);
	~UnionStep();

	const JobStepAssociation& inputAssociation() const;
	void inputAssociation(const JobStepAssociation& inputAssociation);
	const JobStepAssociation& outputAssociation() const;
	void outputAssociation(const JobStepAssociation& outputAssociation);

	void run();
	void join();

	const std::string toString() const;
	void stepId(uint16_t);
	uint16_t stepId() const;
	uint32_t sessionId()   const;
	uint32_t txnId()       const;
	uint32_t statementId() const;

	execplan::CalpontSystemCatalog::OID tableOid() const;

	void doUnion();

	void logger(const SPJL& logger) { fLogger = logger; }
// @bug 598 for self-join
   	std::string alias1() const { return fAlias1; }
   	void alias1(const std::string& alias) { fAlias = fAlias1 = alias; }
   	std::string alias2() const { return fAlias2; }
   	void alias2(const std::string& alias) { fAlias2 = alias; }

   	std::string view1() const { return fView1; }
   	void view1(const std::string& vw) { fView = fView1 = vw; }
   	std::string view2() const { return fView2; }
   	void view2(const std::string& vw) { fView2 = vw; }

	void unblockDatalists(uint16_t status);

protected:
	template<typename element1_t, typename element2_t>
	void unionByOrderedFifo(FIFO<element1_t> *, FIFO<element2_t> *);
	template<typename element1_t, typename element2_t>
	void unionByZdl(ZDL<element1_t> *, ZDL<element2_t> *);
	template<typename element1_t, typename element2_t>
	void unionByMixedDl(FIFO<element1_t> *, ZDL<element2_t> *);

private:
	explicit UnionStep();
	UnionStep(const UnionStep &);

	JobStepAssociation inJSA;  // [0] is the "input" DL, [1] is the "driver" DL
	JobStepAssociation outJSA;
	execplan::CalpontSystemCatalog::OID fTableOID;
	uint32_t sessionID;
	uint32_t txnID;
	uint32_t verID;
	uint16_t stepID;
	uint32_t statementID;
	boost::shared_ptr<boost::thread> runner;  // does the prep work
	uint64_t inputSize, outputSize;
	SPJL fLogger;
// @bug 598 for self-join
	std::string fAlias1;
	std::string fAlias2;
	std::string fView1;
	std::string fView2;
	// substiture first/last read time for start/end times
	TimeSet fTimeSet;
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

protected:
	void unblockDataLists(FifoDataList* fifo, StringFifoDataList* strFifo, StrDataList* strResult, DataList_t* result);

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
	FifoDataList* fFAp;                // Used to internally pass data to
	FifoDataList* fFBp;                // FilterOperation thread.
	uint64_t  resultCount;

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

#ifdef PROFILE
	timespec ts1, ts2;
#endif
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

/** @brief class AggregationFilterStep
*/
class AggregateFilterStep : public JobStep
{
public:
    /** @brief aggregation result data type */
    enum ResultType
    {
        INT64,
        DOUBLE,
        FLOAT,
        STRING
    };
    
    /** @brief filter argument type*/
    struct FilterArg
    {
        uint8_t cop;
        int64_t intVal;
        std::string  strVal;
        /* @brief normalization flag
         * if this filter value has been normalized so direct compare
         * can be made. It's set when function parm is one SimpleColumn.
         */
        bool isNorm;    
    };
    
    /** @brief filter with template val type for evaluation purpose */
    template <typename result_t>
    struct TFilter
    {
        int8_t cop;
        result_t val;
        TFilter<result_t> (int8_t c, result_t v): cop(c), val(v) {}
    };

	/** @brief Constructor */
	AggregateFilterStep(const JobStepAssociation &in, 
		const JobStepAssociation &out,
		const std::string& functionName,
		const execplan::CalpontSelectExecutionPlan::ReturnedColumnList groupByCols,
	    execplan::CalpontSelectExecutionPlan::ReturnedColumnList aggCols,
	    execplan::SRCP aggParam,	    
	    execplan::CalpontSystemCatalog::OID tableOID,
		uint32_t sessionID,
		uint32_t txnId,
		uint32_t verId,
		uint16_t stepID,
		uint32_t statementID,
		ResourceManager& rm);
    
    /** @brief Destructor */
	~AggregateFilterStep();

	const JobStepAssociation& inputAssociation() const {return fInJSA;}
	void inputAssociation(const JobStepAssociation& in) {fInJSA = in;}
	
	const JobStepAssociation& outputAssociation() const {return fOutJSA;}
	void outputAssociation(const JobStepAssociation& out) {fOutJSA = out;}
    
	uint16_t stepId() const {return fStepID;}
    void stepId(uint16_t stepID) {fStepID = stepID;}
    
   	uint32_t sessionId() const {return fSessionID;}
   	void sessionId(uint32_t sessionID) {fSessionID = sessionID;}

	uint32_t txnId() const {return fTxnID;}
	void txnId (uint32_t txnID) {fTxnID = txnID;}
	
	uint32_t statementId() const {return fStatementID;}
	void statementId (uint32_t statementId) {fStatementID = statementId;}
	
    execplan::CalpontSystemCatalog::OID tableOid () const {return fTableOID;}
    void tableOid(execplan::CalpontSystemCatalog::OID tableOID) {fTableOID = tableOID;}
    
    const execplan::CalpontSelectExecutionPlan::ReturnedColumnList& groupByCols() const
    {
        return fGroupByCols;
    }
    void groupByCols(const execplan::CalpontSelectExecutionPlan::ReturnedColumnList& groupByCols)
    {
        fGroupByCols = groupByCols;
    }
    
    const execplan::CalpontSelectExecutionPlan::ReturnedColumnList& aggCols() const
    {
        return fAggCols;
    }
    void aggCols(const execplan::CalpontSelectExecutionPlan::ReturnedColumnList& aggCols)
    {
        fAggCols = aggCols;
    }
    
    const uint8_t BOP() const {return fBOP;}
    void BOP (const uint8_t BOP) {fBOP = BOP;}
    
    const uint8_t resultType() const {return fResultType;}
    void resultType (const uint8_t resultType) {fResultType = resultType;}
    
    const std::vector<FilterArg>& filters() const;
    void filters(const std::vector<FilterArg>& filters) {fFilters = filters;}
    
    const execplan::SRCP aggParam() const {return fAggParam;}    
    void aggParam(const execplan::SRCP& aggParam) {fAggParam = aggParam;}
        
    const execplan::CalpontSystemCatalog::OID& outputCol() const {return fOutputCol;}   
    void outputCol(const execplan::CalpontSystemCatalog::OID& outputCol); 
    
    const MetaDataVec& aggMetas() const {return fAggMetas;}
    void aggMetas(const MetaDataVec& aggMetas) {fAggMetas = aggMetas;}
    
    const MetaDataVec& groupByMetas() const {return fGroupByMetas;}
    void groupByMetas(const MetaDataVec& groupByMetas) {fGroupByMetas = groupByMetas;}
    
    const MetaData& outputMeta() const {return fOutputMeta;}
    void outputMeta(const MetaData& outputMeta) {fOutputMeta = outputMeta;}
	
	void run();
	void join();
    
    void addFilter(int8_t COP, int64_t value, bool isNorm = true);
    void addFilter(int8_t COP, std::string value, bool isNorm = true);
    
	void logger(const SPJL& logger) { fLogger = logger; }
	const std::string toString() const;
	    
	/** @brief round up the colwidth for string column */
    static void roundColType(execplan::CalpontSystemCatalog::ColType& ct);

   	friend struct AFRunner;
	template <typename result_t> friend class ThreadedAggFilter;   	

protected:
	boost::shared_ptr<boost::thread> runner;
private:
    /** @brief constructor for completeness */
	explicit AggregateFilterStep();
	AggregateFilterStep(const AggregateFilterStep &);
	
	template <typename result_t>
    void doFilter (typename AggHashMap<result_t>::SHMP& hashMap, 
                   std::vector<TFilter<result_t> >& filters, std::vector<TupleType> &vt);
	
	/** @brief aggregate filter main function */
    void doAggFilter();      
    
    /** @brief threaded aggfilter main function */
    template <typename result_t>
    void doThreadedAggFilter(uint8_t threadID, std::vector<TFilter<result_t> >& filters);
         
    /** @brief output string */
    template <typename result_t, typename element_t>
    void output( typename AggHashMap<result_t>::SHMP& shmp,
                 DataList<element_t> *outDL, std::vector<TupleType> &vt );
    
    // @bug 1177. add support for fifo output                
    template <typename result_t, typename element_t>
    void outputFifo( typename AggHashMap<result_t>::SHMP& shmp,
                 FIFO<element_t> *outDL, std::vector<TupleType> &vt );                    
                                 
    /** @brief compare values*/
    template <typename result_t>
    bool compare(result_t& v1, result_t& v2, int8_t cop);
    
    /** @brief help function to spawn agg filter threads */
    template <typename result_t>
    void startThreads(std::vector<TFilter<result_t> >& vec);
    
    void initialize();
        
	JobStepAssociation fInJSA;  // should have size 1
	JobStepAssociation fOutJSA; // should have size 1
	std::string fFunctionName;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList fGroupByCols;
	execplan::CalpontSelectExecutionPlan::ReturnedColumnList fAggCols;
	execplan::SRCP fAggParam;
	execplan::CalpontSystemCatalog::OID fOutputCol;
	MetaDataVec fAggMetas;
	MetaDataVec fGroupByMetas;
	MetaData fOutputMeta;
	uint8_t fResultType;
	uint8_t fBOP;
	std::vector<FilterArg> fFilters;
	execplan::CalpontSystemCatalog::OID fTableOID;	
	uint32_t fSessionID;
	uint32_t fTxnID;
	uint32_t fVerID;
	uint16_t fStepID;
	uint32_t fStatementID;
	uint64_t fHashLen;
	uint64_t fDataLen;
	SPJL fLogger;
// 	config::Config *fConfig;
	execplan::CalpontSystemCatalog *fCsc;
	Aggregator::AggOp fOp;
	uint8_t fNumThreads;
	ResourceManager& fRm;
	boost::mutex fFifoMutex;
	uint64_t fFifoRowCount;
};


/** @brief class BucketReuseStep
 *
 */
class BucketReuseStep : public JobStep
{
public:
    /** @brief BucketReuseStep constructors
     */
    BucketReuseStep(const pColScanStep& scan);
	BucketReuseStep(execplan::CalpontSystemCatalog::OID colOid,
		execplan::CalpontSystemCatalog::OID tblOid,
		uint32_t sessionId, uint32_t txnId,
		uint32_t verId,     uint32_t statementId);
	BucketReuseStep(const pColScanStep& scan, pDictionaryStep& dict);

	virtual ~BucketReuseStep();

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

	/** @brief reuse the buckets on disk to feed the data list */
    virtual void reuseBuckets();

	template<typename element_t>
    void reuseBuckets(BucketDL<element_t>*);

	/** @brief output to log string */
	virtual const std::string toString() const;

    /** @brief virtual JobStepAssociation * inputAssociation */
    virtual const JobStepAssociation& inputAssociation() const
	    { return fInputJobStepAssociation; }
	virtual void inputAssociation(const JobStepAssociation& inputAssociation)
		{ throw std::logic_error("inputAssociation is set by constuctor."); }

    /** @brief virtual JobStepAssociation * outputAssociation */
    virtual const JobStepAssociation& outputAssociation() const
    	{ return fOutputJobStepAssociation; }
	virtual void outputAssociation(const JobStepAssociation& outputAssociation)
		{ fOutputJobStepAssociation = outputAssociation; }

	virtual void stepId(uint16_t stepId) { fStepId = stepId; }
	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }
	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	uint32_t sessionId() const { return fSessionId; }
	uint32_t txnId() const { return fTxnId; }
	uint32_t verId() const { return fVerId; }
	uint16_t stepId() const { return fStepId; }
	uint32_t statementId() const { return fStatementId; }
	void logger(const SPJL& logger) { fLogger = logger; }
	const SPJL& logger() const { return fLogger; }
	uint8_t colWidth() const { return fColWidth; }
	bool dictColumn() const { return fDictColumn; }

protected:

private:
    /** @brief disabled constructor for completeness, no implementation
     */
    BucketReuseStep();

	struct Runner {
		Runner(BucketReuseStep *s) : fStep(s) { }

		void operator()() { fStep->reuseBuckets(); }

		BucketReuseStep *fStep;
	};

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
	uint32_t fStatementId;
	uint16_t fStepId;
	uint8_t  fColWidth;
	bool     fDictColumn;

	boost::shared_ptr<boost::thread> runner;
	SPJL fLogger;
};


}

#endif
// vim:ts=4 sw=4:

