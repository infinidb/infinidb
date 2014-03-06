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

//  $Id: primitivestep.h 9688 2013-07-15 19:27:22Z pleblanc $


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
class PseudoColStep;


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
	 * @param flushInterval The interval in msgs at which the sending side should
	 * wait for the receiveing side to catch up.  0 (default) means never.
     */
    pColStep(
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		const execplan::CalpontSystemCatalog::ColType& ct,
		const JobInfo& jobInfo);

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

	virtual const std::string toString() const;

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

	uint32_t filterCount() const { return fFilterCount; }
	const messageqcpp::ByteStream &filterString() const { return fFilterString; }
	int8_t BOP() const { return fBOP; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	void appendFilter(const messageqcpp::ByteStream& filter, unsigned count);
	uint32_t flushInterval() const { return fFlushInterval; }
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
	boost::shared_ptr<execplan::CalpontSystemCatalog> sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::ColType fColType;
	uint32_t fFilterCount;
	int8_t fBOP;
	int8_t fOutputType;
	uint16_t realWidth;
	DataList_t* ridList;
	StrDataList* strRidList;
	messageqcpp::ByteStream fFilterString;
	std::vector<struct BRM::EMEntry> extents;
	uint32_t extentSize, divShift, modMask, ridsPerBlock, rpbShift, blockSizeShift, numExtents;
	uint64_t rpbMask;
	uint64_t msgsSent, msgsRecvd;
	bool finishedSending, recvWaiting, fIsDict;
	bool isEM;
	int64_t ridCount;
	uint32_t fFlushInterval;

	// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is TRACE_FLAGS::TRACE_NO_ROWS4.
	// 	      Running with this one will swallow rows at projection.
	bool fSwallowRows;
	uint32_t fProjectBlockReqLimit;     // max number of rids to send in a scan
                                     // request to primproc
    uint32_t fProjectBlockReqThreshold; // min level of rids backlog before
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
	SP_LBIDList lbidList;
	std::vector<int> scanFlags; // use to keep track of which extents to eliminate from this step
	uint32_t uniqueID;

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
     */
    pColScanStep(
	execplan::CalpontSystemCatalog::OID oid,
	execplan::CalpontSystemCatalog::OID tableOid,
	const execplan::CalpontSystemCatalog::ColType& ct,
	const JobInfo& jobInfo);

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
	uint32_t filterCount() const { return fFilterCount; }

	virtual const std::string toString() const;

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	ResourceManager& resourceManager() const { return fRm; }

	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return recvCount;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}
    uint32_t getRidsPerBlock() const {return ridsPerBlock;}

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
		uint32_t msgLbidCount);
	uint64_t getFBO(uint64_t lbid);
	bool isEmptyVal(const uint8_t *val8) const;

	ResourceManager& fRm;
    ColByScanRangeRequestHeader fMsgHeader;
    SPTHD fConsumerThread;
    /// number of threads on the receive side
	uint32_t fNumThreads;

    SPTHD * fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint32_t fFilterCount;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	execplan::CalpontSystemCatalog::ColType fColType;
	int8_t fBOP;
	int8_t fOutputType;
    uint32_t sentCount;
	uint32_t recvCount;
	BRM::LBIDRange_v lbidRanges;
	BRM::DBRM dbrm;
	SP_LBIDList lbidList;

	boost::mutex mutex;
	boost::mutex dlMutex;
	boost::mutex cpMutex;
	boost::condition condvar;
	boost::condition condvarWakeupProducer;
	bool finishedSending, sendWaiting, rDoNothing, fIsDict;
	uint32_t recvWaiting, recvExited;
	uint64_t ridsReturned;

	std::vector<struct BRM::EMEntry> extents;
	uint32_t extentSize, divShift, ridsPerBlock, rpbShift, numExtents;
// 	config::Config *fConfig;

	uint32_t fScanLbidReqLimit;     // max number of LBIDs to send in a scan
                                     // request to primproc
	uint32_t fScanLbidReqThreshold; // min level of scan LBID backlog before
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
     */

    pDictionaryStep(
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tabelOid,
		const execplan::CalpontSystemCatalog::ColType& ct,
		const JobInfo& jobInfo);

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

	virtual const std::string toString() const;

	execplan::CalpontSystemCatalog::ColType& colType() { return fColType; }
	execplan::CalpontSystemCatalog::ColType colType() const { return fColType; }

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }
	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
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

	boost::shared_ptr<execplan::CalpontSystemCatalog> sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
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
	uint32_t fFilterCount;

	DataList_t* requestList;
	//StringDataList* stringList;
	boost::mutex mutex;
	boost::condition condvar;
	uint32_t fInterval;
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
public:

    /** @brief pDictionaryScan constructor
     */

    pDictionaryScan(
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		const execplan::CalpontSystemCatalog::ColType& ct,
		const JobInfo& jobInfo);

    ~pDictionaryScan();

    /** @brief virtual void Run method
     */
    virtual void run();
	virtual void join();
	void setInputList(DataList_t* rids);
	void setBOP(int8_t b);
	void sendPrimitiveMessages();
	void receivePrimitiveMessages();
	void setSingleThread();
	virtual const std::string toString() const;

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

    DistributedEngineComm* fDec;
	boost::shared_ptr<execplan::CalpontSystemCatalog> sysCat;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
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
	uint32_t fLogicalBlocksPerScan;
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
	uint32_t fScanLbidReqLimit;     // max number of LBIDs to send in a scan
                                     // request to primproc
	uint32_t fScanLbidReqThreshold; // min level of scan LBID backlog before
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

	BatchPrimitive(const JobInfo& jobInfo) : JobStep(jobInfo) {}
	virtual bool getFeederFlag() const = 0;
	virtual uint64_t getLastTupleId() const = 0;
	virtual uint32_t getStepCount () const = 0;
	virtual void setBPP(JobStep* jobStep) = 0;
	virtual void setFirstStepType(PrimitiveStepType firstStepType) = 0;
	virtual void setIsProjectionOnly() = 0;
	virtual void setLastTupleId(uint64_t) = 0;
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
	TupleBPS(const pColStep& rhs, const JobInfo& jobInfo);
	TupleBPS(const pColScanStep& rhs, const JobInfo& jobInfo);
	TupleBPS(const pDictionaryStep& rhs, const JobInfo& jobInfo);
	TupleBPS(const pDictionaryScan& rhs, const JobInfo& jobInfo);
	TupleBPS(const PassThruStep& rhs, const JobInfo& jobInfo);
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

	void abort();
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
    void receiveMultiPrimitiveMessages(uint32_t threadID);

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

	/* Base class interface fcn that can go away */
	void setOutputType(BPSOutputType) { } //Can't change the ot of a TupleBPS
	BPSOutputType getOutputType() const { return ROW_GROUP; }
	void setBppStep() { }
	void setIsProjectionOnly() { }

	uint64_t getRows() const { return rowsReturned; }
	void setFirstStepType(PrimitiveStepType firstStepType) { ffirstStepType = firstStepType;}
	PrimitiveStepType getPrimitiveStepType () { return ffirstStepType; }
	void setStepCount() { fStepCount++; }
	uint32_t getStepCount () const { return fStepCount; }
	void setLastTupleId(uint64_t id) { fLastTupleId = id; }
	uint64_t getLastTupleId() const { return fLastTupleId; }

	/** @brief Set the DistributedEngineComm object this instance should use
	 *
	 * Set the DistributedEngineComm object this instance should use
	 */
	void dec(DistributedEngineComm* dec);

	virtual void stepId(uint16_t stepId);
	virtual uint16_t stepId() const { return fStepId; }
	virtual const std::string toString() const;

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }
	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	const OIDVector& getProjectOids() const { return projectOids; }
	virtual uint64_t phyIOCount    () const { return fPhysicalIO; }
	virtual uint64_t cacheIOCount  () const { return fCacheIO;    }
	virtual uint64_t msgsRcvdCount () const { return msgsRecvd;   }
	virtual uint64_t msgBytesIn    () const { return fMsgBytesIn; }
	virtual uint64_t msgBytesOut   () const { return fMsgBytesOut;}
	virtual uint64_t blockTouched  () const { return fBlockTouched;}
	uint32_t nextBand(messageqcpp::ByteStream &bs);

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
	void newPMOnline(uint32_t connectionNumber);

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
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;

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

	virtual bool stringTableFriendly() { return true; }

protected:
	void sendError(uint16_t status);
	void processError(const std::string& ex, uint16_t err, const std::string& src);

private:
	void formatMiniStats();

    typedef boost::shared_ptr<boost::thread> SPTHD;
	typedef boost::shared_array<SPTHD> SATHD;
    void startPrimitiveThread();
    void startAggregationThread();
    void initializeConfigParms();
	uint64_t getFBO(uint64_t lbid);
	void checkDupOutputColumns(const rowgroup::RowGroup &rg);
	void dupOutputColumns(rowgroup::RowGroup&);
	void dupOutputColumns(rowgroup::RGData&, rowgroup::RowGroup&);
	void rgDataToDl(rowgroup::RGData &, rowgroup::RowGroup&, RowGroupDL*);
	void rgDataVecToDl(std::vector<rowgroup::RGData>&,rowgroup::RowGroup&,RowGroupDL*);

    DistributedEngineComm* fDec;
    boost::shared_ptr<BatchPrimitiveProcessorJL> fBPP;
	uint32_t rowCount;
	uint16_t fNumSteps;
	int fColWidth;
	uint32_t fStepCount;
    bool fCPEvaluated;  // @bug 2123
	uint64_t fEstimatedRows; // @bug 2123
    /// number of threads on the receive side
    uint32_t fMaxNumThreads;
	uint32_t fNumThreads;
	PrimitiveStepType ffirstStepType;
	bool isFilterFeeder;
    SATHD fProducerThread;
	messageqcpp::ByteStream fFilterString;
	uint32_t fFilterCount;
	execplan::CalpontSystemCatalog::ColType fColType;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	uint64_t fLastTupleId;
	BRM::LBIDRange_v lbidRanges;
	std::vector<int32_t> lastExtent;
	std::vector<BRM::LBID_t> lastScannedLBID;
	BRM::DBRM dbrm;
    SP_LBIDList lbidList;
	uint64_t ridsRequested;
	uint64_t totalMsgs;
	volatile uint64_t msgsSent;
	volatile uint64_t msgsRecvd;
	volatile bool finishedSending;
	bool firstRead;
	bool sendWaiting;
	uint32_t recvWaiting;
	uint32_t recvExited;
	uint64_t ridsReturned;
	uint64_t rowsReturned;
	std::map<execplan::CalpontSystemCatalog::OID, std::tr1::unordered_map<int64_t, struct BRM::EMEntry> > extentsMap;
	std::vector<BRM::EMEntry> scannedExtents;
	OIDVector projectOids;
	uint32_t extentSize, divShift, rpbShift, numExtents, modMask;
	uint32_t fRequestSize; // the number of logical extents per batch of requests sent to PrimProc.
	uint32_t fProcessorThreadsPerScan; // The number of messages sent per logical extent.
	bool fSwallowRows;
    uint32_t fMaxOutstandingRequests; // The number of logical extents have not processed by PrimProc
	uint64_t fPhysicalIO;	// total physical I/O count
	uint64_t fCacheIO;		// total cache I/O count
	uint64_t fNumBlksSkipped;//total number of block scans skipped due to CP
	uint64_t fMsgBytesIn;   // total byte count for incoming messages
	uint64_t fMsgBytesOut;  // total byte count for outcoming messages
	uint64_t fBlockTouched; // total blocks touched
    uint32_t fExtentsPerSegFile;//config num of Extents Per Segment File
    boost::shared_ptr<boost::thread> cThread;  //consumer thread
	boost::shared_ptr<boost::thread> pThread;  //producer thread
	boost::mutex mutex;
	boost::mutex dlMutex;
	boost::mutex cpMutex;
	boost::condition condvarWakeupProducer, condvar;

	std::vector<bool> scanFlags; // use to keep track of which extents to eliminate from this step
	bool BPPIsAllocated;
	uint32_t uniqueID;
	ResourceManager& fRm;

	/* HashJoin support */

	void serializeJoiner();
	void serializeJoiner(uint32_t connectionNumber);

	void generateJoinResultSet(const std::vector<std::vector<rowgroup::Row::Pointer> > &joinerOutput,
	  rowgroup::Row &baseRow, const std::vector<boost::shared_array<int> > &mappings,
	  const uint32_t depth, rowgroup::RowGroup &outputRG, rowgroup::RGData &rgData,
	  std::vector<rowgroup::RGData> *outputData,
	  const boost::scoped_array<rowgroup::Row> &smallRows, rowgroup::Row &joinedRow);

	std::vector<boost::shared_ptr<joiner::TupleJoiner> > tjoiners;
	bool doJoin, hasPMJoin, hasUMJoin;
	std::vector<rowgroup::RowGroup> joinerMatchesRGs;   // parses the small-side matches from joiner

	uint32_t smallSideCount;
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
	bool runRan;
	bool joinRan;

	// bug 1965, trace duplicat columns in delivery list <dest, src>
	std::vector<std::pair<uint32_t, uint32_t> > dupColumns;

	/* Functions & Expressions vars */
	boost::shared_ptr<funcexp::FuncExpWrapper> fe1, fe2;
	rowgroup::RowGroup fe1Input, fe2Output;
	boost::shared_array<int> fe2Mapping;
	bool runFEonPM;

	/* for UM F & E 2 processing */
	rowgroup::RGData fe2Data;
	rowgroup::Row fe2InRow, fe2OutRow;

	void processFE2(rowgroup::RowGroup &input, rowgroup::RowGroup &output,
	  rowgroup::Row &inRow, rowgroup::Row &outRow,
	  std::vector<rowgroup::RGData> *rgData,
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

	boost::shared_ptr<RowGroupDL> deliveryDL;
	uint32_t deliveryIt;

	/* shared nothing support */
	struct Job {
		Job(uint32_t d, uint32_t n, uint32_t b, boost::shared_ptr<messageqcpp::ByteStream> &bs) :
			dbroot(d), connectionNum(n), expectedResponses(b), msg(bs) { }
		uint32_t dbroot;
		uint32_t connectionNum;
		uint32_t expectedResponses;
		boost::shared_ptr<messageqcpp::ByteStream> msg;
	};

	void prepCasualPartitioning();
	void makeJobs(std::vector<Job> *jobs);
	void interleaveJobs(std::vector<Job> *jobs) const;
	void sendJobs(const std::vector<Job> &jobs);
	uint32_t numDBRoots;

    /* Pseudo column filter processing.  Think about refactoring into a separate class. */
    bool processPseudoColFilters(uint32_t extentIndex, boost::shared_ptr<std::map<int, int> > dbRootPMMap) const;
    bool processOneFilterType(int8_t colWidth, int64_t value, uint32_t type) const;
    bool processSingleFilterString(int8_t BOP, int8_t colWidth, int64_t val, const uint8_t *filterString,
      uint32_t filterCount) const;
    bool processSingleFilterString_ranged(int8_t BOP, int8_t colWidth, int64_t min, int64_t max,
        const uint8_t *filterString, uint32_t filterCount) const;
    bool processLBIDFilter(const BRM::EMEntry &emEntry) const;
    bool compareSingleValue(uint8_t COP, int64_t val1, int64_t val2) const;
    bool compareRange(uint8_t COP, int64_t min, int64_t max, int64_t val) const;
    bool hasPCFilter, hasPMFilter, hasRIDFilter, hasSegmentFilter, hasDBRootFilter, hasSegmentDirFilter,
        hasPartitionFilter, hasMaxFilter, hasMinFilter, hasLBIDFilter, hasExtentIDFilter;

};

/** @brief class FilterStep
 *
 */
class FilterStep : public JobStep
{
public:

    FilterStep(const execplan::CalpontSystemCatalog::ColType& colType, const JobInfo& jobInfo);
    ~FilterStep();

    /** @brief virtual void Run method
     */
    void run();
    void join();

    const std::string toString() const;

    execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID; }
    void tableOid(execplan::CalpontSystemCatalog::OID tableOid) { fTableOID = tableOid; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
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

    execplan::CalpontSystemCatalog::OID fTableOID;
	execplan::CalpontSystemCatalog::ColType fColType;
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
     */
    PassThruStep(
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		const execplan::CalpontSystemCatalog::ColType& colType,
		const JobInfo& jobInfo);

	PassThruStep(const pColStep& rhs);
	PassThruStep(const PseudoColStep& rhs);

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

	virtual const std::string toString() const;

	virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }

	virtual execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }

	uint8_t getColWidth() const { return colWidth; }
	bool isDictCol() const { return isDictColumn; }
	bool isExeMgr() const { return isEM; }
	const execplan::CalpontSystemCatalog::ColType& colType() const { return fColType; }
	ResourceManager& resourceManager() const { return fRm; }

	void pseudoType(uint32_t p) { fPseudoType = p; }
	uint32_t pseudoType() const { return fPseudoType; }

protected:

private:

    /** @brief constructor for completeness
     */
    explicit PassThruStep();

	uint64_t getLBID(uint64_t rid, bool& scan);
	uint64_t getFBO(uint64_t lbid);

	boost::shared_ptr<execplan::CalpontSystemCatalog> catalog;
	execplan::CalpontSystemCatalog::OID fOid;
	execplan::CalpontSystemCatalog::OID fTableOid;
	uint8_t colWidth;
	uint16_t realWidth;
	uint32_t fPseudoType;
	execplan::CalpontSystemCatalog::ColType fColType;
	bool isDictColumn;
	bool isEM;

	boost::thread* fPTThd;

	// @bug 663 - Added fSwallowRows for calpont.caltrace(16) which is TRACE_FLAGS::TRACE_NO_ROWS4.
	// 	      Running with this one will swallow rows at projection.
	bool fSwallowRows;
	ResourceManager& fRm;
	friend class PassThruCommandJL;
	friend class RTSCommandJL;
	friend class BatchPrimitiveStep;
	friend class TupleBPS;
};

class PseudoColStep : public pColStep
{
public:
    /** @brief PseudoColStep constructor
     */
    PseudoColStep(
		execplan::CalpontSystemCatalog::OID oid,
		execplan::CalpontSystemCatalog::OID tableOid,
		uint32_t pId,
		const execplan::CalpontSystemCatalog::ColType& ct,
		const JobInfo& jobInfo) :
		pColStep(oid, tableOid, ct, jobInfo),
		fPseudoColumnId(pId)
    {}

    PseudoColStep(const PassThruStep& rhs) :
		pColStep(rhs),
		fPseudoColumnId(rhs.pseudoType())
	{}

	virtual ~PseudoColStep() {}

	uint32_t pseudoColumnId() const   { return fPseudoColumnId; }
	void pseudoColumnId(uint32_t pId) { fPseudoColumnId = pId;  }

protected:
	uint32_t fPseudoColumnId;

private:
	/** @brief disabled constuctor
	 */
    PseudoColStep(const pColScanStep&);
    PseudoColStep(const pColStep&);
};


}

#endif  // JOBLIST_PRIMITIVESTEP_H
// vim:ts=4 sw=4:


