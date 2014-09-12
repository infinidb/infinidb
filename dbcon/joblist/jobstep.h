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

//  $Id: jobstep.h 8774 2012-07-31 21:00:09Z pleblanc $


/** @file */

#ifndef JOBLIST_JOBSTEP_H_
#define JOBLIST_JOBSTEP_H_

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <sys/time.h>
#include <stdexcept>

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "calpontsystemcatalog.h"
#include "calpontselectexecutionplan.h"
#include "elementtype.h"
#include "jl_logger.h"
#include "timestamp.h"
#include "rowgroup.h"


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

/** @brief class JobStepAssociation mediator class to connect/control JobSteps and DataLists
 * 
 * Class JobStepAssociation controls JobSteps and DataLists
 */
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

};


/** @brief class JobStep abstract class describing a query execution step
 * 
 * Class JobStep is an abstract class that describes a query execution step
 */
struct JobStep
{
    /** constructor
     */
    JobStep() : fToSort(0), fTraceFlags(0), fCardinality(0), fError(0),
		fDelayedRunFlag(false), fWaitToRunStepCnt(0), die(false), fTupleId(-1),
		_priority(1) { }
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
    virtual std::string schema() const { return fSchema; }
    virtual void  schema(const std::string& s)  { fSchema = s; }
	// @bug 3398, add columns' unique tuple ID to job step
	virtual uint64_t tupleId() const  { return fTupleId; }
	virtual void tupleId(uint64_t id) { fTupleId = id; }
	//util function: convert eight byte to string
	//void intToStr( DataList_t& inList, StringBucketDataList& outList );
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
	bool traceOn() const;
	void setTraceFlags(uint32_t flags) { fTraceFlags = flags; }
	JSTimeStamp dlTimes;

	const std::string& extendedInfo() const { return fExtendedInfo; }
	const std::string& miniInfo() const { return fMiniInfo; }

	uint priority() { return _priority; }
	void priority(uint p) { _priority = p; }

protected:
    std::string fAlias;
    std::string fView;
	std::string fName;
	std::string fSchema;
    uint8_t     fToSort;
	uint32_t    fTraceFlags;
	uint64_t    fCardinality;
	bool        fError;
	bool        fDelayedRunFlag;
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
	uint _priority;

private:
	static boost::mutex mutex;
	friend class CommandJL;
};


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

// calls rhs->toString()
std::ostream& operator<<(std::ostream& os, const JobStep* rhs);


typedef boost::shared_ptr<JobStepAssociation> SJSA;
typedef boost::shared_ptr<JobStepAssociation> JobStepAssociationSPtr; 

typedef boost::shared_ptr<JobStep> SJSTEP;


}

#endif  // JOBLIST_JOBSTEP_H_
// vim:ts=4 sw=4:



