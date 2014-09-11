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

//  $Id: joblist.h 8272 2012-01-19 16:28:34Z xlou $


/** @file */

#ifndef JOBLIST_JOBLIST_H_
#define JOBLIST_JOBLIST_H_

#include <string>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>

#include "calpontsystemcatalog.h"

#include "jobstep.h"
#include "tableband.h"
#include "bytestream.h"

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

#if defined(_MSC_VER) && defined(JOBLIST_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{

typedef std::vector<SJSTEP> JobStepVector;
typedef std::map<execplan::CalpontSystemCatalog::OID, SJSTEP> DeliveredTableMap;

class DistributedEngineComm;

// bug3438, move stats to joblist to cover subqueries
struct QueryStats
{
	uint64_t fMaxMemPct;        // peak memory percentage used during a query
	uint64_t fNumFiles;         // number of temp files used for a query
	uint64_t fFileBytes;        // number of bytes in temp files
	uint64_t fPhyIO;           	// physical block count for a query
	uint64_t fCacheIO;          // cache block count for a query
	uint64_t fMsgRcvCnt;        // msg (block) receive count for a query
	uint64_t fCPBlocksSkipped;  // Casual Partition blks skipped for a query
	uint64_t fMsgBytesIn;       // number of input msg bytes for a query
	uint64_t fMsgBytesOut;      // number of output msg bytes for a query

	QueryStats() { reset(); }
	void reset()
	{
		fMaxMemPct       = 0;
		fNumFiles        = 0;
		fFileBytes       = 0;
		fPhyIO           = 0;
		fCacheIO         = 0;
		fMsgRcvCnt       = 0;
		fCPBlocksSkipped = 0;
		fMsgBytesIn      = 0;
		fMsgBytesOut     = 0;
	}

	QueryStats operator+=(const QueryStats& rhs)
	{
		fNumFiles        += rhs.fNumFiles;
		fFileBytes       += rhs.fFileBytes;
		fPhyIO           += rhs.fPhyIO;
		fCacheIO         += rhs.fCacheIO;
		fMsgRcvCnt       += rhs.fMsgRcvCnt;
		fCPBlocksSkipped += rhs.fCPBlocksSkipped;
		fMsgBytesIn      += rhs.fMsgBytesIn;
		fMsgBytesOut     += rhs.fMsgBytesOut;

		return *this;
	}

};

/** @brief class JobList
 *
 */
class JobList;
typedef boost::shared_ptr<JobList> SJLP;

class JobList
{
public:
	explicit JobList(bool isEM=false);
	virtual ~JobList();
	virtual int doQuery();
	virtual const TableBand projectTable(execplan::CalpontSystemCatalog::OID tableOID);

	/* returns row count */
	virtual uint projectTable(execplan::CalpontSystemCatalog::OID tableOID,
		messageqcpp::ByteStream &bs);
	virtual int  putEngineComm(DistributedEngineComm*);

	virtual void addQuery(const JobStepVector& query) { fQuery = query; }
	virtual void addProject(const JobStepVector& project) { fProject = project; }
	virtual void addDelivery(const DeliveredTableMap& delivery) { fDeliveredTables = delivery; }
	virtual void PutEngineComm(const DistributedEngineComm*) __attribute__((deprecated)) { }
	virtual const DeliveredTableMap& deliveredTables() const { return fDeliveredTables; }
	virtual void querySummary(bool extendedStats);
	virtual void graph(uint32_t sessionID);

	virtual const SErrorInfo& statusPtr() const { return errInfo; }
	virtual void statusPtr(SErrorInfo sp) { errInfo = sp; }

	virtual const uint32_t status() const { return errInfo->errCode; }
	virtual void status(uint32_t ec) { errInfo->errCode = ec; }
	virtual const std::string& errMsg() const { return errInfo->errMsg; }
	virtual void errMsg(const std::string &s) { errInfo->errMsg = s; }

	virtual execplan::CalpontSystemCatalog::OID* projectingTableOIDPtr()
			{ return &projectingTableOID; }

	/** Does some light validation on the final joblist
	 *
	 * Currently only verifies that all JobSteps are not tuple oriented.
	 */
	virtual void validate() const;

	const QueryStats& queryStats() const { return fStats; }
	void queryStats(const QueryStats& stats) { fStats = stats; }
	const std::string& extendedInfo() const { return fExtendedInfo; }
	void extendedInfo(const std::string& extendedInfo) { fExtendedInfo = extendedInfo; }
	const std::string& miniInfo() const { return fMiniInfo; }
	void miniInfo(const std::string& miniInfo) { fMiniInfo = miniInfo; }

	void addSubqueryJobList(const SJLP& sjl) { subqueryJoblists.push_back(sjl); }

	/** Stop the running query
	 *
	 * This notifies the joblist to abort the running query.  It returns right away, not
	 * when the joblist has actually stopped.  The caller may need to drain some data
	 * through projectTable() for the joblist to abort completely.
	 */
	EXPORT virtual void abort();
	EXPORT virtual bool aborted()
#ifdef _MSC_VER
	{ return (fAborted != 0); }
#else
	{ return fAborted; }
#endif

protected:
	//defaults okay
	//JobList(const JobList& rhs);
	//JobList& operator=(const JobList& rhs);
	bool fIsRunning;
	bool fIsExeMgr;
	bool fPmConnected;

	DeliveredTableMap fDeliveredTables;
	execplan::CalpontSystemCatalog::OID projectingTableOID; //DeliveryWSDLs get a reference to this
	SErrorInfo errInfo;
	JobStepVector fQuery;
	JobStepVector fProject;

	// @bug3438, get stats/trace from subqueries
	QueryStats fStats;
	std::string fExtendedInfo;
	std::string fMiniInfo;
	std::vector<SJLP> subqueryJoblists;

#ifdef _MSC_VER
	volatile LONG fAborted;
#else
	volatile bool fAborted;
#endif
};

class TupleJobList : public JobList
{
public:
	TupleJobList(bool isEM=false);
	virtual ~TupleJobList();

	EXPORT uint projectTable(execplan::CalpontSystemCatalog::OID, messageqcpp::ByteStream&);
	EXPORT const rowgroup::RowGroup& getOutputRowGroup() const;
	TupleDeliveryStep* getDeliveryStep() { return ds; }
	const JobStepVector& querySteps() const { return fQuery; }
	void setDeliveryFlag(bool f);
	void abort();

	/** Does some light validation on the final joblist
	 *
	 * Currently verifies that JobSteps are all tuple-oriented, that
	 * there's one and only one projection step, and that its fake table OID is 100.
	 * @note The fake OID check is disabled atm because it's not always 100 although it's supposed to be.
	 */
	EXPORT void validate() const;

private:
	//defaults okay
	//TupleJobList(const TupleJobList& rhs);
	//TupleJobList& operator=(const TupleJobList& rhs);

	TupleDeliveryStep* ds;
	bool moreData;   // used to prevent calling nextBand beyond the last RowGroup
};

typedef boost::shared_ptr<TupleJobList> STJLP;

}

#undef EXPORT

#endif
// vim:ts=4 sw=4:

