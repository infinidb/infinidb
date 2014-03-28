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

//  $Id: jlf_common.h 9702 2013-07-17 19:08:07Z xlou $


/** @file jlf_common.h
 *
 */
#ifndef JLF_COMMON_H__
#define JLF_COMMON_H__

#include <map>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn.h"

#include "dbrm.h"

#include "joblist.h"
#include "jobstep.h"
#include "groupconcat.h"
#include "jl_logger.h"

#include "resourcemanager.h"
#include "rowgroup.h"

// forward reference
namespace execplan
{
class AggregateColumn;
class SimpleColumn;
}


namespace joblist
{
// for output error messages to screen.
const std::string boldStart = "\033[0;1m";
const std::string boldStop = "\033[0;39m";

const int8_t CONST_COL_NONE  = 0;
const int8_t CONST_COL_EXIST = 1;
const int8_t CONST_COL_ONLY  = 2;

struct TupleInfo
{
	TupleInfo(uint w=0, uint o=0, uint k=-1, uint t=-1, uint s=0, uint p=0,
		execplan::CalpontSystemCatalog::ColDataType dt=execplan::CalpontSystemCatalog::BIGINT) :
		width(w), oid(o), key(k), tkey(t), scale(s), precision(p), dtype(dt) { }
	~TupleInfo() { }

	uint width;
	uint oid;
	uint key;
	uint tkey;
	uint scale;
	uint precision;
	execplan::CalpontSystemCatalog::ColDataType dtype;
};

// for compound join
struct JoinData
{
	int64_t fJoinId;
	std::vector<uint> fLeftKeys;
	std::vector<uint> fRightKeys;
	std::vector<JoinType> fTypes; // joblisttypes.h: INNER, LEFTOUTER, RIGHTOUTER
	bool fTypeless;

	JoinData() : fJoinId(-1), fTypeless(false) {}
};

typedef std::stack<JobStepVector> JobStepVectorStack;
typedef std::set<execplan::CalpontSystemCatalog::TableName> DeliveredTablesSet;
typedef std::map<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::OID> DictOidToColOidMap;
typedef std::vector<TupleInfo> TupleInfoVector;
typedef std::map<uint, TupleInfoVector> TupleInfoMap;

//@bug 598 & 1632 self-join
//typedef std::pair<execplan::CalpontSystemCatalog::OID, std::string> OIDAliasPair;  //self-join

//for subquery support
struct UniqId
{
	int         fId;     // OID for real table, sequence # for subquery
//	std::string fName;   // name (table alias + [column name, if column])
	std::string fTable;  // table name (table alias)
	std::string fSchema; // schema name
	std::string fView;   // view name
//	uint64_t	fEngine; // InfiniDB == 0
	uint64_t    fSubId;  // subquery ID

	UniqId() : fId(-1), fSubId(-1) {}
	UniqId(int i, const std::string& t, const std::string& s, const std::string& v, uint64_t l=-1) :
		fId(i), fTable(t), fSchema(s), fView(v), fSubId(l) {}
	UniqId(const execplan::SimpleColumn* sc);
	UniqId(int o, const execplan::SimpleColumn* sc);
};
bool operator < (const struct UniqId& x, const struct UniqId& y);
bool operator == (const struct UniqId& x, const struct UniqId& y);
typedef std::map<UniqId, uint> TupleKeyMap;

//typedef vector<SRCP> RetColsVector;
typedef execplan::CalpontSelectExecutionPlan::ReturnedColumnList RetColsVector;

//join data between table pairs
typedef std::map<std::pair<uint, uint>, JoinData> TableJoinMap;
// map<table<table A id, table B id>, pair<table A joinKey, table B joinKey> >
//typedef std::map<std::pair<uint, uint>, std::pair<uint, uint> > TableJoinKeyMap;
//typedef std::map<std::pair<uint, uint>, JoinType> JoinTypeMap;

struct TupleKeyInfo
{
	uint32_t   nextKey;
	TupleKeyMap tupleKeyMap;
	std::vector<UniqId> tupleKeyVec;
	std::vector<std::string>  tupleKeyToName;
	std::vector<bool>  crossEngine;

	// TODO: better orgineze this structs
	std::map<uint, execplan::CalpontSystemCatalog::OID> tupleKeyToTableOid;
	std::map<uint, execplan::CalpontSystemCatalog::ColType> colType;
	std::map<uint, execplan::CalpontSystemCatalog::ColType> token2DictTypeMap; // i/c token only
	std::map<uint, std::string> keyName;
	std::map<uint, uint> colKeyToTblKey;
	std::map<uint, uint> dictKeyMap;    // map token key to dictionary key
	DictOidToColOidMap dictOidToColOid; // map dictionary OID to column OID
	TupleInfoMap tupleInfoMap;

	TupleKeyInfo() : nextKey(0) {}
};


//------------------------------------------------------------------------------
/** @brief This struct maintains state for the query processing
 *
 */
//------------------------------------------------------------------------------
struct JobInfo
{
	JobInfo(ResourceManager& r) :
		rm(r),
		sessionId(0),
		txnId(0),
		statementId(0),
		maxBuckets(rm.getHjMaxBuckets()),
		maxElems(rm.getHjMaxElems()),
		flushInterval(rm.getJLFlushInterval()),
		fifoSize(rm.getJlFifoSize()),
		fifoSizeLargeSideHj(rm.getHjFifoSizeLargeSide()),
		scanLbidReqLimit(rm.getJlScanLbidReqLimit()),
		scanLbidReqThreshold(rm.getJlScanLbidReqThreshold()),
		tempSaveSize(rm.getScTempSaveSize()),
		logger(new Logger()),
		traceFlags(0),
		tupleDLMaxSize(rm.getTwMaxSize()),
		tupleMaxBuckets(rm.getTwMaxBuckets()),
//		status(new uint16_t(0)),
		projectingTableOID(0),
		isExeMgr(false),
		trace(false),
		tryTuples(false),
		constantCol(CONST_COL_NONE),
		hasDistinct(false),
		hasAggregation(false),
		hasImplicitGroupBy(false),
		limitStart(0),
		limitCount(-1),
		joinNum(0),
		subLevel(0),
		subNum(0),
		subId(0),
		pSubId(-1),
		constantFalse(false),
		cntStarPos(-1),
		stringScanThreshold(1),
		wfqLimitStart(0),
		wfqLimitCount(-1)
	{ }
	ResourceManager& rm;
	uint32_t  sessionId;
	uint32_t  txnId;
	BRM::QueryContext  verId;
	uint32_t  statementId;
	std::string  queryType;
	boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
	DeliveredTablesSet tables;
	int       maxBuckets;
	uint64_t  maxElems;
	JobStepVectorStack stack;
	uint32_t  flushInterval;
	uint32_t  fifoSize;
	uint32_t  fifoSizeLargeSideHj;
	//...joblist does not use scanLbidReqLimit and SdanLbidReqThreshold.
	//...They are actually used by pcolscan and pdictionaryscan, but
	//...we have joblist get and report the values here since they
	//...are global to the job.
	uint32_t  scanLbidReqLimit;
	uint32_t  scanLbidReqThreshold;
	uint32_t  tempSaveSize;
	SPJL      logger;
	uint32_t  traceFlags;
	uint64_t  tupleDLMaxSize;
	uint32_t  tupleMaxBuckets;
	SErrorInfo errorInfo;
	execplan::CalpontSystemCatalog::OID* projectingTableOID; // DeliveryWSDLs get a reference to this
	bool      isExeMgr;
	bool      trace;
	bool      tryTuples;
	int8_t    constantCol;
	TupleInfoVector pjColList;

	// aggregation
	bool       hasDistinct;
	bool       hasAggregation;
	bool       hasImplicitGroupBy;
	std::vector<uint32_t>                  groupByColVec;
	std::vector<uint32_t>                  distinctColVec;
	std::vector<uint32_t>                  expressionVec;
	std::vector<std::pair<uint32_t, int> > returnedColVec;

	// order by and limit
	std::vector<std::pair<uint32_t, bool> > orderByColVec;
	uint64_t                                limitStart;
	uint64_t                                limitCount;

	// tupleInfo
	boost::shared_ptr<TupleKeyInfo> keyInfo;

	// skip dictionary step if the real string is not necessary to projected.
	// In most case, the string is used for return or comparison, so default is false.
	//     when setting to false, no need to check: false overwrites true;
	//     When setting to true, need check: true cannot overwrite false.
	std::map<uint, bool> tokenOnly;

	// unique ID list of the tables in from clause
	std::vector<uint32_t> tableList;

	// table join map
	TableJoinMap tableJoinMap;

	// for expression
	JobStepVector crossTableExpressions;
	JobStepVector returnedExpressions;

	// for function on aggregation
	RetColsVector deliveredCols;    // columns to be sent to connector
	RetColsVector nonConstCols;     // none constant columns
	RetColsVector nonConstDelCols;  // delivered none constant columns
	RetColsVector projectionCols;   // columns for projection
//	std::set<uint64_t> returnColSet;// columns returned
	std::multimap<execplan::ReturnedColumn*, execplan::ReturnedColumn*> cloneAggregateColMap;
	std::vector<std::pair<int, int> > aggEidIndexList;

	// for AVG to support CNX_USE_DECIMAL_SCALE
	//   map<key, column scale << 8 + avg scale>
	std::map<uint, int> scaleOfAvg;

	// table pairs with incompatible join which is treated as expression
	std::map<uint, uint> incompatibleJoinMap;

	// bug 1573 & 3391, having
	SJSTEP         havingStep;
	JobStepVector  havingStepVec;

	// bug 2634, 5311 and 5374, outjoin and predicates
	std::set<uint> outerOnTable;
	std::set<uint> tableHasIsNull;
	JobStepVector  outerJoinExpressions;

	// bug 3759, join in order
	// mixed outer join
	std::map<int, uint64_t> tableSize;
	int64_t joinNum;

	// for subquery
	boost::shared_ptr<int> subCount;      // # of subqueries in the query statement
	int                    subLevel;      // subquery level
	int                    subNum;        // # of subqueries @ level n
	int                    subId;         // id of current subquery
	int                    pSubId;        // id of outer query
	bool                   constantFalse; // has constant false filter
	JobStepVector          correlateSteps;
	JobStepVector          selectAndFromSubs;
	std::set<uint64_t>     returnColSet;
	std::map<UniqId, execplan::CalpontSystemCatalog::ColType> vtableColTypes;

	// step to process orderby, limit and fill in constants
	SJSTEP annexStep;

	// @bug3475, aggregate constant column <position, aggregate column>
	std::map<uint64_t, execplan::SRCP> constAggregate;
	int64_t cntStarPos;  // position of count(*)

	// @bug3321, dictionary scan setting, HWM = stringScanThreshold -1
	uint64_t stringScanThreshold;

	// @bug3362, group_concat
	RetColsVector   groupConcatCols;
	GroupConcatInfo groupConcatInfo;

	// @bug3736, column map
	std::map<uint, std::vector<uint> > columnMap;

	// @bug3438, joblist for trace/stats
	JobList* jobListPtr;  // just reference, NOT delete by JobInfo

	// WORKAROUND for join FE limitation (join Id to expression tables map)
	std::map<uint, std::set<uint> > joinFeTableMap;

	uint stringTableThreshold;

	// @bug4531, Window Function support
	RetColsVector windowCols;
	RetColsVector windowExps;
	RetColsVector windowDels;
	std::set<uint64_t> windowSet;
	RetColsVector wfqOrderby;
	uint64_t      wfqLimitStart;
	uint64_t      wfqLimitCount;
	// workaround for expression of windowfunction in IN/EXISTS sub-query
	//std::map<uint, RetColsVector>  exprWinfuncListMap;

private:
	//defaults okay
	//JobInfo(const JobInfo& rhs);
	//JobInfo& operator=(const JobInfo& rhs);
};


//------------------------------------------------------------------------------
// namespace scoped functions
//------------------------------------------------------------------------------

/** @brief Returns the table alias for the specified column
 *
 */
std::string extractTableAlias(const execplan::SimpleColumn* sc);

/** @brief Returns the table alias for the specified column
 *
 */
std::string extractTableAlias(const execplan::SSC& sc);

/** @brief Returns OID associated with colType if it is a dictionary column
 *
 */
execplan::CalpontSystemCatalog::OID isDictCol(const execplan::CalpontSystemCatalog::ColType& colType);

/** @brief Determines if colType is a character column
 *
 */
bool isCharCol(const execplan::CalpontSystemCatalog::ColType& colType);

/** @brief Returns OID associated with a table
 *
 */
execplan::CalpontSystemCatalog::OID tableOid(const execplan::SimpleColumn* sc,
	boost::shared_ptr<execplan::CalpontSystemCatalog> cat);

/** @brief Returns the unique ID to be used in tupleInfo
 *
 */
uint getTupleKey(const JobInfo& jobInfo,
	const execplan::SimpleColumn* sc);
uint getTableKey(const JobInfo& jobInfo,
	execplan::CalpontSystemCatalog::OID tableOid,
	const std::string& alias,
	const std::string& schema,
	const std::string& view);
uint getTupleKey(JobInfo& jobInfo,
	const execplan::SRCP& srcp,
	bool add = false);
uint getTableKey(const JobInfo& jobInfo,
	uint cid);
uint getTableKey(JobInfo& jobInfo,
	JobStep* js);

uint getExpTupleKey(const JobInfo& jobInfo,
	uint64_t eid);

uint makeTableKey(JobInfo& jobInfo,
	const execplan::SimpleColumn* sc);
uint makeTableKey(JobInfo& jobInfo,
	execplan::CalpontSystemCatalog::OID tableOid,
	const std::string& tbl_name,
	const std::string& tbl_alias,
	const std::string& sch_name,
	const std::string& vw_name,
	uint64_t engine = 0);

/** @brief Returns the tupleInfo associate with the (table, column) key pair
 *
 */
TupleInfo getTupleInfo(uint tableKey, uint columnKey, const JobInfo& jobInfo);

/** @brief Returns the tupleInfo associate with the expression
 *
 */
TupleInfo getExpTupleInfo(uint expKey, const JobInfo& jobInfo);

/** @brief set tuple info for simple column
 *
 */
TupleInfo setTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct,
	execplan::CalpontSystemCatalog::OID col_oid,
	JobInfo& jobInfo,
	execplan::CalpontSystemCatalog::OID tbl_oid,
	const execplan::SimpleColumn* sc,
	const std::string& alias);

/** @brief set tuple info for expressions
 *
 */
TupleInfo setExpTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct,
	uint64_t expressionId,
	const std::string& alias,
	JobInfo& jobInfo);

/** @brief add an aggregate column info
 *
 */
void addAggregateColumn(execplan::AggregateColumn*, int, RetColsVector&, JobInfo&);

void makeJobSteps(execplan::CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
    JobStepVector& querySteps, JobStepVector& projectSteps, DeliveredTableMap& deliverySteps);

void makeUnionJobSteps(execplan::CalpontSelectExecutionPlan* csep, JobInfo& jobInfo,
    JobStepVector& querySteps, JobStepVector&, DeliveredTableMap& deliverySteps);

void updateDerivedColumn(JobInfo&, execplan::SimpleColumn*,
	execplan::CalpontSystemCatalog::ColType&);

bool filterWithDictionary(execplan::CalpontSystemCatalog::OID dictOid, uint64_t n);

} // end of jlf_common namespace

#endif
