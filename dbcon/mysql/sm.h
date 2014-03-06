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

/***********************************************************************
 *   $Id: sm.h 9254 2013-02-04 19:40:31Z rdempsey $
 *
 ***********************************************************************/
/** @file */

#ifndef SM_H__
#define SM_H__

#include <stdint.h>
#include <set>
#include <map>
#include <string>
#include <sys/time.h>
#include <iostream>

#include "calpontsystemcatalog.h"
#include "clientrotator.h"
#include "rowgroup.h"
#include "calpontselectexecutionplan.h"
#include "querystats.h"

#define IDB_SM_DEBUG 0
#define IDB_SM_PROFILE 0

#if defined(_MSC_VER) && defined(xxxDLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#if IDB_SM_PROFILE
#include <sys/time.h>
#define GET_PF_TIME(n) {gettimeofday(&n, NULL);}
#else
#define GET_PF_TIME(n) {}
#endif

namespace sm
{
const int STATUS_OK=0;
const int SQL_NOT_FOUND=-1000;
const int SQL_KILLED=-1001;
const int CALPONT_INTERNAL_ERROR=-1007;

#if IDB_SM_DEBUG
extern std::ofstream smlog;
#define SMDEBUGLOG smlog
#else
#define SMDEBUGLOG if (false) std::cerr
#endif
extern const std::string DEFAULT_SAVE_PATH;

typedef uint64_t tableid_t;
typedef int32_t status_t;

enum QueryState
{
	NO_QUERY = 0,
	QUERY_IN_PROCESS
};

typedef struct Column {
	Column():tableID(-1) {}
	~Column() {}
	int tableID;
	int colPos;
	int dataType;
	std::vector <std::string> data;
} Column;

typedef std::map <int, Column*> ResultMap;

struct Profiler
{
	struct timeval login;
	struct timeval beforePlan;
	struct timeval afterPlan;
	struct timeval resultArrival;
	struct timeval resultReady;
	struct timeval endProcess;
	long prePlan(){
		return (beforePlan.tv_sec-login.tv_sec) * 1000 +
		(beforePlan.tv_usec - login.tv_usec)/1000; }
	long buildPlan() {
		return (afterPlan.tv_sec - beforePlan.tv_sec) * 1000 +
		(afterPlan.tv_usec - beforePlan.tv_usec)/1000; }
	long jobProcess() {
		return (resultArrival.tv_sec - afterPlan.tv_sec) * 1000 +
		(resultArrival.tv_usec - afterPlan.tv_usec)/1000; }
	long buildResult() {
		return (resultReady.tv_sec - resultArrival.tv_sec) * 1000 +
		(resultReady.tv_usec - resultArrival.tv_usec)/1000; }
	long tableFetch () {
		return (endProcess.tv_sec - resultReady.tv_sec) * 1000 +
		(endProcess.tv_usec - resultReady.tv_usec)/1000; }
};

/** @brief Calpont table scan handle */
struct cpsm_tplsch_t
{
	cpsm_tplsch_t() : tableid(0), rowsreturned(0), rowGroup(0), traceFlags(0), bandID(0), saveFlag(0), bandsReturned(0),
		ctp(0) {}
	~cpsm_tplsch_t()
	{
		delete rowGroup;
	}

	tableid_t tableid;
	uint64_t rowsreturned;
	rowgroup::RowGroup *rowGroup;
	messageqcpp::ByteStream bs;	// rowgroup bytestream. need to stay with the life span of rowgroup
	uint32_t traceFlags;
	// @bug 649
	int bandID;		 // the band that being read from the disk
	int key;			// unique key for the table's scan context
	// @bug 626
	uint16_t saveFlag;
	uint32_t bandsReturned;
	std::vector<execplan::CalpontSystemCatalog::ColType> ctp;
	std::string errMsg;
	rowgroup::RGData rgData;
	void deserializeTable(messageqcpp::ByteStream& bs)
	{
		if (!rowGroup)
		{
			rowGroup = new rowgroup::RowGroup();
			rowGroup->deserialize(bs);
		}
		else
		{
			// XXXST: the 'true' is to ease the transition to RGDatas.  Take it out when the
			// transition is done.
			rgData.deserialize(bs, true);
			rowGroup->setData(&rgData);
			//rowGroup->setData(const_cast<uint8_t*>(bs.buf()));
		}
	}

	uint16_t getStatus()
	{
		idbassert(rowGroup != 0);
		return rowGroup->getStatus();
	}

	uint64_t getRowCount()
	{
		if (rowGroup)
			return rowGroup->getRowCount();
		else
			return 0;
	}

	void setErrMsg()
	{
		if (rowGroup && getStatus())
		{
			//bs.advance(rowGroup->getDataSize());
			bs >> errMsg;
		}
		else
		{
			errMsg = "NOERROR";
		}
	}
};
typedef boost::shared_ptr<cpsm_tplsch_t> sp_cpsm_tplsch_t;

/** @brief Calpont connection handle structure */
class cpsm_conhdl_t
{
public:
	cpsm_conhdl_t(time_t v, const uint32_t sid, bool infinidb_local_query) :
	value(v), sessionID(sid), queryState (NO_QUERY),
	exeMgr( new execplan::ClientRotator(sid, "ExeMgr", infinidb_local_query)),
	tblinfo_idx(0), idxinfo_idx(0), curFetchTb (0)
	{ }


/** @brief connnect ExeMgr
 *
 * Try connecting to ExeMgr.  If no connection, try ExeMgr1,
 * ExeMgr2... until timeout lapses. Then throw exception.
 */
	void connect(double timeout=0.005) {
		exeMgr->connect(timeout);
	}
	EXPORT void write(messageqcpp::ByteStream bs);

	~cpsm_conhdl_t() {
			delete exeMgr;
	}
	EXPORT const std::string toString() const;
	time_t value;
	uint32_t sessionID;
	short queryState;   // 0 -- NO_QUERY; 1 -- QUERY_IN_PROCESS
   	execplan::ClientRotator* exeMgr;
	ResultMap resultSet;
	Profiler pf;
	int tblinfo_idx;
	int idxinfo_idx;
	std::string schemaname;
	std::string tablename;
	int tboid;
	short requestType; // 0 -- ID2NAME; 1 -- NAME2ID
	boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
	// @bug 649; @bug 626
	std::map <int, int> tidMap;	 // tableid-tableStartCount map
	std::map <int, sp_cpsm_tplsch_t> tidScanMap;
	std::map <int, int> keyBandMap; // key-savedBandCount map
	int curFetchTb;				 // current fetching table key
	std::string queryStats;
	std::string extendedStats;
	std::string miniStats;
private:
};
std::ostream& operator<<(std::ostream& output, const cpsm_conhdl_t& rhs);

// @bug 626 save table bands to avoid sending plan too many times
enum SavingFlag
{
	NO_SAVE = 0,
	SAVING,
	SAVED
};

/** @brief Calpont table handle */
struct cpsm_tplh_t
{
	cpsm_tplh_t() : tableid(0), rowsintable(0), bandID(0), saveFlag(NO_SAVE), bandsInTable(0) {}
	tableid_t tableid;
	int rowsintable;
	// @bug 649
	int bandID;		 // the band that being read from the disk
	int key;			// unique key for the table's scan context
	// @bug 626
	uint16_t saveFlag;
	int bandsInTable;
};

struct cpsm_tid_t
{
	cpsm_tid_t() : valid(false), value(0) {}
	bool valid;
	int value;
};

extern status_t sm_init(uint32_t, cpsm_conhdl_t**, uint32_t infinidb_local_query = false);
extern status_t sm_cleanup(cpsm_conhdl_t*);

extern status_t tpl_open(tableid_t, cpsm_tplh_t*, cpsm_conhdl_t*);
extern status_t tpl_scan_open(tableid_t, sp_cpsm_tplsch_t&, cpsm_conhdl_t*);
extern status_t tpl_scan_fetch(sp_cpsm_tplsch_t&, cpsm_conhdl_t*, int* k=0);
extern status_t tpl_scan_close(sp_cpsm_tplsch_t&);
extern status_t tpl_close(cpsm_tplh_t*, cpsm_conhdl_t**, querystats::QueryStats& stats);

}

#undef EXPORT

#endif

