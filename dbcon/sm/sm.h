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
 *   $Id: sm.h 9262 2013-02-06 21:07:19Z zzhu $
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

const int MAX_IDLEN=32+1;

#if IDB_SM_DEBUG
extern std::ofstream smlog;
#define DEBUG smlog
#else
#define DEBUG if (false) std::cerr
#endif
extern const std::string DEFAULT_SAVE_PATH;

typedef enum
{
    TPL_FH_READ,   /* Fetch for read       */
    TPL_FH_WRITE   /* Fetch for update     */
} tpl_fetch_hint_t;
typedef unsigned short field_t;
typedef int32_t typeid_t;
typedef unsigned char sm_boolean;
typedef struct {
    typeid_t       dt_data_type;   /* Data type                       */
    sm_boolean          dt_is_data_null;/* Is the value stored NULL        */
    unsigned short      dt_maxlen;      /* Maximum length of data          */
    unsigned short      dt_data_len;    /* Actual length of data - defined */
                                        /* for char and binary types.      */
    short               dt_width;       /* Data type precision - Ignored   */
                                        /* except for numeric types        */
    short               dt_scale;       /* Scale for numeric types         */
    void                *dt_data;       /* Pointer to store the data       */
} data_t;
typedef struct {
    field_t   fv_field;    /* Field id in the table                */
    field_t   fv_tfield;   /* Field id in the table if fv_field is */
                                /* index field id.                      */
    unsigned short fv_maxlen;   /* Maximum length of the field          */
    data_t    *fv_data;    /* Data value in the field              */
} fv_item_t;
typedef struct {
    short               fv_nitems ;/* Number of field values in the list  */
    fv_item_t      *fv_item ; /* Pointer to an array of field values */
} fldl_val_t;

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

	char tbName[100];
	tableid_t tableid;
	uint64_t rowsreturned;
	rowgroup::RowGroup *rowGroup;
	messageqcpp::ByteStream bs;	// rowgroup bytestream. need to stay with the life span of rowgroup
	uint32_t traceFlags;
	// @bug 649
	int bandID;         // the band that being read from the disk
	int key;            // unique key for the table's scan context
	// @bug 626
	uint16_t saveFlag;  
	uint32_t bandsReturned;
	std::vector<execplan::CalpontSystemCatalog::ColType> ctp;
	std::string errMsg;
	void deserializeTable(messageqcpp::ByteStream& bs)
	{
		if (!rowGroup)
		{
			rowGroup = new rowgroup::RowGroup();
			rowGroup->deserialize(bs);
		}
		else
		{
			rowGroup->setData(const_cast<uint8_t*>(bs.buf())); 
		}
	}
	
	uint16_t getStatus()
	{
		assert (rowGroup != 0);
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
			bs.advance(rowGroup->getDataSize());
			bs >> errMsg;
		}
		else
		{
			errMsg = "NOERROR";
		}
	}
};
typedef boost::shared_ptr<cpsm_tplsch_t> sp_cpsm_tplsch_t;

enum RequestType
{
    ID2NAME = 0,
    NAME2ID
};

/** @brief Calpont connection handle structure */
class cpsm_conhdl_t
{
public:
  cpsm_conhdl_t(time_t v, const uint32_t sid) : 
    value(v), sessionID(sid), queryState (NO_QUERY),
    exeMgr( new execplan::ClientRotator(sid, "ExeMgr")), 
    tblinfo_idx(0), idxinfo_idx(0), curFetchTb (0) //, EXEMGR("ExeMgr")
    { }


/** @brief connnect ExeMgr
 *
 * Try connecting to ExeMgr.  If no connection, try ExeMgr1,
 * ExeMgr2... until timeout lapses. Then throw exception.
 */   
     EXPORT void connect(double timeout=0.005);
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
	execplan::CalpontSystemCatalog* csc;
	// @bug 649; @bug 626
	std::map <int, int> tidMap;     // tableid-tableStartCount map
	std::map <int, sp_cpsm_tplsch_t> tidScanMap;
	std::map <int, int> keyBandMap; // key-savedBandCount map
	int curFetchTb;                 // current fetching table key
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
	char tbName[100];
	// @bug 649
	int bandID;         // the band that being read from the disk
	int key;            // unique key for the table's scan context
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

typedef struct {
    tableid_t       id;
    char       table_name[MAX_IDLEN];
    char       table_owner[MAX_IDLEN];
    sm_boolean read_only ;
    sm_boolean ispublic ;
} tblinfo_t;

extern status_t sm_init(const char*, const char*, const char*, const char*, void**);
extern status_t sm_cleanup(void*);

extern status_t tpl_open(tableid_t, void**, void*);
extern status_t tpl_scan_open(tableid_t, tpl_fetch_hint_t, sp_cpsm_tplsch_t&, void*);
extern status_t tpl_scan_fetch(sp_cpsm_tplsch_t&, fldl_val_t*, void*, void*, int* = NULL);
extern status_t tpl_scan_close(sp_cpsm_tplsch_t&, void*);
extern status_t tpl_close(void*, cpsm_conhdl_t**, querystats::QueryStats& stats);

extern int tbname2id(const char*, const char*, void*);
/** @brief Util function to map table id to table name */
extern char* tblid2name(char* workbuf, tableid_t tableid, void* conn_hdl);

extern status_t char_to_tid( short len, const char *in_buf, void *tid, void *conn_hdl);
extern status_t get_tblinfo ( tblinfo_t *info, void *conn_hdl);

}

#undef EXPORT

#endif

