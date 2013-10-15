/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

// $Id: inggwif.cpp 9210 2013-01-21 14:10:42Z rdempsey $

#include <cstring>
#include <cstdlib>
#include <stdexcept>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dhcs.h"
#include "dhcs_misc.h"
using namespace sm;

#include "inggwif.h"
#include "te2.h"

#pragma pack(push, 1)
struct AD_TIMESTAMP
{
    short        dn_year;                /* year component (1-9999) */
    char         dn_month;               /* month component (1-12) */
    char         dn_day;                 /* day component (1-31) */
    int          dn_time;                /* seconds since midnight */
    int          dn_nsecond;             /* nanoseconds since last second */
    char         dn_tzhour;              /* timezone hour component (-12 to 14)*/
    char         dn_tzminute;            /* timezone minute component (0-59) */
};
#pragma pack(pop)
typedef void* PTR;
typedef int i4;
//typedef void DB_TAB_NAME;
//typedef void DB_TAB_ID;
typedef void DMT_TBL_ENTRY;
typedef void DMT_ATT_ENTRY;
typedef void DMT_IDX_ENTRY;
typedef void GWX_VECTOR;
//typedef void* DM_DATA;
//typedef void* DM_PTR;
typedef int DB_ERROR;
typedef int DB_STATUS;
typedef void* GW_SESSION;
const int DB_MAXNAME=32;
struct DB_TAB_NAME
{
	char db_tab_name[DB_MAXNAME];
};
struct  DB_TAB_ID
{
	i4 db_tab_base;
	i4 db_tab_index;
};
struct DM_DATA
{
	char *data_address;          /* Pointer to data space.  */
	i4 data_in_size;           /* Input data length. */
	i4 data_out_size;          /* Output data length. */
};
struct DM_PTR
{
	PTR ptr_address;            /* Pointer to array of pointers. */
	i4 ptr_in_count;           /* Input number of array pointers. */
	i4 ptr_out_count;          /* Output number of pointers used. */
	i4 ptr_size;               /* The size of each object. */
	i4 ptr_size_filler;        /* Pad for raat, which writes a pointer to ptr_size */
};
struct GWX_RCB
{
    PTR             xrcb_rsb;           /* Pointer to the GW_RSB for this table
                                        ** or NULL if this operation does not
                                        ** apply to an open table.
                                        */
    i4      xrcb_gwf_version;   /* GWF version id */
    i4              xrcb_gw_id;         /* gateway id */
    DB_TAB_NAME     *xrcb_tab_name;     /* INGRES table name */
    DB_TAB_ID       *xrcb_tab_id;       /* INGRES table id */
    i4      xrcb_access_mode;   /* DMT_READ, DMT_WRITE */
    i4      xrcb_flags;         /* Flags for record operations */
    DMT_TBL_ENTRY   *xrcb_table_desc;   /* INGRES table description */
    i4      xrcb_column_cnt;    /* column count */
    DMT_ATT_ENTRY   *xrcb_column_attr;  /* Array of DMT_ATT_ENTRY */
    DMT_IDX_ENTRY   *xrcb_index_desc;   /* Pointer to index entry if index */
    GWX_VECTOR      *xrcb_exit_table;   /* array of ptrs to exit functions */
    i4      xrcb_exit_cb_size;  /* exit rsb size */
    i4      xrcb_xrelation_sz;  /* iigwX_relation size */
    i4      xrcb_xattribute_sz; /* iigwX_attribute size */
    i4      xrcb_xindex_sz;     /* iigwX_index size */

                                        /* shared use data buffer structures */
    DM_DATA         xrcb_var_data1;     /* a variable length data */
    DM_DATA         xrcb_var_data2;     /* a variable length data */
    DM_DATA         xrcb_var_data3;     /* a variable length data */
    DM_DATA         xrcb_var_data4;     /* a variable length data */
    DM_PTR          xrcb_var_data_list; /* a list of variable length data */
    DM_PTR          xrcb_mtuple_buffs;  /* multiple tuple bufferss, same size */
    i4      xrcb_page_cnt;      /* number of "pages" in gateway file */
    PTR             xrcb_xatt_tidp;     /* ptr to tidp tuple for extended attr.
                                           relation for this gateway */
    DB_ERROR        xrcb_error;         /* Contains error information */
    DB_STATUS       (*xrcb_dmf_cptr)(); /* This function pointer contains the
                                        ** address of dmf_call(). It is for
                                        ** the DMF gateway only. The DMF gateway
                                        ** uses this call pointer to avoid link
                                        ** problems.
                                        */
                                        /* Next 2 fields used by DMF gateway */
    PTR             xrcb_database_id;   /* DMF database ID */
    PTR             xrcb_xact_id;       /* DMF transaction ID */
    PTR             *xrcb_xhandle;      /* field to support server wide data */
                                        /* for a specific gateway,i.e. RMS   */
    i4              xrcb_xbitset;       /* bit vector */
    GW_SESSION      *xrcb_gw_session;   /* session information */
    i4         xrcb_gchdr_size;    /* GC hdr size */
};

namespace
{

struct IngQCtx
{
	IngQCtx(uint32_t sid) : sessionID(sid), dhcs_ses_ctx(0)
	{
	}
	~IngQCtx() { }
	uint32_t sessionID;
	void* dhcs_ses_ctx;
	CalpontSelectExecutionPlan csep;
private:
	//defaults okay?
	IngQCtx(const IngQCtx& rhs);
	IngQCtx operator=(const IngQCtx& rhs);
};

struct IngTCtx
{
	IngTCtx() :dhcs_tpl_ctx(0), dhcs_tpl_scan_ctx(0) { }
	~IngTCtx() { }
	void* dhcs_tpl_ctx;
	void* dhcs_tpl_scan_ctx;
private:
	//defaults okay?
	IngTCtx(const IngTCtx& rhs);
	IngTCtx operator=(const IngTCtx& rhs);
};

}

void inggwif_init() __attribute__((constructor));
void inggwif_fini() __attribute__((destructor));

void inggwif_init()
{
	//setenv("CALPONT_CONFIG_FILE", "/home/rdempsey/Calpont/Calpont.xml", 1);
	//setenv("CALPONT_HOME", "/home/rdempsey/Calpont", 1);
}

void inggwif_fini()
{
}

void* ing_getqctx()
{
try {
	uint32_t sessionID = 100;
	char sessionIDstr[80];
	snprintf(sessionIDstr, 80, "%u", sessionID);
	sessionIDstr[79] = 0;
	IngQCtx* qctx = new IngQCtx(sessionID);
	dhcs_rss_init(0, 0, 0, sessionIDstr, &qctx->dhcs_ses_ctx);
	cpsm_conhdl_t* cal_conn_hndl = reinterpret_cast<cpsm_conhdl_t*>(qctx->dhcs_ses_ctx);
	cal_conn_hndl->csc  = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	cal_conn_hndl->csc->sessionID(sessionID);
	cal_conn_hndl->csc->identity(CalpontSystemCatalog::FE);
	return qctx;
} catch (...) {
}
	return NULL;
}

int ing_sendplan(void* qctx)
{
try {
	if (!qctx) return -1;
	IngQCtx* ingQctx = reinterpret_cast<IngQCtx*>(qctx);
	cpsm_conhdl_t* cal_conn_hndl = reinterpret_cast<cpsm_conhdl_t*>(ingQctx->dhcs_ses_ctx);

	ByteStream bs;
	bs.load(te2, te2len);
	ByteStream tbs(bs);
	ingQctx->csep.unserialize(tbs);

	cal_conn_hndl->connect();
	cal_conn_hndl->exeMgr->write(bs);

	return 0;
} catch (...) {
}
	return -1;
}

void* ing_opentbl(void* qctx, const char* sn, const char* tn)
{
try {
	if (!qctx) return 0;
	if (!sn) return 0;
	if (!tn) return 0;
	if (strlen(sn) == 0) return 0;
	if (strlen(tn) == 0) return 0;

	IngQCtx* ingQctx = reinterpret_cast<IngQCtx*>(qctx);
	cpsm_conhdl_t* cal_conn_hndl = reinterpret_cast<cpsm_conhdl_t*>(ingQctx->dhcs_ses_ctx);
	IngTCtx* tctx = new IngTCtx();

	dhcs_tableid_t tableid = 0;
	//tableid = tbname2id(sn, tn, cal_conn_hndl);
	tableid = tbname2id("te", "f_trans", cal_conn_hndl);

	RequiredColOidSet requiredColOidSet;
	const CalpontSelectExecutionPlan::ColumnMap colMap = ingQctx->csep.columnMap();
	CalpontSelectExecutionPlan::ColumnMap::const_iterator colIter = colMap.begin();
	CalpontSelectExecutionPlan::ColumnMap::const_iterator colend = colMap.end();
	while (colIter != colend)
	{
		SRCP srcp = colIter->second;
		SimpleColumn* scp = dynamic_cast<SimpleColumn*>(srcp.get());
		if (scp)
			requiredColOidSet.insert(scp->oid());
		++colIter;
	}
	tctx->dhcs_tpl_ctx = new cpsm_tplh_t();
	dhcs_tpl_open(tableid, &tctx->dhcs_tpl_ctx, ingQctx->dhcs_ses_ctx, requiredColOidSet);
	tctx->dhcs_tpl_scan_ctx = new cpsm_tplsch_t();
	dhcs_tpl_scan_open(tableid, DHCS_TPL_FH_READ, &tctx->dhcs_tpl_scan_ctx, ingQctx->dhcs_ses_ctx);

	return tctx;
} catch (...) {
}
	return NULL;
}

int ing_getrows(void* qctx, void* tctx, char* buf, unsigned rowlen, unsigned nrows)
{
try {
	if (!qctx) return -1;
	if (!tctx) return -1;
	if (!buf) return -1;
	if (rowlen == 0) return -1;
	if (nrows == 0) return 0;

	IngQCtx* ingQctx = reinterpret_cast<IngQCtx*>(qctx);
	IngTCtx* ingTctx = reinterpret_cast<IngTCtx*>(tctx);
	cpsm_conhdl_t* cal_conn_hndl = reinterpret_cast<cpsm_conhdl_t*>(ingQctx->dhcs_ses_ctx);
	dhcs_status_t sm_stat;
	dhcs_fldl_val_t flds;

	const int num_attr = 42;
	const int alen = 40;
	int len = 40;
	flds.fv_nitems = num_attr;
	dhcs_fv_item_t fitems[num_attr];
	dhcs_data_t fditems[num_attr];
	flds.fv_item = fitems;
	CalpontSystemCatalog::ColType ct[num_attr];
	cal_conn_hndl->ctp = ct;

	for (int i = 0; i < num_attr; i++)
	{
		fditems[i].dt_data_type = DHCS_CHAR;
		fditems[i].dt_is_data_null = 0;
		fditems[i].dt_maxlen = alen;
		fditems[i].dt_data_len = 0;
		fditems[i].dt_width = 0;
		fditems[i].dt_data = (char*)alloca(alen);

		fitems[i].fv_field = i;
		fitems[i].fv_tfield = 0;
		fitems[i].fv_maxlen = len;
		fitems[i].fv_data = &fditems[i];
	}

	sm_stat = dhcs_tpl_scan_fetch(ingTctx->dhcs_tpl_scan_ctx, &flds, 0, ingQctx->dhcs_ses_ctx);

	if (sm_stat == SQL_NOT_FOUND)
		return 1;
	else if (sm_stat != STATUS_OK)
		return -1;

        AD_TIMESTAMP ts;
        char* ptr;
        ptr = buf;

        //prim_cookie
        len = 8;
        memcpy(ptr, fditems[1].dt_data, len);
        ptr += len;

        //trans_typ
        len = 1;
        memcpy(ptr, fditems[3].dt_data, len);
        ptr += len;

        //site_nbr
        len = 4;
        memcpy(ptr, fditems[11].dt_data, len);
        ptr += len;

        //site_section_id
        len = 4;
        memcpy(ptr, fditems[14].dt_data, len);
        ptr += len;

        //creativeid
        len = 4;
        memcpy(ptr, fditems[23].dt_data, len);
        ptr += len;

        //cmpgn_nbr
        len = 4;
        memcpy(ptr, fditems[34].dt_data, len);
        ptr += len;

        //record_timestamp 35
        ts.dn_year = 2007;
        ts.dn_month = 7;
        ts.dn_day = 1;
        ts.dn_time = 60 * 60 * 8;
        ts.dn_nsecond = 0;
        ts.dn_tzhour = -5;
        ts.dn_tzminute = 0;
        //len = sizeof(ts);
        len = 14;
        memcpy(ptr, &ts, len);
        ptr += len;

        //revenue
        len = 8;
        memcpy(ptr, fditems[37].dt_data, len);
        ptr += len;

        //ping_type
        len = 1;
        memcpy(ptr, fditems[40].dt_data, len);
        ptr += len;

	return 0;
} catch (...) {
}
	return -1;
}

int ing_closetbl(void* qctx, void* tctx)
{
try {
	if (!qctx) return -1;
	if (!tctx) return -1;
	IngQCtx* ingQctx = reinterpret_cast<IngQCtx*>(qctx);
	IngTCtx* ingTctx = reinterpret_cast<IngTCtx*>(tctx);

	dhcs_tpl_scan_close(ingTctx->dhcs_tpl_scan_ctx, ingQctx->dhcs_ses_ctx);
	dhcs_tpl_close(ingTctx->dhcs_tpl_ctx, ingQctx->dhcs_ses_ctx);

	delete ingTctx;
	return 0;
} catch (...) {
}
	return -1;
}

int ing_relqctx(void* qctx)
{
try {
	if (!qctx) return -1;
	IngQCtx* ingQctx = reinterpret_cast<IngQCtx*>(qctx);

	dhcs_rss_cleanup(ingQctx->dhcs_ses_ctx);

	delete ingQctx;
	return 0;
} catch (...) {
}
	return -1;
}

void ing_debugfcn(void* data)
{
	int x;
	if (!data) return;
	GWX_RCB* rcbp = reinterpret_cast<GWX_RCB*>(data);
	if (rcbp->xrcb_gwf_version > 0)
	{
		x = rcbp->xrcb_gwf_version;
	}
	else
	{
		x = 0;
	}
	return;
}

