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
 *   $Id: sm.cpp 8910 2012-09-18 18:52:26Z zzhu $
 *
 ***********************************************************************/

#include <unistd.h>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <sstream>
#include <cstdlib>
#include <signal.h>
#include <cstdio>
#if __FreeBSD__
typedef sig_t sighandler_t;
#endif
using namespace std;

#include <boost/thread.hpp>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "bytestream.h"
using namespace messageqcpp;

#include "errorcodes.h"
using namespace logging;

#include "sm.h"

namespace {
using namespace sm;

// @bug 159 fix. clean up routine when error happened
void cleanup(cpsm_conhdl_t* hndl)
{
		// remove system catalog instance for this statement.
		CalpontSystemCatalog::removeCalpontSystemCatalog(hndl->sessionID);		
		hndl->queryState = NO_QUERY;
		hndl->resultSet.erase(hndl->resultSet.begin(), hndl->resultSet.end());	
}

}

namespace sm
{
#ifdef _MSC_VER
	const std::string DEFAULT_SAVE_PATH = "C:\\Calpont\\tmp";
#else
	const std::string DEFAULT_SAVE_PATH = "/var/tmp";
#endif

status_t 
tpl_open (
		tableid_t tableid,			/* IN - Table id */
		void				 **tplhdl,			/* OUT - Tuple handle */
		void			*conn_hdl		/* IN - connection handle */
)
{
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
		DEBUG << "tpl_open: " << hndl << " tableid: " << tableid << endl;
		
		// @bug 649. No initialization here. take passed in reference
		cpsm_tplh_t* ntplh = static_cast<cpsm_tplh_t*>(*tplhdl);

		// if first time enter this function for a statement, set
		// queryState to QUERY_IN_PRCOESS and get execution plan.
		if (hndl->queryState == NO_QUERY)
		{
				hndl->queryState = QUERY_IN_PROCESS;
		}
	
		try {
				// @bug 626. check saveFlag, if SAVED, do not project
				if (ntplh->saveFlag != SAVED)
				{
						// Request ExeMgr to start projecting table tableid
						CalpontSystemCatalog::OID tableOID = static_cast<CalpontSystemCatalog::OID>(tableid);
						ByteStream::quadbyte qb = static_cast<ByteStream::quadbyte>(tableOID);
						ByteStream bs;
						bs << qb;
						//hndl->exeMgr->write(bs);
						hndl->write(bs);
				}
		}
		catch (std::exception& ex)
		{
				DEBUG << "Exception caught in tpl_open: " << ex.what() << endl;
				//hndl->queryState = NO_QUERY;
				cleanup(hndl);
				return CALPONT_INTERNAL_ERROR;
		}

		ntplh->tableid = tableid;
		char* tmp = (char*)alloca(sizeof(ntplh->tbName));
		strcpy(ntplh->tbName, tblid2name(tmp, ntplh->tableid, conn_hdl));

		return STATUS_OK;
}

status_t 
tpl_scan_open (
		tableid_t				tableid,	/* IN - Table id of the table */
		tpl_fetch_hint_t fh,			/* IN - Tuple fetch hint */
		void **tpl_scanhdl,			/* OUT - Tuple scan handle */
		void *conn_hdl			/* IN - connection handle */
) 
{	
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
		DEBUG << "tpl_scan_open: " << hndl << " tableid: " << tableid << endl;
		
		// @bug 649. No initialization here. take passed in reference
		cpsm_tplsch_t* ntplsch = static_cast<cpsm_tplsch_t*> (*tpl_scanhdl);
		ntplsch->tableid = tableid;
		char* tmp = (char*)alloca(sizeof(ntplsch->tbName));
		strcpy(ntplsch->tbName, tblid2name(tmp, ntplsch->tableid, conn_hdl));

		ntplsch->rowsreturned = 0;
		return STATUS_OK;
}

status_t tpl_scan_fetch_getband(cpsm_conhdl_t* hndl, cpsm_tplsch_t* ntplsch, int* killed)
{	 
		// @bug 649 check keybandmap first
		map<int, int>::iterator keyBandMapIter = hndl->keyBandMap.find(ntplsch->key);
			
		if (keyBandMapIter != hndl->keyBandMap.end())
		{
				ByteStream bs;
				ostringstream oss;
				oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->key << '_' << ntplsch->bandID << ".band";
				ifstream bandFile (oss.str().c_str(), ios::in);
				bandFile >> bs;
				unlink (oss.str().c_str());
			
				// not possible for vtable
				ntplsch->deserializeTable(bs);
				ntplsch->bandID++;
				
				// end of result set
				if (ntplsch->bandID == keyBandMapIter->second)
				{
						hndl->keyBandMap.erase(keyBandMapIter);
						return SQL_NOT_FOUND;
				}
		}
		else
		{		 
				ByteStream bs;							 
				// @bug 626. check saveFlag. If SAVING, read band from socket and save to disk
				if (ntplsch->saveFlag == SAVING)
				{
						ByteStream bs;
						// @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
						//            to reestablish a connection with ExeMgr which ends up causing mysql
						//            session to hang.
						bs = hndl->exeMgr->read();
						ostringstream oss;
						oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->tableid << '_' << ntplsch->bandsReturned << ".band";
						ofstream saveFile (oss.str().c_str(), ios::out);
						saveFile << bs;
						saveFile.close();
						ntplsch->bandsReturned++;
						// not possible for vtable
						ntplsch->deserializeTable(bs);						
				}
				// if SAVED, read from saved file. not possible for vtable
				else if (ntplsch->saveFlag == SAVED)
				{
						ostringstream oss;
						oss << DEFAULT_SAVE_PATH << '/' << hndl->sessionID << '/' << ntplsch->tableid << '_' << ntplsch->bandsReturned << ".band";
						ifstream saveFile (oss.str().c_str(), ios::in);
						saveFile >> bs;
						saveFile.close();
						ntplsch->bandsReturned++;
						ntplsch->deserializeTable(bs);	 			
				}
				// most normal path. also the path for vtable
				else
				{
					ntplsch->bs.restart();
					// @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
					//            to reestablish a connection with ExeMgr which ends up causing mysql
					//            session to hang.
					// @bug 3386. need to abort the query when user does ctrl+c
					timespec t;
					t.tv_sec = 5L;
					t.tv_nsec = 0L;
					if (killed && *killed)
						return SQL_KILLED;
					ntplsch->bs = hndl->exeMgr->read();

					if (ntplsch->bs.length() != 0)
					{
						ntplsch->deserializeTable(ntplsch->bs);
						if (ntplsch->rowGroup && ntplsch->rowGroup->getData() == NULL)
						{
							ntplsch->bs.restart();
							// @bug 2244. Bypass ClientRotator::read() because if I/O error occurs, it tries
							//            to reestablish a connection with ExeMgr which ends up causing mysql
							//            session to hang.
							// @bug4380. clientrotator throws exception for read/write now, so we can remove 
							// the timeout work around.
							ntplsch->bs = hndl->exeMgr->read();
							if (killed && *killed)
								return SQL_KILLED;							

							if (ntplsch->bs.length() == 0)
							{
								hndl->curFetchTb = 0;
								return logging::ERR_LOST_CONN_EXEMGR;
							}
							ntplsch->deserializeTable(ntplsch->bs);
						}
					
						uint16_t error = ntplsch->getStatus();
						if (0 != error)	
						{
							ntplsch->setErrMsg();
							return error;
						}
					}
					else // @todo error handling
					{
						hndl->curFetchTb = 0;
						if (ntplsch->saveFlag == NO_SAVE)
							hndl->tidScanMap[ntplsch->tableid] = *ntplsch;
						return logging::ERR_LOST_CONN_EXEMGR;
					}
				}

				// All done with this table. reset curFetchTb when finish SOCKET reading
				if (ntplsch->getRowCount() == 0)
				{
						hndl->curFetchTb = 0;
						if (ntplsch->saveFlag == NO_SAVE)
								hndl->tidScanMap[ntplsch->tableid] = *ntplsch;
						return SQL_NOT_FOUND;
				}
		}
		ntplsch->rowsreturned = 0;
		return STATUS_OK;
}
status_t 
tpl_scan_fetch (
		void* tpl_scanhdl,				/* IN - Tuple scan handle */
		fldl_val_t* fld_vlist,	/* OUT - Field values fetched */
		void* tid,							/* OUT - Tuple id of the fetched tuple */
		void* conn_hdl,						/* IN - connection handle */
		int* killed
)
{
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
		cpsm_tplsch_t* ntplsch = static_cast<cpsm_tplsch_t*>(tpl_scanhdl);
		cpsm_tid_t* tplid = static_cast<cpsm_tid_t*>(tid);
		
		// @770. force end of result set when this is not the first table to be fetched.
		if (ntplsch->traceFlags & CalpontSelectExecutionPlan::TRACE_NO_ROWS2)
				if (hndl->tidScanMap.size() >= 1)
						return SQL_NOT_FOUND;

		if (tid)
		{
				char b[1024];
				sprintf(b, "%u", (unsigned)ntplsch->rowsreturned);
				char_to_tid(strlen(b), b, tplid, hndl);
		}

		// need another band
		status_t status = STATUS_OK;
		if (ntplsch->rowsreturned == ntplsch->getRowCount())
			status = tpl_scan_fetch_getband(hndl, ntplsch, killed);

		return status;
}

status_t 
tpl_scan_close (
		void *tpl_scanhdl,			/* IN - Tuple scan handle */
		void *conn_hdl			/* IN - connection handle */
) 
{
	cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);

	cpsm_tplsch_t* ntplsch = static_cast<cpsm_tplsch_t*>(tpl_scanhdl);

	DEBUG << "tpl_scan_close: " << hndl;
	if (ntplsch) DEBUG << " tableid: " << ntplsch->tableid;
			DEBUG << endl;

	delete ntplsch;

	return STATUS_OK;
}

status_t
tpl_close (
		void *tplhdl,			/* IN - Tuple handle */
		void *conn_hdl			/* IN - connection handle */
) 
{
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
		
		cpsm_tplh_t* ntplh = static_cast<cpsm_tplh_t*>(tplhdl);
		
		DEBUG << "tpl_close: " << hndl;
		if (ntplh) DEBUG << " tableid: " << ntplh->tableid;
		DEBUG << endl;

		// determine end of result set and end of statement execution	
		if (hndl->queryState == QUERY_IN_PROCESS)
		{ 
				// Get the query stats
				ByteStream bs;
				ByteStream::quadbyte qb = 3;
				bs << qb;
				//hndl->exeMgr->write(bs);
				hndl->write(bs);
				//keep reading until we get a string
				//TODO: really need to fix this! Why is ExeMgr sending other stuff?
				for (int tries = 0; tries < 10; tries++)
				{
					bs = hndl->exeMgr->read();
					if (bs.length() == 0) break;
					try {
						bs >> hndl->queryStats;
						bs >> hndl->extendedStats;
						bs >> hndl->miniStats;
						break;
					} catch (...) {
					}
				}
				// Tell ExeMgr we are done with this query
				bs.reset();
				qb = 0;
				bs << qb;
				//hndl->exeMgr->write(bs);
				hndl->write(bs);
				// remove system catalog instance for this statement.
				// @bug 695. turn on system catalog session cache for FE
				// CalpontSystemCatalog::removeCalpontSystemCatalog(hndl->sessionID);
				hndl->queryState = NO_QUERY;
				// reset at the end of query
				hndl->curFetchTb = 0;
				// @bug 626 clear up 
				hndl->tidMap.clear();
				hndl->tidScanMap.clear();
				hndl->keyBandMap.clear();
		}
		delete ntplh;
		return STATUS_OK;
}

status_t
sm_init (
    const char *database,	/*!< IN - Database name */
    const char *userid,		/*!< IN - User ID to connect to the database */
    const char *passwd,		/*!< IN - Authorization for the given user */

    const char *dboptions,      /*!< IN - Additional connection options */

    void        **conn_hdl      /*!< OUT- connection handle */
) 
{
    // clear file content
#if IDB_SM_DEBUG
    smlog.close();
    smlog.open("/tmp/sm.log");
#endif    
    DEBUG << "sm_init: " << dboptions << endl;
    cpsm_conhdl_t* hndl = new cpsm_conhdl_t(time(0), atoi(dboptions));
    cpsm_conhdl_t** chp = reinterpret_cast<cpsm_conhdl_t**>(conn_hdl);
    *chp = hndl;
    hndl->sessionID = atoi(dboptions);
    
    // profiling statistics
    GET_PF_TIME(hndl->pf.login);
    
    return STATUS_OK;
}

status_t 
sm_cleanup (
	void	*conn_hdl	/*!< IN - Connection handle */
)
{
    cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
    DEBUG << "sm_cleanup: " << hndl << endl;
    
#if IDB_SM_DEBUG    
    DEBUG.close();
#endif
		if (hndl)
    	delete hndl;
    return STATUS_OK;
}

int tbname2id(const char* schema, const char* table, void* conn_hdl)
{
		tblinfo_t ti;
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
		hndl->schemaname = string(schema);
		hndl->tablename = string(table);
		hndl->requestType = NAME2ID;
		get_tblinfo(&ti, conn_hdl);
		return ti.id;
}

char* tblid2name(char* workbuf, tableid_t tableid, void* conn_hdl)
{
	strcpy(workbuf, "unknown");
	
	return workbuf;
}

status_t 
char_to_tid (
    short len,				/* IN - Length of input buffer */
    const char *in_buf,			/* IN - input buffer */
    void *tid,				/* OUT - resultant Tuple id */
    void *conn_hdl			/* IN - connection handle */
) 
{
    //cout << "char_to_tid" << endl;
    cpsm_tid_t* ntid = static_cast<cpsm_tid_t*>(tid);
    if (in_buf)
    {
	ntid->value = atoi(in_buf);
	ntid->valid = true;
    }

    return STATUS_OK;
}

status_t get_tblinfo (
    tblinfo_t* info,	/* OUT - pointer to table information */
    void*          conn_hdl	/* IN - connection handle */
)
{    
    // NOTE: this call and get_colinfo are not comply with dharma interface
    // anymore. Dharma expects them to look up all the tables in the schema and return
    // table info until the end (return SQL_NOT_FOUND). Current implementation only
    // search for the table info given table and schema name. this works fine with Oracle.
    // TODO: need a dharma version for ODBC solution.
    cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(conn_hdl);
    uint32_t  sessionid = hndl->sessionID;
    CalpontSystemCatalog *csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
    csc->sessionID(sessionid);
    csc->identity(CalpontSystemCatalog::FE);
    
    if (hndl->requestType == ID2NAME)
    {
        // should not need this
        return STATUS_OK;
    }
    else //(hndl->requestType == NAME2ID)
    {
        CalpontSystemCatalog::TableName tn;
        tn.schema = hndl->schemaname;
        tn.table = hndl->tablename;
        CalpontSystemCatalog::ROPair rp = csc->tableRID(tn);
        info->id = rp.objnum;
        return STATUS_OK;
    }  
}

// @bug 1054, 863 - SIGPIPE handler
bool sigFlag = false;
void sighandler(int sig_num)
{
    FILE* p;
    char buf[1024];

	if ((p = fopen("/tmp/f1.dat", "a")) != NULL)
	{
		sprintf(buf, "sighandler() hit with %d\n", sig_num);
		fwrite(buf, 1, strlen(buf), p);
		fclose(p);
	}
	sigFlag = true;
	throw runtime_error("zerror");
}

void cpsm_conhdl_t::write(ByteStream bs)
{
#ifdef _MSC_VER
    exeMgr->write(bs);
#else
    sighandler_t old_handler = signal(SIGPIPE, sighandler);
    sigFlag = false;
    exeMgr->write(bs);
	signal(SIGPIPE, old_handler);
    if (sigFlag)
	    throw runtime_error("Broken Pipe Error");
#endif
}

void cpsm_conhdl_t::connect(double timeout)
{
    exeMgr->connect(timeout);
}

}

