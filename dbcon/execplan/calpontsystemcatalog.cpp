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
 *   $Id: calpontsystemcatalog.cpp 8993 2012-10-16 15:59:46Z wweeks $
 *
 *
 ***********************************************************************/
#include <unistd.h>
#include <stdexcept>
#include <string>
#include <iostream>

using namespace std;

#include "messagequeue.h"
#include "calpontsystemcatalog.h"
#include "ddlpkg.h"
#include "expressionparser.h"
#include "calpontselectexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "clientrotator.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "expressionparser.h"
#include "constantcolumn.h"
#include "treenode.h"
#include "operator.h"
#include "sessionmanager.h"
#include "columnresult.h"

#include "joblistfactory.h"
#include "joblist.h"
#include "distributedenginecomm.h"
#include "resourcemanager.h"
using namespace joblist;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "configcpp.h"
#include "liboamcpp.h"
using namespace config;

#include "exceptclasses.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/version.hpp>

#ifdef _MSC_VER
#include "idbregistry.h"
#endif

#undef BAIL_IF_0
#if 1
//We are unlikely to ever get anything more out of this connection, so bail out
#define BAIL_IF_0(m) \
if ((m).length() == 0) \
{ \
fExeMgr->shutdown(); \
throw runtime_error("CALPONT_INTERNAL_ERROR"); \
}
#else
#define BAIL_IF_0(m)
#endif

#undef CSC_DEBUG
#define CSC_DEBUG 0
#if CSC_DEBUG
namespace {
std::ofstream csclog("/tmp/csc.log", std::ios::app);
}
#define DEBUG csclog
#else
#define DEBUG if (false) cerr
#endif

namespace execplan
{

const SOP opeq(new Operator("="));

const string colDataTypeToString(CalpontSystemCatalog::ColDataType cdt)
{
	switch (cdt)
	{
	case CalpontSystemCatalog::BIT:
		return "bit";
		break;
	case CalpontSystemCatalog::TINYINT:
		return "tinyint";
		break;
	case CalpontSystemCatalog::CHAR:
		return "char";
		break;
	case CalpontSystemCatalog::SMALLINT:
		return "smallint";
		break;
	case CalpontSystemCatalog::DECIMAL:
		return "decimal";
		break;
	case CalpontSystemCatalog::MEDINT:
		return "medint";
		break;
	case CalpontSystemCatalog::INT:
		return "int";
		break;
	case CalpontSystemCatalog::FLOAT:
		return "float";
		break;
	case CalpontSystemCatalog::DATE:
		return "date";
		break;
	case CalpontSystemCatalog::BIGINT:
		return "bigint";
		break;
	case CalpontSystemCatalog::DOUBLE:
		return "double";
		break;
	case CalpontSystemCatalog::DATETIME:
		return "datetime";
		break;
	case CalpontSystemCatalog::VARCHAR:
		return "varchar";
		break;
	case CalpontSystemCatalog::VARBINARY:
		return "varbinary";
		break;
	case CalpontSystemCatalog::CLOB:
		return "clob";
		break;
	case CalpontSystemCatalog::BLOB:
		return "blob";
		break;
	default:
		break;
	}
	return "invalid!";	
}

}

namespace execplan
{

typedef CalpontSelectExecutionPlan::ColumnMap::value_type CMVT_;

boost::shared_ptr<SessionManager> fSessionManager;
CalpontSystemCatalog::NJLSysDataList::~NJLSysDataList()
{
    NJLSysDataVector::iterator it;
    for (it = sysDataVec.begin(); it != sysDataVec.end(); it++)
        delete *it;
}
    
const CalpontSystemCatalog::TableColName make_tcn(const string& s, const string& t, const string& c)
{
    CalpontSystemCatalog::TableColName tcns;

    tcns.schema = s;
    tcns.table = t;
    tcns.column = c;
    transform (tcns.schema.begin(), tcns.schema.end(), tcns.schema.begin(), to_lower());
    transform (tcns.table.begin(), tcns.table.end(), tcns.table.begin(), to_lower());
    transform (tcns.column.begin(), tcns.column.end(), tcns.column.begin(), to_lower());
    return tcns;
}

const CalpontSystemCatalog::TableName make_table(const string& s, const string& t)
{
    CalpontSystemCatalog::TableName tn;
    tn.schema = s;
    tn.table = t;
    transform (tn.schema.begin(), tn.schema.end(), tn.schema.begin(), to_lower());
    transform (tn.table.begin(), tn.table.end(), tn.table.begin(), to_lower());
    return tn;
}

const CalpontSystemCatalog::TableAliasName make_aliastable(const string& s, const string& t, const string& a, const bool isInfiniDB)
{
    CalpontSystemCatalog::TableAliasName tn;
    tn.schema = s;
    tn.table = t;
    tn.alias = a;
    tn.view = "";
    tn.fIsInfiniDB = isInfiniDB;
    transform (tn.schema.begin(), tn.schema.end(), tn.schema.begin(), to_lower());
    transform (tn.table.begin(), tn.table.end(), tn.table.begin(), to_lower());
    transform (tn.alias.begin(), tn.alias.end(), tn.alias.begin(), to_lower());
    return tn;
}

const CalpontSystemCatalog::TableAliasName make_aliasview(const string& s, const string& t, const string& a, const string& v, const bool isInfiniDB)
{
    CalpontSystemCatalog::TableAliasName tn;
    tn.schema = s;
    tn.table = t;
    tn.alias = a;
    tn.view = v;
    tn.fIsInfiniDB = isInfiniDB;
    transform (tn.schema.begin(), tn.schema.end(), tn.schema.begin(), to_lower());
    transform (tn.table.begin(), tn.table.end(), tn.table.begin(), to_lower());
    transform (tn.alias.begin(), tn.alias.end(), tn.alias.begin(), to_lower());
    transform (tn.view.begin(), tn.view.end(), tn.view.begin(), to_lower());
    return tn;
}

bool CalpontSystemCatalog::TableColName::operator<(const TableColName& rhs) const
{
    if (schema < rhs.schema)
    {
        return true;
    }
    else if (schema == rhs.schema)
    {
        if (table < rhs.table)
        {
            return true;
        }
        else if (table == rhs.table)
        {
            if (column < rhs.column)
            {
                return true;
            }
        }
    }

    return false;
}

bool CalpontSystemCatalog::TableName::operator<(const TableName& rhs) const
{
    if (schema < rhs.schema)
    {
        return true;
    }
    else if (schema == rhs.schema)
    {
        if (table < rhs.table)
        {
            return true;
        }
    }

    return false;
}

bool CalpontSystemCatalog::TableAliasName::operator<(const TableAliasName& rhs) const
{
    if (schema < rhs.schema)
    {
        return true;
    }
    else if (schema == rhs.schema)
    {
        if (table < rhs.table)
        {
            return true;
        }
        else if (table == rhs.table)
        {
            if (alias < rhs.alias)
            {
                return true;
            }
            else if (alias == rhs.alias)
            {
            	if (view < rhs.view)
            	{
            		return true;
            	}
            	else if (view == rhs.view)
            	{
            		if (fIsInfiniDB < rhs.fIsInfiniDB)
            			return true;
            	}
            }
        }
    }

    return false;
}

void CalpontSystemCatalog::TableAliasName::serialize(messageqcpp::ByteStream& b) const
{
	b << schema;
	b << table;
	b << alias;
	b << view;
	b << static_cast<const ByteStream::doublebyte>(fIsInfiniDB);
}

void CalpontSystemCatalog::TableAliasName::unserialize(messageqcpp::ByteStream& b)
{
	b >> schema;
	b >> table;
	b >> alias;
	b >> view;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fIsInfiniDB);
}

/*static*/
boost::mutex CalpontSystemCatalog::map_mutex;
/*static*/
CalpontSystemCatalog::CatalogMap CalpontSystemCatalog::fCatalogMap;
/*static*/
uint32_t CalpontSystemCatalog::fModuleID = numeric_limits<uint32_t>::max();

const CalpontSystemCatalog::OID CalpontSystemCatalog::lookupOID(const TableColName& tableColName)
{
    if (tableColName.schema.length() == 0 || tableColName.table.length() == 0 || tableColName.column.length() == 0)
        return -1;
    TableColName aTableColName;
    aTableColName.schema = tableColName.schema;
    aTableColName.table = tableColName.table;
    aTableColName.column = tableColName.column;
    transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
    transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
    transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter lookupOID: " << tableColName.schema << "|" << tableColName.table 
                                << "|" << tableColName.column << endl;
	//Check whether cache needs to be flushed
	if ( aTableColName.schema.compare(CALPONT_SCHEMA) != 0) {
		checkSysCatVer();
	}
    boost::mutex::scoped_lock lk2(fOIDmapLock);
    if ( fOIDmap.size() > 0 )
    {
    OIDmap::const_iterator iter = fOIDmap.find(aTableColName);
    	
    // @bug 1358. double check fColRIDMap in case it's not filled with valid rid.       
    if (iter != fOIDmap.end())
    {
    	if (fIdentity == EC && aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
	    {
	    	ColRIDmap::const_iterator iter1 = fColRIDmap.find(aTableColName);
				if ( iter1 != fColRIDmap.end() ) {
	    		return iter->second;
				}
	  	}
		else {
	    	return iter->second;
		}
    }
    }
    lk2.unlock();

    // select objectid,columnlength,datatype,dictobjectid,listobjectid,treeobjectid,columnposition,scale,prec,
    //    defaultvalue from syscolumn where schema=schema and tablename=table and columnname=column;
    CalpontSelectExecutionPlan csep;
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;

    string columnlength = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNLEN_COL;
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
    string datatype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DATATYPE_COL;
    string dictobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL;
    string listobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL;
    string treeobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL;
    string columnposition = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNPOS_COL;
    string scale = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCALE_COL;
    string precision = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+PRECISION_COL;
    string defaultvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DEFAULTVAL_COL;
    // the following columns will be save in cache although it's not needed for now
    string columnname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL;
    string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
    string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
	string compressiontype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COMPRESSIONTYPE_COL;
	string autoincrement = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
	string nextVal = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;
	string nullable = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NULLABLE_COL;

    SimpleColumn* col[17];
    col[0] = new SimpleColumn(columnlength, fSessionID);
    col[1] = new SimpleColumn(objectid, fSessionID); 
    col[2] = new SimpleColumn(datatype, fSessionID);
    col[3] = new SimpleColumn(dictobjectid, fSessionID);
    col[4] = new SimpleColumn(listobjectid, fSessionID);
    col[5] = new SimpleColumn(treeobjectid, fSessionID);
    col[6] = new SimpleColumn(columnposition, fSessionID);
    col[7] = new SimpleColumn(scale, fSessionID);
    col[8] = new SimpleColumn(precision, fSessionID);
    col[9] = new SimpleColumn(defaultvalue, fSessionID);
    col[10] = new SimpleColumn(schemaname, fSessionID);
    col[11] = new SimpleColumn(tablename, fSessionID);
    col[12] = new SimpleColumn(columnname, fSessionID);
	col[13] = new SimpleColumn(compressiontype, fSessionID);
	col[14] = new SimpleColumn(autoincrement, fSessionID);
	col[15] = new SimpleColumn(nextVal, fSessionID);
	col[16] = new SimpleColumn(nullable, fSessionID);
	
    SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(columnlength, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(objectid, srcp));
    srcp.reset(col[2]);
    colMap.insert(CMVT_(datatype, srcp));
    srcp.reset(col[3]);
    colMap.insert(CMVT_(dictobjectid, srcp));
    srcp.reset(col[4]);
    colMap.insert(CMVT_(listobjectid, srcp));
    srcp.reset(col[5]);
    colMap.insert(CMVT_(treeobjectid, srcp));
    srcp.reset(col[6]);
    colMap.insert(CMVT_(columnposition, srcp));
    srcp.reset(col[7]);
    colMap.insert(CMVT_(scale, srcp));
    srcp.reset(col[8]);
    colMap.insert(CMVT_(precision, srcp));
    // TODO: NULL value handling
    srcp.reset(col[9]);
    colMap.insert(CMVT_(defaultvalue, srcp));
    srcp.reset(col[10]);
    colMap.insert(CMVT_(schemaname, srcp));
    srcp.reset(col[11]);
    colMap.insert(CMVT_(tablename, srcp));
    srcp.reset(col[12]);
    colMap.insert(CMVT_(columnname, srcp));
	srcp.reset(col[13]);
    colMap.insert(CMVT_(compressiontype, srcp));
	srcp.reset(col[14]);
    colMap.insert(CMVT_(autoincrement, srcp));
	srcp.reset(col[15]);
    colMap.insert(CMVT_(nextVal, srcp));
	srcp.reset(col[16]);
    colMap.insert(CMVT_(nullable, srcp));
    csep.columnMapNonStatic(colMap);

    // ignore returnedcolumn, because it's not read by Joblist for now
    csep.returnedCols(returnedColumnList);
    OID oid[17];
    for (int i = 0; i < 17; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[10]->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));

    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         col[11]->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     

    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         col[12]->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 

    ostringstream oss;
    oss << "select objectid,columnlength,datatype,dictobjectid,listobjectid,treeobjectid,columnposition,scale,"
        "prec,defaultvalue from syscolumn where schema='" << aTableColName.schema << "' and tablename='" <<
        aTableColName.table << "' and columnname='" << aTableColName.column << "' --lookupOID/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;
    TableColName tcn;
    ColType ct;
    OID coloid = -1;
    getSysData (csep, sysDataList, SYSCOLUMN_TABLE);

    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid[1])
        {
            // populate cache
            coloid = (OID)((*it)->GetData(0));
            lk2.lock();
            fOIDmap[aTableColName] = coloid;
            if (fIdentity == EC)
                fColRIDmap[aTableColName] = (*it)->GetRid(0);
            // @bug 1358. do not insert this entry to map
            //else
            //    fColRIDmap[aTableColName] = 0;
            lk2.unlock();
        }
        if ((*it)->ColumnOID() == oid[0])
            ct.colWidth = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[2])
            ct.colDataType = (ColDataType)((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[3])
            ct.ddn.dictOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[4])
            ct.ddn.listOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[5])
            ct.ddn.treeOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[6])
            ct.colPosition = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[7])
            ct.scale = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[8])
            ct.precision = ((*it)->GetData(0));
		else if ((*it)->ColumnOID() == oid[13])
            ct.compressionType = ct.ddn.compressionType = ((*it)->GetData(0));
		else if ((*it)->ColumnOID() == oid[14])
		{
				ostringstream os;
				os << (char) (*it)->GetData(0);
				if (os.str().compare("y") == 0)
					ct.autoincrement = true; 		
				else 
					ct.autoincrement = false;
		}
		else if ((*it)->ColumnOID() == oid[15])
            ct.nextvalue = ((*it)->GetData(0));
		else if ((*it)->ColumnOID() == oid[16])
        {
            if (static_cast<ConstraintType> ((*it)->GetData(0)) == 0)
			{
                ct.constraintType = NOTNULL_CONSTRAINT;
			}
        }
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_DEFAULTVAL)
		{
            ct.defaultValue = ((*it)->GetStringData(0));
			if ((!ct.defaultValue.empty()) || (ct.defaultValue.length() > 0))
			{
				if (ct.constraintType != NOTNULL_CONSTRAINT)
					ct.constraintType = DEFAULT_CONSTRAINT;
			}
		}
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_SCHEMA)
            tcn.schema = ((*it)->GetStringData(0));
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_TABLENAME)
            tcn.table = ((*it)->GetStringData(0));
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_COLNAME)
            tcn.column = ((*it)->GetStringData(0));
    }

    // temporialy memory leak fix until defaultvalue is added.
   // delete col[9];
    ct.columnOID = coloid;
    // populate colinfomap cache and oidbitmap
    boost::mutex::scoped_lock lk3(fColinfomapLock);
    fColinfomap[coloid] = ct;
    return coloid;

}

void CalpontSystemCatalog::getSysData (CalpontSelectExecutionPlan& csep,
                                         NJLSysDataList& sysDataList,
					 const string& sysTableName)
{
    // start up new transaction
    
    BRM::TxnID txnID;
	int oldTxnID;
	txnID = fSessionManager->getTxnID(fSessionID);
	
    if (!txnID.valid)
    {
        txnID.id = 0;
        txnID.valid = true;
    }
	
    CalpontSystemCatalog::SCN verID, oldVerID;
    verID = fSessionManager->verID();
    oldTxnID = csep.txnID();
    csep.txnID(txnID.id);
	oldVerID = csep.verID();
    csep.verID(verID);
    //We need to use a session ID that's separate from the actual query SID, because the dbcon runs queries
    //  in the middle of receiving data bands for the real query.
    //TODO: we really need a flag or something to identify this as a syscat query: there are assumptions made
	// in joblist that a high-bit-set session id is always a syscat query. This will be okay for a long time,
	// but not forever...

    csep.sessionID(fSessionID | 0x80000000);
	int tryCnt = 0;
	
	// add the tableList to csep for tuple joblist to use
	CalpontSelectExecutionPlan::TableList tablelist;
	tablelist.push_back(make_aliastable("calpontsys", sysTableName, ""));
	csep.tableList(tablelist);
	
	// populate the returned column list as column map
	csep.returnedCols().clear();
	CalpontSelectExecutionPlan::ColumnMap::const_iterator it;
	for	 (it = csep.columnMap().begin(); it != csep.columnMap().end(); ++it)
	{
		csep.returnedCols().push_back(it->second);
	}
	
	if (fIdentity == EC)
	{
		try {
			getSysData_EC(csep, sysDataList, sysTableName);		
		}
        catch ( IDBExcept& ){
            throw;
        }
		catch ( runtime_error& e ) {
			throw runtime_error ( e.what() );
		}					
	}
	else
	{
		while (tryCnt < 5)
		{
			tryCnt++;
			try {
				getSysData_FE(csep, sysDataList, sysTableName);
				break;
			}
			catch(IDBExcept&) // error already occured. this is not a broken pipe
			{
				throw;
			}
			catch(...)
			{
				// may be a broken pipe. re-establish exeMgr and send the message
				delete fExeMgr;
				fExeMgr = new ClientRotator(0, "ExeMgr");
				try {
					fExeMgr->connect(5);
				} catch (...) {
					throw IDBExcept(ERR_LOST_CONN_EXEMGR);
				}
			}
		} 

		if (tryCnt >= 5)
			//throw runtime_error("Error occured when calling system catalog. ExeMgr is not functioning.");
			throw IDBExcept(ERR_SYSTEM_CATALOG);
	}
	csep.sessionID(fSessionID);
	csep.txnID(oldTxnID);
	csep.verID(oldVerID);
}
                                       
void CalpontSystemCatalog::getSysData_EC(CalpontSelectExecutionPlan& csep,
                                         NJLSysDataList& sysDataList,
                                         const string& sysTableName)
{
	DEBUG << "Enter getSysData_EC " << fSessionID << endl;

	uint32_t tableOID = IDB_VTABLE_ID;
	ByteStream bs;
	uint status;

	ResourceManager rm(true);
	DistributedEngineComm* fEc = DistributedEngineComm::instance(rm);
	SJLP jl = JobListFactory::makeJobList(&csep, rm, true);
	//@bug 2221. Work around to prevent DMLProc crash.
	int retryNum = 0;
	while ( jl->status() != 0 )
	{
		if ( retryNum >= 6 )
			throw runtime_error("Error occured when calling makeJobList");
#ifdef _MSC_VER
		Sleep(1 * 1000);
#else
		sleep(1);
#endif
		jl = JobListFactory::makeJobList(&csep, rm, true);
		retryNum++;
	}

	if (jl->status() != 0 || jl->putEngineComm(fEc) != 0)
	{
		string emsg = jl->errMsg();
		throw runtime_error("Error occured when calling system catalog (1). " + emsg);
	}
	if ( jl->doQuery() != 0)
	{
		throw runtime_error("Error occured when calling system catalog (2). Make sure all processes are running.");
	}
	
	TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jl.get());
	idbassert(tjlp);
	RowGroup rowGroup = tjlp->getOutputRowGroup();
	while (true)
	{
		bs.restart();
		uint rowCount = jl->projectTable(tableOID, bs);
		rowGroup.setData(const_cast<uint8_t*>(bs.buf())); 
		if ((status = rowGroup.getStatus()) != 0)
		{
			if (status >= 1000) // new error system
				throw IDBExcept(status);
			else
				throw IDBExcept(ERR_SYSTEM_CATALOG);
		}
		
		if (rowCount > 0)
			rowGroup.addToSysDataList(sysDataList);
		else
			break;
		}
}

void CalpontSystemCatalog::getSysData_FE(const CalpontSelectExecutionPlan& csep,
                                                 NJLSysDataList& sysDataList,
						 const string& sysTableName)
{
	DEBUG << "Enter getSysData_FE " << fSessionID << endl;
	
	ByteStream msg;
	
	// send code to indicat tuple
	ByteStream::quadbyte qb = 4;
	msg << qb;
	fExeMgr->write(msg);
	msg.restart();

	// Send the CalpontSelectExecutionPlan to ExeMgr.
	csep.serialize(msg);
	fExeMgr->write(msg); 
	
	// Get the table oid for the system table being queried.
	TableName tableName;
	tableName.schema = CALPONT_SCHEMA;
	tableName.table = sysTableName;
	uint32_t tableOID = IDB_VTABLE_ID;
	uint16_t status = 0;
	
	// Send the request for the table.
	qb = static_cast<ByteStream::quadbyte>(tableOID);
	ByteStream bs;
	bs << qb;
	fExeMgr->write(bs);
	boost::scoped_ptr<rowgroup::RowGroup> rowGroup;

	msg.restart();
	bs.restart();
	msg = fExeMgr->read();			
	bs = fExeMgr->read();	
							
	if (bs.length() == 0)
	{
		throw IDBExcept(ERR_LOST_CONN_EXEMGR);
	}
	string emsgStr;
	bs >> emsgStr;
	bool err = false;
				
	if (msg.length() == 4)
	{
		msg >> qb;
		if (qb != 0) 
			err = true;
	}
	else
	{
		err = true;
	}
	if (err)
	{
		throw runtime_error(emsgStr);
	}
	
	while(true) 
	{
		bs.restart();
		bs = fExeMgr->read();
		// @bug 1782. check ExeMgr connection lost
		if (bs.length() == 0)
			throw IDBExcept(ERR_LOST_CONN_EXEMGR);

		if (!rowGroup)
		{
			rowGroup.reset(new RowGroup());
			rowGroup->deserialize(bs);
			continue;
		}
		else
		{
			rowGroup->setData(const_cast<uint8_t*>(bs.buf())); 
		}
		if ((status = rowGroup->getStatus()) != 0)
		{
			if (status >= 1000) // new error system
			{
				bs.advance(rowGroup->getDataSize());
				bs >> emsgStr;
				throw IDBExcept(emsgStr, rowGroup->getStatus());			}
			else
			{
				throw IDBExcept(ERR_SYSTEM_CATALOG);
			}
		}
		
		if (rowGroup->getRowCount() > 0)
			rowGroup->addToSysDataList(sysDataList);
		else
			break;
	}
	bs.reset();
	qb = 0;
	bs << qb;
	fExeMgr->write(bs);
}

const CalpontSystemCatalog::ColType CalpontSystemCatalog::colType(const OID& Oid)
{
    if ( Oid >= 3000)
        DEBUG << "Enter colType: " << Oid << endl;
    ColType ct;
    // invalid oid
    if (Oid < 1000)
        return ct;
    
    //Check whether cache needs to be flushed
    if ( Oid >= 3000) {
    	checkSysCatVer();
	}
    // check colinfomap first for system table column or cached column type
    boost::mutex::scoped_lock lk3(fColinfomapLock);
    if ( fColinfomap.size() > 0 )
    {
    	Colinfomap::const_iterator iter = fColinfomap.find(Oid);
    	if (iter != fColinfomap.end())
        	return iter->second;
    }
    lk3.unlock();
    
    /* SQL statement: select columnlength, datatype, dictobjectid,listobjectid,treeobjectid, 
     * columnposition, scale, prec, defaultvalue, schema, tablename, columnname
     * from syscolumn where objectid = Oid;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    string columnlength = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNLEN_COL;
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
    string datatype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DATATYPE_COL;
    string dictobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL;
    string listobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL;
    string treeobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL;
    string columnposition = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNPOS_COL;
    string scale = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCALE_COL;
    string precision = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+PRECISION_COL;
    string defaultvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DEFAULTVAL_COL;
    // the following columns will be save in cache although it's not needed for now
    string columnname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL;
    string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
    string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
    string nullable = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NULLABLE_COL;
	string compressionType = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COMPRESSIONTYPE_COL;
	string autoincrement = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
    string nextvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;
	
    SimpleColumn* col[17];
    col[0] = new SimpleColumn(columnlength, fSessionID);    
    col[1] = new SimpleColumn(objectid, fSessionID); 
    col[2] = new SimpleColumn(datatype, fSessionID);
    col[3] = new SimpleColumn(dictobjectid, fSessionID);
    col[4] = new SimpleColumn(listobjectid, fSessionID);
    col[5] = new SimpleColumn(treeobjectid, fSessionID);
    col[6] = new SimpleColumn(columnposition, fSessionID);
    col[7] = new SimpleColumn(scale, fSessionID);
    col[8] = new SimpleColumn(precision, fSessionID);
    col[9] = new SimpleColumn(defaultvalue, fSessionID);
    col[10] = new SimpleColumn(schemaname, fSessionID);
    col[11] = new SimpleColumn(tablename, fSessionID);
    col[12] = new SimpleColumn(columnname, fSessionID);
    col[13] = new SimpleColumn(nullable, fSessionID);
    col[14] = new SimpleColumn(compressionType, fSessionID);  
	col[15] = new SimpleColumn(autoincrement, fSessionID);
	col[16] = new SimpleColumn(nextvalue, fSessionID);
	
    SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(columnlength, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(objectid, srcp));
    srcp.reset(col[2]);
    colMap.insert(CMVT_(datatype, srcp));    
    srcp.reset(col[3]);
    colMap.insert(CMVT_(dictobjectid, srcp));
    srcp.reset(col[4]);
    colMap.insert(CMVT_(listobjectid, srcp));
    srcp.reset(col[5]);
    colMap.insert(CMVT_(treeobjectid, srcp));
    srcp.reset(col[6]);
    colMap.insert(CMVT_(columnposition, srcp));    
    srcp.reset(col[7]);
    colMap.insert(CMVT_(scale, srcp));
    srcp.reset(col[8]);
    colMap.insert(CMVT_(precision, srcp));
    // TODO: NULL value handling & convert to boost::any
    // delete this manually at fcn exit
    srcp.reset(col[9]);
    colMap.insert(CMVT_(defaultvalue, srcp));
    srcp.reset(col[10]);
    colMap.insert(CMVT_(schemaname, srcp));
    srcp.reset(col[11]);
    colMap.insert(CMVT_(tablename, srcp));
    srcp.reset(col[12]);
    colMap.insert(CMVT_(columnname, srcp));
    srcp.reset(col[13]);
    colMap.insert(CMVT_(nullable, srcp));
	srcp.reset(col[14]);
    colMap.insert(CMVT_(compressionType, srcp));
	srcp.reset(col[15]);
    colMap.insert(CMVT_(autoincrement, srcp));
	srcp.reset(col[16]);
    colMap.insert(CMVT_(nextvalue, srcp));
    
    csep.columnMapNonStatic(colMap);
        
    // ignore returnedcolumn, because it's not read by Joblist for now
    csep.returnedCols(returnedColumnList);
    OID oid[17];
    for (int i = 0; i < 17; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[1]->clone(),
                                         new ConstantColumn(Oid, ConstantColumn::NUM));
    filterTokenList.push_back(f1);
    
	csep.filterTokenList(filterTokenList);  
    ostringstream oss;
    oss << "select columnlength,datatype,dictobjectid,listobjectid,treeobjectid,columnposition,scale,compressiontype"
        "prec,defaultvalue,schema,tablename,columnname from syscolumn where objectid=" << Oid <<
        " --colType/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;       
    getSysData(csep, sysDataList, SYSCOLUMN_TABLE);
    
    TableColName tcn;
    vector<ColumnResult*>::const_iterator it;
    RID rid = std::numeric_limits<RID>::max();
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        
        if ((*it)->ColumnOID() == oid[0]) 
        {       
            ct.colWidth = ((*it)->GetData(0));   
            if (fIdentity == EC)                 
                rid = (*it)->GetRid(0);
        }
        else if ((*it)->ColumnOID() == oid[2])
            ct.colDataType = (ColDataType)((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[3])
            ct.ddn.dictOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[4])
            ct.ddn.listOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[5])
            ct.ddn.treeOID = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[6])
            ct.colPosition = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[7])
            ct.scale = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == oid[8])
            ct.precision = ((*it)->GetData(0));
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_DEFAULTVAL)
		{
            ct.defaultValue = ((*it)->GetStringData(0));
			if ((!ct.defaultValue.empty()) || (ct.defaultValue.length() > 0))
			{
				if (ct.constraintType != NOTNULL_CONSTRAINT)
					ct.constraintType = DEFAULT_CONSTRAINT;
			}
		}
	// NJL fix.  The schema, table, and column now return the oids for the dictionary columns
	// on schema, table, and column.
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_SCHEMA)
            tcn.schema = ((*it)->GetStringData(0));
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_TABLENAME)
            tcn.table = ((*it)->GetStringData(0));
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_COLNAME)
            tcn.column = ((*it)->GetStringData(0));
        else if ((*it)->ColumnOID() == oid[13])
        {
            if (static_cast<ConstraintType> ((*it)->GetData(0)) == 0)
			{
                ct.constraintType = NOTNULL_CONSTRAINT;
			}
        }
		else if ((*it)->ColumnOID() == oid[14])
            ct.compressionType = ct.ddn.compressionType = ((*it)->GetData(0));
		else if ((*it)->ColumnOID() == oid[15])
		{
			ostringstream os;
			os << (char) (*it)->GetData(0);
			if (os.str().compare("y") == 0)
				ct.autoincrement = true; 		
			else 
				ct.autoincrement = false;
		}
		else if ((*it)->ColumnOID() == oid[16])
            ct.nextvalue = ((*it)->GetData(0));
			
		ct.columnOID = Oid;
    }

    // populate colinfomap cache and oidbitmap    
    lk3.lock();
    boost::mutex::scoped_lock lk2(fOIDmapLock);
    fColinfomap[Oid] = ct;
    fOIDmap[tcn] = Oid;
    if (fIdentity == EC)
    	fColRIDmap[tcn] = rid;

    //Prevent mem leak
    //delete col[9];

    return ct;     
}

const CalpontSystemCatalog::ColType CalpontSystemCatalog::colTypeDct(const OID& dictOid)
{
    if ( dictOid >= 3000)
        DEBUG << "Enter colType: " << dictOid << endl;
    ColType ct;
    // invalid oid
    if (dictOid < 1000)
        return ct;
    
    //Check whether cache needs to be flushed
    if ( dictOid >= 3000) {
    	checkSysCatVer();
	}

    // check map first cached column type
    boost::mutex::scoped_lock lk3(fDctTokenMapLock);
	DctTokenMap::const_iterator iter = fDctTokenMap.find(dictOid);
	if (iter != fDctTokenMap.end())
		return colType(iter->second);
    lk3.unlock();

    /* SQL statement: select objectid from syscolumn where dictobjectid = dictOid;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
    string dictobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL;

    SimpleColumn* col[2];
    col[0] = new SimpleColumn(objectid, fSessionID); 
    col[1] = new SimpleColumn(dictobjectid, fSessionID);
	
    SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(objectid, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(dictobjectid, srcp));

    csep.columnMapNonStatic(colMap);
        
    // ignore returnedcolumn, because it's not read by Joblist for now
    csep.returnedCols(returnedColumnList);
    OID oid[2];
    for (int i = 0; i < 2; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[1]->clone(),
                                         new ConstantColumn(dictOid, ConstantColumn::NUM));
    filterTokenList.push_back(f1);
    
	csep.filterTokenList(filterTokenList);  
    ostringstream oss;
    oss << "select objectid from syscolumn where dictobjectid=" << dictOid << " --colTypeDct/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;       
    getSysData(csep, sysDataList, SYSCOLUMN_TABLE);

    vector<ColumnResult*>::const_iterator it;

	OID tokenOID = 0;
  
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid[0]) 
            tokenOID = ((*it)->GetData(0));   
    }

    // populate cache
    lk3.lock();
    fDctTokenMap[dictOid] = tokenOID;

    return colType(tokenOID);     
}

const CalpontSystemCatalog::TableColName CalpontSystemCatalog::colName(const OID& oid)
{
    if (oid >= 3000)
        DEBUG << "Enter colName: " << oid;
    
    TableColName tableColName;
    
    // invalid oid
    if (oid < 1000)
        return tableColName;
    
    //Check whether cache needs to be flushed
    if ( oid >= 3000) {
    	checkSysCatVer();
	}
    // check oidmap for system table columns and cached columns
    boost::mutex::scoped_lock lk2(fOIDmapLock);
    OIDmap::const_iterator iter = fOIDmap.begin();
    while ( iter != fOIDmap.end() )
    {
        if (oid == (*iter).second )
        {
            tableColName = (*iter).first;
            DEBUG << "|in cache|" << tableColName.schema << "|" << tableColName.table << "|" << tableColName.column << endl;
            return tableColName;
        }
        ++iter;
    }
    lk2.unlock();
    
    // SQL statement: select schema, tablename, columnname from syscolumn where objectid = oid;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL, fSessionID);       
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c3->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c4->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);

    // NJL fix.  The dictionary column OID is now returned from getSysData.
    // OID oid2 = c2->oid();
    // OID oid3 = c3->oid();
    // OID oid4 = c4->oid();
    OID oid2 = DICTOID_SYSCOLUMN_SCHEMA;
    OID oid3 = DICTOID_SYSCOLUMN_TABLENAME;
    OID oid4 = DICTOID_SYSCOLUMN_COLNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(oid, ConstantColumn::NUM));
    filterTokenList.push_back(f1);
    csep.filterTokenList(filterTokenList); 

    ostringstream oss;
    oss << "select schema,tablename,columnname from syscolumn where objectid=" << oid <<
        " --colName/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCOLUMN_TABLE);  
  
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid2)
            tableColName.schema = (*it)->GetStringData(0);
        else if ((*it)->ColumnOID() == oid3)
            tableColName.table = (*it)->GetStringData(0);
        else if ((*it)->ColumnOID() == oid4)
            tableColName.column = (*it)->GetStringData(0);        
    }   

    if (oid > 3000)
        DEBUG << "|" << tableColName.schema << "|" << tableColName.table << "|" << tableColName.column << endl;    
#if BOOST_VERSION < 103800
    if (!lk2.locked()) lk2.lock();
#else
    if (!lk2.owns_lock()) lk2.lock();
#endif
    fOIDmap[tableColName] = oid;
    
    return tableColName;
}

const int64_t CalpontSystemCatalog::nextAutoIncrValue ( TableName aTableName)
{
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
	
	TableInfo tbInfo;
	try {
		tbInfo = tableInfo (aTableName);
	}
	catch (runtime_error& /*ex*/)
	{
		return -2;
	}
	
	if (tbInfo.tablewithautoincr == NO_AUTOINCRCOL)
		return 0;

	//Build a plan to get current nextvalue:  select nextvalue from syscolumn where schema = tableName.schema and tablename = tableName.table and autoincrement='y';
	CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
    string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
	string autoincrement = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
    string nextvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;
	
    SimpleColumn* col[5];
    col[0] = new SimpleColumn(tablename, fSessionID);    
    col[1] = new SimpleColumn(schemaname, fSessionID); 
    col[2] = new SimpleColumn(autoincrement, fSessionID);
    col[3] = new SimpleColumn(nextvalue, fSessionID);
    
	SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(tablename, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(schemaname, srcp));
	srcp.reset(col[2]);
    colMap.insert(CMVT_(autoincrement, srcp));
	srcp.reset(col[3]);
    colMap.insert(CMVT_(nextvalue, srcp));
	
    csep.columnMapNonStatic(colMap);
	
	csep.returnedCols(returnedColumnList);
	
    OID oid[4];
    for (int i = 0; i < 4; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[1]->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));

    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         col[0]->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     

    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         col[2]->clone(),
                                         new ConstantColumn("y", ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    ostringstream oss;
    oss << "select nextvalue from syscolumn where schema = aTableName.schema and tablename = aTableName.table and autoincrement='y'";       
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;    
	try {	
		getSysData(csep, sysDataList, SYSCOLUMN_TABLE);
	}
	catch  (runtime_error& e)
	{
		throw runtime_error ( e.what() );
	}
	int64_t nextVal = 0;
	vector<ColumnResult*>::const_iterator it; 
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        
        if ((*it)->ColumnOID() == oid[3]) 
        {       
            nextVal = ((*it)->GetData(0));   
        }
	}
	
	return (nextVal);
	
}

int32_t CalpontSystemCatalog::autoColumOid ( TableName aTableName)
{
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
	
	TableInfo tbInfo;
	try {
		tbInfo = tableInfo (aTableName);
	}
	catch (runtime_error& /*ex*/)
	{
		return -2;
	}
	
	if (tbInfo.tablewithautoincr == NO_AUTOINCRCOL)
		return 0;

	//Build a plan to get column oid:  select objectid from syscolumn where schema = tableName.schema and tablename = tableName.table and autoincrement='y';
	CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
    string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
	string autoincrement = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
	
    SimpleColumn* col[5];
    col[0] = new SimpleColumn(tablename, fSessionID);    
    col[1] = new SimpleColumn(schemaname, fSessionID); 
    col[2] = new SimpleColumn(autoincrement, fSessionID);
    col[3] = new SimpleColumn(objectid, fSessionID);
    
	SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(tablename, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(schemaname, srcp));
	srcp.reset(col[2]);
    colMap.insert(CMVT_(autoincrement, srcp));
	srcp.reset(col[3]);
    colMap.insert(CMVT_(objectid, srcp));
	
    csep.columnMapNonStatic(colMap);
	
	csep.returnedCols(returnedColumnList);
	
    OID oid[4];
    for (int i = 0; i < 4; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[1]->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));

    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         col[0]->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     

    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         col[2]->clone(),
                                         new ConstantColumn("y", ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    ostringstream oss;
    oss << "select nextvalue from syscolumn where schema = aTableName.schema and tablename = aTableName.table and autoincrement='y'";       
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;    
	try {	
		getSysData(csep, sysDataList, SYSCOLUMN_TABLE);
	}
	catch  (runtime_error& e)
	{
		throw runtime_error ( e.what() );
	}
	int32_t columnOid = 0;
	vector<ColumnResult*>::const_iterator it;  
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        
        if ((*it)->ColumnOID() == oid[3]) 
        {       
            columnOid = ((*it)->GetData(0));   
        }
	}
	
	return (columnOid);
	
}

const CalpontSystemCatalog::ROPair CalpontSystemCatalog::nextAutoIncrRid ( const OID& columnoid)
{
	
	//Build a plan to get rid of nextvalue:  select nextvalue from syscolumn where objectid = columnoid;
	CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
    string nextvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;
	
    SimpleColumn* col[2];
    col[0] = new SimpleColumn(objectid, fSessionID);    
    col[1] = new SimpleColumn(nextvalue, fSessionID);
    
	SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(objectid, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(nextvalue, srcp));
	
    csep.columnMapNonStatic(colMap);
	
	csep.returnedCols(returnedColumnList);
	
    OID oid[2];
    for (int i = 0; i < 2; i++)
        oid[i] = col[i]->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[0]->clone(),
                                         new ConstantColumn(columnoid, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
   
    csep.filterTokenList(filterTokenList); 
    ostringstream oss;
    oss << "select nextvalue from syscolumn objectid = columnoid";       
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;  
	try {
		getSysData(csep, sysDataList, SYSCOLUMN_TABLE);
	}
	catch  (runtime_error& e)
	{
		throw runtime_error ( e.what() );
	}
	
	vector<ColumnResult*>::const_iterator it;  
	ROPair roPair;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        
        if ((*it)->ColumnOID() == oid[1]) 
        {       
            roPair.rid = ((*it)->GetRid(0));  
			roPair.objnum = oid[1];
			return roPair;
        }
	}
	return roPair;
}

#if 0 //Not implemented
const CalpontSystemCatalog::OID CalpontSystemCatalog::colBitmap(const OID& oid) const
{
    return oid;
    DEBUG << "Enter colBitmap: Not implemented" << endl;  
}

const CalpontSystemCatalog::SCN CalpontSystemCatalog::scn(void) const
{
    DEBUG << "Enter scn: Not implemented" << endl;
    SCN scn;
    scn = 1;
    return scn;
}
#endif

/* static */
boost::shared_ptr<CalpontSystemCatalog> CalpontSystemCatalog::makeCalpontSystemCatalog(u_int32_t sessionID) 
{
    boost::mutex::scoped_lock lock(map_mutex);
    boost::shared_ptr<CalpontSystemCatalog> instance;
    CatalogMap::const_iterator it = fCatalogMap.find(sessionID);
    if (sessionID == 0)
    {
        if (it == fCatalogMap.end())
        {
            instance.reset(new CalpontSystemCatalog());
            fCatalogMap[0] = instance;
            return instance;
        }

#if 0
		//Is it really an error for a non-sessionid-0 catalog to be present at this point?
        if (fCatalogMap.size() != 1)
    	{
            //throw runtime_error ("No calpont system catalog instance found.");
			ostringstream oss;
			oss << "Preposterous number of system catalog instances found when looking for "
				"session 0: " << fCatalogMap.size();
			throw runtime_error(oss.str());
    	}
#endif

        return it->second;
    }

    if (it == fCatalogMap.end())
    {
        instance.reset(new CalpontSystemCatalog());
        instance->sessionID(sessionID);
	    instance->fExeMgr->setSessionId(sessionID);
        fCatalogMap[sessionID] = instance;
        return instance;
    }

    return it->second;    
}

/* static */
void CalpontSystemCatalog::removeCalpontSystemCatalog(u_int32_t sessionID)
{
    boost::mutex::scoped_lock lock(map_mutex);
    DEBUG << "remove calpont system catalog for session " << sessionID << endl;
	fCatalogMap.erase(sessionID);
/*
    CatalogMap::iterator it = fCatalogMap.find(sessionID);
    if (it != fCatalogMap.end())
    {
        delete (*it).second;
        fCatalogMap.erase(it);
    }    
*/
}

CalpontSystemCatalog::CalpontSystemCatalog():
    fExeMgr (new ClientRotator(0, "ExeMgr")),
    fSessionID (0)
{
    // Set fIdentity based on the module on which we are running.
    fIdentity = EC;
	if ( fSessionManager.get() == 0 )
		fSessionManager.reset(new SessionManager());
    try
    {
        string localModuleType;
        const char* p = 0;
        //see if env is set to override identity lookup
#ifdef _MSC_VER
		p = "EC";
		string cfStr = IDBreadRegistry("SyscatIdent");
		if (!cfStr.empty())
			p = cfStr.c_str();
#else        
		p = getenv("CALPONT_CSC_IDENT");
#endif
        if (p && *p)
        {
            localModuleType = p;
        }
        else
        {
            oam::Oam oam;
            oam::oamModuleInfo_t t = oam.getModuleInfo();
            localModuleType = boost::get<1>(t);
        }

	    // If dm (director module), set the identity to FE (Front End).
	    // @bug 1029. set "FE" for beetlejuice (xm)
        if (localModuleType == "dm" || localModuleType == "xm")
        {
            fIdentity = FE;
        }
    }
    catch(exception&)
    {
		// If not in an environment with OAM set up, default to Front End.
		fIdentity = FE;
    }

	buildSysColinfomap();
    buildSysOIDmap();
    buildSysTablemap();
    buildSysDctmap();
	fSyscatSCN = fSessionManager->sysCatVerID();
}

CalpontSystemCatalog::~CalpontSystemCatalog()
{
    delete fExeMgr;
}

#if 0
const CalpontSystemCatalog::RIDList CalpontSystemCatalog::indexRIDs(const TableName& tableName)
{
    /* SQL statement: select indexname from sysindex where schema=tableName.schema and tablename=tableName.table;*/
    ROPair rid;
    TableColName aTableName;
	aTableName.schema = tableName.schema;
	aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );

    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter constraintRIDs: " << tableName.schema << "|" << tableName.table << endl;
    
    RIDList rl;    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEX_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            // TODO: get rowid from columnresult. new feather of columnresult
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
              if (fIdentity == EC)
                rid.rid = (*it)->GetRid(i);   
              else
                rid.rid = 0;
              rl.push_back(rid);
            }
            return rl;
        }
    }
    return rl;
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::indexColRIDs(const TableName& tableName)
{
    /* SQL statement: select indexname from sysindexcol where schema=tableColName.schema and 
     * tablename=tableColName.table;
     */
    RIDList ridlist;
    TableName aTableColName;
	aTableColName.schema = tableName.schema;
	aTableColName.table = tableName.table;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter indexColRIDs: " << tableName.schema << "|"
              << tableName.table << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    ROPair rid;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                if (fIdentity == EC)
                    rid.rid = (*it)->GetRid(i);
                ridlist.push_back(rid);
            }
            return ridlist;
        }
    }
    
    return ridlist;
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::indexColRIDs(const IndexName& indexName)
{
   /* SQL statement: select indexname from sysindexcol where schema=indexName.schema and 
     * tablename=indexName.table and indexname=indexName.index;
     */
    RIDList ridlist;
    IndexName aIndexName;
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );	

    if (aIndexName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter indexColRIDs: " << aIndexName.schema << "|"
              << aIndexName.table << aIndexName.index << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
      
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);

    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    ROPair rid;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                if (fIdentity == EC)
                    rid.rid = (*it)->GetRid(i);
                ridlist.push_back(rid);
            }
            return ridlist;
        }
    }
    
    return ridlist;
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::constraintRIDs(const TableName& tableName)
{   
    /* SQL statement: select constraintname from sysconstraint where schema=tableName.schema and tablename=tableName.table;*/
    ROPair rid;
    TableColName aTableName;
	aTableName.schema = tableName.schema;
	aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );

    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter constraintRIDs: " << tableName.schema << "|" << tableName.table << endl;
    
    RIDList rl;    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINT_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINT_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            // TODO: get rowid from columnresult. new feather of columnresult
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
              if (fIdentity == EC)
                rid.rid = (*it)->GetRid(i);   
              rl.push_back(rid);
            }
            return rl;
        }
    }      
    return rl;
}

const CalpontSystemCatalog::IndexNameList CalpontSystemCatalog::colValueSysconstraint (const TableColName& tableColName, bool useCache)
{
	/* SQL statement: select constraintname from sysconstraint where schema = schema and table=table and column=column;*/
	IndexNameList indexNameList;
	TableColName aTableColName;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );    

#if BOOST_VERSION < 104000
	boost::mutex::scoped_lock lk1(fColIndexListmapLock, false);
#else
	boost::mutex::scoped_lock lk1(fColIndexListmapLock, boost::defer_lock);
#endif
	if(useCache)
	{
		lk1.lock();
		ColIndexListmap::const_iterator iter = fColIndexListmap.find(aTableColName);
    	if (iter != fColIndexListmap.end())
    	{
        	indexNameList = iter->second;
			return indexNameList;
    	}
		lk1.unlock();
	}

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter colValueSysconstraint: " << tableColName.schema << "|"
              << tableColName.table << "|"
              << tableColName.column << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, fSessionID);        
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINTCOL_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {     
        if ((*it)->ColumnOID() == oid)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                IndexName indexName;
                indexName.schema = aTableColName.schema;
                indexName.table = aTableColName.table;
                indexName.index = ((*it)->GetStringData(0));
                indexNameList.push_back(indexName);
            }
        }
    }

	lk1.lock();
	fColIndexListmap[aTableColName] = indexNameList;	
	lk1.unlock();           
    
    return indexNameList;  
}

// TODO: should take index name as parameter and filter on schema name and table name also
const CalpontSystemCatalog::RID CalpontSystemCatalog::constraintRID(const std::string constraintName)
{
    DEBUG << "Enter constraintRID: " << constraintName << endl;
    
	/* SQL statement: select constraintname from sysconstraint where constraintname=constraintName;
     */
    RID rid = std::numeric_limits<RID>::max();
    string aConstraintName = constraintName;
	transform( aConstraintName.begin(), aConstraintName.end(), aConstraintName.begin(), to_lower() );
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINT_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aConstraintName, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    csep.filterTokenList(filterTokenList);
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINT_TABLE);  
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
      
        if ((*it)->ColumnOID() == oid)
        {
        	if (fIdentity == EC)
        	    rid = (*it)->GetRid(0);
        	
            return rid;
        }
    }  
    string msg("CalpontSystemCatalog::constraintRID: no RID found for ");
    msg += constraintName;
    throw runtime_error(msg);      
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::constraintColRID(const std::string constraintName)
{
    DEBUG << "Enter constraintColRID: " << constraintName << endl;
    /* SQL statement: select constraintname from sysconstraintcol where constraintname=constraintName;
     */
    RIDList ridlist;
    string aConstraintName = constraintName;
	transform( aConstraintName.begin(), aConstraintName.end(), aConstraintName.begin(), to_lower() );
  
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINTCOL_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aConstraintName, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    csep.filterTokenList(filterTokenList);
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    CalpontSystemCatalog::ROPair ropair;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                if (fIdentity == EC)
                {
                    ropair.rid = (*it)->GetRid(i);
                    ropair.objnum = 0;
                }
                ridlist.push_back(ropair);
            }
            return ridlist;
        }
    }        
    return ridlist;
}

const std::string CalpontSystemCatalog::colValueSysconstraintCol (const TableColName& tableColName)
{
    /* SQL statement: select constraintname from sysconstraintcol where schema = schema and table=table and column=column;*/
	TableColName aTableColName;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );    

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter colValueSysconstraintCol: " << tableColName.schema << "|"
              << tableColName.table << "|"
              << tableColName.column << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, fSessionID);        
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINTCOL_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    string constraintname = "";
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return constraintname;
        if ((*it)->ColumnOID() == oid)
            return (*it)->GetStringData(0);
    }
    return constraintname;
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::constraintColRIDs(const TableName& tableName)
{
    /* SQL statement: select constraintname from sysconstraintcol where schema=tableColName.schema and 
     * tablename=tableColName.table;
     */
    RIDList ridlist;
    TableColName aTableColName;
	aTableColName.schema = tableName.schema;
	aTableColName.table = tableName.table;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter constraintColRIDs: " << tableName.schema << "|"
              << tableName.table << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINTCOL_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    ROPair rid;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                if (fIdentity == EC)
                    rid.rid = (*it)->GetRid(i);
                ridlist.push_back(rid);
            }
            return ridlist;
        }
    }    
    return ridlist;
}

const CalpontSystemCatalog::RID CalpontSystemCatalog::constraintColRID(const TableColName& tableColName)
{
    /* SQL statement: select constraintname from sysconstraintcol where schema=tableColName.schema and 
     * tablename=tableColName.table and columnname=tableColName.column;
     */
    RID rid = 0;
    TableColName aTableColName;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );    

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter constraintColRID: " << tableColName.schema << "|"
              << tableColName.table << "|"
              << tableColName.column << "(note: rowid not fully implemented)" << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, fSessionID);        
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSCONSTRAINTCOL_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            if (fIdentity == EC)
                rid = (*it)->GetRid(0);
            return rid;
        }
    }    
    return std::numeric_limits<RID>::max();
}
#endif

const vector< pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> > CalpontSystemCatalog::getTables (const std::string schema)
{
    string schemaname = schema;
    vector < pair<OID, TableName> > tables;
    transform( schemaname.begin(), schemaname.end(), schemaname.begin(), to_lower() );
    if (schemaname == CALPONT_SCHEMA)
    {
        // systables
        tables.push_back( make_pair(SYSTABLE_BASE, make_table(CALPONT_SCHEMA, SYSTABLE_TABLE)));
        tables.push_back( make_pair(SYSCOLUMN_BASE, make_table(CALPONT_SCHEMA, SYSCOLUMN_TABLE)));
        return tables;
    }
      
    DEBUG << "Enter getTables" << endl;
    // SQL statement: select tablename from systable where schemaname = schema;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = DICTOID_SYSTABLE_TABLENAME;
    OID oid2 = DICTOID_SYSTABLE_SCHEMA;
    
    if (!schema.empty())
    {
      // Filters
      SimpleFilter *f1 = new SimpleFilter (opeq,
                                           c2->clone(),
                                           new ConstantColumn(schemaname, ConstantColumn::LITERAL));
      filterTokenList.push_back(f1);
      csep.filterTokenList(filterTokenList); 
    }
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSTABLE_TABLE);  
 
    vector<ColumnResult*>::const_iterator it;
    vector<string> tnl;
    ROPair rp;
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1)
        {
          for (int i = 0; i < (*it)->dataCount(); i++)
          {
            tables.push_back( make_pair(0, make_table("", (*it)->GetStringData(i))));
            tnl.push_back((*it)->GetStringData(i));
          }
        }
    }
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {    
      if ((*it)->ColumnOID() == oid2)
      {
        for (int i = 0; i < (*it)->dataCount(); i++)
          tables[i].second.schema = (*it)->GetStringData(i);
      }
    }
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
      if ((*it)->ColumnOID() == c3->oid())
      {
        for (int i = 0; i < (*it)->dataCount(); i++)
        {
          rp.objnum = (OID)((*it)->GetData(i));
          if (fIdentity == EC)
            rp.rid = (*it)->GetRid(i);
          fTablemap[tables[i].second] = rp.objnum;
          fTableRIDmap[tables[i].second] = rp.rid;
          tables[i].first = rp.objnum;
        }
      }
    }   
    
    return tables;
}

/* SQL statement: select objectid from systable */
const int CalpontSystemCatalog::getTableCount ()
{
	int tableCnt = 0;
	
    DEBUG << "Enter getTableCount" << endl;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = OID_SYSTABLE_OBJECTID;
    
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSTABLE_TABLE);  
 
    vector<ColumnResult*>::const_iterator it;
        
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1)
        {
        	tableCnt = (*it)->dataCount(); 
        }
    }
    
    return tableCnt;
}
/* SQL statement: select objectid from syscolumn where schema=tableColName.schema and 
 * tablename=tableColName.table and columnname=tableColName.column;*/
const CalpontSystemCatalog::ROPair CalpontSystemCatalog::columnRID(const TableColName& tableColName)
{
    ROPair rp;    
    TableColName aTableColName;
    aTableColName.schema = tableColName.schema;
    aTableColName.table = tableColName.table;
    aTableColName.column = tableColName.column;
    transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
    transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
    transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)	
        DEBUG << "Enter columnRID: " << tableColName.schema << "|" << tableColName.table 
              << "|" << tableColName.column << endl;
	//Check whether cache needs to be flushed
	if ( aTableColName.schema.compare(CALPONT_SCHEMA) ) {
    	checkSysCatVer();	
	}
    /* SQL statement: select objectid from syscolumn where schema=tableColName.schema and
       tablename=tableColName.table and columnname=tableColName.column;*/
    // this function is duplicate to lookupOID() and will be deprecated soon
    rp.objnum = lookupOID(tableColName);
    boost::mutex::scoped_lock lk2(fOIDmapLock);

    ColRIDmap::const_iterator iter = fColRIDmap.find(aTableColName);
    if (iter != fColRIDmap.end())
        rp.rid = (*iter).second;
    return rp;

}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::columnRIDs(const TableName& tableName, bool useCache)
{
    TableName aTableName(tableName);
    transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
    transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
		
		if (aTableName.schema.empty() || aTableName.table.empty())
			throw runtime_error("ColumnRIDs: Invalid table name");
    if (aTableName.schema != CALPONT_SCHEMA)
        DEBUG << "Enter columnRIDs: " << tableName.schema << "|" << tableName.table << endl;

    RIDList rl;
    //Check whether cache needs to be flushed
    if ( aTableName.schema != CALPONT_SCHEMA) {
    	checkSysCatVer();
	}
	boost::mutex::scoped_lock lk1(fTableInfoMapLock);
    TableInfoMap::const_iterator ti_iter = fTableInfoMap.find(aTableName);

    // search fOIDmap for system catalog tables
    // or if fTableInfoMap has entry for this table, column oids are cached. 
    // because columnRIDs(), colType() and tableInfo() are actually binded.
#if BOOST_VERSION < 103800
    boost::mutex::scoped_lock lk2(fOIDmapLock, false);
#else
    boost::mutex::scoped_lock lk2(fOIDmapLock, boost::defer_lock);
#endif
    boost::mutex::scoped_lock lk3(fColinfomapLock);
   if (aTableName.schema == CALPONT_SCHEMA || (useCache && ti_iter != fTableInfoMap.end()))
   {
        if (aTableName.schema == CALPONT_SCHEMA)
            lk3.unlock();            
        else
            rl.resize(ti_iter->second.numOfCols);
        lk2.lock();
        if (aTableName.schema != CALPONT_SCHEMA)
            DEBUG << "for " << aTableName << ", searching " << fOIDmap.size() << " oids" << endl;
        OIDmap::const_iterator iter = fOIDmap.begin();
        while ( iter != fOIDmap.end() )
        {
            TableColName tableColName = (*iter).first;
            if ( tableColName.schema == aTableName.schema
                    && tableColName.table == aTableName.table )
            {
                ROPair rp;
                rp.objnum = (*iter).second;
                ColRIDmap::const_iterator rid_iter = fColRIDmap.find(tableColName);
				if (rid_iter != fColRIDmap.end())
					rp.rid = (*rid_iter).second;
                // @bug 1584. make sure the columns are in position order
                if (aTableName.schema == CALPONT_SCHEMA)
                {
                    rl.push_back(rp);
                }
                else
                {
                    Colinfomap::const_iterator ct_iter = fColinfomap.find(rp.objnum);
                    rl[ct_iter->second.colPosition] = rp;
                }
            }
            ++iter;
        }
        if (aTableName.schema != CALPONT_SCHEMA)
            DEBUG << aTableName << " was cached: " << rl.size() << " rows" << endl;
        return rl;
    }
    lk1.unlock();
    lk3.unlock();
    
    if (aTableName.schema != CALPONT_SCHEMA)
        DEBUG << aTableName << " was not cached, fetching..." << endl;

    // get real data from system catalog for all user tables. don't check cache
    // because cache may not have complete columns for this table
    // SQL statement: select objectid,columnname from syscolumn where schema=tableName.schema and 
    // tablename=tableName.table;
    CalpontSelectExecutionPlan csep;
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;

    string columnlength = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNLEN_COL;
    string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
    string datatype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DATATYPE_COL;
    string dictobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL;
    string listobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL;
    string treeobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL;
    string columnposition = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNPOS_COL;
    string scale = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCALE_COL;
    string precision = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+PRECISION_COL;
    string defaultvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DEFAULTVAL_COL;
    // the following columns will be save in cache although it's not needed for now
    string columnname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL;
    string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
    string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
    string nullable = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NULLABLE_COL;
	string compressiontype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COMPRESSIONTYPE_COL;
	string autoIncrement = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
	string nextVal = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;
    
    SimpleColumn* col[17];
    col[0] = new SimpleColumn(columnlength, fSessionID);    
    col[1] = new SimpleColumn(objectid, fSessionID); 
    col[2] = new SimpleColumn(datatype, fSessionID);
    col[3] = new SimpleColumn(dictobjectid, fSessionID);
    col[4] = new SimpleColumn(listobjectid, fSessionID);
    col[5] = new SimpleColumn(treeobjectid, fSessionID);
    col[6] = new SimpleColumn(columnposition, fSessionID);
    col[7] = new SimpleColumn(scale, fSessionID);
    col[8] = new SimpleColumn(precision, fSessionID);
    col[9] = new SimpleColumn(defaultvalue, fSessionID);
    col[10] = new SimpleColumn(schemaname, fSessionID);
    col[11] = new SimpleColumn(tablename, fSessionID);
    col[12] = new SimpleColumn(columnname, fSessionID);
    col[13] = new SimpleColumn(nullable, fSessionID);
	col[14] = new SimpleColumn(compressiontype, fSessionID);
	col[15] = new SimpleColumn(autoIncrement, fSessionID);
	col[16] = new SimpleColumn(nextVal, fSessionID);
	
    SRCP srcp;
    srcp.reset(col[0]);
    colMap.insert(CMVT_(columnlength, srcp));
    srcp.reset(col[1]);
    colMap.insert(CMVT_(objectid, srcp));
    srcp.reset(col[2]);
    colMap.insert(CMVT_(datatype, srcp));    
    srcp.reset(col[3]);
    colMap.insert(CMVT_(dictobjectid, srcp));
    srcp.reset(col[4]);
    colMap.insert(CMVT_(listobjectid, srcp));
    srcp.reset(col[5]);
    colMap.insert(CMVT_(treeobjectid, srcp));
    srcp.reset(col[6]);
    colMap.insert(CMVT_(columnposition, srcp));    
    srcp.reset(col[7]);
    colMap.insert(CMVT_(scale, srcp));
    srcp.reset(col[8]);
    colMap.insert(CMVT_(precision, srcp));
    
    srcp.reset(col[9]);
    colMap.insert(CMVT_(defaultvalue, srcp));
    srcp.reset(col[10]);
    colMap.insert(CMVT_(schemaname, srcp));
    srcp.reset(col[11]);
    colMap.insert(CMVT_(tablename, srcp));
    srcp.reset(col[12]);
    colMap.insert(CMVT_(columnname, srcp));
    srcp.reset(col[13]);
    colMap.insert(CMVT_(nullable, srcp));
	srcp.reset(col[14]);
    colMap.insert(CMVT_(compressiontype, srcp));
	srcp.reset(col[15]);
    colMap.insert(CMVT_(autoIncrement, srcp));
	srcp.reset(col[16]);
    colMap.insert(CMVT_(nextVal, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(col[1]->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);

    OID oid[17];
    for (int i = 0; i < 17; i++)
        oid[i] = col[i]->oid();
    
    oid[12] = DICTOID_SYSCOLUMN_COLNAME;
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         col[10]->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));

    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         col[11]->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList);

    ostringstream oss;
    oss << "select objectid,columnname from syscolumn where schema='" << aTableName.schema << "' and tablename='" <<
        aTableName.table << "' --columnRIDs/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    csep.data(oss.str());
    NJLSysDataList sysDataList;
    getSysData (csep, sysDataList, SYSCOLUMN_TABLE);

    vector<ColumnResult*>::const_iterator it;
    ColType ct;
    ColType *ctList = NULL;
    TableInfo ti;
    ti.tablewithautoincr = NO_AUTOINCRCOL;

    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid[1]) // objectid
        {
            DEBUG << "column count: " << (*it)->dataCount() << endl;
            // populate tableinfo cache for numOfCols
            ti.numOfCols = (*it)->dataCount();
//			ti.tablewithautoincr = NO_AUTOINCRCOL;
         
            ctList = new ColType[ti.numOfCols];
            for (int i = 0 ; i < (*it)->dataCount(); i++)
            {
                ROPair rp;
//                 rp.rid = -1;
                rp.objnum = (*it)->GetData(i);
                if (fIdentity == EC)
                    rp.rid = (*it)->GetRid(i);
                DEBUG << rp.rid << " ";
                rl.push_back(rp);
                ColType ct;
                ct.columnOID = rp.objnum;
                ctList[i] = ct;
            }
            DEBUG << endl;
        }
		else if ((*it)->ColumnOID() == oid[15]) //autoincrement
		{
			
			for (int i = 0 ; i < (*it)->dataCount(); i++)
            {
				ostringstream os;
				os << (char) (*it)->GetData(i);
				if (os.str().compare("y") == 0)
				{
					ti.tablewithautoincr = AUTOINCRCOL;	
					break;
				} 		
			}
		}
		lk1.lock();
        fTableInfoMap[aTableName] = ti;
        lk1.unlock();
    }

    // loop 2nd time to make sure rl has been populated.
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid[12])
        {
            lk2.lock();
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                TableColName tcn = make_tcn(aTableName.schema, aTableName.table, (*it)->GetStringData(i));
                fOIDmap[tcn] = rl[i].objnum;
                if (fIdentity == EC)
                	fColRIDmap[tcn] = rl[i].rid;
            }
            lk2.unlock();            
        }
        else if ((*it)->ColumnOID() == oid[0])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].colWidth = (*it)->GetData(i);
        }
        else if ((*it)->ColumnOID() == oid[2])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].colDataType = (ColDataType)((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[3])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].ddn.dictOID = ((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[4])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].ddn.listOID = ((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[5])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].ddn.treeOID = ((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[6])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].colPosition = ((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[7])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].scale = ((*it)->GetData(i));
        }
        else if ((*it)->ColumnOID() == oid[8])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].precision = ((*it)->GetData(i));
        }
        // TODO: check datatype to call GetData() or GetStringData()
        else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_DEFAULTVAL)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
			{
                ctList[i].defaultValue = ((*it)->GetStringData(i));
				if ((!ctList[i].defaultValue.empty()) || (ctList[i].defaultValue.length() > 0))
				{
					if (ctList[i].constraintType != NOTNULL_CONSTRAINT)
						ctList[i].constraintType = DEFAULT_CONSTRAINT;
				}
			}
        }
        else if ((*it)->ColumnOID() == oid[13])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                if ((*it)->GetData(i) == 0)
                    ctList[i].constraintType = NOTNULL_CONSTRAINT;
        }
		else if ((*it)->ColumnOID() == oid[14])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].compressionType = ctList[i].ddn.compressionType = ((*it)->GetData(i));
        }
		else if ((*it)->ColumnOID() == oid[15])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
			{
				ostringstream os;
				os << (char) (*it)->GetData(i);
				if (os.str().compare("y") == 0)
					ctList[i].autoincrement = true;
				else
					ctList[i].autoincrement = false;			
			}
        }
		else if ((*it)->ColumnOID() == oid[16])
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
                ctList[i].nextvalue = ((*it)->GetData(i));
        }
    }
    
    // populate colinfo cache
    lk3.lock();
    for (int i = 0; i < ti.numOfCols; i++)
        fColinfomap[ctList[i].columnOID] = ctList[i];
    lk3.unlock();
    
    delete [] ctList;
   // delete col[9];    
    if (rl.size() != 0)
    {
        return rl;
    }
		
		Message::Args args;
		args.add("'" + tableName.schema + "." + tableName.table + "'");
		throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
}

const CalpontSystemCatalog::TableName CalpontSystemCatalog::tableName(const OID& tableoid)
{
	//Check whether cache needs to be flushed
	if ( tableoid >= 3000) 
	{
		checkSysCatVer();
	}

	// check cache
	boost::mutex::scoped_lock lk(fTableNameMapLock);
	if ( fTableNameMap.size() > 0 )
	{
		TableNameMap::const_iterator iter = fTableNameMap.find(tableoid);
		if (iter != fTableNameMap.end())
			return iter->second;
	}
	lk.unlock();
	
	//select schema, tablename from systable where objectid = tableoid
	TableName tableName;
	CalpontSelectExecutionPlan csep;          
	CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
	CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
	CalpontSelectExecutionPlan::ColumnMap colMap;   
	        
	SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, fSessionID);
	SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, fSessionID);
	SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, fSessionID);     
	
	SRCP srcp;
	srcp.reset(c1);
	colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, srcp));
	srcp.reset(c2);
	colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, srcp));
	srcp.reset(c3);
	colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, srcp));
	csep.columnMapNonStatic(colMap);
	
	srcp.reset(c2->clone());
	returnedColumnList.push_back(srcp);
	srcp.reset(c3->clone());
	returnedColumnList.push_back(srcp);
	csep.returnedCols(returnedColumnList);
	
	
	OID oid2 = DICTOID_SYSTABLE_SCHEMA;
	OID oid3 = DICTOID_SYSTABLE_TABLENAME;
	
	// Filters
	SimpleFilter *f1 = new SimpleFilter (opeq,
	                                     c1->clone(),
	                                     new ConstantColumn(tableoid, ConstantColumn::NUM));
	filterTokenList.push_back(f1);
	csep.filterTokenList(filterTokenList); 
	
	ostringstream oss;
	oss << "select schema,tablename,columnname from syscolumn where objectid=" << tableoid <<
	    " --colName/";
	if (fIdentity == EC) oss << "EC";
	else oss << "FE";
	csep.data(oss.str());
	NJLSysDataList sysDataList;  
	
	try {
		getSysData (csep, sysDataList, SYSTABLE_TABLE);  
	}
	catch  (runtime_error& e)
	{
		throw runtime_error ( e.what() );
	}
	vector<ColumnResult*>::const_iterator it;
	
	for (it = sysDataList.begin(); it != sysDataList.end(); it++)
	{
		if ((*it)->dataCount() == 0)
		{
			Message::Args args;
			oss <<tableoid;
			args.add("'" + oss.str() + "'");
			throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
		}
		
		if ((*it)->ColumnOID() == oid2)
			tableName.schema = (*it)->GetStringData(0);
		else if ((*it)->ColumnOID() == oid3)
			tableName.table = (*it)->GetStringData(0);
	}
	//@Bug 2682. datacount 0 sometimes does not mean the table is not found.
	if ( (tableName.schema.length() == 0) || (tableName.table.length() == 0) )
	{
		Message::Args args;
		oss <<tableoid;
		args.add("'" + oss.str() + "'");
		throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
	}

	// populate cache
	lk.lock();
	fTableNameMap[tableoid] = tableName;
	lk.unlock();
	return tableName;
}


const CalpontSystemCatalog::ROPair CalpontSystemCatalog::tableRID(const TableName& tableName)
{
    TableName aTableName;
    aTableName.schema = tableName.schema;
    aTableName.table = tableName.table;
    transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
    transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );

    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter tableRID: " << tableName.schema << "|" << tableName.table << endl;

    // look up cache first for system table and cached table
    ROPair rp;
//     rp.rid = -1; @bug1866  use default

    // calpontsys only needs oid
	boost::mutex::scoped_lock lk1(fTableInfoMapLock);
	Tablemap::const_iterator iter = fTablemap.find(aTableName);
    if (aTableName.schema.compare("calpontsys") == 0 && iter != fTablemap.end() )
    {
        rp.objnum = (*iter).second;
        return rp;
    }
	lk1.unlock();

	checkSysCatVer();	

	lk1.lock();
	iter = fTablemap.find(aTableName);
    TableRIDmap::const_iterator rid_iter = fTableRIDmap.find(aTableName);
    if (iter != fTablemap.end() && rid_iter != fTableRIDmap.end())
    {
        rp.objnum = (*iter).second;
        rp.rid = (*rid_iter).second;
        return rp;
    }
	lk1.unlock();

	// select objectid from systable where schema = tableName.schema and tablename = tableName.table;
    CalpontSelectExecutionPlan csep;
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;

    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, fSessionID);

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+OBJECTID_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSTABLE_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = c1->oid();

    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));

    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList);

    ostringstream oss;
    oss << "select objectid from systable where schema='" << aTableName.schema << "' and tablename='" <<
        aTableName.table << "' --tableRID/";
    if (fIdentity == EC) oss << "EC";
    else oss << "FE";
    NJLSysDataList sysDataList;
	try {
		getSysData (csep, sysDataList, SYSTABLE_TABLE);
	}
    catch ( IDBExcept& ){
        throw;
    }
	catch ( runtime_error& e ) {
		throw runtime_error ( e.what() );
	}
	
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
        {
			Message::Args args;
			args.add("'" + tableName.schema + "." + tableName.table + "'");
			//throw logging::NoTableExcept(msg);
			throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
        }
        if ((*it)->ColumnOID() == oid)
        {
            rp.objnum = (OID)((*it)->GetData(0));
            if (fIdentity == EC) {
                rp.rid = (*it)->GetRid(0);
            }
			// populate cache
			lk1.lock();
			fTablemap[aTableName] = rp.objnum;
			fTableRIDmap[aTableName] = rp.rid;
			return rp;
        }	
    }
	
    //string msg("CalpontSystemCatalog::tableRID: no OID found for ");
    //msg += tableName.schema;
    //msg += ".";
    //msg += tableName.table;
	Message::Args args;
	args.add("'" + tableName.schema + "." + tableName.table + "'");
    //throw logging::NoTableExcept(msg);
	throw IDBExcept(ERR_TABLE_NOT_IN_CATALOG, args);
}

#if 0
const CalpontSystemCatalog::IndexNameList CalpontSystemCatalog::indexNames(const TableName& tableName)
{
    DEBUG << "Enter indexNames: " << tableName.schema << "|" << tableName.table << endl;
    IndexNameList indexlist;
    
    TableName aTableName;
	aTableName.schema = tableName.schema;
	aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );

    /* SQL statement: select indexname from sysindex where schema=indexName.schema and
     * tablename=indexName.table and indexname=indexName.index;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   

    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, fSessionID);            
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, fSessionID);       

    SRCP srcp;
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c5->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid5 = DICTOID_SYSINDEX_INDEXNAME;
    OID oid2 = c2->oid();
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    IndexName indexName;
    indexName.schema = tableName.schema;
    indexName.table = tableName.table;
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid5)
        {
            if (indexlist.size() != 0)
            {
                for (int i = 0; i < (*it)->dataCount(); i++)                            
                    indexlist[i].index = (*it)->GetStringData(i);  
            }
            else
            {                                  
	            for (int i = 0; i < (*it)->dataCount(); i++)
	            {
	                indexName.index = (*it)->GetStringData(i);
	                indexlist.push_back(indexName);
	            }
            }
        }
        else if ((*it)->ColumnOID() == oid2)
        {
            if (indexlist.size() != 0)
            {
                for (int i = 0; i < (*it)->dataCount(); i++)            
                    indexlist[i].multiColFlag = ((*it)->GetData(i) == 't'? true : false);                        
            }
            else
            {
            	for (int i = 0; i < (*it)->dataCount(); i++)
            	{
                indexName.multiColFlag = ((*it)->GetData(i) == 't'? true : false);
                indexlist.push_back(indexName);
              }
            }
        }
    }	
	return indexlist;	
}

const CalpontSystemCatalog::TableColNameList CalpontSystemCatalog::indexColNames ( const IndexName& indexName)
{
    DEBUG << "Enter indexColNames: " << indexName.schema << "|" << indexName.table << "|" << indexName.index << endl;
    
    // not cached yet
	CalpontSystemCatalog::TableColNameList tableColNameList;
	    
    IndexName aIndexName;   
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;	
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    /* SQL statement: select columnname, columnposition from sysindexcol where schema=indexname.schema and 
     * tablename=indexName.table and indexname=indexName.index;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLUMNPOS_COL, fSessionID);    
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);       
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLUMNPOS_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = DICTOID_SYSINDEXCOL_COLNAME;
    OID oid2 = c2->oid();
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aIndexName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c5->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);      
    vector<ColumnResult*>::const_iterator it;
    
    // help arrays. assume max index in a table. for sorting purpose
    int dataCount = (*sysDataList.begin())->dataCount();
    TableColName result[dataCount]; 
    vector<int> colPos;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1)
        {
            TableColName tcn;
            tcn.schema = indexName.schema;
            tcn.table = indexName.table;   
            for (int i = 0; i < dataCount; i++)
            {                
                tcn.column = (*it)->GetStringData(i);
                tableColNameList.push_back(tcn);
            }
        }
        else if ((*it)->ColumnOID() == oid2)
        {
            for (int i = 0; i < dataCount; i++)             
                colPos.push_back((*it)->GetData(i));
        }
    }    

    // sorting tableColName based on columnPosition
    vector<int>::iterator iter = colPos.begin();
    TableColNameList::iterator iter1 = tableColNameList.begin();
    for (; iter != colPos.end(); iter++)
    {
        result[(*iter)] = (*iter1);
        tableColNameList.erase(iter1);
    }
    
    for (int i = 0; i < dataCount; i++)
        tableColNameList.push_back(result[i]);            
    
	return tableColNameList;
}

const CalpontSystemCatalog::TableColNameList CalpontSystemCatalog::constraintColNames ( const std::string constraintName)
{
    DEBUG << "Enter constraintColNames: " << constraintName << endl;
	    
    std::string aConstraintName( constraintName );   
	
	transform( aConstraintName.begin(), aConstraintName.end(), aConstraintName.begin(), to_lower() );

    /* SQL statement: select columnname from sysconstraintcol where constraintname = aConstraintName 
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, fSessionID);       

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+COLNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+CONSTRAINTNAME_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINTCOL_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c3->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c4->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = DICTOID_SYSCONSTRAINTCOL_COLNAME;
    OID oid3 = DICTOID_SYSCONSTRAINTCOL_SCHEMA;
    OID oid4 = DICTOID_SYSCONSTRAINTCOL_TABLENAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aConstraintName, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINTCOL_TABLE);      
    vector<ColumnResult*>::const_iterator it;
    
    // help arrays. assume max index in a table. for sorting purpose
    //int dataCount = (*sysDataList.begin())->dataCount();
   
	vector<TableColName> colNameList;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                TableColName tableColName;                        
                tableColName.column = (*it)->GetStringData(i);
                colNameList.push_back(tableColName);
            }
            break;
        }
    }
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid3)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                colNameList[i].schema = (*it)->GetStringData(i);
            }
            continue;
        }
        if ((*it)->ColumnOID() == oid4)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                colNameList[i].table = (*it)->GetStringData(i);
            }
            continue;
        }
    }                
    
	return colNameList;
}

const CalpontSystemCatalog::ROPair CalpontSystemCatalog::indexRID(const IndexName& indexName)
{
    DEBUG << "Enter indexRID: " << indexName.schema << "|" << indexName.table << indexName.index << endl;
    ROPair rp;
    
    IndexName aIndexName;
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    /* SQL statement: select indexname from sysindex where schema=indexName.schema and indexname=indexName.index;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   

    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, fSessionID);       

    SRCP srcp;
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
                         
    csep.returnedCols(returnedColumnList);
    OID oid5 = DICTOID_SYSINDEX_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c5->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return rp;
        if ((*it)->ColumnOID() == oid5)
        {
            if (fIdentity == EC)
                rp.rid = (*it)->GetRid(0);
        }
            
    }	
	return rp;	
}
#endif

const int CalpontSystemCatalog::colNumbers(const TableName& tableName)
{
    DEBUG << "Enter colNumbers: " << tableName.schema << "|" << tableName.table << endl;
    
    TableInfo ti = tableInfo(tableName);

    return ti.numOfCols;
}

#if 0
const std::string CalpontSystemCatalog::colValueSysindex (const TableColName& tableColName)
{
/* SQL statement: select indexname from sysindex where schema = schema and table=table and column=column;*/
	TableColName aTableColName;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );    
    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter colValueSysindex: " << tableColName.schema << "|"
              << tableColName.table << "|"
              << tableColName.column << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, fSessionID);        
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
        {
            string msg("CalpontSystemCatalog::colValueSysindex: no indexname found for ");
            msg += tableColName.schema;
            msg += ".";
            msg += tableColName.table;
            msg += ".";
            msg += tableColName.column;
            throw runtime_error(msg);
        }
        
        if ((*it)->ColumnOID() == oid)
            return (*it)->GetStringData(0);
    }       
    
    string msg("CalpontSystemCatalog::colValueSysindex: no indexname found for ");
    msg += tableColName.schema;
    msg += ".";
    msg += tableColName.table;
    msg += ".";
    msg += tableColName.column;
    throw runtime_error(msg); 
}

const CalpontSystemCatalog::RIDList CalpontSystemCatalog::indexColRID(const IndexName& indexName)
{
    /* SQL statement: select indexname from sysindexcol where schema=tableColName.schema and 
     * tablename=tableColName.table and columnname = tableColName.column;
     */
    IndexName aIndexName;
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    if (aIndexName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter indexColRID: " << aIndexName.schema << "|"
              << aIndexName.table << "|" << aIndexName.index << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    RIDList ridlist;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        for (int i = 0; i < (*it)->dataCount(); i++)
        {
            ROPair rp;
            if (fIdentity == EC)
                rp.rid = (*it)->GetRid(i);
            ridlist.push_back(rp);
        }
    }
    return ridlist;
}

const CalpontSystemCatalog::ROPair CalpontSystemCatalog::indexColRID(const TableColName& tableColName)
{
    /* SQL statement: select indexname from sysindexcol where schema=tableColName.schema and 
     * tablename=tableColName.table and columnname = tableColName.column;
     */
    TableColName aTableColName;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter indexColRID: " << tableColName.schema << "|"
              << tableColName.table << "|" << tableColName.column << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    ROPair rid;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return rid;
        if ((*it)->ColumnOID() == oid)
        {
            if (fIdentity == EC)
                rid.rid = (*it)->GetRid(0);
            return rid;
        }
    }
    return rid;
}

const CalpontSystemCatalog::IndexNameList CalpontSystemCatalog::colValueSysindexCol (const TableColName& tableColName)
{
    /* SQL statement: select indexname from sysindex where schema = schema and table=table and column=column;*/
	TableColName aTableColName;
	CalpontSystemCatalog::IndexNameList indexNameList;
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );    

	return indexNameList;  //so colxml can run when indexes are not made

    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter colValueSysindexCol: " << tableColName.schema << "|"
              << tableColName.table << "|"
              << tableColName.column << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, fSessionID);        

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid)
        {
            IndexName indexName;
            indexName.schema = aTableColName.schema;
            indexName.table = aTableColName.table;
            for ( int i = 0; i < (*it)->dataCount(); i++)
            {
                indexName.index = (*it)->GetStringData(i);
                indexNameList.push_back(indexName);
            }
        }
    }
    return indexNameList;
}

const CalpontSystemCatalog::TableName CalpontSystemCatalog::lookupTableForIndex(const std::string indexName, const std::string schema) 
{
    DEBUG << "Enter lookupTableForIndex" << endl;
    CalpontSystemCatalog::TableName tablename;
	//select tablename from sysindex where indexname = indexName and schema = schema;
	std::string aIndexName( indexName );
	std::string aSchema ( schema);
    transform( aIndexName.begin(), aIndexName.end(), aIndexName.begin(), to_lower() );
    transform( aSchema.begin(), aSchema.end(), aSchema.begin(), to_lower() );
    tablename.schema = aSchema;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, fSessionID);    
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, fSessionID);       

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, srcp));   
    csep.columnMapNonStatic(colMap);

    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid2 = DICTOID_SYSINDEX_TABLENAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aSchema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));                              
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
        {
            string msg("CalpontSystemCatalog::lookupTableForIndex: no table name found for ");
            msg += aIndexName;
            throw runtime_error(msg);
        }
        if ((*it)->ColumnOID() == oid2)
            tablename.table = (*it)->GetStringData(0);       
    }
    return tablename;
}

const CalpontSystemCatalog::IndexOID CalpontSystemCatalog::lookupIndexNbr(const IndexName& indexName)
{
    CalpontSystemCatalog::IndexOID indexoid = {-1, -1};
    IndexName aIndexName;   
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;	
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    if (aIndexName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter lookupIndexNbr: " << indexName.schema << "|" << indexName.table 
              << "|" << indexName.index << endl;
      
	// return pre-defined indexoid for system catalog index. currently no index
	// created for system catalog, return invalid(default) indexoid.
	if (aIndexName.schema.compare(CALPONT_SCHEMA) == 0)
	    return indexoid;	

    /* SQL statement: select listobjectoid, treeobjectoid, multicolflag from sysindex where schema=indexName.schema and
     * tablename=indexName.table and indexname=indexName.index;
     */
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+LISTOBJID_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TREEOBJID_COL, fSessionID);    
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, fSessionID);    
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, fSessionID);       
    SimpleColumn *c6 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, fSessionID);           
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+LISTOBJID_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TREEOBJID_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c6);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c6->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = c1->oid();
    OID oid2 = c2->oid();
    OID oid3 = c6->oid();
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));                              
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aIndexName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));       
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c5->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
        {
            string msg("CalpontSystemCatalog::lookupIndexNbr: no indexid found for ");
            msg += indexName.schema;
            msg += ".";
            msg += indexName.table;
            msg += ".";
            msg += indexName.index;
            throw runtime_error(msg);
        }
        if ((*it)->ColumnOID() == oid1)
            indexoid.listOID = (*it)->GetData(0);
        else if ((*it)->ColumnOID() == oid2)
            indexoid.objnum = (*it)->GetData(0);
        else if ((*it)->ColumnOID() == oid3)
            indexoid.multiColFlag = ((*it)->GetData(0)== 't'? true : false);            
    }
    
    return indexoid;   
}

const CalpontSystemCatalog::IndexOID CalpontSystemCatalog::lookupIndexNbr(const TableColName& tableColName)
{
    /*SQL statement: select indexname from sysindexcol where schema=tableColName.schema and
     *  tablename=tableColName.table and columnname=tableColName.column;
     *  select listobjectoid, treeobjectoid from sysindex where schema=tableColName.schema and
     *  table=tableColName.table and indexname=indexname(previous statement);*/
    IndexName index;   
    TableColName aTableColName;
    CalpontSystemCatalog::IndexOID indexoid = {-1, -1};
    
	aTableColName.schema = tableColName.schema;
	aTableColName.table = tableColName.table;
	aTableColName.column = tableColName.column;
	transform( aTableColName.schema.begin(), aTableColName.schema.end(), aTableColName.schema.begin(), to_lower() );
	transform( aTableColName.table.begin(), aTableColName.table.end(), aTableColName.table.begin(), to_lower() );
	transform( aTableColName.column.begin(), aTableColName.column.end(), aTableColName.column.begin(), to_lower() );
    
    if (aTableColName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter lookupIndexNbr: " << tableColName.schema << "|" << tableColName.table 
              << "|" << tableColName.column << endl;
        
    index.schema = tableColName.schema;
    index.table = tableColName.table;

	// return pre-defined indexoid for system catalog index. currently no index
	// created for system catalog, return invalid(default) indexoid.
	if (aTableColName.schema.compare(CALPONT_SCHEMA) == 0)
	    return indexoid;
	        
    // select objectid from syscolumn where schema = tableColName.schema and tablename = tableColName.table and columnname = tableColName.column;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, fSessionID);        
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+INDEXNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEXCOL_TABLE+"."+COLNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid = DICTOID_SYSINDEXCOL_INDEXNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableColName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableColName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));                                     
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableColName.column, ConstantColumn::LITERAL));                                         
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEXCOL_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return indexoid;
        if ((*it)->ColumnOID() == oid)
        {
            index.index = ((*it)->GetStringData(0));
            return lookupIndexNbr(index);
        }
    }
    return indexoid;
}

const CalpontSystemCatalog::IndexOIDList CalpontSystemCatalog::indexOIDs( const TableName& tableName )
{   
    /*select listobjectoid, treeobjectoid from sysindex where schema=tableName.schema and
     * tablename=tableName.table;*/
    // not cached yet. can be done in the future
    TableColName aTableName;
    CalpontSystemCatalog::IndexOID indexoid = {-1, -1};
    IndexOIDList indexlist;
    
	aTableName.schema = tableName.schema;
	aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
    
    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter indexOIDs: " << tableName.schema << "|" << tableName.table << endl;
        
	// return pre-defined indexoid for system catalog index. currently no index
	// created for system catalog, return invalid(default) indexoid.
	if (aTableName.schema.compare(CALPONT_SCHEMA) == 0)
	    return indexlist;
	        
    // select objectid from syscolumn where schema = tableColName.schema and tablename = tableColName.table and columnname = tableColName.column;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+LISTOBJID_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TREEOBJID_COL, fSessionID);    
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c6 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, fSessionID);           

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+LISTOBJID_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TREEOBJID_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c6);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSINDEX_TABLE+"."+MULTICOLFLAG_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c6->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = c1->oid();
    OID oid2 = c2->oid();
    OID oid3 = c6->oid();
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSINDEX_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return indexlist;
        if ((*it)->ColumnOID() == oid1)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                indexoid.listOID = (*it)->GetData(i);
                indexlist.push_back(indexoid);
            }
            break;   
        }
    }
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {        
        if ((*it)->ColumnOID() == oid2)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)            
                indexlist[i].objnum = (*it)->GetData(i);    
        }
            
        else if ((*it)->ColumnOID() == oid3)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)            
                indexlist[i].multiColFlag = ((*it)->GetData(i) == 't'? true : false);              
        }
    }
    
    return indexlist;    
}
#endif

const CalpontSystemCatalog::DictOIDList CalpontSystemCatalog::dictOIDs( const TableName& tableName )
{
    /* SQL statement: select dictobjectid, listobjectid, treeobjectid from syscolumn where 
      * schema=tableName.schema and table=tableName.table;*/
    DictOIDList dictOIDList;
    TableColName aTableName;
    CalpontSystemCatalog::DictOID dictoid;
    
	aTableName.schema = tableName.schema;
	aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
    
    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter dictOIDs: " << tableName.schema << "|" << tableName.table << endl;
        
	// return pre-defined indexoid for system catalog index. currently no index
	// created for system catalog, return invalid(default) indexoid.
	if (aTableName.schema.compare(CALPONT_SCHEMA) == 0)
	    return dictOIDList;
	        
    // select objectid from syscolumn where schema = tableColName.schema and tablename = tableColName.table and columnname = tableColName.column;
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL, fSessionID);    
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL, srcp));    
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c2->clone());
    returnedColumnList.push_back(srcp);
    srcp.reset(c5->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = c1->oid();
    OID oid2 = c2->oid();
    OID oid3 = c5->oid();
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
	SOP opisnotnull(new Operator("isnotnull"));
	filterTokenList.push_back(new Operator("and"));
	SimpleFilter *f3 = new SimpleFilter (opisnotnull,
										c5->clone(),
										new ConstantColumn("", ConstantColumn::NULLDATA));
	filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCOLUMN_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    
    // loop for oid1 first to make sure dictOIDList is populated
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
            return dictOIDList;
        if ((*it)->ColumnOID() == oid1)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                dictoid.listOID = (*it)->GetData(i);
                dictOIDList.push_back(dictoid);
            }     
            break; 
        }
    }
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid2)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)            
                dictOIDList[i].treeOID = (*it)->GetData(i);    
        }
            
        else if ((*it)->ColumnOID() == oid3)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)            
                dictOIDList[i].dictOID = (*it)->GetData(i);             
        }
    }
    
    return dictOIDList;
}

#if 0 //Not implemented
void CalpontSystemCatalog::storeColOID(void) 
{
}

void CalpontSystemCatalog::storeDictOID(void) 
{
}

void CalpontSystemCatalog::storeIndexOID(void)
{
}

void CalpontSystemCatalog::updateColInfo(void) 
{
}
#endif

const int CalpontSystemCatalog::colPosition (const OID& oid)
{
    DEBUG << "Enter colPosition: " << oid << endl;
    ColType col = colType (oid);
    return col.colPosition;
}

const CalpontSystemCatalog::TableInfo CalpontSystemCatalog::tableInfo (const TableName& tb)
{
	TableName aTableName;
	aTableName.schema = tb.schema;
	aTableName.table = tb.table;	
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );

    if (aTableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter tableInfo: " << tb.schema << "|" << tb.table << endl;
    
    // look up cache first
    TableInfo ti;
    RIDList ridlist ;
    // select count(objectid) from syscolumn where schema=tableName.schema and tablename=tableName.table;
	try {
		ridlist = columnRIDs(tb);
	}
	catch (logging::IDBExcept& noTable)
	{
		 throw runtime_error (noTable.what());
	}
    if (ridlist.size() == 0)
        throw runtime_error ("No table info found for" + tb.schema + "." + tb.table);
	
	if ( aTableName.schema.compare(CALPONT_SCHEMA) == 0)
	{
		ti.numOfCols = ridlist.size();
		ti.tablewithautoincr = 0;
		return ti;
	}
	
	boost::mutex::scoped_lock lk1(fTableInfoMapLock);
    TableInfoMap::const_iterator ti_iter = fTableInfoMap.find(aTableName);
	if (ti_iter !=  fTableInfoMap.end())
	{
		return (*ti_iter).second;
	} 
	else
		throw runtime_error ("No table info found for" + tb.schema + "." + tb.table);
}

#if 0
const CalpontSystemCatalog::ConstraintInfo CalpontSystemCatalog::constraintInfo (const IndexName& indexName)
{
    /* SQL statement: select constraintType, constraintText, referencedTableName, referencedSchema, referencedConstraintName from sysconstraint where schema=indexName.schema and tablename=indexName.table and constraintName=indexName.index;*/
    ConstraintInfo constraintInfo;
    IndexName aIndexName;
	aIndexName.schema = indexName.schema;
	aIndexName.table = indexName.table;
	aIndexName.index = indexName.index;
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    if (aIndexName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter constraintInfo: " << aIndexName.schema << "|" << aIndexName.table << aIndexName.index << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, fSessionID);
    SimpleColumn *c5 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTEXT_COL, fSessionID);
    SimpleColumn *c6 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDSCHEMA_COL, fSessionID);
    SimpleColumn *c7 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDTABLENAME_COL, fSessionID);
    SimpleColumn *c8 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDCONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c9 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTSTATUS_COL, fSessionID);

    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, srcp));
    srcp.reset(c5);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTEXT_COL, srcp));
    srcp.reset(c6);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDSCHEMA_COL, srcp));
    srcp.reset(c7);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDTABLENAME_COL, srcp));
    srcp.reset(c8);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDCONSTRAINTNAME_COL, srcp));
    srcp.reset(c9);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTSTATUS_COL, srcp));
    csep.columnMapNonStatic(colMap);

    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid4 = c4->oid();
    OID oid5 = DICTOID_SYSCONSTRAINT_CONSTRAINTTEXT;
    OID oid6 = DICTOID_SYSCONSTRAINT_REFERENCEDSCHEMA;
    OID oid7 = DICTOID_SYSCONSTRAINT_REFERENCEDTABLENAME;
    OID oid8 = DICTOID_SYSCONSTRAINT_REFERENCEDCONSTRAINTNAME;
    OID oid9 = DICTOID_SYSCONSTRAINT_CONSTRAINTSTATUS;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aIndexName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c1->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 
    
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINT_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->dataCount() == 0)
        {
            string msg("CalpontSystemCatalog::constraintInfo: no constraint info found for ");
            msg += indexName.schema;
            msg += ".";
            msg += indexName.table;
            msg += ".";
            msg += indexName.index;
            throw runtime_error(msg);
        }
        if ((*it)->ColumnOID() == oid4)
        {
            if ((*it)->GetData(0) == 'p')
                constraintInfo.constraintType = PRIMARYKEY_CONSTRAINT;
            else if ((*it)->GetData(0) == 'f')
                constraintInfo.constraintType = REFERENCE_CONSTRAINT;
            else if ((*it)->GetData(0) == 'n')
                constraintInfo.constraintType = NOTNULL_CONSTRAINT;            
            else if ((*it)->GetData(0) == 'c')
                constraintInfo.constraintType = CHECK_CONSTRAINT; 
            else if ((*it)->GetData(0) == 'u')
                constraintInfo.constraintType = UNIQUE_CONSTRAINT; 
            else if ((*it)->GetData(0) == '0')
                constraintInfo.constraintType = DEFAULT_CONSTRAINT;
            else    // should never be here
                constraintInfo.constraintType = NO_CONSTRAINT; 
            continue;
        }
        if ((*it)->ColumnOID() == oid5)
        {
            constraintInfo.constraintText = (*it)->GetStringData(0);
            continue;
        }
        if ((*it)->ColumnOID() == oid6)
        {
            constraintInfo.referenceSchema = (*it)->GetStringData(0);
            continue;
        }
        if ((*it)->ColumnOID() == oid7)
        {
            constraintInfo.referenceTable = (*it)->GetStringData(0);
            continue;
        }
        if ((*it)->ColumnOID() == oid8)
        {
            constraintInfo.referencePKName = (*it)->GetStringData(0);
            continue;
        }
        if ((*it)->ColumnOID() == oid9)
        {
            constraintInfo.constraintStatus = (*it)->GetStringData(0);
            continue;
        }
    }      
    constraintInfo.constraintName = aIndexName;
    return constraintInfo;    
}

const CalpontSystemCatalog::IndexNameList CalpontSystemCatalog::referenceConstraints( const IndexName& referencePKName)
{
    /*  SQL statement: select schema, tablename, constraintname from sysconstraint 
        where referencedTableName = referencePKName.table
        and referencedSchema=referencePKName.schema 
        and referencedConstraintName=referencePKName.index 
        and constraintType = 'f';*/    
    IndexName aIndexName;
	aIndexName.schema = referencePKName.schema;
	aIndexName.table = referencePKName.table;
	aIndexName.index = referencePKName.index;
	transform( aIndexName.schema.begin(), aIndexName.schema.end(), aIndexName.schema.begin(), to_lower() );
	transform( aIndexName.table.begin(), aIndexName.table.end(), aIndexName.table.begin(), to_lower() );
	transform( aIndexName.index.begin(), aIndexName.index.end(), aIndexName.index.begin(), to_lower() );

    if (aIndexName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter referenceConstraints: " << aIndexName.schema << "|" << aIndexName.table << aIndexName.index << endl;
    
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, fSessionID);
    SimpleColumn *c6 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDSCHEMA_COL, fSessionID);
    SimpleColumn *c7 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDTABLENAME_COL, fSessionID);
    SimpleColumn *c8 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDCONSTRAINTNAME_COL, fSessionID);
    
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, srcp));
    srcp.reset(c6);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDSCHEMA_COL, srcp));
    srcp.reset(c7);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDTABLENAME_COL, srcp));
    srcp.reset(c8);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+REFERENCEDCONSTRAINTNAME_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = DICTOID_SYSCONSTRAINT_CONSTRAINTNAME;
    OID oid2 = DICTOID_SYSCONSTRAINT_SCHEMA;
    OID oid3 = DICTOID_SYSCONSTRAINT_TABLENAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c6->clone(),
                                         new ConstantColumn(aIndexName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c7->clone(),
                                         new ConstantColumn(aIndexName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c8->clone(),
                                         new ConstantColumn(aIndexName.index, ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f4 = new SimpleFilter(opeq,
                                        c4->clone(),
                                        new ConstantColumn("f", ConstantColumn::LITERAL));
    filterTokenList.push_back(f4);
    csep.filterTokenList(filterTokenList); 
    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINT_TABLE);  
    //IndexNameList indexNameList;
    vector<IndexName> indexNameList;
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                IndexName indexName;                        
                indexName.index = (*it)->GetStringData(i);
                indexNameList.push_back(indexName);
            }
            break;
        }
    }
    
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid2)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                indexNameList[i].schema = (*it)->GetStringData(i);
            }
            continue;
        }
        if ((*it)->ColumnOID() == oid3)
        {
            for (int i = 0; i < (*it)->dataCount(); i++)
            {
                indexNameList[i].table = (*it)->GetStringData(i);
            }
            continue;
        }
    }      
    return indexNameList; 
}

const string CalpontSystemCatalog::primaryKeyName (const TableName& tableName )
{
    TableName aTableName;
    aTableName.schema = tableName.schema;
    aTableName.table = tableName.table;
	transform( aTableName.schema.begin(), aTableName.schema.end(), aTableName.schema.begin(), to_lower() );
	transform( aTableName.table.begin(), aTableName.table.end(), aTableName.table.begin(), to_lower() );
    
    if (tableName.schema.compare(CALPONT_SCHEMA) != 0)
        DEBUG << "Enter primaryKeyName: " << tableName.schema << "|" << tableName.table << endl;
 
    string primaryKeyName = "";
    CalpontSelectExecutionPlan csep;          
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::ColumnMap colMap;   
            
    SimpleColumn *c1 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, fSessionID);
    SimpleColumn *c2 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, fSessionID);
    SimpleColumn *c3 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, fSessionID);
    SimpleColumn *c4 = new SimpleColumn(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, fSessionID);
    
    SRCP srcp;
    srcp.reset(c1);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTNAME_COL, srcp));
    srcp.reset(c2);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+SCHEMA_COL, srcp));
    srcp.reset(c3);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+TABLENAME_COL, srcp));
    srcp.reset(c4);
    colMap.insert(CMVT_(CALPONT_SCHEMA+"."+SYSCONSTRAINT_TABLE+"."+CONSTRAINTTYPE_COL, srcp));
    csep.columnMapNonStatic(colMap);
        
    srcp.reset(c1->clone());
    returnedColumnList.push_back(srcp);
    csep.returnedCols(returnedColumnList);
    OID oid1 = DICTOID_SYSCONSTRAINT_CONSTRAINTNAME;
    
    // Filters
    SimpleFilter *f1 = new SimpleFilter (opeq,
                                         c2->clone(),
                                         new ConstantColumn(aTableName.schema, ConstantColumn::LITERAL));
    filterTokenList.push_back(f1);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f2 = new SimpleFilter (opeq,
                                         c3->clone(),
                                         new ConstantColumn(aTableName.table, ConstantColumn::LITERAL));
    filterTokenList.push_back(f2);
    filterTokenList.push_back(new Operator("and"));
    
    SimpleFilter *f3 = new SimpleFilter (opeq,
                                         c4->clone(),
                                         new ConstantColumn("p", ConstantColumn::LITERAL));
    filterTokenList.push_back(f3);
    csep.filterTokenList(filterTokenList); 

    NJLSysDataList sysDataList;  
    getSysData (csep, sysDataList, SYSCONSTRAINT_TABLE);  
    
    vector<ColumnResult*>::const_iterator it;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
        if ((*it)->ColumnOID() == oid1 && (*it)->dataCount() == 1)
        {
            primaryKeyName = (*it)->GetStringData(0);
            break;
        }
    }
    return primaryKeyName;
}
#endif

void CalpontSystemCatalog::getSchemaInfo(const string& in_schema)
{
	string schema = in_schema;
	transform( schema.begin(), schema.end(), schema.begin(), to_lower() );
	
	if (schema == CALPONT_SCHEMA)    
	    return;
	else
	    DEBUG << "Enter getSchemaInfo: " << schema << endl;
	    
	//Check whether cache needs to be flushed
	checkSysCatVer();
	
	boost::mutex::scoped_lock lk(fSchemaCacheLock);
	set<string>::iterator setIt = fSchemaCache.find(schema);
	if (setIt != fSchemaCache.end())
	{
	    DEBUG << "getSchemaInfo Cached" << endl;
	    return;
	}
	lk.unlock();
	
	// get table info first
	getTables(schema);
	
	// get column info now
	RIDList rl;
	
	// get real data from system catalog for all user tables under the schema
	CalpontSelectExecutionPlan csep;
	CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
	CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
	CalpontSelectExecutionPlan::ColumnMap colMap;
	
	string columnlength = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNLEN_COL;
	string objectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+OBJECTID_COL;
	string datatype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DATATYPE_COL;
	string dictobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DICTOID_COL;
	string listobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+LISTOBJID_COL;
	string treeobjectid = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TREEOBJID_COL;
	string columnposition = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLUMNPOS_COL;
	string scale = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCALE_COL;
	string precision = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+PRECISION_COL;
	string defaultvalue = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+DEFAULTVAL_COL;
	// the following columns will be save in cache although it's not needed for now
	string columnname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COLNAME_COL;
	string tablename = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+TABLENAME_COL;
	string schemaname = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+SCHEMA_COL;
	string nullable = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NULLABLE_COL;
	string compressiontype = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+COMPRESSIONTYPE_COL;
	string autoinc = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+AUTOINC_COL;
	string nextval = CALPONT_SCHEMA+"."+SYSCOLUMN_TABLE+"."+NEXTVALUE_COL;

	SimpleColumn* col[17];
	col[0] = new SimpleColumn(columnlength, fSessionID);    
	col[1] = new SimpleColumn(objectid, fSessionID); 
	col[2] = new SimpleColumn(datatype, fSessionID);
	col[3] = new SimpleColumn(dictobjectid, fSessionID);
	col[4] = new SimpleColumn(listobjectid, fSessionID);
	col[5] = new SimpleColumn(treeobjectid, fSessionID);
	col[6] = new SimpleColumn(columnposition, fSessionID);
	col[7] = new SimpleColumn(scale, fSessionID);
	col[8] = new SimpleColumn(precision, fSessionID);
	col[9] = new SimpleColumn(defaultvalue, fSessionID);
	col[10] = new SimpleColumn(schemaname, fSessionID);
	col[11] = new SimpleColumn(tablename, fSessionID);
	col[12] = new SimpleColumn(columnname, fSessionID);
	col[13] = new SimpleColumn(nullable, fSessionID);
	col[14] = new SimpleColumn(compressiontype, fSessionID);
	col[15] = new SimpleColumn(autoinc, fSessionID);
	col[16] = new SimpleColumn(nextval, fSessionID);

	SRCP srcp;
	srcp.reset(col[0]);
	colMap.insert(CMVT_(columnlength, srcp));
	srcp.reset(col[1]);
	colMap.insert(CMVT_(objectid, srcp));
	srcp.reset(col[2]);
	colMap.insert(CMVT_(datatype, srcp));    
	srcp.reset(col[3]);
	colMap.insert(CMVT_(dictobjectid, srcp));
	srcp.reset(col[4]);
	colMap.insert(CMVT_(listobjectid, srcp));
	srcp.reset(col[5]);
	colMap.insert(CMVT_(treeobjectid, srcp));
	srcp.reset(col[6]);
	colMap.insert(CMVT_(columnposition, srcp));    
	srcp.reset(col[7]);
	colMap.insert(CMVT_(scale, srcp));
	srcp.reset(col[8]);
	colMap.insert(CMVT_(precision, srcp));
	// TODO: NULL value handling & convert to boost::any
	// delete this manually at fcn exit
	srcp.reset(col[9]);
	colMap.insert(CMVT_(defaultvalue, srcp));
	srcp.reset(col[10]);
	colMap.insert(CMVT_(schemaname, srcp));
	srcp.reset(col[11]);
	colMap.insert(CMVT_(tablename, srcp));
	srcp.reset(col[12]);
	colMap.insert(CMVT_(columnname, srcp));
	srcp.reset(col[13]);
	colMap.insert(CMVT_(nullable, srcp));
	srcp.reset(col[14]);
	colMap.insert(CMVT_(compressiontype, srcp));
	srcp.reset(col[15]);
	colMap.insert(CMVT_(autoinc, srcp));
	srcp.reset(col[16]);
	colMap.insert(CMVT_(nextval, srcp));
	csep.columnMapNonStatic(colMap);

	srcp.reset(col[1]->clone());
	returnedColumnList.push_back(srcp);
	csep.returnedCols(returnedColumnList);
	
	OID oid[17];
	for (int i = 0; i < 17; i++)
	    oid[i] = col[i]->oid();
	
	oid[12] = DICTOID_SYSCOLUMN_COLNAME;
	oid[11] = DICTOID_SYSCOLUMN_TABLENAME;
	
	// Filters
	SimpleFilter *f1 = new SimpleFilter (opeq,
	                                     col[10]->clone(),
	                                     new ConstantColumn(schema, ConstantColumn::LITERAL));
	filterTokenList.push_back(f1);
	
	csep.filterTokenList(filterTokenList);
	
	ostringstream oss;
	oss << "select objectid,columnname from syscolumn where schema='" << schema << "' --getSchemaInfo/";
	if (fIdentity == EC) oss << "EC";
	else oss << "FE";
	csep.data(oss.str());
	NJLSysDataList sysDataList;
	getSysData (csep, sysDataList, SYSCOLUMN_TABLE);
	
	vector<ColumnResult*>::const_iterator it;
	ColType ct;
	//ColType *ctList = NULL;
	vector<ColType> ctList;
	vector<string> tableNames;
	TableInfo ti;
	map<string, TableInfo> tbInfo;
	map<string, TableInfo>::iterator tbIter;
		
	for (it = sysDataList.begin(); it != sysDataList.end(); it++)
	{
		if ((*it)->ColumnOID() == oid[1]) // objectid
		{
			for (int i = 0 ; i < (*it)->dataCount(); i++)
			{
				ROPair rp;
				rp.objnum = (*it)->GetData(i);
				if (fIdentity == EC)
				    rp.rid = (*it)->GetRid(i);
			
				//DEBUG << rp.rid << " ";
				rl.push_back(rp);
				ColType ct;
				ct.columnOID = rp.objnum;
				ctList.push_back(ct);
			}
			DEBUG << endl;
		}
		// table name
		else if ((*it)->ColumnOID() == oid[11])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
			{
				tableNames.push_back((*it)->GetStringData(i));
				tbIter = tbInfo.find(tableNames[i]);
				if (tbIter == tbInfo.end())
				{
					tbInfo[tableNames[i]].numOfCols = 1;
				}
				else
					tbInfo[tableNames[i]].numOfCols += 1;
			}
		}
	}

	// loop 3rd time to populate col cache
	for (it = sysDataList.begin(); it != sysDataList.end(); it++)
	{
		if ((*it)->ColumnOID() == oid[15])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
			{
				ostringstream os;
				os << (char) (*it)->GetData(i);
				tbIter = tbInfo.find(tableNames[i]);
				if (tbIter == tbInfo.end())
				{
					if (os.str().compare("y") == 0)
					{
						tbInfo[tableNames[i]].tablewithautoincr = AUTOINCRCOL;	
					}
					else
					{
						tbInfo[tableNames[i]].tablewithautoincr = NO_AUTOINCRCOL;	
					}
				}
			}
		}
		// column name
		else if ((*it)->ColumnOID() == oid[12])
		{
		    //lk2.lock();
		    for (int i = 0; i < (*it)->dataCount(); i++)
		    {
		        TableColName tcn = make_tcn(schema, tableNames[i], (*it)->GetStringData(i));
		        fOIDmap[tcn] = rl[i].objnum;
		        if (fIdentity == EC)
		        	fColRIDmap[tcn] = rl[i].rid;
		    }
		    //lk2.unlock();            
		}
		else if ((*it)->ColumnOID() == oid[0])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].colWidth = (*it)->GetData(i);
		}
		else if ((*it)->ColumnOID() == oid[2])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].colDataType = (ColDataType)((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[3])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].ddn.dictOID = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[4])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].ddn.listOID = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[5])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].ddn.treeOID = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[6])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].colPosition = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[7])
		{
		    for (int i = 0; i < (*it)->dataCount(); i++)
		        ctList[i].scale = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[8])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
				ctList[i].precision = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == DICTOID_SYSCOLUMN_DEFAULTVAL)
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
			{
				ctList[i].defaultValue = ((*it)->GetStringData(i));
				if ((!ctList[i].defaultValue.empty()) || (ctList[i].defaultValue.length() > 0))
				{
					if (ctList[i].constraintType != NOTNULL_CONSTRAINT)
						ctList[i].constraintType = DEFAULT_CONSTRAINT;
				}
			}
		}
		else if ((*it)->ColumnOID() == oid[13])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
				if ((*it)->GetData(i) == 0)
					ctList[i].constraintType = NOTNULL_CONSTRAINT;
		}
		else if ((*it)->ColumnOID() == oid[14])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
				ctList[i].compressionType = ctList[i].ddn.compressionType = ((*it)->GetData(i));
		}
		else if ((*it)->ColumnOID() == oid[15])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
			{
				ostringstream os;
				os << (char) (*it)->GetData(i);
				if (os.str().compare("y") == 0)
					ctList[i].autoincrement = true;
				else
					ctList[i].autoincrement = false;
			}
		}
		else if ((*it)->ColumnOID() == oid[16])
		{
			for (int i = 0; i < (*it)->dataCount(); i++)
				ctList[i].nextvalue = ((*it)->GetData(i));
		}
	}

	// populate colinfo cache
	//boost::mutex::scoped_lock lk3(fColinfomapLock);
	for (uint i = 0; i < ctList.size(); i++)
		fColinfomap[ctList[i].columnOID] = ctList[i];
	//lk3.unlock();
	// populate tbinfomap
	for (tbIter = tbInfo.begin(); tbIter != tbInfo.end(); ++tbIter)
	{
		TableName tn(schema, tbIter->first);
		//ti.numOfCols = (tbIter->second).numOfCols;
		//ti.tablewithautoincr = (tbIter->second).withAutoInc;
		fTableInfoMap[tn] = tbIter->second;
		DEBUG << tbIter->first << " " << tbIter->second.numOfCols << " " << (tbIter->second).tablewithautoincr << endl;
	}
   
	//delete col[9];    
	lk.lock();
	fSchemaCache.insert(schema);
	lk.unlock();
}

#if 0
ostream& operator<<(ostream& os, const CalpontSystemCatalog::TableName& rhs)
{
    os << rhs.schema << '.' << rhs.table;
    return os;
}
#endif
const string CalpontSystemCatalog::TableName::toString() const
{
	string str = schema + "." + table;
	return str;
}
ostream& operator<<(ostream& os, const CalpontSystemCatalog::TableAliasName& rhs)
{
    os << rhs.schema << '.' << rhs.table << "(" << rhs.alias << "/" << rhs.view 
       << ") engineType=" << (rhs.fIsInfiniDB? "InfiniDB" : "ForeignEngine");
    return os;
}

ostream& operator<<(ostream& os, const CalpontSystemCatalog::TableColName& rhs)
{
    os << rhs.schema << '.' << rhs.table << '.' << rhs.column;
    return os;
}

void CalpontSystemCatalog::flushCache()
{
	boost::mutex::scoped_lock lk1(fOIDmapLock);
	fOIDmap.clear();
    buildSysOIDmap();
	lk1.unlock();

	boost::mutex::scoped_lock lk2(fColinfomapLock);
	fColinfomap.clear();
	buildSysColinfomap();
	lk2.unlock();

	boost::mutex::scoped_lock lk3(fTableInfoMapLock);
	fTableInfoMap.clear();
	fTablemap.clear();
	fTableRIDmap.clear();
	buildSysTablemap();
	lk3.unlock();

	boost::mutex::scoped_lock lk4(fDctTokenMapLock);
	fDctTokenMap.clear();
	buildSysDctmap();
	lk4.unlock();

	fSyscatSCN = fSessionManager->sysCatVerID();
	//cout << "Cache flushed and current sysCatVerID is " << newScn << endl;
}

void CalpontSystemCatalog::updateColinfoCache(CalpontSystemCatalog::OIDNextvalMap & oidNextvalMap)
{
	boost::mutex::scoped_lock lk(fColinfomapLock);
	CalpontSystemCatalog::OIDNextvalMap::const_iterator iter = oidNextvalMap.begin();
	OID oid = 0;
	long long nextVal = 0;
	while (iter != oidNextvalMap.end())
	{
		oid = (*iter).first;
		nextVal = (*iter).second;
		fColinfomap[oid].nextvalue = nextVal;
		iter++;
	}
	
}
void CalpontSystemCatalog::buildSysColinfomap()
{
	ColType aCol;
   // aCol.defaultValue = "";
    aCol.scale = 0;
    aCol.precision = 10;
	aCol.compressionType = 0;
	DictOID notDict;

    // @bug 4433 - Increase object width from 64 to 128 for schema names, table names, and column names.
    aCol.colWidth = 129; // @bug 4433
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSTABLE_TABLENAME;
    aCol.ddn.listOID = LISTOID_SYSTABLE_TABLENAME;
    aCol.ddn.treeOID = TREEOID_SYSTABLE_TABLENAME;
    aCol.colPosition = 0;
	aCol.columnOID = OID_SYSTABLE_TABLENAME;
    fColinfomap[aCol.columnOID] = aCol;
 
    aCol.colWidth = 129; // @bug 4433
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSTABLE_SCHEMA;
    aCol.ddn.listOID = LISTOID_SYSTABLE_SCHEMA;
    aCol.ddn.treeOID = TREEOID_SYSTABLE_SCHEMA;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_SCHEMA;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_OBJECTID;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = DATE;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_CREATEDATE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = DATE;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_LASTUPDATE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_INIT;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_NEXT;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_NUMOFROWS;
    fColinfomap[aCol.columnOID] = aCol;
	
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_AVGROWLEN;
    fColinfomap[aCol.columnOID] = aCol;
	
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_NUMOFBLOCKS;
    fColinfomap[aCol.columnOID] = aCol;
	
	aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSTABLE_AUTOINCREMENT;
    fColinfomap[aCol.columnOID] = aCol;
	
    fTablemap[make_table(CALPONT_SCHEMA, SYSCOLUMN_TABLE )] = SYSCOLUMN_BASE;
    
    aCol.colWidth = 129; // @bug 4433
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_SCHEMA;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_SCHEMA;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_SCHEMA;
    aCol.colPosition = 0;
	aCol.columnOID = OID_SYSCOLUMN_SCHEMA;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 129; // @bug 4433
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_TABLENAME;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_TABLENAME;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_TABLENAME;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_TABLENAME;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 129; // @bug 4433
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_COLNAME;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_COLNAME;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_COLNAME;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_COLNAME;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_OBJECTID;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_DICTOID;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_LISTOBJID;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_TREEOBJID;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_DATATYPE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_COLUMNLEN;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_COLUMNPOS;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = DATE;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_LASTUPDATE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 64;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_DEFAULTVAL;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_DEFAULTVAL;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_DEFAULTVAL;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_DEFAULTVAL;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_NULLABLE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_SCALE;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_PRECISION;
    fColinfomap[aCol.columnOID] = aCol;
	
    aCol.colWidth = 1;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = CHAR;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_AUTOINC;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_DISTCOUNT;
    fColinfomap[aCol.columnOID] = aCol;
    
    aCol.colWidth = 4;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_NULLCOUNT;
    fColinfomap[aCol.columnOID] = aCol;
   
    aCol.colWidth = 65;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_MINVALUE;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_MINVALUE;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_MINVALUE;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_MINVALUE;
    fColinfomap[aCol.columnOID] = aCol;
   
    aCol.colWidth = 65;
    aCol.constraintType = NO_CONSTRAINT;
    aCol.colDataType = VARCHAR;
    aCol.ddn.dictOID = DICTOID_SYSCOLUMN_MAXVALUE;
    aCol.ddn.listOID = LISTOID_SYSCOLUMN_MAXVALUE;
    aCol.ddn.treeOID = TREEOID_SYSCOLUMN_MAXVALUE;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_MAXVALUE;
    fColinfomap[aCol.columnOID] = aCol;
        
	aCol.colWidth = 4;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = INT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_COMPRESSIONTYPE;
    fColinfomap[aCol.columnOID] = aCol;
	
	aCol.colWidth = 8;
    aCol.constraintType = NOTNULL_CONSTRAINT;
    aCol.colDataType = BIGINT;
    aCol.ddn = notDict;
    aCol.colPosition++;
	aCol.columnOID = OID_SYSCOLUMN_NEXTVALUE;
    fColinfomap[aCol.columnOID] = aCol;
}

void CalpontSystemCatalog::buildSysOIDmap()
{
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, TABLENAME_COL)] = OID_SYSTABLE_TABLENAME;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, SCHEMA_COL)] = OID_SYSTABLE_SCHEMA;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, OBJECTID_COL)] = OID_SYSTABLE_OBJECTID;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, CREATEDATE_COL)] = OID_SYSTABLE_CREATEDATE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, LASTUPDATE_COL)] = OID_SYSTABLE_LASTUPDATE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, INIT_COL)] = OID_SYSTABLE_INIT;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, NEXT_COL)] = OID_SYSTABLE_NEXT;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, NUMOFROWS_COL)] = OID_SYSTABLE_NUMOFROWS;
	fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, AVGROWLEN_COL)] = OID_SYSTABLE_AVGROWLEN;
	fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, NUMOFBLOCKS_COL)] = OID_SYSTABLE_NUMOFBLOCKS;
	fOIDmap[make_tcn(CALPONT_SCHEMA, SYSTABLE_TABLE, AUTOINC_COL)] = OID_SYSTABLE_AUTOINCREMENT;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, SCHEMA_COL)] = OID_SYSCOLUMN_SCHEMA;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, TABLENAME_COL)] = OID_SYSCOLUMN_TABLENAME;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, COLNAME_COL)] = OID_SYSCOLUMN_COLNAME;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, OBJECTID_COL)] = OID_SYSCOLUMN_OBJECTID;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, DICTOID_COL)] = OID_SYSCOLUMN_DICTOID;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, LISTOBJID_COL)] = OID_SYSCOLUMN_LISTOBJID;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, TREEOBJID_COL)] = OID_SYSCOLUMN_TREEOBJID;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, DATATYPE_COL)] = OID_SYSCOLUMN_DATATYPE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, COLUMNLEN_COL)] = OID_SYSCOLUMN_COLUMNLEN;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, COLUMNPOS_COL )] = OID_SYSCOLUMN_COLUMNPOS;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, LASTUPDATE_COL)] = OID_SYSCOLUMN_LASTUPDATE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, DEFAULTVAL_COL)] = OID_SYSCOLUMN_DEFAULTVAL;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, NULLABLE_COL)] = OID_SYSCOLUMN_NULLABLE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, SCALE_COL)] = OID_SYSCOLUMN_SCALE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, PRECISION_COL)] = OID_SYSCOLUMN_PRECISION;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, AUTOINC_COL)] = OID_SYSCOLUMN_AUTOINC;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, DISTCOUNT_COL)] = OID_SYSCOLUMN_DISTCOUNT;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, NULLCOUNT_COL)] = OID_SYSCOLUMN_NULLCOUNT;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, MINVALUE_COL)] = OID_SYSCOLUMN_MINVALUE;
    fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, MAXVALUE_COL)] = OID_SYSCOLUMN_MAXVALUE;
	fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, COMPRESSIONTYPE_COL)] = OID_SYSCOLUMN_COMPRESSIONTYPE;
	fOIDmap[make_tcn(CALPONT_SCHEMA, SYSCOLUMN_TABLE, NEXTVALUE_COL)] = OID_SYSCOLUMN_NEXTVALUE;
}

void CalpontSystemCatalog::buildSysTablemap()
{
    fTablemap[make_table(CALPONT_SCHEMA, SYSTABLE_TABLE)] = SYSTABLE_BASE;
    fTablemap[make_table(CALPONT_SCHEMA, SYSCOLUMN_TABLE )] = SYSCOLUMN_BASE;       
}

void CalpontSystemCatalog::buildSysDctmap()
{
	fDctTokenMap[DICTOID_SYSTABLE_TABLENAME] = OID_SYSTABLE_TABLENAME;
	fDctTokenMap[DICTOID_SYSTABLE_SCHEMA] = OID_SYSTABLE_SCHEMA;

	fDctTokenMap[DICTOID_SYSCOLUMN_SCHEMA] = OID_SYSCOLUMN_SCHEMA;
	fDctTokenMap[DICTOID_SYSCOLUMN_TABLENAME] = OID_SYSCOLUMN_TABLENAME;
	fDctTokenMap[DICTOID_SYSCOLUMN_COLNAME] = OID_SYSCOLUMN_COLNAME;
	fDctTokenMap[DICTOID_SYSCOLUMN_DEFAULTVAL] = OID_SYSCOLUMN_DEFAULTVAL;
	fDctTokenMap[DICTOID_SYSCOLUMN_MINVALUE] = OID_SYSCOLUMN_MINVALUE;
	fDctTokenMap[DICTOID_SYSCOLUMN_MAXVALUE] = OID_SYSCOLUMN_MAXVALUE;
}

void CalpontSystemCatalog::checkSysCatVer()
{
	SCN newScn = fSessionManager->sysCatVerID();
	if (newScn < 0)
	{
		fSessionManager.reset(new SessionManager());
		newScn = fSessionManager->sysCatVerID();
	}
	boost::mutex::scoped_lock sysCatLk(fSyscatSCNLock);
	if ( fSyscatSCN != newScn ) {
		flushCache();
	}		
}

const string CalpontSystemCatalog::ColType::toString() const
{
	ostringstream output;
	output <<  "cw: " << colWidth
	       << " dt: " << colDataTypeToString(colDataType)
	       << " do: " << ddn.dictOID
	       << " lo: " << ddn.listOID
	       << " to: " << ddn.treeOID
	       << " cp: " << colPosition
	       << " sc: " << scale
	       << " pr: " << precision
	       << " od: " << columnOID
	       << " ct: " << compressionType
		   << " ai: " << autoincrement
		   << " nv: " << nextvalue;
	return output.str();
}

//format a session id that includes the module id
//we want the top bit clear to use as a syscat flag, then we want 7 bits of module id, then 24 bits of thread id
/*static*/
uint32_t CalpontSystemCatalog::idb_tid2sid(const uint32_t tid)
{
	//don't care about locking here...
	if (fModuleID == numeric_limits<uint32_t>::max())
	{
		uint32_t tmid = 1;
		oam::Oam oam;
		oam::oamModuleInfo_t minfo;
		try {
			minfo = oam.getModuleInfo();
			tmid = static_cast<uint32_t>(boost::get<2>(minfo));
			if (tmid == 0)
				tmid = 1;
		} catch (...) {
			tmid = 1;
		}
		fModuleID = tmid;
	}
	uint32_t mid = fModuleID;
	mid--; //make module id zero-based
	mid &= 0x0000007f;
	uint32_t sid = (mid << 24) | (tid & 0x00ffffff);
	return sid;
}

ostream& operator<<(ostream& output, const CalpontSystemCatalog::ColType& rhs)
{
	output << rhs.toString();
	return output;
}

vector<CalpontSystemCatalog::OID> getAllSysCatOIDs()
{
	vector<CalpontSystemCatalog::OID> ret;
	CalpontSystemCatalog::OID oid;

	for (oid = SYSTABLE_BASE + 1; oid < SYSTABLE_MAX; oid++)
		ret.push_back(oid);
	for (oid = SYSCOLUMN_BASE + 1; oid < SYSCOLUMN_MAX; oid++)
		ret.push_back(oid);
	for (oid = SYSTABLE_DICT_BASE + 1; oid < SYSTABLE_DICT_MAX; oid++)
		ret.push_back(oid);
	for (oid = SYSCOLUMN_DICT_BASE + 1; oid < SYSCOLUMN_DICT_MAX; oid++)
		ret.push_back(oid);
	return ret;
}

} // namespace execplan
// vim:sw=4 ts=4:
