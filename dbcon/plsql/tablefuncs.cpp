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

/*************************
*
* $Id: tablefuncs.cpp 9320 2013-03-19 21:39:21Z dhall $
*
*************************/
#include "tablefuncs.h"
#include "checkerr.h"

#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <string>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <set>
#include <vector>

using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/scoped_array.hpp>

#include "configcpp.h"
using namespace config;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "dmlpackage.h"
#include "calpontdmlpackage.h"
#include "insertdmlpackage.h"
#include "vendordmlstatement.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
#include "simplecolumn.h"
using namespace execplan;

#include "liboamcpp.h"
using namespace oam;

#include "getplan.h"
#include "dhcs_misc.h"
#include "dhcs_ti.h"
using namespace sm;

#include "messagelog.h"
#include "messageobj.h"
#include "messageids.h"
#include "errorcodes.h"
using namespace logging;

#include "threadpool.h"
using namespace threadpool;

#include "resourcemanager.h"
using namespace joblist;

#ifndef OCI_ORACLE
# include <oci.h>
#endif
#ifndef ODCI_ORACLE
# include <odci.h>
#endif

#include "planutils.h"
namespace pu = plsql::planutils;

#define SWALLOW_ROWS (traceFlags & CalpontSelectExecutionPlan::TRACE_NO_ROWS1)
#define DEBUG_DUMP_ROWS (traceFlags & 512)

//#define TBLDEBUG 1
const char* f1file = "/tmp/f1.dat";
const string planfile("/var/log/Calpont/trace/plan.");

struct insertColInfo
{
	int colPosition;
	std::string value;
	bool constFlag;
	char schemaName[80];
};

struct StoredCtx
{
	OCIType* rowType;
	ub4 sessionID;
	bool tableDone;
	void* conn_hndl;
	void* tplhdl;
	void* tpl_scanhdl;
	//way bigger than we currently allow (<30)
	char schemaName[80];
	char tableName[80];
	int rowsReturned;
	OCIAnyDataSet* anyDS;
};
typedef struct StoredCtx StoredCtx;
typedef set< int, less<int> > int_set;

namespace {
const OCIDuration ociDuration = OCI_DURATION_CALL;

bool isDML = false;
bool isInsert = false;
string insertText;
cpsm_conhdl_t* cal_conn_hndl = 0;
bool planReady = false;
bool alreadyDumped = false;
ub4 traceFlags = CalpontSelectExecutionPlan::TRACE_TUPLE_OFF;
static u_int32_t statementID = 1; // one-up number assigned to SQL statement
OCIServer *srvhp;
OCIStmt *updatestmthp;
OCIStmt *dmlplanhp;
OCIStmt *dmlplanhp1;
dvoid* tmp;
sword status;
OCIStmt *dmlstmthp;
char dmlsqltext[4096] = {0};;
char* dmlstmt;
//dhcs_tableid_t prevTblid = 0;
int_set prevTblids;
OCIBind *bnd1p = 0;
OCIBind *bnd2p = 0;
OCIBind *bnd3p = 0;
OCIBind *bnd4p = 0;
OCIBind *bnd5p = 0;
OCIBind *bnd6p = 0;
OCIBind *bnd7p = 0;
OCIBind *bnd8p = 0;
char sqltext[4096]= {0};
char ssqltext[4000]= {0};
Handles_t handles;
char* stmt;
bool setupSQL = true;
bool dmlsetupSQL = true;
bool dmlplanSetup = true;
std::vector <insertColInfo> insertCols;
MessageLog* ml = 0;
int dmlRowCount = 0;
string savePath;
RequiredColOidSet rcos;
string queryStats;

ErrorCodes errorCodes;
vector<RMParam> rmParms;

/* DML statment alias replacement funtion. For update and delete only.
   dmlFlag: 1, update; 2, delete. */
std::string aliasReplace( std::string dmlStmt, int dmlFlag )
{
	std::string newStmt;
	//Skip white space
	string::size_type startPlace = dmlStmt.find_first_not_of(" ");
	dmlStmt = dmlStmt.substr( startPlace, dmlStmt.length()-startPlace);
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep( " ;");
	std::string alias;
	std::string tableName;
	string::size_type pos1 = 0;
	string::size_type pos2 = 0;
	string::size_type pos3 = 0;
	if ( dmlFlag == 1 ) //UPDATE
	{
		std::string cpyStmt( dmlStmt );
		std::string secondPart;
		boost::algorithm::to_lower(cpyStmt);
		string::size_type stopPlace = cpyStmt.find("set");
		cpyStmt = dmlStmt.substr( 0, stopPlace); //should be table part
		secondPart = dmlStmt.substr( stopPlace, dmlStmt.length()-stopPlace );
		//Find alias
		tokenizer tokens(cpyStmt, sep);
		std::vector<std::string> dataList;
		for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
		{
			dataList.push_back(*tok_iter);
		}

		if ( dataList.size() > 1 && strcasecmp(dataList[1].c_str(), "as") == 0 )
		{
			if ( dataList.size() == 3 )
			{
				alias = dataList[2];
				tableName = dataList[0];
			}
			else
			{
				alias = "NULL";
			}
		}
		else if ( dataList.size() > 1 )
		{
			alias = dataList[1];
			tableName = dataList[0];
		}
		else //no alias
		{
			newStmt = dmlStmt;
			return newStmt;
		}

		//Replace the aliases in the rest statement with real table name.
		if ( strcasecmp(alias.c_str(), "NULL") != 0 )
		{
			pos1 = 0;
			pos2 = 0;
			// try to replace the dot in '' in order not to confuse table.
			while (pos2 < secondPart.length() && pos1 != string::npos)
			{
				pos1 = secondPart.find_first_of("'", pos2);
				pos2 = secondPart.find_first_of("'", pos1+1);
				pos3 = secondPart.find_first_of(".", pos1);
				if (pos3 < pos2)
					secondPart.replace(pos3, 1, "^");
				pos2++;
			}
			alias += ".";
			startPlace = secondPart.find( alias );
			while ( startPlace < secondPart.length() )
			{
				secondPart.replace( startPlace, alias.length(), "" );
				startPlace = secondPart.find( alias, startPlace + 1 );
			}

			// recover '^' to '.'
			for (string::size_type i = 0; i < secondPart.length(); i++)
				if (secondPart[i] == '^') secondPart[i] = '.';

			cout << secondPart << endl;
		}
		else
		{
			newStmt = "";
			return newStmt;
		}
		newStmt = tableName + " " + secondPart;
	}
	else //DELETE
	{
		std::string cpyStmt( dmlStmt );
		std::string secondPart;
		boost::algorithm::to_lower(cpyStmt);
		string::size_type stopPlace = cpyStmt.find("where");
		if ( stopPlace < string::npos )
		{
			cpyStmt = dmlStmt.substr( 0, stopPlace); //should be table part
			secondPart = dmlStmt.substr( stopPlace, dmlStmt.length()-stopPlace );
		}
		else
		{
			secondPart = "";

		}
		//Find alias
		tokenizer tokens(cpyStmt, sep);
		std::vector<std::string> dataList;
		for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
		{
			dataList.push_back(*tok_iter);
		}

		if ( dataList.size() > 1 && strcasecmp(dataList[1].c_str(), "as") == 0 )
		{
			if ( dataList.size() == 3 )
			{
				alias = dataList[2];
				tableName = dataList[0];
			}
			else
			{
				alias = "NULL";
			}
		}
		else if ( dataList.size() > 1 )
		{
			alias = dataList[1];
			tableName = dataList[0];
		}
		else //no alias
		{
			newStmt = dmlStmt;
			return newStmt;
		}

		//Replace the aliases in the rest statement with real table name.
		if ( strcasecmp(alias.c_str(), "NULL") != 0 )
		{
			pos1 = 0;
			pos2 = 0;
			// try to replace the dot in '' in order not to confuse table.
			while (pos2 < secondPart.length() && pos1 != string::npos)
			{
				pos1 = secondPart.find_first_of("'", pos2);
				pos2 = secondPart.find_first_of("'", pos1+1);
				pos3 = secondPart.find_first_of(".", pos1);
				if (pos3 < pos2)
					secondPart.replace(pos3, 1, "^");
				pos2++;
			}
			alias += ".";
			startPlace = secondPart.find( alias );
			while ( startPlace < secondPart.length() )
			{
				secondPart.replace( startPlace, alias.length(), "" );
				startPlace = secondPart.find( alias, startPlace + 1 );
			}

			// recover '^' to '.'
			for (string::size_type i = 0; i < secondPart.length(); i++)
				if (secondPart[i] == '^') secondPart[i] = '.';

			cout << secondPart << endl;
		}
		else
		{
			newStmt = "";
			return newStmt;
		}
		newStmt = tableName + " " + secondPart;
	}

	return newStmt;

}


/** @bug 159 fix. error handling routine. */
void errHandle(StoredCtx* storedCtx, Handles_t& handles)
{
	planReady = false;
	if (!storedCtx) return;
	if (storedCtx->conn_hndl)
	{
		if (storedCtx->tpl_scanhdl) {
			dhcs_tpl_scan_close(storedCtx->tpl_scanhdl, storedCtx->conn_hndl);
			storedCtx->tpl_scanhdl = NULL;
		}
		if (storedCtx->tplhdl) {
			dhcs_tpl_close(storedCtx->tplhdl, storedCtx->conn_hndl);
			storedCtx->tplhdl = NULL;
		}
	}

	/* Free the memory for the stored context */
	OCIMemoryFree((dvoid*) handles.usrhp, handles.errhp, (dvoid*) storedCtx);
}

// @bug 1054, 863
void sigErrHandle(StoredCtx* storedCtx)
{
	// this error message returned to user is for debugging purpose. a more
	// appropriate message need to be given for a product version.
	planReady = false;
	cal_conn_hndl = 0;  // connection will be reestablished on next query
	char errbuf[1024] = {0};
	if (!storedCtx) return;
		
	/* Free the memory for the stored context */
	OCIMemoryFree((dvoid*) handles.usrhp, handles.errhp, (dvoid*) storedCtx);
		
	sprintf((char*)errbuf, "The connection with UM was broken. The query was canceled. The connection has been reestablished so you can still send queries.");
	OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
}

/***********************************************************************/
/* Get the stored context using the key in the scan context */

StoredCtx* GetStoredCtx(Handles_t* handles, CalpontImpl* self, CalpontImpl_ind* self_ind)
{
	StoredCtx *storedCtx;           /* Stored context pointer */
	ub1 *key;                       /* key to retrieve context */
	ub4 keylen;                     /* length of key */

	/* return NULL if the PL/SQL context is NULL */
	if (self_ind->_atomic == OCI_IND_NULL) return NULL;

	/* Get the key */
	key = OCIRawPtr(handles->envhp, self->key);
	keylen = OCIRawSize(handles->envhp, self->key);

	/* Retrieve stored context using the key */
	if (checkerr(handles, OCIContextGetValue((dvoid*) handles->usrhp, handles->errhp, key, (ub1)keylen,
		(dvoid**)&storedCtx), "OCIContextGetValue"))
		return NULL;

	return storedCtx;
}

string get_sql_text(OCIExtProcContext* ctx, const string& sConnectAsUser, const string& sConnectAsUserPwd,
	const string& sConnectDBLink, ub4 sessionid)
{
/*  OCIServer *srvhp;
	OCIStmt *stmthp;
	dvoid* tmp;
	sword status;
	OCIBind *bnd1p = 0;
	OCIBind *bnd2p = 0;
	char sqltext[4096];
	Handles_t handles;
	char* stmt;
*/
	if ( setupSQL )
	{
		handles.extProcCtx = ctx;

		status = OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0);

		status = OCIHandleAlloc((dvoid *)NULL, (dvoid **)&handles.envhp, (ub4)OCI_HTYPE_ENV, 52, (dvoid **)&tmp);

		status = OCIEnvInit(&handles.envhp, (ub4)OCI_DEFAULT, 21, (dvoid **)&tmp);

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.errhp, (ub4)OCI_HTYPE_ERROR, 52,
			(dvoid **)&tmp);
		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&srvhp, (ub4)OCI_HTYPE_SERVER, 52, (dvoid **)&tmp);

		checkerr(&handles, OCIServerAttach(srvhp, handles.errhp, (text *)sConnectDBLink.c_str(),
			(sb4)sConnectDBLink.length(), (ub4)OCI_DEFAULT));

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.svchp, (ub4)OCI_HTYPE_SVCCTX, 52,
			(dvoid **)&tmp);

		/* set attribute server context in the service context */
		status = OCIAttrSet((dvoid *) handles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)srvhp, (ub4)0,
			(ub4)OCI_ATTR_SERVER, (OCIError *)handles.errhp);

		/* allocate a user context handle */
		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.usrhp, (ub4)OCI_HTYPE_SESSION, (size_t)0,
			(dvoid **)0);

		status = OCIAttrSet((dvoid *)handles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUser.c_str(),
			(ub4)sConnectAsUser.length(), OCI_ATTR_USERNAME, handles.errhp);

		status = OCIAttrSet((dvoid *)handles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUserPwd.c_str(),
			(ub4)sConnectAsUserPwd.length(), OCI_ATTR_PASSWORD, handles.errhp);

		checkerr(&handles, OCISessionBegin(handles.svchp, handles.errhp, handles.usrhp, OCI_CRED_RDBMS, OCI_DEFAULT));

		status = OCIAttrSet((dvoid *)handles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)handles.usrhp, (ub4)0,
			OCI_ATTR_SESSION, handles.errhp);

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&updatestmthp, (ub4)OCI_HTYPE_STMT, 50, (dvoid **)&tmp);

		stmt = "begin :sqltext := calpont.cal_get_sql_text(:sessionid); end;";

		checkerr(&handles, OCIStmtPrepare(updatestmthp, handles.errhp, (text*)stmt, (ub4)strlen(stmt),
			(ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

		sqltext[0] = 0;
		checkerr(&handles, OCIBindByName(updatestmthp, (OCIBind **)&bnd1p, handles.errhp,
			(text *)":sqltext", (sb4)strlen(":sqltext"),
			(dvoid *)sqltext, (sb4)4096,  SQLT_STR, (dvoid *)0,
			(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,(ub4)OCI_DEFAULT));

		checkerr(&handles, OCIBindByName(updatestmthp, (OCIBind **)&bnd2p, handles.errhp,
			(text *)":sessionid", (sb4)strlen(":sessionid"),
			(dvoid *)&sessionid, (sb4)sizeof(sessionid),  SQLT_INT, (dvoid *)0,
			(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT));

		setupSQL = false;
	}
	checkerr(&handles, OCIStmtExecute(handles.svchp, updatestmthp, handles.errhp, (ub4)1, (ub4)0, (OCISnapshot *)NULL,
		(OCISnapshot *)NULL, (ub4)OCI_DEFAULT));
/*
	status = OCIHandleFree((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT);

	status = OCISessionEnd(handles.svchp, handles.errhp, handles.usrhp, (ub4)OCI_DEFAULT);
	status = OCIServerDetach(srvhp, handles.errhp, (ub4)OCI_DEFAULT );
	status = OCIHandleFree((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER);
	status = OCIHandleFree((dvoid *)handles.svchp, (ub4)OCI_HTYPE_SVCCTX);
	status = OCIHandleFree((dvoid *)handles.errhp, (ub4)OCI_HTYPE_ERROR);
*/
	return sqltext;
}

string get_dml_sql_text(OCIExtProcContext* ctx, const string& sConnectAsUser, const string& sConnectAsUserPwd,
	const string& sConnectDBLink, ub4 sessionid)
{
/*  OCIServer *dmlsrvhp;
	OCIStmt *dmlstmthp;
	dvoid* tmp;
	sword status;
	OCIBind *bnd7p = 0;
	OCIBind *bnd8p = 0;
	char dmlsqltext[4096];
	Handles_t dmlhandles;
	char* dmlstmt;
*/
	if ( dmlsetupSQL )
	{
		handles.extProcCtx = ctx;

		status = OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0);

		status = OCIHandleAlloc((dvoid *)NULL, (dvoid **)&handles.envhp, (ub4)OCI_HTYPE_ENV, 52, (dvoid **)&tmp);

		status = OCIEnvInit(&handles.envhp, (ub4)OCI_DEFAULT, 21, (dvoid **)&tmp);

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.errhp, (ub4)OCI_HTYPE_ERROR, 52,
			(dvoid **)&tmp);
		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&srvhp, (ub4)OCI_HTYPE_SERVER, 52, (dvoid **)&tmp);

		checkerr(&handles, OCIServerAttach(srvhp, handles.errhp, (text *)sConnectDBLink.c_str(),
			(sb4)sConnectDBLink.length(), (ub4)OCI_DEFAULT));

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.svchp, (ub4)OCI_HTYPE_SVCCTX, 52,
			(dvoid **)&tmp);

		/* set attribute server context in the service context */
		status = OCIAttrSet((dvoid *) handles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)srvhp, (ub4)0,
			(ub4)OCI_ATTR_SERVER, (OCIError *)handles.errhp);

		/* allocate a user context handle */
		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&handles.usrhp, (ub4)OCI_HTYPE_SESSION, (size_t)0,
			(dvoid **)0);

		status = OCIAttrSet((dvoid *)handles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUser.c_str(),
			(ub4)sConnectAsUser.length(), OCI_ATTR_USERNAME, handles.errhp);

		status = OCIAttrSet((dvoid *)handles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUserPwd.c_str(),
			(ub4)sConnectAsUserPwd.length(), OCI_ATTR_PASSWORD, handles.errhp);

		checkerr(&handles, OCISessionBegin(handles.svchp, handles.errhp, handles.usrhp, OCI_CRED_RDBMS, OCI_DEFAULT));

		status = OCIAttrSet((dvoid *)handles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)handles.usrhp, (ub4)0,
			OCI_ATTR_SESSION, handles.errhp);

		status = OCIHandleAlloc((dvoid *)handles.envhp, (dvoid **)&dmlstmthp, (ub4)OCI_HTYPE_STMT, 50, (dvoid **)&tmp);

		dmlstmt = "begin :sqltext := calpont.cal_get_sql_text(:sessionid); end;";

		checkerr(&handles, OCIStmtPrepare(dmlstmthp, handles.errhp, (text*)dmlstmt, (ub4)strlen(dmlstmt),
			(ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

		dmlsqltext[0] = 0;
		checkerr(&handles, OCIBindByName(dmlstmthp, (OCIBind **)&bnd7p, handles.errhp,
			(text *)":sqltext", (sb4)strlen(":sqltext"),
			(dvoid *)dmlsqltext, (sb4)4096,  SQLT_STR, (dvoid *)0,
			(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,(ub4)OCI_DEFAULT));

		checkerr(&handles, OCIBindByName(dmlstmthp, (OCIBind **)&bnd8p, handles.errhp,
			(text *)":sessionid", (sb4)strlen(":sessionid"),
			(dvoid *)&sessionid, (sb4)sizeof(sessionid),  SQLT_INT, (dvoid *)0,
			(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT));

		dmlsetupSQL = false;
	}
	checkerr(&handles, OCIStmtExecute(handles.svchp, dmlstmthp, handles.errhp, (ub4)1, (ub4)0, (OCISnapshot *)NULL,
		(OCISnapshot *)NULL, (ub4)OCI_DEFAULT));
/*
	status = OCIHandleFree((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT);

	status = OCISessionEnd(handles.svchp, handles.errhp, handles.usrhp, (ub4)OCI_DEFAULT);
	status = OCIServerDetach(srvhp, handles.errhp, (ub4)OCI_DEFAULT );
	status = OCIHandleFree((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER);
	status = OCIHandleFree((dvoid *)handles.svchp, (ub4)OCI_HTYPE_SVCCTX);
	status = OCIHandleFree((dvoid *)handles.errhp, (ub4)OCI_HTYPE_ERROR);
*/
	return dmlsqltext;
}

void envDMLInit(const char* sConnectAsUser,
			 const char* sConnectAsUserPwd,
			 const char* sConnectDBLink,
			 const char* sProcedureName)
{

	if ( dmlplanSetup)
	{
		status = OCIHandleAlloc( (dvoid *)handles.envhp, (dvoid **) &dmlplanhp, (ub4) OCI_HTYPE_STMT, 50, (dvoid **) &tmp);

		// Prepare, bind parameter for stored function
		string mytest = "begin :cursor := calpont.cal_get_DML_explain_plan(:sessionid, :ssqltext, :curschema); end;";

		status = OCIStmtPrepare(dmlplanhp, handles.errhp, (text*)mytest.c_str(), (ub4)mytest.length(),
				(ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT);

		status = OCIHandleAlloc( (dvoid *) handles.envhp, (dvoid **) &dmlplanhp1,
				(ub4) OCI_HTYPE_STMT, 50, (dvoid **) &tmp);

		status = OCIBindByName (dmlplanhp, (OCIBind **) &bnd3p, handles.errhp,
				(text *)":cursor", (sb4)strlen((char *)":cursor"),
				(dvoid *)&dmlplanhp1, (sb4) 0,  SQLT_RSET, (dvoid *)0,
				(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT);

		status = OCIBindByName (dmlplanhp, (OCIBind **) &bnd4p, handles.errhp,
				(text *)":sessionid", -1,
				(ub1 *)&pu::sessionid, (sword)sizeof(pu::sessionid),  SQLT_INT, (dvoid *)0,
				(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT);

		status = OCIBindByName (dmlplanhp, (OCIBind **) &bnd5p, handles.errhp,
				(text *)":ssqltext", (sb4)strlen(":ssqltext"),
				(dvoid *)ssqltext, (sb4)4000,  SQLT_STR, (dvoid *)0,
				(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT);

		status = OCIBindByName (dmlplanhp, (OCIBind **) &bnd6p, handles.errhp,
				(text *)":curschema", (sb4)strlen(":curschema"),
				(dvoid *)pu::curschema, (sb4)80,  SQLT_STR, (dvoid *)0,
				(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT);
		dmlplanSetup = false;
	}

}

/** @brief the wrapper function to Oracle stored function cal_get_explain_plan
 */
void getDMLPlan( OCIExtProcContext* ctx,
	int sessionId, const char* sqlStmt,
	const char* curSchemaName,
	execplan::CalpontSelectExecutionPlan &plan)
{
	pu::myCtx = ctx;
	pu::sessionid = sessionId;
	strcpy(ssqltext, sqlStmt);

	strcpy(pu::curschema, curSchemaName);
	status = OCIStmtExecute(handles.svchp, dmlplanhp, handles.errhp, (ub4)1, (ub4)0,
				(OCISnapshot *)NULL, (OCISnapshot *)NULL,
				(ub4)OCI_DEFAULT);

	// fetch result and convert
	pu::getPlanRecords(dmlplanhp1);
	pu::doConversion(plan);
	return;
}

#ifdef TBLDEBUG
int describe_row(Handles_t* handles, OCIType* rowType, FILE* p)
{
	dvoid* list_attr;
	ub2 num_attr;
	OCIParam* parmp;
	OCIDescribe* dschp;
	char buf[1024];

	dschp = 0;
	if (checkerr(handles, OCIHandleAlloc(handles->envhp, (dvoid**)&dschp, OCI_HTYPE_DESCRIBE, 0, 0), "OCIHandleAlloc"))
		return ODCI_ERROR;

	if (checkerr(handles, OCIDescribeAny(handles->svchp, handles->errhp, rowType, 0, OCI_OTYPE_PTR, OCI_DEFAULT,
		OCI_PTYPE_TYPE, dschp), "OCIDescrbeAny"))
		return ODCI_ERROR;

	parmp = 0;
	if (checkerr(handles, OCIAttrGet(dschp, OCI_HTYPE_DESCRIBE, &parmp, 0, OCI_ATTR_PARAM, handles->errhp),
		"OCIAttrGet"))
		return ODCI_ERROR;

	list_attr = 0;
	if (checkerr(handles, OCIAttrGet(parmp, OCI_DTYPE_PARAM, &list_attr, 0, OCI_ATTR_LIST_TYPE_ATTRS, handles->errhp),
		"OCIAttrGet"))
		return ODCI_ERROR;

	num_attr = 0;
	if (checkerr(handles, OCIAttrGet(parmp, OCI_DTYPE_PARAM, &num_attr, 0, OCI_ATTR_NUM_TYPE_ATTRS, handles->errhp),
		"OCIAttrGet"))
		return ODCI_ERROR;

	sprintf(buf, "describe_row: %d attrs\n", num_attr);
	fwrite(buf, 1, strlen(buf), p), fflush(p);

	for (ub2 i = 0; i < num_attr; i++)
	{
		dvoid* parmdp;
		ub2 len;
		text* namep;
		ub4 sizep;
		ub2 colType;

		parmdp = 0;
		if (checkerr(handles, OCIParamGet(list_attr, OCI_DTYPE_PARAM, handles->errhp, &parmdp, i + 1), "OCIParamGet"))
			return ODCI_ERROR;

		len = 0;
		if (checkerr(handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &len, 0, OCI_ATTR_DATA_SIZE, handles->errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		namep = 0;
		sizep = 0;
		if (checkerr(handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &namep, &sizep, OCI_ATTR_NAME, handles->errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		colType = 0;
		if (checkerr(handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &colType, 0, OCI_ATTR_DATA_TYPE, handles->errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		sprintf(buf, "\t%s\t%d\t%d\n", namep, len, colType);
		fwrite(buf, 1, strlen(buf), p), fflush(p);
	}

	return ODCI_SUCCESS;
}
#endif

void send_dml_command(const string& command, ub4 sessionID)
{
	VendorDMLStatement dmlStmt(command, sessionID);
	CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);

	if (pDMLPackage == 0)
	{
		return;
	}

	ByteStream bytestream;
	bytestream << sessionID;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	MessageQueueClient mq("DMLProc");
	try
	{
		mq.write(bytestream);
		bytestream = mq.read();
	}
	catch (...)
	{
	}
}

void pause_()
{
	struct timespec req;
	struct timespec rem;

	req.tv_sec = 1;
	req.tv_nsec = 0;

	rem.tv_sec = 0;
	rem.tv_nsec = 0;

again:
	if (nanosleep(&req, &rem) != 0)
		if (rem.tv_sec > 0 || rem.tv_nsec > 0)
		{
			req = rem;
			goto again;
		}
}

}

/*--------------------------------------------------------------------------*/
/* Function definitions */
/*--------------------------------------------------------------------------*/

extern "C" EXECPROC_API int CalGetQueryStats(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* statsStr, short* statsStr_ind)
{
	Handles_t handles;
	ub4 sessionID;

	if (sessionIDn_ind == OCI_IND_NULL) return -1;

	if (GetHandles(extProcCtx, &handles)) return -2;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

	*statsStr_ind = OCI_IND_NULL;
	if (!queryStats.empty())
	{
		strncpy(statsStr, queryStats.c_str(), 4000);
		*statsStr_ind = OCI_IND_NOTNULL;
	}

	return 0;
}

extern "C" EXECPROC_API int CalRollback(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind)
{
	Handles_t handles;
	ub4 sessionID;

	if (sessionIDn_ind == OCI_IND_NULL) return -1;

	if (GetHandles(extProcCtx, &handles)) return -2;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

#ifdef TBLDEBUG
	{
		char buf[10240];
		FILE* p;
		if ((p = fopen(f1file, "w")) != NULL)
		{
			sprintf(buf, "This is CalRollback(%u)\n", sessionID);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	send_dml_command("ROLLBACK;", sessionID);

	return 0;
}

extern "C" EXECPROC_API int CalCommit(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind)
{
	Handles_t handles;
	ub4 sessionID;

	if (sessionIDn_ind == OCI_IND_NULL) return -1;

	if (GetHandles(extProcCtx, &handles)) return -2;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

#ifdef TBLDEBUG
	{
		char buf[10240];
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is CalCommit(%u)\n", sessionID);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	send_dml_command("COMMIT;", sessionID);

	return 0;
}

extern "C" EXECPROC_API int CalSetEnv(OCIExtProcContext* extProcCtx,
	char* name, short name_ind,
	char* value, short value_ind)
{
	if (name_ind == OCI_IND_NULL || value_ind == OCI_IND_NULL) return -1;
	return setenv(name, value, 1);
}

extern "C" EXECPROC_API int CalTraceOn(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind,
	OCINumber* flagsn, short flagsn_ind)
{
	Handles_t handles;
	ub4 sessionID;
	ub4 flags;

	if (sessionIDn_ind == OCI_IND_NULL || flagsn_ind == OCI_IND_NULL) return -1;

	if (GetHandles(extProcCtx, &handles)) return -2;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

	if (OCINumberToInt(handles.errhp, flagsn, sizeof(flags), OCI_NUMBER_UNSIGNED,
		(dvoid *)&flags) != OCI_SUCCESS)
		return -3;

#ifdef TBLDEBUG
	{
		char buf[10240];
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is CalTraceOn(%u, %u)\n", sessionID, flags);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	// Make sure flags are sensible
	if (flags & CalpontSelectExecutionPlan::TRACE_NO_ROWS4)
		flags |= CalpontSelectExecutionPlan::TRACE_NO_ROWS3;

	if (flags & CalpontSelectExecutionPlan::TRACE_NO_ROWS3)
		flags |= CalpontSelectExecutionPlan::TRACE_NO_ROWS2;

	if (flags & CalpontSelectExecutionPlan::TRACE_NO_ROWS2)
		flags |= CalpontSelectExecutionPlan::TRACE_NO_ROWS1;

	if (flags & 512)
		flags |= CalpontSelectExecutionPlan::TRACE_NO_ROWS1;

	// set vtable_off for oracle connector
	flags |= CalpontSelectExecutionPlan::TRACE_TUPLE_OFF; 
	traceFlags = flags;

	return 0;
}

const char* UmSmallSideMaxMemory = "ummaxmemorysmallside";
const char* PmSmallSideMaxMemory = "pmmaxmemorysmallside";


extern "C" EXECPROC_API int calSetParms(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* parmn, short parmn_ind, char* valuen, short valuen_ind)
{
	Handles_t handles;
	ub4 sessionID;

	if (OCI_IND_NULL == sessionIDn_ind == OCI_IND_NULL || OCI_IND_NULL == valuen_ind || OCI_IND_NULL == parmn_ind ) return -1;

	if (GetHandles(extProcCtx, &handles)) return -2;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

#ifdef TBLDEBUG
	{
		char buf[10240];
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is calSetParm(%u, %s, %s)\n", sessionID, parmn, valuen);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif
	try
	{
		uint64_t value = Config::uFromText(valuen);
		if (0 == strcmp(strlower(parmn), PmSmallSideMaxMemory))
		{		
			rmParms.push_back(RMParam(sessionID, execplan::PMSMALLSIDEMEMORY, value));
			return 0;
		}
		else if (0 == strcmp(strlower(parmn), UmSmallSideMaxMemory))
		{		
			rmParms.push_back(RMParam(sessionID, execplan::UMSMALLSIDEMEMORY, value));
			return 0;
		}

	}
	catch(const exception& error)
	{
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)error.what(), strlen(error.what()));	
		return -3;
	}

	string errorstr((string)"CALSETPARMS() Invalid parameter: " + parmn );
	OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errorstr.c_str(), errorstr.length());	
	return -3;
}



extern "C" EXECPROC_API void CalLogon(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind, char* schemaName, short schemaName_ind)
{
	Handles_t handles;
	ub4 sessionID;
	char errbuf[1024]= {0};

	if (sessionIDn_ind == OCI_IND_NULL) return;

	if (GetHandles(extProcCtx, &handles)) return;
	pu::errhp = handles.errhp;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return;
			
	// @bug 1029. Don't get sys cache for beetlejuice config because no database is setup yet.
	Oam oam;
	// get local module type
	string moduleType;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleType = boost::get<1>(st);
	}

	catch (...) {
		//default to dm
		moduleType = "dm";
	}

	if (moduleType != "xm" )
	{
		// @bug 682. get all system catalog info for this schema and store them in cache
		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
		if (csc)
		{
			// Make getSchemaInfo cover getTables. related to bug 1021.
			csc->getSchemaInfo(schemaName);
		}
	}
	
#ifdef TBLDEBUG
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "w")) != NULL)
		{
			sprintf(buf, "This is CalLogon(%u) %s\n", sessionID, schemaName);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif
	// @bug 649, @bug 626
	ostringstream oss;
	mode_t mode, dir_mode;
	mode = (S_IRWXU | S_IRWXG | S_IRWXO) & ~umask(0);
	dir_mode = mode | S_IWUSR | S_IXUSR;
	savePath = sm::DEFAULT_SAVE_PATH;
	Config* cf = Config::makeConfig();
	string val = cf->getConfig("OracleConnector", "SavePath");
	if (val.length() > 0)
		savePath = val;
	oss << savePath << '/' << sessionID;
	mkdir(oss.str().c_str(), dir_mode);
	oss.str("");
	oss << sessionID;
	dhcs_status_t rc = STATUS_OK+1;

	try {
		// initialize getplan environment
		envInit("calpont", "calpont", "CALFEDBMS", NULL);
		rc = dhcs_rss_init(0, 0, 0, oss.str().c_str(), (void**)&cal_conn_hndl);
		
	}
	catch (...) {
	}

	if (rc != STATUS_OK)
	{
		planReady = false;
		cal_conn_hndl = 0;
		strcpy(errbuf, "Error establishing communication channel with Calpont Engine");
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
		return;
	}
}

extern "C" EXECPROC_API void CalLogoff(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind)
{
	Handles_t handles;
	ub4 sessionID;

	if (sessionIDn_ind == OCI_IND_NULL) return;
	if (GetHandles(extProcCtx, &handles)) return;
	pu::errhp = handles.errhp;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return;

	Config* cf = Config::makeConfig();
	if (cf->getConfig("OracleConnector", "OnDisconnect") == "Rollback")
		send_dml_command("ROLLBACK;", sessionID);
	else
		send_dml_command("COMMIT;", sessionID);
	status = OCIHandleFree((dvoid *)updatestmthp, (ub4)OCI_HTYPE_STMT);
	status = OCIHandleFree((dvoid *)dmlstmthp, (ub4)OCI_HTYPE_STMT);
	status = OCISessionEnd(handles.svchp, handles.errhp, handles.usrhp, (ub4)OCI_DEFAULT);
	status = OCIServerDetach(srvhp, handles.errhp, (ub4)OCI_DEFAULT );
	status = OCIHandleFree((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER);
	status = OCIHandleFree((dvoid *)handles.svchp, (ub4)OCI_HTYPE_SVCCTX);
	status = OCIHandleFree((dvoid *)handles.errhp, (ub4)OCI_HTYPE_ERROR);
	status = OCIHandleFree((dvoid *) dmlplanhp, (ub4) OCI_HTYPE_STMT);
	status = OCIHandleFree((dvoid *) dmlplanhp1, (ub4) OCI_HTYPE_STMT);
	dhcs_rss_cleanup(cal_conn_hndl);
	// clean up getplan environment
	envCleanup();
	// @bug 695 remove system catalog instance
	CalpontSystemCatalog::removeCalpontSystemCatalog(sessionID);

	// @bug 626
	ostringstream oss;
	oss << "/bin/rm -rf " << savePath << '/' << sessionID;
	system (oss.str().c_str());

#ifdef TBLDEBUG
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is CalLogoff(%u)\n", sessionID);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif
}

/*
Prepare
Describe
Start tbl1
	Fetch tbl1
Start tbl2
	Fetch tbl2
Close tbl2
Close tbl1
*/

/* Callout for ODCITablePrepare */
extern "C" EXECPROC_API int ODCITablePrepare(
	OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind,
	ODCITabFuncInfo* info, short info_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind)
{
	if (schemaName_ind == OCI_IND_NULL ||
		tableName_ind == OCI_IND_NULL ||
		ownerName_ind == OCI_IND_NULL ||
		tblTypeName_ind == OCI_IND_NULL ||
		rowTypeName_ind == OCI_IND_NULL)
		return ODCI_ERROR;

#ifdef TBLDEBUG
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			fwrite(buf, 1, strlen(buf), p);
			sprintf(buf, "This is ODCITablePrepare('%s', '%s', '%s', '%s', '%s', '%s')\n", schemaName,
				tableName, ownerName, tblTypeName, rowTypeName, curSchemaName);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	return ODCI_SUCCESS;
}

/* Callout for ODCITableStart */

extern "C" EXECPROC_API int ODCITableStart(OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind)
{
	Handles_t handles;                   /* OCI hanldes */
	StoredCtx* storedCtx;                /* Stored context pointer */
	char errbuf[1024]= {0};
	ub4 key;                             /* key to retrieve stored context */
	ub4 sessionID = 0;

#ifdef TBLDEBUG_ALL
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			{
				sprintf(buf, "This is ODCITableStart(%u, '%s', '%s', '%s', '%s', '%s', '%s')\n", sessionID, schemaName,
				tableName, ownerName, tblTypeName, rowTypeName, curSchemaName);
				fwrite(buf, 1, strlen(buf), p);
				fclose(p);
			}
		}
	}
#endif
	if (schemaName_ind == OCI_IND_NULL ||
		tableName_ind == OCI_IND_NULL ||
		ownerName_ind == OCI_IND_NULL ||
		tblTypeName_ind == OCI_IND_NULL ||
		rowTypeName_ind == OCI_IND_NULL)
		return ODCI_ERROR;

	/* Get OCI handles */
	if (GetHandles(extProcCtx, &handles))
		return ODCI_ERROR;
	pu::errhp = handles.errhp;

	if (sessionIDn_ind != OCI_IND_NULL)
		if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
			(dvoid *)&sessionID) != OCI_SUCCESS)
			sessionID = 1;

#ifdef TBLDEBUG
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is ODCITableStart(%u, '%s', '%s', '%s', '%s', '%s', '%s')\n", sessionID, schemaName,
				tableName, ownerName, tblTypeName, rowTypeName, curSchemaName);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	/* Allocate memory to hold the stored context */
	if (checkerr(&handles, OCIMemoryAlloc((dvoid*) handles.usrhp, handles.errhp, (dvoid**) &storedCtx,
		OCI_DURATION_SESSION, (ub4) sizeof(StoredCtx), OCI_MEMORY_CLEARED), "OCIMemoryAlloc"))
		return ODCI_ERROR;

	// TODO: we might be able to fix this by calling CalLogon again, rather than bailing here
	if (cal_conn_hndl == 0)
	{
		// @bug 863. try to reestablish the connection
		ostringstream oss;
		oss << sessionID;
		envInit("calpont", "calpont", "CALFEDBMS", NULL);
		uint rc = dhcs_rss_init(0, 0, 0, oss.str().c_str(), (void**)&cal_conn_hndl);
		if (rc != 0)
		{
			errHandle(storedCtx, handles);
			strcpy(errbuf, "Error establishing communication channel with Calpont Engine");
			OCIExtProcRaiseExcpWithMsg(extProcCtx, 9998, (text*)errbuf, strlen(errbuf));
			return ODCI_ERROR;
		}
	}
			
	storedCtx->sessionID = sessionID;
	strcpy(storedCtx->schemaName, schemaName);
	strcpy(storedCtx->tableName, tableName);
	storedCtx->conn_hndl = cal_conn_hndl;
	
	Config* cf = Config::makeConfig();
	uint32_t rowsReturned = 0;
	uint32_t bandSize = DEFAULT_BAND_SIZE;
	savePath = sm::DEFAULT_SAVE_PATH;
	
	// @bug 649. Since Oracle may switch table fetching in the middle, rowsreturned
	// needs to be integer size of bandsize
	// get config variables
	string val;
	val = cf->getConfig("JobList", "FlushInterval");
	if (val.length() > 0)
		bandSize = static_cast<uint32_t>(Config::uFromText(val));
	
	rowsReturned = bandSize;
	storedCtx->rowsReturned = rowsReturned;

	dhcs_tableid_t tblid = 0;
	cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(storedCtx->conn_hndl);
	hndl->tblinfo_idx = 0;

	try {
		tblid = tbname2id(storedCtx->schemaName, storedCtx->tableName, storedCtx->conn_hndl);
	}
	catch (runtime_error &err) {
		errHandle(storedCtx, handles);
		sprintf(errbuf, "No tableID found for: %s.%s", storedCtx->schemaName, storedCtx->tableName );
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9997, (text*)errbuf, strlen(errbuf));
		return ODCI_ERROR;
	}

	//If the table has been requested before, send another plan.
#if 0
	if (!SWALLOW_ROWS)
	{
		pair<int_set::const_iterator, bool > p;
		p = prevTblids.insert( tblid );
		if ( !p.second )
			planReady = false;
	}
#endif
	
	// @Bug 626. check table start count to set saveFlag
	cpsm_tplh_t* ntplh = new cpsm_tplh_t();
	cpsm_tplsch_t* ntplsch = new cpsm_tplsch_t();
	uint8_t maxPlanCount = DEFAULT_MAX_PLAN_COUNT;
	uint8_t planCount = 0;
	val = cf->getConfig("OracleConnector", "MaxPlan");
	if (val.length() > 0)
		planCount = static_cast<uint8_t>(Config::uFromText(val));
	
	map<int, int>::iterator tidMapIter = cal_conn_hndl->tidMap.find(tblid);
	map<int, cpsm_tplsch_t>::iterator tidScanMapIter = cal_conn_hndl->tidScanMap.find(tblid);
	bool checkPlan = false;
	
	// this table has been started before
	if (tidScanMapIter != cal_conn_hndl->tidScanMap.end())
	{
		if (tidScanMapIter->second.saveFlag == SAVED)
		{
			ntplh->saveFlag = SAVED;
			ntplsch->saveFlag = SAVED;
		}
		else if (tidScanMapIter->second.saveFlag == SAVING)
		{
			ntplh->saveFlag = SAVED;
			ntplsch->saveFlag = SAVED;
			tidScanMapIter->second.saveFlag = SAVED;
		}
		// NO_SAVE flag, small rowcount
		else if (tidScanMapIter->second.bandsReturned <= SMALL_BAND_NUMBER)
		{
			ntplh->saveFlag = SAVING;
			ntplsch->saveFlag = SAVING;
			tidScanMapIter->second.saveFlag = SAVING;
			checkPlan = true;
		}
	}
	else
		checkPlan = true;
	
	if (checkPlan && tidMapIter != cal_conn_hndl->tidMap.end())
	{
		// NO_SAVE flag, big rowcount, this table has been requested since last plan sent
		if ( tidMapIter->second < maxPlanCount)
		{
			int startCtn = tidMapIter->second + 1;
			if (!SWALLOW_ROWS)
			{
				cal_conn_hndl->tidMap.clear();
				cal_conn_hndl->tidMap[tblid] = startCtn;
				planReady = false;
			}
		}
		else if (tidMapIter->second == maxPlanCount)
		{
			int startCtn = tidMapIter->second + 1;
			ntplh->saveFlag = SAVING;
			ntplsch->saveFlag = SAVING;
			tidScanMapIter->second.saveFlag = SAVING;
			if (!SWALLOW_ROWS)
			{
				cal_conn_hndl->tidMap.clear();
				cal_conn_hndl->tidMap[tblid] = startCtn;
				planReady = false;
			}
		}
	}
	
	if (tidMapIter == cal_conn_hndl->tidMap.end())
	{
		cal_conn_hndl->tidMap[tblid] = 1;
	}
	
	// @Bug 649
	if (cal_conn_hndl->curFetchTb != 0)
	{
		// need to fetch bands from socket for the previous started table
		int count = 0;
		joblist::TableBand tableBand;
		ByteStream bs;
		
		val = cf->getConfig("OracleConnector", "SavePath");
		if (val.length() > 0)
			savePath = val;
		
		// save all bands to disk
		for (;;)
		{
			bs.reset();
			bs = cal_conn_hndl->exeMgr->read();
			ostringstream oss;
			oss << savePath << '/' << sessionID << '/' << cal_conn_hndl->curFetchTb << '_' << count <<".band";
			ofstream bandFile(oss.str().c_str(), ios::out);
			if (!bandFile)
			{
				errHandle(storedCtx, handles);
				sprintf(errbuf, "Can not create file under %s, check permission.", savePath.c_str());
				OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
				return ODCI_ERROR;
			}
			bandFile << bs;
			bandFile.close();
			tableBand.unserialize(bs);
			count++;
			if (tableBand.getRowCount() == 0)
			{
				cal_conn_hndl->keyBandMap[cal_conn_hndl->curFetchTb] = count;
				break;
			}
		}
			
	}

	try {
		if (!planReady)
		{
			planReady = true;
			alreadyDumped = false;
			rcos.clear();

			// start up new transaction
			SessionManager sm;
			SessionManager::TxnID txnID;
			txnID = sm.getTxnID(sessionID);
			QueryContext verID;
			verID = sm.verID();
			// integrate with tablefuncs. get oracle execution plan and convert
			// to calpont execution plan here.

			CalpontSelectExecutionPlan plan;
			plan.txnID(txnID.id);
			plan.verID(verID);
			plan.sessionID(sessionID);

			// get calpont system catalog instance here when we have access to
			// sessionid and txnID
			boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
			csc->sessionID(sessionID);
			csc->identity(CalpontSystemCatalog::FE);

			const string calpontUser("calpont");
			const string calpontPassword("calpont");
			const string calpontDBMS("calfedbms");

			// Changing the scheme of things to go get the sqltext to determine if we are performing a DMLUpdate
			// operation instead of a standard SQLQuery operation.
			string sqltext = get_sql_text(extProcCtx, calpontUser, calpontPassword,calpontDBMS, sessionID);
			//Strip off leading space
			string::size_type startPlace = sqltext.find_first_not_of(" ");
			sqltext = sqltext.substr( startPlace, sqltext.length()-startPlace);
			string updateSql(sqltext);

			string::size_type blankspace = sqltext.find(" ",0);
			string sqlCommand = sqltext.substr(0, blankspace);
			
			// @bug 1331. partially support "create table as select" syntax. drop
			// "create table as" part.
			if (strcasecmp(sqlCommand.c_str(), "CREATE") == 0)
			{
				string tmpsql = sqltext;
				boost::to_upper(tmpsql);
				string::size_type sPos = tmpsql.find("SELECT");
				if (sPos != string::npos)
				{
					sqlCommand = "SELECT";
					sqltext = sqltext.substr(sPos);
				}
			}			
				
			ByteStream msg;
			if(strcasecmp(sqlCommand.c_str(), "SELECT") != 0)
			{
				// If this isn't a SELECT statement but is trying to be run like one, then it has to be
				// some sort of DML statement that is needing to reduce its rows through a preliminary query.
				//Check for delete statement. If there is no "from" key word, add it.
				if ( strcasecmp(sqlCommand.c_str(), "delete") == 0)
				{
					string::size_type fromPlace;
					std::string tablePart = sqltext.substr(blankspace, sqltext.length() - blankspace);
					startPlace = tablePart.find_first_not_of(" ");
					tablePart = tablePart.substr( startPlace, tablePart.length()-startPlace);
					fromPlace = tablePart.find_first_of(" ");
					string fromStr = tablePart.substr( 0, fromPlace);
					if ( strcasecmp( fromStr.c_str(), "from") != 0 )
					{
						sqltext = sqlCommand  + " from " + tablePart;
					}
				}

				//sqltext += ";";
				//VendorDMLStatement dmlStmt(sqltext, sessionID);
				isDML = true;
				isInsert = false;
				string selectStmt = "SELECT ";

				CalpontDMLPackage* pDMLPackage = 0;
				if ( strcasecmp(sqlCommand.c_str(), "delete") == 0)
				{
					std::string deleteStmt;
					string::size_type fromPlace;
					deleteStmt = sqltext.substr( blankspace, sqltext.length() - blankspace );
					std::string noFromStr( deleteStmt );
					boost::algorithm::to_lower( noFromStr );
					fromPlace = noFromStr.find( "from" );
					deleteStmt = deleteStmt.substr( fromPlace+4, deleteStmt.length() - fromPlace-4 );

					int dmlFlag = 2;
					deleteStmt = aliasReplace( deleteStmt, dmlFlag );
					deleteStmt = "delete from " + deleteStmt + ";";
					//updateStmt = updateSql + ";";

					VendorDMLStatement dmlStmt(deleteStmt, sessionID);
					//To Do: add real schema from the qualified table name.
					pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt, schemaName);
					//DMLTable *pTable = pDMLPackage->get_Table();

					string queryString = pDMLPackage->get_QueryString();
					typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
					boost::char_separator<char> sep( "<; ;>;>=;<=;=;!=;<>;{;};");
					tokenizer tokens(queryString, sep);
					std::vector<std::string> dataList;
					for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
					{
						dataList.push_back(*tok_iter);
					}
					string colName;
					if ( dataList.size() > 1 && strcasecmp(dataList[0].c_str(), "WHERE") == 0 )
					{
						//check whether there is any indexes in this table
						execplan::CalpontSystemCatalog::TableName tableName;
						tableName.schema = storedCtx->schemaName;
						tableName.table = storedCtx->tableName;
						CalpontSystemCatalog::IndexNameList indexNameList = csc->indexNames( tableName );
						std::vector<std::string> columnNames;
						CalpontSystemCatalog::TableColName tableColName;
						if ( indexNameList.size() > 0 )
						{
							CalpontSystemCatalog::IndexNameList::const_iterator indexName_iter = indexNameList.begin();
							while ( indexName_iter != indexNameList.end() )
							{
								CalpontSystemCatalog::IndexName indexName = *indexName_iter;
								CalpontSystemCatalog::TableColNameList tableColumns = csc->indexColNames( indexName );
								CalpontSystemCatalog::TableColNameList::const_iterator colNames_iter = tableColumns.begin();
								while ( colNames_iter != tableColumns.end() )
								{
									tableColName = *colNames_iter;
									CalpontSystemCatalog::OID oid = csc->lookupOID( tableColName );

									CalpontSystemCatalog::ColType colType;
									colType = csc->colType( oid );

									 if (((colType.colDataType == execplan::CalpontSystemCatalog::CHAR) &&
												(colType.colWidth > 8))
										|| ((colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR) &&
												(colType.colWidth > 7))
										|| ((colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL) &&
												(colType.precision > 18)))
                                        || ((colType.colDataType == execplan::CalpontSystemCatalog::UDECIMAL) &&
                                                (colType.precision > 18)))
									{
										columnNames.push_back( tableColName.column );
									}
									++colNames_iter;
								}

								++indexName_iter;
							}
						}
						if ( columnNames.size() > 0 )
						{
							for( unsigned int j=0; j < columnNames.size(); j++ )
							{
								selectStmt += columnNames[j];
								if ( j < columnNames.size()-1)
								{
									selectStmt += ", ";
								}
								else
								{
									selectStmt += " ";
								}
							}
						}
						else
						{

							CalpontSystemCatalog::RIDList ridList = csc->columnRIDs( tableName );
							//use the first column to fetch row IDs
							CalpontSystemCatalog::TableColName tableColName = csc->colName( ridList[0].objnum );
							colName = tableColName.column;

							selectStmt += colName;
							selectStmt += " ";
						}
						selectStmt += sqltext.substr(blankspace+1, sqltext.length()-blankspace) ;

					}
					else //No where clause
					{
						ByteStream bytestream;
						bytestream << sessionID;
						pDMLPackage->write(bytestream);
						delete pDMLPackage;
						MessageQueueClient mq("DMLProc");
						ByteStream::byte b;
						ByteStream::octbyte rows;
						std::string errorMsg;
						try
						{
							mq.write(bytestream);
							bytestream = mq.read();
							bytestream >> b;
							bytestream >> rows;
							bytestream >> errorMsg;
							if ( b != 0 )
							{
								dmlRowCount = 0;
								errHandle(storedCtx, handles);
								sprintf(errbuf, "%s", errorMsg.c_str());
								OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
								return ODCI_ERROR;
							}
							dmlRowCount = rows;
							storedCtx->tableDone = false;

							storedCtx->rowType = 0;
							storedCtx->anyDS = 0;
							if (checkerr(&handles, OCITypeByName(handles.envhp, handles.errhp, handles.svchp,
								(text*)ownerName, strlen(ownerName), (text*)rowTypeName, strlen(rowTypeName), (text*)0, 0,
								OCI_DURATION_STATEMENT, OCI_TYPEGET_ALL, &storedCtx->rowType), "OCITypeByName"))
								return ODCI_ERROR;

							/* generate a key */
							if (checkerr(&handles, OCIContextGenerateKey((dvoid*) handles.usrhp, handles.errhp, &key),
								"OCIContextGenerateKey"))
								return ODCI_ERROR;

							/* associate the key value with the stored context address */
							if (checkerr(&handles, OCIContextSetValue((dvoid*)handles.usrhp, handles.errhp,
								OCI_DURATION_STATEMENT, (ub1*)&key, (ub1)sizeof(key), (dvoid*)storedCtx), "OCIContextSetValue"))
								return ODCI_ERROR;

							/* stored the key in the scan context */
							if (checkerr(&handles, OCIRawAssignBytes(handles.envhp, handles.errhp, (ub1*)&key, (ub4)sizeof(key),
								&(self->key)), "OCIRawAssignBytes"))
								return ODCI_ERROR;

							/* set indicators of the scan context */
							self_ind->_atomic = OCI_IND_NOTNULL;
							self_ind->key = OCI_IND_NOTNULL;

						}
						catch (runtime_error& rex)
						{
							errHandle(storedCtx, handles);
							return ODCI_ERROR;
						}
						return ODCI_SUCCESS;
					}

				}
				else if ( strcasecmp(sqlCommand.c_str(), "update") == 0)
				{
					std::string updateStmt;

					updateStmt = sqltext.substr( blankspace, sqltext.length() - blankspace );
					int dmlFlag = 1;
					updateStmt = aliasReplace( updateStmt, dmlFlag );
					updateSql = sqlCommand + " " + updateStmt;
					updateStmt = updateSql + ";";

					VendorDMLStatement dmlStmt(updateStmt, sessionID);
					//To Do: add real schema from the qualified table name.
					pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt, schemaName);
					DMLTable *pTable = pDMLPackage->get_Table();
					RowList rowlist = pTable->get_RowList();
					Row* firstRow = rowlist.at(0);
					//bool firstCol = true;
					if(firstRow != NULL)
					{
						//select all columns back to use for update index.
						selectStmt += "* FROM " + pTable->get_SchemaName() + "." + pTable->get_TableName() + " ";
						//string wherePart = pDMLPackage->get_QueryString();
						string wherePart = updateSql;
						boost::algorithm::to_lower(wherePart);
						string::size_type pos = wherePart.find ( "where" );
						if ( pos < string::npos )
						{
							selectStmt += updateSql.substr(pos, updateSql.length()-pos);
						}

				  }
				}
				else //insert
				{
					insertText = sqltext;
					boost::algorithm::to_lower( sqltext );
					isInsert= true;
					string::size_type wherePlace = sqltext.find("select");

					if ( wherePlace < string::npos )
					{
						if ( sqltext.find("where") < string::npos )
						{
							selectStmt = sqltext.substr( wherePlace, sqltext.length()-wherePlace-1);
						}
						else
						{
							selectStmt = sqltext.substr( wherePlace, sqltext.length()-wherePlace -1);
						}
					}
					string::size_type end = selectStmt.find( ")");
					if ( end < string::npos )
					{

					}
					else if ( (end = selectStmt.find( ";")) < string::npos )
					{
					}
					else
					{
						end = selectStmt.length();
					}
					if ( end != selectStmt.length())
					{
						selectStmt = selectStmt.substr( 0, end);
					}
					//Find coltype info
					string::size_type endPlace = sqltext.find("from");
					std::string selectStr;
					if ( endPlace < string::npos )
					{
						selectStr = sqltext.substr(wherePlace, endPlace - wherePlace);
					}
					else
					{
						selectStr = "";
					}
					/*Look up system catalog for all column names belong to this table to handle
					  constant column */
					CalpontSystemCatalog::TableName tableName;
					tableName.schema =  storedCtx->schemaName;
					tableName.table = storedCtx->tableName;
					CalpontSystemCatalog::RIDList ridList = csc->columnRIDs( tableName );
					CalpontSystemCatalog::RIDList::const_iterator rid_iter = ridList.begin();
					CalpontSystemCatalog::ROPair roPair;
					CalpontSystemCatalog::TableColName tableColName;
					std::vector<std::string> colNames;
					while ( rid_iter != ridList.end() )
					{
						roPair = *rid_iter;
						tableColName = csc->colName( roPair.objnum );
						colNames.push_back( tableColName.column );
						++rid_iter;
					}
					typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
					boost::char_separator<char> sep( "<; ;,;");
					tokenizer tokens(selectStr, sep);
					std::vector<std::string> dataList;
					for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
					{
						dataList.push_back(*tok_iter);
					}
					string colName;
					vector<std::string>::const_iterator p1 ;
					vector<std::string>::const_iterator colname_iter;
					//CalpontSystemCatalog::TableColName tableColName;
					tableColName.schema = storedCtx->schemaName;
					tableColName.table = storedCtx->tableName;
					CalpontSystemCatalog::OID oid;
					CalpontSystemCatalog::ColType colType;
					insertColInfo colInfo;
					if ( dataList.size() > 0 && strcasecmp(dataList[0].c_str(), "SELECT") == 0 )
					{
						if ( dataList.size() > 1)
						{
							insertCols.clear();
							if ( strcasecmp(dataList[1].c_str(), "*") != 0 )
							{
								selectStr = selectStr.substr( dataList[0].length()+1, selectStr.length()-dataList[0].length() );

								boost::char_separator<char> sepl( "<;,;");
								tokenizer tokensl(selectStr, sepl);
								dataList.clear();
								for (tokenizer::iterator tok_iter = tokensl.begin(); tok_iter != tokensl.end(); ++tok_iter)
								{
									dataList.push_back(*tok_iter);
								}
								string::size_type dotPlace;
								p1 = dataList.begin();

								int pos = 0;

								for (; p1 != dataList.end(); ++p1 )
								{

									if (( (*p1).substr(0,1) == "'" ) || ((*p1).substr(0,1) == "\"" ))
									{
										colName = *p1;
									}
									else
									{
										dotPlace = (*p1).find(".");
										if ( dotPlace < string::npos )
										{
											colName = (*p1).substr( dotPlace+1, (*p1).length()-dotPlace );
										}
										else
										{
											colName = *p1;
										}
									}
									/* Check whether the colname belong to this table*/
									boost::algorithm::to_lower(colName);
									//strip off leading space
									string::size_type lead=colName.find_first_not_of(" ");
									colName = colName.substr( lead, colName.length()-lead);
									//strip off ending space
									if ( colName.substr(0,1) != "\'" && colName.substr(0,1) != "\"")
									{
										lead = colName.find_first_of(" ");
										colName = colName.substr( 0, lead);
									}
									colname_iter = colNames.begin();
									while ( colname_iter != colNames.end() )
									{
										std::string col;
										col = *colname_iter;
										if ( col.compare(colName) == 0 )
											break;
										++colname_iter;
									}
									if ( colname_iter == colNames.end())
									{
										/*Constant column. */
										colInfo.colPosition = pos ;
										colInfo.constFlag = true;
										colInfo.value = colName;
										strcpy( colInfo.schemaName, schemaName );
										insertCols.push_back( colInfo );

									}
									else
									{
										tableColName.column = colName;
										oid = csc->lookupOID( tableColName );
										colType = csc->colType( oid );
										colInfo.colPosition = colType.colPosition;
										colInfo.constFlag = false;
										strcpy( colInfo.schemaName, schemaName );
										insertCols.push_back( colInfo );
									}
									++pos;
								}
							}
						}
					}
				}

				if ( !isInsert )
				{
					execplan::CalpontSelectExecutionPlan *cp = pDMLPackage->get_ExecutionPlan();
					cp->sessionID(sessionID);
					cp->txnID(txnID.id);
					cp->verID(verID);
					envDMLInit("calpont", "calpont", "CALFEDBMS", NULL);
					getDMLPlan(extProcCtx, sessionID, selectStmt.c_str(), curSchemaName, *cp);

					ByteStream bytestream;
					bytestream << sessionID;
					pDMLPackage->write(bytestream);
					delete pDMLPackage;
					MessageQueueClient mq("DMLProc");
					ByteStream::byte b;
					ByteStream::octbyte rows;
					std::string errorMsg;
					try
					{
						mq.write(bytestream);
						bytestream = mq.read();
						bytestream >> b;
						bytestream >> rows;
						bytestream >> errorMsg;
						storedCtx->tableDone = false;

						if ( b != 0 )
						{
							dmlRowCount = 0;
							errHandle(storedCtx, handles);
							sprintf(errbuf, "%s", errorMsg.c_str());
							OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
							return ODCI_ERROR;
						}
						dmlRowCount = rows;
						storedCtx->rowType = 0;
						storedCtx->anyDS = 0;
						if (checkerr(&handles, OCITypeByName(handles.envhp, handles.errhp, handles.svchp, (text*)ownerName,
							strlen(ownerName), (text*)rowTypeName, strlen(rowTypeName), (text*)0, 0, OCI_DURATION_STATEMENT,
							OCI_TYPEGET_ALL, &storedCtx->rowType), "OCITypeByName"))
						{
							errHandle(storedCtx, handles);
							return ODCI_ERROR;
						}

						/* generate a key */
						if (checkerr(&handles, OCIContextGenerateKey((dvoid*) handles.usrhp, handles.errhp, &key),
							"OCIContextGenerateKey"))
						{
							errHandle(storedCtx, handles);
							return ODCI_ERROR;
						}

						/* associate the key value with the stored context address */
						if (checkerr(&handles, OCIContextSetValue((dvoid*)handles.usrhp, handles.errhp,
							OCI_DURATION_STATEMENT, (ub1*)&key, (ub1)sizeof(key), (dvoid*)storedCtx), "OCIContextSetValue"))
						{
							errHandle(storedCtx, handles);
							return ODCI_ERROR;
						}

						/* stored the key in the scan context */
						if (checkerr(&handles, OCIRawAssignBytes(handles.envhp, handles.errhp, (ub1*)&key, (ub4)sizeof(key),
							&(self->key)), "OCIRawAssignBytes"))
						{
							errHandle(storedCtx, handles);
							return ODCI_ERROR;
						}

						/* set indicators of the scan context */
						self_ind->_atomic = OCI_IND_NOTNULL;
						self_ind->key = OCI_IND_NOTNULL;

					}
					catch (runtime_error& rex)
					{
						errHandle(storedCtx, handles);
						return ODCI_ERROR;
					}

					return ODCI_SUCCESS;
				}
				else
				{

					envDMLInit("calpont", "calpont", "CALFEDBMS", NULL);
					getDMLPlan(extProcCtx, sessionID, selectStmt.c_str(), curSchemaName, plan);
					delete pDMLPackage;
					isDML = false;
					plan.serialize(msg);
					cal_conn_hndl = static_cast<cpsm_conhdl_t*> (storedCtx->conn_hndl);
				}
			}
			else
			{
				isDML = false;
				isInsert= false;
				//setupSQL = true;
				//send serialized calpont plan to exeManager to generate joblist
				plan.statementID(statementID);
				statementID++;
				getPlan(extProcCtx, sessionID, curSchemaName, plan, bindValList);

				plan.traceFlags(traceFlags);
				plan.rmParms(rmParms);
				plan.serialize(msg);
				rmParms.clear();

				//Just write a seralized plan & stop
				if (traceFlags & CalpontSelectExecutionPlan::TRACE_PLAN_ONLY)
				{
					ostringstream oss;
					oss << planfile << sessionID;
					ofstream of(oss.str().c_str());
					of << msg;
					string msg("SQL plan file in " + oss.str());
					OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)msg.c_str(), msg.length());

					return ODCI_SUCCESS;//return ODCI_ERROR;
				}

				// Optimization. to populate the local oidmap. so later on only
				// required columns will be called system catalog
				const CalpontSelectExecutionPlan::ColumnMap colMap = plan.columnMap();
				CalpontSelectExecutionPlan::ColumnMap::const_iterator colIter = colMap.begin();
				while (colIter != colMap.end())
				{
					SRCP srcp = colIter->second;
					SimpleColumn* scp = dynamic_cast<SimpleColumn*>(srcp.get());
					rcos.insert(scp->oid());
					++colIter;
				}
			}
			cal_conn_hndl->connect();
			cal_conn_hndl->exeMgr->write(msg);
		}

		// @bug 649. move key generation here before calling dhcs_tpl_open
		storedCtx->tableDone = false;
	
		storedCtx->rowType = 0;
		storedCtx->anyDS = 0;
		if (checkerr(&handles, OCITypeByName(handles.envhp, handles.errhp, handles.svchp, (text*)ownerName, strlen(ownerName),
			(text*)rowTypeName, strlen(rowTypeName), (text*)0, 0, OCI_DURATION_STATEMENT, OCI_TYPEGET_ALL,
			&storedCtx->rowType), "OCITypeByName"))
			return ODCI_ERROR;
	
		/* generate a key */
		if (checkerr(&handles, OCIContextGenerateKey((dvoid*) handles.usrhp, handles.errhp, &key), "OCIContextGenerateKey"))
			return ODCI_ERROR;
	
		/* associate the key value with the stored context address */
		if (checkerr(&handles, OCIContextSetValue((dvoid*)handles.usrhp, handles.errhp, OCI_DURATION_STATEMENT, (ub1*)&key,
			(ub1)sizeof(key), (dvoid*)storedCtx), "OCIContextSetValue"))
			return ODCI_ERROR;
	
		/* stored the key in the scan context */
		if (checkerr(&handles, OCIRawAssignBytes(handles.envhp, handles.errhp, (ub1*)&key, (ub4)sizeof(key),
			&(self->key)), "OCIRawAssignBytes"))
			return ODCI_ERROR;
	
		/* set indicators of the scan context */
		self_ind->_atomic = OCI_IND_NOTNULL;
		self_ind->key = OCI_IND_NOTNULL;
	
		// @bug 649. Initialize table handle and scan handle here to carry key to sm
		//cpsm_tplh_t* ntplh = new cpsm_tplh_t();
		ntplh->key = key;
		storedCtx->tplhdl = ntplh;
		//cpsm_tplsch_t* ntplsch = new cpsm_tplsch_t();
		ntplsch->key = key;
		storedCtx->tpl_scanhdl = ntplsch;
		
		dhcs_status_t rc;
		cal_conn_hndl->curFetchTb = key;

		if (SWALLOW_ROWS)
		{
			if (cal_conn_hndl->queryState != QUERY_IN_PROCESS || !DEBUG_DUMP_ROWS)
			{
				try {
					ByteStream bs;
					bs << (ByteStream::quadbyte)1;
					cal_conn_hndl->exeMgr->write(bs);
					bs.reset();
					bs << (ByteStream::quadbyte)2;
					cal_conn_hndl->exeMgr->write(bs);
				} catch (...)
				{
					sigErrHandle(storedCtx);
					return ODCI_ERROR;
				}
			}
			cal_conn_hndl->queryState = QUERY_IN_PROCESS;
			return ODCI_SUCCESS;
		}

		if ((rc=dhcs_tpl_open(tblid, &storedCtx->tplhdl, storedCtx->conn_hndl, rcos)) != STATUS_OK)
		{
			errHandle(storedCtx, handles);
			sprintf(errbuf, "Internal error occured in dhcs_tpl_open(%d): %d", tblid, rc);
			OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
			return ODCI_ERROR;
		}
		if ((rc=dhcs_tpl_scan_open(tblid, DHCS_TPL_FH_READ, &storedCtx->tpl_scanhdl,
				storedCtx->conn_hndl)) != STATUS_OK)
		{
			errHandle(storedCtx, handles);
			sprintf(errbuf, "Internal error occured in dhcs_tpl_scan_open(%d):  %d", tblid, rc);
			OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
			return ODCI_ERROR;
		}
	}
	catch (runtime_error& re) {
		errHandle(storedCtx, handles);
		sprintf(errbuf, "Exception caught in ODCITableStart: %s", re.what());
		OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
		return ODCI_ERROR;
	}
	catch (...) {
		errHandle(storedCtx, handles);
		strcpy(errbuf, "Unknown execption caught in ODCITableStart");
		OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
		return ODCI_ERROR;
	}

	return ODCI_SUCCESS;
}
/***********************************************************************/

/* Callout for ODCITableDescribe */

extern "C" EXECPROC_API int ODCITableDescribe(OCIExtProcContext* extProcCtx,
	OCIType** rtype, short* rtype_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind)
{
	Handles_t handles;
	sword status;
	int rc = ODCI_ERROR;
#ifdef TBLDEBUG_ALL
	ub4 sessionID = 0;
	FILE* p;
	char buf[10240]= {0};
#endif

	*rtype_ind = OCI_IND_NULL;
	*rtype = 0;

	if (schemaName_ind == OCI_IND_NULL ||
		tableName_ind == OCI_IND_NULL ||
		ownerName_ind == OCI_IND_NULL ||
		tblTypeName_ind == OCI_IND_NULL ||
		rowTypeName_ind == OCI_IND_NULL)
		goto out;

	/* Get OCI handles */
	if (GetHandles(extProcCtx, &handles))
		goto out;

#ifdef TBLDEBUG_ALL
	p=fopen(f1file, "a");

	sprintf(buf, "This is ODCITableDescribe(%u, '%s', '%s', '%s', '%s', '%s', '%s')\n", sessionID, schemaName,
		tableName, ownerName, tblTypeName, rowTypeName, curSchemaName);
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

	status = OCITypeByName(handles.envhp, handles.errhp, handles.svchp, (text*)ownerName, strlen(ownerName),
		(text*)tblTypeName, strlen(tblTypeName), (text*)0, 0, OCI_DURATION_STATEMENT, OCI_TYPEGET_ALL, rtype);
	if (checkerr(&handles, status, "OCITypeByName")) goto out;

	*rtype_ind = OCI_IND_NOTNULL;
	rc = ODCI_SUCCESS;

out:
#ifdef TBLDEBUG_ALL
	strcpy(buf, "This is ODCITableDescribe exit\n");
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);

	if (p) fclose(p);
#endif

	return rc;
}
/***********************************************************************/

namespace
{
#if 0
struct DumpFunctor
{
typedef vector<joblist::TableBand> TBV;

DumpFunctor() : fBandNum(1) { }

void operator()()
{
	ofstream os;
	boost::scoped_array<char> buf(new char[1024 * 1024]);
	os.rdbuf()->pubsetbuf(buf.get(), 1024 * 1024);
	ostringstream oss1;
	oss1 << fPrefix << '.' << fBandNum << ".tbl";
	os.open(oss1.str().c_str(), ios_base::out);
	TBV::const_iterator iter = fTb.begin();
	TBV::const_iterator end = fTb.end();
	while (iter != end)
	{
		iter->formatToCSV(os, '|');
		++iter;
	}
	os.close();
}

TBV fTb;
string fPrefix;
unsigned fBandNum;
};
#endif
struct DumpFunctor
{
typedef boost::shared_ptr<ofstream> SPOfStr;

void operator()()
{
	fTb.formatToCSV(*fOsp, '|');
}

joblist::TableBand fTb;
SPOfStr fOsp;

};
}

/* Callout for ODCITableFetch */

extern "C" EXECPROC_API int ODCITableFetch(OCIExtProcContext* extProcCtx, CalpontImpl* self,
	CalpontImpl_ind* self_ind, OCINumber* nrows,
	OCIAnyDataSet** outSet, short* outSet_ind)
{
	Handles_t handles;                   /* OCI hanldes */
	StoredCtx* storedCtx;                /* Stored context pointer */
	int nrowsval;                        /* number of rows to return */
	OCIAnyData* anyData;
	OCINumber numberType_n;
	OCIDate dateType_d;
	OCIInd ind1;
	OCIAnyDataSet* anyDataSet;

#ifdef TBLDEBUG
	FILE* p;
	char buf[10240]= {0};
#endif
	float floatType;
	double doubleType;

	/* Get OCI handles */
	if (GetHandles(extProcCtx, &handles))
		return ODCI_ERROR;

	/* Get the stored context */
	storedCtx=GetStoredCtx(&handles,self,self_ind);
	if (!storedCtx) return ODCI_ERROR;

	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(storedCtx->sessionID);
	csc->sessionID(storedCtx->sessionID);
	csc->identity(CalpontSystemCatalog::FE);

	//If update statement, do not fetch
	if ( isDML )
	{
		storedCtx->tableDone = true;
	}
	/* get value of nrows */
	if (checkerr(&handles, OCINumberToInt(handles.errhp, nrows, sizeof(nrowsval), OCI_NUMBER_SIGNED,
		(dvoid *)&nrowsval), "OCINumberToInt"))
		return ODCI_ERROR;

#ifdef TBLDEBUG
	p = fopen(f1file, "a");

	sprintf(buf, "This is ODCITableFetch(%d)\n", nrowsval);
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

	*outSet_ind=OCI_IND_NULL;
	*outSet = 0;

#ifdef TBLDEBUG_ALL
	strcpy(buf, "This is ODCITableFetch 1\n");
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);

	sprintf(buf, "This is ODCITableFetch 1.1: rowType = %p\n", storedCtx->rowType);
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);

	if (p) describe_row(&handles, storedCtx->rowType, p);
#endif

	if (storedCtx->anyDS)
		checkerr(&handles, OCIAnyDataSetDestroy(handles.svchp, handles.errhp, storedCtx->anyDS));

	storedCtx->anyDS = 0;
	anyDataSet = 0;
	if (checkerr(&handles, OCIAnyDataSetBeginCreate(handles.svchp, handles.errhp, OCI_TYPECODE_OBJECT,
		storedCtx->rowType, ociDuration, &anyDataSet), "OCIAnyDataSetBeginCreate"))
		return ODCI_ERROR;

	if (DEBUG_DUMP_ROWS)
	{
		boost::scoped_array<char> buf1(new char[1024 * 1024]);
		boost::scoped_array<char> buf2(new char[1024 * 1024]);
		boost::scoped_array<char> buf3(new char[1024 * 1024]);
		boost::scoped_array<char> buf4(new char[1024 * 1024]);
		std::vector<DumpFunctor::SPOfStr> outStreams;
		DumpFunctor::SPOfStr os1(new ofstream);
		outStreams.push_back(os1);
		DumpFunctor::SPOfStr os2(new ofstream);
		outStreams.push_back(os2);
		DumpFunctor::SPOfStr os3(new ofstream);
		outStreams.push_back(os3);
		DumpFunctor::SPOfStr os4(new ofstream);
		outStreams.push_back(os4);
		typedef boost::shared_ptr<ThreadPool> SPTP;
		std::vector<SPTP> writeThreads;
		SPTP tp1(new ThreadPool(1, 10));
		writeThreads.push_back(tp1);
		SPTP tp2(new ThreadPool(1, 10));
		writeThreads.push_back(tp2);
		SPTP tp3(new ThreadPool(1, 10));
		writeThreads.push_back(tp3);
		SPTP tp4(new ThreadPool(1, 10));
		writeThreads.push_back(tp4);
		os1->setstate(ios_base::badbit);
		os2->setstate(ios_base::badbit);
		os3->setstate(ios_base::badbit);
		os4->setstate(ios_base::badbit);
		string val = config::Config::makeConfig()->getConfig("OracleConnector", "DumpPath");
		string dumpPath;
		if (val.length() > 0)
			dumpPath = val;
		else
			dumpPath = "/var/log/Calpont/trace";
		ByteStream bs;
		ByteStream::byte endFlag = 0;
		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(storedCtx->conn_hndl);
		unsigned bandNum = 0;
		if (alreadyDumped) goto dump_out;
		alreadyDumped = true;
		for (;;)
		{
			bandNum = 0;
			unsigned tidx = 0;
			for (;;)
			{
				joblist::TableBand tb;
				bs = hndl->exeMgr->read();
				if (bs.length() == 1)
				{
					bs >> endFlag;
				}
				if (endFlag == 1) break;
				tb.unserialize(bs);
				tidx = bandNum % 4;
				if (!outStreams[0]->good())
				{
					ostringstream oss1;
					//TODO: need a reliable way to get schema.table from table oid
					//CalpontSystemCatalog::TableColName tcn = csc->colName(tb.tableOID() + 1);
					CalpontSystemCatalog::TableColName tcn = csc->colName(tb.getColumns()[0]->getColumnOID());
					oss1 << dumpPath << '/' << tcn.schema << '.' << tcn.table << '.' <<
						storedCtx->sessionID << '.';
					string pfx = oss1.str();
					//outStreams[tidx]->rdbuf()->pubsetbuf(buf.get(), 1024 * 1024);
					for (unsigned ix = 1; ix <= 4; ix++)
					{
						ostringstream oss2;
						oss2 << pfx << ix << ".tbl";
						outStreams[ix - 1]->open(oss2.str().c_str());
					}
					outStreams[0]->rdbuf()->pubsetbuf(buf1.get(), 1024 * 1024);
					outStreams[1]->rdbuf()->pubsetbuf(buf2.get(), 1024 * 1024);
					outStreams[2]->rdbuf()->pubsetbuf(buf3.get(), 1024 * 1024);
					outStreams[3]->rdbuf()->pubsetbuf(buf4.get(), 1024 * 1024);
				}
				if ((traceFlags & (512 << 1)) == 0)
				{
					DumpFunctor dp;
					dp.fTb = tb;
					dp.fOsp = outStreams[tidx];
					writeThreads[tidx]->invoke(dp);
				}
				if (tb.getRowCount() == 0)
				{
					break;
				}
				bandNum++;
#if 0
				if (!debugStream.good())
				{
					ostringstream oss1;
					//TODO: need a reliable way to get schema.table from table oid
					//CalpontSystemCatalog::TableColName tcn = csc->colName(tb.tableOID() + 1);
					CalpontSystemCatalog::TableColName tcn = csc->colName(tb.getColumns()[0]->getColumnOID());
					oss1 << dumpPath << '/' << tcn.schema << '.' << tcn.table << '.' <<
						storedCtx->sessionID << ".tbl";
					debugStream.rdbuf()->pubsetbuf(buf.get(), 1024 * 1024);
					debugStream.open(oss1.str().c_str(), ios_base::out|ios_base::ate);
				}
				if ((traceFlags & (512 << 1)) == 0)
					tb.formatToCSV(debugStream, '|');
#endif
#if 0
				if (tb.getRowCount() == 0)
				{
					if (!dp.fTb.empty())
					{
						if ((traceFlags & (512 << 1)) == 0)
							tp1.invoke(dp);
					}
					break;
				}
				if (dp.fPrefix.length() == 0)
				{
					ostringstream oss1;
					CalpontSystemCatalog::TableColName tcn = csc->colName(tb.getColumns()[0]->getColumnOID());
					oss1 << dumpPath << '/' << tcn.schema << '.' << tcn.table << '.' <<
						storedCtx->sessionID;
					dp.fPrefix = oss1.str();
				}
				dp.fTb.push_back(tb);
				if (dp.fTb.size() >= 32)
				{
					if ((traceFlags & (512 << 1)) == 0)
						tp1.invoke(dp);
					dp.fTb.clear();
					dp.fBandNum++;
				}
#endif
			}
#if 0
			debugStream.close();
			debugStream.setstate(ios_base::badbit);
#endif
			tp1->wait();
			tp2->wait();
			tp3->wait();
			tp4->wait();
			os1->close();
			os1->setstate(ios_base::badbit);
			os2->close();
			os2->setstate(ios_base::badbit);
			os3->close();
			os3->setstate(ios_base::badbit);
			os4->close();
			os4->setstate(ios_base::badbit);

			if (endFlag == 1) break;
			bs.reset();
			bs << (ByteStream::quadbyte)2;
			try {
				hndl->exeMgr->write(bs);
			}catch (...)
			{
				sigErrHandle(storedCtx);
				return ODCI_ERROR;
			}
		}
		//tp1.wait();
dump_out:
		OCIAnyDataSetEndCreate(handles.svchp, handles.errhp, anyDataSet);
		*outSet_ind=OCI_IND_NOTNULL;
		*outSet = anyDataSet;
		storedCtx->anyDS = anyDataSet;
		cal_conn_hndl->curFetchTb = 0;
		return ODCI_SUCCESS;
	}

	if (!storedCtx->tableDone)
	{

#ifdef TBLDEBUG_ALL
		sprintf(buf, "This is ODCITableFetch 2: anyDataSet = %p\n", anyDataSet);
		if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

		dvoid* list_attr;
		ub2 num_attr;
		OCIParam* parmp;
		OCIDescribe* dschp;

		dschp = 0;
		if (checkerr(&handles, OCIHandleAlloc(handles.envhp, (dvoid**)&dschp, OCI_HTYPE_DESCRIBE, 0, 0), "OCIHandleAlloc"))
			return ODCI_ERROR;

		if (checkerr(&handles, OCIDescribeAny(handles.svchp, handles.errhp, storedCtx->rowType, 0, OCI_OTYPE_PTR,
			OCI_DEFAULT, OCI_PTYPE_TYPE, dschp), "OCIDescrbeAny"))
			return ODCI_ERROR;

		parmp = 0;
		if (checkerr(&handles, OCIAttrGet(dschp, OCI_HTYPE_DESCRIBE, &parmp, 0, OCI_ATTR_PARAM, handles.errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		list_attr = 0;
		if (checkerr(&handles, OCIAttrGet(parmp, OCI_DTYPE_PARAM, &list_attr, 0, OCI_ATTR_LIST_TYPE_ATTRS, handles.errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		num_attr = 0;
		if (checkerr(&handles, OCIAttrGet(parmp, OCI_DTYPE_PARAM, &num_attr, 0, OCI_ATTR_NUM_TYPE_ATTRS, handles.errhp),
			"OCIAttrGet"))
			return ODCI_ERROR;

		dhcs_fv_item_t fitems[num_attr];
		dhcs_data_t fditems[num_attr];
		ub2 colTypes[num_attr];
		CalpontSystemCatalog::ColType ct[num_attr];
		vector<string> colNames;

		for (ub2 i = 0; i < num_attr; i++)
		{
			dvoid* parmdp;
			ub2 len;
			ub2 colType;
			ub2 alen;
			text* name;
			ub4 namelen;

			parmdp = 0;
			if (checkerr(&handles, OCIParamGet(list_attr, OCI_DTYPE_PARAM, handles.errhp, &parmdp, i + 1), "OCIParamGet"))
				return ODCI_ERROR;

			len = 0;
			if (checkerr(&handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &len, 0, OCI_ATTR_DATA_SIZE, handles.errhp),
				"OCIAttrGet"))
				return ODCI_ERROR;
			alen = 80;
			if (len > alen) alen = len;

			colType = 0;
			if (checkerr(&handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &colType, 0, OCI_ATTR_DATA_TYPE, handles.errhp),
				"OCIAttrGet"))
				return ODCI_ERROR;

			name = 0;
			if (checkerr(&handles, OCIAttrGet(parmdp, OCI_DTYPE_PARAM, &name, &namelen, OCI_ATTR_NAME, handles.errhp),
				"OCIAttrGet"))
				return ODCI_ERROR;
			colNames.push_back(string((const char*)name, namelen));

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
			colTypes[i] = colType;
		}

		dhcs_fldl_val_t flds;
		flds.fv_nitems = num_attr;
		flds.fv_item = fitems;
		dhcs_status_t sm_stat;

		nrowsval = storedCtx->rowsReturned;

		if (SWALLOW_ROWS)
			nrowsval = INT_MAX;

		string newDMLStatement;
		string::size_type selectPos;

		cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(storedCtx->conn_hndl);

		memset(&ct[0], 0, sizeof(CalpontSystemCatalog::ColType) * num_attr);
		hndl->csc = csc;
		cpsm_tplsch_t* ntplsch = static_cast<cpsm_tplsch_t*>(storedCtx->tpl_scanhdl);
		//This is needed by tpl_scan_fetch to enable it to be thread-safe
		ntplsch->ctp = ct;
		ntplsch->traceFlags = traceFlags;

		OCIString* str = 0;

		for (int k = 0; k < nrowsval; k++)
		{
			//probably can't get here...
			if (storedCtx->tableDone) break;
				
			// @bug 770. do not fetch anything if SWALLOW_ROWS on and this is not the
			// first table to be fetched.
			if (SWALLOW_ROWS && cal_conn_hndl->tidScanMap.size() >= 1)
				break;

			// must have dhcs_convfn_ptr point to an actual fcn to call tpl_scan_fetch with anything other
			//  than DHCS_CHAR output :-(
			sm_stat = dhcs_tpl_scan_fetch(storedCtx->tpl_scanhdl, &flds, 0, storedCtx->conn_hndl);

			if (sm_stat != STATUS_OK)
			{ 
				storedCtx->tableDone = true;

				if (SQL_NOT_FOUND != sm_stat)
				{
					hndl->queryState = NO_QUERY;
					hndl->curFetchTb = 0;
					hndl->tidMap.clear();
					hndl->tidScanMap.clear();
					hndl->keyBandMap.clear();
					//errHandle(storedCtx, handles);
					string errorString(errorCodes.errorString(sm_stat));
					OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, (OraText*)errorString.c_str(), errorString.length());
					return ODCI_ERROR;
					//break;//return sm_stat;
				}
				// @bug 626 set bandsintable if saveFlag = SAVING

				if (SWALLOW_ROWS)
				{
					ByteStream bs;
					ByteStream::byte endFlag = 0;
					for (;;)
					{
						bs.reset();
						bs << (ByteStream::quadbyte)2;
						try {
							hndl->write(bs);
						} catch (...)
						{
							sigErrHandle(storedCtx);
							return ODCI_ERROR;
						}
				
						for (;;)
						{
							joblist::TableBand tb;
							try {
								bs = hndl->exeMgr->read();
							} catch (...)
							{
								sigErrHandle(storedCtx);
								return ODCI_ERROR;
							}
							if (bs.length() == 0)
							{
								// check: if sigpipe is the only cause
								sigErrHandle(storedCtx);
								return ODCI_ERROR;
							}
							if (bs.length() == 1)
								bs >> endFlag;
							if (endFlag == 1) break;
							tb.unserialize(bs);
							if (tb.getRowCount() == 0) break;
						}
						if (endFlag == 1) break;
						//bs.reset();
						//bs << (ByteStream::quadbyte)2;
						//hndl->exeMgr->write(bs);
					}
				}

				break;
			}

#ifdef TBLDEBUG1
{
buf[0] = 0;
strcat(buf, "|");
for (int x = 0; x < flds.fv_nitems; x++)
{
	if (flds.fv_item[x].fv_data->dt_is_data_null == 0)
		strcat(buf, (const char*)flds.fv_item[x].fv_data->dt_data);
	else
		strcat(buf, "NULL");
	strcat(buf, "|");
}
strcat(buf, "\n");
if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
}

#endif

			anyData = 0;
			if (!SWALLOW_ROWS)
			{
				if (checkerr(&handles, OCIAnyDataSetAddInstance(handles.svchp, handles.errhp, anyDataSet, &anyData),
					"OCIAnyDataSetAddInstance"))
					return ODCI_ERROR;

#ifdef TBLDEBUG_ALL
				strcpy(buf, "This is ODCITableFetch 2.1\n");
				if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

				if (checkerr(&handles, OCIAnyDataBeginCreate(handles.svchp, handles.errhp, OCI_TYPECODE_OBJECT,
					storedCtx->rowType, ociDuration, &anyData), "OCIAnyDataBeginCreate"))
					return ODCI_ERROR;
			} //SWALLOW_ROWS

#ifdef TBLDEBUG_ALL
			sprintf(buf, "This is ODCITableFetch 3 anyData = %p\n", anyData);
			if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

			for (ub2 i = 0; i < num_attr; i++)
			{
/*
from the Oracle docs, here are the possible column datatypes:
Column Type                   Max Len Val SQLT define        OCI define
----------------------------- ------- --- ------------------ ----------
VARCHAR                                 1 SQLT_CHR           OCI_TYPECODE_VARCHAR
NUMBER                             21   2 SQLT_NUM           OCI_TYPECODE_NUMBER
LONG                              2GB   8 SQLT_LNG
VARCHAR2,NVARCHAR2               4000   9 SQLT_VCS           OCI_TYPECODE_VARCHAR2
DATE                                7  12 SQLT_DAT           OCI_TYPECODE_DATE
RAW                              2000  23 SQLT_BIN           OCI_TYPECODE_UNSIGNED8
LONG RAW                          2GB  24 SQLT_LBI
ROWID                              10
CHAR,NCHAR                       2000  96 SQLT_AFC           OCI_TYPECODE_CHAR
BINARY_FLOAT                        4 100 SQLT_IBFLOAT       OCI_TYPECODE_BFLOAT
BINARY_DOUBLE                       8 101 SQLT_IBDOUBLE      OCI_TYPECODE_BDOUBLE
User Defined Type                 N/A 108 SQLT_NTY           OCI_TYPECODE_OBJECT
REF                               N/A 110 SQLT_REF           OCI_TYPECODE_REF
CLOB,NCLOB                      128TB 112 SQLT_CLOB          OCI_TYPECODE_CLOB
BLOB                            128TB 113 SQLT_BLOB          OCI_TYPECODE_BLOB
BFILE                             N/A 114 SQLT_BFILE         OCI_TYPECODE_BFILE
TIMESTAMP                          11 187 SQLT_TIMESTAMP     OCI_TYPECODE_TIMESTAMP
TIMESTAMP WITH TIMEZONE            13 188 SQLT_TIMESTAMP_TZ  OCI_TYPECODE_TIMESTAMP_TZ
INTERVAL YEAR TO MONTH              5 189 SQLT_INTERVAL_YM   OCI_TYPECODE_INTERVAL_YM
INTERVAL DAY TO SECOND             11 190 SQLT_INTERVAL_DS   OCI_TYPECODE_INTERVAL_DS
UROWID                           3950 104 SQLT_RDD           OCI_TYPECODE_UROWID
TIMESTAMP WITH LOCAL TIMEZONE      11 232 SQLT_TIMESTAMP_LTZ OCI_TYPECODE_TIMESTAMP_LTZ
*/

					//@bug 173
					if (k == 0) // get once for the first row
					{
						if (ct[i].colWidth == 0)
						{
						CalpontSystemCatalog::OID oid = csc->lookupOID(
							make_tcn(storedCtx->schemaName, storedCtx->tableName, colNames[i]));
						ct[i] = csc->colType(oid);
						}
						fditems[i].dt_scale = ct[i].scale;
					}

				if (fditems[i].dt_is_data_null)
					ind1 = OCI_IND_NULL;
				else
					ind1 = OCI_IND_NOTNULL;

				text errbuf[1024];
				if (!SWALLOW_ROWS)
				{
					switch (colTypes[i])
					{
					case OCI_TYPECODE_VARCHAR:
					case OCI_TYPECODE_VARCHAR2:
					case OCI_TYPECODE_CHAR:
					case OCI_TYPECODE_UROWID:
						if (ind1 != OCI_IND_NULL)
						{
							//fudge when string is empty...
							if (fditems[i].dt_data_len > 0)
								OCIStringAssignText(handles.envhp, handles.errhp, (text*)fditems[i].dt_data,
								  fditems[i].dt_data_len, &str);
							else
								ind1 = OCI_IND_NULL;
						}
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_VARCHAR2, 0, &ind1, str,
							0, FALSE);
						if (str)
						{
							OCIStringResize(handles.envhp, handles.errhp, 0, &str);
							str = 0;
						}
						break;
					case OCI_TYPECODE_NUMBER:
						if (ind1 != OCI_IND_NULL)
						{
							if (ct[i].colDataType == CalpontSystemCatalog::FLOAT)
							{
								double d = strtod((const char*)fditems[i].dt_data, 0);
								OCINumberFromReal ( handles.errhp,
									&d,
									sizeof(d),
									&numberType_n );
							}
							else
							{
								int64_t val;
								if (ct[i].colWidth == 1)
								{
									////u_int8_t tmp = strtoul((char*)fditems[i].dt_data, char**)NULL, 10);
									u_int8_t tmp;
									memcpy (&tmp, fditems[i].dt_data, 1);
									int8_t a = (int8_t)tmp;
									OCINumberFromInt ( handles.errhp,
										 &a,
										 sizeof(a),
										 OCI_NUMBER_SIGNED,
										 &numberType_n );
									val = a;
								}
								else if (ct[i].colWidth == 2)
								{
									////u_int16_t tmp = strtoul((char*)fditems[i].dt_data, (char**)NULL, 10);
									u_int16_t tmp;
									memcpy (&tmp, fditems[i].dt_data, 2);
									int16_t a = (int16_t)tmp;
									OCINumberFromInt ( handles.errhp,
										 &a,
										 sizeof(a),
										 OCI_NUMBER_SIGNED,
										 &numberType_n );
									val = a;
								}
								else if (ct[i].colWidth == 4)
								{
									////u_int32_t tmp = strtoul((char*)fditems[i].dt_data, (char**)NULL, 10);
									u_int32_t tmp;
									memcpy (&tmp, fditems[i].dt_data, 4);
									int32_t a = (int32_t)tmp;
									OCINumberFromInt ( handles.errhp,
										 &a,
										 sizeof(a),
										 OCI_NUMBER_SIGNED,
										 &numberType_n );
									val = a;
								}
								else // should be 8
								{
									////u_int64_t tmp = strtoull((char*)fditems[i].dt_data, (char**)NULL, 10);
									u_int64_t tmp;
									memcpy (&tmp, fditems[i].dt_data, 8);
									int64_t a = (int64_t)tmp;
									OCINumberFromInt ( handles.errhp,
										 &a,
										 sizeof(a),
										 OCI_NUMBER_SIGNED,
										 &numberType_n );
									 val = a;
								}

								// check scale to adjust output data
								if (fditems[i].dt_scale > 0)   // fix point decimal
								{
									//biggest Calpont supports is DECIMAL(18,x), or 18 total digits+dp+sign
									const int ctmp_size = 18+1+1+1;
									char ctmp[ctmp_size];
									char fmt[] = "9999999999999999999"; //19 9's (one spare that will get replaced)
									snprintf(ctmp, ctmp_size,
#if __WORDSIZE <= 32
										"%lld",
#else
										"%ld",
#endif
										val);
									//we want to move the last dt_scale chars right by one spot to insert the dp
									//we want to move the trailing null as well, so it's really dt_scale+1 chars
									size_t l1 = strlen(ctmp);
									//need to make sure we have enough leading zeros for this to work...
									//at this point scale is always > 0
									if ((unsigned)fditems[i].dt_scale > l1)
									{
										const char* zeros = "000000000000000000"; //18 0's
										size_t diff = fditems[i].dt_scale - l1; //this will always be > 0
										memmove(&ctmp[diff], &ctmp[0], l1 + 1); //also move null
										memcpy(&ctmp[0], zeros, diff);
										l1 = 0;
									}
									else
										l1 -= fditems[i].dt_scale;
									memmove(&ctmp[l1 + 1], &ctmp[l1], fditems[i].dt_scale + 1); //also move null
									ctmp[l1] = '.';
									fmt[strlen(fmt) - fditems[i].dt_scale - 1] = '.';
									OCINumberFromText(handles.errhp, (const oratext*)ctmp, strlen(ctmp),
										(const oratext*)fmt, strlen(fmt),
										(const oratext*)"", 0,
										&numberType_n);
								}
							}
						}
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_NUMBER, 0, &ind1,
							&numberType_n, 0, FALSE);
						break;
					case OCI_TYPECODE_BFLOAT:
						floatType = strtof((const char*)fditems[i].dt_data, 0);
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_BFLOAT, 0, &ind1,
							&floatType, 0, FALSE);
						break;
					case OCI_TYPECODE_BDOUBLE:
						doubleType = strtod((const char*)fditems[i].dt_data, 0);
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_BDOUBLE, 0, &ind1,
							&doubleType, 0, FALSE);
						break;
					case OCI_TYPECODE_DATE:
						if (ind1 != OCI_IND_NULL)
							OCIDateFromText(handles.errhp, (text*)fditems[i].dt_data, fditems[i].dt_data_len,
								(text*)"YYYY-MM-DD", 10, 0, 0, &dateType_d);
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_DATE, 0, &ind1,
							&dateType_d, 0, FALSE);
						break;
					case OCI_TYPECODE_RAW:
						OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_RAW, 0, &ind1,
							fditems[i].dt_data, fditems[i].dt_data_len, FALSE);
						break;
					case OCI_TYPECODE_TIMESTAMP:
					{
						OCIDateTime* datetimeType_n;
						if (ind1 != OCI_IND_NULL)
						{
							OCIDescriptorAlloc((dvoid *)handles.envhp,(dvoid **)&datetimeType_n, OCI_DTYPE_TIMESTAMP,
									0, (dvoid **) 0);

							const std::string fmt("YYYY-MM-DD HH24:MI:SS.FF");
							char fmt1[64];
							memset(fmt1, 0, 64);

							strcpy(fmt1, fmt.c_str());

							OCIDateTimeFromText((dvoid *)handles.usrhp, handles.errhp,
								(CONST OraText *)fditems[i].dt_data, (ub4)strlen((char *)fditems[i].dt_data),
								(CONST OraText *)fmt1, (ub1)strlen((char *)fmt1), (CONST OraText *)0, 0,
								datetimeType_n);

							OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_TIMESTAMP, 0,
								(OCIInd *)&ind1, (dvoid*)datetimeType_n, 0, FALSE);

							OCIDescriptorFree((dvoid*)datetimeType_n, (ub4)OCI_DTYPE_TIMESTAMP);
						}
						else
						{
							int status;

							status = OCIAnyDataAttrSet(handles.svchp, handles.errhp, anyData, OCI_TYPECODE_TIMESTAMP, 0,
								(OCIInd *)&ind1, ( dvoid *)datetimeType_n, 0, FALSE);
						}

						break;
					}
					case OCI_TYPECODE_TIMESTAMP_TZ:
					case OCI_TYPECODE_TIMESTAMP_LTZ:
					case OCI_TYPECODE_INTERVAL_YM:
					case OCI_TYPECODE_INTERVAL_DS:
						errHandle(storedCtx, handles);
						sprintf((char*)errbuf, "ODCITableFetch: convert column: converter not implemented for output "
							"column type %u!", colTypes[i]);
						OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, errbuf, strlen((char*)errbuf));
						return ODCI_ERROR;
						break;

					case OCI_TYPECODE_INTEGER:
					case OCI_TYPECODE_REF:
					case OCI_TYPECODE_FLOAT:
					case OCI_TYPECODE_DECIMAL:
					case OCI_TYPECODE_UNSIGNED8:
					case OCI_TYPECODE_MLSLABEL:
					case OCI_TYPECODE_OBJECT:
					case OCI_TYPECODE_NAMEDCOLLECTION:
					case OCI_TYPECODE_BLOB:
					case OCI_TYPECODE_BFILE:
					case OCI_TYPECODE_CLOB:
					case OCI_TYPECODE_CFILE:
					case OCI_TYPECODE_TIME:
					case OCI_TYPECODE_TIME_TZ:
					//case OCI_TYPECODE_ITABLE:
					//case OCI_TYPECODE_RECORD:
					//case OCI_TYPECODE_BOOLEAN:
					default:
						errHandle(storedCtx, handles);
						sprintf((char*)errbuf, "ODCITableFetch: convert column: cannot handle output column type %u!",
							colTypes[i]);
						OCIExtProcRaiseExcpWithMsg(handles.extProcCtx, 29400, errbuf, strlen((char*)errbuf));
						return ODCI_ERROR;
						break;
					}
				} //SWALLOW_ROWS
			}

			if (!SWALLOW_ROWS)
			{
				if (checkerr(&handles, OCIAnyDataEndCreate(handles.svchp, handles.errhp, anyData),
					"OCIAnyDataEndCreate"))
					return ODCI_ERROR;
			} //SWALLOW_ROWS

			if ( isInsert )
			{
				//Build sql statement
				boost::algorithm::to_lower( insertText );
				selectPos = insertText.find("select",0);
				string::size_type rparaPos = insertText.find_last_of ( ")");
				if ( rparaPos < string::npos )
				{
					if ( rparaPos == (insertText.length()-2) ) //select is in ()
					{
						string::size_type lparaPos = insertText.find_last_of ( "(");
						if ( lparaPos < string::npos  )
						{
							selectPos = lparaPos;
						}

					}
				}
				newDMLStatement = insertText.substr(0, selectPos-1);
				newDMLStatement += " values ( ";
				char schema[80];
				if ( insertCols.size() > 0 )
				{
					vector<insertColInfo>::const_iterator iter;
					unsigned int place = 0;
					strcpy( schema, insertCols[0].schemaName );
					for ( iter = insertCols.begin(); iter != insertCols.end(); ++iter)
					{
						if ( (*iter).constFlag ) //constant column
						{
							newDMLStatement += (*iter).value;
						}
						else
						{
							for ( ub2 colNum = 0; colNum < num_attr; colNum++)
							{
								if ( ct[colNum].colPosition == (*iter).colPosition )
								{
									newDMLStatement += "'";
									newDMLStatement += (char*)fditems[colNum].dt_data;
									newDMLStatement += "'";
								}
							}
						}
						if ( place == insertCols.size()-1 )
							continue;
						newDMLStatement += ", " ;
						place++;
					}
				}
				else //select *
				{
					for ( ub2 colNum = 0; colNum < num_attr; colNum++)
					{
						newDMLStatement += (char*)fditems[colNum].dt_data;

						if ( colNum == num_attr -1 )
							continue;
						newDMLStatement += ", " ;
					}
				}
				newDMLStatement += " );";

				VendorDMLStatement dmlStmt(newDMLStatement, csc->sessionID());
				CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt,schema );

				ByteStream bytestream;
				bytestream << csc->sessionID();
				pDMLPackage->write(bytestream);
				delete pDMLPackage;
				MessageQueueClient mq("DMLProc");
				ByteStream::byte b;
				std::string errorMsg;
				ByteStream::octbyte rows;
				mq.write(bytestream);
				bytestream = mq.read();
				bytestream >> b;
				bytestream >> rows;
				bytestream >> errorMsg;
				char errbuf[1024]= {0};
				if (b != 0)
				{
					sprintf(errbuf, "%s",errorMsg.c_str());
					OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
					return ODCI_ERROR;
				}
			}

		}

		OCIHandleFree(dschp, OCI_HTYPE_DESCRIBE);

	} //tableDone

#ifdef TBLDEBUG_ALL
	strcpy(buf, "This is ODCITableFetch 4\n");
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif
	if (checkerr(&handles, OCIAnyDataSetEndCreate(handles.svchp, handles.errhp, anyDataSet), "OCIAnyDataSetEndCreate"))
		return ODCI_ERROR;
#ifdef TBLDEBUG_ALL
	strcpy(buf, "This is ODCITableFetch 5\n");
	if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#endif

	*outSet_ind=OCI_IND_NOTNULL;
	*outSet = anyDataSet;
	storedCtx->anyDS = anyDataSet;
#ifdef TBLDEBUG
	ub4 n;
	if (checkerr(&handles, OCIAnyDataSetGetCount(handles.svchp, handles.errhp, anyDataSet, &n),
		"OCIAnyDataSetGetCount"))
		return ODCI_ERROR;

	{
		sprintf(buf, "This is ODCITableFetch 5: count = %d\n", n);
		if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
#ifdef TBLDEBUG_ALL
		for (ub4 j = 0; j < n; j++)
		{
			OCIAnyData* ad;
			ad = 0;
			if (checkerr(&handles, OCIAnyDataSetGetInstance(handles.svchp, handles.errhp, anyDataSet, &ad),
				"OCIAnyDataSetGetInstance"))
				return ODCI_ERROR;
			sprintf(buf, "This is ODCITableFetch 6: anyData = %p\n", ad);
			if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
			OCIInd ind2;
			OCINumber num2;
			OCINumber* num2p;
			ub4 num2_sz;
			num2p = &num2;
			sprintf(buf, "This is ODCITableFetch 6.1: num2p = %p\n", num2p);
			if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
			if (checkerr(&handles, OCIAnyDataAttrGet(handles.svchp, handles.errhp, ad, OCI_TYPECODE_NUMBER, 0, &ind2,
				&num2p, &num2_sz, FALSE), "OCIAnyDataAttrGet"))
				return ODCI_ERROR;
			ub4 num2_int;
			OCINumberToInt(handles.errhp, num2p, sizeof(num2_int), 0, &num2_int);
			sprintf(buf, "This is ODCITableFetch 6.2: R_REGIONKEY = %d, num2p = %p\n", num2_int, num2p);
			if (p) fwrite(buf, 1, strlen(buf), p), fflush(p);
		}
#endif
	}

	if (p) fclose(p);
#endif

	return ODCI_SUCCESS;

}

/***********************************************************************/

/* Callout for ODCITableClose */

extern "C" EXECPROC_API int ODCITableClose(OCIExtProcContext* extProcCtx, CalpontImpl* self, CalpontImpl_ind* self_ind)
{
	Handles_t handles;                   /* OCI hanldes */
	StoredCtx* storedCtx;                /* Stored context pointer */

	/* Get OCI handles */
	if (GetHandles(extProcCtx, &handles))
		return ODCI_ERROR;

	/* Get the stored context */
	storedCtx=GetStoredCtx(&handles,self,self_ind);
	if (!storedCtx) return ODCI_ERROR;
	
	cpsm_conhdl_t* hndl = static_cast<cpsm_conhdl_t*>(storedCtx->conn_hndl);

	try {
		if (storedCtx->tpl_scanhdl) {
			dhcs_tpl_scan_close(storedCtx->tpl_scanhdl, storedCtx->conn_hndl);
			storedCtx->tpl_scanhdl = NULL;
		}
		if (storedCtx->tplhdl) {
			dhcs_tpl_close(storedCtx->tplhdl, storedCtx->conn_hndl);
			storedCtx->tplhdl = NULL;
		}
	} catch (...)
	{
		sigErrHandle(storedCtx);
		return ODCI_ERROR;
	}
		
	queryStats = hndl->queryStats;

	// reset planReady flag
	planReady = false;
	prevTblids.clear();
#ifdef TBLDEBUG
	{
		char buf[10240]= {0};
		FILE* p;
		if ((p = fopen(f1file, "a")) != NULL)
		{
			sprintf(buf, "This is ODCITableClose('%s')\n", storedCtx->tableName);
			fwrite(buf, 1, strlen(buf), p);
			fclose(p);
		}
	}
#endif

	/* Free the memory for the stored context */
	if (checkerr(&handles, OCIMemoryFree((dvoid*) handles.usrhp, handles.errhp, (dvoid*) storedCtx), "OCIMemoryFree"))
		return ODCI_ERROR;
	return ODCI_SUCCESS;
}

extern "C" int Process_dml(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn,
	short sessionIDn_ind,
	char* sDateFormat, short sDateFormat_ind,
	char* sDatetimeFormat, short sDatetimeFormat_ind,
	char* sOwner, short sOwner_ind,
	char* sCalUser, short sCalUser_ind,
	char* sCalPswd, short sCalPswd_ind)
{
	char errbuf[1024]= {0};
	Message::Args args;
	Message dmlStmtMsg(M0013);
	Message commError(M0014);
	Message DMLProcError(M0015);

	if (sessionIDn_ind == OCI_IND_NULL ||
		sOwner_ind == OCI_IND_NULL ||
		sCalUser_ind == OCI_IND_NULL ||
		sCalPswd_ind == OCI_IND_NULL) return -1;

	const string calpontUser(sCalUser);
	const string calpontPassword(sCalPswd);
	const string calpontDBMS("CALFEDBMS");

	Handles_t handles;
	ub4 sessionID;

	if (GetHandles(extProcCtx, &handles)) return -2;
	pu::errhp = handles.errhp;

	if (OCINumberToInt(handles.errhp, sessionIDn, sizeof(sessionID), OCI_NUMBER_UNSIGNED,
		(dvoid *)&sessionID) != OCI_SUCCESS)
		return -3;

	if (ml == 0) ml = new MessageLog(LoggingID(24, sessionID, 0, 1));
	if ( isInsert )
	{
		return 0;
	}
	string dmlStatement;

	dmlStatement = get_dml_sql_text(extProcCtx, calpontUser, calpontPassword, calpontDBMS, sessionID);

	for (int i = 0; i < 10 && dmlStatement.length() == 0; i++)
	{
		pause_();
		dmlStatement = get_dml_sql_text(extProcCtx, calpontUser, calpontPassword, calpontDBMS, sessionID);
	}

	if (dmlStatement.length() == 0)
	{
		planReady = false;
		strcpy(errbuf, "Error retrieving SQL statement");
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
		return -4;
	}
	dmlStatement += ';';

	args.reset();
	args.add(dmlStatement);
	dmlStmtMsg.format(args);
	ml->logDebugMessage(dmlStmtMsg);

	VendorDMLStatement dmlStmt(dmlStatement, sessionID);
	CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt, sOwner);

	if (pDMLPackage == 0)
	{
		ml->logWarningMessage(dmlStmtMsg);
		planReady = false;
		strcpy(errbuf, "Error parsing DML statement");
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
		return -5;
	}
	/* Check which columns are date or timestamp */
	CalpontSystemCatalog::TableName tableName;
	tableName.schema = pDMLPackage->get_SchemaName();
	tableName.table = pDMLPackage->get_TableName();
	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
	csc->sessionID(sessionID);
	csc->identity(CalpontSystemCatalog::FE);
	execplan::CalpontSystemCatalog::RIDList ridList;
	try {
		ridList = csc->columnRIDs( tableName );
	}
	catch (exception& ex) {
		ml->logWarningMessage(dmlStmtMsg);
		planReady = false;
		sprintf(errbuf, "Error processing DML statement: %s", ex.what());
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
		return -9;
	}
	catch (...) {
		ml->logWarningMessage(dmlStmtMsg);
		planReady = false;
		strcpy(errbuf, "Error processing DML statement");
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
		return -10;
	}
	execplan::CalpontSystemCatalog::RIDList::const_iterator iter;
	execplan::CalpontSystemCatalog::ROPair roPair;
	std::vector <execplan::CalpontSystemCatalog::ColType> colTypeList;
	for ( iter = ridList.begin(); iter != ridList.end(); ++iter)
	{
		roPair = *iter;
		execplan::CalpontSystemCatalog::ColType colType;
		colType = csc->colType( roPair.objnum);
		if ( (colType.colDataType == CalpontSystemCatalog::DATE)
			|| ( colType.colDataType == CalpontSystemCatalog::DATETIME ) )
		{
			colTypeList.push_back( colType );
		}
	}

	if ( colTypeList.size() > 0 )
	{
		//Convert to Calpont data or datetime format
		vector <execplan::CalpontSystemCatalog::ColType>::const_iterator colTypeIter;
		std::string colValue;
		DMLTable* tablePtr =  pDMLPackage->get_Table();
		RowList rows = tablePtr->get_RowList();
		Row* rowPtr = rows.at(0);
		execplan::CalpontSystemCatalog::ColType type;
		sword status;
		ub4 size = 80;
		char valueBuf[80];
		for ( colTypeIter=colTypeList.begin(); colTypeIter != colTypeList.end(); ++colTypeIter)
		{
			type = *colTypeIter;
			const DMLColumn* columnPtr = rowPtr->get_ColumnAt( type.colPosition );
			//@Bug 1136, don't convert null value
			if ( columnPtr )
			{
				colValue = columnPtr->get_Data();
				ub4 len = colValue.length();
				ub1 fmtlen = strlen( sDateFormat );
				if ( type.colDataType == CalpontSystemCatalog::DATE )
				{
					OCIDate dateType_d;
					status = OCIDateFromText(handles.errhp, (text*)colValue.c_str(), len, (text *)sDateFormat, fmtlen,
						0, 0, &dateType_d);
					//cout << "Status is " << status << endl;
					std::string calDateFmt("YYYY-MM-DD");
					fmtlen = calDateFmt.length();
					status = OCIDateToText(handles.errhp, &dateType_d, (text*)calDateFmt.c_str(), fmtlen, 0, 0, &size,
						(text *)valueBuf );
					//cout << "Status is " << status << endl;
				}
				else //DATETIME
				{

					OCIDateTime *dateTime_d;

					/* set user attribute to session */

					sword ret;
					//ub4 type1 = OCI_DTYPE_TIMESTAMP_TZ;
					ub4 type1 = OCI_DTYPE_TIMESTAMP;
					ret = OCIDescriptorAlloc((dvoid *)handles.envhp,(dvoid **)&dateTime_d, type1,
						0, (dvoid **) 0);
					OraText *str1, *fmt1;
					str1 = (OraText *)malloc(64);
					memset ((void *) str1, '\0', 64);
					fmt1 = (OraText *)malloc(64);
					memset ((void *) fmt1, '\0', 64);

					//@bug 1180 Fix unsafe copy
					strncpy((char *)fmt1,sDatetimeFormat, 64);
					
					strncpy((char *)str1,colValue.c_str(), 64);

					status = OCIDateTimeFromText((dvoid *)handles.usrhp, handles.errhp, (CONST OraText *)str1,
						(ub4) strlen((char *)str1), (CONST OraText *)fmt1, (ub1) strlen((char *)fmt1), (CONST OraText *)0, 0,
						dateTime_d);
					std::string calDateTimeFmt("YYYY-MM-DD HH24:MI:SS.FF");
					fmtlen = calDateTimeFmt.length();
					ub1 fspres = 6;
					size = 80;
					status = OCIDateTimeToText((dvoid *)handles.envhp,handles.errhp, dateTime_d,
						(text *)calDateTimeFmt.c_str(),fmtlen, fspres,0, 0, &size, (text *)valueBuf  );
					if (dateTime_d)
					{
						OCIDescriptorFree((dvoid *) dateTime_d, (ub4) type1);
					}
					//@bug 1180 fix memory leak.
					free ( str1 );
					free ( fmt1 ); 
				}
				std::string newValue( valueBuf);
				DMLColumn* nonconstColumnPtr = const_cast<DMLColumn*>(columnPtr);
				nonconstColumnPtr->set_Data( newValue );
			}
		}
	}

	ByteStream bytestream;
	bytestream << sessionID;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	MessageQueueClient mq("DMLProc");
	ByteStream::byte b;
	ByteStream::octbyte rows;
	std::string errorMsg;
	try
	{
		mq.write(bytestream);
		bytestream = mq.read();
		bytestream >> b;
		bytestream >> rows;
		bytestream >> errorMsg;
		dmlRowCount = rows;
	}
	catch (runtime_error& rex)
	{
		args.reset();
		args.add(rex.what());
		commError.format(args);
		ml->logSeriousMessage(commError);
		planReady = false;
		sprintf(errbuf, "Error communicating with Engine Controller: %s", rex.what());
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
		return -6;
	}
	catch (...)
	{
		args.reset();
		args.add("caught unknown exception");
		commError.format(args);
		ml->logSeriousMessage(commError);
		planReady = false;
		strcpy(errbuf, "Error communicating with Engine Controller");
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 9999, (text*)errbuf, strlen(errbuf));
		return -7;
	}

	if (b != 0)
	{
		args.reset();
		args.add(errorMsg);
		args.add(dmlStatement);
		DMLProcError.format(args);
		ml->logCriticalMessage(DMLProcError);
		planReady = false;
		sprintf(errbuf, "%s", errorMsg.c_str());
		OCIExtProcRaiseExcpWithMsg(extProcCtx, 29400, (text*)errbuf, strlen(errbuf));
		return -8;
	}

	return 0;
}

extern "C" int Get_rowcount()
{
	return dmlRowCount;
}
// vim:ts=4 sw=4:
