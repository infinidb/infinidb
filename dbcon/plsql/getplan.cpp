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

/* $Id: getplan.cpp 9210 2013-01-21 14:10:42Z rdempsey $ */

/** @brief convert oracle execution plan to Calpont execution plan
 * Call calpont.cal_get_explain_plan stored function of Oracle to get
 * the oracle execution plan. Then convert it to Calpont execution plan
 * structure.
 */

#ifndef OCI_ORACLE
#include <oci.h>
#endif
#ifndef ODCI_ORACLE
#include <odci.h>
#endif

#include <string>
using namespace std;

#include <boost/algorithm/string/case_conv.hpp>

#include "calpontselectexecutionplan.h"
using namespace execplan;

#include "planutils.h"
namespace pu = plsql::planutils;
#include "getplan.h"
#include "tablefuncs.h"


#define BIND_VARIABLE_SUPPORT 1

namespace {
const int SQL_MAX_SIZE = 15000;
const int MAXROWS = 100;
OCIBind *bnd1p = (OCIBind*)0;
OCIBind *bnd2p = (OCIBind*)0;
OCIBind *bnd3p = (OCIBind*)0;
OCIBind *bnd4p = (OCIBind*)0;
OCISvcCtx *svchp;
OCIStmt *stmthp;
OCIEnv *envhp;
OCIServer *srvhp;
OCISession *usrhp;
OCIStmt* stmthp1;   // for explain_plan returned cursor
char sqltext[SQL_MAX_SIZE] = {0};
}// anon namespace

/** @brief the wrapper function to Oracle stored function cal_get_explain_plan
 */
int getPlan( OCIExtProcContext* ctx,
    int sessionId,
    const char* curSchemaName,
    CalpontSelectExecutionPlan &plan,
    BindValueSet* bindValList)
{
    pu::myCtx = ctx;
    pu::sessionid = sessionId;
    strcpy(pu::curschema, curSchemaName);
    
    string stmt = get_sql_text(sessionId); 
    plan.data(stmt);
    
    
    // @bug 1331
    string sqlCommand = stmt.substr(0, stmt.find_first_of(' ', 0));
    if (strcasecmp(sqlCommand.c_str(), "CREATE") == 0)
	{
		string tmpsql = stmt;
		boost::to_upper(tmpsql);
		string::size_type sPos = tmpsql.find("SELECT");
		if (sPos != string::npos)
			stmt = stmt.substr(sPos);
	}
    
    
    string newStmt = stmt;  

#if BIND_VARIABLE_SUPPORT
    BindValue *elem;          
    BindValue_ind *elem_ind;   
    boolean j;
    sb4 size;    
    pu::checkerr (pu::errhp, OCICollSize ( envhp, pu::errhp, bindValList, &size ));
    char sql[SQL_MAX_SIZE];
    char bindVar[SQL_MAX_SIZE];
    char bindVal[SQL_MAX_SIZE];
    
    for (sb4 i = 0; i < size; i++)
    {            
        pu::checkerr(pu::errhp, OCICollGetElem ( envhp, pu::errhp, bindValList, i, &j,(void**)&elem, (void**)&elem_ind ));
        if (!elem->sqltext || !elem->bindVar || !elem->bindValue)
            continue;
        strcpy(sql, (char*) OCIStringPtr(envhp, elem->sqltext));
        strcpy(bindVar, (char*) OCIStringPtr(envhp, elem->bindVar));
        strcpy(bindVal, (char*) OCIStringPtr(envhp, elem->bindValue));
        
        // replace bind variable with bind values if sql stmt matches
        if (strcmp(sql, stmt.c_str()) == 0) 
        {
            string bindName = ":";
            bindName.append(bindVar);
            string::size_type pos = newStmt.find(bindName); 
            newStmt.replace(pos, bindName.length(), bindVal);  
        }
	}
#endif
	
    strcpy(sqltext, newStmt.c_str());
    pu::checkerr(pu::errhp, OCIStmtExecute(svchp, stmthp, pu::errhp, (ub4)1, (ub4)0,
                (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                (ub4)OCI_DEFAULT));

    // fetch result and convert
    pu::getPlanRecords(stmthp1);
    pu::doConversion(plan);
    return 0;
}

string get_sql_text(int sessionID)
{
	OCIStmt *stmthp;
	dvoid* tmp;
	//sword status;
	OCIBind *bnd1p = 0;
	OCIBind *bnd2p = 0;
	char sql[SQL_MAX_SIZE] = {0};
	char* stmt;

	pu::checkerr(pu::errhp, OCIHandleAlloc((dvoid *)envhp, (dvoid **)&stmthp, (ub4)OCI_HTYPE_STMT, 50, (dvoid **)&tmp));

	stmt = "begin :sqltext := calpont.cal_get_sql_text(:sessionid); end;";

	pu::checkerr(pu::errhp, OCIStmtPrepare(stmthp, pu::errhp, (text*)stmt, (ub4)strlen(stmt),
		(ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

	pu::checkerr(pu::errhp, OCIBindByName(stmthp, (OCIBind **)&bnd1p, pu::errhp,
		(text *)":sqltext", (sb4)strlen(":sqltext"),
		(dvoid *)sql, (sb4)SQL_MAX_SIZE,  SQLT_STR, (dvoid *)0,
		(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,(ub4)OCI_DEFAULT));                

	pu::checkerr(pu::errhp, OCIBindByName(stmthp, (OCIBind **)&bnd2p, pu::errhp,
		(text *)":sessionid", (sb4)strlen(":sessionid"),
		(dvoid *)&sessionID, (sb4)sizeof(sessionID),  SQLT_INT, (dvoid *)0,
		(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0, (ub4)OCI_DEFAULT));  

	pu::checkerr(pu::errhp, OCIStmtExecute(svchp, stmthp, pu::errhp, (ub4)1, (ub4)0, (OCISnapshot *)NULL,
		(OCISnapshot *)NULL, (ub4)OCI_DEFAULT));

	pu::checkerr(pu::errhp, OCIHandleFree((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT));

	return sql;
}

void envInit(const char* sConnectAsUser,
             const char* sConnectAsUserPwd,
             const char* sConnectDBLink,
             const char* sProcedureName )
{
    dvoid* tmp;

    pu::checkerr (pu::errhp, OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0));

    pu::checkerr (pu::errhp, OCIEnvInit( &envhp, (ub4) OCI_DEFAULT, 21, (dvoid **) &tmp  ));

    pu::checkerr (pu::errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &pu::errhp,
                (ub4) OCI_HTYPE_ERROR, 52, (dvoid **) &tmp));

    pu::checkerr (pu::errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &srvhp,
                (ub4)OCI_HTYPE_SERVER, 52, (dvoid **) &tmp));

    pu::checkerr(pu::errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp,
                (ub4)OCI_HTYPE_SVCCTX, 52, (dvoid **) &tmp));

    pu::checkerr(pu::errhp, OCIServerAttach(srvhp, pu::errhp, (text *)sConnectDBLink,
                (sb4)strlen(sConnectDBLink), (ub4)OCI_DEFAULT));

    // set attribute server context in the service context
    pu::checkerr(pu::errhp, OCIAttrSet( (dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX,
                (dvoid *) srvhp, (ub4) 0, (ub4) OCI_ATTR_SERVER, (OCIError *) pu::errhp));

    // allocate a user context handle
    pu::checkerr (pu::errhp, OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp,
                (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0));

    pu::checkerr (pu::errhp, OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
                (dvoid *)sConnectAsUser, (ub4)strlen(sConnectAsUser),
                OCI_ATTR_USERNAME, pu::errhp));

    pu::checkerr (pu::errhp, OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION,
                (dvoid *)sConnectAsUserPwd, (ub4)strlen(sConnectAsUserPwd),
                OCI_ATTR_PASSWORD, pu::errhp));

    pu::checkerr (pu::errhp, OCISessionBegin (svchp, pu::errhp, usrhp, OCI_CRED_RDBMS, OCI_DEFAULT));

    pu::checkerr (pu::errhp, OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX,
                (dvoid *)usrhp, (ub4)0, OCI_ATTR_SESSION, pu::errhp));

    pu::checkerr (pu::errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp,
                (ub4) OCI_HTYPE_STMT, 50, (dvoid **) &tmp));
    
    // Prepare, bind parameter for stored function
    string mytest = "begin :cursor := calpont.cal_get_explain_plan(:sessionid, :curschema, :sqltext); end;";

    pu::checkerr(pu::errhp, OCIStmtPrepare(stmthp, pu::errhp, (text*)mytest.c_str(), (ub4)mytest.length(),
                (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

    pu::checkerr (pu::errhp, OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp1,
                (ub4) OCI_HTYPE_STMT, 50, (dvoid **) &tmp));

    pu::checkerr( pu::errhp, OCIBindByName (stmthp, (OCIBind **) &bnd1p, pu::errhp,
                (text *)":cursor", (sb4)strlen((char *)":cursor"),
                (dvoid *)&stmthp1, (sb4) 0,  SQLT_RSET, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));

    pu::checkerr( pu::errhp, OCIBindByName (stmthp, (OCIBind **) &bnd2p, pu::errhp,
                (text *)":sessionid", -1,
                (ub1 *)&pu::sessionid, (sword)sizeof(pu::sessionid),  SQLT_INT, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));

    pu::checkerr( pu::errhp, OCIBindByName (stmthp, (OCIBind **) &bnd3p, pu::errhp,
                (text *)":curschema", (sb4)strlen(":curschema"),
                (dvoid *)pu::curschema, (sb4)80,  SQLT_STR, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));
                
    pu::checkerr( pu::errhp, OCIBindByName (stmthp, (OCIBind **) &bnd4p, pu::errhp,
                (text *)":sqltext", (sb4)strlen(":sqltext"),
                (dvoid *)sqltext, (sb4)SQL_MAX_SIZE,  SQLT_STR, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));             

}

void envCleanup()
{
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) stmthp, (ub4) OCI_HTYPE_STMT));
    pu::checkerr (pu::errhp, OCISessionEnd(svchp, pu::errhp, usrhp, (ub4)OCI_DEFAULT));
    pu::checkerr (pu::errhp, OCIServerDetach( srvhp, pu::errhp, (ub4) OCI_DEFAULT ));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) srvhp, (ub4) OCI_HTYPE_SERVER));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) pu::errhp, (ub4) OCI_HTYPE_ERROR));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) stmthp1, (ub4) OCI_HTYPE_STMT));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) envhp, (ub4) OCI_HTYPE_ENV ));
    pu::checkerr (pu::errhp, OCITerminate (OCI_DEFAULT));
}

#ifdef STANDALONE

int main(int argc, char** argv)
{  		    	
    OCIExtProcContext* ctx = 0;
    CalpontSelectExecutionPlan plan;
    if (argc < 2)
    {
        cerr << "Please give session id" << endl;
        exit(1);
    }
    envInit("calpont", "calpont", "XE", NULL);
    getPlan(ctx, atoi(argv[1]), "TPCH", plan);
    envCleanup();
    return 0;
}
#endif
