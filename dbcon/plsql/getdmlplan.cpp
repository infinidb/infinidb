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

/* $Id: getdmlplan.cpp 9210 2013-01-21 14:10:42Z rdempsey $ */

/** @brief convert oracle execution plan to Calpont execution plan
 * Call calpont.cal_get_DML_explain_plan stored function of Oracle to get
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

#include "calpontselectexecutionplan.h"
using namespace execplan;

#include "planutils.h"
namespace pu = plsql::planutils;
#include "getdmlplan.h"

namespace {
OCIBind *bnd1p = (OCIBind*)0;
OCIBind *bnd2p = (OCIBind*)0;
OCIBind *bnd3p = (OCIBind*)0;
OCIBind *bnd4p = (OCIBind*)0;
char ssqltext[4000] = {0};
OCISvcCtx *svchp;
OCIStmt *stmthp;
OCIEnv *envhp;
OCIServer *srvhp;
OCISession *usrhp;
OCIStmt* stmthp1;

}// anon namespace

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
    pu::checkerr(pu::errhp, OCIStmtExecute(svchp, stmthp, pu::errhp, (ub4)1, (ub4)0,
                (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                (ub4)OCI_DEFAULT));

    // fetch result and convert
    pu::getPlanRecords(stmthp1);
    pu::doConversion(plan);
    return;
}

void envDMLInit(const char* sConnectAsUser,
             const char* sConnectAsUserPwd,
             const char* sConnectDBLink,
             const char* sProcedureName)
{
    dvoid* tmp;

    pu::checkerr (pu::errhp, OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0));

    pu::checkerr (pu::errhp, OCIEnvInit( &envhp, (ub4) OCI_DEFAULT, 21, (dvoid **) &tmp  ));
	
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
    string mytest = "begin :cursor := calpont.cal_get_DML_explain_plan(:sessionid, :ssqltext, :curschema); end;";

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
                (text *)":ssqltext", (sb4)strlen(":ssqltext"),
                (dvoid *)ssqltext, (sb4)4000,  SQLT_STR, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));

    pu::checkerr( pu::errhp, OCIBindByName (stmthp, (OCIBind **) &bnd4p, pu::errhp,
                (text *)":curschema", (sb4)strlen(":curschema"),
                (dvoid *)pu::curschema, (sb4)80,  SQLT_STR, (dvoid *)0,
                (ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,   (ub4)OCI_DEFAULT));

}

void envDMLCleanup()
{
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) stmthp, (ub4) OCI_HTYPE_STMT));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) srvhp, (ub4) OCI_HTYPE_SERVER));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) stmthp1, (ub4) OCI_HTYPE_STMT));
    pu::checkerr (pu::errhp, OCIHandleFree((dvoid *) envhp, (ub4) OCI_HTYPE_ENV ));
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
    envDMLInit("calpont", "calpont", "XE", NULL);
    getDMLPlan(ctx, atoi(argv[1]),
    "update region set r_regionkey = 100 where r_regionkey = 0;",
    "TPCH",
    plan);
    envDMLCleanup();
    return 0;
}
#endif
