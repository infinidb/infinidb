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
* $Id: execproc.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*************************/

// execproc.cpp : Defines the entry point for the DLL application.
//

#include "execproc.h"
#include "checkerr.h"

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include <iostream>
using namespace std;

#ifndef OCI_ORACLE
# include <oci.h>
#endif
#ifndef ODCI_ORACLE
# include <odci.h>
#endif

// This is an example of an exported function.
extern "C" EXECPROC_API int Execute_procedure(OCIExtProcContext* ctx,
	char* sConnectAsUser, short sConnectAsUser_i,
	char* sConnectAsUserPwd, short sConnectAsUserPwd_i,
	char* sConnectDBLink, short sConnectDBLink_i,
	char* sProcedureName, short sProcedureName_i,
	char* arg1, short arg1_i,
	char* arg2, short arg2_i,
	char* arg3, short arg3_i,
	char* arg4, short arg4_i,
	char* arg5, short arg5_i,
	char* arg6, short arg6_i)
{
	OCIEnv *envhp;
	OCIServer *srvhp;
	OCIError *errhp;
	OCISvcCtx *svchp;
	OCIStmt *stmthp;
	OCISession *usrhp;
	dvoid* tmp;
	sword status;
	Handles_t handles;

	status = OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0);

	status = OCIHandleAlloc( (dvoid *) NULL, (dvoid **) &envhp, (ub4) OCI_HTYPE_ENV, 52, (dvoid **) &tmp);

	status = OCIEnvInit( &envhp, (ub4) OCI_DEFAULT, 21, (dvoid **) &tmp  );

	status = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhp, (ub4) OCI_HTYPE_ERROR, 52, (dvoid **) &tmp);
	status = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &srvhp, (ub4) OCI_HTYPE_SERVER, 52, (dvoid **) &tmp);

	handles.errhp = errhp;
	handles.extProcCtx = ctx;

	checkerr(&handles, OCIServerAttach(srvhp, errhp, (text *)sConnectDBLink, (sb4)strlen(sConnectDBLink),
		(ub4)OCI_DEFAULT));

	status = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, (ub4) OCI_HTYPE_SVCCTX, 52, (dvoid **) &tmp);

	/* set attribute server context in the service context */
	status = OCIAttrSet( (dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX, (dvoid *) srvhp, (ub4) 0,
		(ub4) OCI_ATTR_SERVER, (OCIError *) errhp);

	/* allocate a user context handle */
	status = OCIHandleAlloc((dvoid *)envhp, (dvoid **)&usrhp, (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);

	status = OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUser, (ub4)strlen(sConnectAsUser),
		OCI_ATTR_USERNAME, errhp);

	status = OCIAttrSet((dvoid *)usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUserPwd,
		(ub4)strlen(sConnectAsUserPwd), OCI_ATTR_PASSWORD, errhp);

	checkerr(&handles, OCISessionBegin (svchp, errhp, usrhp, OCI_CRED_RDBMS, OCI_DEFAULT));

	status = OCIAttrSet((dvoid *)svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)usrhp, (ub4)0, OCI_ATTR_SESSION, errhp);

	status = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &stmthp, (ub4) OCI_HTYPE_STMT, 50, (dvoid **) &tmp);

	// do something
	//@bug 1022
	char selvalstmt[32000];
	char temp[32000];
	//sprintf(selvalstmt, "BEGIN %s('%s', '%s', '%s', '%s'); END;", sProcedureName, arg1, arg2, arg3, arg4);
	selvalstmt[0] = 0;
	sprintf(temp, "BEGIN %s(", sProcedureName);
	strcat(selvalstmt, temp);
	if (arg1_i == OCI_IND_NOTNULL && strlen(arg1))
	{
		sprintf(temp, "'%s'", arg1);
		strcat(selvalstmt, temp);
		if (arg2_i == OCI_IND_NOTNULL && strlen(arg2))
		{
			sprintf(temp, ", '%s'", arg2);
			strcat(selvalstmt, temp);
			if (arg3_i == OCI_IND_NOTNULL && strlen(arg3))
			{
				sprintf(temp, ", '%s'", arg3);
				strcat(selvalstmt, temp);
				if (arg4_i == OCI_IND_NOTNULL && strlen(arg4))
				{
					sprintf(temp, ", '%s'", arg4);
					strcat(selvalstmt, temp);
					if (arg5_i == OCI_IND_NOTNULL && strlen(arg5))
					{
						sprintf(temp, ", '%s'", arg5);
						strcat(selvalstmt, temp);
						if (arg6_i == OCI_IND_NOTNULL && strlen(arg6))
						{
							sprintf(temp, ", '%s'", arg6);
							strcat(selvalstmt, temp);
						}
					}
				}
			}
		}
	}
	strcat(selvalstmt, "); END;");
	checkerr(&handles, OCIStmtPrepare(stmthp, errhp, (text*)selvalstmt, (ub4)strlen(selvalstmt), (ub4)OCI_NTV_SYNTAX,
		(ub4)OCI_DEFAULT));

	checkerr(&handles, OCIStmtExecute(svchp, stmthp, errhp, (ub4)1, (ub4)0, (OCISnapshot *)NULL, (OCISnapshot *)NULL,
		(ub4)OCI_DEFAULT));

	status = OCIHandleFree((dvoid *) stmthp, (ub4) OCI_HTYPE_STMT);

	status = OCISessionEnd(svchp, errhp, usrhp, (ub4)OCI_DEFAULT);
	status = OCIServerDetach( srvhp, errhp, (ub4) OCI_DEFAULT );
	status = OCIHandleFree((dvoid *) srvhp, (ub4) OCI_HTYPE_SERVER);
	status = OCIHandleFree((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX);
	status = OCIHandleFree((dvoid *) errhp, (ub4) OCI_HTYPE_ERROR);

	return 0;
}

#ifdef STANDALONE

namespace {
	const int MaxArgs = 10 + 1;
}

int main(int argc, char** argv)
{
	OCIExtProcContext* ctx = 0;

	short s[MaxArgs];

	if (argc < 6)
	{
		cerr << "Need at least 5 args" << endl;
		return 1;
	}

	for (int i = 0; i < MaxArgs; i++) s[i] = i < argc ? OCI_IND_NOTNULL : OCI_IND_NULL;

	return Execute_procedure(ctx, argv[1], s[1], argv[2], s[2], argv[3], s[3], argv[4], s[4], argv[5], s[5], argv[6], s[6],
		argv[7], s[7], argv[8], s[8], argv[9], s[9], argv[10], s[10]);
}
#endif
