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

/***********************************************************************
*   $Id: orasession.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#ifndef OCI_ORACLE
#include <oci.h>
#endif
#ifndef ODCI_ORACLE
#include <odci.h>
#endif

#include <string>
#include <iostream>
#include <stdexcept>
#include "orasession.h"
namespace plsql
{

OraSession::OraSession() { 
	connectedUser = "";
}

int OraSession::checkerr(OCIHandles_t* handles, sword status, const char* info)
{
	text errbuf[512];     /* error message buffer */
	text errbuf1[512];     /* error message buffer */
	sb4 errcode;          /* OCI error code */

	switch (status)
	{
	case OCI_SUCCESS:
	case OCI_SUCCESS_WITH_INFO:
		return 0;
	case OCI_ERROR:
		OCIErrorGet((dvoid*)handles->errhp, (ub4)1, (text *)NULL, &errcode, errbuf1, (ub4)sizeof(errbuf1),
			(ub4)OCI_HTYPE_ERROR);
		sprintf((char*)errbuf, "%s: OCI ERROR code %d: %s", info, errcode, (char*)errbuf1);
		break;
	case OCI_INVALID_HANDLE:
		sprintf((char*)errbuf, "Error - OCI_INVALID_HANDLE: %s", info);
		break;
	case OCI_NEED_DATA:
		sprintf((char*)errbuf, "Error - OCI_NEED_DATA\n");
		break;
	case OCI_NO_DATA:
		sprintf((char*)errbuf, "Error - OCI_NO_DATA\n");
		break;
	case OCI_STILL_EXECUTING:
		sprintf((char*)errbuf, "Error - OCI_STILL_EXECUTE\n");
		break;
	case OCI_CONTINUE:
		sprintf((char*)errbuf, "Error - OCI_CONTINUE\n");
		break;
	default:
		sprintf((char*)errbuf, "Warning - error status %d: %s",status, info);
		break;
	}

	std::cout << errbuf << std::endl;

	throw std::runtime_error((char*)errbuf);
}

inline int OraSession::checkerr(OCIHandles_t* ociHandles, sword status) { return checkerr(ociHandles, status, "OCI"); }

// Begins an Oracle session.
void OraSession::startSession(const std::string& sConnectAsUser, const std::string& sConnectAsUserPwd,
		const std::string& sConnectDBLink) 
{
	dvoid* tmp;
	sword status;

	status = OCIInitialize((ub4) OCI_THREADED | OCI_OBJECT, 0, 0, 0, 0);

	status = OCIHandleAlloc((dvoid *)NULL, (dvoid **)&ociHandles.envhp, (ub4)OCI_HTYPE_ENV, 52, (dvoid **)&tmp);

	status = OCIEnvInit(&ociHandles.envhp, (ub4)OCI_DEFAULT, 21, (dvoid **)&tmp);

	status = OCIHandleAlloc((dvoid *)ociHandles.envhp, (dvoid **)&ociHandles.errhp, (ub4)OCI_HTYPE_ERROR, 52, (dvoid **)&tmp);

	status = OCIHandleAlloc((dvoid *)ociHandles.envhp, (dvoid **)&ociHandles.srvhp, (ub4)OCI_HTYPE_SERVER, 52, (dvoid **)&tmp);

	if(checkerr(&ociHandles, OCIServerAttach(ociHandles.srvhp, ociHandles.errhp, (text *)sConnectDBLink.c_str(),
		(sb4)sConnectDBLink.length(), (ub4)OCI_DEFAULT)) != 0) {
		std::cout << "OCIServerAttach failed." << std::endl << std::endl;
		return;
	}

	status = OCIHandleAlloc((dvoid *)ociHandles.envhp, (dvoid **)&ociHandles.svchp, 
		(ub4)OCI_HTYPE_SVCCTX, 52, (dvoid **)&tmp);

	/* set attribute server context in the service context */
	status = OCIAttrSet((dvoid *) ociHandles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)ociHandles.srvhp, (ub4)0,
		(ub4)OCI_ATTR_SERVER, (OCIError *)ociHandles.errhp);

	/* allocate a user context handle */
	status = OCIHandleAlloc((dvoid *)ociHandles.envhp, (dvoid **)&ociHandles.usrhp, (ub4)OCI_HTYPE_SESSION, (size_t)0,
		(dvoid **)0);

	status = OCIAttrSet((dvoid *)ociHandles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUser.c_str(),
		(ub4)sConnectAsUser.length(), OCI_ATTR_USERNAME, ociHandles.errhp);

	status = OCIAttrSet((dvoid *)ociHandles.usrhp, (ub4)OCI_HTYPE_SESSION, (dvoid *)sConnectAsUserPwd.c_str(),
		(ub4)sConnectAsUserPwd.length(), OCI_ATTR_PASSWORD, ociHandles.errhp);

	if(checkerr(&ociHandles, OCISessionBegin(ociHandles.svchp, ociHandles.errhp, ociHandles.usrhp, OCI_CRED_RDBMS, OCI_DEFAULT)) != 0) {
		std::cout << "OCISessionBegin failed." << std::endl << std::endl;
		return;
	}

	status = OCIAttrSet((dvoid *)ociHandles.svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid *)ociHandles.usrhp, (ub4)0, OCI_ATTR_SESSION,
		ociHandles.errhp);

	status = OCIHandleAlloc((dvoid *)ociHandles.envhp, (dvoid **)&ociHandles.stmthp, (ub4)OCI_HTYPE_STMT, 50, (dvoid **)&tmp);

	connectedUser = sConnectAsUser;
}

void OraSession::issueStatement(const std::string& stmt) {
	
	try {

		if(checkerr(&ociHandles, OCIStmtPrepare(ociHandles.stmthp, ociHandles.errhp, (text*)stmt.c_str(),
			(ub4)strlen(stmt.c_str()), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT)) != 0) {
			std::cout << "OCIStmtPrepare((dvoid *)ociHandles.envhp, (dvoid **)&stmthp failed." << std::endl << std::endl;
			return;
		}
		
		if(checkerr(&ociHandles, OCIStmtExecute(ociHandles.svchp, ociHandles.stmthp, ociHandles.errhp, (ub4)1, (ub4)0,
			(OCISnapshot *)NULL, (OCISnapshot *)NULL, (ub4)OCI_DEFAULT)) != 0) {
			std::cout << "OCIStmtExecute((dvoid *)ociHandles.envhp, (dvoid **)&stmthp failed." << std::endl << std::endl;
			return;
		}
	} catch(std::runtime_error e) {
		throw std::runtime_error("Error executing Statement:  " + stmt + "\n" + e.what());
	}
}

std::string OraSession::getPasswordForUser(const std::string& user) {
	char password[4096];
	OCIBind *bnd1p = 0;

	std::string stmt = "begin select password into :password from sys.dba_users where username = upper('" + user + "'); end;";

	checkerr(&ociHandles, OCIStmtPrepare(ociHandles.stmthp, ociHandles.errhp, (text*)stmt.c_str(),
		(ub4)strlen(stmt.c_str()), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

	password[0] = 0;

	checkerr(&ociHandles, OCIBindByName(ociHandles.stmthp, (OCIBind **)&bnd1p, ociHandles.errhp,
		(text *)":password", (sb4)strlen(":password"),
		(dvoid *)password, (sb4)4096,  SQLT_STR, (dvoid *)0,
		(ub2 *)0, (ub2 *)0, (ub4)0, (ub4 *)0,(ub4)OCI_DEFAULT));            

	checkerr(&ociHandles, OCIStmtExecute(ociHandles.svchp, ociHandles.stmthp, ociHandles.errhp, (ub4)1, (ub4)0, (OCISnapshot *)NULL,
		(OCISnapshot *)NULL, (ub4)OCI_DEFAULT));


	return password;
	
}

void OraSession::endSession() {
	sword status;

	status = OCIHandleFree((dvoid *)ociHandles.stmthp, (ub4)OCI_HTYPE_STMT);
	status = OCISessionEnd(ociHandles.svchp, ociHandles.errhp, ociHandles.usrhp, (ub4)OCI_DEFAULT);
	status = OCIServerDetach(ociHandles.srvhp, ociHandles.errhp, (ub4)OCI_DEFAULT );
	status = OCIHandleFree((dvoid *)ociHandles.srvhp, (ub4)OCI_HTYPE_SERVER);
	status = OCIHandleFree((dvoid *)ociHandles.svchp, (ub4)OCI_HTYPE_SVCCTX);
	status = OCIHandleFree((dvoid *)ociHandles.errhp, (ub4)OCI_HTYPE_ERROR);
	connectedUser = "";
}

std::string OraSession::getConnectedUser() {
	return connectedUser;
}
	
} //namespace plsql



