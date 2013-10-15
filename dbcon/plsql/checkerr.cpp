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
* $Id: checkerr.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*************************/

#include <cstring>
#include <cstdio>

#ifndef OCI_ORACLE
# include <oci.h>
#endif

#include "checkerr.h"

/***********************************************************************/
/* Check the error status and throw exception if necessary */

int checkerr(Handles_t* handles, sword status, const char* info)
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

	OCIExtProcRaiseExcpWithMsg(handles->extProcCtx, 29400, errbuf, strlen((char*)errbuf));

	return -1;
}

int GetHandles(OCIExtProcContext* extProcCtx, Handles_t* handles)
{
	/* store the ext-proc context in the handles struct */
	handles->extProcCtx=extProcCtx;

	/* Get OCI handles */
	if (checkerr(handles, OCIExtProcGetEnv(extProcCtx, &handles->envhp, &handles->svchp, &handles->errhp)))
		return -1;

	/* get the user handle */
	if (checkerr(handles, OCIAttrGet((dvoid*)handles->svchp, (ub4)OCI_HTYPE_SVCCTX, (dvoid*)&handles->usrhp, (ub4*)0,
		(ub4)OCI_ATTR_SESSION, handles->errhp)))
		return -1;

	return 0;
}

