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
* $Id: checkerr.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*************************/
/** @file */

#ifndef __CHECKERR_H__
#define __CHECKERR_H__

#ifndef OCI_ORACLE
# include <oci.h>
#endif

/* OCI Handles */
struct Handles_t_
{
	OCIExtProcContext* extProcCtx;
	OCIEnv* envhp;
	OCISvcCtx* svchp;
	OCIError* errhp;
	OCISession* usrhp;
};
typedef struct Handles_t_ Handles_t;

/***********************************************************************/
/* Check the error status and throw exception if necessary */

extern int checkerr(Handles_t* handles, sword status, const char* info);

inline int checkerr(Handles_t* handles, sword status) { return checkerr(handles, status, "OCI"); }

extern int GetHandles(OCIExtProcContext* extProcCtx, Handles_t* handles);

#endif

