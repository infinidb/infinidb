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
*   $Id: getplan.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef CALPONT_GETPLAN_H
#define CALPONT_GETPLAN_H

#ifndef OCI_ORACLE
#include <oci.h>
#endif
#ifndef ODCI_ORACLE
#include <odci.h>
#endif

#include "calpontselectexecutionplan.h"

/*struct BindValue
{
  OCIString* sqltext;
  OCIString* bindVar;
  OCIString* bindValue;
};
typedef struct BindValue BindValue;
*/
/* Table function's output collection type */

typedef OCITable BindValueSet;


/** @brief the wrapper function to Oracle stored function cal_get_explain_plan 
 */
extern int getPlan( OCIExtProcContext* ctx,
            int sessionId,
            const char* curSchemaName,
            execplan::CalpontSelectExecutionPlan &plan,
            BindValueSet* bindValList );

extern void envInit(const char* sConnectAsUser, 
             const char* sConnectAsUserPwd,
             const char* sConnectDBLink,
             const char* sProcedureName);

extern void envCleanup();

extern string get_sql_text(int sessionID);

#endif

