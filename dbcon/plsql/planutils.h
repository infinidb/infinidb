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
*   $Id: planutils.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef PLSQL_PLANUTILS_H_
#define PLSQL_PLANUTILS_H_

#ifndef OCI_ORACLE
#include <oci.h>
#endif
#ifndef ODCI_ORACLE
#include <odci.h>
#endif

#include <string>

#include "expressionparser.h"
#include "calpontselectexecutionplan.h"

namespace plsql
{
namespace planutils
{
extern OCIExtProcContext* myCtx;
extern int sessionid;
extern OCIError *errhp;
extern char curschema[];

extern void combine(execplan::ParseTree* n, void *stack);
extern void checkerr(OCIError* errhp, sword status);
extern void getPlanRecords(OCIStmt* stmthp);
extern execplan::ReturnedColumn* getColumn (std::string& token, bool& outerJoinFlag, bool& indexFlag);
extern execplan::Filter* getFilter (std::string& token);
extern void parseFilters (std::string& token, execplan::CalpontSelectExecutionPlan::FilterTokenList& list);
extern void doConversion(execplan::CalpontSelectExecutionPlan &csep);
extern int stateFunc(const char);
} //namespace plantuils
} //namespace plsql

#endif

