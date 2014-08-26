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

//  $Id: jlf_subquery.h 6383 2010-03-24 04:38:39Z xlou $


/** @file */

#ifndef JLF_SUBQUERY_H
#define JLF_SUBQUERY_H

#include "jlf_common.h"

namespace execplan
{

class ParseTree;
class SimpleScalarFilter;
}


namespace joblist
{

bool simpleScalarFilterToParseTree(SimpleScalarFilter* sf, ParseTree*& pt, JobInfo& jobInfo);

void addOrderByAndLimit(execplan::CalpontSelectExecutionPlan*, JobInfo&);

void doExistsFilter(const execplan::ParseTree*, JobInfo&);

int  doFromSubquery(execplan::CalpontExecutionPlan*, const std::string&, const std::string&, JobInfo&);

void doSelectFilter(const execplan::ParseTree*, JobInfo&);

void doSimpleScalarFilter(execplan::ParseTree*, JobInfo&);

void preprocessHavingClause(execplan::CalpontSelectExecutionPlan*, JobInfo&);

void preprocessSelectSubquery(execplan::CalpontSelectExecutionPlan*, JobInfo&);

SJSTEP doUnionSub(execplan::CalpontExecutionPlan*, JobInfo&);

}

#endif  // JLF_SUBQUERY_H

