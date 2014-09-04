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

/***********************************************************************
*   $Id: jlf_execplantojoblist.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

/** @file jlf_execplantojoblist.h
 *
 */
#ifndef JLF_EXECPLANTOJOBLIST_H__
#define JLF_EXECPLANTOJOBLIST_H__

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

namespace joblist
{

//------------------------------------------------------------------------------
/** @brief Class makes initial pass at converting Calpont Exec Plan to a joblist
 *
 */
//------------------------------------------------------------------------------
class JLF_ExecPlanToJobList
{
public:
	/** @brief This function is the entry point into CEP to joblist conversion
	 *
	 * @param ParseTree (in) is CEP to be translated to a joblist
	 * @param void*     (in/out)is the JobInfo pointer that is loaded
	 */
	static void walkTree(ParseTree* n, void* obj);

private:
	// Disable constructor since this class only contains a static method
	JLF_ExecPlanToJobList();
};

} // end of joblist namespace

#endif
