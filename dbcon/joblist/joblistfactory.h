/* Copyright (C) 2013 Calpont Corp.

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
*   $Id: joblistfactory.h 9655 2013-06-25 23:08:13Z xlou $
*
*
***********************************************************************/
/** @file */

#ifndef JOBLISTFACTORY_H
#define JOBLISTFACTORY_H

#include <string>

#include "joblist.h"

#if defined(_MSC_VER) && defined(JOBLISTFACTORY_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace execplan
{
class CalpontExecutionPlan;
}

namespace joblist
{

class ResourceManager;
/** @brief create a JobList object from a CalpontExecutionPlan object
 *
 * Class JobListFactory creates a JobList object from a CalpontExecutionPlan object
 */

/*
 * For right now, this class just has static methods. In general, a singleton is
 * preferred for this, since you can't override static member functions (and thus
 * can't inherit from this class). This class can be easily enough converted into
 * a singleton if necessary later.
 */
class JobListFactory
{

public:
	/** @brief static JobList constructor method
	 *
	 * @param cplan the CalpontExecutionPlan from which the JobList is constructed
	 */
	EXPORT static SJLP makeJobList(
		execplan::CalpontExecutionPlan* cplan,
		ResourceManager& rm,
		bool tryTuple=false,
		bool isExeMgr = false);

private:

};

}

#undef EXPORT

#endif //JOBLISTFACTORY_H

