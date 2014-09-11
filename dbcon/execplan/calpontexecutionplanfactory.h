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
*   $Id: calpontexecutionplanfactory.h 7409 2011-02-08 14:38:50Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef CALPONTEXECUTIONPLANFACTORY_H
#define CALPONTEXECUTIONPLANFACTORY_H

#include "calpontexecutionplan.h"
#include "vendorexecutionplan.h"

namespace execplan { 

class CalpontExecutionPlanFactory {

public:
	//static CalpontExecutionPlan* makeCalpontExecutionPlan(const VendorExecutionPlan* vplan);

};

}  //namespace execplan
#endif //CALPONTEXECUTIONPLANFACTORY_H

