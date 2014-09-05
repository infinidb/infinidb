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

// $Id: tdriver1.cpp 9210 2013-01-21 14:10:42Z rdempsey $
#include <iostream>
using namespace std;

#include "jobstep.h"
#include "distributedenginecomm.h"
#include "bandeddl.h"
using namespace joblist;

#include "calpontsystemcatalog.h"
using namespace execplan;

int main(int argc, char** argv)
{
	DistributedEngineComm* dec;
	boost::shared_ptr<CalpontSystemCatalog> cat;

	ResourceManager rm;
	dec = DistributedEngineComm::instance(rm);
	cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

	JobStepAssociation inJs;
	JobStepAssociation outJs;

	AnyDataListSPtr spdl1(new AnyDataList());
	FifoDataList* dl1 = new FifoDataList(1, 128);
	spdl1->fifoDL(dl1);
	outJs.outAdd(spdl1);

	pColScanStep step0(inJs, outJs, dec, cat, 1003, 1000, 12345, 999, 7, 0, 0, rm);
	int8_t cop;
	int64_t filterValue;
	cop = COMPARE_GE;
	filterValue = 3010;
	step0.addFilter(cop, filterValue);
	cop = COMPARE_LE;
	filterValue = 3318;
	step0.addFilter(cop, filterValue);
	step0.setBOP(BOP_AND);
	inJs = outJs;

	step0.run();

	step0.join();

	DeliveryStep step1(inJs, outJs, make_table("CALPONTSYS", "SYSTABLE"), cat, 1000, 0, 1, 0);
	inJs = outJs;

	step1.run();

	step1.join();

	return 0;
}

