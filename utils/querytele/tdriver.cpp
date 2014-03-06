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

#include <iostream>
using namespace std;

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
namespace bu=boost::uuids;

#include "querytele.h"
using namespace querytele;

int main(int argc, char** argv)
{
	bu::random_generator rg;
	QueryTeleServerParms qtsp;
	qtsp.host = "localhost";
	qtsp.port = 9090;
	QueryTeleClient qtc(qtsp);
	QueryTeleClient qtc1(qtc);
	QueryTeleClient qtc2;
	qtc2 = qtc;
	QueryTeleStats qts;
	qts.query_uuid = rg();
	qts.msg_type = QueryTeleStats::QT_START;
	qts.query = "SELECT * FROM NATION;";
	qtc.postQueryTele(qts);

	sleep(1);

	StepTeleStats sts;
	sts.query_uuid = qts.query_uuid;
	sts.step_uuid = rg();
	sts.msg_type = StepTeleStats::ST_START;
	qtc.postStepTele(sts);

	sleep(1);

	sts.msg_type = StepTeleStats::ST_PROGRESS;
	qtc.postStepTele(sts);

	sleep(1);

	sts.msg_type = StepTeleStats::ST_SUMMARY;
	qtc.postStepTele(sts);

	sleep(1);

	qts.msg_type = QueryTeleStats::QT_SUMMARY;
	qtc.postQueryTele(qts);

	sleep(20);

	return 0;
}

