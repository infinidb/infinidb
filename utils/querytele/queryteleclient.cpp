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

#include <unistd.h>
#include <stdint.h>
#include <string>
using namespace std;

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
namespace bu=boost::uuids;

#include "queryteleserverparms.h"
#include "querytele_types.h"
#include "QueryTeleService.h"
#include "queryteleprotoimpl.h"
#include "telestats.h"

#include "queryteleclient.h"

namespace
{
using namespace querytele;

#define QT_ASSIGN_(x) out. __set_ ## x (qts. x)
QueryTele qts2qt(const QueryTeleStats& qts)
{
	QueryTele out;

	out.query_uuid = bu::to_string(qts.query_uuid);
	switch (qts.msg_type)
	{
	case QueryTeleStats::QT_SUMMARY:
		out.msg_type = QTType::QT_SUMMARY;
		break;
	case QueryTeleStats::QT_PROGRESS:
		out.msg_type = QTType::QT_PROGRESS;
		break;
	case QueryTeleStats::QT_START:
		out.msg_type = QTType::QT_START;
		break;
	default:
		out.msg_type = QTType::QT_INVALID;
		break;
	}
	QT_ASSIGN_(max_mem_pct);
	QT_ASSIGN_(num_files);
	QT_ASSIGN_(phy_io);
	QT_ASSIGN_(cache_io);
	QT_ASSIGN_(msg_rcv_cnt);
	QT_ASSIGN_(cp_blocks_skipped);
	QT_ASSIGN_(msg_bytes_in);
	QT_ASSIGN_(msg_bytes_out);
	QT_ASSIGN_(rows);
	QT_ASSIGN_(start_time);
	QT_ASSIGN_(end_time);
	QT_ASSIGN_(error_no);
	QT_ASSIGN_(blocks_changed);
	QT_ASSIGN_(session_id);
	QT_ASSIGN_(query_type);
	QT_ASSIGN_(query);
	QT_ASSIGN_(user);
	QT_ASSIGN_(host);
	QT_ASSIGN_(priority);
	QT_ASSIGN_(priority_level);
	QT_ASSIGN_(system_name);
	QT_ASSIGN_(module_name);
	QT_ASSIGN_(local_query);
	QT_ASSIGN_(schema_name);

	return out;
}
#undef QT_ASSIGN_

#define QT_ASSIGN_(x) out. __set_ ## x (sts. x)
StepTele sts2st(const StepTeleStats& sts)
{
	StepTele out;

	out.query_uuid = bu::to_string(sts.query_uuid);
	switch (sts.msg_type)
	{
	case StepTeleStats::ST_SUMMARY:
		out.msg_type = STType::ST_SUMMARY;
		break;
	case StepTeleStats::ST_PROGRESS:
		out.msg_type = STType::ST_PROGRESS;
		break;
	case StepTeleStats::ST_START:
		out.msg_type = STType::ST_START;
		break;
	default:
		out.msg_type = STType::ST_INVALID;
		break;
	}
	out.step_uuid = bu::to_string(sts.step_uuid);
	QT_ASSIGN_(phy_io);
	QT_ASSIGN_(cache_io);
	QT_ASSIGN_(msg_rcv_cnt);
	QT_ASSIGN_(cp_blocks_skipped);
	QT_ASSIGN_(msg_bytes_in);
	QT_ASSIGN_(msg_bytes_out);
	QT_ASSIGN_(rows);
	QT_ASSIGN_(start_time);
	QT_ASSIGN_(end_time);
	QT_ASSIGN_(total_units_of_work);
	QT_ASSIGN_(units_of_work_completed);

	return out;
}
#undef QT_ASSIGN_

#define QT_ASSIGN_(x) out. __set_ ## x (its. x)
ImportTele its2it(const ImportTeleStats& its)
{
	ImportTele out;

	out.job_uuid = bu::to_string(its.job_uuid);
	out.import_uuid = bu::to_string(its.import_uuid);
	switch (its.msg_type)
	{
	case ImportTeleStats::IT_SUMMARY:
		out.msg_type = ITType::IT_SUMMARY;
		break;
	case ImportTeleStats::IT_PROGRESS:
		out.msg_type = ITType::IT_PROGRESS;
		break;
	case ImportTeleStats::IT_START:
		out.msg_type = ITType::IT_START;
		break;
	case ImportTeleStats::IT_TERM:
		out.msg_type = ITType::IT_TERM;
		break;
	default:
		out.msg_type = ITType::IT_INVALID;
		break;
	}
	QT_ASSIGN_(start_time);
	QT_ASSIGN_(end_time);
	QT_ASSIGN_(table_list);
	QT_ASSIGN_(rows_so_far);
	QT_ASSIGN_(system_name);
	QT_ASSIGN_(module_name);
	QT_ASSIGN_(schema_name);

	return out;
}
#undef QT_ASSIGN_

}

namespace querytele
{

QueryTeleClient::QueryTeleClient(const QueryTeleServerParms& sp) :
	fProtoImpl(0),
	fServerParms(sp)
{
	if (fServerParms.host.empty() || fServerParms.port == 0) return;
	fProtoImpl = new QueryTeleProtoImpl(fServerParms);
}

QueryTeleClient::~QueryTeleClient()
{
	delete fProtoImpl;
}

QueryTeleClient::QueryTeleClient(const QueryTeleClient& rhs) :
	fProtoImpl(0)
{
	fServerParms = rhs.fServerParms;
	if (rhs.fProtoImpl)
	{
		fProtoImpl = new QueryTeleProtoImpl(*rhs.fProtoImpl);
	}
}

QueryTeleClient& QueryTeleClient::operator=(const QueryTeleClient& rhs)
{
	if (&rhs != this)
	{
		fProtoImpl = 0;
		fServerParms = rhs.fServerParms;
		if (rhs.fProtoImpl)
		{
			fProtoImpl = new QueryTeleProtoImpl(*rhs.fProtoImpl);
		}
	}
	return *this;
}

void QueryTeleClient::serverParms(const QueryTeleServerParms& sp)
{
	fServerParms = sp;
	delete fProtoImpl;
	fProtoImpl = 0;
	if (fServerParms.host.empty() || fServerParms.port == 0) return;
	fProtoImpl = new QueryTeleProtoImpl(fServerParms);
}

void QueryTeleClient::postQueryTele(const QueryTeleStats& qts)
{
	if (!fProtoImpl) return;
	QueryTele qtdata = qts2qt(qts);
	fProtoImpl->enqQueryTele(qtdata);
}

#define QT_STYPE_CASE_(x) case StepTeleStats:: x: {stdata.step_type = StepType:: x; break;}
void QueryTeleClient::postStepTele(const StepTeleStats& sts)
{
	if (!fProtoImpl) return;
	StepTele stdata = sts2st(sts);
	switch (fStepParms.stepType)
	{
	QT_STYPE_CASE_(T_HJS);
	QT_STYPE_CASE_(T_DSS);
	QT_STYPE_CASE_(T_CES);
	QT_STYPE_CASE_(T_SQS);
	QT_STYPE_CASE_(T_TAS);
	QT_STYPE_CASE_(T_TNS);
	QT_STYPE_CASE_(T_BPS);
	QT_STYPE_CASE_(T_TCS);
	QT_STYPE_CASE_(T_HVS);
	QT_STYPE_CASE_(T_WFS);
	QT_STYPE_CASE_(T_SAS);
	QT_STYPE_CASE_(T_TUN);
	default:
		stdata.step_type = StepType::T_INVALID;
		break;
	}
	fProtoImpl->enqStepTele(stdata);
}
#undef QT_STYPE_CASE_

void QueryTeleClient::postImportTele(const ImportTeleStats& its)
{
	if (!fProtoImpl) return;
	ImportTele itdata = its2it(its);
	fProtoImpl->enqImportTele(itdata);
}

void QueryTeleClient::waitForQueues()
{
	if (fProtoImpl)
		fProtoImpl->waitForQueues();
}

}

