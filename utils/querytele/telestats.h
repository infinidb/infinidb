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

#ifndef TELESTATS_H__
#define TELESTATS_H__

#include <unistd.h>
#include <stdint.h>
#include <string>
#include <ctime>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace querytele
{

struct QueryTeleStats
{
	enum QTType
	{
		QT_INVALID,
		QT_SUMMARY,
		QT_PROGRESS,
		QT_START,
	};

	QueryTeleStats() :
		msg_type(QT_INVALID),
		max_mem_pct(0),
		num_files(0),
		phy_io(0),
		cache_io(0),
		msg_rcv_cnt(0),
		cp_blocks_skipped(0),
		msg_bytes_in(0),
		msg_bytes_out(0),
		rows(0),
		start_time(-1),
		end_time(-1),
		error_no(0),
		blocks_changed(0),
		session_id(0),
		priority_level(0),
		local_query(0)
	{ query_uuid = boost::uuids::nil_generator()(); }

	~QueryTeleStats() { }

	boost::uuids::uuid query_uuid;
	QTType msg_type;
	int64_t max_mem_pct;
	int64_t num_files;
	int64_t phy_io;
	int64_t cache_io;
	int64_t msg_rcv_cnt;
	int64_t cp_blocks_skipped;
	int64_t msg_bytes_in;
	int64_t msg_bytes_out;
	int64_t rows;
	int64_t start_time;
	int64_t end_time;
	int64_t error_no;
	int64_t blocks_changed;
	int64_t session_id;
	std::string query_type;
	std::string query;
	std::string user;
	std::string host;
	std::string priority;
	int32_t priority_level;
	std::string system_name;
	std::string module_name;
	int32_t local_query;
	std::string schema_name;
};

struct StepTeleStats
{
	enum STType
	{
		ST_INVALID,
		ST_SUMMARY,
		ST_PROGRESS,
		ST_START,
	};

	enum StepType
	{
		T_INVALID,
		T_HJS,		//TupleHashJoinStep
		T_DSS,		//DictionaryScanStep
		T_CES,		//CrossEngineStep
		T_SQS,		//SubQueryStep
		T_TAS,		//TupleAggregateStep
		T_TNS,		//TupleAnnexStep
		T_BPS,		//TupleBPS
		T_TCS,		//TupleConstantStep
		T_HVS,		//TupleHavingStep
		T_WFS,		//WindowFunctionStep
		T_SAS,		//SubAdapterStep
		T_TUN,		//TupleUnion
	};

	StepTeleStats() :
		msg_type(ST_INVALID),
		step_type(T_INVALID),
		phy_io(0),
		cache_io(0),
		msg_rcv_cnt(0),
		cp_blocks_skipped(0),
		msg_bytes_in(0),
		msg_bytes_out(0),
		rows(0),
		start_time(-1),
		end_time(-1),
		total_units_of_work(0),
		units_of_work_completed(0)
	{ query_uuid = boost::uuids::nil_generator()();
	step_uuid = boost::uuids::nil_generator()(); }

	~StepTeleStats() { }

	boost::uuids::uuid query_uuid;
	STType msg_type;
	StepType step_type;
	boost::uuids::uuid step_uuid;
	int64_t phy_io;
	int64_t cache_io;
	int64_t msg_rcv_cnt;
	int64_t cp_blocks_skipped;
	int64_t msg_bytes_in;
	int64_t msg_bytes_out;
	int64_t rows;
	int64_t start_time;
	int64_t end_time;
	int32_t total_units_of_work;
	int32_t units_of_work_completed;
};

struct ImportTeleStats
{
	typedef std::vector<std::string> StringList;
	typedef std::vector<int64_t> I64List;

	enum ITType
	{
		IT_INVALID,
		IT_SUMMARY,
		IT_PROGRESS,
		IT_START,
		IT_TERM,
	};

	ImportTeleStats() :
		msg_type(IT_INVALID),
		start_time(-1),
		end_time(-1)
	{ job_uuid = boost::uuids::nil_generator()();
	import_uuid = boost::uuids::nil_generator()(); }

	boost::uuids::uuid job_uuid;
	boost::uuids::uuid import_uuid;
	ITType msg_type;
	int64_t start_time;
	int64_t end_time;
	StringList table_list;
	I64List rows_so_far;
	std::string system_name;
	std::string module_name;
	std::string schema_name;
};

}

#endif

