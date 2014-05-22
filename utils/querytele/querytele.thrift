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

namespace cpp querytele

enum QTType
{
  QT_INVALID,
  QT_SUMMARY,
  QT_PROGRESS,
  QT_START,
}

struct QueryTele
{
  1: string query_uuid ,	// UUID for this query
  2: QTType msg_type ,		// What kind of tele msg this is

  3: optional i64 max_mem_pct ,
  4: optional i64 num_files ,
  5: optional i64 phy_io ,
  6: optional i64 cache_io ,
  7: optional i64 msg_rcv_cnt ,
  8: optional i64 cp_blocks_skipped ,
  9: optional i64 msg_bytes_in ,
  10: optional i64 msg_bytes_out,
  11: optional i64 rows,
  12: optional i64 start_time,
  13: optional i64 end_time,
  14: optional i64 error_no,
  15: optional i64 blocks_changed,
  16: optional i64 session_id,
  17: optional string query_type,
  18: optional string query,
  19: optional string user,
  20: optional string host,
  21: optional string priority,
  22: optional i32 priority_level,
  23: optional string system_name,
  24: optional string module_name,
  25: optional i32 local_query,
  26: optional string schema_name,

}

enum STType
{
  ST_INVALID,
  ST_SUMMARY,
  ST_PROGRESS,
  ST_START,
}

enum StepType
{
  T_INVALID,
  T_HJS,		//TupleHashJoinStep
  T_DSS,		//DictionaryScanStep
  T_CES,		//CrossEngineStep
  T_SQS,		//SubQueryStep
  T_TAS,		//TupleAggregateStep
  T_TXS,		//TupleAnnexStep
  T_BPS,		//TupleBPS
  T_TCS,		//TupleConstantStep
  T_HVS,		//TupleHavingStep
  T_WFS,		//WindowFunctionStep
  T_SAS,		//SubAdapterStep
  T_TUN,		//TupleUnion
}

struct StepTele
{
  1: string query_uuid,		// UUID of the query this step is part of
  2: STType msg_type,		// What kind of tele msg this is
  3: StepType step_type,	// What kind of query step this is
  4: string step_uuid,		// UUID for this step

  5: optional i64 phy_io,
  6: optional i64 cache_io,
  7: optional i64 msg_rcv_cnt,
  8: optional i64 cp_blocks_skipped,
  9: optional i64 msg_bytes_in,
  10: optional i64 msg_bytes_out,
  11: optional i64 rows,
  12: optional i64 start_time,
  13: optional i64 end_time,

  14: optional i32 total_units_of_work,
  15: optional i32 units_of_work_completed,

}

enum ITType
{
  IT_INVALID,
  IT_SUMMARY,	// import job summary (good completion)
  IT_PROGRESS,	// import progress
  IT_START,	// import job start
  IT_TERM,	// import job terminiated (too many errs, ctrl-c, etc)
}

typedef list<string> StringList
typedef list<i64> I64List

struct ImportTele
{
  1: string job_uuid,
  2: string import_uuid,
  3: ITType msg_type,
  4: optional i64 start_time,
  5: optional i64 end_time,
  6: optional StringList table_list,
  7: optional I64List rows_so_far,
  8: optional string system_name,
  9: optional string module_name,
  10: optional string schema_name,
}

service QueryTeleService
{
  void postQuery(1: QueryTele query),
  void postStep(1: StepTele query),
  void postImport(1: ImportTele query),
}

