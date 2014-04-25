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
/******************************************************************************************
* $Id: genErrId.pl 3048 2012-04-04 15:33:45Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef LOGGING_ERRORIDS_H
#define LOGGING_ERRORIDS_H

namespace logging {

const unsigned ERR_MISS_JOIN = 1000;
const unsigned ERR_NON_SUPPORTED_FUNCTION = 1001;
const unsigned ERR_INCOMPATIBLE_JOIN = 1002;
const unsigned ERR_CIRCULAR_JOIN = 1003;
const unsigned ERR_MIX_JOIN = 1004;
const unsigned ERR_UPDATE_SUB = 1005;
const unsigned ERR_DATATYPE_NOT_SUPPORT = 1006;
const unsigned ERR_DML_NOT_SUPPORT_FEATURE = 1007;
const unsigned ERR_CREATE_DATATYPE_NOT_SUPPORT = 1008;
const unsigned ERR_ENTERPRISE_ONLY = 1009;
const unsigned ERR_AGGREGATE_TYPE_NOT_SUPPORT = 1010;
const unsigned ERR_DML_VIEW = 1011;
const unsigned ERR_UPDATE_NOT_SUPPORT_FEATURE = 1012;
const unsigned ERR_CREATE_AUTOINCREMENT_NOT_SUPPORT = 1013;
const unsigned ERR_ROLLUP_NOT_SUPPORT = 1014;
const unsigned ERR_OUTER_JOIN_SUBSELECT = 1015;
const unsigned ERR_PARTITION_BY_RANGE = 1016;
const unsigned ERR_SP_FUNCTION_NOT_SUPPORT = 1017;
const unsigned ERR_JOIN_TOO_BIG = 2001;
const unsigned ERR_UNION_TOO_BIG = 2002;
const unsigned ERR_AGGREGATION_TOO_BIG = 2003;
const unsigned ERR_LOST_CONN_EXEMGR = 2004;
const unsigned ERR_EXEMGR_MALFUNCTION = 2005;
const unsigned ERR_TABLE_NOT_IN_CATALOG = 2006;
const unsigned ERR_DICTBUFFER_OVERFLOW = 2007;
const unsigned ERR_VERSIONBUFFER_OVERFLOW = 2008;
const unsigned ERR_TABLE_LOCKED = 2009;
const unsigned ERR_ACTIVE_TRANSACTION = 2010;
const unsigned ERR_VIOLATE_NOT_NULL = 2011;
const unsigned ERR_EXTENT_DISK_SPACE = 2012;
const unsigned ERR_NON_NUMERIC_DATA = 2013;
const unsigned ERR_JOBLIST = 2014;
const unsigned ERR_ORDERBY_TOO_BIG = 2015;
const unsigned ERR_NON_SUPPORT_GROUP_BY = 2016;
const unsigned ERR_IN_DELIVERY = 2017;
const unsigned ERR_LIMIT_TOO_BIG = 2018;
const unsigned ERR_IN_PROCESS = 2019;
const unsigned ERR_MUL_ARG_AGG = 2020;
const unsigned ERR_NOT_GROUPBY_EXPRESSION = 2021;
const unsigned ERR_ORDERBY_NOT_IN_DISTINCT = 2022;
const unsigned ERR_NO_PRIMPROC = 2023;
const unsigned ERR_FUNC_MULTI_COL = 2024;
const unsigned WARN_DATA_TRUNC = 2025;
const unsigned ERR_AGG_IN_WHERE = 2026;
const unsigned ERR_NON_SUPPORT_AGG_ARGS = 2027;
const unsigned ERR_NO_FROM = 2028;
const unsigned ERR_LOCK_TABLE = 2029;
const unsigned ERR_FILTER_COND_EXP = 2030;
const unsigned ERR_BRM_LOOKUP = 2031;
const unsigned ERR_INCORRECT_VALUE = 2032;
const unsigned ERR_SYSTEM_CATALOG = 2033;
const unsigned ERR_DATA_OFFLINE = 2034;
const unsigned ERR_ASSERTION_FAILURE = 2035;
const unsigned ERR_PARTITION_NO_SCHEMA = 2036;
const unsigned ERR_INVALID_FUNC_ARGUMENT = 2037;
const unsigned ERR_O_DIRECT = 2038;
const unsigned ERR_ENOENT = 2039;
const unsigned WARN_NO_PARTITION_FOUND = 2040;
const unsigned SYSTABLE_PARTITION = 2041;
const unsigned WARN_NO_PARTITION_PERFORMED = 2042;
const unsigned ERR_DICTIONARY_SCAN = 2043;
const unsigned ERR_TUPLE_BPS = 2044;
const unsigned ERR_PRIMPROC_DOWN = 2045;
const unsigned ERR_READ_INPUT_DATALIST = 2046;
const unsigned ERR_LOCAL_QUERY_UM = 2047;
const unsigned ERR_NON_SUPPORT_SUB_QUERY_TYPE = 3001;
const unsigned ERR_MORE_THAN_1_ROW = 3002;
const unsigned ERR_MEMORY_MAX_FOR_LIMIT_TOO_LOW = 3003;
const unsigned ERR_CORRELATE_SCOPE_NOT_SUPPORTED = 3004;
const unsigned ERR_CORRELATED_DATA_TYPE_INCOMPATIBLE = 3005;
const unsigned ERR_INVALID_OPERATOR_WITH_LIST = 3006;
const unsigned ERR_CORRELATE_FAIL = 3007;
const unsigned ERR_AGG_EXISTS = 3008;
const unsigned ERR_UNKNOWN_COL = 3009;
const unsigned ERR_AMBIGUOUS_COL = 3010;
const unsigned ERR_NON_SUPPORT_ORDER_BY = 3011;
const unsigned ERR_NON_SUPPORT_SCALAR = 3012;
const unsigned ERR_UNION_IN_SUBQUERY = 3013;
const unsigned ERR_ALL_SOME_IN_SUBQUERY = 3014;
const unsigned ERR_NON_SUPPORT_HAVING = 3015;
const unsigned ERR_NON_SUPPORT_SELECT_SUB = 3016;
const unsigned ERR_NON_SUPPORT_DELETE_SUB = 3017;
const unsigned ERR_MISS_JOIN_IN_SUB = 3018;
const unsigned ERR_NON_SUPPORT_LIMIT_SUB = 3019;
const unsigned ERR_NON_SUPPORT_INSERT_SUB = 3020;
const unsigned ERR_SUB_EXPRESSION = 3021;
const unsigned ERR_NON_SUPPORT_FUNC_SUB = 3022;
const unsigned ERR_CORRELATED_SUB_OR = 3033;
const unsigned ERR_CORRELATE_COL_MISSING = 3034;
const unsigned ERR_NON_SUPPORT_NEQ_AGG_SUB = 3035;
const unsigned ERR_INVALID_LAST_PARTITION = 4001;
const unsigned ERR_PARTITION_ALREADY_DISABLED = 4002;
const unsigned ERR_PARTITION_NOT_EXIST = 4003;
const unsigned ERR_PARTITION_ALREADY_ENABLED = 4004;
const unsigned NO_VALID_TRANSACTION_ID = 4005;
const unsigned ERR_INVALID_START_VALUE = 4006;
const unsigned ERR_INVALID_COMPRESSION_TYPE = 4007;
const unsigned ERR_INVALID_AUTOINCREMENT_TYPE = 4008;
const unsigned ERR_INVALID_NUMBER_AUTOINCREMENT = 4009;
const unsigned ERR_NEGATIVE_STARTVALUE = 4010;
const unsigned ERR_INVALID_STARTVALUE = 4011;
const unsigned ERR_EXCEED_LIMIT = 4012;
const unsigned ERR_INVALID_VARBINARYVALUE = 4013;
const unsigned ERR_CONSTRAINTS = 4014;
const unsigned ERR_NOT_NULL_CONSTRAINTS = 4015;
const unsigned ERR_DML_DDL_SLAVE = 4016;
const unsigned ERR_DML_DDL_LOCAL = 4017;
const unsigned ERR_NON_SUPPORT_SYNTAX = 4018;
const unsigned ERR_FUNC_NON_IMPLEMENT = 5001;
const unsigned ERR_PSEUDOCOL_IDB_ONLY = 5002;
const unsigned ERR_PSEUDOCOL_WRONG_ARG = 5003;
const unsigned ERR_NETWORK = 6001;
const unsigned ERR_BRM_MUTEX = 6002;
const unsigned ERR_UNRECOVERABLE_LOCK_STATE = 6003;
const unsigned ERR_RECOVERABLE_LOCK_STATE = 6004;
const unsigned ERR_SUCCESSFUL_RECOVERY = 6005;
const unsigned ERR_HARD_FAILURE = 6006;
const unsigned ERR_NON_IDB_TABLE = 7001;
const unsigned ERR_CROSS_ENGINE_CONFIG = 8001;
const unsigned ERR_CROSS_ENGINE_CONNECT = 8002;
const unsigned ERR_WF_NON_SUPPORT = 9001;
const unsigned ERR_WF_WINDOW_WITHOUT_ORDER = 9002;
const unsigned ERR_WF_FUNCTION_NOT_EXISTS = 9003;
const unsigned ERR_WF_INVALID_WINDOW = 9004;
const unsigned ERR_WF_ORDER_BY_DISTINCT = 9005;
const unsigned ERR_WF_INVALID_BOUND = 9006;
const unsigned ERR_WF_INVALID_ORDER_KEY = 9007;
const unsigned ERR_WF_BOUND_OUT_OF_RANGE = 9008;
const unsigned ERR_WF_WINDOW_FUNC_NOT_ALLOWED = 9009;
const unsigned ERR_WF_INVALID_ORDER_TYPE = 9010;
const unsigned ERR_WF_INVALID_BOUND_TYPE = 9011;
const unsigned ERR_WF_WRONG_ARGS = 9012;
const unsigned ERR_WF_ORDER_MISSING = 9013;
const unsigned ERR_WF_WINDOW_CLAUSE = 9014;
const unsigned ERR_WF_UPDATE = 9015;
const unsigned ERR_WF_ORDER_BY = 9016;
const unsigned ERR_WF_INVALID_ORDER_KEY_WITHIN = 9017;
const unsigned ERR_WF_NOT_SUPPORT = 9018;
const unsigned ERR_WF_UNKNOWN_COL_TYPE = 9019;
const unsigned ERR_EXECUTE_WINDOW_FUNCTION = 9020;
const unsigned ERR_WINDOW_FUNCTION_WRAPPER = 9021;
const unsigned ERR_WF_INVALID_PARM_TYPE = 9022;
const unsigned ERR_WF_OVERFLOW = 9023;
const unsigned ERR_WF_COLUMN_MISSING = 9024;
const unsigned ERR_WF_UNKNOWN_BOUND = 9025;
const unsigned ERR_WF_NOT_IN_COL_MAP = 9026;
const unsigned ERR_WF_ARG_OUT_OF_RANGE = 9027;
const unsigned ERR_WF_NOT_ALLOWED = 9028;
const unsigned ERR_WF_IDB_ONLY = 9029;
const unsigned ERR_WF_DATA_SET_TOO_BIG = 9030;

}//namespace logging

#endif //LOGGING_ERRORIDS_H

