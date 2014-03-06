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

/*
 * $Id: ha_calpont_udf.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 */

#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
using namespace std;

#include "idb_mysql.h"

/** @brief Calpont User Defined Function Connector Stub */

extern "C"
{

// Connector function stub -- CPFUNC1
long long cpfunc1(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc1_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC1() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc1_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC2
long long cpfunc2(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc2_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC2() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc2_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC3
long long cpfunc3(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc3_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC3() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc3_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC4
long long cpfunc4(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc4_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC4() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc4_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC5
long long cpfunc5(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc5_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC5() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc5_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC6
long long cpfunc6(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc6_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC6() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc6_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC7
long long cpfunc7(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc7_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC7() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc7_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC8
long long cpfunc8(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc8_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC8() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc8_deinit(UDF_INIT* initid)
{
}

// Connector function stub -- CPFUNC9
long long cpfunc9(UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error)
{
	return 0;
}

my_bool cpfunc9_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"CALFUNC9() requires one COLUMN argument");
		return 1;
	}
	return 0;
}

void cpfunc9_deinit(UDF_INIT* initid)
{
}

}

// vim:sw=4 ts=4:

