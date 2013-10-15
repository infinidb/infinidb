#include <cmath>
#include <iostream>
#include <sstream>
using namespace std;

#include "idb_mysql.h"

namespace {
inline double cvtArgToDouble(int t, const char* v)
{
	double d = 0.0;
	switch (t)
	{
	case INT_RESULT:
		d = (double)(*((long long*)v));
		break;
	case REAL_RESULT:
		d = *((double*)v);
		break;
	case DECIMAL_RESULT:
	case STRING_RESULT:
		d = strtod(v, 0);
		break;
	case ROW_RESULT:
		break;
	}
	return d;
}
inline long long cvtArgToInt(int t, const char* v)
{
	long long ll = 0;
	switch (t)
	{
	case INT_RESULT:
		ll = *((long long*)v);
		break;
	case REAL_RESULT:
		ll = (long long)(*((double*)v));
		break;
	case DECIMAL_RESULT:
	case STRING_RESULT:
		ll = strtoll(v, 0, 0);
		break;
	case ROW_RESULT:
		break;
	}
	return ll;
}
inline string cvtArgToString(int t, const char* v)
{
	string str;
	switch (t)
	{
	case INT_RESULT:
	{
		long long ll;
		ll = *((long long*)v);
		ostringstream oss;
		oss << ll;
		str = oss.str();
		break;
	}
	case REAL_RESULT:
	{
		double d;
		d = *((double*)v);
		ostringstream oss;
		oss << d;
		str = oss.str();
		break;
	}
	case DECIMAL_RESULT:
	case STRING_RESULT:
		str = v;
		break;
	case ROW_RESULT:
		break;
	}
	return str;
}
}

/****************************************************************************
 * UDF function interface for MySQL connector to recognize is defined in
 * this section. MySQL's UDF function creation guideline needs to be followed.
 *
 * Three interface need to be defined on the connector for each UDF function.
 *
 * XXX_init: To allocate the necessary memory for the UDF function and validate
 *           the input.
 * XXX_deinit: To clean up the memory.
 * XXX: The function implementation.
 * Detailed instruction can be found at MySQL source directory:
 * ~/sql/udf_example.cc.
 *
 * Please note that the implementation of the function defined on the connector
 * will only be called when all the input arguments are constant. e.g., 
 * idb_add(2,3). That way, the function does not run in a distributed fashion 
 * and could be slow. If there is a need for the UDF function to run with
 * pure constant input, then one needs to put a implementation in the XXX
 * body, which is very similar to the ones in getXXXval API. If there's no
 * such need for a given UDF, then the XXX interface can just return a dummy
 * result because this function will never be called.
 */
extern "C"
{
/**
 * IDB_ADD connector stub
 */
#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idb_add_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 2)
	{
		strcpy(message,"idb_add() requires two argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idb_add_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
double idb_add(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	double op1, op2;

	op1 = cvtArgToDouble(args->arg_type[0], args->args[0]);
	op2 = cvtArgToDouble(args->arg_type[1], args->args[1]);

	return op1+op2;
}

/**
 * IDB_ISNULL connector stub
 */
 
#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idb_isnull_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idb_isnull() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idb_isnull_deinit(UDF_INIT* initid)
{
}	

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idb_isnull(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	return 0;
}

}
// vim:ts=4 sw=4:

