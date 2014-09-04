/***********************************************************************
*   $Id$
*
*
***********************************************************************/

#include <cmath>
#include <iostream>
using namespace std;

#include "idb_mysql.h"
#define UDFSDK_DLLEXPORT
#include "udfsdk.h"
#undef UDFSDK_DLLEXPORT

#include "treenode.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"
using namespace funcexp;

namespace udfsdk
{
/**
 * UDFSDK definition
 */
UDFSDK::UDFSDK()
{
}

UDFSDK::~UDFSDK()
{
}

/**
 * All UDF functions should be registered in the function map. They will be
 * picked up by the InfiniDB F&E framework when the servers are started. 
 * That will make sure the UDF functions runs distributedly in InfiniDB
 * engines just like the internal InfiniDB functions.
 */
FuncMap UDFSDK::UDFMap() const
{
	FuncMap fm;
	
	// first: function name
	// second: Function pointer
	// please use lower case for the function name. Because the names might be 
	// case-insensitive in MySQL depending on the setting. In such case, 
	// the function names passed to the interface is always in lower case.
	fm["idb_add"] = new IDB_add();
	fm["idb_isnull"] = new IDB_isnull();
	
	return fm;
}

/***************************************************************************
 * IDB_ADD implementation 
 *
 * OperationType() definition
 */
CalpontSystemCatalog::ColType IDB_add::operationType (FunctionParm& fp, 
                       CalpontSystemCatalog::ColType& resultType)
{
	// operation type of idb_add is determined by the argument types
	assert (fp.size() == 2);
	CalpontSystemCatalog::ColType rt;
	if (fp[0]->data()->resultType() == fp[1]->data()->resultType())
	{
		rt = fp[0]->data()->resultType();
	}
	else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
		       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
		       fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
		       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
		       fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE ||
		       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE)
	{
		rt.colDataType = CalpontSystemCatalog::DOUBLE;
		rt.colWidth = 8;
	}
	else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
		       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
		       fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME ||
		       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME)
	{
		rt.colDataType = CalpontSystemCatalog::BIGINT;
		rt.colWidth = 8;
	}
	else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
	       fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL)
	{
		rt.colDataType = CalpontSystemCatalog::DECIMAL;
		rt.colWidth = 8;
	}
	else
	{
		rt.colDataType = CalpontSystemCatalog::BIGINT;
		rt.colWidth = 8;
	}
	return rt;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when an double value is needed to return from the UDF function
 */
double IDB_add::getDoubleVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	switch (op_ct.colDataType)
	{
		// The APIs for the evaluation of the function parameters are the same getXXXval()
		// functions. However, only two arguments are passed in, the current
		// row reference and the NULL indicator isNull.
		case CalpontSystemCatalog::BIGINT:
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::SMALLINT:
		case CalpontSystemCatalog::TINYINT:
			return ( parm[0]->data()->getIntVal(row, isNull) +
			         parm[1]->data()->getIntVal(row, isNull));
		case CalpontSystemCatalog::DOUBLE:
			return ( parm[0]->data()->getDoubleVal(row, isNull) +
			         parm[1]->data()->getDoubleVal(row, isNull));
		case CalpontSystemCatalog::DECIMAL:
		{
			IDB_Decimal dec;
			IDB_Decimal op1 = parm[0]->data()->getDecimalVal(row, isNull);
			IDB_Decimal op2 = parm[1]->data()->getDecimalVal(row, isNull);
			if (op1.scale == op2.scale)
			{
				dec.scale = op1.scale;
			}
			else if (op1.scale >= op2.scale)
			{
				dec.scale = op2.scale;
				op1.value *= (int64_t)pow((double)10, op1.scale-op2.scale);
			}
			else
			{
				dec.scale = op1.scale;
				op2.value *= (int64_t)pow((double)10, op2.scale-op1.scale);
			}
			dec.value = op1.value + op2.value;
			return (double)(dec.value / pow((double)10, dec.scale));
		}
		default:
			return ( parm[0]->data()->getDoubleVal(row, isNull) +
			         parm[1]->data()->getDoubleVal(row, isNull));
	}
	return 0;
}

float IDB_add::getFloatVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (float)getDoubleVal(row, parm, isNull, op_ct);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type idb_add is double(real), all the other API can simply call 
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementation.
 */
int64_t IDB_add::getIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

string IDB_add::getStrVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	// One will need a more efficient implementation if this API is frequently
	// called for this UDF function. This code is for demonstration purpose.
	ostringstream oss;
	oss << getDoubleVal(row, parm, isNull, op_ct);
	return oss.str();
}

IDB_Decimal IDB_add::getDecimalVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	IDB_Decimal dec;
	dec.value = getIntVal(row, parm, isNull, op_ct);
	dec.scale = 0;
	return dec;
}

/**
 * This API should never be called for IDB_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an IDB-5001 error will be thrown,
 * or throw a customized exception here.
 */
int32_t IDB_add::getDateIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	throw logic_error("Invalid API called for IDB_ADD");
}

/**
 * This API should never be called for IDB_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an IDB-5001 error will be thrown,
 * or throw a customized exception here.
 */
int64_t IDB_add::getDatetimeIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

bool IDB_add::getBoolVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return false;
}

/***************************************************************************
 * IDB_ISNULL implementation 
 *
 * OperationType() definition
 */
CalpontSystemCatalog::ColType IDB_isnull::operationType (FunctionParm& fp, 
                       CalpontSystemCatalog::ColType& resultType)
{
	// operation type of idb_isnull should be the same as the argument type
	assert (fp.size() == 1);
	return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 *
 * This would be the most commonly called API for idb_isnull function
 */
bool IDB_isnull::getBoolVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	switch (op_ct.colDataType)
	{
		// For the purpose of this function, one does not need to get the value of
		// the argument. One only need to know if the argument is NULL. The passed
		// in parameter isNull will be set if the parameter is evaluated NULL. 
		// Please note that before this function returns, isNull should be set to 
		// false, otherwise the result of the function would be considered NULL,
		// which is not possible for idb_isnull().
		case CalpontSystemCatalog::DECIMAL:
			parm[0]->data()->getDecimalVal(row, isNull);
			break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			parm[0]->data()->getStrVal(row, isNull);
			break;
		default:
			parm[0]->data()->getIntVal(row, isNull);
	}
	bool ret = isNull;
	// It's important to reset isNull indicator.
	isNull = false;
	return ret;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when a double value is needed to return from the UDF function
 */
double IDB_isnull::getDoubleVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

float IDB_isnull::getFloatVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type idb_add is double(real), all the other API can simply call 
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementations.
 */
int64_t IDB_isnull::getIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

string IDB_isnull::getStrVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	// This needs to be more efficient if this API will be frequently
	// called for this UDF function. 
		return (getBoolVal(row, parm, isNull, op_ct) ? "1" : "0");

}

IDB_Decimal IDB_isnull::getDecimalVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	IDB_Decimal dec;
	dec.value = (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
	dec.scale = 0;
	return dec;
}

int32_t IDB_isnull::getDateIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

int64_t IDB_isnull::getDatetimeIntVal(Row& row,
							FunctionParm& parm,
							bool& isNull,
							CalpontSystemCatalog::ColType& op_ct)
{
	return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

}

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

