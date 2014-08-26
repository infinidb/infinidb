#include <cmath>
#include <iostream>
#include <sstream>
using namespace std;

#include "idb_mysql.h"

#include "errorids.h"
#include "idberrorinfo.h"
#include "exceptclasses.h"
using namespace logging;

#include "pseudocolumn.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
using namespace execplan;

#include "functor.h"
#include "functor_str.h"

#include "ha_calpont_impl_if.h"
using namespace cal_impl_if;

namespace {

/*******************************************************************************
 * Pseudo column connector interface
 *
 * idbdbroot
 * idbpm
 * idbextentrelativerid
 * idbsegmentdir
 * idbsegment
 * idbpartition
 * idbextentmin
 * idbextentmax
 * idbextentid
 * idbblockid
 *
 * All the pseudo column functions are only executed in InfiniDB.
 */

void bailout(char* error, const string& funcName)
{
	string errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PSEUDOCOL_IDB_ONLY, funcName);
	current_thd->get_stmt_da()->set_overwrite_status(true);
	current_thd->get_stmt_da()->set_error_status(HA_ERR_INTERNAL_ERROR, errMsg.c_str(), mysql_errno_to_sqlstate(HA_ERR_INTERNAL_ERROR), 0);

	*error = 1;
}

int64_t idblocalpm()
{
	THD* thd = current_thd;
	if (!thd->infinidb_vtable.cal_conn_info)
		thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(thd->infinidb_vtable.cal_conn_info);

	if (ci->localPm == -1)
	{
		string module = ClientRotator::getModule();
		if (module.size() >= 3 && (module[0] == 'p' || module[0] == 'P'))
			ci->localPm = atol(module.c_str()+2);
		else
			ci->localPm = 0;
	}
	return ci->localPm;
}

extern "C"
{
/**
 * IDBDBROOT
 */
#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbdbroot_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbdbroot() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbdbroot_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbdbroot(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbdbroot");
	return 0;
}

/**
 * IDBPM
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbpm_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbpm() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbpm_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbpm(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbpm");
	return 0;
}

/**
 * IDBEXTENTRELATIVERID
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbextentrelativerid_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbextentrelativerid() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbextentrelativerid_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbextentrelativerid(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbextentrelativerid");
	return 0;
}

/**
 * IDBBLOCKID
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbblockid_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbblockid() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbblockid_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbblockid(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbblockid");
	return 0;
}

/**
 * IDBEXTENTID
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbextentid_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbextentid() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbextentid_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbextentid(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbextentid");
	return 0;
}

/**
 * IDBSEGMENT
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbsegment_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbsegment() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbsegment_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbsegment(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbsegment");
	return 0;
}

/**
 * IDBSEGMENTDIR
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbsegmentdir_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbsegmentdir() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbsegmentdir_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idbsegmentdir(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	bailout(error, "idbsegmentdir");
	return 0;
}

/**
 * IDBPARTITION
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbpartition_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbpartition() requires one argument");
		return 1;
	}

	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbpartition_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* idbpartition(UDF_INIT* initid, UDF_ARGS* args,
					char* result, unsigned long* length,
					char* is_null, char* error)
{
	bailout(error, "idbpartition");
	return result;
}

/**
 * IDBEXTENTMIN
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbextentmin_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbpm() requires one argument");
		return 1;
	}
	initid->maybe_null = 1;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbextentmin_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* idbextentmin(UDF_INIT* initid, UDF_ARGS* args,
                         char* result, unsigned long* length,
                         char* is_null, char* error)
{
	bailout(error, "idbextentmin");
	return result;
}

/**
 * IDBEXTENTMAX
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idbextentmax_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1)
	{
		strcpy(message,"idbextentmax() requires one argument");
		return 1;
	}
	initid->maybe_null = 1;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idbextentmax_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
const char* idbextentmax(UDF_INIT* initid, UDF_ARGS* args,
                         char* result, unsigned long* length,
                         char* is_null, char* error)
{
	bailout(error, "idbextentmax");
	return result;
}

/**
 * IDBLOCALPM
 */

#ifdef _MSC_VER
__declspec(dllexport)
#endif
my_bool idblocalpm_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 0)
	{
		strcpy(message,"idblocalpm() should take no argument");
		return 1;
	}
	initid->maybe_null = 1;
	return 0;
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
void idblocalpm_deinit(UDF_INIT* initid)
{
}

#ifdef _MSC_VER
__declspec(dllexport)
#endif
long long idblocalpm(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
	longlong localpm = idblocalpm();
	if (localpm == 0)
		*is_null = 1;
	return localpm;
}

}

}

namespace cal_impl_if {

ReturnedColumn* nullOnError(gp_walk_info& gwi, string& funcName)
{
	gwi.fatalParseError = true;
	gwi.parseErrorText =
	   logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_PSEUDOCOL_WRONG_ARG, funcName);
	return NULL;
}

uint32_t isPseudoColumn(string funcName)
{
	return execplan::PseudoColumn::pseudoNameToType(funcName);
}

execplan::ReturnedColumn* buildPseudoColumn(Item* item,
                                            gp_walk_info& gwi,
                                            bool& nonSupport,
                                            uint32_t pseudoType)
{
	if (!(gwi.thd->infinidb_vtable.cal_conn_info))
		gwi.thd->infinidb_vtable.cal_conn_info = (void*)(new cal_connection_info());
	cal_connection_info* ci = reinterpret_cast<cal_connection_info*>(gwi.thd->infinidb_vtable.cal_conn_info);

	Item_func* ifp = (Item_func*)item;

	// idblocalpm is replaced by constant
	if (pseudoType == PSEUDO_LOCALPM)
	{
		int64_t localPm = idblocalpm();
		ConstantColumn* cc;
		if (localPm)
			cc = new ConstantColumn(localPm);
		else
			cc = new ConstantColumn("", ConstantColumn::NULLDATA);
		cc->alias(ifp->full_name()? ifp->full_name() : "");
		return cc;
	}

	// convert udf item to pseudocolumn item.
	// adjust result type
	// put arg col to column map
	string funcName = ifp->func_name();
	if (ifp->arg_count != 1 ||
	    !(ifp->arguments()) ||
	    !(ifp->arguments()[0]) ||
	    ifp->arguments()[0]->type() != Item::FIELD_ITEM)
		return nullOnError(gwi, funcName);

	Item_field* field = (Item_field*)(ifp->arguments()[0]);

	// @todo rule out derive table
	if (!field->field || !field->db_name || strlen(field->db_name) == 0)
		return nullOnError(gwi, funcName);

	SimpleColumn *sc = buildSimpleColumn(field, gwi);
	if (!sc)
		return nullOnError(gwi, funcName);

	if ((pseudoType == PSEUDO_EXTENTMIN || pseudoType == PSEUDO_EXTENTMAX) &&
	   (sc->colType().colDataType == CalpontSystemCatalog::VARBINARY ||
	   (sc->colType().colDataType == CalpontSystemCatalog::VARCHAR && sc->colType().colWidth > 7) ||
	   (sc->colType().colDataType == CalpontSystemCatalog::CHAR && sc->colType().colWidth > 8)))
		return nullOnError(gwi, funcName);

	// put arg col to column map
	if (gwi.clauseType == SELECT || gwi.clauseType == GROUP_BY) // select clause
	{
		SRCP srcp(sc);
		gwi.columnMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(sc->columnName(), srcp));
		gwi.tableMap[make_aliastable(sc->schemaName(), sc->tableName(), sc->tableAlias(), sc->isInfiniDB())] =
		       make_pair(1, field->cached_table);
	}
	else if (!gwi.rcWorkStack.empty())
	{
		gwi.rcWorkStack.pop();
	}

	if (pseudoType == PSEUDO_PARTITION)
	{
		// parms: psueducolumn dbroot, segmentdir, segment
		SPTP sptp;
		FunctionColumn *fc = new FunctionColumn(funcName);
		funcexp::FunctionParm parms;
		PseudoColumn *dbroot = new PseudoColumn(*sc, PSEUDO_DBROOT);
		sptp.reset(new ParseTree(dbroot));
		parms.push_back(sptp);
		PseudoColumn *pp = new PseudoColumn(*sc, PSEUDO_SEGMENTDIR);
		sptp.reset(new ParseTree(pp));
		parms.push_back(sptp);
		PseudoColumn* seg = new PseudoColumn(*sc, PSEUDO_SEGMENT);
		sptp.reset(new ParseTree(seg));
		parms.push_back(sptp);
		fc->functionParms(parms);
		fc->expressionId(ci->expressionId++);

		// string result type
		CalpontSystemCatalog::ColType ct;
		ct.colDataType = CalpontSystemCatalog::VARCHAR;
		ct.colWidth = 256;
		fc->resultType(ct);

		// operation type integer
		funcexp::Func_idbpartition* idbpartition = new funcexp::Func_idbpartition();
		fc->operationType(idbpartition->operationType(parms, fc->resultType()));
		fc->alias(ifp->full_name()? ifp->full_name() : "");
		return fc;
	}

	PseudoColumn *pc = new PseudoColumn(*sc, pseudoType);

	// @bug5892. set alias for derived table column matching.
	pc->alias(ifp->full_name()? ifp->full_name() : "");
	return pc;
}

}
// vim:ts=4 sw=4:

