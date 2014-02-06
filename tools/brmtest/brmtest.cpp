/*
* $Id: brmtest.cpp 1739 2012-03-22 12:57:59Z pleblanc $
*/

#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
using namespace std;

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "extentmap.h"
#include "blockresolutionmanager.h"
using namespace BRM;

#include "configcpp.h"
using namespace config;

extern int query_locks();
extern int reset_locks();

namespace {

boost::shared_ptr<CalpontSystemCatalog> Cat;

void LBIDList(CalpontSystemCatalog::OID OID, ostringstream& cout_str)
{
	LBIDRange_v LBIDRanges;

	LBIDRange_v::size_type RangeCount;
	ExtentMap em;
	LBIDRange_v::size_type i;
	LBIDRange LBIDR;

	cout_str << "HWM = " << em.getHWM(OID) << endl;

	em.lookup(OID, LBIDRanges);
	RangeCount = LBIDRanges.size();
	idbassert(RangeCount < 1000);

	for (i = 0; i < RangeCount; i++)
	{
		LBIDR = LBIDRanges[i];
		cout_str << LBIDR.start << " - " << (LBIDR.start + LBIDR.size - 1) << " (" << LBIDR.size << ')';
		int64_t max=-1, min=-1;
		int32_t seqNum=0;
		em.getMaxMin(LBIDR.start, max, min, seqNum);
		cout_str << " min: " << min << ", max: " << max << ", seqNum: " << seqNum << endl;
	}
	cout_str << endl;
}

void doit(const CalpontSystemCatalog::TableColName& tcn, ostringstream& cout_str)
{
	CalpontSystemCatalog::OID OID;
	try {
		OID = Cat->columnRID(tcn).objnum;
	}
	catch (...) {
    		cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": OID not found" << endl;
		return;
	}

	if (OID > 0)
    {

    	CalpontSystemCatalog::OID ioid = Cat->lookupOID(tcn);
    	CalpontSystemCatalog::ColType CT = Cat->colType(ioid);
    
    	CalpontSystemCatalog::DictOID DOID = CT.ddn;
    
    	int DictOID = DOID.dictOID;
    	int ListOID = DOID.listOID;
    	int TreeOID = DOID.treeOID;
    
    	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": OID = " << OID << endl;
    	try {
    		LBIDList(OID, cout_str);
    	}
    	catch (exception& ex) {
    		cerr << ex.what() << endl;
    	}
        
    	if(DictOID > 0)
        {
        	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": DictOID = " << DictOID << endl;
        
        	try {
        		LBIDList(DictOID, cout_str);
        	}
        	catch (exception& ex) {
        		cerr << ex.what() << endl;
        	}
        }
    	if(ListOID > 0)
        {
        	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": DictListOID = " << ListOID << endl;
        
        	try {
        		LBIDList(ListOID, cout_str);
        	}
        	catch (exception& ex) {
        		cerr << ex.what() << endl;
        	}
        }
    	if(TreeOID > 0)
        {
        	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": DictTreeOID = " << TreeOID << endl;
        
        	try {
        		LBIDList(TreeOID, cout_str);
        	}
        	catch (exception& ex) {
        		cerr << ex.what() << endl;
        	}
        }

	CalpontSystemCatalog::IndexOID IOID = Cat->lookupIndexNbr(tcn);
	if (IOID.objnum > 0)
	{
        	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": IndexOID = " << IOID.objnum << endl;
        
        	try {
        		LBIDList(IOID.objnum, cout_str);
        	}
        	catch (exception& ex) {
        		cerr << ex.what() << endl;
        	}
	}
	if (IOID.listOID > 0)
	{
        	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": IndexLstOID = " << IOID.listOID << endl;
        
        	try {
        		LBIDList(IOID.listOID, cout_str);
        	}
        	catch (exception& ex) {
        		cerr << ex.what() << endl;
        	}
	}
    }
    else
    {
    	cout_str << tcn.schema << '.' << tcn.table << '.' << tcn.column << ": OID was zero!" << endl;
    }
}

void usage(ostringstream& cout_str)
{
	cout_str << "usage: brmtest [-hrts] [-l LBID] [-c schema]" << endl;
	cout_str << "\t-h display this help" << endl;
	cout_str << "\t-r reset brm locks" << endl;
	cout_str << "\t-l LBID display info about LBID" << endl;
	cout_str << "\t-t dump TPC-H tables" << endl;
	cout_str << "\t-s don't dump system catalog" << endl;
	cout_str << "\t-c schema seach for TPC-H tables in schema" << endl;
}

}

int main(int argc, char** argv)
{
	int c;

	bool rflg = false;
	bool lflg = false;
	bool tflg = false;
	bool sflg = true;
	bool qflg = false;

	opterr = 0;

	uint64_t lbid = 0;

	ostringstream cout_str;
	ostringstream cerr_str;

	string schema("tpch");

	while ((c = getopt(argc, argv, "hrl:tsc:q")) != EOF)
		switch (c)
		{
		case 'r':
			rflg = true;
			break;
		case 'l':
			lflg = true;
			lbid = strtoul(optarg, 0, 0);
			break;
		case 'h':
			usage(cout_str);
			cerr << cout_str.str()  << endl;
			exit(0);
			break;
		case 't':
			tflg = true;
			break;
		case 's':
			sflg = false;
			break;
		case 'c':
			schema = optarg;
			break;
		case 'q':
			qflg = true;
			break;
		default:
			usage(cout_str);
			cerr << cout_str.str()  << endl;
			exit(1);
			break;
		}

	if (rflg)
	{
		reset_locks();
		return 0;
	}

	if (query_locks() != 0)
	{
		cerr << "BRM is locked!" << endl;
		return 1;
	}

	if (lflg)
	{
		BlockResolutionManager brm;
		uint16_t ver = 0;
		BRM::OID_t oid;
		uint32_t fbo;
		int rc;
		rc = brm.lookup(lbid, ver, false, oid, fbo);
		idbassert(rc == 0);
		if (qflg)
			cout << oid << endl;
		else
			cout << "LBID " << lbid << " is part of OID " << oid << " at FBO " << fbo << endl;
		return 0;
	}

	//Now, close out all output so we don't get any debug from PG/RA
	int fd;
	::close(2);
	::close(1);
	//fd = open("./brmtest.out", O_WRONLY|O_CREAT|O_TRUNC, 0666);
	fd = open("/dev/null", O_WRONLY);
	idbassert(fd >= 0);
	if (fd != 1) dup2(fd, 1);
	//fd = open("./brmtest.err", O_WRONLY|O_CREAT|O_TRUNC, 0666);
	fd = open("/dev/null", O_WRONLY);
	idbassert(fd >= 0);
	if (fd != 2) dup2(fd, 2);

	Cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

	fd = ::open("/dev/tty", O_WRONLY);
	idbassert(fd >= 0);

	string status;
	if (tflg)
	{
		const string region("region");
		const string nation("nation");
		const string customer("customer");
		const string orders("orders");
		const string supplier("supplier");
		const string partsupp("partsupp");
		const string lineitem("lineitem");
		const string part("part");

		status = "Reading REGION...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, region, "r_regionkey"), cout_str);
		doit(make_tcn(schema, region, "r_name"), cout_str);
		doit(make_tcn(schema, region, "r_comment"), cout_str);

		status = "Reading NATION...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, nation, "n_nationkey"), cout_str);
		doit(make_tcn(schema, nation, "n_name"), cout_str);
		doit(make_tcn(schema, nation, "n_regionkey"), cout_str);
		doit(make_tcn(schema, nation, "n_comment"), cout_str);

		status = "Reading CUSTOMER...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, customer, "c_custkey"), cout_str);
		doit(make_tcn(schema, customer, "c_name"), cout_str);
		doit(make_tcn(schema, customer, "c_address"), cout_str);
		doit(make_tcn(schema, customer, "c_nationkey"), cout_str);
		doit(make_tcn(schema, customer, "c_phone"), cout_str);
		doit(make_tcn(schema, customer, "c_acctbal"), cout_str);
		doit(make_tcn(schema, customer, "c_mktsegment"), cout_str);
		doit(make_tcn(schema, customer, "c_comment"), cout_str);

		status = "Reading ORDERS...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, orders, "o_orderkey"), cout_str);
		doit(make_tcn(schema, orders, "o_custkey"), cout_str);
		doit(make_tcn(schema, orders, "o_orderstatus"), cout_str);
		doit(make_tcn(schema, orders, "o_totalprice"), cout_str);
		doit(make_tcn(schema, orders, "o_orderdate"), cout_str);
		doit(make_tcn(schema, orders, "o_orderpriority"), cout_str);
		doit(make_tcn(schema, orders, "o_clerk"), cout_str);
		doit(make_tcn(schema, orders, "o_shippriority"), cout_str);
		doit(make_tcn(schema, orders, "o_comment"), cout_str);

		status = "Reading PART...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, part, "p_partkey"), cout_str);
		doit(make_tcn(schema, part, "p_name"), cout_str);
		doit(make_tcn(schema, part, "p_mfgr"), cout_str);
		doit(make_tcn(schema, part, "p_brand"), cout_str);
		doit(make_tcn(schema, part, "p_type"), cout_str);
		doit(make_tcn(schema, part, "p_size"), cout_str);
		doit(make_tcn(schema, part, "p_container"), cout_str);
		doit(make_tcn(schema, part, "p_retailprice"), cout_str);
		doit(make_tcn(schema, part, "p_comment"), cout_str);

		status = "Reading SUPPLIER...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, supplier, "s_suppkey"), cout_str);
		doit(make_tcn(schema, supplier, "s_name"), cout_str);
		doit(make_tcn(schema, supplier, "s_address"), cout_str);
		doit(make_tcn(schema, supplier, "s_nationkey"), cout_str);
		doit(make_tcn(schema, supplier, "s_phone"), cout_str);
		doit(make_tcn(schema, supplier, "s_acctbal"), cout_str);
		doit(make_tcn(schema, supplier, "s_comment"), cout_str);

		status = "Reading PARTSUPP...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, partsupp, "ps_partkey"), cout_str);
		doit(make_tcn(schema, partsupp, "ps_suppkey"), cout_str);
		doit(make_tcn(schema, partsupp, "ps_availqty"), cout_str);
		doit(make_tcn(schema, partsupp, "ps_supplycost"), cout_str);
		doit(make_tcn(schema, partsupp, "ps_comment"), cout_str);

		status = "Reading LINEITEM...\n";
		::write(fd, status.c_str(), status.length());

		doit(make_tcn(schema, lineitem, "l_orderkey"), cout_str);
		doit(make_tcn(schema, lineitem, "l_linenumber"), cout_str);
		doit(make_tcn(schema, lineitem, "l_partkey"), cout_str);
		doit(make_tcn(schema, lineitem, "l_suppkey"), cout_str);
		doit(make_tcn(schema, lineitem, "l_quantity"), cout_str);
		doit(make_tcn(schema, lineitem, "l_extendedprice"), cout_str);
		doit(make_tcn(schema, lineitem, "l_discount"), cout_str);
		doit(make_tcn(schema, lineitem, "l_tax"), cout_str);
		doit(make_tcn(schema, lineitem, "l_returnflag"), cout_str);
		doit(make_tcn(schema, lineitem, "l_linestatus"), cout_str);
		doit(make_tcn(schema, lineitem, "l_shipdate"), cout_str);
		doit(make_tcn(schema, lineitem, "l_commitdate"), cout_str);
		doit(make_tcn(schema, lineitem, "l_receiptdate"), cout_str);
		doit(make_tcn(schema, lineitem, "l_shipinstruct"), cout_str);
		doit(make_tcn(schema, lineitem, "l_shipmode"), cout_str);
		doit(make_tcn(schema, lineitem, "l_comment"), cout_str);
	}

	if (sflg)
	{
		status = "Reading CALPONTSYS...\n\n";
		::write(fd, status.c_str(), status.length());

		schema = CALPONT_SCHEMA;
		string table = SYSCOLUMN_TABLE;
		doit(make_tcn(schema, table, SCHEMA_COL), cout_str);
		doit(make_tcn(schema, table, TABLENAME_COL), cout_str);
		doit(make_tcn(schema, table, COLNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, MULTICOLFLAG_COL), cout_str);
		doit(make_tcn(schema, table, OBJECTID_COL), cout_str);
		doit(make_tcn(schema, table, DICTOID_COL), cout_str);
		doit(make_tcn(schema, table, LISTOBJID_COL), cout_str);
		doit(make_tcn(schema, table, TREEOBJID_COL), cout_str);
		doit(make_tcn(schema, table, DATATYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNTYPE_COL), cout_str);
		doit(make_tcn(schema, table, COLUMNLEN_COL), cout_str);
		doit(make_tcn(schema, table, COLUMNPOS_COL), cout_str);
		///doit(make_tcn(schema, table, CREATEDATE_COL), cout_str);
		doit(make_tcn(schema, table, LASTUPDATE_COL), cout_str);
		doit(make_tcn(schema, table, DEFAULTVAL_COL), cout_str);
		doit(make_tcn(schema, table, NULLABLE_COL), cout_str);
		doit(make_tcn(schema, table, SCALE_COL), cout_str);
		doit(make_tcn(schema, table, PRECISION_COL), cout_str);
		///doit(make_tcn(schema, table, NUMNULLS_COL), cout_str);
		///doit(make_tcn(schema, table, NUMDISTINCTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MINVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MAXVAL_COL), cout_str);
		///doit(make_tcn(schema, table, DENSITY_COL), cout_str);
		///doit(make_tcn(schema, table, AVGRECORDLEN_COL), cout_str);
		///doit(make_tcn(schema, table, RECORDCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, LASTANYLDATE_COL), cout_str);
		///doit(make_tcn(schema, table, SAMPLESIZE_COL), cout_str);
		///doit(make_tcn(schema, table, PROPERTY_COL), cout_str);
		doit(make_tcn(schema, table, AUTOINC_COL), cout_str);
		///doit(make_tcn(schema, table, DATANAME_COL), cout_str);
		///doit(make_tcn(schema, table, CATEGORY_COL), cout_str);
		///doit(make_tcn(schema, table, SIZE_COL), cout_str);
		///doit(make_tcn(schema, table, DESC_COL), cout_str);
		///doit(make_tcn(schema, table, INIT_COL), cout_str);
		///doit(make_tcn(schema, table, NEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNAME_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNUM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTPRIM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTSTATUS_COL), cout_str);
		///doit(make_tcn(schema, table, TREELEVEL_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, DISTINCTKEYS_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFBLOCKS_COL), cout_str);
		///doit(make_tcn(schema, table, AVGLEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, AVGDATABLOCK_COL), cout_str);
		///doit(make_tcn(schema, table, CLUSTERFACTOR_COL), cout_str);

		table = SYSTABLE_TABLE;
		doit(make_tcn(schema, table, SCHEMA_COL), cout_str);
		doit(make_tcn(schema, table, TABLENAME_COL), cout_str);
		///doit(make_tcn(schema, table, COLNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, MULTICOLFLAG_COL), cout_str);
		doit(make_tcn(schema, table, OBJECTID_COL), cout_str);
		///doit(make_tcn(schema, table, DICTOID_COL), cout_str);
		///doit(make_tcn(schema, table, LISTOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, TREEOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, DATATYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNLEN_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNPOS_COL), cout_str);
		doit(make_tcn(schema, table, CREATEDATE_COL), cout_str);
		doit(make_tcn(schema, table, LASTUPDATE_COL), cout_str);
		///doit(make_tcn(schema, table, DEFAULTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, NULLABLE_COL), cout_str);
		///doit(make_tcn(schema, table, SCALE_COL), cout_str);
		///doit(make_tcn(schema, table, PRECISION_COL), cout_str);
		///doit(make_tcn(schema, table, NUMNULLS_COL), cout_str);
		///doit(make_tcn(schema, table, NUMDISTINCTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MINVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MAXVAL_COL), cout_str);
		///doit(make_tcn(schema, table, DENSITY_COL), cout_str);
		///doit(make_tcn(schema, table, AVGRECORDLEN_COL), cout_str);
		///doit(make_tcn(schema, table, RECORDCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, LASTANYLDATE_COL), cout_str);
		///doit(make_tcn(schema, table, SAMPLESIZE_COL), cout_str);
		///doit(make_tcn(schema, table, PROPERTY_COL), cout_str);
		///doit(make_tcn(schema, table, AUTOINC_COL), cout_str);
		///doit(make_tcn(schema, table, DATANAME_COL), cout_str);
		///doit(make_tcn(schema, table, CATEGORY_COL), cout_str);
		///doit(make_tcn(schema, table, SIZE_COL), cout_str);
		///doit(make_tcn(schema, table, DESC_COL), cout_str);
		doit(make_tcn(schema, table, INIT_COL), cout_str);
		doit(make_tcn(schema, table, NEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNAME_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNUM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTPRIM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTSTATUS_COL), cout_str);
		///doit(make_tcn(schema, table, TREELEVEL_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, DISTINCTKEYS_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFBLOCKS_COL), cout_str);
		///doit(make_tcn(schema, table, AVGLEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, AVGDATABLOCK_COL), cout_str);
		///doit(make_tcn(schema, table, CLUSTERFACTOR_COL), cout_str);

		table = SYSSCHEMA_TABLE;
		///doit(make_tcn(schema, table, SCHEMA_COL), cout_str);
		///doit(make_tcn(schema, table, TABLENAME_COL), cout_str);
		///doit(make_tcn(schema, table, COLNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, MULTICOLFLAG_COL), cout_str);
		///doit(make_tcn(schema, table, OBJECTID_COL), cout_str);
		///doit(make_tcn(schema, table, DICTOID_COL), cout_str);
		///doit(make_tcn(schema, table, LISTOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, TREEOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, DATATYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNLEN_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNPOS_COL), cout_str);
		///doit(make_tcn(schema, table, CREATEDATE_COL), cout_str);
		///doit(make_tcn(schema, table, LASTUPDATE_COL), cout_str);
		///doit(make_tcn(schema, table, DEFAULTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, NULLABLE_COL), cout_str);
		///doit(make_tcn(schema, table, SCALE_COL), cout_str);
		///doit(make_tcn(schema, table, PRECISION_COL), cout_str);
		///doit(make_tcn(schema, table, NUMNULLS_COL), cout_str);
		///doit(make_tcn(schema, table, NUMDISTINCTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MINVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MAXVAL_COL), cout_str);
		///doit(make_tcn(schema, table, DENSITY_COL), cout_str);
		///doit(make_tcn(schema, table, AVGRECORDLEN_COL), cout_str);
		///doit(make_tcn(schema, table, RECORDCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, LASTANYLDATE_COL), cout_str);
		///doit(make_tcn(schema, table, SAMPLESIZE_COL), cout_str);
		///doit(make_tcn(schema, table, PROPERTY_COL), cout_str);
		///doit(make_tcn(schema, table, AUTOINC_COL), cout_str);
		///doit(make_tcn(schema, table, DATANAME_COL), cout_str);
		///doit(make_tcn(schema, table, CATEGORY_COL), cout_str);
		///doit(make_tcn(schema, table, SIZE_COL), cout_str);
		///doit(make_tcn(schema, table, DESC_COL), cout_str);
		///doit(make_tcn(schema, table, INIT_COL), cout_str);
		///doit(make_tcn(schema, table, NEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNAME_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNUM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTPRIM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTSTATUS_COL), cout_str);
		///doit(make_tcn(schema, table, TREELEVEL_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, DISTINCTKEYS_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFBLOCKS_COL), cout_str);
		///doit(make_tcn(schema, table, AVGLEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, AVGDATABLOCK_COL), cout_str);
		///doit(make_tcn(schema, table, CLUSTERFACTOR_COL), cout_str);

		table = SYSINDEX_TABLE;
		doit(make_tcn(schema, table, SCHEMA_COL), cout_str);
		doit(make_tcn(schema, table, TABLENAME_COL), cout_str);
		///doit(make_tcn(schema, table, COLNAME_COL), cout_str);
		doit(make_tcn(schema, table, INDEXNAME_COL), cout_str);
		doit(make_tcn(schema, table, INDEXTYPE_COL), cout_str);
		doit(make_tcn(schema, table, MULTICOLFLAG_COL), cout_str);
		///doit(make_tcn(schema, table, OBJECTID_COL), cout_str);
		///doit(make_tcn(schema, table, DICTOID_COL), cout_str);
		doit(make_tcn(schema, table, LISTOBJID_COL), cout_str);
		doit(make_tcn(schema, table, TREEOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, DATATYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNLEN_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNPOS_COL), cout_str);
		doit(make_tcn(schema, table, CREATEDATE_COL), cout_str);
		doit(make_tcn(schema, table, LASTUPDATE_COL), cout_str);
		///doit(make_tcn(schema, table, DEFAULTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, NULLABLE_COL), cout_str);
		///doit(make_tcn(schema, table, SCALE_COL), cout_str);
		///doit(make_tcn(schema, table, PRECISION_COL), cout_str);
		///doit(make_tcn(schema, table, NUMNULLS_COL), cout_str);
		///doit(make_tcn(schema, table, NUMDISTINCTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MINVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MAXVAL_COL), cout_str);
		///doit(make_tcn(schema, table, DENSITY_COL), cout_str);
		///doit(make_tcn(schema, table, AVGRECORDLEN_COL), cout_str);
		doit(make_tcn(schema, table, RECORDCOUNT_COL), cout_str);
		doit(make_tcn(schema, table, LASTANYLDATE_COL), cout_str);
		doit(make_tcn(schema, table, SAMPLESIZE_COL), cout_str);
		///doit(make_tcn(schema, table, PROPERTY_COL), cout_str);
		///doit(make_tcn(schema, table, AUTOINC_COL), cout_str);
		///doit(make_tcn(schema, table, DATANAME_COL), cout_str);
		///doit(make_tcn(schema, table, CATEGORY_COL), cout_str);
		///doit(make_tcn(schema, table, SIZE_COL), cout_str);
		///doit(make_tcn(schema, table, DESC_COL), cout_str);
		///doit(make_tcn(schema, table, INIT_COL), cout_str);
		///doit(make_tcn(schema, table, NEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNAME_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNUM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTPRIM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTSTATUS_COL), cout_str);
		doit(make_tcn(schema, table, TREELEVEL_COL), cout_str);
		doit(make_tcn(schema, table, LEAFCOUNT_COL), cout_str);
		doit(make_tcn(schema, table, DISTINCTKEYS_COL), cout_str);
		doit(make_tcn(schema, table, LEAFBLOCKS_COL), cout_str);
		doit(make_tcn(schema, table, AVGLEAFCOUNT_COL), cout_str);
		doit(make_tcn(schema, table, AVGDATABLOCK_COL), cout_str);
		doit(make_tcn(schema, table, CLUSTERFACTOR_COL), cout_str);

		table = SYSINDEXCOL_TABLE;
		doit(make_tcn(schema, table, SCHEMA_COL), cout_str);
		doit(make_tcn(schema, table, TABLENAME_COL), cout_str);
		doit(make_tcn(schema, table, COLNAME_COL), cout_str);
		doit(make_tcn(schema, table, INDEXNAME_COL), cout_str);
		///doit(make_tcn(schema, table, INDEXTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, MULTICOLFLAG_COL), cout_str);
		///doit(make_tcn(schema, table, OBJECTID_COL), cout_str);
		///doit(make_tcn(schema, table, DICTOID_COL), cout_str);
		///doit(make_tcn(schema, table, LISTOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, TREEOBJID_COL), cout_str);
		///doit(make_tcn(schema, table, DATATYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, COLUMNLEN_COL), cout_str);
		doit(make_tcn(schema, table, COLUMNPOS_COL), cout_str);
		///doit(make_tcn(schema, table, CREATEDATE_COL), cout_str);
		///doit(make_tcn(schema, table, LASTUPDATE_COL), cout_str);
		///doit(make_tcn(schema, table, DEFAULTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, NULLABLE_COL), cout_str);
		///doit(make_tcn(schema, table, SCALE_COL), cout_str);
		///doit(make_tcn(schema, table, PRECISION_COL), cout_str);
		///doit(make_tcn(schema, table, NUMNULLS_COL), cout_str);
		///doit(make_tcn(schema, table, NUMDISTINCTVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MINVAL_COL), cout_str);
		///doit(make_tcn(schema, table, MAXVAL_COL), cout_str);
		///doit(make_tcn(schema, table, DENSITY_COL), cout_str);
		///doit(make_tcn(schema, table, AVGRECORDLEN_COL), cout_str);
		///doit(make_tcn(schema, table, RECORDCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, LASTANYLDATE_COL), cout_str);
		///doit(make_tcn(schema, table, SAMPLESIZE_COL), cout_str);
		///doit(make_tcn(schema, table, PROPERTY_COL), cout_str);
		///doit(make_tcn(schema, table, AUTOINC_COL), cout_str);
		///doit(make_tcn(schema, table, DATANAME_COL), cout_str);
		///doit(make_tcn(schema, table, CATEGORY_COL), cout_str);
		///doit(make_tcn(schema, table, SIZE_COL), cout_str);
		///doit(make_tcn(schema, table, DESC_COL), cout_str);
		///doit(make_tcn(schema, table, INIT_COL), cout_str);
		///doit(make_tcn(schema, table, NEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNAME_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTNUM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTYPE_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTPRIM_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTTEXT_COL), cout_str);
		///doit(make_tcn(schema, table, CONSTRAINTSTATUS_COL), cout_str);
		///doit(make_tcn(schema, table, TREELEVEL_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, DISTINCTKEYS_COL), cout_str);
		///doit(make_tcn(schema, table, LEAFBLOCKS_COL), cout_str);
		///doit(make_tcn(schema, table, AVGLEAFCOUNT_COL), cout_str);
		///doit(make_tcn(schema, table, AVGDATABLOCK_COL), cout_str);
		///doit(make_tcn(schema, table, CLUSTERFACTOR_COL), cout_str);
	}

	cout_str << ends;
	::write(fd, cout_str.str().c_str(), cout_str.str().length());

	return 0;
}

