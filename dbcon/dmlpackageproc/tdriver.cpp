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

#include <stdio.h>
#include <string>
#include <stdexcept>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <errno.h>

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "createtableprocessor.h"
//#include "createindexprocessor.h"

//using namespace ddlpackageprocessor;

#include "writeengine.h"
#include "we_colop.h"
using namespace WriteEngine;

#include "messagelog.h"
#include "calpontdmlfactory.h"
#include "vendordmlstatement.h"
#include "insertdmlpackage.h"
#include "deletedmlpackage.h"
#include "updatedmlpackage.h"
#include "commanddmlpackage.h"
#include "dmlpackageprocessor.h"
#include "dmlpackageprocessorfactory.h"
#include "insertpackageprocessor.h"
#include "deletepackageprocessor.h"
#include "commandpackageprocessor.h"

using namespace dmlpackageprocessor;
using namespace dmlpackage;
using namespace std;


class SystemCatalogBuilder
{
public:
    static void build()
    {
        ColumnOp colOp;
        Column curCol;
        WriteEngine::WriteEngineWrapper fWriteEngine;
        WriteEngine::TxnID txnID = 0;
        int rc;

        remove();

        cout << "Creating System Catalog..." << endl;

        colOp.initColumn( curCol );
        // SYSTABLE

        // TableName
        rc = colOp.createColumn( curCol, 0, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1001 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2001, 2002, 2003);
        // Schema
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1002 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2004, 2005, 2006);
        // CreateDate
        rc = colOp.createColumn( curCol, 2, 8, WriteEngine::DATE, WriteEngine::WR_CHAR, 1003 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LastUpdateDate
        rc = colOp.createColumn( curCol, 3, 8, WriteEngine::DATE, WriteEngine::WR_CHAR, 1004 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // INIT
        rc = colOp.createColumn( curCol, 4, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1005 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // NEXT
        rc = colOp.createColumn( curCol, 5, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1006 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        //SYSCOLUMN
        // Shema
        rc = colOp.createColumn( curCol, 0, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1007 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2007, 2008, 2009);

        // TableName
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1008 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2010, 2011, 2012);

        // ColumnName
        rc = colOp.createColumn( curCol, 2, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1009 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2013, 2014, 2015);
        // ObjectID
        rc = colOp.createColumn( curCol, 3, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1010 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // DictOID
        rc = colOp.createColumn( curCol, 4, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1011 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // ListOID
        rc = colOp.createColumn( curCol, 5, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1012 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // TreeOID
        rc = colOp.createColumn( curCol, 6, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1013 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // DataType
        rc = colOp.createColumn( curCol, 7, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1014 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // ColumnLength
        rc = colOp.createColumn( curCol, 8, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1015 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // ColumnPos
        rc = colOp.createColumn( curCol, 9, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1016 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LastUpdate
        rc = colOp.createColumn( curCol, 10, 8, WriteEngine::DATE, WriteEngine::WR_CHAR, 1017 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // DefaultValue
        rc = colOp.createColumn( curCol, 11, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1018 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2016, 2017, 2018);
        // Nullable
        rc = colOp.createColumn( curCol, 12, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1019 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // Scale
        rc = colOp.createColumn( curCol, 13, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1020 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // Precision
        rc = colOp.createColumn( curCol, 14, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1021 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // AutoInc
        rc = colOp.createColumn( curCol, 15, 1, WriteEngine::CHAR, WriteEngine::WR_CHAR, 1022 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // SYSCONSTRAINT

        // ConstraintName
        rc = colOp.createColumn( curCol, 0, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1023 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2019, 2020, 2021);
        // Schema
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1024 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2022, 2023, 2024);
        // TableName
        rc = colOp.createColumn( curCol, 2, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1025 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2025, 2026, 2027);

        // ConstraintType
        rc = colOp.createColumn( curCol, 3, 1, WriteEngine::CHAR, WriteEngine::WR_CHAR, 1026 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // ConstraintPrim
        rc = colOp.createColumn( curCol, 4, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1027 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2028, 2029, 2030);

        // ConstraintText
        rc = colOp.createColumn( curCol, 5, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1028 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2031, 2032, 2033);

        // ConstraintStatus
        rc = colOp.createColumn( curCol, 6, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1029 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2034, 2035, 2036);

        // IndexName
        rc = colOp.createColumn( curCol, 7, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1030 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2037, 2038, 2039);

        //SYSCONSTRAINTCOL
        // Schema
        rc = colOp.createColumn( curCol, 0, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1031 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2040, 2041, 2042);

        // TableName
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1032 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2043, 2044, 2045);

        // ColumnName
        rc = colOp.createColumn( curCol, 2, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1033 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2046, 2047, 2048);

        // ConstraintName
        rc = colOp.createColumn( curCol, 3, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1034 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2049, 2050, 2051);

        // SYSINDEX
        // Schema
        rc = colOp.createColumn( curCol, 4, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1035 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2052, 2053, 2054);

        //TableName
        rc = colOp.createColumn( curCol, 5, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1036 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2055, 2056, 2057);

        // IndexName
        rc = colOp.createColumn( curCol, 6, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1037 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2058, 2059, 2060);

        // ListOID
        rc = colOp.createColumn( curCol, 7, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1038 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // TreeOID
        rc = colOp.createColumn( curCol, 8, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1039 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // IndexType
        rc = colOp.createColumn( curCol, 9, 1, WriteEngine::CHAR, WriteEngine::WR_CHAR, 1040 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // CreateDate
        rc = colOp.createColumn( curCol, 10, 8, WriteEngine::DATE, WriteEngine::WR_CHAR, 1041 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LastUpdateDate
        rc = colOp.createColumn( curCol, 11, 8, WriteEngine::DATE, WriteEngine::WR_CHAR, 1042 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // RecordCount
        rc = colOp.createColumn( curCol, 12, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1043 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // TreeLevel
        rc = colOp.createColumn( curCol, 13, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1044 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LeafCount
        rc = colOp.createColumn( curCol, 14, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1045 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // DistinctKeys
        rc = colOp.createColumn( curCol, 15, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1046 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LeafBlocks
        rc = colOp.createColumn( curCol, 16, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1047 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // AvgLeafCount
        rc = colOp.createColumn( curCol, 17, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1048 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // AvgDataBlock
        rc = colOp.createColumn( curCol, 18, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1049 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // SampleSize
        rc = colOp.createColumn( curCol, 19, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1050 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // ClusterFactor
        rc = colOp.createColumn( curCol, 20, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1051 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // LastAnalysisDate
        rc = colOp.createColumn( curCol, 21, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1052 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        // SYSINDEXCOL
        // Schema
        rc = colOp.createColumn( curCol, 0, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1053 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2061, 2062, 2063);

        // TableName
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1054 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2064, 2065, 2066);

        // ColumnName
        rc = colOp.createColumn( curCol, 2, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1055 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2067, 2068, 2069);

        // IndexName
        rc = colOp.createColumn( curCol, 3, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1056 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2070, 2071, 2072);

        // ColumnPos
        rc = colOp.createColumn( curCol, 4, 4, WriteEngine::INT, WriteEngine::WR_CHAR, 1057 );
        CPPUNIT_ASSERT( rc == NO_ERROR );
    }

    static void remove()
    {
        ColumnOp colOp;

        for ( int i = 1001; i <= 1057; i++ )
            colOp.deleteFile( i );
    }


};

class TpchDBBuilder
{
public:
    static void build()
    {

        cout << "Creating tpch database..." << endl;

        std::string createStmt = "create table tpch.region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));";
        buildTable(createStmt);

        createStmt = "create table tpch.nation( n_nationkey integer NOT NULL , n_name char(25) ,n_regionkey integer NOT NULL,n_comment varchar(152))";
        buildTable(createStmt);

        createStmt = "create table tpch.customer(c_custkey integer NOT NULL, c_name varchar(25) ,c_address varchar(40),    c_nationkey integer ,c_phone char(15) ,c_acctbal integer , c_mktsegment char(8) , c_comment varchar(117))";
        buildTable(createStmt);

        createStmt = "create table tpch.orders(o_orderkey integer NOT NULL, o_custkey integer , o_orderstatus char(1),    o_totalprice integer , o_orderdate date , o_orderpriority char(15) , o_clerk char(15), o_shippriority integer,o_comment varchar(79))";
        buildTable(createStmt);

        createStmt = "create table tpch.lineitem( l_orderkey integer NOT NULL , l_partkey integer NOT NULL,l_suppkey integer NOT NULL, l_linenumber integer NOT NULL, l_quantity integer NOT NULL,l_extendedprice integer NOT NULL, l_discount integer NOT NULL, l_tax integer NOT NULL, l_returnflag char(1) , l_linestatus char(1) , l_shipdate date , l_commitdate date,l_receiptdate date , l_shipinstruct char(25), l_shipmode char(10),l_comment varchar(44))";
        buildTable(createStmt);

        createStmt = "create table tpch.partsupp( ps_partkey integer NOT NULL, ps_suppkey integer,ps_availqty integer , ps_supplycost integer , ps_comment varchar(199))";
        buildTable(createStmt);

        createStmt = "create table tpch.supplier( s_suppkey integer NOT NULL, s_name char(25) , s_address varchar(40),    s_nationkey integer , s_phone char(15) , s_acctbal integer, s_comment varchar(101))";
        buildTable(createStmt);  
	
	createStmt = "create table tpch.part( p_partkey integer NOT NULL ,p_name varchar(55) , p_mfgr char(25), p_brand char(10) , p_type varchar(25) , p_size integer , p_container char(10) ,p_retailprice integer , p_comment varchar(23), PRIMARY KEY(p_partkey))";
        buildTable(createStmt);

    }

    static void buildTable(std::string createText)
    {

        ddlpackage::SqlParser parser;
        parser.Parse(createText.c_str());
        if (parser.Good())
        {
            const ddlpackage::ParseTree &ptree = parser.GetParseTree();
            try
            {
                ddlpackageprocessor::CreateTableProcessor processor;
                processor.setDebugLevel(ddlpackageprocessor::CreateTableProcessor::VERBOSE);

                ddlpackage::SqlStatement &stmt = *ptree.fList[0];
                ddlpackageprocessor::CreateTableProcessor::DDLResult result;

                result  = processor.processPackage(dynamic_cast<ddlpackage::CreateTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {

                throw;
            }
        }
    }
};


/*
 * destroySemaphores() and destroyShmseg() will print error messages
 * if there are no objects to destroy.  That's OK.
 */
void destroySemaphores()
{
    key_t semkey;
    int sems, err;

    semkey = 0x2149bdd2;
    sems = semget(semkey, 2, 0666);
    if (sems != -1)
    {
        err = semctl(sems, 0, IPC_RMID);
        if (err == -1)
            perror("tdriver: semctl");
    }
}

void destroyShmseg()
{
    key_t shmkey;
    int shms, err;

    shmkey = 0x2149bdd2;
    shms = shmget(shmkey, 0, 0666);
    if (shms != -1)
    {
        err = shmctl(shms, IPC_RMID, NULL);
        if (err == -1 && errno != EINVAL)
        {
            perror("tdriver: shmctl");
            return;
        }
    }
}

void setUp()
{
    destroySemaphores();
    destroyShmseg();
    unlink("/tmp/oidbitmap");
    execplan::ObjectIDManager fObjectIDManager;
    int dummyOid = fObjectIDManager.allocOIDs(5);
    SystemCatalogBuilder::build();

    TpchDBBuilder::build();

}

void tearDown()
{
    destroySemaphores();
    destroyShmseg();
    unlink("/tmp/oidbitmap");
}



class DMLPackageProcessorTest : public CppUnit::TestFixture
{

    CPPUNIT_TEST_SUITE( DMLPackageProcessorTest );
    /*
    CPPUNIT_TEST( test_insert_package1);
    CPPUNIT_TEST( test_insert_package2);
    CPPUNIT_TEST( test_insert_package3);
    CPPUNIT_TEST( test_insert_NULL );
    CPPUNIT_TEST( test_update_package);
    CPPUNIT_TEST( test_delete_package);
    CPPUNIT_TEST( test_insert_package_fail);
    CPPUNIT_TEST( test_update_package_fail);
    CPPUNIT_TEST( test_delete_package_fail);
    CPPUNIT_TEST( test_command_package ); 
    */
    CPPUNIT_TEST_SUITE_END();

private:


public:

    void test_insert_package1()
    {

        cout << "Begin insert package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "insert into tpch.part(p_partkey,p_name,p_mfgr) values(3, 'widget', 'acmea');";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_insert_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_insert_object( bytestream );

        cout << "\nEnd insert package test" << endl;

    }

    void test_insert_package2()
    {

        cout << "Begin insert package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "insert into tpch.part(p_partkey,p_name,p_mfgr) values(1, 'widget', 'acmea');";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_insert_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_insert_object( bytestream );

        cout << "\nEnd insert package test" << endl;

    }
    void test_insert_package3()
    {

        cout << "Begin insert package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "insert into tpch.part values(6, 'name', 'mfgr', 'brand', 'type', 6, 'container', 12, 'comment');";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_insert_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_insert_object( bytestream );

        cout << "\nEnd insert package test" << endl;

    }
    void test_insert_NULL()
    {

        cout << "Begin insert package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "insert into tpch.part(p_partkey,p_name,p_mfgr) values(2, 'widget2', NULL);";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_insert_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_insert_object( bytestream );

        cout << "\nEnd insert package test" << endl;

    }

    void test_insert_package_fail()
    {

        cout << "Begin insert package failure test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "insert into tpch.region(r_regionkey, r_name) values(1,2);";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_insert_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_insert_object( bytestream );

        cout << "\nEnd insert package test" << endl;

    }

    void write_insert_object( ByteStream& bs, CalpontDMLPackage* pDMLPackage )
    {
        cout << "Writing out package over bytestream" << endl;
        pDMLPackage->write( bs );
    }

    void processs_insert_object( ByteStream& bs )
    {

        cout << "Reading in package from bytestream" << endl;

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_INSERT == package_type );

        InsertDMLPackage *pObject = new InsertDMLPackage();

        pObject->read( bs );

        cout << "Success reading package from bytestream" << endl;

        DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  package_type, *pObject );

        CPPUNIT_ASSERT( 0 != pkgProcPtr );

        pkgProcPtr->setDebugLevel(DMLPackageProcessor::VERBOSE);
        DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *pObject );
        if ( DMLPackageProcessor::NO_ERROR != result.result )
        {
            cout << "Insert failed!" << endl;
            logging::LoggingID lid(21);
            logging::MessageLog ml(lid);

            ml.logDebugMessage( result.message );
        }

        delete pkgProcPtr;
        delete pObject;

    }

    void test_delete_package()
    {

        cout << "Begin delete package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "delete from tpch.part;";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_delete_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_delete_object( bytestream );

        cout << "\nEnd delete package test" << endl;

    }

    void test_delete_package_fail()
    {

        cout << "Begin delete package failure test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "delete from tpch.region;";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_delete_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // 	process the package
        processs_delete_object( bytestream );

        cout << "\nEnd delete package test" << endl;

    }
    void write_delete_object( ByteStream& bs, CalpontDMLPackage* pDMLPackage )
    {
        cout << "Writing out package over bytestream" << endl;
        pDMLPackage->write( bs );
    }

    void processs_delete_object( ByteStream& bs )
    {

        cout << "Reading in package from bytestream" << endl;

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_DELETE == package_type );

        DeleteDMLPackage *pObject = new DeleteDMLPackage();

        pObject->read( bs );

        cout << "Success reading package from bytestream" << endl;

        DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  package_type, *pObject );

        CPPUNIT_ASSERT( 0 != pkgProcPtr );

        pkgProcPtr->setDebugLevel(DMLPackageProcessor::VERBOSE);
        DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *pObject );

        if ( DMLPackageProcessor::NO_ERROR != result.result )
        {
            cout << "Delete failed!" << endl;
            logging::LoggingID lid(21);
            logging::MessageLog ml(lid);

            ml.logDebugMessage( result.message );
        }
        delete pkgProcPtr;
        delete pObject;

    }

    void test_update_package()
    {

        cout << "Begin update package test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "update tpch.part set p_mfgr = 'bigco';";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_update_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        // process the package
        processs_update_object( bytestream );

        cout << "\nEnd update package test" << endl;

    }

    void test_command_package()
    {
        std::vector<std::string> commands;
        commands.push_back("COMMIT;");
        commands.push_back("ROLLBACK;");
        commands.push_back("BOGUS;");


        cout << "Begin command package test" << endl;

        ByteStream  bytestream;

        std::vector<std::string>::const_iterator iter = commands.begin();
        while (iter != commands.end())
        {
            std::string command = *iter;
            cout << command << endl;

            VendorDMLStatement dml_command(command, 1);
            CalpontDMLPackage* dmlCommandPkgPtr = CalpontDMLFactory::makeCalpontDMLPackage(dml_command);
            if (dmlCommandPkgPtr)
            {
                cout << "CalpontDMLFactory::makeCalpontDMLPackage: success"  << endl;
                cout << "Writing out package over bytestream" << endl;
                dmlCommandPkgPtr->write(bytestream);
                delete dmlCommandPkgPtr;

                process_command_object(bytestream);
            }

            ++iter;
        }

        cout << "\nEnd commit package test" << endl;

    }

    void process_command_object( ByteStream& bs )
    {

        cout << "Reading in package from bytestream" << endl;

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_COMMAND == package_type );

        CommandDMLPackage *pObject = new CommandDMLPackage();

        pObject->read( bs );

        cout << "Success reading package from bytestream" << endl;

        DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  package_type, *pObject );

        CPPUNIT_ASSERT( 0 != pkgProcPtr );
        pkgProcPtr->setDebugLevel(DMLPackageProcessor::VERBOSE);

        DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *pObject );

        if ( DMLPackageProcessor::NO_ERROR != result.result )
        {
            cout << "Command process failed!" << endl;
            logging::LoggingID lid(21);
            logging::MessageLog ml(lid);

            ml.logDebugMessage( result.message );
        }
        delete pkgProcPtr;
        delete pObject;
    }

    void test_update_package_fail()
    {

        cout << "Begin update package failure test" << endl;

        ByteStream  bytestream;
        std::string dmlStatement = "update tpch.region set r_name = NORTH;";

        cout << "\nDML statement:" << dmlStatement.c_str() << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        cout << "CalpontDMLFactory::makeCalpontDMLPackage:" << "success" << endl;

        write_update_object(bytestream, pDMLPackage);
        delete pDMLPackage;

        //process the package
        processs_update_object( bytestream );

        cout << "\nEnd update package test" << endl;

    }

    void write_update_object( ByteStream& bs, CalpontDMLPackage* pDMLPackage )
    {
        cout << "Writing out package over bytestream" << endl;
        pDMLPackage->write( bs );
    }

    void processs_update_object( ByteStream& bs )
    {

        cout << "Reading in package from bytestream" << endl;

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_UPDATE == package_type );

        UpdateDMLPackage *pObject = new UpdateDMLPackage();

        pObject->read( bs );

        cout << "Success reading package from bytestream" << endl;

        DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  package_type, *pObject );

        CPPUNIT_ASSERT( 0 != pkgProcPtr );

        pkgProcPtr->setDebugLevel(DMLPackageProcessor::VERBOSE);

        DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *pObject );

        if ( DMLPackageProcessor::NO_ERROR != result.result )
        {
            cout << "Update failed!" << endl;
            logging::LoggingID lid(21);
            logging::MessageLog ml(lid);

            ml.logDebugMessage( result.message );
        }

        delete pkgProcPtr;
        delete pObject;

    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( DMLPackageProcessorTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
    // Uncomment before running tests
     //setUp();

    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );
    bool wasSuccessful = runner.run( "", false );

    tearDown();

    return (wasSuccessful ? 0 : 1);
}
