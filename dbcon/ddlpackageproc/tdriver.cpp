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
#include "createindexprocessor.h"
#include "altertableprocessor.h"
#include "dropindexprocessor.h"
#include "droptableprocessor.h"

using namespace ddlpackageprocessor;

#include "writeengine.h"
#include "we_colop.h"
using namespace WriteEngine;

#include "messagelog.h"
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

        colOp.initColumn( curCol );
        // SYSTABLE

        cout << "Creating System Catalog..." << endl;

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

        cout << "Creating Dictionary..." << endl;
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

        cout << "Creating Dictionary..." << endl;
        //Dictionary files
        rc = fWriteEngine.createStore (txnID, 2040, 2041, 2042);

        // TableName
        rc = colOp.createColumn( curCol, 1, 8, WriteEngine::VARCHAR, WriteEngine::WR_CHAR, 1032 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

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
        SystemCatalogBuilder::build();
    }

    void tearDown()
    {
        destroySemaphores();
        destroyShmseg();
        unlink("/tmp/oidbitmap");
    }

class DDLPackageProcessorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DDLPackageProcessorTest);
   
    /*
    CPPUNIT_TEST( test_createtable );
    CPPUNIT_TEST( test_createtable_region );
    CPPUNIT_TEST( test_createindex );
    CPPUNIT_TEST( test_createuniqueindex );

    CPPUNIT_TEST( test_altertable_renamecolumn );
    CPPUNIT_TEST( test_altertable_renamecolumnwithcons );

    CPPUNIT_TEST( test_altertable_addacolumn );
    CPPUNIT_TEST( test_altertable_addacolumnconstrain );
    CPPUNIT_TEST( test_altertable_addacolumnunique );
    CPPUNIT_TEST( test_altertable_dropacolumn );
    CPPUNIT_TEST( test_altertable_dropcolumns );
    CPPUNIT_TEST( test_altertable_addcolumns );
    CPPUNIT_TEST( test_altertable_addtableconstraint );
    CPPUNIT_TEST( test_altertable_droptableconstraint ); 
    CPPUNIT_TEST( test_altertable_setcolumndefault );
    CPPUNIT_TEST( test_altertable_dropcolumndefault ); 
    CPPUNIT_TEST( test_altertable_renametable ); 
    CPPUNIT_TEST( test_dropindex );
    CPPUNIT_TEST( test_droptable );  */

    CPPUNIT_TEST_SUITE_END();
private:

public:

    /*
     * destroySemaphores() and destroyShmseg() will print error messages
     * if there are no objects to destroy.  That's OK.
     */
    void test_createtable_region()
    {
	cout << "Begining create region table testing..." << endl;
        std::string sqlbuf = "create table region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));"; 
        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                CreateTableProcessor processor;
                processor.setDebugLevel(CreateTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                CreateTableProcessor::DDLResult result;

                result  = processor.processPackage(dynamic_cast<CreateTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_createtable()
    {
        //setUp();
        //removeSystemCatalog();
        //createSystemCatalog();
	cout << "Begining create table testing..." << endl;
        std::string sqlbuf = "create table tpch.part( p_partkey integer NOT NULL ,p_name varchar(55) default 'helloworld', p_mfgr char(6), p_brand char(10) , p_type varchar(25) default 'foobar' , p_size integer , p_container char(10) ,p_retailprice integer , p_comment varchar(23), CONSTRAINT PK_PART PRIMARY KEY(p_partkey) )"; 
        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                CreateTableProcessor processor;
                processor.setDebugLevel(CreateTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
		        
				CreateTableStatement& ct = dynamic_cast<CreateTableStatement&>(stmt);
					
				cout << "Parsed CreateTable:" << endl;
				cout << ct << endl;

                CreateTableProcessor::DDLResult result;

                result  = processor.processPackage(ct);
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_createindex()
    {
        cout << "Begining create index test ... " << endl;
        std::string sqlbuf = "CREATE  INDEX test_idx ON tpch.part (p_size)";
        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();

            try
            {
                CreateIndexProcessor processor;
                processor.setDebugLevel(CreateIndexProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                CreateIndexProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_createuniqueindex()
    {
	cout << "Begining create unique index test ..." << endl;
        std::string sqlbuf = "CREATE UNIQUE INDEX test_idx ON tpch.part (p_mfgr)";
        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();

            try
            {
                CreateIndexProcessor processor;
                processor.setDebugLevel(CreateIndexProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                CreateIndexProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_altertable_addacolumn()
    {
	cout << "Begoning Alter Table Add column test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part ADD COLUMN c3 char(50)";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_addacolumnconstrain()
    {
        cout << "Begining Alter Table add column constraint test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part ADD COLUMN c3 char(50) NOT NULL";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

   void test_altertable_addacolumnunique()
    {
        cout << "Begining Alter Table add column with constraint test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part ADD COLUMN c3 char(50) UNIQUE";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_altertable_dropacolumn()
    {
        cout << "Begining Alter Table drop a column test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part DROP p_size ";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_dropcolumns()
    {
        cout << "Begining Alter Table drop columns test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part DROP p_size, DROP p_type ";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_addcolumns()
    {
        cout << "Begining Alter Table add columns test ..." << endl;
        std::string sqlbuf = "ALTER TABLE test ADD COLUMN c3 char(50), ADD COLUMN c4 char(10), ADD COLUMN C5 int;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_dropindex()
    {
        cout << "Begining Alter Table drop index test ..." << endl;
        std::string sqlbuf = "drop index tpch.test_idx";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();

            try
            {
                DropIndexProcessor processor;
                processor.setDebugLevel(DropIndexProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                DropIndexProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<DropIndexStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_altertable_renamecolumn()
    {
        cout << "Begining Alter Table rename a column ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part RENAME COLUMN p_size TO size;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }


    void test_altertable_renamecolumnwithcons()
    {
        cout << "Begining Alter Table rename a column with constraints ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part RENAME COLUMN p_partkey TO partlkey;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_altertable_addtableconstraint()
    {
        cout << "Begining Alter Table add table constraint test ... " << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part add CONSTRAINT unique(p_type);";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_droptableconstraint()
    {
        cout << "Begining Alter Table drop table constraint test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part drop constraint pk_tpch cascade;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_setcolumndefault()
    {
        cout << "Begining Alter Table set column default test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part alter p_partkey set default 3 ;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }
    void test_altertable_dropcolumndefault()
    {
        cout << "Begining Alter Table drop column default test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.part ALTER COLUMN p_type DROP DEFAULT; ;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_altertable_renametable()
    {
        cout << "Begining Alter Table rename a table test ..." << endl;
        std::string sqlbuf = "ALTER TABLE tpch.region rename to tpch.ready ;";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
            try
            {
                AlterTableProcessor processor;
				processor.setDebugLevel(AlterTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                AlterTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

    void test_droptable()
    {
        cout << "Begining drop table test ..." << endl;
        std::string sqlbuf = "drop table tpch.part";

        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();

            try
            {
                DropTableProcessor processor;
                processor.setDebugLevel(DropTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                DropTableProcessor::DDLResult result;

                result = processor.processPackage(dynamic_cast<DropTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                tearDown();
                throw;
            }
        }
        tearDown();
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( DDLPackageProcessorTest );

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
    return (wasSuccessful ? 0 : 1);
}
