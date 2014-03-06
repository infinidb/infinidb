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

#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>

#include <cppunit/extensions/HelperMacros.h>

#include "calpontdmlpackage.h"
#include "dmlpackage.h"
#include "calpontdmlfactory.h"
#include "vendordmlstatement.h"
#include "insertdmlpackage.h"
#include "deletedmlpackage.h"
#include "updatedmlpackage.h"
#include "commanddmlpackage.h"
#include "messagequeue.h"
#include "calpontdmlpackage.h"
#include "dmlparser.h"

using namespace std;
using namespace dmlpackage;
using namespace messageqcpp;

bool parse_file(char* fileName)
{
    DMLFileParser parser;
    parser.parse(fileName);
    bool good = parser.good();
    if (good)
    {
        const ParseTree &ptree = parser.getParseTree();

        cout << "Parser succeeded." << endl;
        cout << ptree.fList.size() << " " << "SQL statements" << endl;
        cout << ptree.fSqlText << endl;
        cout << ptree;

        SqlStatement* statementPtr = ptree[0];
        if (statementPtr)
            cout << statementPtr->getQueryString();
        cout << endl;
    }


    return good;
}

class DMLParserTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DMLParserTest );
    CPPUNIT_TEST( test_i01 );
    CPPUNIT_TEST( test_i02 );
    CPPUNIT_TEST( test_i03 );
    CPPUNIT_TEST( test_i04 );
    CPPUNIT_TEST( test_u01 );
    CPPUNIT_TEST( test_u02 );
    CPPUNIT_TEST( test_d01 );
    CPPUNIT_TEST( test_d02 );
    CPPUNIT_TEST( test_d03 );
    CPPUNIT_TEST( test_d04 );
    CPPUNIT_TEST_SUITE_END();

private:

public:
    void setUp() {}

    void tearDown() {}

    void test_i01() { CPPUNIT_ASSERT(parse_file("sql/i01.sql")); }

    void test_i02() { CPPUNIT_ASSERT(parse_file("sql/i02.sql")); }

    void test_i03() { CPPUNIT_ASSERT(parse_file("sql/i03.sql")); }

    void test_i04() { CPPUNIT_ASSERT(parse_file("sql/i04.sql")); }

    void test_u01() { CPPUNIT_ASSERT(parse_file("sql/u01.sql")); }

    void test_u02() { CPPUNIT_ASSERT(parse_file("sql/u02.sql")); }

    void test_d01() { CPPUNIT_ASSERT(parse_file("sql/d01.sql")); }

    void test_d02() { CPPUNIT_ASSERT(parse_file("sql/d02.sql")); }

    void test_d03() { CPPUNIT_ASSERT(parse_file("sql/d03.sql")); }

    void test_d04() { CPPUNIT_ASSERT(parse_file("sql/d04.sql")); }
};

class DMLTest : public CppUnit::TestFixture
{

    CPPUNIT_TEST_SUITE( DMLTest );
   // CPPUNIT_TEST( test_direct_insert );
   // CPPUNIT_TEST( test_query_insert );
    CPPUNIT_TEST( test_direct_update );
   // CPPUNIT_TEST( test_query_update );
   // CPPUNIT_TEST( test_delete_all );
   // CPPUNIT_TEST( test_delete_query );
   // CPPUNIT_TEST( test_commit );
   // CPPUNIT_TEST( test_rollback );
    CPPUNIT_TEST_SUITE_END();

private:
public:
    void setUp()  {}

    void tearDown() {}

    void test_direct_insert()
    {

        ByteStream  bytestream;
        std::string dmlStatement = "INSERT INTO tpch.supplier (supplier_id, supplier_name) VALUES(24553, 'IBM');";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );

        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_insert_object(bytestream);

    }

    void test_query_insert()
    {
        ByteStream  bytestream;
        std::string dmlStatement = "INSERT INTO supplier (supplier_id, supplier_name) SELECT account_no, name FROM customers WHERE city = 'Newark';";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        if ( pDMLPackage->HasFilter() )
        {
            cout << "This INSERT statement has a filter:" << endl;
            cout << pDMLPackage->get_QueryString() << endl;
        }

        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_insert_object(bytestream);

    }

    void write_DML_object( ByteStream& bs, CalpontDMLPackage* pDMLPackage )
    {

        pDMLPackage->write( bs );
    }

    void read_insert_object( ByteStream& bs )
    {

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_INSERT == package_type );

        InsertDMLPackage *pObject = new InsertDMLPackage();

        pObject->read( bs );

        delete pObject;

    }

    void test_delete_all()
    {

        ByteStream  bytestream;
        std::string dmlStatement = "DELETE FROM tpch.part;";

        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);

        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_delete_object(bytestream);
    }

    void test_delete_query()
    {
        ByteStream  bytestream;
        std::string dmlStatement = "DELETE FROM tpch.supplier WHERE supplier_name = 'IBM';";

        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);

        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        if (pDMLPackage->HasFilter())
        {
            cout << "This DELETE statement has a filter:" << endl;
            cout << pDMLPackage->get_QueryString() << endl;
        }
        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_delete_object(bytestream);

    }

    void read_delete_object( ByteStream& bs )
    {

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_DELETE == package_type );

        DeleteDMLPackage *pObject = new DeleteDMLPackage();

        pObject->read( bs );

        delete pObject;

    }

    void test_direct_update()
    {
        ByteStream bytestream;
        std::string dmlStatement = "UPDATE tpch.part SET p_partno = 1, p_name = 'joe' where p_partno=2;";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_update_object(bytestream);
    }

    void test_query_update()
    {

        ByteStream  bytestream;
        std::string dmlStatement = "UPDATE tpch.supplier SET supplier_name='joe',supplier_state='ca' WHERE EXISTS ( SELECT customer.name FROM customers WHERE customers.customer_id = supplier.supplier_id);";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement, 1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        if (pDMLPackage->HasFilter())
        {
            cout << "This UPDATE statement has a filter:" << endl;
            cout << pDMLPackage->get_QueryString() << endl;
        }

        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_update_object(bytestream);
    }

    void read_update_object( ByteStream& bs )
    {

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_UPDATE == package_type );

        UpdateDMLPackage *pObject = new UpdateDMLPackage();

        pObject->read( bs );

        delete pObject;

    }

    void test_commit()
    {
        ByteStream bytestream;
        std::string dmlStatement = "COMMIT;";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_command_object(bytestream);

    }

    void test_rollback()
    {
        ByteStream bytestream;
        std::string dmlStatement = "ROLLBACK;";
        cout << dmlStatement << endl;

        VendorDMLStatement dmlStmt(dmlStatement,1);
        CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
        CPPUNIT_ASSERT( 0 != pDMLPackage );
        write_DML_object(bytestream, pDMLPackage);
        delete pDMLPackage;
        read_command_object(bytestream);
    }

    void read_command_object( ByteStream& bs )
    {

        ByteStream::byte package_type;
        bs >> package_type;

        CPPUNIT_ASSERT( DML_COMMAND == package_type );

        CommandDMLPackage *pObject = new CommandDMLPackage();

        pObject->read( bs );

        delete pObject;

    }
};

//CPPUNIT_TEST_SUITE_REGISTRATION( DMLParserTest );
CPPUNIT_TEST_SUITE_REGISTRATION( DMLTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );
    bool wasSuccessful = runner.run( "", false );
    return (wasSuccessful ? 0 : 1);
}
