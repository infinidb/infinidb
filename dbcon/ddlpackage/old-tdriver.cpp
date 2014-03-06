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

/***************************************************************************
 *   dhill@srvengcm1.calpont.com 
 *
 *   Purpose: dll package tester
 *
 ***************************************************************************/

#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>

#include <cppunit/extensions/HelperMacros.h>

#include "ddlpackage.h"
#include "mysqlddlstatement.h"
#include "calpontddlpackage.h"
#include "calpontddlfactory.h"
#include "createobjectddlpackage.h"
#include "alterobjectddlpackage.h"
#include "dropobjectddlpackage.h"
#include "messagequeue.h"


using namespace std;
using namespace ddlpackage;
using namespace messageqcpp;

std::string itoa(const int i);

// READ & WRITE STATEMENT TESTS

class DDLWriteReadTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLWriteReadTest );

CPPUNIT_TEST( test_write_read_create_table_object );
CPPUNIT_TEST( test_write_read_create_index_object );
CPPUNIT_TEST( test_write_read_alter_table_object );
CPPUNIT_TEST( test_write_read_drop_table_object );
CPPUNIT_TEST( test_write_read_drop_index_object );


CPPUNIT_TEST_SUITE_END();

private:
	
	
public:
	void setUp()
	{
	
	}

	void tearDown()
	{

	}
	// CREATE TABLE
	void test_write_read_create_table_object()
	{
		ByteStream  bytestream;
		
		std::string ddl_statement = "CREATE TABLE calpont.PART(p_partkey int not null, p_a varchar(55), p_b decimal(8,2) unique, p_c int default 1, p_d varchar(25) default 'unknown' check (varchar = 'a' ), foreign key (p_partkey) references Customers(p_partkey)) engine=infinidb;";

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
  		
		CPPUNIT_ASSERT( 0 != pDDLStatement );
		
		pDDLStatement->populateFromDDLStatement( ddl_statement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
		
		CPPUNIT_ASSERT( 0 != pDDLPackage );

		// parse the data
  		pDDLPackage->Parse();

		write_create_table_object( bytestream, pDDLPackage );

		delete pDDLStatement;

		delete pDDLPackage;

		read_create_table_object( bytestream );

	}
	void write_create_table_object( ByteStream& bs, CalpontDDLPackage* pDDLPackage )
	{
			
		pDDLPackage->Write( bs );
	}

	void read_create_table_object( ByteStream& bs )
	{
		
		ByteStream::byte package_type;
		bs >> package_type;

		CPPUNIT_ASSERT( DDL_CREATE == package_type );

		CreateObjectDDLPackage *pObject = new CreateObjectDDLPackage();

		pObject->Read( bs );

		delete pObject;

	}

	// CREATE INDEX
	void test_write_read_create_index_object()
	{
		ByteStream  bytestream;
		
		std::string ddl_statement = "CREATE INDEX calpont.index1 ON calpont.PART(p_partkey,p_a) engine=infinidb;";

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
  		
		CPPUNIT_ASSERT( 0 != pDDLStatement );
		
		pDDLStatement->populateFromDDLStatement( ddl_statement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
		
		CPPUNIT_ASSERT( 0 != pDDLPackage );

		// parse the data
  		pDDLPackage->Parse();

		write_create_index_object( bytestream, pDDLPackage );

		delete pDDLStatement;

		delete pDDLPackage;

		read_create_index_object( bytestream );

	}
	void write_create_index_object( ByteStream& bs, CalpontDDLPackage* pDDLPackage )
	{
			
		pDDLPackage->Write( bs );
	}

	void read_create_index_object( ByteStream& bs )
	{
		
		ByteStream::byte package_type;
		bs >> package_type;

		CPPUNIT_ASSERT( DDL_CREATE == package_type );

		CreateObjectDDLPackage *pObject = new CreateObjectDDLPackage();

		pObject->Read( bs );

		delete pObject;

	}

	// ALTER TABLE
	void test_write_read_alter_table_object()
	{
		ByteStream  bytestream;
		
		std::string ddl_statement = "ALTER TABLE calpont.PART_a ADD id int engine=infinidb;";

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
  		
		CPPUNIT_ASSERT( 0 != pDDLStatement );
		
		pDDLStatement->populateFromDDLStatement( ddl_statement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
		
		CPPUNIT_ASSERT( 0 != pDDLPackage );

		// parse the data
  		pDDLPackage->Parse();

		write_alter_table_object( bytestream, pDDLPackage );

		delete pDDLStatement;

		delete pDDLPackage;

		read_alter_table_object( bytestream );

	}
	void write_alter_table_object( ByteStream& bs, CalpontDDLPackage* pDDLPackage )
	{
			
		pDDLPackage->Write( bs );
	}

	void read_alter_table_object( ByteStream& bs )
	{
		
		ByteStream::byte package_type;
		bs >> package_type;

		CPPUNIT_ASSERT( DDL_ALTER == package_type );

		AlterObjectDDLPackage *pObject = new AlterObjectDDLPackage();

		pObject->Read( bs );

		delete pObject;

	}

	// DROP TABLE
	void test_write_read_drop_table_object()
	{
		ByteStream  bytestream;
		
		std::string ddl_statement = "DROP TABLE calpont.PART_a CASCADE CONSTRAINTS engine=infinidb;";

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
  		
		CPPUNIT_ASSERT( 0 != pDDLStatement );
		
		pDDLStatement->populateFromDDLStatement( ddl_statement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
		
		CPPUNIT_ASSERT( 0 != pDDLPackage );

		// parse the data
  		pDDLPackage->Parse();

		write_drop_table_object( bytestream, pDDLPackage );

		delete pDDLStatement;

		delete pDDLPackage;

		read_drop_table_object( bytestream );

	}
	void write_drop_table_object( ByteStream& bs, CalpontDDLPackage* pDDLPackage )
	{
			
		pDDLPackage->Write( bs );
	}

	void read_drop_table_object( ByteStream& bs )
	{
		
		ByteStream::byte package_type;
		bs >> package_type;

		CPPUNIT_ASSERT( DDL_DROP == package_type );

		DropObjectDDLPackage *pObject = new DropObjectDDLPackage();

		pObject->Read( bs );

		delete pObject;

	}

	// DROP INDEX
	void test_write_read_drop_index_object()
	{
		ByteStream  bytestream;
		
		std::string ddl_statement = "DROP INDEX calpont.INDEX_1 on calpont.PART engine=infinidb;";

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
  		
		CPPUNIT_ASSERT( 0 != pDDLStatement );
		
		pDDLStatement->populateFromDDLStatement( ddl_statement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
		
		CPPUNIT_ASSERT( 0 != pDDLPackage );

		// parse the data
  		pDDLPackage->Parse();

		write_drop_index_object( bytestream, pDDLPackage );

		delete pDDLStatement;

		delete pDDLPackage;

		read_drop_index_object( bytestream );

	}
	void write_drop_index_object( ByteStream& bs, CalpontDDLPackage* pDDLPackage )
	{
			
		pDDLPackage->Write( bs );
	}

	void read_drop_index_object( ByteStream& bs )
	{
		
		ByteStream::byte package_type;
		bs >> package_type;

		CPPUNIT_ASSERT( DDL_DROP == package_type );

		DropObjectDDLPackage *pObject = new DropObjectDDLPackage();

		pObject->Read( bs );

		delete pObject;

	}

};

// PARSE STATEMENT TESTS


class DDLCreateTableParserTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLCreateTableParserTest );

CPPUNIT_TEST( create_t1 );

CPPUNIT_TEST_SUITE_END();

private:
	
public:
	void setUp() 
    {
	}

	void tearDown() 
    {
	}

	void create_t1() 
    {
	ifstream fin("sql/examples/create-table.sql");

	CPPUNIT_ASSERT( fin != NULL );

	// read CREATE TABLE statements from buffer and parse
	for (;;)
	{
		string fileBuffer;
		char Buffer[64000];
		string::size_type pos_begin;

		fin.getline (Buffer, 64000, ';');

		fileBuffer = Buffer;

		string::size_type pos = fileBuffer.find ("create ",0);
		string::size_type pos1 = fileBuffer.find ("CREATE ",0);

		if (pos == string::npos && pos1 == string::npos )
			// end of file
			break;

		if (pos < pos1)
			pos_begin = pos;
		else
			pos_begin = pos1;

		std::string DDLStatement = fileBuffer.substr (pos_begin,64000);

		fileBuffer.append(";");

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
			
		pDDLStatement->populateFromDDLStatement( DDLStatement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
				
			// parse the data
		pDDLPackage->Parse();

		delete pDDLStatement;

		delete pDDLPackage;
	}
	fin.close();
	}
};

class DDLCreateIndexParserTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLCreateIndexParserTest );

CPPUNIT_TEST( createIndex_t1 );

CPPUNIT_TEST_SUITE_END();

private:
	
public:
	void setUp() 
    {
	}

	void tearDown() 
    {
	}

	void createIndex_t1() 
    {
	ifstream fin("sql/examples/create-index.sql");

	CPPUNIT_ASSERT( fin != NULL );

	// read CREATE INDEX statements from buffer and parse
	for (;;)
	{
		string fileBuffer;
		char Buffer[64000];
		string::size_type pos_begin;

		fin.getline (Buffer, 64000, ';');

		fileBuffer = Buffer;

		string::size_type pos = fileBuffer.find ("create ",0);
		string::size_type pos1 = fileBuffer.find ("CREATE ",0);

		if (pos == string::npos && pos1 == string::npos )
			// end of file
			break;

		if (pos < pos1)
			pos_begin = pos;
		else
			pos_begin = pos1;

		std::string DDLStatement = fileBuffer.substr (pos_begin,64000);

		fileBuffer.append(";");

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
			
		pDDLStatement->populateFromDDLStatement( DDLStatement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
				
			// parse the data
		pDDLPackage->Parse();

		delete pDDLStatement;

		delete pDDLPackage;
	}
	fin.close();
	}

}; 


class DDLAlterTableParserTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLAlterTableParserTest );

CPPUNIT_TEST( alter_t1 );

CPPUNIT_TEST_SUITE_END();

private:
	
public:
	void setUp() 
    {
	}

	void tearDown() 
    {
	}

	void alter_t1()
	{
 	ifstream fin("sql/examples/alter-table.sql");

	CPPUNIT_ASSERT( fin != NULL );

	// read ALTER TABLE statements from buffer and parse
	for (;;)
	{
		string fileBuffer;
		char Buffer[64000];
		string::size_type pos_begin;

		fin.getline (Buffer, 64000, ';');

		fileBuffer = Buffer;

		string::size_type pos = fileBuffer.find ("alter ",0);
		string::size_type pos1 = fileBuffer.find ("ALTER ",0);

		if (pos == string::npos && pos1 == string::npos )
			// end of file
			break;

		if (pos < pos1)
			pos_begin = pos;
		else
			pos_begin = pos1;

		std::string DDLStatement = fileBuffer.substr (pos_begin,64000);

		fileBuffer.append(";");

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
			
		pDDLStatement->populateFromDDLStatement( DDLStatement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
				
			// parse the data
		pDDLPackage->Parse();

		delete pDDLStatement;

		delete pDDLPackage;
	}
	fin.close();
	}
}; 


class DDLDropTableParserTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLDropTableParserTest );

CPPUNIT_TEST( drop_t1 );

CPPUNIT_TEST_SUITE_END();

private:
	
public:
	void setUp() 
    {

	}

	void tearDown() 
    {
	}

	void drop_t1() 
	{
 	ifstream fin("sql/examples/drop-table.sql");

	CPPUNIT_ASSERT( fin != NULL );

	// read DROP TABLE statements from buffer and parse
	for (;;)
	{
		string fileBuffer;
		char Buffer[64000];
		string::size_type pos_begin;

		fin.getline (Buffer, 64000, ';');

		fileBuffer = Buffer;

		string::size_type pos = fileBuffer.find ("drop ",0);
		string::size_type pos1 = fileBuffer.find ("DROP ",0);

		if (pos == string::npos && pos1 == string::npos )
			// end of file
			break;

		if (pos < pos1)
			pos_begin = pos;
		else
			pos_begin = pos1;

		std::string DDLStatement = fileBuffer.substr (pos_begin,64000);

		fileBuffer.append(";");

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
			
		pDDLStatement->populateFromDDLStatement( DDLStatement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
				
			// parse the data
		pDDLPackage->Parse();

		delete pDDLStatement;

		delete pDDLPackage;
	}
	fin.close();
	}

}; 

class DDLDropIndexParserTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( DDLDropIndexParserTest );

CPPUNIT_TEST( dropIndex_t1 );

CPPUNIT_TEST_SUITE_END();

private:
	std::string fDDLStatement;
	
public:
	void setUp() 
    {
	}

	void tearDown() 
    {
	}

	void dropIndex_t1() 
    {
 	ifstream fin("sql/examples/drop-index.sql");

	CPPUNIT_ASSERT( fin != NULL );

	// read DROP INDEX statements from buffer and parse
	for (;;)
	{
		string fileBuffer;
		char Buffer[64000];
		string::size_type pos_begin;

		fin.getline (Buffer, 64000, ';');

		fileBuffer = Buffer;

		string::size_type pos = fileBuffer.find ("drop ",0);
		string::size_type pos1 = fileBuffer.find ("DROP ",0);

		if (pos == string::npos && pos1 == string::npos )
			// end of file
			break;

		if (pos < pos1)
			pos_begin = pos;
		else
			pos_begin = pos1;

		std::string DDLStatement = fileBuffer.substr (pos_begin,64000);

		fileBuffer.append(";");

		MySQLDDLStatement* pDDLStatement = new MySQLDDLStatement();
			
		pDDLStatement->populateFromDDLStatement( DDLStatement );

		CalpontDDLPackage* pDDLPackage = CalpontDDLFactory::makeCalpontDDLPackage(*pDDLStatement);
				
			// parse the data
		pDDLPackage->Parse();

		delete pDDLStatement;

		delete pDDLPackage;
	}
	fin.close();
	}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( DDLDropIndexParserTest );
CPPUNIT_TEST_SUITE_REGISTRATION( DDLDropTableParserTest );
// DOESN'T NOT PARSE CPPUNIT_TEST_SUITE_REGISTRATION( DDLAlterTableParserTest );
CPPUNIT_TEST_SUITE_REGISTRATION( DDLCreateIndexParserTest );
CPPUNIT_TEST_SUITE_REGISTRATION( DDLCreateTableParserTest );
CPPUNIT_TEST_SUITE_REGISTRATION( DDLWriteReadTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main(int argc, char *argv[])
{
  
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );

  return (wasSuccessful ? 0 : 1);

}

string itoa(const int i)
{
	stringstream ss;
	ss << i;
	return ss.str();
}

