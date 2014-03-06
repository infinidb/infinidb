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
#include <algorithm>
#include <functional>
#include <sstream>
#include <exception>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <memory>

#include <cppunit/extensions/HelperMacros.h>

#include "sqlparser.h"
#include "messagequeue.h"

using namespace std;
using namespace ddlpackage;
using namespace messageqcpp;

std::string itoa(const int i);

bool parse_file(char* fileName)
{
	SqlFileParser parser;
 	parser.Parse(fileName);
	return parser.Good();
}

class ParserTest : public CppUnit::TestFixture {
	CPPUNIT_TEST_SUITE(ParserTest);
	CPPUNIT_TEST(atac01);
	CPPUNIT_TEST(atac05);
	CPPUNIT_TEST(atac09);
	CPPUNIT_TEST(atmcdd01);
	CPPUNIT_TEST(atrt01);
	CPPUNIT_TEST(ct02);
	CPPUNIT_TEST(ct06);
	CPPUNIT_TEST(ct10);
	CPPUNIT_TEST(dt02);
	CPPUNIT_TEST(atac02);
	CPPUNIT_TEST(atac06);
	CPPUNIT_TEST(atatc01);
	CPPUNIT_TEST(atmcsd01);
	CPPUNIT_TEST(ci01);
	CPPUNIT_TEST(ct03);
	CPPUNIT_TEST(ct07);
	CPPUNIT_TEST(ct11);
	CPPUNIT_TEST(empty_stmt);
	CPPUNIT_TEST(atac03);
	CPPUNIT_TEST(atac07);
	CPPUNIT_TEST(atdc01);
	CPPUNIT_TEST(atdc02);
	CPPUNIT_TEST(atdc03);
	CPPUNIT_TEST(atmct01);
	CPPUNIT_TEST(atmct02);
	CPPUNIT_TEST(comment);
	CPPUNIT_TEST(ct04);
	CPPUNIT_TEST(ct08);
	CPPUNIT_TEST(di01);
	CPPUNIT_TEST(atac04);
	CPPUNIT_TEST(atac08);
	CPPUNIT_TEST(atdtc01);
	CPPUNIT_TEST(atdtc02);
	CPPUNIT_TEST(atrc01);
	CPPUNIT_TEST(ct01);
	CPPUNIT_TEST(ct05);
	CPPUNIT_TEST(ct09);
	CPPUNIT_TEST(dt01);
	CPPUNIT_TEST(foo);
	
	CPPUNIT_TEST_SUITE_END();
private:
	
	
public:
	void setUp()
	{
	}
	void tearDown()
	{
	}

	void foo()
	{
		SqlParser p;
		p.Parse("drop table foo");
		cout << p.GetParseTree();
	}
	
	void atac01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac01.sql"));
	}
	void atac05()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac05.sql"));
	}
	void atac09()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac09.sql"));
	}
	void atmcdd01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atmcdd01.sql"));
	}
	void atrt01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atrt01.sql"));
	}
	void ct02()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct02.sql"));
	}
	void ct06()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct06.sql"));
	}
	void ct10()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct10.sql"));
	}
	void dt02()
	{
		CPPUNIT_ASSERT(parse_file("sql/dt02.sql"));
	}
	void atac02()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac02.sql"));
	}
	void atac06()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac06.sql"));
	}
	void atatc01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atatc01.sql"));
	}
	void atmcsd01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atmcsd01.sql"));
	}
	void ci01()
	{
		CPPUNIT_ASSERT(parse_file("sql/ci01.sql"));
	}
	void ct03()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct03.sql"));
	}
	void ct07()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct07.sql"));
	}
	void ct11()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct11.sql"));
	}
	void empty_stmt()
	{
		CPPUNIT_ASSERT(parse_file("sql/empty-stmt.sql"));
	}
	void atac03()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac03.sql"));
	}
	void atac07()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac07.sql"));
	}
	void atdc01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atdc01.sql"));
	}
	void atdc02()
	{
		CPPUNIT_ASSERT(parse_file("sql/atdc02.sql"));
	}
	void atdc03()
	{
		CPPUNIT_ASSERT(parse_file("sql/atdc03.sql"));
	}
	void atmct01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atmct01.sql"));
	}
	void atmct02()
	{
		CPPUNIT_ASSERT(parse_file("sql/atmct02.sql"));
	}
	void comment()
	{
		CPPUNIT_ASSERT(parse_file("sql/comment.sql"));
	}
	void ct04()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct04.sql"));
	}
	void ct08()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct08.sql"));
	}
	void di01()
	{
		CPPUNIT_ASSERT(parse_file("sql/di01.sql"));
	}
	void atac04()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac04.sql"));
	}
	void atac08()
	{
		CPPUNIT_ASSERT(parse_file("sql/atac08.sql"));
	}
	void atdtc01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atdtc01.sql"));
	}
	void atdtc02()
	{
		CPPUNIT_ASSERT(parse_file("sql/atdtc02.sql"));
	}
	void atrc01()
	{
		CPPUNIT_ASSERT(parse_file("sql/atrc01.sql"));
	}
	void ct01()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct01.sql"));
	}
	void ct05()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct05.sql"));
	}
	void ct09()
	{
		CPPUNIT_ASSERT(parse_file("sql/ct09.sql"));
	}
	void dt01()
	{
		CPPUNIT_ASSERT(parse_file("sql/dt01.sql"));
	}
}; 


template<class T>
void u_sertest(T* x)
{
	ByteStream bs;
	stringstream s1,s2;
	auto_ptr<T> y(new T);

	x->serialize(bs);
	y->unserialize(bs);
	
	s1 << *x;
	s2 << *y;

	cout << "String Compare" << endl;
	cout << "------------------" << endl;
	cout << s1.str() << endl
		 << s2.str() << endl;
	cout << "------------------" << endl;

	CPPUNIT_ASSERT(s1.str() == s2.str());
}


/** @brief Just like u_sertest except that a typecode is pulled off
	the ByteStream before the unserialize. */
template<class T>
void t_sertest(T* x)
{
 	ByteStream bs;
 	stringstream s1,s2;
 	auto_ptr<T> y(new T);
	
	x->serialize(bs);

	quadbyte type;
	bs >> type;

	y->unserialize(bs);
	
	s1 << *x;
	s2 << *y;

	cout << "String Compare" << endl;
	cout << "------------------" << endl;
	cout << s1.str() << endl
		 << s2.str() << endl;
	cout << "------------------" << endl;

	CPPUNIT_ASSERT(s1.str() == s2.str());
}


class SerializeTest : public CppUnit::TestFixture {

	CPPUNIT_TEST_SUITE(SerializeTest);
	CPPUNIT_TEST(qname);
	CPPUNIT_TEST(columntype);
	CPPUNIT_TEST(columndefaultvalue);
	CPPUNIT_TEST(columnconstraintdef);
	CPPUNIT_TEST(columndef_01);
	CPPUNIT_TEST(referentialaction);
	CPPUNIT_TEST(tablecheckconstraint);
	CPPUNIT_TEST(tableuniqueconstraint);
	CPPUNIT_TEST(tableprimarykeyconstraint);
	CPPUNIT_TEST(tablereferencesconstraint);
	CPPUNIT_TEST(tabledef);

	CPPUNIT_TEST(createtable);
	CPPUNIT_TEST(droptable);
	CPPUNIT_TEST(createindex);
	CPPUNIT_TEST(dropindex);

	CPPUNIT_TEST(altertableaddcolumn);
	CPPUNIT_TEST(altertableaddcolumns);
	CPPUNIT_TEST(altertabledropcolumns);
	CPPUNIT_TEST(altertableaddtableconstraint);
	CPPUNIT_TEST(altertabledropcolumn);

	CPPUNIT_TEST(altertablesetcolumndefault);
	CPPUNIT_TEST(altertabledropcolumndefault);
	CPPUNIT_TEST(altertabledroptableconstraint);

	CPPUNIT_TEST(altertablerenametable);

	CPPUNIT_TEST(altertablemodifycolumntype);

	CPPUNIT_TEST(altertablerenamecolumn);
	
	
 	CPPUNIT_TEST_SUITE_END();

private:
	
	
public:
	void setUp()
	{
	}


	void tearDown()
	{
	}

	void altertablerenamecolumn()
	{
		SqlFileParser p;
		p.Parse("sql/atrc01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}


	void altertablemodifycolumntype()
	{
		SqlFileParser p;
		p.Parse("sql/atmct01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}


	void altertablerenametable()
	{
		SqlFileParser p;
		p.Parse("sql/atrt01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}

	void altertabledroptableconstraint()
	{
		SqlFileParser p;
		p.Parse("sql/atdtc01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}

	void altertabledropcolumndefault()
	{
		SqlFileParser p;
		p.Parse("sql/atmcdd01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}

	void altertablesetcolumndefault()
	{
		SqlFileParser p;
		p.Parse("sql/atmcsd01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}


	void altertabledropcolumn()
	{
		SqlFileParser p;
		p.Parse("sql/atdc01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}

	void altertableaddtableconstraint()
	{
		SqlFileParser p;
		p.Parse("sql/atatc01.sql");
		if(p.Good())
		{
			const ParseTree& stmts = p.GetParseTree();
			AlterTableStatement* stmt = dynamic_cast<AlterTableStatement*>(stmts[0]);
			CPPUNIT_ASSERT(stmt);
			t_sertest<AlterTableStatement>(stmt);
		}
	}


	void altertableaddcolumn()
	{
		cout << "Serialize test: AtaAddColumn" << endl;
		auto_ptr<AtaAddColumn> ata(new AtaAddColumn);

		vector<string> files;
		files.push_back("sql/ct01.sql");
		files.push_back("sql/ct02.sql");
		files.push_back("sql/ct03.sql");
		files.push_back("sql/ct04.sql");
		files.push_back("sql/ct05.sql");
		files.push_back("sql/ct06.sql");
		files.push_back("sql/ct07.sql");
		files.push_back("sql/ct08.sql");
		files.push_back("sql/ct09.sql");
		files.push_back("sql/ct10.sql");
		files.push_back("sql/ct11.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateTableStatement* ct = dynamic_cast<CreateTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(ct);

				ColumnDefList* columns = &(ct->fTableDef->fColumns);

				ColumnDefList::const_iterator itr;
				for(itr = columns->begin();
					itr != columns->end();
					++itr)
				{
					ata->fColumnDef = *itr;
					t_sertest<AtaAddColumn>(ata.get());
					// We borrowed the column def from the parse
					// tree.  Null it to avoid double free.
					ata->fColumnDef = 0;
				}
			}
		}
	}

	void altertableaddcolumns()
	{
		cout << "Serialize test: AtaAddColumnS" << endl;
		auto_ptr<AtaAddColumns> ata(new AtaAddColumns);

		vector<string> files;
		files.push_back("sql/ct01.sql");
		files.push_back("sql/ct02.sql");
		files.push_back("sql/ct03.sql");
		files.push_back("sql/ct04.sql");
		files.push_back("sql/ct05.sql");
		files.push_back("sql/ct06.sql");
		files.push_back("sql/ct07.sql");
		files.push_back("sql/ct08.sql");
		files.push_back("sql/ct09.sql");
		files.push_back("sql/ct10.sql");
		files.push_back("sql/ct11.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateTableStatement* ct = dynamic_cast<CreateTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(ct);

				ata->fColumns = ct->fTableDef->fColumns;
				t_sertest<AtaAddColumns>(ata.get());
				// We borrowed the column defs from the parse
				// tree.  Null it to avoid double free.
				ata->fColumns.clear();
			}
		}
	}


	void altertabledropcolumns()
	{
		ColumnNameList* names = new ColumnNameList;
		names->push_back("c1");
		names->push_back("c2");
		auto_ptr<AtaDropColumns> stmt(new AtaDropColumns(names));
		t_sertest<AtaDropColumns>(stmt.get());
	}
	

	void qname()
	{
		cout << "Serialize test: QualifiedName" << endl;
		auto_ptr<QualifiedName> name(new QualifiedName("one","two","three"));
		u_sertest<QualifiedName>(name.get());
	}
	

	void columntype()
	{
		cout << "Serialize test: ColumnType" << endl;
		
		auto_ptr<ColumnType> type(new ColumnType);
		type->fType = 1;
		type->fLength = 2;
		type->fPrecision = 3;
		type->fScale = 4;
		type->fWithTimezone = true;
		u_sertest<ColumnType>(type.get());
	}

	void columndefaultvalue()
	{
		cout << "Serialize test: ColumnDefaultValue" << endl;
		auto_ptr<ColumnDefaultValue> dval(new ColumnDefaultValue);
		dval->fNull = false;
		dval->fValue = "1.234";
		u_sertest<ColumnDefaultValue>(dval.get());
	}
	
	void columnconstraintdef()
	{
		cout << "Serialize test: ColumnConstraintDef" << endl;
		auto_ptr<ColumnConstraintDef> con(new ColumnConstraintDef);
		con->fDeferrable = true;
		con->fCheckTime = DDL_INITIALLY_DEFERRED;
		con->fConstraintType = DDL_CHECK;
		con->fCheck = "thecheck";
		u_sertest<ColumnConstraintDef>(con.get());
	}

	/** @brief Can we serialize ColumDefs? 
	 *
	 * Here we leverage the parser and the ostream operators for
	 * parser objects to test serialization for ColumnDefs.  We use
	 * the parser to build parse trees for some create table
	 * statements.  Then we push the ColumnDefs through a ByteStream
	 * and compare the results as strings.  In other words, if C is a
	 * ColumnDef in the parser constructed tree, we write C to the
	 * ByteStream and then unserialize it back out making a new ColumnDef,
	 * C'.  We then format C and C' to respective stringstreams and
	 * then compare the strings.  If they are equal, chances are good
	 * the serialization is OK.
	 */

	void columndef_01()
	{
		cout << "columndef_01" << endl;

		vector<string> files;
		files.push_back("sql/ct01.sql");
		files.push_back("sql/ct02.sql");
		files.push_back("sql/ct03.sql");
		files.push_back("sql/ct04.sql");
		files.push_back("sql/ct05.sql");
		files.push_back("sql/ct06.sql");
		files.push_back("sql/ct07.sql");
		files.push_back("sql/ct08.sql");
		files.push_back("sql/ct09.sql");
		files.push_back("sql/ct10.sql");
		files.push_back("sql/ct11.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking columndef serialization for " << *itr << endl;
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateTableStatement* ct = dynamic_cast<CreateTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(ct);
		
				cout << "Parsed CreateTable:" << endl;
				cout << *ct << endl;

				ColumnDefList* columns = &(ct->fTableDef->fColumns);

				ColumnDefList::const_iterator itr;
				for(itr = columns->begin();
					itr != columns->end();
					++itr)
				{
					u_sertest<ColumnDef>(*itr);
				}
			}
		}
		
	}
	
	void referentialaction()
	{
		cout << "Serialize test: ReferentialAction" << endl;
		auto_ptr<ReferentialAction> ref(new ReferentialAction);
		ref->fOnUpdate = DDL_CASCADE;
		ref->fOnDelete = DDL_SET_NULL;
		u_sertest<ReferentialAction>(ref.get());
	}

	void tablecheckconstraint()
	{
		cout << "Serialize test: TableCheckConstraintDef" << endl;
		auto_ptr<TableCheckConstraintDef> tc(new TableCheckConstraintDef);
		tc->fName = "fooby";
		tc->fCheck = "check constraint text";
		u_sertest<TableCheckConstraintDef>(tc.get());
	}

	void tableuniqueconstraint()
	{
		cout << "Serialize test: TableUniqueConstraintDef" << endl;
		auto_ptr<TableUniqueConstraintDef> tc(new TableUniqueConstraintDef);
		tc->fName = "fooby";
		tc->fColumnNameList.push_back("C1");
		tc->fColumnNameList.push_back("C2");
		tc->fColumnNameList.push_back("C3");
		u_sertest<TableUniqueConstraintDef>(tc.get());
	}

	void tableprimarykeyconstraint()
	{
		cout << "Serialize test: TablePrimaryKeyConstraintDef" << endl;
		auto_ptr<TablePrimaryKeyConstraintDef> tc(new TablePrimaryKeyConstraintDef);
		tc->fName = "fooby";
		tc->fColumnNameList.push_back("C1");
		tc->fColumnNameList.push_back("C2");
		tc->fColumnNameList.push_back("C3");
		u_sertest<TablePrimaryKeyConstraintDef>(tc.get());
	}
	
	void tablereferencesconstraint()
	{
		cout << "Serialize test: TableReferencesConstraintDef" << endl;
		auto_ptr<TableReferencesConstraintDef> tc(new TableReferencesConstraintDef);

		tc->fName = "fooby";
		tc->fColumns.push_back("C1");
		tc->fColumns.push_back("C2");
		tc->fColumns.push_back("C3");

		QualifiedName* qname = new QualifiedName;
		qname->fCatalog = "";
		qname->fSchema = "calpont";
		qname->fName = "table_fooby";
		tc->fTableName = qname;
			

		tc->fForeignColumns.push_back("F1");
		tc->fForeignColumns.push_back("F2");
		tc->fForeignColumns.push_back("F3");

		tc->fMatchType = DDL_FULL;

		ReferentialAction* ref = new ReferentialAction;
		ref->fOnUpdate = DDL_CASCADE;
		ref->fOnDelete = DDL_NO_ACTION;
		tc->fRefAction = ref;

		u_sertest<TableReferencesConstraintDef>(tc.get());
	}
	
	void tabledef()
	{
		cout << "tabledef" << endl;

		vector<string> files;
		files.push_back("sql/ct01.sql");
		files.push_back("sql/ct02.sql");
		files.push_back("sql/ct03.sql");
		files.push_back("sql/ct04.sql");
		files.push_back("sql/ct05.sql");
		files.push_back("sql/ct06.sql");
		files.push_back("sql/ct07.sql");
		files.push_back("sql/ct08.sql");
		files.push_back("sql/ct09.sql");
		files.push_back("sql/ct10.sql");
		files.push_back("sql/ct11.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking tabledef serialization for " << *itr << endl;
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateTableStatement* ct = dynamic_cast<CreateTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(ct);
		
				cout << "Parsed CreateTable:" << endl;
				cout << *ct << endl;

				u_sertest<TableDef>(ct->fTableDef);
			}
		}
		
	}
	

	void createtable()
	{
		vector<string> files;
		files.push_back("sql/ct01.sql");
		files.push_back("sql/ct02.sql");
		files.push_back("sql/ct03.sql");
		files.push_back("sql/ct04.sql");
		files.push_back("sql/ct05.sql");
		files.push_back("sql/ct06.sql");
		files.push_back("sql/ct07.sql");
		files.push_back("sql/ct08.sql");
		files.push_back("sql/ct09.sql");
		files.push_back("sql/ct10.sql");
		files.push_back("sql/ct11.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking CreateTableStatement serialization for " << *itr << endl;
			SqlFileParser p;
			p.setDefaultSchema("tpch");
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateTableStatement* ct = dynamic_cast<CreateTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(ct);
		
				cout << "Parsed CreateTable:" << endl;
				cout << *ct << endl;

				t_sertest<CreateTableStatement>(ct);
			}
		}
	}
	
	void droptable()
	{
		vector<string> files;
		files.push_back("sql/dt01.sql");
		files.push_back("sql/dt02.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking DropTableStatement serialization for " << *itr << endl;
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				DropTableStatement* stmt = dynamic_cast<DropTableStatement*>(stmts[0]);
				CPPUNIT_ASSERT(stmt);
		
				cout << "Parsed DropTable:" << endl;
				cout << *stmt << endl;

				t_sertest<DropTableStatement>(stmt);
			}
		}
	}
	void createindex()
	{
		vector<string> files;
		files.push_back("sql/ci01.sql");
		files.push_back("sql/ci02.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking Create Index serialization for " << *itr << endl;
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				CreateIndexStatement* stmt = dynamic_cast<CreateIndexStatement*>(stmts[0]);
				CPPUNIT_ASSERT(stmt);
		
				cout << "Parsed:" << endl;
				cout << *stmt << endl;

				t_sertest<CreateIndexStatement>(stmt);
			}
		}
	}
	void dropindex()
	{
		vector<string> files;
		files.push_back("sql/di01.sql");

		vector<string>::const_iterator itr;
		for(itr = files.begin(); itr != files.end(); ++itr)
		{
			cout << "* * * Checking Drop Index serialization for " << *itr << endl;
			SqlFileParser p;
			p.Parse(*itr);
			if(p.Good())
			{
				const ParseTree& stmts = p.GetParseTree();
				DropIndexStatement* stmt = dynamic_cast<DropIndexStatement*>(stmts[0]);
				CPPUNIT_ASSERT(stmt);
		
				cout << "Parsed:" << endl;
				cout << *stmt << endl;

				t_sertest<DropIndexStatement>(stmt);
			}
		}
	}
}; 

CPPUNIT_TEST_SUITE_REGISTRATION(SerializeTest);
CPPUNIT_TEST_SUITE_REGISTRATION(ParserTest);

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

