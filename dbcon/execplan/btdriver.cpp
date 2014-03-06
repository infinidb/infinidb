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
#include <stdexcept>
#include <typeinfo>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include<sstream>
#include<exception>
#include<iostream>
#include <unistd.h>

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "expressionparser.h"
#include "constantcolumn.h"
#include "treenode.h"
#include "operator.h"
#include "arithmeticcolumn.h"

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_1 );

CPPUNIT_TEST_SUITE_END();

private:
public:

	static void walkfnString(const ParseTree* n)
	{
		char *r;
		static bool is_init = false;
		const char* mname = typeid(*(n->data())).name();
		if (!is_init)
		{
			::init_demangler(0, 0, 0);
			is_init = true;
		}
		r = ::cplus_demangle_with_style(mname, 7, 27);
		if (r != 0)
		{
			//cout << "mangle: " << mname << " demangle: " << r << endl;
			::free(r);
		}
		if (typeid(*(n->data())) == typeid(SimpleFilter))
		{
			cout << "SimpleFilter: " << endl;
			const SimpleFilter* sf = dynamic_cast<SimpleFilter*>(n->data());
			const ReturnedColumn* lhs = sf->lhs();
			const ReturnedColumn* rhs = sf->rhs();
			const Operator* op = sf->op();
			cout << '\t' << lhs->data() << ' ' << op->data() << ' ' << rhs->data();
			cout << endl << "\t\t";
			if (typeid(*lhs) == typeid(SimpleColumn))
			{
				cout << "SimpleColumn: " << lhs->data() << " / ";
			}
			else if (typeid(*lhs) == typeid(ConstantColumn))
			{
				cout << "ConstantColumn: " << lhs->data() << " / ";
			}
			else
			{
				cout << "UNK: " << lhs->data() << " / ";
			}
			cout << "Operator: " << op->data() << " / ";
			if (typeid(*rhs) == typeid(SimpleColumn))
			{
				cout << "SimpleColumn: " << rhs->data();
			}
			else if (typeid(*rhs) == typeid(ConstantColumn))
			{
				cout << "ConstantColumn: " << rhs->data();
			}
			else
			{
				cout << "UNK: " << rhs->data();
			}
		}
		else if (typeid(*(n->data())) == typeid(Operator))
		{
			cout << "Operator: ";
			const Operator* op = dynamic_cast<Operator*>(n->data());
			cout << '\t' << op->data();
		}
		else
		{
			cout << mname << " -x-: ";
		}
		cout << endl;
	}

    void setUp() {
    }
    
    void tearDown() {
    }
    
    void selectExecutionPlan_1() {
        cout << "SQL: select r_regionkey from region, nation where n_regionkey = r_regionkey and n_regionkey = 2;" << endl;
        CalpontSelectExecutionPlan csep;
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        ParseTree* filterList;
        
        // returned columns
        SimpleColumn r_regionkey("tpch.region.r_regionkey");
        SimpleColumn n_regionkey("tpch.nation.n_regionkey");
        SimpleColumn p_partkey("tpch.part.p_partkey");
        SimpleColumn n_name("tpch.nation.n_name");
        SimpleColumn c_custkey("tpch.customer.c_custkey");

        colList.push_back(new SimpleColumn(r_regionkey));
               
        // filters
        CalpontSelectExecutionPlan::Parser parser;
        std::vector<Token> tokens;
        
        //tokens.push_back(Token(new Operator("(")));
        //tokens.push_back(Token(new Operator(")")));
        
        tokens.push_back(Token(new SimpleFilter(new Operator("="),
				new SimpleColumn(r_regionkey),
				new SimpleColumn(n_regionkey))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new SimpleFilter(new Operator("="),
				new SimpleColumn(r_regionkey),
				new SimpleColumn(c_custkey))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new Operator("(")));

        tokens.push_back(Token(new SimpleFilter(new Operator("="),
				new SimpleColumn(n_regionkey),
				new ConstantColumn("779"))));
        
        tokens.push_back(Token(new Operator("or")));
        
        tokens.push_back(Token(new SimpleFilter(new Operator("!="),
				new SimpleColumn(n_name),
				new ConstantColumn("'ASIA'"))));
        
        tokens.push_back(Token(new Operator(")")));

        tokens.push_back(Token(new Operator ("and")));
        
        tokens.push_back(Token(new Operator("(")));

        tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(n_regionkey),
				new ConstantColumn("77"))));
        
        tokens.push_back(Token(new Operator("or")));
        
        tokens.push_back(Token(new SimpleFilter(new Operator(">"),
				new SimpleColumn(p_partkey),
				new ConstantColumn("7007"))));
        
        tokens.push_back(Token(new Operator(")")));

        filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
        filterList->drawTree("selectExecutionPlan_1.dot");
                     
        // calpont execution plan        
        csep.returnedCols (colList);
        csep.filters (filterList);
        cout << "\nCalpont Execution Plan:" << endl;
        cout << csep << endl;
        cout << " --- end of test 1 ---" << endl;

			filterList->walk(walkfnString);

    }

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( ExecPlanTest );

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


