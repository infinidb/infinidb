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
#include "aggregatecolumn.h"

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan14 );

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
    
    void selectExecutionPlan14() {

cout << 
"SQL: select \
 100.00 * sum(decode(substring(p_type from 1 for 5),'%PROMO', \
     l_extendedprice * ( 1 - l_discount), 0)) / \
     sum(l_extendedprice * (1 - l_discount)) as promo_revenue \
from \
  lineitem \
  part \
where \
  ;_partkey = p_partkey \
  and l_shipdate >= '[DATE]' \
  and l_receiptdate < '[DATE]' + interval '1' month;"
  << endl; 
/* ---- This was the orginal query. The Program is using the alternative tpch Q14 code. 
cout << 
"SQL: select \
 100.00 * \
 sum(case \
   when p_type like '%PROMO%' \
     then l_extendedprice * ( 1 - l_discount) \
     else 0 \
   end ) / sum (l_extendedprice * (1 - l_discount)) as promo_revenue \
from \
  lineitem \
  part \
where \
  ;_partkey = p_partkey \
  and l_shipdate >= '1995-09-01' \
  and l_receiptdate < '1995-0901' + interval '1' month;"
  << endl; 
------- end of comment line ---------  */ 

 
        CalpontSelectExecutionPlan csep;
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        CalpontSelectExecutionPlan::GroupByColumnList groupbyList;
        CalpontSelectExecutionPlan::OrderByColumnList orderbyList;
        ParseTree* filterList;
        
        // returned columns
        //SimpleColumn l_shipmode("tpch.lineitem.l_shipmode");
        AggregateColumn *promo_revenue = new AggregateColumn(); 
        promo_revenue->functionName("sum");
        ArithmeticColumn *a3 = new ArithmeticColumn ("100.00 * sum(decode(substring(p_type from 1 for 5),'%PROMO', \
     l_extendedprice * ( 1 - l_discount), 0)) / sum(l_extendedprice * (1 - l_discount))");

        promo_revenue->functionParms(a3);
  
        // Create the Projection
        colList.push_back(promo_revenue);

        // Filter columns
        SimpleColumn l_partkey("tpch.lineitem.l_shipmode");
        SimpleColumn l_shipdate("tpch.lineitem.l_shipdate");
        //SimpleColumn l_shipdate("tpch.lineitem.l_shipmode");
        //SimpleColumn l_commitdate("tpch.lineitem.l_commitdate");
        //SimpleColumn l_receiptdate("tpch.lineitem.l_receiptdate");
        //SimpleColumn l_receiptdate1("tpch.lineitem.l_receiptdate");

               
        // filters
        CalpontSelectExecutionPlan::Parser parser;
        std::vector<Token> tokens;
        
        //tokens.push_back(Token(new Operator("(")));
        //tokens.push_back(Token(new Operator(")")));
        
        tokens.push_back(Token(new SimpleFilter(new Operator(">="),
				new SimpleColumn(l_shipdate),
				new ConstantColumn("1995-09-01"))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(l_shipdate),
				new ConstantColumn("1995-10-01"))));
        
        filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
        filterList->drawTree("selectExecutionPlan14.dot");
    
        // calpont execution plan        
        csep.returnedCols (colList);
        csep.filters (filterList);
        cout << "\nCalpont Execution Plan:" << endl;
        cout << csep << endl;
        cout << " --- end of test 14 ---" << endl;

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


