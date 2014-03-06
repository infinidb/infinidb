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

CPPUNIT_TEST( selectExecutionPlan_12 );

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
    
    void selectExecutionPlan_12() {

//        cout << "SQL: select r_regionkey from region, nation where n_regionkey = r_regionkey and n_regionkey = 2;" << endl;
cout << 
"SQL: select \
 l_shipmode, \
 sum(case \
   when o_orderpriority = '1-URGENT' \
     or o_orderpriority = '2-HIGH' \
     then 1 \
     else 0 \
 end ) as high_line_count, \
 sum(case \
   when o_orderpriority <> '1-URGENT' \
    and o_orderpriority <> '2-HIGH' \
     then 1 \
     else 0 \
 end ) as low_line_count \
from \
  orders, \
  lineitem \
where \
  o_orderkey = l_orderkey \
  and l_shipmode in (':1', ':2') \
  and l_commitdate < l_reciptdate \
  and l_shipdate < l_commitdate \
  and l_receiptdate >= date ':3' \
  and l_receiptdate < date ':3' + interval '1' year' \
group by \
  l_shipmode \
order by \
  l_shipmode;" << endl; 

 
        CalpontSelectExecutionPlan csep;
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        CalpontSelectExecutionPlan::GroupByColumnList groupbyList;
        CalpontSelectExecutionPlan::OrderByColumnList orderbyList;
        ParseTree* filterList;
        
        // returned columns
        SimpleColumn l_shipmode("tpch.lineitem.l_shipmode");
        AggregateColumn *high_line_count = new AggregateColumn(); 
        high_line_count->functionName("sum");
        ArithmeticColumn *a1 = new ArithmeticColumn ("( CASE WHEN o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' THEN 1 ELSE 0  END )"); 
        high_line_count->functionParms(a1);
  
        AggregateColumn *low_line_count = new AggregateColumn(); 
        low_line_count->functionName("sum");
        ArithmeticColumn *a2 = new ArithmeticColumn ("( CASE WHEN o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' THEN 1 ELSE 0  END )"); 
        low_line_count->functionParms(a2);
  

        // Create the Projection
        colList.push_back(high_line_count);
        colList.push_back(low_line_count);

        // Filter columns
        SimpleColumn l_shipmode0("tpch.lineitem.l_shipmode");
        SimpleColumn l_shipmode1("tpch.lineitem.l_shipmode");
        SimpleColumn l_commitdate("tpch.lineitem.l_commitdate");
        SimpleColumn l_receiptdate("tpch.lineitem.l_receiptdate");
        SimpleColumn l_receiptdate1("tpch.lineitem.l_receiptdate");
        SimpleColumn l_shipdate("tpch.lineitem.l_shipdate");

               
        // filters
        CalpontSelectExecutionPlan::Parser parser;
        std::vector<Token> tokens;
        
        //tokens.push_back(Token(new Operator("(")));
        //tokens.push_back(Token(new Operator(")")));
        
        tokens.push_back(Token(new SimpleFilter(new Operator("="),
				new SimpleColumn(l_shipmode),
				new ConstantColumn("1"))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new SimpleFilter(new Operator("="),
				new SimpleColumn(l_shipmode),
				new ConstantColumn("2"))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(l_commitdate),
				new SimpleColumn(l_receiptdate))));
        
        tokens.push_back(Token(new Operator("and")));

        tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(l_shipdate),
				new SimpleColumn(l_commitdate))));
        
        tokens.push_back(Token(new Operator("and")));

	tokens.push_back(Token(new SimpleFilter(new Operator(">="),
				new SimpleColumn(l_receiptdate),
				new ConstantColumn("06/01/2005"))));
        
        tokens.push_back(Token(new Operator("and")));

	tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(l_receiptdate),
				new ConstantColumn("06/01/2006"))));
        
       // old below
        filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
        filterList->drawTree("selectExecutionPlan_12.dot");
                     
	// Build Group By List
        SimpleColumn *l_shipmode2 = new SimpleColumn("tpch.lineitem.l_shipmode");
        groupbyList.push_back(l_shipmode2); 
    
	// Build Order By List
        // SimpleColumn *l_shipmode2 = new SimpleColumn("tpch.lineitem.l_shipmode");
        orderbyList.push_back(l_shipmode2); 
    
        // calpont execution plan        
        csep.returnedCols (colList);
        csep.filters (filterList);
        cout << "\nCalpont Execution Plan:" << endl;
        cout << csep << endl;
        cout << " --- end of test 12 ---" << endl;

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


