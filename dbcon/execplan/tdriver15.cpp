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
#include "selectfilter.h"

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_15 );

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
    
    void selectExecutionPlan_15() {

cout << 
"SQL: \
create view revenue:s (supplier_no, total_revenue) as \
	select \
		l_suppkey, \
		sum(l_extendedprice * (1 - l_discount)) \
	from \
		lineitem \
	where \
		l_shipdate >= date ':1' \
		and l_shipdate < date ':1' + interval '3' month \
	group by \
		l_suppkey; \
 \
select \
	s_suppkey, \
	s_name, \
	s_address, \
	s_phone, \
	total_revenue \
from \
	supplier, \
	revenue:s \
where \
	s_suppkey = supplier_no \
	and total_revenue = ( \
		select \
			max(total_revenue) \
		from \
			revenue:s \
	) \
order by \
	s_suppkey; \
 \
drop view revenue:s; \
  " << endl; 

/* ------------The create view statement is being re-written as an inline view  ----- */ 
/* ------------ (dynamic table) to be used in a from cluase.                    ----- */ 

        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        CalpontSelectExecutionPlan::GroupByColumnList groupbyList;
        CalpontSelectExecutionPlan::OrderByColumnList orderbyList;
        ParseTree* filterList;
        
        // returned columns
        //SimpleColumn l_suppkey("tpch.lineitem.l_suppkey");
        SimpleColumn *l_suppkey = new SimpleColumn("tpch.lineitem.l_suppkey");
        AggregateColumn *total_revenue = new AggregateColumn(); 
        total_revenue->functionName("sum");
        total_revenue->alias("total_revenue");
        ArithmeticColumn *a1 = new ArithmeticColumn ("l_extenedprice * (1 - l_discount)"); 
        total_revenue->functionParms(a1);

        // Create the Projection
        colList.push_back(l_suppkey);
        colList.push_back(total_revenue);

        // Filter columns
        SimpleColumn l_shipdate("tpch.lineitem.l_shipdate");
        //SimpleColumn l_shipmode0("tpch.lineitem.l_shipmode");
        //SimpleColumn l_shipmode1("tpch.lineitem.l_shipmode");
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
				new ConstantColumn("06/01/2005"))));
        
        tokens.push_back(Token(new Operator("and")));

	tokens.push_back(Token(new SimpleFilter(new Operator("<"),
				new SimpleColumn(l_shipdate),
				new ConstantColumn("07/01/2005"))));
        
       // old below
        filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
        filterList->drawTree("selectExecutionPlan_15sub.dot");
                     
	// Build Group By List
        SimpleColumn *l_suppkey2 = new SimpleColumn("tpch.lineitem.l_suppkey");
        groupbyList.push_back(l_suppkey2); 
    
        // calpont execution plan        
        csep->returnedCols (colList);
        csep->filters (filterList);

        csep->location (CalpontSelectExecutionPlan::FROM);  // Use scope resolution to use enum.

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 15 ---" << endl;
 
/*  ------ This is the begining of the outer query  ---------------------- */ 
/*  ------ This is the begining of the outer query  ---------------------- */ 


        CalpontSelectExecutionPlan *csep_parent = new CalpontSelectExecutionPlan();
        cout << "********* I'm ok to here 01 ***************" << endl; 
        CalpontSelectExecutionPlan::ReturnedColumnList colList2;
        CalpontSelectExecutionPlan::GroupByColumnList groupbyList2;
        cout << "********* I'm ok to here 02 ***************" << endl; 
        CalpontSelectExecutionPlan::OrderByColumnList orderbyList2;
        CalpontSelectExecutionPlan::SelectList  subSelectList2;
        //`ParseTree* filterList2;
        
        cout << "********* I'm ok to here 1 ***************" << endl; 
        // returned columns
        SimpleColumn *s_suppkey = new SimpleColumn ("tpch.supplier.s_suppkey");
        SimpleColumn *s_name = new SimpleColumn("tpch.supplier.s_name"); 
        SimpleColumn *s_address = new SimpleColumn("tpch.supplier.s_address");
        SimpleColumn *s_phone = new SimpleColumn("tpch.supplier.s_phone"); 
        cout << "********* I'm ok to here 1a ***************" << endl; 
        AggregateColumn *s_total_revenue = new AggregateColumn(); 
        cout << "********* I'm ok to here 1b ***************" << endl; 
        s_total_revenue->functionName("sum");
        ArithmeticColumn *a5 = new ArithmeticColumn ("o_orderpriority = '2-HIGH' THEN 1 ELSE 0  END )"); 
        cout << "********* I'm ok to here 1c ***************" << endl; 
        s_total_revenue->functionParms(a5);
  
        // Create the Projection
        cout << "********* I'm ok to here 1d ***************" << endl; 
        colList2.push_back(s_suppkey);
        colList2.push_back(s_name);
        colList2.push_back(s_address);
        colList2.push_back(s_phone);
        colList2.push_back(s_total_revenue);
        cout << "********* I'm ok to here 1e ***************" << endl; 

/*  ------ This is the begining of the query filter ---------------------- */ 
/*  ------ This is the begining of the query filter ---------------------- */ 
      
        //std::vector<Token> tokens_n;
        CalpontSelectExecutionPlan::FilterTokenList regFilterTokenList; 
	SimpleFilter *f1 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.supplier.s_suppkey"),
		           new SimpleColumn ("tpch.supplier.s_supplier_no"));

	regFilterTokenList.push_back(f1); 
        regFilterTokenList.push_back(new Operator("and"));

        
        cout << "********* I'm ok to here 1 ***************" << endl; 
        //Sub select filter 
        CalpontSelectExecutionPlan *subcsep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  subColList; 

        ArithmeticColumn *sc1 = new ArithmeticColumn ("max(total_revenue)"); 
        //CalpontSelectExecutionPlan::SelectList  subSelectList;

        subColList.push_back(sc1); 
        subcsep->returnedCols(subColList); 
        cout << "********* I'm ok to here 2 ***************" << endl; 
            
        //subselect Filters 
        //CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList; 
	SelectFilter *f2 = new SelectFilter (new SimpleColumn ("tpch.revenue.total_revenue"),
	                      new Operator("="), 
		              subcsep);
  
        regFilterTokenList.push_back(f2); 


        cout << "********* I'm ok to here 3 ***************" << endl; 
        // Filter columns
        //SimpleColumn l_shipmode0("tpch.lineitem.l_shipmode");
        // ***  Already creted via regFilterTokenList object **** 
               
        // filters
        //CalpontSelectExecutionPlan::Parser parser;
        //std::vector<Token> tokens;
        // ***  Already creted via regFilterTokenList object **** 
        
        //tokens.push_back(Token(new Operator("(")));
        //tokens.push_back(Token(new Operator(")")));
        
        //tokens.push_back(Token(new SimpleFilter(new Operator("="),
	//			new SimpleColumn(l_shipmode),
	//			new ConstantColumn("1"))));
        
        //tokens.push_back(Token(new Operator("and")));

        //tokens.push_back(Token(new SimpleFilter(new Operator("="),
	//			new SimpleColumn(l_shipmode),
	//			new ConstantColumn("2"))));
        
        
       // old below
       // filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
        //filterList->drawTree("selectExecutionPlan_15n.dot");
                     
	// Build Group By List
        //SimpleColumn *l_shipmode2 = new SimpleColumn("tpch.lineitem.l_shipmode");
        //groupbyList.push_back(l_shipmode2); 
    
	// Build Order By List
        SimpleColumn *s_suppkey3 = new SimpleColumn("tpch.supplier.s_suppkey");
        orderbyList.push_back(s_suppkey3); 
    
        // calpont execution plan        
        csep_parent->returnedCols (colList2);
        csep_parent->filterTokenList(regFilterTokenList);
        csep_parent->orderByCols(orderbyList);

        cout << "********* I'm ok to here 4 ***************" << endl; 
        // draw filterList tree
        //csep_parent->filterTokenList->drawTree("selectExecutionPlan_15.dot");
        ParseTree *pt = const_cast<ParseTree*>(csep_parent->filters());
        pt->drawTree("selectExecutionPlan_15.dot");
                     
        cout << "\nCalpont Execution Plan:" << endl;
        cout << csep << endl;
        cout << " --- end of test 15 ---" << endl;

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


