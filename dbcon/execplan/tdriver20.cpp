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
//#include "conditionfilter.h"
//#include "existsfilter.h"

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_20 );

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
    
    void selectExecutionPlan_20() {

cout << 
"SQL: \
select \
	s_name, \
	s_address \
from \
	supplier, \
	nation \
where \
	s_suppkey in ( \
		select \
			ps_suppkey \
		from \
			partsupp \
		where \
			ps_partkey in ( \
				select \
					p_partkey \
				from \
					part \
				where \
					p_name like ':1%' \
			) \
			and ps_availqty > ( \
				select \
					0.5 * sum(l_quantity) \
				from \
					lineitem \
				where \
					l_partkey = ps_partkey \
					and l_suppkey = ps_suppkey \
					and l_shipdate >= date ':2' \
					and l_shipdate < date ':2' + interval '1' year \
			) \
	) \
	and s_nationkey = n_nationkey \
	and n_name = ':3' \
order by \
	s_name;"
   << endl; 

/* ------------The create view statement is being re-written as an inline view  ----- */ 
/* ------------ (dynamic table) to be used in a from cluase.                    ----- */ 

        //This is the main query body for query 20 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("tpch.supplier.s_name");
        colList.push_back(c1);
        SimpleColumn *c2 = new SimpleColumn("tpch.supplier.s_address");
        colList.push_back(c2);
        csep->returnedCols(colList);  // set Projection columns
          
        cout << "I am here 1"  << endl; 
        // Filter columns (csep)
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 

        //   --------------- first sub select begins here ----------------  x
        //Sub select filter 
        CalpontSelectExecutionPlan *sub01csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub01ColList; 

        SimpleColumn *s01c1 = new SimpleColumn ("tpch.partsupp.ps_suppkey"); 
        sub01ColList.push_back(s01c1); 
  	// Append returned columns to subselect 
        sub01csep->returnedCols(sub01ColList); 

        cout << "I am here 2"  << endl; 
        //subselect Filters (sub011csep)
        CalpontSelectExecutionPlan::FilterTokenList sub01csep_filterlist; 

        //   --------------- inner sub select begins here ----------------  x 
        //Sub select filter 
        CalpontSelectExecutionPlan *sub011csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub011ColList; 

        SimpleColumn *s011c1 = new SimpleColumn ("tpch.part.p_partkey"); 
        sub011ColList.push_back(s011c1); 
  	// Append returned columns to subselect 
        sub011csep->returnedCols(sub011ColList); 
    
        cout << "I am here 3"  << endl; 
        //subselect Filters (sub011csep)
        CalpontSelectExecutionPlan::FilterTokenList sub011csep_filterlist; 
	SimpleFilter *s011f1 = new SimpleFilter ( new Operator("LIKE"), 
                               new SimpleColumn ("tpch.part.p_name"),
		               new ConstantColumn("%:1"));
	sub011csep_filterlist.push_back(s011f1); 
        sub011csep->filterTokenList(sub011csep_filterlist); 

        //   --------------- inner sub ends here ----------------  x

        //   --------------- second inner sub select begins here ----------------  x 
        //Sub select filter 
        CalpontSelectExecutionPlan *sub012csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub012ColList; 

        ArithmeticColumn *s012c1 = new ArithmeticColumn ("0.5 * sum(l_quantity)"); 
        sub012ColList.push_back(s012c1); 
  	// Append returned columns to subselect 
        sub012csep->returnedCols(sub012ColList); 
    
        cout << "I am here 31"  << endl; 
        //subselect Filters (sub012csep)
        CalpontSelectExecutionPlan::FilterTokenList sub012csep_filterlist; 
	SimpleFilter *s012f1 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_partkey"),
                            new SimpleColumn ("tpch.partsupp.ps_partkey"));
	sub012csep_filterlist.push_back(s012f1); 

        sub012csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s012f2 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_suppkey"),
                            new SimpleColumn ("tpch.partsupp.ps_suppkey"));
	sub012csep_filterlist.push_back(s012f2); 

        sub012csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s012f3 = new SimpleFilter ( new Operator(">="), 
                            new SimpleColumn ("tpch.lineitem.l_shipdate"),
                            new ConstantColumn ("date ':2'"));
	sub012csep_filterlist.push_back(s012f3); 

        sub012csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s012f4 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipdate"),
                            new ConstantColumn ("date ':2' + interval '1' year"));
	sub012csep_filterlist.push_back(s012f4); 

        sub012csep->filterTokenList(sub012csep_filterlist); 


        //   --------------- second inner sub ends here ----------------  x
 
	// Now resolve the first inner sub_query 
	SelectFilter *s01f1 = new SelectFilter (new SimpleColumn ("tpch.partsupp.ps_suppkey"),
	                      new Operator("IN"), 
		              sub011csep);
  
	// Now resolve the second  inner sub_query 
	SelectFilter *s01f2 = new SelectFilter (new SimpleColumn ("tpch.partsupp.ps_availqty"),
	                      new Operator(">"), 
		              sub012csep);
  
	sub01csep_filterlist.push_back(s01f1); 
        sub01csep_filterlist.push_back(new Operator("and"));
	sub01csep_filterlist.push_back(s01f2); 
        sub01csep->filterTokenList(sub01csep_filterlist); 

        cout << "I am here 4"  << endl; 
        //   --------------- first sub ends here ----------------  x  
	// Now resolve the outer query 
	SelectFilter *f1 = new SelectFilter (new SimpleColumn ("tpch.supplier.s_suppkey"),
	                      new Operator("IN"), 
		              sub01csep);
	csep_filterlist.push_back(f1); 
        cout << "I am here 5"  << endl; 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.supplier.s_nationkey"),
                            new SimpleColumn ("tpch.nation.n_nationkey"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f3 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.nation.n_name"),
		            new ConstantColumn(":3"));
	csep_filterlist.push_back(f3); 

        cout << "I am here 6"  << endl; 
	csep->filterTokenList(csep_filterlist); 

	// Build Group By List
/*
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        SimpleColumn *g1 = new SimpleColumn("tpch.part.p_brand");
        csep_groupbyList.push_back(g1);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 
*/

	// Order By List
        cout << "I am here 7"  << endl; 
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o1 = new SimpleColumn("tpch.supplier.s_name");
        csep_orderbyList.push_back(o1);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        cout << "I am here 8"  << endl; 
        
	//filterList->walk(walkfnString); ?? 

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        cout << "I am here 9"  << endl; 
        pt->drawTree("selectExecutionPlan_20.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 20 ---" << endl;
   
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


