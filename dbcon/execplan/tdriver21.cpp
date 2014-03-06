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
#include "existsfilter.h"
//#include "conditionfilter.h"

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_21 );

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
    
    void selectExecutionPlan_21() {

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

//NOTE(7/19):  Add "NOT" to second Exists stanza sub02csep, filter f6. 

        //This is the main query body for query 21 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("tpch.supplier.s_name");
        colList.push_back(c1);
        ArithmeticColumn *c2 = new ArithmeticColumn("count(all)");
        c2->alias("numwait");
        colList.push_back(c2);
        csep->returnedCols(colList);  // set Projection columns
          
        // Filter columns (csep)
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 

	SimpleFilter *f1 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.supplier.s_suppkey"),
                            new SimpleColumn ("tpch.lineitem.l_suppkey"));
	csep_filterlist.push_back(f1); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.orders.o_orderkey"),
                            new SimpleColumn ("tpch.lineitem.l_orderkey"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f3 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.orders.o_orderstatus"),
                            new ConstantColumn ("F"));
	csep_filterlist.push_back(f3); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f4 =  new SimpleFilter ( new Operator(">"),  
                            new SimpleColumn ("tpch.lineitem.l_receiptdate"),
                            new SimpleColumn ("tpch.lineitem.l_commitdate"));
	csep_filterlist.push_back(f4); 

        csep_filterlist.push_back(new Operator("and"));

        //   --------------- first sub select begins here ----------------  x
        //Sub select filter 
        // Note:   A select "*" is translated to select 
        // each fully qualified column in an Oracle execution plan.  In these 
        // examples the shorthand notation for "*" will be "ALL" and the query 
        // will be written as it would be represented in an exeution pland. 

        CalpontSelectExecutionPlan *sub01csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub01ColList; 

        SimpleColumn *s01c1 = new SimpleColumn ("tpch.lineitem.l_orderkey"); 
        sub01ColList.push_back(s01c1); 
        SimpleColumn *s01c2 = new SimpleColumn ("tpch.lineitem.l_partkey"); 
        sub01ColList.push_back(s01c2); 
        SimpleColumn *s01c3 = new SimpleColumn ("tpch.lineitem.l_suppkey"); 
        sub01ColList.push_back(s01c3); 
        SimpleColumn *s01c4 = new SimpleColumn ("tpch.lineitem.l_linenumber"); 
        sub01ColList.push_back(s01c4); 
        SimpleColumn *s01c5 = new SimpleColumn ("tpch.lineitem.l_quantity"); 
        sub01ColList.push_back(s01c5); 
        SimpleColumn *s01c6 = new SimpleColumn ("tpch.lineitem.l_extenedprice"); 
        sub01ColList.push_back(s01c6); 
        SimpleColumn *s01c7 = new SimpleColumn ("tpch.lineitem.l_discount"); 
        sub01ColList.push_back(s01c7); 
        SimpleColumn *s01c8 = new SimpleColumn ("tpch.lineitem.l_tax"); 
        sub01ColList.push_back(s01c8); 
        SimpleColumn *s01c9 = new SimpleColumn ("tpch.lineitem.l_returnflag"); 
        sub01ColList.push_back(s01c9); 
        SimpleColumn *s01c10 = new SimpleColumn ("tpch.lineitem.l_linestatus"); 
        sub01ColList.push_back(s01c10); 
        SimpleColumn *s01c11 = new SimpleColumn ("tpch.lineitem.l_shipdate"); 
        sub01ColList.push_back(s01c11); 
        SimpleColumn *s01c12 = new SimpleColumn ("tpch.lineitem.l_commitdate"); 
        sub01ColList.push_back(s01c12); 
        SimpleColumn *s01c13 = new SimpleColumn ("tpch.lineitem.l_receiptdate"); 
        sub01ColList.push_back(s01c13); 
        SimpleColumn *s01c14 = new SimpleColumn ("tpch.lineitem.l_shipinstruct"); 
        sub01ColList.push_back(s01c14); 
        SimpleColumn *s01c15 = new SimpleColumn ("tpch.lineitem.l_shipmode"); 
        sub01ColList.push_back(s01c15); 
        SimpleColumn *s01c16 = new SimpleColumn ("tpch.lineitem.l_comment"); 
        sub01ColList.push_back(s01c16); 
  	// Append returned columns to subselect 
        sub01csep->returnedCols(sub01ColList); 

        cout << "I am here 2"  << endl; 
        //subselect Filters (sub01csep)
        CalpontSelectExecutionPlan::FilterTokenList sub01csep_filterlist; 

	SimpleFilter *s01f1 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_orderkey"),
                            new SimpleColumn ("tpch.lineitem.l_orderkey"));
	sub01csep_filterlist.push_back(s01f1); 

        sub01csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s01f2 = new SimpleFilter ( new Operator("<>"), 
                            new SimpleColumn ("tpch.lineitem.l_suppkey"),
                            new SimpleColumn ("tpch.lineitem.l_suppkey"));
	sub01csep_filterlist.push_back(s01f2); 

        sub01csep->filterTokenList(sub01csep_filterlist); 

        cout << "I am here 4"  << endl; 
        //   --------------- first sub ends here ----------------  x  
	// Now resolve the outer query 
	ExistsFilter *f5 = new ExistsFilter (sub01csep);
	csep_filterlist.push_back(f5); 
        cout << "I am here 5"  << endl; 

        csep_filterlist.push_back(new Operator("and"));

        //   --------------- second sub select begins here ----------------  x
        //Sub select filter 
        // Note:   A select "*" is translated to select 
        // each fully qualified column in an Oracle execution plan.  In these 
        // examples the shorthand notation for "*" will be "ALL" and the query 
        // will be written as it would be represented in an exeution pland. 

        CalpontSelectExecutionPlan *sub02csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub02ColList; 

        SimpleColumn *s02c1 = new SimpleColumn ("tpch.lineitem.l_orderkey"); 
        sub01ColList.push_back(s02c1); 
        SimpleColumn *s02c2 = new SimpleColumn ("tpch.lineitem.l_partkey"); 
        sub01ColList.push_back(s02c2); 
        SimpleColumn *s02c3 = new SimpleColumn ("tpch.lineitem.l_suppkey"); 
        sub01ColList.push_back(s02c3); 
        SimpleColumn *s02c4 = new SimpleColumn ("tpch.lineitem.l_linenumber"); 
        sub01ColList.push_back(s02c4); 
        SimpleColumn *s02c5 = new SimpleColumn ("tpch.lineitem.l_quantity"); 
        sub01ColList.push_back(s02c5); 
        SimpleColumn *s02c6 = new SimpleColumn ("tpch.lineitem.l_extenedprice"); 
        sub01ColList.push_back(s02c6); 
        SimpleColumn *s02c7 = new SimpleColumn ("tpch.lineitem.l_discount"); 
        sub01ColList.push_back(s02c7); 
        SimpleColumn *s02c8 = new SimpleColumn ("tpch.lineitem.l_tax"); 
        sub01ColList.push_back(s02c8); 
        SimpleColumn *s02c9 = new SimpleColumn ("tpch.lineitem.l_returnflag"); 
        sub01ColList.push_back(s02c9); 
        SimpleColumn *s02c10 = new SimpleColumn ("tpch.lineitem.l_linestatus"); 
        sub01ColList.push_back(s02c10); 
        SimpleColumn *s02c11 = new SimpleColumn ("tpch.lineitem.l_shipdate"); 
        sub01ColList.push_back(s02c11); 
        SimpleColumn *s02c12 = new SimpleColumn ("tpch.lineitem.l_commitdate"); 
        sub01ColList.push_back(s02c12); 
        SimpleColumn *s02c13 = new SimpleColumn ("tpch.lineitem.l_receiptdate"); 
        sub01ColList.push_back(s02c13); 
        SimpleColumn *s02c14 = new SimpleColumn ("tpch.lineitem.l_shipinstruct"); 
        sub01ColList.push_back(s02c14); 
        SimpleColumn *s02c15 = new SimpleColumn ("tpch.lineitem.l_shipmode"); 
        sub01ColList.push_back(s02c15); 
        SimpleColumn *s02c16 = new SimpleColumn ("tpch.lineitem.l_comment"); 
        sub01ColList.push_back(s02c16); 
  	// Append returned columns to subselect 
        sub02csep->returnedCols(sub02ColList); 

        cout << "I am here 3"  << endl; 
        //subselect Filters (sub01csep)
        CalpontSelectExecutionPlan::FilterTokenList sub02csep_filterlist; 

	SimpleFilter *s02f1 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_orderkey"),
                            new SimpleColumn ("tpch.lineitem.l_orderkey"));
	sub02csep_filterlist.push_back(s02f1); 

        sub02csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s02f2 = new SimpleFilter ( new Operator("<>"), 
                            new SimpleColumn ("tpch.lineitem.l_suppkey"),
                            new SimpleColumn ("tpch.lineitem.l_suppkey"));
	sub02csep_filterlist.push_back(s02f2); 

        sub02csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *s02f3 = new SimpleFilter ( new Operator(">"), 
                            new SimpleColumn ("tpch.lineitem.l_receiptdate"),
                            new SimpleColumn ("tpch.lineitem.l_commitdate"));
	sub02csep_filterlist.push_back(s02f3); 
        sub02csep->filterTokenList(sub02csep_filterlist); 

        cout << "I am here 4"  << endl; 
        //   --------------- first sub ends here ----------------  x  
	// Now resolve the outer query 
	ExistsFilter *f6 = new ExistsFilter (sub02csep, true);
	csep_filterlist.push_back(f6); 
        cout << "I am here 5"  << endl; 

        csep_filterlist.push_back(new Operator("and"));
    
	SimpleFilter *f7 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.supplier.s_nationkey"),
                            new SimpleColumn ("tpch.nation.n_nationkey"));
	csep_filterlist.push_back(f7); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f8 =  new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.nation.n_name"),
		            new ConstantColumn(":1"));
	csep_filterlist.push_back(f8); 

        cout << "I am here 6"  << endl; 
	csep->filterTokenList(csep_filterlist); 

	// Build Group By List
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        SimpleColumn *g1 = new SimpleColumn("tpch.supplier.s_name");
        csep_groupbyList.push_back(g1);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        cout << "I am here 7"  << endl; 
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o1 = new SimpleColumn("tpch.temp.numwait");
        o1->asc(false); 
        csep_orderbyList.push_back(o1);
        SimpleColumn *o2 = new SimpleColumn("tpch.supplier.s_name");
        csep_orderbyList.push_back(o2);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        cout << "I am here 8"  << endl; 
        
	//filterList->walk(walkfnString); ?? 

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        cout << "I am here 9"  << endl; 
        pt->drawTree("selectExecutionPlan_21.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 21 ---" << endl;
   
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


