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

extern "C" char* cplus_demangle_with_style(const char*, int, int);
extern "C" void init_demangler(int, int, int);

using namespace execplan;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_18 );

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
    
    void selectExecutionPlan_18() {

cout << 
"SQL: \
select \
	c_name, \
	c_custkey, \
	o_orderkey, \
	o_orderdate, \
	o_totalprice, \
	sum(l_quantity) \
from \
	customer, \
	orders, \
	lineitem \
where \
	o_orderkey in ( \
		select \
			l_orderkey \
		from \
			lineitem \
		group by \
			l_orderkey having \
				sum(l_quantity) > :1 \
	) \
	and c_custkey = o_custkey \
	and o_orderkey = l_orderkey \
group by \
	c_name, \
	c_custkey, \
	o_orderkey, \
	o_orderdate, \
	o_totalprice \
order by \
	o_totalprice desc, \
	o_orderdate;" 
   << endl; 

// ****
// **** THIS QUERY IS MISSING THE HAVING CLAUSE ON THE GROUP BY **** 
// ****

        //This is the main query body for query 18 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("tpch.customer.c_name");
        colList.push_back(c1);
        SimpleColumn *c2 = new SimpleColumn("tpch.customer.c_custkey");
        colList.push_back(c2);
        SimpleColumn *c3 = new SimpleColumn("tpch.orders.o_orderkey");
        colList.push_back(c3);
        SimpleColumn *c4 = new SimpleColumn("tpch.orders.o_orderdate");
        colList.push_back(c4);
        SimpleColumn *c5 = new SimpleColumn("tpch.orders.o_totalprice");
        colList.push_back(c5);
        ArithmeticColumn *c6 = new ArithmeticColumn("sum(l_quantity)"); 
        colList.push_back(c6);
          
        csep->returnedCols(colList);  // set Projection columns

        // Filter columns
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 
 
        //Sub select filter 
        CalpontSelectExecutionPlan *subcsep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  subColList; 

        SimpleColumn *sc1 = new SimpleColumn ("tpch.lineitem.l_orderkey"); 
        subColList.push_back(sc1); 
  	// Append returned columns to subselect 
        subcsep->returnedCols(subColList); 

        //subselect Filters 
        CalpontSelectExecutionPlan::FilterTokenList subcsep_filterlist; 
	SimpleFilter *sf1 = new SimpleFilter ( new Operator("IN"), 
                            new SimpleColumn ("tpch.lineitem.??"),
		            new ConstantColumn("%Customer%Complaints%"));
	subcsep_filterlist.push_back(sf1); 

	// Build subGroup By List
        CalpontSelectExecutionPlan::GroupByColumnList subcsep_groupbyList;
        SimpleColumn *sg1 = new SimpleColumn ("tpch.lineitem.l_orderkey"); 
        subcsep_groupbyList.push_back(sg1);
        subcsep->groupByCols(subcsep_groupbyList);  //Set GroupBy columns 

        //subHaving Filters 
        CalpontSelectExecutionPlan::FilterTokenList subcsep_havingList; 
   
	SimpleFilter *sh1 = new SimpleFilter ( new Operator(">"), 
	                    new ArithmeticColumn("sum(l_quantity)"),
	                    new ConstantColumn(":1"));
        subcsep_havingList.push_back(sh1);
        subcsep->havingTokenList(subcsep_havingList);  //Set GroupBy columns 
   
        //subcsep->filterTokenList(subcsep_havingList);  //Set GroupBy columns 
        /*   --------------- sub select ends  here ----------------  */ 
 
        //CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList; 
	SelectFilter *f1 = new SelectFilter (new SimpleColumn ("tpch.orders.o_orderkey"),
	                      new Operator("IN"), 
		              subcsep);
  
	csep_filterlist.push_back(f1); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.customer.c_custkey"),
                           new SimpleColumn ("tpch.orders.o_custkey"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));
        
	SimpleFilter *f3 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.orders.o_orderkey"),
                           new SimpleColumn ("tpch.lineitem.l_orderkey"));
	csep_filterlist.push_back(f3); 

        csep->filterTokenList(csep_filterlist); //Set Filter Columns


	// Build Group By List
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        SimpleColumn *g1 = new SimpleColumn("tpch.customer.c_name");
        csep_groupbyList.push_back(g1);
        SimpleColumn *g2 = new SimpleColumn("tpch.customer.c_custkey");
        csep_groupbyList.push_back(g2);
        SimpleColumn *g3 = new SimpleColumn("tpch.orders.o_orderkey");
        csep_groupbyList.push_back(g3);
        SimpleColumn *g4 = new SimpleColumn("tpch.orders.o_orderdate");
        csep_groupbyList.push_back(g4);
        SimpleColumn *g5 = new SimpleColumn("tpch.orders.o_totalprice");
        csep_groupbyList.push_back(g5);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o1 = new SimpleColumn("tpch.orders.o_totalprice");
        o1->asc(false);
        csep_orderbyList.push_back(o1);
        SimpleColumn *o2 = new SimpleColumn("tpch.orders.o_orderdate");
        csep_orderbyList.push_back(o2);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        
	//filterList->walk(walkfnString); ?? 

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        pt->drawTree("selectExecutionPlan_18.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 18 ---" << endl;
   
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


