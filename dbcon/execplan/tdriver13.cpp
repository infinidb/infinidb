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

CPPUNIT_TEST( selectExecutionPlan_13 );

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
    
    void selectExecutionPlan_13() {

cout << 
"SQL: \
select \
	c_count, \
	count(*) as custdist \
from \
	( \
		select \
			c_custkey, \
			count(o_orderkey) \
		from \
			customer left outer join orders on \
				c_custkey = o_custkey \
				and o_comment not like '%:1%:2%' \
		group by \
			c_custkey \
	) as c_orders (c_custkey, c_count) \
group by \
	c_count \
order by \
	custdist desc,\
	c_count desc;"
   << endl; 

//x ------------The create view statement is being re-written as an inline view  ----- x/ 

        //This is the main query body for query 16 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("calpont.FROMTABLE.c_count");
        c1->tableAlias("c_orders");
        colList.push_back(c1);
        ArithmeticColumn *c2 = new ArithmeticColumn("count(ALL)"); 
        c2->alias("custdist");
        colList.push_back(c2);
          
        csep->returnedCols(colList);  // set Projection columns

        // Filter columns (Not needed for outer query) 
        //CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 

        /*   --------------- sub select begins here ----------------  */ 
        //Sub select filter 
        CalpontSelectExecutionPlan *subcsep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  subColList; 

        SimpleColumn *sc1 = new SimpleColumn ("tpch.customer.c_custkey"); 
        subColList.push_back(sc1); 
        ArithmeticColumn *sc2 = new ArithmeticColumn ("count(tpch.orders.o_orderkey)"); 
        sc2->alias("c_count"); 
        subColList.push_back(sc2); 
  	// Append returned columns to subselect 
        subcsep->returnedCols(subColList); 

        //subselect Filters 
        CalpontSelectExecutionPlan::FilterTokenList subcsep_filterlist; 
	SimpleFilter *sf1 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.customer.c_custkey"),
		           new SimpleColumn ("tpch.orders.o_custkey"));
	subcsep_filterlist.push_back(sf1); 

        subcsep_filterlist.push_back(new Operator("and"));

	SimpleFilter *sf2 = new SimpleFilter ( new Operator("NOT LIKE"), 
                            new SimpleColumn ("tpch.orders.o_comment"),
		            new ConstantColumn("%:1%:2%"));
	subcsep_filterlist.push_back(sf2); 

        subcsep->filterTokenList(subcsep_filterlist); //Set Filter Columns

	// sub Group By List
        CalpontSelectExecutionPlan::GroupByColumnList subcsep_groupbyList;
        SimpleColumn *sg1 = new SimpleColumn("tpch.customer.c_custkey");
        subcsep_groupbyList.push_back(sg1);
        subcsep->groupByCols(subcsep_groupbyList);  //Set GroupBy columns 
        subcsep->tableAlias("c_orders");  

        // Draw the subselect tree. 
        ParseTree *pt = const_cast<ParseTree*>(subcsep->filters());
        pt->drawTree("selectExecutionPlan_13_subquery.dot");

        // x --------------- sub select begins here ----------------  x/ 
 
        CalpontSelectExecutionPlan::SelectList subselectlist; 
        subselectlist.push_back(subcsep); 
        csep->subSelects(subselectlist); 


	// Build Group By List
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        //SimpleColumn *g1 = new SimpleColumn("calpont.FROMTABLE.c_count");
        SimpleColumn *g1 = new SimpleColumn(*c1);
        g1->alias("c_count");
        cout<< "\n I am testing \n\n" << "g1=" << *g1 << "\n end of data\n" << endl;
        csep_groupbyList.push_back(g1);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        //ArithmeticColumn *o1 = new ArithmeticColumn("count(ALL)");  
        ArithmeticColumn *o1 = new ArithmeticColumn(*c2);  
        o1->alias("custdist");
        o1->asc(false);
        csep_orderbyList.push_back(o1);
        SimpleColumn *o2 = new SimpleColumn(*c1);
        o2->alias("c_count");
        o2->asc(false);
        csep_orderbyList.push_back(o2);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        
        //SimpleColumn *abc = new SimpleColumn(*c1); 

	//filterList->walk(walkfnString); ?? 

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 13 ---" << endl;
   
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


