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

CPPUNIT_TEST( selectExecutionPlan_17 );

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
    
    void selectExecutionPlan_17() {

cout << 
"SQL: \
select \
	sum(l_extendedprice) / 7.0 as avg_yearly \
from \
	lineitem, \
	part \
where \
	p_partkey = l_partkey \
	and p_brand = ':1' \
	and p_container = ':2' \
	and l_quantity < ( \
		select \
			0.2 * avg(l_quantity) \
		from \
			lineitem \
		where \
			l_partkey = p_partkey \
	);" 
   << endl; 

/* ------------ Start Query Execution --------- */ 

        //This is the main query body for query 17 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
	AggregateColumn *c1 = new AggregateColumn(); 
        c1->functionName("sum");
        c1->alias("avg_yearly");
        ArithmeticColumn *a1 = new ArithmeticColumn("(l_extendedprice ) / 7.0 "); 
        c1->functionParms(a1);
        colList.push_back(c1);
        csep->returnedCols(colList);  // set Projection columns

        // Filter columns
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 
 
	SimpleFilter *f1 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_partkey"),
                           new SimpleColumn ("tpch.lineitem.l_partkey"));
	csep_filterlist.push_back(f1); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_brand"),
		           new ConstantColumn(":1"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));
        
	SimpleFilter *f3 = new SimpleFilter ( new Operator("not like"), 
                           new SimpleColumn ("tpch.part.p_container"),
		           new ConstantColumn(":2"));
	csep_filterlist.push_back(f3); 

        csep_filterlist.push_back(new Operator("and"));

        /*   --------------- sub select begins here ----------------  */ 
        //Sub select filter 
        CalpontSelectExecutionPlan *subcsep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  subColList; 

        ArithmeticColumn *sc1 = new ArithmeticColumn ("0.2 * avg(l_quantity)"); 
        subColList.push_back(sc1); 
  	// Append returned columns to subselect 
        subcsep->returnedCols(subColList); 

        //subselect Filters 
        CalpontSelectExecutionPlan::FilterTokenList subcsep_filterlist; 
	SimpleFilter *sf1 = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_suppkey"),
                            new SimpleColumn ("tpch.part.p_partkey"));
	subcsep_filterlist.push_back(sf1); 

        /*   --------------- sub select ends here ----------------  */ 
 
        //CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList; 
	SelectFilter *f4 = new SelectFilter (new SimpleColumn ("tpch.lineitem.l_quantity"),
	                      new Operator("<"), 
		              subcsep);
  
	csep_filterlist.push_back(f4); 

        csep->filterTokenList(csep_filterlist); //Set Filter Columns

      /* *** not used here **
	// Build Group By List
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        SimpleColumn *g1 = new SimpleColumn("tpch.part.p_brand");
        csep_groupbyList.push_back(g1);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o0 = new SimpleColumn("supplier_cnt");  // AggregateColumn has no asc/desc order
        o0->alias("supplier_cnt");
        o0->asc(false);
        csep_groupbyList.push_back(o0);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
      */

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        pt->drawTree("selectExecutionPlan_17.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 17 ---" << endl;
   
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


