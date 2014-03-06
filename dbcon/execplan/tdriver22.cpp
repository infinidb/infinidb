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

CPPUNIT_TEST( selectExecutionPlan_22 );

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
    
    void selectExecutionPlan_22() {

cout << 
"SQL: \
select \
	cntrycode, \
	count(*) as numcust, \
	sum(c_acctbal) as totacctbal \
from \
	( \
		select \
			substring(c_phone from 1 for 2) as cntrycode, \
			c_acctbal \
		from \
			customer \
		where \
			substring(c_phone from 1 for 2) in \
				(':1', ':2', ':3', ':4', ':5', ':6', ':7') \
			and c_acctbal > ( \
				select \
					avg(c_acctbal) \
				from \
					customer \
				where \
					c_acctbal > 0.00 \
					and substring(c_phone from 1 for 2) in \
						(':1', ':2', ':3', ':4', ':5', ':6', ':7') \
			) \
			and not exists ( \
				select \
					* \
				from \
					orders \
				where \
					o_custkey = c_custkey \
			) \
	) as custsale \
group by \
	cntrycode \
order by \
	cntrycode; "
   << endl; 

/* ------------ Starting Query 22 from Here                   ----- */ 

        //This is the main query body for query 22 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("calpont.FROMTABLE.cntrycode");
        //c1->tableAlias("custsale");
        colList.push_back(c1);
        ArithmeticColumn *c2 = new ArithmeticColumn("count(ALL)");
        c2->alias("numcust");
        //c2->tableAlias("custsale");
        colList.push_back(c2);
        ArithmeticColumn *c3 = new ArithmeticColumn("sum(c_acctbal)");
        c3->alias("totacctbal");
        //c3->tableAlias("custsale");
        colList.push_back(c3);
        csep->returnedCols(colList);  // set Projection columns
          
        // Filter columns (csep)
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 

        //   --------------- first sub select begins here ----------------  x
        //Sub select filter 
        CalpontSelectExecutionPlan *sub01csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);

        sub01csep->tableAlias("custsale"); 

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub01ColList; 

        ArithmeticColumn *s01c1 = new ArithmeticColumn ("substring(c_phone from 1 for 2 )"); 
        sub01ColList.push_back(s01c1); 
        s01c1->alias("cntrycode");
        SimpleColumn *s01c2 = new SimpleColumn ("tpch.customer.c_acctbal"); 
        sub01ColList.push_back(s01c2); 
  	// Append returned columns to subselect 
        sub01csep->returnedCols(sub01ColList); 

        //subselect Filters (sub01csep)
        CalpontSelectExecutionPlan::FilterTokenList sub01csep_filterlist; 

        sub01csep_filterlist.push_back(new Operator("("));

	SimpleFilter *s01f1 = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:1"));
	sub01csep_filterlist.push_back(s01f1); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1a = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:2"));
	sub01csep_filterlist.push_back(s01f1a); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1b = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:3"));
	sub01csep_filterlist.push_back(s01f1b); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1c = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:4"));
	sub01csep_filterlist.push_back(s01f1c); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1d = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:5"));
	sub01csep_filterlist.push_back(s01f1d); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1e = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:6"));
	sub01csep_filterlist.push_back(s01f1e); 

        sub01csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s01f1f = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:7"));
	sub01csep_filterlist.push_back(s01f1f); 

        sub01csep_filterlist.push_back(new Operator(")"));

  
        sub01csep_filterlist.push_back(new Operator("and")); 

        //   --------------- inner sub select begins here ----------------  x 
        //Sub select filter 
        CalpontSelectExecutionPlan *sub011csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub011ColList; 

        ArithmeticColumn *s011c1 = new ArithmeticColumn ("avg(c_acctbal)"); 
        sub011ColList.push_back(s011c1); 
  	// Append returned columns to subselect 
        sub011csep->returnedCols(sub011ColList); 
    
        //subselect Filters (sub011csep)
        CalpontSelectExecutionPlan::FilterTokenList sub011csep_filterlist; 
	SimpleFilter *s011f1 = new SimpleFilter ( new Operator(">"), 
                               new SimpleColumn ("tpch.customer.c_acctbal"),
		               new ConstantColumn("0.00"));
	sub011csep_filterlist.push_back(s011f1); 
 
        sub011csep_filterlist.push_back(new Operator("and"));

        sub011csep_filterlist.push_back(new Operator("("));

	SimpleFilter *s011f2 = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:1"));
	sub011csep_filterlist.push_back(s011f2); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2a = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:2"));
	sub011csep_filterlist.push_back(s011f2a); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2b = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:3"));
	sub011csep_filterlist.push_back(s011f2b); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2c = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:4"));
	sub011csep_filterlist.push_back(s011f2c); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2d = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:5"));
	sub011csep_filterlist.push_back(s011f2d); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2e = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:6"));
	sub011csep_filterlist.push_back(s011f2e); 

        sub011csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *s011f2f = new SimpleFilter ( new Operator("="), 
                               new ArithmeticColumn ("substring(c_phone from 1 for 2 )"),
		               new ConstantColumn("%:7"));
	sub011csep_filterlist.push_back(s011f2f); 

        sub011csep_filterlist.push_back(new Operator(")"));

        sub011csep->filterTokenList(sub011csep_filterlist); 
        //   --------------- inner sub ends here ----------------  x

	// Now resolve the first inner sub_query 
	SelectFilter *s01f2 = new SelectFilter (new SimpleColumn ("tpch.customer.c_acctbal"),
	                      new Operator(">"), 
		              sub011csep);
	sub01csep_filterlist.push_back(s01f2); 
     
  
        //   --------------- second inner sub select begins here ----------------  x 
        //Sub select filter 
        CalpontSelectExecutionPlan *sub02csep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  sub02colList; 

        SimpleColumn *c21 = new SimpleColumn("tpch.orders.o_orderkey");
        sub02colList.push_back(c21);
        SimpleColumn *c22 = new SimpleColumn("tpch.orders.o_custkey");
        sub02colList.push_back(c22);
        SimpleColumn *c23 = new SimpleColumn("tpch.orders.o_orderstatus");
        sub02colList.push_back(c23);
        SimpleColumn *c24 = new SimpleColumn("tpch.orders.o_totalprice");
        sub02colList.push_back(c24);
        SimpleColumn *c25 = new SimpleColumn("tpch.orders.o_orderdate");
        sub02colList.push_back(c25);
        SimpleColumn *c26 = new SimpleColumn("tpch.orders.o_orderpriority");
        sub02colList.push_back(c26);
        SimpleColumn *c27 = new SimpleColumn("tpch.orders.o_clerk");
        sub02colList.push_back(c27);
        SimpleColumn *c28 = new SimpleColumn("tpch.orders.o_shippriority");
        sub02colList.push_back(c28);
        SimpleColumn *c29 = new SimpleColumn("tpch.orders.o_comment");
        sub02colList.push_back(c29);
  	// Append returned columns to subselect 
        sub02csep->returnedCols(sub02colList); 
    
        //subselect Filters (sub02csep)
        CalpontSelectExecutionPlan::FilterTokenList sub02csep_filterlist; 
	SimpleFilter *s02f1 = new SimpleFilter ( new Operator("="), 
                              new SimpleColumn ("tpch.orders.o_custkey"),
                              new SimpleColumn ("tpch.customer.c_custkey"));
	sub02csep_filterlist.push_back(s02f1); 

        sub02csep->filterTokenList(sub02csep_filterlist); 
 
        //   --------------- second inner sub ends here ----------------  x
 
	ExistsFilter *s02f2 = new ExistsFilter (sub02csep,"FALSE");

//	sub01csep_filterlist.push_back(s01f1); //Previous added.  
        sub01csep_filterlist.push_back(new Operator("and"));
  	sub01csep_filterlist.push_back(s02f2); 

        //   --------------- first sub ends here ----------------  x  
	// Now resolve the outer query 
  
        sub01csep->filterTokenList(sub01csep_filterlist); 
        /*
	SelectFilter *f1 = new SelectFilter (new SimpleColumn ("tpch.supplier.s_suppkey"),
	                      new Operator("IN"), 
		              sub01csep);
	csep_filterlist.push_back(f1); 
	csep->filterTokenList(csep_filterlist); 
        */
          
        // -- New: Implemented with subselect versus selectFilter -- 
        CalpontSelectExecutionPlan::SelectList subselectlist; 
        subselectlist.push_back(sub01csep); 
        csep->subSelects(subselectlist); 
           

	// Build Group By List
   
  
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        //SimpleColumn *g1 = new SimpleColumn("tpch.custsale.cntrycode");
        SimpleColumn *g1 = new SimpleColumn(*c1);
        g1->alias("cntrycode"); 
        g1->tableAlias("custtable"); 
        csep_groupbyList.push_back(g1);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o1 = new SimpleColumn("calpont.FROMTABLE.cntrycode");
        o1->tableAlias("custsale"); 
        csep_orderbyList.push_back(o1);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        
   
	//filterList->walk(walkfnString); ?? 

  
        // Print the parse tree 

        ParseTree *sub011pt = const_cast<ParseTree*>(sub011csep->filters());
        sub011pt->drawTree("selectExecutionPlan_sub011_22.dot");

        ParseTree *sub01pt = const_cast<ParseTree*>(sub01csep->filters());
        sub01pt->drawTree("selectExecutionPlan_sub01_22.dot");

        ParseTree *sub02pt = const_cast<ParseTree*>(sub02csep->filters());
        sub02pt->drawTree("selectExecutionPlan_sub02_22.dot");

        //ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        //pt->drawTree("selectExecutionPlan_22.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 22 ---" << endl;
   
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


