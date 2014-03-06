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

CPPUNIT_TEST( selectExecutionPlan_19 );

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
    
    void selectExecutionPlan_19() {

cout << 
"SQL: \
select \
	sum(l_extendedprice* (1 - l_discount)) as revenue \
from \
	lineitem, \
	part \
where \
	( \
		p_partkey = l_partkey \
		and p_brand = ':1' \
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
		and l_quantity >= :4 and l_quantity <= :4 + 10 \
		and p_size between 1 and 5 \
		and l_shipmode in ('AIR', 'AIR REG') \
		and l_shipinstruct = 'DELIVER IN PERSON' \
	) \
	or \
	( \
		p_partkey = l_partkey \
		and p_brand = ':2' \
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
		and l_quantity >= :5 and l_quantity <= :5 + 10 \
		and p_size between 1 and 10 \
		and l_shipmode in ('AIR', 'AIR REG') \
		and l_shipinstruct = 'DELIVER IN PERSON' \
	) \
	or \
	( \
		p_partkey = l_partkey \
		and p_brand = ':3' \
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
		and l_quantity >= :6 and l_quantity <= :6 + 10 \
		and p_size between 1 and 15 \
		and l_shipmode in ('AIR', 'AIR REG') \
		and l_shipinstruct = 'DELIVER IN PERSON' \
	); " 
   << endl; 

/* ------------The create view statement is being re-written as an inline view  ----- */ 
/* ------------ (dynamic table) to be used in a from cluase.                    ----- */ 

        //This is the main query body for query 16 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        ArithmeticColumn *c1 = new ArithmeticColumn("sum(l_quantity * (1 - l_discount))"); 
        c1->alias("revenue"); 
        colList.push_back(c1);
        csep->returnedCols(colList);  // set Projection columns

        // Filter columns
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 

        /* --  Where: Part 1 -- */ 
        csep_filterlist.push_back(new Operator("("));
	   
	SimpleFilter *f1 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_partkey"),
                           new SimpleColumn ("tpch.lineitem.l_partkey"));
	csep_filterlist.push_back(f1); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_brand"),
                           new ConstantColumn (":1"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f3a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("SM CASE"));
	csep_filterlist.push_back(f3a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f3b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("SM BOX"));
	csep_filterlist.push_back(f3b); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f3c = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("SM PACK"));
	csep_filterlist.push_back(f3c); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f3d = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("SM PKG"));
	csep_filterlist.push_back(f3d); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));
        
	SimpleFilter *f4a = new SimpleFilter ( new Operator(">="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ConstantColumn (":4"));
	csep_filterlist.push_back(f4a); 

        csep_filterlist.push_back(new Operator("and")); 
 
	SimpleFilter *f4b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ArithmeticColumn ("(l_quantity :4 + 10)"));
	csep_filterlist.push_back(f4b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f5a = new SimpleFilter ( new Operator(">="),
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("1"));
	csep_filterlist.push_back(f5a); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f5b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("5"));
	csep_filterlist.push_back(f5b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f6a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR"));
	csep_filterlist.push_back(f6a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f6b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR REG"));
	csep_filterlist.push_back(f6b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f7 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.lineitem.o_orderkey"),
                           new ConstantColumn ("DELIVER IN PERSON"));
	csep_filterlist.push_back(f7); 
          
        csep_filterlist.push_back(new Operator(")"));

        /* --  Where: Part 2 -- */ 
        csep_filterlist.push_back(new Operator("or"));
        csep_filterlist.push_back(new Operator("("));
	     
	SimpleFilter *f8 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_partkey"),
                           new SimpleColumn ("tpch.lineitem.l_partkey"));
	csep_filterlist.push_back(f8); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f9 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_brand"),
                           new ConstantColumn (":2"));
	csep_filterlist.push_back(f9); 

        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f10a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("MED BAG"));
	csep_filterlist.push_back(f10a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f10b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("MED BOX"));
	csep_filterlist.push_back(f10b); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f10c = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("MED PKG"));
	csep_filterlist.push_back(f10c); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f10d = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("MED PACK"));
	csep_filterlist.push_back(f10d); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));
        
	SimpleFilter *f11a = new SimpleFilter ( new Operator(">="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ConstantColumn (":5"));
	csep_filterlist.push_back(f11a); 

        csep_filterlist.push_back(new Operator("and")); 
 
	SimpleFilter *f11b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ArithmeticColumn ("(l_quantity :5 + 10)"));
	csep_filterlist.push_back(f11b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f12a = new SimpleFilter ( new Operator(">="),
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("1"));
	csep_filterlist.push_back(f12a); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f12b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("10"));
	csep_filterlist.push_back(f12b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f13a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR"));
	csep_filterlist.push_back(f13a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f13b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR REG"));
	csep_filterlist.push_back(f13b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f14 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.lineitem.o_orderkey"),
                           new ConstantColumn ("DELIVER IN PERSON"));
	csep_filterlist.push_back(f14); 
           
        csep_filterlist.push_back(new Operator(")"));
   
        /* --  Where: Part 3 -- */ 
        csep_filterlist.push_back(new Operator("or"));
        csep_filterlist.push_back(new Operator("("));
	   
	SimpleFilter *f15 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_partkey"),
                           new SimpleColumn ("tpch.lineitem.l_partkey"));
	csep_filterlist.push_back(f15); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f16 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_brand"),
                           new ConstantColumn (":3"));
	csep_filterlist.push_back(f16); 

        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f17a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("LG CASE"));
	csep_filterlist.push_back(f17a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f17b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("LG BOX"));
	csep_filterlist.push_back(f17b); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f17c = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("LG PACK"));
	csep_filterlist.push_back(f17c); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f17d = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.part.p_container"),
                            new ConstantColumn ("LG PKG"));
	csep_filterlist.push_back(f17d); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));
        
	SimpleFilter *f18a = new SimpleFilter ( new Operator(">="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ConstantColumn (":6"));
	csep_filterlist.push_back(f18a); 

        csep_filterlist.push_back(new Operator("and")); 
 
	SimpleFilter *f18b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.lineitem.l_quantity"),
                            new ArithmeticColumn ("(l_quantity :6 + 10)"));
	csep_filterlist.push_back(f18b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f19a = new SimpleFilter ( new Operator(">="),
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("1"));
	csep_filterlist.push_back(f19a); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f19b = new SimpleFilter ( new Operator("<="), 
                            new SimpleColumn ("tpch.part.p_size"),
                            new ConstantColumn ("15"));
	csep_filterlist.push_back(f19b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));
        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f20a = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR"));
	csep_filterlist.push_back(f20a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f20b = new SimpleFilter ( new Operator("="), 
                            new SimpleColumn ("tpch.lineitem.l_shipmode"),
                            new ConstantColumn ("AIR REG"));
	csep_filterlist.push_back(f20b); 

        csep_filterlist.push_back(new Operator(")"));
        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f21 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.lineitem.o_orderkey"),
                           new ConstantColumn ("DELIVER IN PERSON"));
	csep_filterlist.push_back(f21); 
           
        csep_filterlist.push_back(new Operator(")"));

 
        /*-----  THIS IS WHERE IT ALL COMES TOGETHER ----- */ 
        csep->filterTokenList(csep_filterlist); //Set Filter Columns

	// Build Group By List
        //CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        //csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        //CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        //csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        
	//filterList->walk(walkfnString); ?? 

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        pt->drawTree("selectExecutionPlan_19.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 19 ---" << endl;
   
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


