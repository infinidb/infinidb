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

CPPUNIT_TEST( selectExecutionPlan_16 );

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
    
    void selectExecutionPlan_16() {

cout << 
"SQL: \
select \
	p_brand, \
	p_type, \
	p_size, \
	count(distinct ps_suppkey) as supplier_cnt \
from \
	partsupp, \
	part \
where \
	p_partkey = ps_partkey \
	and p_brand <> ':1' \
	and p_type not like ':2%' \
	and p_size in (:3, :4, :5, :6, :7, :8, :9, :10) \
	and ps_suppkey not in ( \
		select \
			s_suppkey \
		from \
			supplier \
		where \
			s_comment like '%Customer%Complaints%' \
	) \
group by \
	p_brand, \
	p_type, \
	p_size \
order by \
	supplier_cnt desc, \
	p_brand, \
	p_type, \
	p_size;" 
   << endl; 

/* ------------The create view statement is being re-written as an inline view  ----- */ 
/* ------------ (dynamic table) to be used in a from cluase.                    ----- */ 

        //This is the main query body for query 16 
        CalpontSelectExecutionPlan *csep = new CalpontSelectExecutionPlan();
        
        // Create the Projection (returned columns)
        CalpontSelectExecutionPlan::ReturnedColumnList colList;
        SimpleColumn *c1 = new SimpleColumn("tpch.part.p_brand");
        colList.push_back(c1);
        SimpleColumn *c2 = new SimpleColumn("tpch.part.p_type");
        colList.push_back(c2);
        SimpleColumn *c3 = new SimpleColumn("tpch.part.p_size");
        colList.push_back(c3);
	AggregateColumn *c4 = new AggregateColumn(); 
        c4->functionName("count");
        c4->alias("supplier_cnt");
        ArithmeticColumn *a1 = new ArithmeticColumn("(distinct tpch.part.ps_suppkey)"); 
        c4->functionParms(a1);
        colList.push_back(c4);
          
        csep->returnedCols(colList);  // set Projection columns

        // Filter columns
        CalpontSelectExecutionPlan::FilterTokenList csep_filterlist; 
 
	SimpleFilter *f1 = new SimpleFilter ( new Operator("="), 
                           new SimpleColumn ("tpch.part.p_partkey"),
		           new SimpleColumn ("tpch.partsupp.ps_partkey"));
	csep_filterlist.push_back(f1); 

        csep_filterlist.push_back(new Operator("and"));

	SimpleFilter *f2 = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_brand"),
		           new ConstantColumn(":1"));
	csep_filterlist.push_back(f2); 

        csep_filterlist.push_back(new Operator("and"));
        
        //These next condition blocks where orginally written with ConditionFilter
	SimpleFilter *f3 = new SimpleFilter ( new Operator("not like"), 
                           new SimpleColumn ("tpch.part.p_type"),
		           new ConstantColumn(":2"));
	csep_filterlist.push_back(f3); 

        csep_filterlist.push_back(new Operator("and"));

        csep_filterlist.push_back(new Operator("("));

	SimpleFilter *f4a = new SimpleFilter ( new Operator("<>"), 
                            new SimpleColumn ("tpch.part.p_size"),
		            new ConstantColumn(":3"));
	csep_filterlist.push_back(f4a); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4b = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":4"));
	csep_filterlist.push_back(f4b); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4c = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":5"));
	csep_filterlist.push_back(f4c); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4d = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":6"));
	csep_filterlist.push_back(f4d); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4e = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":7"));
	csep_filterlist.push_back(f4e); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4f = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":8"));
	csep_filterlist.push_back(f4f); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4g = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":9"));
	csep_filterlist.push_back(f4g); 

        csep_filterlist.push_back(new Operator("or"));

	SimpleFilter *f4h = new SimpleFilter ( new Operator("<>"), 
                           new SimpleColumn ("tpch.part.p_size"),
		           new ConstantColumn(":10"));
	csep_filterlist.push_back(f4h); 

        csep_filterlist.push_back(new Operator(")"));

        csep_filterlist.push_back(new Operator("and"));

        /*   --------------- sub select begins here ----------------  */ 
        //Sub select filter 
        CalpontSelectExecutionPlan *subcsep = 
            new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);

        //subselect return column(s)
        CalpontSelectExecutionPlan::ReturnedColumnList  subColList; 

        SimpleColumn *sc1 = new SimpleColumn ("tpch.supplier.s_suppkey"); 
        subColList.push_back(sc1); 
  	// Append returned columns to subselect 
        subcsep->returnedCols(subColList); 

        //subselect Filters 
        CalpontSelectExecutionPlan::FilterTokenList subcsep_filterlist; 
	SimpleFilter *sf1 = new SimpleFilter ( new Operator("LIKE"), 
                            new SimpleColumn ("tpch.supplier.s_comment"),
		            new ConstantColumn("%Customer%Complaints%"));
	subcsep_filterlist.push_back(sf1); 

        /*   --------------- sub select begins here ----------------  */ 
 
        //CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList; 
	SelectFilter *f5 = new SelectFilter (new SimpleColumn ("tpch.partsupp.ps_suppkey"),
	                      new Operator("not in"), 
		              subcsep);
  
	csep_filterlist.push_back(f5); 
/*

        csep_filterlist.push_back(new Operator("and"));

	//ConditionFilter *f3 = new ConditionFilter ("p_brand not like ':2%'");
	ConditionFilter *f3 = new ConditionFilter ();
        f3->expression ("p_brand not like ':2%'");
	csep_filterlist.push_back(f3); 

        csep_filterlist.push_back(new Operator("and"));

	//ConditionFilter *f4 = new ConditionFilter ("p_brand not in (:3, :4, :5, :6, :7, :8, :9, :10");
	ConditionFilter *f4 = new ConditionFilter ();
	f4->expression ("p_brand not in (:3, :4, :5, :6, :7, :8, :9, :10");
	csep_filterlist.push_back(f4); 

*/ 
        csep->filterTokenList(csep_filterlist); //Set Filter Columns


	// Build Group By List
        CalpontSelectExecutionPlan::GroupByColumnList csep_groupbyList;
        SimpleColumn *g1 = new SimpleColumn("tpch.part.p_brand");
        csep_groupbyList.push_back(g1);
        SimpleColumn *g2 = new SimpleColumn("tpch.part.p_type");
        csep_groupbyList.push_back(g2);
        SimpleColumn *g3 = new SimpleColumn("tpch.part.p_size");
        csep_groupbyList.push_back(g3);
        csep->groupByCols(csep_groupbyList);  //Set GroupBy columns 

	// Order By List
        CalpontSelectExecutionPlan::OrderByColumnList csep_orderbyList;
        SimpleColumn *o0 = new SimpleColumn("supplier_cnt");  // AggregateColumn has no asc/desc order
        o0->alias("supplier_cnt");
        o0->asc(false);
        csep_groupbyList.push_back(o0);
        SimpleColumn *o1 = new SimpleColumn("tpch.part.p_brand");
        csep_orderbyList.push_back(o1);
        SimpleColumn *o2 = new SimpleColumn("tpch.part.p_type");
        csep_orderbyList.push_back(o2);
        SimpleColumn *o3 = new SimpleColumn("tpch.part.p_size");
        csep_orderbyList.push_back(o3);
        csep->orderByCols(csep_orderbyList); //Set OrderBy columns 
        
	//filterList->walk(walkfnString); ?? 

        // Print the parse tree 
        ParseTree *pt = const_cast<ParseTree*>(csep->filters());
        pt->drawTree("selectExecutionPlan_16.dot");

        cout << "\nCalpont Execution Plan:" << endl;
        cout << *csep << endl;
        cout << " --- end of test 16 ---" << endl;
   
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


