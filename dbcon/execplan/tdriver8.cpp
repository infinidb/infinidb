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
#include "treenodeimpl.h"
#include "operator.h"
#include "arithmeticcolumn.h"
#include "aggregatecolumn.h"
#include "existsfilter.h"
#include "selectfilter.h"

using namespace execplan;

class TPCH_EXECPLAN : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( TPCH_EXECPLAN );

CPPUNIT_TEST( Q1 );

CPPUNIT_TEST_SUITE_END();

private:
public:
   
    void setUp() {
    }
    
    void tearDown() {
    }
    
    void Q1() {
            string sql = "\
    select\
	o_year,\
	sum(decode(nation, ':1', volumn, 0)) / sum(volume) as mkt_share\
from\
	(\
		select\
			extract(year from o_orderdate) as o_year,\
			l_extendedprice * (1 - l_discount) as volume,\
			n2.n_name as nation\
		from\
			part,\
			supplier,\
			lineitem,\
			orders,\
			customer,\
			nation n1,\
			nation n2,\
			region\
		where\
			p_partkey = l_partkey\
			and s_suppkey = l_suppkey\
			and l_orderkey = o_orderkey\
			and o_custkey = c_custkey\
			and c_nationkey = n1.n_nationkey\
			and n1.n_regionkey = r_regionkey\
			and r_name = ':2'\
			and s_nationkey = n2.n_nationkey\
			and o_orderdate between date '1995-01-01' and date '1996-12-31'\
			and p_type = ':3'\
	) as all_nations\
group by\
	o_year\
order by\
	o_year;";
        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        
        // these columns are from the temp table of FROM clause. 
        // I hereby give schema name "calpont", table name "FROMTABLE",
        SimpleColumn *c1 = new SimpleColumn("calpont.FROMTABLE.o_year");
        returnedColumnList.push_back(c1);
        ArithmeticColumn *c2 = new ArithmeticColumn("sum(decode(calpont.FROMTABLE.nation, ':1', calpont.FROMTABLE.volumn, 0))/sum(tpch.FROMTABLE.volumn)");
        returnedColumnList.push_back(c2);
        
        csep.returnedCols(returnedColumnList);
     
        // subselect in FROM clause      
        CalpontSelectExecutionPlan *subsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);
        
        // subselect returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        ArithmeticColumn *sc1 = new ArithmeticColumn("extract(year from tpch.orders.o_orderdate)");
        sc1->alias ("o_year");
        subReturnedColList.push_back(sc1);
              
        ArithmeticColumn *sc2 = new ArithmeticColumn("tpch.lineitem.l_extendeprice * (1-tpch.lineitem.l_discount)");
        sc2->alias("volume");
        subReturnedColList.push_back(sc2);
        
        SimpleColumn *sc3 = new SimpleColumn("tpch.nation.n_name");
        sc3->tableAlias("n2");
        subReturnedColList.push_back(sc3);
        
        subsep->returnedCols(subReturnedColList);
        
        // subselect filters
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        
        SimpleFilter *sf1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_partkey"),
                                             new SimpleColumn("tpch.lineitem.l_partkey"));
        subFilterTokenList.push_back(sf1);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_suppkey"),
                                             new SimpleColumn("tpch.lineitem.l_suppkey"));
        subFilterTokenList.push_back(sf2);
                                             
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.lineitem.l_ordertkey"),
                                             new SimpleColumn("tpch.orders.o_orderkey"));
        subFilterTokenList.push_back(sf3);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf4 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.orders.o_custkey"),
                                             new SimpleColumn("tpch.customer.c_custkey"));
        subFilterTokenList.push_back(sf4);
        subFilterTokenList.push_back(new Operator("and"));        
        
        SimpleColumn *n1_nationkey = new SimpleColumn("tpch.nation.n_nationkey");
        n1_nationkey->tableAlias("n1");
        SimpleFilter *sf5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.customer.c_nationkey"),
                                             n1_nationkey);
        subFilterTokenList.push_back(sf5);
        subFilterTokenList.push_back(new Operator("and"));          

        SimpleColumn *n1_regionkey = new SimpleColumn("tpch.nation.n_regionkey");
        n1_regionkey->tableAlias ("n1");
        SimpleFilter *sf6 = new SimpleFilter ( new Operator("="),
                                              n1_regionkey,
                                              new SimpleColumn("tpch.region.r_regionkey"));
        subFilterTokenList.push_back(sf6);
        subFilterTokenList.push_back(new Operator("and"));

        SimpleFilter *sf7 = new SimpleFilter ( new Operator("="),
                                              new SimpleColumn("tpch.region.r_name"),
                                              new ConstantColumn (":2"));
        subFilterTokenList.push_back(sf7);
        subFilterTokenList.push_back(new Operator("and"));                                                      
         
        SimpleColumn *n2_nationkey = new SimpleColumn("tpch.nation.n_nationkey");
        n2_nationkey->tableAlias("n2");
        SimpleFilter *sf9 = new SimpleFilter ( new Operator("="),
                                              new SimpleColumn("tpch.supplier.s_nationkey"),
                                              n2_nationkey);   
        subFilterTokenList.push_back(sf9);                                                                                                                         
        
        subFilterTokenList.push_back(new Operator("and"));      
        
        SimpleFilter *sf10 = new SimpleFilter (new Operator (">="),
                                               new SimpleColumn ("tpch.orders.o_orderdate"),
                                               new ConstantColumn ("1995-01-01")); 
        subFilterTokenList.push_back(sf10);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf11 = new SimpleFilter (new Operator ("<="),
                                               new SimpleColumn ("tpch.orders.o_orderdate"),
                                               new ConstantColumn ("1995-01-06"));                                                         
        subFilterTokenList.push_back(sf11);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf12 = new SimpleFilter (new Operator ("="),
                                               new SimpleColumn ("tpch.part.p_type"),
                                               new ConstantColumn ("3"));            
        subFilterTokenList.push_back(sf12);

        subsep->filterTokenList(subFilterTokenList);  
        subsep->tableAlias("all_nations");
        
        // end of subselect in FROM. push FROM subselect to selectList
        // NOTE: only FROM subselect needs to be pushed into selectList.
        // Subselects in WHERE or HAVING clause are in where or having
        // filter parse tree. It may make more sense to change the member
        // fSelectList of CSEP class to fFromSubSelect (type CSEP*)
        CalpontSelectExecutionPlan::SelectList fromSubSelectList;
        fromSubSelectList.push_back(subsep);
        csep.subSelects(fromSubSelectList);
       
        ParseTree* pt = const_cast<ParseTree*>(subsep->filters());
        pt->drawTree("q8.dot");   
        
        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
	    SimpleColumn *g1 = new SimpleColumn (*c1);
	    groupByList.push_back (g1);	
	    
	    csep.groupByCols(groupByList);
	    
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    SimpleColumn *o1 = new SimpleColumn(*c1);
	    orderByList.push_back(o1);
	         
        csep.orderByCols(orderByList);
        
        cout << csep;
    }  

    
}; 

CPPUNIT_TEST_SUITE_REGISTRATION( TPCH_EXECPLAN );

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


