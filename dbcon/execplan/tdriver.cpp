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
#include <cstdlib>
#include <pthread.h>
#include <values.h>
#include <signal.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>

#include "configcpp.h"
using namespace config;

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
#include "existsfilter.h"
#include "functioncolumn.h"
#include "selectfilter.h"
#include "objectreader.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
#include "sessionmonitor.h"
#include "treenodeimpl.h"

using namespace execplan;

const char *OIDBitmapFilename = NULL;
int maxNewTxns=1000;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST( selectExecutionPlan_1 );
CPPUNIT_TEST( selectExecutionPlan_2 );
CPPUNIT_TEST( expressionParser_1 );
CPPUNIT_TEST( expressionParser_2 );
CPPUNIT_TEST( aggregatecolumn_1 );
CPPUNIT_TEST( arithmeticExpression_1 );
CPPUNIT_TEST( arithmeticExpression_2 );
CPPUNIT_TEST( arithmeticExpression_3 );
CPPUNIT_TEST( copyTree );
CPPUNIT_TEST( treeNodeImpl );
CPPUNIT_TEST( serializeSimpleColumn );
CPPUNIT_TEST( serializeAggregateColumn );
CPPUNIT_TEST( serializeOperator );
CPPUNIT_TEST( serializeFilter );
CPPUNIT_TEST( serializeFunctionColumn );
CPPUNIT_TEST( serializeConstantColumn );
CPPUNIT_TEST( serializeParseTree );
CPPUNIT_TEST( serializeArithmeticColumn );
CPPUNIT_TEST( serializeSimpleFilter );
CPPUNIT_TEST( serializeCSEP );
CPPUNIT_TEST( serializeSelectFilter );
CPPUNIT_TEST( serializeExistsFilter );

CPPUNIT_TEST( sessionManager_1 );
CPPUNIT_TEST( sessionManager_2 );
CPPUNIT_TEST( sessionManager_3 );
CPPUNIT_TEST( sessionManager_4 );

unlink("/tmp/CalpontSessionMonitorShm");
//CPPUNIT_TEST( MonitorTestPlan_1 );
//CPPUNIT_TEST( MonitorTestPlan_1 );
unlink("/tmp/CalpontSessionMonitorShm");
CPPUNIT_TEST( objectIDManager_1 );
if (OIDBitmapFilename != NULL)
	unlink(OIDBitmapFilename);

CPPUNIT_TEST_SUITE_END();

private:
	      boost::shared_ptr<CalpontSystemCatalog> csc;
public:
	
    static void walkfnString(const ParseTree* n)
    {
        cout << *(n->data()) << endl;
    }
    
    static void walkfnInt(const ExpressionTree<int>* n)
    {
        cout << n->data() << endl;
    }    
    
    void setUp() {
    	  csc = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
        csc->identity(CalpontSystemCatalog::FE);
    }
    
    void tearDown() {
    }
    
    void selectExecutionPlan_1() {
        cout << "SQL: select region.r_regionkey from region, nation where nation.n_regionkey = region.r_regionkey and nation.n_regionkey != 3;" << endl;

        CalpontSelectExecutionPlan csep;
        CPPUNIT_ASSERT (csep.location() == CalpontSelectExecutionPlan::MAIN);
        CPPUNIT_ASSERT (csep.dependent() == false);
        CPPUNIT_ASSERT (csep.subSelects().size() == 0);
        
        // returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList colList;        
        SimpleColumn *sc = new SimpleColumn("tpch.region.r_regionkey", 0);
        colList.push_back(sc);        
        ArithmeticColumn *ac = new ArithmeticColumn("a+sum(r_regionkey)", 0);
        colList.push_back(ac);
        csep.returnedCols (colList);   
        CPPUNIT_ASSERT(csep.returnedCols().size() == 2);     
               
        // filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
       
        SimpleFilter *sf = new SimpleFilter();
        SimpleColumn *lhs = new SimpleColumn();
        *lhs = *sc;       
        SimpleColumn *rhs = new SimpleColumn("tpch.nation.n_regionkey", 0);
        CPPUNIT_ASSERT (*lhs == *sc);
        CPPUNIT_ASSERT (*rhs != *lhs);
        Operator *op = new Operator("=");
        
        sf->op(op);
        sf->lhs(lhs);
        sf->rhs(rhs);
        filterTokenList.push_back (sf);
        
        filterTokenList.push_back( new Operator ("And") );   
        SimpleFilter *sf1 = new SimpleFilter (new Operator("="), sc->clone(), ac->clone());
        
        filterTokenList.push_back (sf1);
               
        csep.filterTokenList (filterTokenList);
        ParseTree *filterList = const_cast<ParseTree*> (csep.filters());
        
        // draw filterList tree
        filterList->drawTree("selectExecutionPlan_1.dot");                     
        csep.filters (filterList);

        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
        groupByList.push_back(sc->clone());        
        csep.groupByCols (groupByList);
        CPPUNIT_ASSERT(csep.groupByCols().size() == 1);
        
        // Having
        CalpontSelectExecutionPlan::FilterTokenList havingTokenList;
        SimpleFilter *having = new SimpleFilter( new Operator("="),
                                                 new ArithmeticColumn("sum(volumn)", 0),
                                                 new ConstantColumn(8));
        havingTokenList.push_back (having);
        csep.havingTokenList (havingTokenList);
        CPPUNIT_ASSERT (*sf1 != *having);
        CPPUNIT_ASSERT (csep.havingTokenList().size() == 1);                                                         
        
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    ArithmeticColumn *o1 = new ArithmeticColumn(*ac);
	    o1->asc(false);
	    orderByList.push_back(o1);
        csep.orderByCols(orderByList);
        CPPUNIT_ASSERT(csep.orderByCols().size() == 1);        
        
        // another csep
        CalpontSelectExecutionPlan *newcsep = new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);
        CalpontSelectExecutionPlan::ReturnedColumnList ncolList;        
        SimpleColumn *newsc = new SimpleColumn("tpch.region.r_regionkey", 0);
        ncolList.push_back(newsc);        
        newcsep->returnedCols (ncolList);  
        CalpontSelectExecutionPlan::FilterTokenList nfilterTokenList; 
        SimpleFilter *newsf = new SimpleFilter ( new Operator (">"),
                                    sc->clone(),
                                    newsc->clone());
        nfilterTokenList.push_back(newsf);
        newcsep->filterTokenList (nfilterTokenList);
        CalpontSelectExecutionPlan::FilterTokenList nhavingTokenList;
        SimpleFilter *newhaving = new SimpleFilter ( new Operator (">"),
                                    sc->clone(),
                                    newsc->clone());  
        CPPUNIT_ASSERT (*newsf == *newhaving);                                    
        nhavingTokenList.push_back(newhaving);
        newcsep->havingTokenList (nhavingTokenList);
        CPPUNIT_ASSERT (*newcsep != csep); 
        CPPUNIT_ASSERT (*newcsep->filters() == *newcsep->having());
        ByteStream b;
        csep.serialize (b);
        newcsep->unserialize (b);
        CPPUNIT_ASSERT (csep == *newcsep);
        CalpontSelectExecutionPlan::SelectList selectList;
        selectList.push_back(newcsep);
        csep.subSelects(selectList);        
        cout << "\nCalpont Execution Plan:" << endl;
        cout << csep;
        cout << " --- end of test 1 ---" << endl;
    }
       
    void selectExecutionPlan_2()
    {
        CalpontSelectExecutionPlan cep;
        
        // select filter
        CalpontSelectExecutionPlan *subselect = new CalpontSelectExecutionPlan;
        subselect->location(CalpontSelectExecutionPlan::WHERE);
        subselect->dependent (false);
        CPPUNIT_ASSERT (subselect->location() == CalpontSelectExecutionPlan::WHERE);
        CPPUNIT_ASSERT (subselect->dependent() == false);
        ByteStream selb;
        subselect->serialize(selb);
        CalpontSelectExecutionPlan *subselect1 = new CalpontSelectExecutionPlan;
        subselect->location(CalpontSelectExecutionPlan::WHERE);
        subselect1->unserialize(selb);
        
        SelectFilter *sef = new SelectFilter ( new SimpleColumn ("tpch.region.r_regionkey", 0),
                                                new Operator (">="),
                                                subselect);
        cout << *sef;
        ByteStream b;
        sef->serialize(b);
        SelectFilter *newsef = new SelectFilter ( new SimpleColumn ("tpch.nation.n_regionke", 0),
                                                new Operator ("<="),
                                                subselect1);
        newsef->unserialize(b);
        CPPUNIT_ASSERT(*sef == *newsef);                                                
        delete sef;
        delete newsef;
        
        // simple filter
        Filter *sf = new SimpleFilter(new Operator("="), 
        															new SimpleColumn ("tpch.nation.n_regionke", 0),
        															new SimpleColumn ("tpch.region.r_regionkey", 0));
        cout << *sf;
        
        ByteStream sfb;
        SimpleFilter *sf2 = new SimpleFilter();
        sf2->serialize (sfb);
        sf->unserialize (sfb);
        CPPUNIT_ASSERT (*sf == *sf2);
        delete sf2;
        delete sf;
         
        // exist filter
        CalpontSelectExecutionPlan *cep1 = new CalpontSelectExecutionPlan();
        ExistsFilter *filter = new ExistsFilter();
        delete filter;
        filter = new ExistsFilter(cep1);        
        filter->exists(cep1);
        const CalpontSelectExecutionPlan* cep2 = filter->exists();
        cout << *filter;
        CPPUNIT_ASSERT (*cep1 == *cep2);
        CalpontSelectExecutionPlan::Parser parser;
        std::vector<Token> tokens;
        Token t;
        t.value = filter;
        tokens.push_back(t);
        cep.filters(parser.parse(tokens.begin(), tokens.end())); 
        cout << cep;
        
        cout << " --- end of test 2 ---" << endl;
    }

       
    void expressionParser_1() {
        cout << "\nPost order of int expression tree(pointer):" << endl;
        ExpressionTree<int> *et = new ExpressionTree<int>(3);
        ExpressionTree<int> *et1 = new ExpressionTree<int>(5);
        ExpressionTree<int> *et2 = new ExpressionTree<int>(6);
        et->left(et1);
        et->right(et2);
        et->walk(walkfnInt);
        et->destroyTree(et);
        cout << " --- end of test 3 ---" << endl;
    }
    
    void expressionParser_2() {
        cout << "\nPost order of int expression tree (reference):" << endl;
        ExpressionTree<int> et(3);
        ExpressionTree<int> et1(5);
        ExpressionTree<int> et2(6);
        et.left(&et1);
        et.right(&et2);
        et.walk(walkfnInt);
        cout << " --- end of test 4 ---" << endl;        
    }
    
    void aggregatecolumn_1() {  
        cout << "\naggregate column: " << endl;    
        AggregateColumn a;
        cout << a;
        cout << "\nsum(p_type)" << endl;
        AggregateColumn d("sum", "p_type"); 
        cout << d;
        a.functionName("avg");
        ArithmeticColumn *b = new ArithmeticColumn("a-(b-c)");
        a.functionParms(b);
        CPPUNIT_ASSERT(a.functionName() == "avg");
        cout << *const_cast<ReturnedColumn*>(a.functionParms());
        cout << " --- end of test 5 ---" << endl;         
    }
    
    void arithmeticExpression_1()
    {
        cout << "\narithmetic expression: " << endl;
        string exp("substr(a)+ 100.00 * sum(tpch.part.p_type) / sum(tpch.lineitem.l_extendedprice *(1-tpch.lineitem.l_discount))");
        cout << exp << endl;
        ArithmeticColumn a(exp, 0);
        ParseTree* pt = const_cast<ParseTree*>(a.expression());
        if (pt != NULL)
        {
            pt->walk(walkfnString);
            pt->drawTree("arithmeticExpression_1.dot");
        }
        cout << " --- end of test 6 ---" << endl;         
    }
    
    void copyTree()
    {
        //cout << "\narithmetic expression: " << endl;
        string exp("substr(a)+ 100.00 * sum(tpch.part.p_type) / sum(tpch.lineitem.l_extendedprice *(1-tpch.lineitem.l_discount))");
        ArithmeticColumn a(exp);
        ParseTree* pt = const_cast<ParseTree*>(a.expression());
        
        ParseTree* newTree = new ParseTree();
        
        // copy 1st time
        newTree->copyTree(*pt);        
        
        // copy 2nd time, see if the existing tree is deleted.
        newTree->copyTree (*pt);
        
        // explicitly delete the 2nd copied tree
        delete newTree;
    }    
    
    void arithmeticExpression_2()
    {
        cout << "\nbuild arithmetic column by using accessing methods:" << endl;
        CalpontSelectExecutionPlan::Parser parser;
        std::vector<Token> tokens;
        Token t;
        
        ArithmeticColumn b;
        cout << b;
        
        ConstantColumn *c1 = new ConstantColumn();
        c1->constval("'ASIA'");
        c1->type(ConstantColumn::LITERAL);
        //CPPUNIT_ASSERT (c1->data() == "'ASIA'(l)");
        
        t.value = c1;
        tokens.push_back(t);
        
        t.value = new Operator ("/");
        tokens.push_back(t);        
        
        ConstantColumn *c2 = new ConstantColumn(5);        
        CPPUNIT_ASSERT (c2->type() == ConstantColumn::NUM);
        t.value = c2;
        tokens.push_back(t);
        
        ParseTree* tree = parser.parse(tokens.begin(), tokens.end());
        b.expression (tree);

        cout << b;
        cout << " --- end of test 7 ---" << endl;                 
    }

    void arithmeticExpression_3()
    {       		
        // invalid expression test
        try {
            ArithmeticColumn a ("-a+b", 0);
            ArithmeticColumn d("a* b-", 0);              
        }catch (const runtime_error& e) { cerr << e.what() << endl; } 
        
        try { 
            ArithmeticColumn e("a+substr (c from 1 4", 0);           
        } catch (const runtime_error& e) { cerr << e.what() << endl; } 
        
        try {
            ArithmeticColumn f("a + b c", 0);  
        }
        catch (const runtime_error& e) { cerr << e.what() << endl; }       
        try {
            ArithmeticColumn b("a + ((b+ c -e)", 0);  
        }
        catch (const runtime_error& e) { cerr << e.what() << endl; }             
        try {
            ArithmeticColumn g("a ++ b", 0);  // valid
        }
        catch (const runtime_error& e) { cerr << e.what() << endl; }                               
    }
    
    void treeNodeImpl ()
    {
        TreeNodeImpl *node1 = new TreeNodeImpl();
        TreeNodeImpl *node2 = new TreeNodeImpl( "node2" );
        CPPUNIT_ASSERT (node2->data() == "node2");
        CPPUNIT_ASSERT (*node1 != *node2);
        
        ByteStream b;
        node2->serialize (b);
        node1->unserialize (b);
        CPPUNIT_ASSERT (*node1 == *node2);
        
        node2->data ("node3");
        cout << *node2;
        
        TreeNodeImpl *node4 = node2->clone();
        CPPUNIT_ASSERT (*node2 == node4);
        
        delete node1;
        delete node2;
        delete node4;
    }

	void serializeSimpleColumn()
	{
		SimpleColumn s1, s2;
		TreeNode *t;
		ByteStream b;
		
		t = &s2;
		
		CPPUNIT_ASSERT(s1 == s2);
		CPPUNIT_ASSERT(!(s1 != s2));
		CPPUNIT_ASSERT(s1 == t);
		CPPUNIT_ASSERT(!(s1 != t));		
		
		s1.schemaName("Schema Name 1");
		s1.tableName("Table Name 1");
		s1.columnName("Column Name 1");
		//s1.tcn(5);
		s1.data("sc1");
		
		s1.serialize(b);
		
		CPPUNIT_ASSERT(s2.schemaName() == "");
		CPPUNIT_ASSERT(s2.tableName() == "");
		CPPUNIT_ASSERT(s2.columnName() == "");
		//CPPUNIT_ASSERT(s2.tcn() == 0);
		
		CPPUNIT_ASSERT(s1 != s2);
		CPPUNIT_ASSERT(s1 == s1);
		CPPUNIT_ASSERT(s1 != t);
		CPPUNIT_ASSERT(!(s1 == t));
		
		s2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		
		CPPUNIT_ASSERT(s2.schemaName() == "Schema Name 1");
		CPPUNIT_ASSERT(s2.tableName() == "Table Name 1");
		CPPUNIT_ASSERT(s2.columnName() == "Column Name 1");
		//CPPUNIT_ASSERT(s2.tcn() == 5);
		CPPUNIT_ASSERT(s2.data() == "sc1");
		
		CPPUNIT_ASSERT(s1 == s2);
		CPPUNIT_ASSERT(!(s1 != s2));
		CPPUNIT_ASSERT(s1 == t);
		CPPUNIT_ASSERT(!(s1 != t));
	}
	
	void serializeAggregateColumn()
	{
		AggregateColumn a1, a2;
		SimpleColumn *s1;
		TreeNode *t;
		ByteStream b;
		
		t = &a2;
		
		s1 = new SimpleColumn();
		
		CPPUNIT_ASSERT(a1 == a2);
		CPPUNIT_ASSERT(!(a1 != a2));
		CPPUNIT_ASSERT(a1 == t);
		CPPUNIT_ASSERT(!(a1 != t));
		
		a1.functionName("AggregateColumn test");
		a1.data("agg1");
		a1.functionParms(s1);
		
		a1.serialize(b);
		
		CPPUNIT_ASSERT(a2.functionName() == "");
		CPPUNIT_ASSERT(a2.data() == "");
		CPPUNIT_ASSERT(a2.functionParms() == NULL);

		CPPUNIT_ASSERT(a1 != a2);
		CPPUNIT_ASSERT(!(a1 == a2));
		CPPUNIT_ASSERT(a1 != t);
		CPPUNIT_ASSERT(!(a1 == t));
		
		a2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		
		CPPUNIT_ASSERT(a2.functionName() == "AggregateColumn test");
		CPPUNIT_ASSERT(a2.data() == "agg1");
		CPPUNIT_ASSERT(a2.functionParms() != NULL);
		CPPUNIT_ASSERT(*(a2.functionParms()) == s1);
		
		CPPUNIT_ASSERT(a1 == a2);
		CPPUNIT_ASSERT(!(a1 != a2));
		CPPUNIT_ASSERT(a1 == t);
		CPPUNIT_ASSERT(!(a1 != t));
	}
	
	void serializeOperator()
	{
		Operator o1, o2;
		TreeNode *t;
		ByteStream b;
		
		t = &o2;
		
		CPPUNIT_ASSERT(o1 == o2);
		CPPUNIT_ASSERT(!(o1 != o2));
		CPPUNIT_ASSERT(o1 == t);
		CPPUNIT_ASSERT(!(o1 != t));
		
		o1.data("=");
		
		CPPUNIT_ASSERT(o1 != o2);
		CPPUNIT_ASSERT(!(o1 == o2));
		CPPUNIT_ASSERT(o1 != t);
		CPPUNIT_ASSERT(!(o1 == t));
		
		o1.serialize(b);
		
		CPPUNIT_ASSERT(o2.data() == "");
		o2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(o2.data() == "=");
		
		CPPUNIT_ASSERT(o1 == o2);
		CPPUNIT_ASSERT(!(o1 != o2));
		CPPUNIT_ASSERT(o1 == t);
		CPPUNIT_ASSERT(!(o1 != t));
	}
	
	void serializeFilter()
	{
		Filter f1, f2;
		TreeNode *t;
		ByteStream b;
		
		t = &f2;
		
		CPPUNIT_ASSERT(f1 == f2);
		CPPUNIT_ASSERT(!(f1 != f2));
		CPPUNIT_ASSERT(f1 == t);
		CPPUNIT_ASSERT(!(f1 != t));
		
		f1.data("Filter test");
		
		CPPUNIT_ASSERT(f1 != f2);
		CPPUNIT_ASSERT(!(f1 == f2));
		CPPUNIT_ASSERT(f1 != t);
		CPPUNIT_ASSERT(!(f1 == t));
		
		f1.serialize(b);
		
		CPPUNIT_ASSERT(f2.data() == "");
		f2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(f2.data() == "Filter test");
		
		CPPUNIT_ASSERT(f1 == f2);
		CPPUNIT_ASSERT(!(f1 != f2));
		CPPUNIT_ASSERT(f1 == t);
		CPPUNIT_ASSERT(!(f1 != t));
	}
	
	void serializeFunctionColumn()
	{
		FunctionColumn fc1, fc2;
		TreeNode *t;
		ByteStream b;
		
		t = &fc2;
		
		CPPUNIT_ASSERT(fc1 == fc2);
		CPPUNIT_ASSERT(!(fc1 != fc2));
		CPPUNIT_ASSERT(fc1 == t);
		CPPUNIT_ASSERT(!(fc1 != t));
		
		/* FunctionColumn */
		fc1.sessionID(0);
		fc1.functionName("FunctionColumn test");
		fc1.functionParms("tpch.region.r_regionkey, tpch.nation.n_nationkey");
		fc1.data("fc1");
		
		CPPUNIT_ASSERT(fc1 != fc2);
		CPPUNIT_ASSERT(!(fc1 == fc2));
		CPPUNIT_ASSERT(fc1 != t);
		CPPUNIT_ASSERT(!(fc1 == t));
		
		FunctionColumn::FunctionParm functionParms;
		functionParms = fc1.functionParms();
		CPPUNIT_ASSERT(functionParms.size() == 2);		
		
		fc1.serialize(b);
		
		CPPUNIT_ASSERT(fc2.functionName() == "");
		CPPUNIT_ASSERT(fc2.functionParms().size() == 0);
			
		fc2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(fc2.functionName() == "FunctionColumn test");
		CPPUNIT_ASSERT(fc2.functionParms().size() == 2);
		functionParms = fc2.functionParms();
		CPPUNIT_ASSERT(functionParms.size() == 2);	
		
		CPPUNIT_ASSERT(fc1 == fc2);
		CPPUNIT_ASSERT(!(fc1 != fc2));
		CPPUNIT_ASSERT(fc1 == t);
		CPPUNIT_ASSERT(!(fc1 != t));
	}
	
	void serializeConstantColumn()
	{
		ConstantColumn c1, c2;
		TreeNode *t;
		ByteStream b;
		
		t = &c2;
		
		CPPUNIT_ASSERT(c1 == c2);
		CPPUNIT_ASSERT(!(c1 != c2));
		CPPUNIT_ASSERT(c1 == t);
		CPPUNIT_ASSERT(!(c1 != t));
		
		c1.type(5);
		c1.constval("ConstantColumn test");
		c1.data("c1");
		
		CPPUNIT_ASSERT(c1 != c2);
		CPPUNIT_ASSERT(!(c1 == c2));
		CPPUNIT_ASSERT(c1 != t);
		CPPUNIT_ASSERT(!(c1 == t));
		
		c1.serialize(b);
		CPPUNIT_ASSERT(c2.constval() == "");
		c2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(c2.type() == 5);
		CPPUNIT_ASSERT(c2.constval() == "ConstantColumn test");
		
		CPPUNIT_ASSERT(c1 == c2);
		CPPUNIT_ASSERT(!(c1 != c2));
		CPPUNIT_ASSERT(c1 == t);
		CPPUNIT_ASSERT(!(c1 != t));
	}
	
	ParseTree* makeParseTree()
	{
		ParseTree *t[5];
		SimpleColumn *s[5];
		int i;
		
		/* ParseTree (ExpressionTree<TreeNode*>) */
		
		/*
		               t0(s0)
		                /  \
		              s1   s2
		              /      \
		            s3       s4
		*/

		for (i = 0; i < 5; i++) {
			t[i] = new ParseTree(NULL);
			s[i] = new SimpleColumn();
			t[i]->data(s[i]);
		}

		s[0]->schemaName("Schema Name 0");
		s[1]->schemaName("Schema Name 1");
		s[2]->schemaName("Schema Name 2");
		s[3]->schemaName("Schema Name 3");
		s[4]->schemaName("Schema Name 4");
		
		t[0]->left(t[1]);
		t[0]->right(t[2]);
		t[1]->left(t[3]);
		t[2]->right(t[4]);
		
		return t[0];
	}
	
	void verifyParseTree(const ParseTree* t)
	{
		const ParseTree *ct, *ct2;
		SimpleColumn *s;
		
		CPPUNIT_ASSERT(t != NULL);
		s = dynamic_cast<SimpleColumn*>(t->data());
		CPPUNIT_ASSERT(s != NULL);
		CPPUNIT_ASSERT(s->schemaName() == "Schema Name 0");
		ct = t->left();
		CPPUNIT_ASSERT(ct != NULL);
		s = dynamic_cast<SimpleColumn*>(ct->data());
		CPPUNIT_ASSERT(s != NULL);
		CPPUNIT_ASSERT(s->schemaName() == "Schema Name 1");
		ct2 = ct->left();
		CPPUNIT_ASSERT(ct2 != NULL);
		s = dynamic_cast<SimpleColumn*>(ct2->data());
		CPPUNIT_ASSERT(s != NULL);
		CPPUNIT_ASSERT(s->schemaName() == "Schema Name 3");
		CPPUNIT_ASSERT(ct->right() == NULL);
		CPPUNIT_ASSERT(ct2->left() == NULL);
		CPPUNIT_ASSERT(ct2->right() == NULL);
		ct = t->right();
		CPPUNIT_ASSERT(ct != NULL);
		s = dynamic_cast<SimpleColumn*>(ct->data());
		CPPUNIT_ASSERT(s != NULL);
		CPPUNIT_ASSERT(s->schemaName() == "Schema Name 2");
		ct2 = ct->right();
		CPPUNIT_ASSERT(ct2 != NULL);
		s = dynamic_cast<SimpleColumn*>(ct2->data());
		CPPUNIT_ASSERT(s != NULL);
		CPPUNIT_ASSERT(s->schemaName() == "Schema Name 4");
		CPPUNIT_ASSERT(ct->left() == NULL);
		CPPUNIT_ASSERT(ct2->left() == NULL);
		CPPUNIT_ASSERT(ct2->right() == NULL);
	}
	
	void serializeParseTree()
	{
		ByteStream b;
		ParseTree *t, *tmodel;
		
		t = makeParseTree();
		tmodel = makeParseTree();
		verifyParseTree(t);    //sanity check on the test itself
		
		CPPUNIT_ASSERT(*t == *tmodel);
		CPPUNIT_ASSERT(!(*t != *tmodel));
		
		ObjectReader::writeParseTree(t, b);
		
		delete t;
		
		t = ObjectReader::createParseTree(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(t != NULL);
		verifyParseTree(t);
		
		CPPUNIT_ASSERT(*t == *tmodel);
		CPPUNIT_ASSERT(!(*t != *tmodel));
		
		delete t;
		delete tmodel;
	}
	
	ArithmeticColumn* makeArithmeticColumn()
	{
		ArithmeticColumn *ret;
		ParseTree *t;
		
		t = makeParseTree();
		ret = new ArithmeticColumn();
		ret->expression(t);
		ret->alias("ArithmeticColumn");
		ret->data("AD");
		return ret;
	}
	
	void serializeArithmeticColumn()
	{
		ParseTree *t;
		const ParseTree *ct;
		ArithmeticColumn ac, ac2;
		TreeNode *tn;
		ByteStream b;
		
		tn = &ac2;
		
		CPPUNIT_ASSERT(ac == ac2);
		CPPUNIT_ASSERT(!(ac != ac2));
		CPPUNIT_ASSERT(ac == tn);
		CPPUNIT_ASSERT(!(ac != tn));
		
		t = makeParseTree();
		
		ac.expression(t);
		ac.alias("ArithmeticColumn");
		ac.data("AD");
		
		CPPUNIT_ASSERT(ac != ac2);
		CPPUNIT_ASSERT(!(ac == ac2));
		CPPUNIT_ASSERT(ac != tn);
		CPPUNIT_ASSERT(!(ac == tn));
		
		ac.serialize(b);
		ac2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		ct = ac2.expression();
		verifyParseTree(ct);
		CPPUNIT_ASSERT(ac2.alias() == "ArithmeticColumn");
		CPPUNIT_ASSERT(ac2.data() == "AD");
		
		CPPUNIT_ASSERT(ac == ac2);
		CPPUNIT_ASSERT(!(ac != ac2));
		CPPUNIT_ASSERT(ac == tn);
		CPPUNIT_ASSERT(!(ac != tn));
	}
	
	void serializeSimpleFilter()
	{
		ArithmeticColumn *pac1, *pac2;
		const ArithmeticColumn *cpac1, *cpac2;
		SimpleFilter sf1, sf2;
		Operator *o1;
		const Operator *co2;
		TreeNode *t;
		ByteStream b;
		
		t = &sf2;
		
		CPPUNIT_ASSERT(sf1 == sf2);
		CPPUNIT_ASSERT(!(sf1 != sf2));
		CPPUNIT_ASSERT(sf1 == t);
		CPPUNIT_ASSERT(!(sf1 != t));
		
		pac1 = makeArithmeticColumn();
		pac2 = makeArithmeticColumn();
		
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(pac1 != NULL);
		CPPUNIT_ASSERT(pac2 != NULL);
		
		o1 = new Operator("=");
		sf1.lhs(pac1);
		sf1.rhs(pac2);
		sf1.op(o1);
		
		sf1.serialize(b);
		
		CPPUNIT_ASSERT(sf1 != sf2);
		CPPUNIT_ASSERT(!(sf1 == sf2));
		CPPUNIT_ASSERT(sf1 != t);
		CPPUNIT_ASSERT(!(sf1 == t));
		
		sf2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		
		cpac1 = dynamic_cast<const ArithmeticColumn*>(sf2.lhs());
		CPPUNIT_ASSERT(cpac1 != NULL);
		verifyParseTree(cpac1->expression());
		cpac2 = dynamic_cast<const ArithmeticColumn*>(sf2.rhs());
		CPPUNIT_ASSERT(cpac2 != NULL);
		verifyParseTree(cpac2->expression());
		co2 = sf2.op();
		CPPUNIT_ASSERT(co2 != NULL);
		CPPUNIT_ASSERT(co2->data() == o1->data());
		
		CPPUNIT_ASSERT(sf1 == sf2);
		CPPUNIT_ASSERT(!(sf1 != sf2));
		CPPUNIT_ASSERT(sf1 == t);
		CPPUNIT_ASSERT(!(sf1 != t));
		
	}
	
	void serializeCSEP()
	{
	   /*
		* CalpontSelectExecutionPlan
		* This is a large class; it makes more sense to write == operators
		* for everything than to write a giant equivalance test here.
		* For now this is mostly a regression test.
		*/

		CalpontSelectExecutionPlan csep1, csep2;
		CalpontSelectExecutionPlan::ReturnedColumnList colList;
		ParseTree* filterList;
		CalpontExecutionPlan *cep;
		ByteStream b;
		
		cep = &csep2;
		
		CPPUNIT_ASSERT(csep1 == csep2);
		CPPUNIT_ASSERT(!(csep1 != csep2));
		CPPUNIT_ASSERT(csep1 == cep);
		CPPUNIT_ASSERT(!(csep1 != cep));
		
        // returned columns
		SimpleColumn *sc = new SimpleColumn("tpch.region.r_regionkey");
		colList.push_back(sc);
               
        // filters
		CalpontSelectExecutionPlan::Parser parser;
		std::vector<Token> tokens;
		Token t;
        
		SimpleFilter *sf = new SimpleFilter();
		SimpleColumn *lhs = new SimpleColumn(*sc);       
		SimpleColumn *rhs = new SimpleColumn("tpch.nation.n_regionkey");
		Operator *op = new Operator("=");
        
		sf->op(op);
		sf->lhs(lhs);
		sf->rhs(rhs);
        
		t.value = sf;
		tokens.push_back(t);
        
		Operator *op1 = new Operator ("and");
		t.value = op1;
		tokens.push_back(t);
        
		SimpleFilter *sf1 = new SimpleFilter();
		SimpleColumn *lhs1 = new SimpleColumn (*rhs);       
		ConstantColumn *constCol = new ConstantColumn("3", ConstantColumn::NUM);        
		Operator *op2 = new Operator("!=");
        
		sf1->op(op2);
		sf1->lhs(lhs1);
		sf1->rhs(constCol);

		t.value = sf1;
		tokens.push_back(t);
        
		filterList = parser.parse(tokens.begin(), tokens.end());
        
        // draw filterList tree
		filterList->drawTree("selectExecutionPlan_1.dot");
                     
        // calpont execution plan        
		csep1.returnedCols (colList);
		csep1.filters (filterList);
		
		CPPUNIT_ASSERT(csep1 != csep2);
		CPPUNIT_ASSERT(!(csep1 == csep2));
		CPPUNIT_ASSERT(csep1 != cep);
		CPPUNIT_ASSERT(!(csep1 == cep));
		
		csep1.serialize(b);
		csep2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		
		CPPUNIT_ASSERT(csep1 == csep2);
		CPPUNIT_ASSERT(!(csep1 != csep2));
		CPPUNIT_ASSERT(csep1 == cep);
		CPPUNIT_ASSERT(!(csep1 != cep));
		
		CalpontSelectExecutionPlan csep3, csep4;
        
        // subselect
		CalpontSelectExecutionPlan *subselect = new CalpontSelectExecutionPlan;
		subselect->location(CalpontSelectExecutionPlan::WHERE);
		subselect->dependent (false);
		CPPUNIT_ASSERT (subselect->location() == CalpontSelectExecutionPlan::WHERE);
		CPPUNIT_ASSERT (subselect->dependent() == false);
		CalpontSelectExecutionPlan::SelectList selectList;
		selectList.push_back(subselect);
		csep3.subSelects(selectList);
        
        // exist filter
		CalpontSelectExecutionPlan* cep1 = new CalpontSelectExecutionPlan();
		ExistsFilter *filter = new ExistsFilter();
		delete filter;
		filter = new ExistsFilter(cep1);        
		filter->exists(cep1);
		//CalpontSelectExecutionPlan* cep2 = const_cast<CalpontSelectExecutionPlan*>(filter->exists());

		CalpontSelectExecutionPlan::Parser parser1;
		std::vector<Token> tokens1;
		Token t1;
		t1.value = filter;
		tokens1.push_back(t1);
		csep3.filters(parser1.parse(tokens1.begin(), tokens1.end()));
		
		csep3.serialize(b);
		csep4.unserialize(b);
		
		CPPUNIT_ASSERT(csep3 == csep4);
		CPPUNIT_ASSERT(!(csep3 != csep4));
		
		
	}
	
	void serializeSelectFilter()
	{
		ByteStream b;
		ArithmeticColumn *pac1;
		Operator *o1;
		const Operator *co2;
		CalpontSelectExecutionPlan csep1;
		SelectFilter sel1, sel2;
		const ArithmeticColumn *cpac1;
		const ParseTree *ct;
		TreeNode *t;
		
		t = &sel2;
		
		CPPUNIT_ASSERT(sel1 == sel2);
		CPPUNIT_ASSERT(!(sel1 != sel2));
		CPPUNIT_ASSERT(sel1 == t);
		CPPUNIT_ASSERT(!(sel1 != t));
		
		pac1 = makeArithmeticColumn();
		o1 = new Operator("=");
		sel1.lhs(pac1);
		sel1.op(o1);
		
		CPPUNIT_ASSERT(sel1 != sel2);
		CPPUNIT_ASSERT(!(sel1 == sel2));
		CPPUNIT_ASSERT(sel1 != t);
		CPPUNIT_ASSERT(!(sel1 == t));
		
		sel1.serialize(b);
		sel2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		
		CPPUNIT_ASSERT(sel1 == sel2);
		CPPUNIT_ASSERT(!(sel1 != sel2));
		CPPUNIT_ASSERT(sel1 == t);
		CPPUNIT_ASSERT(!(sel1 != t));
		
		cpac1 = dynamic_cast<const ArithmeticColumn*>(sel2.lhs());
		CPPUNIT_ASSERT(cpac1 != NULL);
		ct = cpac1->expression();
		verifyParseTree(ct);
		co2 = sel2.op();
		CPPUNIT_ASSERT(co2 != NULL);
		CPPUNIT_ASSERT(co2->data() == o1->data());
	}
	
	/* ExistFilters get tested as part of the CSEP test at the moment. */
	void serializeExistsFilter()
	{
		ExistsFilter ef1, ef2;
		ByteStream b;
		
		ef2.data("ExistsFilter test");
		ef1.serialize(b);
		ef2.unserialize(b);
		CPPUNIT_ASSERT(b.length() == 0);
		CPPUNIT_ASSERT(ef2.data() == "");
	}

	void objectIDManager_1()
	{
		int oid, oidBase;

		// fake out the objmgr...
		setenv("CALPONT_CONFIG_FILE", "/usr/local/Calpont/etc/Calpont.xml", 1);
		Config* cf = Config::makeConfig();
		cf->setConfig("OIDManager", "OIDBitmapFile", "./oidbitmap");
		
		try {
			ObjectIDManager o;
			
			OIDBitmapFilename = strdup(o.getFilename().c_str());
			oidBase = o.allocOID();
			oid = o.allocOID();
			CPPUNIT_ASSERT(oid == oidBase+1);
			oid = o.allocOIDs(20);
			CPPUNIT_ASSERT(oid == oidBase+2);
			oid = o.allocOIDs(20);
			CPPUNIT_ASSERT(oid == oidBase+22);
			o.returnOID(oidBase+5);
			oid = o.allocOID();
			CPPUNIT_ASSERT(oid == oidBase+5);
			o.returnOID(oidBase+5);
			oid = o.allocOIDs(20);
			CPPUNIT_ASSERT(oid == oidBase+42);
			oid = o.allocOID();
			CPPUNIT_ASSERT(oid == oidBase+5);
			o.returnOIDs(oidBase, oidBase+61);
			oid = o.allocOID();
			CPPUNIT_ASSERT(oid == oidBase);
			o.returnOID(0);
		}
		catch(...) {
			if (OIDBitmapFilename != NULL)
				unlink(OIDBitmapFilename);   // XXXPAT: fix this when libstdc++ regains its sanity
			throw;
		}
		unlink(OIDBitmapFilename);
	}
	
	/*
	 * destroySemaphores() and destroyShmseg() will print error messages
	 * if there are no objects to destroy.  That's OK.
	 */
	void destroySemaphores()
	{
		key_t semkey;
		int sems, err;
		
		semkey = SESSIONMANAGER_SYSVKEY;
		sems = semget(semkey, 2, 0666);
		if (sems != -1) {
			err = semctl(sems, 0, IPC_RMID);
			if (err == -1)
				perror("tdriver: semctl");
		}
	}
	
	void destroyShmseg()
	{
		key_t shmkey;
		int shms, err;
		
		shmkey = SESSIONMANAGER_SYSVKEY;
		shms = shmget(shmkey, 0, 0666);
		if (shms != -1) {
			err = shmctl(shms, IPC_RMID, NULL);
			if (err == -1 && errno != EINVAL) {
				perror("tdriver: shmctl");
				return;
			}
		}
	}
	
	void sessionManager_1()
	{
		SessionManager *sm = NULL;
		SessionManager::TxnID txn;
		const SessionManager::SIDTIDEntry* activeTxns;
		int len;
		string filename;
		
// 		destroySemaphores();
// 		destroyShmseg();
		
		try {
			sm = new SessionManager();
			//CPPUNIT_ASSERT(sm->verID() == 0);
			filename = sm->getTxnIDFilename();
			delete sm;
			sm = new SessionManager();
			txn = sm->newTxnID(0);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 1);
// 			CPPUNIT_ASSERT(sm->verID() == 1);
			txn = sm->newTxnID(1);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 2);
// 			CPPUNIT_ASSERT(sm->verID() == 2);
			activeTxns = sm->SIDTIDMap(len);
			CPPUNIT_ASSERT(activeTxns != NULL);
			CPPUNIT_ASSERT(len == 2);
			txn = sm->getTxnID(0);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 1);
			CPPUNIT_ASSERT(txn.valid == activeTxns[0].txnid.valid);
// 			CPPUNIT_ASSERT(txn.id == activeTxns[0].txnid.id);
			txn = sm->getTxnID(1);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 2);
			CPPUNIT_ASSERT(txn.valid == activeTxns[1].txnid.valid);
// 			CPPUNIT_ASSERT(txn.id == activeTxns[1].txnid.id);
			delete [] activeTxns;
			
			//make sure it's consistent across invocations
			delete sm;
			sm = new SessionManager();
			activeTxns = sm->SIDTIDMap(len);
			CPPUNIT_ASSERT(activeTxns != NULL);
			CPPUNIT_ASSERT(len == 2);
			txn = sm->getTxnID(0);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 1);
			CPPUNIT_ASSERT(txn.valid == activeTxns[0].txnid.valid);
// 			CPPUNIT_ASSERT(txn.id == activeTxns[0].txnid.id);
			txn = sm->getTxnID(1);
			CPPUNIT_ASSERT(txn.valid == true);
// 			CPPUNIT_ASSERT(txn.id == 2);
			CPPUNIT_ASSERT(txn.valid == activeTxns[1].txnid.valid);
// 			CPPUNIT_ASSERT(txn.id == activeTxns[1].txnid.id);
			sm->rolledback(txn);
			CPPUNIT_ASSERT(txn.valid == false);
			txn = sm->getTxnID(0);
			sm->committed(txn);
			CPPUNIT_ASSERT(txn.valid == false);
			delete [] activeTxns;
			activeTxns = sm->SIDTIDMap(len);
			CPPUNIT_ASSERT(len == 0);
			delete [] activeTxns;			
		}
		catch(runtime_error &e) {
			cout << "caught runtime_error (why doesn't cppunit notice these?): " << e.what() << endl;
			if (sm != NULL)
				delete sm;
// 			destroySemaphores();
// 			destroyShmseg();
			throw logic_error("Hey!  Stop!");
		}
		catch(exception &e) {
			cout << "caught exception: " << e.what() << endl;
			if (sm != NULL)
				delete sm;
// 			destroySemaphores();
// 			destroyShmseg();
			throw;
		}
		
		delete sm;		
// 		destroySemaphores();
// 		destroyShmseg();
	}
	
	/** Verifies that we can only have MaxTxns (1000 right now) active transactions at 
	any given time */
	void sessionManager_2() {
		int i;
		SessionManager *sm;
		SessionManager::TxnID txns[1001];
		string filename;
		
// 		destroySemaphores();
// 		destroyShmseg();
		
		sm = new SessionManager();
		filename = sm->getTxnIDFilename();
		delete sm;
		sm = new SessionManager();		

		for (i = 0; i < 1000; i++) {
			txns[i] = sm->newTxnID(i, false);
			CPPUNIT_ASSERT(txns[i].valid == true);
			//CPPUNIT_ASSERT(sm->verID() == txns[i].id);
		}
		txns[1000] = sm->newTxnID(i, false);
		CPPUNIT_ASSERT(txns[1000].valid == false);

		for (i = 999; i >= 0; i--) {
			SessionManager::TxnID tmp = sm->getTxnID(i);
			CPPUNIT_ASSERT(tmp.valid == txns[i].valid == true);
			CPPUNIT_ASSERT(tmp.id == txns[i].id);
			sm->committed(txns[i]);
			tmp = sm->getTxnID(i);
			CPPUNIT_ASSERT(tmp.valid == false);
			CPPUNIT_ASSERT(txns[i].valid == false);
		}
		
		try {
			sm->committed(txns[1000]);
		}
		// expected exception
		catch(invalid_argument& e)
		{ }
	
		delete sm;
// 		destroySemaphores();
// 		destroyShmseg();
	}

	/** Verifies that transaction IDs get saved and restored correctly across "reboots" */
	void sessionManager_3()
	{
	
		SessionManager *sm;
		string filename;
		SessionManager::TxnID txnid;

		// scrub env
		sm = new SessionManager();
		filename = sm->getTxnIDFilename();
		delete sm;
// 		destroyShmseg();
// 		destroySemaphores();

		sm = new SessionManager();

		txnid = sm->newTxnID(0);
//   		CPPUNIT_ASSERT(txnid.id == 1);

		delete sm;
// 		destroyShmseg();
// 		destroySemaphores();

		sm = new SessionManager();
		sm->committed(txnid);
		txnid = sm->newTxnID(1);
//   		CPPUNIT_ASSERT(txnid.id == 2);

		delete sm;
		sm = new SessionManager();
		sm->committed(txnid);

// 		destroyShmseg();
// 		destroySemaphores();

	}

	void sessionManager_4()
	{
		char *buf;
		int len;
		SessionManager sm;

		buf = sm.getShmContents(len);
		CPPUNIT_ASSERT(len > 0);
		CPPUNIT_ASSERT(buf != NULL);
		delete [] buf;
	}


	SessionManager* manager;
	SessionManager::TxnID managerTxns[1000];

int createTxns(const int& start, const int& end) {

	const int first=start;
	const int last=end;
	int newTxns=0;
	int verifyLen=0;

	verifyLen=manager->verifySize();
	for (int idx = first; idx<last && verifyLen<maxNewTxns; idx++)
	{
		managerTxns[idx] = manager->newTxnID((uint32_t)idx+1000);
		CPPUNIT_ASSERT(managerTxns[idx].id>0);
		CPPUNIT_ASSERT(managerTxns[idx].valid==true);
		verifyLen=manager->verifySize();
		newTxns++;
	}

	CPPUNIT_ASSERT(newTxns==last-first);
	return newTxns;
}

int closeTxns(const int& start, const int& end) {

	int first=start;
	int last=end;
	int totalClosed=0;

	for (int idx=first; idx<last ; idx++)
	{
		try
		{
			SessionManager::TxnID tmp = manager->getTxnID(idx+1000);
			if (tmp.valid == true)
			{
				manager->committed(tmp);
				CPPUNIT_ASSERT(tmp.valid==false);
				totalClosed++;
			}
			
		}
		catch (exception& e)
		{
			cerr << e.what() << endl;
			continue;
		}
	}
	return totalClosed;

} //closeTxns

void MonitorTestPlan_1() {

	int currStartTxn=0;
	int currEndTxn=5;
	int txnCntIncr=5;
	const int sleepTime=11;
	const int iterMax=1;
	vector<SessionMonitor::MonSIDTIDEntry*> toTxns;

	destroySemaphores();
	destroyShmseg();

	manager = new SessionManager();
	manager->reset();
	CPPUNIT_ASSERT(manager->verifySize()==0);

	SessionMonitor* monitor = NULL;
	for(int jdx=0; jdx<iterMax; jdx++) {

		// store the current state of the SessionManager
		monitor = new SessionMonitor();
		delete monitor;
		int idx=0;
		int grpStart=currStartTxn;
		for (idx=0; idx < 3; idx++ ) {

			createTxns(currStartTxn, currEndTxn);
			CPPUNIT_ASSERT(manager->verifySize()==(idx+1)*txnCntIncr);

			currStartTxn+=txnCntIncr;
			currEndTxn+=txnCntIncr;
			sleep(sleepTime); //make sessions time out

			monitor = new SessionMonitor(); // read Monitor data
    		toTxns.clear();
			toTxns = monitor->timedOutTxns(); // get timed out txns
			CPPUNIT_ASSERT(toTxns.size()==(uint32_t)txnCntIncr*idx);

			delete monitor;
		}

		int grpEnd=currEndTxn;
		monitor = new SessionMonitor();
		closeTxns(grpStart, grpEnd); // close this iteration of txns
		CPPUNIT_ASSERT(manager->verifySize()==0);
		toTxns = monitor->timedOutTxns(); // get timed out txns
		CPPUNIT_ASSERT(toTxns.size()==0);

		delete monitor;

	}

	monitor = new SessionMonitor(); // readload Monitor data

    toTxns.clear();
	toTxns = monitor->timedOutTxns(); // get timed out txns
	CPPUNIT_ASSERT(toTxns.size()==0);
	delete monitor;

	CPPUNIT_ASSERT(manager->verifySize()==0);
	if (manager)
		delete manager;

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


