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

/******************************************************************************************
* $Id: tpchschema.cpp 2101 2013-01-21 14:12:52Z rdempsey $
*
******************************************************************************************/
#include <boost/timer.hpp>

#include "dbbuilder.h"
#include "tpchschema.h"
#include "ddlpkg.h"
#include "sqlparser.h"
#include "calpontsystemcatalog.h"
using namespace ddlpackage;
using namespace ddlpackageprocessor;
using namespace execplan;
using namespace std;

void TpchSchema::build()
{
    cout << "Creating tpch database..." << endl;
    cout << endl;
    cout << "create table tpch.region" << endl;
    cout << "---------------------------------------" << endl;
    std::string createStmt = "create table tpch.region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.nation" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.nation( n_nationkey integer NOT NULL , n_name char(25) ,n_regionkey integer NOT NULL,n_comment varchar(152))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.customer" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.customer(c_custkey integer NOT NULL, c_name varchar(25) ,c_address varchar(40),    c_nationkey integer ,c_phone char(15) ,c_acctbal number(9,2) , c_mktsegment char(10) , c_comment varchar(117))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.orders" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.orders(o_orderkey integer NOT NULL, o_custkey integer , o_orderstatus char(1),    o_totalprice number(9,2) , o_orderdate date , o_orderpriority char(15) , o_clerk char(15), o_shippriority integer,o_comment varchar(79))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.part" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.part( p_partkey integer NOT NULL ,p_name varchar(55) , p_mfgr char(25), p_brand char(10) , p_type varchar(25) , p_size integer , p_container char(10) ,p_retailprice number(9,2) , p_comment varchar(23))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.supplier" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.supplier( s_suppkey integer NOT NULL, s_name char(25) , s_address varchar(40),    s_nationkey integer , s_phone char(15) , s_acctbal number(9,2), s_comment varchar(101))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.partsupp" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.partsupp( ps_partkey integer NOT NULL, ps_suppkey integer,ps_availqty integer , ps_supplycost number(9,2) , ps_comment varchar(199))";
    buildTable(createStmt);
    
    cout << endl;
    cout << "create table tpch.lineitem" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.lineitem( l_orderkey integer NOT NULL , l_partkey integer NOT NULL,l_suppkey integer NOT NULL, l_linenumber integer NOT NULL, l_quantity number(9,2) NOT NULL,l_extendedprice number(9,2) NOT NULL, l_discount number(9,2) NOT NULL, l_tax number(9,2) NOT NULL, l_returnflag char(1) , l_linestatus char(1) , l_shipdate date , l_commitdate date,l_receiptdate date , l_shipinstruct char(25), l_shipmode char(10),l_comment varchar(44))";
    buildTable(createStmt);

	cout << endl;
#if 0
    cout << "create index l_partkey_idx ON tpch.lineitem(l_partkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_partkey_idx ON tpch.lineitem(l_partkey)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)";
    createindex(createStmt);

	cout << endl;
    cout << "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index o_orderkey_idx ON tpch.orders(o_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index o_orderkey_idx ON tpch.orders(o_orderkey)";
    createindex(createStmt);
	
	cout << endl;
    cout << "create index p_partkey_idx ON tpch.part(p_partkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index p_partkey_idx ON tpch.part(p_partkey)";
    createindex(createStmt);
    
    cout << endl;
#endif
    cout << "Finished creating tpch database" << endl;
}

void TpchSchema::buildTpchTables(std::string schema)
{
    cout << "Creating tpch database..." << endl;
    cout << endl;
    cout << "create table " << schema << ".region" << endl;
    cout << "---------------------------------------" << endl;
    std::string createStart("create table ");
    std::string createFirst = createStart + schema;
    std::string createStmt = ".region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".nation" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".nation( n_nationkey integer NOT NULL , n_name char(25) ,n_regionkey integer NOT NULL,n_comment varchar(152))";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".customer" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".customer(c_custkey integer NOT NULL, c_name varchar(25) ,c_address varchar(40),    c_nationkey integer ,c_phone char(15) ,c_acctbal number(9,2) , c_mktsegment char(10) , c_comment varchar(117))";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".orders" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".orders(o_orderkey integer NOT NULL, o_custkey integer , o_orderstatus char(1),    o_totalprice number(9,2) , o_orderdate date , o_orderpriority char(15) , o_clerk char(15), o_shippriority integer,o_comment varchar(79))";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".part" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".part( p_partkey integer NOT NULL ,p_name varchar(55) , p_mfgr char(25), p_brand char(10) , p_type varchar(25) , p_size integer , p_container char(10) ,p_retailprice number(9,2) , p_comment varchar(23))";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".supplier" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".supplier( s_suppkey integer NOT NULL, s_name char(25) , s_address varchar(40),    s_nationkey integer , s_phone char(15) , s_acctbal number(9,2), s_comment varchar(101))";
    buildTable(createFirst+createStmt);

    cout << endl;
    cout << "create table " << schema << ".partsupp" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".partsupp( ps_partkey integer NOT NULL, ps_suppkey integer,ps_availqty integer , ps_supplycost number(9,2) , ps_comment varchar(199))";
    buildTable(createFirst+createStmt);
    
    cout << endl;
    cout << "create table " << schema << ".lineitem" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = ".lineitem( l_orderkey integer NOT NULL , l_partkey integer NOT NULL,l_suppkey integer NOT NULL, l_linenumber integer NOT NULL, l_quantity number(9,2) NOT NULL,l_extendedprice number(9,2) NOT NULL, l_discount number(9,2) NOT NULL, l_tax number(9,2) NOT NULL, l_returnflag char(1) , l_linestatus char(1) , l_shipdate date , l_commitdate date,l_receiptdate date , l_shipinstruct char(25), l_shipmode char(10),l_comment varchar(44))";
    buildTable(createFirst+createStmt);

	cout << endl;
#if 0
	createStart = "create index ";
	std::string createIndex("l_partkey_idx");
	std::string on(" ON ");
  createFirst = createStart + createIndex + on + schema;
  cout << "create index l_partkey_idx ON " << schema << ".lineitem(l_partkey)" << endl;
  cout << "---------------------------------------" << endl;
  createStmt = ".lineitem(l_partkey)";
  createindex(createFirst+createStmt);
    
	cout << endl;
	createIndex = "l_suppkey_idx";
	createFirst = createStart + createIndex + on + schema;
  cout << "create index l_suppkey_idx ON " << schema << ".lineitem(l_suppkey)" << endl;
  cout << "---------------------------------------" << endl;
  createStmt = ".lineitem(l_suppkey)";
    createindex(createFirst+createStmt);

	cout << endl;
	createIndex = "l_orderkey_idx";
	createFirst = createStart + createIndex + on + schema;
  cout << "create index l_orderkey_idx ON " << schema << ".lineitem(l_orderkey)" << endl;
  cout << "---------------------------------------" << endl;
  createStmt = ".lineitem(l_orderkey)";
  createindex(createFirst+createStmt);
            
	cout << endl;
	createIndex = "o_orderkey_idx";
	createFirst = createStart + createIndex + on + schema;
  cout << "create index o_orderkey_idx ON " << schema << ".orders(o_orderkey)" << endl;
  cout << "---------------------------------------" << endl;
  createStmt = ".orders(o_orderkey)";
  createindex(createFirst+createStmt);
	
	cout << endl;
	createIndex = "p_partkey_idx";
	createFirst = createStart + createIndex + on + schema;
  	cout << "create index p_partkey_idx ON " << schema << ".part(p_partkey)" << endl;
  	cout << "---------------------------------------" << endl;
  	createStmt = ".part(p_partkey)";
  	createindex(createFirst+createStmt);
	
    cout << endl;
#endif
    cout << "Finished creating tpch database" << endl;
}

void TpchSchema::buildMultiColumnIndex()
{
    cout << "Creating tpch database..." << endl;
    cout << endl;
    cout << "create table tpch.region" << endl;
    cout << "---------------------------------------" << endl;
    std::string createStmt = "create table tpch.region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.nation" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.nation( n_nationkey integer NOT NULL , n_name char(25) ,n_regionkey integer NOT NULL,n_comment varchar(152))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.customer" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.customer(c_custkey integer NOT NULL, c_name varchar(25) ,c_address varchar(40),    c_nationkey integer ,c_phone char(15) ,c_acctbal number(9,2) , c_mktsegment char(10) , c_comment varchar(117))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.orders" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.orders(o_orderkey integer NOT NULL, o_custkey integer , o_orderstatus char(1),    o_totalprice number(9,2) , o_orderdate date , o_orderpriority char(15) , o_clerk char(15), o_shippriority integer,o_comment varchar(79))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.part" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.part( p_partkey integer NOT NULL ,p_name varchar(55) , p_mfgr char(25), p_brand char(10) , p_type varchar(25) , p_size integer , p_container char(10) ,p_retailprice number(9,2) , p_comment varchar(23), PRIMARY KEY(p_partkey))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.supplier" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.supplier( s_suppkey integer NOT NULL, s_name char(25) , s_address varchar(40),    s_nationkey integer , s_phone char(15) , s_acctbal number(9,2), s_comment varchar(101))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.partsupp" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.partsupp( ps_partkey integer NOT NULL, ps_suppkey integer,ps_availqty integer , ps_supplycost number(9,2) , ps_comment varchar(199))";
    buildTable(createStmt);
    
    cout << endl;
    cout << "create table tpch.lineitem" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.lineitem( l_orderkey integer NOT NULL , l_partkey integer NOT NULL,l_suppkey integer NOT NULL, l_linenumber integer NOT NULL, l_quantity number(9,2) NOT NULL,l_extendedprice number(9,2) NOT NULL, l_discount number(9,2) NOT NULL, l_tax number(9,2) NOT NULL, l_returnflag char(1) , l_linestatus char(1) , l_shipdate date , l_commitdate date,l_receiptdate date , l_shipinstruct char(25), l_shipmode char(10),l_comment varchar(44))";
    buildTable(createStmt);
	
	cout << endl;
#if 0
    cout << "create index l_partkey_idx ON tpch.lineitem(l_partkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_partkey_idx ON tpch.lineitem(l_partkey)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)";
    createindex(createStmt);

	cout << endl;
    cout << "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)";
    createindex(createStmt);
    
   	cout << endl;
    cout << "create index TestMulticolKey ON tpch.lineitem(l_orderkey,l_linenumber )" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index TestMulticolKey ON tpch.lineitem(l_orderkey, l_linenumber)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index o_orderkey_idx ON tpch.orders(o_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index o_orderkey_idx ON tpch.orders(o_orderkey)";
    createindex(createStmt);

    cout << endl;
#endif
    cout << "Finished creating tpch database" << endl;
}

void TpchSchema::buildTable()
{
    cout << "Creating tpch database..." << endl;
    cout << endl;
    cout << "create table tpch.region" << endl;
    cout << "---------------------------------------" << endl;
    std::string createStmt = "create table tpch.region( r_regionkey integer NOT NULL, r_name char(25) ,r_comment varchar(152));";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.nation" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.nation( n_nationkey integer NOT NULL , n_name char(25) ,n_regionkey integer NOT NULL,n_comment varchar(152))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.customer" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.customer(c_custkey integer NOT NULL, c_name varchar(25) ,c_address varchar(40),    c_nationkey integer ,c_phone char(15) ,c_acctbal number(9,2) , c_mktsegment char(10) , c_comment varchar(117))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.orders" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.orders(o_orderkey integer NOT NULL, o_custkey integer , o_orderstatus char(1),    o_totalprice number(9,2) , o_orderdate date , o_orderpriority char(15) , o_clerk char(15), o_shippriority integer,o_comment varchar(79))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.part" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.part( p_partkey integer NOT NULL ,p_name varchar(55) , p_mfgr char(25), p_brand char(10) , p_type varchar(25) , p_size integer , p_container char(10) ,p_retailprice number(9,2) , p_comment varchar(23), PRIMARY KEY(p_partkey))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.supplier" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.supplier( s_suppkey integer NOT NULL, s_name char(25) , s_address varchar(40),    s_nationkey integer , s_phone char(15) , s_acctbal number(9,2), s_comment varchar(101))";
    buildTable(createStmt);

    cout << endl;
    cout << "create table tpch.partsupp" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.partsupp( ps_partkey integer NOT NULL, ps_suppkey integer,ps_availqty integer , ps_supplycost number(9,2) , ps_comment varchar(199))";
    buildTable(createStmt);
    
    cout << endl;
    cout << "create table tpch.lineitem" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create table tpch.lineitem( l_orderkey integer NOT NULL , l_partkey integer NOT NULL,l_suppkey integer NOT NULL, l_linenumber integer NOT NULL, l_quantity number(9,2) NOT NULL,l_extendedprice number(9,2) NOT NULL, l_discount number(9,2) NOT NULL, l_tax number(9,2) NOT NULL, l_returnflag char(1) , l_linestatus char(1) , l_shipdate date , l_commitdate date,l_receiptdate date , l_shipinstruct char(25), l_shipmode char(10),l_comment varchar(44))";
    buildTable(createStmt);

    cout << endl;
    cout << "Finished creating tpch database" << endl;
}
void TpchSchema::buildTable(string createText)
{
	cout << endl;
	cout << createText << endl;

	boost::timer theTimer;
    ddlpackage::SqlParser parser;
    parser.Parse(createText.c_str());
    if (parser.Good())
    {
        const ddlpackage::ParseTree &ptree = parser.GetParseTree();
        try
        {
			cout << ptree << endl;	
            ddlpackageprocessor::CreateTableProcessor processor;
            processor.setDebugLevel(ddlpackageprocessor::CreateTableProcessor::VERBOSE);

            ddlpackage::SqlStatement &stmt = *ptree.fList[0];
            ddlpackageprocessor::CreateTableProcessor::DDLResult result;
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
        		CalpontSystemCatalog::makeCalpontSystemCatalog( 1 );
            result  = processor.processPackage(dynamic_cast<ddlpackage::CreateTableStatement&>(stmt));
            systemCatalogPtr->removeCalpontSystemCatalog(1);
            std::cout << "return: " << result.result << std::endl;
        }
        catch(...)
        {

            throw;
        }
    }
	cout << "Create table took :" << theTimer.elapsed() << " seconds to complete." << endl;
}

#if 0
void TpchSchema::createindex(string createText)
{
	cout << endl;
	cout << createText << endl;

	boost::timer theTimer;
    ddlpackage::SqlParser parser;
    parser.Parse(createText.c_str());
    if (parser.Good())
    {
        const ddlpackage::ParseTree &ptree = parser.GetParseTree();
        try
            {
                CreateIndexProcessor processor;
                //processor.setDebugLevel(CreateIndexProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                CreateIndexProcessor::DDLResult result;
				boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
        		CalpontSystemCatalog::makeCalpontSystemCatalog( 1 );
                result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));
                systemCatalogPtr->removeCalpontSystemCatalog(1);
                std::cout << "return: " << result.result << std::endl;
            }
        catch(...)
        {

            throw;
        }
    }
	cout << "Create index took :" << theTimer.elapsed() << " seconds to complete." << endl;
}

void TpchSchema::buildIndex()
{
    std::string createStmt;
    
	cout << endl;
    cout << "create index l_partkey_idx ON tpch.lineitem(l_partkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_partkey_idx ON tpch.lineitem(l_partkey)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_suppkey_idx ON tpch.lineitem(l_suppkey)";
    createindex(createStmt);

	cout << endl;
    cout << "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index l_orderkey_idx ON tpch.lineitem(l_orderkey)";
    createindex(createStmt);
    
	cout << endl;
    cout << "create index o_orderkey_idx ON tpch.orders(o_orderkey)" << endl;
    cout << "---------------------------------------" << endl;
    createStmt = "create index o_orderkey_idx ON tpch.orders(o_orderkey)";
    createindex(createStmt);

    cout << endl;
    cout << "Finished creating tpch database" << endl;
}
#endif
