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

//  $Id: jlf_execplantojoblist.cpp 9702 2013-07-17 19:08:07Z xlou $


#include "jlf_execplantojoblist.h"

#include <iostream>
#include <stack>
#include <iterator>
#include <algorithm>
#include <cassert>
#include <vector>
#include <set>
#include <map>
#include <climits>
#include <cmath>
using namespace std;

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/split.hpp>
namespace ba=boost::algorithm;

#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "dbrm.h"
#include "filter.h"
#include "simplefilter.h"
#include "outerjoinonfilter.h"
#include "constantfilter.h"
#include "existsfilter.h"
#include "selectfilter.h"
#include "windowfunctioncolumn.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "pseudocolumn.h"
#include "simplecolumn.h"
#include "simplecolumn_int.h"
#include "simplecolumn_uint.h"
#include "simplecolumn_decimal.h"
#include "returnedcolumn.h"
#include "treenodeimpl.h"
#include "calpontsystemcatalog.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "simplescalarfilter.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "elementtype.h"
#include "joblist.h"
#include "jobstep.h"
#include "primitivestep.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "expressionstep.h"
#include "tupleconstantstep.h"

#include "jlf_common.h"
#include "jlf_subquery.h"
#include "jlf_tuplejoblist.h"


namespace
{
using namespace joblist;

const string boldStart = "\033[0;1m";
const string boldStop = "\033[0;39m";

typedef stack<const TreeNode*> TreeNodeStack;
//@bug 598 self-join
//typedef std::pair<execplan::CalpontSystemCatalog::OID, std::string> OIDAliasPair;  //self-join

const Operator opeq("=");
const Operator oplt("<");
const Operator ople("<=");
const Operator opgt(">");
const Operator opge(">=");
const Operator opne("<>");
const Operator opand("and");
const Operator opAND("AND");
const Operator opor("or");
const Operator opOR("OR");
const Operator opxor("xor");
const Operator opXOR("XOR");
const Operator oplike("like");
const Operator opLIKE("LIKE");
const Operator opis("is");
const Operator opIS("IS");
const Operator opisnot("is not");
const Operator opISNOT("IS NOT");
const Operator opnotlike("not like");
const Operator opNOTLIKE("NOT LIKE");
const Operator opisnotnull("isnotnull");
const Operator opisnull("isnull");


const JobStepVector doSimpleFilter(SimpleFilter* sf, JobInfo& jobInfo);


/* This looks like an inefficient way to get NULL values. Much easier ways
   to do it. */
int64_t valueNullNum(const CalpontSystemCatalog::ColType& ct)
{
	int64_t n = 0;
	bool pushWarning = false;
	boost::any anyVal = DataConvert::convertColumnData(ct, "", pushWarning, true);

	switch (ct.colDataType)
	{
	case CalpontSystemCatalog::BIT:
		//n = boost::any_cast<bool>(anyVal);
		break;
	case CalpontSystemCatalog::TINYINT:
		n = boost::any_cast<char>(anyVal);
		break;
	case CalpontSystemCatalog::UTINYINT:
		n = boost::any_cast<uint8_t>(anyVal);
		break;
	case CalpontSystemCatalog::SMALLINT:
		n = boost::any_cast<short>(anyVal);
		break;
    case CalpontSystemCatalog::USMALLINT:
        n = boost::any_cast<uint16_t>(anyVal);
        break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
		n = boost::any_cast<int>(anyVal);
		break;
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
        n = boost::any_cast<uint32_t>(anyVal);
        break;
	case CalpontSystemCatalog::BIGINT:
		n = boost::any_cast<long long>(anyVal);
		break;
    case CalpontSystemCatalog::UBIGINT:
        n = boost::any_cast<uint64_t>(anyVal);
        break;
	case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
		{
			float f = boost::any_cast<float>(anyVal);
			//N.B. There is a bug in boost::any or in gcc where, if you store a nan, you will get back a nan,
			//  but not necessarily the same bits that you put in. This only seems to be for float (double seems
			//  to work).
			if (isnan(f))
			{
				uint32_t ti = joblist::FLOATNULL;
				float* tfp = (float*)&ti;
				f = *tfp;
			}
			float* fp = &f;
			int32_t* ip = reinterpret_cast<int32_t*>(fp);
		  	n = *ip;
		}
		break;
	case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
		{
			double d = boost::any_cast<double>(anyVal);
			double* dp = &d;
			int64_t* ip = reinterpret_cast<int64_t*>(dp);
		  	n = *ip;
		}
		break;
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::VARBINARY:
	case CalpontSystemCatalog::BLOB:
	case CalpontSystemCatalog::CLOB:
	case CalpontSystemCatalog::CHAR:

		// @bug 532 - where is null was not returning null rows for char(1) - char(8).
		// @Bug 972 Varchar should be treated differently from char
		if ( (ct.colDataType == CalpontSystemCatalog::VARCHAR && ct.colWidth <= 7) ||
			(ct.colDataType == CalpontSystemCatalog::VARBINARY && ct.colWidth <= 7) ||
			(ct.colDataType == CalpontSystemCatalog::CHAR && ct.colWidth <= 8) )
		{
			const string &i = boost::any_cast<string>(anyVal);
			//n = *((uint64_t *) i.c_str());
			/* this matches what dataconvert is returning; not valid to copy
			 * 8 bytes every time. */
			if (ct.colDataType == CalpontSystemCatalog::CHAR) {
				switch (ct.colWidth) {
					case 1: n = *((uint8_t *) i.data()); break;
					case 2: n = *((uint16_t *) i.data()); break;
					case 3:
					case 4: n = *((uint32_t *) i.data()); break;
					default: n = *((uint64_t *) i.data()); break;
				}
			}
			else {
				switch (ct.colWidth) {
					case 1: n = *((uint16_t *) i.data()); break;
					case 2: n = *((uint32_t *) i.data()); break;
					default: n = *((uint64_t *) i.data()); break;
				}
			}
			
		}
		else
		{
			WriteEngine::Token t = boost::any_cast<WriteEngine::Token>(anyVal);
			n = *(uint64_t*)&t;
		}
		break;

	case CalpontSystemCatalog::DATE:
		n = boost::any_cast<uint32_t>(anyVal);
		break;
	case CalpontSystemCatalog::DATETIME:
		n = boost::any_cast<uint64_t>(anyVal);
		break;
	case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
		if (ct.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
			n = boost::any_cast<char>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
			n = boost::any_cast<short>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::FOUR_BYTE)
			n = boost::any_cast<int>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::EIGHT_BYTE)
			n = boost::any_cast<long long>(anyVal);
		else
			n = 0xfffffffffffffffeLL;
		break;
	default:
		break;
	}

	return n;
}

int64_t convertValueNum(const string& str, const CalpontSystemCatalog::ColType& ct, bool isNull, uint8_t& rf)
{
	if (str.size() == 0 || isNull ) return valueNullNum(ct);

	int64_t v = 0;
	rf = 0;
	bool pushWarning = false;
	boost::any anyVal = DataConvert::convertColumnData(ct, str, pushWarning, false, true);

	switch (ct.colDataType)
	{
	case CalpontSystemCatalog::BIT:
		v = boost::any_cast<bool>(anyVal);
		break;
	case CalpontSystemCatalog::TINYINT:
#if BOOST_VERSION >= 105200
		v = boost::any_cast<char>(anyVal);
#else
		v = boost::any_cast<int8_t>(anyVal);
#endif
		break;
    case CalpontSystemCatalog::UTINYINT:
        v = boost::any_cast<uint8_t>(anyVal);
        break;
	case CalpontSystemCatalog::SMALLINT:
		v = boost::any_cast<int16_t>(anyVal);
		break;
    case CalpontSystemCatalog::USMALLINT:
        v = boost::any_cast<uint16_t>(anyVal);
        break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
#ifdef _MSC_VER
		v = boost::any_cast<int>(anyVal);
#else
		v = boost::any_cast<int32_t>(anyVal);
#endif
		break;
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
#ifdef _MSC_VER
        v = boost::any_cast<unsigned int>(anyVal);
#else
        v = boost::any_cast<uint32_t>(anyVal);
#endif
        break;
	case CalpontSystemCatalog::BIGINT:
		v = boost::any_cast<long long>(anyVal);
		break;
    case CalpontSystemCatalog::UBIGINT:
        v = boost::any_cast<uint64_t>(anyVal);
        break;
	case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
		{
			float f = boost::any_cast<float>(anyVal);
			//N.B. There is a bug in boost::any or in gcc where, if you store a nan,
			//     you will get back a nan, but not necessarily the same bits that you put in.
			//     This only seems to be for float (double seems to work).
			if (isnan(f))
			{
				uint32_t ti = joblist::FLOATNULL;
				float* tfp = (float*)&ti;
				f = *tfp;
			}
			float* fp = &f;
			int32_t* ip = reinterpret_cast<int32_t*>(fp);
		  	v = *ip;
		}
		break;
	case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
		{
			double d = boost::any_cast<double>(anyVal);
			double* dp = &d;
			int64_t* ip = reinterpret_cast<int64_t*>(dp);
		  	v = *ip;
		}
		break;
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::VARBINARY:
	case CalpontSystemCatalog::BLOB:
	case CalpontSystemCatalog::CLOB:
		{
			string i = boost::any_cast<string>(anyVal);
			// bug 1932, pad nulls up to the size of v
			i.resize(sizeof(v), 0);
		  	v = *((uint64_t *) i.data());
			if (pushWarning)
				rf = ROUND_POS;
		}
		break;
	case CalpontSystemCatalog::DATE:
		v = boost::any_cast<uint32_t>(anyVal);
		break;
	case CalpontSystemCatalog::DATETIME:
		v = boost::any_cast<uint64_t>(anyVal);
		break;
	case CalpontSystemCatalog::DECIMAL:
	case CalpontSystemCatalog::UDECIMAL:
		if (ct.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
			v = boost::any_cast<char>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
			v = boost::any_cast<int16_t>(anyVal);
		else if (ct.colWidth == execplan::CalpontSystemCatalog::FOUR_BYTE)
#ifdef _MSC_VER
			v = boost::any_cast<int>(anyVal);
#else
			v = boost::any_cast<int32_t>(anyVal);
#endif
		else
			v = boost::any_cast<long long>(anyVal);
		break;
	default:
		break;
	}

	if ((ct.colDataType == CalpontSystemCatalog::TINYINT ||
		 ct.colDataType == CalpontSystemCatalog::SMALLINT ||
		 ct.colDataType == CalpontSystemCatalog::MEDINT ||
		 ct.colDataType == CalpontSystemCatalog::INT ||
		 ct.colDataType == CalpontSystemCatalog::BIGINT ||
		 ct.colDataType == CalpontSystemCatalog::DECIMAL ||
         ct.colDataType == CalpontSystemCatalog::UTINYINT ||
		 ct.colDataType == CalpontSystemCatalog::USMALLINT ||
		 ct.colDataType == CalpontSystemCatalog::UMEDINT ||
		 ct.colDataType == CalpontSystemCatalog::UINT ||
		 ct.colDataType == CalpontSystemCatalog::UBIGINT ||
		 ct.colDataType == CalpontSystemCatalog::UDECIMAL) &&
		pushWarning)
	{
		// get rid of leading white spaces and parentheses
		string data(str);
		size_t fpos = data.find_first_of(" \t()");
		while (string::npos != fpos)
		{
			data.erase(fpos, 1);
			fpos = data.find_first_of(" \t()");
		}
		rf = (data[0] == '-') ? ROUND_NEG : ROUND_POS;
	}

	return v;
}

//TODO: make this totaly case-insensitive
int8_t op2num(const SOP& sop)
{
	if (*sop == opeq)
		return COMPARE_EQ;
	else if (*sop == oplt)
		return COMPARE_LT;
	else if (*sop == ople)
		return COMPARE_LE;
	else if (*sop == opgt)
		return COMPARE_GT;
	else if (*sop == opge)
		return COMPARE_GE;
	else if (*sop == opne)
		return COMPARE_NE;
	else if (*sop == oplike || *sop == opLIKE)
		return COMPARE_LIKE;
	else if (*sop == opis || *sop == opIS || *sop == opisnull)
		return COMPARE_EQ;
	else if (*sop == opisnot || *sop == opISNOT || *sop == opisnotnull)
		return COMPARE_NE;
	else if (*sop == opnotlike || *sop == opNOTLIKE)
		return COMPARE_NLIKE;
	else
		cerr << boldStart << "op2num: Unhandled operator >" << *sop << '<' << boldStop << endl;

	return COMPARE_NIL;
}

int8_t bop2num(const SOP& sop)
{
	if (*sop == opand || *sop == opAND)
		return BOP_AND;
	else if (*sop == opor || *sop == opOR)
		return BOP_OR;
	else if (*sop == opxor || *sop == opXOR)
		return BOP_XOR;
	else
		cerr << boldStart << "bop2num: Unhandled operator " << *sop << boldStop << endl;

	return BOP_NONE;
}

enum TreeNodeType
{
	TREENODE,
	FILTER,
	CONSTANTFILTER,
	EXISTSFILTER,
	SELECTFILTER,
	SIMPLEFILTER,
	OUTERJOINONFILTER,
	OPERATOR,
	RETURNEDCOLUMN,
	AGGREGATECOLUMN,
	WINDOWFUNCTIONCOLUMN,
	ARITHMETICCOLUMN,
	CONSTANTCOLUMN,
	FUNCTIONCOLUMN,
	SIMPLECOLUMN,
	TREENODEIMPL,
	SIMPLESCALARFILTER,
	UNKNOWN,
};

TreeNodeType TreeNode2Type(const TreeNode* tn)
{
	//The indentation here is to show inheritance only.
	if (typeid(*tn) == typeid(TreeNode))
		return TREENODE;
	if (typeid(*tn) == typeid(Filter))
		return FILTER;
	if (typeid(*tn) == typeid(ConstantFilter))
		return CONSTANTFILTER;
	if (typeid(*tn) == typeid(ExistsFilter))
		return EXISTSFILTER;
	if (typeid(*tn) == typeid(SelectFilter))
		return SELECTFILTER;
	if (typeid(*tn) == typeid(SimpleFilter))
		return SIMPLEFILTER;
	if (typeid(*tn) == typeid(OuterJoinOnFilter))
		return OUTERJOINONFILTER;
	if (typeid(*tn) == typeid(Operator) ||
		typeid(*tn) == typeid(PredicateOperator) ||
		typeid(*tn) == typeid(LogicOperator))
		return OPERATOR;
	if (typeid(*tn) == typeid(ReturnedColumn))
		return RETURNEDCOLUMN;
	if (typeid(*tn) == typeid(AggregateColumn))
		return AGGREGATECOLUMN;
	if (typeid(*tn) == typeid(WindowFunctionColumn))
		return WINDOWFUNCTIONCOLUMN;
	if (typeid(*tn) == typeid(ArithmeticColumn))
		return ARITHMETICCOLUMN;
	if (typeid(*tn) == typeid(ConstantColumn))
		return CONSTANTCOLUMN;
	if (typeid(*tn) == typeid(FunctionColumn))
		return FUNCTIONCOLUMN;
	if (typeid(*tn) == typeid(SimpleColumn))
		return SIMPLECOLUMN;
	if (typeid(*tn) == typeid(SimpleColumn_INT<1>) ||
		typeid(*tn) == typeid(SimpleColumn_INT<2>) ||
		typeid(*tn) == typeid(SimpleColumn_INT<4>) ||
		typeid(*tn) == typeid(SimpleColumn_INT<8>) ||
	    typeid(*tn) == typeid(SimpleColumn_UINT<1>) ||
		typeid(*tn) == typeid(SimpleColumn_UINT<2>) ||
		typeid(*tn) == typeid(SimpleColumn_UINT<4>) ||
		typeid(*tn) == typeid(SimpleColumn_UINT<8>))
		return SIMPLECOLUMN;
	if (typeid(*tn) == typeid(SimpleColumn_Decimal<1>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<2>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<4>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<8>))
		return SIMPLECOLUMN;
	if (typeid(*tn) == typeid(PseudoColumn))
		return SIMPLECOLUMN;
	if (typeid(*tn) == typeid(TreeNodeImpl))
		return TREENODEIMPL;
	if (typeid(*tn) == typeid(SimpleScalarFilter))
		return SIMPLESCALARFILTER;
	return UNKNOWN;
}

void walkTreeNode(const ParseTree* n, void* obj)
{
	TreeNodeStack* stack = reinterpret_cast<TreeNodeStack*>(obj);
	const TreeNode* tn = n->data();
	switch (TreeNode2Type(tn))
	{
	case OPERATOR:
		stack->push(tn);
		break;
	case CONSTANTCOLUMN:
		stack->push(tn);
		break;
	case SIMPLECOLUMN:
		stack->push(tn);
		break;
	case FUNCTIONCOLUMN:
		stack->push(tn);
		break;
	case UNKNOWN:
		cerr << boldStart << "walkTreeNode: Unknown" << boldStop << endl;
		break;
	default:
/*
	TREENODE,
	FILTER,
	SIMPLEFILTER,
	OUTERJOINONFILTER,
	CONSTANTFILTER,
	EXISTSFILTER,
	SELECTFILTER,
	RETURNEDCOLUMN,
	ARITHMETICCOLUMN,
	TREENODEIMPL,
*/
		cerr << boldStart << "walkTreeNode: Not handled: " << TreeNode2Type(tn) << boldStop << endl;
		break;
	}
}

boost::shared_ptr<pColStep> doWhereFcn(const SimpleColumn* sc, const SOP& sop, const ArithmeticColumn* ac, const JobInfo& jobInfo)
{
	// We only handle one very specific form here right now, the rest we bail on

	boost::shared_ptr<pColStep> ret;
	TreeNodeStack stack;
	ac->expression()->walk(walkTreeNode, &stack);

	if (stack.size() != 3) return ret;

	const TreeNode* tncc1;
	const TreeNode* tncc2;
	const TreeNode* tnop;

	tnop = stack.top();
	stack.pop();
	if (TreeNode2Type(tnop) != OPERATOR) return ret;
	tncc1 = stack.top();
	stack.pop();
	if (TreeNode2Type(tncc1) != CONSTANTCOLUMN) return ret;
	tncc2 = stack.top();
	stack.pop();
	if (TreeNode2Type(tncc2) != SIMPLECOLUMN) return ret;

	cerr << boldStart << "doWhereFcn: Not handled." << boldStop << endl;
	return ret;
}


const JobStepVector doColFilter(const SimpleColumn* sc1, const SimpleColumn* sc2, JobInfo& jobInfo,
								const SOP& sop, SimpleFilter* sf)
{
	//The idea here is to take the two SC's and pipe them into a filter step.
	//The output of the filter step is one DL that is the minimum rid list met the condition.
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
	string alias1(extractTableAlias(sc1));
	string alias2(extractTableAlias(sc2));
	CalpontSystemCatalog::ColType ct1 = sc1->colType();
	CalpontSystemCatalog::ColType ct2 = sc2->colType();
	const PseudoColumn* pc1 = dynamic_cast<const PseudoColumn*>(sc1);
	const PseudoColumn* pc2 = dynamic_cast<const PseudoColumn*>(sc2);
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
			if (!sc1->schemaName().empty() && sc1->isInfiniDB() && !pc1)
				ct1 = jobInfo.csc->colType(sc1->oid());
			if (!sc2->schemaName().empty() && sc2->isInfiniDB() && !pc2)
				ct2 = jobInfo.csc->colType(sc2->oid());
//X
	int8_t op = op2num(sop);

	pColStep* pcs1 = NULL;
	if (pc1 == NULL)
		pcs1 = new pColStep(sc1->oid(), tableOid1, ct1, jobInfo);
	else
		pcs1 = new PseudoColStep(sc1->oid(), tableOid1, pc1->pseudoType(), ct1, jobInfo);
	CalpontSystemCatalog::OID dictOid1 = isDictCol(ct1);
	pcs1->alias(alias1);
	pcs1->view(sc1->viewName());
	pcs1->name(sc1->columnName());
	pcs1->schema(sc1->schemaName());
	pcs1->cardinality(sc1->cardinality());
	pcs1->setFeederFlag(true);

	pColStep* pcs2 = NULL;
	if (pc2 == NULL)
		pcs2 = new pColStep(sc2->oid(), tableOid2, ct2, jobInfo);
	else
		pcs2 = new PseudoColStep(sc2->oid(), tableOid2, pc2->pseudoType(), ct2, jobInfo);
	CalpontSystemCatalog::OID dictOid2 = isDictCol(ct2);
	pcs2->alias(alias2);
	pcs2->view(sc2->viewName());
	pcs2->name(sc2->columnName());
	pcs2->schema(sc2->schemaName());
	pcs2->cardinality(sc2->cardinality());
	pcs2->setFeederFlag(true);

	//Associate the steps
	JobStepVector jsv;

	TupleInfo ti1(setTupleInfo(ct1, sc1->oid(), jobInfo, tableOid1, sc1, alias1));
	pcs1->tupleId(ti1.key);
	TupleInfo ti2(setTupleInfo(ct2, sc2->oid(), jobInfo, tableOid2, sc2, alias2));
	pcs2->tupleId(ti2.key);

	// check if they are string columns greater than 8 bytes.
	if ((!isDictCol(ct1)) && (!isDictCol(ct2)))
	{
		// not strings, no need for dictionary steps, output fifo datalist
		AnyDataListSPtr spdl1(new AnyDataList());
		FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
		spdl1->fifoDL(dl1);
		dl1->OID(sc1->oid());

		JobStepAssociation outJs1;
		outJs1.outAdd(spdl1);
		pcs1->outputAssociation(outJs1);

		AnyDataListSPtr spdl2(new AnyDataList());
		FifoDataList* dl2 = new FifoDataList(1, jobInfo.fifoSize);
		spdl2->fifoDL(dl2);
		dl2->OID(sc2->oid());

		JobStepAssociation outJs2;
		outJs2.outAdd(spdl2);
		pcs2->outputAssociation(outJs2);
		pcs2->inputAssociation(outJs1);


		FilterStep* filt=new FilterStep(ct1, jobInfo);
		filt->alias(extractTableAlias(sc1));
		filt->tableOid(tableOid1);
		filt->name(pcs1->name()+","+pcs2->name());
		filt->view(pcs1->view());
		filt->schema(pcs1->schema());
		filt->addFilter(sf);
		if (op)
			filt->setBOP(op);

		JobStepAssociation outJs3;
		outJs3.outAdd(spdl1);
		outJs3.outAdd(spdl2);
		filt->inputAssociation(outJs3);

		SJSTEP step;
		step.reset(pcs1);
		jsv.push_back(step);
		step.reset(pcs2);
		jsv.push_back(step);
		step.reset(filt);
		jsv.push_back(step);
	}
	else if ((isCharCol(ct1)) && (isCharCol(ct2)))
	{
		//check where any column is greater than eight bytes
		if ((isDictCol(ct1) != 0 ) && (isDictCol(ct2) !=0 ))
		{
			// extra steps for string column greater than eight bytes -- from token to string
			pDictionaryStep* pdss1 = new pDictionaryStep(dictOid1, tableOid1, ct1, jobInfo);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc1->oid();
			pdss1->alias(extractTableAlias(sc1));
			pdss1->view(sc1->viewName());
			pdss1->name(sc1->columnName());
			pdss1->schema(sc1->schemaName());
			pdss1->cardinality(sc1->cardinality());

			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1;
			outJs1.outAdd(spdl11);
			pcs1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());

			JobStepAssociation outJs2;
			outJs2.outAdd(spdl12);
			pdss1->outputAssociation(outJs2);

			//Associate pcs1 with pdss1
			JobStepAssociation outJs11;
			outJs11.outAdd(spdl11);
			pdss1->inputAssociation(outJs11);


			pDictionaryStep* pdss2 = new pDictionaryStep(dictOid2, tableOid2, ct2, jobInfo);
			jobInfo.keyInfo->dictOidToColOid[dictOid2] = sc2->oid();
			pdss2->alias(extractTableAlias(sc2));
			pdss2->view(sc2->viewName());
			pdss2->name(sc2->columnName());
			pdss2->schema(sc2->schemaName());
			pdss2->cardinality(sc2->cardinality());


			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3;
			outJs3.outAdd(spdl21);
			pcs2->outputAssociation(outJs3);
			pcs2->inputAssociation(outJs2);
			//Associate pcs2 with pdss2
			JobStepAssociation outJs22;
			outJs22.outAdd(spdl21);
			pdss2->inputAssociation(outJs22);

			// data list for column 2 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl22(new AnyDataList());
			StringFifoDataList* dl22 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl22->stringDL(dl22);
			dl22->OID(sc2->oid());

			JobStepAssociation outJs4;
			outJs4.outAdd(spdl22);
			pdss2->outputAssociation(outJs4);

			FilterStep* filt = new FilterStep(ct1, jobInfo);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			filt->name(pcs1->name()+","+pcs2->name());
			filt->view(pcs1->view());
			filt->schema(pcs1->schema());
			filt->addFilter(sf);
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5;
			outJs5.outAdd(spdl12);
			outJs5.outAdd(spdl22);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcs1);
			jsv.push_back(step);
			step.reset(pdss1);
			jsv.push_back(step);
			step.reset(pcs2);
			jsv.push_back(step);
			step.reset(pdss2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			TupleInfo ti1(setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1));
			pdss1->tupleId(ti1.key);
			jobInfo.keyInfo->dictKeyMap[pcs1->tupleId()] = ti1.key;
			jobInfo.tokenOnly[pcs1->tupleId()] = false;

			TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
			pdss2->tupleId(ti2.key);
			jobInfo.keyInfo->dictKeyMap[pcs2->tupleId()] = ti2.key;
			jobInfo.tokenOnly[pcs2->tupleId()] = false;
		}
		else if ((isDictCol(ct1) != 0 ) && (isDictCol(ct2) ==0 )) //col1 is dictionary column
		{
			// extra steps for string column greater than eight bytes -- from token to string
			pDictionaryStep* pdss1 = new pDictionaryStep(dictOid1, tableOid1, ct1, jobInfo);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc1->oid();
			pdss1->alias(extractTableAlias(sc1));
			pdss1->view(sc1->viewName());
			pdss1->name(sc1->columnName());
			pdss1->schema(sc1->schemaName());
			pdss1->cardinality(sc1->cardinality());

			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1;
			outJs1.outAdd(spdl11);
			pcs1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());

			JobStepAssociation outJs2;
			outJs2.outAdd(spdl12);
			pdss1->outputAssociation(outJs2);

			//Associate pcs1 with pdss1
			JobStepAssociation outJs11;
			outJs11.outAdd(spdl11);
			pdss1->inputAssociation(outJs11);

			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3;
			outJs3.outAdd(spdl21);
			pcs2->outputAssociation(outJs3);
			pcs2->inputAssociation(outJs2);

			FilterStep* filt = new FilterStep(ct1, jobInfo);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			filt->view(pcs1->view());
			filt->schema(pcs1->schema());
			filt->addFilter(sf);
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5;
			outJs5.outAdd(spdl12);
			outJs5.outAdd(spdl21);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcs1);
			jsv.push_back(step);
			step.reset(pdss1);
			jsv.push_back(step);
			step.reset(pcs2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			TupleInfo ti1(setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1));
			pdss1->tupleId(ti1.key);
			jobInfo.keyInfo->dictKeyMap[pcs1->tupleId()] = ti1.key;
			jobInfo.tokenOnly[pcs1->tupleId()] = false;
		}
		else // if ((isDictCol(ct1) == 0 ) && (isDictCol(ct2) !=0 )) //col2 is dictionary column
		{
			// extra steps for string column greater than eight bytes -- from token to string
			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1;
			outJs1.outAdd(spdl11);
			pcs1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());


			pDictionaryStep* pdss2 = new pDictionaryStep(dictOid2, tableOid2, ct2, jobInfo);
			jobInfo.keyInfo->dictOidToColOid[dictOid2] = sc2->oid();
			pdss2->alias(extractTableAlias(sc2));
			pdss2->view(sc2->viewName());
			pdss2->name(sc2->columnName());
			pdss2->schema(sc2->schemaName());
			pdss2->cardinality(sc2->cardinality());


			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3;
			outJs3.outAdd(spdl21);
			pcs2->outputAssociation(outJs3);
			pcs2->inputAssociation(outJs1);
			//Associate pcs2 with pdss2
			JobStepAssociation outJs22;
			outJs22.outAdd(spdl21);
			pdss2->inputAssociation(outJs22);

			// data list for column 2 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl22(new AnyDataList());
			StringFifoDataList* dl22 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl22->stringDL(dl22);
			dl22->OID(sc2->oid());

			JobStepAssociation outJs4;
			outJs4.outAdd(spdl22);
			pdss2->outputAssociation(outJs4);

			FilterStep* filt = new FilterStep(ct1, jobInfo);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			filt->view(pcs1->view());
			filt->schema(pcs1->schema());
			filt->addFilter(sf);
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5;
			outJs5.outAdd(spdl11);
			outJs5.outAdd(spdl22);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcs1);
			jsv.push_back(step);
			step.reset(pcs2);
			jsv.push_back(step);
			step.reset(pdss2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
			pdss2->tupleId(ti2.key);
			jobInfo.keyInfo->dictKeyMap[pcs2->tupleId()] = ti2.key;
			jobInfo.tokenOnly[pcs2->tupleId()] = false;
		}
	}
	else
	{
		cerr << boldStart << "Filterstep: Filter with different type is not supported " << boldStop << endl;
		throw QueryDataExcept("Filter with different column types is not supported.", incompatFilterCols);
	}

	return jsv;
}


const JobStepVector doFilterExpression(const SimpleColumn* sc1, const SimpleColumn* sc2, JobInfo& jobInfo, const SOP& sop)
{
	JobStepVector jsv;
	SJSTEP sjstep;
	ExpressionStep* es = new ExpressionStep(jobInfo);
	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 2");

	SimpleFilter sf;
	sf.op(sop);
	sf.lhs(sc1->clone());
	sf.rhs(sc2->clone());
	es->expressionFilter(&sf, jobInfo);

	sjstep.reset(es);
	jsv.push_back(sjstep);

	return jsv;
}

const JobStepVector doJoin(
	SimpleColumn* sc1, SimpleColumn* sc2, JobInfo& jobInfo, const SOP& sop, SimpleFilter* sf)
{
	//The idea here is to take the two SC's and pipe them into a HJ step. The output of the HJ step
	// is 2 DL's (one for each table) that are the minimum rid list for each side of the join.
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
	string alias1(extractTableAlias(sc1));
	string alias2(extractTableAlias(sc2));
	string view1(sc1->viewName());
	string view2(sc2->viewName());
	string schema1(sc1->schemaName());
	string schema2(sc2->schemaName());

	CalpontSystemCatalog::ColType ct1 = sc1->colType();
	CalpontSystemCatalog::ColType ct2 = sc2->colType();
	PseudoColumn* pc1 = dynamic_cast<PseudoColumn*>(sc1);
	PseudoColumn* pc2 = dynamic_cast<PseudoColumn*>(sc2);
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
			if (!sc1->schemaName().empty() && sc1->isInfiniDB() && !pc1)
				ct1 = jobInfo.csc->colType(sc1->oid());
			if (!sc2->schemaName().empty() && sc2->isInfiniDB() && !pc2)
				ct2 = jobInfo.csc->colType(sc2->oid());
//X
	uint64_t joinInfo = sc1->joinInfo() | sc2->joinInfo();

	if (sc1->schemaName().empty())
	{
		SimpleColumn* tmp = (sc1);
		updateDerivedColumn(jobInfo, tmp, ct1);
	}

	if (sc2->schemaName().empty())
	{
		SimpleColumn* tmp = (sc2);
		updateDerivedColumn(jobInfo, tmp, ct2);
	}

	//@bug 566 Dont do a hash join if the table and the alias are the same
	//Bug 590
	if (tableOid1 == tableOid2 && alias1 == alias2 && view1 == view2 && joinInfo == 0)
	{
		if (sc1->schemaName().empty() || !compatibleColumnTypes(ct1, ct2, false))
		{
			return doFilterExpression(sc1, sc2, jobInfo, sop);
		}

		JobStepVector colFilter = doColFilter(sc1, sc2, jobInfo, sop, sf);
		//jsv.insert(jsv.end(), colFilter.begin(), colFilter.end());
		return colFilter;
	}

	// different tables
	if (!compatibleColumnTypes(ct1, ct2, true))
	{
		JobStepVector jsv;
		jsv = doFilterExpression(sc1, sc2, jobInfo, sop);
		uint32_t t1 = makeTableKey(jobInfo, sc1);
		uint32_t t2 = makeTableKey(jobInfo, sc2);
		jobInfo.incompatibleJoinMap[t1] = t2;
		jobInfo.incompatibleJoinMap[t2] = t1;

		return jsv;
	}

	pColStep* pcs1 = NULL;
	CalpontSystemCatalog::OID oid1 = sc1->oid();
	CalpontSystemCatalog::OID dictOid1 = isDictCol(ct1);
	if (sc1->schemaName().empty() == false)
	{
		if (pc1 == NULL)
			pcs1 = new pColStep(oid1, tableOid1, ct1, jobInfo);
		else
			pcs1 = new PseudoColStep(oid1, tableOid1, pc1->pseudoType(), ct1, jobInfo);
		pcs1->alias(alias1);
		pcs1->view(view1);
		pcs1->name(sc1->columnName());
		pcs1->schema(sc1->schemaName());
		pcs1->cardinality(sc1->cardinality());
	}

	pColStep* pcs2 = NULL;
	CalpontSystemCatalog::OID oid2 = sc2->oid();
	CalpontSystemCatalog::OID dictOid2 = isDictCol(ct2);
	if (sc2->schemaName().empty() == false)
	{
		if (pc2 == NULL)
			pcs2 = new pColStep(oid2, tableOid2, ct2, jobInfo);
		else
			pcs2 = new PseudoColStep(oid2, tableOid2, pc2->pseudoType(), ct2, jobInfo);
		pcs2->alias(alias2);
		pcs2->view(view2);
		pcs2->name(sc2->columnName());
		pcs2->schema(sc2->schemaName());
		pcs2->cardinality(sc2->cardinality());
	}

	JoinType jt = INNER;
	if (sc1->returnAll() && sc2->returnAll())
	{
		// Due to current connector limitation, INNER may have both returnAll set.
		// @bug3037, compound outer join may have both flag set if not the 1st pair.
		jt = INNER;
	}
	else if (sc1->returnAll())
	{
		jt = LEFTOUTER;
	}
	else if (sc2->returnAll())
	{
		jt = RIGHTOUTER;
	}

	//Associate the steps
	JobStepVector jsv;
	SJSTEP step;

	// bug 1495 compound join, v-table handles string join and compound join the same way
	AnyDataListSPtr spdl1(new AnyDataList());
	RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
	spdl1->rowGroupDL(dl1);
	dl1->OID(oid1);

	if (pcs1)
	{
		JobStepAssociation outJs1;
		outJs1.outAdd(spdl1);
		pcs1->outputAssociation(outJs1);

		step.reset(pcs1);
		jsv.push_back(step);
	}

	AnyDataListSPtr spdl2(new AnyDataList());
	RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
	spdl2->rowGroupDL(dl2);
	dl2->OID(oid2);

	if (pcs2)
	{
		JobStepAssociation outJs2;
		outJs2.outAdd(spdl2);
		pcs2->outputAssociation(outJs2);

		step.reset(pcs2);
		jsv.push_back(step);
	}

	TupleHashJoinStep* thj = new TupleHashJoinStep(jobInfo);
	thj->tableOid1(tableOid1);
	thj->tableOid2(tableOid2);
	thj->alias1(alias1);
	thj->alias2(alias2);
	thj->view1(view1);
	thj->view2(view2);
	thj->schema1(schema1);
	thj->schema2(schema2);
	thj->oid1(oid1);
	thj->oid2(oid2);
	thj->dictOid1(dictOid1);
	thj->dictOid2(dictOid2);
	thj->sequence1(sc1->sequence());
	thj->sequence2(sc2->sequence());
	thj->column1(sc1);
	thj->column2(sc2);
	thj->joinId((joinInfo == 0) ? (++jobInfo.joinNum) : 0);

	// Check if SEMI/ANTI join.
	// INNER/OUTER join and SEMI/ANTI are mutually exclusive,
	if (joinInfo != 0)
	{
		// @bug3998, keep the OUTER join type
		// jt = INIT;

		if (joinInfo & JOIN_SEMI)
			jt |= SEMI;

		if (joinInfo & JOIN_ANTI)
			jt |= ANTI;

		if (joinInfo & JOIN_SCALAR)
			jt |= SCALAR;

		if (joinInfo & JOIN_NULL_MATCH)
			jt |= MATCHNULLS;

		if (joinInfo & JOIN_CORRELATED)
			jt |= CORRELATED;

		if (joinInfo & JOIN_OUTER_SELECT)
			jt |= LARGEOUTER;

		if (sc1->joinInfo() & JOIN_CORRELATED)
			thj->correlatedSide(1);
		else if (sc2->joinInfo() & JOIN_CORRELATED)
			thj->correlatedSide(2);
	}
	thj->setJoinType(jt);

	JobStepAssociation outJs3;
	outJs3.outAdd(spdl1);
	outJs3.outAdd(spdl2);
	thj->inputAssociation(outJs3);
	step.reset(thj);

	TupleInfo ti1(setTupleInfo(ct1, oid1, jobInfo, tableOid1, sc1, alias1));
	if (pcs1)
	{
		pcs1->tupleId(ti1.key);
		thj->tupleId1(ti1.key);
		if (dictOid1 > 0)
		{
			ti1 = setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = oid1;
			jobInfo.keyInfo->dictKeyMap[pcs1->tupleId()] = ti1.key;
			jobInfo.tokenOnly[pcs1->tupleId()] = false;
//			thj->tupleId1(ti1.key);
		}
	}
	else
	{
		thj->tupleId1(getTupleKey(jobInfo, sc1));
	}

	TupleInfo ti2(setTupleInfo(ct2, oid2, jobInfo, tableOid2, sc2, alias2));
	if (pcs2)
	{
		pcs2->tupleId(ti2.key);
		thj->tupleId2(pcs2->tupleId());
		if (dictOid2 > 0)
		{
			TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
			jobInfo.keyInfo->dictOidToColOid[dictOid2] = oid2;
			jobInfo.keyInfo->dictKeyMap[pcs2->tupleId()] = ti2.key;
			jobInfo.tokenOnly[pcs2->tupleId()] = false;
//			thj->tupleId2(ti2.key);
		}
	}
	else
	{
		thj->tupleId2(getTupleKey(jobInfo, sc2));
	}

	jsv.push_back(step);

	return jsv;
}


const JobStepVector doSemiJoin(const SimpleColumn* sc, const ReturnedColumn* rc, JobInfo& jobInfo)
{
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = execplan::CNX_VTABLE_ID;
	string alias1(extractTableAlias(sc));
	string alias2(jobInfo.subAlias);
	CalpontSystemCatalog::ColType ct1 = sc->colType();
	CalpontSystemCatalog::ColType ct2 = rc->resultType();
	const PseudoColumn* pc1 = dynamic_cast<const PseudoColumn*>(sc);
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
			if (!sc->schemaName().empty() && sc->isInfiniDB() && !pc1)
				ct1 = jobInfo.csc->colType(sc->oid());
//X
	JobStepVector jsv;
	SJSTEP step;

	CalpontSystemCatalog::OID dictOid1 = 0;
	uint64_t tupleId1 = -1;
	uint64_t tupleId2 = -1;
	if (sc->schemaName().empty() == false)
	{
		pColStep* pcs1 = NULL;
		if (pc1 == NULL)
			pcs1 = new pColStep(sc->oid(), tableOid1, ct1, jobInfo);
		else
			pcs1 = new PseudoColStep(sc->oid(), tableOid1, pc1->pseudoType(), ct1, jobInfo);
		dictOid1 = isDictCol(ct1);
		pcs1->alias(alias1);
		pcs1->view(sc->viewName());
		pcs1->name(sc->columnName());
		pcs1->schema(sc->schemaName());
		pcs1->cardinality(sc->cardinality());

		step.reset(pcs1);
		jsv.push_back(step);

		TupleInfo ti1(setTupleInfo(ct1, sc->oid(), jobInfo, tableOid1, sc, alias1));
		pcs1->tupleId(ti1.key);
		tupleId1 = ti1.key;
		if (dictOid1 > 0)
		{
			ti1 = setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc, alias1);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc->oid();
			jobInfo.keyInfo->dictKeyMap[tupleId1] = ti1.key;
			jobInfo.tokenOnly[pcs1->tupleId()] = false;
		}
	}

	TupleHashJoinStep* thj = new TupleHashJoinStep(jobInfo);
	thj->tableOid1(tableOid1);
	thj->tableOid2(tableOid2);
	thj->alias1(alias1);
	thj->view1(sc->viewName());
	thj->schema1(sc->schemaName());
	thj->oid1(sc->oid());
	thj->oid2(tableOid2 + 1 + rc->sequence());
	thj->alias2(alias2);
	thj->dictOid1(dictOid1);
	thj->dictOid2(0);
	thj->sequence1(sc->sequence());
	thj->sequence2(rc->sequence());
	thj->column1(sc);
	thj->column2(rc);
	thj->joinId(0);
	thj->tupleId1(tupleId1);
	thj->tupleId2(tupleId2);

	// set join type
	JoinType jt = INIT;
	uint64_t joinInfo = sc->joinInfo();
	if (joinInfo & JOIN_SEMI)
		jt |= SEMI;

	if (joinInfo & JOIN_ANTI)
		jt |= ANTI;

	if (joinInfo & JOIN_SCALAR)
		jt |= SCALAR;

	if (joinInfo & JOIN_NULL_MATCH)
		jt |= MATCHNULLS;

	if (joinInfo & JOIN_CORRELATED)
		jt |= CORRELATED;

	if (joinInfo & JOIN_OUTER_SELECT)
		jt |= LARGEOUTER;

	thj->setJoinType(jt);

	// set correlated side
	thj->correlatedSide(1);

	step.reset(thj);
	jsv.push_back(step);
	return jsv;
}


SJSTEP expressionToFuncJoin(ExpressionStep* es, JobInfo& jobInfo)
{
	idbassert(es);
	boost::shared_ptr<FunctionJoinInfo> fji = es->functionJoinInfo();
	es->functionJoin(true);
	TupleHashJoinStep* thjs= new TupleHashJoinStep(jobInfo);
	thjs->tableOid1(fji->fTableOid[0]);
	thjs->tableOid2(fji->fTableOid[1]);
	thjs->oid1(fji->fOid[0]);
	thjs->oid2(fji->fOid[1]);
	thjs->alias1(fji->fAlias[0]);
	thjs->alias2(fji->fAlias[1]);
	thjs->view1(fji->fView[0]);
	thjs->view2(fji->fView[1]);
	thjs->schema1(fji->fSchema[0]);
	thjs->schema2(fji->fSchema[1]);
	thjs->column1(fji->fExpression[0]);
	thjs->column2(fji->fExpression[1]);
	thjs->sequence1(fji->fSequence[0]);
	thjs->sequence2(fji->fSequence[1]);
	thjs->joinId(fji->fJoinId);
	thjs->setJoinType(fji->fJoinType);
	thjs->correlatedSide(fji->fCorrelatedSide);
	thjs->funcJoinInfo(fji);
	thjs->tupleId1(fji->fJoinKey[0]);
	thjs->tupleId2(fji->fJoinKey[1]);

	updateTableKey(fji->fJoinKey[0], fji->fTableKey[0], jobInfo);
	updateTableKey(fji->fJoinKey[1], fji->fTableKey[1], jobInfo);

	return SJSTEP(thjs);
}


const JobStepVector doExpressionFilter(const ParseTree* n, JobInfo& jobInfo)
{
	JobStepVector jsv;
	ExpressionStep* es = new ExpressionStep(jobInfo);
	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 1");

	es->expressionFilter(n, jobInfo);
	SJSTEP sjstep(es);
	jsv.push_back(sjstep);

	return jsv;
}


const JobStepVector doExpressionFilter(const Filter* f, JobInfo& jobInfo)
{
	JobStepVector jsv;
	ExpressionStep* es = new ExpressionStep(jobInfo);
	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 2");

	es->expressionFilter(f, jobInfo);
	SJSTEP sjstep(es);
	jsv.push_back(sjstep);

	// @bug3683, support in/exists suquery function join
	const SimpleFilter* sf = dynamic_cast<const SimpleFilter*>(f);
	if (sf != NULL)
	{
		const ReturnedColumn* lrc =  static_cast<const ReturnedColumn*>(sf->lhs());
		const ReturnedColumn* rrc =  static_cast<const ReturnedColumn*>(sf->rhs());
		if (lrc->joinInfo())
		{
			const ReturnedColumn* lac = dynamic_cast<const ArithmeticColumn*>(lrc);
			const ReturnedColumn* lfc = dynamic_cast<const FunctionColumn*>(lrc);
			const ReturnedColumn* lsc = dynamic_cast<const SimpleColumn*>(lrc);
			if ((lac || lfc || lsc) && es->functionJoinInfo())
				jsv.push_back(expressionToFuncJoin(es, jobInfo));
		}
		else if (rrc->joinInfo())
		{
			const ReturnedColumn* rac = dynamic_cast<const ArithmeticColumn*>(lrc);
			const ReturnedColumn* rfc = dynamic_cast<const FunctionColumn*>(lrc);
			const ReturnedColumn* rsc = dynamic_cast<const SimpleColumn*>(lrc);
			if ((rac || rfc || rsc) && es->functionJoinInfo())
				jsv.push_back(expressionToFuncJoin(es, jobInfo));
		}
	}

	return jsv;
}


const JobStepVector doConstantBooleanFilter(const ParseTree* n, JobInfo& jobInfo)
{
	JobStepVector jsv;
	TupleConstantBooleanStep* tcbs = new
		TupleConstantBooleanStep(jobInfo, n->data()->getBoolVal());

	if (tcbs == NULL)
		throw runtime_error("Failed to create Constant Boolean Step");

	SJSTEP sjstep(tcbs);
	jsv.push_back(sjstep);

	return jsv;
}


bool optimizeIdbPatitionSimpleFilter(SimpleFilter* sf, JobStepVector& jsv, JobInfo& jobInfo)
{
	//@bug5848, not equal filter is not optimized.
	if (sf->op()->op() != opeq.op())
		return false;

	const FunctionColumn* fc = static_cast<const FunctionColumn*>(sf->lhs());
	const ConstantColumn* cc = static_cast<const ConstantColumn*>(sf->rhs());
	if (fc == NULL)
	{
		cc = static_cast<const ConstantColumn*>(sf->lhs());
		fc = static_cast<const FunctionColumn*>(sf->rhs());
	}

	// not a function or not idbparttition
	if (fc == NULL || cc == NULL || fc->functionName().compare("idbpartition") != 0)
		return false;

	// make sure the cc has 3 tokens
	vector<string> cv;
	boost::split(cv, cc->constval(), boost::is_any_of("."));
	if (cv.size() != 3)
		return false;

	// construct separate filters, then make job steps
	JobStepVector v;
	SOP sop = sf->op();
	const funcexp::FunctionParm& parms = fc->functionParms();
	
	for (uint64_t i = 0; i < 3; i++)
	{
		// very weird parms order is: db root, physical partition, segment
		// logical partition string : physical partition, segment, db root
		ReturnedColumn* lhs = dynamic_cast<ReturnedColumn*>(parms[(i+1)%3]->data());
		ConstantColumn* rhs = new ConstantColumn(cv[i]);
		SimpleFilter* f = new SimpleFilter(sop, lhs->clone(), rhs);
		v = doSimpleFilter(f, jobInfo); 
		jsv.insert(jsv.end(), v.begin(), v.end());
	}

	return true;
}


const JobStepVector doSimpleFilter(SimpleFilter* sf, JobInfo& jobInfo)
{
	JobStepVector jsv;
	if (sf == 0) return jsv;
	//cout << "doSimpleFilter " << endl;
	//cout << *sf << endl;
	ReturnedColumn* lhs;
	ReturnedColumn* rhs;
	SOP sop;

	lhs = sf->lhs();
	rhs = sf->rhs();
	sop = sf->op();

	TreeNodeType lhsType;
	TreeNodeType rhsType;

	lhsType = TreeNode2Type(lhs);
	rhsType = TreeNode2Type(rhs);

	CalpontSystemCatalog::OID tbl_oid = 0;

	SJSTEP sjstep;
	if (lhsType == SIMPLECOLUMN && rhsType == CONSTANTCOLUMN )
	{
		int8_t cop = op2num(sop);
		const SimpleColumn* sc = static_cast<const SimpleColumn*>(lhs);
		const ConstantColumn* cc = static_cast<const ConstantColumn*>(rhs);
		string alias(extractTableAlias(sc));
		string view(sc->viewName());
		string schema(sc->schemaName());
		tbl_oid = tableOid(sc, jobInfo.csc);

		if (sc->joinInfo() != 0 && cc->sequence() != -1)
		{
			// correlated, like in 'c1 in select 1' type sub queries.
			return doSemiJoin(sc, cc, jobInfo);
		}
		else if (sc->schemaName().empty())
		{
			// bug 3749, mark outer join table with isNull filter
			if (ConstantColumn::NULLDATA == cc->type() && (opis == *sop || opisnull == *sop))
				jobInfo.tableHasIsNull.insert(getTableKey(jobInfo, tbl_oid, alias, "", view));

			return doExpressionFilter(sf, jobInfo);
		}

		// trim trailing space char in the predicate
		string constval(cc->constval());
		size_t spos = constval.find_last_not_of(" ");
		if (spos != string::npos) constval = constval.substr(0, spos+1);

		CalpontSystemCatalog::OID dictOid = 0;
		CalpontSystemCatalog::ColType ct = sc->colType();
		const PseudoColumn* pc = dynamic_cast<const PseudoColumn*>(sc);
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
		if (!sc->schemaName().empty() && sc->isInfiniDB() && !pc)
			ct = jobInfo.csc->colType(sc->oid());
//X
		//@bug 339 nulls are not stored in dictionary
		if ((dictOid = isDictCol(ct)) > 0  && ConstantColumn::NULLDATA != cc->type())
		{
			if (jobInfo.trace)
				cout << "Emit pTokenByScan/pCol for SimpleColumn op ConstantColumn" << endl;

			// dictionary, cannot be pseudo column
			pColStep* pcs = new pColStep(sc->oid(), tbl_oid, ct, jobInfo);
			pcs->alias(alias);
			pcs->view(view);
			pcs->name(sc->columnName());
			pcs->schema(sc->schemaName());
			pcs->cardinality(sc->cardinality());

			if (filterWithDictionary(dictOid, jobInfo.stringScanThreshold))
			{
				pDictionaryStep* pds = new pDictionaryStep(dictOid, tbl_oid, ct, jobInfo);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->schema(sc->schemaName());
				pds->cardinality(sc->cardinality());

				//Add the filter
				pds->addFilter(cop, constval);

				// data list for pcolstep output
				AnyDataListSPtr spdl1(new AnyDataList());
				FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
				spdl1->fifoDL(dl1);
				dl1->OID(sc->oid());

				JobStepAssociation outJs1;
				outJs1.outAdd(spdl1);
				pcs->outputAssociation(outJs1);

				// data list for pdictionarystep output
				AnyDataListSPtr spdl2(new AnyDataList());
				StringFifoDataList* dl2 = new StringFifoDataList(1, jobInfo.fifoSize);
				spdl2->stringDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2;
				outJs2.outAdd(spdl2);
				pds->outputAssociation(outJs2);

				//Associate pcs with pds
				JobStepAssociation outJs;
				outJs.outAdd(spdl1);
				pds->inputAssociation(outJs);

				sjstep.reset(pcs);
				jsv.push_back(sjstep);
				sjstep.reset(pds);
				jsv.push_back(sjstep);

				// save for expression transformation
				pds->addFilter(sf);

				// token column
				CalpontSystemCatalog::ColType tct;
				tct.colDataType = CalpontSystemCatalog::BIGINT;
				tct.colWidth = 8;
				tct.scale = 0;
				tct.precision = 0;
				tct.compressionType = ct.compressionType;
				TupleInfo ti(setTupleInfo(tct, sc->oid(), jobInfo, tbl_oid, sc, alias));
				jobInfo.keyInfo->token2DictTypeMap[ti.key] = ct;
				pcs->tupleId(ti.key);

				// string column
				ti = setTupleInfo(ct, dictOid, jobInfo, tbl_oid, sc, alias);
				pds->tupleId(ti.key);
				jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
				jobInfo.tokenOnly[ti.key] = false;
			}
			else
			{
				pDictionaryScan* pds = new pDictionaryScan(dictOid, tbl_oid, ct, jobInfo);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->schema(sc->schemaName());
				pds->cardinality(sc->cardinality());

				//Add the filter
				pds->addFilter(cop, constval);

				// save for expression transformation
				pds->addFilter(sf);

				TupleHashJoinStep* thj = new TupleHashJoinStep(jobInfo);
				thj->tableOid1(0);
				thj->tableOid2(tbl_oid);
				thj->alias1(alias);
				thj->alias2(alias);
				thj->view1(view);
				thj->view2(view);
				thj->schema1(schema);
				thj->schema2(schema);
				thj->oid1(sc->oid());
				thj->oid2(sc->oid());
				thj->joinId(0);
				thj->setJoinType(INNER);

				CalpontSystemCatalog::ColType dct;
				dct.colDataType = CalpontSystemCatalog::BIGINT;
				dct.colWidth = 8;
				dct.scale = 0;
				dct.precision = 0;
				dct.compressionType = ct.compressionType;

				TupleInfo ti(setTupleInfo(dct, sc->oid(), jobInfo, tbl_oid, sc, alias));
				jobInfo.keyInfo->token2DictTypeMap[ti.key] = ct;
				pds->tupleId(ti.key); // pcs, pds use same tuple key, both 8-byte column
				pcs->tupleId(ti.key);
				thj->tupleId1(ti.key);
				thj->tupleId2(ti.key);

				if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
					jobInfo.tokenOnly[ti.key] = true;

				//Associate the steps
				AnyDataListSPtr spdl1(new AnyDataList());

				RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
				spdl1->rowGroupDL(dl1);
				dl1->OID(dictOid);

				JobStepAssociation outJs1;
				outJs1.outAdd(spdl1);
				pds->outputAssociation(outJs1);

				AnyDataListSPtr spdl2(new AnyDataList());

				RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
				spdl2->rowGroupDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2;
				outJs2.outAdd(spdl2);
				pcs->outputAssociation(outJs2);

				JobStepAssociation outJs3;
				outJs3.outAdd(spdl1);
				outJs3.outAdd(spdl2);
				sjstep.reset(pds);
				jsv.push_back(sjstep);
				sjstep.reset(pcs);
				jsv.push_back(sjstep);

				thj->inputAssociation(outJs3);
				sjstep.reset(thj);
				jsv.push_back(sjstep);
			}
		}
		else if ( CalpontSystemCatalog::CHAR != ct.colDataType &&
				 CalpontSystemCatalog::VARCHAR != ct.colDataType &&
				 CalpontSystemCatalog::VARBINARY != ct.colDataType &&
				 ConstantColumn::NULLDATA != cc->type() &&
				 (cop & COMPARE_LIKE) ) // both like and not like
		{
				return doExpressionFilter(sf, jobInfo);
		}
		else
		{
			// @bug 1151 string longer than colwidth of char/varchar.
			int64_t value = 0;
			uint8_t rf = 0;
#ifdef FAILED_ATOI_IS_ZERO
			//if cvn throws (because there's non-digit data in the string, treat that as zero rather than
			//   throwing
			try
			{
				bool isNull = ConstantColumn::NULLDATA == cc->type();
				if ((ct.colDataType == CalpontSystemCatalog::DATE ||
					  ct.colDataType == CalpontSystemCatalog::DATETIME) &&
					  constval == "0000-00-00")
					value = 0;
				else
					value = convertValueNum(constval, ct, isNull, rf);
				if (ct.colDataType == CalpontSystemCatalog::FLOAT && !isNull)
				{
					float f = cc->getFloatVal();
					value = *(reinterpret_cast<int32_t*>(&f));
				}
				else if (ct.colDataType == CalpontSystemCatalog::DOUBLE && !isNull)
				{
					double d = cc->getDoubleVal();
					value = *(reinterpret_cast<int64_t*>(&d));
				}
			}
			catch (...)
			{
				switch (ct.colDataType)
				{
				case CalpontSystemCatalog::TINYINT:
				case CalpontSystemCatalog::SMALLINT:
				case CalpontSystemCatalog::MEDINT:
				case CalpontSystemCatalog::INT:
				case CalpontSystemCatalog::BIGINT:
				case CalpontSystemCatalog::UTINYINT:
				case CalpontSystemCatalog::USMALLINT:
				case CalpontSystemCatalog::UMEDINT:
				case CalpontSystemCatalog::UINT:
				case CalpontSystemCatalog::UBIGINT:
					value = 0;
					rf = 0;
					break;
				default:
					throw;
					break;
				}
			}
#else
			bool isNull = ConstantColumn::NULLDATA == cc->type();
			if ((ct.colDataType == CalpontSystemCatalog::DATE ||
				   ct.colDataType == CalpontSystemCatalog::DATETIME) &&
				   constval == "0000-00-00")
					value = 0;
			else
				value = convertValueNum(constval, ct, isNull, rf);

			if (ct.colDataType == CalpontSystemCatalog::FLOAT && !isNull)
			{
				float f = cc->getFloatVal();
				value = *(reinterpret_cast<int32_t*>(&f));
			}
			else if (ct.colDataType == CalpontSystemCatalog::DOUBLE && !isNull)
			{
				double d = cc->getDoubleVal();
				value = *(reinterpret_cast<int64_t*>(&d));
			}
#endif
			// @bug 2584, make "= null" to COMPARE_NIL.
			if (ConstantColumn::NULLDATA == cc->type() && (opeq == *sop || opne == *sop))
				cop = COMPARE_NIL;

			if (jobInfo.trace)
				cout << "doSimpleFilter Emit pCol for SimpleColumn op ConstantColumn = " << value <<
					" (" << cc->constval() << ')' << endl;
			if (sf->indexFlag() == 0)
			{
				pColStep* pcs = NULL;
				if (pc == NULL)
					pcs = new pColStep(sc->oid(), tbl_oid, ct, jobInfo);
				else
					pcs = new PseudoColStep(sc->oid(), tbl_oid, pc->pseudoType(), ct, jobInfo);
				if (sc->isInfiniDB())
					pcs->addFilter(cop, value, rf);
				pcs->alias(alias);
				pcs->view(view);
				pcs->name(sc->columnName());
				pcs->schema(sc->schemaName());
				pcs->cardinality(sf->cardinality());

				sjstep.reset(pcs);
				jsv.push_back(sjstep);

				// save for expression transformation
				pcs->addFilter(sf);

				TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tbl_oid, sc, alias));
				pcs->tupleId(ti.key);

				if (dictOid > 0) // cc->type() == ConstantColumn::NULLDATA
				{
					if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
						jobInfo.tokenOnly[ti.key] = true;
				}

				if (ConstantColumn::NULLDATA == cc->type() &&
					(opis == *sop || opisnull == *sop))
					jobInfo.tableHasIsNull.insert(
						getTableKey(jobInfo, tbl_oid, alias, sc->schemaName(), view));
			}
			else
			{
				throw runtime_error("Uses indexes?");
#if 0
				CalpontSystemCatalog::IndexName indexName;
				indexName.schema = sc->schemaName();
				indexName.table = sc->tableName();
				indexName.index = sc->indexName();
				CalpontSystemCatalog::IndexOID indexOID = jobInfo.csc->lookupIndexNbr(indexName);
				pIdxWalk* pidxw = new pIdxWalk(JobStepAssociation(), JobStepAssociation(), 0,
					jobInfo.csc, indexOID.objnum, sc->oid(), tbl_oid,
					jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0,
					jobInfo.statementId);

				pidxw->addSearchStr(cop, value);
				pidxw->cardinality(sf->cardinality());

				sjstep.reset(pidxw);
				jsv.push_back(sjstep);
				pIdxList* pidxL = new pIdxList(JobStepAssociation(),
					JobStepAssociation(), 0, jobInfo.csc,
					indexOID.listOID, tbl_oid, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId);

				/*output of idxwalk */
				AnyDataListSPtr spdl1(new AnyDataList());
				ZonedDL* dl1 = new ZonedDL(1, jobInfo.rm);
				dl1->OID(indexOID.objnum);
				spdl1->zonedDL(dl1);
				JobStepAssociation outJs1;
				outJs1.outAdd(spdl1);
				pidxw->outputAssociation(outJs1);

				/*inputput of idxlist */
				pidxL->inputAssociation(outJs1);

				/*output of idxlist */
				AnyDataListSPtr spdl2(new AnyDataList());
				ZonedDL* dl2 = new ZonedDL(1, jobInfo.rm);
				dl2->OID(indexOID.listOID);
				spdl2->zonedDL(dl2);
				JobStepAssociation outJs2;
				outJs2.outAdd(spdl2);
				pidxL->outputAssociation(outJs2);

				sjstep.reset(pidxL);
				jsv.push_back(sjstep);
#endif
			}
		}
	}
	else if (lhsType == SIMPLECOLUMN && rhsType == SIMPLECOLUMN)
	{
		//This is a join (different table) or filter step (same table)
		SimpleColumn* sc1 = static_cast<SimpleColumn*>(lhs);
		SimpleColumn* sc2 = static_cast<SimpleColumn*>(rhs);

		// @bug 1496. handle non equal operator as expression in v-table mode
		if ((sc1->tableName() != sc2->tableName() ||
			 sc1->tableAlias() != sc2->tableAlias() ||
			 sc1->viewName() != sc2->viewName())
			&& (sop->data() != "="))
		{
			return doExpressionFilter(sf, jobInfo);
		}

		// @bug 1349. no-op rules:
		// 1. If two columns of a simple filter are of the two different tables, and the
		//    filter operator is not "=", no-op this simple filter,
		// 2. If a join filter has "ANTI" option, no op this filter before ANTI hashjoin
		//    is supported in ExeMgr.
		// @bug 1933. Throw exception instead of no-op for MySQL virtual table
        //            (no connector re-filter).
		if (sf->joinFlag() == SimpleFilter::ANTI)
			throw runtime_error("Anti join is not currently supported");

		// Do a simple column join
		JobStepVector join = doJoin(sc1, sc2, jobInfo, sop, sf);
		// set cardinality for the hashjoin step. hj result card <= larger input card
		uint32_t card = 0;
		if (sf->cardinality() > sc1->cardinality() && sf->cardinality() > sc2->cardinality())
			card = (sc1->cardinality() > sc2->cardinality() ? sc1->cardinality() : sc2->cardinality());
		else
			card = sf->cardinality();
		join[join.size()-1].get()->cardinality(card);

		jsv.insert(jsv.end(), join.begin(), join.end());
	}
	else if (lhsType == CONSTANTCOLUMN && rhsType == SIMPLECOLUMN)
	{
		//swap the two and process as normal
		SOP opsop(sop->opposite());
		sf->op(opsop);
		sf->lhs(rhs);
		sf->rhs(lhs);
		jsv = doSimpleFilter(sf, jobInfo);
		if (jsv.empty())
			throw runtime_error("Unhandled SimpleFilter");
	}
	else if (lhsType == ARITHMETICCOLUMN || rhsType == ARITHMETICCOLUMN ||
			 lhsType == FUNCTIONCOLUMN || rhsType == FUNCTIONCOLUMN)
	{
		const ReturnedColumn* rc = static_cast<const ReturnedColumn*>(lhs);
		if (rc && (!rc->joinInfo()) && rhsType == AGGREGATECOLUMN)
			throw IDBExcept(ERR_AGG_IN_WHERE);

		// @bug5848, CP elimination for idbPartition(col)
		JobStepVector sv;
		if (optimizeIdbPatitionSimpleFilter(sf, sv, jobInfo))
			jsv.swap(sv);
		else
			jsv = doExpressionFilter(sf, jobInfo);
	}
	else if (lhsType == SIMPLECOLUMN &&
			(rhsType == AGGREGATECOLUMN || rhsType == ARITHMETICCOLUMN || rhsType == FUNCTIONCOLUMN))
	{
		const SimpleColumn* sc = static_cast<const SimpleColumn*>(lhs);
		const ReturnedColumn* rc = static_cast<const ReturnedColumn*>(rhs);

		if (sc->joinInfo() != 0)
			return doSemiJoin(sc, rc, jobInfo);
		else if (rhsType == AGGREGATECOLUMN)
			throw IDBExcept(ERR_AGG_IN_WHERE);
		else
			throw logic_error("doSimpleFilter: Unhandled SimpleFilter.");
	}
	else if (rhsType == SIMPLECOLUMN &&
			(lhsType == AGGREGATECOLUMN || lhsType == ARITHMETICCOLUMN || lhsType == FUNCTIONCOLUMN))
	{
		const SimpleColumn* sc = static_cast<const SimpleColumn*>(rhs);
		const ReturnedColumn* rc = static_cast<const ReturnedColumn*>(lhs);

		if (sc->joinInfo() != 0)
			return doSemiJoin(sc, rc, jobInfo);
		else if (lhsType == AGGREGATECOLUMN)
			throw IDBExcept(ERR_AGG_IN_WHERE);
		else
			throw logic_error("doSimpleFilter: Unhandled SimpleFilter.");
	}
	else if (rhsType == WINDOWFUNCTIONCOLUMN)
	{
		// workaroud for IN subquery with window function
		// window function as arguments of a function is not covered !!
		jsv = doExpressionFilter(sf, jobInfo);
	}
	else if (lhsType == CONSTANTCOLUMN && rhsType == CONSTANTCOLUMN)
	{
		jsv = doExpressionFilter(sf, jobInfo);
	}
	else
	{
		cerr << boldStart << "doSimpleFilter: Unhandled SimpleFilter: left = " << lhsType <<
			", right = " << rhsType << boldStop << endl;

		throw logic_error("doSimpleFilter: Unhandled SimpleFilter.");
	}

	return jsv;
}


const JobStepVector doOuterJoinOnFilter(OuterJoinOnFilter* oj, JobInfo& jobInfo)
{
	JobStepVector jsv;
	if (oj == 0) return jsv;

	//cout << "doOuterJoinOnFilter " << endl;
	//cout << *oj << endl;

	// Parse the join on filter to join steps and an expression step, if any.
	stack<ParseTree*> nodeStack;        // stack used for pre-order traverse
	set<ParseTree*> doneNodes;          // solved joins and simple filters
	map<ParseTree*, ParseTree*> cpMap;  // <child, parent> link for node removal
	JobStepVector join;                 // join step with its projection steps
	set<ParseTree*> nodesToRemove;      // nodes to be removed after converted to steps

	// To compromise the front end difficulty on setting outer attributes.
	set<uint64_t> tablesInOuter;

	// root
	ParseTree* filters = new ParseTree(*(oj->pt().get()));
	nodeStack.push(filters);
	cpMap[filters] = NULL;

	// @bug5311, optimization for outer join on clause
	set<uint64_t> tablesInJoin;

	// @bug3683, function join
	vector<pair<ParseTree*, SJSTEP> > fjCandidates;

	// while stack is not empty
	while(!nodeStack.empty())
	{
		ParseTree* cn = nodeStack.top();  // current node
		nodeStack.pop();
		TreeNode*  tn = cn->data();
		Operator* op = dynamic_cast<Operator*>(tn);  // AND | OR
		if (op != NULL && (*op == opOR || *op == opor))
			continue;

		// join is expressed as SimpleFilter
		SimpleFilter* sf = dynamic_cast<SimpleFilter*>(tn);
		if (sf != NULL)
		{
			if (sf->joinFlag() == SimpleFilter::ANTI)
				throw runtime_error("Anti join is not currently supported");

			ReturnedColumn* lhs = sf->lhs();
			ReturnedColumn* rhs = sf->rhs();
			SOP sop = sf->op();

			SimpleColumn* sc1 = dynamic_cast<SimpleColumn*>(lhs);
			SimpleColumn* sc2 = dynamic_cast<SimpleColumn*>(rhs);
			if ((sc1 != NULL && sc2 != NULL) && (sop->data() == "=") &&
				(sc1->tableName() != sc2->tableName() ||
				 sc1->tableAlias() != sc2->tableAlias() ||
				 sc1->viewName() != sc2->viewName()))
			{
				// @bug3037, workaround on join order, wish this can be corrected soon,
				// cascade outer table attribute.
				CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
				uint64_t tid1 = getTableKey(
					jobInfo, tableOid1, sc1->tableAlias(), sc1->schemaName(), sc1->viewName());
				CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
				uint64_t tid2 = getTableKey(
					jobInfo, tableOid2, sc2->tableAlias(), sc2->schemaName(), sc2->viewName());

				if (tablesInOuter.find(tid1) != tablesInOuter.end())
					sc1->returnAll(true);
				else if (tablesInOuter.find(tid2) != tablesInOuter.end())
					sc2->returnAll(true);

				if (sc1->returnAll() && !sc2->returnAll())
					tablesInOuter.insert(tid1);
				else if (!sc1->returnAll() && sc2->returnAll())
					tablesInOuter.insert(tid2);

				tablesInJoin.insert(tid1);
				tablesInJoin.insert(tid2);

				join = doJoin(sc1, sc2, jobInfo, sop, sf);
				// set cardinality for the hashjoin step.
				uint32_t card = sf->cardinality();
				if (sf->cardinality() > sc1->cardinality() &&
					sf->cardinality() > sc2->cardinality())
					card = ((sc1->cardinality() > sc2->cardinality()) ?
							sc1->cardinality() : sc2->cardinality());
				join[join.size()-1].get()->cardinality(card);

				jsv.insert(jsv.end(), join.begin(), join.end());
				doneNodes.insert(cn);
			}
			else
			{
				ExpressionStep* es = new ExpressionStep(jobInfo);
				if (es == NULL)
					throw runtime_error("Failed to create ExpressionStep 2");

				SJSTEP sjstep;
				es->expressionFilter(sf, jobInfo);
				sjstep.reset(es);
				if (es->functionJoinInfo())
					fjCandidates.push_back(make_pair(cn, sjstep));
			}
		}

		// Add right and left to the stack.
		ParseTree* right = cn->right();
		if (right != NULL)
		{
			cpMap[right] = cn;
			nodeStack.push(right);
		}

		ParseTree* left  = cn->left();
		if (left != NULL)
		{
			cpMap[left] = cn;
			nodeStack.push(left);
		}
	}

	// check if there are any join steps in jsv.
	bool isOk = true;
	TupleHashJoinStep* thjs = NULL;
	for (JobStepVector::iterator i = jsv.begin(); i != jsv.end(); i++)
	{
		if (dynamic_cast<TupleHashJoinStep*>(i->get()) != thjs)
			thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
	}

	// no simple column join found, try function join
	if (thjs == NULL)
	{
		// check any expressions can be converted to function joins.
		vector<pair<ParseTree*, SJSTEP> >::iterator i = fjCandidates.begin();
		while (i != fjCandidates.end())
		{
			ExpressionStep* es = dynamic_cast<ExpressionStep*>((i->second).get());
			SJSTEP sjstep = expressionToFuncJoin(es, jobInfo);
			idbassert(sjstep.get());
			jsv.push_back(sjstep);

			doneNodes.insert(i->first);

			i++;
		}
	}

	// check again if we got a join step.
	for (JobStepVector::iterator i = jsv.begin(); i != jsv.end(); i++)
	{
		if (dynamic_cast<TupleHashJoinStep*>(i->get()) != thjs)
			thjs = dynamic_cast<TupleHashJoinStep*>(i->get());
	}

	if (thjs != NULL)
	{
		// @bug5311, optimization for outer join on clause, move out small side simple filters.
		// some repeat code, but better done after join sides settled.
		nodeStack.push(filters);
		cpMap[filters] = NULL;

		// while stack is not empty
		while(!nodeStack.empty())
		{
			ParseTree* cn = nodeStack.top();  // current node
			nodeStack.pop();
			TreeNode*  tn = cn->data();
			Operator* op = dynamic_cast<Operator*>(tn);  // AND | OR
			if (op != NULL && (*op == opOR || *op == opor))
				continue;

			SimpleFilter* sf = dynamic_cast<SimpleFilter*>(tn);
			if ((sf != NULL) && (doneNodes.find(cn) == doneNodes.end()))
			{
				// joins are done, this is not a join.
				ReturnedColumn* lhs = sf->lhs();
				ReturnedColumn* rhs = sf->rhs();
				SOP sop = sf->op();

				// handle simple-simple | simple-constant | constant-simple
				SimpleColumn* sc  = NULL;
				SimpleColumn* sc1 = dynamic_cast<SimpleColumn*>(lhs);
				SimpleColumn* sc2 = dynamic_cast<SimpleColumn*>(rhs);
				if ((sc1 != NULL && sc2 != NULL) &&
					(sc1->tableName() == sc2->tableName() &&
					 sc1->tableAlias() == sc2->tableAlias() &&
					 sc1->viewName() == sc2->viewName()))
				{
					sc = sc1; // same table, just check sc1
				}
				else if (sc1 != NULL && dynamic_cast<ConstantColumn*>(rhs) != NULL)
				{
					sc = sc1;
				}
				else if (sc2 != NULL && dynamic_cast<ConstantColumn*>(lhs) != NULL)
				{
					sc = sc2;
				}

				if (sc != NULL)
				{
					CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
					uint64_t tid = getTableKey(
						jobInfo, tblOid, sc->tableAlias(), sc->schemaName(), sc->viewName());

					// skip outer table filters or table not directly involved in the outer join
					if (tablesInOuter.find(tid) != tablesInOuter.end() ||
						tablesInJoin.find(tid)  == tablesInJoin.end())
						continue;

					JobStepVector sfv = doSimpleFilter(sf, jobInfo);
					ExpressionStep* es = NULL;
					for (JobStepVector::iterator k = sfv.begin(); k != sfv.end(); k++)
					{
						k->get()->onClauseFilter(true);
						if ((es = dynamic_cast<ExpressionStep*>(k->get())) != NULL)
			            	es->associatedJoinId(thjs->joinId());
					}

					jsv.insert(jsv.end(), sfv.begin(), sfv.end());

					doneNodes.insert(cn);
				}
			}

			// Add right and left to the stack.
			ParseTree* right = cn->right();
			if (right != NULL)
			{
				cpMap[right] = cn;
				nodeStack.push(right);
			}

			ParseTree* left  = cn->left();
			if (left != NULL)
			{
				cpMap[left] = cn;
				nodeStack.push(left);
			}
		}

		// remove joins from the original filters
		ParseTree* nullTree = NULL;
		for (set<ParseTree*>::iterator i = doneNodes.begin(); i != doneNodes.end() && isOk; i++)
		{
			ParseTree* c = *i;
			map<ParseTree*, ParseTree*>::iterator j = cpMap.find(c);
			if (j == cpMap.end())
			{
				isOk = false;
				continue;
			}

			ParseTree* p = j->second;
			if (p == NULL)
			{
				filters = NULL;
				nodesToRemove.insert(c);
			}
			else
			{
				map<ParseTree*, ParseTree*>::iterator k = cpMap.find(p);
				if (k == cpMap.end())
				{
					isOk = false;
					continue;
				}

				ParseTree* pp = k->second;
				if (pp == NULL)
				{
					if (p->left() == c)
						filters = p->right();
					else
						filters = p->left();
					cpMap[filters] = NULL;
				}
				else
				{
					if (p->left() == c)
					{
						if (pp->left() == p)
							pp->left(p->right());
						else
							pp->right(p->right());
						cpMap[p->right()] = pp;
					}
					else
					{
						if (pp->left() == p)
							pp->left(p->left());
						else
							pp->right(p->left());
						cpMap[p->left()] = pp;
					}
				}

				p->left(nullTree);
				p->right(nullTree);
				nodesToRemove.insert(p);
				nodesToRemove.insert(c);
			}
		}

		// construct an expression step, if additional comparison exists.
		if (isOk && filters != NULL && filters->data() != NULL)
		{
			ExpressionStep* es = new ExpressionStep(jobInfo);
			if (es == NULL)
				throw runtime_error("Failed to create ExpressionStep 1");

			es->expressionFilter(filters, jobInfo);
			es->associatedJoinId(thjs->joinId());
			SJSTEP sjstep(es);
			jsv.push_back(sjstep);
		}
	}
	else
	{
		// Due to Calpont view handling, some joins may treated as expressions.
		ExpressionStep* es = new ExpressionStep(jobInfo);
		if (es == NULL)
			throw runtime_error("Failed to create ExpressionStep 1");

		es->expressionFilter(filters, jobInfo);
		SJSTEP sjstep(es);
		jsv.push_back(sjstep);
	}

	delete filters;

	if (!isOk)
	{
		throw runtime_error("Failed to parse join condition.");
	}

	if (jobInfo.trace)
	{
		ostringstream oss;
		oss << "\nOuterJoinOn steps: " << endl;
		ostream_iterator<JobStepVector::value_type> oIter(oss, "\n");
		copy(jsv.begin(), jsv.end(), oIter);
		cout << oss.str();
	}

	return jsv;
}

bool tryCombineDictionary(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop)
{
	JobStepVector::iterator it2 = jsv2.end() - 1;
	// already checked: (typeid(*(it2->get()) != typeid(pDictionaryStep))
	if (typeid(*((it2-1)->get())) != typeid(pColStep))
		return false;

	pDictionaryStep* ipdsp = dynamic_cast<pDictionaryStep*>(it2->get());
	bool onClauseFilter = ipdsp->onClauseFilter();

	JobStepVector::iterator iter = jsv1.begin();
	JobStepVector::iterator end = jsv1.end();

	if (bop == BOP_OR)
	{
		iter = end - 1;
	}

	while (iter != end)
	{
		pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>(iter->get());
		if (pdsp != NULL && pdsp->onClauseFilter() == onClauseFilter)
		{
			// If the OID's match and the BOP's match and the previous step is pcolstep,
			// then append the filters.
			if ((ipdsp->tupleId() == pdsp->tupleId()) &&
				(dynamic_cast<pColStep*>((iter-1)->get()) != NULL))
			{
				if (pdsp->BOP() == BOP_NONE)
				{
 					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->setBOP(bop);
						pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
				else if (pdsp->BOP() == bop)
				{
					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
			}
		}
		++iter;
	}

	return false;
}

bool tryCombineDictionaryScan(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop)
{
// disable dictionary scan -- bug3321
#if 0
	JobStepVector::iterator it2 = jsv2.begin();
	if (typeid(*((it2+1)->get())) != typeid(pColStep))
		return false;

	if ((typeid(*((it2+2)->get())) != typeid(TupleHashJoinStep)) &&
		(typeid(*((it2+2)->get())) != typeid(HashJoinStep)))
		return false;

	pDictionaryScan* ipdsp = dynamic_cast<pDictionaryScan*>(it2->get());
	bool onClauseFilter = ipdsp->onClauseFilter();

	JobStepVector::iterator iter = jsv1.begin();
	JobStepVector::iterator end = jsv1.end();

	if (bop == BOP_OR)
	{
		if (jsv1.size() >= 3)
			iter = end - 3;
		else
			return false;
	}

	while (iter != end)
	{
		pDictionaryScan* pdsp = dynamic_cast<pDictionaryScan*>((*iter).get());
		if (pdsp != NULL && pdsp->onClauseFilter() == onClauseFilter)
		{
			// If the OID's match and the BOP's match and the previous step is pcolstep,
			// then append the filters.
			if ((ipdsp->tupleId() == pdsp->tupleId()) &&
				(typeid(*((iter+1)->get())) == typeid(pColStep)))
			{
				if (pdsp->BOP() == BOP_NONE)
				{
 					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->setBOP(bop);
						pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
				else if (pdsp->BOP() == bop)
				{
					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
			}
		}
		++iter;
	}
#endif

	return false;
}

// We want to search the existing filters in the stack for this column to see if we can just add the
// filters for this step to a previous pColStep or pDictionary.
bool tryCombineFilters(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop)
{
	// A couple of tests to make sure we're operating on the right things...
	if (jsv1.size() < 1) return false;

	// if filter by pDictionary, there are two steps: pcolstep and pdictionarystep.
	if (jsv2.size() == 2 && typeid(*jsv2.back().get()) == typeid(pDictionaryStep))
		return tryCombineDictionary(jsv1, jsv2, bop);

	// if filter by pDictionaryScan, there are three steps: pdictionaryscan, pcolstep and join.
	if (jsv2.size() == 3 && typeid(*jsv2.front().get()) == typeid(pDictionaryScan))
		return tryCombineDictionaryScan(jsv1, jsv2, bop);

	// non-dictionary filters
	if (jsv2.size() != 1) return false;

	pColStep* ipcsp = dynamic_cast<pColStep*>(jsv2.back().get());
	if (ipcsp == NULL)
		return false;

	bool onClauseFilter = ipcsp->onClauseFilter();

	JobStepVector::iterator iter = jsv1.begin();
	JobStepVector::iterator end = jsv1.end();

	// only try last step in jsv1 if operator is OR.
	if (bop == BOP_OR)
	{
		iter = end - 1;
	}

	while (iter != end)
	{
		pColStep* pcsp = dynamic_cast<pColStep*>(iter->get());
		if (pcsp != NULL && pcsp->onClauseFilter() == onClauseFilter)
		{
			idbassert(pcsp);
			// If the OID's match and the BOP's match then append the filters
			if (ipcsp->tupleId() == pcsp->tupleId())
			{
				if (pcsp->BOP() == BOP_NONE)
				{
 					if (ipcsp->BOP() == BOP_NONE || ipcsp->BOP() == bop)
					{
						pcsp->appendFilter(ipcsp->filterString(), ipcsp->filterCount());
						pcsp->setBOP(bop);
						pcsp->appendFilter(ipcsp->getFilters());
						return true;
					}
				}
				else if (pcsp->BOP() == bop)
				{
					if (ipcsp->BOP() == BOP_NONE || ipcsp->BOP() == bop)
					{
						pcsp->appendFilter(ipcsp->filterString(), ipcsp->filterCount());
						pcsp->appendFilter(ipcsp->getFilters());
						return true;
					}
				}
			}
		}
		++iter;
	}

	return false;
}


const JobStepVector doConstantFilter(const ConstantFilter* cf, JobInfo& jobInfo)
{
	JobStepVector jsv;
	if (cf == 0) return jsv;
	SOP op = cf->op();
	// default op is 'and'
	string opStr("and");
	if (op) opStr = ba::to_lower_copy(op->data());

	SJSTEP sjstep;
	if (opStr == "and" || opStr == "or")
	{
		SOP sop;

		const SSC sc = boost::dynamic_pointer_cast<SimpleColumn>(cf->col());
		// if column from subquery
		if (!sc || sc->schemaName().empty())
		{
			return doExpressionFilter(cf, jobInfo);
		}

		ConstantFilter::FilterList fl = cf->filterList();
		CalpontSystemCatalog::OID dictOid = 0;
		CalpontSystemCatalog::ColType ct = sc.get()->colType();
		PseudoColumn* pc = dynamic_cast<PseudoColumn*>(sc.get());
//XXX use this before connector sets colType in sc correctly.
//    type of pseudo column is set by connector
		if (!sc->schemaName().empty() && sc->isInfiniDB() && !pc)
			ct = jobInfo.csc->colType(sc->oid());
//X
		CalpontSystemCatalog::OID tbOID = tableOid(sc.get(), jobInfo.csc);
		string alias(extractTableAlias(sc));
		string view(sc->viewName());
		string schema(sc->schemaName());
		if ((dictOid = isDictCol(ct)) > 0)
		{
			if (jobInfo.trace)
				cout << "Emit pTokenByScan/pCol for SimpleColumn op ConstantColumn "
					"[op ConstantColumn]" << endl;

			pColStep* pcs = new pColStep(sc->oid(), tbOID, ct, jobInfo);
			pcs->alias(alias);
			pcs->view(view);
			pcs->name(sc->columnName());
			pcs->schema(sc->schemaName());
			pcs->cardinality(sc->cardinality());

			if (filterWithDictionary(dictOid, jobInfo.stringScanThreshold))
			{
				pDictionaryStep* pds = new pDictionaryStep(dictOid, tbOID, ct, jobInfo);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->schema(sc->schemaName());
				pds->cardinality(sc->cardinality());
				if (op)
					pds->setBOP(bop2num(op));

				//Add the filter(s)
				for (unsigned i = 0; i < fl.size(); i++)
				{
					const SSFP sf = fl[i];
					const ConstantColumn* cc;
					cc = static_cast<const ConstantColumn*>(sf->rhs());
					sop = sf->op();

					//add each filter to pColStep
					int8_t cop = op2num(sop);

					// @bug 2584, make "= null" to COMPARE_NIL.
					if (ConstantColumn::NULLDATA == cc->type() && (opeq == *sop || opne == *sop))
						cop = COMPARE_NIL;

					// trim trailing space char
					string value = cc->constval();
					size_t spos = value.find_last_not_of(" ");
					if (spos != string::npos) value = value.substr(0, spos+1);
					pds->addFilter(cop, value);
				}

				// data list for pcolstep output
				AnyDataListSPtr spdl1(new AnyDataList());
				FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
				spdl1->fifoDL(dl1);
				dl1->OID(sc->oid());

				JobStepAssociation outJs1;
				outJs1.outAdd(spdl1);
				pcs->outputAssociation(outJs1);

				// data list for pdictionarystep output
				AnyDataListSPtr spdl2(new AnyDataList());
				StringFifoDataList* dl2 = new StringFifoDataList(1, jobInfo.fifoSize);
				spdl2->stringDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2;
				outJs2.outAdd(spdl2);
				pds->outputAssociation(outJs2);

				//Associate pcs with pds
				JobStepAssociation outJs;
				outJs.outAdd(spdl1);
				pds->inputAssociation(outJs);

				sjstep.reset(pcs);
				jsv.push_back(sjstep);
				sjstep.reset(pds);
				jsv.push_back(sjstep);

				// save for expression transformation
				pds->addFilter(cf);

				// token column
				CalpontSystemCatalog::ColType tct;
				tct.colDataType = CalpontSystemCatalog::BIGINT;
				tct.colWidth = 8;
				tct.scale = 0;
				tct.precision = 0;
				tct.compressionType = ct.compressionType;
				TupleInfo ti(setTupleInfo(tct, sc->oid(), jobInfo, tbOID, sc.get(), alias));
				jobInfo.keyInfo->token2DictTypeMap[ti.key] = ct;
				pcs->tupleId(ti.key);

				// string column
				ti = setTupleInfo(ct, dictOid, jobInfo, tbOID, sc.get(), alias);
				pds->tupleId(ti.key);
				jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
				jobInfo.tokenOnly[ti.key] = false;
			}
			else
			{
				pDictionaryScan* pds = new pDictionaryScan(dictOid, tbOID, ct,  jobInfo);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc.get()->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc.get()->columnName());
				pds->schema(sc.get()->schemaName());
				if (op)
					pds->setBOP(bop2num(op));

				//Add the filter(s)
				for (unsigned i = 0; i < fl.size(); i++)
				{
					const SSFP sf = fl[i];
					const ConstantColumn* cc;
					cc = static_cast<const ConstantColumn*>(sf->rhs());
					sop = sf->op();

					//add each filter to pColStep
					int8_t cop = op2num(sop);

					// @bug 2584, make "= null" to COMPARE_NIL.
					if (ConstantColumn::NULLDATA == cc->type() && (opeq == *sop || opne == *sop))
						cop = COMPARE_NIL;

					// trim trailing space char
					string value = cc->constval();
					size_t spos = value.find_last_not_of(" ");
					if (spos != string::npos) value = value.substr(0, spos+1);
					pds->addFilter(cop, value);
				}

				// save for expression transformation
				pds->addFilter(cf);

				TupleHashJoinStep* thj = new TupleHashJoinStep(jobInfo);
				thj->tableOid1(0);
				thj->tableOid2(tbOID);
				thj->alias1(alias);
				thj->alias2(alias);
				thj->view1(view);
				thj->view2(view);
				thj->schema1(schema);
				thj->schema2(schema);
				thj->oid1(sc->oid());
				thj->oid2(sc->oid());
				thj->joinId(0);
				thj->setJoinType(INNER);

				//Associate the steps
				AnyDataListSPtr spdl1(new AnyDataList());
				RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
				spdl1->rowGroupDL(dl1);
				dl1->OID(dictOid);

				JobStepAssociation outJs1;
				outJs1.outAdd(spdl1);
				pds->outputAssociation(outJs1);

				AnyDataListSPtr spdl2(new AnyDataList());
				RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
				spdl2->rowGroupDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2;
				outJs2.outAdd(spdl2);
				pcs->outputAssociation(outJs2);

				JobStepAssociation outJs3;
				outJs3.outAdd(spdl1);
				outJs3.outAdd(spdl2);
				thj->inputAssociation(outJs3);

				sjstep.reset(pds);
				jsv.push_back(sjstep);
				sjstep.reset(pcs);
				jsv.push_back(sjstep);
				sjstep.reset(thj);
				jsv.push_back(sjstep);

				CalpontSystemCatalog::ColType dct;
				dct.colDataType = CalpontSystemCatalog::BIGINT;
				dct.colWidth = 8;
				dct.scale = 0;
				dct.precision = 0;
				dct.compressionType = ct.compressionType;

				TupleInfo ti(setTupleInfo(dct, sc->oid(), jobInfo, tbOID, sc.get(), alias));
				jobInfo.keyInfo->token2DictTypeMap[ti.key] = ct;
				pds->tupleId(ti.key); // pcs, pds use same tuple key, both 8-byte column
				pcs->tupleId(ti.key);
				thj->tupleId1(ti.key);
				thj->tupleId2(ti.key);

				if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
					jobInfo.tokenOnly[ti.key] = true;
			}
		}
		else
		{
			if (jobInfo.trace)
				cout << "Emit pCol for SimpleColumn op ConstantColumn [op ConstantColumn]" << endl;

			CalpontSystemCatalog::OID tblOid = tableOid(sc.get(), jobInfo.csc);
			string alias(extractTableAlias(sc));
			pColStep* pcs = NULL;
			if (pc == NULL)
				pcs = new pColStep(sc->oid(), tblOid, ct, jobInfo);
			else
				pcs = new PseudoColStep(sc->oid(), tblOid, pc->pseudoType(), ct, jobInfo);

			pcs->alias(extractTableAlias(sc));
			pcs->view(sc->viewName());
			pcs->name(sc->columnName());
			pcs->schema(sc->schemaName());

			if (sc->isInfiniDB())
			{
				if (op)
					pcs->setBOP(bop2num(op));

				for (unsigned i = 0; i < fl.size(); i++)
				{
					const SSFP sf = fl[i];
					const ConstantColumn* cc;
					cc = static_cast<const ConstantColumn*>(sf->rhs());
					sop = sf->op();

					//add each filter to pColStep
					int8_t cop = op2num(sop);
					int64_t value = 0;
					string constval = cc->constval();
					// trim trailing space char
					size_t spos = constval.find_last_not_of(" ");
					if (spos != string::npos) constval = constval.substr(0, spos+1);

					// @bug 1151 string longer than colwidth of char/varchar.
					uint8_t rf = 0;
					bool isNull = ConstantColumn::NULLDATA == cc->type();
					value = convertValueNum(constval, ct, isNull, rf);
					if (ct.colDataType == CalpontSystemCatalog::FLOAT && !isNull)
					{
						float f = cc->getFloatVal();
						value = *(reinterpret_cast<int32_t*>(&f));
					}
					else if (ct.colDataType == CalpontSystemCatalog::DOUBLE && !isNull)
					{
						double d = cc->getDoubleVal();
						value = *(reinterpret_cast<int64_t*>(&d));
					}

					// @bug 2584, make "= null" to COMPARE_NIL.
					if (ConstantColumn::NULLDATA == cc->type() && (opeq == *sop || opne == *sop))
						cop = COMPARE_NIL;

					pcs->addFilter(cop, value, rf);
				}
			}

			// save for expression transformation
			pcs->addFilter(cf);

			sjstep.reset(pcs);
			jsv.push_back(sjstep);

//XXX use this before connector sets colType in sc correctly.
			CalpontSystemCatalog::ColType ct = sc->colType();
			if (!sc->schemaName().empty() && sc->isInfiniDB() && !pc)
				ct = jobInfo.csc->colType(sc->oid());
			TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc.get(), alias));
//X			TupleInfo ti(setTupleInfo(sc->colType(), sc->oid(), jobInfo, tblOid, sc.get(), alias));
			pcs->tupleId(ti.key);
		}
	}
	else
	{
		cerr << boldStart << "doConstantFilter: Can only handle 'and' and 'or' right now, got '"
			 << opStr << "'" << boldStop << endl;
		throw logic_error("doConstantFilter: Not handled operation type.");
	}

	return jsv;
}


const JobStepVector doFunctionFilter(const ParseTree* n, JobInfo& jobInfo)
{
	FunctionColumn* fc = dynamic_cast<FunctionColumn*>(n->data());
	idbassert(fc);

	JobStepVector jsv;
	string functionName = ba::to_lower_copy(fc->functionName());
	if (functionName.compare("in") == 0 || functionName.compare(" in ") == 0)
	{
		const funcexp::FunctionParm& parms = fc->functionParms();
		FunctionColumn* parm0Fc = dynamic_cast<FunctionColumn*>(parms[0]->data());
		PseudoColumn*   parm0Pc = dynamic_cast<PseudoColumn*>(parms[0]->data());
		vector<vector<string> > constParms(3);
		uint64_t constParmsCount = 0;
		if (parm0Fc && parm0Fc->functionName().compare("idbpartition") == 0)
		{
			for (uint64_t i = 1; i < parms.size(); i++)
			{
				ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[i]->data());
				if (cc)
				{
					vector<string> cv;
					boost::split(cv, cc->constval(), boost::is_any_of("."));
					if (cv.size() == 3)
					{
						constParms[1].push_back(cv[0]);
						constParms[2].push_back(cv[1]);
						constParms[0].push_back(cv[2]);
						constParmsCount++;
					}
				}
			}

			if (constParmsCount == (parms.size() - 1))
			{
				const funcexp::FunctionParm& pcs = parm0Fc->functionParms();
				for (uint64_t i = 0; i < 3; i++)
				{
					ConstantFilter* cf = new ConstantFilter();
					SOP sop(new LogicOperator("or"));
					PseudoColumn* pc = dynamic_cast<PseudoColumn*>(pcs[i]->data());
					idbassert(pc);
					SSC col(pc->clone());
					cf->op(sop);
					cf->col(col);
					sop.reset(new PredicateOperator("="));
					for (uint64_t j = 0; j < constParmsCount; j++)
					{
						SimpleFilter* f = new SimpleFilter(sop, col->clone(),
						                                   new ConstantColumn(constParms[i][j]));
						cf->pushFilter(f);
					}

					JobStepVector sv = doConstantFilter(cf, jobInfo);
					delete cf;

					jsv.insert(jsv.end(), sv.begin(), sv.end());
				}
			}

			// put the separate filtered resulted together
			JobStepVector sv = doExpressionFilter(n, jobInfo);
			jsv.insert(jsv.end(), sv.begin(), sv.end());
		}
		else if (parm0Pc != NULL)
		{
			for (uint64_t i = 1; i < parms.size(); i++)
			{
				ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[i]->data());
				if (cc)
				{
					constParms[0].push_back(cc->constval());
					constParmsCount++;
				}
			}

			if (constParmsCount == (parms.size() - 1))
			{
				ConstantFilter* cf = new ConstantFilter();
				SOP sop(new LogicOperator("or"));
				SSC col(parm0Pc->clone());
				cf->op(sop);
				cf->col(col);
				sop.reset(new PredicateOperator("="));
				for (uint64_t j = 0; j < constParmsCount; j++)
				{
					SimpleFilter* f = new SimpleFilter(sop, col->clone(),
					                                   new ConstantColumn(constParms[0][j]));
					cf->pushFilter(f);
				}

				JobStepVector sv = doConstantFilter(cf, jobInfo);
				delete cf;

				jsv.insert(jsv.end(), sv.begin(), sv.end());
			}
		}
	}

	if (jsv.empty())
		jsv = doExpressionFilter(n, jobInfo);

	return jsv;
}


void doAND(JobStepVector& jsv, JobInfo& jobInfo)
{
//	idbassert(jobInfo.stack.size() >= 2);
	if (jobInfo.stack.size() < 2)
		return;

	JobStepVector rhv = jobInfo.stack.top();
	jobInfo.stack.pop();
	jsv = jobInfo.stack.top();
	jobInfo.stack.pop();

	// if the jobstep vector on any side of the operator is empty,
	// just push them to the stack without further process
	// # empty vector could be the result of an unhandled filter #
	if (jsv.size() == 0 || rhv.size() == 0)
	{
		jsv.insert(jsv.end(), rhv.begin(), rhv.end());
		jobInfo.stack.push(jsv);
		return;
	}

	//We need to do one optimization here. We can get multiple filters on the same column.
	// We should combine these here into one pColStep.
	// A query like "select LOG_KEY from F_TRANS where RECORD_TIMESTAMP between
	// '2007-01-01 00:00:00' and '2007-01-31 00:00:00';"  will cause this on the
	// RECORD_TIMESTAMP col
	// @bug 618 The filters are not always next to each other, so scan the whole jsv.

	if (tryCombineFilters(jsv, rhv, BOP_AND))
	{
		jobInfo.stack.push(jsv);
		return;
	}

	// concat rhv into jsv
	jsv.insert(jsv.end(), rhv.begin(), rhv.end());
	jobInfo.stack.push(jsv);
}


void doOR(const ParseTree* n, JobStepVector& jsv, JobInfo& jobInfo, bool tryCombine)
{
	idbassert(jobInfo.stack.size() >= 2);
	JobStepVector rhv = jobInfo.stack.top();
	jobInfo.stack.pop();
	jsv = jobInfo.stack.top();
	jobInfo.stack.pop();

	// @bug3570, attempt to combine only if there is one column involved.
	if (tryCombine && ((jsv.size() == 1 && dynamic_cast<pColStep*>(jsv.begin()->get()) != NULL) ||
		 (jsv.size() == 2 && dynamic_cast<pDictionaryStep*>((jsv.end()-1)->get()) != NULL) ||
		 (jsv.size() == 3 && dynamic_cast<pDictionaryScan*>(jsv.begin()->get()) != NULL)) &&
 		tryCombineFilters(jsv, rhv, BOP_OR))
	{
		jobInfo.stack.push(jsv);
		return;
	}

	// OR is processed as an expression
	jsv = doExpressionFilter(n, jobInfo);
	jobInfo.stack.push(jsv);
}


} // end of unnamed namespace


namespace joblist
{

// This method is the entry point into the execution plan to joblist
// conversion performed by the functions in this file.
/* static */ void
JLF_ExecPlanToJobList::walkTree(ParseTree* n, void* obj)
{
	JobInfo* jobInfo = reinterpret_cast<JobInfo*>(obj);
	TreeNode* tn = n->data();
	JobStepVector jsv;
	const Operator* op = 0;
	switch (TreeNode2Type(tn))
	{
	case SIMPLEFILTER:
		jsv = doSimpleFilter(dynamic_cast<SimpleFilter*>(tn), *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case OUTERJOINONFILTER:
		jsv = doOuterJoinOnFilter(dynamic_cast<OuterJoinOnFilter*>(tn), *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case OPERATOR:
		op = static_cast<const Operator*>(tn);
		if (*op == opAND || *op == opand)
		{
			doAND(jsv, *jobInfo);
		}
		else if (*op == opOR || *op == opor)
		{
			doOR(n, jsv, *jobInfo, true);
		}
		else if (*op == opXOR || *op == opxor)
		{
			doOR(n, jsv, *jobInfo, false);
		}
		else
		{
			cerr << boldStart
				 << "walkTree: only know how to handle 'and' and 'or' right now, got: " << *op
				 << boldStop << endl;
		}
		break;
	case CONSTANTFILTER:
		//cout << "ConstantFilter" << endl;
		jsv = doConstantFilter(dynamic_cast<const ConstantFilter*>(tn), *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case FUNCTIONCOLUMN:
		jsv = doFunctionFilter(n, *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case ARITHMETICCOLUMN:
		jsv = doExpressionFilter(n, *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case SIMPLECOLUMN:
		jsv = doExpressionFilter(n, *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case CONSTANTCOLUMN:
		jsv = doConstantBooleanFilter(n, *jobInfo);
		jobInfo->stack.push(jsv);
		break;
	case SIMPLESCALARFILTER:
		doSimpleScalarFilter(n, *jobInfo);
		break;
	case EXISTSFILTER:
		doExistsFilter(n, *jobInfo);
		break;
	case SELECTFILTER:
		doSelectFilter(n, *jobInfo);
		break;
	case UNKNOWN:
		cerr << boldStart << "walkTree: Unknown" << boldStop << endl;
		throw logic_error("walkTree: unknow type.");
		break;
	default:
/*
	TREENODE,
	FILTER,
	RETURNEDCOLUMN,
	AGGREGATECOLUMN,
	ARITHMETICCOLUMN,
	SIMPLECOLUMN,
	CONSTANTCOLUMN,
	TREENODEIMPL,
*/
		cerr << boldStart << "walkTree: Not handled: " << TreeNode2Type(tn) << boldStop << endl;
		throw logic_error("walkTree: Not handled treeNode type.");
		break;
	}
	//cout << *tn << endl;
}

} // end of joblist namespace
// vim:ts=4 sw=4:

