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

//  $Id: jlf_execplantojoblist.cpp 8752 2012-07-26 22:08:02Z xlou $


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
#include <boost/algorithm/string/case_conv.hpp>
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
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "simplecolumn_int.h"
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
#include "bucketdl.h"
#include "hashjoin.h"
#include "joblist.h"
#include "jobstep.h"
#include "jl_logger.h"
#include "pidxlist.h"
#include "pidxwalk.h"
#include "largedatalist.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "expressionstep.h"
#include "tupleconstantstep.h"

#include "jlf_common.h"
#include "jlf_subquery.h"

#define FIFODEBUG() {} //  do{cout<<"new FifoDataList allocated at: "<<__FILE__<<':'<<__LINE__<<endl;}while(0)

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
	case CalpontSystemCatalog::SMALLINT:
		n = boost::any_cast<short>(anyVal);
		break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
		n = boost::any_cast<int>(anyVal);
		break;
	case CalpontSystemCatalog::BIGINT:
		n = boost::any_cast<long long>(anyVal);
		break;
	case CalpontSystemCatalog::FLOAT:
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
			string i = boost::any_cast<string>(anyVal);
		  	n = *((uint64_t *) i.c_str());
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
		v = boost::any_cast<char>(anyVal);
		break;
	case CalpontSystemCatalog::SMALLINT:
		v = boost::any_cast<int16_t>(anyVal);
		break;
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
#ifdef _MSC_VER
		v = boost::any_cast<int>(anyVal);
#else
		v = boost::any_cast<int32_t>(anyVal);
#endif
		break;
	case CalpontSystemCatalog::BIGINT:
		v = boost::any_cast<long long>(anyVal);
		break;
	case CalpontSystemCatalog::FLOAT:
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
		  	v = *ip;
		}
		break;
	case CalpontSystemCatalog::DOUBLE:
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
		 ct.colDataType == CalpontSystemCatalog::DECIMAL) &&
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
		typeid(*tn) == typeid(SimpleColumn_INT<8>))
		return SIMPLECOLUMN;
	if (typeid(*tn) == typeid(SimpleColumn_Decimal<1>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<2>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<4>) ||
		typeid(*tn) == typeid(SimpleColumn_Decimal<8>))
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

// @Bug 1955
// Don't allow filter on "incompatible" cols
// Compatible columns:
// any 1,2,4,8-byte int/decimal to any 1,2,4,8-byte int/decimal
// date to date
// datetime to datetime
// string to string
bool compatibleFilterColumns(const SimpleColumn* sc1, const CalpontSystemCatalog::ColType& ct1,
	const SimpleColumn* sc2, const CalpontSystemCatalog::ColType& ct2)
{
	switch (ct1.colDataType)
	{
	case CalpontSystemCatalog::BIT:
		if (ct2.colDataType != CalpontSystemCatalog::BIT) return false;
		break;
	case CalpontSystemCatalog::TINYINT:
	case CalpontSystemCatalog::SMALLINT:
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::BIGINT:
	case CalpontSystemCatalog::DECIMAL:
		if (ct2.colDataType != CalpontSystemCatalog::TINYINT &&
			ct2.colDataType != CalpontSystemCatalog::SMALLINT &&
			ct2.colDataType != CalpontSystemCatalog::MEDINT &&
			ct2.colDataType != CalpontSystemCatalog::INT &&
			ct2.colDataType != CalpontSystemCatalog::BIGINT &&
			ct2.colDataType != CalpontSystemCatalog::DECIMAL) return false;
		break;
	case CalpontSystemCatalog::DATE:
		if (ct2.colDataType != CalpontSystemCatalog::DATE) return false;
		break;
	case CalpontSystemCatalog::DATETIME:
		if (ct2.colDataType != CalpontSystemCatalog::DATETIME) return false;
		break;
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::CHAR:
		if (ct2.colDataType != CalpontSystemCatalog::VARCHAR &&
			ct2.colDataType != CalpontSystemCatalog::CHAR) return false;
		break;
	case CalpontSystemCatalog::VARBINARY:
		if (ct2.colDataType != CalpontSystemCatalog::VARBINARY) return false;
		break;
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::DOUBLE:
		if (ct2.colDataType != CalpontSystemCatalog::FLOAT &&
			ct2.colDataType != CalpontSystemCatalog::DOUBLE) return false;
		break;
	default:
		return false;
		break;
	}

	return true;
}

const JobStepVector doColFilter(const SimpleColumn* sc1, const SimpleColumn* sc2, JobInfo& jobInfo, const SOP& sop)
{
	//The idea here is to take the two SC's and pipe them into a filter step.
	//The output of the filter step is one DL that is the minimum rid list met the condition.
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
	string alias1(extractTableAlias(sc1));
	string alias2(extractTableAlias(sc2));
	int8_t op = op2num(sop);

	pColStep* pcss1 = new pColStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0, jobInfo.csc, sc1->oid(), tableOid1,
			jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
	CalpontSystemCatalog::ColType ct1 = jobInfo.csc->colType(sc1->oid());
	CalpontSystemCatalog::OID dictOid1 = isDictCol(ct1);
	pcss1->logger(jobInfo.logger);
	pcss1->alias(alias1);
	pcss1->view(sc1->viewName());
	pcss1->name(sc1->columnName());
	pcss1->cardinality(sc1->cardinality());
	pcss1->setFeederFlag(true);

	pColStep* pcss2 = new pColStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0, jobInfo.csc, sc2->oid(), tableOid2,
			jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
	CalpontSystemCatalog::ColType ct2 = jobInfo.csc->colType(sc2->oid());
	CalpontSystemCatalog::OID dictOid2 = isDictCol(ct2);
	pcss2->logger(jobInfo.logger);
	pcss2->alias(alias2);
	pcss2->view(sc2->viewName());
	pcss2->name(sc2->columnName());
	pcss2->cardinality(sc2->cardinality());
	pcss2->setFeederFlag(true);

	//Associate the steps
	JobStepVector jsv;

	if (jobInfo.tryTuples)
	{
		TupleInfo ti1(setTupleInfo(ct1, sc1->oid(), jobInfo, tableOid1, sc1, alias1));
		pcss1->tupleId(ti1.key);
		TupleInfo ti2(setTupleInfo(ct2, sc2->oid(), jobInfo, tableOid2, sc2, alias2));
		pcss2->tupleId(ti2.key);
	}

	// check if they are string columns greater than 8 bytes.
	if ((!isDictCol(ct1)) && (!isDictCol(ct2)))
	{
		// not strings, no need for dictionary steps, output fifo datalist
		AnyDataListSPtr spdl1(new AnyDataList());
FIFODEBUG();
		FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
		spdl1->fifoDL(dl1);
		dl1->OID(sc1->oid());

		JobStepAssociation outJs1(jobInfo.status);
		outJs1.outAdd(spdl1);
		pcss1->outputAssociation(outJs1);

		AnyDataListSPtr spdl2(new AnyDataList());
FIFODEBUG();
		FifoDataList* dl2 = new FifoDataList(1, jobInfo.fifoSize);
		spdl2->fifoDL(dl2);
		dl2->OID(sc2->oid());

		JobStepAssociation outJs2(jobInfo.status);
		outJs2.outAdd(spdl2);
		pcss2->outputAssociation(outJs2);
		pcss2->inputAssociation(outJs1);


		FilterStep* filt=new FilterStep(jobInfo.sessionId, jobInfo.txnId, jobInfo.statementId, ct1);
		filt->logger(jobInfo.logger);
		filt->alias(extractTableAlias(sc1));
		filt->tableOid(tableOid1);
		filt->name(pcss1->name()+","+pcss2->name());
		if (op)
			filt->setBOP(op);

		JobStepAssociation outJs3(jobInfo.status);
		outJs3.outAdd(spdl1);
		outJs3.outAdd(spdl2);
		filt->inputAssociation(outJs3);

		SJSTEP step;
		step.reset(pcss1);
		jsv.push_back(step);
		step.reset(pcss2);
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
			pDictionaryStep* pdss1 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0,
				jobInfo.csc, dictOid1, ct1.ddn.compressionType, tableOid1, jobInfo.sessionId,
				jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
			pdss1->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc1->oid();
			pdss1->alias(extractTableAlias(sc1));
			pdss1->view(sc1->viewName());
			pdss1->name(sc1->columnName());
			pdss1->cardinality(sc1->cardinality());

			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1(jobInfo.status);
			outJs1.outAdd(spdl11);
			pcss1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());

			JobStepAssociation outJs2(jobInfo.status);
			outJs2.outAdd(spdl12);
			pdss1->outputAssociation(outJs2);

			//Associate pcss1 with pdss1
			JobStepAssociation outJs11(jobInfo.status);
			outJs11.outAdd(spdl11);
			pdss1->inputAssociation(outJs11);


			pDictionaryStep* pdss2 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0,
				jobInfo.csc, dictOid2, ct2.ddn.compressionType, tableOid2, jobInfo.sessionId, jobInfo.txnId,
				jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
			pdss2->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid2] = sc2->oid();
			pdss2->alias(extractTableAlias(sc2));
			pdss2->view(sc2->viewName());
			pdss2->name(sc2->columnName());
			pdss2->cardinality(sc2->cardinality());


			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3(jobInfo.status);
			outJs3.outAdd(spdl21);
			pcss2->outputAssociation(outJs3);
			pcss2->inputAssociation(outJs2);
			//Associate pcss2 with pdss2
			JobStepAssociation outJs22(jobInfo.status);
			outJs22.outAdd(spdl21);
			pdss2->inputAssociation(outJs22);

			// data list for column 2 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl22(new AnyDataList());
			StringFifoDataList* dl22 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl22->stringDL(dl22);
			dl22->OID(sc2->oid());

			JobStepAssociation outJs4(jobInfo.status);
			outJs4.outAdd(spdl22);
			pdss2->outputAssociation(outJs4);

			FilterStep* filt = new FilterStep(jobInfo.sessionId, jobInfo.txnId,
				jobInfo.statementId, ct1);
			filt->logger(jobInfo.logger);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			filt->name(pcss1->name()+","+pcss2->name());
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5(jobInfo.status);
			outJs5.outAdd(spdl12);
			outJs5.outAdd(spdl22);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcss1);
			jsv.push_back(step);
			step.reset(pdss1);
			jsv.push_back(step);
			step.reset(pcss2);
			jsv.push_back(step);
			step.reset(pdss2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			if (jobInfo.tryTuples)
			{
				TupleInfo ti1(setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1));
				pdss1->tupleId(ti1.key);
				jobInfo.keyInfo->dictKeyMap[pcss1->tupleId()] = ti1.key;
				jobInfo.tokenOnly[pcss1->tupleId()] = false;

				TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
				pdss2->tupleId(ti2.key);
				jobInfo.keyInfo->dictKeyMap[pcss2->tupleId()] = ti2.key;
				jobInfo.tokenOnly[pcss2->tupleId()] = false;
			}
		}
		else if ((isDictCol(ct1) != 0 ) && (isDictCol(ct2) ==0 )) //col1 is dictionary column
		{
			// extra steps for string column greater than eight bytes -- from token to string
			pDictionaryStep* pdss1 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid1, ct1.ddn.compressionType, tableOid1,
				jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
			pdss1->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc1->oid();
			pdss1->alias(extractTableAlias(sc1));
			pdss1->view(sc1->viewName());
			pdss1->name(sc1->columnName());
			pdss1->cardinality(sc1->cardinality());

			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1(jobInfo.status);
			outJs1.outAdd(spdl11);
			pcss1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());

			JobStepAssociation outJs2(jobInfo.status);
			outJs2.outAdd(spdl12);
			pdss1->outputAssociation(outJs2);

			//Associate pcss1 with pdss1
			JobStepAssociation outJs11(jobInfo.status);
			outJs11.outAdd(spdl11);
			pdss1->inputAssociation(outJs11);

			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3(jobInfo.status);
			outJs3.outAdd(spdl21);
			pcss2->outputAssociation(outJs3);
			pcss2->inputAssociation(outJs2);

			FilterStep* filt = new FilterStep(jobInfo.sessionId, jobInfo.txnId,
				jobInfo.statementId, ct1);
			filt->logger(jobInfo.logger);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5(jobInfo.status);
			outJs5.outAdd(spdl12);
			outJs5.outAdd(spdl21);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcss1);
			jsv.push_back(step);
			step.reset(pdss1);
			jsv.push_back(step);
			step.reset(pcss2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			if (jobInfo.tryTuples)
			{
				TupleInfo ti1(setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1));
				pdss1->tupleId(ti1.key);
				jobInfo.keyInfo->dictKeyMap[pcss1->tupleId()] = ti1.key;
				jobInfo.tokenOnly[pcss1->tupleId()] = false;
			}
		}
		else // if ((isDictCol(ct1) == 0 ) && (isDictCol(ct2) !=0 )) //col2 is dictionary column
		{
			// extra steps for string column greater than eight bytes -- from token to string
			// data list for column 1 step 1 (pcolstep) output
			AnyDataListSPtr spdl11(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
			spdl11->fifoDL(dl11);
			dl11->OID(sc1->oid());

			JobStepAssociation outJs1(jobInfo.status);
			outJs1.outAdd(spdl11);
			pcss1->outputAssociation(outJs1);

			// data list for column 1 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl12(new AnyDataList());
			StringFifoDataList* dl12 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl12->stringDL(dl12);
			dl12->OID(sc1->oid());


			pDictionaryStep* pdss2 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid2, ct2.ddn.compressionType, tableOid2,
				jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
			pdss2->logger(jobInfo.logger);
			jobInfo.keyInfo->dictOidToColOid[dictOid2] = sc2->oid();
			pdss2->alias(extractTableAlias(sc2));
			pdss2->view(sc2->viewName());
			pdss2->name(sc2->columnName());
			pdss2->cardinality(sc2->cardinality());


			// data list for column 2 step 1 (pcolstep) output
			AnyDataListSPtr spdl21(new AnyDataList());
FIFODEBUG();
			FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
			spdl21->fifoDL(dl21);
			dl21->OID(sc2->oid());

			JobStepAssociation outJs3(jobInfo.status);
			outJs3.outAdd(spdl21);
			pcss2->outputAssociation(outJs3);
			pcss2->inputAssociation(outJs1);
			//Associate pcss2 with pdss2
			JobStepAssociation outJs22(jobInfo.status);
			outJs22.outAdd(spdl21);
			pdss2->inputAssociation(outJs22);

			// data list for column 2 step 2 (pdictionarystep) output
			AnyDataListSPtr spdl22(new AnyDataList());
			StringFifoDataList* dl22 = new StringFifoDataList(1, jobInfo.fifoSize);
			spdl22->stringDL(dl22);
			dl22->OID(sc2->oid());

			JobStepAssociation outJs4(jobInfo.status);
			outJs4.outAdd(spdl22);
			pdss2->outputAssociation(outJs4);

			FilterStep* filt = new FilterStep(
				jobInfo.sessionId,
				jobInfo.txnId,
				jobInfo.statementId, ct1);
			filt->logger(jobInfo.logger);
			filt->alias(extractTableAlias(sc1));
			filt->tableOid(tableOid1);
			if (op)
				filt->setBOP((op));

			JobStepAssociation outJs5(jobInfo.status);
			outJs5.outAdd(spdl11);
			outJs5.outAdd(spdl22);
			filt->inputAssociation(outJs5);

			SJSTEP step;
			step.reset(pcss1);
			jsv.push_back(step);
			step.reset(pcss2);
			jsv.push_back(step);
			step.reset(pdss2);
			jsv.push_back(step);
			step.reset(filt);
			jsv.push_back(step);

			if (jobInfo.tryTuples)
			{
				TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
				pdss2->tupleId(ti2.key);
				jobInfo.keyInfo->dictKeyMap[pcss2->tupleId()] = ti2.key;
				jobInfo.tokenOnly[pcss2->tupleId()] = false;
			}
		}
	}
	else
	{
		cerr << boldStart << "Filterstep: Filter with different type is not supported " << boldStop << endl;
		throw QueryDataExcept("Filter with different column types is not supported.", incompatFilterCols);
	}

	return jsv;
}

bool sameTable(const SimpleColumn* sc1, const SimpleColumn* sc2)
{
	if (sc1->schemaName() != sc2->schemaName()) return false;
	if (sc1->tableName() != sc2->tableName()) return false;
	return true;
}

// @Bug 1230
// Don't allow join on "incompatible" cols
// Compatible columns:
// any 1,2,4,8-byte int to any 1,2,4,8-byte int
// decimal w/scale x to decimal w/scale x
// date to date
// datetime to datetime
// string to string
bool compatibleJoinColumns(const SimpleColumn* sc1, const CalpontSystemCatalog::ColType& ct1,
	const SimpleColumn* sc2, const CalpontSystemCatalog::ColType& ct2, bool tryTuples)
{
	// disable VARBINARY used in join
	if (ct1.colDataType == CalpontSystemCatalog::VARBINARY ||
		ct2.colDataType == CalpontSystemCatalog::VARBINARY)
		throw runtime_error("VARBINARY in join is not supported.");

	switch (ct1.colDataType)
	{
	case CalpontSystemCatalog::BIT:
		if (ct2.colDataType != CalpontSystemCatalog::BIT) return false;
		break;
	case CalpontSystemCatalog::TINYINT:
	case CalpontSystemCatalog::SMALLINT:
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::BIGINT:
	case CalpontSystemCatalog::DECIMAL:
		if (ct2.colDataType != CalpontSystemCatalog::TINYINT &&
			ct2.colDataType != CalpontSystemCatalog::SMALLINT &&
			ct2.colDataType != CalpontSystemCatalog::MEDINT &&
			ct2.colDataType != CalpontSystemCatalog::INT &&
			ct2.colDataType != CalpontSystemCatalog::BIGINT &&
			ct2.colDataType != CalpontSystemCatalog::DECIMAL) return false;
			if (ct2.scale != ct1.scale) return false;
		break;
	case CalpontSystemCatalog::DATE:
		if (ct2.colDataType != CalpontSystemCatalog::DATE) return false;
		break;
	case CalpontSystemCatalog::DATETIME:
		if (ct2.colDataType != CalpontSystemCatalog::DATETIME) return false;
		break;
	case CalpontSystemCatalog::VARCHAR:
		// @bug 1495 compound/string join
		if (tryTuples && (ct2.colDataType == CalpontSystemCatalog::VARCHAR ||
							ct2.colDataType == CalpontSystemCatalog::CHAR))
			break;
		// @bug 1920. disable joins on dictionary column
		if (ct1.colWidth > 7 ) return false;
		if (ct2.colDataType != CalpontSystemCatalog::VARCHAR && ct2.colDataType != CalpontSystemCatalog::CHAR) return false;
		if (ct2.colDataType == CalpontSystemCatalog::VARCHAR && ct2.colWidth > 7) return false;
		if (ct2.colDataType == CalpontSystemCatalog::CHAR && ct2.colWidth > 8) return false;   
		break;  
	case CalpontSystemCatalog::CHAR:
		// @bug 1495 compound/string join
		if (tryTuples && (ct2.colDataType == CalpontSystemCatalog::VARCHAR ||
							ct2.colDataType == CalpontSystemCatalog::CHAR))
			break;
		// @bug 1920. disable joins on dictionary column
		if (ct1.colWidth > 8 ) return false;
		if (ct2.colDataType != CalpontSystemCatalog::VARCHAR && ct2.colDataType != CalpontSystemCatalog::CHAR) return false;
		if (ct2.colDataType == CalpontSystemCatalog::VARCHAR && ct2.colWidth > 7) return false;
		if (ct2.colDataType == CalpontSystemCatalog::CHAR && ct2.colWidth > 8) return false;   
		break;
	case CalpontSystemCatalog::VARBINARY:
		if (ct2.colDataType != CalpontSystemCatalog::VARBINARY) return false;
		break;
/*
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::DOUBLE:
		if (ct2.colDataType != CalpontSystemCatalog::FLOAT &&
			ct2.colDataType != CalpontSystemCatalog::DOUBLE) return false;
		break;
*/
	default:
		return false;
		break;
	}

	return true;
}

const JobStepVector doFilterExpression(const SimpleColumn* sc1, const SimpleColumn* sc2, JobInfo& jobInfo, const SOP& sop)
{
	JobStepVector jsv;
	SJSTEP sjstep;
	ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
											jobInfo.txnId,
											jobInfo.verId,
											jobInfo.statementId);

	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 2");

	es->logger(jobInfo.logger);

	SimpleFilter sf;
	sf.op(sop);
	sf.lhs(sc1->clone());
	sf.rhs(sc2->clone());
	es->expressionFilter(&sf, jobInfo);

	sjstep.reset(es);
	jsv.push_back(sjstep);

	return jsv;
}

const JobStepVector doJoin(SimpleColumn* sc1, SimpleColumn* sc2, JobInfo& jobInfo, const SOP& sop)
{
	//The idea here is to take the two SC's and pipe them into a HJ step. The output of the HJ step
	// is 2 DL's (one for each table) that are the minimum rid list for each side of the join.
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
	string alias1(extractTableAlias(sc1));
	string alias2(extractTableAlias(sc2));
	string view1(sc1->viewName());
	string view2(sc2->viewName());

	CalpontSystemCatalog::ColType ct1 = jobInfo.csc->colType(sc1->oid());
	CalpontSystemCatalog::ColType ct2 = jobInfo.csc->colType(sc2->oid());
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
		if (sc1->schemaName().empty() || !compatibleFilterColumns(sc1, ct1, sc2, ct2))
		{
			if (jobInfo.tryTuples)
				return doFilterExpression(sc1, sc2, jobInfo, sop);
			else
				throw QueryDataExcept( "Incompatible or non-support column types specified for filter condition", incompatFilterCols);
		}

		JobStepVector colFilter = doColFilter(sc1, sc2, jobInfo, sop);
		//jsv.insert(jsv.end(), colFilter.begin(), colFilter.end());
		return colFilter;
	}

	// different tables
	if (!compatibleJoinColumns(sc1, ct1, sc2, ct2, jobInfo.tryTuples))
	{
		if (!jobInfo.tryTuples)
			throw QueryDataExcept("Incompatible or non-support column types specified for join condition", incompatJoinCols);

		JobStepVector jsv;
		jsv = doFilterExpression(sc1, sc2, jobInfo, sop);
		uint t1 = tableKey(jobInfo, tableOid1, alias1, view1);
		uint t2 = tableKey(jobInfo, tableOid2, alias2, view2);
		jobInfo.incompatibleJoinMap[t1] = t2;
		jobInfo.incompatibleJoinMap[t2] = t1;

		return jsv;
	}

	pColStep* pcs1 = NULL;
	CalpontSystemCatalog::OID oid1 = sc1->oid();
	CalpontSystemCatalog::OID dictOid1 = isDictCol(ct1);
	if (sc1->schemaName().empty() == false)
	{
		pcs1 = new pColStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0, jobInfo.csc, oid1, tableOid1,
			jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
		pcs1->logger(jobInfo.logger);
		pcs1->alias(alias1);
		pcs1->view(view1);
		pcs1->name(sc1->columnName());
		pcs1->cardinality(sc1->cardinality());
	}

	pColStep* pcs2 = NULL;
	CalpontSystemCatalog::OID oid2 = sc2->oid();
	CalpontSystemCatalog::OID dictOid2 = isDictCol(ct2);
	if (sc2->schemaName().empty() == false)
	{
		pcs2 = new pColStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0, jobInfo.csc, oid2, tableOid2,
			jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
		pcs2->logger(jobInfo.logger);
		pcs2->alias(alias2);
		pcs2->view(view2);
		pcs2->name(sc2->columnName());
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

	// check if this is a string join
	if (((dictOid1 == 0) && (dictOid2 == 0))
		// bug 1495 compound join, v-table handles string join and compound join the same way
		|| jobInfo.tryTuples)
	{
		// not strings, no need for dictionary steps, output banded or bucket datalist
		AnyDataListSPtr spdl1(new AnyDataList());
		if (jobInfo.tryTuples)
		{
			RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
			spdl1->rowGroupDL(dl1);
			dl1->OID(oid1);
		}
		else
		{
			FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
			spdl1->fifoDL(dl1);
			dl1->OID(oid1);
		}

		if (pcs1)
		{
			JobStepAssociation outJs1(jobInfo.status);
			outJs1.outAdd(spdl1);
			pcs1->outputAssociation(outJs1);

			step.reset(pcs1);
			jsv.push_back(step);
		}

		AnyDataListSPtr spdl2(new AnyDataList());
		if (jobInfo.tryTuples)
		{
			RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
			spdl2->rowGroupDL(dl2);
			dl2->OID(oid2);
		}
		else
		{
			FifoDataList* dl2 = new FifoDataList(1, jobInfo.fifoSize);
			spdl2->fifoDL(dl2);
			dl2->OID(oid2);
		}

		if (pcs2)
		{
			JobStepAssociation outJs2(jobInfo.status);
			outJs2.outAdd(spdl2);
			pcs2->outputAssociation(outJs2);

			step.reset(pcs2);
			jsv.push_back(step);
		}

		if (jobInfo.tryTuples)
		{
			TupleHashJoinStep* thj = new TupleHashJoinStep(
				jobInfo.sessionId,
				jobInfo.txnId,
				jobInfo.statementId,
				&jobInfo.rm);
			thj->logger(jobInfo.logger);
			thj->tableOid1(tableOid1);
			thj->tableOid2(tableOid2);
			thj->alias1(alias1);
			thj->alias2(alias2);
			thj->view1(view1);
			thj->view2(view2);
			thj->oid1(oid1);
			thj->oid2(oid2);
			thj->dictOid1(dictOid1);
			thj->dictOid2(dictOid2);
			thj->sequence1(sc1->sequence());
			thj->sequence2(sc2->sequence());
			thj->column1(sc1);
			thj->column2(sc2);
//			thj->joinId(jobInfo.joinNum++);
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

			JobStepAssociation outJs3(jobInfo.status);
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
//					thj->tupleId1(ti1.key);
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
//					thj->tupleId2(ti2.key);
				}
			}
			else
			{
				thj->tupleId2(getTupleKey(jobInfo, sc2));
			}
		}
		else
		{
			HashJoinStep* hj = new HashJoinStep(jt,
				jobInfo.sessionId,
				jobInfo.txnId,
				jobInfo.statementId,
				&jobInfo.rm);
			hj->logger(jobInfo.logger);
			hj->tableOid1(tableOid1);
			hj->tableOid2(tableOid2);
			//@bug 598 self-join
			hj->alias1(alias1);
			hj->alias2(alias2);
			hj->view1(view1);
			hj->view2(view2);

			JobStepAssociation outJs3(jobInfo.status);
			outJs3.outAdd(spdl1);
			outJs3.outAdd(spdl2);
			hj->inputAssociation(outJs3);
			step.reset(hj);
		}

		jsv.push_back(step);
	}
	// table mode only support dictionary-dictionary join
	else if ((dictOid1 > 0) && (dictOid2 > 0))
	{
		// extra steps for string join -- from token to string
		pDictionaryStep* pds1 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0,
			jobInfo.csc, dictOid1, ct1.ddn.compressionType, tableOid1, jobInfo.sessionId, jobInfo.txnId,
			jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
		pds1->logger(jobInfo.logger);
		jobInfo.keyInfo->dictOidToColOid[dictOid1] = sc1->oid();
		pds1->alias(alias1);
		pds1->view(view1);
		pds1->name(sc1->columnName());
		pds1->cardinality(sc1->cardinality());

		pDictionaryStep* pds2 = new pDictionaryStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0,
			jobInfo.csc, dictOid2, ct2.ddn.compressionType, tableOid2, jobInfo.sessionId, jobInfo.txnId,
			jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
		pds2->logger(jobInfo.logger);
		jobInfo.keyInfo->dictOidToColOid[dictOid2] = sc2->oid();
		pds2->alias(alias2);
		pds2->view(view2);
		pds2->name(sc2->columnName());
		pds2->cardinality(sc2->cardinality());

		// data list for table 1 step 1 (pcolscanstep) output
		AnyDataListSPtr spdl11(new AnyDataList());
FIFODEBUG();
		FifoDataList* dl11 = new FifoDataList(1, jobInfo.fifoSize);
		spdl11->fifoDL(dl11);
		dl11->OID(sc1->oid());

		JobStepAssociation outJs1(jobInfo.status);
		outJs1.outAdd(spdl11);
		pcs1->outputAssociation(outJs1);

		// data list for table 1 step 2 (pdictionarystep) output
		AnyDataListSPtr spdl12(new AnyDataList());
		StringBucketDataList* dl12 = new StringBucketDataList(jobInfo.maxBuckets, 1, jobInfo.maxElems, jobInfo.rm);
		dl12->setHashMode(1);
		spdl12->stringBucketDL(dl12);
		dl12->OID(dictOid1);

		JobStepAssociation outJs2(jobInfo.status);
		outJs2.outAdd(spdl12);
		pds1->outputAssociation(outJs2);

		// data list for table 2 step 1 (pcolscanstep) output
		AnyDataListSPtr spdl21(new AnyDataList());
FIFODEBUG();
		FifoDataList* dl21 = new FifoDataList(1, jobInfo.fifoSize);
		spdl21->fifoDL(dl21);
		dl21->OID(sc2->oid());

		JobStepAssociation outJs3(jobInfo.status);
		outJs3.outAdd(spdl21);
		pcs2->outputAssociation(outJs3);

		// data list for table 2 step 2 (pdictionarystep) output
		AnyDataListSPtr spdl22(new AnyDataList());
		StringBucketDataList* dl22 = new StringBucketDataList(jobInfo.maxBuckets, 1, jobInfo.maxElems, jobInfo.rm);
		dl22->setHashMode(1);
		spdl22->stringBucketDL(dl22);
		dl22->OID(dictOid2);

		JobStepAssociation outJs4(jobInfo.status);
		outJs4.outAdd(spdl22);
		pds2->outputAssociation(outJs4);

		StringHashJoinStep* hj = new StringHashJoinStep(jt,
			jobInfo.sessionId,
			jobInfo.txnId,
			jobInfo.statementId,
			jobInfo.rm);
		hj->logger(jobInfo.logger);
		hj->tableOid1(tableOid1);
		hj->tableOid2(tableOid2);
		//@bug 598 self-join
		hj->alias1(alias1);
		hj->alias2(alias2);
		hj->view1(view1);
		hj->view2(view2);

		JobStepAssociation outJs5(jobInfo.status);
		outJs5.outAdd(spdl12);
		outJs5.outAdd(spdl22);
		hj->inputAssociation(outJs5);

		step.reset(pcs1);
		jsv.push_back(step);
		step.reset(pds1);
		jsv.push_back(step);
		step.reset(pcs2);
		jsv.push_back(step);
		step.reset(pds2);
		jsv.push_back(step);
		step.reset(hj);
		jsv.push_back(step);

		if (jobInfo.tryTuples)
		{
			TupleInfo ti1(setTupleInfo(ct1, dictOid1, jobInfo, tableOid1, sc1, alias1));
			pds1->tupleId(ti1.key);
			jobInfo.keyInfo->dictKeyMap[pcs1->tupleId()] = ti1.key;
			jobInfo.tokenOnly[pcs1->tupleId()] = false;

			TupleInfo ti2(setTupleInfo(ct2, dictOid2, jobInfo, tableOid2, sc2, alias2));
			pds2->tupleId(ti2.key);
			jobInfo.keyInfo->dictKeyMap[pcs2->tupleId()] = ti2.key;
			jobInfo.tokenOnly[pcs2->tupleId()] = false;
		}
	}
	else
	{
		return jsv;
	}

	return jsv;
}


const JobStepVector doSemiJoin(const SimpleColumn* sc, const ReturnedColumn* rc, JobInfo& jobInfo)
{
	CalpontSystemCatalog::OID tableOid1 = tableOid(sc, jobInfo.csc);
	CalpontSystemCatalog::OID tableOid2 = execplan::CNX_VTABLE_ID;
	string alias1(extractTableAlias(sc));
	CalpontSystemCatalog::ColType ct1 = jobInfo.csc->colType(sc->oid());
	CalpontSystemCatalog::ColType ct2 = rc->resultType();

	JobStepVector jsv;
	SJSTEP step;

	CalpontSystemCatalog::OID dictOid1 = 0;
	uint64_t tupleId1 = -1;
	uint64_t tupleId2 = -1;
	if (sc->schemaName().empty() == false)
	{
		pColStep* pcs1 = new pColStep(JobStepAssociation(jobInfo.status),
			JobStepAssociation(jobInfo.status), 0, jobInfo.csc, sc->oid(), tableOid1,
			jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
		dictOid1 = isDictCol(ct1);
		pcs1->logger(jobInfo.logger);
		pcs1->alias(alias1);
		pcs1->view(sc->viewName());
		pcs1->name(sc->columnName());
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

	TupleHashJoinStep* thj = new TupleHashJoinStep(
		jobInfo.sessionId,
		jobInfo.txnId,
		jobInfo.statementId,
		&jobInfo.rm);
	thj->logger(jobInfo.logger);
	thj->tableOid1(tableOid1);
	thj->tableOid2(tableOid2);
	thj->alias1(alias1);
	thj->view1(sc->viewName());
	thj->oid1(sc->oid());
	thj->oid2(tableOid2 + 1 + rc->sequence());
	thj->dictOid1(dictOid1);
	thj->dictOid2(0);
	thj->sequence1(sc->sequence());
	thj->sequence2(rc->sequence());
	thj->column1(sc);
	thj->column2(rc);
//	thj->joinId(jobInfo.joinNum++);
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


const JobStepVector doFunctionColumn(const FunctionColumn* fc, JobInfo& jobInfo)
{
	throw logic_error("jlf_execplantojoblist.cpp:doFunctionColumn(): Not implemented");
}

// This function returns a JobStepVector with the steps necessary to evaluate the passed Arithmetic column.
// It recurses directly and indirectly through calls to doFunctionColumn.
const JobStepVector doArithmeticColumn(const ArithmeticColumn* ac, JobInfo& jobInfo)
{
	JobStepVector jsv;


	TreeNodeStack stack;
	ac->expression()->walk(walkTreeNode, &stack);

	const TreeNode* tnop;

	tnop = stack.top();
	stack.pop();

	// Only handling ArithmeticColumns that contain FunctionColumn for now.
	if(TreeNode2Type(tnop) == FUNCTIONCOLUMN)
	{
		const FunctionColumn* fc = static_cast<const FunctionColumn*>(tnop);
		jsv = doFunctionColumn(fc, jobInfo);
	}
	else
	{
		cerr << boldStart << "doArithmeticColumn: Unhandled Type." << boldStop << endl;
	}

	return jsv;

}


const JobStepVector doExpressionFilter(const ParseTree* n, JobInfo& jobInfo)
{
	JobStepVector jsv;
	ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
											jobInfo.txnId,
											jobInfo.verId,
											jobInfo.statementId);

	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 1");

	es->logger(jobInfo.logger);
	es->expressionFilter(n, jobInfo);
	SJSTEP sjstep(es);
	jsv.push_back(sjstep);

	return jsv;
}


const JobStepVector doExpressionFilter(const Filter* f, JobInfo& jobInfo)
{
	JobStepVector jsv;
	ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
											jobInfo.txnId,
											jobInfo.verId,
											jobInfo.statementId);

	if (es == NULL)
		throw runtime_error("Failed to create ExpressionStep 2");

	es->logger(jobInfo.logger);
	es->expressionFilter(f, jobInfo);
	SJSTEP sjstep(es);
	jsv.push_back(sjstep);

	return jsv;
}


const JobStepVector doConstantBooleanFilter(const ParseTree* n, JobInfo& jobInfo)
{
	JobStepVector jsv;
	TupleConstantBooleanStep* tcbs = new
		TupleConstantBooleanStep(JobStepAssociation(),
								 JobStepAssociation(),
								 jobInfo.sessionId,
								 jobInfo.txnId,
								 jobInfo.verId,
								 jobInfo.statementId,
								 n->data()->getBoolVal());

	if (tcbs == NULL)
		throw runtime_error("Failed to create Constant Boolean Step");

	SJSTEP sjstep(tcbs);
	jsv.push_back(sjstep);

	return jsv;
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
		tbl_oid = tableOid(sc, jobInfo.csc);

		if (sc->joinInfo() != 0 && (int32_t)cc->sequence() != -1 && jobInfo.tryTuples)
		{
			// correlated, like in 'c1 in select 1' type sub queries.
			return doSemiJoin(sc, cc, jobInfo);
		}
		else if (sc->schemaName().empty() && jobInfo.tryTuples)
		{
			// bug 3749, mark outer join table with isNull filter
			if (ConstantColumn::NULLDATA == cc->type() && (opis == *sop || opisnull == *sop))
				jobInfo.tableHasIsNull.insert(getTupleKey(jobInfo, tbl_oid, alias, view));
			return doExpressionFilter(sf, jobInfo);
		}

		// trim trailing space char in the predicate
		string constval(cc->constval());
		size_t spos = constval.find_last_not_of(" ");
		if (spos != string::npos) constval = constval.substr(0, spos+1);

		if (!sc->schemaName().empty())
			jobInfo.tables.insert(make_table(sc->schemaName(), sc->tableName()));
		CalpontSystemCatalog::OID dictOid;
		CalpontSystemCatalog::ColType ct = jobInfo.csc->colType(sc->oid());
		//@bug 339 nulls are not stored in dictionary
		if ((dictOid = isDictCol(ct)) > 0  && ConstantColumn::NULLDATA != cc->type())
		{
			if (jobInfo.trace)
				cout << "Emit pTokenByScan/pCol for SimpleColumn op ConstantColumn" << endl;

			pColStep* pcs = new pColStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0, jobInfo.csc, sc->oid(),
				tbl_oid, jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0,
				jobInfo.statementId, jobInfo.rm);
			pcs->logger(jobInfo.logger);
			pcs->alias(alias);
			pcs->view(view);
			pcs->name(sc->columnName());
			pcs->cardinality(sc->cardinality());

			if (filterWithDictionary(dictOid, jobInfo.stringScanThreshold))
			{
				pDictionaryStep* pds = new pDictionaryStep(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid,
					ct.ddn.compressionType, tbl_oid, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
				pds->logger(jobInfo.logger);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->cardinality(sc->cardinality());

				//Add the filter
				pds->addFilter(cop, constval);

				// data list for pcolstep output
				AnyDataListSPtr spdl1(new AnyDataList());
FIFODEBUG();
				FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
				spdl1->fifoDL(dl1);
				dl1->OID(sc->oid());

				JobStepAssociation outJs1(jobInfo.status);
				outJs1.outAdd(spdl1);
				pcs->outputAssociation(outJs1);

				// data list for pdictionarystep output
				AnyDataListSPtr spdl2(new AnyDataList());
				StringFifoDataList* dl2 = new StringFifoDataList(1, jobInfo.fifoSize);
				spdl2->stringDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2(jobInfo.status);
				outJs2.outAdd(spdl2);
				pds->outputAssociation(outJs2);

				//Associate pcs with pds
				JobStepAssociation outJs(jobInfo.status);
				outJs.outAdd(spdl1);
				pds->inputAssociation(outJs);

				sjstep.reset(pcs);
				jsv.push_back(sjstep);
				sjstep.reset(pds);
				jsv.push_back(sjstep);

				if (jobInfo.tryTuples)
				{
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
					pcs->tupleId(ti.key);

					// string column
					ti = setTupleInfo(ct, dictOid, jobInfo, tbl_oid, sc, alias);
					pds->tupleId(ti.key);
					jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
					jobInfo.tokenOnly[ti.key] = false;
				}
			}
			else
			{
				pDictionaryScan* pds = new pDictionaryScan(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid,
					ct.ddn.compressionType, tbl_oid, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
				pds->logger(jobInfo.logger);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->cardinality(sc->cardinality());

				//Add the filter
				pds->addFilter(cop, constval);

				HashJoinStep* hj = 0;
				TupleHashJoinStep* thj = 0;

				if (jobInfo.tryTuples)
				{
					// save for expression transformation
					pds->addFilter(sf);

					thj = new TupleHashJoinStep(
						jobInfo.sessionId,
						jobInfo.txnId,
						jobInfo.statementId,
						&jobInfo.rm);
					thj->logger(jobInfo.logger);
					thj->tableOid1(0);
					thj->tableOid2(tbl_oid);
					thj->alias1(alias);
					thj->alias2(alias);
					thj->view1(view);
					thj->view2(view);
					thj->oid1(sc->oid());
					thj->oid2(sc->oid());
//					thj->joinId(jobInfo.joinNum++);
					thj->joinId(0);
					thj->setJoinType(INNER);

					CalpontSystemCatalog::ColType dct;
					dct.colDataType = CalpontSystemCatalog::BIGINT;
					dct.colWidth = 8;
					dct.scale = 0;
					dct.precision = 0;
					dct.compressionType = ct.compressionType;

					TupleInfo ti(setTupleInfo(dct, sc->oid(), jobInfo, tbl_oid, sc, alias));
					pds->tupleId(ti.key); // pcs, pds use same tuple key, both 8-byte column
					pcs->tupleId(ti.key);
					thj->tupleId1(ti.key);
					thj->tupleId2(ti.key);

					if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
						jobInfo.tokenOnly[ti.key] = true;
				}
				else
				{
					hj = new HashJoinStep(INNER,
										jobInfo.sessionId,
										jobInfo.txnId,
										jobInfo.statementId,
										&jobInfo.rm);
					hj->logger(jobInfo.logger);
					hj->tableOid1(0);
					hj->tableOid2(tbl_oid);
					hj->alias1(alias);
					hj->alias2(alias);
					hj->view1(view);
					hj->view2(view);
					hj->cardinality(sf->cardinality());
				}

				//Associate the steps
				AnyDataListSPtr spdl1(new AnyDataList());

				if (jobInfo.tryTuples)
				{
					RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
					spdl1->rowGroupDL(dl1);
					dl1->OID(dictOid);
				}
				else
				{
					FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
					spdl1->fifoDL(dl1);
					dl1->OID(dictOid);
				}

				JobStepAssociation outJs1(jobInfo.status);
				outJs1.outAdd(spdl1);
				pds->outputAssociation(outJs1);

				AnyDataListSPtr spdl2(new AnyDataList());

				if (jobInfo.tryTuples)
				{
					RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
					spdl2->rowGroupDL(dl2);
					dl2->OID(sc->oid());
				}
				else
				{
					FifoDataList* dl2 = new FifoDataList(1, jobInfo.fifoSize);
					spdl2->fifoDL(dl2);
					dl2->OID(sc->oid());
				}

				JobStepAssociation outJs2(jobInfo.status);
				outJs2.outAdd(spdl2);
				pcs->outputAssociation(outJs2);

				JobStepAssociation outJs3(jobInfo.status);
				outJs3.outAdd(spdl1);
				outJs3.outAdd(spdl2);
				sjstep.reset(pds);
				jsv.push_back(sjstep);
				sjstep.reset(pcs);
				jsv.push_back(sjstep);

				if (jobInfo.tryTuples)
				{
					thj->inputAssociation(outJs3);
					sjstep.reset(thj);
				}
				else
				{
					hj->inputAssociation(outJs3);
					sjstep.reset(hj);
				}
				jsv.push_back(sjstep);
			}
		}
		else if ( CalpontSystemCatalog::CHAR != ct.colDataType && 
				 CalpontSystemCatalog::VARCHAR != ct.colDataType &&
				 CalpontSystemCatalog::VARBINARY != ct.colDataType &&
				 ConstantColumn::NULLDATA != cc->type() &&
				 (cop & COMPARE_LIKE) ) // both like and not like
		{
			if (jobInfo.tryTuples)
				return doExpressionFilter(sf, jobInfo);
			else
				throw runtime_error("Numerical LIKE/!LIKE operator is not supported in tablemode");
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
				if ((ct.colDataType == CalpontSystemCatalog::DATE && constval == "0000-00-00")||
				     (ct.colDataType == CalpontSystemCatalog::DATETIME && constval == "0000-00-00 00:00:00"))
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
			if ((ct.colDataType == CalpontSystemCatalog::DATE && constval == "0000-00-00")||
                            (ct.colDataType == CalpontSystemCatalog::DATETIME && constval == "0000-00-00 00:00:00"))
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
				pColStep* pcss = new pColStep(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc,
					sc->oid(), tbl_oid, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
				pcss->logger(jobInfo.logger);
				pcss->addFilter(cop, value, rf);
				pcss->alias(alias);
				pcss->view(view);
				pcss->name(sc->columnName());
				pcss->cardinality(sf->cardinality());

				sjstep.reset(pcss);
				jsv.push_back(sjstep);

				if (jobInfo.tryTuples)
				{
					// save for expression transformation
					pcss->addFilter(sf);

					TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tbl_oid, sc, alias));
					pcss->tupleId(ti.key);

					if (dictOid > 0) // cc->type() == ConstantColumn::NULLDATA
					{
						if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
							jobInfo.tokenOnly[ti.key] = true;
					}

					if (ConstantColumn::NULLDATA == cc->type() &&
						(opis == *sop || opisnull == *sop))
						jobInfo.tableHasIsNull.insert(getTupleKey(jobInfo, tbl_oid, alias, view));
				}
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
				pIdxWalk* pidxw = new pIdxWalk(JobStepAssociation(jobInfo.status), JobStepAssociation(jobInfo.status), 0,
					jobInfo.csc, indexOID.objnum, sc->oid(), tbl_oid,
					jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0,
					jobInfo.statementId);

				pidxw->logger(jobInfo.logger);
				pidxw->addSearchStr(cop, value);
				pidxw->cardinality(sf->cardinality());

				sjstep.reset(pidxw);
				jsv.push_back(sjstep);
				pIdxList* pidxL = new pIdxList(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc,
					indexOID.listOID, tbl_oid, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId);

				/*output of idxwalk */
				AnyDataListSPtr spdl1(new AnyDataList());
				ZonedDL* dl1 = new ZonedDL(1, jobInfo.rm);
				dl1->OID(indexOID.objnum);
				spdl1->zonedDL(dl1);
				JobStepAssociation outJs1(jobInfo.status);
				outJs1.outAdd(spdl1);
				pidxw->outputAssociation(outJs1);

				/*inputput of idxlist */
				pidxL->inputAssociation(outJs1);

				/*output of idxlist */
				AnyDataListSPtr spdl2(new AnyDataList());
				ZonedDL* dl2 = new ZonedDL(1, jobInfo.rm);
				dl2->OID(indexOID.listOID);
				spdl2->zonedDL(dl2);
				JobStepAssociation outJs2(jobInfo.status);
				outJs2.outAdd(spdl2);
				pidxL->outputAssociation(outJs2);

				pidxL->logger(jobInfo.logger);
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
		
		// @bug 1349. no-op rules:
		// 1. If two columns of a simple filter are of the two different tables, and the
		// filter operator is not "=", no-op this simple filter,
		// 2. If a join filter has "ANTI" option, no op this filter before ANTI hashjoin
		// is supported in ExeMgr.
		// @bug 1933. Throw exception instead of no-op for MySQL virtual table (no connector re-filter). 
		// @bug 1496. handle non equal operator as expression in v-table mode
		if ((sc1->tableName() != sc2->tableName() ||
			 sc1->tableAlias() != sc2->tableAlias() ||
			 sc1->viewName() != sc2->viewName())
			&& (sop->data() != "=")) 
		{
			if (jobInfo.tryTuples)
				return doExpressionFilter(sf, jobInfo);
			else
				throw runtime_error("Join with non equal operator is not supported in table mode");
		}

		if (sf->joinFlag() == SimpleFilter::ANTI)
			throw runtime_error("Anti join is not currently supported");		

		if (!sc1->schemaName().empty())
			jobInfo.tables.insert(make_table(sc1->schemaName(), sc1->tableName()));
		if (!sc2->schemaName().empty())
			jobInfo.tables.insert(make_table(sc2->schemaName(), sc2->tableName()));
		JobStepVector join = doJoin(sc1, sc2, jobInfo, sop);
		// set cardinality for the hashjoin step. hj result card <= larger input card
		uint card = 0;
		if (sf->cardinality() > sc1->cardinality() && sf->cardinality() > sc2->cardinality())
			card = (sc1->cardinality() > sc2->cardinality() ? sc1->cardinality() : sc2->cardinality());
		else
			card = sf->cardinality();
		join[join.size()-1].get()->cardinality(card);

		jsv.insert(jsv.end(), join.begin(), join.end());
	}
	// @bug 844. support aggregate functio in having clause
	else if (lhsType == AGGREGATECOLUMN && rhsType == CONSTANTCOLUMN)
	{
		JobStepAssociation in(jobInfo.status), out(jobInfo.status);
		// input - TupleBucket
		AnyDataListSPtr adl1(new AnyDataList());
		TupleBucketDataList *tbdl = new TupleBucketDataList(jobInfo.tupleMaxBuckets, 1, jobInfo.tupleDLMaxSize,
			jobInfo.rm);
		tbdl->setElementMode(1);
		tbdl->setMultipleProducers(true);

		adl1->tupleBucketDL(tbdl);
		in.outAdd(adl1);
		
		// output - ElementType Fifo
		AnyDataListSPtr adl2(new AnyDataList());
FIFODEBUG();
		FifoDataList* fdl = new FifoDataList(1, jobInfo.fifoSize);
		adl2->fifoDL(fdl);
		out.outAdd(adl2);
		
		const AggregateColumn* ac = static_cast<const AggregateColumn*>(lhs);
		
		// check one group by column to get table oid
		SimpleColumn *sc = dynamic_cast<SimpleColumn*>(ac->projectColList()[0].get());
		assert (sc != 0);
		tbl_oid = tableOid(sc, jobInfo.csc);
		AggregateFilterStep *afs = 
			new AggregateFilterStep(in, 
									out,
									ac->functionName(),
									ac->groupByColList(),
									ac->projectColList(),
									ac->functionParms(),		
									tbl_oid,
									jobInfo.sessionId,
									jobInfo.txnId,
									jobInfo.verId,
									0,
									jobInfo.statementId,
									jobInfo.rm);

		afs->alias(extractTableAlias(sc));
		afs->view(sc->viewName());
		afs->cardinality(sf->cardinality());

		CalpontSystemCatalog::ColType ct;
		const ConstantColumn *cc = static_cast<const ConstantColumn*>(rhs);
		int64_t intVal;
		string strVal;
		SOP sop;
		sop = sf->op();
		int8_t cop = op2num(sop);

		if (typeid((*ac->functionParms().get())) == typeid(SimpleColumn))
		{
			SimpleColumn* sc = reinterpret_cast<SimpleColumn*>(ac->functionParms().get());
			uint8_t rf = 0;
			ct = jobInfo.csc->colType(sc->oid());
			intVal = convertValueNum(cc->constval(), ct, false, rf);
			afs->addFilter(cop, intVal);
		}
		else
		{
			if (cc->type() == ConstantColumn::NUM)
			{
				intVal = atol(cc->constval().c_str());
				afs->addFilter(cop, intVal, false);
			}
			else if (cc->type() == ConstantColumn::LITERAL)
				afs->addFilter(cop, cc->constval(), false);
		}

		sjstep.reset(afs);
		jsv.push_back(sjstep);
	}
	else if (jobInfo.tryTuples && lhsType == CONSTANTCOLUMN && rhsType == SIMPLECOLUMN)
	{
		//swap the two and process as normal
		const ConstantColumn* ccp = static_cast<const ConstantColumn*>(lhs);
		const SimpleColumn* scp = dynamic_cast<const SimpleColumn*>(rhs);
		SimpleFilter nsf;
		SOP nsop(sop->opposite());
		nsf.op(nsop);
		nsf.lhs(scp->clone());
		nsf.rhs(ccp->clone());
		jsv = doSimpleFilter(&nsf, jobInfo);
		if (jsv.empty())
			throw runtime_error("Unhandled SimpleFilter");
	}
	else if (jobInfo.tryTuples &&
			 (lhsType == ARITHMETICCOLUMN || rhsType == ARITHMETICCOLUMN ||
			 lhsType == FUNCTIONCOLUMN || rhsType == FUNCTIONCOLUMN))
	{
		jsv = doExpressionFilter(sf, jobInfo);
	}
	else if (jobInfo.tryTuples && lhsType == SIMPLECOLUMN &&
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
	else if (jobInfo.tryTuples && rhsType == SIMPLECOLUMN &&
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
	else
	{
		if (jobInfo.tryTuples == true)
			throw logic_error("doSimpleFilter: Unhandled SimpleFilter.");

		cerr << boldStart << "doSimpleFilter: Unhandled SimpleFilter: left = " << lhsType <<
			", right = " << rhsType << boldStop << endl;
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
	vector<ParseTree*> joinNodes;       // vector of joins
	map<ParseTree*, ParseTree*> cpMap;  // <child, parent> link for node removal
	JobStepVector join;                 // join step with its projection steps
	set<ParseTree*> nodesToRemove;      // nodes to be removed after converted to steps

	// To compromise the front end difficulty on setting outer attributes.
	set<uint64_t> tablesInOuter;

	// root
	ParseTree* filters = new ParseTree(*(oj->pt().get()));
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
				if (!sc1->schemaName().empty())
					jobInfo.tables.insert(make_table(sc1->schemaName(), sc1->tableName()));
				if (!sc2->schemaName().empty())
					jobInfo.tables.insert(make_table(sc2->schemaName(), sc2->tableName()));

				// @bug3037, workaround on join order, whish this can be corrected soon,
				// cascade outer table attribute.
				CalpontSystemCatalog::OID tableOid1 = tableOid(sc1, jobInfo.csc);
				uint64_t tid1 =
							getTableKey(jobInfo, tableOid1, sc1->tableAlias(), sc1->viewName());
				CalpontSystemCatalog::OID tableOid2 = tableOid(sc2, jobInfo.csc);
				uint64_t tid2 =
							getTableKey(jobInfo, tableOid2, sc2->tableAlias(), sc2->viewName());

				if (tablesInOuter.find(tid1) != tablesInOuter.end())
					sc1->returnAll(true);
				else if (tablesInOuter.find(tid2) != tablesInOuter.end())
					sc2->returnAll(true);

				if (sc1->returnAll() && !sc2->returnAll())
					tablesInOuter.insert(tid1);
				else if (!sc1->returnAll() && sc2->returnAll())
					tablesInOuter.insert(tid2);

				join = doJoin(sc1, sc2, jobInfo, sop);
				// set cardinality for the hashjoin step.
				uint card = sf->cardinality();
				if (sf->cardinality() > sc1->cardinality() &&
					sf->cardinality() > sc2->cardinality())
					card = ((sc1->cardinality() > sc2->cardinality()) ?
							sc1->cardinality() : sc2->cardinality());
				join[join.size()-1].get()->cardinality(card);

				jsv.insert(jsv.end(), join.begin(), join.end());
				joinNodes.push_back(cn);
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

	if (thjs != NULL)
	{
		// remove joins from the original filters
		ParseTree* nullTree = NULL;
		for (vector<ParseTree*>::iterator i = joinNodes.begin(); i != joinNodes.end() && isOk; i++)
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

		for (set<ParseTree*>::iterator i = nodesToRemove.begin(); i != nodesToRemove.end(); i++)
			delete *i;

		// construct an expression step, if additional comparison exists.
		if (isOk && filters != NULL && filters->data() != NULL)
		{
			if (!jobInfo.tryTuples)
				throw runtime_error("Join with additional filters is not supported in table mode");

			ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
													jobInfo.txnId,
													jobInfo.verId,
													jobInfo.statementId);

			if (es == NULL)
				throw runtime_error("Failed to create ExpressionStep 1");

			es->logger(jobInfo.logger);
			es->expressionFilter(filters, jobInfo);
			es->associatedJoinId(thjs->joinId());
			SJSTEP sjstep(es);
			jsv.push_back(sjstep);
		}
	}
	else
	{
		// Due to Calpont view handling, some joins may treated as expressions.
		ExpressionStep* es = new ExpressionStep(jobInfo.sessionId,
												jobInfo.txnId,
												jobInfo.verId,
												jobInfo.statementId);

		if (es == NULL)
			throw runtime_error("Failed to create ExpressionStep 1");

		es->logger(jobInfo.logger);
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

bool tryCombineDictionary(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop, bool tryTuples)
{
	JobStepVector::iterator it2 = jsv2.end() - 1;
	// already checked: (typeid(*(it2->get()) != typeid(pDictionaryStep))
	if (typeid(*((it2-1)->get())) != typeid(pColStep)) return false;
	pDictionaryStep* ipdsp = dynamic_cast<pDictionaryStep*>(it2->get());

	JobStepVector::iterator iter = jsv1.begin();
	JobStepVector::iterator end = jsv1.end();

	if (bop == BOP_OR)
	{
		iter = end - 1;
	}

	while (iter != end)
	{
		if (typeid(*(iter->get())) == typeid(pDictionaryStep))
		{
			pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>((*iter).get());

			// If the OID's match and the BOP's match and the previous step is pcolstep,
			// then append the filters.
			bool match = false;
			if (tryTuples)
				match = (ipdsp->tupleId() == pdsp->tupleId());
			else
				match = (ipdsp->oid() == pdsp->oid() &&
						ipdsp->alias() == pdsp->alias() &&
						ipdsp->view() == pdsp->view());

			if (match && (typeid(*((iter-1)->get())) == typeid(pColStep)))
			{
				if (pdsp->BOP() == BOP_NONE)
				{
 					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->setBOP(bop);
						if (tryTuples)
							pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
				else if (pdsp->BOP() == bop)
				{
					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						if (tryTuples)
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

bool tryCombineDictionaryScan(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop, bool tryTuples)
{
// disable dictionary scan 
#if 0
	JobStepVector::iterator it2 = jsv2.begin();
	if (typeid(*((it2+1)->get())) != typeid(pColStep))
		return false;

	if ((typeid(*((it2+2)->get())) != typeid(TupleHashJoinStep)) &&
		(typeid(*((it2+2)->get())) != typeid(HashJoinStep)))
		return false;

	pDictionaryScan* ipdsp = dynamic_cast<pDictionaryScan*>(it2->get());

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
		if (typeid(*(iter->get())) == typeid(pDictionaryScan))
		{
			pDictionaryScan* pdsp = dynamic_cast<pDictionaryScan*>((*iter).get());

			// If the OID's match and the BOP's match and the previous step is pcolstep,
			// then append the filters.
			bool match = false;
			if (tryTuples)
				match = (ipdsp->tupleId() == pdsp->tupleId());
			else
				match = (ipdsp->oid() == pdsp->oid() &&
						ipdsp->alias() == pdsp->alias() &&
						ipdsp->view() == pdsp->view());

			if (match && (typeid(*((iter+1)->get())) == typeid(pColStep)))
			{
				if (pdsp->BOP() == BOP_NONE)
				{
 					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						pdsp->setBOP(bop);
						if (tryTuples)
							pdsp->appendFilter(ipdsp->getFilters());
						return true;
					}
				}
				else if (pdsp->BOP() == bop)
				{
					if (ipdsp->BOP() == BOP_NONE || ipdsp->BOP() == bop)
					{
						pdsp->appendFilter(ipdsp->filterString(), ipdsp->filterCount());
						if (tryTuples)
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
bool tryCombineFilters(JobStepVector& jsv1, JobStepVector& jsv2, int8_t bop, bool tryTuples)
{
	// A couple of tests to make sure we're operating on the right things...
	if (jsv1.size() < 1) return false;

	// if filter by pDictionary, there are two steps: pcolstep and pdictionarystep.
	if (jsv2.size() == 2 && typeid(*jsv2.back().get()) == typeid(pDictionaryStep))
		return tryCombineDictionary(jsv1, jsv2, bop, tryTuples);

	// if filter by pDictionaryScan, there are three steps: pdictionaryscan, pcolstep and join.
	if (jsv2.size() == 3 && typeid(*jsv2.front().get()) == typeid(pDictionaryScan))
		return tryCombineDictionaryScan(jsv1, jsv2, bop, tryTuples);

	// non-dictionary filters
	if (jsv2.size() != 1) return false;
	if (typeid(*jsv2.back().get()) != typeid(pColStep)) return false;

	pColStep* ipcsp = dynamic_cast<pColStep*>(jsv2.back().get());
	assert(ipcsp);

	JobStepVector::iterator iter = jsv1.begin();
	JobStepVector::iterator end = jsv1.end();

	// only try last step in jsv1 if operator is OR.
	if (bop == BOP_OR)
	{
		iter = end - 1;
	}

	while (iter != end)
	{
		if (typeid(*(iter->get())) == typeid(pColStep))
		{
			pColStep* pcsp = dynamic_cast<pColStep*>((*iter).get());
			assert(pcsp);
			// If the OID's match and the BOP's match then append the filters
			bool match = false;
			if (tryTuples)
				match = (ipcsp->tupleId() == pcsp->tupleId());
			else
				match = (ipcsp->oid() == pcsp->oid() &&
						ipcsp->alias() == pcsp->alias() &&
						ipcsp->view() == pcsp->view());

			if (match)
			{
				if (pcsp->BOP() == BOP_NONE)
				{
 					if (ipcsp->BOP() == BOP_NONE || ipcsp->BOP() == bop)
					{
						pcsp->appendFilter(ipcsp->filterString(), ipcsp->filterCount());
						pcsp->setBOP(bop);
						if (tryTuples)
							pcsp->appendFilter(ipcsp->getFilters());
						return true;
					}
				}
				else if (pcsp->BOP() == bop)
				{
					if (ipcsp->BOP() == BOP_NONE || ipcsp->BOP() == bop)
					{
						pcsp->appendFilter(ipcsp->filterString(), ipcsp->filterCount());
						if (tryTuples)
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

		const SSC sc = cf->col();
		// if column from subquery
		if (sc->schemaName().empty() && jobInfo.tryTuples)
		{
			return doExpressionFilter(cf, jobInfo);
		}

		jobInfo.tables.insert(make_table(sc->schemaName(), sc->tableName()));
		ConstantFilter::FilterList fl = cf->filterList();
		CalpontSystemCatalog::OID dictOid;
		CalpontSystemCatalog::ColType ct = jobInfo.csc->colType(sc.get()->oid());
		CalpontSystemCatalog::OID tbOID = tableOid(sc.get(), jobInfo.csc);
		string alias(extractTableAlias(sc));
		string view(sc->viewName());
		if ((dictOid = isDictCol(ct)) > 0)
		{
			if (jobInfo.trace)
				cout << "Emit pTokenByScan/pCol for SimpleColumn op ConstantColumn "
					"[op ConstantColumn]" << endl;

			pColStep* pcs = new pColStep(JobStepAssociation(jobInfo.status), JobStepAssociation(jobInfo.status), 0,
				jobInfo.csc, sc->oid(), tbOID, jobInfo.sessionId, jobInfo.txnId,
				jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
			pcs->logger(jobInfo.logger);
			pcs->alias(alias);
			pcs->view(view);
			pcs->name(sc->columnName());
			pcs->cardinality(sc->cardinality());

			if (filterWithDictionary(dictOid, jobInfo.stringScanThreshold))
			{
				pDictionaryStep* pds = new pDictionaryStep(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid,
					ct.ddn.compressionType, tbOID, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
				pds->logger(jobInfo.logger);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc->columnName());
				pds->cardinality(sc->cardinality());
				if (op)
					pds->setBOP(bop2num(op));

				//Add the filter(s)
				uint64_t card = 0;
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
					card = sf->cardinality();
				}

				// data list for pcolstep output
				AnyDataListSPtr spdl1(new AnyDataList());
FIFODEBUG();
				FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
				spdl1->fifoDL(dl1);
				dl1->OID(sc->oid());

				JobStepAssociation outJs1(jobInfo.status);
				outJs1.outAdd(spdl1);
				pcs->outputAssociation(outJs1);

				// data list for pdictionarystep output
				AnyDataListSPtr spdl2(new AnyDataList());
				StringFifoDataList* dl2 = new StringFifoDataList(1, jobInfo.fifoSize);
				spdl2->stringDL(dl2);
				dl2->OID(sc->oid());

				JobStepAssociation outJs2(jobInfo.status);
				outJs2.outAdd(spdl2);
				pds->outputAssociation(outJs2);

				//Associate pcs with pds
				JobStepAssociation outJs(jobInfo.status);
				outJs.outAdd(spdl1);
				pds->inputAssociation(outJs);

				sjstep.reset(pcs);
				jsv.push_back(sjstep);
				sjstep.reset(pds);
				jsv.push_back(sjstep);

				if (jobInfo.tryTuples)
				{
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
					pcs->tupleId(ti.key);

					// string column
					ti = setTupleInfo(ct, dictOid, jobInfo, tbOID, sc.get(), alias);
					pds->tupleId(ti.key);
					jobInfo.keyInfo->dictKeyMap[pcs->tupleId()] = ti.key;
					jobInfo.tokenOnly[ti.key] = false;
				}
			}
			else
			{
				pDictionaryScan* pds = new pDictionaryScan(JobStepAssociation(jobInfo.status),
					JobStepAssociation(jobInfo.status), 0, jobInfo.csc, dictOid,
					ct.ddn.compressionType, tbOID, jobInfo.sessionId, jobInfo.txnId,
					jobInfo.verId, 0, jobInfo.statementId, jobInfo.rm);
				pds->logger(jobInfo.logger);
				jobInfo.keyInfo->dictOidToColOid[dictOid] = sc.get()->oid();
				pds->alias(alias);
				pds->view(view);
				pds->name(sc.get()->columnName());
				if (op)
					pds->setBOP(bop2num(op));

				//Add the filter(s)
				uint64_t card = 0;
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
					card = sf->cardinality();
				}

				if (jobInfo.tryTuples)
				{
					// save for expression transformation
					pds->addFilter(cf);

					TupleHashJoinStep* thj = new TupleHashJoinStep(
						jobInfo.sessionId,
						jobInfo.txnId,
						jobInfo.statementId,
						&jobInfo.rm);
					thj->logger(jobInfo.logger);
					thj->tableOid1(0);
					thj->tableOid2(tbOID);
					thj->alias1(alias);
					thj->alias2(alias);
					thj->view1(view);
					thj->view2(view);
					thj->oid1(sc->oid());
					thj->oid2(sc->oid());
//					thj->joinId(jobInfo.joinNum++);
					thj->joinId(0);
					thj->setJoinType(INNER);

					//Associate the steps
					AnyDataListSPtr spdl1(new AnyDataList());
					RowGroupDL* dl1 = new RowGroupDL(1, jobInfo.fifoSize);
					spdl1->rowGroupDL(dl1);
					dl1->OID(dictOid);

					JobStepAssociation outJs1(jobInfo.status);
					outJs1.outAdd(spdl1);
					pds->outputAssociation(outJs1);

					AnyDataListSPtr spdl2(new AnyDataList());
					RowGroupDL* dl2 = new RowGroupDL(1, jobInfo.fifoSize);
					spdl2->rowGroupDL(dl2);
					dl2->OID(sc->oid());

					JobStepAssociation outJs2(jobInfo.status);
					outJs2.outAdd(spdl2);
					pcs->outputAssociation(outJs2);

					JobStepAssociation outJs3(jobInfo.status);
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
					pds->tupleId(ti.key); // pcs, pds use same tuple key, both 8-byte column
					pcs->tupleId(ti.key);
					thj->tupleId1(ti.key);
					thj->tupleId2(ti.key);

					if (jobInfo.tokenOnly.find(ti.key) == jobInfo.tokenOnly.end())
						jobInfo.tokenOnly[ti.key] = true;
				}
				else
				{
					HashJoinStep* hj = new HashJoinStep(INNER,
						jobInfo.sessionId,
						jobInfo.txnId,
						jobInfo.statementId,
						&jobInfo.rm);
					hj->logger(jobInfo.logger);
					hj->tableOid1(0);
					hj->tableOid2(tbOID);
					hj->alias2(extractTableAlias(sc));
					hj->view2(sc->viewName());
					hj->cardinality(card);

					//Associate the steps
					AnyDataListSPtr spdl1(new AnyDataList());
FIFODEBUG();
					FifoDataList* dl1 = new FifoDataList(1, jobInfo.fifoSize);
					spdl1->fifoDL(dl1);
					dl1->OID(dictOid);

					JobStepAssociation outJs1(jobInfo.status);
					outJs1.outAdd(spdl1);
					pds->outputAssociation(outJs1);

					AnyDataListSPtr spdl2(new AnyDataList());
FIFODEBUG();
					FifoDataList* dl2 = new FifoDataList(1, jobInfo.fifoSize);
					spdl2->fifoDL(dl2);
					dl2->OID(sc->oid());

					JobStepAssociation outJs2(jobInfo.status);
					outJs2.outAdd(spdl2);
					pcs->outputAssociation(outJs2);

					JobStepAssociation outJs3(jobInfo.status);
					outJs3.outAdd(spdl1);
					outJs3.outAdd(spdl2);
					hj->inputAssociation(outJs3);

					sjstep.reset(pds);
					jsv.push_back(sjstep);
					sjstep.reset(pcs);
					jsv.push_back(sjstep);
					sjstep.reset(hj);
					jsv.push_back(sjstep);
				}
			}
		}
		else
		{
			if (jobInfo.trace)
				cout << "Emit pCol for SimpleColumn op ConstantColumn [op ConstantColumn]" << endl;

			CalpontSystemCatalog::OID tblOid = tableOid(sc.get(), jobInfo.csc);
			string alias(extractTableAlias(sc));
			pColStep* pcss = new pColStep(JobStepAssociation(jobInfo.status),
				JobStepAssociation(jobInfo.status), 0, jobInfo.csc, sc->oid(), tblOid,
				jobInfo.sessionId, jobInfo.txnId, jobInfo.verId, 0, jobInfo.statementId,
				jobInfo.rm);
			pcss->logger(jobInfo.logger);
			pcss->alias(extractTableAlias(sc));
			pcss->view(sc->viewName());
			pcss->name(sc->columnName());

			if (op)
				pcss->setBOP(bop2num(op));
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

				pcss->addFilter(cop, value, rf);

				if (jobInfo.tryTuples)
				{
					// save for expression transformation
					pcss->addFilter(cf);
//					setTupleInfo(ct, sc->oid(), jobInfo, tbOID, sc.get(), alias);
				}
			}
			if (!cf->functionName().empty())
			{
				//This is one of the CNX UDF functions. We want to make this look like
				//SimpleColumn op ConstantColumn, but mark the SimpleColumn as using a distributed UDF
				string fPfx(cf->functionName(), 0, 6);
				if (fPfx != "cpfunc")
				{
					string fn = cf->functionName();
					if (fn == "abs" || fn == "acos" || fn == "asin" || fn == "atan" || fn == "cos" || fn == "cot" ||
						fn == "exp" || fn == "ln" || fn == "log" || fn == "log2" || fn == "log10" || fn == "sin" ||
						fn == "sqrt" || fn == "tan")
						;
					else
						throw runtime_error("Unhandled distributed UDF");
				}
				//set the func on pColStep here...
				pcss->udfName(cf->functionName());
			}
			sjstep.reset(pcss);
			jsv.push_back(sjstep);

			if (jobInfo.tryTuples)
			{
				TupleInfo ti(setTupleInfo(
					jobInfo.csc->colType(sc->oid()), sc->oid(), jobInfo, tblOid, sc.get(), alias));
				pcss->tupleId(ti.key);
			}
		}
	}
	else
	{
		if (jobInfo.tryTuples == true)
			throw logic_error("doConstantFilter: Not handled operation type.");

		cerr << boldStart << "doConstantFilter: Can only handle 'and' and 'or' right now, got '" <<
			opStr << "'" << boldStop << endl;
	}

	return jsv;
}


//------------------------------------------------------------------------------
// Purpose of this function is to change a jobstep association to ZDL.  We call
// this when we need to change HashJoin output to ZDL to feed a UnionStep.
//------------------------------------------------------------------------------
void adjustAssociationForHashJoin(JobStep* js, AnyDataListSPtr& adl, int pos, JobInfo& jobInfo)
{
	// for hashjoin, there are two output datalists,
	// need to put the adl at right position, 0 or 1.
	assert(pos == 0 || pos == 1);
	JobStepAssociation jsa(jobInfo.status);
	adl->zonedDL()->setMultipleProducers(true);
	if (js->outputAssociation().outSize() == 0)
	{
		AnyDataListSPtr adl1(new AnyDataList());
		ZonedDL* dl1 = new ZonedDL(1, jobInfo.rm);
		adl1->zonedDL(dl1);
		dl1->OID(js->oid());

		if (pos == 0)
		{
			jsa.outAdd(adl);
			jsa.outAdd(adl1);
		}
		else
		{
			jsa.outAdd(adl1);
			jsa.outAdd(adl);
		}
	}
	else
	{
		if (pos == 0)
		{
			AnyDataListSPtr adl1 = js->outputAssociation().outAt(1);
			jsa.outAdd(adl);
			jsa.outAdd(adl1);
		}
		else
		{
			AnyDataListSPtr adl1 = js->outputAssociation().outAt(0);
			jsa.outAdd(adl1);
			jsa.outAdd(adl);
		}
	}
	jsa.toSort(1);
	js->outputAssociation(jsa);
}

void buildTableJobStepMap(JobStepVector& jsv, map<UniqId, SJSTEP>& tjMap)
{
	// the map will have the last jobstep in the vector for each table
	JobStepVector::iterator iter = jsv.begin(), end = jsv.end();
	for (; iter != end; ++iter)
	{
		// get the jobstep pointer
		JobStep *js = iter->get();

		// skip the delimiters
		if (dynamic_cast<OrDelimiter*>(js) != NULL)
			continue;

		// add both tables into the map for hashjoin, stringhashjoin, etc.
		HashJoinStep *hjsp = dynamic_cast<HashJoinStep*>(js);
		StringHashJoinStep *shjsp = dynamic_cast<StringHashJoinStep*>(js);
		TupleHashJoinStep *thjsp = dynamic_cast<TupleHashJoinStep*>(js);
		if (hjsp != NULL && hjsp->tableOid1() != 0)
		{
			tjMap[UniqId(hjsp->tableOid1(), hjsp->alias1(), hjsp->view1())] = *iter;
			tjMap[UniqId(hjsp->tableOid2(), hjsp->alias2(), hjsp->view2())] = *iter;
		}
		else if (shjsp != NULL && shjsp->tableOid1() != 0)
		{
			tjMap[UniqId(shjsp->tableOid1(), shjsp->alias1(), shjsp->view1())] = *iter;
			tjMap[UniqId(shjsp->tableOid2(), shjsp->alias2(), shjsp->view2())] = *iter;
		}
		else if (thjsp != NULL && thjsp->tableOid1() != 0)
		{
			//TODO: is this right?
			tjMap[UniqId(thjsp->tableOid1(), thjsp->alias(), thjsp->view())] = *iter;
			tjMap[UniqId(thjsp->tableOid2(), thjsp->alias(), thjsp->view())] = *iter;
		}
		else
		{
			tjMap[UniqId(js->tableOid(), js->alias(), js->view())] = *iter;
		}
	}
}


void doORInTableMode(JobStepVector& jsv, JobStepVector& rhv, JobInfo& jobInfo)
{
	// need union all the matching tables in the rhv and jsv
	// tables are on either side, which may need reducesteps
	map<UniqId, SJSTEP> rhsTableJSMap;
	buildTableJobStepMap(rhv, rhsTableJSMap);
	map<UniqId, SJSTEP> lhsTableJSMap;
	buildTableJobStepMap(jsv, lhsTableJSMap);
	map<UniqId, SJSTEP>::iterator rhsIt = rhsTableJSMap.begin();
	for (; rhsIt != rhsTableJSMap.end(); ++rhsIt)
	{
		map<UniqId, SJSTEP>::iterator lhsIt = lhsTableJSMap.find((*rhsIt).first);
		if (lhsIt == lhsTableJSMap.end())
			continue;

		CalpontSystemCatalog::OID tableOid = rhsIt->first.fId;
		JobStep *rjs = rhsIt->second.get();
		JobStep *ljs = lhsIt->second.get();
		JobStepAssociation rjsa(jobInfo.status), ljsa(jobInfo.status), injsa(jobInfo.status);
		AnyDataListSPtr adl1(new AnyDataList()), adl2(new AnyDataList());

		// unionstep takes ordered fifo or zdl
		FilterStep* rfs = dynamic_cast<FilterStep*>(rjs);
		ReduceStep* rrs = dynamic_cast<ReduceStep*>(rjs);
		UnionStep*  rus = dynamic_cast<UnionStep*>(rjs);
		if (rfs)
		{
			// if filterstep, the output can be string fifo
			JobStepVector::iterator iter = find(rhv.begin(), rhv.end(), rhsIt->second);
			// the filerstep must exist in the job step vector
			//	assert(iter != rhv.end());
			if (typeid(*((iter-1)->get())) == typeid(pDictionaryStep))
			{
				StringFifoDataList* rInput = new StringFifoDataList(1, jobInfo.fifoSize);
				rInput->OID(rjs->oid());
				rInput->inOrder(true);
				adl1->stringDL(rInput);
			}
			else
			{
FIFODEBUG();
				FifoDataList* rInput = new FifoDataList(1, jobInfo.fifoSize);
				rInput->OID(rjs->oid());
				rInput->inOrder(true);
				adl1->fifoDL(rInput);
			}
		}
		else if (rrs || rus)
		{
FIFODEBUG();
			FifoDataList* rInput = new FifoDataList(1, jobInfo.fifoSize);
			rInput->OID(rjs->oid());
			rInput->inOrder(true);
			adl1->fifoDL(rInput);
		}
		else
		{
			if (jobInfo.tryTuples)
			{
				RowGroupDL* rInput = new RowGroupDL(1, jobInfo.fifoSize);
				rInput->OID(rjs->oid());
				adl1->rowGroupDL(rInput);
			}
			else
			{
				ZDL<ElementType>* rInput = new ZonedDL(1, jobInfo.rm);
				rInput->OID(rjs->oid());
				adl1->zonedDL(rInput);
			}
		}

		// unionstep takes ordered fifo or zdl
		FilterStep* lfs = dynamic_cast<FilterStep*>(ljs);
		ReduceStep* lrs = dynamic_cast<ReduceStep*>(ljs);
		UnionStep*  lus = dynamic_cast<UnionStep*>(ljs);
		if (lfs)
		{
			// if filterstep, the output can be string fifo
			JobStepVector::iterator iter = find(jsv.begin(), jsv.end(), lhsIt->second);
			// the filerstep must exist in the job step vector
			//	assert(iter != jsv.end());
			if (typeid(*((iter-1)->get())) == typeid(pDictionaryStep))
			{
				StringFifoDataList* lInput = new StringFifoDataList(1, jobInfo.fifoSize);
				lInput->OID(ljs->oid());
				lInput->inOrder(true);
				adl2->stringDL(lInput);
			}
			else
			{
FIFODEBUG();
				FifoDataList* lInput = new FifoDataList(1, jobInfo.fifoSize);
				lInput->OID(ljs->oid());
				lInput->inOrder(true);
				adl2->fifoDL(lInput);
			}
		}
		else if (lrs || lus)
		{
FIFODEBUG();
			FifoDataList* lInput = new FifoDataList(1, jobInfo.fifoSize);
			lInput->OID(ljs->oid());
			lInput->inOrder(true);
			adl2->fifoDL(lInput);
		}
		else
		{
			if (jobInfo.tryTuples)
			{
				RowGroupDL* lInput = new RowGroupDL(1, jobInfo.fifoSize);
				lInput->OID(ljs->oid());
				adl2->rowGroupDL(lInput);
			}
			else
			{
				ZDL<ElementType>* lInput = new ZonedDL(1, jobInfo.rm);
				lInput->OID(ljs->oid());
				adl2->zonedDL(lInput);
			}
		}

		// handling HashJoin is a little different
		// similar to the ReduceStep above, only touch one datalist
		HashJoinStep* hjsp;
		TupleHashJoinStep* thjsp;
		StringHashJoinStep* shjsp;
		if ((hjsp = dynamic_cast<HashJoinStep*>(rjs)) != NULL)
		{
			if (hjsp->tableOid2() == tableOid)
				adjustAssociationForHashJoin(hjsp, adl1, 1, jobInfo);
			else
				adjustAssociationForHashJoin(hjsp, adl1, 0, jobInfo);
			rjsa = hjsp->outputAssociation();
		}
		else if ((shjsp = dynamic_cast<StringHashJoinStep*>(rjs)) != NULL)
		{
			if (shjsp->tableOid2() == tableOid)
				adjustAssociationForHashJoin(shjsp, adl1, 1, jobInfo);
			else
				adjustAssociationForHashJoin(shjsp, adl1, 0, jobInfo);
			rjsa = shjsp->outputAssociation();
		}
		else if ((thjsp = dynamic_cast<TupleHashJoinStep*>(rjs)) != NULL)
		{
			if (thjsp->tableOid1() != 0)
				throw runtime_error("Unhandled Union 1");

			AnyDataListSPtr adl0(new AnyDataList());
			RowGroupDL* input0 = new RowGroupDL(1, jobInfo.fifoSize);
			input0->OID(0);
			adl0->rowGroupDL(input0);
			rjsa.outAdd(adl0);
			rjsa.outAdd(adl1);
			adl1->rowGroupDL()->OID(thjsp->inputAssociation().outAt(1)->dataList()->OID());
		}
		else
		{
			rjsa.outAdd(adl1);
		}

		if ((hjsp = dynamic_cast<HashJoinStep*>(ljs)) != NULL)
		{
			if (hjsp->tableOid2() == tableOid)
				adjustAssociationForHashJoin(hjsp, adl2, 1, jobInfo);
			else
				adjustAssociationForHashJoin(hjsp, adl2, 0, jobInfo);
			ljsa = hjsp->outputAssociation();
		}
		else if ((shjsp = dynamic_cast<StringHashJoinStep*>(ljs)) != NULL)
		{
			if (shjsp->tableOid2() == tableOid)
				adjustAssociationForHashJoin(shjsp, adl2, 1, jobInfo);
			else
				adjustAssociationForHashJoin(shjsp, adl2, 0, jobInfo);
			ljsa = shjsp->outputAssociation();
		}
		else if ((thjsp = dynamic_cast<TupleHashJoinStep*>(ljs)) != NULL)
		{
			if (thjsp->tableOid1() != 0)
				throw runtime_error("Unhandled Union 2");

			AnyDataListSPtr adl0(new AnyDataList());
			RowGroupDL* input0 = new RowGroupDL(1, jobInfo.fifoSize);
			input0->OID(0);
			adl0->rowGroupDL(input0);
			ljsa.outAdd(adl0);
			ljsa.outAdd(adl2);
			adl2->rowGroupDL()->OID(thjsp->inputAssociation().outAt(1)->dataList()->OID());
		}
		else
		{
			ljsa.outAdd(adl2);
		}

		rjs->outputAssociation(rjsa);
		ljs->outputAssociation(ljsa);

		injsa.outAdd(adl1);
		injsa.outAdd(adl2);

		SJSTEP suStep;
		UnionStep *uStep = new UnionStep(injsa, JobStepAssociation(jobInfo.status),
			rjs->tableOid(),
			jobInfo.sessionId,
			jobInfo.txnId,
			jobInfo.verId,
			0, // stepId
			jobInfo.statementId);
		suStep.reset(uStep);
		uStep->alias1(rjs->alias());
		uStep->alias2(ljs->alias());
		uStep->view1(rjs->view());
		uStep->view2(ljs->view());

		rhv.push_back(suStep);

		// update the table map
		lhsTableJSMap.erase(lhsIt);
	}

	// add a delimiter at the beginning of left and right operands,
	// and concat jsv and rhv into tmpJsv
	JobStepVector tmpJsv;
	SJSTEP sdelim;
	sdelim.reset(new OrDelimiterLhs());
	tmpJsv.push_back(sdelim);
	tmpJsv.insert(tmpJsv.end(), jsv.begin(), jsv.end());
	sdelim.reset(new OrDelimiterRhs());
	tmpJsv.push_back(sdelim);
	tmpJsv.insert(tmpJsv.end(), rhv.begin(), rhv.end());

	/* replace the jsv with the tmpJsv*/
	jsv.swap(tmpJsv);

	/* At this point, jsv contains DELIM lhv DELIM rhv UNIONSTEP */
	jobInfo.stack.push(jsv);
}


void doAND(JobStepVector& jsv, JobInfo& jobInfo)
{
//	assert(jobInfo.stack.size() >= 2);
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

	if (tryCombineFilters(jsv, rhv, BOP_AND, jobInfo.tryTuples))
	{
		jobInfo.stack.push(jsv);
		return;
	}

	// concat rhv into jsv
	jsv.insert(jsv.end(), rhv.begin(), rhv.end());
	jobInfo.stack.push(jsv);
}


void doOR(const ParseTree* n, JobStepVector& jsv, JobInfo& jobInfo)
{
	assert(jobInfo.stack.size() >= 2);
	JobStepVector rhv = jobInfo.stack.top();
	jobInfo.stack.pop();
	jsv = jobInfo.stack.top();
	jobInfo.stack.pop();

	/* XXXPAT: The dumbest implementation is to blindly connect
	the outputs of rhv and jsv to the inputs of a union step.
	Optimization: combine the filters of like pCols on the same
	OID */
	//@bug 664. comment out inappropriate  assertation
//@bug 664//	assert(rhv.size() > 0);
//@bug 664//	assert(jsv.size() > 0);

	// @bug3570, attempt to combine only if there is one column involved.
	if (((jsv.size() == 1 && (typeid(*(jsv.begin()->get())) == typeid(pColStep))) ||
		 (jsv.size() == 2 && (typeid(*((jsv.end()-1)->get())) == typeid(pDictionaryStep))) ||
		 (jsv.size() == 3 && (typeid(*(jsv.begin()->get())) == typeid(pDictionaryScan)))) &&
 		tryCombineFilters(jsv, rhv, BOP_OR, jobInfo.tryTuples))
	{
		jobInfo.stack.push(jsv);
		return;
	}

	// table-mode and vtable-mode handles OR differently
	if (jobInfo.tryTuples == true)
	{
		// OR is processed as an expression
		jsv = doExpressionFilter(n, jobInfo);
		jobInfo.stack.push(jsv);
	}
	else
	{
		doORInTableMode(jsv, rhv, jobInfo);
	}
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
			doOR(n, jsv, *jobInfo);
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
		if (jobInfo->tryTuples == true)
		{
			jsv = doExpressionFilter(n, *jobInfo);
			jobInfo->stack.push(jsv);
		}
		else
		{
			cerr << boldStart << "walkTree: FUNCTIONCOLUMN in table-mode" << boldStop << endl;
		}
		break;
	case SIMPLECOLUMN:
		if (jobInfo->tryTuples == true)
		{
			jsv = doExpressionFilter(n, *jobInfo);
			jobInfo->stack.push(jsv);
		}
		else
		{
			cerr << boldStart << "walkTree: SIMPLECOLUMN filter in table-mode" << boldStop << endl;
		}
		break;
	case CONSTANTCOLUMN:
		if (jobInfo->tryTuples == true)
		{
			jsv = doConstantBooleanFilter(n, *jobInfo);
			jobInfo->stack.push(jsv);
		}
		else
		{
			cerr << boldStart << "walkTree: SIMPLECOLUMN filter in table-mode" << boldStop << endl;
		}
		break;
	case SIMPLESCALARFILTER:
		if (jobInfo->tryTuples == true)
		{
			doSimpleScalarFilter(n, *jobInfo);
		}
		else
		{
			cerr << boldStart << "walkTree: Subquery in table-mode" << boldStop << endl;
		}
		break;
	case EXISTSFILTER:
		if (jobInfo->tryTuples == true)
		{
			doExistsFilter(n, *jobInfo);
		}
		else
		{
			cerr << boldStart << "walkTree: Subquery in table-mode" << boldStop << endl;
		}
		break;
	case SELECTFILTER:
		if (jobInfo->tryTuples == true)
		{
			doSelectFilter(n, *jobInfo);
		}
		else
		{
			cerr << boldStart << "walkTree: Subquery in table-mode" << boldStop << endl;
		}
		break;
	case UNKNOWN:
		if (jobInfo->tryTuples == true)
			throw logic_error("walkTree: unknow type.");

		cerr << boldStart << "walkTree: Unknown" << boldStop << endl;
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
		if (jobInfo->tryTuples == true)
			throw logic_error("walkTree: Not handled treeNode type.");

		cerr << boldStart << "walkTree: Not handled: " << TreeNode2Type(tn) << boldStop << endl;
		break;
	}
	//cout << *tn << endl;
}

} // end of joblist namespace
// vim:ts=4 sw=4:

