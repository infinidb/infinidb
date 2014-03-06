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

// $Id: dmlif.cpp 2101 2013-01-21 14:12:52Z rdempsey $

//#define NDEBUG
#include <cassert>
#include <string>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
using namespace boost;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "simplecolumn.h"
#include "calpontselectexecutionplan.h"
#include "sessionmanager.h"
#include "simplefilter.h"
#include "constantcolumn.h"
#include "constantfilter.h"
using namespace execplan;

#include "brmtypes.h"

#include "dmlif.h"
using namespace dmlif;

namespace dmlif
{

DMLIF::DMLIF(uint32_t sessionid, uint32_t tflg, bool dflg, bool vflg) :
	fSessionID(sessionid), fTflg(tflg), fDflg(dflg), fVflg(vflg), fOPt(0), fLPt(0)
{
	fMqp.reset(new MessageQueueClient("DMLProc"));
}

DMLIF::~DMLIF()
{
}

int DMLIF::sendOne(const string& stmt)
{
	int rc;

	string tStmt(stmt);
	if (*tStmt.rbegin() != ';')
		tStmt += ";";

	VendorDMLStatement dmlStmt(tStmt, fSessionID);
	CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
	if (pDMLPackage == 0)
	{
		cerr << "Failed to parse statement: " << tStmt << endl;
		return -1;
	}

	string queryString = pDMLPackage->get_QueryString();
	if (fDflg) cout << "qs: >" << queryString << '<' << endl;

	string dmlStatement = pDMLPackage->get_DMLStatement();
	if (fDflg) cout << "DML: " << dmlStatement << endl;
	bool isDML = true;
	if (dmlStatement == "COMMIT" || dmlStatement == "ROLLBACK")
	{
		isDML = false;
	}

	if (isDML)
	{
		char_separator<char> sep(" ");
		tokenizer<char_separator<char> > tok(queryString, sep);
		tokenizer<char_separator<char> >::iterator iter = tok.begin();
		idbassert(iter != tok.end());
		string where = *iter; ++iter;
		idbassert(iter != tok.end());
		string col1 = *iter; ++iter;
		idbassert(iter != tok.end());
		string op = *iter; ++iter;
		idbassert(iter != tok.end());
		string col2 = *iter; ++iter;
		idbassert(iter == tok.end());
		if (fDflg) cout << "SQL: " << pDMLPackage->get_SQLStatement() << endl;
		if (fDflg) cout << "hf: " << pDMLPackage->HasFilter() << endl;
		DMLTable* tp = pDMLPackage->get_Table();
		if (fDflg) cout << "sn: " << tp->get_SchemaName() << " tn: " << tp->get_TableName() << endl;
		if (fDflg) cout << "row count: " << tp->get_RowList().size() << endl;
		SRCP srcp(new SimpleColumn(tp->get_SchemaName(), tp->get_TableName(), col1, fSessionID));
		CalpontSelectExecutionPlan::ColumnMap cm;
		cm.insert(make_pair(col1, srcp));
		pDMLPackage->get_ExecutionPlan()->columnMap(cm);
		CalpontSelectExecutionPlan::ReturnedColumnList rcl;
		rcl.push_back(srcp);
		pDMLPackage->get_ExecutionPlan()->returnedCols(rcl);
		pDMLPackage->get_ExecutionPlan()->sessionID(fSessionID);
		pDMLPackage->get_ExecutionPlan()->traceFlags(fTflg);
		SessionManager sm;
		BRM::TxnID txnid = sm.getTxnID(fSessionID);
		if (!txnid.valid)
			txnid = sm.newTxnID(fSessionID);
		pDMLPackage->get_ExecutionPlan()->txnID(txnid.id);
		pDMLPackage->get_ExecutionPlan()->verID(sm.verID());
		ParseTree* pt = new ParseTree();
		ReturnedColumn* rc1 = srcp->clone();
		ReturnedColumn* rc2 = new ConstantColumn(col2, ConstantColumn::NUM);
		SOP sop(new Operator(op));
		SimpleFilter* sf = new SimpleFilter(sop, rc1, rc2);
		pt->data(sf);
		pDMLPackage->get_ExecutionPlan()->filters(pt);
		if (fDflg) cout << "ep: " << *pDMLPackage->get_ExecutionPlan() << endl;
	}

	ByteStream bytestream;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	ByteStream::octbyte rows;

	rc = DMLSend(bytestream, rows);

	if (isDML && fVflg)
		cout << rows << " rows affected" << endl;

	return rc;
}

int DMLIF::DMLSend(ByteStream& bytestream, ByteStream::octbyte& rows)
{
	ByteStream::byte b;
	string errorMsg;
	try
	{
		fMqp->connect();
		fMqp->write(bytestream);
		bytestream = fMqp->read();
		fMqp->shutdown();
		if (fDflg) cout << "read " << bytestream.length() << " bytes from DMLProc" << endl;
		bytestream >> b;
		if (fDflg) cout << "b = " << (int)b << endl;
		bytestream >> rows;
		if (fDflg) cout << "rows = " << rows << endl;
		bytestream >> errorMsg;
		if (fDflg) cout << "errorMsg = " << errorMsg << endl;
	}
	catch (runtime_error& rex)
	{
		cerr << "runtime_error in engine: " << rex.what() << endl;
		return -1;
	}
	catch (...)
	{
		cerr << "uknown error in engine" << endl;
		return -1;
	}

	if (b != 0)
	{
		cerr << "DMLProc error: " << errorMsg << endl;
		return -1;
	}

	return 0;
}

void DMLIF::rf2Start(const string& sn)
{
	fSchema = sn;
	fOFilterStr = "";
	fLFilterStr = "";
	fOPt = 0;
	fLPt = 0;
}

void DMLIF::rf2Add(int64_t okey)
{
	ostringstream oss;
	oss << okey;
	string okeyStr(oss.str());

	if (fOFilterStr.empty())
	{
		fOFilterStr = "o_orderkey=" + okeyStr;
		ReturnedColumn* rc1 = new SimpleColumn(fSchema, "orders", "o_orderkey", fSessionID);
		ReturnedColumn* rc2 = new ConstantColumn(okeyStr, ConstantColumn::NUM);
		SOP sop(new Operator("="));
		ConstantFilter* cf = new ConstantFilter(sop, rc1, rc2);
		sop.reset(new Operator("or"));
		cf->op(sop);
		fOPt = new ParseTree(cf);
	}
	else
	{
		fOFilterStr += " or o_orderkey=" + okeyStr;
		ReturnedColumn* rc1 = new SimpleColumn(fSchema, "orders", "o_orderkey", fSessionID);
		ReturnedColumn* rc2 = new ConstantColumn(okeyStr, ConstantColumn::NUM);
		SOP sop(new Operator("="));
		ConstantFilter* cf = dynamic_cast<ConstantFilter*>(fOPt->data());
		cf->pushFilter(new SimpleFilter(sop, rc1, rc2));
	}
	if (fLFilterStr.empty())
	{
		fLFilterStr = "l_orderkey=" + okeyStr;
		ReturnedColumn* rc1 = new SimpleColumn(fSchema, "lineitem", "l_orderkey", fSessionID);
		ReturnedColumn* rc2 = new ConstantColumn(okeyStr, ConstantColumn::NUM);
		SOP sop(new Operator("="));
		ConstantFilter* cf = new ConstantFilter(sop, rc1, rc2);
		sop.reset(new Operator("or"));
		cf->op(sop);
		fLPt = new ParseTree(cf);
	}
	else
	{
		fLFilterStr += " or l_orderkey=" + okeyStr;
		ReturnedColumn* rc1 = new SimpleColumn(fSchema, "lineitem", "l_orderkey", fSessionID);
		ReturnedColumn* rc2 = new ConstantColumn(okeyStr, ConstantColumn::NUM);
		SOP sop(new Operator("="));
		ConstantFilter* cf = dynamic_cast<ConstantFilter*>(fLPt->data());
		cf->pushFilter(new SimpleFilter(sop, rc1, rc2));
	}
}

int DMLIF::rf2Send()
{
	if (fOFilterStr.empty())
		return -1;

	int rc = 0;
	string dmlstr;
	dmlstr = "delete from " + fSchema + ".orders where " + fOFilterStr + ';';
	if (fDflg) cout << dmlstr << endl;

	VendorDMLStatement dmlStmt(dmlstr, fSessionID);
	CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);
	if (pDMLPackage == 0)
	{
		cerr << "Failed to parse statement: " << dmlstr << endl;
		return -1;
	}

	SRCP srcp(new SimpleColumn(fSchema, "orders", "o_orderkey", fSessionID));
	CalpontSelectExecutionPlan::ColumnMap cm;
	cm.insert(make_pair("o_orderkey", srcp));
	pDMLPackage->get_ExecutionPlan()->columnMap(cm);
	CalpontSelectExecutionPlan::ReturnedColumnList rcl;
	rcl.push_back(srcp);
	pDMLPackage->get_ExecutionPlan()->returnedCols(rcl);
	pDMLPackage->get_ExecutionPlan()->sessionID(fSessionID);
	pDMLPackage->get_ExecutionPlan()->traceFlags(fTflg);
	SessionManager sm;
	BRM::TxnID txnid = sm.getTxnID(fSessionID);
	if (!txnid.valid)
		txnid = sm.newTxnID(fSessionID);
	pDMLPackage->get_ExecutionPlan()->txnID(txnid.id);
	pDMLPackage->get_ExecutionPlan()->verID(sm.verID());
	pDMLPackage->get_ExecutionPlan()->filters(fOPt);
	if (fDflg) cout << "ep: " << *pDMLPackage->get_ExecutionPlan() << endl;

	ByteStream bytestream;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	pDMLPackage = 0;
	ByteStream::octbyte rows = 0;

	rc = DMLSend(bytestream, rows);

	if (fVflg)
		cout << rows << " rows affected" << endl;

	dmlstr = "delete from " + fSchema + ".lineitem where " + fLFilterStr + ';';
	if (fDflg) cout << dmlstr << endl;

	VendorDMLStatement dmlStmt1(dmlstr, fSessionID);
	pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt1);
	if (pDMLPackage == 0)
	{
		cerr << "Failed to parse statement: " << dmlstr << endl;
		return -1;
	}

	srcp.reset(new SimpleColumn(fSchema, "lineitem", "l_orderkey", fSessionID));
	cm.clear();
	cm.insert(make_pair("l_orderkey", srcp));
	pDMLPackage->get_ExecutionPlan()->columnMap(cm);
	rcl.clear();
	rcl.push_back(srcp);
	pDMLPackage->get_ExecutionPlan()->returnedCols(rcl);
	pDMLPackage->get_ExecutionPlan()->sessionID(fSessionID);
	pDMLPackage->get_ExecutionPlan()->traceFlags(fTflg);
	txnid = sm.getTxnID(fSessionID);
	if (!txnid.valid)
		txnid = sm.newTxnID(fSessionID);
	pDMLPackage->get_ExecutionPlan()->txnID(txnid.id);
	pDMLPackage->get_ExecutionPlan()->verID(sm.verID());
	pDMLPackage->get_ExecutionPlan()->filters(fLPt);
	if (fDflg) cout << "ep: " << *pDMLPackage->get_ExecutionPlan() << endl;

	bytestream.reset();
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	pDMLPackage = 0;
	rows = 0;

	rc = DMLSend(bytestream, rows);

	if (fVflg)
		cout << rows << " rows affected" << endl;

	return 0;
}

}

