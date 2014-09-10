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

//  $Id: jlf_common.cpp 9655 2013-06-25 23:08:13Z xlou $


#include "calpontsystemcatalog.h"
#include "aggregatecolumn.h"
#include "pseudocolumn.h"
#include "simplecolumn.h"
using namespace std;
using namespace execplan;

#include "messagelog.h"
using namespace logging;

#include <boost/algorithm/string/case_conv.hpp>
namespace ba=boost::algorithm;

#include "dbrm.h"
#include "extentmap.h"
using namespace BRM;

#include "jlf_common.h"
using namespace joblist;

namespace
{

// @brief Returns unique key for a column, table, or expresssion.
uint32_t uniqTupleKey(JobInfo& jobInfo,
                      CalpontSystemCatalog::OID& o,
                      CalpontSystemCatalog::OID& t,
                      const string& cn,
                      const string& ca,
                      const string& tn,
                      const string& ta,
                      const string& sn,
                      const string& vw,
                      uint32_t      pi,
                      uint64_t      en,
                      bool correlated=false)
{
	uint64_t subId = jobInfo.subId;
	if (correlated)
		subId = jobInfo.pJobInfo->subId;

	string alias(ta);
//	if (!ca.empty())
//		alias += "." + ca;
	string nm(ta);
	if (!cn.empty())
		nm += "." + cn;
	UniqId id(o, ta, sn, vw, pi, subId);
	TupleKeyMap::iterator iter = jobInfo.keyInfo->tupleKeyMap.find(id);
	if (iter != jobInfo.keyInfo->tupleKeyMap.end())
		return iter->second;

	uint32_t newId = jobInfo.keyInfo->nextKey++;
//cout << "new id: " << newId << " -- " << o << ", " << pi << ", " << nm << ", " << vw << ", " << sn << ", " << subId << endl;
	jobInfo.keyInfo->tupleKeyMap[id] = newId;
	jobInfo.keyInfo->tupleKeyVec.push_back(id);
	jobInfo.keyInfo->tupleKeyToTableOid.insert(make_pair(newId, t));
	jobInfo.keyInfo->crossEngine.push_back((en != 0));

	string ss = vw;
	if (ss.length() > 0)
		ss += ".";

	if (sn.length() > 0)
		ss += sn + ".";

	if (o != t)
	{
		string name = cn;
		if (!ca.empty()) // has an alias
			name = ca;
		else if (ta.compare(0,5,"$sub_") && ta.compare(0,4,"$exp")) // compare != 0
			name = ss + ta + "." + name;

		jobInfo.keyInfo->tupleKeyToName.push_back(name);
		jobInfo.keyInfo->keyName.insert(make_pair(newId, cn));
	}
	else
	{
		string name = ta;
		bool useAlias = true;
		if (name.empty())
		{
			name = tn;
			useAlias = false;
		}

		if (tn.compare(0,4,"$sub") && tn.compare(0,4,"$exp"))
		{
			if (!useAlias)
				name = ss + name;
		}
		else if (tn.compare(0,4,"$sub") == 0)
		{
			name = "sub-query";
		}
		else
		{
			name = "expression";
		}

		jobInfo.keyInfo->tupleKeyToName.push_back(name);
		jobInfo.keyInfo->keyName.insert(make_pair(newId, tn));
	}

	return newId;
}


// @brief Returns a suitably fudged column width
uint32_t fudgeWidth(const CalpontSystemCatalog::ColType& ict, CalpontSystemCatalog::OID oid)
{
	CalpontSystemCatalog::OID dictOid = isDictCol(ict);
	CalpontSystemCatalog::ColType ct = ict;
	if (ct.colDataType != CalpontSystemCatalog::VARBINARY)
	{
		if (ct.colDataType == CalpontSystemCatalog::VARCHAR)
			ct.colWidth++;

		//Round colWidth up
		if (ct.colWidth == 3)
			ct.colWidth = 4;
		else if (ct.colWidth == 5 || ct.colWidth == 6 || ct.colWidth == 7)
			ct.colWidth = 8;
		else if (dictOid > 0 && oid != dictOid)  // token column
			ct.colWidth = 8;
	}
	else
	{
		ct.colWidth += 2;  // insert the length bytes
	}

	return ct.colWidth;
}


// @brief Set some tuple info
TupleInfo setTupleInfo_(const CalpontSystemCatalog::ColType& ct,
                        CalpontSystemCatalog::OID col_oid,
                        JobInfo& jobInfo,
                        CalpontSystemCatalog::OID tbl_oid,
                        const string& col_name,
                        const string& col_alias,
                        const string& sch_name,
                        const string& tbl_name,
                        const string& tbl_alias,
                        const string& vw_name,
                        bool correlated = false,
                        uint32_t pc_id = 0,
                        uint64_t engine = 0)
{
	// get the unique tupleOids for this column
	uint32_t tbl_key = uniqTupleKey(jobInfo, tbl_oid, tbl_oid, "", "", tbl_name, tbl_alias,
	                                sch_name, vw_name, 0, engine, correlated);
	uint32_t col_key = uniqTupleKey(jobInfo, col_oid, tbl_oid, col_name, col_alias, tbl_name,
                                    tbl_alias, sch_name, vw_name, pc_id, engine, correlated);
	//If this is the first time we've seen this col, add it to the tim
	TupleInfoMap::iterator it = jobInfo.keyInfo->tupleInfoMap.find(col_key);
	TupleInfo ti;
	if (it != jobInfo.keyInfo->tupleInfoMap.end())
	{
		//We've seen the key
		ti = it->second;
	}
	else
	{
		//Haven't even seen the table yet, much less this col
		ti = TupleInfo(fudgeWidth(ct, col_oid), col_oid, col_key, tbl_key,
	                             ct.scale, ct.precision, ct.colDataType);
		jobInfo.keyInfo->tupleInfoMap[col_key] = ti;
		jobInfo.keyInfo->colKeyToTblKey[col_key] = tbl_key;
		jobInfo.keyInfo->colKeyToTblKey[tbl_key] = tbl_key;
		jobInfo.keyInfo->colType[col_key] = ct;
		jobInfo.keyInfo->pseudoType[col_key] = pc_id;
	}

	if (pc_id > 0 && jobInfo.pseudoColTable.find(tbl_key) == jobInfo.pseudoColTable.end())
		jobInfo.pseudoColTable.insert(tbl_key);

	return ti;
}


uint32_t getTupleKey_(const JobInfo& jobInfo,
				CalpontSystemCatalog::OID oid,
				const string& colName,
				const string& tblAlias,
				const string& schema,
				const string& view,
				bool correlated = false,
				uint32_t pseudo = 0,
				uint64_t engine = 0)
{
	uint64_t subId = jobInfo.subId;
	if (correlated)
		subId = jobInfo.pJobInfo->subId;

	string alias(tblAlias);
	string name(tblAlias);
	if (!colName.empty())
		name += "." + colName;
//	if (!colAlias.empty())
//		alias += "." + colAlias;
	UniqId id(oid, tblAlias, schema, view, pseudo, subId);
	TupleKeyMap::const_iterator iter = jobInfo.keyInfo->tupleKeyMap.find(id);
	if (iter != jobInfo.keyInfo->tupleKeyMap.end())
		return iter->second;

	// dictionaryscan tableOid is 0 in the tuplehashjoin.
	if (oid != 0)
	{
		ostringstream strstm;
		strstm << "(" << oid << ", ";
		if (!alias.empty())
			strstm << alias;
		if (!view.empty())
			strstm << ", " << view;
		strstm << ") not found in tuple info map.";

		Message::Args args;
		args.add(strstm.str());
		jobInfo.logger->logMessage(LOG_TYPE_DEBUG, LogMakeJobList, args,
			LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));
		cerr << strstm.str() << endl;

//cout << "not found: " << oid << ", " << name << ", " << view << ", " << schema << ", " << subId << endl;
		throw logic_error("column is not found in info map.");
	}

	return static_cast<uint32_t>(-1);
}
}


namespace joblist
{

UniqId::UniqId(const execplan::SimpleColumn* sc) :
	fId(sc->oid()),
	//fName(extractTableAlias(sc)+"."+sc->columnName()),
	fTable(extractTableAlias(sc)),
	fSchema(sc->schemaName()),
	fView(sc->viewName()),
	fPseudo(0),
	fSubId(-1)
{
	const PseudoColumn* pc = dynamic_cast<const execplan::PseudoColumn*>(sc);
	uint32_t pseudoType = (pc) ? pc->pseudoType() : execplan::PSEUDO_UNKNOWN;
	fPseudo = pseudoType;
}


UniqId::UniqId(int o, const execplan::SimpleColumn* sc) :
	fId(o),
	//fName(extractTableAlias(sc)+"."+sc->columnName()),
	fTable(extractTableAlias(sc)),
	fSchema(sc->schemaName()),
	fView(sc->viewName()),
	fPseudo(0),
	fSubId(-1)
{
}


string UniqId::toString() const
{
	ostringstream strstm;
	strstm << fId << ":" << fTable << ":" << fSchema << ":" << fView << ":"
	       << fPseudo << ":" << (int64_t)fSubId;
	return strstm.str();
}


//------------------------------------------------------------------------------
// Returns the table alias for the specified column
//------------------------------------------------------------------------------
string extractTableAlias(const SimpleColumn* sc)
{
	return  ba::to_lower_copy(sc->tableAlias());
}


//------------------------------------------------------------------------------
// Returns the table alias for the specified column
//------------------------------------------------------------------------------
string extractTableAlias(const SSC& sc)
{
	return  ba::to_lower_copy(sc->tableAlias());
}


//------------------------------------------------------------------------------
// Returns OID associated with colType if it is a dictionary column, else
// the value returned for the OID is 0.
//------------------------------------------------------------------------------
CalpontSystemCatalog::OID isDictCol(const CalpontSystemCatalog::ColType& colType)
{
	if (colType.colDataType == CalpontSystemCatalog::BIN16) return 0;
	if (colType.colWidth > 8) return colType.ddn.dictOID;
	if (colType.colDataType == CalpontSystemCatalog::VARCHAR &&
		colType.colWidth > 7) return colType.ddn.dictOID;
	if (colType.colDataType == CalpontSystemCatalog::VARBINARY)
		return colType.ddn.dictOID;

	return 0;
}


//------------------------------------------------------------------------------
// Determines if colType is a character column
//------------------------------------------------------------------------------
bool isCharCol(const CalpontSystemCatalog::ColType& colType)
{
	switch (colType.colDataType)
	{
	case CalpontSystemCatalog::VARCHAR:
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::BLOB:
	case CalpontSystemCatalog::CLOB:
		return true;
		break;
	default:
		return false;
		break;
	}
	return false;
}


//------------------------------------------------------------------------------
// Returns OID associated with a table
//------------------------------------------------------------------------------
CalpontSystemCatalog::OID tableOid(const SimpleColumn* sc, boost::shared_ptr<CalpontSystemCatalog> cat)
{
	if (sc->schemaName().empty())
		return execplan::CNX_VTABLE_ID;

	if (sc->isInfiniDB() == false)
		return 0;

	CalpontSystemCatalog::ROPair p = cat->tableRID(make_table(sc->schemaName(),
		sc->tableName()));
	return p.objnum;
}


uint32_t getTupleKey(const JobInfo& jobInfo,
				const execplan::SimpleColumn* sc)
{
	const PseudoColumn* pc = dynamic_cast<const execplan::PseudoColumn*>(sc);
	uint32_t pseudoType = (pc) ? pc->pseudoType() : execplan::PSEUDO_UNKNOWN;
	return getTupleKey_(jobInfo, sc->oid(), sc->columnName(), extractTableAlias(sc),
	                    sc->schemaName(), sc->viewName(),
	                    ((sc->joinInfo() & execplan::JOIN_CORRELATED) != 0),
	                    pseudoType, (sc->isInfiniDB() ? 0 : 1));
}


uint32_t getTupleKey(JobInfo& jobInfo, const SRCP& srcp, bool add)
{
	int key = -1;

	if (add)
	{
		// setTupleInfo first if add is ture, ok if already set.
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp.get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
			{
				SimpleColumn tmp(*sc, jobInfo.sessionId);
				tmp.oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
				key = getTupleKey(jobInfo, &tmp); // sub-query should be there
			}
			else
			{
				CalpontSystemCatalog::ColType ct = sc->colType();
				string alias(extractTableAlias(sc));
				CalpontSystemCatalog::OID tblOid = tableOid(sc, jobInfo.csc);
				TupleInfo ti(setTupleInfo(ct, sc->oid(), jobInfo, tblOid, sc, alias));
				key = ti.key;

				CalpontSystemCatalog::OID dictOid = isDictCol(ct);
				if (dictOid > 0)
				{
					ti = setTupleInfo(ct, dictOid, jobInfo, tblOid, sc, alias);
					jobInfo.keyInfo->dictKeyMap[key] = ti.key;
					key = ti.key;
				}
			}
		}
		else
		{
			CalpontSystemCatalog::ColType ct = srcp->resultType();
			TupleInfo ti(setExpTupleInfo(ct, srcp->expressionId(), srcp->alias(), jobInfo));
			key = ti.key;
		}
	}
	else
	{
		// TupleInfo is expected to be set already
		const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(srcp.get());
		if (sc != NULL)
		{
			if (sc->schemaName().empty())
			{
				SimpleColumn tmp(*sc, jobInfo.sessionId);
				tmp.oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());
				key = getTupleKey(jobInfo, &tmp);
			}
			else
			{
				key = getTupleKey(jobInfo, sc);
			}

			// check if this is a dictionary column
			if (jobInfo.keyInfo->dictKeyMap.find(key) != jobInfo.keyInfo->dictKeyMap.end())
				key = jobInfo.keyInfo->dictKeyMap[key];
		}
		else
		{
			key = getExpTupleKey(jobInfo, srcp->expressionId());
		}
	}

	return key;
}


uint32_t getTableKey(const JobInfo& jobInfo, execplan::CalpontSystemCatalog::OID tableOid,
                     const string& alias, const string& schema, const string& view)
{
	return getTupleKey_(jobInfo, tableOid, "", alias, schema, view);
}


uint32_t getTableKey(const JobInfo& jobInfo, uint32_t cid)
{
	return jobInfo.keyInfo->colKeyToTblKey[cid];
}


void updateTableKey(uint32_t cid, uint32_t tid, JobInfo& jobInfo)
{
	jobInfo.keyInfo->colKeyToTblKey[cid] = tid;
}


uint32_t getTableKey(JobInfo& jobInfo, JobStep* js)
{
	CalpontSystemCatalog::OID tableOid = js->tableOid();
	return getTupleKey_(jobInfo, tableOid, "", js->alias(), js->schema(), js->view());
}


uint32_t makeTableKey(JobInfo& jobInfo, const execplan::SimpleColumn* sc)
{
	CalpontSystemCatalog::OID o = tableOid(sc, jobInfo.csc);
	return uniqTupleKey(jobInfo, o, o, "", "", sc->tableName(), extractTableAlias(sc),
	                    sc->schemaName(), sc->viewName(), 0, (sc->isInfiniDB() ? 0 : 1),
	                    ((sc->joinInfo() & execplan::JOIN_CORRELATED) != 0));
}


uint32_t makeTableKey(JobInfo& jobInfo,
				CalpontSystemCatalog::OID o,
				const string& tn,
				const string& ta,
				const string& sn,
				const string& vn,
				uint64_t      en)
{
	return uniqTupleKey(jobInfo, o, o, "", "", tn, ta, sn, vn, 0, en);
}


TupleInfo getTupleInfo(uint32_t columnKey, const JobInfo& jobInfo)
{
	TupleInfoMap::const_iterator cit = jobInfo.keyInfo->tupleInfoMap.find(columnKey);
	if ((cit == jobInfo.keyInfo->tupleInfoMap.end()) ||
	    (cit->second.dtype == CalpontSystemCatalog::BIT))
	{
		ostringstream strstm;
		strstm << "TupleInfo for (" << jobInfo.keyInfo->tupleKeyVec[columnKey].fId << ","
			<< jobInfo.keyInfo->tupleKeyVec[columnKey].fTable;
		if (jobInfo.keyInfo->tupleKeyVec[columnKey].fView.length() > 0)
			strstm << "," << jobInfo.keyInfo->tupleKeyVec[columnKey].fView;
		strstm << ") could not be found." << endl;
		cerr << strstm.str();

		Message::Args args;
		args.add(strstm.str());
		jobInfo.logger->logMessage(LOG_TYPE_DEBUG, LogMakeJobList, args,
			LoggingID(5, jobInfo.sessionId, jobInfo.txnId, 0));

		throw runtime_error("column's tuple info could not be found");
	}

	return cit->second;
}


TupleInfo setTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct,
	execplan::CalpontSystemCatalog::OID col_oid,
	JobInfo& jobInfo,
	execplan::CalpontSystemCatalog::OID tbl_oid,
	const execplan::SimpleColumn* sc,
	const string& alias)
{
	const PseudoColumn* pc = dynamic_cast<const execplan::PseudoColumn*>(sc);
	uint32_t pseudoType = (pc) ? pc->pseudoType() : execplan::PSEUDO_UNKNOWN;
	return setTupleInfo_(ct, col_oid, jobInfo, tbl_oid, sc->columnName(), sc->alias(),
	                     sc->schemaName(), sc->tableName(), alias, sc->viewName(),
	                     ((sc->joinInfo() & execplan::JOIN_CORRELATED) != 0),
	                     pseudoType, (sc->isInfiniDB() ? 0 : 1));
}


TupleInfo setExpTupleInfo(const execplan::CalpontSystemCatalog::ColType& ct, uint64_t expressionId,
	const string& alias, JobInfo& jobInfo, bool cr)
{
	// pretend all expressions belong to "virtual" table EXPRESSION, (CNX_EXP_TABLE_ID, expression)
	// CNX_EXP_TABLE_ID(999) is not for user table or column, there will be no confilict in queries.
	JobInfo* ji = &jobInfo;
	if (cr)
		ji = jobInfo.pJobInfo;

	string expAlias("$exp");
	if (!(ji->subAlias.empty()))
		expAlias = ji->subAlias;

	return setTupleInfo_(
		ct, expressionId, jobInfo, CNX_EXP_TABLE_ID, "", alias, "", "$exp", expAlias, "", cr);
}


TupleInfo setExpTupleInfo(const execplan::ReturnedColumn* rc, JobInfo& jobInfo)
{
	return setExpTupleInfo(rc->resultType(), rc->expressionId(), rc->alias(), jobInfo,
	                       ((rc->joinInfo() & execplan::JOIN_CORRELATED) != 0));
}


uint32_t getExpTupleKey(const JobInfo& jobInfo, uint64_t eid, bool cr)
{
	const JobInfo* ji = &jobInfo;
	if (cr)
		ji = jobInfo.pJobInfo;

	string expAlias("$exp");
	if (!(ji->subAlias.empty()))
		expAlias = ji->subAlias;

	return getTupleKey_(jobInfo, eid, "", expAlias, "", "", cr);
}


void addAggregateColumn(AggregateColumn* agc, int idx, RetColsVector& vec, JobInfo& jobInfo)
{
	uint32_t eid = agc->expressionId();
	setExpTupleInfo(agc->resultType(), eid, agc->alias(), jobInfo);

	vector<pair<int, int> >::iterator i;
	for (i = jobInfo.aggEidIndexList.begin(); i != jobInfo.aggEidIndexList.end(); ++i)
	{
		if (i->first == (int) eid)
			break;
	}

	if (idx < 0 && i != jobInfo.aggEidIndexList.end())
	{
		agc->inputIndex(i->second);
		jobInfo.cloneAggregateColMap.insert(make_pair(vec[i->second].get(), agc));
	}
	else
	{
		SRCP srcp;
		if (idx < 0)
		{
			srcp.reset(agc->clone());
			idx = vec.size();
			vec.push_back(srcp);
		}
		else
		{
			srcp = vec[idx];
		}

		jobInfo.aggEidIndexList.push_back(make_pair(eid, idx));
		agc->inputIndex(idx);
		jobInfo.cloneAggregateColMap.insert(make_pair(srcp.get(), agc));
	}
}


bool operator < (const struct UniqId& x, const struct UniqId& y)
{
	return (
		(x.fId < y.fId) ||
		(x.fId == y.fId && x.fTable < y.fTable) ||
		(x.fId == y.fId && x.fTable == y.fTable && x.fSchema < y.fSchema) ||
		(x.fId == y.fId && x.fTable == y.fTable && x.fSchema == y.fSchema && x.fView < y.fView) ||
		(x.fId == y.fId && x.fTable == y.fTable && x.fSchema == y.fSchema && x.fView == y.fView &&
		 x.fPseudo < y.fPseudo) ||
		(x.fId == y.fId && x.fTable == y.fTable && x.fSchema == y.fSchema && x.fView == y.fView &&
		 x.fPseudo == y.fPseudo && x.fSubId < y.fSubId));
}


bool operator == (const struct UniqId& x, const struct UniqId& y)
{
	return (
		x.fId     == y.fId &&
		x.fTable  == y.fTable &&
		x.fSchema == y.fSchema &&
		x.fView   == y.fView &&
		x.fPseudo == y.fPseudo &&
		x.fSubId  == y.fSubId);
}


void updateDerivedColumn(JobInfo& jobInfo, SimpleColumn* sc, CalpontSystemCatalog::ColType& ct)
{
	sc->oid(tableOid(sc, jobInfo.csc) + 1 + sc->colPosition());

	map<UniqId, execplan::CalpontSystemCatalog::ColType>::iterator i =
		jobInfo.vtableColTypes.find(UniqId(sc));
	if (i != jobInfo.vtableColTypes.end())
		ct = i->second;
}


bool filterWithDictionary(execplan::CalpontSystemCatalog::OID dictOid, uint64_t n)
{
	// if n == 0, no dictionary scan, alway filter with dictionary.
	if (n == 0)
		return true;

	// if n == ulong_max, always use dictionary scan
	if (n == ULONG_MAX)
		return false;

	vector<struct EMEntry> entries;
	DBRM dbrm;
	if (dbrm.getExtents(dictOid, entries) != 0)
		return false;  // Just do pdictionaryscan and let job step handle this.

	vector<struct EMEntry>::iterator it = entries.begin();
	bool ret = false;
	n--;  // HWM starts at 0
	while (it != entries.end())
	{
		if (it->HWM > n)
		{
			ret = true;
			break;
		}

		it++;
	}

	return ret;
}


// @Bug 1230 & 1955
// Don't allow join/compare on "incompatible" cols
// Compatible columns:
// any 1,2,4,8-byte int to any 1,2,4,8-byte int
// decimal w/scale x to decimal w/scale x
// date to date
// datetime to datetime
// string to string
bool compatibleColumnTypes(const CalpontSystemCatalog::ColType& ct1,
                           const CalpontSystemCatalog::ColType& ct2,
                           bool  forJoin)
{
	return compatibleColumnTypes(ct1.colDataType, ct1.scale, ct2.colDataType, ct2.scale, forJoin);
}


bool compatibleColumnTypes(const CalpontSystemCatalog::ColDataType& dt1, uint32_t scale1,
                           const CalpontSystemCatalog::ColDataType& dt2, uint32_t scale2,
                           bool  forJoin)
{
	// disable VARBINARY used in join
	if (dt1 == CalpontSystemCatalog::VARBINARY ||
		dt2 == CalpontSystemCatalog::VARBINARY)
		throw runtime_error("Comparsion between VARBINARY columns is not supported.");

	switch (dt1)
	{
	case CalpontSystemCatalog::BIT:
		if (dt2 != CalpontSystemCatalog::BIT) return false;
		break;
	case CalpontSystemCatalog::TINYINT:
	case CalpontSystemCatalog::SMALLINT:
	case CalpontSystemCatalog::MEDINT:
	case CalpontSystemCatalog::INT:
	case CalpontSystemCatalog::BIGINT:
	case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UDECIMAL:
		if (dt2 != CalpontSystemCatalog::TINYINT &&
			dt2 != CalpontSystemCatalog::SMALLINT &&
			dt2 != CalpontSystemCatalog::MEDINT &&
			dt2 != CalpontSystemCatalog::INT &&
			dt2 != CalpontSystemCatalog::BIGINT &&
			dt2 != CalpontSystemCatalog::DECIMAL &&
            dt2 != CalpontSystemCatalog::UTINYINT &&
            dt2 != CalpontSystemCatalog::USMALLINT &&
            dt2 != CalpontSystemCatalog::UMEDINT &&
            dt2 != CalpontSystemCatalog::UINT &&
            dt2 != CalpontSystemCatalog::UBIGINT &&
            dt2 != CalpontSystemCatalog::UDECIMAL) return false;
			if (scale2 != scale1) return false;
		break;
	case CalpontSystemCatalog::DATE:
		if (dt2 != CalpontSystemCatalog::DATE) return false;
		break;
	case CalpontSystemCatalog::DATETIME:
		if (dt2 != CalpontSystemCatalog::DATETIME) return false;
		break;
	case CalpontSystemCatalog::CHAR:
	case CalpontSystemCatalog::VARCHAR:
		// @bug 1495 compound/string join
		if (dt2 != CalpontSystemCatalog::VARCHAR &&
			dt2 != CalpontSystemCatalog::CHAR)
			return false;
		break;
	case CalpontSystemCatalog::VARBINARY:
		if (dt2 != CalpontSystemCatalog::VARBINARY) return false;
		break;
	case CalpontSystemCatalog::FLOAT:
	case CalpontSystemCatalog::UFLOAT:
		if (forJoin && (dt2 != CalpontSystemCatalog::FLOAT &&
		                dt2 != CalpontSystemCatalog::FLOAT)) return false;
		else if (dt2 != CalpontSystemCatalog::FLOAT &&
				 dt2 != CalpontSystemCatalog::DOUBLE &&
				 dt2 != CalpontSystemCatalog::UFLOAT &&
		         dt2 != CalpontSystemCatalog::UDOUBLE) return false;
		break;
	case CalpontSystemCatalog::DOUBLE:
	case CalpontSystemCatalog::UDOUBLE:
		if (forJoin && (dt2 != CalpontSystemCatalog::DOUBLE &&
		                dt2 != CalpontSystemCatalog::UDOUBLE)) return false;
		else if (dt2 != CalpontSystemCatalog::FLOAT &&
				 dt2 != CalpontSystemCatalog::DOUBLE &&
				 dt2 != CalpontSystemCatalog::UFLOAT &&
		         dt2 != CalpontSystemCatalog::UDOUBLE) return false;
		break;
	default:
		return false;
		break;
	}

	return true;
}


}
// vim:ts=4 sw=4:

