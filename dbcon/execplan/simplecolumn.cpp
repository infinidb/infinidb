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

/***********************************************************************
*   $Id: simplecolumn.cpp 8565 2012-05-28 20:48:07Z xlou $
*
*
***********************************************************************/

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sstream>

using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "objectreader.h"
#include "calpontselectexecutionplan.h"

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "dataconvert.h"

#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "constantfilter.h"

namespace execplan
{

void getSimpleCols(execplan::ParseTree* n, void* obj)
{
	vector<SimpleColumn*>* list = reinterpret_cast< vector<SimpleColumn*>*>(obj);
	TreeNode* tn = n->data();
	SimpleColumn *sc = dynamic_cast<SimpleColumn*>(tn);
	FunctionColumn *fc = dynamic_cast<FunctionColumn*>(tn);
	ArithmeticColumn *ac = dynamic_cast<ArithmeticColumn*>(tn);
	SimpleFilter *sf = dynamic_cast<SimpleFilter*>(tn);
	ConstantFilter *cf = dynamic_cast<ConstantFilter*>(tn);
	if (sc)
		list->push_back(sc);
	else if (fc)
		list->insert(list->end(), fc->simpleColumnList().begin(), fc->simpleColumnList().end());
	else if (ac)
		list->insert(list->end(), ac->simpleColumnList().begin(), ac->simpleColumnList().end());
	else if (sf)
		list->insert(list->end(), sf->simpleColumnList().begin(), sf->simpleColumnList().end());
	else if (cf)
		list->insert(list->end(), cf->simpleColumnList().begin(), cf->simpleColumnList().end());
}

/**
 * Constructors/Destructors
 */
SimpleColumn::SimpleColumn():
    ReturnedColumn(),
    fOid (0),
    fIsInfiniDB (true)
{
	fDistinct=false;
}

SimpleColumn::SimpleColumn(const string& token, const u_int32_t sessionID):
    ReturnedColumn(sessionID),
    fOid (0),
    fData(token),
    fIsInfiniDB (true)
{
	parse (token);
	setOID();
	fDistinct=false;
}

SimpleColumn::SimpleColumn(const string& schemaName,
                           const string& tableName,
                           const string& columnName,
                           const u_int32_t sessionID):
    ReturnedColumn(sessionID),
    fSchemaName (schemaName),
    fTableName (tableName),
    fColumnName (columnName),
    fIsInfiniDB (true)
{
	setOID();
	fDistinct=false;
	fAsc=false;
}

SimpleColumn::SimpleColumn(const string& schemaName,
                           const string& tableName,
                           const string& columnName,
                           const bool isInfiniDB,
                           const u_int32_t sessionID):
    ReturnedColumn(sessionID),
    fSchemaName (schemaName),
    fTableName (tableName),
    fColumnName (columnName),
    fIsInfiniDB (isInfiniDB)
{
	if (isInfiniDB)
		setOID();
	fDistinct=false;
	fAsc=false;
}

SimpleColumn::SimpleColumn (const SimpleColumn& rhs,const u_int32_t sessionID):
                ReturnedColumn(rhs, sessionID),
                fSchemaName (rhs.schemaName()),
				fTableName (rhs.tableName()),
				fColumnName (rhs.columnName()),
				fOid (rhs.oid()),
				fTableAlias (rhs.tableAlias()),
				fData (rhs.data()),
				fIndexName (rhs.indexName()),
				fViewName (rhs.viewName()),
				fIsInfiniDB (rhs.isInfiniDB())
{
}

SimpleColumn::~SimpleColumn()
{}

/**
 * Methods
 */

const string SimpleColumn::data() const
{
	if (!fData.empty())
		return fData;
	else if (!fTableAlias.empty())
		return string(fSchemaName + '.' + fTableAlias + '.' + fColumnName);
	return string(fSchemaName + '.' + fTableName + '.' + fColumnName);
}

SimpleColumn& SimpleColumn::operator=(const SimpleColumn& rhs)
{
	if (this != &rhs)
	{
		fTableName = rhs.tableName();
		fColumnName = rhs.columnName();
		fOid = rhs.oid();
		fSchemaName = rhs.schemaName();
		fAlias = rhs.alias();
		fTableAlias = rhs.tableAlias();
		fAsc = rhs.asc();
		fIndexName = rhs.indexName();
		fViewName = rhs.viewName();
		fData = rhs.data();
		fSequence = rhs.sequence();
		fDistinct = rhs.distinct();
		fIsInfiniDB = rhs.isInfiniDB();
	}

	return *this;
}

ostream& operator<<(ostream& output, const SimpleColumn& rhs)
{
	output << rhs.toString();

	return output;
}

const string SimpleColumn::toString() const
{
	ostringstream output;
	output << "SimpleColumn " << data() << endl;
	output << "  s/t/c/v/o/TA/CA/RA/#/card/join/source/engine: " << schemaName() << '/'
	                          << tableName() << '/'
	                          << columnName() << '/'
	                          << viewName() << '/'
	                          << oid() << '/'
	                          << tableAlias() << '/'
	                          << alias() << '/'
	                          << returnAll() << '/'
	                          << (int32_t)sequence() << '/'
	                          << cardinality() << '/'
	                          << joinInfo() << '/'
	                          << colSource() << '/'
	                          << (isInfiniDB()? "InfiniDB" : "ForeignEngine") << endl;

	return output.str();
}

void SimpleColumn::parse(const string& token)
{
    // get schema name, table name and column name for token.
    string::size_type pos = token.find_first_of(".", 0);

    // if no '.' in column name, consider it a function name;
    if (pos == string::npos)
    {
        fData = token;
        fColumnName = token;
        return;
    }
    fSchemaName = token.substr(0, pos);

    string::size_type newPos = token.find_first_of(".", pos+1);
    if (newPos == string::npos)
    {
        // only one '.' in column name, consider table.col pattern
        fTableName = fSchemaName;
        fColumnName = token.substr(pos+1, token.length());
        return;
    }

    fTableName = token.substr(pos+1, newPos-pos-1);
    fColumnName = token.substr(newPos+1, token.length());
}

void SimpleColumn::setOID()
{
    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
    CalpontSystemCatalog::TableColName tcn;
    // @bug #393
    if (fSchemaName.length() == 0 || fTableName.length() == 0)
        tcn = make_tcn(CalpontSelectExecutionPlan::schemaName(), CalpontSelectExecutionPlan::tableName(), fColumnName);
    else
        tcn = make_tcn(fSchemaName, fTableName, fColumnName);

    fOid = csc->lookupOID (tcn);
    if (fOid != -1)
    {
        if (fSchemaName.length() == 0 || fTableName.length() == 0)
        {
            fSchemaName = CalpontSelectExecutionPlan::schemaName();
            fTableName = CalpontSelectExecutionPlan::tableName();
        }
    }
    else
    {
        // get colMap from CalpontSelectExecutionPlan
        // and try to map the schema and table name
        CalpontSelectExecutionPlan::ColumnMap::iterator iter;
        CalpontSelectExecutionPlan::ColumnMap colMap = CalpontSelectExecutionPlan::colMap();

        // if this is the only column name exist in the map, return it ??
        for (iter = colMap.find(fColumnName); iter != colMap.end(); ++iter)
        {
            SRCP srcp = iter->second;
            SimpleColumn* scp = dynamic_cast<SimpleColumn*>(srcp.get());
            if (colMap.count(fColumnName) == 1 ||
                scp->tableName().compare(fTableName) == 0)
            {
                fOid = scp->oid();
                //@bug 221 fix
                fTableName = scp->tableName();
                fSchemaName = scp->schemaName();
                //@info assign tableAlias also. watch for other conflict
                fTableAlias = scp->tableAlias();
                return;
            }
        }
    }
}

void SimpleColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::SIMPLECOLUMN;
	ReturnedColumn::serialize(b);
	b << fSchemaName;
	b << fTableName;
	b << fColumnName;
	b << fIndexName;
	b << fViewName;
	b << (uint32_t) fOid;
	b << fData;
	//b << fAlias;
	b << fTableAlias;
	b << static_cast<const ByteStream::doublebyte>(fReturnAll);
	b << (uint32_t) fSequence;
	b << static_cast<const ByteStream::doublebyte>(fIsInfiniDB);
}

void SimpleColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN);
	ReturnedColumn::unserialize(b);
	b >> fSchemaName;
	b >> fTableName;
	b >> fColumnName;
	b >> fIndexName;
	b >> fViewName;
	b >> (uint32_t&) fOid;
	b >> fData;
	//b >> fAlias;
	b >> fTableAlias;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fReturnAll);
	b >> (uint32_t&) fSequence;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fIsInfiniDB);
}

bool SimpleColumn::operator==(const SimpleColumn& t) const
{
	const ReturnedColumn *rc1, *rc2;

	rc1 = static_cast<const ReturnedColumn*>(this);
	rc2 = static_cast<const ReturnedColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fSchemaName != t.fSchemaName)
		return false;
	if (fTableName != t.fTableName)
		return false;
	if (fColumnName != t.fColumnName)
		return false;
	if (fIndexName != t.fIndexName)
	    return false;
	if (fViewName != t.fViewName)
	    return false;
	if (fOid != t.fOid)
		return false;
	if (fData != t.fData)
		return false;
	if (fAlias != t.fAlias)
		return false;
	if (fTableAlias != t.fTableAlias)
	    return false;
	if (fAsc != t.fAsc)
		return false;
	if (fReturnAll != t.fReturnAll)
	    return false;
	if (fIsInfiniDB != t.fIsInfiniDB)
		return false;
	return true;
}

bool SimpleColumn::operator==(const TreeNode* t) const
{
	const SimpleColumn *sc;

	sc = dynamic_cast<const SimpleColumn*>(t);
	if (sc == NULL)
		return false;
	return *this == *sc;
}

bool SimpleColumn::operator!=(const SimpleColumn& t) const
{
	return !(*this == t);
}

bool SimpleColumn::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

bool SimpleColumn::sameColumn(const ReturnedColumn* rc) const
{
    /** NOTE: Operations can still be merged on different table alias */
    const SimpleColumn *sc = dynamic_cast<const SimpleColumn*>(rc);
    if (!sc) return false;
    return (fSchemaName.compare(sc->schemaName()) == 0 &&
            fTableName.compare(sc->tableName()) == 0 &&
            fColumnName.compare(sc->columnName()) == 0 &&
            fTableAlias.compare(sc->tableAlias()) == 0 &&
            fViewName.compare(sc->viewName()) == 0 &&
            fIsInfiniDB == sc->isInfiniDB());
}

// @todo move to inline
void SimpleColumn::evaluate(Row& row, bool& isNull)
{
	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::DATE:
		{
			if (row.equals<4>(DATENULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DATETIME:
		{
			if (row.equals<8>(DATETIMENULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
		case CalpontSystemCatalog::STRINT:
		{
			switch (row.getColumnWidth(fInputIndex))
			{
				case 1:
					if (row.equals<1>(CHAR1NULL, fInputIndex))
						isNull = true;
					else
						fResult.origIntVal = row.getUintField<1>(fInputIndex);
					break;
				case 2:
					if (row.equals<2>(CHAR2NULL, fInputIndex))
						isNull = true;
					else
						fResult.origIntVal = row.getUintField<2>(fInputIndex);
					break;
				case 3:
				case 4:
					if (row.equals<4>(CHAR4NULL, fInputIndex))
						isNull = true;
					else
						fResult.origIntVal = row.getUintField<4>(fInputIndex);
					break;
				case 5:
				case 6:
				case 7:
				case 8:
					if (row.equals<8>(CHAR8NULL, fInputIndex))
						isNull = true;
					else
						fResult.origIntVal = row.getUintField<8>(fInputIndex);
					break;
				default:
					if (row.equals(CPNULLSTRMARK, fInputIndex))
						isNull = true;
					else
						fResult.strVal = row.getStringField(fInputIndex);
					// stringColVal is padded with '\0' to colWidth so can't use str.length()
					if (strlen(fResult.strVal.c_str()) == 0)
						isNull = true;
					break;
			}
			if (fResultType.colDataType == CalpontSystemCatalog::STRINT)
				fResult.intVal = uint64ToStr(fResult.origIntVal);
			else
				fResult.intVal = atoll((char*)&fResult.origIntVal);
			break;
		}
		case CalpontSystemCatalog::BIGINT:
		{
			if (row.equals<8>(BIGINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::MEDINT:
		{
			if (row.equals<4>(INTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::SMALLINT:
		{
			if (row.equals<2>(SMALLINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<2>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::TINYINT:
		{
			if (row.equals<1>(TINYINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getIntField<1>(fInputIndex);
			break;
		}
		//In this case, we're trying to load a double output column with float data. This is the
		// case when you do sum(floatcol), e.g.
		case CalpontSystemCatalog::FLOAT:
		{
			if (row.equals<4>(FLOATNULL, fInputIndex))
				isNull = true;
			else
				fResult.floatVal = row.getFloatField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
		{
			if (row.equals<8>(DOUBLENULL, fInputIndex))
				isNull = true;
			else
				fResult.doubleVal = row.getDoubleField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
		{
				switch (fResultType.colWidth)
				{
					case 1:
					{
						if (row.equals<1>(TINYINTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<1>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					case 2:
					{
						if (row.equals<2>(SMALLINTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<2>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					case 4:
					{
						if (row.equals<4>(INTNULL, fInputIndex))
							isNull = true;
						else
						{
							fResult.decimalVal.value = row.getIntField<4>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
					default:
					{
						if (row.equals<8>(BIGINTNULL, fInputIndex))
						isNull = true;
						else
						{
							fResult.decimalVal.value = (int64_t)row.getUintField<8>(fInputIndex);
							fResult.decimalVal.scale = (unsigned)fResultType.scale;
						}
						break;
					}
				}
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
			if (row.isNullValue(fInputIndex))
				isNull = true;
			else
				fResult.strVal = row.getVarBinaryStringField(fInputIndex);
			break;
		default:	// treat as int64
		{
			if (row.equals<8>(BIGINTNULL, fInputIndex))
				isNull = true;
			else
				fResult.intVal = row.getUintField<8>(fInputIndex);
			break;
		}
	}
}

} // namespace execplan
