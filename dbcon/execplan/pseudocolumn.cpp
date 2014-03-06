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
*   $Id: pseudocolumn.cpp 9576 2013-05-29 21:02:11Z zzhu $
*
*
***********************************************************************/

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sstream>
using namespace std;

#include <boost/algorithm/string.hpp>

#include "bytestream.h"
using namespace messageqcpp;

#include "objectreader.h"
#include "calpontselectexecutionplan.h"

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#include "dataconvert.h"

#include "pseudocolumn.h"

namespace execplan
{

/**
 * Constructors/Destructors
 */
PseudoColumn::PseudoColumn():
    SimpleColumn(),
    fPseudoType (PSEUDO_UNKNOWN)
{}

PseudoColumn::PseudoColumn(const uint32_t pseudoType):
    SimpleColumn(),
    fPseudoType(pseudoType)
{}

PseudoColumn::PseudoColumn(const string& token,
                           const uint32_t pseudoType,
                           const uint32_t sessionID):
    SimpleColumn(token, sessionID),
    fPseudoType(pseudoType)
{
	adjustResultType();
}

PseudoColumn::PseudoColumn(const string& schemaName,
                           const string& tableName,
                           const string& columnName,
                           const uint32_t pseudoType,
                           const uint32_t sessionID):
    SimpleColumn(schemaName, tableName, columnName, sessionID),
    fPseudoType(pseudoType)

{}

PseudoColumn::PseudoColumn(const string& schemaName,
                           const string& tableName,
                           const string& columnName,
                           const bool isInfiniDB,
                           const uint32_t pseudoType,
                           const uint32_t sessionID):
    SimpleColumn(schemaName, tableName, columnName, isInfiniDB, sessionID),
    fPseudoType(pseudoType)
{
	adjustResultType();
}

PseudoColumn::PseudoColumn (const SimpleColumn& rhs,
                            const uint32_t pseudoType,
                            const uint32_t sessionID):
    SimpleColumn(rhs, sessionID),
    fPseudoType(pseudoType)
{
	adjustResultType();
}

PseudoColumn::PseudoColumn(const PseudoColumn& rhs, const uint32_t sessionID):
    SimpleColumn(rhs, sessionID),
    fPseudoType (rhs.pseudoType())
{
	adjustResultType();
}



PseudoColumn::~PseudoColumn()
{}

/**
 * Methods
 */

PseudoColumn& PseudoColumn::operator=(const PseudoColumn& rhs)
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
		fPseudoType = rhs.pseudoType();
	}

	return *this;
}

ostream& operator<<(ostream& output, const PseudoColumn& rhs)
{
	output << rhs.toString();

	return output;
}

const string PseudoColumn::toString() const
{
	ostringstream output;
	output << "PseudoColumn " << data() << endl;

	output << "SimpleColumn " << data() << endl;
	output << "  s/t/c/v/o/ct/TA/CA/RA/#/card/join/source/engine: " << schemaName() << '/'
							  << tableName() << '/'
							  << columnName() << '/'
							  << viewName() << '/'
							  << oid() << '/'
							  << colDataTypeToString(fResultType.colDataType) << '/'
							  << tableAlias() << '/'
							  << alias() << '/'
							  << returnAll() << '/'
							  << (int32_t)sequence() << '/'
							  << cardinality() << '/'
							  << joinInfo() << '/'
							  << colSource() << '/'
							  << (isInfiniDB()? "InfiniDB" : "ForeignEngine") << endl;

	output << "Pseudotype=" << fPseudoType << endl;
	return output.str();
}

void PseudoColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::PSEUDOCOLUMN;
	SimpleColumn::serialize(b);
	b << static_cast<ByteStream::quadbyte>(fPseudoType);
}

void PseudoColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::PSEUDOCOLUMN);
	SimpleColumn::unserialize(b);
	b >> reinterpret_cast< ByteStream::quadbyte&>(fPseudoType);
}

bool PseudoColumn::operator==(const PseudoColumn& t) const
{
	const SimpleColumn *rc1, *rc2;

	rc1 = static_cast<const SimpleColumn*>(this);
	rc2 = static_cast<const SimpleColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fPseudoType != t.fPseudoType)
		return false;
	return true;
}

bool PseudoColumn::operator==(const TreeNode* t) const
{
	const PseudoColumn *pc;

	pc = dynamic_cast<const PseudoColumn*>(t);
	if (pc == NULL)
		return false;
	return *this == *pc;
}

bool PseudoColumn::operator!=(const PseudoColumn& t) const
{
	return !(*this == t);
}

bool PseudoColumn::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

uint32_t PseudoColumn::pseudoNameToType (string& name)
{
	if (boost::iequals(name, "idbpm"))
		return PSEUDO_PM;
	if (boost::iequals(name, "idbdbroot"))
		return PSEUDO_DBROOT;
	if (boost::iequals(name, "idbextentrelativerid"))
		return PSEUDO_EXTENTRELATIVERID;
	if (boost::iequals(name, "idbsegment"))
		return PSEUDO_SEGMENT;
	if (boost::iequals(name, "idbsegmentdir"))
		return PSEUDO_SEGMENTDIR;
	if (boost::iequals(name, "idbextentmin"))
		return PSEUDO_EXTENTMIN;
	if (boost::iequals(name, "idbextentmax"))
		return PSEUDO_EXTENTMAX;
	if (boost::iequals(name, "idbblockid"))
		return PSEUDO_BLOCKID;
	if (boost::iequals(name, "idbextentid"))
		return PSEUDO_EXTENTID;
	if (boost::iequals(name, "idbpartition"))
		return PSEUDO_PARTITION;
	if (boost::iequals(name, "idblocalpm"))
		return PSEUDO_LOCALPM;
	return PSEUDO_UNKNOWN;
}

/*bool SimpleColumn::sameColumn(const ReturnedColumn* rc) const
{
    // NOTE: Operations can still be merged on different table alias
    const SimpleColumn *sc = dynamic_cast<const SimpleColumn*>(rc);
    if (!sc) return false;
    return (fSchemaName.compare(sc->schemaName()) == 0 &&
            fTableName.compare(sc->tableName()) == 0 &&
            fColumnName.compare(sc->columnName()) == 0 &&
            fTableAlias.compare(sc->tableAlias()) == 0 &&
            fViewName.compare(sc->viewName()) == 0 &&
            fIsInfiniDB == sc->isInfiniDB());
}*/

// @todo move to inline
/*
void SimpleColumn::evaluate(Row& row, bool& isNull)
{
	bool isNull2 = row.isNullValue(fInputIndex);
	if (isNull2) {
		isNull = true;
		return;
	}

	switch (fResultType.colDataType)
	{
		case CalpontSystemCatalog::DATE:
		{
			fResult.intVal = row.getUintField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DATETIME:
		{
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
				{
					fResult.origIntVal = row.getUintField<1>(fInputIndex);
					break;
				}
				case 2:
				{
					fResult.origIntVal = row.getUintField<2>(fInputIndex);
					break;
				}
				case 3:
				case 4:
				{
					fResult.origIntVal = row.getUintField<4>(fInputIndex);
					break;
				}
				case 5:
				case 6:
				case 7:
				case 8:
				{
					fResult.origIntVal = row.getUintField<8>(fInputIndex);
					break;
				}
				default:
				{
					fResult.strVal = row.getStringField(fInputIndex);
					break;
				}
			}
			if (fResultType.colDataType == CalpontSystemCatalog::STRINT)
				fResult.intVal = uint64ToStr(fResult.origIntVal);
			else
				fResult.intVal = atoll((char*)&fResult.origIntVal);
			break;
		}
		case CalpontSystemCatalog::BIGINT:
		{
			fResult.intVal = row.getIntField<8>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::INT:
		case CalpontSystemCatalog::MEDINT:
		{
			fResult.intVal = row.getIntField<4>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::SMALLINT:
		{
			fResult.intVal = row.getIntField<2>(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::TINYINT:
		{
			fResult.intVal = row.getIntField<1>(fInputIndex);
			break;
		}
		//In this case, we're trying to load a double output column with float data. This is the
		// case when you do sum(floatcol), e.g.
		case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
		{
			fResult.floatVal = row.getFloatField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
		{
			fResult.doubleVal = row.getDoubleField(fInputIndex);
			break;
		}
		case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
		{
            switch (fResultType.colWidth)
            {
                case 1:
                {
                    fResult.decimalVal.value = row.getIntField<1>(fInputIndex);
                    fResult.decimalVal.scale = (unsigned)fResultType.scale;
                    break;
                }
                case 2:
                {
                    fResult.decimalVal.value = row.getIntField<2>(fInputIndex);
                    fResult.decimalVal.scale = (unsigned)fResultType.scale;
                    break;
                }
                case 4:
                {
                    fResult.decimalVal.value = row.getIntField<4>(fInputIndex);
                    fResult.decimalVal.scale = (unsigned)fResultType.scale;
                    break;
                }
                default:
                {
                    fResult.decimalVal.value = (int64_t)row.getUintField<8>(fInputIndex);
                    fResult.decimalVal.scale = (unsigned)fResultType.scale;
                    break;
                }
            }
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
		{
			fResult.strVal = row.getVarBinaryStringField(fInputIndex);
			break;
		}
        case CalpontSystemCatalog::UBIGINT:
        {
            fResult.uintVal = row.getUintField<8>(fInputIndex);
            break;
        }
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
        {
            fResult.uintVal = row.getUintField<4>(fInputIndex);
            break;
        }
        case CalpontSystemCatalog::USMALLINT:
        {
            fResult.uintVal = row.getUintField<2>(fInputIndex);
            break;
        }
        case CalpontSystemCatalog::UTINYINT:
        {
            fResult.uintVal = row.getUintField<1>(fInputIndex);
            break;
        }
		default:	// treat as int64
		{
			fResult.intVal = row.getUintField<8>(fInputIndex);
			break;
		}
	}
}
*/

void PseudoColumn::adjustResultType()
{
	switch (fPseudoType)
	{
		case PSEUDO_EXTENTRELATIVERID:
		case PSEUDO_DBROOT:
		case PSEUDO_PM:
		case PSEUDO_SEGMENT:
		case PSEUDO_SEGMENTDIR:
		case PSEUDO_BLOCKID:
		case PSEUDO_EXTENTID:
		{
			fResultType.colDataType = CalpontSystemCatalog::BIGINT;
			fResultType.colWidth = 8;
			break;
		}
		case PSEUDO_PARTITION:
		{
			fResultType.colDataType = CalpontSystemCatalog::VARCHAR;
			fResultType.colWidth = 256;
			break;
		}
		default:
		{
			// same type of pseudocolumn arg
		}
	}

}

} // namespace execplan
