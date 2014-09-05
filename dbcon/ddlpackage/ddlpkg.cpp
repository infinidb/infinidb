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
*   $Id: ddlpkg.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include <iostream>

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

namespace ddlpackage 
{
	using namespace std;

	QualifiedName::QualifiedName(const char *name):
		fName(name)
	{
	}

	QualifiedName::QualifiedName(const char *schema, const char *name):
		fName(name),
		fSchema(schema)
	{
	}

	QualifiedName::QualifiedName(const char* catalog, const char *schema, const char *name):
		fCatalog(catalog),
		fName(name),
		fSchema(schema)
	{
	}

	ostream& operator<<(ostream &os, const QualifiedName& qname)
	{
		if(!qname.fCatalog.empty())
			os << qname.fCatalog << ".";
		if(!qname.fSchema.empty())
			os << qname.fSchema << ".";
		os << qname.fName;
		return os;
	}


	/** @brief Map a DECIMAL precision to data width in bytes */
	unsigned int precision_width(unsigned p)
	{
		switch(p)
		{
		case 1:
		case 2:
			return 1;
		case 3:
		case 4:
			return 2;
		case 5:
		case 6:
		case 7:
		case 8:
		case 9:
			return 4;
		default:
			return 8;
		}
	}

	ColumnType::ColumnType(int prec, int scale) :
		fType(DDL_INVALID_DATATYPE),
		fLength(0),
		fPrecision(prec),
		fScale(scale),
		fWithTimezone(false)
	{
		fLength = precision_width(fPrecision);
	}

    ColumnType::ColumnType(int type) :
		fType(type),
		fLength(0),
		fScale(0),
		fWithTimezone(false)
	{
		switch ( type )
		{
			case DDL_TINYINT:
            case DDL_UNSIGNED_TINYINT:
				fPrecision = 3;
				break;
			case DDL_SMALLINT:
            case DDL_UNSIGNED_SMALLINT:
				fPrecision = 5;
				break;
			case DDL_INT:
			case DDL_UNSIGNED_INT:
            case DDL_MEDINT:
				fPrecision = 10;
				break;
			case DDL_BIGINT:
                fPrecision = 19;
            case DDL_UNSIGNED_BIGINT:
                fPrecision = 20;
				break;
			default:
				fPrecision = 10;
				break;				
		}		
	}
#if 0
    ColumnType::ColumnType(int type, int length, int precision, int scale, int compressiontype, const char* autoIncrement, int64_t nextValue, bool withTimezone) :
        fType(type)                            ,
        fLength(length),
        fPrecision(precision),
        fScale(scale),
        fWithTimezone(withTimezone),
		fCompressiontype(compressiontype),
		fAutoincrement(autoIncrement),
		fNextvalue(nextValue)
    {
    }        
#endif
	ColumnConstraintDef::ColumnConstraintDef(DDL_CONSTRAINTS type) :
		SchemaObject(),
		fDeferrable(false),
		fCheckTime(DDL_INITIALLY_IMMEDIATE),
		fConstraintType(type),
		fCheck("")
	{
	}
	
	ColumnConstraintDef::ColumnConstraintDef(const char *check) :
		SchemaObject(),
		fDeferrable(false),
		fCheckTime(DDL_INITIALLY_IMMEDIATE),
		fConstraintType(DDL_CHECK),
		fCheck(check)
	{
	}
	

	AtaAddColumn::AtaAddColumn(ColumnDef *columnDef) :
		fColumnDef(columnDef)
	{
	}

	ostream &operator<<(ostream& os, const ReferentialAction& ref)
	{
		os << "ref action: u=" << ReferentialActionStrings[ref.fOnUpdate] << " "
		   << "d=" << ReferentialActionStrings[ref.fOnDelete];
		return os;
	}

    void ColumnDef::convertDecimal()
    {		
    	//@Bug 2089 decimal precision default to 10 if 0 is used.
    	if (fType->fPrecision <= 0)
    		fType->fPrecision = 10;

    	if (fType->fPrecision == -1 || fType->fPrecision == 0)
    	{
    		fType->fType = DDL_BIGINT;
    		fType->fLength = 8;
    		fType->fScale = 0;
    	}
    	else if ((fType->fPrecision > 0) && (fType->fPrecision < 3))
    	{
    					//dataType = CalpontSystemCatalog::TINYINT;
    		fType->fType = DDL_TINYINT;
    		fType->fLength = 1;
    	}

    	else if (fType->fPrecision < 5 && (fType->fPrecision > 2))
    	{
    					//dataType = CalpontSystemCatalog::SMALLINT;
    		fType->fType = DDL_SMALLINT;
    		fType->fLength = 2;
    	}
    	else if (fType->fPrecision > 4 && fType->fPrecision < 10)
    	{
    					//dataType = CalpontSystemCatalog::INT;
    		fType->fType = DDL_INT;
    		fType->fLength = 4;
    	}
    	else if (fType->fPrecision > 9 && fType->fPrecision < 19)
    	{
    					//dataType = CalpontSystemCatalog::BIGINT;
    		fType->fType = DDL_BIGINT;
    		fType->fLength = 8;
    	}
    }
}
