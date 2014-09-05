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
*   $Id: columndef.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include <iostream>
#include <iomanip>

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

namespace ddlpackage {

	using namespace std;

	ColumnDef::~ColumnDef()
	{
		delete fType;
		delete fDefaultValue;
		ColumnConstraintList::iterator itr;
		for(itr=fConstraints.begin(); itr != fConstraints.end(); ++itr) {
			delete *itr;
		}
	}
	
	ColumnDef::ColumnDef(const char *name, ColumnType* columnType, ColumnConstraintList *constraints,
						 ColumnDefaultValue *defaultValue, const char * comment ) :
		SchemaObject(name),
		fType(columnType),
		fDefaultValue(defaultValue)
	{
		if(constraints) {
			fConstraints = *constraints;
			delete constraints;
		}
		if ( comment )
			fComment = comment;
	}


	ostream &operator<<(ostream& os, const ColumnType& columnType)
	{
		os   << setw(12) << left << DDLDatatypeString[columnType.fType]
			 << "["
			 << "L=" << setw(2) << columnType.fLength << ","
			 << "P=" << setw(2) << columnType.fPrecision << ","
			 << "S=" << setw(2) << columnType.fScale << ","
			 << "T=" << setw(2) << columnType.fWithTimezone
			 << "]";
		return os;
	}
	
	
	ostream &operator<<(ostream& os, const ColumnDef &column)
	{
		os << "Column: " << column.fName << " " << *column.fType;
		
		if(column.fDefaultValue) {
			os << " def=";
			
			if (column.fDefaultValue->fNull)
				os << "NULL";
			else
				os << column.fDefaultValue->fValue;
		}

		os   << endl << " " << column.fConstraints.size()
			 << " constraints ";

		ColumnConstraintList::const_iterator itr;
		for(itr = column.fConstraints.begin();
			itr != column.fConstraints.end();
			++itr) {
			ColumnConstraintDef *con = *itr;
			os << *con;
		}

		

		return os;
	}


	ostream &operator<<(ostream& os, const ColumnConstraintDef &con)
	{
		os << "   Constraint: "
		   << con.fName << " "
		   << ConstraintString[con.fConstraintType] << " "
		   << "defer=" << con.fDeferrable << " "
		   << ConstraintAttrStrings[con.fCheckTime] << " ";
		if(!con.fCheck.empty())
			os << "check=" << "\"" << con.fCheck << "\"";
		
		return os;
	}
	
	std::ostream &operator<<(std::ostream& os, const ColumnDefList &clist)
	{
		ColumnDefList::const_iterator itr;
		for(itr = clist.begin(); itr != clist.end(); ++itr){
			os << **itr;
		}
		return os;
	}


	ColumnDefaultValue::ColumnDefaultValue(const char *value) :
		fNull(false)
	{
		if(0 == value)
			fNull = true;
		else
			fValue = value;
	}

	
	std::ostream &operator<<(std::ostream& os, const ColumnDefaultValue &defaultValue)
	{
		os << " def=";
			
		if (defaultValue.fNull)
			os << "NULL";
		else
			os << defaultValue.fValue;
		return os;
	}
	
}
