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
*   $Id: tabledef.cpp 9210 2013-01-21 14:10:42Z rdempsey $
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

		
	TableDef::~TableDef() 
	{
		{
			ColumnDefList::iterator itr;
			for(itr=fColumns.begin(); itr != fColumns.end(); itr++) {
				delete *itr;
			}
		}
		{
			TableConstraintDefList::iterator itr;
			for(itr=fConstraints.begin(); itr != fConstraints.end(); itr++) {
				delete *itr;
			}
		}
		
		delete fQualifiedName;
	}

	
	TableDef::TableDef(QualifiedName* name, TableElementList* elements, TableOptionMap* options) :
		fQualifiedName(name)
	{
		if(options) {
			fOptions = *options;
			delete options;
		}
		
		ColumnDef *column;
		TableConstraintDef *constraint;
		
		/* When parsing, it is necessary to collect ColumnDefs and
		   TableConstraintDefs as TableElements.  Here we separate
		   them out into separately typed lists.
		 */
		TableElementList::iterator itr;
		for(itr = elements->begin(); itr != elements->end(); ++itr) {
			column = dynamic_cast<ColumnDef*>(*itr);
			if(column) {
				fColumns.push_back(column);
			}
			else {
				constraint = dynamic_cast<TableConstraintDef *>(*itr);
				if(constraint) {
					fConstraints.push_back(constraint);
				}
			}
		}

		delete elements;
	}


	/** \brief Put to ostream. */
	ostream& operator<<(ostream& os, const TableDef& tableDef)
	{
		os << "CreateTable ";
		if(tableDef.fQualifiedName->fSchema != "")
			//cout << tableDef.fQualifiedName->fSchema << ".";
		os << tableDef.fQualifiedName->fName
		   << " " << tableDef.fConstraints.size()
		   << " table constraints"
		   << endl;

		{
			ColumnDefList::const_iterator itr;
			for(itr = tableDef.fColumns.begin();
				itr != tableDef.fColumns.end(); ++itr)
			{
				ColumnDef* col = *itr;
				os << *col << endl;
			}
		}
		
		
		{
			TableConstraintDefList::const_iterator itr;
			for(itr = tableDef.fConstraints.begin();
				itr != tableDef.fConstraints.end();
				++itr)
			{
				os << (**itr);
			}
		}

		pair<string, string> oval;
		TableOptionMap::const_iterator oitr;
		os << "Table Options" << endl;
		if(!tableDef.fOptions.empty()) {
			TableOptionMap::const_iterator oitr;
			for(oitr = tableDef.fOptions.begin();
				oitr != tableDef.fOptions.end(); ++oitr) {
				oval = *oitr;
				os << "   " << oval.first << "=" << oval.second << endl;
			}
		}
		
		return os;
	}




	ostream& operator<<(ostream &os, const TableConstraintDef& constraint)
	{
		return constraint.put(os);
	}

	std::ostream& TableConstraintDef::put(std::ostream& os) const
	{
		os << "No!!!" << endl;
		 
		return os;
	}

	
	TableConstraintDef::TableConstraintDef(DDL_CONSTRAINTS cType) :
		fConstraintType(cType)
	{
	}

	TableConstraintDef::TableConstraintDef() :
		fConstraintType(DDL_INVALID_CONSTRAINT)
	{
	}

	TableCheckConstraintDef::TableCheckConstraintDef(const char *check) :
		TableConstraintDef(DDL_CHECK),
		fCheck(check)
	{
	}
	std::ostream& TableCheckConstraintDef::put(std::ostream& os) const
	{
		os << "Constraint: "
		   << ConstraintString[fConstraintType] << " ";
		os << "\"" << fCheck << "\"";
		os << endl;
		
		return os;
	}


	TableUniqueConstraintDef::TableUniqueConstraintDef(ColumnNameList *columns) :
		TableConstraintDef(DDL_UNIQUE),
		fColumnNameList(*columns)
	{
		delete columns;
	}
	std::ostream& TableUniqueConstraintDef::put(std::ostream& os) const
	{
		os << "Constraint: "
		   << fName << " "
		   << ConstraintString[fConstraintType] << " ";

		ColumnNameList::const_iterator itr;
		os << "(";
		for(itr = fColumnNameList.begin();
			itr != fColumnNameList.end();
			++itr)
		{
			os << *itr << " ";
		}
		os << ")";
		return os;
	}

	TablePrimaryKeyConstraintDef::TablePrimaryKeyConstraintDef(ColumnNameList *columns) :
		TableConstraintDef(DDL_PRIMARY_KEY),
		fColumnNameList(*columns)
	{
		delete columns;
	}
	std::ostream& TablePrimaryKeyConstraintDef::put(std::ostream& os) const
	{
		os << "Constraint: "
		   << fName << " "
		   << ConstraintString[fConstraintType] << " ";

		ColumnNameList::const_iterator itr;
		os << "(";
		for(itr = fColumnNameList.begin();
			itr != fColumnNameList.end();
			++itr)
		{
			os << *itr << " ";
		}
		os << ")";
		
		return os;
	}
	
	TableReferencesConstraintDef::TableReferencesConstraintDef
	(ColumnNameList *columns,
	 QualifiedName *tableName,
	 ColumnNameList *foreignColumns,
	 DDL_MATCH_TYPE matchType,
	 ReferentialAction *refAction) :
		TableConstraintDef(DDL_REFERENCES),
		fColumns(*columns),
		fTableName(tableName),
		fForeignColumns(*foreignColumns),
		fMatchType(matchType),
		fRefAction(refAction)
	{
		delete columns;
		delete foreignColumns;
	}
	std::ostream& TableReferencesConstraintDef::put(std::ostream& os) const
	{
		os << "Constraint: "
		   << fName << " "
		   << ConstraintString[fConstraintType] << " ";

		ColumnNameList::const_iterator itr;
		os << "lcols (";
		for(itr = fColumns.begin();
			itr != fColumns.end();
			++itr)
		{
			os << *itr << " ";
		}
		os << ")";

		os << " ftable=" << *fTableName;

		os << " ";

		os << "fcols (";
		for(itr = fForeignColumns.begin();
			itr != fForeignColumns.end();
			++itr)
		{
			os << *itr << " ";
		}
		os << ")";

		return os;
	}
	std::ostream &operator<<(std::ostream& os, const ColumnNameList &columnNames)
	{
		ColumnNameList::const_iterator itr;
		os << '(';
		for(itr = columnNames.begin();
			itr != columnNames.end();
			++itr)
		{
			os << *itr << " ";
		}
		os << ')';
		return os;
	}


	TableReferencesConstraintDef::~TableReferencesConstraintDef() 
	{
		delete fTableName;
		delete fRefAction;
	}
	
}

