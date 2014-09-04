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
*   $Id: altertable.cpp 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

namespace ddlpackage {
	using namespace std;

	AlterTableStatement::AlterTableStatement(QualifiedName *qName, AlterTableActionList *ataList):
		fTableName(qName),
		fActions(*ataList)
	{
		delete ataList;
	}

	AlterTableStatement::~AlterTableStatement()
	{
		delete fTableName;
		AlterTableActionList::iterator itr;
		for(itr=fActions.begin(); itr != fActions.end(); ++itr) {
			delete *itr;
		}
	}

	std::ostream& AlterTableStatement::put(std::ostream& os) const
	{
		AlterTableActionList::const_iterator itr;
		os << "Alter Table " << *fTableName << endl;
	
		for(itr = fActions.begin(); itr != fActions.end(); ++itr) {
			os << **itr << endl;
		}
		return os;
	}



	/** @brief Format to ostream.  Diagnostic. */
	std::ostream& AlterTableAction::put(std::ostream& os) const
	{
		os << "AlterTableAction put stub";
		return os;
	}

	/** @brief Invokes the virtual function put, to dispatch to subclass
		ostream writers. */
	std::ostream &operator<<(std::ostream& os, const AlterTableAction &ata) 
	{
		return ata.put(os);
	}
	

	AtaAddColumn::~AtaAddColumn()
	{
		delete fColumnDef;
	}


	/** @brief ostream output */
	std::ostream& AtaAddColumn::put(std::ostream& os) const
	{
		os << "Add Column" << endl;
		os << *fColumnDef << endl;
		return os;
	}



	AtaDropColumn::AtaDropColumn(std::string columnName, DDL_REFERENTIAL_ACTION dropBehavior) :
		fColumnName(columnName),
		fDropBehavior(dropBehavior) 
	{
	}

	std::ostream& AtaDropColumn::put(std::ostream& os) const
	{
		os << "Drop Column: " << fColumnName << " "
		   << ReferentialActionStrings[fDropBehavior];
		return os;
	}
	

	AtaModifyColumnType::~AtaModifyColumnType() 
	{
		delete fColumnType;
	}

	std::ostream& AtaModifyColumnType::put(std::ostream& os) const
	{
		os << "Modify column type: " << fName << " " << *fColumnType;
		
		return os;
	}



	AtaRenameColumn::~AtaRenameColumn()
	{
		delete fNewType;
	}

	std::ostream& AtaRenameColumn::put(std::ostream& os) const
	{
		os << "Rename Column: " << fName << " -> " << fNewName << " (" << *fNewType << ')';
		return os;
	}

	

	AtaSetColumnDefault::AtaSetColumnDefault(const char *colName, ColumnDefaultValue *defaultValue) :
		fColumnName(colName),
		fDefaultValue(defaultValue)
	{
	}


	AtaSetColumnDefault::~AtaSetColumnDefault()
	{
		delete fDefaultValue;
	}


	std::ostream& AtaSetColumnDefault::put(std::ostream& os) const
	{
		os << "Set Column Default: " << fColumnName << " "
		   << *fDefaultValue << endl;
		return os;
	}




	AtaDropColumnDefault::AtaDropColumnDefault(const char *colName) :
		fColumnName(colName)
	{
	}

	std::ostream& AtaDropColumnDefault::put(std::ostream& os) const
	{
		os << "Drop Column Default: " << fColumnName << " ";
		return os;
	}




	AtaRenameTable::AtaRenameTable(QualifiedName *qualifiedName) :
		fQualifiedName(qualifiedName)
	{
	}

	AtaRenameTable::~AtaRenameTable()
	{
		delete fQualifiedName;
	}


	std::ostream& AtaRenameTable::put(std::ostream& os) const
	{
		os << "Rename Table: " << *fQualifiedName << endl;
		return os;
	}




	AtaAddColumns::~AtaAddColumns()
	{
		ColumnDefList::iterator itr;
		for(itr=fColumns.begin(); itr != fColumns.end(); itr++)
			delete *itr;
	}


	AtaAddColumns::AtaAddColumns(TableElementList *tableElements)
	{
		/* It is convenient to reuse the grammar rules for
		   table_element_list, and we do.  So, it is possible for
		   there to be errant table constraint defs in the input list.
		   We ignore them.  That is all we are doing here.
		 */
		ColumnDef *column;
		TableElementList::const_iterator itr;
		for(itr = tableElements->begin();
			itr != tableElements->end();
			++itr)
		{
			column = dynamic_cast<ColumnDef *>(*itr);
			if(0 != column) {
				fColumns.push_back(column);
			}
		}
		delete tableElements;
	}

	std::ostream& AtaAddColumns::put(std::ostream& os) const
	{
		os << "Add Columns: " << endl;
		ColumnDefList::const_iterator itr;
		for(itr = fColumns.begin();
			itr != fColumns.end();
			++itr)
		{
			os << **itr << endl;
		}
		
		return os;
	}
	//////////////////////////

	AtaDropColumns::~AtaDropColumns()
	{
	}


	AtaDropColumns::AtaDropColumns(ColumnNameList *columnNames)
	{
		fColumns = *columnNames;
		delete columnNames;
	}

	std::ostream& AtaDropColumns::put(std::ostream& os) const
	{
		os << "Drop Columns: " << endl;
		ColumnNameList::const_iterator itr;
		for(itr = fColumns.begin();
			itr != fColumns.end();
			++itr)
		{
			os << *itr << endl;
		}
		
		return os;
	}




	AtaAddTableConstraint::AtaAddTableConstraint(TableConstraintDef *tableConstraint) :
		fTableConstraint(tableConstraint)
	{
	}

	AtaAddTableConstraint::~AtaAddTableConstraint()
	{
		delete fTableConstraint;
	}


	std::ostream& AtaAddTableConstraint::put(std::ostream& os) const
	{
		os << "Add Table Constraint:" << endl;
		os << *fTableConstraint << endl;
		return os;
	}




	AtaDropTableConstraint::AtaDropTableConstraint
	(const char *constraintName, DDL_REFERENTIAL_ACTION dropBehavior) :
		fConstraintName(constraintName),
		fDropBehavior(dropBehavior)
	{
	}

	std::ostream& AtaDropTableConstraint::put(std::ostream& os) const
	{
		os << "Drop Table Constraint: " << fConstraintName;
		return os;
	}

}
