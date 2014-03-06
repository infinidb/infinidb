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
*   $Id: serialize.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include <stdexcept>
#include <iostream>
#include <string>
#include <vector>
#include "brmtypes.h"

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

using namespace messageqcpp;
using namespace std;
using namespace ddlpackage;

template<class T>
void write_vec(vector<T*>& v, ByteStream& bs)
{
	bs << (quadbyte) v.size();
	typename vector<T*>::const_iterator itr;
	
	for(itr = v.begin(); itr != v.end(); ++itr)
		(*itr)->serialize(bs);
}

template<class T>
void read_vec(vector<T*>& v, ByteStream& bs)
{
	T* x;
	quadbyte count;
	bs >> count;
	while(count--) {
		x = new T;
		x->unserialize(bs);
		v.push_back(x);
	}
}


///////////////////////////////////////
/// CreateTableStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int CreateTableStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fTableDef = new TableDef();
	fTableDef->unserialize( bytestream );
    bytestream >> fSessionID;
    bytestream >> fSql;
    bytestream >> fOwner;
	bytestream >> fTableWithAutoi;
    return ret;
}

/** @brief Serialize to ByteStream */
int CreateTableStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_CREATE_TABLE_STATEMENT;

	// write table def
	fTableDef->serialize( bytestream );
	
	// write sessionid
	bytestream << fSessionID;

	// write the original ddl statement.
	bytestream << fSql;

	// write the owner (default schema).
	bytestream << fOwner;
	
	bytestream << fTableWithAutoi;

    return ret;
}



///////////////////////////////////////
/// AlterTableStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AlterTableStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;
	AlterTableAction* ata;
	messageqcpp::ByteStream::quadbyte type;

	fTableName = new QualifiedName();
	
	// read table name
	fTableName->unserialize( bytestream );

	// read alter action list
	quadbyte action_count;
	bytestream >> action_count;
	for ( unsigned int i = 0; i < action_count; i++ )
	{
		// read action type
		bytestream >> type;
	
		switch(type) 
		{
		case DDL_ATA_ADD_COLUMN:
			ata = new AtaAddColumn();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_ADD_COLUMNS:
			ata = new AtaAddColumns();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_DROP_COLUMN:
			ata = new AtaDropColumn();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_DROP_COLUMNS:
			ata = new AtaDropColumns();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_ADD_TABLE_CONSTRAINT:
			ata = new AtaAddTableConstraint();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_SET_COLUMN_DEFAULT:
			ata = new AtaSetColumnDefault();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_DROP_COLUMN_DEFAULT:
			ata = new AtaDropColumnDefault();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_DROP_TABLE_CONSTRAINT:
			ata = new AtaDropTableConstraint();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_RENAME_TABLE:
			ata = new AtaRenameTable();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_RENAME_COLUMN:
			ata = new AtaRenameColumn();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		case DDL_ATA_MODIFY_COLUMN_TYPE:
			ata = new AtaModifyColumnType();
			ata->unserialize(bytestream);
			fActions.push_back( ata );
			break;
		default:
			throw("Bad typecode for AlterTableAction");
			break;
		}
	    bytestream >> fSessionID;
	    bytestream >> fSql;
	    bytestream >> fOwner;
		bytestream >> fTableWithAutoi;
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int AlterTableStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ALTER_TABLE_STATEMENT;

	// write table name
	fTableName->serialize( bytestream );

	write_vec<AlterTableAction>(fActions,bytestream);
	
	// write sessionid
	bytestream << fSessionID;

	// write original ddl statement.
	bytestream << fSql;

	// write the owner (default schema).
	bytestream << fOwner; 
	
	bytestream << fTableWithAutoi;
	
    return ret;
}



///////////////////////////////////////
/// CreateIndexStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int CreateIndexStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	// read the index and schema name
	fIndexName = new QualifiedName();
	fIndexName->unserialize( bytestream );

	// read the table and schema name
	fTableName = new QualifiedName();
	fTableName->unserialize( bytestream );


	quadbyte column_count;
	bytestream >> column_count;


	std::string columnname;	
	for ( unsigned int i = 0; i < column_count; i++ )
	{
	    bytestream >> columnname;
		fColumnNames.push_back( columnname );
	}

	// read unique flag
	quadbyte unique;
	bytestream >> unique;
	fUnique = (unique != 0);
	bytestream >> fSessionID;
	bytestream >> fSql;
	bytestream >> fOwner;

    return ret;
}

/** @brief Serialize to ByteStream */
int CreateIndexStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_CREATE_INDEX;
	
	// write the index and schema name
	fIndexName->serialize( bytestream );

	// write the table and schema name
	fTableName->serialize( bytestream );

	// write column name list
    bytestream << (quadbyte) fColumnNames.size();
	ColumnNameList::const_iterator itr;
	for(itr = fColumnNames.begin();
		itr != fColumnNames.end();
		++itr)
	{
	    bytestream << *itr;
	}

	// write Unique flag
        bytestream << (quadbyte) fUnique;
    
    	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema).
	bytestream << fOwner;
	
	return ret;
}



///////////////////////////////////////
/// DropIndexStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int DropIndexStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fIndexName = new QualifiedName();

	// read the table and schema name
	fIndexName->unserialize(bytestream);
	
	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddlACK
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;
	return ret;
}

/** @brief Serialize to ByteStream */
int DropIndexStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_DROP_INDEX_STATEMENT;

	// write the table and schema name
	fIndexName->serialize( bytestream );
	
	// write sessionid
	bytestream << fSessionID;

	// write ddl statement
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;

    return ret;
}


///////////////////////////////////////
/// DropTableStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int DropTableStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	//cout << endl << "DropTableStatement unserialize testing started" << endl;

	fTableName = new QualifiedName();
	fTableName->unserialize(bytestream);

	messageqcpp::ByteStream::quadbyte cascade;

	// read cascade flag
	bytestream >> cascade;
	
	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddl
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;

	fCascade = (cascade != 0);

    return ret;
}

/** @brief Serialize to ByteStream */
int DropTableStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	//cout << "DropTableStatement serialize testing started" << endl;

	bytestream << (quadbyte) DDL_DROP_TABLE_STATEMENT;

	// write the table and schema name
	fTableName->serialize( bytestream );

	// read cascade flag
	bytestream << (quadbyte) fCascade;
	
	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;

    return ret;
}

///////////////////////////////////////
/// TruncTableStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TruncTableStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	//cout << endl << "TruncTableStatement unserialize testing started" << endl;

	fTableName = new QualifiedName();
	fTableName->unserialize(bytestream);

	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddl
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;

    return ret;
}

/** @brief Serialize to ByteStream */
int TruncTableStatement::serialize(ByteStream& bytestream)
{
    int ret=1;

	//cout << "TruncTableStatement serialize testing started" << endl;

	bytestream << (quadbyte) DDL_TRUNC_TABLE_STATEMENT;

	// write the table and schema name
	fTableName->serialize( bytestream );

	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;

    return ret;
}


///////////////////////////////////////
/// MarkPartitionStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int MarkPartitionStatement::unserialize(ByteStream& bytestream)
{
	int ret=1;

	fTableName = new QualifiedName();
	fTableName->unserialize(bytestream);

	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddl
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;
	
	fPartitions.clear();
	uint32_t size = 0;
	bytestream >> size;
	BRM::LogicalPartition part;
	for (uint32_t i = 0; i < size; i++)
	{
		part.unserialize(bytestream);
		fPartitions.insert(part);
	}

	return ret;
}

/** @brief Serialize to ByteStream */
int MarkPartitionStatement::serialize(ByteStream& bytestream)
{
	int ret=1;


	bytestream << (quadbyte) DDL_MARK_PARTITION_STATEMENT;

	// write the table and schema name
	fTableName->serialize( bytestream );

	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;

	bytestream << (uint32_t)fPartitions.size();
	set<BRM::LogicalPartition>::iterator it;
	for (it = fPartitions.begin(); it != fPartitions.end(); ++it)
		(*it).serialize(bytestream);
	
	return ret;
}

///////////////////////////////////////
/// DropPartitionStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int DropPartitionStatement::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fTableName = new QualifiedName();
	fTableName->unserialize(bytestream);

	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddl
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;
	
	uint32_t size = 0;
	bytestream >> size;
	BRM::LogicalPartition part;
	for (uint32_t i = 0; i < size; i++)
	{
		part.unserialize(bytestream);
		fPartitions.insert(part);
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int DropPartitionStatement::serialize(ByteStream& bytestream)
{
	int ret=1;

	bytestream << (quadbyte) DDL_DROP_PARTITION_STATEMENT;

	// write the table and schema name
	fTableName->serialize( bytestream );

	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;
	bytestream << (uint32_t)fPartitions.size();
	set<BRM::LogicalPartition>::iterator it;
	for (it = fPartitions.begin(); it != fPartitions.end(); ++it)
		(*it).serialize(bytestream);
	
	return ret;
}

///////////////////////////////////////
/// RestorePartitionStatement Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int RestorePartitionStatement::unserialize(ByteStream& bytestream)
{
	int ret=1;

	fTableName = new QualifiedName();
	fTableName->unserialize(bytestream);

	// read the sessionID
	bytestream >> fSessionID;

	// read the original ddl
	bytestream >> fSql;

	// read the owner (default schema)
	bytestream >> fOwner;
	
	uint32_t size = 0;
	bytestream >> size;
	BRM::LogicalPartition part;
	for (uint32_t i = 0; i < size; i++)
	{
		part.unserialize(bytestream);
		fPartitions.insert(part);
	}

	return ret;
}

/** @brief Serialize to ByteStream */
int RestorePartitionStatement::serialize(ByteStream& bytestream)
{
	int ret=1;

	bytestream << (quadbyte) DDL_RESTORE_PARTITION_STATEMENT;

	// write the table and schema name
	fTableName->serialize( bytestream );

	// write sessionid
	bytestream << fSessionID;

	// write original ddl
	bytestream << fSql;

	// write the owner (default schema)
	bytestream << fOwner;

	bytestream << (uint32_t)fPartitions.size();
	set<BRM::LogicalPartition>::iterator it;
	for (it = fPartitions.begin(); it != fPartitions.end(); ++it)
		(*it).serialize(bytestream);

	return ret;
}

///////////////////////////////////////
/// AtaAddColumn Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaAddColumn::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fColumnDef = new ColumnDef();

	// read column
	fColumnDef->unserialize( bytestream );

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaAddColumn::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_ADD_COLUMN;

	// write column
	fColumnDef->serialize( bytestream );

    return ret;
}



///////////////////////////////////////
/// AtaAddColumns Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaAddColumns::unserialize(ByteStream& bytestream)
{
    int ret=1;

	read_vec<ColumnDef>(fColumns, bytestream);

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaAddColumns::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_ADD_COLUMNS;

	write_vec<ColumnDef>(fColumns, bytestream);
	
    return ret;
}

///////////////////////////////////////
/// AtaDropColumns Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaDropColumns::unserialize(ByteStream& bytestream)
{
    int ret=1;

	quadbyte count;
	bytestream >> count;
	string colName;
	while(count--)
	{
		bytestream >> colName;
		fColumns.push_back(colName);
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaDropColumns::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_DROP_COLUMNS;

	bytestream << (quadbyte) fColumns.size();
	
	ColumnNameList::const_iterator itr;
	for(itr = fColumns.begin(); itr != fColumns.end(); itr++)
	{
		bytestream << *itr;
	}
	
    return ret;
}



///////////////////////////////////////
/// AtaAddTableConstraint Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaAddTableConstraint::unserialize(ByteStream& bytestream)
{
    int ret=1;

	quadbyte ctype;
	
	bytestream >> ctype;
	
	switch(ctype)
	{
	case DDL_TABLE_UNIQUE_CONSTRAINT_DEF:
		fTableConstraint = new TableUniqueConstraintDef;
		fTableConstraint->unserialize(bytestream);
		break;
	case DDL_TABLE_PRIMARY_CONSTRAINT_DEF:
		fTableConstraint = new TablePrimaryKeyConstraintDef;
		fTableConstraint->unserialize(bytestream);
		break;
	case DDL_TABLE_CHECK_CONSTRAINT_DEF:
		fTableConstraint = new TableCheckConstraintDef;
		fTableConstraint->unserialize(bytestream);
		break;
	case DDL_TABLE_REFERENCES_CONSTRAINT_DEF:
		fTableConstraint = new TableReferencesConstraintDef;
		fTableConstraint->unserialize(bytestream);
		break;
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaAddTableConstraint::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ATA_ADD_TABLE_CONSTRAINT;
	bytestream << (quadbyte) fTableConstraint->getSerialType();
	fTableConstraint->serialize( bytestream );

    return ret;
}


///////////////////////////////////////
/// AtaDropColumn Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaDropColumn::unserialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream >> fColumnName;
	quadbyte action;
	bytestream >> action;
	fDropBehavior = (DDL_REFERENTIAL_ACTION) action;

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaDropColumn::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_DROP_COLUMN;
	bytestream << fColumnName;
	bytestream << (quadbyte) fDropBehavior;

    return ret;
}


///////////////////////////////////////
/// AtaSetColumnDefault Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaSetColumnDefault::unserialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream >> fColumnName;
	fDefaultValue = new ColumnDefaultValue();
	fDefaultValue->unserialize(bytestream);

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaSetColumnDefault::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ATA_SET_COLUMN_DEFAULT;
	bytestream << fColumnName;
	fDefaultValue->serialize( bytestream );

    return ret;
}



///////////////////////////////////////
/// AtaDropColumnDefault Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaDropColumnDefault::unserialize(ByteStream& bytestream)
{
    int ret=1;

	// read column name
	bytestream >> fColumnName;

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaDropColumnDefault::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ATA_DROP_COLUMN_DEFAULT;
	bytestream << fColumnName;

    return ret;
}



///////////////////////////////////////
/// AtaDropTableConstraint Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaDropTableConstraint::unserialize(ByteStream& bytestream)
{
    int ret=1;

	// read table constraint
	bytestream >> fConstraintName;
	quadbyte action;
	bytestream >> action;

	fDropBehavior = (DDL_REFERENTIAL_ACTION) action;

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaDropTableConstraint::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ATA_DROP_TABLE_CONSTRAINT;
	bytestream << fConstraintName;
	bytestream << (quadbyte) fDropBehavior;

    return ret;
}



///////////////////////////////////////
/// AtaRenameTable Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaRenameTable::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fQualifiedName = new QualifiedName();

	// read the table and schema name
	fQualifiedName->unserialize( bytestream );

    return ret;
}

/** @brief Serialize to ByteStream */
int AtaRenameTable::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_RENAME_TABLE;

	// write the table and schema name
	fQualifiedName->serialize( bytestream );

    return ret;
}



///////////////////////////////////////
/// AtaModifyColumnType Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaModifyColumnType::unserialize(ByteStream& bytestream)
{
    int ret=1;

	fColumnType = new ColumnType();

	// read column type and name
	fColumnType->unserialize( bytestream );

	bytestream >> fName;

	return ret;
}

/** @brief Serialize to ByteStream */
int AtaModifyColumnType::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write type code
	bytestream << (quadbyte) DDL_ATA_MODIFY_COLUMN_TYPE;

	// write column type and name
	fColumnType->serialize( bytestream );

	bytestream << fName;

    return ret;
}



///////////////////////////////////////
/// AtaRenameColumn Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int AtaRenameColumn::unserialize(ByteStream& bytestream)
{
    int ret=1;

	// read column names
	bytestream >> fName;
	bytestream >> fNewName;
	if (!fNewType)
		fNewType = new ColumnType(DDL_INT);
	fNewType->unserialize(bytestream);

	read_vec<ColumnConstraintDef>(fConstraints, bytestream);

	// read default value. It might not be there since the parser does
	// not make one unless specified.

	quadbyte type;
	bytestream >> type;

	if(type == DDL_NULL) {
		fDefaultValue = 0;
	}
	else {
		fDefaultValue = new ColumnDefaultValue();
		fDefaultValue->unserialize(bytestream);
	}
    return ret;
}

/** @brief Serialize to ByteStream */
int AtaRenameColumn::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << (quadbyte) DDL_ATA_RENAME_COLUMN;
	bytestream << fName;
	bytestream << fNewName;
	if (!fNewType)
		fNewType = new ColumnType(DDL_INT);
	fNewType->serialize(bytestream);
	
	// serialize column constraints.
	write_vec<ColumnConstraintDef>(fConstraints, bytestream);

	if(0 == fDefaultValue) {
		bytestream << (quadbyte)DDL_NULL;
	}
	else {
		bytestream << (quadbyte)DDL_COLUMN_DEFAULT_VALUE;
		fDefaultValue->serialize( bytestream );
	}
    return ret;
}



///////////////////////////////////////
/// ColumnType Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int ColumnType::unserialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte ftype;
	messageqcpp::ByteStream::quadbyte length;
	messageqcpp::ByteStream::quadbyte precision;
	messageqcpp::ByteStream::quadbyte scale;
	messageqcpp::ByteStream::quadbyte withtimezone;
	messageqcpp::ByteStream::quadbyte compressiontype;
	std::string autoincrement;
	messageqcpp::ByteStream::octbyte  nextVal;
	
	// read column types
	bytestream >> ftype;
	bytestream >> length;
	bytestream >> precision;
	bytestream >> scale;
	bytestream >> withtimezone;
	bytestream >> compressiontype;
	bytestream >> autoincrement;
	bytestream >> nextVal;

	fType = ftype;
	fLength = length;
	fPrecision = precision;
	fScale = scale;
	fWithTimezone = (withtimezone != 0);
	fCompressiontype = compressiontype;
	fAutoincrement = autoincrement;
	fNextvalue = nextVal;

//	cout << "BS length = " << bytestream.length() << endl;

    return ret;
}

/** @brief Serialize to ByteStream */
int ColumnType::serialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte ftype = fType;
	messageqcpp::ByteStream::quadbyte length = fLength;
	messageqcpp::ByteStream::quadbyte precision = fPrecision;
	messageqcpp::ByteStream::quadbyte scale = fScale;
	messageqcpp::ByteStream::quadbyte withtimezone = fWithTimezone;
	messageqcpp::ByteStream::quadbyte compressiontype = fCompressiontype;
	std::string autoincrement = fAutoincrement;
	messageqcpp::ByteStream::octbyte  nextVal = fNextvalue;

	// write column types
	bytestream << ftype;
	bytestream << length;
	bytestream << precision;
	bytestream << scale;
	bytestream << withtimezone;
	bytestream << compressiontype;
	bytestream << autoincrement;
	bytestream << nextVal;

//	cout << "BS length = " << bytestream.length() << endl;

    return ret;
}



///////////////////////////////////////
/// ColumnConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int ColumnConstraintDef::unserialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte deferrable;
	messageqcpp::ByteStream::quadbyte checktime;
	messageqcpp::ByteStream::quadbyte constrainttype;

	// read constaint defs
	bytestream >> fName;
	bytestream >> deferrable;
	bytestream >> checktime;
	bytestream >> constrainttype;
	bytestream >> fCheck;

	fDeferrable = (deferrable != 0);
	fCheckTime = (DDL_CONSTRAINT_ATTRIBUTES) checktime;
	fConstraintType = (DDL_CONSTRAINTS) constrainttype;

	return ret;
}

/** @brief Serialize to ByteStream */
int ColumnConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte deferrable = fDeferrable;
	messageqcpp::ByteStream::quadbyte checktime = fCheckTime;
	messageqcpp::ByteStream::quadbyte constrainttype = fConstraintType;

	// write constaint defs
	bytestream << fName;
	bytestream << deferrable;
	bytestream << checktime;
	bytestream << constrainttype;
	bytestream << fCheck;

    return ret;
}



///////////////////////////////////////
/// ColumnDefaultValue Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int ColumnDefaultValue::unserialize(ByteStream& bytestream)
{
    int ret=1;

	// read update and delete actions
	quadbyte qb;
	
	bytestream >> qb;
	fNull = (qb != 0);
	
	bytestream >> fValue;

    return ret;
}

/** @brief Serialize to ByteStream */
int ColumnDefaultValue::serialize(ByteStream& bytestream)
{
    int ret=1;

	// write update and delete actions
	bytestream << (quadbyte)fNull;
	bytestream << fValue;

    return ret;
}



///////////////////////////////////////
/// ColumnDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int ColumnDef::unserialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream >> fName;

	// read column type
	fType = new ColumnType();
	fType->unserialize( bytestream );

	read_vec<ColumnConstraintDef>(fConstraints, bytestream);

	// read default value. It might not be there since the parser does
	// not make one unless specified.

	quadbyte type;
	bytestream >> type;

	if(type == DDL_NULL) {
		fDefaultValue = 0;
	}
	else {
		fDefaultValue = new ColumnDefaultValue();
		fDefaultValue->unserialize(bytestream);
	}

//	cout << "BS length = " << bytestream.length() << endl;
    return ret;
}

/** @brief Serialize to ByteStream */
int ColumnDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << fName;

	// write column type
	fType->serialize( bytestream );

	// serialize column constraints.
	write_vec<ColumnConstraintDef>(fConstraints, bytestream);

	if(0 == fDefaultValue) {
		bytestream << (quadbyte)DDL_NULL;
	}
	else {
		bytestream << (quadbyte)DDL_COLUMN_DEFAULT_VALUE;
		fDefaultValue->serialize( bytestream );
	}
	

// 	cout << "BS length = " << bytestream.length() << endl;

    return ret;
}



///////////////////////////////////////
/// TableConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TableConstraintDef::unserialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte constrainttype;

	// read constraint def
	bytestream >> constrainttype;

	fConstraintType = (DDL_CONSTRAINTS) constrainttype;



    return ret;
}

/** @brief Serialize to ByteStream */
int TableConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte constrainttype = fConstraintType;

	// write constraint def
	bytestream << constrainttype;

	
    return ret;
}



///////////////////////////////////////
/// TableUniqueConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TableUniqueConstraintDef::unserialize(ByteStream& bytestream)
{
    int ret=1;
	quadbyte count;

	bytestream >> fName;
	bytestream >> count;

	string str;
	
	while(count-- > 0)
	{
		bytestream >> str;
		fColumnNameList.push_back(str);
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int TableUniqueConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << fName;
	
	bytestream << (quadbyte) fColumnNameList.size();
	
	ColumnNameList::const_iterator itr;
	for(itr = fColumnNameList.begin(); itr != fColumnNameList.end(); ++itr)
	{
		bytestream << *itr;
	}

    return ret;
}



///////////////////////////////////////
/// TablePrimaryKeyConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TablePrimaryKeyConstraintDef::unserialize(ByteStream& bytestream)
{
    int ret=1;
	quadbyte count;

	bytestream >> fName;
	bytestream >> count;

	string str;
	
	while(count-- > 0)
	{
		bytestream >> str;
		fColumnNameList.push_back(str);
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int TablePrimaryKeyConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << fName;
	
	bytestream << (quadbyte) fColumnNameList.size();
	
	ColumnNameList::const_iterator itr;
	for(itr = fColumnNameList.begin(); itr != fColumnNameList.end(); ++itr)
	{
		bytestream << *itr;
	}

    return ret;
}



///////////////////////////////////////
/// ReferentialAction Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int ReferentialAction::unserialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte onupdate;
	messageqcpp::ByteStream::quadbyte ondelete;

	// read check
	bytestream >> onupdate;
	bytestream >> ondelete;

	fOnUpdate = (DDL_REFERENTIAL_ACTION) onupdate;
	fOnDelete = (DDL_REFERENTIAL_ACTION) ondelete;


    return ret;
}

/** @brief Serialize to ByteStream */
int ReferentialAction::serialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte onupdate = fOnUpdate;
	messageqcpp::ByteStream::quadbyte ondelete = fOnDelete;

	// write update and delete actions
	bytestream << onupdate;
	bytestream << ondelete;


    return ret;
}



///////////////////////////////////////
/// TableReferencesConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TableReferencesConstraintDef::unserialize(ByteStream& bytestream)
{
	string str;
    int ret=1;
	quadbyte count;

	// Name
	bytestream >> fName;

	// Local columns
	bytestream >> count;
	while(count-- > 0) {
		bytestream >> str;
		fColumns.push_back(str);
	}
	
	// Table name
	fTableName = new QualifiedName;
	fTableName->unserialize(bytestream);

	// Foreign columns
	bytestream >> count;
	while(count-- > 0) {
		bytestream >> str;
		fForeignColumns.push_back(str);
	}

	// Match type
	quadbyte matchType;
	bytestream >> matchType;
	fMatchType = (DDL_MATCH_TYPE) matchType;

	// Ref Action
	quadbyte sertype;
	bytestream >> sertype;
	if(sertype == DDL_NULL) {
		fRefAction = 0;
	}
	else {
		fRefAction = new ReferentialAction();
		fRefAction->unserialize(bytestream);
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int TableReferencesConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;
	messageqcpp::ByteStream::quadbyte size;
	bytestream << fName;
	
	// local columns
	size = fColumns.size();
	bytestream << size;
	ColumnNameList::const_iterator itr;
	for(itr = fColumns.begin(); itr != fColumns.end(); ++itr)
		bytestream << *itr;

	// Table name
	fTableName->serialize(bytestream);

	// Foreign columns
	size = fForeignColumns.size();
	bytestream << size;
	for(itr = fForeignColumns.begin(); itr != fForeignColumns.end(); ++itr)
		bytestream << *itr;

	// Match type
	bytestream << (quadbyte) fMatchType;

	// Ref action
	if(0 == fRefAction) {
		bytestream << (quadbyte) DDL_NULL;
	}
	else {
		bytestream << (quadbyte) DDL_REF_ACTION;
		fRefAction->serialize(bytestream);
	}

    return ret;
}



///////////////////////////////////////
/// TableCheckConstraintDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TableCheckConstraintDef::unserialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream >> fName;
	bytestream >> fCheck;

    return ret;
}

/** @brief Serialize to ByteStream */
int TableCheckConstraintDef::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << fName;
	bytestream << fCheck;

    return ret;
}



///////////////////////////////////////
/// TableDef Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int TableDef::unserialize(ByteStream& bytestream)
{
    int ret=1;
	messageqcpp::ByteStream::quadbyte type;	
	
	// table name
	fQualifiedName = new QualifiedName();
	fQualifiedName->unserialize(bytestream);
	
	// ColumnDefs
	read_vec<ColumnDef>(fColumns, bytestream);

	// read column constraint list
	quadbyte count;
    bytestream >> count;
	TableConstraintDef* constraint;
	
	while (count-- > 0)
	{
		bytestream >> type;

		switch(type) 
		{
		case DDL_TABLE_UNIQUE_CONSTRAINT_DEF:
			constraint = new TableUniqueConstraintDef();
			constraint->unserialize(bytestream);
			fConstraints.push_back( constraint );
			break;
		case DDL_TABLE_PRIMARY_CONSTRAINT_DEF:
			constraint = new TablePrimaryKeyConstraintDef();
			constraint->unserialize(bytestream);
			fConstraints.push_back( constraint );
			break;
		case DDL_TABLE_REFERENCES_CONSTRAINT_DEF:
			constraint = new TableReferencesConstraintDef();
			constraint->unserialize(bytestream);
			fConstraints.push_back( constraint );
			break;
		case DDL_TABLE_CHECK_CONSTRAINT_DEF:
			constraint = new TableCheckConstraintDef();
			constraint->unserialize(bytestream);
			fConstraints.push_back( constraint );
			break;
		default:
			throw("Bad typecode for TableConstraintDef");
			break;
		}
	}

	// read option maps list
    bytestream >> count;
	for ( unsigned int i = 0; i < count; i++ )
	{
		//  read option map
		string map, map1;
	   	bytestream >> map;
	   	bytestream >> map1;
		fOptions.insert( pair<string, string> (map, map1) );
	}

    return ret;
}

/** @brief Serialize to ByteStream */
int TableDef::serialize(ByteStream& bytestream)
{
    int ret=1;
    messageqcpp::ByteStream::quadbyte size;
	// table name
	fQualifiedName->serialize( bytestream );

	// ColumnDef's
	write_vec<ColumnDef>(fColumns, bytestream);
	
	// write table constraint list
	size = fConstraints.size();
	bytestream << size;
	
	TableConstraintDefList::const_iterator itr;
	for(itr = fConstraints.begin();
		itr != fConstraints.end();
		++itr)
	{
		bytestream << (quadbyte) (*itr)->getSerialType();
		(*itr)->serialize( bytestream );
	}

	// serialize TableOptions
	size = fOptions.size();
    bytestream << size;

	pair<string, string> oval;
	TableOptionMap::const_iterator itr2;
	for(itr2 = fOptions.begin();
		itr2 != fOptions.end();
		++itr2)
	{
		oval = *itr2;
    	bytestream << oval.first;
    	bytestream << oval.second;
	}

    return ret;
}

///////////////////////////////////////
/// QualifiedName Serialization
///////////////////////////////////////

/** @brief Construct from Bytestream */
int QualifiedName::unserialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream >> fCatalog;
	bytestream >> fSchema;
	bytestream >> fName;


    return ret;
}

/** @brief Serialize to ByteStream  */
int QualifiedName::serialize(ByteStream& bytestream)
{
    int ret=1;

	bytestream << fCatalog;
	bytestream << fSchema;
	bytestream << fName;

    return ret;
}


///////////////////////////////////////
/// ConstraintAttributes Serialization
///////////////////////////////////////


/** @brief Construct from Bytestream */
int ConstraintAttributes::unserialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte checktime;
	messageqcpp::ByteStream::quadbyte deferrable;

	// read the checktime and deferrable flag
	bytestream >> checktime;
	bytestream >> deferrable;

	fCheckTime = (DDL_CONSTRAINT_ATTRIBUTES) checktime;
	fDeferrable = (deferrable != 0);


    return ret;
}

/** @brief Serialize to ByteStream */
int ConstraintAttributes::serialize(ByteStream& bytestream)
{
    int ret=1;

	messageqcpp::ByteStream::quadbyte checktime = fCheckTime;
	messageqcpp::ByteStream::quadbyte deferrable = fDeferrable;

	// write the checktime and deferrable flag
	bytestream << checktime;
	bytestream << deferrable;


    return ret;
}



