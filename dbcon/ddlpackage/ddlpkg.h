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
*   $Id: ddlpkg.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file
 *
 * Class definitions for ddl parser objects.  These are the things
 * manufactured by the bison SQL parser.  SqlStatementList is the
 * toplevel parser result.
 *
 * Parser objects are described as structs.  These objects never
 * change state following construction.  An accessor/mutator
 * discipline would amount to clutter.
 *
 * The constructor forms here are directly driven by the structure of
 * the grammar in ddl.y.
 *
 */

#ifndef DDLPACKAGE_H
#define DDLPACKAGE_H

#include <vector>
#include <string>
#include <map>
#include <set>
#include <utility>
#include <iostream>
#include "bytestream.h"
#include "logicalpartition.h"

#if defined(_MSC_VER) && defined(xxxDDLPKG_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace ddlpackage
{
typedef messageqcpp::ByteStream::byte byte;
typedef messageqcpp::ByteStream::doublebyte doublebyte;
typedef messageqcpp::ByteStream::quadbyte quadbyte;

class AlterTable;
struct AlterTableAction;
struct ColumnDef;
struct ColumnDefaultValue;
struct ColumnType;
struct QualifiedName;
struct TableDef;
struct CreateTableStatement;
struct SqlStatementList;
struct SqlStatement;
struct ColumnConstraintDef;
struct TableConstraintDef;
struct SchemaObject;
struct ReferentialAction;

typedef SqlStatement DDLPkg;

typedef std::vector<ColumnConstraintDef*> ColumnConstraintList;
typedef std::vector<SchemaObject*> SchemaObjectList;
typedef std::vector<std::string> ColumnNameList;
typedef std::vector<AlterTableAction*> AlterTableActionList;
typedef std::vector<ColumnDef*> ColumnDefList;
typedef std::map<std::string, std::string> TableOptionMap;
typedef std::vector<SchemaObject*> TableElementList;
typedef std::vector<TableConstraintDef*> TableConstraintDefList;

std::ostream& operator<<(std::ostream& os, const ColumnType& columnType);
std::ostream& operator<<(std::ostream &os, const QualifiedName& constraint);
std::ostream& operator<<(std::ostream &os, const TableConstraintDef& constraint);
std::ostream& operator<<(std::ostream& os, const ColumnConstraintDef& con);
std::ostream& operator<<(std::ostream& os, const ColumnDef& column);
EXPORT std::ostream& operator<<(std::ostream& os, const SqlStatementList& ct);
EXPORT std::ostream& operator<<(std::ostream& os, const SqlStatement& stmt);
std::ostream& operator<<(std::ostream& os, const ColumnDefList& columnList);
std::ostream& operator<<(std::ostream& os, const AlterTableAction& columnList);
std::ostream& operator<<(std::ostream& os, const ColumnDefaultValue& defaultValue);
std::ostream& operator<<(std::ostream& os, const ColumnNameList& columnNames);
std::ostream& operator<<(std::ostream& os, const ReferentialAction& ref);
std::ostream& operator<<(std::ostream& os, const TableDef& tableDef);


/** @brief Verb List
*   Make sure to keep the enum and string list in-sync
*/
enum DDL_VERBS {
    DDL_CREATE,
    DDL_ALTER,
    DDL_DROP,
    DDL_INVALID_VERB
};
/** @brief Subject List
*   Make sure to keep the enum and string list in-sync
*/
enum DDL_SUBJECTS {
    DDL_TABLE,
    DDL_INDEX,
    DDL_INVALID_SUBJECT
};

enum DDL_CONSTRAINT_ATTRIBUTES
{
    DDL_DEFERRABLE,
    DDL_NON_DEFERRABLE,
    DDL_INITIALLY_IMMEDIATE,
    DDL_INITIALLY_DEFERRED,
    DDL_INVALID_ATTRIBUTE
};

const std::string ConstraintAttrStrings[] =
    {
        "deferrable",
        "non-deferrable",
        "initially-immediate",
        "initially-deferred",
        "invalid"
    };


enum DDL_REFERENTIAL_ACTION {
    DDL_CASCADE,
    DDL_SET_NULL,
    DDL_SET_DEFAULT,
    DDL_NO_ACTION,
    DDL_RESTRICT,
    DDL_INVALID_REFERENTIAL_ACTION
};

const std::string ReferentialActionStrings[] =
    {
        "cascade",
        "set_null",
        "set_default",
        "no_action",
        "invalid_action"
    };

enum DDL_MATCH_TYPE {
    DDL_FULL,
    DDL_PARTIAL,
    DDL_INVALID_MATCH_TYPE
};

const std::string MatchTypeStrings[] =
    {
        "full",
        "partial",
        "invalid_match_type"
    };


/** @brief Constraint List
 *   Make sure to keep the enum and string list in-sync
 */
enum DDL_CONSTRAINTS {
    DDL_PRIMARY_KEY,
    DDL_FOREIGN_KEY,
    DDL_CHECK,
    DDL_UNIQUE,
    DDL_REFERENCES,
    DDL_NOT_NULL,
    DDL_AUTO_INCREMENT,
    DDL_INVALID_CONSTRAINT
};
/** @brief
 */
const std::string ConstraintString[] =
    {
        "primary",
        "foreign",
        "check",
        "unique",
        "references",
        "not_null",
        "auto_increment"
        ""
    };

/** @brief Datatype List
 *   Make sure to keep the enum, string, and length list in-sync
 */
enum DDL_DATATYPES {
    DDL_BIT,
    DDL_TINYINT,
    DDL_CHAR,
    DDL_SMALLINT,
    DDL_DECIMAL,
    DDL_MEDINT,
    DDL_INT,
    DDL_FLOAT,
    DDL_DATE,
    DDL_BIGINT,
    DDL_DOUBLE,
    DDL_DATETIME,
    DDL_VARCHAR,
    DDL_VARBINARY,
    DDL_CLOB,
    DDL_BLOB,
    DDL_REAL,
    DDL_NUMERIC,
    DDL_NUMBER,
    DDL_INTEGER,
    DDL_UNSIGNED_TINYINT,
    DDL_UNSIGNED_SMALLINT,
    DDL_UNSIGNED_MEDINT,
    DDL_UNSIGNED_INT,
    DDL_UNSIGNED_BIGINT,
    DDL_UNSIGNED_DECIMAL,
    DDL_UNSIGNED_FLOAT,
    DDL_UNSIGNED_DOUBLE,
    DDL_UNSIGNED_NUMERIC,
    DDL_INVALID_DATATYPE
};

/** @brief Datatype string list
 */
const std::string DDLDatatypeString[] =
    {
        "bit",
        "tinyint",
        "char",
        "smallint",
        "decimal",
        "medint",
        "integer",
        "float",
        "date",
        "bigint",
        "double",
        "datetime",
        "varchar",
        "varbinary",
        "clob",
        "blob",
        "real",
        "numeric",
        "number",
        "integer",
        "unsigned-tinyint",
        "unsigned-smallint",
        "unsigned-medint",
        "unsigned-int",
        "unsigned-bigint",
        "unsigned-decimal",
        "unsigned-float",
        "unsigned-double",
        "unsigned-numeric",
        ""
    };

/** @brief Alter table action string list
 */
const std::string AlterActionString[] =
    {
        "AtaAddColumn",
        "AtaAddColumns",
        "AtaDropColumn",
        "AtaDropColumns",
        "AtaAddTableConstraint",
        "AtaSetColumnDefault",
        "AtaDropColumnDefault",
        "AtaDropTableConstraint",
        "AtaRenameTable",
        "AtaModifyColumnType",
        "AtaRenameColumn"
    };
/** @brief Datatype Length list
 *
 */
const int  DDLDatatypeLength[] =
    {
        1,		// BIT                
        1,		// TINYINT       
        1,		// CHAR          
        2,		// SMALLINT      
        2,		// DECIMAL       
        4,		// MEDINT        
        4,		// INT           
        4,		// FLOAT         
        4,		// DATE          
        8,		// BIGINT        
        8,		// DOUBLE        
        8,		// DATETIME      
        8, 		// VARCHAR       
        8, 		// VARBINAR      
        8,		// CLOB          
        8,		// BLOB          
        4,		// REAL          
        2,		// NUMERIC       
        4,		// NUMBER        
        4,		// INTEGER       
        1,      // UNSIGNED_TINYINT, 
        2,      // UNSIGNED_SMALLINT,
        4,      // UNSIGNED_MEDINT,  
        4,      // UNSIGNED_INT,     
        8,      // UNSIGNED_BIGINT,  
        2,      // UNSIGNED_DECIMAL, 
        4,      // UNSIGNED_FLOAT,   
        8,      // UNSIGNED_DOUBLE,  
        2,      // UNSIGNED_NUMERIC, 
        -1		// INVALID LENGTH
    };

enum DDL_SERIAL_TYPE {
    DDL_TABLE_DEF,
    DDL_COLUMN_DEF,
    DDL_COLUMN_CONSTRAINT_DEF,
    DDL_TABLE_CONSTRAINT_DEF,
    DDL_SQL_STATEMENT_LIST,
    DDL_CREATE_TABLE_STATEMENT,
    DDL_CREATE_INDEX,
    DDL_ALTER_TABLE_STATEMENT,
    DDL_ATA_ADD_COLUMN,
    DDL_ATA_ADD_COLUMNS,
    DDL_ATA_DROP_COLUMN,
    DDL_ATA_ADD_TABLE_CONSTRAINT,
    DDL_ATA_SET_COLUMN_DEFAULT,
    DDL_ATA_DROP_COLUMN_DEFAULT,
    DDL_ATA_DROP_TABLE_CONSTRAINT,
    DDL_ATA_RENAME_TABLE,
    DDL_ATA_RENAME_COLUMN,
    DDL_ATA_MODIFY_COLUMN_TYPE,
    DDL_COLUMN_TYPE,
    DDL_COLUMN_DEFAULT_VALUE,
    DDL_TABLE_UNIQUE_CONSTRAINT_DEF,
    DDL_TABLE_PRIMARY_CONSTRAINT_DEF,
    DDL_REF_ACTION,
    DDL_TABLE_REFERENCES_CONSTRAINT_DEF,
    DDL_TABLE_CHECK_CONSTRAINT_DEF,
    DDL_QUALIFIED_NAME,
    DDL_CONSTRAINT_ATTRIBUTES_DEF,
    DDL_DROP_INDEX_STATEMENT,
    DDL_DROP_TABLE_STATEMENT,
    DDL_ATA_DROP_COLUMNS,
    DDL_NULL,
    DDL_INVALID_SERIAL_TYPE,
    DDL_TRUNC_TABLE_STATEMENT,
	DDL_MARK_PARTITION_STATEMENT,
	DDL_RESTORE_PARTITION_STATEMENT,
	DDL_DROP_PARTITION_STATEMENT
};


/** @brief An abstract base for TableDef, ColumnDef, ...
 *
 * The primary purpose of this class is to provide a unified type
 * for things that can appear together in syntactic elements.  For
 * example, column definitions and table constraints can appear
 * together in the table_element_list of a create table statement.
 * We need a base class so that we can return different concrete
 * types as the semantic value of bison rules, while having a
 * single, more abstract type to report to bison as the type of
 * the semantic value.
 */
struct SchemaObject
{
    virtual ~SchemaObject()
    {}

    SchemaObject(std::string name):
            fName(name)
    {}

    SchemaObject() :
            fName("unnamed")
    {}

    std::string fName;

};




/** @brief SqlStatement represents a toplevel
 * syntactic element such as a create table or alter table SQL
 * statement.
 *
 * SqlStatements are containers for the various structures
 * manufactured by the parsing process for a single SQL
 * statement.
 */
struct SqlStatement
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs)=0;

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs)=0;


    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream &os) const = 0;

    EXPORT SqlStatement();

    EXPORT virtual ~SqlStatement();

    /** @brief The session ID assigned to this stmt by the front end (in theory)
     *
     * XXXPAT: need to fix this.  It should be type execplan::SessionManager::SID, but
     * that causes a circular dependency in the header files.  Should unravel that at
     * some point.
     	 * 
     * Right now this var is initialized from a counter.  At some point we need
     * to serialize/unserialize it from a byte stream.
     */
    uint32_t fSessionID;
    
    /** @brief The original sql string
    */
    std::string fSql;

    /** @brief the default schema (owner that will be used when not specified)
    */
    std::string fOwner;

	
	uint32_t fTableWithAutoi; // has autoincrement column? 
	
};



/** @brief Collects SqlStatements so that we can support the
 * parsing of sqltext containing multiple statements.
 *
 * The SqlParser also accepts empty statements (a mixture of
 * whitespace and semicolons) in which case the result can be a
 * SqlStatementList of zero items.
 */
struct SqlStatementList
{
    SqlStatementList()
    {}

    SqlStatement* operator[](int i) const
    {
        return fList[i];
    }


    virtual ~SqlStatementList();

    /** @brief Add a statement to the underlying container. */
    void push_back(SqlStatement* v);

    std::vector<SqlStatement*> fList;
    std::string fSqlText;

private:
    SqlStatementList(const SqlStatementList& x);

};



/** @brief Represents the create table statement
 *
 * @note It takes possession of the TableDef given to it.
 */
struct CreateTableStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    EXPORT CreateTableStatement();

    /** @brief You can't have a CreateTableStatement without a
    	table defintion */
    EXPORT CreateTableStatement(TableDef* tableDef);

    EXPORT virtual ~CreateTableStatement();

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream& os) const;

    TableDef* fTableDef; ///< The table defintion.

};


/**
 * @brief The subforms of alter table are represented as
 * subclasses of AlterTableAction
 *
 * SQL-92 specifies that an alter_table_statement has exactly one
 * alter_table_action.  But many vendors support aggregating
 * alter_table_actions under one alter_table_statement.  We
 * support that.  The subforms of alter table are represented as
 * subclasses of AlterTableAction, all of which are named
 * according to the convention AtaFoo, where Ata stands for
 * AlterTableAction.
 */
struct AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs)=0;

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs)=0;

    /** @brief Ctor for deserialization */
    AlterTableAction()
    {}

    virtual ~AlterTableAction()
    {}

    /** @brief QualifiedName of the focal table for this
    	statement. */
    //		QualifiedName *fTableName;

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream& os) const;
};




/** @brief Represents alter table add column forms.
 */
struct AtaAddColumn : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
	AtaAddColumn() : fColumnDef(0) {}

    /** @brief You can't add a column without specifying a column
    	definition. */
    AtaAddColumn(ColumnDef *columnDef);

    virtual ~AtaAddColumn();

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    /** @brief The focal column definition. */
    ColumnDef *fColumnDef;
};


/** @brief Represents the table_element_list style of add column
	which is not part of SQL-92.
 */
struct AtaAddColumns : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    AtaAddColumns()
    {}

    AtaAddColumns(TableElementList *tableElements);

    virtual ~AtaAddColumns();

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    ColumnDefList fColumns;
};


/** @brief Represents the table_element_list style of drop column
	which is not part of SQL-92.
 */
struct AtaDropColumns : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    AtaDropColumns()
    {}

    EXPORT AtaDropColumns(ColumnNameList *tableElements);

    EXPORT virtual ~AtaDropColumns();

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream& os) const;

    ColumnNameList fColumns;
};



/** AtaAddTableConstraint
 */
struct AtaAddTableConstraint : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
	AtaAddTableConstraint() : fTableConstraint(0)
    {}

    AtaAddTableConstraint(TableConstraintDef *tableConstraint);

    virtual ~AtaAddTableConstraint();

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    TableConstraintDef *fTableConstraint;
};



/** @brief alter table drop column.
 */
struct AtaDropColumn : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    AtaDropColumn()
    {}

    /** @brief Ctor for parser construction */
    EXPORT AtaDropColumn(std::string columnName, DDL_REFERENTIAL_ACTION dropBehavior);

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream& os) const;

    virtual ~AtaDropColumn()
    {}
    std::string fColumnName;
    DDL_REFERENTIAL_ACTION fDropBehavior;
};



/** @brief alter table set column default */

struct AtaSetColumnDefault : AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

	AtaSetColumnDefault() : fDefaultValue(0) {}

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    virtual ~AtaSetColumnDefault();

    AtaSetColumnDefault(const char *colName, ColumnDefaultValue *defaultValue);

    std::string fColumnName;
    ColumnDefaultValue *fDefaultValue;

};



/** @brief alter table drop column default. */
struct AtaDropColumnDefault : AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    AtaDropColumnDefault()
    {}

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    virtual ~AtaDropColumnDefault()
    {}

    /** @brief Ctor for parser construction */
    AtaDropColumnDefault(const char *colName);

    std::string fColumnName;
};


/** @brief alter table drop table constraint. */
struct AtaDropTableConstraint : AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    /** @brief Ctor for deserialization */
    AtaDropTableConstraint()
    {}

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream& os) const;

    virtual ~AtaDropTableConstraint()
    {}

    AtaDropTableConstraint(const char *constraintName, DDL_REFERENTIAL_ACTION dropBehavior);

    std::string fConstraintName;
    DDL_REFERENTIAL_ACTION fDropBehavior;
};



/** alter table rename */
struct AtaRenameTable : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
	AtaRenameTable() : fQualifiedName(0) {}
    AtaRenameTable(QualifiedName *qualifiedName);

    /** @brief Dump to stdout. */
    std::ostream& put(std::ostream& os) const;

    virtual ~AtaRenameTable();

    QualifiedName *fQualifiedName;
};



/** @brief alter table modify column */
struct AtaModifyColumnType : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
	AtaModifyColumnType() : fColumnType(0) {}

    /** @brief Ctor for parser construction */
    AtaModifyColumnType(const char* name, ColumnType* columnType) :
            fColumnType(columnType),
            fName(name)
    {}

    AtaModifyColumnType(QualifiedName *qualifiedName);

    /** @brief Dump to stdout. */
    std::ostream& put(std::ostream& os) const;

    virtual ~AtaModifyColumnType();

    ColumnType* fColumnType;

    std::string fName;
};



/** @brief alter table rename column */
struct AtaRenameColumn : public AlterTableAction
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief Ctor for deserialization */
    AtaRenameColumn() : fNewType(0), fDefaultValue(0) { }

    AtaRenameColumn(const char* name, const char* newName, ColumnType* newType,  const char * comment=NULL) :
            fName(name),
            fNewName(newName),
            fNewType(newType)    
    {
		if (comment)
			fComment = comment;
		fDefaultValue = 0;
	}

	AtaRenameColumn(const char* name, const char* newName, ColumnType* newType, ColumnConstraintList *constraint_list,
              ColumnDefaultValue *defaultValue, const char * comment=NULL) :
            fName(name),
            fNewName(newName),
            fNewType(newType),
			fDefaultValue(defaultValue)
    {
		if (constraint_list)
			fConstraints = *constraint_list;
			
		//if (defaultValue)
		//{
			//fDefaultValue = defaultValue;
		//}
			
		if (comment)
			fComment = comment;
	}
	
    AtaRenameColumn(QualifiedName *qualifiedName);

    /** @brief Dump to stdout. */
    std::ostream& put(std::ostream& os) const;

    virtual ~AtaRenameColumn();

    std::string fName; ///< current column name
    std::string fNewName; ///< new column name
    ColumnType* fNewType;
	 /** @brief Zero or more constraints. */
    ColumnConstraintList fConstraints;

    /** @brief NULL if there was no DEFAULT clause */
    ColumnDefaultValue *fDefaultValue;
    std::string fComment;
};




/** @brief Stores the type information for a column. */

struct ColumnType
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

    /** @brief For deserialization. */
    ColumnType()
    {}

    friend std::ostream &operator<<(std::ostream& os, const ColumnType &ac);

    /** @brief This constructor is used by the parser to construct the
    	ColumnType when a precision/scale clause is encountered. */

    EXPORT ColumnType(int prec, int scale);

    /** @brief Used in cases where we don't need to create an
    	object until we have seen all it's parts. */

    EXPORT ColumnType(int type);

    /** @brief This constructor is used by Dharma interface to
        create a ColumnType object easily */

    //EXPORT ColumnType(int type, int length, int precision, int scale, int compressiontype, const char* autoIncrement, int64_t nextValue, bool withTimezone = false);

    virtual ~ColumnType()
    {}

    /** @brief Type code from DDL_DATATYPES */
    int fType;

    /** @brief Length of datatype in bytes */
    int fLength;

    /** @brief SQL precision. This is the number of digits in the representation. */
    int fPrecision;

    /** @brief SQL scale.  This is is the number of digits to the
    	right of the decimal point. */
    int fScale;

    /** @brief SQL "with timezone" specifier */
    bool fWithTimezone;
	
	int fCompressiontype;
	
	std::string fAutoincrement;
	
	uint64_t fNextvalue;
	
};



/** @brief A column constraint definition.
 *
 * Since we aren't supporting references constraint specifications
 * for columns, it seemed simpler to use a single column
 * constraint class instead of articulating the varieties among
 * several classes like we do with table constraints.
 */
struct ColumnConstraintDef : public SchemaObject
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


    ColumnConstraintDef()
    {}

    /** @brief Constructs as check constraint. */
    EXPORT ColumnConstraintDef(const char *check);


    /** @brief Constructs as other constraint. */
    EXPORT ColumnConstraintDef(DDL_CONSTRAINTS type);

    virtual ~ColumnConstraintDef()
    {}

    /** @brief Whether deferrable. */
    bool fDeferrable;

    /** @brief Immediate or defferred */
    DDL_CONSTRAINT_ATTRIBUTES fCheckTime;

    /** @brief Distinguish kinds of constraints */
    DDL_CONSTRAINTS fConstraintType;

    /** @brief Stores foo from check(foo) */
    std::string fCheck;
};




/** @brief Represents a columns default value. */
struct ColumnDefaultValue
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    ColumnDefaultValue()
    {}

    ColumnDefaultValue(const char *value);

    virtual ~ColumnDefaultValue()
    {}


    /** @brief Is NULL the default value? */
    bool fNull;

    /** @brief Specified default value as a string. */
    std::string fValue;
};



/** @brief Represents a column definition. */

struct ColumnDef : public SchemaObject
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


    /** @brief For deserialization. */
	ColumnDef() : fType(0) {}

    EXPORT virtual ~ColumnDef();

    /** @brief Parser ctor. */
    EXPORT ColumnDef(const char *name,
              ColumnType* type,
              ColumnConstraintList *constraint_list,
              ColumnDefaultValue *defaultValue, const char * comment=NULL);

    /** @brief ColumnDef ctor.
     * Convenient ctor for Dharma use */
    ColumnDef(const char *name,
              ColumnType* type,
              ColumnConstraintList constraints,
              ColumnDefaultValue *defaultValue = NULL, const char * comment=NULL) :
            SchemaObject(name),
            fType (type),
            fConstraints (constraints),
            fDefaultValue (defaultValue)		
    {}

    void convertDecimal();

    /** @brief Never NULL since all Columns must have a type. */
    ColumnType* fType;

    /** @brief Zero or more constraints. */
    ColumnConstraintList fConstraints;

    /** @brief NULL if there was no DEFAULT clause */
    ColumnDefaultValue *fDefaultValue;
	
	std::string fComment;
};



/** @brief Abstract base for table constraint definitions. */

struct TableConstraintDef : public SchemaObject
{
    /** @brief Return DDL_SERIAL code */
    virtual DDL_SERIAL_TYPE getSerialType()=0;

    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs)=0;

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs)=0;


    TableConstraintDef();

    TableConstraintDef(DDL_CONSTRAINTS cType);

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream &os) const;

    virtual ~TableConstraintDef()
    {}
    //		std::string fName;
    DDL_CONSTRAINTS fConstraintType;
};




/** @brief Unique table constraint. */

struct TableUniqueConstraintDef : public TableConstraintDef
{
    /** @brief Return DDL_SERIAL code */
    virtual DDL_SERIAL_TYPE getSerialType()
    {
        return DDL_TABLE_UNIQUE_CONSTRAINT_DEF;
    }


    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    TableUniqueConstraintDef() : TableConstraintDef(DDL_UNIQUE)
    {}

    TableUniqueConstraintDef(ColumnNameList *columns);
    virtual ~TableUniqueConstraintDef()
    {}

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream &os) const;

    ColumnNameList fColumnNameList;
};



/** @brief Primary key table constraint.
 */
struct TablePrimaryKeyConstraintDef : public TableConstraintDef
{
    /** @brief Return DDL_SERIAL code */
    virtual DDL_SERIAL_TYPE getSerialType()
    {
        return DDL_TABLE_PRIMARY_CONSTRAINT_DEF;
    }

    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


    TablePrimaryKeyConstraintDef() : TableConstraintDef(DDL_PRIMARY_KEY)
    {}

    EXPORT TablePrimaryKeyConstraintDef(ColumnNameList *columns);

    virtual ~TablePrimaryKeyConstraintDef()
    {}

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream &os) const;

    ColumnNameList fColumnNameList;
};



/** @brief ReferentialAction specifies what to do about table
	relationships when elements are updated and deleted.
 */
struct ReferentialAction
{
    virtual ~ReferentialAction()
    {}

    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    DDL_REFERENTIAL_ACTION fOnUpdate;
    DDL_REFERENTIAL_ACTION fOnDelete;
};



/** @brief TableReferencesConstraintDef represents a foreign key constraint.
 */
struct TableReferencesConstraintDef : public TableConstraintDef
{
    /** @brief Return DDL_SERIAL code */
    virtual DDL_SERIAL_TYPE getSerialType()
    {
        return DDL_TABLE_REFERENCES_CONSTRAINT_DEF;
    }

    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    TableReferencesConstraintDef() :
		TableConstraintDef(DDL_REFERENCES),
		fTableName(0),
		fRefAction(0)
		{}

    TableReferencesConstraintDef
    (ColumnNameList *columns,
     QualifiedName *fTableName,
     ColumnNameList *foreignColumns,
     DDL_MATCH_TYPE matchType,
     ReferentialAction *refAction);

    virtual ~TableReferencesConstraintDef();

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream &os) const;

    ColumnNameList fColumns;
    QualifiedName *fTableName;
    ColumnNameList fForeignColumns;
    DDL_MATCH_TYPE fMatchType;
    ReferentialAction *fRefAction;
};



/** @brief Table check constraint.
 */
struct TableCheckConstraintDef : public TableConstraintDef
{
    /** @brief Return DDL_SERIAL code */
    virtual DDL_SERIAL_TYPE getSerialType()
    {
        return DDL_TABLE_CHECK_CONSTRAINT_DEF;
    }

    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    TableCheckConstraintDef() : TableConstraintDef(DDL_CHECK)
    {}

    TableCheckConstraintDef(const char *check);

    /** @brief Dump to stdout. */
    virtual std::ostream& put(std::ostream &os) const;

    virtual ~TableCheckConstraintDef()
    {}
    std::string fCheck;
};



/** TableDef represents a table definition.
 */
struct TableDef : public SchemaObject
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


	TableDef() : fQualifiedName(0)
    {}

    EXPORT TableDef(QualifiedName* name, TableElementList* elements, TableOptionMap* options);

    /** @brief TableDef ctor.
    * Convenient ctor for Dharma use 
    */
    TableDef( QualifiedName *name,
              ColumnDefList columns,
              TableConstraintDefList constraints, int tableWithAutoinc) :
            fQualifiedName (name),
            fColumns (columns),
            fConstraints (constraints)
			
			
    {}

    EXPORT virtual ~TableDef();

    QualifiedName* fQualifiedName;

    ColumnDefList fColumns;
    TableConstraintDefList fConstraints;

    TableOptionMap fOptions;
};



/** @brief Stores catalog, schema, object names.
 *
 * ddl.y does not yet support catalog.  So, expect catalog
 * qualified names to fail parsing for the moment.
 */
struct QualifiedName
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


    QualifiedName()
    {}

    EXPORT QualifiedName(const char* name);
    EXPORT QualifiedName(const char* name, const char* schema);
    EXPORT QualifiedName(const char* name, const char* schema, const char* catalog);

    virtual ~QualifiedName()
    {}

    std::string fCatalog;
    std::string fName;
    std::string fSchema;
};



/** @brief Represents the alter table command.
 *
 * All forms of alter_table_statements are represented as
 * subclasses of this.
 */

struct AlterTableStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


	AlterTableStatement() : fTableName(0)
    {}

    EXPORT AlterTableStatement(QualifiedName *qName, AlterTableActionList *ataList);

    /** @brief Dump to stdout. */
    EXPORT virtual std::ostream& put(std::ostream &os) const;

    /** @brief Delete members. */
    EXPORT virtual ~AlterTableStatement();

    QualifiedName* fTableName;
    AlterTableActionList fActions;
};



/** @brief This is used during parsing when constraint attributes
	are recognized.  This is always before the parent column def
	is recognized.  That's why we need a separate structure during
	parsing.  When the column def is recognized, we'll just copy
	these into the Column structure.
*/
struct ConstraintAttributes
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


    ConstraintAttributes()
    {}

    ConstraintAttributes(DDL_CONSTRAINT_ATTRIBUTES checkTime, bool deferrable) :
            fCheckTime(checkTime),
            fDeferrable(deferrable)
    {}

    virtual ~ConstraintAttributes()
    {}


    DDL_CONSTRAINT_ATTRIBUTES fCheckTime;
    bool fDeferrable;
};

/** @brief CreateIndex represents the CreateIndex operation.
 *
 * @note This currently takes ownership of the objects assigned to
 * fIndexName & fTableName.
 */
struct CreateIndexStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);

    CreateIndexStatement();
    CreateIndexStatement(QualifiedName *qualifiedName1, QualifiedName *qualifiedName2,
                         ColumnNameList *columnNames, bool unique);

    /** @brief Dump to stdout. */
    std::ostream& put(std::ostream& os) const;

    virtual ~CreateIndexStatement();

    QualifiedName *fIndexName;
    QualifiedName *fTableName;
    ColumnNameList fColumnNames;
    bool fUnique;
};

/** @brief DropIndexStatement represents the drop index operation. */

struct DropIndexStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    virtual int serialize(messageqcpp::ByteStream& bs);


	DropIndexStatement() : fIndexName(0)
    {}
    DropIndexStatement(QualifiedName *qualifiedName);

    /** @brief Dump to stdout. */
    std::ostream& put(std::ostream& os) const;

    virtual ~DropIndexStatement();

    QualifiedName *fIndexName;
};

/** @brief DropTableStatement represents the drop table operation
 */
struct DropTableStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


	DropTableStatement() : fTableName(0)
    {}
    EXPORT DropTableStatement(QualifiedName *qualifiedName, bool cascade);

    /** @brief Dump to stdout. */
    EXPORT std::ostream& put(std::ostream& os) const;

    virtual ~DropTableStatement()
    {
        delete fTableName;
    }
    QualifiedName *fTableName;
    bool fCascade;
};

/** @brief TruncTableStatement represents the drop table operation
 */
struct TruncTableStatement : public SqlStatement
{
    /** @brief Deserialize from ByteStream */
    EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

    /** @brief Serialize to ByteStream */
    EXPORT virtual int serialize(messageqcpp::ByteStream& bs);


	TruncTableStatement() : fTableName(0)
    {}
    EXPORT TruncTableStatement(QualifiedName *qualifiedName);

    /** @brief Dump to stdout. */
    EXPORT std::ostream& put(std::ostream& os) const;

    virtual ~TruncTableStatement()
    {
        delete fTableName;
    }
    QualifiedName *fTableName;
};
/** @brief Represents the mark partition out of service statement
 *
 */
struct MarkPartitionStatement : public SqlStatement
{
	/** @brief Deserialize from ByteStream */
	EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

	/** @brief Serialize to ByteStream */
	EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

	/** @brief Ctor for deserialization */
	MarkPartitionStatement() : fTableName(0)
	{}

	/** @brief You can't have a CreateTableStatement without a table defintion */
	EXPORT MarkPartitionStatement(QualifiedName *qualifiedName);

	/** @brief Dump to stdout. */
	EXPORT virtual std::ostream& put(std::ostream& os) const;

	virtual ~MarkPartitionStatement()
	{
		delete fTableName;
	}

	QualifiedName *fTableName; ///< The table defintion
	std::set<BRM::LogicalPartition> fPartitions; // partition numbers
};

/** @brief Represents the mark partition out of service statement
 *
 */
struct RestorePartitionStatement : public SqlStatement
{
	/** @brief Deserialize from ByteStream */
	EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

	/** @brief Serialize to ByteStream */
	EXPORT virtual int serialize(messageqcpp::ByteStream& bs);
	
	/** @brief Ctor for deserialization */
	RestorePartitionStatement() : fTableName(0)
	{}

	EXPORT RestorePartitionStatement(QualifiedName *qualifiedName);
	
	/** @brief Dump to stdout. */
	EXPORT virtual std::ostream& put(std::ostream& os) const;

	virtual ~RestorePartitionStatement()
	{
		delete fTableName;
	}

	QualifiedName *fTableName; ///< The table name.
	std::set<BRM::LogicalPartition> fPartitions; // partition numbers
};

/** @brief Represents the mark partition out of service statement
 *
 */
struct DropPartitionStatement : public SqlStatement
{
	/** @brief Deserialize from ByteStream */
	EXPORT virtual int unserialize(messageqcpp::ByteStream& bs);

	/** @brief Serialize to ByteStream */
	EXPORT virtual int serialize(messageqcpp::ByteStream& bs);

	/** @brief Ctor for deserialization */
	DropPartitionStatement() : fTableName(0)
	{}

	EXPORT DropPartitionStatement(QualifiedName *qualifiedName);

	/** @brief Dump to stdout. */
	EXPORT virtual std::ostream& put(std::ostream& os) const;

	virtual ~DropPartitionStatement()
	{
		delete fTableName;
	}

	QualifiedName *fTableName; ///< The table name.
	std::set<BRM::LogicalPartition> fPartitions; // partition numbers
};

}

#undef EXPORT

#endif
