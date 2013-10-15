/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
 *   $Id: dmlpkg.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef DMLPKG_H
#define DMLPKG_H

#include <vector>
#include <string>
#include <map>
#include <utility>
#include <iostream>
#include <stdint.h>

namespace dmlpackage
{

class DeleteSqlStatement;
class UpdateSqlStatement;
class InsertSqlStatement;
class SqlStatement;
class SqlStatementList;
class TableName;
class ValuesOrQuery;
class QuerySpec;
class TableExpression;
class WhereClause;
class SearchCondition;
class ExistanceTestPredicate;
class AllOrAnyPredicate;
class InPredicate;
class NullTestPredicate;
class LikePredicate;
class BetweenPredicate;
class ComparisonPredicate;
class Predicate;
class FromClause;
class SelectFilter;
class GroupByClause;
class HavingClause;
class Escape;
class ColumnAssignment;

typedef std::vector<TableName*> TableNameList;
typedef std::vector<ColumnAssignment*> ColumnAssignmentList;
typedef std::vector<std::string> ColumnNameList;
typedef std::vector<std::string> ValuesList;
typedef std::vector<std::string> AtomList;

typedef std::vector<char*> QueryBuffer;

typedef std::vector<std::string> ColValuesList;
typedef std::vector<std::string> ColNameList;
typedef std::map<uint32_t, ColValuesList> TableValuesMap;

std::ostream& operator<<(std::ostream& os, const SqlStatementList& ct);
std::ostream& operator<<(std::ostream& os, const SqlStatement& stmt);

/** @brief Predicate types
 */
enum PREDICATE_TYPE
{
    COMPARE_PREDICATE,
    BETWEEN_PREDICATE,
    LIKE_PREDICATE,
    NULLTEST_PREDICATE,
    IN_PREDICATE,
    ALLORANY_PREDICATE,
    EXIST_PREDICATE,
    INVALID_PREDICATE
};

/** @brief DML Statement types
 */
enum DML_TYPE
{
    DML_INSERT,
    DML_UPDATE,
    DML_DELETE,
    DML_COMMAND,
    DML_INVALID_TYPE
};

/** @brief SqlStatement represents a toplevel
 * syntactic element such as a insert, update or delete SQL
 * statement.
 *
 * SqlStatements are containers for the various structures
 * manufactured by the parsing process for a single SQL
 * statement.
 */
class SqlStatement
{
public:
    /** @brief ctor
     */
    SqlStatement();

    /** @brief dtor
     */
    virtual ~SqlStatement();

    /** @brief dump to stdout.
     */
    virtual std::ostream& put(std::ostream &os) const = 0;

    /** @brief get the query string associated with the
     * SqlStatement
     */
    virtual std::string getQueryString() const = 0;

    /** @brief get the statement type
     */
    virtual int getStatementType() const = 0;

    /** @brief get the schema name from the
     * TableName data member
     */
    virtual std::string getSchemaName() const;

    /** @brief get the table name from the
     * TableName data member
     */
    virtual std::string getTableName() const;

    TableName* fNamePtr;

};

/** @brief Collects SqlStatements so that we can support the
 * parsing of sqltext containing multiple statements.
 *
 * The SqlParser also accepts empty statements (a mixture of
 * whitespace and semicolons) in which case the result can be a
 * SqlStatementList of zero items.
 */
class SqlStatementList
{
public:
    /** @brief ctor
     */
    SqlStatementList()
    {}

    /** @brief dtor
     */
    virtual ~SqlStatementList();

    /** @brief get the SqlStatement at the given index
     *
     * @param i the index
     */
    SqlStatement* operator[](int i) const
    {
        return fList[i];
    }

    /** @brief push the supplied SqlStatement pointer onto the list
     *
     * @param v a pointer to a SqlStatement
     */
    void push_back(SqlStatement* v);

    std::vector<SqlStatement*> fList;
    std::string fSqlText;

private:
    SqlStatementList(const SqlStatementList& x);

};

/** @brief The representation of a parsed
 *  INSERT statement
 *
 *  insert_statement:
 *	    INSERT INTO table opt_column_commalist values_or_query_spec
 */
class InsertSqlStatement : public SqlStatement
{
public:
    /** @brief ctor
     */
    InsertSqlStatement();

    /** @brief ctor
     *
     * @param tableNamePtr pointer to a TableName object
     * @param valsOrQueryPtr pointer to a ValuesOrQueryObject
     */
    InsertSqlStatement(TableName* tableNamePtr, ValuesOrQuery* valsOrQueryPtr);

    /** @brief ctor
     *
     * @param tableNamePtr pointer to a TableName object
     * @param columnNamesPtr pointer to ColumnNamesList object
     * @param valsOrQueryPtr pointer to ValuesOrQueryObject
     */
    InsertSqlStatement(TableName* tableNamePtr, ColumnNameList* columnNamesPtr,
                       ValuesOrQuery* valsOrQueryPtr);

    /** @brief dtor
     */
    virtual ~InsertSqlStatement();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get a string representation of the query spec
     */
    virtual std::string getQueryString() const;

    /** @brief get the statement type - DML_INSERT
     */
    inline virtual int getStatementType() const { return DML_INSERT; }

    ValuesOrQuery* fValuesOrQueryPtr;
    ColumnNameList fColumnList;
};

/** @brief The representation of a parsed
 * UPDATE statement
 *
 *  update_statement_searched:
 *  	UPDATE table SET assignment_commalist opt_where_clause
 */
class UpdateSqlStatement : public SqlStatement
{
public:
    /** @brief ctor
     */
    UpdateSqlStatement();

    /** @brief ctor
     *
     * @param tableNamePtr pointer to a TableName object
     * @param colAssignmentPtr pointer to a ColumnAssignmentList object
     * @param whereClausePtr pointer to a WhereClause object - default 0
     */
    UpdateSqlStatement(TableName* tableNamePtr, ColumnAssignmentList* colAssignmentListPtr,
                       WhereClause* whereClausePtr = 0);

    /** @brief dtor
     */
    virtual ~UpdateSqlStatement();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the
     *  SET assignment_commalist opt_where_clause
     *  statement
     */
    virtual std::string getQueryString() const;

    /** @brief get the statement type - DML_UPDATE
     */
    inline virtual int getStatementType() const { return DML_UPDATE; }

    ColumnAssignmentList* fColAssignmentListPtr;
    WhereClause* fWhereClausePtr;
};

/** @brief The representation of a parsed
 * DELETE statement
 *
 *  delete_statement_searched:
 *  	DELETE FROM table opt_where_clause
 */
class DeleteSqlStatement : public SqlStatement
{
public:
    /** @brief ctor
     */
    DeleteSqlStatement();

    /** @brief ctor
     *
     * @param tableNamePtr pointer to a TableName object
     * @param whereClausePtr pointer to a WhereClause object - default = 0
     */
    DeleteSqlStatement(TableName* tableNamePtr, WhereClause* whereClausePtr = 0);

    /** @brief dtor
     */
    virtual ~DeleteSqlStatement();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the WHERE clause
     */
    virtual std::string getQueryString() const;

    /** @brief get the statement type - DML_DELETE
     */
    inline virtual int getStatementType() const { return DML_DELETE; }

    WhereClause* fWhereClausePtr;
};

/** @brief The representation of a parsed
 * COMMIT or ROLLBACK statement
 *
 *  commit_statement:
 *	   COMMIT WORK
 *   | COMMIT
 *
 *   rollback_statement:
 *     ROLLBACK WORK
`*   | ROLLBACK
 */
class CommandSqlStatement : public SqlStatement
{
public:
    /** @brief ctor
     *
     * @param command the COMMIT or ROLLBACK string
     */
    CommandSqlStatement(std::string command);

    /** @brief get the statement type - DML_COMMAND
     */
    inline virtual int getStatementType() const { return DML_COMMAND; }

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the COMMIT or ROLLBACK string
     */
    virtual std::string getQueryString() const;

    std::string fCommandText;
};

/** @brief Stores schema, object names.
 *
 */
class TableName
{
public:
    /** @brief ctor
     */
    TableName();

    /** @brief ctor
     *
     * @param name the table name
     */
    TableName(char* name);

    /** @brief ctor
     *
     * @param schema the schema name
     * @param name the table name
     */
    TableName(char* shema, char* name);

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    std::string fName;
    std::string fSchema;
};

/** @brief Stores a column assignment
 *
 *  assignment:
 * 	    column '=' scalar_exp
 *   |	column '=' NULLX
 */
class ColumnAssignment
{
public:
    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of
     * the column assignment
     */
    std::string getColumnAssignmentString() const;

    std::string fColumn;
    std::string fOperator;
    std::string fScalarExpression;
	bool fFromCol;
	uint32_t fFuncScale;
};

/** @brief Stores a value list or a query specification
 *
 *	 values_or_query_spec:
 * 	    VALUES '(' insert_atom_commalist ')'
 *    |	query_spec
 */
class ValuesOrQuery
{
public:
    /** @brief ctor
     */
    ValuesOrQuery();

    /** @brief ctor
     *
     * @param valuesPtr pointer to a ValuesList object
     */
    ValuesOrQuery(ValuesList* valuesPtr);

    /** @brief  ctor
     *
     * @param querySpecPtr pointer to a QuerySpec object
     */
    ValuesOrQuery(QuerySpec* querySpecPtr);

    /** @brief dtor
     */
    ~ValuesOrQuery();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string reperesentation of
     * the ValuesList or the QuerySpec
     */
    std::string getQueryString() const;

    ValuesList fValuesList;
    QuerySpec* fQuerySpecPtr;
};

/** @brief Stores a SELECT filter
 *
 *  selection:
 * 	   scalar_exp_commalist
 *   | '*'
 */
class SelectFilter
{
public:
    /** @brief ctor
     */
    SelectFilter();

    /** @brief ctor
     *
     * @param columnListPtr pointer to a ColumnNameList object
     */
    SelectFilter(ColumnNameList* columnListPtr);

    /** @brief dtor
     */
    ~SelectFilter();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string represntation of the SELECT statement
     */
    std::string getSelectString() const;

    ColumnNameList fColumnList;
};

/** @brief Stores a FROM clause
 *
 *  from_clause:
 *     FROM table_ref_commalist
 */
class FromClause
{
public:
    /** @brief ctor
     */
    FromClause();

    /** @brief ctor
     *
     * @param tableNameList pointer to a TableNameList object
     */
    FromClause(TableNameList* tableNameList);

    /** @brief dtor
     */
    ~FromClause();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the FROM clause
     */
    std::string getFromClauseString() const;

    TableNameList* fTableListPtr;
};

/** @brief Stores a WHERE clause
 *
 * where_clause:
 *  	WHERE search_condition
 */
class WhereClause
{
public:
    /** @brief ctor
     */
    WhereClause();

    /** @brief dtor
     */
    ~WhereClause();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the WHERE clause
     */
    std::string getWhereClauseString() const;

    SearchCondition* fSearchConditionPtr;

};

/** @brief Stores a GROUP BY clause
 *
 * opt_group_by_clause:
 *     empty
 *	  | GROUP BY column_ref_commalist
 */
class GroupByClause
{
public:
    /** @brief ctor
     */
    GroupByClause();

    /** @brief dtor
     */
    ~GroupByClause();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the GROUP BY clause
     */
    std::string getGroupByClauseString() const;

    ColumnNameList* fColumnNamesListPtr;
};

/** @brief Stores a HAVING clause
 *
 *  opt_having_clause:
 *	    empty
 *   |	HAVING search_condition
 */
class HavingClause
{
public:
    /** @brief ctor
     */
    HavingClause();

    /** @brief dtor
     */
    ~HavingClause();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the HAVING clause
     */
    std::string getHavingClauseString() const;

    SearchCondition* fSearchConditionPtr;
};

/** @brief Stores an ESCAPE sequence
 *
 *
 *  opt_escape:
 *    empty
 *  | ESCAPE atom
 */
class Escape
{
public:
    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    std::string fEscapeChar;
};

/** @brief The base class representaion of a parsed SQL predicate
 *
 *  predicate:
 * 	comparison_predicate
 *   |	between_predicate
 *   |	like_predicate
 *   |	test_for_null
 *   |	in_predicate
 *   |	all_or_any_predicate
 *   |	existence_test
 */
class Predicate
{
public:
    /** @brief ctor
     */
    Predicate();

    /** @brief ctor
     *
     * @param predicateType the PREDICATE_TYPE
     */
    Predicate(PREDICATE_TYPE predicateType);

    /** @brief dtor
     */
    virtual ~Predicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @param get the string representation of the predicate
     */
    virtual std::string getPredicateString() const;

    PREDICATE_TYPE fPredicateType;
};

/** @brief The representation of a parsed
 * Comparison Predicate:
 *
 *  comparison_predicate:
 * 	   scalar_exp COMPARISON scalar_exp
 *   | scalar_exp COMPARISON subquery
 */
class ComparisonPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    ComparisonPredicate();

    /** @brief dtor
     */
    ~ComparisonPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the COMPARISON
     * predicate
     */
    virtual std::string getPredicateString() const;

    std::string fLHScalarExpression;
    std::string fRHScalarExpression;

    std::string fOperator;

    QuerySpec* fSubQuerySpec;
};

/** @brief The representation of a parsed
 *  Between Predicate:
 *
 * between_predicate:
 * 	    scalar_exp NOT BETWEEN scalar_exp AND scalar_exp
 *   |	scalar_exp BETWEEN scalar_exp AND scalar_exp
 */
class BetweenPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    BetweenPredicate();

    /** @brief dtor
     */
    ~BetweenPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the BETWEEN
     *  predicate
     */
    virtual std::string getPredicateString() const;

    std::string fLHScalarExpression;
    std::string fRH1ScalarExpression;
    std::string fRH2ScalarExpression;

    std::string fOperator1;
    std::string fOperator2;
};

/** @brief The representation of a parsed
 * Like Predicate:
 *
 *  like_predicate:
 *  	scalar_exp NOT LIKE atom opt_escape
 *   |	scalar_exp LIKE atom opt_escape
 */
class LikePredicate : public Predicate
{
public:
    /** @brief ctor
     */
    LikePredicate();

    /** @brief dtor
     */
    ~LikePredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the LIKE
     * predicate
     */
    virtual std::string getPredicateString() const;

    std::string fLHScalarExpression;
    std::string fAtom;

    std::string fOperator;
    Escape* fOptionalEscapePtr;
};

/** @brief The representation of a parsed
 * Null test Predicate:
 *
 *  test_for_null:
 *  	column_ref IS NOT NULLX
 *   |	column_ref IS NULLX
 */
class NullTestPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    NullTestPredicate();

    /** @brief dtor
     */
    ~NullTestPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the NULL test
     * predicate
     */
    std::string getPredicateString() const;

    std::string  fColumnRef;

    std::string fOperator;
};

/** @brief The representation of a parsed
 * In Predicate:
 *
 *	in_predicate:
 *		scalar_exp NOT IN '(' subquery ')'
 *   |	scalar_exp IN '(' subquery ')'
 *   |	scalar_exp NOT IN '(' atom_commalist ')'
 *   |	scalar_exp IN '(' atom_commalist ')'
 */
class InPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    InPredicate();

    /** @brief dtor
     */
    ~InPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the IN
     * predicate
     */
    virtual std::string getPredicateString() const;

    std::string fScalarExpression;
    std::string fOperator;

    AtomList   fAtomList;
    QuerySpec* fSubQuerySpecPtr;
};

/** @brief The representation of a parsed
 *  All Or Any Predicate:
 *
 *	 all_or_any_predicate:
 *     scalar_exp COMPARISON any_all_some subquery
 */
class AllOrAnyPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    AllOrAnyPredicate();

    /** @brief dtor
     */
    ~AllOrAnyPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the
     * ALL or ANY predicate
     */
    virtual std::string getPredicateString() const;

    std::string fScalarExpression;
    std::string fOperator;
    std::string fAnyAllSome;

    QuerySpec* fSubQuerySpecPtr;
};

/** @brief The representation of a parsed
 * Existance test Predicate:
 *
 *  existence_test:
 *     EXISTS subquery
 */
class ExistanceTestPredicate : public Predicate
{
public:
    /** @brief ctor
     */
    ExistanceTestPredicate();

    /** @brief dtor
     */
    ~ExistanceTestPredicate();

    /** @brief dump to stdout
     */
    virtual std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the EXISTS
     * predicate
     */
    virtual std::string getPredicateString() const;

    QuerySpec* fSubQuerySpecPtr;
};

/** @brief The representation of a parsed
 *	Search Condition:
 *
 *  search_condition:
 *     |	search_condition OR search_condition
 *     |	search_condition AND search_condition
 *	   |	NOT search_condition
 *     |	'(' search_condition ')'
 *     |	predicate
 */
class SearchCondition
{
public:
    /** @brief ctor
     */
    SearchCondition();

    /** @brief dtor
     */
    ~SearchCondition();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the striong representation of the
     * search condition
     */
    std::string getSearchConditionString() const;

    Predicate* fPredicatePtr;
    SearchCondition* fLHSearchConditionPtr;
    SearchCondition* fRHSearchConditionPtr;

    std::string fOperator;
};

/** @brief The representation of a parsed
 * Table Expression:
 *
 *   table_exp:
 *   	from_clause
 *  	opt_where_clause
 *  	opt_group_by_clause
 * 	    opt_having_clause
 */
class TableExpression
{
public:
    /** @brief ctor
     */
    TableExpression();

    /** @brief ctor
     *
     * @param fromClausePtr pointer to a FromClause object
     * @param whereClausePtr pointer to a WhereClause object
     * @param groupByPtr pointer to a GroupByClause object
     * @param havingPtr pointer to a HavingClause object
     */
    TableExpression(FromClause* fromClausePtr, WhereClause* whereClausePtr,
                    GroupByClause* groupByPtr, HavingClause* havingPtr);

    /** @brief dtor
     */
    ~TableExpression();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the
     * table expression
     */
    std::string getTableExpressionString() const;

    FromClause* fFromClausePtr;
    WhereClause* fWhereClausePtr;
    GroupByClause* fGroupByPtr;
    HavingClause* fHavingPtr;
};

/** @brief The representation of a parsed
 * Query Specification:
 *
 * query_spec:
 * 	  SELECT opt_all_distinct selection table_exp
 */
class QuerySpec
{
public:
    /** @brief ctor
     */
    QuerySpec();

    /** @brief ctor
     *
     * @param selectFilter pointer to a SelectFilter object
     * @param tableExpression pointer to a TableExpression object
     */
    QuerySpec(SelectFilter* selectFilter, TableExpression* tableExpression);

    /** @brief ctor
     *
     * @param selectFilter pointer to a SelectFilter object
     * @param tableExpression pointer to a TableExpression object
     * @param allOrDistinct pointer to a ALL or DISTINCT string
     */
    QuerySpec(SelectFilter* selectFilter, TableExpression* tableExpression,
              char* allOrDistinct);

    /** @brief dtor
     */
    ~QuerySpec();

    /** @brief dump to stdout
     */
    std::ostream& put(std::ostream &os) const;

    /** @brief get the string representation of the
     * query specification
     */
    std::string  getQueryString() const;

    SelectFilter* fSelectFilterPtr;
    TableExpression* fTableExpressionPtr;

    std::string fOptionAllOrDistinct;
};

}                                                 // namespace dmlpackage
#endif                                            // DMLPKG_H
