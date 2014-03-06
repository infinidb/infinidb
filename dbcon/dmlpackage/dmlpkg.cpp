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
 *   $Id: dmlpkg.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include "dmlpkg.h"

namespace dmlpackage
{
    using namespace std;

    /** SqlStatementList
      */
    ostream &operator<<(ostream& os, const SqlStatementList &ssl)
    {
        vector<SqlStatement*>::const_iterator itr;

        for(itr = ssl.fList.begin(); itr != ssl.fList.end(); ++itr)
        {
            SqlStatement &stmt = **itr;
            os << stmt;
        }
        return os;
    }

    void SqlStatementList::push_back(SqlStatement* v)
    {
        fList.push_back(v);
    }

    SqlStatementList::~SqlStatementList()
    {
        vector<SqlStatement*>::iterator itr;
        for(itr = fList.begin(); itr != fList.end(); ++itr)
        {
            delete *itr;
        }
    }

	/** SqlStatement
      */
    SqlStatement::SqlStatement()
        :fNamePtr(0)
    {

    }

    SqlStatement::~SqlStatement()
    {
        if (0 != fNamePtr)
            delete fNamePtr;
    }

    ostream& operator<<(ostream &os, const SqlStatement& stmt)
    {
        return stmt.put(os);
    }

    std::string SqlStatement::getSchemaName() const
    {
        std::string schema_name;

        if (0 != fNamePtr)
            schema_name = fNamePtr->fSchema;

        return schema_name;
    }

    std::string SqlStatement::getTableName() const
    {
        std::string table_name;

        if (0 != fNamePtr)
            table_name = fNamePtr->fName;

        return table_name;
    }

    /** InsertSqlStatement
     */
    InsertSqlStatement::InsertSqlStatement()
        :fValuesOrQueryPtr(0)
    {

    }

    InsertSqlStatement::InsertSqlStatement(TableName* tableNamePtr,  ValuesOrQuery* valsOrQueryPtr)
        :fValuesOrQueryPtr(valsOrQueryPtr)
    {
        fNamePtr = tableNamePtr;
    }

    InsertSqlStatement::InsertSqlStatement(TableName* tableNamePtr, ColumnNameList* columnNamesPtr,
        ValuesOrQuery* valsOrQueryPtr)
        :fValuesOrQueryPtr(valsOrQueryPtr)
    {
        fNamePtr = tableNamePtr;
        fColumnList = *columnNamesPtr;
        delete columnNamesPtr;
    }

    InsertSqlStatement::~InsertSqlStatement()
    {
        if (0 != fValuesOrQueryPtr)
            delete fValuesOrQueryPtr;
    }

    ostream& InsertSqlStatement::put(ostream& os) const
    {
        os << "Insert " << endl;

        if (0 != fNamePtr )
        {
            fNamePtr->put(os);
        }

        ColumnNameList::const_iterator itr;
        for(itr = fColumnList.begin(); itr != fColumnList.end(); ++itr)
        {
            os << *itr << endl;
        }

        if (0 != fValuesOrQueryPtr )
        {
            fValuesOrQueryPtr->put(os);

        }
        return os;
    }

    string InsertSqlStatement::getQueryString() const
    {
        std::string query_string;

        if (0 != fValuesOrQueryPtr)
        {
            query_string = fValuesOrQueryPtr->getQueryString();
        }

        return query_string;
    }

    /** UpdateSqlStatement
     */
    UpdateSqlStatement::UpdateSqlStatement()
        :fColAssignmentListPtr(0), fWhereClausePtr(0)
    {

    }

    UpdateSqlStatement::UpdateSqlStatement(TableName* tableNamePtr,
        ColumnAssignmentList* colAssignmentListPtr, WhereClause* whereClausePtr /*=0*/)
        :fColAssignmentListPtr(colAssignmentListPtr),
        fWhereClausePtr(whereClausePtr)
    {
        fNamePtr = tableNamePtr;
    }

    UpdateSqlStatement::~UpdateSqlStatement()
    {
        if (0 != fColAssignmentListPtr)
        {
            ColumnAssignmentList::iterator iter = fColAssignmentListPtr->begin();
            while (iter != fColAssignmentListPtr->end())
            {
                delete *iter;
                ++iter;
            }
            fColAssignmentListPtr->clear();
            delete fColAssignmentListPtr;
        }
        if (0 != fWhereClausePtr)
        {
            delete fWhereClausePtr;
        }
    }

    ostream& UpdateSqlStatement::put(ostream& os) const
    {
        os << "Update " << endl;
        if (0 != fNamePtr)
        {
            fNamePtr->put(os);
        }
        if (0 != fColAssignmentListPtr)
        {
            os << "SET " << endl;
            ColumnAssignmentList::const_iterator iter = fColAssignmentListPtr->begin();
            while (iter != fColAssignmentListPtr->end())
            {
                ColumnAssignment *cola = *iter;
                cola->put(os);
                ++iter;
            }
        }
        if (0 != fWhereClausePtr)
        {
            fWhereClausePtr->put(os);
        }
        return os;
    }

    string UpdateSqlStatement::getQueryString() const
    {
        std::string query_string;

        if (0 != fColAssignmentListPtr)
        {
            query_string += "SET ";
            ColumnAssignmentList::const_iterator iter = fColAssignmentListPtr->begin();
            while (iter != fColAssignmentListPtr->end())
            {
                ColumnAssignment *cola = *iter;
                query_string += cola->getColumnAssignmentString();

                ++iter;
                if (iter != fColAssignmentListPtr->end())
                    query_string += ",";
            }
        }
        if (0 != fWhereClausePtr)
        {
            query_string += " ";
            query_string += fWhereClausePtr->getWhereClauseString();
        }

        return query_string;
    }

    /** DeleteSqlStatement
     */
    DeleteSqlStatement::DeleteSqlStatement()
        :fWhereClausePtr(0)
    {

    }

    DeleteSqlStatement::DeleteSqlStatement(TableName* tableNamePtr, WhereClause* whereClausePtr /*=0*/)
        :fWhereClausePtr(whereClausePtr)
    {
        fNamePtr = tableNamePtr;

    }

    DeleteSqlStatement::~DeleteSqlStatement()
    {
        if (0 != fWhereClausePtr)
        {
            delete fWhereClausePtr;
        }

    }

    ostream& DeleteSqlStatement::put(ostream& os) const
    {
        os << "Delete " << endl;

        if (0 != fNamePtr)
        {
            fNamePtr->put(os);
        }
        if (0 != fWhereClausePtr)
        {
            fWhereClausePtr->put(os);
        }
        return os;
    }

    string DeleteSqlStatement::getQueryString() const
    {
        std::string query_string;

        if (0 != fWhereClausePtr)
        {
            query_string += fWhereClausePtr->getWhereClauseString();
        }

        return query_string;
    }

    /** CommandSqlStatement
     */
    CommandSqlStatement::CommandSqlStatement(std::string command)
        :fCommandText(command)
    {

    }

    ostream& CommandSqlStatement::put(ostream& os) const
    {
        os << fCommandText << endl;

        return os;
    }

    string CommandSqlStatement::getQueryString() const
    {
        return fCommandText;
    }

    /** TableName
     */
    TableName::TableName()
    {

    }

    TableName::TableName(char* name)
    {
        fName = name;

    }

    TableName::TableName(char* schema, char* name)
    {
        fSchema = schema;
        fName = name;
    }

    ostream& TableName::put(ostream& os) const
    {
        if (fSchema != "")
            os << fSchema << ".";
        os << fName << endl;
        return os;
    }

    /** ColumnAssignment
     */
    ostream& ColumnAssignment::put(ostream& os) const
    {
        os << fColumn << endl;
        os << fOperator << endl;
        os << fScalarExpression << endl;
        return os;
    }

    string ColumnAssignment::getColumnAssignmentString() const
    {
        std::string column_assignment = fColumn;
        column_assignment += " ";
        column_assignment += fOperator;
        column_assignment += " ";
        column_assignment += fScalarExpression;

        return column_assignment;
    }

    /** ValuesOrQuery
     */
    ValuesOrQuery::ValuesOrQuery()
        :fQuerySpecPtr(0)
    {
    }

    ValuesOrQuery::ValuesOrQuery(ValuesList* valuesPtr)
        :fQuerySpecPtr(0)
    {
        fValuesList = *valuesPtr;
        delete valuesPtr;
    }

    ValuesOrQuery::ValuesOrQuery(QuerySpec* querySpecPtr)
    {
        fQuerySpecPtr = querySpecPtr;
    }

    ValuesOrQuery::~ValuesOrQuery()
    {
        if (0 != fQuerySpecPtr)
            delete fQuerySpecPtr;
    }

    ostream& ValuesOrQuery::put(ostream& os) const
    {
        ValuesList::const_iterator iter = fValuesList.begin();
        while ( iter != fValuesList.end() )
        {
            os << *iter << endl;
            ++iter;
        }

        if (0 != fQuerySpecPtr)
        {
            fQuerySpecPtr->put(os);
        }

        return os;
    }

    string ValuesOrQuery::getQueryString() const
    {
        std::string query_string;

        if (0 != fQuerySpecPtr)
        {
            query_string = fQuerySpecPtr->getQueryString();
        }

        return query_string;
    }

    /** QuerySpec
     */
    QuerySpec::QuerySpec()
        :fSelectFilterPtr(0), fTableExpressionPtr(0)
    {
    }

    QuerySpec::QuerySpec(SelectFilter* selectFilter, TableExpression* tableExpression)
        :fSelectFilterPtr(selectFilter), fTableExpressionPtr(tableExpression)
    {

    }

    QuerySpec::QuerySpec(SelectFilter* selectFilter, TableExpression* tableExpression,
        char* allOrDistinct)
        :fSelectFilterPtr(selectFilter), fTableExpressionPtr(tableExpression)
    {
        fOptionAllOrDistinct = allOrDistinct;
    }

    QuerySpec::~QuerySpec()
    {
        if (0 != fSelectFilterPtr)
            delete fSelectFilterPtr;
        if (0 != fTableExpressionPtr)
            delete fTableExpressionPtr;
    }

    ostream& QuerySpec::put(ostream& os) const
    {
        if (0 != fSelectFilterPtr)
        {
            fSelectFilterPtr->put(os);
        }
        if (0 != fTableExpressionPtr)
        {
            fTableExpressionPtr->put(os);
        }

        if (fOptionAllOrDistinct != "")
        {
            os << fOptionAllOrDistinct << endl;
        }

        return os;
    }

    string QuerySpec::getQueryString() const
    {
        std::string query_string;
        if (0 != fSelectFilterPtr)
        {
            query_string += fSelectFilterPtr->getSelectString();
        }
        if (0 != fTableExpressionPtr)
        {
            query_string += " ";
            query_string += fTableExpressionPtr->getTableExpressionString();
        }
        if (fOptionAllOrDistinct != "")
        {
            query_string += " ";
            query_string += fOptionAllOrDistinct;
        }

        return query_string;
    }

    /** SelectFilter
     */
    SelectFilter::SelectFilter()
        :fColumnList(0)
    {
    }

    SelectFilter::SelectFilter(ColumnNameList* columnListPtr)
    {
        fColumnList = *columnListPtr;

        delete columnListPtr;
    }

    SelectFilter::~SelectFilter()
    {

    }

    ostream& SelectFilter::put(ostream& os) const
    {
        os << "SELECT" << endl;
        ColumnNameList::const_iterator iter = fColumnList.begin();
        while ( iter != fColumnList.end() )
        {
            os << *iter << endl;
            ++iter;
        }
        if (0 == fColumnList.size())
            os << "*" << endl;
        return os;
    }

    string SelectFilter::getSelectString() const
    {
        std::string select_filter = "SELECT ";
        ColumnNameList::const_iterator iter = fColumnList.begin();
        while ( iter != fColumnList.end() )
        {
            select_filter += *iter;
            ++iter;
            if (iter != fColumnList.end())
                select_filter += ",";
        }
        if (0 == fColumnList.size())
            select_filter += "*";

        return select_filter;
    }

    /** TableExpression
     */
    TableExpression::TableExpression()
        :fFromClausePtr(0), fWhereClausePtr(0),
        fGroupByPtr(0), fHavingPtr(0)
    {

    }

    TableExpression::TableExpression(FromClause* fromClausePtr, WhereClause* whereClausePtr,
        GroupByClause* groupByPtr, HavingClause* havingPtr)
        :fFromClausePtr(fromClausePtr), fWhereClausePtr(whereClausePtr),
        fGroupByPtr(groupByPtr), fHavingPtr(havingPtr)
    {

    }

    TableExpression::~TableExpression()
    {
        if (0 != fFromClausePtr)
            delete fFromClausePtr;
        if (0 != fWhereClausePtr)
            delete fWhereClausePtr;
        if (0 != fGroupByPtr)
            delete fGroupByPtr;
        if (0 != fHavingPtr)
            delete fHavingPtr;

    }

    ostream& TableExpression::put(ostream& os) const
    {
        if (0 != fFromClausePtr)
        {
            fFromClausePtr->put(os);
        }
        if (0 != fWhereClausePtr)
        {
            fWhereClausePtr->put(os);
        }
        if (0 != fGroupByPtr)
        {
            fGroupByPtr->put(os);
        }
        if (0 != fHavingPtr)
        {
            fHavingPtr->put(os);
        }

        return os;
    }

    string TableExpression::getTableExpressionString() const
    {
        std::string table_expression;
        if (0 != fFromClausePtr)
        {
            table_expression += fFromClausePtr->getFromClauseString();
        }
        if (0 != fWhereClausePtr)
        {
            table_expression += " ";
            table_expression += fWhereClausePtr->getWhereClauseString();
        }
        if (0 != fGroupByPtr)
        {
            table_expression += " ";
            table_expression += fGroupByPtr->getGroupByClauseString();
        }
        if (0 != fHavingPtr)
        {
            table_expression += " ";
            table_expression += fHavingPtr->getHavingClauseString();
        }

        return table_expression;
    }

    /** FromClause
     */
    FromClause::FromClause()
        :fTableListPtr(0)
    {
    }

    FromClause::FromClause(TableNameList* tableNameList)
    {
        fTableListPtr = tableNameList;
    }

    FromClause::~FromClause()
    {
        if (0 != fTableListPtr)
        {
            TableNameList::iterator iter = fTableListPtr->begin();
            while ( iter != fTableListPtr->end() )
            {
                TableName* tableNamePtr = *iter;
                delete tableNamePtr;
                ++iter;
            }
            fTableListPtr->clear();
            delete fTableListPtr;
        }
    }

    ostream& FromClause::put(ostream& os) const
    {
        os << "FROM" << endl;

        if (0 != fTableListPtr)
        {
            TableNameList::const_iterator iter = fTableListPtr->begin();
            while ( iter != fTableListPtr->end() )
            {
                TableName* tableNamePtr = *iter;
                tableNamePtr->put(os);
                ++iter;
            }
        }
        return os;
    }

    string FromClause::getFromClauseString() const
    {
        std::string from_clause = "FROM ";
        if (0 != fTableListPtr)
        {
            TableNameList::const_iterator iter = fTableListPtr->begin();
            while (iter != fTableListPtr->end())
            {
                TableName* tableNamePtr = *iter;
                if (tableNamePtr->fSchema != "")
                {
                    from_clause += tableNamePtr->fSchema;
                    from_clause += ".";
                }
                from_clause += tableNamePtr->fName;

                ++iter;
                if (iter != fTableListPtr->end())
                    from_clause += ",";
            }
        }
        return from_clause;
    }

    /** WhereClause
     */
    WhereClause::WhereClause()
        :fSearchConditionPtr(0)
    {

    }

    WhereClause::~WhereClause()
    {
        if (0 != fSearchConditionPtr)
        {
            delete fSearchConditionPtr;
        }
    }

    ostream& WhereClause::put(ostream& os) const
    {
        os << "WHERE" << endl;
        if (0 != fSearchConditionPtr)
        {
            fSearchConditionPtr->put(os);
        }

        return os;
    }

    string WhereClause::getWhereClauseString() const
    {
        std::string where_clause = "WHERE";
        if (0 != fSearchConditionPtr)
        {
            where_clause += " ";
            where_clause += fSearchConditionPtr->getSearchConditionString();
        }
        return where_clause;
    }

    /** HavingClause
     */
    HavingClause::HavingClause()
        :fSearchConditionPtr(0)
    {
    }

    HavingClause::~HavingClause()
    {
        if (fSearchConditionPtr != 0)
        {
            delete fSearchConditionPtr;
        }
    }

    ostream& HavingClause::put(ostream& os) const
    {
        os << "HAVING" << endl;
        if (0 != fSearchConditionPtr)
        {
            fSearchConditionPtr->put(os);
        }
        return os;
    }

    string HavingClause::getHavingClauseString() const
    {
        std::string having_clause = "HAVING";
        if (0 != fSearchConditionPtr)
        {
            having_clause += " ";
            having_clause += fSearchConditionPtr->getSearchConditionString();
        }

        return having_clause;
    }

    /** GroupByClause
     */
    GroupByClause::GroupByClause()
        :fColumnNamesListPtr(0)
    {
    }

    GroupByClause::~GroupByClause()
    {
        if (0 != fColumnNamesListPtr)
        {
            fColumnNamesListPtr->clear();
            delete fColumnNamesListPtr;
        }
    }

    ostream& GroupByClause::put(ostream& os) const
    {
        os << "GROUP BY" << endl;
        if (0 != fColumnNamesListPtr)
        {
            ColumnNameList::const_iterator iter = fColumnNamesListPtr->begin();
            if (iter != fColumnNamesListPtr->end())
            {
                os << *iter << endl;
                ++iter;
            }
        }
        return os;
    }

    string GroupByClause::getGroupByClauseString() const
    {
        std::string group_by_clause = "GROUP BY ";
        if (0 != fColumnNamesListPtr)
        {
            ColumnNameList::const_iterator iter = fColumnNamesListPtr->begin();
            if (iter != fColumnNamesListPtr->end())
            {
                group_by_clause += *iter;
                ++iter;
                if (iter != fColumnNamesListPtr->end())
                    group_by_clause += ",";
            }
        }

        return group_by_clause;
    }

    /** Escape
     */
    ostream& Escape::put(ostream& os) const
    {
        os << "ESCAPE" << endl;

        os << fEscapeChar << endl;
        return os;
    }

    /** SearchCondition
     */
    SearchCondition::SearchCondition()
        :fPredicatePtr(0),fLHSearchConditionPtr(0),
        fRHSearchConditionPtr(0)
    {
    }

    SearchCondition::~SearchCondition()
    {
        if (0 != fPredicatePtr)
        {
            delete fPredicatePtr;
        }
        if (0 != fLHSearchConditionPtr)
        {
            delete fLHSearchConditionPtr;
        }
        if (0 != fRHSearchConditionPtr)
        {
            delete fRHSearchConditionPtr;
        }

    }

    ostream& SearchCondition::put(ostream& os) const
    {
        if (0 != fPredicatePtr)
        {
            fPredicatePtr->put(os);
        }

        if (0 != fLHSearchConditionPtr)
        {
            fLHSearchConditionPtr->put(os);
        }
        if (0 != fRHSearchConditionPtr)
        {
            os << fOperator << endl;
            fRHSearchConditionPtr->put(os);
        }

        return os;
    }

    string SearchCondition::getSearchConditionString() const
    {
        std::string search_condition;
        if (0 != fPredicatePtr)
        {
            search_condition += fPredicatePtr->getPredicateString();
        }
        if (0 != fLHSearchConditionPtr)
        {
            search_condition += fLHSearchConditionPtr->getSearchConditionString();
            search_condition += " ";
        }
        if (0 != fRHSearchConditionPtr)
        {
            search_condition += fOperator;
            search_condition += " ";
            search_condition += fRHSearchConditionPtr->getSearchConditionString();
        }

        return search_condition;
    }

    /** ExistanceTestPredicate
     */
    ExistanceTestPredicate::ExistanceTestPredicate()
        :Predicate(EXIST_PREDICATE),fSubQuerySpecPtr(0)
    {
    }

    ExistanceTestPredicate::~ExistanceTestPredicate()
    {
        if (0 != fSubQuerySpecPtr)
        {
            delete fSubQuerySpecPtr;
        }
    }

    ostream& ExistanceTestPredicate::put(ostream& os) const
    {
        //cout << "EXISTS" << endl;
        //cout << "("  << endl;
        if (0 != fSubQuerySpecPtr)
        {
            fSubQuerySpecPtr->put(os);
        }
        //cout << ")" << endl;
        return os;
    }

    string ExistanceTestPredicate::getPredicateString() const
    {
        std::string exists_predicate = "EXISTS";
        exists_predicate += "(";
        if (0 != fSubQuerySpecPtr)
        {
            exists_predicate += " ";
            exists_predicate += fSubQuerySpecPtr->getQueryString();
        }
        exists_predicate += ")";
        return exists_predicate;
    }

    /** AllOrAnyPredicate
     */
    AllOrAnyPredicate::AllOrAnyPredicate()
        :Predicate(ALLORANY_PREDICATE),fSubQuerySpecPtr(0)
    {

    }

    AllOrAnyPredicate::~AllOrAnyPredicate()
    {
        if (0 != fSubQuerySpecPtr)
        {
            delete fSubQuerySpecPtr;
        }
    }

    ostream& AllOrAnyPredicate::put(ostream& os) const
    {
        os << fScalarExpression << endl;
        os << fOperator;
        os << fAnyAllSome;

        if (0 != fSubQuerySpecPtr)
        {
            fSubQuerySpecPtr->put(os);
        }
        return os;
    }

    string AllOrAnyPredicate::getPredicateString() const
    {
        std::string all_or_any = fScalarExpression;
        all_or_any += " ";
        all_or_any += fOperator;
        all_or_any += " ";
        all_or_any += fAnyAllSome;

        if (0 != fSubQuerySpecPtr)
        {
            all_or_any += fSubQuerySpecPtr->getQueryString();
        }

        return all_or_any;
    }

    /** InPredicate
     */
    InPredicate::InPredicate()
        :Predicate(IN_PREDICATE),fSubQuerySpecPtr(0)
    {

    }

    InPredicate::~InPredicate()
    {
        if (0 != fSubQuerySpecPtr)
        {
            delete fSubQuerySpecPtr;
        }
    }

    ostream& InPredicate::put(ostream& os) const
    {
        os << fScalarExpression << endl;
        os << fOperator << endl;

        os << "(" << endl;
        AtomList::const_iterator iter = fAtomList.begin();
        while (iter != fAtomList.end())
        {
            os << *iter << endl;
            ++iter;
        }

        if (0 != fSubQuerySpecPtr)
        {
            fSubQuerySpecPtr->put(os);
        }

        os << ")" << endl;

        return os;
    }

    string InPredicate::getPredicateString() const
    {
        std::string in_predicate = fScalarExpression;
        in_predicate += " ";
        in_predicate += fOperator;
        in_predicate += " ";
        in_predicate += "(";

        AtomList::const_iterator iter = fAtomList.begin();
        while (iter != fAtomList.end())
        {
            in_predicate += *iter;
            ++iter;
            if (iter != fAtomList.end())
                in_predicate += ",";
        }
        if (0 != fSubQuerySpecPtr)
        {
            in_predicate += fSubQuerySpecPtr->getQueryString();
        }

        in_predicate += ")";

        return in_predicate;
    }

    /** NullTestPredicate
     */
    NullTestPredicate::NullTestPredicate()
        :Predicate(NULLTEST_PREDICATE)
    {
    }

    NullTestPredicate::~NullTestPredicate()
    {

    }

    ostream& NullTestPredicate::put(ostream& os) const
    {
        os << fColumnRef;
        os << fOperator;

        return os;
    }

    string NullTestPredicate::getPredicateString() const
    {
        std::string null_test_predicate = fColumnRef;
        null_test_predicate += " ";
        null_test_predicate += fOperator;

        return null_test_predicate;
    }

    /** LikePredicate
     */
    LikePredicate::LikePredicate()
        :Predicate(LIKE_PREDICATE),fOptionalEscapePtr(0)
    {

    }

    LikePredicate::~LikePredicate()
    {
        if (0 != fOptionalEscapePtr)
            delete fOptionalEscapePtr;
    }

    ostream& LikePredicate::put(ostream& os) const
    {
        os << fLHScalarExpression << endl;
        os << fAtom << endl;

        if (0 != fOptionalEscapePtr)
            fOptionalEscapePtr->put(os);

        return os;
    }

    string LikePredicate::getPredicateString() const
    {
        std::string like_predicate = fLHScalarExpression;
        like_predicate += " ";
        like_predicate += fAtom;

        if (0 != fOptionalEscapePtr)
        {
            like_predicate += " ";
            like_predicate += fOptionalEscapePtr->fEscapeChar;
        }

        return like_predicate;
    }

    /** BetweenPredicate
     */
    BetweenPredicate::BetweenPredicate()
        :Predicate(BETWEEN_PREDICATE)
    {
    }

    BetweenPredicate::~BetweenPredicate()
    {

    }

    ostream& BetweenPredicate::put(ostream& os) const
    {

        os << fLHScalarExpression << endl;
        os << fOperator1 << endl;
        os << fRH1ScalarExpression << endl;
        os << fOperator2 << endl;
        os << fRH2ScalarExpression << endl;

        return os;
    }

    string BetweenPredicate::getPredicateString() const
    {
        std::string between_predicate = fLHScalarExpression;
        between_predicate += " ";
        between_predicate += fOperator1;
        between_predicate += " ";
        between_predicate += fRH1ScalarExpression;
        between_predicate += " ";
        between_predicate += fOperator2;
        between_predicate + " ";
        between_predicate += fRH2ScalarExpression;

        return between_predicate;
    }

    /** ComparisonPredicate
     */
    ComparisonPredicate::ComparisonPredicate()
        :Predicate(COMPARE_PREDICATE),fSubQuerySpec(0)
    {
    }

    ComparisonPredicate::~ComparisonPredicate()
    {
        if (0 != fSubQuerySpec)
            delete fSubQuerySpec;
    }

    ostream& ComparisonPredicate::put(ostream& os) const
    {
        os << fLHScalarExpression << endl;
        os << fOperator << endl;
        os << fRHScalarExpression << endl;

        if (0 != fSubQuerySpec)
            fSubQuerySpec->put(os);

        return os;
    }

    string ComparisonPredicate::getPredicateString() const
    {
        std::string comparison_predicate = fLHScalarExpression;
        comparison_predicate += " ";
        comparison_predicate += fOperator;
        comparison_predicate += " ";
        comparison_predicate += fRHScalarExpression;

        if (0 != fSubQuerySpec)
        {
            comparison_predicate += " ";
            comparison_predicate += fSubQuerySpec->getQueryString();
        }

        return comparison_predicate;
    }

    /** Predicate
     */
    Predicate::Predicate()
        :fPredicateType(INVALID_PREDICATE)
    {
    }

    Predicate::Predicate(PREDICATE_TYPE predicateType)
        :fPredicateType(predicateType)
    {

    }

    Predicate::~Predicate()
    {

    }

    ostream& Predicate::put(ostream& os) const
    {
        return os;
    }

    string Predicate::getPredicateString() const
    {
        std::string predicate;

        return predicate;
    }

}                                                 //namespace dmlpackage
