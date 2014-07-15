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
*   $Id: arithmeticcolumn.cpp 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
#include <string>
#include <exception>
#include <stdexcept>
#include <sstream>
using namespace std;

#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
#include "operator.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "objectreader.h"
#include "expressionparser.h"
#include "calpontselectexecutionplan.h"
#include "treenodeimpl.h"
#include "functioncolumn.h"
#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"

namespace {
/** print the tree
 *
 * this function is mostly for debug purpose
 */
void walkfn(const execplan::ParseTree* n, ostream& output)
{
	output << *(n->data()) << endl;
}

}

namespace execplan {

/**
 *  Constructors/Destructors
 */
ArithmeticColumn::ArithmeticColumn():
    ReturnedColumn(),
    fExpression (0)
{}

ArithmeticColumn::ArithmeticColumn(const string& sql, const uint32_t sessionID):
    ReturnedColumn(sessionID),
    fData(sql),
    fExpression(0)
{
	buildTree();
}

ArithmeticColumn::ArithmeticColumn(const ArithmeticColumn& rhs, const uint32_t sessionID):
    ReturnedColumn(rhs, sessionID),
    fTableAlias (rhs.fTableAlias),
    fAsc (rhs.fAsc),
    fData (rhs.fData),
    fExpression (new ParseTree (*(rhs.expression())))
{
	fAlias = rhs.fAlias;
	fSimpleColumnList.clear();
	fExpression->walk(getSimpleCols, &fSimpleColumnList);
	fAggColumnList.clear();
	fExpression->walk(getAggCols, &fAggColumnList);
	fWindowFunctionColumnList.clear();
	fExpression->walk(getWindowFunctionCols, &fWindowFunctionColumnList);
}

ArithmeticColumn::~ArithmeticColumn()
{
	if (fExpression != NULL)
		delete fExpression;
	fExpression = NULL;
}

/**
 * Methods
 */

void ArithmeticColumn::expression(ParseTree*& expression)
{
    if (fExpression != NULL)
        delete fExpression;
    fExpression = expression;
    expression = 0;
}

void ArithmeticColumn::buildTree()
{
    CalpontSelectExecutionPlan::Parser parser;
    vector<Token> tokens;
    Token t;

    string::size_type i = 0;

	//string fData = ReturnedColumn::data();

    try {
    while (fData[i])
    {
        if (isdigit(fData[i]) || fData[i] == '.')
        {
            string num;
            while (isdigit(fData[i]) || fData[i] == '.')
            {
                num.push_back(fData[i++]);
            }

            ConstantColumn *cc = new ConstantColumn(num, ConstantColumn::NUM);
            t.value = cc;

            tokens.push_back(t);
            continue;
        }
        else if (fData[i] == '+' ||
                 fData[i] == '-' ||
                 fData[i] == '*' ||
                 fData[i] == '/' ||
                 fData[i] == '^' ||
                 fData[i] == '(' ||
                 fData[i] == ')' )
        {
            // t.is_operator now indicate the previous token type
            // if prev token is operand, then this '(' is func_open
            // otherwise, this '(' is open
            if (fData[i] == '(' && fData[i+1] != '-' && !t.is_operator())
            {
                // open '('
                Operator *op1 = new Operator("(");
                t.value = op1;
                tokens.push_back(t);

                //This is not complete... we shouldn't be creating TreeNodes
                string param = nextToken(++i, ')');

                TreeNode *tn = new TreeNodeImpl(param);
                t.value = tn;

                tokens.push_back(t);

                // close ')'
                Operator *op2 = new Operator(")");
                t.value = op2;
                tokens.push_back(t);
                continue;
            }

            string op;

            // Bug 319 fix. recover '^' to '||'
            if (fData[i] == '^')
                op = "||";
            else
                op.push_back(fData[i]);
            Operator *oper = new Operator(op);
            t.value = oper;

            tokens.push_back(t);
            ++i;

            // t.is_operator now indicate the previous token type
            // if prev token is operand, then this '(' is func_open
            // otherwise, this '(' is open
            // @bug 241 fix. check (-n_nationkey) case
            if (fData[i] == '(' && fData[i] != '-' && !t.is_operator())
            {
                //This is not complete... we shouldn't be creating TreeNodes
                string param = nextToken(++i, ')');
                TreeNode *sc = new TreeNodeImpl(param);
                t.value = sc;

                tokens.push_back(t);

                // close ')'
                Operator *oper = new Operator(")");
                t.value = oper;
                tokens.push_back(t);
            }
            continue;
        }

        else if (isalpha(fData[i]) || fData[i] == '_' )
        {
            string identifier;
            while (isalnum(fData[i]) ||
                   fData[i] == '_' ||
                   fData[i] == '.' )
            {
                identifier.push_back(fData[i++]);
            }

            SimpleColumn *sc = new SimpleColumn(identifier, fSessionID );
            t.value = sc;

            tokens.push_back(t);
            continue;
        }

        else if (fData[i] == '\'')
        {
            string literal = nextToken(++i, '\'');
            ConstantColumn *cc = new ConstantColumn (literal, ConstantColumn::LITERAL);
            t.value = cc;

            tokens.push_back(t);
            continue;
        }
        ++i;
    }

    fExpression = parser.parse(tokens.begin(), tokens.end());
    }
    catch (const invalid_argument& e)
    {
        // clean up tokens
        for (unsigned int i=0; i<tokens.size(); i++)
        {
            delete tokens[i].value;
            tokens[i].value = 0;
        }
        throw runtime_error(e.what());
    }
}

const string ArithmeticColumn::nextToken(string::size_type &pos, char end) const
{
    string token;
    // string fData = ReturnedColumn::data();

    // increment num when get '(' and decrement when get ')'
    // to find the mathing ')' when num = 0
    int num = 1;
    for (; pos < fData.length(); )
    {
        if (end == ')')
        {
            if (fData[pos] == '(')
                num++;
            else if (fData[pos] == ')')
                num--;
            if (num == 0)
            {
                pos++;
                return token;
            }
        }
        else
        {
            if (fData[pos] == end)
            {
                pos++;
                return token;
            }
        }
        token.push_back(fData[pos++]);
    }

    string msg = "No ";
    msg.append(1, end);
    msg.append(" found in " + fData);
    throw invalid_argument ( msg );
    return 0;
}

ostream& operator<<(ostream& output, const ArithmeticColumn& rhs)
{
	output << rhs.toString();
	return output;
}

const string ArithmeticColumn::toString() const
{
	ostringstream oss;
	oss << "ArithmeticColumn: ";
	if (fAlias.length() > 0) oss << "Alias: " << fAlias << endl;
	if (fExpression != 0) fExpression->walk(walkfn, oss);
	oss << "expressionId=" << fExpressionId << endl;
	oss << "joinInfo=" << fJoinInfo << " returnAll=" << fReturnAll << " sequence#=" << fSequence << endl;
	oss << "resultType=" << colDataTypeToString(fResultType.colDataType) << "|" << fResultType.colWidth << 
endl;
	return oss.str();
}

void ArithmeticColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << static_cast<ObjectReader::id_t>(ObjectReader::ARITHMETICCOLUMN);
	ReturnedColumn::serialize(b);
	ObjectReader::writeParseTree(fExpression, b);
	b << fTableAlias;
	b << fData;
	b << static_cast<const ByteStream::doublebyte>(fAsc);
}

void ArithmeticColumn::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::ARITHMETICCOLUMN);
	ReturnedColumn::unserialize(b);
	if (fExpression != NULL)
		delete fExpression;
	fExpression = ObjectReader::createParseTree(b);
	b >> fTableAlias;
	b >> fData;
	b >> reinterpret_cast< ByteStream::doublebyte&>(fAsc);

	fSimpleColumnList.clear();
	fExpression->walk(getSimpleCols, &fSimpleColumnList);
	fAggColumnList.clear();
	fExpression->walk(getAggCols, &fAggColumnList);
	fWindowFunctionColumnList.clear();
	fExpression->walk(getWindowFunctionCols, &fWindowFunctionColumnList);
}

bool ArithmeticColumn::operator==(const ArithmeticColumn& t) const
{
	const ReturnedColumn *rc1, *rc2;

	rc1 = static_cast<const ReturnedColumn*>(this);
	rc2 = static_cast<const ReturnedColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fExpression != NULL && t.fExpression != NULL) {
		if (*fExpression != *t.fExpression)
			return false;
	}
	else if (fExpression != NULL || t.fExpression != NULL)
		return false;
	if (fAlias != t.fAlias)
		return false;
	if (fTableAlias != t.fTableAlias)
	    return false;
	if (fData != t.fData)
		return false;
	return true;
}

bool ArithmeticColumn::operator==(const TreeNode* t) const
{
	const ArithmeticColumn *o;

	o = dynamic_cast<const ArithmeticColumn*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool ArithmeticColumn::operator!=(const ArithmeticColumn& t) const
{
	return (!(*this == t));
}

bool ArithmeticColumn::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

bool ArithmeticColumn::hasAggregate()
{
	if (fHasAggregate) return true;
	fAggColumnList.clear();
	fExpression->walk(getAggCols, &fAggColumnList);
	if (!fAggColumnList.empty())
		fHasAggregate = true;
	return fHasAggregate;
}

bool ArithmeticColumn::hasWindowFunc()
{
	fWindowFunctionColumnList.clear();
	fExpression->walk(getWindowFunctionCols, &fWindowFunctionColumnList);
	if (fWindowFunctionColumnList.empty())
		return false;
	return true;
}

void ArithmeticColumn::setDerivedTable()
{
	if (hasAggregate())
	{
		fDerivedTable = "";
		return;
	}
	if (fExpression)
	{
		fExpression->setDerivedTable();
		fDerivedTable = fExpression->derivedTable();
	}
}

void ArithmeticColumn::replaceRealCol(std::vector<SRCP>& derivedColList)
{
	if (fExpression)
		replaceRefCol(fExpression, derivedColList);
}

void ArithmeticColumn::setSimpleColumnList()
{
	fSimpleColumnList.clear();
	fExpression->walk(getSimpleCols, &fSimpleColumnList);
}

bool ArithmeticColumn::singleTable(CalpontSystemCatalog::TableAliasName& tan)
{
	tan.clear();
	setSimpleColumnList();
	for (uint32_t i = 0; i < fSimpleColumnList.size(); i++)
	{
		CalpontSystemCatalog::TableAliasName stan(fSimpleColumnList[i]->schemaName(),
		                    fSimpleColumnList[i]->tableName(),
		                    fSimpleColumnList[i]->tableAlias(),
		                    fSimpleColumnList[i]->viewName());
		if (tan.table.empty())
			tan = stan;
		else if (stan != tan)
			return false;
	}
	return true;
}

} // namespace

