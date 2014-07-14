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
*   $Id: expressionparser.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
#include <stack>
#include <sstream>
using namespace std;

#include "expressionparser.h"
#include "aggregatecolumn.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "operator.h"
#include "treenode.h"

namespace execplan {

/**
 * Constructor / Destructor
 */
ExpressionParser::ExpressionParser()
{}

ExpressionParser::~ExpressionParser()
{} 

void ExpressionParser::invalid_operator_position(TreeNode* oper)
{
    std::string str = oper->data();
    delete oper;
	throw std::runtime_error ("Invalid operator position: " + str + "\n");
}
    
void ExpressionParser::invalid_operator_position(const Token& oper)
{
	throw std::runtime_error ("Invalid operator position: " + oper.value->data() + "\n");
}
    
void ExpressionParser::invalid_operand_position(ParseTree* operand)
{
    delete operand;
    throw std::runtime_error ("Invalid operand position\n");
}
    
void ExpressionParser::unbalanced_confix(TreeNode* oper)
{
    delete oper;
	throw std::runtime_error ("Unbalanced confix operator\n");
}

void ExpressionParser::missing_operand (const Token& oper)
{
    delete oper.value;
    throw std::runtime_error ("Imissing operand\n");    
}

int ExpressionParser::positions(Token t) 
{
    string oper = t.value->data();
    char c = oper.at(0);
    
    switch (c) 
    {
    case '+':
    case '-':
        return expression::infix | expression::prefix;        
    
    case '*':
    case '/':
    case '^':
    case '|': // concat ||                
        return expression::infix;
    
    case '(':
        return expression::open | expression::function_open;
    
    case ')':
        return expression::close | expression::function_close;
    
    default:
        transform (oper.begin(), oper.end(), oper.begin(), to_lower());
        if (oper.compare ("and") == 0 || oper.compare ("or") == 0 )
            return expression::infix;
    }
    
    ostringstream oss;
    oss << "ExpressionParser::positions(Token): invalid input token: >" << oper << '<';
    throw std::runtime_error(oss.str());
    return 0;
}
	
int ExpressionParser::position(TreeNode* op) 
{
    string oper = op->data();
    char c = oper.at(0);
    
    switch (c) 
    {
    case '+':
    case '-':
    case '*':
    case '/':
    case '|': // concat ||        
        return expression::infix;
    
    case 'M':
    case 'm':
    case 'I':
    case 'i':
        return expression::prefix;
    
    case '(':
        return expression::open;
    
    case '[':
        return expression::function_open;
    
    case ')':
        return expression::close;

    case ']':
        return expression::function_close;        
    
    default:
        transform (oper.begin(), oper.end(), oper.begin(), to_lower());
        if (oper.compare ("and") == 0 || oper.compare ("or") == 0 )
            return expression::infix;
    }
    
    ostringstream oss;
    oss << "ExpressionParser::position(TreeNode*): invalid input token: >" << oper << '<';
    throw std::runtime_error(oss.str());
}
	
ParseTree* ExpressionParser::as_operand(Token t)
{ 
    return new ParseTree(t.value);
}
  	
TreeNode* ExpressionParser::as_operator(Token t, int pos) 
{
    string oper = t.value->data();
    char c = oper.at(0);
    switch (c) 
    {
    case '+':
        if (pos == expression::infix)
            return t.value;
        else
        {
            delete t.value;
            return new Operator("I");
        }
    
    case '-':
        if (pos == expression::infix)
            return t.value;
        else
        {
            delete t.value;
            return new Operator("M");
        }        
    
    case '*':
    case '/':
    case ')':
    case '|':
        return t.value;
        
    case '(':
        if (pos == expression::open)
            return t.value;
        else
        {
            delete t.value;
            return new Operator("[");
        }

    default:
        transform (oper.begin(), oper.end(), oper.begin(), to_lower());
        if (oper.compare ("and") == 0 || oper.compare ("or") == 0 )
            return t.value;
    }    
    
    ostringstream oss;
    oss << "ExpressionParser::as_operator(Token,int): invalid input token: >" << oper << '<';
    throw std::runtime_error(oss.str());
}

expression::precedence ExpressionParser::precedence (TreeNode* op1, TreeNode* op2)
{
    int p1 = precnum(op1);
    int p2 = precnum(op2);
    
    if (p1 < p2)
        return expression::lower;
    else if (p1 > p2)
        return expression::higher;
    else if (p1 == p2)
        return expression::equal;
    else
        return expression::none;
}

expression::associativity ExpressionParser::associativity(TreeNode* op1, TreeNode* op2)
{
    expression::associativity assoc1 = assoc(op1);
    expression::associativity assoc2 = assoc(op2);
    
    if (assoc1 == assoc2)
        return assoc1;
    else
        return expression::non_associative;
}

/**
* Build an expression tree with the tokens
*/
ParseTree* ExpressionParser::reduce(TreeNode* op, ParseTree* value)
{
    char c = op->data().at(0);
    switch (c) {
    case 'M':
    case 'm':
    {
        ParseTree *root = new ParseTree(op);
        ParseTree *lhs = new ParseTree(new ConstantColumn("0", ConstantColumn::NUM));
        root->left(lhs);
        root->right(value);
        return root;
    }
    case 'I':
    case 'i':
        delete op;
        return value;
    default:
        idbassert(0);
    }
    
    ostringstream oss;
    oss << "ExpressionParser::reduce(TreeNode*,ParseTree*): invalid input token: >" << op->data() << '<';
    throw std::runtime_error(oss.str());
    return 0;
}

ParseTree* ExpressionParser::reduce(TreeNode* op, ParseTree* lhs, ParseTree* rhs)
{
    ParseTree* root = new ParseTree(op);
    root->left(lhs);
    root->right(rhs);
    return root;
}

// parenthesis
ParseTree* ExpressionParser::reduce(TreeNode* a, TreeNode* b, ParseTree* value)
{
    delete a;
    delete b; 
    return value;
}

// function call: handle aggregation functions and other functions
ParseTree* ExpressionParser::reduce(ParseTree* a, TreeNode* b, 
                                        ParseTree* value, TreeNode* d)
{
    string functionName = a->data()->data();
    string content = value->data()->data();
    ParseTree *root;
    
    transform (functionName.begin(), functionName.end(), functionName.begin(), to_lower());
    if (functionName.compare("sum") == 0 ||
        functionName.compare("avg") == 0 ||
        functionName.compare("count") == 0 ||
        functionName.compare("min") == 0 ||
        functionName.compare("max") == 0)
    {
        AggregateColumn *ac = new AggregateColumn(functionName, content);
        root = new ParseTree(ac);
    }
    else
    {
        FunctionColumn *fc = new FunctionColumn(functionName, content);
        root = new ParseTree(fc);
    }

    delete a;
    delete value;
    delete b;
    delete d;
    return root;
}

int ExpressionParser::precnum(TreeNode* op)
{
    string oper = op->data();
    char c = oper.at(0);
    switch (c) 
    {
        case '+':
        case '-':
        case '|':
            return 3;
        
        case '*':
        case '/':
            return 4;
    
        case 'M':
        case 'I':
            return 5;
        
        case '(':
            return 6;
            
        case '[':
            return 7;
        
        default:
            transform (oper.begin(), oper.end(), oper.begin(), to_lower());
            if (oper.compare("or") == 0)
                return 1;
            else if (oper.compare("and") == 0)
                return 2;
    }    
    
    return 0;
}

expression::associativity ExpressionParser::assoc(TreeNode* op)
{
    string oper = op->data();
    char c = oper.at(0);
    switch (c) 
    {
        case '+':
        case '-':
        case '*':
        case '/':
        case '|':
            return expression::left_associative;
        
        default:
            transform (oper.begin(), oper.end(), oper.begin(), to_lower());
            if (oper.compare("or") == 0 || oper.compare("and") == 0)
                return expression::left_associative;
            return expression::non_associative;
    }
}

void ExpressionParser::cleanup(stack<ParseTree*> operandStack, 
                               stack<TreeNode*> operatorStack)
{
    while ( !operandStack.empty() )
    {
        ParseTree* a = operandStack.top();
        operandStack.pop();
        delete a;
    }
    while ( !operatorStack.empty() )
    {
        TreeNode* a = operatorStack.top();
        operatorStack.pop();
        delete a;
    }
}

bool operator==(const ParseTree& t1,
				const ParseTree& t2)
{
	if (t1.data() != NULL && t2.data() != NULL) {
		if (*t1.data() != t2.data())
			return false;
	}
	else if (t1.data() != NULL || t2.data() != NULL)
		return false;

	if (t1.left() != NULL && t2.left() != NULL) {
		if (*t1.left() != *t2.left())
			return false;
	}
	else if (t1.left() != NULL || t2.left() != NULL)
		return false;
	
	if (t1.right() != NULL && t2.right() != NULL) {
		if (*t1.right() != *t2.right())
			return false;
	}
	else if (t1.right() != NULL || t2.right() != NULL)
		return false;
	
	return true;
}

bool operator!=(const ParseTree& t1,
				const ParseTree& t2)
{
	return !(t1 == t2);
}

}
