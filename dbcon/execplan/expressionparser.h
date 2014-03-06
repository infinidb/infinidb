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
*   $Id: expressionparser.h 9633 2013-06-19 13:36:01Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef EXPRESSION_PARSER
#define EXPRESSION_PARSER

#include <iostream>
#include <cassert>
#include <vector>
#include <stack>
#include <cmath>
#include <cctype>
#include <cstdlib>
#include <string>
#include "exp_templates.h"
#include "treenode.h"
#include "parsetree.h"
#include "operator.h"

namespace execplan {

/**
 * type define
 */
typedef std::stack<ParseTree*> OperandStack;
typedef std::stack<TreeNode*> OperatorStack;

/**@brief util struct for converting string to lower case
 * 
 */
struct to_lower
{
    char operator() (char c) const { return tolower(c); }
};

/**@brief a structure to respent a token accepted by the parser
 * 
 * token structure
 */
struct Token {
    TreeNode* value;
	bool is_operator() const
    { 
		if (value == 0) return false;
        return (typeid(*value) == typeid(Operator)); 
    }
	Token() : value(0) {}
	Token(TreeNode* v) : value(v) {}
private:
	// technically, these should be defined since Token is used in a 
	// std::vector, but it appears to work okay...ownership of the 
	// value ptr is dubious
	//Token(const Token& rhs);
	//Token& operator=(const Token& rhs);
};


/**
 * @brief this class builds an expression tree
 *
 * This struct parse the incomming tokens from an expression and 
 * build a expression tree according to the pre-defined precedence 
 * rules. The operators handled are: +, -, *, /, ||, (, ), func (, 
 * func ), and, or.
 */
class ExpressionParser : public expression::default_expression_parser_error_policy 
{
public:
    /**
     * Constructors/Destructors
     */
    ExpressionParser();
    virtual ~ExpressionParser();
    
    /**
     * Operations
     */
     
    /*
     * Error handling
     */
    static void invalid_operator_position(TreeNode* oper);   
    static void invalid_operator_position(const Token& oper);
    static void invalid_operand_position(ParseTree* operand);
    static void unbalanced_confix(TreeNode* oper);
    static void missing_operand(const Token& oper);
    
    /**
    * Syntax and precedence rules
    */
    static int positions(Token t);
    static int position(TreeNode* op);
    inline static bool is_operator(const Token& t)  
    { 
        return t.is_operator();
    }
    static ParseTree* as_operand(Token t);
    static TreeNode* as_operator(Token t, int pos);
    static expression::precedence precedence (TreeNode* op1, TreeNode* op2);
    static expression::associativity associativity(TreeNode* op1, TreeNode* op2);
    
    /**
    * Build an expression tree with the tokens
    */
    static ParseTree* reduce(TreeNode* op, ParseTree* value);
    static ParseTree* reduce(TreeNode* op, ParseTree* lhs, ParseTree* rhs);
    
    // parenthesis
    static ParseTree* reduce(TreeNode* a, TreeNode* b, ParseTree* value);
    
    // function call
    static ParseTree* reduce(ParseTree* a, TreeNode* b, ParseTree* value, TreeNode* d);
    
            
    /**
    * to clean up operator and operand stack when exception throws.
    * this is to be used by Calpont execplan project, where the operator
    * and operand are pointers. added by Zhixuan Zhu 06/30/06
    */
    static void cleanup(std::stack<ParseTree*> operandStack, std::stack<TreeNode*> operatorStack);

private:
    static int precnum(TreeNode* op);
    static expression::associativity assoc(TreeNode* op);
};

/** @brief Do a deep, strict (as opposed to semantic) equivalence test
 *
 * Do a deep, strict (as opposed to semantic) equivalence test.
 * @return true iff every member of t1 is a duplicate copy of every member of t2; false otherwise
 */
bool operator==(const ParseTree& t1,
				const ParseTree& t2);

/** @brief Do a deep, strict (as opposed to semantic) equivalence test
 *
 * Do a deep, strict (as opposed to semantic) equivalence test.
 * @return false iff every member of t1 is a duplicate copy of every member of t2; true otherwise
 */
bool operator!=(const ParseTree& t1,
				const ParseTree& t2);


} // namespace execplan

#endif
