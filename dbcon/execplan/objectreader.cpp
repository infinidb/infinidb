/* Copyright (C) 2013 Calpont Corp.

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

/*****************************************************************************
 *
 * $Id: objectreader.cpp 9559 2013-05-22 17:58:13Z xlou $
 *
 ****************************************************************************/

/*
 * Implements ObjectReader
 */

#include "treenode.h"
#include "returnedcolumn.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "constantcolumn.h"
#include "functioncolumn.h"
#include "rowcolumn.h"
#include "simplecolumn.h"
#include "simplecolumn_int.h"
#include "simplecolumn_uint.h"
#include "simplecolumn_decimal.h"
#include "filter.h"
#include "existsfilter.h"
#include "selectfilter.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "simplescalarfilter.h"
#include "operator.h"
#include "arithmeticoperator.h"
#include "windowfunctioncolumn.h"
#include "logicoperator.h"
#include "predicateoperator.h"
#include "treenodeimpl.h"
#include "calpontexecutionplan.h"
#include "calpontselectexecutionplan.h"
#include "groupconcatcolumn.h"
#include "outerjoinonfilter.h"

#include "bytestream.h"
#include "expressionparser.h"
#include "objectreader.h"

using namespace std;

namespace execplan {

TreeNode* ObjectReader::createTreeNode(messageqcpp::ByteStream& b) {

	CLASSID id = ZERO;
	TreeNode* ret;

    b.peek(reinterpret_cast<messageqcpp::ByteStream::byte&>(id));
	switch(id) {
		case TREENODEIMPL:
			ret = new TreeNodeImpl();
			break;
//		case RETURNEDCOLUMN:
//			ret = new ReturnedColumn();
//			break;
		case SIMPLECOLUMN:
			ret = new SimpleColumn();
			break;
		case SIMPLECOLUMN_INT2:
			ret = new SimpleColumn_INT<2>();
			break;
		case SIMPLECOLUMN_INT4:
			ret = new SimpleColumn_INT<4>();
			break;
		case SIMPLECOLUMN_INT8:
			ret = new SimpleColumn_INT<8>();
			break;
		case SIMPLECOLUMN_INT1:
			ret = new SimpleColumn_INT<1>();
			break;
        case SIMPLECOLUMN_UINT2:
            ret = new SimpleColumn_UINT<2>();
            break;
        case SIMPLECOLUMN_UINT4:
            ret = new SimpleColumn_UINT<4>();
            break;
        case SIMPLECOLUMN_UINT8:
            ret = new SimpleColumn_UINT<8>();
            break;
        case SIMPLECOLUMN_UINT1:
            ret = new SimpleColumn_UINT<1>();
            break;
		case SIMPLECOLUMN_DECIMAL2:
			ret = new SimpleColumn_Decimal<2>();
			break;
		case SIMPLECOLUMN_DECIMAL4:
			ret = new SimpleColumn_Decimal<4>();
			break;
		case SIMPLECOLUMN_DECIMAL8:
			ret = new SimpleColumn_Decimal<8>();
			break;
		case SIMPLECOLUMN_DECIMAL1:
			ret = new SimpleColumn_Decimal<1>();
			break;
		case AGGREGATECOLUMN:
			ret = new AggregateColumn();
			break;
		case GROUPCONCATCOLUMN:
			ret = new GroupConcatColumn();
			break;
		case ARITHMETICCOLUMN:
			ret = new ArithmeticColumn();
			break;
		case CONSTANTCOLUMN:
			ret = new ConstantColumn();
			break;
		case FUNCTIONCOLUMN:
			ret = new FunctionColumn();
			break;
		case ROWCOLUMN:
			ret = new RowColumn();
			break;
		case WINDOWFUNCTIONCOLUMN:
			ret = new WindowFunctionColumn();
			break;
		case FILTER:
			ret = new Filter();
			break;
		case EXISTSFILTER:
			ret = new ExistsFilter();
			break;
		case SELECTFILTER:
			ret = new SelectFilter();
			break;
		case SIMPLEFILTER:
			ret = new SimpleFilter();
			break;
		case CONSTANTFILTER:
			ret = new ConstantFilter();
			break;
		case SIMPLESCALARFILTER:
			ret = new SimpleScalarFilter();
			break;
		case OUTERJOINONFILTER:
			ret = new OuterJoinOnFilter();
			break;
		case OPERATOR:
			ret = new Operator();
			break;
		case ARITHMETICOPERATOR:
			ret = new ArithmeticOperator();
			break;
		case LOGICOPERATOR:
			ret = new LogicOperator();
			break;
		case PREDICATEOPERATOR:
			ret = new PredicateOperator();
			break;
		case NULL_CLASS:
			b >> (id_t&) id;   //eat the ID
			return NULL;
		default:
			throw UnserializeException("Bad type.  Stream out of sync?");
	};

	ret->unserialize(b);
	return ret;
}

CalpontExecutionPlan* ObjectReader::createExecutionPlan(messageqcpp::ByteStream& b)
{
	CLASSID id = ZERO;
	CalpontExecutionPlan *ret;
	
        b.peek(reinterpret_cast<messageqcpp::ByteStream::byte&>(id));
	switch (id) {
		case CALPONTSELECTEXECUTIONPLAN:
			ret = new CalpontSelectExecutionPlan();
			break;
		case NULL_CLASS:
			b >> reinterpret_cast<id_t&>(id);
			return NULL;
		default:
			throw UnserializeException("Bad type.  Stream out of sync?");
	}
	ret->unserialize(b);
	return ret;
}

void ObjectReader::writeParseTree(const ParseTree* tree, messageqcpp::ByteStream& b)
{
	if (tree == NULL) {
		b << (id_t) NULL_CLASS;
		return;
	}
	b << (id_t) PARSETREE;
	writeParseTree(tree->left(), b);
	writeParseTree(tree->right(), b);
	if (tree->data() == NULL)
		b << (id_t) NULL_CLASS;
	else
		tree->data()->serialize(b);
}
	
ParseTree* ObjectReader::createParseTree(messageqcpp::ByteStream& b)
{
	CLASSID id = ZERO;
	ParseTree* ret;
	
	b >> (id_t&) id;
	if (id == NULL_CLASS)
		return NULL;
	if (id != PARSETREE)
		throw UnserializeException("Not a ParseTree");
	
	ret = new ParseTree();
	ret->left(createParseTree(b));
	ret->right(createParseTree(b));
	ret->data(createTreeNode(b));
	return ret;
}	

void ObjectReader::checkType(messageqcpp::ByteStream& b, const CLASSID id)
{
	CLASSID readId = ZERO;
	
	b >> (id_t&) readId;
	if (readId != id)
		switch (id) {
			case TREENODEIMPL:
				throw UnserializeException("Not a TreeNodeImpl");
			case RETURNEDCOLUMN:
				throw UnserializeException("Not a ReturnedColumn");
			case SIMPLECOLUMN:
				throw UnserializeException("Not a SimpleColumn");
			case AGGREGATECOLUMN:
				throw UnserializeException("Not an AggregateColumn");
			case ARITHMETICCOLUMN:
				throw UnserializeException("Not an ArithmeticColumn");
			case CONSTANTCOLUMN:
				throw UnserializeException("Not a ConstantColumn");
			case FUNCTIONCOLUMN:
				throw UnserializeException("Not a FunctionColumn");
			case ROWCOLUMN:
				throw UnserializeException("Not a RowColumn");
			case FILTER:
				throw UnserializeException("Not a Filter");
			case CONDITIONFILTER:
				throw UnserializeException("Not a ConditionFilter");
			case EXISTSFILTER:
				throw UnserializeException("Not an ExistsFilter");
			case SELECTFILTER:
				throw UnserializeException("Not a SelectFilter");
			case SIMPLEFILTER:
				throw UnserializeException("Not a SimpleFilter");
		    case CONSTANTFILTER:
				throw UnserializeException("Not a ConstantFilter");
			case OPERATOR:
				throw UnserializeException("Not an Operator");
			case PARSETREE:
				throw UnserializeException("Not an ParseTree");
			case CALPONTSELECTEXECUTIONPLAN:
				throw UnserializeException("Not a CalpontSelectExecutionPlan");
			case NULL_CLASS:
				throw UnserializeException("Not NULL");   // ??
			default:
				throw UnserializeException("Bad id");
		}
	return;
}
	
ObjectReader::UnserializeException::UnserializeException(std::string msg)
    throw() : fWhat(msg)
{
}

ObjectReader::UnserializeException::~UnserializeException() throw() {
}

const char* ObjectReader::UnserializeException::what() const throw() {
	return fWhat.c_str();
}


}   /* namespace */
