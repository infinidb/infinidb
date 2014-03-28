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

/******************************************************************************
 * $Id: objectreader.h 9633 2013-06-19 13:36:01Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class ObjectReader interface
 */

#ifndef EXECPLAN_OBJECTREADER_H
#define EXECPLAN_OBJECTREADER_H

#include <exception>
#include <string>
#include <stdint.h>

namespace messageqcpp {
class ByteStream;
}

namespace execplan {

	class TreeNode;
	class ParseTree;
	class CalpontExecutionPlan;
	
/** @brief A class for creating execplan classes from ByteStreams.
 *
 * A class currently used for recreating execplan polymorphic classes
 * and dynamic objects from ByteStreams.
 */

class ObjectReader {

public:

	class UnserializeException : public std::exception {
	public:
		UnserializeException(std::string) throw();
		virtual ~UnserializeException() throw();
		virtual const char* what() const throw();
	private:
		std::string fWhat;
	};

	/** @brief Enumerates classes supporting serialization
	 *
	 * This defines one constant for each class that supports
	 * serialization.
	 */
	enum CLASSID {
		ZERO,      // an appropriate initializer
		NULL_CLASS,		// to denote that some member is NULL
		
		/**** TreeNodes */
		TREENODE,
		TREENODEIMPL,
		RETURNEDCOLUMN,
		AGGREGATECOLUMN,
		GROUPCONCATCOLUMN,
		ARITHMETICCOLUMN,
		CONSTANTCOLUMN,
		FUNCTIONCOLUMN,
		ROWCOLUMN,
		WINDOWFUNCTIONCOLUMN,
		
		SIMPLECOLUMN,
		SIMPLECOLUMN_INT1,
		SIMPLECOLUMN_INT2,
		SIMPLECOLUMN_INT4,
		SIMPLECOLUMN_INT8,
		SIMPLECOLUMN_UINT1,
		SIMPLECOLUMN_UINT2,
		SIMPLECOLUMN_UINT4,
		SIMPLECOLUMN_UINT8,
		SIMPLECOLUMN_DECIMAL1,
		SIMPLECOLUMN_DECIMAL2,
		SIMPLECOLUMN_DECIMAL4,
		SIMPLECOLUMN_DECIMAL8,

		FILTER,
		CONDITIONFILTER,
		EXISTSFILTER,
		SELECTFILTER,
		SIMPLEFILTER,
		SIMPLESCALARFILTER,

		OPERATOR,
		ARITHMETICOPERATOR,
		PREDICATEOPERATOR,
		LOGICOPERATOR,

		/**** /TreeNodes */
		
		PARSETREE,
		CALPONTSELECTEXECUTIONPLAN,
		CONSTANTFILTER,
		OUTERJOINONFILTER,
	};

	typedef u_int8_t id_t;    //expand as necessary

	/** @brief Creates a new TreeNode object from the ByteStream
	 *
	 * @param b The ByteStream to create it from
	 * @return A newly allocated TreeNode
	 */
	static TreeNode* createTreeNode(messageqcpp::ByteStream& b);
	
	/** @brief Creates a new ParseTree from the ByteStream
	 *
	 * @param b The ByteStream to create it from
	 * @return A newly allocated ParseTree
	 */
	static ParseTree* createParseTree(messageqcpp::ByteStream& b);
	
	/** @brief Creates a new CalpontExecutionPlan from the ByteStream
	 *
	 * @param b The ByteStream to create it from
	 * @return A newly allocated CalpontExecutionPlan
	 */
	static CalpontExecutionPlan* createExecutionPlan(messageqcpp::ByteStream& b);
	
	/** @brief Serialize() for ParseTrees
	 *
	 * This function effectively serializes a ParseTree.
	 * @param tree The ParseTree to write out
	 * @param b The ByteStream to write tree to
	 */
	static void writeParseTree(const ParseTree* tree, 
							   messageqcpp::ByteStream& b);
	
	/** @brief Verify the type of the next object in the ByteStream
	 *
	 * @param b The ByteStream to read from
	 * @param type The type it should be
	 * @throw UnserializeException if the type does not match; this is a fatal error.
	 */
	static void checkType(messageqcpp::ByteStream &b, const CLASSID type);
};

}
#endif // EXECPLAN_OBJECTREADER_H

