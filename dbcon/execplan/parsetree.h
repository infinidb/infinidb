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
*   $Id: parsetree.h 9635 2013-06-19 21:42:30Z bwilkinson $
*
*
***********************************************************************/
/** @file */

#ifndef PARSETREE_H
#define PARSETREE_H

#include <iostream>
#include <fstream>
#include "treenode.h"
#include "operator.h"

namespace rowgroup {
	class Row;
}

namespace execplan {
//class Operator;
/**
 * @brief A template class template to represent an expression tree
 *
 * This class is a simple binary tree implementation which
 * represents an expression tree. The expression can be used
 * for both arithmetic and logical expression.
 * Please note that to use this template class T can only be pointers.
 */

class ParseTree
{

public:
	/**
	* Constructor / Destructor
	*/
	inline ParseTree();
	inline ParseTree(TreeNode* data);
	inline ParseTree(const ParseTree& rhs);
	inline virtual ~ParseTree();

	/**
	* Access methods
	*/
	inline void left(ParseTree* expressionTree)
	{ fLeft = expressionTree; }

	inline void left(TreeNode* node)
	{
		ParseTree* pt = new ParseTree(node);
		left(pt);
	}

	inline ParseTree* left() const
	{ return fLeft; }

	inline void right(ParseTree* expressionTree)
	{ fRight = expressionTree; }

	inline void right(TreeNode* node)
	{
		ParseTree* pt = new ParseTree(node);
		right(pt);
	}

	inline ParseTree* right() const
	{ return fRight; }

	inline void data(TreeNode* data)
	{ fData = data; }

	inline TreeNode* data() const
	{ return fData; }

	/** walk the tree
	*
	* postfix walking of a const tree
	*/
	inline void walk(void (*fn)( ParseTree* n)) const;

	/** walk the tree
	*
	* postfix walking of a non-const tree. This is for deleting the tree
	*/
	inline void walk(void (*fn)( const ParseTree* n)) const;

	/** output the tree
	*
	* take ostream argument to walk and output the tree
	*/
	inline void walk(void (*fn)(const ParseTree* n, std::ostream& output), std::ostream& output) const;

	/** output the tree
	*
	* take user argument to walk and output the tree
	*/
	inline void walk(void (*fn)(const ParseTree* n, void* obj), void* object) const;

	/** output the tree
	*
	* take user argument to walk and output the tree
	*/
	inline void walk(void (*fn)(ParseTree* n, void* obj), void* object) const;

	/** output the tree to string
	* for debug purpose
	*/
	inline std::string toString() const;

	/** assignment operator
	*
	*/
	inline ParseTree& operator=(const ParseTree& rhs);

	/** deep copy of a tree
	*
	* copy src tree on top of dest tree. Dest tree is destroyed before copy
	* takes place. Src tree still owns the tree data, though. Assume tree node
	* are pointers.
	*/
	inline void copyTree (const ParseTree &src);

	/** destroy a tree
	*
	* destroy a tree where tree nodes are values
	*/
	inline static void destroyTree (ParseTree *root);

	/** draw the tree using dot
	 *
	 * this function is mostly for debug purpose, where T represent a TreeNode
	 * pointer
	 */
	inline void drawTree(std::string filename);

	/** print the tree
	*
	* this function is mostly for debug purpose, where T represent a TreeNode
	* pointer
	*/
	inline static void print(const ParseTree* n, std::ostream& output)
	{
		output << *n->data() << std::endl;
	}

	/** delete treenode
	*
	* delete fData of a tree node, and then delete the treenode
	*/
	inline static void deleter( ParseTree*& n)
	{
		delete n->fData;
		n->fData = 0;
		delete n;
		n = 0;
	}

	inline void derivedTable (const std::string& derivedTable)
	{ fDerivedTable = derivedTable; }

	inline std::string& derivedTable()
	{ return fDerivedTable; }

	inline void setDerivedTable();

private:
	TreeNode* fData;
	ParseTree* fLeft;
	ParseTree* fRight;
	std::string fDerivedTable;

/**********************************************************************
 *                      F&E Framework                                 *
 **********************************************************************/

public:
	inline const std::string& getStrVal(rowgroup::Row& row, bool& isNull)
	{
		if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getStrVal(row, isNull, fLeft, fRight);
		else
			return fData->getStrVal(row, isNull);
	}

	inline int64_t getIntVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getIntVal(row, isNull, fLeft, fRight);
		else
			return fData->getIntVal(row, isNull);
	}

	inline uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
	{
		if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getUintVal(row, isNull, fLeft, fRight);
		else
			return fData->getUintVal(row, isNull);
	}

	inline float getFloatVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getFloatVal(row, isNull, fLeft, fRight);
		else
			return fData->getFloatVal(row, isNull);
	}

	inline double getDoubleVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getDoubleVal(row, isNull, fLeft, fRight);
		else
			return fData->getDoubleVal(row, isNull);
	}

	inline IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getDecimalVal(row, isNull, fLeft, fRight);
		else
			return fData->getDecimalVal(row, isNull);
	}

	inline bool getBoolVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getBoolVal(row, isNull, fLeft, fRight);
		else
			return fData->getBoolVal(row, isNull);
	}

	inline int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getDateIntVal(row, isNull, fLeft, fRight);
		else
			return fData->getDateIntVal(row, isNull);
	}

	inline int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
	{
	 	if (fLeft && fRight)
			return (reinterpret_cast<Operator*>(fData))->getDatetimeIntVal(row, isNull, fLeft, fRight);
		else
			return fData->getDatetimeIntVal(row, isNull);
	}

private:
	/** draw the tree
	 *
	 * this function is used by draw tree to print out dot file
	 */
	static void draw(const ParseTree* n, std::ostream& dotFile);

	// F&E framework
	void evaluate (rowgroup::Row& row, bool& isNull);

};

}

#include "operator.h"

namespace execplan {
/**
 * Class Definition
 */
inline ParseTree::ParseTree() :
	fData(0), fLeft(0), fRight(0), fDerivedTable("")
{
}

inline ParseTree::ParseTree(TreeNode* data) :
	   fData(data),
	   fLeft(0),
	   fRight(0)
{
	// bug5984. Need to validate data to be not null
	if (data)
		fDerivedTable = data->derivedTable();
}

inline ParseTree::ParseTree(const ParseTree& rhs):
	   fData(0),
	   fLeft(0),
	   fRight(0),
	   fDerivedTable(rhs.fDerivedTable)
{
	copyTree(rhs);
}

inline ParseTree::~ParseTree()
{
	if (fLeft != NULL)
		delete fLeft;

	if (fRight != NULL)
		delete fRight;

	if (fData != NULL)
		delete fData;

	fLeft = NULL;
	fRight = NULL;
	fData = NULL;
}

inline void ParseTree::walk(void (*fn)( ParseTree* n)) const
{
	if (fLeft != 0) fLeft->walk(fn);
	if (fRight != 0) fRight->walk(fn);
	ParseTree* temp = const_cast<ParseTree*>(this);
	fn (temp);
}

inline void ParseTree::walk(void (*fn)( const ParseTree* n)) const
{
	if (fLeft != 0) fLeft->walk(fn);
	if (fRight != 0) fRight->walk(fn);
	fn (this);
}

inline void ParseTree::walk(void (*fn)(const ParseTree* n, std::ostream& output), std::ostream& output) const
{
	if (fLeft != 0) fLeft->walk(fn, output);
	if (fRight != 0) fRight->walk(fn, output);
		fn(this, output);
}

inline void ParseTree::walk(void (*fn)(const ParseTree* n, void* obj), void* obj) const
{
	if (fLeft != 0) fLeft->walk(fn, obj);
	if (fRight != 0) fRight->walk(fn, obj);
		fn(this, obj);
}

inline std::string ParseTree::toString() const
{
	std::ostringstream oss;
	walk (ParseTree::print, oss);
	return oss.str();
}

inline void ParseTree::walk(void (*fn)(ParseTree* n, void* obj), void* obj) const
{
	if (fLeft != 0) fLeft->walk(fn, obj);
	if (fRight != 0) fRight->walk(fn, obj);
		fn(const_cast<ParseTree*>(this), obj);
}

inline ParseTree& ParseTree::operator=(const ParseTree& rhs)
{
	if (this != &rhs)
	{
		//copyTree(*this, rhs);
		copyTree(rhs);
	}
	return *this;
}

inline void ParseTree::copyTree(const ParseTree &src)
{
	if (fLeft != NULL)
		delete fLeft;
	if (fRight != NULL)
		delete fRight;
	fLeft = NULL;
	fRight = NULL;

	if (src.left() != NULL)
	{
		fLeft = new ParseTree();
		fLeft->copyTree(*(src.left()));
	}
	if (src.right() != NULL)
	{
		fRight = new ParseTree();
		fRight->copyTree(*(src.right()));
	}

	delete fData;
	if (src.data() == NULL)
		fData = NULL;
	else
		fData = src.data()->clone();
}

inline void ParseTree::destroyTree(ParseTree* root)
{
	if (root == NULL)
		return;
	if (root->left() != NULL)
	{
		destroyTree (root->fLeft);
	}
	if (root->right() != NULL)
	{
		destroyTree (root->fRight);
	}
	delete root;
	root = 0;
}

inline void ParseTree::draw(const ParseTree* n, std::ostream& dotFile)
{
	const ParseTree* r;
	const ParseTree* l;
	l = n->left();
	r = n->right();

	if (l != 0)
		dotFile << "n" << (void*)n << " -> " << "n" << (void*)l << std::endl;
	if (r != 0)
		dotFile << "n" << (void*)n << " -> " << "n" << (void*)r << std::endl;
	dotFile << "n" << (void*)n << " [label=\""
			<< n->data()->data() << "\"]" << std::endl;
}

inline void ParseTree::drawTree(std::string filename)
{
	std::ofstream dotFile (filename.c_str(), std::ios::out);

	dotFile << "digraph G {" << std::endl;
	walk (draw, dotFile);
	dotFile << "}" << std::endl;

	dotFile.close();
}

inline void ParseTree::evaluate(rowgroup::Row& row, bool& isNull)
{
	// Non-leaf node is operator. leaf node is SimpleFilter for logical expression,
	// or ReturnedColumn for arithmetical expression.
	if (fLeft && fRight)
	{
		Operator* op = reinterpret_cast<Operator*>(fData);
		op->evaluate(row, isNull, fLeft, fRight);
	}
	else
	{
		fData->evaluate(row, isNull);
	}
}

inline void ParseTree::setDerivedTable()
{
	std::string lDerivedTable = "";
	std::string rDerivedTable = "";

	if (fLeft)
	{
		fLeft->setDerivedTable();
		lDerivedTable = fLeft->derivedTable();
	}
	else
	{
		lDerivedTable = "*";
	}

	if (fRight)
	{
		fRight->setDerivedTable();
		rDerivedTable = fRight->derivedTable();
	}
	else
	{
		rDerivedTable = "*";
	}

	Operator *op = dynamic_cast<Operator*>(fData);
	if (op)
	{
		if (lDerivedTable == "*")
		{
			fDerivedTable = rDerivedTable;
		}
		else if (rDerivedTable == "*")
		{
			fDerivedTable = lDerivedTable;
		}
		else if (lDerivedTable == rDerivedTable)
		{
			fDerivedTable = lDerivedTable; // should be the same as rhs
		}
		else
		{
			fDerivedTable = "";
		}
	}
	else
	{
		fData->setDerivedTable();
		fDerivedTable = fData->derivedTable();
		fDerivedTable = fData->derivedTable();
	}
}

typedef boost::shared_ptr<ParseTree> SPTP;

} // namespace

#endif
