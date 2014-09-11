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
*   $Id: expressiontree.h 7409 2011-02-08 14:38:50Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef EXPRESSIONTREE_H
#define EXPRESSIONTREE_H

#include <iostream>
#include <fstream>
#include "treenode.h"

namespace rowgroup {
	class Row;
}

namespace execplan {

/**
 * @brief A template class template to represent an expression tree
 *
 * This class is a simple binary tree implementation which
 * represents an expression tree. The expression can be used
 * for both arithmetic and logical expression. 
 * Please note that to use this template class T can only be pointers. 
 */
 
template <class T>
class ExpressionTree
{

public:
    /**
    * Constructor / Destructor
    */	
    ExpressionTree();
    ExpressionTree(const T& data);
    ExpressionTree(const ExpressionTree& rhs);
    virtual ~ExpressionTree();
    
    /**
    * Access methods
    */
    inline void left(ExpressionTree* expressionTree)
    { fLeft = expressionTree; }
    
    inline const ExpressionTree* left() const
    { return fLeft; }
    
    inline void right(ExpressionTree* expressionTree)
    { fRight = expressionTree; }
     
    inline const ExpressionTree* right() const
    { return fRight; }
    
    inline void data(const T& data)
    { fData = data; }
    
    inline const T& data() const
    { return fData; }
    
    /** walk the tree
    *
    * postfix walking of a const tree
    */
    void walk(void (*fn)( ExpressionTree*& n)) const;
    
    /** walk the tree
    *
    * postfix walking of a non-const tree. This is for deleting the tree
    */
    void walk(void (*fn)( const ExpressionTree* n)) const;
    
    /** output the tree
    *
    * take ostream argument to walk and output the tree
    */
    void walk(void (*fn)(const ExpressionTree* n, std::ostream& output), std::ostream& output) const;

    /** output the tree
    *
    * take user argument to walk and output the tree
    */
    void walk(void (*fn)(const ExpressionTree* n, void* obj), void* object) const;

    /** output the tree
    *
    * take user argument to walk and output the tree
    */
    void walk(void (*fn)(ExpressionTree* n, void* obj), void* object) const;

    /** assignment operator
    * 
    */
    ExpressionTree& operator=(const ExpressionTree& rhs);
    
    /** deep copy of a tree
    *
    * copy src tree on top of dest tree. Dest tree is destroyed before copy
    * takes place. Src tree still owns the tree data, though. Assume tree node
    * are pointers.
    */
    void copyTree (const ExpressionTree &src);
     
    /** destroy a tree
    *
    * destroy a tree where tree nodes are pointers
    */
    inline void destroyPointerTree () { walk (deleter); }
    
    /** destroy a tree
    *
    * destroy a tree where tree nodes are values
    */
    static void destroyTree (ExpressionTree *root);    
    
    /** draw the tree using dot
     *
     * this function is mostly for debug purpose, where T represent a TreeNode
     * pointer
     */
    void drawTree(std::string filename);
    
    /** print the tree
    *
    * this function is mostly for debug purpose, where T represent a TreeNode
    * pointer
    */
    inline static void print(const ExpressionTree<T>* n, std::ostream& output)
    {
        output << *n->data() << std::endl;
    } 
    
    /** delete treenode
    *
    * delete fData of a tree node, and then delete the treenode
    */
    inline static void deleter( ExpressionTree<T>*& n)
    {
        delete n->fData;
        n->fData = 0;
        delete n;
        n = 0;
    } 
    
    // F&E framework. Tree nodes are only TreeNode*
  	//void evaluate (rowgroup::Row& row) {}
    std::string getStrVal(rowgroup::Row& row) 
    { 
    	evaluate(row);
    	return (reinterpret_cast<TreeNode*>(fData))->getStrVal(row); 
    }
		int64_t getIntVal(rowgroup::Row& row) 
		{ 
			evaluate(row);
			return (reinterpret_cast<TreeNode*>(fData))->getIntVal(row); 
		}
		float getFloatVal(rowgroup::Row& row) 
		{ 
			evaluate(row);
			return (reinterpret_cast<TreeNode*>(fData))->getFloatVal(row); 
		}
		double getDoubleVal(rowgroup::Row& row)
		{ 
			evaluate(row);
			return (reinterpret_cast<TreeNode*>(fData))->getDoubleVal(row); 
		}
		IDB_Decimal& getDecimalVal(rowgroup::Row& row) 
		{ 
			evaluate(row);
			return (reinterpret_cast<TreeNode*>(fData))->getDecimalVal(row); 
		}
		bool getBoolVal(rowgroup::Row& row) 
		{ 
			evaluate(row);
			return (reinterpret_cast<TreeNode*>(fData))->getBoolVal(row); 
		}
    
private:
    T fData;
    ExpressionTree* fLeft;
    ExpressionTree* fRight;
    
    /** draw the tree
     *
     * this function is used by draw tree to print out dot file
     */
    static void draw(const ExpressionTree<T>* n, std::ostream& dotFile);
    // F&E framework
  	void evaluate (rowgroup::Row& row);
    
};

/**
 * Class Definition
 */
template <class T>
ExpressionTree<T>::ExpressionTree() :
    fData(), fLeft(0), fRight(0)
    
{
}

template <class T>
ExpressionTree<T>::ExpressionTree(const T& data) :
       fData(data), fLeft(0), fRight(0)
{
}

template <class T>
ExpressionTree<T>::ExpressionTree(const ExpressionTree& rhs):
    fData(), fLeft(0), fRight(0)
{
    copyTree(rhs);
}

template <class T>
ExpressionTree<T>::~ExpressionTree()
{
}

template <class T>
void ExpressionTree<T>::walk(void (*fn)( ExpressionTree*& n)) const
{
    if (fLeft != 0) fLeft->walk(fn);
    if (fRight != 0) fRight->walk(fn);
    ExpressionTree<T>* temp = const_cast<ExpressionTree<T>*>(this);
    fn (temp);
}

template <class T>
void ExpressionTree<T>::walk(void (*fn)( const ExpressionTree* n)) const
{
    if (fLeft != 0) fLeft->walk(fn);
    if (fRight != 0) fRight->walk(fn);
    fn (this);
}

template <class T>
void ExpressionTree<T>::walk(void (*fn)(const ExpressionTree* n, std::ostream& output), std::ostream& output) const
{
    if (fLeft != 0) fLeft->walk(fn, output);
    if (fRight != 0) fRight->walk(fn, output);
        fn(this, output);
}

template <class T>
void ExpressionTree<T>::walk(void (*fn)(const ExpressionTree* n, void* obj), void* obj) const
{
    if (fLeft != 0) fLeft->walk(fn, obj);
    if (fRight != 0) fRight->walk(fn, obj);
        fn(this, obj);
}

template <class T>
void ExpressionTree<T>::walk(void (*fn)(ExpressionTree* n, void* obj), void* obj) const
{
    if (fLeft != 0) fLeft->walk(fn, obj);
    if (fRight != 0) fRight->walk(fn, obj);
        fn(const_cast<ExpressionTree<T>*>(this), obj);
}

template <class T>
ExpressionTree<T>& ExpressionTree<T>::operator=(const ExpressionTree& rhs)
{
    if (this != &rhs)
    {  
        copyTree(*this, rhs); 
    } 
    return *this;
}

template <class T>
void ExpressionTree<T>::copyTree(const ExpressionTree &src)
{
    if (fLeft != NULL)
        fLeft->destroyPointerTree();
    if (fRight != NULL)
        fRight->destroyPointerTree();
    fLeft = NULL;
    fRight = NULL;
    
    if (src.left() != NULL)
    {
        fLeft = new ExpressionTree<T>();
        fLeft->copyTree(*(src.left()));
    }
    if (src.right() != NULL)
    {
        fRight = new ExpressionTree<T>();
        fRight->copyTree(*(src.right()));
    }
    
    delete fData;
    if (src.data() == NULL)
        fData = NULL;
    else
        fData = src.data()->clone();
} 

template <class T>
void ExpressionTree<T>::destroyTree(ExpressionTree* root)
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

template <class T>
void ExpressionTree<T>::draw(const ExpressionTree<T>* n, std::ostream& dotFile)
{
    const ExpressionTree<T>* r;
    const ExpressionTree<T>* l;
    l = n->left();
    r = n->right();
    
    if (l != 0)
    	dotFile << "n" << (u_long)n << " -> " << "n" << (u_long)l << std::endl;
    if (r != 0)
    	dotFile << "n" << (u_long)n << " -> " << "n" << (u_long)r << std::endl;
    dotFile << "n" << (u_long)n << " [label=\"" 
            << n->data()->data() << "\"]" << std::endl;
}

template <class T>
void ExpressionTree<T>::drawTree(std::string filename)   
{
    std::ofstream dotFile (filename.c_str(), std::ios::out);
    
    dotFile << "digraph G {" << std::endl;
    walk (draw, dotFile);
    dotFile << "}" << std::endl;
    
    dotFile.close();	    
}

template <class T>
void ExpressionTree<T>::evaluate(rowgroup::Row& row)
{
	// Non-leaf node is operator. leaf node is SimpleFilter for logical expression, 
	// or ReturnedColumn for arithmetical expression.
	if (fLight && fRight)
	{
		Operator* = reinterpret_cast<Operator*>(fData);
	}
	if (!fLight && !fRight)
	{
		
	}
}

} // namespace

#endif
