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
*   $Id: treenode.cpp 9317 2013-03-19 21:37:22Z dhall $
*
*
***********************************************************************/
/** @file */

#include <unistd.h>
#include <string>
#include <exception>
#include <typeinfo>
#include <cstring>

#include "bytestream.h"
#include "treenode.h"
#include "objectreader.h"

using namespace std;
namespace execplan{

/**
 * Constructors/Destructors
 */
TreeNode::TreeNode(): fDerivedTable(""),
                      fRefCount(0),
                      fDerivedRefCol(NULL)
{ memset(tmp, 0, 312); }

TreeNode::TreeNode(const TreeNode& rhs):
           fResult(rhs.fResult),
           fResultType(rhs.resultType()),
           fOperationType(rhs.operationType()),
           fRegex (rhs.regex()),
           fDerivedTable (rhs.derivedTable()),
           fRefCount(rhs.refCount()),
           fDerivedRefCol(rhs.derivedRefCol())
{ memcpy(tmp, rhs.tmp, 312); }

TreeNode::~TreeNode() {}

void TreeNode::resultType ( const execplan::CalpontSystemCatalog::ColType& resultType)
{
	fResultType = resultType;

	// set scale/precision for the result
	if (fResultType.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
	    fResultType.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
	{
		fResult.decimalVal.scale = fResultType.scale;
		fResult.decimalVal.precision = fResultType.precision;
	}
}

/**
 * ostream function
 */
ostream& operator<<(ostream& output, const TreeNode& rhs)
{
	output << rhs.toString();
	return output;
}

}  /* namespace */
