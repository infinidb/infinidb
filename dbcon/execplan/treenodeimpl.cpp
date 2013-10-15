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
*   $Id: treenodeimpl.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <string>
using namespace std;

#include "bytestream.h"
#include "treenodeimpl.h"
#include "objectreader.h"

namespace execplan {
/**
 * Constructors/Destructors
 */
TreeNodeImpl::TreeNodeImpl()
{}

TreeNodeImpl::TreeNodeImpl(const string& sql) :
    fData(sql)
{}

TreeNodeImpl::~TreeNodeImpl()
{}

/**
 * The serialization interface
 */
void TreeNodeImpl::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::TREENODEIMPL;
	b << fData;
}

void TreeNodeImpl::unserialize(messageqcpp::ByteStream& b)
{
	ObjectReader::checkType(b, ObjectReader::TREENODEIMPL);
	b >> fData;
}
	
const string TreeNodeImpl::toString() const
{
	return string(">TreeNodeImpl<");
}

bool TreeNodeImpl::operator==(const TreeNodeImpl& t) const
{
	if (fData == t.fData)
		return true;
	return false;
}

bool TreeNodeImpl::operator==(const TreeNode* t) const
{
	const TreeNodeImpl *tni;
	
	tni = dynamic_cast<const TreeNodeImpl*>(t);
	if (tni == NULL)
		return false;
	return *this == *tni;
}

bool TreeNodeImpl::operator!=(const TreeNodeImpl& t) const
{
	return !(*this == t);
}

bool TreeNodeImpl::operator!=(const TreeNode* t) const
{
	return !(*this == t);
}

/**
 * Friend function
 */
ostream& operator<<(ostream& output, const TreeNodeImpl& rhs)
{
	output << rhs.toString();
	return output;
} 

} // namespace execplan
