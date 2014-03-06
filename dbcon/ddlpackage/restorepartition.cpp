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
*   $Id: markpartition.cpp 6566 2010-04-27 18:02:51Z rdempsey $
*
*
***********************************************************************/

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

using namespace std;

namespace ddlpackage {

RestorePartitionStatement::RestorePartitionStatement(QualifiedName *qualifiedName) :
	fTableName(qualifiedName)
{
}

ostream& RestorePartitionStatement::put(ostream& os) const
{
	os << "Mark partition out of service: " << *fTableName;
	os << " partitions: ";
	set<BRM::LogicalPartition>::const_iterator it;
	for (it=fPartitions.begin(); it!=fPartitions.end(); ++it)
                os << (*it) << "  ";
	os << endl;
	return os;
}

}
