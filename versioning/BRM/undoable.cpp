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

/*****************************************************************************
 * $Id: undoable.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <stdexcept>

#define UNDOABLE_DLLEXPORT
#include "undoable.h"
#undef UNDOABLE_DLLEXPORT

using namespace std;

namespace BRM {

Undoable::Undoable()
{
}

Undoable::~Undoable() 
{
}

inline void Undoable::makeUndoRecord(void *start, int size)
{
	ImageDelta d;

	if (size > ID_MAXSIZE)
		throw overflow_error("Undoable::makeUndoRecord(): size > max");

	d.start = start;
	d.size = size;
	memcpy(d.data, start, size);
 	undoRecords.push_back(d);
}

void Undoable::undoChanges()
{
	vector<ImageDelta>::reverse_iterator it;

	for (it = undoRecords.rbegin(); it != undoRecords.rend(); it++)
		memcpy((*it).start, (*it).data, (*it).size);

	undoRecords.clear();
}

void Undoable::confirmChanges()
{
	undoRecords.clear();
}

}
