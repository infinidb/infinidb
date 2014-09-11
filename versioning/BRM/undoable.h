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

/******************************************************************************
 * $Id: undoable.h 1266 2011-02-08 14:36:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * This class defines the interface the BRM shared structures inherit
 * to support revoking any changes made on error.
 */

#ifndef _UNDOABLE_H_
#define _UNDOABLE_H_

#include <vector>

#include "brmtypes.h"

#if defined(_MSC_VER) && defined(UNDOABLE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class Undoable
{
	public:
		EXPORT Undoable();
		EXPORT virtual ~Undoable();

		EXPORT virtual void confirmChanges();
		EXPORT virtual void undoChanges();

	protected:
		virtual void makeUndoRecord(void *start, int size);
 		std::vector<ImageDelta> undoRecords;
};

#undef EXPORT

}

#endif
