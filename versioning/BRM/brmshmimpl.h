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
 * $Id$
 *
 *****************************************************************************/

/** @file 
 * class BRMShmImpl
 */

#ifndef IDBSHMIMPL_H_
#define IDBSHMIMPL_H_

#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

namespace BRM {

class BRMShmImpl
{
public:
	BRMShmImpl(unsigned key, off_t size, bool readOnly=false);
	~BRMShmImpl() { }

	inline unsigned key() const { return fKey; }
	inline off_t size() const { return fSize; }
	inline bool isReadOnly() const { return fReadOnly; }

	void setReadOnly();
	int grow(unsigned newKey, off_t newSize);
	int clear(unsigned newKey, off_t newSize);

	void swap(BRMShmImpl& rhs);
	void destroy();

	boost::interprocess::shared_memory_object fShmobj;
	boost::interprocess::mapped_region fMapreg;

private:
	BRMShmImpl(const BRMShmImpl& rhs);
	BRMShmImpl& operator=(const BRMShmImpl& rhs);

	unsigned fKey;
	off_t fSize;
	bool fReadOnly;
};

} //namespace

#endif
