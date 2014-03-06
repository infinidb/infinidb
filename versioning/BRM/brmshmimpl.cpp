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
 * $Id$
 *
 ****************************************************************************/

#include <iostream>
#include <string>
//#define NDEBUG
#include <cassert>
#include <algorithm>
using namespace std;

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/version.hpp>
namespace bi=boost::interprocess;

#include "shmkeys.h"
#include "brmshmimpl.h"
#include "brmtypes.h"

namespace BRM
{

BRMShmImpl::BRMShmImpl(unsigned key, off_t size, bool readOnly) :
	fKey(key), fSize(size), fReadOnly(readOnly)
{
	string keyName = ShmKeys::keyToName(fKey);
	if (fSize == 0)
	{
		unsigned tries = 0;
again:
		try
		{
			bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
			off_t curSize = 0;
#ifdef _MSC_VER
			bi::offset_t tmp = 0;
			shm.get_size(tmp);
			curSize = static_cast<off_t>(tmp);
#else
			shm.get_size(curSize);
#endif
			if (curSize == 0) throw
#if BOOST_VERSION < 104500
				bi::interprocess_exception();
#else
				bi::interprocess_exception("shm size is zero");
#endif
		}
		catch (bi::interprocess_exception&)
		{
			if (++tries > 10) {
				log("BRMShmImpl::BRMShmImpl(): retrying on size==0");
				throw;
			}
			cerr << "BRMShmImpl::BRMShmImpl(): retrying on size==0" << endl;
			usleep(500 * 1000);
			goto again;
		}
	}
	try
	{
#if BOOST_VERSION < 104500
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
		{
			string pname = "/dev/shm/" + keyName;
			chmod(pname.c_str(), 0666);
		}
#endif
#else
		bi::permissions perms;
		perms.set_unrestricted();
		bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
		idbassert(fSize > 0);
		shm.truncate(fSize);
		fShmobj.swap(shm);
	}
	catch (bi::interprocess_exception&)
	{
		bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
		off_t curSize = 0;
#ifdef _MSC_VER
		bi::offset_t tmp = 0;
		shm.get_size(tmp);
		curSize = static_cast<off_t>(tmp);
#else
		shm.get_size(curSize);
#endif
		idbassert(curSize > 0);
		idbassert(curSize >= fSize);
		fShmobj.swap(shm);
		fSize = curSize;
	}
	catch (...)
	{
		throw;
	}

	if (fReadOnly)
	{
		bi::mapped_region ro_region(fShmobj, bi::read_only);
		fMapreg.swap(ro_region);
	}
	else
	{
		bi::mapped_region region(fShmobj, bi::read_write);
		fMapreg.swap(region);
	}
}

int BRMShmImpl::grow(unsigned newKey, off_t newSize)
{
	idbassert(newKey != fKey);
	idbassert(newSize >= fSize);

	string oldName = fShmobj.get_name();

	string keyName = ShmKeys::keyToName(newKey);
#if BOOST_VERSION < 104500
	bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
	{
		string pname = "/dev/shm/" + keyName;
		chmod(pname.c_str(), 0666);
	}
#endif
#else
	bi::permissions perms;
	perms.set_unrestricted();
	bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
	shm.truncate(newSize);

	bi::mapped_region region(shm, bi::read_write);

	//Copy old data into new region
	memcpy(region.get_address(), fMapreg.get_address(), fSize);
	//clear new region
	//make some versions of gcc happier...
	memset(reinterpret_cast<void*>(reinterpret_cast<ptrdiff_t>(region.get_address()) + fSize), 0, newSize - fSize);

	fShmobj.swap(shm);
	fMapreg.swap(region);

	if (!oldName.empty()) bi::shared_memory_object::remove(oldName.c_str());

	fKey = newKey;
	fSize = newSize;

	if (fReadOnly)
	{
		bi::mapped_region ro_region(fShmobj, bi::read_only);
		fMapreg.swap(ro_region);
	}

	return 0;
}

int BRMShmImpl::clear(unsigned newKey, off_t newSize)
{
	idbassert(newKey != fKey);

	string oldName = fShmobj.get_name();

	string keyName = ShmKeys::keyToName(newKey);
#if BOOST_VERSION < 104500
	bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write);
#ifdef __linux__
	{
		string pname = "/dev/shm/" + keyName;
		chmod(pname.c_str(), 0666);
	}
#endif
#else
	bi::permissions perms;
	perms.set_unrestricted();
	bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
#endif
	shm.truncate(newSize);

	bi::mapped_region region(shm, bi::read_write);

	//clear new region
	memset(region.get_address(), 0, newSize);

	fShmobj.swap(shm);
	fMapreg.swap(region);

	if (!oldName.empty()) bi::shared_memory_object::remove(oldName.c_str());

	fKey = newKey;
	fSize = newSize;

	if (fReadOnly)
	{
		bi::mapped_region ro_region(fShmobj, bi::read_only);
		fMapreg.swap(ro_region);
	}

	return 0;
}

void BRMShmImpl::setReadOnly()
{
	if (fReadOnly) return;

	bi::mapped_region ro_region(fShmobj, bi::read_only);
	fMapreg.swap(ro_region);

	fReadOnly = true;
}

void BRMShmImpl::swap(BRMShmImpl& rhs)
{
	fShmobj.swap(rhs.fShmobj);
	fMapreg.swap(rhs.fMapreg);
	std::swap(fKey, rhs.fKey);
	std::swap(fSize, rhs.fSize);
	std::swap(fReadOnly, rhs.fReadOnly);
}

void BRMShmImpl::destroy()
{
	string oldName = fShmobj.get_name();
	if (!oldName.empty()) bi::shared_memory_object::remove(oldName.c_str());
}

} //namespace

// vim:ts=4 sw=4:

