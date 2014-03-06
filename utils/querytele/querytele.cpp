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

#include <unistd.h>
#include <stdint.h>
#include <ctime>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/thread/mutex.hpp>
using namespace boost;

#include "querytele.h"

namespace
{
//It's not clear that random_generator is thread-safe, so we'll just mutex it...
uuids::random_generator uuidgen;
mutex uuidgenMtx;
}

namespace querytele
{

/*static*/
uuids::uuid QueryTeleClient::genUUID()
{
	mutex::scoped_lock lk(uuidgenMtx);
	return uuidgen();
}

/*static*/
int64_t QueryTeleClient::timeNowms()
{
	int64_t nowms=-1;
	struct timeval tv;

	if (gettimeofday(&tv, 0) == 0)
		nowms = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);

	return nowms;
}

}

