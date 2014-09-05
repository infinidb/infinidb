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

#ifndef STDINT_H
#define STDINT_H 1

#ifdef __cplusplus

#include <boost/cstdint.hpp>

using boost::uint8_t;
using boost::uint16_t;
using boost::uint32_t;
using boost::uint64_t;

using boost::int8_t;
using boost::int16_t;
using boost::int32_t;
using boost::int64_t;

typedef boost::int64_t ssize_t;

typedef boost::uint8_t u_int8_t;
typedef boost::uint16_t u_int16_t;
typedef boost::uint32_t u_int32_t;
typedef boost::uint64_t u_int64_t;

#endif //__cplusplus

#ifndef _WINSOCK2API_
typedef unsigned long u_long;
#endif
#ifndef HAVE_UINT
typedef unsigned int uint;
#endif
typedef int key_t;
typedef int pid_t;
typedef int mode_t;
typedef int clockid_t;
typedef int uid_t;

struct lldiv_t__
{
	long long quot;
	long long rem;
};
typedef struct lldiv_t__ lldiv_t;

typedef long long off64_t;

#endif
