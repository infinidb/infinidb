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

#ifndef INTTYPES_H
#define INTTYPES_H 1

typedef int key_t;
typedef int pid_t;
typedef int mode_t;
typedef int clockid_t;
typedef int uid_t;
typedef __int64 ssize_t;
#ifndef HAVE_UINT
typedef unsigned int uint32_t;
#endif

#endif
