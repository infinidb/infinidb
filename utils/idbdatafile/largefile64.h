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

#ifndef _LARGEFILE64_H
#define _LARGEFILE64_H

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#ifdef _MSC_VER
#ifndef off64_t
#define off64_t __int64
#endif
#define fseek64 _fseeki64
#define lseek64 _lseeki64
#define fstat64 _fstati64
#define ftell64 _ftelli64
#define stat64 _stati64
#endif

#ifdef __FreeBSD__
typedef int64_t off64_t;
#  ifndef O_LARGEFILE
#    define O_LARGEFILE 0
#  endif
#  ifndef O_NOATIME
#    define O_NOATIME 0
#  endif
#endif

#endif
