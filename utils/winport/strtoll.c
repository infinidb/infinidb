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

// $Id$

#include <stdlib.h>

#include "unistd.h"

long long llabs(const long long in)
{
	if (in < 0) return -in;
	return in;
}

long long atoll(const char* nptr)
{
	return idb_strtoll(nptr, 0, 0);
}

unsigned long long idb_strtoull(const char *nptr, char **endptr, int base)
{
	long long out;
	out = (long long)_strtoui64(nptr, endptr, base);
	return out;
}

long long idb_strtoll(const char *nptr, char **endptr, int base)
{
	long long out;
	out = (long long)_strtoi64(nptr, endptr, base);
	return out;
}
