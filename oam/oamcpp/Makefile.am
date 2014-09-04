#  Copyright (C) 2014 InfiniDB, Inc.
#
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; version 2 of
#  the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.

# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
lib_LTLIBRARIES = liboamcpp.la
liboamcpp_la_SOURCES = liboamcpp.cpp oamcache.cpp
liboamcpp_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)
liboamcpp_la_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
include_HEADERS = liboamcpp.h oamcache.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

