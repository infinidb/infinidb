# Copyright (C) 2014 InfiniDB, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.

# $Id$
## Process this file with automake to produce Makefile.in


AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libboost_idb.la
libboost_idb_la_SOURCES = \
	c_regex_traits.cpp \
	cpp_regex_traits.cpp \
	cregex.cpp \
	date_generators.cpp \
	error_code.cpp \
	fileiter.cpp \
	greg_month.cpp \
	greg_weekday.cpp \
	gregorian_types.cpp \
	icu.cpp \
	instances.cpp \
	once.cpp \
	operations.cpp \
	path.cpp \
	portability.cpp \
	posix_api.cpp \
	posix_time_types.cpp \
	regex.cpp \
	regex_debug.cpp \
	regex_raw_buffer.cpp \
	regex_traits_defaults.cpp \
	static_mutex.cpp \
	thread.cpp \
	usinstances.cpp \
	w32_regex_traits.cpp \
	wc_regex_traits.cpp \
	wide_posix_api.cpp \
	winstances.cpp
include_HEADERS =

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

install-data-hook:
	cp -Rpf boost $(DESTDIR)$(includedir)

