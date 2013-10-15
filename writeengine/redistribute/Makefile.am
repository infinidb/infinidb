# Copyright (C) 2013 Calpont Corp.
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

# $Id: Makefile.am 4218 2012-09-30 14:49:19Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libwriteengineredistribute.la
libwriteengineredistribute_la_SOURCES = we_redistribute.cpp \
we_redistributecontrol.cpp \
we_redistributecontrolthread.cpp \
we_redistributeworkerthread.cpp
include_HEADERS = we_redistributedef.h we_redistribute.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

