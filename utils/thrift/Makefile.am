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

# $Id: Makefile.am 3925 2013-06-20 14:08:39Z rdempsey $
## Process this file with automake to produce Makefile.in


AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags) -DTHRIFT_SQUELCH_CONSOLE_OUTPUT
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libthrift.la
libthrift_la_SOURCES = \
	thrift/Thrift.cpp \
	thrift/server/TSimpleServer.cpp \
	thrift/transport/TSocket.cpp \
	thrift/transport/TServerSocket.cpp \
	thrift/transport/TBufferTransports.cpp \
	thrift/TApplicationException.cpp
include_HEADERS = 

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

install-data-hook:
	cp -Rpf thrift $(DESTDIR)$(includedir)

