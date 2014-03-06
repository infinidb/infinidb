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

# $Id: Makefile.am 864 2009-04-02 19:22:49Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags) $(idb_common_includes)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
noinst_LIBRARIES = libwe_xml.a
libwe_xml_a_SOURCES = we_xmlop.cpp we_xmljob.cpp we_xmlgendata.cpp we_xmlgenproc.cpp
libwe_xml_a_CPPFLAGS = -D_FILE_OFFSET_BITS=64 $(AM_CPPFLAGS)
include_HEADERS = we_xmljob.h we_xmlop.h we_xmltag.h we_xmlgendata.h we_xmlgenproc.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

