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

# $Id: Makefile.am 864 2009-04-02 19:22:49Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags) $(idb_common_includes)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
noinst_LIBRARIES = libwe_bulk.a
libwe_bulk_a_SOURCES = \
	we_brmreporter.cpp \
	we_bulkload.cpp \
	we_bulkloadbuffer.cpp \
	we_bulkstatus.cpp \
	we_colopbulk.cpp \
	we_colbuf.cpp \
	we_colbufcompressed.cpp \
	we_colbufmgr.cpp \
	we_colbufmgrdctnry.cpp \
	we_colbufsec.cpp \
	we_colextinf.cpp \
	we_columninfo.cpp \
	we_columninfocompressed.cpp \
	we_columnautoinc.cpp \
	we_extentstripealloc.cpp \
	we_tableinfo.cpp \
	we_tempxmlgendata.cpp \
	we_workers.cpp
libwe_bulk_a_CPPFLAGS = $(march_flags) -D_FILE_OFFSET_BITS=64 $(AM_CPPFLAGS)

bin_PROGRAMS = cpimport.bin
cpimport_bin_SOURCES = cpimport.cpp
cpimport_bin_LDFLAGS = $(idb_common_ldflags) $(idb_write_libs) $(AM_LDFLAGS)
cpimport_bin_LDADD = libwe_bulk.a ../xml/libwe_xml.a
cpimport_bin_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

install_bulk: install

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

