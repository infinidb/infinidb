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

# $Id: Makefile.am 3720 2012-04-04 18:18:49Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags) $(idb_common_includes)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
lib_LTLIBRARIES = libwriteengine.la
libwriteengine_la_SOURCES = writeengine.cpp \
	we_colop.cpp \
	we_colopcompress.cpp \
	we_dctnrycompress.cpp \
	we_tablemetadata.cpp \
	../shared/we_blockop.cpp \
	../shared/we_brm.cpp \
	../shared/we_cache.cpp \
	../shared/we_chunkmanager.cpp \
	../shared/we_config.cpp \
	../shared/we_convertor.cpp \
	../shared/we_dbfileop.cpp \
	../shared/we_define.cpp \
	../shared/we_fileop.cpp \
	../shared/we_log.cpp \
	../shared/we_stats.cpp \
	../shared/we_bulkrollbackmgr.cpp \
	../shared/we_simplesyslog.cpp \
	../shared/we_bulkrollbackfilecompressed.cpp \
	../shared/we_bulkrollbackfile.cpp \
	../shared/we_rbmetawriter.cpp \
	../shared/we_dbrootextenttracker.cpp \
	../dictionary/we_dctnry.cpp \
	../xml/we_xmlop.cpp \
	../xml/we_xmljob.cpp \
	../xml/we_xmlgendata.cpp \
	../xml/we_xmlgenproc.cpp
libwriteengine_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)
libwriteengine_la_CPPFLAGS = -D_FILE_OFFSET_BITS=64 $(AM_CPPFLAGS)
libwriteengine_la_LIBADD =
include_HEADERS = writeengine.h we_colop.h we_colopcompress.h we_dctnrycompress.h we_tablemetadata.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

