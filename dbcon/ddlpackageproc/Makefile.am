#  Copyright (C) 2013 Calpont Corp.
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
AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libddlpackageproc.la
libddlpackageproc_la_SOURCES = ddlpackageprocessor.cpp \
createtableprocessor.cpp \
altertableprocessor.cpp \
droptableprocessor.cpp \
markpartitionprocessor.cpp \
restorepartitionprocessor.cpp \
droppartitionprocessor.cpp 
include_HEADERS = ddlpackageprocessor.h \
createtableprocessor.h \
altertableprocessor.h \
droptableprocessor.h \
markpartitionprocessor.h \
restorepartitionprocessor.h \
droppartitionprocessor.h 

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

