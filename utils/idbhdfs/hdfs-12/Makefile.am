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
#
# InfiniDB FOSS License Exception
# We want free and open source software applications under certain
# licenses to be able to use the GPL-licensed InfiniDB idbhdfs
# libraries despite the fact that not all such FOSS licenses are
# compatible with version 2 of the GNU General Public License.  
# Therefore there are special exceptions to the terms and conditions 
# of the GPLv2 as applied to idbhdfs libraries, which are 
# identified and described in more detail in the FOSS License 
# Exception in the file utils/idbhdfs/FOSS-EXCEPTION.txt
#

## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libhdfs-12.la
libhdfs_12_la_SOURCES = \
../hdfs-shared/HdfsFile.cpp \
../hdfs-shared/HdfsFileSystem.cpp \
../hdfs-shared/HdfsFsCache.cpp	\
../hdfs-shared/HdfsRdwrFileBuffer.cpp \
../hdfs-shared/HdfsRdwrMemBuffer.cpp \
../hdfs-shared/HdfsPlugin.cpp

include_HEADERS = 

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

