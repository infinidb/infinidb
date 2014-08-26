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
AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
lib_LTLIBRARIES = libcalmysql.la
libcalmysql_la_SOURCES = ha_calpont.cpp ha_calpont_impl.cpp ha_calpont_dml.cpp ha_calpont_ddl.cpp ha_calpont_execplan.cpp ha_scalar_sub.cpp ha_in_sub.cpp ha_exists_sub.cpp ha_from_sub.cpp ha_select_sub.cpp ha_view.cpp sm.cpp ha_window_function.cpp ha_calpont_partition.cpp ha_pseudocolumn.cpp
libcalmysql_la_LDFLAGS = -version-info 1:0:0 $(idb_common_ldflags) $(idb_common_libs) $(idb_write_libs) $(AM_LDFLAGS)
libcalmysql_la_CPPFLAGS = -I/usr/include/libxml2 -I../../../mysql/include -I../../../mysql/sql -I../../../mysql/regex -DMYSQL_DYNAMIC_PLUGIN $(AM_CPPFLAGS)
include_HEADERS = idb_mysql.h

dist_mysql_DATA = syscatalog_mysql.sql dumpcat_mysql.sql calsetuserpriority.sql calremoveuserpriority.sql calshowprocesslist.sql my.cnf
dist_mysql_SCRIPTS = install_calpont_mysql.sh mysql-Calpont dumpcat.pl pre-mysql-upgrade.sh post-mysql-upgrade.sh

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

