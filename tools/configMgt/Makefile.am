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

# $Id: Makefile.am 1929 2012-09-14 16:18:25Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = autoInstaller autoConfigure svnQuery

autoInstaller_SOURCES = autoInstaller.cpp
autoInstaller_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
autoInstaller_LDFLAGS = $(idb_common_ldflags) $(idb_brm_libs) $(idb_oam_libs) $(idb_exec_libs) -lreadline -lncurses $(AM_LDFLAGS)

autoConfigure_SOURCES = autoConfigure.cpp
autoConfigure_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
autoConfigure_LDFLAGS = $(idb_common_ldflags) $(idb_brm_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)

svnQuery_SOURCES = svnQuery.cpp
svnQuery_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
svnQuery_LDFLAGS = $(idb_common_ldflags) $(idb_brm_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

