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

# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags) -lreadline -lncurses
bin_PROGRAMS = postConfigure installer patchInstaller amazonInstaller mycnfUpgrade
postConfigure_SOURCES = postConfigure.cpp helpers.cpp
postConfigure_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
postConfigure_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) -lreadline -lncurses $(AM_LDFLAGS)
installer_SOURCES = installer.cpp helpers.cpp
installer_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
installer_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)
patchInstaller_SOURCES = patchInstaller.cpp
patchInstaller_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
patchInstaller_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)
amazonInstaller_SOURCES = amazonInstaller.cpp helpers.cpp
amazonInstaller_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
amazonInstaller_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)
mycnfUpgrade_SOURCES = mycnfUpgrade.cpp
mycnfUpgrade_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
mycnfUpgrade_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)


test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

