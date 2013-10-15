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

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = 
libunwind_la_SOURCES = 
include_HEADERS = libunwind.h \
demangle.h \
libunwind-x86_64.h \
libiberty.h \
libunwind-dynamic.h \
libunwind-common.h \
ansidecl.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am
	mkdir -p $(libdir)
	$(install_sh) libunwind.so.8.0.1 $(libdir)
	$(install_sh) libunwind-x86_64.so.8.0.1 $(libdir)
	$(install_sh) libiberty.a $(libdir)

