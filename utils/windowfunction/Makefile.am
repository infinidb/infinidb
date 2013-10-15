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

# $Id: Makefile.am 3821 2013-05-17 23:58:16Z xlou $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libwindowfunction.la
libwindowfunction_la_SOURCES = \
	framebound.cpp \
	frameboundrange.cpp \
	frameboundrow.cpp \
	idborderby.cpp \
	windowframe.cpp \
	windowfunction.cpp \
	windowfunctiontype.cpp \
	wf_count.cpp \
	wf_lead_lag.cpp \
	wf_min_max.cpp \
	wf_nth_value.cpp \
	wf_ntile.cpp \
	wf_percentile.cpp \
	wf_ranking.cpp \
	wf_row_number.cpp \
	wf_stats.cpp \
	wf_sum_avg.cpp

include_HEADERS = \
	framebound.h \
	frameboundrange.h \
	frameboundrow.h \
	idborderby.h \
	windowframe.h \
	windowfunction.h \
	windowfunctiontype.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

