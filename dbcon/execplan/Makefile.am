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

# $Id: Makefile.am 864 2009-04-02 19:22:49Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libexecplan.la
libexecplan_la_SOURCES = calpontsystemcatalog.cpp \
aggregatecolumn.cpp \
arithmeticcolumn.cpp \
calpontexecutionplan.cpp \
calpontexecutionplanfactory.cpp \
calpontselectexecutionplan.cpp \
constantcolumn.cpp \
existsfilter.cpp \
filter.cpp \
functioncolumn.cpp \
groupconcatcolumn.cpp \
mysqlexecutionplan.cpp \
objectidmanager.cpp \
objectreader.cpp \
operator.cpp \
outerjoinonfilter.cpp \
oracleexecutionplan.cpp \
range.cpp \
returnedcolumn.cpp \
rowcolumn.cpp \
selectfilter.cpp \
sessionmanager.cpp \
sessionmonitor.cpp \
simplecolumn.cpp \
simplefilter.cpp \
simplescalarfilter.cpp \
vendorexecutionplan.cpp \
treenode.cpp \
expressionparser.cpp \
treenodeimpl.cpp \
constantfilter.cpp \
clientrotator.cpp \
arithmeticoperator.cpp \
logicoperator.cpp \
predicateoperator.cpp
include_HEADERS = aggregatecolumn.h \
arithmeticcolumn.h \
calpontexecutionplanfactory.h \
calpontexecutionplan.h \
calpontselectexecutionplan.h \
columnresult.h \
constantcolumn.h \
existsfilter.h \
filter.h \
functioncolumn.h \
groupconcatcolumn.h \
mysqlexecutionplan.h \
njlcolumnresult.h \
objectidmanager.h \
objectreader.h \
operator.h \
outerjoinonfilter.h \
oracleexecutionplan.h \
range.h \
returnedcolumn.h \
rowcolumn.h \
selectfilter.h \
sessionmanager.h \
sessionmonitor.h \
simplecolumn.h \
simplefilter.h \
simplescalarfilter.h \
vendorexecutionplan.h \
calpontsystemcatalog.h \
treenode.h \
parsetree.h \
expressionparser.h \
treenodeimpl.h \
constantfilter.h \
clientrotator.h \
blocksize.h \
exp_templates.h \
arithmeticoperator.h \
logicoperator.h \
predicateoperator.h \
simplecolumn_int.h \
simplecolumn_decimal.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

