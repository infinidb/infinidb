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
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libexecplan.la
libexecplan_la_SOURCES = calpontsystemcatalog.cpp \
aggregatecolumn.cpp \
arithmeticcolumn.cpp \
arithmeticoperator.cpp \
calpontexecutionplan.cpp \
calpontexecutionplanfactory.cpp \
calpontselectexecutionplan.cpp \
clientrotator.cpp \
constantcolumn.cpp \
constantfilter.cpp \
existsfilter.cpp \
expressionparser.cpp \
filter.cpp \
functioncolumn.cpp \
groupconcatcolumn.cpp \
intervalcolumn.cpp \
logicoperator.cpp \
mysqlexecutionplan.cpp \
objectidmanager.cpp \
objectreader.cpp \
operator.cpp \
oracleexecutionplan.cpp \
outerjoinonfilter.cpp \
predicateoperator.cpp \
pseudocolumn.cpp \
range.cpp \
returnedcolumn.cpp \
rowcolumn.cpp \
selectfilter.cpp \
sessionmanager.cpp \
simplecolumn.cpp \
simplefilter.cpp \
simplescalarfilter.cpp \
treenode.cpp \
treenodeimpl.cpp \
vendorexecutionplan.cpp \
windowfunctioncolumn.cpp

include_HEADERS = aggregatecolumn.h \
arithmeticcolumn.h \
arithmeticoperator.h \
blocksize.h \
calpontexecutionplanfactory.h \
calpontexecutionplan.h \
calpontsystemcatalog.h \
calpontselectexecutionplan.h \
clientrotator.h \
columnresult.h \
constantcolumn.h \
constantfilter.h \
existsfilter.h \
expressionparser.h \
exp_templates.h \
filter.h \
functioncolumn.h \
groupconcatcolumn.h \
intervalcolumn.h \
logicoperator.h \
mysqlexecutionplan.h \
njlcolumnresult.h \
objectidmanager.h \
objectreader.h \
operator.h \
oracleexecutionplan.h \
outerjoinonfilter.h \
parsetree.h \
predicateoperator.h \
pseudocolumn.h \
range.h \
returnedcolumn.h \
rowcolumn.h \
selectfilter.h \
sessionmanager.h \
simplecolumn.h \
simplecolumn_decimal.h \
simplecolumn_int.h \
simplecolumn_uint.h \
simplefilter.h \
simplescalarfilter.h \
treenode.h \
treenodeimpl.h \
vendorexecutionplan.h \
windowfunctioncolumn.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

