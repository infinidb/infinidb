# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags) -I../../dbcon/mysql -I../../mysql/include -I../../mysql/sql -I../../mysql/regex -DMYSQL_DYNAMIC_PLUGIN
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libudfsdk.la
libudfsdk_la_SOURCES = udfinfinidb.cpp
include_HEADERS = udfsdk.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

