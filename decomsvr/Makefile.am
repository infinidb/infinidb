# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = DecomSvr
DecomSvr_SOURCES = quicklz.c server.cpp
DecomSvr_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
DecomSvr_LDFLAGS = $(idb_common_ldflags) -lboost_idb $(AM_LDFLAGS) -lpthread

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

