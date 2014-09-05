# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = sendtrap
sendtrap_SOURCES = sendtrap.cpp
sendtrap_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
sendtrap_LDFLAGS = @idb_common_ldflags@ @idb_common_libs@ @idb_write_libs@ @netsnmp_libs@ $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

