# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = calpontConsole
calpontConsole_SOURCES = calpontConsole.cpp
calpontConsole_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
calpontConsole_LDFLAGS = @idb_common_ldflags@ @idb_common_libs@ @idb_oam_libs@ -lreadline -lncurses $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

