# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = ProcMon
ProcMon_SOURCES = main.cpp processmonitor.cpp
ProcMon_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
ProcMon_LDFLAGS = $(idb_common_ldflags) $(idb_brm_libs) $(idb_oam_libs) $(idb_exec_libs) -lcacheutils $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

