# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = ServerMonitor
ServerMonitor_SOURCES = main.cpp serverMonitor.cpp cpuMonitor.cpp diskMonitor.cpp \
memoryMonitor.cpp procmonMonitor.cpp msgProcessor.cpp dbhealthMonitor.cpp
ServerMonitor_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
ServerMonitor_LDFLAGS = @idb_common_ldflags@ @idb_common_libs@ @idb_oam_libs@ $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

