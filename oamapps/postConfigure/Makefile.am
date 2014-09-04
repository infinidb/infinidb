# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags) -lreadline -lncurses
bin_PROGRAMS = postConfigure installer patchInstaller
postConfigure_SOURCES = postConfigure.cpp helpers.cpp
postConfigure_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
postConfigure_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) -lreadline -lncurses $(AM_LDFLAGS)
installer_SOURCES = installer.cpp helpers.cpp
installer_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
installer_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)
patchInstaller_SOURCES = patchInstaller.cpp
patchInstaller_CPPFLAGS = $(idb_common_includes) $(AM_CPPFLAGS)
patchInstaller_LDFLAGS = $(idb_common_ldflags) $(idb_common_libs) $(idb_oam_libs) $(idb_exec_libs) $(AM_LDFLAGS)


test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

