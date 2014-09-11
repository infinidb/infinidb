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
AM_LDFLAGS = $(idb_ldflags)
lib_LTLIBRARIES = libbrm.la
libbrm_la_SOURCES = blockresolutionmanager.cpp \
brmshmimpl.cpp \
brmtypes.cpp \
copylocks.cpp \
dbrm.cpp \
extentmap.cpp \
lbidresourcegraph.cpp \
mastersegmenttable.cpp \
resourcenode.cpp \
rgnode.cpp \
rwlockmonitor.cpp \
sessionmanagerserver.cpp \
shmkeys.cpp \
slavedbrmnode.cpp \
slavecomm.cpp \
tracer.cpp \
transactionnode.cpp \
undoable.cpp \
vbbm.cpp \
vss.cpp
libbrm_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)
libbrm_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)
include_HEADERS = blockresolutionmanager.h \
brm.h \
brmshmimpl.h \
brmtypes.h \
copylocks.h \
dbrm.h \
extentmap.h \
mastersegmenttable.h \
sessionmanagerserver.h \
shmkeys.h \
tracer.h \
undoable.h \
vbbm.h \
vss.h

bin_PROGRAMS = controllernode workernode dbrmctl reset_locks rollback save_brm load_brm print_journal

controllernode_SOURCES = masternode.cpp masterdbrmnode.cpp
controllernode_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
controllernode_LDADD = libbrm.la

workernode_SOURCES = slavenode.cpp
workernode_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
workernode_LDADD = libbrm.la

dbrmctl_SOURCES = dbrmctl.cpp
dbrmctl_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
dbrmctl_LDADD = libbrm.la

reset_locks_SOURCES = reset_locks.cpp
reset_locks_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
reset_locks_LDADD = libbrm.la

rollback_SOURCES = rollback.cpp
rollback_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
rollback_LDADD = libbrm.la

save_brm_SOURCES = save_brm.cpp
save_brm_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
save_brm_LDADD = libbrm.la

load_brm_SOURCES = load_brm.cpp
load_brm_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
load_brm_LDADD = libbrm.la

print_journal_SOURCES = print_journal.cpp
print_journal_LDFLAGS = $(idb_common_ldflags) $(idb_oam_libs) $(idb_brm_libs) $(netsnmp_libs)
print_journal_LDADD = libbrm.la

.PHONY: dbrm
.PRECIOUS: dbrm
dbrm: controllernode workernode dbrmctl 

tools: reset_locks rollback save_brm load_brm print_journal

install_dbrm:

install_tools:

test-dbrm:

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

