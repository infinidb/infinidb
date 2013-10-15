# $Id: Makefile.am 2425 2011-02-24 20:54:35Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
# comes out as libstatgrab.so.6.2.0
AM_LDFLAGS = -version-info 8:0:2 $(idb_ldflags)
lib_LTLIBRARIES = libstatgrab.la
libstatgrab_la_SOURCES = cpu_stats.c \
disk_stats.c \
load_stats.c \
memory_stats.c \
network_stats.c \
os_info.c \
page_stats.c \
process_stats.c \
swap_stats.c \
user_stats.c \
tools.c \
vector.c \
error.c \
statgrab_deprecated.c
include_HEADERS = statgrab.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

