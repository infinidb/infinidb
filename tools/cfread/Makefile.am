# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
bin_PROGRAMS = cfread mtread
cfread_SOURCES = cfread.cpp
cfread_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
cfread_LDFLAGS = @idb_common_ldflags@ -lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp -ldl -lconfigcpp -lxml2 -lloggingcpp -lcacheutils -lboost_idb -lpthread -lrt $(AM_LDFLAGS)
mtread_SOURCES = mtread.cpp
mtread_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
mtread_LDFLAGS = @idb_common_ldflags@ -lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp -ldl -lconfigcpp -lxml2 -lloggingcpp -lcacheutils -lboost_idb -lpthread -lrt $(AM_LDFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

