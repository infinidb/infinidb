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
AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libddlpackage.la
libddlpackage_la_SOURCES = serialize.cpp \
ddl-scan.cpp \
ddl-gram.cpp \
ddlpkg.cpp \
columndef.cpp \
createtable.cpp \
tabledef.cpp \
sqlstatement.cpp \
sqlstatementlist.cpp \
altertable.cpp \
createindex.cpp \
dropindex.cpp \
droptable.cpp \
sqlparser.cpp \
markpartition.cpp \
restorepartition.cpp \
droppartition.cpp 
include_HEADERS = ddlpkg.h \
sqlparser.h

ddl-gram.h ddl-gram.cpp: ddl.y
	$(YACC) -l -v -d -p ddl -o ddl-gram-temp.cpp ddl.y
	set +e; \
	if [ -f ddl-gram.cpp ]; \
		then diff -abBq ddl-gram-temp.cpp ddl-gram.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
			else touch ddl-gram.cpp; \
		fi; \
		else mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
	fi
	set +e; \
	if [ -f ddl-gram.h ]; \
		then diff -abBq ddl-gram-temp.hpp ddl-gram.h >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-gram-temp.hpp ddl-gram.h; \
			else touch ddl-gram.h; \
		fi; \
		else mv -f ddl-gram-temp.hpp ddl-gram.h; \
	fi
	rm -f ddl-gram-temp.cpp ddl-gram-temp.hpp ddl-gram-temp.output

ddl-scan.cpp: ddl.l
	$(LEX) -i -L -Pddl -oddl-scan-temp.cpp ddl.l
	set +e; \
	if [ -f ddl-scan.cpp ]; \
		then diff -abBq ddl-scan-temp.cpp ddl-scan.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-scan-temp.cpp ddl-scan.cpp; \
			else touch ddl-scan.cpp; \
		fi; \
		else mv -f ddl-scan-temp.cpp ddl-scan.cpp; \
	fi
	rm -f ddl-scan-temp.cpp

ddl.l: ddl-gram.h

test:

coverage:

leakcheck:

docs:

bootstrap: ddl-gram.h ddl-gram.cpp ddl-scan.cpp install-data-am

