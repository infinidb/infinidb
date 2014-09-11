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

# $Id: Makefile.am 1310 2009-10-15 14:00:24Z rdempsey $

SUBDIRS = boost_idb common configcpp loggingcpp messageqcpp threadpool rwlock dataconvert joiner rowgroup cacheutils multicast funcexp compress

test:

coverage:

leakcheck:

docs:

bootstrap:
	for subdir in $(SUBDIRS); \
		do $(MAKE) -C $$subdir bootstrap || exit $$?; \
	done

