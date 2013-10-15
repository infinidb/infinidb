# Copyright (C) 2013 Calpont Corp.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.

# $Id: Makefile.am 4038 2013-08-07 14:02:48Z rdempsey $

SUBDIRS = boost_idb startup common configcpp loggingcpp messageqcpp threadpool rwlock dataconvert joiner rowgroup cacheutils multicast funcexp udfsdk compress batchloader ddlcleanup mysqlcl_idb querystats jemalloc windowfunction idbdatafile idbhdfs

test:

coverage:

leakcheck:

docs:

bootstrap:
	for subdir in $(SUBDIRS); \
		do $(MAKE) -C $$subdir bootstrap || exit $$?; \
	done

