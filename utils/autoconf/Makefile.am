# Copyright (C) 2014 InfiniDB, Inc.
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

# $Id$

ACLOCAL_AMFLAGS = -I ./m4

all-local: bootstrap
	cd utils && $(MAKE) && $(MAKE) install
	cd oam; \
		$(MAKE) -C oamcpp install
	cd snmpd && $(MAKE) && $(MAKE) install
	cd dbcon; \
		$(MAKE) -C execplan install; \
		$(MAKE) -C joblist install
	cd versioning && $(MAKE) && $(MAKE) install
	cd oam && $(MAKE) && $(MAKE) install
	cd versioning && $(MAKE) dbrm tools && $(MAKE) install_dbrm install_tools
	cd writeengine; \
		$(MAKE) -C wrapper install; \
		$(MAKE) -C client install; \
		$(MAKE) -C xml install; \
		$(MAKE) -C redistribute install
	cd dbcon; \
		$(MAKE) -C ddlpackage install; \
		$(MAKE) -C ddlpackageproc install; \
		$(MAKE) -C dmlpackage install; \
		$(MAKE) -C dmlpackageproc install
	cd dbcon && $(MAKE) && $(MAKE) install
	cd exemgr && $(MAKE) && $(MAKE) install
	cd ddlproc && $(MAKE) && $(MAKE) install
	cd dmlproc && $(MAKE) && $(MAKE) install
	cd procmon && $(MAKE) && $(MAKE) install
	cd procmgr && $(MAKE) && $(MAKE) install
	cd oamapps && $(MAKE) && $(MAKE) install
	cd decomsvr && $(MAKE) && $(MAKE) install
	cd primitives && $(MAKE) && $(MAKE) install
	cd tools && $(MAKE) && $(MAKE) install
	cd versioning && $(MAKE) tools install_tools
	$(MAKE) -C writeengine/server install_server
	$(MAKE) -C writeengine/bulk install_bulk
	$(MAKE) -C writeengine/splitter install_splitter
	echo $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags

compile: all-local

.PHONY: test coverage leakcheck docs bootstrap
test:

coverage:

leakcheck:

docs:

bootstrap:
	./build/genVersion.sh --prefix=@prefix@/Calpont
	for subdir in dbcon ddlproc dmlproc exemgr mysql net-snmp oam \
		oamapps decomsvr primitives procmgr procmon qa snmpd tools utils versioning \
		writeengine ; do \
		if test -f $$subdir/Makefile; then \
			$(MAKE) BOOTSTRAP=1 -C $$subdir bootstrap || exit $$?; \
		fi; \
	done

clean: clean-am
	for subdir in dbcon ddlproc dmlproc exemgr mysql net-snmp oam \
		oamapps decomsvr primitives procmgr procmon qa snmpd tools utils versioning \
		writeengine ; do \
		if test -f $$subdir/Makefile; then \
			$(MAKE) BOOTSTRAP=1 -C $$subdir clean || exit $$?; \
		fi; \
	done

