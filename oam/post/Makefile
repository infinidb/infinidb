include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 2827 2012-04-04 18:19:32Z rdempsey $
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

SCRIPTS=functions \
	test-001.sh \
	test-002.sh \
	test-003.sh \
	test-004.sh

bootstrap:
	umask 002; \
	mkdir -p $(INSTALL_ROOT_POST); \
	pushd $(INSTALL_ROOT_POST) >/dev/null; \
		rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(SCRIPTS) $(INSTALL_ROOT_POST)
	pushd $(INSTALL_ROOT_POST) >/dev/null; \
	chmod -f 555 $(SCRIPTS); \
	popd >/dev/null

clean:
	rm -f *~ 

install: bootstrap

test:

docs:

coverage:

leakcheck:

