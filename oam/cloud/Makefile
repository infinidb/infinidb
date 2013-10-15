include ../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 2646 2012-01-06 18:47:12Z dhill $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

SCRIPTS=IDBInstanceCmds.sh IDBVolumeCmds.sh

bootstrap:
	umask 002; \
	mkdir -p $(INSTALL_ROOT_BIN); \
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
		rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(SCRIPTS) $(INSTALL_ROOT_BIN)
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
	chmod -f 555 $(SCRIPTS); \
	popd >/dev/null

clean:
	rm -f $(OBJS) $(PROGRAM) core *~ *.tag *.d *.d.*

install: bootstrap

test:

docs:

coverage:

leakcheck:

