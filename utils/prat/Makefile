include ../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 1338 2009-04-08 19:46:14Z dhill $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

SCRIPTS=copy2here.sh getlogs.sh getps.sh getuserinput.sh prat ps README getmodules.sh getsar.sh prat.sh pscollect tarfiles.sh

bootstrap:
	umask 002; \
	mkdir -p $(INSTALL_ROOT_TOOLS); \
	pushd $(INSTALL_ROOT_TOOLS) >/dev/null; \
		rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(SCRIPTS) $(INSTALL_ROOT_TOOLS)
	pushd $(INSTALL_ROOT_TOOLS) >/dev/null; \
	chmod -f 555 $(SCRIPTS); \
	popd >/dev/null

clean:
	rm -f $(OBJS) $(PROGRAM) core *~ *.tag *.d *.d.*

install: bootstrap

test:

docs:

coverage:

leakcheck:

