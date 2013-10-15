include ../../rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

LETC=AlarmConfig.xml Calpont.xml ProcessConfig.xml ConsoleCmds.xml Calpont.xml.singleserver  ProcessConfig.xml.singleserver

bootstrap:
	umask 002; \
	mkdir -p $(INSTALL_ROOT_ETC); \
	pushd $(INSTALL_ROOT_ETC) >/dev/null; \
		rm -f $(LETC); \
	popd >/dev/null; \
	$(INSTALL) $(LETC) $(INSTALL_ROOT_ETC)
	pushd $(INSTALL_ROOT_ETC) >/dev/null; \
	chmod -f 666 $(LETC); \
	popd >/dev/null
	mkdir -p $(INSTALL_ROOT_LOCAL)

install: bootstrap

clean:
	rm -f *~ 

test:

docs:

coverage:

leakcheck:

