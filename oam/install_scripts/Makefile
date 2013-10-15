include ../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 3250 2012-09-26 17:44:13Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

SCRIPTS=post-install pre-uninstall remote_command.sh \
calpontLogRotate transactionLog transactionLogArchiver.sh system_installer.sh \
module_installer.sh patch_installer.sh xm_installer.sh startupTests.sh \
director_installer.sh user_installer.sh performance_installer.sh \
remote_scp_get.sh calpontAlias run.sh upgrade-infinidb.sh \
post-mysql-install post-mysqld-install binary_installer.sh \
os_check.sh infinidb calpontSyslog calpontSyslog-ng syslogSetup.sh install-infinidb.sh \
remote_scp_put.sh calpontUninstall.sh infinidb.def \
remotessh.exp

LOCAL=module

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
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LOCAL); \
	pushd $(INSTALL_ROOT_LOCAL) >/dev/null; \
		rm -f $(LOCAL); \
	popd >/dev/null; \
	$(INSTALL) $(LOCAL) $(INSTALL_ROOT_LOCAL)
	pushd $(INSTALL_ROOT_LOCAL) >/dev/null; \
	chmod -f 555 $(LOCAL); \
	popd >/dev/null

clean:
	rm -f $(OBJS) $(PROGRAM) core *~ *.tag *.d *.d.*

install: bootstrap

test:

docs:

coverage:

leakcheck:

