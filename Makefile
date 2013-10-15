#
# $Id$
#

include rules.mak

.PHONY: export all full scratch compile clean dist

all: compile

# TODO: fix this! a compile step should not install anything
install: compile

full: export
	cd utils && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd snmpd && $(MAKE) -C../oam install && $(MAKE) && $(MAKE) install docs
	cd versioning && $(MAKE) && $(MAKE) install docs
	cd oam && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd dbcon/execplan && $(MAKE) && $(MAKE) install docs
	cd dbcon/joblist && $(MAKE) && $(MAKE) install docs
	cd versioning && $(MAKE) dbrm tools install_dbrm install_tools test test-dbrm coverage leakcheck
	cd writeengine && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd dbcon && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd exemgr && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd ddlproc && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd dmlproc && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd procmon && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd procmgr && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd oamapps && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd primitives && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd decomsvr && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	cd tools && $(MAKE) && $(MAKE) install test coverage leakcheck docs
	$(MAKE) -C writeengine/server install_server
	$(MAKE) -C writeengine/bulk install_bulk
	$(MAKE) -C writeengine/splitter install_splitter
	echo $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags

scratch: export
	$(MAKE) clean all

export:
	build/bootstrap

compile: export
	cd utils && $(MAKE) && $(MAKE) install
	cd snmpd && $(MAKE) -C../oam install && $(MAKE) && $(MAKE) install
	cd versioning && $(MAKE) && $(MAKE) install
	cd oam && $(MAKE) && $(MAKE) install
	cd dbcon/execplan && $(MAKE) && $(MAKE) install
	cd dbcon/joblist && $(MAKE) && $(MAKE) install
	cd versioning && $(MAKE) dbrm tools && $(MAKE) install_dbrm install_tools
	cd writeengine && $(MAKE) && $(MAKE) install
	cd dbcon && $(MAKE) && $(MAKE) install
	cd exemgr && $(MAKE) && $(MAKE) install
	cd ddlproc && $(MAKE) && $(MAKE) install
	cd dmlproc && $(MAKE) && $(MAKE) install
	cd procmon && $(MAKE) && $(MAKE) install
	cd procmgr && $(MAKE) && $(MAKE) install
	cd oamapps && $(MAKE) && $(MAKE) install
	cd primitives && $(MAKE) && $(MAKE) install
	cd decomsvr && $(MAKE) && $(MAKE) install
	cd tools && $(MAKE) && $(MAKE) install
	cd versioning && $(MAKE) tools install_tools
	$(MAKE) -C writeengine/server install_server
	$(MAKE) -C writeengine/bulk install_bulk
	$(MAKE) -C writeengine/splitter install_splitter
	echo $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags

clean:
	build/clean
	rm -rf export

dist:
	build/make_src_tar

