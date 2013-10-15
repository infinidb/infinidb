include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#
# $Id: Makefile 4624 2013-04-29 16:19:59Z rdempsey $
#
#*****************************************************************************************/
# The name of the executable
PROGRAM=WriteEngineServer
VERSION=1.0.0

.PHONY: install clean docs test coverage leakcheck install_server
TLIBS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS)
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS) $(IDB_SNMP_LIBS) -lthreadpool -lwriteengineredistribute -Wl,--rpath -Wl,/usr/local/Calpont/lib
GLIBS=$(LDFLAGS)

SRCS= \
	we_brmrprtparser.cpp \
	we_cleartablelockcmd.cpp \
	we_cpifeederthread.cpp \
	we_dataloader.cpp \
	we_ddlcommandproc.cpp \
	we_dmlcommandproc.cpp \
	we_getfilesizes.cpp \
	we_observer.cpp \
	we_readthread.cpp \
	we_server.cpp

LINCLUDES= we_messages.h

OBJS=$(SRCS:.cpp=.o)

GOBJS=$(LOBJS:.o=-gcov.o)

objects: $(OBJS)

install_server: bootstrap WriteEngineServer
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

WriteEngineServer: $(OBJS)
	$(LINK.cpp) -o $@ $^

install: bootstrap

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(PROGRAM) $(OBJS) core *~ *.tag *-gcov.* *.gcov *.d *.swp *.dat
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

test:

xtest: tbulk
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tbulk

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tbulk-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tbulk-gcov: tbulk-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GOBJS) $(GLIBS)
	cp *-gcov.o $(LIBDIR)

coverage:

xcoverage: tbulk-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tbulk-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck:  tbulk
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --error-limit=no --tool=memcheck --leak-check=yes ./tbulk

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
