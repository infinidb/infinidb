include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 1872 2013-04-26 18:13:19Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libbrm.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

# Note: performance tests show the DEBUG macro introduces negligible overhead.

## Add -DPARTITIONING to turn on range partitioning code
CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I../../utils/rwlock -I/usr/include/libxml2
## gcc 4.5.3 tends to generate an ICE when LTO is turned on for libbrm 
NEW_DEBUG_FLAGS=$(shell echo "$(DEBUG_FLAGS)" | sed s/-flto//g)
CXXFLAGS+=$(NEW_DEBUG_FLAGS) -Wall -fPIC

TLIBS=-L. -lbrm -L$(EXPORT_ROOT)/lib -lrwlock -lmessageqcpp -ldl -lpthread -lcppunit \
-lconfigcpp -lxml2 -lexecplan -ljoblist -lfuncexp -ljoiner -lcommon \
-ldataconvert -lloggingcpp -lrowgroup

GLIBS=-L. -lbrm -L$(EXPORT_ROOT)/lib $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

OAMLIBS=-loamcpp -lsnmpmanager -lnetsnmp \
-lnetsnmpagent -lnetsnmphelpers -lnetsnmpmibs

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lrwlock

SRCS=\
autoincrementmanager.cpp \
blockresolutionmanager.cpp \
brmshmimpl.cpp \
brmtypes.cpp \
copylocks.cpp \
dbrm.cpp \
extentmap.cpp \
lbidresourcegraph.cpp \
logicalpartition.cpp \
mastersegmenttable.cpp \
oidserver.cpp \
resourcenode.cpp \
rgnode.cpp \
rwlockmonitor.cpp \
sessionmanagerserver.cpp \
shmkeys.cpp \
slavedbrmnode.cpp \
slavecomm.cpp \
tablelockserver.cpp \
tracer.cpp \
transactionnode.cpp \
undoable.cpp \
vbbm.cpp \
vss.cpp

TOOLSRCS=\
dbrmctl.cpp \
load_brm.cpp \
load_em.cpp \
masterdbrmnode.cpp \
masternode.cpp \
reset_locks.cpp \
rollback.cpp \
save_brm.cpp \
slavenode.cpp 

LINCLUDES=\
blockresolutionmanager.h \
brm.h \
brmshmimpl.h \
brmtypes.h \
copylocks.h \
dbrm.h \
extentmap.h \
logicalpartition.h \
mastersegmenttable.h \
sessionmanagerserver.h \
shmkeys.h \
slavecomm.h \
slavedbrmnode.h \
tracer.h \
undoable.h \
vbbm.h \
vss.h

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -lrt
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)
	$(INSTALL) dbrm script-fcns $(INSTALL_ROOT_BIN)

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) tdriver \
	tdriver-brm tdriver-load tdriver-brm.o *~ *.o *.d* *-gcov* *.gcov \
 	html config.tag {EM,CL,VSS,VBBM}Image \
	reset_locks rollback save_brm load_brm dbrmctl load_brm64 \
	load_em TestImage_{em,cl,vss,vbbm} controllernode workernode tdriver-dbrm \
	tdriver-dbrm2
	#./dbrm stop

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tools: $(LIBRARY) reset_locks rollback save_brm load_brm load_em tablelocktest autoinctest copylocktest

install_tools: tools
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) save_brm load_brm reset_locks rollback $(INSTALL_ROOT_BIN)

install_all: install install_dbrm install_tools

copylocktest: $(LIBRARY) copylocktest.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

autoinctest: $(LIBRARY) autoinctest.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

tablelocktest: $(LIBRARY) tablelocktest.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

reset_locks: $(LIBRARY) reset_locks.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

rollback: rollback.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

save_brm: save_brm.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

load_brm: load_brm.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

load_brm64: load_brm64.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

load_em: load_em.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

cvt_em: cvt_em.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

test-dbrm: dbrm tdriver-dbrm tdriver-dbrm2
	./dbrm stop
	CALPONT_CONFIG_FILE=./Calpont.xml ./dbrm start
	CALPONT_CONFIG_FILE=./Calpont.xml ./tdriver-dbrm
	./dbrm stop

.PHONY: dbrm
.PRECIOUS: dbrm
dbrm: $(LIBRARY) controllernode workernode dbrmctl 

install_dbrm: dbrm
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) controllernode workernode dbrmctl $(INSTALL_ROOT_BIN)	

controllernode: $(LIBRARY) masternode.o masterdbrmnode.o 
	$(LINK.cpp) -o $@ masternode.o masterdbrmnode.o $(GLIBS)

workernode: slavenode.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

dbrmctl: dbrmctl.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

lock_grabber: lock_grabber.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

lock_state: lock_state.o
	$(LINK.cpp) -o $@ $^ $(GLIBS)

tdriver: tdriver.o
	$(MAKE) -C ../../dbcon/execplan install
	$(MAKE) -C ../../dbcon/joblist install
	$(LINK.cpp) -o $@ $^ $(TLIBS) 

tdriver-dbrm: tdriver-dbrm.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) $(OAMLIBS)
	
tdriver-dbrm2: tdriver-dbrm2.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) $(OAMLIBS) -lpthread

tdriver-brm: tdriver-brm.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -lpthread

tdriver-load: tdriver-load.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -lpthread

test:

xtest: $(LIBRARY) tdriver
	./dbrm stop
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-brm

test-load: $(LIBRARY) tdriver-load
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-load

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(TLIBS)
	
tdriver-brm-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-brm-gcov: tdriver-brm-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(TLIBS)
	
tdriver-load-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-load-gcov: tdriver-load-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(TLIBS)

coverage:

xcoverage:
	rm -f tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage

coverage-brm:
	rm -f tdriver-brm-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage-brm

coverage-load:
	rm -f tdriver-load-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage-load

zcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)
	
zcoverage-brm: tdriver-brm-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-brm-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)
	
zcoverage-load: tdriver-load-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-load-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)
	
leakcheck:

xleakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver
	
leakcheck-brm: $(LIBRARY) tdriver-brm
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-brm

leakcheck-load: $(LIBRARY) tdriver-load
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-load

tdriver%.o: tdriver%.cpp

tdriver%: tdriver%.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -liberty

alltests: test test-load test-brm

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) $(TOOLSRCS:.cpp=.d) tdriver.d
endif
