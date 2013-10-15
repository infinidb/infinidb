include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 9550 2013-05-17 23:58:07Z xlou $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libjoblist.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

MYSQL_DIR=../../mysql
CPPFLAGS+= -I. -I$(EXPORT_ROOT)/include -I$(MYSQL_DIR)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic -fno-strict-aliasing $(JL_DEBUG_FLAGS)
#CXXFLAGS+=-DFIFO_SINK

# setup libraries for driver

DLIBS=-L. -ljoblist -lfuncexp -L$(CALPONT_LIBRARY_PATH) -lrt -lexecplan -lbrm 
DLIBS+=-lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ldmlpackageproc -ldmlpackage
DLIBS+=-lwriteengine -lloggingcpp -ldataconvert -lcppunit -ldl -ljoiner -lcommon
DLIBS+=-lcacheutils
DLIBS+=-lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -lnetsnmp \
-lnetsnmpmibs -lnetsnmphelpers -loamcpp -lrowgroup -lfuncexp -lmysqlcl_idb
DLIBS+=-lwindowfunction

#DLIBS=-L. -ljoblist -lfuncexp -L$(CALPONT_LIBRARY_PATH) -lexecplan -lbrm -lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ldataconvert

ELIBS=-L. -ljoblist -lfuncexp -L$(CALPONT_LIBRARY_PATH) -lrt -lexecplan -lbrm
ELIBS+=-lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ldmlpackageproc -ldmlpackage
ELIBS+=-lwriteengine -lloggingcpp -ldataconvert  -ldl
ELIBS+=-lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -lnetsnmp \
-lnetsnmpmibs -lnetsnmphelpers -loamcpp -lfuncexp -lmysqlcl_idb
ELIBS+=-lwindowfunction


# setup libraries for tdriver

TLIBS=-L. -ljoblist -lfuncexp -L$(EXPORT_ROOT)/lib -lrt -lmysqlcl_idb
TLIBS+=-lmessageqcpp -lconfigcpp -lxml2 -lcppunit -ldl -lxml2 
TLIBS+=-lexecplan -lm -lthreadpool -lloggingcpp -lrwlock -lbrm 
TLIBS+=-ldmlpackageproc -ldmlpackage -lwriteengine -lloggingcpp -ldataconvert -lrowgroup
TLIBS+=-lwindowfunction

GLIBS=-L$(EXPORT_ROOT)/lib -lrt -lmessageqcpp -lconfigcpp -lxml2 -lcppunit
GLIBS+=-ldl -liberty -lxml2 -ldmlpackageproc -ldmlpackage -lwriteengine -lloggingcpp -ldataconvert -lfuncexp -lmysqlcl_idb
GLIBS+=-lwindowfunction

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lexecplan -lrowgroup -loamcpp -lmulticast -ljoiner -lquerystats -lcommon -lrt -lmysqlcl_idb -lwindowfunction

SRCS=\
	anydatalist.cpp \
	batchprimitiveprocessor-jl.cpp \
	columncommand-jl.cpp \
	command-jl.cpp \
	crossenginestep.cpp \
	dictstep-jl.cpp \
	distributedenginecomm.cpp \
	elementtype.cpp \
	expressionstep.cpp \
	filtercommand-jl.cpp \
	filterstep.cpp \
	groupconcat.cpp \
	jl_logger.cpp \
	jlf_common.cpp \
	jlf_execplantojoblist.cpp \
	jlf_graphics.cpp \
	jlf_tuplejoblist.cpp \
	jlf_subquery.cpp \
	joblist.cpp \
	joblistfactory.cpp \
	jobstep.cpp \
	jobstepassociation.cpp \
	lbidlist.cpp \
	limitedorderby.cpp \
	passthrucommand-jl.cpp \
	passthrustep.cpp \
	pcolscan.cpp \
	pcolstep.cpp \
	pdictionary.cpp \
	pdictionaryscan.cpp \
	primitivemsg.cpp \
	resourcedistributor.cpp \
 	resourcemanager.cpp \
	rowestimator.cpp \
 	rtscommand-jl.cpp \
	subquerystep.cpp \
	subquerytransformer.cpp \
	tablecolumn.cpp \
	timestamp.cpp \
	tuple-bps.cpp \
	tupleaggregatestep.cpp \
	tupleannexstep.cpp \
	tupleconstantstep.cpp \
	tuplehashjoin.cpp \
	tuplehavingstep.cpp \
	tupleunion.cpp \
	unique32generator.cpp \
	virtualtable.cpp \
	windowfunctionstep.cpp


LINCLUDES=\
	bpp-jl.h \
	distributedenginecomm.h \
	elementtype.h \
	groupconcat.h \
	jl_logger.h \
	joblist.h \
	joblistfactory.h \
	joblisttypes.h \
	jobstep.h \
	lbidlist.h \
	limitedorderby.h \
	primitivemsg.h \
	resourcedistributor.h \
	resourcemanager.h \
	rtscommand-jl.h \
	tablecolumn.h \
	threadsafequeue.h \
	timeset.h \
	timestamp.h \
	unique32generator.h \
	windowfunctionstep.h

DATALIST_SRC=\
	datalist.h \
	datalistimpl.h \
	elementcompression.h \
	fifo.h

TABLEBAND_SRC=\
	tablecolumn.h \
	tableband.h

DRIVERS=\
#	tdriver-datalist.cpp \
#	tdriver-tableband.cpp \
#	tdriver-dec.cpp \
#	tdriver1.cpp \
#	tdriver2.cpp \
#	tdriver-pdict.cpp \
#	tdriver-jobstep.cpp \
#	tdriver-hashjoin.cpp \
#	tdriver-deliver.cpp \
#	tdriver-index.cpp \
#	tdriver-zdl.cpp \
#	tdriver-bdl.cpp \
#	tdriver-bru.cpp \
#	tdriver-agg.cpp

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) $(LLIBS)
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
	$(INSTALL) $(LINCLUDES) $(DATALIST_SRC) $(INSTALL_ROOT_INCLUDE)

clean-drivers:
	rm -f tdriver*.o tdriver*[0-9] tdriver-datalist tdriver-dec tdriver-tableband tdriver-filter tdriver-jobstep
	rm -f tdriver-pdict tdriver-hashjoin tdriver tdriver-gcov tdriver-index tdriver-function

clean: clean-drivers
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) core *~ *.tag *-gcov.* *.gcov *.d *.d.*
	rm -rf html
	rm -f LDL-* core.*

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

drivers: tdriver-datalist tdriver-dec tdriver1 tdriver2 tdriver-filter \
	tdriver-hashjoin tdriver-function

tdriver-pdict: tdriver-pdict.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-dictscan: tdriver-dictscan.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-jobstep: tdriver-jobstep.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-datalist: tdriver-datalist.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-filter: tdriver-filter.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-function: tdriver-function.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-tableband: tdriver-tableband.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-dec: tdriver-dec.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver1: tdriver1.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver2: tdriver2.o
	$(LINK.cpp) -o $@ $^ $(ELIBS) -lboost_idb -lcompress -ludfsdk -lcacheutils

tdriver-hashjoin: tdriver-hashjoin.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)

tdriver-deliver: tdriver-deliver.o
	$(LINK.cpp) -o $@ $^ $(ELIBS)

fifo-bench: fifo-bench.o
	$(LINK.cpp) -o $@ $^ $(ELIBS)

tdriver-index: tdriver-index.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)
	
tdriver-zdl: tdriver-zdl.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)	

tdriver-bdl: tdriver-bdl.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)	

tdriver-bru: tdriver-bru.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)	

tdriver-agg: tdriver-agg.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)	

tdriver-rm: tdriver-rm.o
	$(LINK.cpp) -o $@ $^ $(DLIBS)	

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: tdriver-datalist tdriver-dec
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-datalist
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-dec --leakcheck
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-filter

leakcheck1: tdriver-deliver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-deliver



%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) $(DRIVERS:.cpp=.d)
endif

