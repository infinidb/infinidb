include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 9559 2013-05-22 17:58:13Z xlou $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck bootstrap

PROGRAM=libexecplan.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -ldl -lmessageqcpp -lconfigcpp -lxml2 -ljoblist -lfuncexp -lloggingcpp
TLIBS=-L. -lexecplan -L$(EXPORT_ROOT)/lib -lm -lthreadpool -lrwlock -lbrm -lrowgroup -ljoiner -lcommon -lfuncexp -lmulticast -ldataconvert -loamcpp $(CLIBS) -lboost_idb -ludfsdk -lcppunit
GLIBS=$(CLIBS)

SRCS=\
aggregatecolumn.cpp \
arithmeticcolumn.cpp \
arithmeticoperator.cpp \
calpontexecutionplan.cpp \
calpontexecutionplanfactory.cpp \
calpontselectexecutionplan.cpp \
calpontsystemcatalog.cpp \
clientrotator.cpp \
constantcolumn.cpp \
constantfilter.cpp \
existsfilter.cpp \
expressionparser.cpp \
filter.cpp \
functioncolumn.cpp \
groupconcatcolumn.cpp \
intervalcolumn.cpp \
logicoperator.cpp \
mysqlexecutionplan.cpp \
objectidmanager.cpp \
objectreader.cpp \
operator.cpp \
oracleexecutionplan.cpp \
outerjoinonfilter.cpp \
predicateoperator.cpp \
range.cpp \
returnedcolumn.cpp \
rowcolumn.cpp \
selectfilter.cpp \
sessionmanager.cpp \
simplecolumn.cpp \
simplefilter.cpp \
simplescalarfilter.cpp \
treenode.cpp \
treenodeimpl.cpp \
vendorexecutionplan.cpp \
windowfunctioncolumn.cpp

LINCLUDES=\
aggregatecolumn.h \
arithmeticcolumn.h \
arithmeticoperator.h \
blocksize.h \
calpontexecutionplanfactory.h \
calpontexecutionplan.h \
calpontselectexecutionplan.h \
calpontsystemcatalog.h \
clientrotator.h \
columnresult.h \
constantcolumn.h \
constantfilter.h \
existsfilter.h \
expressionparser.h \
exp_templates.h \
filter.h \
functioncolumn.h \
groupconcatcolumn.h \
intervalcolumn.h \
logicoperator.h \
mysqlexecutionplan.h \
njlcolumnresult.h \
objectidmanager.h \
objectreader.h \
operator.h \
oracleexecutionplan.h \
outerjoinonfilter.h \
parsetree.h \
predicateoperator.h \
range.h \
returnedcolumn.h \
rowcolumn.h \
selectfilter.h \
sessionmanager.h \
simplecolumn.h \
simplecolumn_decimal.h \
simplecolumn_int.h \
simplecolumn_uint.h \
simplefilter.h \
simplescalarfilter.h \
treenode.h \
treenodeimpl.h \
vendorexecutionplan.h \
windowfunctioncolumn.h

OBJS=$(SRCS:.cpp=.o)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -ljoblist -lbrm -loamcpp

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -L$(EXPORT_ROOT)/lib -loamcpp 
#	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -L$(EXPORT_ROOT)/lib -loamcpp -lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers
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

clean:
	rm -f $(OBJS) tdriver.o btdriver.o btdriver $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov* *.gcov tdriver-gcov *.d
	rm -rf html
	rm -f tdriver*.o tdriver*[0-9]
	rm -f oidbitmap tdriver-oid tdriver-sm

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

btdriver: btdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) 

test:

xtest: $(LIBRARY) tdriver
#	-ipcs-pat -d >& /dev/null
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver
#	-ipcs-pat -d >& /dev/null

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage:
	rm -f tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage

zcoverage: tdriver-gcov
	rm -f *.gcda
	-ipcs-pat -d >& /dev/null
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	-ipcs-pat -d >& /dev/null
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	-ipcs-pat -d >& /dev/null
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver
	-ipcs-pat -d >& /dev/null

tdriver%.o: tdriver%.cpp

tdriver%: tdriver%.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

tdriver-oid.o: tdriver-oid.cpp

tdriver-oid: $(LIBRARY) tdriver-oid.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

tdriver-sm.o: tdriver-sm.cpp

tdriver-sm: $(LIBRARY) tdriver-sm.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -lpthread

tdriver_fe.o: tdriver_fe.cpp

tdriver_fe: $(LIBRARY) tdriver_fe.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test1: $(LIBRARY) tdriver1
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver1

test2: $(LIBRARY) tdriver2
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver2

test3: $(LIBRARY) tdriver3
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver3
	
test4: $(LIBRARY) tdriver4
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver4
	
test5: $(LIBRARY) tdriver5
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver5
	
test6: $(LIBRARY) tdriver6
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver6
	
test7: $(LIBRARY) tdriver7
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver7
	
test8: $(LIBRARY) tdriver8
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver8
	
test9: $(LIBRARY) tdriver9
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver9
	
test10: $(LIBRARY) tdriver10
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver10
	
test11: $(LIBRARY) tdriver11
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver11
	
test12: $(LIBRARY) tdriver12
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver12
	
test13: $(LIBRARY) tdriver13
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver13	
	
test14: $(LIBRARY) tdriver14
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver14	
	
test15: $(LIBRARY) tdriver15
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver15	
	
test16: $(LIBRARY) tdriver16
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver16
	
test17: $(LIBRARY) tdriver17
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver17
	
test18: $(LIBRARY) tdriver18
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver18	
	
test19: $(LIBRARY) tdriver19
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver19	
	
test20: $(LIBRARY) tdriver20
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver20	
	
test21: $(LIBRARY) tdriver21
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver21	
	
test22: $(LIBRARY) tdriver22
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver22

test23: $(LIBRARY) tdriver23
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver23

test-oid: $(LIBRARY) tdriver-oid
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-oid

test-sm: $(LIBRARY) tdriver-sm
	-ipcs-pat -d >& /dev/null
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-sm
	-ipcs-pat -d >& /dev/null

test-fe: $(LIBRARY) tdriver_fe.o
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver_fe

alltests: test test1 test2 test3 test4 test5 test6 test7 test8 test9 test10 \
          test11 test12 test13 test14 test15 test16 test17 test18 test19 \
          test20 test21 test22 test23 test-oid test-sm

leakcheck1: $(LIBRARY) tdriver1
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver1

leakcheck2: $(LIBRARY) tdriver2
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver2

leakcheck3: $(LIBRARY) tdriver3
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver3

leakcheck4: $(LIBRARY) tdriver4
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver4

leakcheck5: $(LIBRARY) tdriver5
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver5

leakcheck6: $(LIBRARY) tdriver6
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver6

leakcheck7: $(LIBRARY) tdriver7
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver7

leakcheck8: $(LIBRARY) tdriver8
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver8

leakcheck9: $(LIBRARY) tdriver9
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver9

leakcheck10: $(LIBRARY) tdriver10
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver10

leakcheck11: $(LIBRARY) tdriver11
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver11

leakcheck12: $(LIBRARY) tdriver12
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver12

leakcheck13: $(LIBRARY) tdriver13
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver13

leakcheck14: $(LIBRARY) tdriver14
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver14

leakcheck15: $(LIBRARY) tdriver15
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver15

leakcheck16: $(LIBRARY) tdriver16
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver16

leakcheck17: $(LIBRARY) tdriver17
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver17

leakcheck18: $(LIBRARY) tdriver18
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver18

leakcheck19: $(LIBRARY) tdriver19
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver19

leakcheck20: $(LIBRARY) tdriver20
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver20

leakcheck21: $(LIBRARY) tdriver21
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver21

leakcheck22: $(LIBRARY) tdriver22
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver22

leakcheck23: $(LIBRARY) tdriver23
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver23

leakcheck-oid: $(LIBRARY) tdriver-oid
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-oid

leakcheck-sm: $(LIBRARY) tdriver-sm
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-sm

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

TDRIVERS=$(wildcard *tdriver*.cpp)

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) $(TDRIVERS:.cpp=.d)
endif
