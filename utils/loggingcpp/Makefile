#******************************************************************************************
# $Id: Makefile 3464 2012-12-20 20:03:38Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak

.PHONY: install clean test docs coverage leakcheck

PROGRAM=libloggingcpp.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=$(CXXFLAGS) -L$(EXPORT_ROOT)/lib -lconfigcpp -lxml2 -lcppunit -ldl
TLIBS=-L. -lloggingcpp $(CLIBS) -lmessageqcpp -lboost_idb
GLIBS=$(CLIBS) -lmessageqcpp

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lconfigcpp

SRCS=message.cpp messagelog.cpp logger.cpp errorcodes.cpp sqllogger.cpp stopwatch.cpp idberrorinfo.cpp
LINCLUDES=loggingid.h messageobj.h messagelog.h messageids.h logger.h errorcodes.h exceptclasses.h sqllogger.h stopwatch.h \
errorids.h idberrorinfo.h 

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
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

bootstrap: messageids.h errorids.h
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)
	$(INSTALL) MessageFile.txt ErrorMessage.txt $(INSTALL_ROOT_ETC)

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:$(LD_LIBRARY_PATH):/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp messageids.h errorids.h
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

messageids.h: MessageFile.txt genMsgId.pl
	./genMsgId.pl > messageids-temp.h
	diff -abBq messageids-temp.h messageids.h >/dev/null 2>&1; \
	if [ $$? -ne 0 ]; then \
		mv -f messageids-temp.h messageids.h; \
	else \
		touch messageids.h; \
	fi; \
	rm -f messageids-temp.h
	
errorids.h: ErrorMessage.txt genErrId.pl
	./genErrId.pl > errorids-temp.h
	diff -abBq errorids-temp.h errorids.h >/dev/null 2>&1; \
	if [ $$? -ne 0 ]; then \
		mv -f errorids-temp.h errorids.h; \
	else \
		touch errorids.h; \
	fi; \
	rm -f errorids-temp.h

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
