include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 1058 2006-08-08 15:38:16Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

PROGRAM=librwlock.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

# CPPFLAGS=-I$(EXPORT_ROOT)/include
CPPFLAGS=-I$(EXPORT_ROOT)/include
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -lrwlock -L$(EXPORT_ROOT)/lib -lcppunit -ldl -lpthread -lrt $(IDB_COMMON_LIBS)
GLIBS=-L$(EXPORT_ROOT)/lib -lcppunit -ldl -lpthread -lrt

LLIBS=-L$(CALPONT_LIBRARY_PATH)

SRCS=\
rwlock.cpp \
rwlock_local.cpp

LINCLUDES=\
rwlock.h \
rwlock_local.h

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
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) tdriver tdriver-rw \
	*~ *.o *.d* *-gcov* *.gcov html config.tag

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: $(LIBRARY) tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) 
	
tdriver-rw: $(LIBRARY) tdriver-rw.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) 

test: tdriver
	rm -f /dev/shm/InfiniDB*
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver
	rm -f /dev/shm/InfiniDB*

test-rw: tdriver-rw
	rm -f /dev/shm/InfiniDB*
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-rw
	rm -f /dev/shm/InfiniDB*

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)
	
tdriver-rw-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-rw-gcov: tdriver-rw-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)
	
coverage: 
	rm -f /dev/shm/InfiniDB*
	rm -f tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage
	rm -f /dev/shm/InfiniDB*

coverage-rw: 
	rm -f /dev/shm/InfiniDB*
	rm -f tdriver-rw-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage-rw
	rm -f /dev/shm/InfiniDB*

zcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)
	
zcoverage-rw: tdriver-rw-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-rw-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver 
	rm -f /dev/shm/InfiniDB*
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver
	rm -f /dev/shm/InfiniDB*

leakcheck-rw: $(LIBRARY) tdriver-rw
	rm -f /dev/shm/InfiniDB*
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-rw
	rm -f /dev/shm/InfiniDB*

tdriver%.o: tdriver%.cpp
	
tdriver%.o: tdriver%.cpp

tdriver%: tdriver%.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -liberty

alltests: test test-rw

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d btdriver.d
endif
