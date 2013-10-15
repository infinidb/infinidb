include ../../rules.mak
#
# $Id: Makefile 8079 2011-11-01 16:43:41Z rdempsey $
#

.PHONY: install clean test docs coverage leakcheck

PROGRAM=libcaling.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

SRCS=inggwif.cpp

LINCLUDES=inggwif.h

OBJS=$(SRCS:.cpp=.o)
LIBS=-L$(EXPORT_ROOT)/lib \
	-L../sm -lsm \
	-L../execplan -lexecplan \
	-L../joblist -ljoblist -lfuncexp \
	-L../ddlpackageproc -lddlpackageproc \
	-L../ddlpackage -lddlpackage \
	-L../dmlpackageproc -ldmlpackageproc \
	-L../dmlpackage -ldmlpackage \
	-lwriteengine -lbrm -lrwlock -lmessageqcpp -lloggingcpp -lconfigcpp \
	-lboost_date_time -lboost_thread -ldataconvert -lxml2
XXLIBS=-L$(EXPORT_ROOT)/lib \
	-L../sm -lsm \
	-L../execplan -lexecplan \
	-L../joblist -ljoblist -lfuncexp \
	-lbrm -lrwlock -lmessageqcpp -lthreadpool -lloggingcpp -lconfigcpp -ldataconvert \
	-lboost_thread \
	-lxml2 -lrt

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(LIBS) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		ln -sf $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

drivers: 

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov* *.gcov tdriver-gcov *.d
	rm -rf html

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

test:

coverage:

leakcheck:

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif

