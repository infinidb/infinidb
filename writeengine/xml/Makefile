include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean docs test coverage leakcheck 

SRCS=we_xmlop.cpp we_xmljob.cpp we_xmlgendata.cpp we_xmlgenproc.cpp
LINCLUDES=we_xmlop.h we_xmljob.h we_xmltag.h we_xmlgendata.h we_xmlgenproc.h
OBJS=$(SRCS:.cpp=.o)

LOBJS= $(LOBJS_SHARED)
GOBJS=$(LOBJS:.o=-gcov.o)

GLIBS=-L. -L$(EXPORT_ROOT)/lib $(IDB_COMMON_LIBS) -lcacheutils $(IDB_SNMP_LIBS)

object: $(OBJS)
	mkdir -p $(LIBDIR)
	cp *.o $(LIBDIR)
	rm -f $(LIBDIR)/txml.o
	make install

install: bootstrap
	mkdir -p $(INSTALL_ROOT_BIN)

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(LIBDIR)/txml.o  $(OBJS) txml.o  txml core *~ *.tag *-gcov.* *.gcov txml-gcov *.d *.swp *.dat *.gcno
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		rm -f $(LIBDIR)/$${bfile}.o ; \
        done
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

txml: $(OBJS) txml.o
	$(LINK.cpp) -o $@ $^ $(LOBJS) $(TLIBS)

test:

xtest: txml
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./txml 


%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^
	cp *-gcov.o $(LIBDIR)

txml-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
txml-gcov: txml-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GOBJS) $(GLIBS)
#	cp *-gcov.o $(LIBDIR)

coverage:

xcoverage: txml-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./txml-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck:  txml
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./txml

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) txml.d
endif
