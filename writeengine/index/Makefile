include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean docs test coverage leakcheck 

SRCS=we_indexlist_multiple_narray.cpp we_indexlist_find_delete.cpp we_indexlist_common.cpp we_indexlist_update_hdr_sub.cpp we_indexlist_narray.cpp we_indexlist.cpp we_freemgr.cpp we_indextree.cpp 
LINCLUDES=we_indexlist.h we_freemgr.h we_indextree.h 
OBJS=$(SRCS:.cpp=.o)

LOBJS= $(LOBJS_SHARED)
GOBJS=$(LOBJS:.o=-gcov.o)

object: $(OBJS)
	mkdir -p $(LIBDIR)
	cp *.o $(LIBDIR)
	rm -f $(LIBDIR)/tindex.o
	make install

install: bootstrap

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(LIBDIR)/tindex.o  $(OBJS) tindex.o  tindex core *~ *.tag *-gcov.* *.gcov tindex-gcov *.d *.swp *.dat
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		rm -f $(LIBDIR)/$${bfile}.o ; \
        done
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tindex: $(OBJS) tindex.o
	$(LINK.cpp) -o $@ $^ $(LOBJS) $(TLIBS)

test:

xtest: tindex object
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tindex 

qindex: $(OBJS) qindex.o
	$(LINK.cpp) -o $@ $^ $(LOBJS) $(TLIBS)

qtest: qindex
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./qindex 


%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tindex-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tindex-gcov: tindex-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GOBJS) $(GLIBS)
	cp *-gcov.o $(LIBDIR)

coverage:

xcoverage: tindex-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tindex-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck:  tindex
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tindex

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tindex.d
endif
