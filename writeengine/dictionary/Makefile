include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean docs test coverage leakcheck 

SRCS=we_dctnry.cpp
LINCLUDES=we_dctnry.h
OBJS=$(SRCS:.cpp=.o)

LOBJS=$(LOBJS_SHARED)
GOBJS=$(LOBJS:.o=-gcov.o)

object: $(OBJS)
	mkdir -p $(LIBDIR)
	cp *.o $(LIBDIR)
	rm -f $(LIBDIR)/tdctnry.o
	make install

install: bootstrap

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(LIBDIR)/tdctnry.o  $(OBJS) tdctnry.o  tdctnry core *~ *.tag *-gcov.* *.gcov tdctnry-gcov *.d *.swp *.dat
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		rm -f $(LIBDIR)/$${bfile}.o ; \
        done
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdctnry: $(OBJS) tdctnry.o
	$(LINK.cpp) -o $@ $^ $(LOBJS) $(TLIBS)

test:

xtest: clean tdctnry object
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdctnry 

qdctnry: $(OBJS) qdctnry.o
	$(LINK.cpp) -o $@ $^ $(LOBJS) $(TLIBS)

qtest: qdctnry object
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./qdctnry

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdctnry-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdctnry-gcov: tdctnry-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GOBJS) $(GLIBS)
	cp *-gcov.o $(LIBDIR)

coverage:

xcoverage: tdctnry-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdctnry-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck:  tdctnry
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdctnry

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdctnry.d
endif
