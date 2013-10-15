include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# $Id: Makefile 4589 2013-04-02 14:41:08Z rdempsey $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

.PHONY: install clean docs test coverage leakcheck

SRCS=we_bulkrollbackmgr.cpp we_bulkrollbackfile.cpp we_bulkrollbackfilecompressed.cpp we_bulkrollbackfilecompressedhdfs.cpp we_fileop.cpp we_blockop.cpp we_dbfileop.cpp we_log.cpp we_simplesyslog.cpp we_convertor.cpp we_brm.cpp we_config.cpp we_cache.cpp we_stats.cpp we_define.cpp we_chunkmanager.cpp we_rbmetawriter.cpp we_dbrootextenttracker.cpp
LINCLUDES=we_bulkrollbackmgr.h we_index.h we_define.h we_type.h we_fileop.h we_blockop.h we_dbfileop.h we_obj.h we_log.h we_simplesyslog.h we_convertor.h we_brm.h we_macro.h we_config.h we_cache.h we_stats.h we_typeext.h we_chunkmanager.h we_rbmetawriter.h we_dbrootextenttracker.h
OBJS=$(SRCS:.cpp=.o)

object: $(OBJS) 
	mkdir -p $(LIBDIR)
	cp *.o $(LIBDIR)
	rm -f $(LIBDIR)/tshared.o
	make install

install: bootstrap

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(LIBDIR)/tshared.o $(LIBDIR)/*gcov.o $(OBJS) tshared.o  tshared core *~ *.tag *-gcov.* *.gcov tshared-gcov *.d *.swp *.dat *.txt *.log
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		rm -f $(LIBDIR)/$${bfile}.o ; \
        done
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tshared: $(OBJS) tshared.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test:

xtest:  tshared object
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tshared

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^
	cp *-gcov.o $(LIBDIR)

tshared-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tshared-gcov: tshared-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tshared-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tshared-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tshared
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tshared

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tshared.d
endif
