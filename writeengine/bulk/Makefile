include ../../rules.mak
include ../build/we_rules.mak
#*******************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#******************************************************************************/
# The name of the executable
PROGRAM=cpimport.bin
VERSION=1.0.0

.PHONY: install clean docs test coverage leakcheck install_bulk
TLIBS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS)
GLIBS=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS) $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

SRCS=we_bulkload.cpp we_workers.cpp we_colopbulk.cpp we_tableinfo.cpp we_bulkloadbuffer.cpp we_colbuf.cpp we_colbufcompressed.cpp we_colbufmgr.cpp we_colbufmgrdctnry.cpp we_colbufsec.cpp we_columninfo.cpp we_columninfocompressed.cpp we_bulkstatus.cpp we_colextinf.cpp we_tempxmlgendata.cpp we_columnautoinc.cpp we_brmreporter.cpp we_extentstripealloc.cpp
BULK_SRCS=cpimport.cpp 
LINCLUDES=
OBJS=$(SRCS:.cpp=.o)
BULK_OBJS=$(BULK_SRCS:.cpp=.o)

object: $(OBJS)
	mkdir -p $(LIBDIR)
	$(MAKE) install

bulk: cpimport.bin

cpimport.bin: $(BULK_OBJS) $(OBJS)
	$(LINK.cpp) -o $@ $^ $(GLIBS)

install: bootstrap

install_bulk: bootstrap $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

# We don't need to export any bulk include files for public use
bootstrap:
#	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(PROGRAM) $(OBJS) $(BULK_OBJS) core *~ *.tag *.d *.swp *.dat
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		rm -f $(LIBDIR)/$${bfile}.o ; \
        done
	rm -f cpimport.o
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

test:

coverage:

leakcheck:

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(BULK_SRCS:.cpp=.d) $(SRCS:.cpp=.d)
endif
