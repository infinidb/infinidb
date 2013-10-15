include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 2097 2013-05-15 16:48:20Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

LIBRARY=libprocessor.a

CPPFLAGS=-I$(TOP)/primitives/blockcache -I$(TOP)/primitives/primproc -I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -DPRIM_DEBUG 
CXXFLAGS+=$(DEBUG_FLAGS) -Wall $(EXTRA_FLAGS)

CLIBS=-L$(EXPORT_ROOT)/lib -lconfigcpp -lxml2
TLIBS=$(CLIBS) -lcppunit -ldl -lloggingcpp
GLIBS=$(CLIBS)

SRCS=\
	primitiveprocessor.cpp \
	dictionary.cpp \
	column.cpp

LINCLUDES=\
	primitiveprocessor.h 

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(AR) scru $(LIBRARY) $(OBJS)

install: bootstrap $(LIBRARY)

bootstrap:

clean:
	rm -rf $(LIBRARY) $(OBJS) tdriver \
	*~ *.o *.d* *-gcov* *.gcov FILE233.cdf \
	html config.tag print_indextree_subblock print_dictblock \
	col{1,2,4,8}block.cdf \
	col8block_{neg,overflow,overflow_neg}.cdf \
	col_double_block.cdf col_float_block.cdf col_neg_double.cdf \
	col_neg_float.cdf

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

print_indextree_subblock: print_indextree_subblock.cpp
	g++ -o print_indextree_subblock $^ $(CPPFLAGS) $(CXXFLAGS) $(TLIBS)

print_dictblock: print_dictblock.cpp
	g++ -o print_dictblock $^ $(CPPFLAGS) $(CXXFLAGS) $(TLIBS)

print_indexlist: print_indexlist.cpp
	g++ -o print_indexlist $^ $(CPPFLAGS) $(CXXFLAGS) $(TLIBS)

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) 

test:

xtest: $(LIBRARY) tdriver 
	cat FILE_990.bz2 | bunzip2 > FILE_990.dat
	cat FILE_991.bz2 | bunzip2 > FILE_991.dat
	cat FILE233.bz2 | bunzip2 > FILE233.cdf
	tar zxf colblocks.tar.gz
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver
	rm col{1,2,4,8}block.cdf
	rm col8block_{neg,overflow,overflow_neg}.cdf
	rm col_double_block.cdf col_float_block.cdf
	rm col_neg_double.cdf col_neg_float.cdf
	rm FILE_990.dat
	rm FILE_991.dat
	rm FILE233.cdf

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(TLIBS)
	
coverage: 

xcoverage: 
	cat FILE_990.bz2 | bunzip2 > FILE_990.dat
	cat FILE_991.bz2 | bunzip2 > FILE_991.dat
	cat FILE233.bz2 | bunzip2 > FILE233.cdf
	tar zxf colblocks.tar.gz
	rm -f tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(MAKE) zcoverage
	rm col{1,2,4,8}block.cdf
	rm col8block_{neg,overflow,overflow_neg}.cdf
	rm FILE_990.dat
	rm FILE_991.dat
	rm FILE233.cdf

zcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)
	
leakcheck:

xleakcheck: $(LIBRARY) tdriver 
	cat FILE_990.bz2 | bunzip2 > FILE_990.dat
	cat FILE_991.bz2 | bunzip2 > FILE_991.dat
	cat FILE233.bz2 | bunzip2 > FILE233.cdf
	tar zxf colblocks.tar.gz
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver
	rm col{1,2,4,8}block.cdf
	rm col8block_{neg,overflow,overflow_neg}.cdf
	rm FILE_990.dat
	rm FILE_991.dat
	rm FILE233.cdf

tdriver%.o: tdriver%.cpp

tdriver%: tdriver%.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -liberty

alltests: test 

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
