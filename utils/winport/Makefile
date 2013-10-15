CPPFLAGS=-DTEST2
CXXFLAGS=-g -O0
LDFLAGS=

idbgetopt: main.o idb_getopt.o
#idbgetopt: main.o
	$(LINK.cc) -o $@ $^

clean:
	rm -f idb_getopt *.o core

test: idbgetopt
	./idbgetopt

#main.o: getopt.h
#getopt.o: getopt.h


