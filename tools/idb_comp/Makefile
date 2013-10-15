CXXFLAGS=-g0 -O3 -Wall
LDFLAGS+=-L/usr/local/lib

idb_comp: main.o
	$(LINK.cpp) -o idb_comp main.o -llzo2

clean:
	rm -f idb_comp main.o

