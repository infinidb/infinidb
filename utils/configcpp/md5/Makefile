include ../../../rules.mak
LIBRARY=libmd5.a

SRCS= md5_dgst.c md5_one.c mem_clr.c

CFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

OBJS=$(SRCS:.c=.o)

$(LIBRARY): $(OBJS)
	$(AR) scru $(LIBRARY) $(OBJS)

bootstrap:

install: bootstrap $(LIBRARY)

clean:
	rm -f $(OBJS) core *~ *-gcov.* *.gcov *.d config.tag *.d.* $(LIBRARY)
	rm -rf html

docs:
	$(EXPORT_ROOT)/etc/Doxyfile

coverage:

test:

leakcheck:

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.c=.d)
endif

