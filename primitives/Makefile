include ../rules.mak
#******************************************************************************************
# $Id: Makefile 91 2006-10-05 16:04:58Z pleblanc $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: $(TARGETS)

# If we've got optimization on (-O3), crank it up further...
#ifneq (,$(findstring -O3,$(DEBUG_FLAGS)))
#EXTRA=EXTRA_FLAGS="-finline-limit=1800 -fgcse-sm -fgcse-las -fgcse-after-reload -funsafe-loop-optimizations"
#else
EXTRA=EXTRA_FLAGS=
#endif

all:
	$(MAKE) -C blockcache $(EXTRA)
	$(MAKE) -C linux-port $(EXTRA)
	$(MAKE) -C primproc $(EXTRA)

$(TARGETS):
	$(MAKE) -C blockcache $(EXTRA) $@
	$(MAKE) -C linux-port $(EXTRA) $@
	$(MAKE) -C primproc $(EXTRA) $@

