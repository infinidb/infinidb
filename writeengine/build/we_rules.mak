IPCS_CLEANUP=$(EXPORT_ROOT)/bin/dbrm stop; ipcs-pat -d > /dev/null; sleep 2; $(EXPORT_ROOT)/bin/dbrm start;

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2

CXXFLAGS+=$(DEBUG_FLAGS) -D_FILE_OFFSET_BITS=64 -Wall -fpic
LLIBS=-L. -L$(EXPORT_ROOT)/lib -lxml2 -lrwlock -lconfigcpp -lmessageqcpp -lbrm -lboost_thread \
      -lloggingcpp  -lboost_date_time -lboost_filesystem
TLIBS=$(LLIBS) -lcppunit -ldl
GLIBS=$(TLIBS:-lcppunit=)

LIBDIR=../obj
LOBJS_SHARED= \
	$(LIBDIR)/we_blockop.o \
	$(LIBDIR)/we_brm.o \
	$(LIBDIR)/we_bulkrollbackmgr.o \
	$(LIBDIR)/we_bulkrollbackfile.o \
	$(LIBDIR)/we_bulkrollbackfilecompressed.o \
	$(LIBDIR)/we_bulkrollbackfilecompressedhdfs.o \
	$(LIBDIR)/we_cache.o \
	$(LIBDIR)/we_chunkmanager.o \
	$(LIBDIR)/we_config.o \
	$(LIBDIR)/we_confirmhdfsdbfile.o \
	$(LIBDIR)/we_convertor.o \
	$(LIBDIR)/we_dbfileop.o \
	$(LIBDIR)/we_dbrootextenttracker.o \
	$(LIBDIR)/we_define.o \
	$(LIBDIR)/we_fileop.o \
	$(LIBDIR)/we_log.o \
	$(LIBDIR)/we_rbmetawriter.o \
	$(LIBDIR)/we_simplesyslog.o \
	$(LIBDIR)/we_stats.o
 
LOBJS_INDEX= \
	$(LIBDIR)/we_freemgr.o \
	$(LIBDIR)/we_indexlist.o \
	$(LIBDIR)/we_indexlist_common.o \
	$(LIBDIR)/we_indexlist_find_delete.o \
	$(LIBDIR)/we_indexlist_multiple_narray.o \
	$(LIBDIR)/we_indexlist_narray.o \
	$(LIBDIR)/we_indexlist_update_hdr_sub.o \
	$(LIBDIR)/we_indextree.o

LOBJS_DCTNRY= $(LIBDIR)/we_dctnry.o
LOBJS_XML= $(LIBDIR)/we_xmlop.o \
	$(LIBDIR)/we_xmljob.o \
	$(LIBDIR)/we_xmlgendata.o \
	$(LIBDIR)/we_xmlgenproc.o

