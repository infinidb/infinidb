include ../../rules.mak
#
# $Id: Makefile 8079 2011-11-01 16:43:41Z rdempsey $
#

.PHONY: install clean test docs coverage leakcheck

LIBS64=$(shell test `arch -k` == 'x86_64' && echo 64)
PROGRAM=libcalora.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

ifeq ($(shell test -d /home/oracle/app/oracle/product/11.1.0/db_1 && echo Y),Y)
	ORACLE_HOME=/home/oracle/app/oracle/product/11.1.0/db_1
	LIBNNZVER=11
else
	ifeq ($(shell test -d /home/oracle/oracle/product/10.2.0/db_1 && echo Y),Y)
		ORACLE_HOME=/home/oracle/oracle/product/10.2.0/db_1
		LIBNNZVER=10
	else
		ifeq ($(shell test -d /usr/lib/oracle/xe/app/oracle/product/10.2.0/server && echo Y),Y)
			ORACLE_HOME=/usr/lib/oracle/xe/app/oracle/product/10.2.0/server
			LIBNNZVER=10
		endif
	endif
endif

ifndef ORACLE_HOME
$(error Could not determine ORACLE_HOME)
endif

CPPFLAGS=-I$(ORACLE_HOME)/rdbms/public -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic #-fno-strict-aliasing

OAMLIBS=-loamcpp -lsnmpmanager -lboost_filesystem -lnetsnmp -lcrypto \
-lnetsnmpagent -lnetsnmphelpers -lnetsnmpmibs

SRCS=tablefuncs.cpp execproc.cpp checkerr.cpp getplan.cpp getdmlplan.cpp ddlfuncs.cpp planutils.cpp orasession.cpp

LINCLUDES=orasession.h

SQLSCRIPTS=create_reg_obj_owner.sql \
install_libraries.sql \
install_oracle_calpont.sql \
install_packages.sql \
install_pkg_calpont.sql \
install_pkg_error.sql \
install_pkg_logging.sql \
install_procs_funcs.sql \
install_tables.sql \
install_user.sql \
reg_obj_owner.sql \
uninstall_oracle_calpont.sql \
unreg_obj_owner.sql \
install_calpontsys.sql \
install_s_calpontsys.sql \
install_calpontsys_privs.sql \
dumpcat.sql \
syscatalog.sql \
create_reg_obj_owner.sh \
install_oracle_calpont.sh \
reg_obj_owner.sh \
uninstall_oracle_calpont.sh \
unreg_obj_owner.sh \
drop_calpont_objects.sql

OBJS=$(SRCS:.cpp=.o)
LIBS=-L$(EXPORT_ROOT)/lib \
	-L../sm -lsm \
	-L../execplan -lexecplan \
	-L../dmlpackageproc -ldmlpackageproc \
	-L../dmlpackage -ldmlpackage \
	-L../ddlpackageproc -lddlpackageproc \
	-L../ddlpackage -lddlpackage \
	-L../joblist -ljoblist -lfuncexp \
	-L../resultset \
	-lbrm -lrwlock -lmessageqcpp -lwriteengine -lthreadpool -lloggingcpp -lconfigcpp -ldataconvert\
	-lboost_thread -lboost_date_time \
	-lxml2 -lrt -ljoiner -lcommon -lrowgroup -lcacheutils \
	$(OAMLIBS)

$(LIBRARY): $(OBJS)
	$(MAKE) -C ../../writeengine
	$(MAKE) -C ../../writeengine install
	$(LINK.cpp) -shared -o $(LIBRARY) $(LIBS) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE) $(INSTALL_ROOT_ETC)/oracle; \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB); \
	$(INSTALL) $(SQLSCRIPTS) $(INSTALL_ROOT_ETC)/oracle

drivers: execproc_driver dml_driver

execproc_driver: execproc.cpp
	$(LINK.cpp) -o execproc_driver -DSTANDALONE execproc.cpp -L$(ORACLE_HOME)/lib -lclntsh -lnnz$(LIBNNZVER)

getplan: getplan.cpp
	$(LINK.cpp) -o getplan -DSTANDALONE getplan.cpp -L$(ORACLE_HOME)/lib -lclntsh -lnnz$(LIBNNZVER) -L. -lcalora $(LIBS)
	
getdmlplan: getdmlplan.cpp
	$(LINK.cpp) -o getdmlplan -DSTANDALONE getdmlplan.cpp -L$(ORACLE_HOME)/lib -lclntsh -lnnz$(LIBNNZVER) -L. -lcalora $(LIBS)

ddl_driver: ddlfuncs.cpp
	$(LINK.cpp) -o ddl_driver -DSTANDALONE ddlfuncs.cpp -L$(ORACLE_HOME)/lib -lclntsh -lnnz$(LIBNNZVER) $(LIBS)

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov* *.gcov tdriver-gcov *.d execproc_driver dml_driver getplan getdmlplan
	rm -rf html

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

test:

coverage:

leakcheck:

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile
%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
