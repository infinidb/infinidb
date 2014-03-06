SHELL=/bin/bash

ifeq (,$(findstring /root,${PWD}))
	TOP=$(shell pwd | cut -d / -f 1,2,3,4)
else
	TOP=$(shell pwd | cut -d / -f 1,2,3)
endif

EXPORT_ROOT=$(TOP)/export
INSTALL=cp --preserve=timestamps

CALPONT_INSTALL_ROOT=$(EXPORT_ROOT)

INSTALL_ROOT=$(CALPONT_INSTALL_ROOT)
INSTALL_ROOT_INCLUDE=$(INSTALL_ROOT)/include
INSTALL_ROOT_LIB=$(INSTALL_ROOT)/lib
INSTALL_ROOT_BIN=$(INSTALL_ROOT)/bin
INSTALL_ROOT_ETC=$(INSTALL_ROOT)/etc
INSTALL_ROOT_POST=$(INSTALL_ROOT)/post
INSTALL_ROOT_LOCAL=$(INSTALL_ROOT)/local
INSTALL_ROOT_MYSQL=$(INSTALL_ROOT)/mysql
INSTALL_ROOT_TOOLS=$(INSTALL_ROOT)/tools
INSTALL_ROOT_DATDUP=$(INSTALL_ROOT)/gluster
INSTALL_MIB=$(INSTALL_ROOT)/share/snmp/mibs

CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib
CALPONT_INCLUDE_PATH=$(EXPORT_ROOT)/include

IDB_COMMON_LIBS=-lwindowfunction -ljoblist -lexecplan -ljoiner -lrowgroup -lfuncexp -ludfsdk \
-loamcpp -lsnmpmanager -ldataconvert -lbrm -lcacheutils -lmessageqcpp -lloggingcpp -lconfigcpp -lrwlock \
-lcommon -lcompress -lxml2 -lidbboot -lboost_idb -lmysqlcl_idb -lquerystats -lidbdatafile -lquerytele \
-lthrift
IDB_WRITE_LIBS=-lddlpackageproc -lddlpackage -ldmlpackageproc -ldmlpackage -lwriteengine -lwriteengineclient -lcompress -lcacheutils
IDB_SNMP_LIBS=-lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers

LDFLAGS=-Wl,--no-as-needed 

DEBUG_FLAGS=-ggdb3 -fno-tree-vectorize
#DEBUG_FLAGS=-g0 -O3 -fno-strict-aliasing -fno-tree-vectorize

#DEBUG_FLAGS+=-DVALGRIND
#DEBUG_FLAGS+=-DSKIP_OAM_INIT

ifeq (i686,$(shell uname -m))
	DEBUG_FLAGS+=-march=pentium4
else ifeq (x86_64,$(shell uname -m))
	ifeq (opteron,$(shell egrep -qs Opteron /proc/cpuinfo && echo 'opteron'))
		DEBUG_FLAGS+=-march=opteron
	endif
endif

ifeq (4.5,$(shell test -x /usr/local/gcc45/bin/gcc && /usr/local/gcc45/bin/gcc --version | awk '/^gcc/ {print $$3}' | cut -c1-3))
	export LD_LIBRARY_PATH=/usr/local/gcc45/lib64:/usr/local/gmp43/lib:/usr/local/mpfr24/lib:/usr/local/mpc08/lib
	export PATH=/usr/local/gcc45/bin:/usr/local/bin:/bin:/usr/bin
	CC=/usr/local/gcc45/bin/gcc
	CXX=/usr/local/gcc45/bin/g++
	ifeq (-O3,$(findstring -O3,$(DEBUG_FLAGS)))
		DEBUG_FLAGS+=-flto
	endif
endif

#Use only the last, non-comment line from MyDebugFlags file
LOCAL_DEBUG_FLAGS=$(shell test -f $(TOP)/MyDebugFlags && awk '/^[^\#]/ {last=$$0}END{print last}' $(TOP)/MyDebugFlags)
ifneq (,$(LOCAL_DEBUG_FLAGS))
	DEBUG_FLAGS=$(LOCAL_DEBUG_FLAGS)
endif

