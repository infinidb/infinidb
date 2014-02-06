#  Copyright (C) 2013 Calpont Corp.
#
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; version 2 of
#  the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libjoblist.la
libjoblist_la_CPPFLAGS = -I../../mysql/include -I../../mysql/sql -I../../mysql/regex $(AM_CPPFLAGS)
libjoblist_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)
libjoblist_la_SOURCES = \
        anydatalist.cpp \
        batchprimitiveprocessor-jl.cpp \
        columncommand-jl.cpp \
        command-jl.cpp \
        crossenginestep.cpp \
        dictstep-jl.cpp \
        distributedenginecomm.cpp \
        elementtype.cpp \
        expressionstep.cpp \
        filtercommand-jl.cpp \
        filterstep.cpp \
        groupconcat.cpp \
        jl_logger.cpp \
        jlf_common.cpp \
        jlf_execplantojoblist.cpp \
        jlf_graphics.cpp \
        jlf_tuplejoblist.cpp \
        jlf_subquery.cpp \
        joblist.cpp \
        joblistfactory.cpp \
        jobstep.cpp \
        jobstepassociation.cpp \
        lbidlist.cpp \
        limitedorderby.cpp \
        passthrucommand-jl.cpp \
        passthrustep.cpp \
        pcolscan.cpp \
        pcolstep.cpp \
        pdictionary.cpp \
        pdictionaryscan.cpp \
        primitivemsg.cpp \
        resourcedistributor.cpp \
        resourcemanager.cpp \
        rowestimator.cpp \
        rtscommand-jl.cpp \
        subquerystep.cpp \
        subquerytransformer.cpp \
        tablecolumn.cpp \
        timestamp.cpp \
        tuple-bps.cpp \
        tupleaggregatestep.cpp \
        tupleannexstep.cpp \
        tupleconstantstep.cpp \
        tuplehashjoin.cpp \
        tuplehavingstep.cpp \
        tupleunion.cpp \
        unique32generator.cpp \
        virtualtable.cpp \
		windowfunctionstep.cpp

include_HEADERS = \
        bpp-jl.h \
        datalist.h \
        datalistimpl.h \
        distributedenginecomm.h \
        elementcompression.h \
        elementtype.h \
        errorinfo.h \
        fifo.h \
        groupconcat.h \
        jl_logger.h \
        joblist.h \
        joblistfactory.h \
        joblisttypes.h \
        jobstep.h \
        lbidlist.h \
        limitedorderby.h \
        primitivemsg.h \
        resourcedistributor.h \
        resourcemanager.h \
        rtscommand-jl.h \
        tablecolumn.h \
        threadsafequeue.h \
        timeset.h \
        timestamp.h \
        unique32generator.h \
		windowfunctionstep.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

