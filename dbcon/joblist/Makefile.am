#  Copyright (C) 2014 InfiniDB, Inc.
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

# $Id: Makefile.am 864 2009-04-02 19:22:49Z rdempsey $
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libjoblist.la
libjoblist_la_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)
libjoblist_la_SOURCES = \
        aggregatefilterstep.cpp \
        aggregator.cpp \
        anydatalist.cpp \
        batchprimitiveprocessor-jl.cpp \
        batchprimitivestep.cpp \
        bucketreuse.cpp \
        bucketreusestep.cpp \
        columncommand-jl.cpp \
        command-jl.cpp \
        deliverystep.cpp \
        deliverywsdl.cpp \
        dictstep-jl.cpp \
        distributedenginecomm.cpp \
        elementtype.cpp \
        expressionstep.cpp \
        filtercommand-jl.cpp \
        filteroperation.cpp \
        filterstep.cpp \
        groupconcat.cpp \
        hashjoin.cpp \
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
        largehashjoin.cpp \
        lbidlist.cpp \
        limitedorderby.cpp \
        passthrucommand-jl.cpp \
        passthrustep.cpp \
        pcolscan.cpp \
        pcolstep.cpp \
        pdictionary.cpp \
        pdictionaryscan.cpp \
        primitivemsg.cpp \
        profiling.cpp \
        reducestep.cpp  \
        resourcedistributor.cpp \
        resourcemanager.cpp \
        rowestimator.cpp \
        rtscommand-jl.cpp \
        subquerystep.cpp \
        subquerytransformer.cpp \
        tableband.cpp \
        tablecolumn.cpp \
        timestamp.cpp \
        tuple-bps.cpp \
        tupleaggregatestep.cpp \
        tupleannexstep.cpp \
        tupleconstantstep.cpp \
        tuplehashjoin.cpp \
        tuplehavingstep.cpp \
        tupleunion.cpp \
        tuplewsdl.cpp \
        unionstep.cpp \
        unique32generator.cpp \
        virtualtable.cpp

include_HEADERS = \
        aggregator.h \
        batchprimitiveprocessor-jl.h \
        bdlwrapper.h \
        bpp-jl.h \
        bucketreuse.h \
        columncommand-jl.h \
        command-jl.h \
        deliverywsdl.h \
        dictstep-jl.h \
        distributedenginecomm.h \
        elementtype.h \
        filtercommand-jl.h \
        filteroperation.h \
        groupconcat.h \
        jl_logger.h \
        joblist.h \
        joblistfactory.h \
        joblisttypes.h \
        jobstep.h \
        lbidlist.h \
        limitedorderby.h \
        passthrucommand-jl.h \
        primitivemsg.h \
        profiling.h \
        resourcedistributor.h \
        resourcemanager.h \
        rowestimator.h \
        rtscommand-jl.h \
        tableband.h \
        tablecolumn.h \
        threadsafequeue.h \
        timeset.h \
        timestamp.h \
        bandeddl.h \
        bucketdl.h \
        constantdatalist.h \
        datalist.h \
        datalistimpl.h \
        elementcompression.h \
        fifo.h \
        largedatalist.h \
        swsdl.h \
        tuplewsdl.h \
        wsdl.h \
        zdl.h \
        unique32generator.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

