/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
* $Id: ddlcleanup.cpp 967 2009-10-15 13:57:29Z rdempsey $
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <unistd.h>
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"
using namespace std;

#include "rwlock.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
using namespace execplan;

#include "exceptclasses.h"
using namespace logging;

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
using namespace oam;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "ddlcleanuputil.h"

namespace fs = boost::filesystem;

namespace {


void usage()
{
	cout << "Usage: ddlcleanup" << endl;
}
}

int main(int argc, char** argv)
{
	int c;
	while ((c = getopt(argc, argv, "h")) != EOF)
	switch (c)
	{
		case 'h':
		case '?':
		default:
			usage();
		return (c == 'h' ? 0 : 1);
			break;
	}
	
	return ddlcleanuputil::ddl_cleanup();
}

// vim:ts=4 sw=4:

