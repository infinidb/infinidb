/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/******************************************************************************************
* $Id$
*
******************************************************************************************/
#include <unistd.h>
#include <string>
using namespace std;

#include <boost/thread/mutex.hpp>
using namespace boost;

#ifdef _MSC_VER
#include "idbregistry.h"
#endif
#include "installdir.h"

namespace startup
{
/* static */
mutex StartUp::fInstallDirLock;
/* static */
string* StartUp::fInstallDirp=0;

/* static */
const string StartUp::installDir()
{
	mutex::scoped_lock lk(fInstallDirLock);

	if (fInstallDirp)
		return *fInstallDirp;

#ifdef _MSC_VER
	fInstallDirp = new string("C:\\Calpont");
	string cfStr = IDBreadRegistry("");
	if (!cfStr.empty())
		*fInstallDirp = cfStr;
#else
	fInstallDirp = new string("/usr/local/Calpont");
	//See if we can figure out the install dir in Linux...
	//1. env var INFINIDB_INSTALL_DIR
	const char* p=0;
	p = getenv("INFINIDB_INSTALL_DIR");
	if (p && *p)
		*fInstallDirp = p;
	//2. up one level from current binary location?
	//3. fall back to /usr/local/Calpont
#endif

	return *fInstallDirp;
}

}
// vim:ts=4 sw=4:

