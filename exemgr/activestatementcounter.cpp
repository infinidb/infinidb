/* Copyright (C) 2013 Calpont Corp.

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

// $Id: activestatementcounter.cpp 940 2013-01-21 14:11:31Z rdempsey $
//

//#define NDEBUG
#include <cassert>
#include <limits>
using namespace std;

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "vss.h"
#include "dbrm.h"
#include "brmtypes.h"
using namespace BRM;

#include "cacheutils.h"

#include "activestatementcounter.h"

void ActiveStatementCounter::incr(bool& counted)
{
	if (counted)
		return;

	counted = true;
	mutex::scoped_lock lk(fMutex);
	if (upperLimit > 0)
		while (fStatementCount >= upperLimit)
			condvar.wait(lk);
	fStatementCount++;
//	cout << "incr: fStatementCount=" << fStatementCount << endl;
}

void ActiveStatementCounter::decr(bool& counted)
{
	if (!counted)
		return;

	counted = false;
	mutex::scoped_lock lk(fMutex);
	//assert(fStatementCount > 0);
	if (fStatementCount == 0)
		return;

	--fStatementCount;
//	cout << "decr:  fStatementCount=" << fStatementCount << endl;
	condvar.notify_one();
	/* This doesn't take into account EC-mode syscat queries.  Need to think
		about whether it's actually beneficial or not.  If it must happen,
		the right place to do it is in PrimProc. */
#if 0
	if (fStatementCount == 0)
	{
		if (!fVss.isEmpty())
		{
			scoped_ptr<DBRM> dbrmp(new DBRM());
			int len;
			shared_array<SIDTIDEntry> entries = dbrmp->SIDTIDMap(len);
			if (len == 0)
			{
				BlockList_t blist;
				dbrmp->getUnlockedLBIDs(&blist);
				if (!blist.empty())
				{
					cacheutils::flushPrimProcBlocks(blist);
					dbrmp->clear();
				}
			}
		}
	}
#endif
}
// vim:ts=4 sw=4:

