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

// $Id: tpchrf2.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <istream>
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/tokenizer.hpp>
using namespace boost;

#include "tpchrf2.h"
#include "dmlif.h"

namespace tpch
{

RF2::RF2(const string& sn, uint32_t sid, uint32_t tflg, int c, int p, bool d, bool v) : 
	fSchema(sn), fSessionID(sid), fTflg(tflg), fIntvl(c), fPack(p), fDflg(d), fVflg(v)
{
}

RF2::~RF2()
{
}

int RF2::run(istream& in)
{
	const streamsize ilinelen = 1024;
	scoped_array<char> iline(new char[ilinelen]);
	int cnt = 0;
	dmlif::DMLIF dmlif(fSessionID, fTflg, fDflg, fVflg);
	for (;;)
	{
		dmlif.rf2Start(fSchema);
		for (int i = 0; i < fPack; i++)
		{
			in.getline(iline.get(), ilinelen);
			if (in.eof())
				break;
			typedef char_separator<char> cs;
			typedef tokenizer<cs> tk;
			cs sep("|");
			tk tok(string(iline.get()), sep);
			tk::iterator iter = tok.begin();
			idbassert(iter != tok.end());
			string keystr = *iter; ++iter;
			//idbassert(iter == tok.end());
			int64_t okey = strtol(keystr.c_str(), 0, 0);
			dmlif.rf2Add(okey);
		}
		dmlif.rf2Send();
		cnt++;
		if ((cnt % fIntvl) == 0)
			dmlif.sendOne("COMMIT;");
		if (in.eof())
			break;
	}
	dmlif.sendOne("COMMIT;");
	return 0;
}

}
