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

/***********************************************************************
*   $Id: sqlstatementlist.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#define DDLPKG_DLLEXPORT
#include "ddlpkg.h"
#undef DDLPKG_DLLEXPORT

namespace ddlpackage {
	using namespace std;

	
	ostream &operator<<(ostream& os, const SqlStatementList &ssl)
	{
		vector<SqlStatement*>::const_iterator itr;
		
 		for(itr = ssl.fList.begin(); itr != ssl.fList.end(); ++itr) {
			SqlStatement &stmt = **itr;
			os << stmt;
		}
		return os;
	}


	void SqlStatementList::push_back(SqlStatement* v)
	{
		fList.push_back(v);
	}


	SqlStatementList::~SqlStatementList()
	{
		vector<SqlStatement*>::iterator itr;
		for(itr = fList.begin(); itr != fList.end(); ++itr) {
			delete *itr;
		}
	}
	
}
