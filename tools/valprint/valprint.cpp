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

// $Id: valprint.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <iostream>
#include <cstdlib>
using namespace std;

#include "configcpp.h"
using namespace config;

int main(int argc, char** argv)
{
	int c;
	opterr = 0;
	while ((c = getopt(argc, argv, "")) != EOF)
		switch (c)
		{
		case '?':
		default:
			break;
		}

	string s;
	for (int i = optind; i < argc; i++)
	{
		s = argv[i];
		cout << s << " = " << Config::fromText(s) << endl;
	}

	return 0;
}

