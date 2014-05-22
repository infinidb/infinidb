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

/******************************************************************************************
* $Id: mycnfUpgrade.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
*		
******************************************************************************************/
/**
 * @file
 */

#include <unistd.h>
#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include <climits>
#include <cstring>
#include <glob.h>

#include "liboamcpp.h"
#include "installdir.h"

using namespace std;
using namespace oam;

int main(int argc, char *argv[])
{
	Oam oam;

	//check for port argument
	string mysqlPort;
	if (argc > 1) {
		mysqlPort = argv[1];

		// set mysql password
		oam.changeMyCnf( "port", mysqlPort );
		exit (0);
	}

	//my.cnf file
	string mycnfFile = startup::StartUp::installDir() + "/mysql/my.cnf";
	ifstream mycnffile (mycnfFile.c_str());
	if (!mycnffile) {
		cerr << "mycnfUpgrade - my.cnf file not found: " << mycnfFile << endl;
		exit (1);
	}

	//my.cnf.rpmsave file
	string mycnfsaveFile = startup::StartUp::installDir() + "/mysql/my.cnf.rpmsave";
	ifstream mycnfsavefile (mycnfsaveFile.c_str());
	if (!mycnfsavefile) {
		cerr << "mycnfUpgrade - my.cnf.rpmsave file not found: " << mycnfsaveFile << endl;
		exit (1);
	}

	//include arguments file
	string includeFile = startup::StartUp::installDir() + "/bin/myCnf-include-args.text";
	ifstream includefile (includeFile.c_str());
	if (!includefile) {
		cerr << "mycnfUpgrade - my.cnf include argument file not found: " << includeFile << endl;
		exit (1);
	}

	//exclude arguments file
	string excludeFile = startup::StartUp::installDir() + "/bin/myCnf-exclude-args.text";
	ifstream excludefile (excludeFile.c_str());
	if (!excludefile) {
		cerr << "mycnfUpgrade - my.cnf exclude argument file not found: " << excludefile << endl;
		exit (1);
	}

	//go though include list
	char line[200];
	string includeArg;
	while (includefile.getline(line, 200))
	{
		includeArg = line;

		//see if in my.cnf.rpmsave
		ifstream mycnfsavefile (mycnfsaveFile.c_str());
		char line[200];
		string oldbuf;
		while (mycnfsavefile.getline(line, 200))
		{
			oldbuf = line;
			string::size_type pos = oldbuf.find(includeArg,0);
			if ( pos != string::npos ) {
				//check if this is commented out
				if ( line[0] != '#' ) 
				{
					// no, find in my.cnf and replace

					ifstream mycnffile (mycnfFile.c_str());
					vector <string> lines;
					char line1[200];
					string newbuf;
					while (mycnffile.getline(line1, 200))
					{
						newbuf = line1;
						string::size_type pos = newbuf.find(includeArg,0);
						if ( pos != string::npos )
							newbuf = oldbuf;

						//output to temp file
						lines.push_back(newbuf);
					}

					//write out a new my.cnf
					mycnffile.close();
					unlink (mycnfFile.c_str());
					ofstream newFile (mycnfFile.c_str());	
					
					//create new file
					int fd = open(mycnfFile.c_str(), O_RDWR|O_CREAT, 0666);
					
					copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
					newFile.close();
					
					close(fd);
				}

				break;
			}
		}
	}

	exit (0);
}
// vim:ts=4 sw=4:

