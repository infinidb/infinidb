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
* $Id: svnQuery.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
*		
******************************************************************************************/
/**
 * @file
 */

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
#include "stdio.h"
#include "ctype.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "liboamcpp.h"

using namespace std;
using namespace oam;

int main(int argc, char *argv[])
{
	Oam oam;
	vector <string> queryDirectory;
	vector <string> queryID;

	// get latest set of svn queries
//	system("rm -rf /root/genii/ > /dev/null 2>&1");
	system("updateGenii.pl > /dev/null 2>&1");

	system("rm -rf /home/qa/bldqry/mysql/* > /dev/null 2>&1");

	string cronFile = "/root/calpontBuildTest";
	unlink(cronFile.c_str());

	// get query directory list
	string topQueryfileName = "/tmp/topQuerydirs";
	string secondQueryfileName = "/tmp/secondQuerydirs";
	string queryfileName = "/home/qa/bldqry/mysql/querydirs";

	string cmd = "ls /root/genii/mysql/queries/. > " + topQueryfileName;
	system(cmd.c_str());

	ifstream file (topQueryfileName.c_str());
	if (!file) {
		cout << " Error: can't open " + topQueryfileName << endl;
		exit (-1);
	}

	char line[200];
	string topQueryDir;
	while (file.getline(line, 200))
	{
		topQueryDir = line;
		if ( topQueryDir.find("queryTester",0) != string::npos || topQueryDir == "Makefile" )
			continue;

		// only do working queries for now
		if ( topQueryDir.find("working",0) == string::npos)
			continue;

		string cmd = "ls /root/genii/mysql/queries/" + topQueryDir + "/. > " + secondQueryfileName;
		system(cmd.c_str());
	
		ifstream file (secondQueryfileName.c_str());
		if (file) {
			char line[200];
			string buf;
			while (file.getline(line, 200))
			{
				buf = line;
				if ( buf.empty() )
					continue;
				string tempdir =  topQueryDir + "/" + buf;
				queryDirectory.push_back(tempdir);
			}
		}
		file.close();
	}

	file.close();

	if ( queryDirectory.size() == 0 ) {
		cout << endl << "Error: no query sub-directories located" << endl;
		exit (-1);
	}

	// get query list for each query directory
	int test = 1;
	std::vector<std::string>::iterator pt1 = queryDirectory.begin();
	for( ; pt1 != queryDirectory.end() ; pt1++)
	{
		string directory = *pt1;
		string::size_type pos = directory.find("/",0);
		if (pos != string::npos)
		{
			string setName;
			setName = directory.substr(0, pos);
			setName = setName + "-";
			setName = setName + directory.substr(pos+1, 200);
		
			string cmd = "cd /home/qa/bldqry/mysql/;echo " + setName + ".sql > " + oam.itoa(test) + ".txt";
			system(cmd.c_str());
			queryID.push_back(oam.itoa(test));
			test++;
	
			cmd = "rm -f /home/qa/bldqry/mysql/" + setName + ".sql";
			system(cmd.c_str());
	
			cmd = "cat /root/genii/mysql/queries/" + directory + "/*.sql >> /home/qa/bldqry/mysql/" + setName +  ".sql";
			system(cmd.c_str());
		}
	}

	// setup calpontBuildTest cron job with directories
	string cronFileTemplate = "/root/calpontBuildTest.template";

	ifstream cfile (cronFileTemplate.c_str());
	if (!cfile) {
		cout << endl << "Error: " + cronFileTemplate + " not found" << endl;
		exit (-1);
	}

	vector <string> lines;
	string newLine;
	string buf;
	while (cfile.getline(line, 200))
	{
		buf = line;
		string::size_type pos = buf.find("buildTester",0);
		if (pos != string::npos)
		{
	        newLine = buf;

			pt1 = queryID.begin();
			for( ; pt1 != queryID.end() ; pt1++)
			{
				newLine.append(" mysql-");
				newLine.append(*pt1);
			}

			newLine.append(" mysql-queryTester");

			buf = newLine;
		}
		//output to temp file
		lines.push_back(buf);
	}
	
	cfile.close();

   	ofstream newFile (cronFile.c_str());	
	
	//create new file
	int fd = open(cronFile.c_str(), O_RDWR|O_CREAT, 0777);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	close(fd);

	cmd = "chmod 777 " + cronFile;
	system(cmd.c_str());


}
