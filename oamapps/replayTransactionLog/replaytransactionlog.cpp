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

// WWW - Add header comment.
#include <iostream>
#include <string>
#include <sstream>
#include "liboamcpp.h"

using namespace std;
using namespace oam;

#include "replaytxnlog.h"
namespace {

void usage(char *prog)
{

	cout << endl;
	cout << "Usage: " << prog << " [options]" << endl;

	cout << endl;
	cout << "This utility can be used after a backup is restored to report transactions that " << endl;
	cout << "occurred after the backup.  It begins with the first transaction that was committed " << endl;
	cout << "after the backup and reports DDL and DML statements as well as imports." << endl;
	cout << endl;

	cout << "Options:" << endl;
/*
	cout << "-u <user>      Database user id." << endl << endl;

	cout << "-p <password>  Password." << endl << endl;
*/
	cout << "-d <stop date> Stop date and time as mm/dd/yy@hh:mm:ss or 'Now'." << endl;
	cout << "               Only transactions committed before this date and time will be reported." << endl;
	cout << "               The current date and time will be used if 'Now'." << endl << endl;

/*
	cout << "-i             Ignore bulk load log entries." << endl;
	cout << "               The program will pause and prompt at bulk load entries by default." << endl << endl;

	cout << "-e             Report mode. The sql statements will be displayed to the console only.  No" << endl;
	cout << "               transactions will be processed.  The user and password will be ignored." << endl << endl;
*/

	cout << "-h             Display this help." << endl << endl;
}

bool isRunningOnPm()
{
    Oam oam;
    oamModuleInfo_t t;
    string moduleType;
    int installType = -1;

    char* csc_ident = getenv("CALPONT_CSC_IDENT");
    if (csc_ident == 0 || *csc_ident == 0)
    {
        //get local module info valdiate running on a pm
        try {
            t = oam.getModuleInfo();
            moduleType = boost::get<1>(t);
            installType = boost::get<5>(t);
        }
        catch (exception& e) {
            moduleType = "pm";
        }
    }
    else
        moduleType = csc_ident;

    if ( installType != oam::INSTALL_COMBINE_DM_UM_PM ) {
        if ( moduleType != "pm" ) {
            cerr << "Exiting, ReplayTransactionLog can only be run on a performance module (pm)" << endl;
            return false;
        }
    }
    return true;
}
}

int main(int argc, char **argv)
{

	string user;
	string password;
	string stopDate;
	bool ignoreBulk = false;
	bool reportMode = false;
	char c;
	
	// Invokes member function `int operator ()(void);'
	while ((c = getopt(argc, argv, "u:p:d:ihe")) != -1) {
		switch (c) {  
/*
			case 'u': 
				user = optarg;
				break;
			case 'p': 
				password = optarg;
				break;
*/
			case 'd':
				stopDate = optarg;
				break;
/*
			case 'i': 
				ignoreBulk = true;
				break;
			case 'e': 
				reportMode = true;
				break;
*/
			case 'h':
				usage(argv[0]);
				return 0;
				break;
			default: 
				usage(argv[0]);
				return 1;
				break;
		}
	}

	if(!isRunningOnPm()) {
		return 0;
	}

	ReplayTxnLog replayTxnLog(user, password, stopDate, ignoreBulk, reportMode);
	replayTxnLog.process();

	return 0;
}

