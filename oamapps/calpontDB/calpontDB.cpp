/******************************************************************************************
 * $Id: calpontDB.cpp 419 2007-07-22 17:18:00Z dhill $
 *
 ******************************************************************************************/
/**
 * @file
 */

#include <iostream>
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
#include <sys/signal.h>
#include <sys/types.h>

#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace oam;
using namespace config;

namespace {

void usage(char *prog)
{

	cout << endl;
	cout << "Usage: " << prog << " [options]" << endl;

	cout << endl;
	cout << "This utility is used to suspend and resume Calpont Database Writes." << endl;
	cout << "Normally this would be done while performing Database Backups and" << endl;
	cout << "Restores " << endl;
	cout << endl;

	cout << "Options:" << endl;
	cout << "-c <command>   Command: suspend or resume" << endl << endl;
	cout << "-h             Display this help." << endl << endl;
}
}

int main(int argc, char **argv)
{
	string command;
	Oam oam;

	char c;
	
	// Invokes member function `int operator ()(void);'
	while ((c = getopt(argc, argv, "c:h")) != -1) {
		switch (c) {  
			case 'c': 
				command = optarg;
				break;
			case 'h':
				usage(argv[0]);
				exit(-1);
				break;
			default: 
				usage(argv[0]);
				exit(1);
				break;
		}
	}

	if ( command == "suspend" ) {
		try{
			oam.stopDDLProcessing();
			oam.stopDMLProcessing();
			
			sleep(5);
			int rtnCode = system("/usr/local/Calpont/bin/save_brm  > /var/log/Calpont/save_brm.log1 2>&1");
			if (rtnCode == 0)
				cout << endl << "Suspend Calpont Database Writes Request successfully completed" << endl;
			else
				cout << endl << "Suspend Calpont Database Writes Failed: save_brm Failed" << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** Suspend Calpont Database Writes Failed: " << e.what() << endl;
		}
		catch(...)
		{
			cout << endl << "**** Suspend Calpont Database Writes Failed" << endl;
		}
	}
	else
	{
		if ( command == "resume" ) {
			try{
				oam.resumeDDLProcessing();
				oam.resumeDMLProcessing();
				cout << endl << "Resume Calpont Database Writes Request successfully completed" << endl;
			}
			catch (exception& e)
			{
				cout << endl << "**** Resume Calpont Database Writes Failed: " << e.what() << endl;
			}
			catch(...)
			{
				cout << endl << "**** Resume Calpont Database Writes Failed" << endl;
			}
		}
		else
		{
			cout << "Invalid Command Entered, please try again" << endl;
			exit(-1);
		}
	}

	exit(0);
}

