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

/*****************************************************************************
 * $Id: dbrmctl.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <unistd.h>
#include <iostream>
using namespace std;

#include "IDBPolicy.h"

#include "brm.h"

using namespace messageqcpp;
using namespace BRM;

DBRM dbrm(true);

namespace
{
bool vflg;

void usage(char *c) {
	cerr << "Usage: " << c << " [-vh] status | halt | resume | readonly | readwrite | reload" << endl;
	exit(1);
}

void errMsg(int err) 
{
	switch (err) {
		case ERR_OK: 
			cout << "OK.";
			if (vflg)
			{
				if (dbrm.getSystemReady() > 0)
					cout << " (and the system is ready)";
				else
					cout << " (but the system is not ready)";
			}
			cout << endl; 
			break;
		case ERR_NETWORK: 
 			cout << "Failure: an unspecific communication error." << endl; 
			break;
		case ERR_TIMEOUT: 
			cout << "Failure: controller node timed out." << endl; 
			break;
		case ERR_READONLY: 
			cout << "DBRM is currently Read Only!" << endl; 
			break;
		case 20:
			cout << "System is ready" <<endl;
			break;
		case 21:
			cout << "System is not ready" <<endl;
			break;
		default:
			cout << "Failure: an unexpected error (" << err << ")" << endl; 
			break;
	}
}

void do_halt()
{
	int err;
	
	err = dbrm.halt();
	errMsg(err);
}

void do_resume()
{
	int err;
	
	err = dbrm.resume();
	errMsg(err);
}

void do_reload()
{
	int err;

	err = dbrm.forceReload();
	errMsg(err);
}

void set_readonly(bool b)
{
	int err;

	err = dbrm.setReadOnly(b);
	errMsg(err);
}

void do_status()
{
	int err;

	err = dbrm.isReadWrite();
	errMsg(err);
}

void do_sysstatus()
{
	int err;

	bool ready = dbrm.getSystemReady() > 0 ? true : false;
	
	if (ready)
		err=20;
	else
		err = 21;
		
	errMsg(err);
}

}

int main(int argc, char **argv)
{
	int c;
	vflg = false;

	opterr = 0;

	while ((c = getopt(argc, argv, "vh")) != EOF)
		switch (c)
		{
		case 'v':
			vflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage(argv[0]);
			return (c == 'h' ? 0 : 1);
			break;
		}

 	string cmd;

	if ((argc - optind) < 1)
		usage(argv[0]);

	idbdatafile::IDBPolicy::configIDBPolicy();

	cmd = argv[optind++];

	if (cmd == "status")
		do_status();
	else if (cmd == "halt")
		do_halt();
	else if (cmd == "resume")
		do_resume();
	else if (cmd == "readonly")
		set_readonly(true);
	else if (cmd == "readwrite")
		set_readonly(false);
	else if (cmd == "reload")
		do_reload();
	else if (cmd == "sysstatus")
		do_sysstatus();
	else
		usage(argv[0]);

	return 0;
}
