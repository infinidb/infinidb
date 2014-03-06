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
 * $Id: rollback.cpp 1825 2013-01-24 18:41:00Z pleblanc $
 *
 ****************************************************************************/

/*
 * A tool to print current transactions & roll them back if necessary.
 * 
 * A tool to print current transactions & roll them back if necessary.
 * The proper way to use it is:
 * 		rollback -p   (to get the transaction(s) that need to be rolled back)
 * 		rollback -r transID  	(for each one to roll them back)
 */

#include "dbrm.h"

#include <iostream>
#include <stdlib.h>

#include "IDBPolicy.h"

using namespace std;
using namespace BRM;

void help(string name)
{

	cout << "Usage: " << endl << name << " -r txnID" << endl;
	cout << name << " -p" << endl << endl;
	cout << "Options:" << endl;
	cout << "	-r -- rollback a transaction in the BRM data structures" << endl;
	cout << "	-p -- print current transactions according to the BRM" << endl;
}



void printTxnIDs() 
{
	DBRM brm;
	set<VER_t> txnList;
	set<VER_t>::iterator it;
	int err;

	err = brm.getCurrentTxnIDs(txnList);
	if (err != 0)
		return;
	for (it = txnList.begin(); it != txnList.end(); it++)
		cout << *it << endl;	
}


void rollbackTxn(VER_t txnID)
{
	DBRM brm;
	vector<LBID_t> lbidList;
	int err;

	err = brm.getUncommittedLBIDs(txnID, lbidList);
	if (err != 0)
		return;
	err = brm.vbRollback(txnID, lbidList);
	if (err != 0)
		return;
	cout << "OK." << endl;
}

int main(int argc, char **argv) {

	int opt;
	char options[] = "pr:";
	VER_t txnID = -1;
	string progname(argv[0]);

	while ((opt = getopt (argc, argv, options)) != -1) {
		switch (opt) {
			case 'r':
				txnID = atoi (optarg);
				if (txnID < 1) {
					help(progname);
					exit(0);
				}
				idbdatafile::IDBPolicy::configIDBPolicy();
				rollbackTxn(txnID);
				exit(0);
			case 'p':
				printTxnIDs();
				exit(0);
		}
	}
	help(progname);
	exit(0);
}

