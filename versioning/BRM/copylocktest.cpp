#include <iostream>
#include "dbrm.h"
#include "IDBPolicy.h"

using namespace std;
using namespace BRM;

CopyLocks copylocks;
DBRM dbrm;

void lockRange()
{
	LBIDRange r;
	r.start = 12345;
	r.size = 5;
	copylocks.lock(CopyLocks::WRITE);
	if (copylocks.isLocked(r))
		cout << "already locked\n";
	else {
		copylocks.lockRange(r, -1);
		cout << "got the 1st lock\n";
	}

	r.start = 12349;
	if (copylocks.isLocked(r))
		cout << "2nd range is locked\n";
	else
		cout << "2nd range is NOT locked\n";


	r.start = 12350;
	if (copylocks.isLocked(r))
		cout << "3rd range is locked\n";
	else {
		copylocks.lockRange(r, -1);
		cout << "got the 3rd lock\n";
	}

	copylocks.release(CopyLocks::WRITE);
}

void dbrmLockRange()
{
	dbrm.lockLBIDRange(12345, 5);
	cout << "OK\n";
}

void dbrmReleaseRange()
{
	dbrm.releaseLBIDRange(12345, 5);
	cout << "OK\n";
}


void releaseRange()
{
	LBIDRange r;
	r.start = 12345;
	r.size = 5;
	copylocks.lock(CopyLocks::WRITE);
	copylocks.releaseRange(r);

	r.start = 12350;
	copylocks.releaseRange(r);
	copylocks.release(CopyLocks::WRITE);


	cout << "OK\n";
}

int main(int argc, char **argv)
{
	char cmd;

	if (argc < 2)
		goto usage;

	cmd = argv[1][0];

	idbdatafile::IDBPolicy::configIDBPolicy();

	switch(cmd) {
		case 'l': lockRange(); break;
		case 'r': releaseRange(); break;
		case 'L': dbrmLockRange(); break;
		case 'R': dbrmReleaseRange(); break;
		default: goto usage;
	}

	exit(0);
usage:
	cout << "Usage: " << argv[0] << " l | r | L | R. lock/release. Check the code for specifics. :P" << endl;
	exit(1);
}

