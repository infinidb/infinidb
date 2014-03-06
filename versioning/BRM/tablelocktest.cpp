#include <iostream>

#include "dbrm.h"


using namespace std;
using namespace BRM;

void grabLock()
{
	DBRM dbrm;
	uint64_t id;
	vector<uint32_t> pmlist;

	string name = "init";
	uint32_t pid = 1;
	int32_t sid = 5678;

	pmlist.push_back(1);

	try {
		id = dbrm.getTableLock(pmlist, 1234, &name, &pid, &sid, &sid, LOADING);
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}

	cout << "lock ID = " << id << endl;
	if (id == 0)
		cout << " existing owner name = " << name << " pid = " << pid << " session = " << sid << endl;

}

void releaseLock(uint64_t id)
{
	DBRM dbrm;
	bool ret;

	cout << "releasing lock " << id << endl;

	try {
		ret = dbrm.releaseTableLock(id);
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}

	if (ret)
		cout << "success\n";
	else
		cout << "failed\n";
}

void changeState(uint64_t id)
{
	DBRM dbrm;
	bool ret;

	cout << "changing state of lock " << id << endl;

	try {
		ret = dbrm.changeState(id, CLEANUP);
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}

	if (ret)
		cout << "success\n";
	else
		cout << "failed\n";
}

void changeOwner(uint64_t id)
{
	DBRM dbrm;
	bool ret;

	cout << "changing owner of lock " << id << endl;

	try {
		ret = dbrm.changeOwner(id, "blah blah", 5678, 1234, 9999);
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}

	if (ret)
		cout << "success\n";
	else
		cout << "failed\n";
}

void printAllLocks()
{
	DBRM dbrm;
	vector<TableLockInfo> locks;
	uint32_t i;

	try {
		locks = dbrm.getAllTableLocks();
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}

	cout << "got " << locks.size() << " locks\n";
	for (i = 0; i < locks.size(); i++)
		cout << "  lock[" << i << "] id=" << locks[i].id << " tableOID=" << locks[i].tableOID <<
				" owner: " << locks[i].ownerName << ":" << locks[i].ownerPID << ":" <<
				locks[i].ownerSessionID << " " << ctime(&locks[i].creationTime) << endl;
}

void releaseAllLocks()
{
	DBRM dbrm;

	try {
		dbrm.releaseAllTableLocks();
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}
	cout << "OK\n";
}

void getLockInfo(uint64_t id)
{
	DBRM dbrm;
	bool ret;
	TableLockInfo tli;

	try {
		ret = dbrm.getTableLockInfo(id, &tli);
	}
	catch (exception &e) {
		cout << "caught an exception: " << e.what() << endl;
		exit(1);
	}
	if (ret)
		cout << "id=" << tli.id << " tableOID=" << tli.tableOID <<
				" owner: " << tli.ownerName << ":" << tli.ownerPID << ":" <<
				tli.ownerSessionID <<  endl;
	else
		cout << "failed\n";
}

int main(int argc, char **argv)
{

	if (argc < 2) {
		cout << "Usage: " << argv[0] << " g | r | s | o | p | R | z. Check the code to see what they do. :P\n";
		exit(1);
	}

	char cmd = argv[1][0];

	if (cmd == 'g')
		grabLock();
	else if (cmd == 'r') {
		if (argc < 3) {
			cout << "need an ID\n";
			exit(1);
		}
		releaseLock(atoi(argv[2]));
	}
	else if (cmd == 's') {
		if (argc < 3) {
			cout << "need an ID\n";
			exit(1);
		}
		changeState(atoi(argv[2]));
	}
	else if (cmd == 'o') {
		if (argc < 3) {
			cout << "need an ID\n";
			exit(1);
		}
		changeOwner(atoi(argv[2]));
	}
	else if (cmd == 'p')
		printAllLocks();
	else if (cmd == 'R')
		releaseAllLocks();
	else if (cmd == 'z') {
		if (argc < 3) {
			cout << "need an ID\n";
			exit(1);
		}
		getLockInfo(atoi(argv[2]));
	}
	else
		cout << "bad command, need g | r | s | o | p\n";
	exit(0);
}




