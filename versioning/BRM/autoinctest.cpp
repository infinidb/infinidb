#include <iostream>

#include "dbrm.h"
#include "IDBPolicy.h"


using namespace std;
using namespace BRM;

/*
EXPORT void startAISequence(uint32_t OID, uint64_t firstNum, uint32_t colWidth);
EXPORT bool getAIRange(uint32_t OID, uint32_t count, uint64_t *firstNum);
EXPORT bool getAIValue(uint32_t OID, uint64_t *value);
EXPORT void resetAISequence(uint32_t OID, uint64_t value);
EXPORT void getAILock(uint32_t OID);
EXPORT void releaseAILock(uint32_t OID);
*/

DBRM dbrm;

void doStartSeq()
{

	try {
		dbrm.startAISequence(12345, 67890, 1, execplan::CalpontSystemCatalog::INT);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	cout << "OK.\n";
}

void doGetRange()
{
	uint64_t firstNum;
	bool ret;

	try {
		ret = dbrm.getAIRange(12345, 110000, &firstNum);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	if (!ret)
		cout << "getAIRange failed\n";
	else
		cout << "got firstNum " << firstNum << endl;
}

void doGetValue()
{
	uint64_t val;
	bool ret;

	try {
		ret = dbrm.getAIValue(12345, &val);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	if (!ret)
		cout << "getAIValue failed\n";
	else
		cout << "got val " << val << endl;
}

void doReset()
{

	try {
		dbrm.resetAISequence(12345, 11111);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	cout << "OK.\n";
}

void doLock()
{

	try {
		dbrm.getAILock(12345);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	cout << "OK.\n";
}

void doUnlock()
{

	try {
		dbrm.releaseAILock(12345);
	}
	catch (exception &e) {
		cout << "got exception: " << e.what() << endl;
		exit(1);
	}
	cout << "OK.\n";
}

int main(int argc, char **argv)
{

	if (argc < 2) {
		cout << "Usage: " << argv[0] << " s | r | v | R | l | u. Check the code to see what they do. :P\n";
		exit(1);
	}

	char cmd = argv[1][0];

	idbdatafile::IDBPolicy::configIDBPolicy();

	switch(cmd) {
		case 's': doStartSeq(); break;
		case 'r': doGetRange(); break;
		case 'v': doGetValue(); break;
		case 'R': doReset(); break;
		case 'l': doLock(); break;
		case 'u': doUnlock(); break;
		default:
			cout << "Usage: " << argv[0] << " s | r | v | R | l | u. Check the code to see what they do. :P\n";
			exit(1);
	}
}






















