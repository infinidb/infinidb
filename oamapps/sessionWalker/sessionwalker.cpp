/***************************************************************************
 * $Id: sessionwalker.cpp 1834 2011-02-08 14:37:11Z rdempsey $
 *
 *   jrodriguez@calpont.com
 *                                                                         *
 ***************************************************************************/

#include <iostream>
using namespace std;

#include "sessionmonitor.h"
using namespace execplan;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

namespace {

void usage()
{
	cout << "sessionwalker [-d|-h]" << endl <<
        "   -r rollback all transactions found" << endl <<
		"   -h display this help" << endl;
}

void rollback(const SessionMonitor::MonSIDTIDEntry& txn)
{
	VendorDMLStatement dmlStmt("ROLLBACK;", txn.sessionid);
	CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);

	if (pDMLPackage == 0)
	{
		return;
	}

	ByteStream bytestream;
	pDMLPackage->write(bytestream);
	delete pDMLPackage;
	MessageQueueClient mq("DMLProc");
	try
	{
		cout << "sending ROLLBACK for sessionID " << txn.sessionid << endl;
		mq.write(bytestream);
		bytestream = mq.read();
	}
	catch (...)
	{
	}
}

}

int main(int argc, char** argv)
{
	bool rflg = false;
	opterr = 0;
	int c;
	while ((c = getopt(argc, argv, "rh")) != EOF)
		switch (c)
		{
		case 'r':
            rflg = true;
			break;
		case 'h':
			usage();
			return 0;
			break;
		default:
			usage();
			return 1;
			break;
		}

	vector<SessionMonitor::MonSIDTIDEntry*> toTxns;
	SessionMonitor* monitor = new SessionMonitor();

    toTxns.clear();
	toTxns = monitor->timedOutTxns(); // get timed out txns

	vector<SessionMonitor::MonSIDTIDEntry*>::iterator iter = toTxns.begin();
	vector<SessionMonitor::MonSIDTIDEntry*>::iterator end = toTxns.end();

	vector<SessionMonitor::MonSIDTIDEntry*> tmp;
	while (iter != end)
	{
		if ((*iter)->sessionid > 0)
			tmp.push_back(*iter);
		++iter;
	}

	toTxns.swap(tmp);

	cout << toTxns.size() << " timed out transactions." << endl;
	for (unsigned idx=0; idx<toTxns.size();idx++)
    {
		monitor->printTxns(*toTxns[idx]);
		if (rflg)
		{
			rollback(*toTxns[idx]);
		}
    }

    delete monitor;

	return 0;
}
// vim:ts=4 sw=4:

