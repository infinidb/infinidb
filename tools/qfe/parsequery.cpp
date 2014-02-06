#include <unistd.h>
#include <string>
#include <memory>
using namespace std;

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "cseputils.h"
#include "parsequery.h"

#include "calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "constantcolumn.h"
#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
using namespace execplan;

#include "brmtypes.h"
using namespace BRM;

extern int qfeparse();
extern int qfedebug;

struct yy_buffer_state;
extern yy_buffer_state* qfe_scan_string(const char*);
extern void qfe_delete_buffer(yy_buffer_state*);
extern CalpontSelectExecutionPlan* ParserCSEP;
extern boost::shared_ptr<CalpontSystemCatalog> ParserCSC;

namespace
{
mutex ParserMutex;
} //anon namespace

namespace qfe
{
extern string DefaultSchema;

CalpontSelectExecutionPlan* parseQuery(const string& query, const uint32_t sid)
{
	//We're going to make parsing the query single-threaded for now. This makes it a lot
	// easier to interface with the parser and doesn;t materially affect overall query
	// performance (I think)
	mutex::scoped_lock lk(ParserMutex);

	boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sid);
	CalpontSelectExecutionPlan* csep=0;
	csep = new CalpontSelectExecutionPlan();
	//we use an auto_ptr here with some trepidation. We only want auto delete on an execption.
	//If the parseing and plan build succeed, we want the ptr to stay around. boost::scoped_ptr<>
	//doesn't have an API to release ownership, so we use auto_ptr...
	auto_ptr<CalpontSelectExecutionPlan> scsep(csep);

	yy_buffer_state* ybs=0;
	ybs = qfe_scan_string(query.c_str());
	if (ybs != 0)
	{
		ParserCSEP = csep;
		ParserCSC = csc;
		if (qfeparse() != 0)
			throw runtime_error("syntax error");
		qfe_delete_buffer(ybs);
	}
	else
		throw runtime_error("Internal parser memory error");

	csep->data(query);

        SessionManager sm;
        TxnID txnID;
        txnID = sm.getTxnID(sid);
        if (!txnID.valid)
        {
            txnID.id = 0;
            txnID.valid = true;
        }
        QueryContext verID;
        verID = sm.verID();

	csep->txnID(txnID.id);
	csep->verID(verID);
	csep->sessionID(sid);

	//cout << *csep << endl;
	scsep.release();
	return csep;
}

}

