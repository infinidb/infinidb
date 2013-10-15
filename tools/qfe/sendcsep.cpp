#include <unistd.h>
#include <stdexcept>
#include <memory>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "calpontselectexecutionplan.h"
using namespace execplan;
#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

namespace qfe
{

MessageQueueClient* sendCSEP(CalpontSelectExecutionPlan* csep)
{
	scoped_ptr<CalpontSelectExecutionPlan> cleaner(csep);

	ByteStream bs;

	MessageQueueClient* mqc=0;

	mqc = new MessageQueueClient("ExeMgr1");
	auto_ptr<MessageQueueClient> smqc(mqc);

	bs.reset();
	ByteStream::quadbyte wantTuples=4;
	bs << wantTuples;
	mqc->write(bs);

	bs.reset();
	csep->serialize(bs);
	mqc->write(bs);

	SBS sbs;
	sbs = mqc->read();
	*sbs >> wantTuples;
	//cerr << "got flag: " << wantTuples << endl;
	string msg;
	sbs = mqc->read();
	*sbs >> msg;
	//cerr << "got msg: " << msg << endl;

	if (wantTuples != 0)
		throw runtime_error(msg);

	smqc.release();
	return mqc;
}

}

