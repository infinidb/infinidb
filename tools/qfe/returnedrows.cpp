#include <unistd.h>
#include <iostream>
#include <string>
#include <cerrno>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#ifndef _MSC_VER
#include "config.h"
#endif

#include "socktype.h"

#include "exceptclasses.h"

#include "socketio.h"

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "rowgroup.h"
using namespace rowgroup;

namespace qfe
{

void processReturnedRows(MessageQueueClient* mqc, SockType fd)
{
	scoped_ptr<MessageQueueClient> cleaner(mqc);
	SBS sbs;
	sbs = mqc->read();
	//cerr << "got a bs of " << sbs->length() << " bytes" << endl;

	RowGroup rg;
	rg.deserialize(*sbs);

	//cerr << "got a base rowgroup with rows of " << rg.getRowSize() << " bytes" << endl;
	//cerr << rg.toString() << endl;

	ByteStream bs;
	ByteStream::quadbyte tableOID=100;
	bs.reset();
	bs << tableOID;
	mqc->write(bs);

	sbs = mqc->read();
	//cerr << "got a bs of " << sbs->length() << " bytes" << endl;
	RGData rgd;
	rgd.deserialize(*sbs, true);
	rg.setData(&rgd);
	//cerr << "got a rowgroup with: " << rg.getRowCount() << " rows" << endl;

	socketio::writeString(fd, "OK");
	Row r;
	while (rg.getRowCount() > 0)
	{
		rg.initRow(&r);
		rg.getRow(0, &r);
		string csv;
		bs.reset();
		for (unsigned i = 0; i < rg.getRowCount(); i++)
		{
			csv = r.toCSV();
			bs << csv;
			r.nextRow();
		}
		//cerr << "writing " << bs.length() << " bytes back to client" << endl;
		SockWriteFcn(fd, bs.buf(), bs.length());

		bs.reset();
		bs << tableOID;
		mqc->write(bs);

		sbs = mqc->read();
		//cerr << "got a bs of " << sbs->length() << " bytes" << endl;
		rgd.deserialize(*sbs, true);
		rg.setData(&rgd);
		//cerr << "got a rowgroup with: " << rg.getRowCount() << " rows" << endl;
	}

	tableOID=0;
	bs.reset();
	bs << tableOID;
	mqc->write(bs);

	//sync with the client on end-of-results
	SockWriteFcn(fd, &tableOID, 4);
	SockReadFcn(fd, &tableOID, 4);

}

}

