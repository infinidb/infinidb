// $Id: sendplan.cpp 1739 2012-03-22 12:57:59Z pleblanc $
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/time.h>
#include <iomanip>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "sessionmanager.h"
using namespace execplan;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "tableband.h"
#include "joblist.h"
#include "joblistfactory.h"
using namespace joblist;

#include "configcpp.h"
#include "errorcodes.h"

#include "rowgroup.h"
using namespace rowgroup;

typedef CalpontSystemCatalog::OID OID;

namespace
{

bool vflg;

void usage()
{
	cout << "usage: sendPlan [-v|h|d|B] [-t lvl] [-s sid] plan_file ..." << endl;
	cout << "-v verbose output" << endl;
	cout << "-t lvl set trace level to lvl" << endl;
	cout << "-s sid set session id to sid" << endl;
	cout << "-d display the query, but don't run it" << endl;
	cout << "-B Bob's personal preferred output format" << endl;
	cout << "-h display this help" << endl;
}

double tm_diff(const struct timeval* st, const struct timeval* et)
{
	double sd = (double)st->tv_sec + (double)st->tv_usec / 1000000.0;
	double ed = (double)et->tv_sec + (double)et->tv_usec / 1000000.0;

	return (ed - sd);
}

//...Extract the stats string to be printed and the runtime start time from
//...the full stats string we receive from ExeMgr.
void parseStatsString(
	const string&   fullStatsString,
	string&         printStatsString,
	struct timeval& queryRunStartTime)
{
	string::size_type delimPos = fullStatsString.find ('|');
	printStatsString = fullStatsString.substr(0,delimPos);

	istringstream startTimeString( fullStatsString.substr(delimPos+1) );
	startTimeString >> queryRunStartTime.tv_sec >> queryRunStartTime.tv_usec;
}

}

int main(int argc, char** argv)
{
	vflg = false;
	uint32_t tlvl = 0;
	bool dflg = false;
	int c;
	int32_t sid = -1;
	bool Bflg = false;

	opterr = 0;

	while ((c = getopt(argc, argv, "vt:ds:Bh")) != EOF)
		switch (c)
		{
		case 't':
			tlvl = static_cast<uint32_t>(strtoul(optarg, 0, 0));
			break;
		case 'v':
			vflg = true;
			break;
		case 'd':
			dflg = true;
			break;
		case 's':
			sid = static_cast<int32_t>(strtol(optarg, 0, 0));
			break;
		case 'B':
			Bflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (dflg)
		vflg = true;

	if ((argc - optind) < 1)
	{
		usage();
		return 1;
	}

	ifstream inputf;
	ByteStream bs;
	ByteStream dbs;
	ByteStream eoq;
	ByteStream tbs;
	ByteStream statsStream;
	ByteStream::quadbyte q = 0;
	eoq << q;
	uint32_t sessionid;
	time_t t;
        SJLP jl;
	DeliveredTableMap tm;
	DeliveredTableMap::iterator iter;
	DeliveredTableMap::iterator end;
	CalpontSelectExecutionPlan csep;
	struct timeval start_time;
	struct timeval end_time;

	MessageQueueClient* mqc = 0;

	if (!dflg)
		mqc = new MessageQueueClient("ExeMgr1");

	if (sid == -1)
	{
		time(&t);
		sessionid = static_cast<uint32_t>(t);
	}
	else
	{
		sessionid = static_cast<uint32_t>(sid);
	}
	sessionid &= 0x7fffffff;
	logging::ErrorCodes errorCodes;
	for ( ; optind < argc; optind++)
	{

		inputf.open(argv[optind]);

		if (!inputf.good())
		{
			cerr << "error opening plan stream " << argv[optind] << endl;
			return 1;
		}

		bs.reset();
		inputf >> bs;

		inputf.close();

		csep.unserialize(bs);

		csep.sessionID(sessionid);
		SessionManager sm;
		csep.verID(sm.verID());
 
		csep.traceFlags(0);
		ResourceManager rm;
		jl = JobListFactory::makeJobList(&csep, rm);
		csep.traceFlags(tlvl);

		if (vflg)
		{
			if (dflg)
				cout << endl << "Query:" << endl;
			else
			{
				cout << endl << "Session: " << sessionid <<
					", Sending Query";
				if (Bflg)
					cout << " (" << argv[optind] << ')';
				cout << ':' << endl;
			}

			if (!Bflg)
				cout << csep.data() << endl << endl;
		}

		if (dflg)
			continue;

		try
		{
			dbs.reset();
			csep.serialize(dbs);
	
			gettimeofday(&start_time, 0);

			//try tuples first, but expect the worst...
			bool expectTuples = false;
			ByteStream tbs;
			ByteStream::quadbyte tqb = 4;
			tbs << tqb;
			mqc->write(tbs);
	
			//send the CSEP
			mqc->write(dbs);

			//read the response to the tuple request
			tbs = mqc->read();
			idbassert(tbs.length() == 4);
			tbs >> tqb;
			if (tqb == 4)
				expectTuples = true;

			if (!expectTuples)
				cout << "Using TableBand I/F" << endl;
			else
				cout << "Using tuple I/F" << endl;

			tm = jl->deliveredTables();
	
			iter = tm.begin();
			end = tm.end();
	
			OID toid;
			uint64_t rowTot;
			bool reported = false;
			bool needRGCtor = true;
			while (iter != end)
			{
				toid = iter->first;
				q = static_cast<ByteStream::quadbyte>(toid);
				tbs.reset();
				tbs << q;
				mqc->write(tbs);
	
				ByteStream tbbs;
				TableBand tb;
				RowGroup rg;
				rowTot = 0;
				uint16_t status = 0;
				TableBand::VBA::size_type rc;
ofstream out;
				for (;;)
				{
					tbbs = mqc->read();
#if 0
cout << tbbs.length() << endl;
out.open("bs1.dat");
idbassert(out.good());
out << tbbs;
out.close();
					tbbs = mqc->read();
cout << tbbs.length() << endl;
out.open("bs2.dat");
idbassert(out.good());
out << tbbs;
out.close();
					tbbs = mqc->read();
cout << tbbs.length() << endl;
out.open("bs3.dat");
idbassert(out.good());
out << tbbs;
out.close();
#endif
					if(tbbs.length()) 
					{
						if (!expectTuples)
							tb.unserialize(tbbs);
						else
						{
							if (needRGCtor)
							{
								rg.deserialize(tbbs);
								needRGCtor = false;
								tbbs = mqc->read();
							}
							rg.setData((uint8_t*)tbbs.buf());
						}
					}
					else
					{//@bug 1346
						if (!status)
							status = logging::makeJobListErr;
						break;
					}
					if (!expectTuples)
					{
						rc = tb.getRowCount();
						status = tb.getStatus();
					}
					else
					{
						rc = rg.getRowCount();
						status = rg.getStatus();
						if (rc == 0) status = 0;
					}
					if (rc == 0)
						break;
					rowTot += rc;
				}
				BatchPrimitive* step = dynamic_cast<BatchPrimitive*>( iter->second.get() );
				if (vflg && step)
				{
					cout << "For table " << step->tableName();
					if (!Bflg)
						cout << " " << toid;
					cout << ": read " << rowTot << " rows" << endl;
				}
				if (status && !reported)
				{
					cout << "### Query failed: " << errorCodes.errorString(status) << "  Check crit.log\n";
					reported = true;
				}
				if (!step && !reported)
				{
					cout << "### Query failed: Did not return project BatchPrimitive. Check crit.log\n";
					reported = true;
				}
	
				++iter;
			}
	
			if (vflg)
			{
				gettimeofday(&end_time, 0);
				cout << "Query time: " << fixed << setprecision(1) << tm_diff(&start_time, &end_time) <<
					" secs" << endl;
	
				//...Ask for query stats through special table id of 3
				const OID TABLE_ID_TO_GET_QUERY_STATS = 3;
				if (!Bflg)
					cout << "Retrieving stats..." << endl;
				toid = TABLE_ID_TO_GET_QUERY_STATS;
				q = static_cast<ByteStream::quadbyte>(toid);
				statsStream.reset();
				statsStream << q;
				mqc->write(statsStream);
	
				ByteStream bs_statsString;
				bs_statsString = mqc->read();	
				string statsString;
				bs_statsString >> statsString;
	
				string printStatsString;
				struct timeval startRunTime;
				parseStatsString (statsString, printStatsString, startRunTime);
				cout << printStatsString << "; QuerySetupTime-" <<
					tm_diff(&start_time, &startRunTime) << "secs" << endl;
			}
			//...Close this query/session
			mqc->write(eoq);
				jl.reset();
		}
		catch(const exception& ex)
		{
			cout << "### SendPlan caught an exception: " << ex.what() << endl;
		}
	}
// 	jl.reset();
      	CalpontSystemCatalog::removeCalpontSystemCatalog( sessionid );
	config::Config::deleteInstanceMap();

	delete mqc;

	return 0;
}
// vim:ts=4 sw=4:

