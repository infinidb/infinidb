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

#include <iostream>
#include <string>
#include <ctime>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "thrift/TProcessor.h"
#include "thrift/protocol/TProtocol.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/transport/TServerTransport.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransport.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/server/TSimpleServer.h"
namespace at=apache::thrift;
namespace atp=at::protocol;
namespace att=at::transport;
namespace ats=at::server;

#include "QueryTeleService.h"
using namespace querytele;

namespace
{

class QueryTeleServiceHandler : public QueryTeleServiceIf
{
public:
	void postQuery(const QueryTele&);
	void postStep(const StepTele&);
	void postImport(const ImportTele&);

protected:

private:

};

const string st2str(enum StepType::type t)
{
	switch (t)
	{
	case StepType::T_HJS: return "HJS";
	case StepType::T_DSS: return "DSS";
	case StepType::T_CES: return "CES";
	case StepType::T_SQS: return "SQS";
	case StepType::T_TAS: return "TAS";
	case StepType::T_TNS: return "TNS";
	case StepType::T_BPS: return "BPS";
	case StepType::T_TCS: return "TCS";
	case StepType::T_HVS: return "HVS";
	case StepType::T_WFS: return "WFS";
	case StepType::T_SAS: return "SAS";
	case StepType::T_TUN: return "TUN";
	default: return "INV";
	}
	return "INV";
}

void QueryTeleServiceHandler::postQuery(const QueryTele& qt)
{
	cout << "postQuery: " << endl;
	cout << "  uuid: " << qt.query_uuid << endl;
	if (qt.msg_type == QTType::QT_SUMMARY)
		cout << "  mt: SUMMARY" << endl;
	else if (qt.msg_type == QTType::QT_START)
		cout << "  mt: START" << endl;
	else
		cout << "  mt: PROGRESS" << endl;
	cout << "  qry: " << qt.query << endl;
	cout << "  mmpct: " << qt.max_mem_pct << endl;
	cout << "  cache: " << qt.cache_io << endl;
	cout << "  nmsgs: " << qt.msg_rcv_cnt << endl;
	cout << "  rows: " << qt.rows << endl;
	cout << "  qt: " << qt.query_type << endl;
	int64_t tt = qt.start_time;
	cout << "  st: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	tt = qt.end_time;
	cout << "  et: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	cout << "  sn: " << qt.system_name << endl;
	cout << "  mn: " << qt.module_name << endl;
	cout << "  lq: " << qt.local_query << endl;
	cout << "  dn: " << qt.schema_name << endl;
	cout << endl;
}

void QueryTeleServiceHandler::postStep(const StepTele& qt)
{
	cout << "postStep: " << endl;
	cout << "  quuid: " << qt.query_uuid << endl;
	cout << "  uuid: " << qt.step_uuid << endl;
	if (qt.msg_type == STType::ST_SUMMARY)
		cout << "  mt: SUMMARY" << endl;
	else if (qt.msg_type == STType::ST_START)
		cout << "  mt: START" << endl;
	else
		cout << "  mt: PROGRESS" << endl;
	cout << "  st: " << st2str(qt.step_type) << endl;
	cout << "  cache: " << qt.cache_io << endl;
	cout << "  nmsgs: " << qt.msg_rcv_cnt << endl;
	cout << "  rows: " << qt.rows << endl;
	if (qt.total_units_of_work > 0)
		cout << "  pct: " << qt.units_of_work_completed*100/qt.total_units_of_work << endl;
	else
		cout << "  pct: n/a" << endl;
	int64_t tt = qt.start_time;
	cout << "  st: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	tt = qt.end_time;
	cout << "  et: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	cout << endl;
}

void QueryTeleServiceHandler::postImport(const ImportTele& qt)
{
	cout << "importStep: " << endl;
	cout << "  juuid: " << qt.job_uuid << endl;
	cout << "  iuuid: " << qt.import_uuid << endl;
	if (qt.msg_type == ITType::IT_SUMMARY)
		cout << "  mt: SUMMARY" << endl;
	else if (qt.msg_type == ITType::IT_START)
		cout << "  mt: START" << endl;
	else if (qt.msg_type == ITType::IT_TERM)
		cout << "  mt: TERM" << endl;
	else
		cout << "  mt: PROGRESS" << endl;
	if (qt.table_list.empty())
		cout << "  tn: " << "(empty)" << endl;
	else
		cout << "  tn: " << qt.table_list[0] << endl;
	if (qt.rows_so_far.empty())
		cout << "  rows: " << "(empty)" << endl;
	else
		cout << "  rows: " << qt.rows_so_far[0] << endl;
	int64_t tt = qt.start_time;
	cout << "  st: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	tt = qt.end_time;
	cout << "  et: (" << tt << ") ";
	tt /= 1000;
	cout << ctime(&tt);
	cout << "  sn: " << qt.system_name << endl;
	cout << "  mn: " << qt.module_name << endl;
	cout << "  dn: " << qt.schema_name << endl;
	cout << endl;
}

}

int main(int argc, char **argv) {

  shared_ptr<atp::TProtocolFactory> protocolFactory(new atp::TBinaryProtocolFactory());
  shared_ptr<QueryTeleServiceHandler> handler(new QueryTeleServiceHandler());
  shared_ptr<at::TProcessor> processor(new QueryTeleServiceProcessor(handler));
  shared_ptr<att::TServerTransport> serverTransport(new att::TServerSocket(9990));
  shared_ptr<att::TTransportFactory> transportFactory(new att::TBufferedTransportFactory());

  ats::TSimpleServer server(processor,
                       serverTransport,
                       transportFactory,
                       protocolFactory);


  cout << "Starting the server..." << endl;
  server.serve();
  cout << "done." << endl;
  return 0;
}

