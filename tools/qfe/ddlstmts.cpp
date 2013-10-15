#include <unistd.h>
#include <string>
#include <stdexcept>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include "ddlstmts.h"

namespace qfe
{
extern string DefaultSchema;
}

namespace
{
using namespace qfe;

int processDDLStmt(const string& stmt, uint32_t sid)
{
	MessageQueueClient* mq=0;
	SqlParser parser;

	parser.setDefaultSchema(DefaultSchema);
	parser.Parse(stmt.c_str());
	if (parser.Good())
	{
		const ddlpackage::ParseTree& ptree = parser.GetParseTree();
		SqlStatement &ddlstmt = *ptree.fList[0];
		ddlstmt.fSessionID = sid;
		ddlstmt.fSql = stmt;
		ddlstmt.fOwner = DefaultSchema;
		ByteStream bytestream;
		bytestream << ddlstmt.fSessionID;
		ddlstmt.serialize(bytestream);
		mq = new MessageQueueClient("DDLProc");
		scoped_ptr<MessageQueueClient> smq(mq);
		ByteStream::byte b=0;
		mq->write(bytestream);
		bytestream = mq->read();
		bytestream >> b;
		string emsg;
		bytestream >> emsg;
		if (b != 0)
			throw runtime_error(emsg);
	}
	else
	{
		throw runtime_error("syntax error");
	}

	return 0;
}

}

namespace qfe
{

void processCreateStmt(const string& stmt, uint32_t sid)
{
	processDDLStmt(stmt, sid);
}

void processDropStmt(const string& stmt, uint32_t sid)
{
	processDDLStmt(stmt, sid);
}

} //namespace qfe

