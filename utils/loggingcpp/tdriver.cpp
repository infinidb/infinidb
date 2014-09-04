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

#include <string>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "messageids.h"
using namespace logging;
using namespace config;

class MessageLoggingTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( MessageLoggingTest );

CPPUNIT_TEST( m1 );
CPPUNIT_TEST( m2 );
CPPUNIT_TEST( m3 );
CPPUNIT_TEST( m4 );
CPPUNIT_TEST( m5 );

CPPUNIT_TEST_SUITE_END();

private:
	LoggingID lid;

public:
	void setUp() {
		setenv("CALPONT_CONFIG_FILE", "./Calpont.xml", 1);
	}

	void tearDown() {
	}

void m1()
{
	Message::Args args;
	args.add("hello");
	args.add("world");
	args.add(123);
	args.add(1234.55);
	Message m(100);
	m.fMsg = "%1% %2% %3% %4%";
	m.format(args);
	//CPPUNIT_ASSERT(m.msg() == "hello world 123 1234.55");
	m.reset();
	m.fMsg = "%1% %2% %3% %4%";
	args.reset();
	m.format(args);
	//CPPUNIT_ASSERT(m.msg() == "   ");
	LoggingID lid(7);
	MessageLog ml(lid);
	m.reset();
	args.add("hello");
	args.add("world");
	args.add(123);
	args.add(1234.55);
	m.format(args);
	ml.logDebugMessage(m);

	args.reset();
	args.add("begin CEP generation");
	m.reset();
	m.format(args);
	ml.logInfoMessage(m);

	args.reset();
	args.add("end CEP generation");
	m.reset();
	m.format(args);
	ml.logInfoMessage(m);

	args.reset();
	args.add("something took too long");
	m.reset();
	m.format(args);
	ml.logWarningMessage(m);

	args.reset();
	args.add("something seriously took too long");
	m.reset();
	m.format(args);
	ml.logSeriousMessage(m);
	ml.logErrorMessage(m);

	args.reset();
	args.add("something critical took too long");
	m.reset();
	m.format(args);
	ml.logCriticalMessage(m);

	LoggingID lid1;
	MessageLog ml1(lid1);
	args.reset();
	m.reset();
	args.add("subsystem 0 = Calpont test");
	m.format(args);
	ml1.logDebugMessage(m);

	LoggingID lid2(1000);
	MessageLog ml2(lid2);
	args.reset();
	m.reset();
	args.add("subsystem above MAX = Calpont test");
	m.format(args);
	ml2.logDebugMessage(m);

	LoggingID lid3(7);
	MessageLog ml3(lid3);
	args.reset();
	m.reset();
	args.add("subsystem 7 = calpont-console test");
	m.format(args);
	ml3.logDebugMessage(m);
        Config::deleteInstanceMap();

}

void m2()
{
	Message m1(100);
	CPPUNIT_ASSERT(m1.msgID() == 100);
	Message m2(10);
	CPPUNIT_ASSERT(m2.msgID() == 10);
	m2 = m1;
	CPPUNIT_ASSERT(m2.msgID() == 100);
	Message m3(m2);
	CPPUNIT_ASSERT(m3.msgID() == m2.msgID());
	Message m4(99);
	Message m5(199);
	CPPUNIT_ASSERT(m4.msgID() == 99);
	CPPUNIT_ASSERT(m5.msgID() == 199);
	m4.swap(m5);
	CPPUNIT_ASSERT(m5.msgID() == 99);
	CPPUNIT_ASSERT(m4.msgID() == 199);
        Config::deleteInstanceMap();
}

void m3()
{
	LoggingID lid1(1, 2, 3, 4);
	MessageLog ml1(lid1);
	CPPUNIT_ASSERT(ml1.fLogData.fSubsysID == 1);

	LoggingID lid2(10, 20, 30, 40);
	MessageLog ml2(lid2);
	CPPUNIT_ASSERT(ml2.fLogData.fSubsysID == 10);

	ml2 = ml1;
	CPPUNIT_ASSERT(ml2.fLogData.fSubsysID == 1);

	MessageLog ml3(ml2);
	CPPUNIT_ASSERT(ml3.fLogData.fSubsysID == 1);
        Config::deleteInstanceMap();
}

void m4()
{
	LoggingID lid1(100, 200, 300, 400);
	MessageLog ml1(lid1);
	Message::Args args;
	Message* m;
	args.add("hello");
	args.add("world");
	args.add(123);
	args.add(1234.55);
	for (int i = 0; i < 4; i++)
	{
		m = new Message(i);
		m->format(args);
		ml1.logDebugMessage(*m);
		delete m;
	}
        Config::deleteInstanceMap();
}

//------------------------------------------------------------------------------
// This method is intended to test the messages used to profile db performance.
// The method also provides an example on how to use these log messages.
// Test can be verified by viewing /var/log/Calpont/debug.log.
//
// Message types are:
//
//   26  Start Transaction
//   27  End Transaction
//   28  Start Statement
//   29  End Statement
//   30  Start Step
//   31  End Step
//   32  I/O Reads
//
// The messages should be logged in the following way:
//
// 1. The application should log a StartTransaction message at the beginning
//    of a database transaction.
// 2. When the application begins processing a statement, the StartStatement
//    message should be logged.
// 3. As each primitive step is executed, it's start time should be recorded
//    by logging a StartStep message.
// 4. During the execution of a step,  1 or more I/O Read messages should be
//    logged to record the I/O block count used in accessing each object. If
//    necessary,  more than 1 message  can be logged for the same object and
//    step.   In these cases, the script that post-processes the syslog will
//    add up the block counts for the same object and step.
// 5. Upon completion of each primitive step,  an EndStep message should be
//    logged.  If multiple steps are executing in parallel, the EndStep msg
//    should be logged as each step completes.  After all these stpes are
//    completed and logged, if needed, a new set of StartStep messages can
//    be logged (for the same statement), for a new set of parallel steps.
// 6. Upon completion of each statement, an EndStatement msg should be logged.
// 7. Upon completion of each transaction, an EndTransaction msg should
//    be logged.
//
//    Some possible enhancements to simplify this profile logging for the
//    application programmer:
//
//    1. Define enum or const ints in a common header file to represent the
//       list of valid subSystem ids.
//
//    2. Add set of helper methods to MessageLog class to reduce the
//       amount of work for the application.  For example a method like:
//
//         void logStartStatement ( int statement, int ver, string SQL );
//
//       would allow the application to log a StartStatement by simply doing;
//
//         Message msgStartStatement ( M0028 );
//         int statementId = 11;
//         int versionId   = 22;
//         string sql ("SELECT column1, column2 FROM table1 WHERE ...
//         msgStartStatement.logStartStatement ( statementId, versionId, sql);
//
//    3. Could also do something similar to #2 except instead of adding helper
//       methods to MessageLog, we could add specialized classes that derive
//       from MessageLog or contain a MessageLog.  One advantage of doing it
//       this way is that a derived class like MessageLogStatement "could" be
//       implemented to log the StartStatement, and its desctructor could then
//       log the EndStatement automatically, on behalf of the application.
//       
//------------------------------------------------------------------------------
void m5()
{
        int subSystem   = 5; // joblist subSystem
        int session     = 100;
        int transaction = 1;
	int thread      = 0;
	
	LoggingID  lid1   ( subSystem, session, transaction, thread );
	MessageLog msgLog ( lid1 );

	Message::Args args;

	// Log the start time of a transaction
	Message msgStartTrans  ( M0026 );
	msgStartTrans.format   ( args  );
	msgLog.logDebugMessage ( msgStartTrans );

	// Log the start of execution time for a SQL statement
	Message msgStartStatement ( M0028 );
	int statementId = 11;
	int versionId   = 22;
	string sql ("SELECT column1, column2 FROM table1 WHERE column1 = 345");
	args.reset ( );
	args.add   ( statementId );
	args.add   ( versionId   );
	args.add   ( sql         ); 
	msgStartStatement.format ( args );
	msgLog.logDebugMessage   ( msgStartStatement );

	const string stepNames[] = { "steponeA", "steptwoA" ,"stepthreeA",
                                     "stepfourB","stepfiveB","stepsixB"   };

	// To process this SQL statement, simulate executing 2 job steps,
	// with each job step consisting of of 3 parallel primitive steps
	for (int jobStep=0; jobStep<2; jobStep++)
	{
		int primStep1 = jobStep * 3;

		// Log 3 parallel steps starting to execute
		for (int i=primStep1; i<(primStep1+3); i++)
		{
			Message msgStartStep ( M0030 );  // Start Step
			int stepId = i+1;
			string stepName = stepNames[i];
			args.reset ( );
			args.add   ( statementId );
			args.add   ( stepId      );
			args.add   ( stepName    );
			msgStartStep.format    ( args );
			msgLog.logDebugMessage ( msgStartStep );
		}

		// Record I/O block count for 0 or more objects per step;
		// for this example we just record I/O for 1 object per step.
		// Then log the completion of each step.
		for (int i=primStep1; i<(primStep1+3); i++)
		{
			Message msgBlockCount ( M0032 ); // I/O block count
			int stepId   = i+1;
			int objectId = stepId * 20;
			int phyCount = stepId * 30;
			int logCount = phyCount + 5;
			args.reset ( );
			args.add   ( statementId );
			args.add   ( stepId      );
			args.add   ( objectId    );
			args.add   ( phyCount    );
			args.add   ( logCount    );
			msgBlockCount.format   ( args );
			msgLog.logDebugMessage ( msgBlockCount );

			Message msgEndStep ( M0031 );    // End Step
			args.reset ( );
			args.add   ( statementId );
			args.add   ( stepId      );
			msgEndStep.format      ( args );
			msgLog.logDebugMessage ( msgEndStep );
		}
	}

	// Log the completion time of the SQL statement
	Message msgEndStatement ( M0029 );
	args.reset ( );
	args.add   ( statementId );
	msgEndStatement.format  ( args );
	msgLog.logDebugMessage  ( msgEndStatement );

	// Log the completion time of the transaction
	Message msgEndTrans ( M0027 );
	args.reset ( );
	args.add   ( string("COMMIT") );
	msgEndTrans.format     ( args );
	msgLog.logDebugMessage ( msgEndTrans );
        Config::deleteInstanceMap();
} 

}; // end of CppUnit::TestFixture class

CPPUNIT_TEST_SUITE_REGISTRATION( MessageLoggingTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}

