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

// $Id: tdriver-tableband.cpp 7396 2011-02-03 17:54:36Z rdempsey $
#include <iostream>
#include <list>
#include <sstream>
#include <pthread.h>
#include <iomanip>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "fifo.h"

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "tablecolumn.h"
#include "tableband.h"
#include <boost/any.hpp>
#include <boost/function.hpp>
#include "bytestream.h"
#include <time.h>
#include <sys/time.h>
#include <vector>

#define DEBUG

using namespace std;
using namespace joblist;
using namespace messageqcpp;

// Timer class used by this tdriver to output elapsed times, etc.
class Timer {
	public:
		void logMessage(const string& message) {
			time_t now;
			time(&now);
			string secondsElapsed;
			getTimeElapsed(secondsElapsed);
			if(fLogCount == 0) {
				if(!fSuppressLogMessages) {
					cout << endl;
					cout << "                  Total" << endl;
					cout << "Time            Seconds   Message" << endl;
				}
				fStarted = now;
				fLastLogged = now;
			}
			if(!fSuppressLogMessages) {
				cout << timestr() << " " << secondsElapsed << "   " << message << endl;
			}
			fLogCount++;
		}

		void logNewTest(const string& message) {
			fLogCount = 0;
			
		}

		void startTimer() {
			gettimeofday(&fTvStart, 0);
		}

		double getTimeElapsed(string& seconds) {
			struct timeval tvStop;
			gettimeofday(&tvStop, 0);
			double secondsElapsed = 
				(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
				(fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));
			ostringstream oss;
			oss << secondsElapsed;
			seconds = oss.str();
			return secondsElapsed;
		}

		void setSuppressLogMessages(const bool& suppressLogMessages) {
			fSuppressLogMessages = suppressLogMessages;
		}
	
	private:

		time_t fStarted;
		time_t fLastLogged;
		int    fLogCount;
		struct timeval fTvStart;
		bool fSuppressLogMessages;

		string timestr()
		{
			struct tm tm;
			struct timeval tv;
		
			gettimeofday(&tv, 0);
			localtime_r(&tv.tv_sec, &tm);
		
			ostringstream oss;
			oss << setfill('0')
				<< setw(2) << tm.tm_hour << ':' 
				<< setw(2) << tm.tm_min << ':' 
				<< setw(2) << tm.tm_sec	<< '.'
				<< setw(6) << tv.tv_usec
				;
			return oss.str();
		}	
};
Timer timer;

// Timer class used by this tdriver to output elapsed times, etc.
class MultiTimer {


	public:
		void start(const string& message);
		void stop(const string& message);
		void finish() {
			
			// Calculate the total seconds elapsed.
			struct timeval tvStop;
			gettimeofday(&tvStop, 0);			
			double totalSeconds = 
				(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
				(fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));			

			cout << endl;
			cout << "Seconds  Percentage  Calls      Description" << endl;

			// Add a last entry into the vector for total.
			ProcessStats total;
			total.fTotalSeconds = totalSeconds;
			total.fProcess = "Total";
			total.fStartCount = 1;
			fProcessStats.push_back(total);

			for(uint i = 0; i < fProcessStats.size(); i++) {

				if(i == (fProcessStats.size() - 1)) {
					cout << endl;
				}

				// Seconds.
				string seconds;
				ostringstream oss;
				oss << fProcessStats[i].fTotalSeconds;
				seconds = oss.str();
				seconds.resize(7, '0');
				cout << seconds << "  ";

				// Percentage.
				string percentage;
				ostringstream oss2;
				oss2 << (fProcessStats[i].fTotalSeconds / totalSeconds) * 100.0;
				percentage = oss2.str();
				percentage.resize(5, ' ');
				cout << percentage << "%      ";

				// Times Initiated.
				ostringstream oss3;
				oss3 << fProcessStats[i].fStartCount;
				string timesInitiated = oss3.str();
				timesInitiated.resize(10, ' ');
				cout << timesInitiated << " ";
				
				// Description.
				cout << fProcessStats[i].fProcess << endl;
			}

			
			
		}
		MultiTimer() : fStarted(false) {};

	private:
		class ProcessStats 
		{
			public:
			
			string fProcess;
			struct timeval fTvProcessStarted;
			double fTotalSeconds;
			long fStartCount;
			long fStopCount;

			ProcessStats() : fProcess(""), fTotalSeconds(0.0), fStartCount(0), fStopCount(0) {};

			void processStart() 
			{
				gettimeofday(&fTvProcessStarted, 0);
				fStartCount++;
			}

			void processStop()
			{
				struct timeval tvStop;
				gettimeofday(&tvStop, 0);
				fStopCount++;
				fTotalSeconds += 
					(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
					(fTvProcessStarted.tv_sec + (fTvProcessStarted.tv_usec / 1000000.0));

			}
		};

 		struct timeval fTvStart;
		vector <ProcessStats> fProcessStats;
		bool fStarted;
};

void MultiTimer::stop(const string& message) {
	bool found = false;
	uint idx = 0;
	for(uint i = 0; i < fProcessStats.size(); i++) {
		if(fProcessStats[i].fProcess == message) {
			idx = i;
			found = true;
			break;
		}
	}
	if(!found) {
		throw std::runtime_error("MultiTimer::stop " + message + " called without calling start first.");
	}
	fProcessStats[idx].processStop();
}

void MultiTimer::start(const string& message) {
	bool found = false;
	uint idx = 0;
	ProcessStats processStats;
	if(!fStarted) {
		fStarted = true;
		gettimeofday(&fTvStart, 0);
	}
	for(uint i = 0; i < fProcessStats.size(); i++) {
		if(fProcessStats[i].fProcess == message) {
			idx = i;
			found = true;
			break;
		}
	}
	if(!found) {
		fProcessStats.push_back(processStats);
		idx = fProcessStats.size() - 1;
	}
	fProcessStats[idx].fProcess = message;
	fProcessStats[idx].processStart();
}

// Creates a FIFO datalist containing a uint64_t column and adds it to the passed TableBand object.
// Args:
//	colOID - The column OID.
//	size   - The number of rows.
//	band   - The TableBand to which the column will be added.
void addInt64FifoToBand(CalpontSystemCatalog::OID colOID, const int& size, TableBand& band) {

	timer.logMessage("Creating and populating FIFO<ElementType>.");
	FIFO<ElementType> fifo(1, size);
	int i;
	ElementType element;
	for(i = 0; i < size; i++) {
		element.first = i;
		element.second = i;
		fifo.insert(element);
	}
	fifo.endOfInput();

	timer.logMessage("Adding column to band.");
	band.addColumn(colOID, &fifo);
}

// Creates a FIFO datalist containing a string column and adds it to the passed TableBand object.
// Args:
//	colOID - The column OID.
//	size   - The number of rows.
//	band   - The TableBand to which the column will be added.


// Compares two instances of TableColumn to verify that they containt the same column and values.
template <typename kind>
void validateColumnsMatch(TableColumn<kind>& col1, TableColumn<kind>& col2) {
	CPPUNIT_ASSERT(col1.getColumnOID() == col2.getColumnOID());
	for(uint i = 0; i < col1.getValues().size(); i++) {
		CPPUNIT_ASSERT(col1.getValues()[i] == col2.getValues()[i]);
	// cout << "Row " << i << " has a value of " << col1.getValues()[i] << endl;
	}
}

// Function bandTest
// 
// Purpose:
//	Performs a bandTest by doing the following:
// 	1) Creates a band of four columns (40 bytes per row) with the number of rows passed in the size argument.
// 	2) Serializes the band to a byte stream
// 	3) Creates another instance of the band from the bytesteam
// 	4) Verifies that the two bands are the same.
// 
// Arguments:
//	size - the number of rows to place in the band.
void bandTest(const int& size) {
	// cout << endl;
	// cout << "-------------------------------------------------------------" << endl;
	TableBand band;
	// int size = 30000; // (30000 rows * 40 bytes / row = about 1200000 bytes in a band)

	// 8 + 12 + 12 + 8 = 40 bytes per row.
	addInt64FifoToBand(1, size, band);	
//	addStringFifoToBand(2, size, band);
//	addStringFifoToBand(3, size, band);
	addInt64FifoToBand(2, size, band);

	timer.logMessage("Serializing band.");
	messageqcpp::ByteStream b;
	band.serialize(b);

	TableBand band2;
	timer.logMessage("Unserializing band.");
	band2.unserialize(b);

	timer.logMessage("Validating results.");
	CPPUNIT_ASSERT(band.tableOID() == band2.tableOID());
	CPPUNIT_ASSERT(band.getColumnCount() == band2.getColumnCount());
	CPPUNIT_ASSERT(band.getRowCount() == band2.getRowCount());

	for(uint i = 0; i < band.getColumns().size(); i++) {
		if(typeid(TableColumn<uint64_t>) == band.getColumns()[i].type()) {
			TableColumn<uint64_t> col1 = 
				boost::any_cast<TableColumn<uint64_t> > (band.getColumns()[i]);
			TableColumn<uint64_t> col2 = 
				boost::any_cast<TableColumn<uint64_t> > (band2.getColumns()[i]);
			validateColumnsMatch(col1, col2);
		}
		else if(typeid(TableColumn<string>) == band.getColumns()[i].type()) {
			TableColumn<string> col1 = 
				boost::any_cast<TableColumn<string> > (band.getColumns()[i]);
			TableColumn<string> col2 = 
				boost::any_cast<TableColumn<string> > (band2.getColumns()[i]);
			validateColumnsMatch(col1, col2);
		}
	}
	timer.logMessage("All done.");
}

class DataListDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(DataListDriver);

 CPPUNIT_TEST(BAND_TEST2);
// CPPUNIT_TEST(MULTI_BAND_TEST);
// CPPUNIT_TEST(BAD_BAND_TEST);
// CPPUNIT_TEST(CONVERT_TO_SYSDATA_TEST);

CPPUNIT_TEST_SUITE_END();

private:
public:

	// Tests a band by and outputs elapsed times for each step of the test.
	void BAND_TEST()
	{
		
		int rowsPerBand = 27000;	// 40 * 27000 = 1,080,000 bytes (a little over 1MB)
		cout << endl;
		cout << "Testing Load of " << rowsPerBand << " rows." << endl;
		cout << "-------------------------------------------------------------" << endl;

		timer.startTimer();
		bandTest(rowsPerBand);
	}

	void BAND_TEST2()
	{
		int rowcount = 2500000;
		int bandSize = 8192;
		int columnCount = 9;
		int rowsRemaining = rowcount;
		MultiTimer timer;

		while(rowsRemaining > 0)
		{

			// Create banded Ds
			string message = "Creating BandedDLs.";
			timer.start(message);
			
			BandedDataList* bandedDLs[columnCount];
			
			for(int i = 0; i < columnCount; i++)
			{
				bandedDLs[i] = new BandedDataList(1);
				bandedDLs[i]->OID(i);
			}
			ElementType element;
			int bandSize2 = bandSize;
			if(bandSize2 > rowsRemaining) 
				bandSize2 = rowsRemaining;
			for(int i = 0; i < bandSize2; i++)
			{
				element.first = i;
				element.second = i;
				for(int j = 0; j < columnCount; j++)
				{
					bandedDLs[j]->insert(element);
				}
			}
			for(int i = 0; i < columnCount; i++)
			{
				bandedDLs[i]->endOfInput();
			}
			timer.stop(message);

			// Create the TableBand.
			message = "Creating TableBands";
			timer.start(message);
			TableBand band;
			for(int i = 0; i < columnCount; i++)
			{
				band.addColumn(bandedDLs[i]->OID(), bandedDLs[i]);
			}
			timer.stop(message);

			// Delete the DLs.
			message = "Deleting BandedDLs";
			timer.start(message);
			for(int i = 0; i < columnCount; i++)
			{
				delete bandedDLs[i];
			}
			timer.stop(message);

			rowsRemaining -= bandSize;
		}
		

		timer.finish();

// 		int i;
// 		ElementType element;
// 		for(i = 0; i < size; i++) {
// 			element.first = i;
// 			element.second = i;
// 			fifo.insert(element);
// 			fifo2.insert(element);
// 			fifo3.insert(element);
// 			fifo4.insert(element);
// 		}
// 		fifo.endOfInput();
// 		fifo2.endOfInput();
// 		fifo3.endOfInput();
// 		fifo4.endOfInput();
	}

	// Repeats a band test for totalRows using various band sizes.  Outputs total time for each test.
	// Used to verify that we get similar performance with various band sizes.
	void MULTI_BAND_TEST() {
		timer.setSuppressLogMessages(true);
		const int totalRows = 200000;
		const int startBandRows = 2000;
		const int stopBandRows = 40000;
		const int increment = 2000;
		cout << endl;
		cout << "Testing " << totalRows << " total rows" << endl;
		cout << endl;
		cout << "Seconds   # Rows / Band     # Bands    Bytes per Band" << endl;
		cout << "---------------------------------------------" << endl;
		int rowsToGo;
		string timeStr;
		int numBands;
		for(int i = startBandRows; i <= stopBandRows; i = i + increment) {
			timer.startTimer();
			rowsToGo = totalRows;
			numBands = 0;
			while(rowsToGo > 0) {
				numBands++;
				if(rowsToGo >= i) {
					bandTest(i);
				}
				else {
					bandTest(rowsToGo);
				}
				rowsToGo -= i;
			}
			double seconds;
			seconds = timer.getTimeElapsed(timeStr);
			cout << timeStr << " " << i << " " << numBands << " " << (i * 40) << endl;
		}
	}

	// Adds a column to a band.  Attempts to add a second column with a different number of rows which shoul
	// produce an error.
	void BAD_BAND_TEST() {
		TableBand band(3000);
		addInt64FifoToBand(1, 100, band);
		addInt64FifoToBand(1, 101, band); // Should fail.  Trying to add a column with wrong number of rows.
	}

	// Adds two bands to an NJLSysDataList and asserts that they were added correctly.
	void CONVERT_TO_SYSDATA_TEST() {
		TableBand band;
		CalpontSystemCatalog *csc = CalpontSystemCatalog::makeCalpontSystemCatalog();

		CalpontSystemCatalog::NJLSysDataList sysDataList;
		int size = 10;
		addInt64FifoToBand(1, size, band);	
//		addStringFifoToBand(2, size, band);
//		addStringFifoToBand(3, size, band);
		addInt64FifoToBand(2, size, band);
		band.convertToSysDataList(sysDataList, csc);

		TableBand band2;
		addInt64FifoToBand(1, size, band2);	
//		addStringFifoToBand(2, size, band2);
//		addStringFifoToBand(3, size, band2);
		addInt64FifoToBand(2, size, band2);
		band2.convertToSysDataList(sysDataList, csc);

		vector<NJLColumnResult*>::const_iterator it;
		for (it = sysDataList.begin(); it != sysDataList.end(); it++)
		{
			CPPUNIT_ASSERT((*it)->dataCount() == 20);
			cout << "OID=" << (*it)->ColumnOID() << endl;
// 			if((*it)->ColumnOID() == 2)
// 				for(int i = 0; i < (*it)->dataCount(); i++) {
// 					cout << "col[" << i << "]=" << (*it)->GetStringData(i) << endl;
// 				}
			for(int i = 0; i < (*it)->dataCount(); i++) {
				cout << "col[" << i << "]=" << (*it)->GetData(i) << endl;
				cout << "rid[" << i << "]=" << (*it)->GetRid(i) << endl;
			}
		}
	}
 }; 

CPPUNIT_TEST_SUITE_REGISTRATION(DataListDriver);


int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


