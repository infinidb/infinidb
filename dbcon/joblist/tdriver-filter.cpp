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

// $Id: tdriver-filter.cpp 9210 2013-01-21 14:10:42Z rdempsey $

#include <list>
#include <sstream>
#include <pthread.h>
#include <iomanip>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "fifo.h"
#include "constantdatalist.h"

#include "calpontsystemcatalog.h"
using namespace joblist;

#include "filteroperation.h"


#include <boost/any.hpp>
#include <boost/function.hpp>
#include "bytestream.h"
#include <time.h>
#include <sys/time.h>

#define DEBUG

using namespace std;
using namespace joblist;
using namespace messageqcpp;

// Timer class used by this tdriver to output elapsed times, etc.
class Timer {
	public:
		void start(const string& message) {
			if(!fHeaderDisplayed) {
				header();
				fHeaderDisplayed = true;
			}
			gettimeofday(&fTvStart, 0);
			cout << timestr() << "          Start " << message << endl;
		}

		void stop(const string& message) {
			time_t now;
			time(&now);
			string secondsElapsed;
			getTimeElapsed(secondsElapsed);
			cout << timestr() << " " << secondsElapsed << " Stop  " << message << endl;
		}

		Timer() : fHeaderDisplayed(false) {}

	private:

 		struct timeval fTvStart;
		bool fHeaderDisplayed;

		double getTimeElapsed(string& seconds) {
			struct timeval tvStop;
			gettimeofday(&tvStop, 0);
			double secondsElapsed = 
				(tvStop.tv_sec + (tvStop.tv_usec / 1000000.0)) -
				(fTvStart.tv_sec + (fTvStart.tv_usec / 1000000.0));
			ostringstream oss;
			oss << secondsElapsed;
			seconds = oss.str();
			seconds.resize(8, '0');
			return secondsElapsed;
		}

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

		void header() {
			cout << endl;
			cout << "Time            Seconds  Activity" << endl;
		}	
};

class FilterDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(FilterDriver);

CPPUNIT_TEST(DOUBLE_TIME_TEST);

CPPUNIT_TEST_SUITE_END();

private:
	// Creates two fifos with numRows and performs the passed comparison against their values.  
	// The first fifo will contain values 0..numRows-1.
	// The second fifo will contain values numrows-1..0.
	// Both bands will containt the same rids.
	// Outputs timing results for loading each fifo, doing the comparison, and iterating over the results.
	// Outputs the number of qualifying rows.
	// Asserts that the number of qualifying rows is correct.
	void filterTest(const FilterOperation::FilterOperator& fo, const int& numRows)
	{
		Timer timer;
		cout << endl;
		cout << "------------------------------------------------------------" << endl;
		int i;
		ElementType element;

		stringstream ss;
		ss << "loading first fifo with " << numRows << " rows.";
		string message = ss.str();
		timer.start(message);
		FIFO<ElementType> fifo1(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = i;
			fifo1.insert(element);
		}
		fifo1.endOfInput();
		timer.stop(message);

		stringstream ss2;
		ss2.flush();
		ss2 << "loading second fifo with " << numRows << " rows.";
		message = ss2.str();
		timer.start(message);

		FIFO<ElementType> fifo2(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = numRows - (i+1);
			fifo2.insert(element);
		}
		fifo2.endOfInput();
		timer.stop(message);

		FIFO<ElementType> fifo3(1, numRows);

		DataList<ElementType> *dl1 =  (DataList<ElementType>*) &fifo1;
		DataList<ElementType> *dl2 =  (DataList<ElementType>*) &fifo2;
		DataList<ElementType> *dl3 =  (DataList<ElementType>*) &fifo3;
		int assertCount;

		switch(fo) {
			case FilterOperation::GT:
				message = "GT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::LT:
				message = "LT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::GTE:
				message = "GTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::LTE:
				message = "LTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::EQ:
				message = "EQ Test";
				assertCount = numRows%2;
				break;
			case FilterOperation::NE:
				message = "NE Test";
				assertCount = numRows - numRows%2;
				break;
			default:
				break;
		}
		timer.start(message);
		FilterOperation filterOperation;
		filterOperation.filter(fo, *dl1, *dl2, *dl3);
		timer.stop(message);
		fifo3.endOfInput();

		timer.start("iterating over result datalist");
		bool more;
		int it;
		it = fifo3.getIterator();
		int count = 0;
		do {
			more = fifo3.next(it, &element);
			if(more) {
				count++;
				// cout << element.fRid << " " << element.fValue << endl;
			}
		} while(more);
		timer.stop("iterating over result datalist");

		cout << count << " rows qualified." << endl;
		idbassert(count == assertCount);
	}

	// Creates two string fifos with numRows and performs the passed comparison against their values.  
	// The first fifo will contain values 0..numRows-1.
	// The second fifo will contain values numrows-1..0.
	// Both bands will containt the same rids.
	// Outputs timing results for loading each fifo, doing the comparison, and iterating over the results.
	// Outputs the number of qualifying rows.
	// Asserts that the number of qualifying rows is correct.
	void stringFilterTest(const FilterOperation::FilterOperator& fo, const int& numRows)
	{
		Timer timer;
		cout << endl;
		cout << "------------------------------------------------------------" << endl;
		int i;
		StringElementType element;

		stringstream ss;
		ss << "loading first fifo with " << numRows << " rows with StringElementType.";
		string message = ss.str();
		timer.start(message);
		FIFO<StringElementType> fifo1(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = i;
			fifo1.insert(element);
		}
		fifo1.endOfInput();
		timer.stop(message);

		stringstream ss2;
		ss2.flush();
		ss2 << "loading second fifo with " << numRows << " rows with StringElementType.";
		message = ss2.str();
		timer.start(message);

		FIFO<StringElementType> fifo2(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = numRows - (i+1);
			fifo2.insert(element);
		}
		fifo2.endOfInput();
		timer.stop(message);

		FIFO<StringElementType> fifo3(1, numRows);

		DataList<StringElementType> *dl1 =  (DataList<StringElementType>*) &fifo1;
		DataList<StringElementType> *dl2 =  (DataList<StringElementType>*) &fifo2;
		DataList<StringElementType> *dl3 =  (DataList<StringElementType>*) &fifo3;
		int assertCount;

		switch(fo) {
			case FilterOperation::GT:
				message = "GT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::LT:
				message = "LT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::GTE:
				message = "GTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::LTE:
				message = "LTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::EQ:
				message = "EQ Test";
				assertCount = numRows%2;
				break;
			case FilterOperation::NE:
				message = "NE Test";
				assertCount = numRows - numRows%2;
				break;
			default:
				break;
		}
		timer.start(message);
		FilterOperation filterOperation;
		filterOperation.filter(fo, *dl1, *dl2, *dl3);
		timer.stop(message);
		fifo3.endOfInput();

		timer.start("iterating over result datalist");
		bool more;
		int it;
		it = fifo3.getIterator();
		int count = 0;
		do {
			more = fifo3.next(it, &element);
			if(more) {
				count++;
				// cout << element.fRid << " " << element.fValue << endl;
			}
		} while(more);
		timer.stop("iterating over result datalist");

		cout << count << " rows qualified." << endl;
		idbassert(count == assertCount);
	}

	// Creates two double fifos with numRows and performs the passed comparison against their values.  
	// The first fifo will contain values 0..numRows-1.
	// The second fifo will contain values numrows-1..0.
	// Both bands will containt the same rids.
	// Outputs timing results for loading each fifo, doing the comparison, and iterating over the results.
	// Outputs the number of qualifying rows.
	// Asserts that the number of qualifying rows is correct.
	void doubleFilterTest(const FilterOperation::FilterOperator& fo, const int& numRows)
	{
		Timer timer;
		cout << endl;
		cout << "------------------------------------------------------------" << endl;
		int i;
		DoubleElementType element;

		stringstream ss;
		ss << "loading first fifo with " << numRows << " rows of DoubleElementType.";
		string message = ss.str();
		timer.start(message);
		FIFO<DoubleElementType> fifo1(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = i + 0.1;
			fifo1.insert(element);
		}
		fifo1.endOfInput();
		timer.stop(message);

		stringstream ss2;
		ss2.flush();
		ss2 << "loading second fifo with " << numRows << " rows.";
		message = ss2.str();
		timer.start(message);

		FIFO<DoubleElementType> fifo2(1, numRows);
		for(i = 0; i < numRows; i++) {
			element.first = i;
			element.second = numRows - (i+1) + 0.1;
			fifo2.insert(element);
		}
		fifo2.endOfInput();
		timer.stop(message);

		FIFO<DoubleElementType> fifo3(1, numRows);

		DataList<DoubleElementType> *dl1 =  (DataList<DoubleElementType>*) &fifo1;
		DataList<DoubleElementType> *dl2 =  (DataList<DoubleElementType>*) &fifo2;
		DataList<DoubleElementType> *dl3 =  (DataList<DoubleElementType>*) &fifo3;
		int assertCount;

		switch(fo) {
			case FilterOperation::GT:
				message = "GT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::LT:
				message = "LT Test";
				assertCount = numRows / 2;
				break;
			case FilterOperation::GTE:
				message = "GTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::LTE:
				message = "LTE Test";
				assertCount = (numRows / 2);
				if(numRows%2 > 0)
					assertCount++;
				break;
			case FilterOperation::EQ:
				message = "EQ Test";
				assertCount = numRows%2;
				break;
			case FilterOperation::NE:
				message = "NE Test";
				assertCount = numRows - numRows%2;
				break;
			default:
				break;
		}
		timer.start(message);
		FilterOperation filterOperation;
		filterOperation.filter(fo, *dl1, *dl2, *dl3);
		timer.stop(message);
		fifo3.endOfInput();

		timer.start("iterating over result datalist");
		bool more;
		int it;
		it = fifo3.getIterator();
		int count = 0;
		do {
			more = fifo3.next(it, &element);
			if(more) {
				count++;
				// cout << element.fRid << " " << element.fValue << endl;
			}
		} while(more);
		timer.stop("iterating over result datalist");

		cout << count << " rows qualified." << endl;
		idbassert(count == assertCount);
	}

public:

	void TIME_TEST()
	{
		int numRows = 1000 * 1000 * 2;
		filterTest(FilterOperation::GT, numRows);
		filterTest(FilterOperation::LT, numRows);
		filterTest(FilterOperation::LTE, numRows);
		filterTest(FilterOperation::GTE, numRows);
		filterTest(FilterOperation::EQ, numRows);
		filterTest(FilterOperation::NE, numRows);
	}

	void STRING_TIME_TEST()
	{
		int numRows = 1000 * 1000 * 2;
		stringFilterTest(FilterOperation::GT, numRows);
		stringFilterTest(FilterOperation::LT, numRows);
		stringFilterTest(FilterOperation::LTE, numRows);
		stringFilterTest(FilterOperation::GTE, numRows);
		stringFilterTest(FilterOperation::EQ, numRows);
		stringFilterTest(FilterOperation::NE, numRows);
	}

	void DOUBLE_TIME_TEST()
	{
		int numRows = 1000 * 1000 * 2;
		doubleFilterTest(FilterOperation::GT, numRows);
		doubleFilterTest(FilterOperation::LT, numRows);
		doubleFilterTest(FilterOperation::LTE, numRows);
		doubleFilterTest(FilterOperation::GTE, numRows);
		doubleFilterTest(FilterOperation::EQ, numRows);
		doubleFilterTest(FilterOperation::NE, numRows);
	}

	void QUICK_TEST()
	{
		float f = 1.1;
		double d = 1.2;
		uint64_t i = 1;
		uint64_t *i_ptr = &i;
		double *d_ptr = &d;
		uint64_t *i2_ptr = (uint64_t*) d_ptr;
		float *f_ptr = &f;
		i_ptr = (uint64_t*) f_ptr;

		cout << "*i_ptr=" << *i_ptr << endl;
		cout << "*i2_ptr=" << *i2_ptr << endl;
		f_ptr = (float*) i_ptr;

		cout << "*f_ptr=" << *f_ptr << endl;
		
		cout << endl;
		if(d > i) 
			cout << "1.2 is greater than 1." << endl;
		if(f > i)
			cout << "1.1 is greater than 1." << endl;
		if(d > f)
			cout << "1.2 is greater than 1.1" << endl;

		if(*i_ptr < *i2_ptr)
			cout << "1.1 < 1.2 when represented as uint64_t." << endl;

		cout << "sizeof(f) = " << sizeof(f) << endl;
		cout << "sizeof(i) = " << sizeof(i) << endl;
		cout << "sizeof(d) = " << sizeof(d) << endl;

		double dbl = 9.7;
		double dbl2 = 1.3;
		i_ptr = (uint64_t*) &dbl;
		i2_ptr = (uint64_t*) &dbl2;
		cout << endl;
		cout << "9.7 as int is " << *i_ptr << endl;
		cout << "9.7 as int is " << *i2_ptr << endl;
		cout << "1.2 < 9.7 is " << (*i_ptr < *i2_ptr) << endl;
	}
 }; 

CPPUNIT_TEST_SUITE_REGISTRATION(FilterDriver);


int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


