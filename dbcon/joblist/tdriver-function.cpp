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

// $Id: tdriver-function.cpp 9210 2013-01-21 14:10:42Z rdempsey $
#include <iostream>
#include <list>
#include <sstream>
#include <pthread.h>
#include <iomanip>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "wsdl.h"
#include "constantdatalist.h"

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "functionoperation.h"
#include <boost/any.hpp>
#include <boost/function.hpp>
#include "bytestream.h"
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <boost/any.hpp>
#include <jobstep.h>

#define DEBUG

using namespace std;
using namespace joblist;
using namespace messageqcpp;

 
// void (*fp) (double, double);

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

			for(uint32_t i = 0; i < fProcessStats.size(); i++) {

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
	uint32_t idx = 0;
	for(uint32_t i = 0; i < fProcessStats.size(); i++) {
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
	uint32_t idx = 0;
	ProcessStats processStats;
	if(!fStarted) {
		fStarted = true;
		gettimeofday(&fTvStart, 0);
	}
	for(uint32_t i = 0; i < fProcessStats.size(); i++) {
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

class functionDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(functionDriver);
CPPUNIT_TEST(FUNCTION_TEST);
CPPUNIT_TEST_SUITE_END();

private:
  ResourceManager fRm;

	void testDrdFunctions()
	{
		cout << endl;
		cout << "double f(double) functions" << endl;
		cout << "---------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		// functions to test
		list<string> functionsToTest;
		functionsToTest.push_back("abs");
		functionsToTest.push_back("acos");
		functionsToTest.push_back("asin");
		functionsToTest.push_back("atan");
		functionsToTest.push_back("ceil");
		functionsToTest.push_back("cos");
		functionsToTest.push_back("cosh");
		functionsToTest.push_back("exp");
		functionsToTest.push_back("floor");
		functionsToTest.push_back("ln");
		functionsToTest.push_back("log2");
		functionsToTest.push_back("log10");
		functionsToTest.push_back("sin");
		functionsToTest.push_back("sinh");
		functionsToTest.push_back("sqrt");
		functionsToTest.push_back("tan");
		functionsToTest.push_back("tanh");

		// type vector is the same for drd functions
		vector<FunctionDataList::FuncDataListType> types;
		types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
		types.push_back(FunctionDataList::DOUBLE_LISTTYPE);

		int numRows = 1;
		DoubleElementType el0, el1;
		list<string>::iterator it;
		for(it = functionsToTest.begin(); it!= functionsToTest.end(); it++)
		{
			FunctionOperation::Function_t* fp = fop->getFunctionObjPtr(*it, types);
			CPPUNIT_ASSERT(fp);
			WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);
			WSDL<DoubleElementType>* wsdlIn1 = new WSDL<DoubleElementType>(2, numRows, fRm);

			for(int i = 0; i < numRows; i++) {
				el1.first = i;
				el1.second = (0.5);
				wsdlIn1->insert(el1);
			}
			wsdlIn1->endOfInput();

			FunctionDataListSPtr listIn1(new FunctionDataList());
			FunctionDataListSPtr listOut(new FunctionDataList());
            FDLVec parms;
			listOut->doubleDl(wsdlOut);
			listIn1->doubleDl(wsdlIn1);
			parms.push_back(listOut);
			parms.push_back(listIn1);
			fop->executeFunction(fop, fp, parms);
			wsdlOut->endOfInput();

			int id0 = wsdlOut->getIterator();
			int id1 = wsdlIn1->getIterator();
			for(int i = 0; i < numRows; i++) {
				wsdlIn1->next(id1, &el1);
				wsdlOut->next(id0, &el0);
				cout << *it << "(" << el1.second << ")=" << el0.second << endl;
			}
		}
	}

	void testDrddFunctions()
	{
		cout << endl;
		cout << "double f(double, double) functions" << endl;
		cout << "---------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		list<string> functionsToTest;
		functionsToTest.push_back("atan2");
		functionsToTest.push_back("power");
		functionsToTest.push_back("+");
		functionsToTest.push_back("-");
		functionsToTest.push_back("*");
		functionsToTest.push_back("/");

        // type vector is the same for drdd functions
        vector<FunctionDataList::FuncDataListType> types;
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);

        int numRows = 1;
        DoubleElementType el0, el1, el2;
        list<string>::iterator it;
        for(it = functionsToTest.begin(); it!= functionsToTest.end(); it++)
        {
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr(*it, types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);
            WSDL<DoubleElementType>* wsdlIn1 = new WSDL<DoubleElementType>(2, numRows, fRm);
            WSDL<DoubleElementType>* wsdlIn2 = new WSDL<DoubleElementType>(2, numRows, fRm);

            for(int i = 0; i < numRows; i++) {
                el1.first = i;
                el1.second = (0.5);
                wsdlIn1->insert(el1);
                wsdlIn2->insert(el1);  // the data in two list is the same
            }
            wsdlIn1->endOfInput();
            wsdlIn2->endOfInput();

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->doubleDl(wsdlIn1);
            listIn2->doubleDl(wsdlIn2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlIn1->getIterator();
            int id2 = wsdlIn2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlIn1->next(id1, &el1);
                wsdlIn2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << *it << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }
	}

	void testSrssFunctions()
	{
		cout << endl;
		cout << "string f(string, string) functions" << endl;
		cout << "---------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		list<string> functionsToTest;
		functionsToTest.push_back("||");
		functionsToTest.push_back("concat");

        // type vector is the same for drdd functions
        vector<FunctionDataList::FuncDataListType> types;
        types.push_back(FunctionDataList::STRING_LISTTYPE);
        types.push_back(FunctionDataList::STRING_LISTTYPE);
        types.push_back(FunctionDataList::STRING_LISTTYPE);

        int numRows = 1;
        StringElementType el0, el1, el2;
        list<string>::iterator it;
        for(it = functionsToTest.begin(); it!= functionsToTest.end(); it++)
        {
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr(*it, types);
            CPPUNIT_ASSERT(fp);
            WSDL<StringElementType>* wsdlOut = new WSDL<StringElementType>(1, numRows, fRm);
            WSDL<StringElementType>* wsdlIn1 = new WSDL<StringElementType>(2, numRows, fRm);
            WSDL<StringElementType>* wsdlIn2 = new WSDL<StringElementType>(2, numRows, fRm);

            for(int i = 0; i < numRows; i++) {
                el1.first = i;
				el1.second = "abc";
                wsdlIn1->insert(el1);
                wsdlIn2->insert(el1);  // the data in two list is the same
            }
            wsdlIn1->endOfInput();
            wsdlIn2->endOfInput();

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->stringDl(wsdlOut);
            listIn1->stringDl(wsdlIn1);
            listIn2->stringDl(wsdlIn2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlIn1->getIterator();
            int id2 = wsdlIn2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlIn1->next(id1, &el1);
                wsdlIn2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << *it << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }
	}

   void testSrsFunctions()
    {
        cout << endl;
        cout << "string f(string) functions" << endl;
        cout << "---------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		list<string> functionsToTest;
		functionsToTest.push_back("upper");
		functionsToTest.push_back("asciistr");
		functionsToTest.push_back("trim");
		functionsToTest.push_back("ltrim");
		functionsToTest.push_back("rtrim");

        // type vector is the same for drdd functions
        vector<FunctionDataList::FuncDataListType> types;
        types.push_back(FunctionDataList::STRING_LISTTYPE);
        types.push_back(FunctionDataList::STRING_LISTTYPE);

        int numRows = 2;
        StringElementType el0, el1, el2;
        list<string>::iterator it;
        for(it = functionsToTest.begin(); it!= functionsToTest.end(); it++)
        {
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr(*it, types);
            CPPUNIT_ASSERT(fp);
            WSDL<StringElementType>* wsdlOut = new WSDL<StringElementType>(1, numRows, fRm);
            WSDL<StringElementType>* wsdlIn1 = new WSDL<StringElementType>(2, numRows, fRm);

            char abc[][10] = {"  abc  ", "second"};
            abc[0][3] = 222; // 0xDE

            for(int i = 0; i < numRows; i++) {
                el1.first = i;
                el1.second = abc[i];
                wsdlIn1->insert(el1);
            }
            wsdlIn1->endOfInput();

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FDLVec parms;
            listOut->stringDl(wsdlOut);
            listIn1->stringDl(wsdlIn1);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlIn1->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlIn1->next(id1, &el1);
                wsdlOut->next(id0, &el0);
                cout << *it << "(\'" << el1.second << "\')=" << "\'" << el0.second << "\'" << endl;
            }
        }
    }

	void testAddFunctions()
	{
		cout << endl;
		cout << "add(<S>, <T>) functions" << endl;
		cout << "---------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

        // the map only have one entry for add_ddd, which is mapped to add(any)
        vector<FunctionDataList::FuncDataListType> types;
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);

        int numRows = 1;

        // double list
        WSDL<DoubleElementType>* wsdlInD1 = new WSDL<DoubleElementType>(20, numRows, fRm);
        WSDL<DoubleElementType>* wsdlInD2 = new WSDL<DoubleElementType>(20, numRows, fRm);
        // uint32_t list
        WSDL<ElementType>* wsdlInU1 = new WSDL<ElementType>(20, numRows, fRm);
        WSDL<ElementType>* wsdlInU2 = new WSDL<ElementType>(20, numRows, fRm);
        // string list
        WSDL<StringElementType>* wsdlInS1 = new WSDL<StringElementType>(20, numRows, fRm);
        WSDL<StringElementType>* wsdlInS2 = new WSDL<StringElementType>(20, numRows, fRm);
        DoubleElementType eld;
        ElementType       elu;
        StringElementType els;
        for(int i = 0; i < numRows; i++) {
            eld.first = i;
            eld.second = (0.5);
            wsdlInD1->insert(eld);
            wsdlInD2->insert(eld);
            elu.first = i;
            elu.second = (6);
            wsdlInU1->insert(elu);
            wsdlInU2->insert(elu);
            els.first = i;
            els.second = "8.8";
            wsdlInS1->insert(els);
            wsdlInS2->insert(els);
        }
        wsdlInD1->endOfInput();
        wsdlInD2->endOfInput();
        wsdlInU1->endOfInput();
        wsdlInU2->endOfInput();
        wsdlInS1->endOfInput();
        wsdlInS2->endOfInput();

		// double add(double, double)
        {
            DoubleElementType el0, el1, el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->doubleDl(wsdlInD1);
            listIn2->doubleDl(wsdlInD2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInD1->getIterator();
            int id2 = wsdlInD2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInD1->next(id1, &el1);
                wsdlInD2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(double, uint32_t)
        {
            DoubleElementType el0, el1;
            ElementType       el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->doubleDl(wsdlInD1);
            listIn2->uint64Dl(wsdlInU2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInD1->getIterator();
            int id2 = wsdlInU2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInD1->next(id1, &el1);
                wsdlInU2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(double, string)
        {
            DoubleElementType el0, el1;
            StringElementType el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->doubleDl(wsdlInD1);
            listIn2->stringDl(wsdlInS2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInD1->getIterator();
            int id2 = wsdlInS2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInD1->next(id1, &el1);
                wsdlInS2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(uint32_t, double)
        {
            DoubleElementType el0, el2;
            ElementType       el1;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->uint64Dl(wsdlInU1);
            listIn2->doubleDl(wsdlInD2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInU1->getIterator();
            int id2 = wsdlInD2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInU1->next(id1, &el1);
                wsdlInD2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(uint32_t, uint32_t)
        {
            DoubleElementType el0;
            ElementType       el1, el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->uint64Dl(wsdlInU1);
            listIn2->uint64Dl(wsdlInU2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInU1->getIterator();
            int id2 = wsdlInU2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInU1->next(id1, &el1);
                wsdlInU2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(uint32_t, string)
        {
            DoubleElementType el0;
            ElementType       el1;
            StringElementType el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->uint64Dl(wsdlInU1);
            listIn2->stringDl(wsdlInS2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInU1->getIterator();
            int id2 = wsdlInS2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInU1->next(id1, &el1);
                wsdlInS2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(string, double)
        {
            DoubleElementType el0, el2;
            StringElementType el1;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->stringDl(wsdlInS1);
            listIn2->doubleDl(wsdlInD2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInS1->getIterator();
            int id2 = wsdlInD2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInS1->next(id1, &el1);
                wsdlInD2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(string, uint32_t)
        {
            DoubleElementType el0;
            StringElementType el1;
            ElementType       el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->stringDl(wsdlInS1);
            listIn2->uint64Dl(wsdlInU2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInS1->getIterator();
            int id2 = wsdlInU2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInS1->next(id1, &el1);
                wsdlInU2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }

		// double add(string, string)
        {
            DoubleElementType el0;
            StringElementType el1, el2;
            FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
            CPPUNIT_ASSERT(fp);
            WSDL<DoubleElementType>* wsdlOut = new WSDL<DoubleElementType>(1, numRows, fRm);

            FunctionDataListSPtr listOut(new FunctionDataList());
            FunctionDataListSPtr listIn1(new FunctionDataList());
            FunctionDataListSPtr listIn2(new FunctionDataList());
            FDLVec parms;
            listOut->doubleDl(wsdlOut);
            listIn1->stringDl(wsdlInS1);
            listIn2->stringDl(wsdlInS2);
            parms.push_back(listOut);
            parms.push_back(listIn1);
            parms.push_back(listIn2);
            fop->executeFunction(fop, fp, parms);
            wsdlOut->endOfInput();

            int id0 = wsdlOut->getIterator();
            int id1 = wsdlInS1->getIterator();
            int id2 = wsdlInS2->getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlInS1->next(id1, &el1);
                wsdlInS2->next(id2, &el2);
                wsdlOut->next(id0, &el0);
				cout << "+" << "(" << el1.second << ", " << el2.second << ")=" << el0.second << endl;
            }
        }
	}

	struct dateTime
	{
		unsigned msecond : 20;
		unsigned second  : 6;
		unsigned minute  : 6;
		unsigned hour    : 6;
		unsigned day     : 6;
		unsigned month   : 4;
		unsigned year    : 16;

		dateTime(int y = 0xFFFF, int mn = 0xF, int d = 0x3F, int h = 0x3F, int mi = 0x3F, int s = 0x3F, int ms = 0xFFFFE)
		{ year = y; month = mn; day = d; hour = h; minute = mi; second = s; msecond = ms; }
    };

	void testDateFunctions()
	{
		cout << endl;
		cout << "to_date function with formats" << endl;
		cout << "-----------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		vector<FunctionDataList::FuncDataListType> types;
		types.push_back(FunctionDataList::STRING_LISTTYPE);
		types.push_back(FunctionDataList::UINT64_LISTTYPE);
		types.push_back(FunctionDataList::STRING_CONST_LISTTYPE);

		// data list
		AnyDataListSPtr spdl1(new AnyDataList());
		BandedDataList* dl1 = new BandedDataList(20, fRm);
        spdl1->bandedDL(dl1);

		dateTime dt(2007, 11, 12, 16, 7, 8, 999);
		uint64_t idt = *(reinterpret_cast<uint64_t*>(&dt));
		ElementType elu(0, idt);

		int numRows = 1;
		for(int i = 0; i < numRows; i++) {
			elu.first = i;
			dl1->insert(elu);
		}
		dl1->endOfInput();

		char fmt[6][30] = {	"YYYYMMDDHH", "YYYYMMDDHHMISS", "YYYYMMDDHHMISSFF",
							"YYYY-MM-DD HH:MI:SS", "MON DD, RRRR",
							"MM/DD/YY HH24:MI:SS.FF"};
		for (int i = 0; i < 6; i++)
		{
			AnyDataListSPtr spdl2(new AnyDataList());
			StringElementType els(0, fmt[i]);
			ConstantDataList<StringElementType>* dl2 = new ConstantDataList<StringElementType>(els);
			spdl2->stringConstantDL(dl2);
			dl2->endOfInput();
	
			JobStepAssociation inJs;
			inJs.outAdd(spdl1);
			inJs.outAdd(spdl2);
//			jobstep->outputAssociation(outJs);  // output of a job step

			AnyDataListSPtr spdlOut(new AnyDataList());
			StringDataList* dlOut = new StringDataList(1, fRm);
			spdlOut->strDataList(dlOut);
			JobStepAssociation outJs;
			outJs.outAdd(spdlOut);
//			funJobstep->inputAssociation(inJs);
//			funJobstep->outputAssociation(outJs);

			FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("to_char", types);
			CPPUNIT_ASSERT(fp);

			FunctionDataListSPtr listOut(new FunctionDataList());
			FunctionDataListSPtr listIn1(new FunctionDataList());
			FunctionDataListSPtr listIn2(new FunctionDataList());
			FDLVec parms;
			listOut->stringDl(outJs.outAt(0)->stringDataList());
			listIn1->uint64Dl(inJs.outAt(0)->dataList());
			listIn2->stringDl(inJs.outAt(1)->stringDataList());
			parms.push_back(listOut);
			parms.push_back(listIn1);
			parms.push_back(listIn2);
			fop->executeFunction(fop, fp, parms);
			dlOut->endOfInput();
	
			int id0 = dlOut->getIterator();
			int id1 = dl1->getIterator();
			int id2 = dl2->getIterator();
			StringElementType el0;
			ElementType       el1;
			StringElementType el2;
			for(int i = 0; i < numRows; i++) {
				dl1->next(id1, &el1);
				dl2->next(id2, &el2);
				dlOut->next(id0, &el0);
				cout << "to_char" << "(" << el1.second << ", \'" << el2.second << "\')=" << el0.second << endl;
			}
		}

		// default format
		{
			JobStepAssociation inJs;
			inJs.outAdd(spdl1);
//			jobstep->outputAssociation(outJs);  // output of a job step

			AnyDataListSPtr spdlOut(new AnyDataList());
			StringDataList* dlOut = new StringDataList(1, fRm);
			spdlOut->strDataList(dlOut);
			JobStepAssociation outJs;
			outJs.outAdd(spdlOut);
//			funJobstep->inputAssociation(inJs);
//			funJobstep->outputAssociation(outJs);

			FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("to_char", types);
			CPPUNIT_ASSERT(fp);

			FunctionDataListSPtr listOut(new FunctionDataList());
			FunctionDataListSPtr listIn1(new FunctionDataList());
			FDLVec parms;
			listOut->stringDl(outJs.outAt(0)->stringDataList());
			listIn1->uint64Dl(inJs.outAt(0)->dataList());
			parms.push_back(listOut);
			parms.push_back(listIn1);
			fop->executeFunction(fop, fp, parms);
			dlOut->endOfInput();
	
			int id0 = dlOut->getIterator();
			int id1 = dl1->getIterator();
			StringElementType el0;
			ElementType       el1;
			for(int i = 0; i < numRows; i++) {
				dl1->next(id1, &el1);
				dlOut->next(id0, &el0);
				cout << "to_char" << "(" << el1.second << ")=" << el0.second << endl;
			}
		}

	}

	void testToNumFunctions()
	{
		cout << endl;
		cout << "to_num function, format ignored" << endl;
		cout << "-------------------------------" << endl;

		FunctionOperation* fop = FunctionOperation::instance();

		vector<FunctionDataList::FuncDataListType> types;
		types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
		types.push_back(FunctionDataList::STRING_LISTTYPE);
		types.push_back(FunctionDataList::STRING_CONST_LISTTYPE);

		AnyDataListSPtr spdl1(new AnyDataList());
		StringFifoDataList* dl1 = new StringFifoDataList(2, 100);
		spdl1->stringDL(dl1);

		const int numRows = 6;
		char num[numRows][30] = {	"2007111218", "1234567.8", "-123456.78",
									"2,007,111,218", "1.23E3", "-1.23E-3"};

		StringRowGroup rows;
		for (int i = 0; i < numRows; i++)
		{
			StringElementType els(0, num[i]);
			rows.et[rows.count++] = els;
			//			dl1->insert(els);
		}
		dl1->insert(rows);
		dl1->endOfInput();

		// format model, ignored
		AnyDataListSPtr spdl2(new AnyDataList());
		StringElementType els(0, "dummy");
		ConstantDataList<StringElementType>* dl2 = new ConstantDataList<StringElementType>(els);
		spdl2->stringConstantDL(dl2);
		dl2->endOfInput();

		JobStepAssociation inJs;
		inJs.outAdd(spdl1);
		inJs.outAdd(spdl2);
//			jobstep->outputAssociation(outJs);  // output of a job step

		AnyDataListSPtr spdlOut(new AnyDataList());
		FIFO<DoubleElementType>* dlOut = new FIFO<DoubleElementType>(1, 100);
		spdlOut->doubleDL(dlOut);
		JobStepAssociation outJs;
		outJs.outAdd(spdlOut);
//			funJobstep->inputAssociation(inJs);
//			funJobstep->outputAssociation(outJs);

		FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("to_number", types);
		CPPUNIT_ASSERT(fp);

		FunctionDataListSPtr listOut(new FunctionDataList());
		FunctionDataListSPtr listIn1(new FunctionDataList());
		FunctionDataListSPtr listIn2(new FunctionDataList());
		FDLVec parms;
		listOut->doubleDl(outJs.outAt(0)->doubleDL());
		listIn1->stringDl(inJs.outAt(0)->stringDataList());
		listIn2->stringDl(inJs.outAt(1)->stringDataList());
		parms.push_back(listOut);
		parms.push_back(listIn1);
		parms.push_back(listIn2);
		fop->executeFunction(fop, fp, parms);
		dlOut->endOfInput();

		int id1 = dl1->getIterator();
		int id2 = dl2->getIterator();
		int id0 = dlOut->getIterator();
// 		StringElementType el1;
		StringRowGroup el1;
 		StringElementType el2;
		DoubleElementType el0;
		for(int i = 0; i < numRows; i++) {
			dl1->next(id1, &el1);
			dl2->next(id2, &el2);
			dlOut->next(id0, &el0);
			cout << "to_number" << "(\'" << el1.et[i].second << "\', \'" << el2.second << "\')=" << setw(20) << setprecision(10) << el0.second << endl;
		}

	}

/*
	void testDrdFunctions()
	{
		cout << endl;
		cout << "double f(double) functions" << endl;
		cout << "---------------------------" << endl;
		FunctionOperation fo;
		drdMap_t::iterator it;
		drdMap_t map = fo.getDrdMap();
		int numRows = 1;
		DoubleElementType el, el2;
		for(it = map.begin(); it!= map.end(); it++)
		{
			CPPUNIT_ASSERT(fo.isDrdFunction(it->first));
			WSDL<DoubleElementType> wsdlIn(2, numRows);
			WSDL<DoubleElementType> wsdlOut(1, numRows);
			for(int i = 0; i < numRows; i++) {
				el.first = i;
				el.second = (0.5);
				wsdlIn.insert(el);
			}
			wsdlIn.endOfInput();
			fo.executeDrdFunction(it->first, wsdlIn, wsdlOut);
			wsdlOut.endOfInput();
			int id = wsdlIn.getIterator();
			int id2 = wsdlOut.getIterator();
			for(int i = 0; i < numRows; i++) {
				wsdlIn.next(id, &el);
				wsdlOut.next(id2, &el2);
				cout << it->first << "(" << el.second << ")=" << el2.second << endl;
			}
		}


	}

	void testDrddFunctions()
	{
		cout << endl;
		cout << "double f(double, double) functions" << endl;
		cout << "---------------------------" << endl;
		FunctionOperation fo;
		drddMap_t::iterator it;
		drddMap_t map = fo.getDrddMap();
		int numRows = 1;
		DoubleElementType el, el2, el3;
		for(it = map.begin(); it!= map.end(); it++)
		{
			CPPUNIT_ASSERT(fo.isDrddFunction(it->first));
			WSDL<DoubleElementType> wsdlIn(2, numRows);
			WSDL<DoubleElementType> wsdlIn2(2, numRows);
			WSDL<DoubleElementType> wsdlOut(1, numRows);
			for(int i = 0; i < numRows; i++) {
				el.first = i;
				el.second = (0.5);
				wsdlIn.insert(el);
				wsdlIn2.insert(el);
			}
			wsdlIn.endOfInput();
			wsdlIn2.endOfInput();
			fo.executeDrddFunction(it->first, wsdlIn, wsdlIn2, wsdlOut);
			wsdlOut.endOfInput();
			int id = wsdlIn.getIterator();
			int id2 = wsdlIn2.getIterator();
			int id3 = wsdlOut.getIterator();
			for(int i = 0; i < numRows; i++) {
				wsdlIn.next(id, &el);
				wsdlIn2.next(id2, &el2);
				wsdlOut.next(id3, &el3);
				cout << it->first << "(" << el.second << ", " << el2.second << ")=" << el3.second << endl;
			}
		}

	}

	void testSrssFunctions()
	{
		cout << endl;
		cout << "string f(string, string) functions" << endl;
		cout << "---------------------------" << endl;
		FunctionOperation fo;
		srssMap_t::iterator it;
		srssMap_t map = fo.getSrssMap();
		int numRows = 1;
		StringElementType el, el2, el3;
		for(it = map.begin(); it!= map.end(); it++)
		{
			CPPUNIT_ASSERT(fo.isSrssFunction(it->first));
			WSDL<StringElementType> wsdlIn(2, numRows);
			WSDL<StringElementType> wsdlIn2(2, numRows);
			WSDL<StringElementType> wsdlOut(1, numRows);
			for(int i = 0; i < numRows; i++) {
				el.first = i;
				el.second = "abc";
				wsdlIn.insert(el);
				wsdlIn2.insert(el);
			}
			wsdlIn.endOfInput();
			wsdlIn2.endOfInput();
			fo.executeSrssFunction(it->first, wsdlIn, wsdlIn2, wsdlOut);
			wsdlOut.endOfInput();
			int id = wsdlIn.getIterator();
			int id2 = wsdlIn2.getIterator();
			int id3 = wsdlOut.getIterator();
			for(int i = 0; i < numRows; i++) {
				wsdlIn.next(id, &el);
				wsdlIn2.next(id2, &el2);
				wsdlOut.next(id3, &el3);
				cout << it->first << "(" << el.second << ", " << el2.second << ")=" << el3.second << endl;
			}
		}

	}

   void testSrsFunctions()
    {
        cout << endl;
        cout << "string f(string) functions" << endl;
        cout << "---------------------------" << endl;
        FunctionOperation fo;
        srsMap_t::iterator it;
        srsMap_t map = fo.getSrsMap();
        int numRows = 2;
        StringElementType elIn, elOut;
        for(it = map.begin(); it!= map.end(); it++)
        {
            CPPUNIT_ASSERT(fo.isSrsFunction(it->first));
            WSDL<StringElementType> wsdlIn(2, numRows);
            WSDL<StringElementType> wsdlOut(1, numRows);
			char abc[][10] = {"  abc  ", "second"};
            abc[0][3] = 222; // 0xDE
            for(int i = 0; i < numRows; i++) {
                elIn.first = i;
                elIn.second = abc[i];
                wsdlIn.insert(elIn);
            }
            wsdlIn.endOfInput();
            fo.executeSrsFunction(it->first, wsdlIn, wsdlOut);
            wsdlOut.endOfInput();
            int idi = wsdlIn.getIterator();
            int ido = wsdlOut.getIterator();
            for(int i = 0; i < numRows; i++) {
                wsdlIn.next(idi, &elIn);
                wsdlOut.next(ido, &elOut);
                cout << it->first << "(\"" << elIn.second << "\")=" << "\"" << elOut.second << "\"" << endl;
            }
        }

    }
*/

public:

	void FUNCTION_TEST()
	{
		testDrdFunctions();
		testDrddFunctions();
		testSrssFunctions();
		testSrsFunctions();
		testAddFunctions();
		testDateFunctions();
		testToNumFunctions();
	}

	// Executes an addition (+ function) between two large DataLists and displays timing results.
	// Asserts that the output DataList contains the correct results.
	void PERFORMANCE_TEST()
	{
		cout << endl << endl;
		cout << "Performance Test" << endl;
		cout << "----------------------------------------------------------------------------------------" << endl;
		Timer timer;
		int numRows = 1000 * 1000 * 2;
        typedef WSDL<DoubleElementType> DoubleWSDL;
	DoubleWSDL* wsdlOut = new DoubleWSDL(1, numRows, fRm);
		DoubleWSDL* wsdlIn1 = new DoubleWSDL(2, numRows, fRm);
		DoubleWSDL* wsdlIn2 = new DoubleWSDL(2, numRows, fRm);

		stringstream ss;
		ss << "Loading " << numRows << " DoubleElementTypes into a WSDL.";
		string message = ss.str();
		timer.start(message);
		
		DoubleElementType el;
		for(int i = 0; i < numRows; i++) {
			el.first = i;
			el.second = (i%5);
			wsdlIn1->insert(el);
		}
		wsdlIn1->endOfInput();
		timer.stop(message);

		stringstream ss2;
		ss2 << "Loading " << numRows << " DoubleElementTypes into a second WSDL.";
		message = ss2.str();
		timer.start(message);
		
		for(int i = 0; i < numRows; i++) {
			el.first = i;
			el.second = (i%5);
			wsdlIn2->insert(el);
		}
		wsdlIn2->endOfInput();
		timer.stop(message);

		message = "Building FunctionDataList and vector";
		timer.start(message);
		FunctionDataListSPtr listOut(new FunctionDataList());
		FunctionDataListSPtr listIn1(new FunctionDataList());
		FunctionDataListSPtr listIn2(new FunctionDataList());
		FDLVec parms;
		listOut->doubleDl(wsdlOut);
		listIn1->doubleDl(wsdlIn1);
		listIn2->doubleDl(wsdlIn2);
		parms.push_back(listOut);
		parms.push_back(listIn1);
		parms.push_back(listIn2);

        vector<FunctionDataList::FuncDataListType> types;
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
        types.push_back(FunctionDataList::DOUBLE_LISTTYPE);
		timer.stop(message);

		message = "Executing addition and loading result into a third WSDL.";
		timer.start(message);
		FunctionOperation* fop = FunctionOperation::instance();
		FunctionOperation::Function_t* fp = fop->getFunctionObjPtr("+", types);
		CPPUNIT_ASSERT(fp);
		fop->executeFunction(fop, fp, parms);
		timer.stop(message);
		wsdlOut->endOfInput();

		message = "Iterating over results and doing asserts.";
		timer.start(message);
		DoubleElementType el0, el1, el2;
		int id0 = wsdlOut->getIterator();
		int id1 = wsdlIn1->getIterator();
		int id2 = wsdlIn2->getIterator();
		for(int i = 0; i < numRows; i++) {
			wsdlOut->next(id0, &el0);
			wsdlIn1->next(id1, &el1);
			wsdlIn2->next(id2, &el2);
			
			CPPUNIT_ASSERT(el0.first == el1.first);
			CPPUNIT_ASSERT(el0.second == (el1.second + el2.second));
		}
		timer.stop(message);
	}
/*
	void PERFORMANCE_TEST()
	{
		cout << endl << endl;
		cout << "Performance Test" << endl;
		cout << "----------------------------------------------------------------------------------------" << endl;
		Timer timer;
		int id;
		int numRows = 1000 * 1000 * 2;
		DoubleElementType el, el2, el3;
		WSDL<DoubleElementType> wsdlIn(2, numRows);
		WSDL<DoubleElementType> wsdlIn2(2, numRows);	
		WSDL<DoubleElementType> wsdlOut(1, numRows);	

		stringstream ss;
		ss << "Loading " << numRows << " DoubleElementTypes into a WSDL.";
		string message = ss.str();
		timer.start(message);
		
		for(int i = 0; i < numRows; i++) {
			el.first = i;
			el.second = (i%5);
			wsdlIn.insert(el);
		}
		wsdlIn.endOfInput();
		timer.stop(message);

		stringstream ss2;
		ss2 << "Loading " << numRows << " DoubleElementTypes into a second WSDL.";
		message = ss2.str();
		timer.start(message);
		
		for(int i = 0; i < numRows; i++) {
			el.first = i;
			el.second = (i%5);
			wsdlIn2.insert(el);
		}
		wsdlIn2.endOfInput();
		timer.stop(message);

		message = "Executing addition and loading result into a third WSDL.";
		timer.start(message);
		FunctionOperation fo;
		fo.executeDrddFunction("+", wsdlIn, wsdlIn2, wsdlOut);
		timer.stop(message);
		wsdlOut.endOfInput();

		message = "Iterating over results and doing asserts.";
		timer.start(message);
		id = wsdlIn.getIterator();
		int id2 = wsdlIn2.getIterator();
		int id3 = wsdlOut.getIterator();
		for(int i = 0; i < numRows; i++) {
			wsdlIn.next(id, &el);
			wsdlIn2.next(id2, &el2);
			wsdlOut.next(id3, &el3);
			
			CPPUNIT_ASSERT(el.first == el3.first);
			CPPUNIT_ASSERT((el.second + el2.second) == el3.second);
			
		}
		timer.stop(message);
	}
*/
 }; 

CPPUNIT_TEST_SUITE_REGISTRATION(functionDriver);


int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


