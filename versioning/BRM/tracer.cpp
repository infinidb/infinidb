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

/*****************************************************************************
 * $Id: tracer.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <iostream>
#include <fstream>
#include <algorithm>

#ifdef _MSC_VER
#include <process.h>
#include <ctime>
#endif

#define TRACER_DLLEXPORT
#include "tracer.h"
#undef TRACER_DLLEXPORT

using namespace std;

namespace BRM {
#ifdef BRM_INFO
std::ofstream brmlog("/var/log/Calpont/brm.log", std::ios::app);

Tracer::Tracer(const std::string& file, int line, const std::string& msg, bool debug, bool writeNow): fFileName(file), fLine(line), fMsg(msg), fDebug(debug), fpid(getpid())
{
	if (writeNow)
		writeBegin();
}

Tracer::~Tracer()
{
	writeEnd();
}


void printIntVec(const pair<string,const int*>& p)
{
	brmlog << p.first << ": (" << *p.second << ") ";
}

  void printBoolVec(const pair<string,const bool*>&  p)
{
	brmlog << p.first << ": (" << *p.second << ") ";
}

void printStrVec(const pair<string,const string*>& p)
{
	brmlog << p.first << ": (" << *p.second << ") ";
}

void printShortVec(const pair<string,const short*>& p)
{
	brmlog << p.first << ": (" << *p.second << ") ";
}

void printInt64Vec(const pair<string,const int64_t*>& p)
{
	brmlog << p.first << ": (" << *p.second << ") ";
}


void Tracer::writeBegin() 
{
	brmlog << timeStamp() << fpid << ":" << fFileName << "@" << fLine << " " << fMsg << " begin - ";	
	for_each(fInputs.begin(), fInputs.end(), printIntVec);
	for_each(fBoolInputs.begin(), fBoolInputs.end(), printBoolVec);
	for_each(fStrInputs.begin(), fStrInputs.end(), printStrVec);
	for_each(fShortInputs.begin(), fShortInputs.end(), printShortVec);
	for_each(fInt64Inputs.begin(), fInt64Inputs.end(), printInt64Vec);
	brmlog << endl << flush;
	
}

void Tracer::writeEnd() 
{
	brmlog << timeStamp() << fpid << ":" << fFileName << " " << fMsg << " end ";
	if (! fOutputs.empty() || ! fBoolOutputs.empty())
		brmlog << "- ";
	for_each(fOutputs.begin(), fOutputs.end(), printIntVec);
	for_each(fBoolOutputs.begin(), fBoolOutputs.end(), printBoolVec);
	for_each(fShortOutputs.begin(), fShortOutputs.end(), printShortVec);
	for_each(fInt64Outputs.begin(), fInt64Outputs.end(), printInt64Vec);
	brmlog << endl << flush;
	
}

void Tracer::writeDirect(const std::string& msg)
{
	brmlog << msg << endl;
}

string Tracer::timeStamp() 
{
	time_t outputTime;
	time(&outputTime);
	string datestr(ctime(&outputTime));
	try 
	{	//replace newline
		return datestr.replace(datestr.length() - 1, 1, string(":"));
	}
	catch(exception& ex)
	{
		size_t tries = 3;
		while (!datestr.length() && tries--)
			datestr = ctime(&outputTime);
		if (datestr.length())
			return datestr.replace(datestr.length() - 1, 1, string(":"));
		else
		{
			std::cerr << __FILE__ << "@" << __LINE__ << " " << ex.what() << " ";
			cerr << datestr << " length " << datestr.length() << " Source: "; 
			cerr << fFileName << "@" << fLine << " " << fMsg << endl;
			return string("timestamp error:");
		}
	}
}


void Tracer::addInput(const string& name, const int* value) 
{
	fInputs.push_back(make_pair(name, value));	
}

void Tracer::addInput(const string& name, const string* value) 
{
	fStrInputs.push_back(make_pair(name, value));	
}

void Tracer::addInput(const string& name,const bool* value) 
{
	fBoolInputs.push_back(make_pair(name, value));	
}

void Tracer::addInput(const string& name, const short* value) 
{
	fShortInputs.push_back(make_pair(name, value));	
}

void Tracer::addInput(const string& name, const int64_t* value) 
{
	fInt64Inputs.push_back(make_pair(name, value));	
}


void Tracer::addOutput(const string& name, const int* value) 
{
	fOutputs.push_back(make_pair(name, value));	
}

void Tracer::addOutput(const string& name, const bool* value) 
{
	fBoolOutputs.push_back(make_pair(name, value));	
}

void Tracer::addOutput(const string& name, const short* value) 
{
	fShortOutputs.push_back(make_pair(name, value));	
}

void Tracer::addOutput(const string& name, const int64_t* value) 
{
	fInt64Outputs.push_back(make_pair(name, value));	
}

#endif

} //namespace
