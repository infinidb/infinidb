/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include <unistd.h>
#include <iostream>
#include <string>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
using namespace boost;
namespace fs=boost::filesystem;

#include "includedir.h"

namespace
{

bool isImportJob(const int argc, const char *const * argv)
{
	return true;
}

void twiddleArgsForImport(const int argc, char * const* argv, char**& newArgv)
{
	newArgv = new char*[argc + 1];
	int i = 0;
	newArgv[i++] = 0;
	for (; i < argc; i++)
		newArgv[i] = argv[i];
	newArgv[i++] = 0;
}

void twiddleArgsForSplitting(const int argc, char * const* argv, char**& newArgv)
{
	newArgv = new char*[argc + 1];
	int i = 0;
	newArgv[i++] = 0;
	for (; i < argc; i++)
		newArgv[i] = argv[i];
	newArgv[i++] = 0;
}

fs::path getBestPath(const string& myName, const string& bn)
{
	fs::path lastResort(startup::StartUp::installDir() + "/bin");
	//If we were invoked with an absolute or relative path, try that path first, then try /usr/local/Calpont/bin
	fs::path myNameP(myName);
	fs::path parent = myNameP.parent_path();
	fs::path testName(parent);
	testName /= bn;
	if (fs::exists(testName))
		return testName.parent_path();
	testName = lastResort;
	testName /= bn;
	if (fs::exists(testName))
		return testName.parent_path();
	//If we were invoked with a bare filename, search the PATH, then try /usr/local/Calpont/bin
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
#ifndef _MSC_VER
	boost::char_separator<char> sep(":");
#else
	boost::char_separator<char> sep(";");
#endif
	string pathStr(getenv("PATH"));
	tokenizer tokens(pathStr, sep);
	tokenizer::iterator tok_iter = tokens.begin();
	tokenizer::iterator tok_end = tokens.end();
	while (tok_iter != tok_end)
	{
		testName = *tok_iter;
		testName /= bn;
		if (fs::exists(testName))
			return testName.parent_path();
		++tok_iter;
	}
	return lastResort;
}

}

int main(int argc, char** argv)
{
	bool execImport = true;

	string binaryName;
	fs::path fullPath;
	fs::path binaryPath;
	char** newArgv = 0;

	execImport = isImportJob(argc, argv);

	if (execImport)
	{
		binaryName = "import.bin";
		twiddleArgsForImport(argc, argv, newArgv);
	}
	else
	{
		binaryName = "splitter";
		twiddleArgsForSplitting(argc, argv, newArgv);
	}

	binaryPath = getBestPath(argv[0], binaryName);

	fullPath = binaryPath;
	fullPath /= binaryName;

	char* argv0 = new char[fullPath.string().size() + 1];
	strcpy(argv0, fullPath.string().c_str());
	newArgv[0] = argv[0];
	execv(argv0, newArgv);

	cerr << "Failure executing " << fullPath << " (check permissions?)" << endl;
	return 1;
}

