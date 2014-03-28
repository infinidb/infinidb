/* Copyright (C) 2013 Calpont Corp.

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

// $Id: idb_getopt.cpp 3495 2013-01-21 14:09:51Z rdempsey $

#include <set>
#include <cctype>
using namespace std;

#include "idb_getopt.h"

struct GetOptData
{
	bool initialized;
	set<char> opSet;
	set<char> opWithArgSet;
	char* nextchar;
	GetOptData() : initialized(false), nextchar(0) {}
};

/* Same as GNU interface. For communication from `getopt' to	the	caller.
	 When	`getopt' finds an option that takes an argument,
	 the argument value is returned here.
	 Also, when `ordering' is RETURN_IN_ORDER,
	 each	non-option ARGV-element is returned	here.	 */

char *optarg;

/* Same as GNU interface. Index in ARGV of the next element to	be scanned.
	 This	is used for communication to and from the caller
	 and for communication between successive calls to `getopt'.

	 When	`getopt' returns -1, this is the index of the first	of the
	 non-option elements that the caller should itself scan.

	 Otherwise, `optind' communicates from one call to the next
	 how much of ARGV has been scanned so far.*/

/* 1003.2	says this must be 1 before any call.*/
int	optind = 1;

/* Same as GNU interface. Callers store zero here to inhibit the error message
	 for unrecognized options. */

int	opterr = 1;

/* Same as GNU interface. Set to an option character which was unrecognized.
	 This must be initialized on some systems	to avoid linking in the
	 system's own getopt implementation. */

int	optopt = '?';

namespace
{
/* Keep a global copy of all internal members of getopt_data. */
GetOptData getopt_data;

// only looks at alphabet and numeric in optstring. The other characters
// will be ignored
void initialize_opt(const char* optstring)
{
	for (unsigned i = 0; i < strlen(optstring); i++)
	{
		if (isalnum(optstring[i]))
		{
			if ((i < strlen(optstring)-1) && (optstring[i+1] == ':'))
			{
				getopt_data.opWithArgSet.insert(optstring[i]);
				i++;
			}
			else
			{
				getopt_data.opSet.insert(optstring[i]);
			}
		}
	}
	getopt_data.initialized = true;
}

} //namespace

int getopt(int argc, char* const* argv, const char* optstring)
{
	if (argc < 1)
		return -1;
		
	if (!getopt_data.initialized)
		initialize_opt(optstring);
	
	int retchar = 0;
	char* arg = 0;
	bool newArg = false;
	int curind = optind;
	
	if (optind < argc)
	{
		if (getopt_data.nextchar == 0)
		{
			arg = argv[optind];
			newArg = true;
		}
		else
		{
			arg = getopt_data.nextchar;
		}
		
		// all valid op should be started with '-', with or without arg following.
		// stop process when hitting the first non '-' arg that does not belong to a 
		// previous option.
		if (newArg && (strlen(arg) <= 1 || *arg != '-'))
			return -1;

		if (newArg && (strcmp(arg, "--") == 0))
		{
			optind++;
			return -1;
		}

		if (newArg && *arg == '-')
			arg++;
		
		if (arg != 0)
		{
			// option without arg
			if (getopt_data.opSet.find(*arg) != getopt_data.opSet.end())
			{
				retchar = *arg;
				if (*(arg+1) == 0)  // "-a"
				{
					optind++;
					getopt_data.nextchar = 0;
				}
				else  // "-abc"
				{
					getopt_data.nextchar = arg+1;
				}
			}
			// option with arg
			else if (getopt_data.opWithArgSet.find(*arg) != getopt_data.opWithArgSet.end())
			{
				getopt_data.nextchar = 0;
				if (*(arg+1) == 0)  // "-c foo" next arg is arg of c
				{
					if (optind < argc-1)
					{
						retchar = *arg;
						optind++;
						optarg = argv[optind++];
					}
					else  // error. no arg provided for argop. this option is invalid
					{
						optopt = *arg;
						optind++;
						retchar = '?';
					}
				}
				else  // "-cfoo" arg is in the string right after option letter
				{
					retchar = *arg;
					optarg = arg+1;
					optind++;
				}
			}
			else  // invalid option
			{
				optopt = *arg;
				retchar = '?';
				if (*(arg+1) == 0)  // "-x"
				{
					optind++;
					getopt_data.nextchar = 0;
				}
				else  //  "-xbc"
				{
					getopt_data.nextchar = arg+1;
				}
			}
		}
	}
	
	if (optind == argc && retchar == 0)
	{
		optind = curind;
		return -1;
	}
	return retchar;
}
