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

// $Id: main.cpp 2336 2013-06-25 19:11:36Z rdempsey $
#include <unistd.h>
#include <cstdio>
#include <iostream>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
#include "loggingid.h"
using namespace logging;

namespace
{
void usage()
{
	cout << "usage: cplogger [-s subsys] [-cwi] [-h] msg_id [args...]" << endl;
}
}

int main(int argc, char** argv)
{
	int c;
	opterr = 0;
	bool cflg = true;
	bool wflg = false;
	unsigned subsysID = 8; //oamcpp

	while ((c = getopt(argc, argv, "s:cwih")) != EOF)
		switch (c)
		{
		case 'c':
			cflg = true;
			wflg = false;
			break;
		case 'w':
			cflg = false;
			wflg = true;
			break;
		case 'i':
			cflg = false;
			wflg = false;
			break;
		case 's':
			subsysID = strtoul(optarg, 0, 0);
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) < 1)
	{
		usage();
		return 1;
	}

	Message::MessageID mid = strtoul(argv[optind++], 0, 0);;
	Message::Args args;
	for (int i = optind; i < argc; i++)
		args.add(argv[optind++]);
	LoggingID logInfo(subsysID);
	Message msg(mid);
	msg.format(args);
	MessageLog log(logInfo);

	if (cflg)
		log.logCriticalMessage(msg);
	else if (wflg)
		log.logWarningMessage(msg);
	else
		log.logInfoMessage(msg);

	return 0;
}

