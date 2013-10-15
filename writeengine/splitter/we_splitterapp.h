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

/*******************************************************************************
* $Id$
*
*******************************************************************************/
/*
 * we_splitterapp.h
 *
 *  Created on: Oct 7, 2011
 *      Author: bpaul
 */

#ifndef WE_SPLITTERAPP_H_
#define WE_SPLITTERAPP_H_

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "we_cmdargs.h"
#include "we_sdhandler.h"
#include "we_simplesyslog.h"
using namespace WriteEngine;

namespace WriteEngine
{


class WESplitterApp
{
public:
	WESplitterApp(WECmdArgs& CmdArgs);
	virtual ~WESplitterApp();



	void processMessages();
	int getMode(){ return fCmdArgs.getMode(); }
	bool getPmStatus(int Id){ return fCmdArgs.getPmStatus(Id); }
	std::string getLocFile() { return fCmdArgs.getLocFile(); }
	std::string getPmFile() { return fCmdArgs.getPmFile(); }
	void updateWithJobFile(int aIdx);


    // setup the signal handlers for the main app
    void setupSignalHandlers();
    static void onSigTerminate(int aInt);
    static void onSigInterrupt(int aInt);
    static void onSigHup(int aInt);

	void invokeCpimport();
	std::string getCalpontHome();
	std::string getPrgmPath(std::string& PrgmName);
	void updateCmdLineWithPath(string& CmdLine);

private:
public:	// for multi table support
	WECmdArgs& fCmdArgs;
    WESDHandler fDh;
    static bool fContinue;

public:
    static bool fSignaled;
    static bool fSigHup;

public:
    static SimpleSysLog* fpSysLog;

public:
    friend class WESDHandler;

};

} /* namespace WriteEngine */
#endif /* WE_SPLITTERAPP_H_ */
