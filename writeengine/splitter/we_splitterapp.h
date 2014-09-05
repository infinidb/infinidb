/*

   Copyright (C) 2009-2013 Calpont Corporation.

   Use of and access to the Calpont InfiniDB Community software is subject to the
   terms and conditions of the Calpont Open Source License Agreement. Use of and
   access to the Calpont InfiniDB Enterprise software is subject to the terms and
   conditions of the Calpont End User License Agreement.

   This program is distributed in the hope that it will be useful, and unless
   otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
   the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   Please refer to the Calpont Open Source License Agreement and the Calpont End
   User License Agreement for more details.

   You should have received a copy of either the Calpont Open Source License
   Agreement or the Calpont End User License Agreement along with this program; if
   not, it is your responsibility to review the terms and conditions of the proper
   Calpont license agreement by visiting http://www.calpont.com for the Calpont
   InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
   the Calpont InfiniDB Community Calpont Open Source License Agreement.

   Calpont may make changes to these license agreements from time to time. When
   these changes are made, Calpont will make a new copy of the Calpont End User
   License Agreement available at http://www.calpont.com and a new copy of the
   Calpont Open Source License Agreement available at http:///www.infinidb.org.
   You understand and agree that if you use the Program after the date on which
   the license agreement authorizing your use has changed, Calpont will treat your
   use as acceptance of the updated License.

*/

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
