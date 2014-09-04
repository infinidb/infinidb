/*

   Copyright (C) 2009-2012 Calpont Corporation.

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
 * we_filereadthread.h
 *
 *  Created on: Oct 25, 2011
 *      Author: bpaul
 */

#ifndef WE_FILEREADTHREAD_H_
#define WE_FILEREADTHREAD_H_

namespace WriteEngine
{

class WESDHandler;
class WEFileReadThread;

class WEReadThreadRunner
{
public:
	WEReadThreadRunner(WEFileReadThread& Owner):fRef(Owner)
	{		// ctor
	}
	~WEReadThreadRunner()
	{
	}

	void operator()();				// Thread function

private:
	WEFileReadThread& fRef;

};




//------------------------------------------------------------------------------


class WEFileReadThread
{
public:
	WEFileReadThread(WESDHandler& aSdh);
	virtual ~WEFileReadThread();

	void reset();
	void setup(std::string FileName);
	void shutdown();
    void feedData();
    unsigned int readDataFile(messageqcpp::SBS& Sbs);
    void openInFile();

    int getNextRow(std::istream& ifs, char*pBuf, int MaxLen);

    boost::thread* getFpThread() const  { return fpThread;  }
    bool isContinue() const  { 	return fContinue;  }
    void setContinue(bool fContinue) {	this->fContinue = fContinue; }
    std::string getInFileName() const {	return fInFileName; }
    unsigned int getTgtPmId() const { return fTgtPmId; }
    unsigned int getBatchQty() const { return fBatchQty; }
    void setFpThread(boost::thread* fpThread) {	this->fpThread = fpThread; }
    void setInFileName(std::string fInFileName)
    {
    	if((0==fInFileName.compare("STDIN"))||(0==fInFileName.compare("stdin")))
    		this->fInFileName = "/dev/stdin";
    	else
    		this->fInFileName = fInFileName;
    }
    //@BUG 4326
    const std::istream& getInFile() const { return fInFile; }
    void setBatchQty(unsigned int BatchQty) { fBatchQty = BatchQty;  }

    bool chkForListOfFiles(std::string& FileName);
    std::string getNextInputDataFile();
    void add2InputDataFileList(std::string& FileName);

private:
    enum { MAXBUFFSIZE=1024*1024 };

    // don't allow anyone else to set
    void setTgtPmId(unsigned int fTgtPmId) { this->fTgtPmId = fTgtPmId; }

    WESDHandler & fSdh;
    boost::thread *fpThread;
    boost::mutex fFileMutex;
    bool fContinue;
    std::string fInFileName;
    std::istream  fInFile;  //@BUG 4326
    std::ifstream fIfFile;  //@BUG 4326

    typedef std::list<std::string> strList;
    strList fInfileList;

    unsigned int fTgtPmId;
    unsigned int fBatchQty;
    bool fEnclEsc;						// Encl/Esc char is set
    char fEncl;							// Encl char
    char fEsc;							// Esc char
    char fDelim;						// Column Delimit char
    char fBuff[MAXBUFFSIZE];			// main data buffer
};

} /* namespace WriteEngine */
#endif /* WE_FILEREADTHREAD_H_ */
