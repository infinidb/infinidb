/*
  Copyright (C) 2009-2012 Calpont Corporation.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

// $Id: batchinsertprocessor.h 525 2010-01-19 23:18:05Z xlou $
//
/** @file */

#ifndef BATCHINSERTPROCESSOR_H__
#define BATCHINSERTPROCESSOR_H__

#include <stdint.h>
#include <queue>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include "insertdmlpackage.h"
#include "resourcemanager.h"
#include "bytestream.h"

namespace dmlprocessor 
{
class BatchInsertProc
{
public:
	typedef std::queue<messageqcpp::ByteStream > pkg_type;	
	typedef boost::shared_ptr<pkg_type>  SP_PKG;
	static BatchInsertProc* makeBatchInsertProc(execplan::CalpontSystemCatalog::SCN txnid, uint32_t tableOid);
	static void removeBatchInsertProc(int & rc, std::string & errMsg);
	~BatchInsertProc();

	SP_PKG  getInsertQueue ();
	uint getNumDBRoots();
	void addPkg(messageqcpp::ByteStream & insertBs, bool lastpkg = false, bool isAutocommitOn = true, uint32_t tableOid=0);
	messageqcpp::ByteStream getPkg(bool & lastPkg, bool & isAutocommitOn, uint32_t  &tableOid);
	void setError(int errorCode, std::string errMsg);
	void getError(int & errorCode, std::string & errMsg);
private:
	/** Constuctors */
    explicit BatchInsertProc();
    explicit BatchInsertProc(const BatchInsertProc& rhs);
	SP_PKG fInsertPkgQueue;
	boost::condition condvar;
	static BatchInsertProc* fInstance;
	static uint fNumDBRoots;
	static boost::thread* fProcessorThread;
	static execplan::CalpontSystemCatalog::SCN fTxnid;
	static int fErrorCode;
	static std::string fErrMsg;
	bool fLastpkg;
	bool fIsAutocommitOn;
	static uint32_t fTableOid;
};

} // namespace dmlprocessor
#endif
// vim:ts=4 sw=4:
