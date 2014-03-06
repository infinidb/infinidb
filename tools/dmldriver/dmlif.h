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

// $Id: dmlif.h 2101 2013-01-21 14:12:52Z rdempsey $

#ifndef DMLIF_H__
#define DMLIF_H__

#include <string>
#include <cstddef>
#include <boost/scoped_ptr.hpp>

#include "messagequeue.h"
#include "expressionparser.h"

namespace dmlif
{

class DMLIF
{
public:
	DMLIF(uint32_t sessionid, uint32_t tflg=0, bool dflg=false, bool vflg=false);
	~DMLIF();

	int sendOne(const std::string& stmt);

	void rf2Start(const std::string& sn);
	void rf2Add(int64_t okey);
	int rf2Send();

protected:
	int DMLSend(messageqcpp::ByteStream& bytestream, messageqcpp::ByteStream::octbyte& rows);

private:
	//DMLIF(const DMLIF& rhs);
	//DMLIF& operator=(const DMLIF& rhs);

	uint32_t fSessionID;
	uint32_t fTflg;
	bool fDflg;
	bool fVflg;

	boost::scoped_ptr<messageqcpp::MessageQueueClient> fMqp;

	std::string fSchema;
	std::string fOFilterStr;
	std::string fLFilterStr;

	execplan::ParseTree* fOPt;
	execplan::ParseTree* fLPt;
};

}

#endif

