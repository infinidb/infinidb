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

// $Id$

/** @file */

#ifndef AUTOINCREMENTDATA_H__
#define AUTOINCREMENTDATA_H__

#include <stdint.h>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>


class AutoincrementData
{
public:
	typedef std::map <uint32_t, AutoincrementData*> AutoincDataMap;
	typedef std::map<uint32_t, long long> OIDNextValue;
	static AutoincrementData* makeAutoincrementData(uint32_t sessionID = 0);
	static void removeAutoincrementData(uint32_t sessionID = 0);
	void setNextValue(uint32_t columnOid, long long nextValue);
	long long getNextValue(uint32_t columnOid);
	OIDNextValue & getOidNextValueMap();

private:
	/** Constuctors */
    explicit AutoincrementData();
    explicit AutoincrementData(const AutoincrementData& rhs);
	~AutoincrementData();

	static boost::mutex map_mutex;
	static AutoincDataMap fAutoincDataMap;
    OIDNextValue fOidNextValueMap;
    boost::mutex fOIDnextvalLock;
};

#endif
// vim:ts=4 sw=4:
