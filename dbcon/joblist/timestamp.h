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

#ifndef TIMESTAMP_H_
#define TIMESTAMP_H_
//
// $Id: timestamp.h 9210 2013-01-21 14:10:42Z rdempsey $
//

/** @file */

#include <string>
#include <ctime>
#include <sys/time.h>
#include <unistd.h>

namespace joblist
{

class JSTimeStamp {

public:
	JSTimeStamp();
	~JSTimeStamp() { };

	inline void setFirstInsertTime() {gettimeofday(&fFirstInsertTime,0);}
	inline void setLastInsertTime() {gettimeofday(&fLastInsertTime,0);}
	inline void setEndOfInputTime() {gettimeofday(&fEndofInputTime,0);}
	inline void setFirstReadTime() {gettimeofday(&fFirstReadTime,0);}
	inline void setLastReadTime() {gettimeofday(&fLastReadTime,0);}

	inline void setFirstInsertTime(const struct timeval& t) {fFirstInsertTime=t;}
	inline void setLastInsertTime(const struct timeval& t) {fLastInsertTime=t;}
	inline void setEndOfInputTime(const struct timeval& t) {fEndofInputTime=t;}
	inline void setFirstReadTime(const struct timeval& t) {fFirstReadTime=t;}
	inline void setLastReadTime(const struct timeval& t) {fLastReadTime=t;}

	inline const struct timeval FirstInsertTime() const {return fFirstInsertTime;}
	inline const struct timeval LastInsertTime() const {return fLastInsertTime;}
	inline const struct timeval EndOfInputTime() const {return fEndofInputTime;}
	inline const struct timeval FirstReadTime() const {return fFirstReadTime;}
	inline const struct timeval LastReadTime() const {return fLastReadTime;}

	const std::string FirstInsertTimeString() const { return format(fFirstInsertTime); }
	const std::string LastInsertTimeString() const { return format(fLastInsertTime); }
	const std::string EndOfInputTimeString() const { return format(fEndofInputTime); }
	const std::string FirstReadTimeString() const { return format(fFirstReadTime); }
	const std::string LastReadTimeString() const { return format(fLastReadTime); }

	//returns str rep of t2 - t1 in seconds
	static const std::string tsdiffstr(const struct timeval& t2, const struct timeval& t1);

	//returns str rep of tvbuf
	static const std::string format(const struct timeval& tvbuf);

	static const std::string timeNow();

protected:

private:
	struct timeval fFirstInsertTime;
	struct timeval fLastInsertTime;
	struct timeval fEndofInputTime;
	struct timeval fFirstReadTime;
	struct timeval fLastReadTime;
};

} //namespace joblist

#endif

